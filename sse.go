package loadgen

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

const sseMode = "sse-fanout"

// sseClient drives Server-Sent-Events load. One long-lived GET /events stream
// is held open per worker; each DoRequest blocks until the next complete event
// is delivered and returns its data-payload length. Inter-event delivery time
// therefore becomes the recorded latency — one DoRequest == one event through
// the existing worker/RecordSuccess/timeseries pipeline.
type sseClient struct {
	host        string // host:port to dial
	path        string // request path, e.g. /events
	tls         bool
	insecure    bool
	headers     map[string]string
	dialTimeout time.Duration

	// closeCtx is cancelled by Close() so in-flight dials, handshakes and
	// backoff sleeps abort instead of outliving the client.
	closeCtx  context.Context
	closeStop context.CancelFunc

	// failFast aborts the run when no stream was EVER established and
	// connect attempts have failed for a sustained window. A client that
	// had live streams retries with backoff forever.
	failFast *failFastTracker

	mu       sync.Mutex
	conns    map[int]*sseConn
	backoffs map[int]*connectBackoff // per-worker reconnect pacing
}

func newSSEClient(host, port, path string, cfg Config) (Client, error) {
	if cfg.Mode != sseMode {
		return nil, fmt.Errorf("unknown mode %q", cfg.Mode)
	}
	if path == "" {
		path = "/"
	}
	closeCtx, closeStop := context.WithCancel(context.Background())
	return &sseClient{
		host:        net.JoinHostPort(host, port),
		path:        path,
		tls:         cfg.scheme == "https",
		insecure:    cfg.InsecureSkipVerify,
		headers:     cfg.Headers,
		dialTimeout: cfg.DialTimeout,
		closeCtx:    closeCtx,
		closeStop:   closeStop,
		failFast:    newFailFastTracker(failFastWindow),
		conns:       make(map[int]*sseConn),
		backoffs:    make(map[int]*connectBackoff),
	}, nil
}

func (c *sseClient) DoRequest(ctx context.Context, workerID int) (int, error) {
	conn, err := c.conn(ctx, workerID)
	if err != nil {
		return 0, err
	}
	if dl, ok := ctx.Deadline(); ok {
		_ = conn.raw.SetDeadline(dl)
	}
	n, err := conn.readEvent()
	if err != nil {
		c.drop(workerID)
		return 0, err
	}
	return n, nil
}

func (c *sseClient) Close() {
	c.closeStop() // abort in-flight dials, handshakes, and backoff sleeps
	c.mu.Lock()
	conns := c.conns
	c.conns = make(map[int]*sseConn)
	c.mu.Unlock()
	for _, conn := range conns {
		conn.close()
	}
}

func (c *sseClient) conn(ctx context.Context, workerID int) (*sseConn, error) {
	c.mu.Lock()
	if conn, ok := c.conns[workerID]; ok {
		c.mu.Unlock()
		return conn, nil
	}
	bo := c.backoffs[workerID]
	if bo == nil {
		bo = &connectBackoff{}
		c.backoffs[workerID] = bo
	}
	c.mu.Unlock()

	conn, err := c.dial(ctx)
	if err != nil {
		if ctx.Err() != nil || c.closeCtx.Err() != nil {
			return nil, err // shutdown, not a server failure
		}
		recordConnectError()
		if fatal := c.failFast.failure(time.Now(), err); fatal != nil {
			return nil, fatal
		}
		// One attempt per DoRequest: sleep the (growing) backoff before
		// surfacing the error so a dead server cannot induce a redial hot
		// loop, then let the worker loop call back in.
		bo.sleep(ctx, c.closeCtx.Done())
		return nil, err
	}
	c.failFast.success()
	bo.reset()

	c.mu.Lock()
	if c.closeCtx.Err() != nil {
		// Close() won the race while we were handshaking; don't leak the
		// conn into a map nobody will drain.
		c.mu.Unlock()
		conn.close()
		return nil, c.closeCtx.Err()
	}
	c.conns[workerID] = conn
	c.mu.Unlock()
	return conn, nil
}

func (c *sseClient) drop(workerID int) {
	c.mu.Lock()
	conn, ok := c.conns[workerID]
	if ok {
		delete(c.conns, workerID)
	}
	c.mu.Unlock()
	if ok {
		conn.close()
	}
}

// dial opens one SSE stream. The whole sequence — TCP/TLS connect plus the
// GET handshake — is bounded by DialTimeout and aborts on ctx cancellation
// or Close().
func (c *sseClient) dial(ctx context.Context) (*sseConn, error) {
	var cancel context.CancelFunc
	if c.dialTimeout > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.dialTimeout)
	} else {
		ctx, cancel = context.WithCancel(ctx)
	}
	defer cancel()
	// Propagate Close() into the dial context (and from there into the
	// in-flight connect and handshake I/O below).
	stopClose := context.AfterFunc(c.closeCtx, cancel)
	defer stopClose()

	var (
		raw net.Conn
		err error
	)
	if c.tls {
		d := &tls.Dialer{Config: &tls.Config{InsecureSkipVerify: c.insecure}} //nolint:gosec // InsecureSkipVerify is opt-in for self-signed test targets
		raw, err = d.DialContext(ctx, "tcp", c.host)
	} else {
		var d net.Dialer
		raw, err = d.DialContext(ctx, "tcp", c.host)
	}
	if err != nil {
		return nil, err
	}

	// The GET handshake is plain conn I/O with no deadline of its own;
	// closing the socket when ctx fires keeps it bounded too.
	stopIO := context.AfterFunc(ctx, func() { _ = raw.Close() })

	if _, err := raw.Write([]byte(sseGetRequest(c.host, c.path, c.headers))); err != nil {
		stopIO()
		_ = raw.Close()
		return nil, err
	}

	br := bufio.NewReader(raw)
	if err := sseReadHandshake(br); err != nil {
		stopIO()
		_ = raw.Close()
		return nil, err
	}
	if !stopIO() {
		// ctx fired while the handshake was completing; raw is closed (or
		// about to be) — treat as a failed dial.
		return nil, ctx.Err()
	}
	return &sseConn{raw: raw, br: br}, nil
}

// sseConn is a single open SSE stream bound to one worker.
type sseConn struct {
	raw net.Conn
	br  *bufio.Reader
}

func (c *sseConn) close() { _ = c.raw.Close() }

// readEvent reads one complete SSE event (terminated by a blank line),
// accumulating "data:" payloads and ignoring comments and other fields. It
// returns the byte length of the accumulated data payload.
func (c *sseConn) readEvent() (int, error) {
	var data strings.Builder
	sawData := false
	for {
		line, err := c.br.ReadString('\n')
		if err != nil {
			return 0, err
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			if sawData {
				return data.Len(), nil
			}
			continue // blank line before any data: keep waiting.
		}
		if strings.HasPrefix(line, ":") {
			continue // comment / heartbeat.
		}
		if v, ok := strings.CutPrefix(line, "data:"); ok {
			v = strings.TrimPrefix(v, " ")
			if sawData {
				data.WriteByte('\n')
			}
			data.WriteString(v)
			sawData = true
		}
		// event:, id:, retry: are not part of the payload length.
	}
}

// sseGetRequest builds the canonical SSE GET, byte-compatible with
// probatorium/validation/sse.go (sseGetRequest) so the celeris /events route
// accepts it.
func sseGetRequest(host, path string, headers map[string]string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "GET %s HTTP/1.1\r\n", path)
	fmt.Fprintf(&b, "Host: %s\r\n", host)
	b.WriteString("Accept: text/event-stream\r\n")
	b.WriteString("Cache-Control: no-cache\r\n")
	b.WriteString("Connection: keep-alive\r\n")
	for k, v := range headers {
		fmt.Fprintf(&b, "%s: %s\r\n", k, v)
	}
	b.WriteString("\r\n")
	return b.String()
}

// sseReadHandshake consumes the response head and asserts 200 +
// Content-Type: text/event-stream, leaving br positioned at the event body.
func sseReadHandshake(br *bufio.Reader) error {
	statusLine, err := br.ReadString('\n')
	if err != nil {
		return err
	}
	if !strings.Contains(statusLine, " 200") {
		return fmt.Errorf("loadgen: expected 200 OK, got: %s", strings.TrimSpace(statusLine))
	}
	gotSSE := false
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		if mt, _, ok := strings.Cut(line, ":"); ok && strings.EqualFold(strings.TrimSpace(mt), "content-type") {
			if strings.Contains(strings.ToLower(line), "text/event-stream") {
				gotSSE = true
			}
		}
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	if !gotSSE {
		return errors.New("loadgen: response is not text/event-stream")
	}
	return nil
}
