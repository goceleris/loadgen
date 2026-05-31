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
)

const sseMode = "sse-fanout"

// sseClient drives Server-Sent-Events load. One long-lived GET /events stream
// is held open per worker; each DoRequest blocks until the next complete event
// is delivered and returns its data-payload length. Inter-event delivery time
// therefore becomes the recorded latency — one DoRequest == one event through
// the existing worker/RecordSuccess/timeseries pipeline.
type sseClient struct {
	host     string // host:port to dial
	path     string // request path, e.g. /events
	tls      bool
	insecure bool
	headers  map[string]string

	mu    sync.Mutex
	conns map[int]*sseConn
}

func newSSEClient(host, port, path string, cfg Config) (Client, error) {
	if cfg.Mode != sseMode {
		return nil, fmt.Errorf("unknown mode %q", cfg.Mode)
	}
	if path == "" {
		path = "/"
	}
	return &sseClient{
		host:     net.JoinHostPort(host, port),
		path:     path,
		tls:      cfg.scheme == "https",
		insecure: cfg.InsecureSkipVerify,
		headers:  cfg.Headers,
		conns:    make(map[int]*sseConn),
	}, nil
}

func (c *sseClient) DoRequest(ctx context.Context, workerID int) (int, error) {
	conn, err := c.conn(workerID)
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
	c.mu.Lock()
	conns := c.conns
	c.conns = make(map[int]*sseConn)
	c.mu.Unlock()
	for _, conn := range conns {
		conn.close()
	}
}

func (c *sseClient) conn(workerID int) (*sseConn, error) {
	c.mu.Lock()
	if conn, ok := c.conns[workerID]; ok {
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()

	conn, err := c.dial()
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
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

func (c *sseClient) dial() (*sseConn, error) {
	var (
		raw net.Conn
		err error
	)
	if c.tls {
		raw, err = tls.Dial("tcp", c.host, &tls.Config{InsecureSkipVerify: c.insecure}) //nolint:gosec // InsecureSkipVerify is opt-in for self-signed test targets
	} else {
		raw, err = net.Dial("tcp", c.host)
	}
	if err != nil {
		return nil, err
	}

	if _, err := raw.Write([]byte(sseGetRequest(c.host, c.path, c.headers))); err != nil {
		_ = raw.Close()
		return nil, err
	}

	br := bufio.NewReader(raw)
	if err := sseReadHandshake(br); err != nil {
		_ = raw.Close()
		return nil, err
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
