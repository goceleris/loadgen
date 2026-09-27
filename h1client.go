package loadgen

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

// h1Client is a zero-allocation HTTP/1.1 benchmark client.
// For keep-alive: each worker owns one dedicated connection (1:1 mapping).
// For Connection: close: each worker owns connsPerWorker connection slots
// and round-robins through them. Every close-mode request travels on a
// connection of its own: the slot's connection is closed after its
// response and the next request on the slot dials a fresh one (see
// DoRequest). The slots are dialed up front in newH1Client; after that
// first cycle the worker dials once per request, so PoolSize does not
// hide the dial.
type h1Client struct {
	conns           []*h1Conn
	connCounters    []int // per-worker round-robin counter (no sync needed)
	addr            string
	reqBuf          []byte // pre-formatted request bytes (immutable)
	keepAlive       bool
	connsPerWorker  int
	dialTimeout     time.Duration
	readBufferSize  int
	writeBufferSize int
	maxResponseSize int64
	scheme          string
	tlsConfig       *tls.Config
}

// h1Conn is one connection slot with a buffered reader.
// Owned by exactly one worker for the request/response I/O path; the mu
// guards conn/reader against concurrent access from (*h1Client).Close(),
// which is invoked from Benchmarker.Run BEFORE the worker WaitGroup drains
// in order to interrupt any in-flight I/O. mu is held only to swap the
// slot's connection (redial, finish, closeAfterPeer, Close), never across
// I/O: a connection is closed after it is released. A keep-alive request
// on a live connection takes no lock; a close-mode request takes it twice
// (redial and closeAfterPeer). Only the owning worker ever writes conn
// (under mu), so its own unlocked reads cannot race, and Close only reads
// it.
type h1Conn struct {
	mu sync.Mutex
	// conn is nil once the slot's connection is finished (closed by
	// finish); the next request on the slot dials a fresh one.
	conn   net.Conn
	reader *bufio.Reader
	// closed is set by (*h1Client).Close. A dial that completes after it
	// closes the new connection instead of installing it, so a worker
	// still running at shutdown cannot leak one.
	closed          bool
	addr            string
	dialTimeout     time.Duration
	readBufferSize  int
	writeBufferSize int
	scheme          string
	tlsConfig       *tls.Config

	// backoff paces reconnect attempts after a failed redial. Only the
	// owning worker touches it (redial runs on the worker goroutine), so it
	// needs no mu.
	backoff connectBackoff

	// peerCloseWait is defaultPeerCloseWait; a field so a test can widen
	// it. Read by the owning worker only.
	peerCloseWait time.Duration
}

// newH1Client creates a new zero-alloc HTTP/1.1 client.
func newH1Client(host, port, path string, cfg Config) (*h1Client, error) {
	addr := net.JoinHostPort(host, port)
	keepAlive := !cfg.DisableKeepAlive
	reqBuf := buildH1Request(cfg.Method, path, host, port, cfg.Headers, cfg.Body, keepAlive)
	scheme := cfg.scheme
	if scheme == "" {
		scheme = "http"
	}

	// Build TLS config for HTTPS connections
	var tlsCfg *tls.Config
	if scheme == "https" {
		if cfg.TLSConfig != nil {
			tlsCfg = cfg.TLSConfig.Clone()
		} else {
			tlsCfg = &tls.Config{}
		}
		if cfg.InsecureSkipVerify {
			tlsCfg.InsecureSkipVerify = true
		}
	}

	connsPerWorker := 1
	if !keepAlive {
		connsPerWorker = cfg.PoolSize
	}
	numConns := cfg.Workers * connsPerWorker

	conns := make([]*h1Conn, numConns)
	for i := range numConns {
		conn, err := dialH1(addr, scheme, cfg.DialTimeout, cfg.ReadBufferSize, cfg.WriteBufferSize, tlsCfg)
		if err != nil {
			for j := range i {
				_ = conns[j].conn.Close()
			}
			return nil, fmt.Errorf("h1client: dial conn[%d]: %w", i, err)
		}
		conns[i] = &h1Conn{
			conn:            conn,
			reader:          bufio.NewReaderSize(conn, 4096),
			addr:            addr,
			dialTimeout:     cfg.DialTimeout,
			readBufferSize:  cfg.ReadBufferSize,
			writeBufferSize: cfg.WriteBufferSize,
			scheme:          scheme,
			tlsConfig:       tlsCfg,
			peerCloseWait:   defaultPeerCloseWait,
		}
	}

	return &h1Client{
		conns:           conns,
		connCounters:    make([]int, cfg.Workers),
		addr:            addr,
		reqBuf:          reqBuf,
		keepAlive:       keepAlive,
		connsPerWorker:  connsPerWorker,
		dialTimeout:     cfg.DialTimeout,
		readBufferSize:  cfg.ReadBufferSize,
		writeBufferSize: cfg.WriteBufferSize,
		maxResponseSize: cfg.MaxResponseSize,
		scheme:          scheme,
		tlsConfig:       tlsCfg,
	}, nil
}

// buildH1Request constructs the raw HTTP/1.1 request bytes.
func buildH1Request(method, path, host, port string, headers map[string]string, body []byte, keepAlive bool) []byte {
	buf := make([]byte, 0, 256+len(body))

	// Request line
	buf = append(buf, method...)
	buf = append(buf, ' ')
	buf = append(buf, path...)
	buf = append(buf, " HTTP/1.1\r\n"...)

	// Host header
	buf = append(buf, "Host: "...)
	buf = append(buf, host...)
	buf = append(buf, ':')
	buf = append(buf, port...)
	buf = append(buf, "\r\n"...)

	// Connection header
	if keepAlive {
		buf = append(buf, "Connection: keep-alive\r\n"...)
	} else {
		buf = append(buf, "Connection: close\r\n"...)
	}

	// Custom headers. A Connection header, in any letter case (RFC 9110
	// §5.1), is skipped: the one above follows the client's mode, and the
	// mode decides whether the connection is reused (nextAfter).
	for k, v := range headers {
		if strings.EqualFold(k, "Connection") {
			continue
		}
		buf = append(buf, k...)
		buf = append(buf, ": "...)
		buf = append(buf, v...)
		buf = append(buf, "\r\n"...)
	}

	// Content-Length for POST
	if len(body) > 0 {
		buf = append(buf, "Content-Length: "...)
		buf = strconv.AppendInt(buf, int64(len(body)), 10)
		buf = append(buf, "\r\n"...)
	}

	buf = append(buf, "\r\n"...)

	if len(body) > 0 {
		buf = append(buf, body...)
	}

	return buf
}

func dialH1(addr, scheme string, dialTimeout time.Duration, readBufSize, writeBufSize int, tlsCfg *tls.Config) (net.Conn, error) {
	if scheme == "https" {
		conn, err := dialTLSRetry(addr, dialTimeout, tlsCfg)
		if err != nil {
			return nil, err
		}
		if tcpConn, ok := conn.NetConn().(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetReadBuffer(readBufSize)
			_ = tcpConn.SetWriteBuffer(writeBufSize)
		}
		return conn, nil
	}
	conn, err := dialTCPRetry(addr, dialTimeout)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetReadBuffer(readBufSize)
		_ = tcpConn.SetWriteBuffer(writeBufSize)
	}
	return conn, nil
}

// defaultPeerCloseWait bounds how long a connection that is done (see
// h1PeerCloses) waits for the server's FIN. A server that closes after its
// response sends the FIN right behind it, so the wait normally ends at once
// and the server, not loadgen, closes first and holds the TIME_WAIT. The
// bound only matters for a server that keeps the connection open (one that
// ignores Connection: close): the client then resets the connection, so
// the loadgen host keeps no TIME_WAIT either (see closeAfterPeer).
const defaultPeerCloseWait = 50 * time.Millisecond

// h1Next says what happens to a slot's connection after a response.
type h1Next uint8

const (
	// h1Reuse: the response was read completely; the connection is
	// positioned at the next response and carries the next request.
	h1Reuse h1Next = iota
	// h1Close: an error left the stream at an unknown position; close the
	// connection now.
	h1Close
	// h1PeerCloses: the response was read completely, but the connection
	// is done: the request carried Connection: close, or the response
	// did. Let the server close first (closeAfterPeer).
	h1PeerCloses
)

// DoRequest sends a pre-formatted HTTP/1.1 request and reads the response.
// Zero contention: each workerID maps to dedicated connection(s).
// For keep-alive: 1 connection per worker.
// For Connection: close: connsPerWorker slots, round-robin selection.
//
// A slot's connection carries another request only while it is known to
// be positioned at the start of the next response: keep-alive mode, the
// previous response read completely, and no Connection: close from the
// server. Otherwise (close mode, a response that announces
// Connection: close, or an error in the middle of a response) the
// connection leaves the slot right after the response and the next request
// on the slot dials a fresh one, inside that request, so the dial is part
// of that request's measured latency. So a request is never written into a
// connection whose request or response carried Connection: close, and the
// EOF of such a close is never counted as a failed request (loadgen#87). A
// genuine failure (a refused dial, a reset, a truncated body) fails its
// request, once.
//
// Two limits. A keep-alive connection the server closes without notice is
// seen only by the request written into it, which fails. It is not
// retried, although RFC 9112 §9.3.1 allows a retry of an idempotent method
// on a new connection when the old one closed before any response byte:
// loadgen#94. And "read completely" is only as good as readResponse's
// framing, which knows Content-Length and chunked; the cases it misreads
// are loadgen#93.
func (c *h1Client) DoRequest(ctx context.Context, workerID int) (int, error) {
	var connIdx int
	if c.connsPerWorker == 1 {
		connIdx = workerID % len(c.conns)
	} else {
		base := (workerID % (len(c.conns) / c.connsPerWorker)) * c.connsPerWorker
		c.connCounters[workerID]++
		connIdx = base + (c.connCounters[workerID] % c.connsPerWorker)
	}
	hc := c.conns[connIdx]

	// fresh: the connection is dialed by this request, so a failure to
	// write into it is not a stale keep-alive connection and is not retried.
	fresh := false
	if hc.conn == nil {
		if err := hc.redial(ctx, connIdx); err != nil {
			return 0, err
		}
		fresh = true
	}

	// Write request — single syscall for pre-formatted bytes
	if _, err := hc.conn.Write(c.reqBuf); err != nil {
		hc.finish()
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		if fresh {
			return 0, fmt.Errorf("h1client: conn[%d] write: %w", connIdx, err)
		}
		// A reused keep-alive connection the server closed while it sat
		// idle: retry once on a fresh connection.
		if err := hc.redial(ctx, connIdx); err != nil {
			return 0, err
		}
		if _, err := hc.conn.Write(c.reqBuf); err != nil {
			hc.finish()
			return 0, fmt.Errorf("h1client: conn[%d] write after reconnect: %w", connIdx, err)
		}
	}

	n, next, err := c.readResponse(hc.reader, connIdx)
	switch next {
	case h1PeerCloses:
		hc.closeAfterPeer()
	case h1Close:
		hc.finish()
	}
	if err != nil && ctx.Err() != nil {
		return 0, ctx.Err()
	}
	return n, err
}

// readResponse reads one response from r and says what to do with the
// connection afterwards. Any error that leaves the stream at an unknown
// position returns h1Close. The body is framed by Content-Length, else
// chunked, else taken as empty; RFC 9112 §6.3 has more cases (a
// close-delimited body, HEAD/1xx/204/304, Transfer-Encoding over
// Content-Length, trailers, an HTTP/1.0 status line), which this does not
// implement: loadgen#93.
func (c *h1Client) readResponse(r *bufio.Reader, connIdx int) (int, h1Next, error) {
	// Read status line: "HTTP/1.1 200 OK\r\n"
	statusLine, err := r.ReadSlice('\n')
	if err != nil {
		return 0, h1Close, fmt.Errorf("h1client: conn[%d] read status: %w", connIdx, err)
	}
	if len(statusLine) < 12 {
		return 0, h1Close, fmt.Errorf("h1client: conn[%d] short status line", connIdx)
	}
	statusCode := parseStatusCode(statusLine[9:12])

	// Read headers: Content-Length, Transfer-Encoding: chunked, and
	// Connection: close.
	contentLength := -1
	chunked := false
	peerCloses := false
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return 0, h1Close, fmt.Errorf("h1client: conn[%d] read header: %w", connIdx, err)
		}
		if len(line) <= 2 {
			break
		}
		if (line[0] == 'C' || line[0] == 'c') && len(line) > 16 {
			if cl := parseContentLengthHeader(line); cl >= 0 {
				contentLength = cl
			} else if isConnectionClose(line) {
				peerCloses = true
			}
		}
		if (line[0] == 'T' || line[0] == 't') && len(line) > 26 {
			if isChunkedHeader(line) {
				chunked = true
			}
		}
	}

	if statusCode >= 400 {
		// Discard the error body so a keep-alive connection stays in sync
		// for the next request. MaxResponseSize bounds it as it bounds a
		// success: a larger body is not read, and the connection closes.
		n, derr := discardH1Body(r, contentLength, chunked, c.maxResponseSize)
		next := c.nextAfter(peerCloses)
		if derr != nil {
			next = h1Close
		}
		return n, next, fmt.Errorf("h1client: conn[%d] status %d", connIdx, statusCode)
	}

	// Read body
	totalRead := 0
	if contentLength >= 0 {
		// MaxResponseSize enforcement for content-length responses. The
		// body is not read: the connection is dropped instead.
		if c.maxResponseSize > 0 && int64(contentLength) > c.maxResponseSize {
			return 0, h1Close, fmt.Errorf("h1client: conn[%d] response body %d bytes exceeds MaxResponseSize %d", connIdx, contentLength, c.maxResponseSize)
		}
		totalRead = contentLength
		if contentLength > 0 {
			discarded, err := r.Discard(contentLength)
			if err != nil {
				return discarded, h1Close, fmt.Errorf("h1client: conn[%d] discard body: %w", connIdx, err)
			}
		}
	} else if chunked {
		totalRead, err = readChunkedWithLimit(r, c.maxResponseSize)
		if err != nil {
			return totalRead, h1Close, fmt.Errorf("h1client: conn[%d] read chunked: %w", connIdx, err)
		}
	}

	return totalRead, c.nextAfter(peerCloses), nil
}

// nextAfter says what happens to a connection after a complete response.
// A connection whose request or response carried Connection: close is done:
// the client must not send another request on it (RFC 9112 §9.6), whether
// or not the server closes it.
func (c *h1Client) nextAfter(peerCloses bool) h1Next {
	if peerCloses || !c.keepAlive {
		return h1PeerCloses
	}
	return h1Reuse
}

// parseStatusCode parses a 3-digit status code from bytes without allocation.
func parseStatusCode(b []byte) int {
	return int(b[0]-'0')*100 + int(b[1]-'0')*10 + int(b[2]-'0')
}

// parseContentLengthHeader extracts the content length from a header line.
// Returns -1 if not a Content-Length header.
func parseContentLengthHeader(line []byte) int {
	if len(line) < 17 {
		return -1
	}
	if !asciiEqualFold(line[1:8], []byte("ontent-")) {
		return -1
	}
	if !asciiEqualFold(line[8:14], []byte("Length")) {
		return -1
	}
	i := 14
	for i < len(line) && (line[i] == ':' || line[i] == ' ') {
		i++
	}
	n := 0
	for i < len(line) && line[i] >= '0' && line[i] <= '9' {
		n = n*10 + int(line[i]-'0')
		i++
	}
	return n
}

// asciiEqualFold compares two ASCII byte slices case-insensitively.
func asciiEqualFold(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		ca, cb := a[i], b[i]
		if ca >= 'A' && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if cb >= 'A' && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}

// isChunkedHeader checks if a header line is "Transfer-Encoding: chunked".
func isChunkedHeader(line []byte) bool {
	if !asciiEqualFold(line[:18], []byte("Transfer-Encoding:")) {
		return false
	}
	i := 18
	for i < len(line) && line[i] == ' ' {
		i++
	}
	rem := line[i:]
	if len(rem) < 7 {
		return false
	}
	return asciiEqualFold(rem[:7], []byte("chunked"))
}

// isConnectionClose reports whether a header line is a Connection header
// whose option list contains "close" (RFC 9110 §7.6.1), case-insensitively.
func isConnectionClose(line []byte) bool {
	const name = "Connection:"
	if len(line) < len(name) || !asciiEqualFold(line[:len(name)], []byte(name)) {
		return false
	}
	v := line[len(name):]
	for len(v) > 0 {
		i := 0
		for i < len(v) && (v[i] == ' ' || v[i] == '\t' || v[i] == ',') {
			i++
		}
		v = v[i:]
		j := 0
		for j < len(v) && v[j] != ',' && v[j] != ' ' && v[j] != '\t' && v[j] != '\r' && v[j] != '\n' {
			j++
		}
		if j == 0 {
			return false // end of the line
		}
		if j == 5 && asciiEqualFold(v[:5], []byte("close")) {
			return true
		}
		v = v[j:]
	}
	return false
}

// readChunkedWithLimit reads a chunked transfer-encoded body, discarding all data.
// If maxSize > 0 and the total exceeds maxSize, returns an error.
func readChunkedWithLimit(r *bufio.Reader, maxSize int64) (int, error) {
	total := 0
	for {
		line, err := r.ReadSlice('\n')
		if err != nil {
			return total, err
		}
		size := 0
		for _, b := range line {
			if b >= '0' && b <= '9' {
				size = size*16 + int(b-'0')
			} else if b >= 'a' && b <= 'f' {
				size = size*16 + int(b-'a'+10)
			} else if b >= 'A' && b <= 'F' {
				size = size*16 + int(b-'A'+10)
			} else {
				break
			}
		}
		if size == 0 {
			_, _ = r.ReadSlice('\n')
			return total, nil
		}
		total += size
		if maxSize > 0 && int64(total) > maxSize {
			return total, fmt.Errorf("chunked body exceeds MaxResponseSize %d", maxSize)
		}
		if _, err := r.Discard(size + 2); err != nil {
			return total, err
		}
	}
}

// discardH1Body reads and discards a response body whose headers have
// been read. It returns the body length (the declared Content-Length, or
// the chunked total) and any read error. A body with neither framing is
// taken as empty. With maxSize > 0, a body larger than maxSize is an error:
// a Content-Length body is then not read at all, and a chunked one stops
// at the chunk that crosses the limit.
func discardH1Body(r *bufio.Reader, contentLength int, chunked bool, maxSize int64) (int, error) {
	if contentLength > 0 {
		if maxSize > 0 && int64(contentLength) > maxSize {
			return 0, fmt.Errorf("response body %d bytes exceeds MaxResponseSize %d", contentLength, maxSize)
		}
		_, err := r.Discard(contentLength)
		return contentLength, err
	}
	if chunked {
		return readChunkedWithLimit(r, maxSize)
	}
	return 0, nil
}

// redial dials a fresh connection into the slot, whose previous one is
// finished. A failed dial is a connect error: it is recorded, paced by the
// slot's backoff (a dead server cannot induce a redial hot loop: the v3.8
// crash cell logged 33.1M dial errors at ~370k/s), and returned, so the
// request that needed the connection fails. The dial runs without hc.mu,
// so Close is never stuck behind it; a dial that completes after Close is
// closed instead of installed.
func (hc *h1Conn) redial(ctx context.Context, connIdx int) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	conn, err := dialH1(hc.addr, hc.scheme, hc.dialTimeout, hc.readBufferSize, hc.writeBufferSize, hc.tlsConfig)
	if err != nil {
		recordConnectError()
		hc.backoff.sleep(ctx, nil)
		return fmt.Errorf("h1client: conn[%d] reconnect failed: %w", connIdx, err)
	}
	hc.mu.Lock()
	if hc.closed {
		hc.mu.Unlock()
		_ = conn.Close()
		return fmt.Errorf("h1client: conn[%d]: %w", connIdx, net.ErrClosed)
	}
	hc.conn = conn
	hc.reader.Reset(conn)
	hc.mu.Unlock()
	hc.backoff.reset()
	return nil
}

// finish closes the slot's connection and drops it; the next request on
// the slot dials a fresh one. The connection is released under mu and
// closed after it: a TLS close writes a close_notify alert.
func (hc *h1Conn) finish() {
	hc.mu.Lock()
	conn := hc.conn
	hc.conn = nil
	hc.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// closeAfterPeer detaches the slot's connection, which is done after a
// complete response, and ends it on a goroutine of its own, so the wait
// stays off the request's measured latency: the response is already
// complete. The slot is free at once; the next request on it dials a fresh
// connection.
//
// The goroutine waits for the server's TCP FIN, at most hc.peerCloseWait
// (see serverClosed):
//   - The FIN arrives (the server closes, as Connection: close asks): the
//     client closes after it, so the client keeps no TIME_WAIT; over plain
//     TCP the server holds it. Over TLS, a server that close()s its socket
//     right behind its close_notify (crypto/tls Conn.Close does) is no
//     longer reading when the client's close_notify arrives, so its kernel
//     answers that close_notify with an RST (Linux counts it in
//     TCPAbortOnData) and neither side keeps a TIME_WAIT. The close_notify
//     is still sent: RFC 5246 §7.2.1 and RFC 8446 §6.1 require it, and a
//     server doing a bidirectional shutdown waits for it (serverClosed).
//   - Anything else, normally the bound expiring on a server that keeps
//     the connection open: the client resets the connection (abortClose),
//     so neither side keeps a TIME_WAIT. A FIN from the client first would
//     leave a TIME_WAIT on the loadgen host for every request, and at
//     churn rates those exhaust its ephemeral ports: the run would then
//     report loadgen's own port ceiling as the server's connect errors.
//
// The goroutine is not tracked. It ends within hc.peerCloseWait (plus a
// TLS close_notify write), after Close and Run have returned if need be.
func (hc *h1Conn) closeAfterPeer() {
	hc.mu.Lock()
	conn := hc.conn
	hc.conn = nil
	hc.mu.Unlock()
	wait := hc.peerCloseWait
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(wait))
		if !serverClosed(conn) {
			abortClose(conn)
			return
		}
		_ = conn.Close()
	}()
}

// serverClosed reads conn until the server's TCP FIN and reports whether
// it arrived before conn's read deadline; anything else (the deadline, a
// byte, a reset) is false. Over TLS, Read returns io.EOF at the server's
// close_notify alert whether or not its FIN has arrived, and one whose FIN
// travels in a later segment would see the client close first. So after
// the TLS EOF the client sends its own close_notify (CloseWrite: the alert
// only, no TCP FIN), as RFC 5246 §7.2.1 and RFC 8446 §6.1 require, and then
// reads the raw TCP connection until its own EOF, under the same deadline.
// A server doing a bidirectional shutdown closes TCP only once that
// close_notify arrives; without it, its FIN would never come.
func serverClosed(conn net.Conn) bool {
	var b [1]byte
	if _, err := conn.Read(b[:]); !errors.Is(err, io.EOF) {
		return false
	}
	if tc, ok := conn.(*tls.Conn); ok {
		_ = tc.CloseWrite()
		if _, err := tc.NetConn().Read(b[:]); !errors.Is(err, io.EOF) {
			return false
		}
	}
	return true
}

// abortClose closes conn with an RST instead of a FIN (SO_LINGER 0), so
// the client keeps no TIME_WAIT for it. Over TLS, Close writes a
// close_notify alert into the socket first (unless serverClosed already
// sent one), but the linger-0 close then discards whatever of it is still
// unsent, so the alert may never reach the wire.
func abortClose(conn net.Conn) {
	raw := conn
	if tc, ok := conn.(*tls.Conn); ok {
		raw = tc.NetConn()
	}
	if tcp, ok := raw.(*net.TCPConn); ok {
		_ = tcp.SetLinger(0)
	}
	_ = conn.Close()
}

// Close closes all connections. Benchmarker.Run calls Close before draining
// the worker WaitGroup (to interrupt in-flight I/O), so we must take hc.mu
// to synchronise with concurrent finish/redial writes on the conn field.
// The connection is closed after mu is released; a worker that released
// the same connection may close it too, which is harmless.
func (c *h1Client) Close() {
	for _, hc := range c.conns {
		hc.mu.Lock()
		hc.closed = true
		conn := hc.conn
		hc.mu.Unlock()
		if conn != nil {
			_ = conn.Close()
		}
	}
}
