package loadgen

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"time"
)

// h1Client is a zero-allocation HTTP/1.1 benchmark client.
// For keep-alive: each worker owns one dedicated connection (1:1 mapping).
// For Connection: close: each worker owns connsPerWorker connections and
// round-robins through them, so while one is reconnecting after close,
// the worker can use the next. This amortizes reconnect latency and
// matches wrk's behavior of having many in-flight reconnects per thread.
type h1Client struct {
	conns           []*h1Conn
	connCounters    []int  // per-worker round-robin counter (no sync needed)
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

// h1Conn is a single persistent TCP connection with a buffered reader.
// Owned by exactly one worker — no synchronization needed.
type h1Conn struct {
	conn            net.Conn
	reader          *bufio.Reader
	addr            string
	dialTimeout     time.Duration
	readBufferSize  int
	writeBufferSize int
	scheme          string
	tlsConfig       *tls.Config
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

	// Custom headers (skip Connection if present — we already set it above)
	for k, v := range headers {
		if k == "Connection" {
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
		conn, err := tls.DialWithDialer(&net.Dialer{Timeout: dialTimeout}, "tcp", addr, tlsCfg)
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
	conn, err := net.DialTimeout("tcp", addr, dialTimeout)
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

// DoRequest sends a pre-formatted HTTP/1.1 request and reads the response.
// Zero contention: each workerID maps to dedicated connection(s).
// For keep-alive: 1 connection per worker.
// For Connection: close: connsPerWorker connections, round-robin selection
// so reconnects are amortized across the pool.
func (c *h1Client) DoRequest(ctx context.Context, workerID int) (int, error) {
	var hc *h1Conn
	if c.connsPerWorker == 1 {
		hc = c.conns[workerID%len(c.conns)]
	} else {
		// Round-robin through the worker's connection pool. For Connection:
		// close, the server closes each connection after responding. The next
		// Write() on a closed connection fails with broken pipe, triggering
		// the reconnect-and-retry path below. With 16 connections per worker,
		// reconnect latency (~2ms) is fully hidden behind the other 15 slots.
		base := (workerID % (len(c.conns) / c.connsPerWorker)) * c.connsPerWorker
		c.connCounters[workerID]++
		hc = c.conns[base+(c.connCounters[workerID]%c.connsPerWorker)]
	}

	connIdx := workerID % len(c.conns)

	// Write request — single syscall for pre-formatted bytes
	_, err := hc.conn.Write(c.reqBuf)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		// Reconnect and retry once
		if reconnErr := hc.reconnect(); reconnErr != nil {
			return 0, fmt.Errorf("h1client: conn[%d] reconnect failed: %w", connIdx, reconnErr)
		}
		if _, err = hc.conn.Write(c.reqBuf); err != nil {
			return 0, fmt.Errorf("h1client: conn[%d] write after reconnect: %w", connIdx, err)
		}
	}

	// Read status line: "HTTP/1.1 200 OK\r\n"
	statusLine, err := hc.reader.ReadSlice('\n')
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, fmt.Errorf("h1client: conn[%d] read status: %w", connIdx, err)
	}

	if len(statusLine) < 12 {
		return 0, fmt.Errorf("h1client: conn[%d] short status line", connIdx)
	}
	statusCode := parseStatusCode(statusLine[9:12])
	if statusCode >= 400 {
		n := drainH1Response(hc.reader)
		return n, fmt.Errorf("h1client: conn[%d] status %d", connIdx, statusCode)
	}

	// Read headers, find Content-Length
	contentLength := -1
	chunked := false

	for {
		line, err := hc.reader.ReadSlice('\n')
		if err != nil {
			return 0, fmt.Errorf("h1client: conn[%d] read header: %w", connIdx, err)
		}
		if len(line) <= 2 {
			break
		}
		if (line[0] == 'C' || line[0] == 'c') && len(line) > 16 {
			if cl := parseContentLengthHeader(line); cl >= 0 {
				contentLength = cl
			}
		}
		if (line[0] == 'T' || line[0] == 't') && len(line) > 26 {
			if isChunkedHeader(line) {
				chunked = true
			}
		}
	}

	// Read body
	totalRead := 0
	if contentLength >= 0 {
		// MaxResponseSize enforcement for content-length responses
		if c.maxResponseSize > 0 && int64(contentLength) > c.maxResponseSize {
			drainH1Response(hc.reader)
			return 0, fmt.Errorf("h1client: conn[%d] response body %d bytes exceeds MaxResponseSize %d", connIdx, contentLength, c.maxResponseSize)
		}
		totalRead = contentLength
		if contentLength > 0 {
			discarded, err := hc.reader.Discard(contentLength)
			if err != nil {
				return discarded, fmt.Errorf("h1client: conn[%d] discard body: %w", connIdx, err)
			}
		}
	} else if chunked {
		totalRead, err = readChunkedWithLimit(hc.reader, c.maxResponseSize)
		if err != nil {
			return totalRead, fmt.Errorf("h1client: conn[%d] read chunked: %w", connIdx, err)
		}
	}

	// For Connection: close, no explicit reconnect needed here. The server
	// closes the connection after responding (it saw "Connection: close").
	// On the next round-robin cycle back to this slot, the Write() will
	// fail with broken pipe, triggering the reconnect-and-retry path above.

	return totalRead, nil
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

// drainH1Response reads and discards the rest of an HTTP response (headers + body).
func drainH1Response(r *bufio.Reader) int {
	contentLength := -1
	chunked := false
	for {
		line, err := r.ReadSlice('\n')
		if err != nil || len(line) <= 2 {
			break
		}
		if (line[0] == 'C' || line[0] == 'c') && len(line) > 16 {
			if cl := parseContentLengthHeader(line); cl >= 0 {
				contentLength = cl
			}
		}
		if (line[0] == 'T' || line[0] == 't') && len(line) > 26 {
			if isChunkedHeader(line) {
				chunked = true
			}
		}
	}
	if contentLength > 0 {
		_, _ = r.Discard(contentLength)
		return contentLength
	}
	if chunked {
		n, _ := readChunkedWithLimit(r, -1)
		return n
	}
	return 0
}

// reconnect closes and re-establishes the TCP connection.
func (hc *h1Conn) reconnect() error {
	_ = hc.conn.Close()
	conn, err := dialH1(hc.addr, hc.scheme, hc.dialTimeout, hc.readBufferSize, hc.writeBufferSize, hc.tlsConfig)
	if err != nil {
		return err
	}
	hc.conn = conn
	hc.reader.Reset(conn)
	return nil
}

// Close closes all connections.
func (c *h1Client) Close() {
	for _, hc := range c.conns {
		_ = hc.conn.Close()
	}
}
