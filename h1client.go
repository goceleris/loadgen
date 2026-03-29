package loadgen

import (
	"bufio"
	"context"
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
	conns          []*h1Conn
	connCounters   []int  // per-worker round-robin counter (no sync needed)
	addr           string
	reqBuf         []byte // pre-formatted request bytes (immutable)
	keepAlive      bool
	connsPerWorker int
}

// h1Conn is a single persistent TCP connection with a buffered reader.
// Owned by exactly one worker — no synchronization needed.
type h1Conn struct {
	conn   net.Conn
	reader *bufio.Reader
	addr   string
}

// connsPerWorkerClose is the number of connections each worker owns in
// Connection: close mode. While one connection is reconnecting (~2-4ms
// round-trip), the worker processes requests on the others. Must be high
// enough that reconnect latency is fully hidden.
const connsPerWorkerClose = 16

// newH1Client creates a new zero-alloc HTTP/1.1 client.
func newH1Client(host, port, path, method string, headers map[string]string, body []byte, numWorkers int, keepAlive bool) (*h1Client, error) {
	addr := net.JoinHostPort(host, port)
	reqBuf := buildH1Request(method, path, host, port, headers, body, keepAlive)

	connsPerWorker := 1
	if !keepAlive {
		connsPerWorker = connsPerWorkerClose
	}
	numConns := numWorkers * connsPerWorker

	conns := make([]*h1Conn, numConns)
	for i := range numConns {
		conn, err := dialH1(addr)
		if err != nil {
			for j := range i {
				_ = conns[j].conn.Close()
			}
			return nil, fmt.Errorf("h1client: dial connection %d: %w", i, err)
		}
		conns[i] = &h1Conn{
			conn:   conn,
			reader: bufio.NewReaderSize(conn, 4096),
			addr:   addr,
		}
	}

	return &h1Client{
		conns:          conns,
		connCounters:   make([]int, numWorkers),
		addr:           addr,
		reqBuf:         reqBuf,
		keepAlive:      keepAlive,
		connsPerWorker: connsPerWorker,
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

func dialH1(addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if tcpConn, ok := conn.(*net.TCPConn); ok {
		_ = tcpConn.SetNoDelay(true)
		_ = tcpConn.SetReadBuffer(256 * 1024)
		_ = tcpConn.SetWriteBuffer(256 * 1024)
	}
	return conn, nil
}

// doRequest sends a pre-formatted HTTP/1.1 request and reads the response.
// Zero contention: each workerID maps to dedicated connection(s).
// For keep-alive: 1 connection per worker.
// For Connection: close: connsPerWorker connections, round-robin selection
// so reconnects are amortized across the pool.
func (c *h1Client) doRequest(ctx context.Context, workerID int) (int, error) {
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

	// Write request — single syscall for pre-formatted bytes
	_, err := hc.conn.Write(c.reqBuf)
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		// Reconnect and retry once
		if reconnErr := hc.reconnect(); reconnErr != nil {
			return 0, fmt.Errorf("h1client: reconnect failed: %w", reconnErr)
		}
		if _, err = hc.conn.Write(c.reqBuf); err != nil {
			return 0, fmt.Errorf("h1client: write after reconnect: %w", err)
		}
	}

	// Read status line: "HTTP/1.1 200 OK\r\n"
	statusLine, err := hc.reader.ReadSlice('\n')
	if err != nil {
		if ctx.Err() != nil {
			return 0, ctx.Err()
		}
		return 0, fmt.Errorf("h1client: read status: %w", err)
	}

	if len(statusLine) < 12 {
		return 0, fmt.Errorf("h1client: short status line")
	}
	statusCode := parseStatusCode(statusLine[9:12])
	if statusCode >= 400 {
		n := drainH1Response(hc.reader)
		return n, fmt.Errorf("status %d", statusCode)
	}

	// Read headers, find Content-Length
	contentLength := -1
	chunked := false

	for {
		line, err := hc.reader.ReadSlice('\n')
		if err != nil {
			return 0, fmt.Errorf("h1client: read header: %w", err)
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
		totalRead = contentLength
		if contentLength > 0 {
			discarded, err := hc.reader.Discard(contentLength)
			if err != nil {
				return discarded, fmt.Errorf("h1client: discard body: %w", err)
			}
		}
	} else if chunked {
		totalRead, err = readChunked(hc.reader)
		if err != nil {
			return totalRead, fmt.Errorf("h1client: read chunked: %w", err)
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

// readChunked reads a chunked transfer-encoded body, discarding all data.
func readChunked(r *bufio.Reader) (int, error) {
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
		n, _ := readChunked(r)
		return n
	}
	return 0
}

// reconnect closes and re-establishes the TCP connection.
func (hc *h1Conn) reconnect() error {
	_ = hc.conn.Close()
	conn, err := dialH1(hc.addr)
	if err != nil {
		return err
	}
	hc.conn = conn
	hc.reader.Reset(conn)
	return nil
}

// close closes all connections.
func (c *h1Client) close() {
	for _, hc := range c.conns {
		_ = hc.conn.Close()
	}
}
