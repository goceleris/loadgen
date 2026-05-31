package loadgen

import (
	"bufio"
	"crypto/rand"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"

	"context"
)

const (
	wsModeEcho      = "ws-echo"
	wsModeLargeEcho = "ws-large-echo"
	wsModeHub       = "ws-hub"
)

// RFC 6455 opcodes used by this client.
const (
	wsOpText   = 0x1
	wsOpBinary = 0x2
	wsOpClose  = 0x8
	wsOpPing   = 0x9
	wsOpPong   = 0xA
)

const wsLargePayloadSize = 64 * 1024

var errWSClosed = errors.New("loadgen: websocket connection closed by peer")

// wsClient drives WebSocket load. One connection (one upgrade) is kept open per
// worker; each DoRequest reuses it for a single echo round-trip or a single
// blocking broadcast read — one DoRequest == one unit through the existing
// worker/RecordSuccess/timeseries pipeline.
type wsClient struct {
	host     string // host:port to dial
	path     string // request path, e.g. /ws
	tls      bool
	srvMode  string // ?mode= value the server understands
	mode     string // public Config.Mode
	payload  []byte // outbound frame payload (nil for hub)
	insecure bool

	mu    sync.Mutex
	conns map[int]*wsConn
}

func newWSClient(host, port, path string, cfg Config) (Client, error) {
	srvMode := cfg.Mode // server selector values are identical to Config.Mode
	switch cfg.Mode {
	case wsModeEcho, wsModeLargeEcho, wsModeHub:
	default:
		return nil, fmt.Errorf("unknown mode %q", cfg.Mode)
	}

	secure := cfg.scheme == "https"
	if path == "" {
		path = "/"
	}

	var payload []byte
	switch cfg.Mode {
	case wsModeEcho:
		payload = []byte("payload")
	case wsModeLargeEcho:
		payload = make([]byte, wsLargePayloadSize)
		for i := range payload {
			payload[i] = byte('a' + i%26)
		}
	}

	return &wsClient{
		host:     net.JoinHostPort(host, port),
		path:     path,
		tls:      secure,
		srvMode:  srvMode,
		mode:     cfg.Mode,
		payload:  payload,
		insecure: cfg.InsecureSkipVerify,
		conns:    make(map[int]*wsConn),
	}, nil
}

func (c *wsClient) DoRequest(ctx context.Context, workerID int) (int, error) {
	conn, err := c.conn(workerID)
	if err != nil {
		return 0, err
	}

	if dl, ok := ctx.Deadline(); ok {
		_ = conn.raw.SetDeadline(dl)
	}

	n, err := c.exchange(conn)
	if err != nil {
		c.drop(workerID)
		return n, err
	}
	return n, nil
}

func (c *wsClient) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = make(map[int]*wsConn)
	c.mu.Unlock()
	for _, conn := range conns {
		conn.close()
	}
}

// exchange performs one unit of work on an already-upgraded connection.
func (c *wsClient) exchange(conn *wsConn) (int, error) {
	if c.mode != wsModeHub {
		if err := conn.writeFrame(wsOpText, c.payload); err != nil {
			return 0, err
		}
	}

	// Read frames until a data frame arrives, answering pings and honoring close.
	for {
		op, data, err := conn.readFrame()
		if err != nil {
			return 0, err
		}
		switch op {
		case wsOpText, wsOpBinary:
			return len(data), nil
		case wsOpPing:
			if err := conn.writeFrame(wsOpPong, data); err != nil {
				return 0, err
			}
		case wsOpClose:
			return 0, errWSClosed
		default:
			// Ignore pong/other control frames; keep waiting for data.
		}
	}
}

func (c *wsClient) conn(workerID int) (*wsConn, error) {
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

func (c *wsClient) drop(workerID int) {
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

func (c *wsClient) dial() (*wsConn, error) {
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

	if _, err := raw.Write([]byte(wsUpgradeRequest(c.host, c.path, c.srvMode))); err != nil {
		_ = raw.Close()
		return nil, err
	}

	br := bufio.NewReader(raw)
	if err := wsReadHandshake(br); err != nil {
		_ = raw.Close()
		return nil, err
	}
	return &wsConn{raw: raw, br: br}, nil
}

// wsConn is a single upgraded WebSocket connection bound to one worker.
type wsConn struct {
	raw net.Conn
	br  *bufio.Reader
}

func (c *wsConn) close() { _ = c.raw.Close() }

func (c *wsConn) writeFrame(opcode byte, payload []byte) error {
	_, err := c.raw.Write(wsFrame(opcode, payload))
	return err
}

func (c *wsConn) readFrame() (opcode byte, payload []byte, err error) {
	return wsReadFrame(c.br)
}

// wsUpgradeRequest builds the RFC 6455 client handshake, byte-compatible with
// probatorium/validation/ws.go so the celeris and gorilla_ws servers accept it.
func wsUpgradeRequest(host, path, mode string) string {
	q := ""
	if mode != "" {
		q = "?mode=" + mode
	}
	return fmt.Sprintf(
		"GET %s%s HTTP/1.1\r\n"+
			"Host: %s\r\n"+
			"Upgrade: websocket\r\n"+
			"Connection: Upgrade\r\n"+
			"Sec-WebSocket-Key: %s\r\n"+
			"Sec-WebSocket-Version: 13\r\n"+
			"\r\n",
		path, q, host, wsKey(),
	)
}

func wsKey() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return base64.StdEncoding.EncodeToString(b[:])
}

func wsReadHandshake(br *bufio.Reader) error {
	var sb strings.Builder
	for {
		line, err := br.ReadString('\n')
		if err != nil {
			return err
		}
		sb.WriteString(line)
		if line == "\r\n" || line == "\n" {
			break
		}
	}
	if !strings.Contains(sb.String(), " 101") {
		first := sb.String()
		if i := strings.IndexByte(first, '\n'); i >= 0 {
			first = first[:i]
		}
		return fmt.Errorf("loadgen: expected 101 Switching Protocols, got: %s", strings.TrimSpace(first))
	}
	return nil
}

// wsFrame builds a masked client frame with full RFC 6455 length encoding
// (≤125 inline, 126→16-bit, 127→64-bit). The 64-bit path is needed for
// ws-large-echo's 64 KiB payload — validation/ws.go only does the ≤125 case.
func wsFrame(opcode byte, payload []byte) []byte {
	var mask [4]byte
	_, _ = rand.Read(mask[:])

	n := len(payload)
	b0 := byte(0x80) | (opcode & 0x0f)

	var header []byte
	switch {
	case n <= 125:
		header = []byte{b0, byte(0x80) | byte(n)}
	case n <= 0xffff:
		header = []byte{b0, byte(0x80) | 126, byte(n >> 8), byte(n)}
	default:
		header = []byte{
			b0, byte(0x80) | 127,
			byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
			byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n),
		}
	}

	out := make([]byte, 0, len(header)+4+n)
	out = append(out, header...)
	out = append(out, mask[:]...)
	for i := 0; i < n; i++ {
		out = append(out, payload[i]^mask[i&3])
	}
	return out
}

// wsReadFrame reads one server frame. Server frames are unmasked, but the mask
// path is handled defensively to mirror validation/ws.go.
func wsReadFrame(br *bufio.Reader) (opcode byte, payload []byte, err error) {
	b0, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	opcode = b0 & 0x0f

	b1, err := br.ReadByte()
	if err != nil {
		return 0, nil, err
	}
	masked := b1&0x80 != 0
	length := int(b1 & 0x7f)

	switch length {
	case 126:
		var ext [2]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = int(ext[0])<<8 | int(ext[1])
	case 127:
		var ext [8]byte
		if _, err := io.ReadFull(br, ext[:]); err != nil {
			return 0, nil, err
		}
		length = 0
		for i := 0; i < 8; i++ {
			length = length<<8 | int(ext[i])
		}
	}

	var maskKey [4]byte
	if masked {
		if _, err := io.ReadFull(br, maskKey[:]); err != nil {
			return 0, nil, err
		}
	}

	payload = make([]byte, length)
	if _, err := io.ReadFull(br, payload); err != nil {
		return 0, nil, err
	}
	if masked {
		for i := 0; i < length; i++ {
			payload[i] ^= maskKey[i&3]
		}
	}
	return opcode, payload, nil
}
