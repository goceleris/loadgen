package loadgen

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2/hpack"
)

// Hot-path errors are pre-allocated and shared across goroutines so the
// 4xx/5xx response storm + RST_STREAM flood (e.g. when the server hits
// SETTINGS_MAX_CONCURRENT_STREAMS) doesn't cap loadgen throughput on
// allocator/string-building. These types implement error and expose
// the structured fields for callers that want to inspect them.

// HTTP2StatusError is the error returned by [h2Client.DoRequest] for
// non-2xx responses. The receiver is a pointer to a pre-allocated
// instance per status code in the 100-599 range — never construct one
// yourself, and don't compare error strings (use errors.As instead).
type HTTP2StatusError struct{ Status int }

// Error renders the canonical loadgen string. Stable across loadgen
// versions for log scraping; the structured Status field is the
// programmatic accessor.
func (e *HTTP2StatusError) Error() string {
	return "h2client: status " + strconv.Itoa(e.Status)
}

// HTTP2ResetError is returned by DoRequest when the server reset the
// stream with RST_STREAM. Pre-allocated for known H2 error codes to
// avoid the per-call fmt.Errorf alloc that capped throughput when
// MaxConcurrentStreams was exceeded.
type HTTP2ResetError struct{ Code uint32 }

func (e *HTTP2ResetError) Error() string {
	return "h2client: stream reset code=" + strconv.FormatUint(uint64(e.Code), 10)
}

var (
	// 100..599 covers every legal HTTP status; 600 entries is a 4.8 KB
	// table on 64-bit. Indexed directly by status int.
	statusErrors    [600]*HTTP2StatusError
	resetErrorsHot  [16]*HTTP2ResetError // H2 error codes 0..14 are defined; 15 reserved
	resetErrorOther = &HTTP2ResetError{Code: ^uint32(0)}
)

func init() {
	for i := 100; i < 600; i++ {
		statusErrors[i] = &HTTP2StatusError{Status: i}
	}
	for i := uint32(0); i < uint32(len(resetErrorsHot)); i++ {
		resetErrorsHot[i] = &HTTP2ResetError{Code: i}
	}
}

// statusError returns a shared *HTTP2StatusError for any int status
// in [100, 600). Returns a freshly-allocated one for out-of-range
// status (which shouldn't happen on a valid HTTP response).
func statusError(status int) error {
	if status < 0 || status >= len(statusErrors) {
		return &HTTP2StatusError{Status: status}
	}
	if e := statusErrors[status]; e != nil {
		return e
	}
	return &HTTP2StatusError{Status: status}
}

// resetError returns a shared *HTTP2ResetError for known codes,
// falling back to resetErrorOther for unknown codes (rare; the
// caller can still detect "this was a reset" via errors.As).
func resetError(code uint32) error {
	if code < uint32(len(resetErrorsHot)) {
		return resetErrorsHot[code]
	}
	return resetErrorOther
}

// h2Client is a zero-allocation HTTP/2 benchmark client.
// Uses pre-encoded HPACK headers, dedicated writer goroutine per connection,
// lock-free stream slot dispatch, and batched WINDOW_UPDATE writes.
type h2Client struct {
	conns       []*h2ConnSlot
	headerBlock []byte // pre-encoded HPACK header block (immutable)
	dataPayload []byte // body bytes for POST (nil for GET)
	hasBody     bool

	// redial re-establishes a single connection (prior-knowledge or h2c
	// upgrade, whichever this client was built with). A server that closes
	// or GOAWAYs a connection mid-cell — hypercorn does this periodically —
	// would otherwise strand the slot dead and the worker would spin
	// closed-conn errors with no recovery (fastapi-h2 logged ~1.1B errors /
	// 0 requests from exactly this). reconnectSlot calls it under the slot
	// lock, paced by the slot's backoff while the server looks down.
	redial func() (*h2Conn, error)

	// dialedViaUpgrade reports whether this client's connections were
	// established via the h2c upgrade handshake vs prior-knowledge H2.
	// Populated by newH2CUpgradeClient so the benchmark report can show
	// "upgraded X/Y conns successfully".
	dialedViaUpgrade bool
	// upgradeAttempted is the configured connection count (i.e. the number
	// of upgrade handshakes attempted). Equal to len(conns) on success;
	// New() currently fails the whole benchmark on any dial error so these
	// are equal in the happy path.
	upgradeAttempted int
}

// h2ConnSlot owns one logical connection that can be re-dialed in place. cur
// holds the live *h2Conn (swapped atomically on reconnect so other workers
// observe the new one without locking); mu single-flights the redial so a
// fleet of workers sharing the slot dials once, not N times; backoff paces
// retries against a server that stays down.
type h2ConnSlot struct {
	cur     atomic.Pointer[h2Conn]
	mu      sync.Mutex
	backoff connectBackoff
}

// h2WriteReq is a frame write request submitted to the writer goroutine.
// For HEADERS: writeLoop allocates the streamID and registers the stream slot,
// eliminating the need for a mutex on the worker side.
type h2WriteReq struct {
	kind     uint8
	block    []byte           // HEADERS: header block fragment
	data     []byte           // HEADERS w/ body: data payload
	hasBody  bool             // HEADERS: has data frames to follow
	pingData [8]byte          // PING: response data
	respCh   *chan h2Response // HEADERS: writeLoop registers this in the stream slot
}

const (
	h2WriteHeaders uint8 = iota
	_                    // was h2WriteWindowUpdate — now handled via atomic counter
	h2WriteSettingsAck
	h2WritePing
	h2WriteGoAway
)

// h2Conn is a single HTTP/2 connection with a framer.
type h2Conn struct {
	conn      net.Conn
	framer    *h2Framer
	bufWriter *bufio.Writer // buffered writer for batching frame writes

	// Writer goroutine — worker requests go through writeCh.
	// readLoop NEVER sends to writeCh to avoid deadlock.
	writeCh chan h2WriteReq

	// Stream management — lock-free fixed-size slot array.
	// Index = (streamID >> 1) % len(streamSlots), sized at 2x
	// effectiveStreams. The semaphore limits concurrent streams, but a slow
	// or abandoned stream can still hold its slot when the IDs come round
	// to it again, so writeLoop skips an ID whose slot is held: no two
	// pending streams share a slot.
	nextStreamID atomic.Uint32
	streamSlots  []h2StreamSlot
	// firstStreamID is the connection's first request stream: 1, or 3 on an
	// h2c-upgraded connection, whose stream 1 is the upgrade request itself.
	firstStreamID uint32

	// Channel pool — pools *chan h2Response (heap-allocated pointers).
	// The pointer in the slot remains valid even after the goroutine exits.
	chanPool sync.Pool

	// Flow control — readLoop accumulates via atomic add,
	// writeLoop flushes between processing worker requests.
	//
	// maxFrameSize is the SERVER's SETTINGS_MAX_FRAME_SIZE (default 16384):
	// DATA frames we SEND must not exceed it or the server replies
	// FRAME_SIZE_ERROR and tears down the conn (the post-64k-h2 failure —
	// a 64KiB frame against a 16384-default server).
	maxFrameSize      uint32
	pendingConnWindow atomic.Uint32

	// SEND-side flow control (data WE send to the server). The conn window
	// starts at 65535 (RFC 7540 §6.9.2, NOT affected by SETTINGS); the
	// per-stream window starts at the server's SETTINGS_INITIAL_WINDOW_SIZE.
	// The writer goroutine is sequential (one body at a time), so the
	// currently-writing stream's window lives in curStreamWindow keyed by
	// curStreamID; readLoop replenishes both on WINDOW_UPDATE. Without this a
	// 64KiB body (post-64k-h2 = 65536 B) exceeds the 65535 window by one byte
	// and the request hangs until the run ends. curStreamReset is the ID of
	// that stream once the server has reset it (RST_STREAM): the body writer
	// then stops, and sends no RST_STREAM of its own (RFC 9113 §5.4.2).
	connSendWindow   atomic.Int64
	curStreamID      atomic.Uint32
	curStreamWindow  atomic.Int64
	curStreamReset   atomic.Uint32
	serverInitWindow uint32

	// Concurrency limit
	streamSem chan struct{}

	// Shutdown signal — closed by closeConn (once, under closeOnce) to unblock
	// readLoop/writeLoop/workers, whether the client closes the connection or
	// it dies under the client (failConn, from readLoop or writeLoop).
	done      chan struct{}
	closeOnce sync.Once

	// loopsDone is closed once readLoop and writeLoop have both returned
	// (loopsLeft counts them down), which they do soon after done closes:
	// closeConn closes the socket too. Each has sent by then every answer it
	// will ever send, so await waits for it before it decides that a request
	// on a closed connection got no answer.
	loopsDone chan struct{}
	loopsLeft atomic.Int32

	addr string
	// closed marks a connection that takes no new request: DoRequest
	// redials the slot instead. Set before done is closed.
	closed atomic.Bool
	// served is set by readLoop once the connection carries a response
	// frame for one of the run's requests (not the h2c upgrade's own stream
	// 1). A connection that ended without one shows the server down or
	// dropping every connection: reconnectSlot fails the request that finds
	// it, after the slot's backoff.
	served atomic.Bool
}

// h2StreamSlot holds an atomic pointer to a response channel.
// writeLoop stores; readLoop loads and clears.
//
// The other fields are the response state of the stream that last sent a
// frame for this slot. Only readLoop touches them, so they need no
// synchronisation, and they are keyed by stream ID: the first frame of a new
// stream resets them (see h2Conn.stream), so nothing leaks from a stream
// that used the slot before, whether it finished, was reset or was
// abandoned.
type h2StreamSlot struct {
	ch atomic.Pointer[chan h2Response]

	streamID uint32
	status   int  // :status of the response; 0 when absent or not parseable
	final    bool // the final (non-1xx) response HEADERS arrived; a later HEADERS frame carries trailers
	bytes    int  // body bytes: the data of every DATA frame so far, padding excluded
}

// h2Response is the result dispatched from the read loop to a waiting worker.
type h2Response struct {
	status    int
	bytesRead int
	err       error
}

const (
	h2InitialWindowSize = 16 << 20 // 16MB
	h2MaxFrameSize      = 64 << 10 // 64KB
	h2ClientPreface     = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"
)

// h2StaticStatus maps HPACK static table indices 8-14 to HTTP status codes.
var h2StaticStatus = [7]int{200, 204, 206, 304, 400, 404, 500}

// extractStatus extracts the HTTP status code from an HPACK-encoded header block
// without allocating. Handles indexed representations from the HPACK static table
// (covers >99% of benchmark responses) and falls back to literal parsing.
// Returns 0 for a block without a :status it can read, which DoRequest counts
// as an error (errH2NoStatus), never as a success.
func extractStatus(headerBlock []byte) int {
	if len(headerBlock) == 0 {
		return 0
	}

	pos := 0

	// Skip dynamic table size updates (RFC 7541 Section 6.3: 001xxxxx).
	// When we set SettingHeaderTableSize=0, the server sends a single 0x20 byte
	// at the start of the first response's header block.
	for pos < len(headerBlock) && headerBlock[pos]&0xE0 == 0x20 {
		if headerBlock[pos]&0x1F < 31 {
			pos++ // single byte size update
		} else {
			// Multi-byte integer encoding for values >= 31
			pos++
			for pos < len(headerBlock) && headerBlock[pos]&0x80 != 0 {
				pos++
			}
			if pos < len(headerBlock) {
				pos++
			}
		}
	}

	if pos >= len(headerBlock) {
		return 0
	}

	b := headerBlock[pos]

	// Fast path: indexed header field (Section 6.1) from HPACK static table.
	// 0x88=:status 200, 0x89=204, 0x8A=206, 0x8B=304, 0x8C=400, 0x8D=404, 0x8E=500
	if b >= 0x88 && b <= 0x8E {
		return h2StaticStatus[b-0x88]
	}

	// Fallback: literal header field whose name is a static :status entry.
	// Static indices 8-14 all name :status, and encoders pick any of them:
	// x/net's (net/http's) names it by the LAST match, 14, so a literal 503
	// arrives as 0x0e 0x03 "503". Safe because we disabled the HPACK dynamic
	// table (SettingHeaderTableSize=0).
	var nameIdx int
	pos++

	switch {
	case b&0xC0 == 0x40: // Incremental indexing (6-bit prefix)
		nameIdx = int(b & 0x3F)
	case b&0xF0 == 0x00: // Without indexing (4-bit prefix)
		nameIdx = int(b & 0x0F)
	case b&0xF0 == 0x10: // Never indexed (4-bit prefix)
		nameIdx = int(b & 0x0F)
	default:
		return 0
	}

	if nameIdx < 8 || nameIdx > 14 {
		// Name index 0: the name is a string literal (RFC 7541 §6.2), legal
		// if unusual for a name the static table holds. Accept ":status",
		// plain or Huffman-coded, and go on as for a static :status name.
		if nameIdx != 0 || pos >= len(headerBlock) {
			return 0
		}
		nameByte := headerBlock[pos]
		pos++
		nameLen := int(nameByte & 0x7F)
		if pos+nameLen > len(headerBlock) {
			return 0
		}
		name := headerBlock[pos : pos+nameLen]
		pos += nameLen
		if nameByte&0x80 != 0 {
			if string(name) != h2HuffmanStatusName {
				return 0
			}
		} else if string(name) != ":status" {
			return 0
		}
	}
	if pos >= len(headerBlock) {
		return 0
	}

	valueByte := headerBlock[pos]
	pos++

	valueLen := int(valueByte & 0x7F)
	if pos+valueLen > len(headerBlock) {
		return 0
	}
	value := headerBlock[pos : pos+valueLen]

	// A Huffman-coded value (RFC 7541 §5.2). Encoders pick it whenever it is
	// shorter than the literal, which for a status is every code with at
	// least two digits from {0, 1, 2}: 401, 402, 410, 412, 502, 103, 201,
	// 301 and more (403 and 503 are not). net/http's server sends them so.
	if valueByte&0x80 != 0 {
		return h2HuffmanStatus[string(value)] // 0 when it is not a 3-digit status
	}

	if valueLen != 3 {
		return 0
	}
	return parseStatusCode(value)
}

// h2HuffmanStatusName is the HPACK Huffman encoding of the name ":status".
var h2HuffmanStatusName = string(hpack.AppendHuffmanString(nil, ":status"))

// h2HuffmanStatus maps the HPACK Huffman encoding of every status code
// 100-599 to the code, so extractStatus decodes a Huffman-coded :status with
// one map lookup (string(b) in a map index does not allocate).
var h2HuffmanStatus = func() map[string]int {
	m := make(map[string]int, 500)
	for status := 100; status < 600; status++ {
		m[string(hpack.AppendHuffmanString(nil, strconv.Itoa(status)))] = status
	}
	return m
}()

// newH2Client creates a new zero-alloc HTTP/2 client.
func newH2Client(host, port, path string, cfg Config) (*h2Client, error) {
	return newH2ClientWithDialer(host, port, path, cfg, false)
}

// newH2CUpgradeClient creates a new zero-alloc HTTP/2 client that establishes
// each connection via the RFC 7540 §3.2 h2c upgrade handshake (starts H1,
// negotiates the upgrade, then switches to H2 on the same TCP conn).
func newH2CUpgradeClient(host, port, path string, cfg Config) (*h2Client, error) {
	return newH2ClientWithDialer(host, port, path, cfg, true)
}

func newH2ClientWithDialer(host, port, path string, cfg Config, upgrade bool) (*h2Client, error) {
	addr := net.JoinHostPort(host, port)
	scheme := cfg.scheme
	if scheme == "" {
		scheme = "http"
	}
	if upgrade && scheme == "https" {
		return nil, fmt.Errorf("h2client: h2c upgrade is only defined over cleartext (got scheme %q)", scheme)
	}
	headerBlock := buildHPACKHeaders(cfg.Method, host, port, path, cfg.Headers, len(cfg.Body), scheme)
	hasBody := len(cfg.Body) > 0
	numConns := cfg.HTTP2Options.Connections
	maxStreams := cfg.HTTP2Options.MaxStreams

	// Build TLS config for HTTPS connections
	var tlsCfg *tls.Config
	if scheme == "https" {
		if cfg.TLSConfig != nil {
			tlsCfg = cfg.TLSConfig.Clone()
		} else {
			tlsCfg = &tls.Config{}
		}
		tlsCfg.NextProtos = []string{"h2"}
		if cfg.InsecureSkipVerify {
			tlsCfg.InsecureSkipVerify = true
		}
	}

	// redial captures the dial parameters so a slot can re-establish its
	// connection after the server closes/GOAWAYs it mid-cell. Used for both
	// the initial dials below and reconnectSlot.
	redial := func() (*h2Conn, error) {
		if upgrade {
			return dialH2CUpgrade(addr, scheme, path, host, port, maxStreams, cfg.DialTimeout, cfg.ReadBufferSize, cfg.WriteBufferSize, tlsCfg)
		}
		return dialH2(addr, scheme, maxStreams, cfg.DialTimeout, cfg.ReadBufferSize, cfg.WriteBufferSize, tlsCfg)
	}

	conns := make([]*h2ConnSlot, numConns)
	for i := range numConns {
		hc, err := redial()
		if err != nil {
			for j := range i {
				if c := conns[j].cur.Load(); c != nil {
					c.closeConn()
				}
			}
			return nil, fmt.Errorf("h2client: dial conn[%d]: %w", i, err)
		}
		slot := &h2ConnSlot{}
		slot.cur.Store(hc)
		conns[i] = slot
	}

	var payload []byte
	if hasBody {
		payload = make([]byte, len(cfg.Body))
		copy(payload, cfg.Body)
	}

	return &h2Client{
		conns:            conns,
		headerBlock:      headerBlock,
		dataPayload:      payload,
		hasBody:          hasBody,
		redial:           redial,
		dialedViaUpgrade: upgrade,
		upgradeAttempted: numConns,
	}, nil
}

// reconnectSlot replaces a slot's dead connection, single-flighted under the
// slot lock: a caller that finds the slot already healed takes the new
// connection. When the server looks down, it fails the request instead, as
// h1client fails a request whose redial fails: the dial failed (a connect
// error), or the connection being replaced ended before it answered a single
// request (errH2NeverServed: the server completes handshakes and drops them,
// and nothing is dialed for this request). That request then sleeps the
// slot's backoff after it releases the lock, so the sleep paces the server,
// lands in no recorded latency (a failed request records none) and holds up
// none of the slot's other workers; the next request dials at once. The
// backoff doubles while the server keeps looking down and starts over once a
// connection has served. After the run's context ends it dials nothing.
func (c *h2Client) reconnectSlot(ctx context.Context, slot *h2ConnSlot) (*h2Conn, error) {
	slot.mu.Lock()
	old := slot.cur.Load()
	if old != nil && !old.closed.Load() {
		slot.mu.Unlock()
		return old, nil // another worker already re-dialed this slot
	}
	if ctx.Err() != nil {
		slot.mu.Unlock()
		return nil, ctx.Err() // Close, which runs after the cancel, would never close a connection dialed now
	}

	var err error
	if old != nil {
		old.closeConn() // release fds/goroutines of the dead conn
		if old.served.Load() {
			slot.backoff.reset() // the server was up: redial at once, and the pace starts over
		} else {
			err = errH2NeverServed
		}
	}
	if err == nil {
		hc, dialErr := c.redial()
		if dialErr == nil {
			slot.cur.Store(hc)
			slot.mu.Unlock()
			return hc, nil
		}
		recordConnectError()
		err = dialErr
	}

	// The server looks down. The next request dials at once; this one fails
	// after the backoff, which it sleeps outside the lock.
	slot.cur.Store(nil)
	pause := slot.backoff.step()
	slot.mu.Unlock()
	sleepFor(ctx, nil, pause)
	return nil, err
}

// h2HopByHopHeaders lists HTTP/1.1 connection-specific headers that are
// forbidden in HTTP/2 (RFC 9113 Section 8.2.2). These must be stripped
// when encoding request headers for H2.
var h2HopByHopHeaders = map[string]struct{}{
	"connection":        {},
	"keep-alive":        {},
	"proxy-connection":  {},
	"transfer-encoding": {},
	"upgrade":           {},
}

// buildHPACKHeaders pre-encodes the HPACK header block for reuse.
// Hop-by-hop headers (Connection, Keep-Alive, etc.) are automatically
// stripped per RFC 9113 Section 8.2.2.
// The optional schemes parameter overrides the :scheme pseudo-header (default "http").
func buildHPACKHeaders(method, host, port, path string, headers map[string]string, bodyLen int, schemes ...string) []byte {
	s := "http"
	if len(schemes) > 0 && schemes[0] != "" {
		s = schemes[0]
	}
	var w hpackWriter
	enc := hpack.NewEncoder(&w)
	enc.SetMaxDynamicTableSizeLimit(0)

	_ = enc.WriteField(hpack.HeaderField{Name: ":method", Value: method})
	_ = enc.WriteField(hpack.HeaderField{Name: ":scheme", Value: s})
	_ = enc.WriteField(hpack.HeaderField{Name: ":authority", Value: net.JoinHostPort(host, port)})
	_ = enc.WriteField(hpack.HeaderField{Name: ":path", Value: path})

	for k, v := range headers {
		lower := strings.ToLower(k)
		if _, hop := h2HopByHopHeaders[lower]; hop {
			continue
		}
		// HTTP/2 requires lowercase header names (RFC 9113 Section 8.2.1).
		_ = enc.WriteField(hpack.HeaderField{Name: lower, Value: v})
	}

	if bodyLen > 0 {
		_ = enc.WriteField(hpack.HeaderField{Name: "content-length", Value: itoa(bodyLen)})
	}

	buf := make([]byte, len(w.buf))
	copy(buf, w.buf)
	return buf
}

// hpackWriter is a simple io.Writer that appends to a byte slice.
type hpackWriter struct {
	buf []byte
}

func (w *hpackWriter) Write(p []byte) (int, error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

func dialH2(addr, scheme string, maxStreams int, dialTimeout time.Duration, readBufSize, writeBufSize int, tlsCfg *tls.Config) (*h2Conn, error) {
	var conn net.Conn
	var err error
	if scheme == "https" {
		tlsConn, tlsErr := dialTLSRetry(addr, dialTimeout, tlsCfg)
		if tlsErr != nil {
			return nil, tlsErr
		}
		conn = tlsConn
		if tcpConn, ok := tlsConn.NetConn().(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetReadBuffer(readBufSize)
			_ = tcpConn.SetWriteBuffer(writeBufSize)
		}
	} else {
		conn, err = dialTCPRetry(addr, dialTimeout)
		if err != nil {
			return nil, err
		}
		if tcpConn, ok := conn.(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetReadBuffer(readBufSize)
			_ = tcpConn.SetWriteBuffer(writeBufSize)
		}
	}

	br := bufio.NewReaderSize(conn, 65536)
	return completeH2Handshake(conn, br, addr, maxStreams, 1)
}

// h2HandshakeSettings is the SETTINGS payload loadgen sends on every H2
// handshake. It is exposed as a package-level value so the h2c-upgrade path
// can base64url-encode it for the HTTP2-Settings request header.
var h2HandshakeSettings = [][2]uint32{
	{settingEnablePush, 0},
	{settingInitialWindowSize, h2InitialWindowSize},
	{settingMaxFrameSize, h2MaxFrameSize},
	{settingHeaderTableSize, 0},
}

// completeH2Handshake runs the client-side H2 handshake over an already-open
// TCP (or TLS) connection: writes the client preface + SETTINGS + WINDOW_UPDATE,
// reads the server SETTINGS, acks it, and waits for the server SETTINGS ack.
//
// initialStreamID selects the first client-initiated stream ID. For a normal
// H2 prior-knowledge connection this is 1. For an h2c-upgrade connection,
// stream 1 is already consumed by the upgrade GET, so callers pass 3.
//
// br is the caller-provided buffered reader. For prior-knowledge H2 the caller
// constructs a fresh one; for h2c-upgrade the caller passes the reader already
// positioned past the `101 Switching Protocols` CRLF CRLF so any buffered bytes
// (typically the server's SETTINGS frame) are not lost.
func completeH2Handshake(conn net.Conn, br *bufio.Reader, addr string, maxStreams int, initialStreamID uint32) (*h2Conn, error) {
	if _, err := conn.Write([]byte(h2ClientPreface)); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write preface: %w", err)
	}

	bw := bufio.NewWriterSize(conn, 65536)
	framer := newH2Framer(bw, br)

	if err := framer.WriteSettings(h2HandshakeSettings); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write settings: %w", err)
	}

	increment := uint32(h2InitialWindowSize - 65535)
	if err := framer.WriteWindowUpdate(0, increment); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("write window update: %w", err)
	}

	// Flush handshake frames (settings + window update) before reading server response
	if err := bw.Flush(); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("flush handshake: %w", err)
	}

	_ = conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	serverSettings, err := framer.ReadFrame()
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("read server settings: %w", err)
	}

	serverMaxStreams := uint32(maxStreams)
	// RFC 7540 defaults: SETTINGS_MAX_FRAME_SIZE 16384, INITIAL_WINDOW_SIZE 65535.
	serverMaxFrame := uint32(16384)
	serverInitWin := uint32(65535)
	if serverSettings.Type == frameSettings {
		serverSettings.ForeachSetting(func(id uint16, val uint32) {
			switch id {
			case settingMaxConcurrentStreams:
				if val > 0 {
					serverMaxStreams = val
				}
			case settingMaxFrameSize:
				// Spec range 16384..16777215; clamp to be safe.
				if val >= 16384 && val <= 16777215 {
					serverMaxFrame = val
				}
			case settingInitialWindowSize:
				if val <= 0x7FFFFFFF {
					serverInitWin = val
				}
			}
		})
		if err := framer.WriteSettingsAck(); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("write settings ack: %w", err)
		}
		if err := bw.Flush(); err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("flush settings ack: %w", err)
		}
	}

	// Connection-level WINDOW_UPDATE the server sends in its preface (before
	// its SETTINGS ack) must be captured here, not discarded: Kestrel/ASP.NET
	// grows the connection flow-control window this way (65535 ->
	// InitialConnectionWindowSize, e.g. 1 MiB). Dropping it strands
	// connSendWindow at the RFC 7540 §6.9.2 floor of 65535, which deadlocks
	// sustained request-body sends (post-4k-h2 / post-64k-h2) after ~65535
	// bytes — the server only replenishes against its larger configured
	// window, a threshold the stranded client never reaches. Servers that send
	// the WINDOW_UPDATE after their ack are handled by readLoop once it starts.
	var connWindowGrant int64
	for range 5 {
		frame, err := framer.ReadFrame()
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("read settings ack: %w", err)
		}
		if frame.Type == frameWindowUpdate && frame.StreamID == 0 {
			connWindowGrant += int64(frame.WindowUpdateIncrement())
			continue
		}
		if frame.Type == frameSettings && frame.IsAck() {
			break
		}
	}

	// The handshake is done: from here the connection has no deadline, as an
	// HTTP/1.1 connection has none. It lives until the server or the network
	// ends it or the client closes it, and Close is what interrupts a read or
	// a write blocked on a peer that stopped reading. An absolute deadline
	// here ended every connection of a run at the same instant, however
	// healthy (#88).
	_ = conn.SetReadDeadline(time.Time{})

	effectiveStreams := min(serverMaxStreams, uint32(maxStreams))
	if effectiveStreams < 1 {
		effectiveStreams = 100
	}

	// Use 2x effectiveStreams for slot array to provide headroom against
	// wrap-around collisions now that streamID allocation moved to writeLoop.
	numSlots := 2 * effectiveStreams

	hc := &h2Conn{
		conn:          conn,
		framer:        framer,
		bufWriter:     bw,
		writeCh:       make(chan h2WriteReq, 4096),
		streamSlots:   make([]h2StreamSlot, numSlots),
		firstStreamID: initialStreamID,
		// DATA-send split = the SERVER's max frame size (not our 64KiB receive cap).
		maxFrameSize:     serverMaxFrame,
		serverInitWindow: serverInitWin,
		streamSem:        make(chan struct{}, effectiveStreams),
		done:             make(chan struct{}),
		loopsDone:        make(chan struct{}),
		addr:             addr,
		chanPool: sync.Pool{
			New: func() any {
				ch := make(chan h2Response, 1)
				return &ch
			},
		},
	}
	hc.nextStreamID.Store(initialStreamID)
	// RFC 7540 §6.9.2 floor of 65535, plus any connection-level WINDOW_UPDATE
	// the server granted in its preface (captured above).
	hc.connSendWindow.Store(65535 + connWindowGrant)

	for range effectiveStreams {
		hc.streamSem <- struct{}{}
	}

	hc.loopsLeft.Store(2)
	go hc.writeLoop()
	go hc.readLoop()

	return hc, nil
}

// writeLoop processes all frame writes for this connection serially.
// For HEADERS frames, it allocates stream IDs and registers stream slots,
// ensuring monotonically increasing IDs (RFC 7540 §5.1.1) without any mutex.
// It flushes pending connection-level WINDOW_UPDATE (accumulated by readLoop
// via atomic counter) both when processing requests AND periodically when idle.
func (hc *h2Conn) writeLoop() {
	defer hc.loopExited()
	ticker := time.NewTicker(1 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case req := <-hc.writeCh:
			hc.flushWindowUpdate()
			hc.processWriteReq(req)
			count := 1
			// Drain remaining requests without blocking to batch writes
		drain:
			for {
				select {
				case req = <-hc.writeCh:
					hc.processWriteReq(req)
					count++
					if count%64 == 0 {
						hc.flushWindowUpdate()
						hc.writeFailed(hc.bufWriter.Flush()) // send WINDOW_UPDATE to the network NOW
					}
				default:
					break drain
				}
			}
			hc.flushWindowUpdate()
			hc.writeFailed(hc.bufWriter.Flush())

		case <-ticker.C:
			hc.flushWindowUpdate()
			hc.writeFailed(hc.bufWriter.Flush())

		case <-hc.done:
			hc.answerQueued()
			return
		}
	}
}

// answerQueued answers every request still queued for a closed connection's
// writer. None of them was written, so none reached the server: each goes
// to the redialed connection (errH2NotSent), as a request still waiting for
// a stream does, instead of counting as an error.
func (hc *h2Conn) answerQueued() {
	for {
		select {
		case req := <-hc.writeCh:
			if req.respCh != nil {
				*req.respCh <- h2Response{err: errH2NotSent}
			}
		default:
			return
		}
	}
}

// loopExited is deferred by readLoop and writeLoop: the second to return
// closes loopsDone.
func (hc *h2Conn) loopExited() {
	if hc.loopsLeft.Add(-1) == 0 {
		close(hc.loopsDone)
	}
}

// writeFailed ends the connection after a write to it failed (nil: no-op).
// The peer is gone, and readLoop does not always see it: the server may keep
// its side open, or its reset may not have reached the read yet. Without
// this the bufio.Writer keeps the error and fails every later request on the
// dead connection at once, a spin of errors that never redials (#89).
func (hc *h2Conn) writeFailed(err error) {
	if err != nil && !hc.closed.Load() {
		hc.failConn(fmt.Errorf("connection error: %w", err))
	}
}

func (hc *h2Conn) flushWindowUpdate() {
	if pending := hc.pendingConnWindow.Swap(0); pending > 0 {
		_ = hc.framer.WriteWindowUpdate(0, pending)
	}
}

func (hc *h2Conn) processWriteReq(req h2WriteReq) {
	switch req.kind {
	case h2WriteHeaders:
		// A connection that takes no new request (it died, the client closed
		// it, or its stream IDs ran out) never writes this one: it goes to
		// the redialed connection, as a request still waiting for a stream
		// does. Written, it would fail on bufio's sticky error, or wait for a
		// response nobody reads, and count as an error.
		if hc.closed.Load() {
			if req.respCh != nil {
				*req.respCh <- h2Response{err: errH2NotSent}
			}
			return
		}

		// Take the next stream ID whose slot is free. A slot still holds the
		// channel of a stream that 2*MaxStreams-1 later streams have outlived
		// (a slow response, or a request its worker abandoned); reusing it
		// would hand that stream's response to this request and leave its
		// worker waiting for the rest of the run. IDs may skip: a new
		// stream's ID need only exceed every earlier one (RFC 9113 §5.1.1).
		// At most MaxStreams slots are held by streams with a token, so a
		// free slot comes within MaxStreams+1 IDs; the bound keeps a slot
		// array held throughout (abandoned streams) from looping.
		numSlots := uint32(len(hc.streamSlots))
		streamID := hc.nextStreamID.Add(2) - 2
		for range numSlots {
			if hc.streamSlots[(streamID>>1)%numSlots].ch.Load() == nil {
				break
			}
			streamID = hc.nextStreamID.Add(2) - 2
		}
		if streamID > 0x7FFFFFFF {
			// The connection's stream IDs are used up, which a connection
			// that lives as long as the server keeps it (#88) reaches after
			// 2^30 requests. RFC 9113 §5.1.1: open a new connection. Marked
			// closed, it takes no new request, and DoRequest redials it and
			// closes it; this request never reached the server, so it goes
			// to the new connection (errH2NotSent), as does every request
			// still queued here.
			hc.closed.Store(true)
			if req.respCh != nil {
				*req.respCh <- h2Response{err: errH2NotSent}
			}
			return
		}

		slotIdx := (streamID >> 1) % numSlots
		hc.streamSlots[slotIdx].ch.Store(req.respCh)
		if req.hasBody {
			// Key the send window to this stream before its HEADERS can reach
			// the server, which may grant window for it or reset it as soon
			// as they do.
			hc.curStreamWindow.Store(int64(hc.serverInitWindow))
			hc.curStreamID.Store(streamID)
		}

		err := hc.framer.WriteHeaders(streamID, req.block, !req.hasBody)
		if err == nil && req.hasBody {
			err = hc.writeBodyFlowControlled(streamID, &hc.streamSlots[slotIdx], req.respCh, req.data)
			if err == errH2BodyAbandoned {
				return // the server ended the stream, and its worker has the answer; the connection is fine
			}
		}
		// Whoever takes respCh out of the slot answers it, exactly once:
		// readLoop may have taken it first (a response, a reset, or
		// failStreams on a connection that died while this was written).
		// Answering it twice would block here on the full channel.
		if err != nil && req.respCh != nil && hc.streamSlots[slotIdx].ch.CompareAndSwap(req.respCh, nil) {
			*req.respCh <- h2Response{err: err}
		}
		hc.writeFailed(err)

	case h2WriteSettingsAck:
		_ = hc.framer.WriteSettingsAck()
	case h2WritePing:
		_ = hc.framer.WritePing(true, req.pingData)
	case h2WriteGoAway:
		_ = hc.framer.WriteGoAway(0, 0, nil)
	}
}

// readLoop continuously reads frames and dispatches responses to waiting workers.
// Uses extractStatus for zero-allocation status code extraction instead of full
// HPACK decoding. Safe because we disable the HPACK dynamic table via SETTINGS.
// CRITICAL: readLoop NEVER sends to writeCh. Doing so can deadlock when writeCh
// is full of worker requests — readLoop blocks, can't read responses, server's
// flow control windows exhaust, everything stalls.
func (hc *h2Conn) readLoop() {
	defer hc.loopExited()
	numSlots := uint32(len(hc.streamSlots))

	for {
		frame, err := hc.framer.ReadFrame()
		if err != nil {
			if hc.closed.Load() {
				return // the client closed it (Close, or reconnectSlot replacing it)
			}
			// The server or the network ended the connection without a
			// GOAWAY: a close, a reset, a timeout. Fail it, so its in-flight
			// streams are errors and DoRequest redials.
			hc.failConn(fmt.Errorf("connection error: %w", err))
			return
		}

		// A HEADERS or DATA frame whose padding does not fit it is a
		// connection error (RFC 9113 §6.1, §6.2): nothing in it can be read,
		// so the connection ends as a dead one does.
		if frame.badPadding() {
			hc.failConn(fmt.Errorf("h2client: PROTOCOL_ERROR: frame type %d on stream %d: its Pad Length does not fit its %d-byte payload",
				frame.Type, frame.StreamID, frame.Length))
			return
		}

		switch frame.Type {
		case frameHeaders:
			// The status is the final response's :status, whatever ends the
			// stream: this HEADERS frame, a DATA frame, or trailers. An
			// interim 1xx response is replaced by the final one; a HEADERS
			// frame after the final one carries trailers, not a status.
			st := hc.stream(frame.StreamID)
			if !st.final {
				st.status = extractStatus(frame.HeaderBlockFragment())
				st.final = st.status < 100 || st.status >= 200
			}

			if frame.StreamEnded() {
				chPtr := st.ch.Swap(nil)
				if chPtr != nil {
					*chPtr <- h2Response{status: st.status, bytesRead: st.bytes}
				}
			}

		case frameData:
			// Accumulate connection-level WINDOW_UPDATE via atomic counter.
			// writeLoop flushes this between processing worker requests.
			// Flow control counts the whole payload, padding included.
			if frame.Length > 0 {
				hc.pendingConnWindow.Add(frame.Length)
			}

			// The body is every DATA frame of the stream, not the one that
			// ends it: a body split over 16 KiB frames, or followed by an
			// empty END_STREAM frame (net/http, after a flush), counts in full.
			// Padding is framing, as chunk sizes are in HTTP/1.1: not counted.
			st := hc.stream(frame.StreamID)
			st.bytes += len(frame.Data())

			if frame.StreamEnded() {
				chPtr := st.ch.Swap(nil)
				if chPtr != nil {
					*chPtr <- h2Response{status: st.status, bytesRead: st.bytes}
				}
			}

		case frameRSTStream:
			if frame.StreamID == hc.curStreamID.Load() {
				hc.curStreamReset.Store(frame.StreamID) // before the answer: the body writer reads them in the other order
			}
			idx := (frame.StreamID >> 1) % numSlots
			chPtr := hc.streamSlots[idx].ch.Swap(nil)
			if chPtr != nil {
				*chPtr <- h2Response{err: resetError(frame.ErrCode())}
			}

		case frameSettings:
			if !frame.IsAck() {
				// Non-blocking: drop if writeLoop is busy — server will resend.
				select {
				case hc.writeCh <- h2WriteReq{kind: h2WriteSettingsAck}:
				default:
				}
			}

		case framePing:
			if !frame.IsAck() {
				// Non-blocking: drop if writeLoop is busy — server will resend.
				select {
				case hc.writeCh <- h2WriteReq{kind: h2WritePing, pingData: frame.PingData()}:
				default:
				}
			}

		case frameGoAway:
			hc.failConn(fmt.Errorf("goaway: code=%d", frame.GoAwayErrCode()))
			return

		case frameWindowUpdate:
			// Replenish our SEND window so writeBodyFlowControlled can finish a
			// body larger than the initial window (post-64k-h2). streamID 0 =
			// connection-level; otherwise it targets a stream — apply it to the
			// currently-writing stream's window (the writer is sequential).
			incr := int64(frame.WindowUpdateIncrement())
			if frame.StreamID == 0 {
				hc.connSendWindow.Add(incr)
			} else if frame.StreamID == hc.curStreamID.Load() {
				hc.curStreamWindow.Add(incr)
			}
		}
	}
}

// stream returns the slot of streamID with its response state reset if the
// slot last held another stream, and marks the connection served when the
// stream is one of the run's requests. The h2c upgrade's own stream 1 is not:
// the server answers it on every upgraded connection (celeris before it even
// reads the client preface), so it shows nothing about whether the server
// serves the run. readLoop only.
func (hc *h2Conn) stream(streamID uint32) *h2StreamSlot {
	st := &hc.streamSlots[(streamID>>1)%uint32(len(hc.streamSlots))]
	if st.streamID != streamID {
		st.streamID = streamID
		st.status = 0
		st.final = false
		st.bytes = 0
		if streamID >= hc.firstStreamID && !hc.served.Load() {
			hc.served.Store(true)
		}
	}
	return st
}

// errH2NeverServed is DoRequest's error for a request that found its
// connection ended before the connection answered any request: the server
// completes handshakes and drops the connections, or is shutting down. The
// server looks down, so the request fails (see reconnectSlot).
var errH2NeverServed = errors.New("h2client: the connection ended before it answered a request")

// errH2NoStatus is DoRequest's error for a response without a :status the
// client can read: no :status at all, or none that extractStatus decodes. It
// is not a success, as h1client fails a response without a status line.
var errH2NoStatus = errors.New("h2client: response without a readable :status")

// errH2NotSent reports a request that never reached the server: the
// connection died before the request was handed to it (roundTrip), or took
// no new request (it died, was closed, or ran out of stream IDs) before its
// writer wrote it (processWriteReq, answerQueued). It is not a failed
// request: DoRequest takes the request to the redialed connection. Never
// returned to DoRequest's caller.
var errH2NotSent = errors.New("h2client: connection gone before the request was sent")

// DoRequest sends an HTTP/2 request and waits for the response.
// Fire-and-forget to writeLoop: no resultCh round-trip. Workers wait only on respCh,
// which receives from either writeLoop (on error) or readLoop (on response).
func (c *h2Client) DoRequest(ctx context.Context, workerID int) (int, error) {
	idx := workerID % len(c.conns)
	slot := c.conns[idx]
	for {
		hc := slot.cur.Load()

		// A server that closed, reset or GOAWAYed this connection mid-cell
		// leaves the slot dead (failConn); re-dial so the request gets a
		// fresh conn instead of spinning closed-conn errors. While the
		// server looks down, reconnectSlot fails the request instead, paced
		// by the slot's backoff.
		if hc == nil || hc.closed.Load() {
			var err error
			if hc, err = c.reconnectSlot(ctx, slot); err != nil {
				switch {
				case ctx.Err() != nil:
					return 0, ctx.Err()
				case err == errH2NeverServed:
					return 0, fmt.Errorf("h2client: conn[%d]: %w", idx, err)
				}
				return 0, fmt.Errorf("h2client: conn[%d] reconnect failed: %w", idx, err)
			}
		}

		n, err := c.roundTrip(ctx, hc, idx)
		if err != errH2NotSent { // the sentinel itself, never wrapped
			return n, err
		}
		// The connection ended before this request was written: while it
		// waited for a stream, or before the connection's writer reached it
		// (the connection died, or ran out of stream IDs). Each pass through
		// here needs a connection to have ended, and reconnectSlot redials
		// only a connection that answered a request: one that never did
		// fails this request, so a server that keeps dropping connections
		// cannot make this loop spin uncounted. ctx ends it too.
	}
}

// roundTrip sends one request on hc and waits for its response. It returns
// errH2NotSent if hc ended before the request was written: before it was
// handed to the writer, or before the writer reached it.
func (c *h2Client) roundTrip(ctx context.Context, hc *h2Conn, idx int) (int, error) {
	// Acquire a stream. When the connection dies, the workers queued here
	// wake through done, or through the tokens its failed streams return.
	select {
	case <-hc.streamSem:
	case <-hc.done:
		return 0, errH2NotSent
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	if hc.closed.Load() {
		hc.streamSem <- struct{}{}
		return 0, errH2NotSent
	}

	// Get heap-allocated response channel pointer from pool
	chPtr := hc.chanPool.Get().(*chan h2Response)
	// Drain any stale value
	select {
	case <-*chPtr:
	default:
	}

	// Submit to writeLoop — fire-and-forget. writeLoop assigns stream IDs
	// in dequeue order, guaranteeing monotonic IDs by construction.
	// On write error, writeLoop sends to *respCh directly.
	select {
	case hc.writeCh <- h2WriteReq{
		kind:    h2WriteHeaders,
		block:   c.headerBlock,
		data:    c.dataPayload,
		hasBody: c.hasBody,
		respCh:  chPtr,
	}:
	case <-hc.done:
		hc.chanPool.Put(chPtr)
		hc.streamSem <- struct{}{}
		return 0, errH2NotSent
	case <-ctx.Done():
		hc.chanPool.Put(chPtr)
		hc.streamSem <- struct{}{}
		return 0, ctx.Err()
	}

	return hc.await(ctx, chPtr, idx)
}

// await waits for the answer to a request handed to hc: a response (from
// readLoop), an error (from writeLoop, or failStreams), or errH2NotSent (from
// writeLoop, for a request it never wrote). Exactly one of them answers each
// request, and done below only when none did, so a request is one outcome,
// never zero or two.
func (hc *h2Conn) await(ctx context.Context, chPtr *chan h2Response, idx int) (int, error) {
	select {
	case resp := <-*chPtr:
		return hc.finish(chPtr, resp)
	case <-ctx.Done():
		// Worker exits, abandons chPtr. The heap-allocated channel (~120 bytes)
		// will be GC'd when the slot is overwritten by the next stream ID at
		// that index. Buffered channel (cap 1) ensures readLoop/writeLoop
		// send never blocks even if nobody receives.
		hc.streamSem <- struct{}{}
		return 0, ctx.Err()
	case <-hc.done:
		// The connection is closing, and this request's answer may still
		// be on its way: readLoop may be delivering a response it took
		// before the close, and writeLoop answers every request it never
		// wrote with errH2NotSent. Both loops return soon after done closes,
		// having sent every answer they will send, so wait for them (or the
		// answer), then take the answer if there is one. Deciding at once
		// turned both into errors: a response the server sent, and a request
		// that never reached it.
		select {
		case resp := <-*chPtr:
			return hc.finish(chPtr, resp)
		case <-hc.loopsDone:
		case <-ctx.Done():
			hc.streamSem <- struct{}{}
			return 0, ctx.Err()
		}
		// writeLoop has returned, so whatever is still queued for it was
		// never written: roundTrip's hand-off can win its race with done
		// after the writer's final drain. Answer those requests too, this
		// one among them (each is taken off the queue once, so answered
		// once).
		hc.answerQueued()
		select {
		case resp := <-*chPtr:
			return hc.finish(chPtr, resp)
		default:
		}
		hc.streamSem <- struct{}{}
		return 0, fmt.Errorf("h2client: conn[%d] connection closing", idx)
	}
}

// finish returns the stream's channel and token and turns its answer into
// DoRequest's result: a status >= 400 is an error, as h1client counts it, and
// so is a response whose :status could not be read.
func (hc *h2Conn) finish(chPtr *chan h2Response, resp h2Response) (int, error) {
	hc.chanPool.Put(chPtr)
	hc.streamSem <- struct{}{}
	if resp.err != nil {
		return 0, resp.err
	}
	if resp.status < 100 {
		return resp.bytesRead, errH2NoStatus
	}
	if resp.status >= 400 {
		return resp.bytesRead, statusError(resp.status)
	}
	return resp.bytesRead, nil
}

// errH2BodyAbandoned is writeBodyFlowControlled's report of a body it stopped
// because the server ended the stream first. Not an error: the stream's worker
// already has its answer, and the connection is fine.
var errH2BodyAbandoned = errors.New("h2client: request body abandoned: the server ended the stream")

// writeBodyFlowControlled sends a request body as DATA frames, respecting BOTH
// the server's SETTINGS_MAX_FRAME_SIZE and its connection + per-stream send
// windows (RFC 7540 §6.9). The writer goroutine is sequential (one body at a
// time), so the active stream's window lives in hc.curStreamWindow keyed by
// hc.curStreamID (processWriteReq keys them); readLoop replenishes
// connSendWindow/curStreamWindow on WINDOW_UPDATE. When a window is exhausted
// we flush the buffered frames (so the server can consume + replenish) and
// poll — no extra goroutine/channel. The wait ends when that flush fails, when
// the connection is closed (done), which Close does at the end of a run and
// failConn when the connection dies, or when the server has ended the stream
// (st no longer holds respCh): the backstops against a peer that never grants
// window.
//
// Without this, a 64KiB body (post-64k-h2 = 65536 B) either trips FRAME_SIZE_ERROR
// (64KiB DATA frame vs a 16384-default server) or overruns the 65535 window.
func (hc *h2Conn) writeBodyFlowControlled(streamID uint32, st *h2StreamSlot, respCh *chan h2Response, data []byte) error {
	maxFrame := int(hc.maxFrameSize)
	if maxFrame < 16384 {
		maxFrame = 16384
	}
	for len(data) > 0 {
		avail := maxFrame
		if cw := int(hc.connSendWindow.Load()); cw < avail {
			avail = cw
		}
		if sw := int(hc.curStreamWindow.Load()); sw < avail {
			avail = sw
		}
		if avail <= 0 {
			select {
			case <-hc.done:
				return fmt.Errorf("h2client: conn closed mid-body (flow-control wait)")
			default:
			}
			// The server has answered or reset the stream (readLoop took its
			// channel out of the slot): RFC 9113 §8.1 lets it answer before
			// the body is complete. It grants no more window for a stream it
			// has ended, so stop the body here instead of waiting for the run
			// to end with every later request of the connection queued
			// behind this one. A response that ended without a reset leaves
			// our half of the stream open: close it with RST_STREAM(CANCEL).
			if respCh != nil && st.ch.Load() != respCh {
				if hc.curStreamReset.Load() != streamID {
					_ = hc.framer.WriteRSTStream(streamID, h2ErrCodeCancel)
				}
				return errH2BodyAbandoned
			}
			// Window exhausted: flush so the peer receives what we've sent and
			// can send WINDOW_UPDATE, then wait for readLoop to replenish.
			// The flush carries the receive window readLoop has credited
			// meanwhile, as writeLoop's own flushes do: a peer that waits for
			// that credit before it grants ours would otherwise deadlock with
			// this wait. A failed flush ends the wait and, through the
			// caller's writeFailed, the connection (#89).
			hc.flushWindowUpdate()
			if err := hc.bufWriter.Flush(); err != nil {
				return err
			}
			time.Sleep(50 * time.Microsecond)
			continue
		}
		if avail > len(data) {
			avail = len(data)
		}
		chunk := data[:avail]
		endStream := len(chunk) == len(data)
		if err := hc.framer.WriteData(streamID, endStream, chunk); err != nil {
			return err
		}
		hc.connSendWindow.Add(-int64(len(chunk)))
		hc.curStreamWindow.Add(-int64(len(chunk)))
		data = data[len(chunk):]
	}
	return nil
}

// Close closes all connections.
func (c *h2Client) Close() {
	for _, slot := range c.conns {
		slot.mu.Lock()
		if hc := slot.cur.Load(); hc != nil {
			hc.closeConn()
		}
		slot.mu.Unlock()
	}
}

// closeConn closes the connection: it takes no new request, done unblocks
// workers, readLoop and writeLoop, and the socket is released. Idempotent,
// and safe from any goroutine.
func (hc *h2Conn) closeConn() {
	hc.closed.Store(true)
	hc.closeOnce.Do(func() {
		close(hc.done) // unblock workers, readLoop, and writeLoop
		_ = hc.conn.Close()
	})
}

// failConn ends a connection that died under the client: the server closed
// or reset it, sent GOAWAY, or a write to it failed. It marks the connection
// closed first, so DoRequest redials instead of queueing onto a connection
// nobody reads, fails every stream in flight on it once, and closes it,
// which stops writeLoop, releases the socket and wakes the workers queued
// for a stream. Called by readLoop and writeLoop; safe from both at once.
func (hc *h2Conn) failConn(err error) {
	hc.closed.Store(true)
	hc.failStreams(err)
	hc.closeConn()
}

// failStreams answers every stream still registered on the connection with
// err. Each response channel is taken out of its slot before it is answered,
// so no stream is answered twice.
func (hc *h2Conn) failStreams(err error) {
	for i := range hc.streamSlots {
		if chPtr := hc.streamSlots[i].ch.Swap(nil); chPtr != nil {
			*chPtr <- h2Response{err: err}
		}
	}
}
