package loadgen

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2/hpack"
)

// h2Client is a zero-allocation HTTP/2 benchmark client.
// Uses pre-encoded HPACK headers, dedicated writer goroutine per connection,
// lock-free stream slot dispatch, and batched WINDOW_UPDATE writes.
type h2Client struct {
	conns       []*h2Conn
	headerBlock []byte // pre-encoded HPACK header block (immutable)
	dataPayload []byte // body bytes for POST (nil for GET)
	hasBody     bool

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

// h2WriteReq is a frame write request submitted to the writer goroutine.
// For HEADERS: writeLoop allocates the streamID and registers the stream slot,
// eliminating the need for a mutex on the worker side.
type h2WriteReq struct {
	kind     uint8
	block    []byte           // HEADERS: header block fragment
	data     []byte           // HEADERS w/ body: data payload
	hasBody  bool             // HEADERS: has data frames to follow
	maxFrame uint32           // HEADERS w/ body: max frame size
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
	// Index = (streamID >> 1) % len(streamSlots). The semaphore limits
	// concurrent streams, and stream IDs are assigned sequentially by
	// writeLoop, so slots cycle predictably with no collisions.
	// Sized at 2x effectiveStreams for headroom against wrap-around.
	nextStreamID atomic.Uint32
	streamSlots  []h2StreamSlot

	// Channel pool — pools *chan h2Response (heap-allocated pointers).
	// The pointer in the slot remains valid even after the goroutine exits.
	chanPool sync.Pool

	// Flow control — readLoop accumulates via atomic add,
	// writeLoop flushes between processing worker requests.
	maxFrameSize      uint32
	pendingConnWindow atomic.Uint32

	// Concurrency limit
	streamSem chan struct{}

	// Shutdown signal — closed by closeConn to unblock readLoop/writeLoop/workers
	done chan struct{}

	addr   string
	closed atomic.Bool
}

// h2StreamSlot holds an atomic pointer to a response channel.
// writeLoop stores; readLoop loads and clears.
type h2StreamSlot struct {
	ch atomic.Pointer[chan h2Response]
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
// Returns 0 for unparseable blocks (treated as success in benchmarks).
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

	// Fallback: literal header field with name index 8 (:status).
	// Safe because we disabled the HPACK dynamic table (SettingHeaderTableSize=0).
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

	if nameIdx != 8 || pos >= len(headerBlock) {
		return 0
	}

	valueByte := headerBlock[pos]
	pos++

	// Skip Huffman-encoded values (unlikely for 3-digit status codes)
	if valueByte&0x80 != 0 {
		return 0
	}

	valueLen := int(valueByte & 0x7F)
	if valueLen != 3 || pos+3 > len(headerBlock) {
		return 0
	}

	return parseStatusCode(headerBlock[pos : pos+3])
}

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

	conns := make([]*h2Conn, numConns)
	for i := range numConns {
		var hc *h2Conn
		var err error
		if upgrade {
			hc, err = dialH2CUpgrade(addr, scheme, path, host, port, maxStreams, cfg.DialTimeout, cfg.ReadBufferSize, cfg.WriteBufferSize, tlsCfg)
		} else {
			hc, err = dialH2(addr, scheme, maxStreams, cfg.DialTimeout, cfg.ReadBufferSize, cfg.WriteBufferSize, tlsCfg)
		}
		if err != nil {
			for j := range i {
				conns[j].closeConn()
			}
			return nil, fmt.Errorf("h2client: dial conn[%d]: %w", i, err)
		}
		conns[i] = hc
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
		dialedViaUpgrade: upgrade,
		upgradeAttempted: numConns,
	}, nil
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
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: dialTimeout}, "tcp", addr, tlsCfg)
		if err != nil {
			return nil, err
		}
		if tcpConn, ok := conn.(*tls.Conn).NetConn().(*net.TCPConn); ok {
			_ = tcpConn.SetNoDelay(true)
			_ = tcpConn.SetReadBuffer(readBufSize)
			_ = tcpConn.SetWriteBuffer(writeBufSize)
		}
	} else {
		conn, err = net.DialTimeout("tcp", addr, dialTimeout)
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
	if serverSettings.Type == frameSettings {
		serverSettings.ForeachSetting(func(id uint16, val uint32) {
			if id == settingMaxConcurrentStreams && val > 0 {
				serverMaxStreams = val
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

	for range 5 {
		frame, err := framer.ReadFrame()
		if err != nil {
			_ = conn.Close()
			return nil, fmt.Errorf("read settings ack: %w", err)
		}
		if frame.Type == frameSettings && frame.IsAck() {
			break
		}
	}

	_ = conn.SetReadDeadline(time.Time{})

	// Set a deadline covering the entire benchmark lifetime.
	// Prevents infinite blocks on TCP write if H2 flow control deadlocks.
	_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))

	effectiveStreams := min(serverMaxStreams, uint32(maxStreams))
	if effectiveStreams < 1 {
		effectiveStreams = 100
	}

	// Use 2x effectiveStreams for slot array to provide headroom against
	// wrap-around collisions now that streamID allocation moved to writeLoop.
	numSlots := 2 * effectiveStreams

	hc := &h2Conn{
		conn:         conn,
		framer:       framer,
		bufWriter:    bw,
		writeCh:      make(chan h2WriteReq, 4096),
		streamSlots:  make([]h2StreamSlot, numSlots),
		maxFrameSize: h2MaxFrameSize,
		streamSem:    make(chan struct{}, effectiveStreams),
		done:         make(chan struct{}),
		addr:         addr,
		chanPool: sync.Pool{
			New: func() any {
				ch := make(chan h2Response, 1)
				return &ch
			},
		},
	}
	hc.nextStreamID.Store(initialStreamID)

	for range effectiveStreams {
		hc.streamSem <- struct{}{}
	}

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
						_ = hc.bufWriter.Flush() // send WINDOW_UPDATE to the network NOW
					}
				default:
					break drain
				}
			}
			hc.flushWindowUpdate()
			_ = hc.bufWriter.Flush()

		case <-ticker.C:
			hc.flushWindowUpdate()
			_ = hc.bufWriter.Flush()

		case <-hc.done:
			// Drain pending requests — send errors to respCh so workers unblock.
			for {
				select {
				case req := <-hc.writeCh:
					if req.respCh != nil {
						*req.respCh <- h2Response{err: fmt.Errorf("connection closing")}
					}
				default:
					return
				}
			}
		}
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
		streamID := hc.nextStreamID.Add(2) - 2
		if streamID > 0x7FFFFFFF {
			if req.respCh != nil {
				*req.respCh <- h2Response{err: fmt.Errorf("h2client: stream ID exhausted")}
			}
			return
		}

		slotIdx := (streamID >> 1) % uint32(len(hc.streamSlots))
		hc.streamSlots[slotIdx].ch.Store(req.respCh)

		err := hc.framer.WriteHeaders(streamID, req.block, !req.hasBody)
		if err == nil && req.hasBody {
			err = writeDataFrames(hc.framer, streamID, req.data, req.maxFrame)
		}
		if err != nil {
			hc.streamSlots[slotIdx].ch.Store(nil)
			if req.respCh != nil {
				*req.respCh <- h2Response{err: err}
			}
		}

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
	numSlots := uint32(len(hc.streamSlots))

	for {
		frame, err := hc.framer.ReadFrame()
		if err != nil {
			if hc.closed.Load() {
				return
			}
			// Notify all pending streams
			for i := range hc.streamSlots {
				chPtr := hc.streamSlots[i].ch.Swap(nil)
				if chPtr != nil {
					*chPtr <- h2Response{err: fmt.Errorf("connection error: %w", err)}
				}
			}
			return
		}

		switch frame.Type {
		case frameHeaders:
			status := extractStatus(frame.HeaderBlockFragment())

			if frame.StreamEnded() {
				idx := (frame.StreamID >> 1) % numSlots
				chPtr := hc.streamSlots[idx].ch.Swap(nil)
				if chPtr != nil {
					*chPtr <- h2Response{status: status, bytesRead: 0}
				}
			}

		case frameData:
			bytesRead := len(frame.Data())

			// Accumulate connection-level WINDOW_UPDATE via atomic counter.
			// writeLoop flushes this between processing worker requests.
			if bytesRead > 0 {
				hc.pendingConnWindow.Add(uint32(bytesRead))
			}

			if frame.StreamEnded() {
				idx := (frame.StreamID >> 1) % numSlots
				chPtr := hc.streamSlots[idx].ch.Swap(nil)
				if chPtr != nil {
					*chPtr <- h2Response{status: 200, bytesRead: bytesRead}
				}
			}

		case frameRSTStream:
			idx := (frame.StreamID >> 1) % numSlots
			chPtr := hc.streamSlots[idx].ch.Swap(nil)
			if chPtr != nil {
				*chPtr <- h2Response{err: fmt.Errorf("stream reset: code=%d", frame.ErrCode())}
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
			hc.closed.Store(true)
			for i := range hc.streamSlots {
				chPtr := hc.streamSlots[i].ch.Swap(nil)
				if chPtr != nil {
					*chPtr <- h2Response{err: fmt.Errorf("goaway: code=%d", frame.GoAwayErrCode())}
				}
			}
			return

		case frameWindowUpdate:
			// Ignore server-side window updates
		}
	}
}

// DoRequest sends an HTTP/2 request and waits for the response.
// Fire-and-forget to writeLoop: no resultCh round-trip. Workers wait only on respCh,
// which receives from either writeLoop (on error) or readLoop (on response).
func (c *h2Client) DoRequest(ctx context.Context, workerID int) (int, error) {
	idx := workerID % len(c.conns)
	hc := c.conns[idx]

	if hc.closed.Load() {
		return 0, fmt.Errorf("h2client: conn[%d] connection closed", idx)
	}

	// Acquire stream semaphore
	select {
	case <-hc.streamSem:
	case <-ctx.Done():
		return 0, ctx.Err()
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
		kind:     h2WriteHeaders,
		block:    c.headerBlock,
		data:     c.dataPayload,
		hasBody:  c.hasBody,
		maxFrame: hc.maxFrameSize,
		respCh:   chPtr,
	}:
	case <-hc.done:
		hc.chanPool.Put(chPtr)
		hc.streamSem <- struct{}{}
		return 0, fmt.Errorf("h2client: conn[%d] connection closing", idx)
	case <-ctx.Done():
		hc.chanPool.Put(chPtr)
		hc.streamSem <- struct{}{}
		return 0, ctx.Err()
	}

	// Single wait — receives normal responses (from readLoop) AND write errors (from writeLoop)
	select {
	case resp := <-*chPtr:
		hc.chanPool.Put(chPtr)
		hc.streamSem <- struct{}{}
		if resp.err != nil {
			return 0, resp.err
		}
		if resp.status >= 400 {
			return resp.bytesRead, fmt.Errorf("h2client: conn[%d] status %d", idx, resp.status)
		}
		return resp.bytesRead, nil
	case <-ctx.Done():
		// Worker exits, abandons chPtr. The heap-allocated channel (~120 bytes)
		// will be GC'd when the slot is overwritten by the next stream ID at
		// that index. Buffered channel (cap 1) ensures readLoop/writeLoop
		// send never blocks even if nobody receives.
		hc.streamSem <- struct{}{}
		return 0, ctx.Err()
	case <-hc.done:
		hc.streamSem <- struct{}{}
		return 0, fmt.Errorf("h2client: conn[%d] connection closing", idx)
	}
}

// writeDataFrames writes body data, splitting into frames if needed.
func writeDataFrames(framer *h2Framer, streamID uint32, data []byte, maxFrameSize uint32) error {
	for len(data) > 0 {
		chunk := data
		endStream := true
		if uint32(len(chunk)) > maxFrameSize {
			chunk = data[:maxFrameSize]
			endStream = false
		}
		if err := framer.WriteData(streamID, endStream, chunk); err != nil {
			return err
		}
		data = data[len(chunk):]
	}
	return nil
}

// Close closes all connections.
func (c *h2Client) Close() {
	for _, hc := range c.conns {
		hc.closeConn()
	}
}

func (hc *h2Conn) closeConn() {
	if !hc.closed.CompareAndSwap(false, true) {
		return // already closed
	}
	close(hc.done) // unblock workers, readLoop, and writeLoop
	_ = hc.conn.Close()
}
