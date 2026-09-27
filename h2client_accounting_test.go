package loadgen

// Regression tests for what the HTTP/2 client records per response and how
// long its connections live:
//
//   - #85: a 4xx/5xx response WITH a body was recorded as status 200 (a
//     success); the status must come from the response HEADERS frame.
//   - #86: throughput counted only the last DATA frame of each response.
//   - #88: every connection carried an absolute 5-minute deadline from dial.
//   - #89: a connection that died with a read error (no GOAWAY) was never
//     redialed, and its workers hung or spun errors; a failed write left a
//     sticky error that failed every later request on the dead connection.
//
// Most tests drive a scripted HTTP/2 server (rawH2Server) that writes every
// response frame by frame, so each wire shape is pinned exactly; the
// status and byte parity tests use net/http's own server, answering the same
// route over HTTP/1.1 and HTTP/2, as the reference.

import (
	"bufio"
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

// ---------------------------------------------------------------------------
// Scripted HTTP/2 server

// rawH2Action is what the scripted server does with a connection after a
// handler has answered (or not answered) a request stream.
type rawH2Action int

const (
	rawKeep   rawH2Action = iota // keep serving the connection
	rawClose                     // close the TCP connection, no GOAWAY
	rawGoAway                    // send GOAWAY(last stream = this one), keep the connection open
)

// rawH2Handler answers one request stream by writing frames on c.
type rawH2Handler func(c *rawH2Conn, streamID uint32, path string) rawH2Action

// rawH2Server speaks just enough HTTP/2, over prior knowledge or the RFC 7540
// §3.2 h2c upgrade, for a handler to script each response frame by frame.
type rawH2Server struct {
	ln             net.Listener
	handler        rawH2Handler
	killAfter      time.Duration   // > 0: the server closes each connection (no GOAWAY) this long after accepting it
	closeAfterData int             // > 0: the server closes a connection (no GOAWAY) once it has read this many DATA bytes on it
	settings       []http2.Setting // the server's SETTINGS (none: every default)
	connWindow     uint32          // > 0: grant the client this much more connection send window in the handshake
	answerUpgrade  bool            // answer the h2c upgrade request on stream 1, as celeris does (see rawH2Handshake)
	goAwayAtOnce   bool            // send GOAWAY as soon as the client's SETTINGS are acked, before any request

	accepted       atomic.Int64 // connections accepted
	killed         atomic.Int64 // connections the server ended (rawClose or killAfter)
	clientClosed   atomic.Int64 // connections the client ended first
	dataSent       atomic.Int64 // DATA frame payload bytes written, padding included
	windowCredit   atomic.Int64 // connection WINDOW_UPDATE increments received, minus each handshake's own
	answeredLate   atomic.Int64 // requests answered on a connection other than the first one
	clientResets   atomic.Int64 // RST_STREAM frames received from the client
	clientCancel   atomic.Int64 // of which with error code CANCEL
	dataAfterReset atomic.Int64 // DATA payload bytes received on streams the server had reset (rawH2Conn.reset)

	mu    sync.Mutex
	conns []net.Conn
	wg    sync.WaitGroup
}

// rawH2Conn is one server-side connection, as a handler sees it.
type rawH2Conn struct {
	srv      *rawH2Server
	nc       net.Conn
	fr       *http2.Framer
	enc      *hpack.Encoder
	hbuf     bytes.Buffer
	index    int64 // accept order, from 1
	requests int   // request streams seen on this connection, the current one included
	held     int   // streams a handler started answering and left open
	dataRecv int   // DATA payload bytes read on this connection
	resetIDs map[uint32]bool
}

// rawH2Opts are the rawH2Server options of the same names.
type rawH2Opts struct {
	killAfter      time.Duration
	closeAfterData int
	settings       []http2.Setting
	connWindow     uint32
	answerUpgrade  bool
	goAwayAtOnce   bool
}

func startRawH2(t *testing.T, h rawH2Handler) *rawH2Server {
	return startRawH2With(t, rawH2Opts{}, h)
}

func startRawH2With(t *testing.T, opts rawH2Opts, h rawH2Handler) *rawH2Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &rawH2Server{ln: ln, handler: h, killAfter: opts.killAfter, closeAfterData: opts.closeAfterData, settings: opts.settings,
		connWindow: opts.connWindow, answerUpgrade: opts.answerUpgrade, goAwayAtOnce: opts.goAwayAtOnce}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, nc)
			s.mu.Unlock()
			s.wg.Add(1)
			go s.serve(nc)
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		s.mu.Lock()
		for _, nc := range s.conns {
			_ = nc.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func (s *rawH2Server) url(path string) string { return "http://" + s.ln.Addr().String() + path }

func (s *rawH2Server) hostPort() (string, string) {
	h, p, _ := net.SplitHostPort(s.ln.Addr().String())
	return h, p
}

func (s *rawH2Server) serve(nc net.Conn) {
	defer s.wg.Done()
	index := s.accepted.Add(1)

	// ended is set by whichever side ends the connection first, so a read
	// error after the server closed it is not counted as the client's doing.
	var ended atomic.Bool
	serverEnd := func() {
		if ended.CompareAndSwap(false, true) {
			s.killed.Add(1)
			_ = nc.Close()
		}
	}
	if s.killAfter > 0 {
		timer := time.AfterFunc(s.killAfter, serverEnd)
		defer timer.Stop()
	}

	br := bufio.NewReader(nc)
	fr := http2.NewFramer(nc, br)
	if !rawH2Handshake(nc, br, fr, s.answerUpgrade, s.settings...) {
		_ = nc.Close()
		return
	}
	if s.connWindow > 0 {
		_ = fr.WriteWindowUpdate(0, s.connWindow)
	}
	c := &rawH2Conn{srv: s, nc: nc, fr: fr, index: index, resetIDs: map[uint32]bool{}}
	c.enc = hpack.NewEncoder(&c.hbuf)
	c.enc.SetMaxDynamicTableSizeLimit(0) // the client advertises SETTINGS_HEADER_TABLE_SIZE=0
	dec := hpack.NewDecoder(4096, nil)
	handshakeCredit := true

	for {
		f, err := fr.ReadFrame()
		if err != nil {
			if ended.CompareAndSwap(false, true) {
				s.clientClosed.Add(1)
			}
			_ = nc.Close()
			return
		}
		switch f := f.(type) {
		case *http2.SettingsFrame:
			if !f.IsAck() {
				_ = fr.WriteSettingsAck()
				if s.goAwayAtOnce {
					_ = fr.WriteGoAway(0, http2.ErrCodeNo, nil) // the handshake is done, and no request will be served
				}
			}
		case *http2.RSTStreamFrame:
			s.clientResets.Add(1)
			if f.ErrCode == http2.ErrCodeCancel {
				s.clientCancel.Add(1)
			}
		case *http2.PingFrame:
			if !f.IsAck() {
				_ = fr.WritePing(true, f.Data)
			}
		case *http2.WindowUpdateFrame:
			if f.StreamID == 0 {
				if handshakeCredit {
					handshakeCredit = false // the client's handshake grant, not a credit for DATA we sent
					continue
				}
				s.windowCredit.Add(int64(f.Increment))
			}
		case *http2.DataFrame:
			c.dataRecv += len(f.Data())
			if c.resetIDs[f.StreamID] {
				s.dataAfterReset.Add(int64(len(f.Data())))
			}
			if s.closeAfterData > 0 && c.dataRecv >= s.closeAfterData {
				serverEnd()
				return
			}
		case *http2.HeadersFrame:
			c.requests++
			path := ""
			if fields, err := dec.DecodeFull(f.HeaderBlockFragment()); err == nil {
				for _, hf := range fields {
					if hf.Name == ":path" {
						path = hf.Value
					}
				}
			}
			switch s.handler(c, f.StreamID, path) {
			case rawClose:
				serverEnd()
				return
			case rawGoAway:
				_ = fr.WriteGoAway(f.StreamID, http2.ErrCodeNo, nil)
			}
		}
	}
}

// rawH2Handshake reads the client connection preface, performing the h2c
// upgrade first when the connection starts with an HTTP/1.1 request, and
// sends the server's SETTINGS. With answerUpgrade, the upgrade request is
// answered on stream 1, as RFC 7540 §3.2 requires, in the order celeris
// writes it (internal/conn/h2.go NewH2StateFromUpgrade): SETTINGS, a SETTINGS
// ACK for the client's HTTP2-Settings, then the response, before the client
// preface is read.
func rawH2Handshake(nc net.Conn, br *bufio.Reader, fr *http2.Framer, answerUpgrade bool, settings ...http2.Setting) bool {
	head, err := br.Peek(len(http2.ClientPreface))
	if err != nil {
		return false
	}
	if string(head) != http2.ClientPreface {
		tp := textproto.NewReader(br)
		if _, err := tp.ReadLine(); err != nil {
			return false
		}
		if _, err := tp.ReadMIMEHeader(); err != nil {
			return false
		}
		if _, err := io.WriteString(nc, "HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n"); err != nil {
			return false
		}
		if err := fr.WriteSettings(settings...); err != nil {
			return false
		}
		if answerUpgrade {
			_ = fr.WriteSettingsAck()
			_ = fr.WriteHeaders(http2.HeadersFrameParam{StreamID: 1, BlockFragment: []byte{0x88}, EndHeaders: true}) // :status 200
			_ = fr.WriteData(1, true, []byte(rawOKBody))
		}
		if head, err = br.Peek(len(http2.ClientPreface)); err != nil || string(head) != http2.ClientPreface {
			return false
		}
		_, err = br.Discard(len(http2.ClientPreface))
		return err == nil
	}
	if _, err := br.Discard(len(http2.ClientPreface)); err != nil {
		return false
	}
	return fr.WriteSettings(settings...) == nil
}

// headers writes one HEADERS frame carrying fields, given as name, value pairs.
func (c *rawH2Conn) headers(streamID uint32, endStream bool, fields ...string) {
	c.headersFrame(http2.HeadersFrameParam{StreamID: streamID, EndStream: endStream, EndHeaders: true}, fields...)
}

func (c *rawH2Conn) headersFrame(p http2.HeadersFrameParam, fields ...string) {
	c.hbuf.Reset()
	for i := 0; i+1 < len(fields); i += 2 {
		_ = c.enc.WriteField(hpack.HeaderField{Name: fields[i], Value: fields[i+1]})
	}
	p.BlockFragment = c.hbuf.Bytes()
	_ = c.fr.WriteHeaders(p)
}

// data writes one DATA frame with n body bytes and, when pad > 0, pad bytes
// of padding.
func (c *rawH2Conn) data(streamID uint32, endStream bool, n, pad int) {
	var padding []byte
	length := n
	if pad > 0 {
		padding = make([]byte, pad)
		length += 1 + pad // the Pad Length field plus the padding
	}
	_ = c.fr.WriteDataPadded(streamID, endStream, make([]byte, n), padding)
	c.srv.dataSent.Add(int64(length))
}

// tornData writes the 9-byte header of a DATA frame announcing declared bytes
// and only the first sent of them, so the connection ends mid-frame.
func (c *rawH2Conn) tornData(streamID uint32, declared, sent int) {
	hdr := []byte{byte(declared >> 16), byte(declared >> 8), byte(declared), 0x0 /* DATA */, 0, 0, 0, 0, 0}
	binary.BigEndian.PutUint32(hdr[5:], streamID)
	_, _ = c.nc.Write(append(hdr, make([]byte, sent)...))
}

// reset ends a stream with RST_STREAM(code). DATA the client sends on it
// afterwards is counted in dataAfterReset.
func (c *rawH2Conn) reset(streamID uint32, code http2.ErrCode) {
	c.resetIDs[streamID] = true
	_ = c.fr.WriteRSTStream(streamID, code)
}

// ok answers a stream with 200 and the 2-byte body "OK", in one DATA frame.
func (c *rawH2Conn) ok(streamID uint32) {
	c.headers(streamID, false, ":status", "200")
	c.data(streamID, true, len(rawOKBody), 0)
	if c.index > 1 {
		c.srv.answeredLate.Add(1)
	}
}

const rawOKBody = "OK"

func respondOK(c *rawH2Conn, streamID uint32, _ string) rawH2Action {
	c.ok(streamID)
	return rawKeep
}

// ---------------------------------------------------------------------------
// Helpers

// runBench runs a Benchmarker and fails the test if Run does not return.
func runBench(t *testing.T, cfg Config) *Result {
	t.Helper()
	if cfg.Method == "" {
		cfg.Method = "GET"
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	type outcome struct {
		res *Result
		err error
	}
	done := make(chan outcome, 1)
	go func() {
		res, err := b.Run(context.Background())
		done <- outcome{res, err}
	}()
	select {
	case o := <-done:
		if o.err != nil {
			t.Fatalf("Run: %v", o.err)
		}
		return o.res
	case <-time.After(cfg.Warmup + cfg.Duration + 20*time.Second):
		t.Fatal("Benchmarker.Run did not return: a worker is hung")
		return nil
	}
}

// h2Once sends one request on a fresh prior-knowledge client for path.
func h2Once(t *testing.T, host, port, path string) (int, error) {
	t.Helper()
	cl, err := newH2Client(host, port, path, testH2Cfg("GET", nil, nil, 1, 10))
	if err != nil {
		t.Fatalf("newH2Client(%s): %v", path, err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cl.DoRequest(ctx, 0)
}

// h1Once sends one request on a fresh HTTP/1.1 client for path.
func h1Once(t *testing.T, host, port, path string) (int, error) {
	t.Helper()
	cl, err := newH1Client(host, port, path, Config{
		Method: "GET", Workers: 1, Connections: 1, PoolSize: 1,
		DialTimeout: 5 * time.Second, ReadBufferSize: 64 << 10, WriteBufferSize: 64 << 10, MaxResponseSize: -1,
	})
	if err != nil {
		t.Fatalf("newH1Client(%s): %v", path, err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return cl.DoRequest(ctx, 0)
}

// h2Status is the status an HTTP/2 DoRequest error reports, 0 for a success.
func h2Status(err error) int {
	var se *HTTP2StatusError
	if errors.As(err, &se) {
		return se.Status
	}
	if err != nil {
		return -1
	}
	return 0
}

// startReferenceServer serves mux over HTTP/1.1 and prior-knowledge HTTP/2
// cleartext on one listener, using net/http's own HTTP/2 server.
func startReferenceServer(t *testing.T, mux *http.ServeMux) (host, port string) {
	t.Helper()
	var protos http.Protocols
	protos.SetHTTP1(true)
	protos.SetUnencryptedHTTP2(true)
	srv := &http.Server{Handler: mux, Protocols: &protos, ReadHeaderTimeout: 5 * time.Second}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	host, port, _ = net.SplitHostPort(ln.Addr().String())
	return host, port
}

// warpThreshold and warpFactor define the deadline warp: a deadline armed
// more than warpThreshold ahead is a connection-lifetime bound, not a
// handshake one (those are 10 s), and fires warpFactor times sooner, so the
// 5-minute bound of #88 fires after 200 ms.
const (
	warpThreshold = 30 * time.Second
	warpFactor    = 1500
)

// warpCounts is what the deadline warp saw: every deadline armed on a warped
// connection (seen) and the long ones it warped (armed).
type warpCounts struct {
	seen  atomic.Int64 // deadlines armed through the warp, of any length
	armed atomic.Int64 // deadlines more than warpThreshold ahead, warped
}

// warpConn applies the deadline warp and counts the deadlines it saw.
type warpConn struct {
	net.Conn
	counts *warpCounts
}

func (c warpConn) warp(t time.Time) time.Time {
	if t.IsZero() {
		return t
	}
	c.counts.seen.Add(1)
	d := time.Until(t)
	if d <= warpThreshold {
		return t
	}
	c.counts.armed.Add(1)
	return time.Now().Add(d / warpFactor)
}

func (c warpConn) SetDeadline(t time.Time) error      { return c.Conn.SetDeadline(c.warp(t)) }
func (c warpConn) SetReadDeadline(t time.Time) error  { return c.Conn.SetReadDeadline(c.warp(t)) }
func (c warpConn) SetWriteDeadline(t time.Time) error { return c.Conn.SetWriteDeadline(c.warp(t)) }

// warpLongDeadlines routes every loadgen dial through warpConn for the rest
// of the test and returns what the warp saw.
func warpLongDeadlines(t *testing.T) *warpCounts {
	t.Helper()
	counts := new(warpCounts)
	dial := dialTimeoutFunc
	t.Cleanup(func() { dialTimeoutFunc = dial })
	dialTimeoutFunc = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		c, err := dial(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		return warpConn{Conn: c, counts: counts}, nil
	}
	return counts
}

// requireWarped fails the test unless every connection the server accepted
// armed a deadline through the warp: the handshake's 10 s read deadline, at
// least. armed == 0 proves no lifetime deadline only for a connection the
// warp wraps; one dialed around the dialTimeoutFunc hook would carry a real
// 5-minute deadline that no sub-second test can see fire.
func requireWarped(t *testing.T, w *warpCounts, accepted int64) {
	t.Helper()
	if seen := w.seen.Load(); seen < accepted {
		t.Fatalf("the warp saw %d deadline(s) for %d connection(s): the connections were not dialed through dialTimeoutFunc, so armed=%d proves nothing",
			seen, accepted, w.armed.Load())
	}
}

// ---------------------------------------------------------------------------
// #85: the status comes from the response HEADERS, whether or not a body follows

// TestH2StatusMatchesH1 answers each route over HTTP/1.1 and HTTP/2 from the
// same net/http handler and requires the HTTP/2 client to classify every
// response as the HTTP/1.1 client does: status >= 400 is an error.
func TestH2StatusMatchesH1(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("OK")) })
	mux.HandleFunc("/unauthorized", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusUnauthorized) })
	mux.HandleFunc("/bad-gateway", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte("upstream failed\n"))
	})
	mux.HandleFunc("/unavailable", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("try later\n"))
	})
	host, port := startReferenceServer(t, mux)

	for _, tc := range []struct {
		path   string
		status int // 0: a success
		shape  string
	}{
		{"/ok", 0, "200, body"},
		{"/missing", 404, "http.NotFound: 404, then a 19-byte body"},
		{"/unauthorized", 401, "401, no body; net/http Huffman-codes the status"},
		{"/bad-gateway", 502, "502, then a body; Huffman-coded status"},
		{"/unavailable", 503, "503, then a body; literal status"},
	} {
		_, h1err := h1Once(t, host, port, tc.path)
		_, h2err := h2Once(t, host, port, tc.path)
		t.Logf("%-13s h1 err=%v | h2 status=%d err=%v", tc.path, h1err, h2Status(h2err), h2err)
		if (h1err != nil) != (tc.status != 0) {
			t.Fatalf("%s: the HTTP/1.1 control disagrees with the route: err=%v", tc.path, h1err)
		}
		if got := h2Status(h2err); got != tc.status {
			t.Errorf("%s (%s): HTTP/2 recorded status %d (err=%v), HTTP/1.1 recorded %d (err=%v): the HTTP/2 client must take the status from the response HEADERS",
				tc.path, tc.shape, got, h2err, tc.status, h1err)
		}
	}
}

// TestH2StatusRecordedFromHeaders pins the frame layouts that carry a status
// on a HEADERS frame that does not end the stream.
func TestH2StatusRecordedFromHeaders(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, path string) rawH2Action {
		switch path {
		case "/404-body":
			c.headers(sid, false, ":status", "404")
			c.data(sid, true, 19, 0)
		case "/500-body-empty-end":
			c.headers(sid, false, ":status", "500")
			c.data(sid, false, 30, 0)
			c.data(sid, true, 0, 0)
		case "/503-body-trailers":
			c.headers(sid, false, ":status", "503")
			c.data(sid, false, 30, 0)
			c.headers(sid, true, "x-checksum", "abc")
		case "/502-no-body":
			c.headers(sid, true, ":status", "502") // Huffman-coded: 16 bits beat the 3 literal bytes
		case "/401-body":
			c.headers(sid, false, ":status", "401") // Huffman-coded
			c.data(sid, true, 12, 0)
		case "/103-then-404":
			c.headers(sid, false, ":status", "103") // interim response, then the final one
			c.headers(sid, false, ":status", "404")
			c.data(sid, true, 19, 0)
		case "/404-padded-priority":
			c.headersFrame(http2.HeadersFrameParam{StreamID: sid, EndHeaders: true, PadLength: 7,
				Priority: http2.PriorityParam{Weight: 200}}, ":status", "404")
			c.data(sid, true, 19, 0)
		case "/103-then-200":
			c.headers(sid, false, ":status", "103")
			c.headers(sid, false, ":status", "200")
			c.data(sid, true, 19, 0)
		default:
			c.ok(sid)
		}
		return rawKeep
	})
	host, port := srv.hostPort()

	for _, tc := range []struct {
		path   string
		status int // 0: a success
	}{
		{"/404-body", 404},
		{"/500-body-empty-end", 500},
		{"/503-body-trailers", 503},
		{"/502-no-body", 502},
		{"/401-body", 401},
		{"/103-then-404", 404},
		{"/404-padded-priority", 404},
		{"/103-then-200", 0},
		{"/ok", 0},
	} {
		_, err := h2Once(t, host, port, tc.path)
		t.Logf("%-21s status=%d err=%v", tc.path, h2Status(err), err)
		if got := h2Status(err); got != tc.status {
			t.Errorf("%s: recorded status %d (err=%v), want %d: the status must come from the final response HEADERS, whatever frames follow it",
				tc.path, got, err, tc.status)
		}
	}
}

// TestExtractStatusEveryEncoding decodes every status 100-599 in every way an
// HPACK encoder may send it when the dynamic table is off: a literal naming
// :status by any of the static indices 8-14 (x/net's encoder, and so
// net/http's server, uses 14), without indexing, never indexed or with
// incremental indexing, and with a plain or a Huffman-coded value (RFC 7541
// §5.2; an encoder picks Huffman whenever it is shorter, as it is for 401,
// 502, 103 and every code with two digits from {0, 1, 2}).
func TestExtractStatusEveryEncoding(t *testing.T) {
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	enc.SetMaxDynamicTableSizeLimit(0) // as the client advertises
	missed, total := 0, 0
	check := func(desc string, block []byte, status int) {
		total++
		if got := extractStatus(block); got != status {
			missed++
			if missed <= 8 {
				t.Errorf("extractStatus(%s, % x) = %d, want %d", desc, block, got, status)
			}
		}
	}
	for status := 100; status < 600; status++ {
		value := strconv.Itoa(status)
		buf.Reset()
		_ = enc.WriteField(hpack.HeaderField{Name: ":status", Value: value})
		check("x/net hpack encoder", bytes.Clone(buf.Bytes()), status)

		huff := hpack.AppendHuffmanString(nil, value)
		for idx := byte(8); idx <= 14; idx++ {
			for _, first := range []byte{idx, 0x10 | idx, 0x40 | idx} { // without indexing, never indexed, incremental
				check("literal "+value, append([]byte{first, byte(len(value))}, value...), status)
				check("Huffman "+value, append([]byte{first, 0x80 | byte(len(huff))}, huff...), status)
			}
		}
	}
	if missed > 0 {
		t.Errorf("%d of %d encodings of a status were not decoded", missed, total)
	}
}

// TestH2ErrorStatusWithBodyCountsAsError is #85 as the issue measured it: the
// Benchmarker on an all-404-with-body route must report errors and no
// requests, over prior knowledge, the h2c upgrade and a -mix of both.
func TestH2ErrorStatusWithBodyCountsAsError(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		c.headers(sid, false, ":status", "404")
		c.data(sid, true, 19, 0)
		return rawKeep
	})
	opts := HTTP2Options{Connections: 1, MaxStreams: 4}
	for _, tc := range []struct {
		name string
		cfg  Config
	}{
		{"prior-knowledge", Config{HTTP2: true}},
		{"h2c-upgrade", Config{H2CUpgrade: true}},
		{"mix", Config{Mix: &MixRatio{H2: 1, Upgrade: 1}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tc.cfg
			cfg.URL = srv.url("/missing")
			cfg.Duration = 150 * time.Millisecond
			cfg.Workers = 4
			cfg.HTTP2Options = opts
			res := runBench(t, cfg)
			t.Logf("requests=%d errors=%d mix=%+v", res.Requests, res.Errors, res.Mix)
			if res.Requests != 0 || res.Errors == 0 {
				t.Errorf("requests=%d errors=%d, want 0 requests and some errors: every response was a 404 with a body", res.Requests, res.Errors)
			}
			if m := res.Mix; m != nil {
				if m.H2Conns == 0 || m.UpgradeConns == 0 {
					t.Fatalf("mix did not exercise both HTTP/2 paths: %+v", *m)
				}
				if m.H2Requests != 0 || m.UpgradeRequests != 0 || m.H2Errors == 0 || m.UpgradeErrors == 0 {
					t.Errorf("mix stats %+v: want every H2 and upgrade response counted as an error", *m)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// #86: every DATA frame's payload counts, padding excluded

// TestH2BodyBytesMatchH1 compares the body bytes each client reports for the
// same net/http route. net/http's HTTP/2 server ends a body it has already
// flushed with an empty DATA frame; HTTP/1.1 counts the Content-Length (or
// the chunk sizes), so HTTP/2 must count every DATA frame's data.
func TestH2BodyBytesMatchH1(t *testing.T) {
	body64k := bytes.Repeat([]byte("x"), 64<<10)
	body1k := bytes.Repeat([]byte("y"), 1<<10)
	mux := http.NewServeMux()
	mux.HandleFunc("/ok", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("OK")) })
	mux.HandleFunc("/64k", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body64k) })
	mux.HandleFunc("/1k-flushed", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write(body1k)
		w.(http.Flusher).Flush()
	})
	host, port := startReferenceServer(t, mux)

	for _, tc := range []struct {
		path string
		want int
	}{
		{"/ok", 2},
		{"/64k", 64 << 10},
		{"/1k-flushed", 1 << 10},
	} {
		n1, err1 := h1Once(t, host, port, tc.path)
		n2, err2 := h2Once(t, host, port, tc.path)
		t.Logf("%-11s h1 bytes=%d | h2 bytes=%d", tc.path, n1, n2)
		if err1 != nil || err2 != nil {
			t.Fatalf("%s: h1 err=%v h2 err=%v", tc.path, err1, err2)
		}
		if n1 != tc.want {
			t.Fatalf("%s: the HTTP/1.1 control read %d body bytes, want %d", tc.path, n1, tc.want)
		}
		if n2 != n1 {
			t.Errorf("%s: HTTP/2 counted %d body bytes, HTTP/1.1 counted %d: every DATA frame of the response must count", tc.path, n2, n1)
		}
	}
}

// TestH2BodyBytesCountEveryDATAFrame pins the DATA frame layouts, including
// padding (never counted as body, as HTTP/1.1 never counts chunk framing)
// and trailers.
func TestH2BodyBytesCountEveryDATAFrame(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, path string) rawH2Action {
		switch path {
		case "/four-frames": // the usual 16 KiB framing, END_STREAM on the last non-empty frame
			c.headers(sid, false, ":status", "200")
			for i := range 4 {
				c.data(sid, i == 3, 16<<10, 0)
			}
		case "/empty-end": // what net/http sends for a flushed body
			c.headers(sid, false, ":status", "200")
			c.data(sid, false, 64<<10, 0)
			c.data(sid, true, 0, 0)
		case "/trailers":
			c.headers(sid, false, ":status", "200")
			c.data(sid, false, 1000, 0)
			c.data(sid, false, 1000, 0)
			c.headers(sid, true, "x-checksum", "abc")
		case "/padded":
			c.headers(sid, false, ":status", "200")
			for i := range 3 {
				c.data(sid, i == 2, 1000, 16)
			}
		default:
			c.ok(sid)
		}
		return rawKeep
	})
	host, port := srv.hostPort()

	for _, tc := range []struct {
		path string
		want int
	}{
		{"/four-frames", 64 << 10},
		{"/empty-end", 64 << 10},
		{"/trailers", 2000},
		{"/padded", 3000},
		{"/ok", 2},
	} {
		n, err := h2Once(t, host, port, tc.path)
		t.Logf("%-12s bytes=%d want=%d err=%v", tc.path, n, tc.want, err)
		if err != nil {
			t.Fatalf("%s: %v", tc.path, err)
		}
		if n != tc.want {
			t.Errorf("%s: counted %d body bytes, want %d", tc.path, n, tc.want)
		}
	}
}

// TestH2FlowControlCreditsPadding guards the other side of #86: the body
// count leaves padding out, but the connection flow-control window must
// still be credited with every DATA payload byte received, padding included
// (RFC 9113 §6.9.1), or padded responses slowly close the window.
func TestH2FlowControlCreditsPadding(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		c.headers(sid, false, ":status", "200")
		for i := range 3 {
			c.data(sid, i == 2, 1000, 16)
		}
		return rawKeep
	})
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 10))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for range 5 {
		if _, err := cl.DoRequest(ctx, 0); err != nil {
			t.Fatal(err)
		}
	}
	// The client flushes its WINDOW_UPDATEs within a millisecond or so.
	deadline := time.Now().Add(2 * time.Second)
	for srv.windowCredit.Load() != srv.dataSent.Load() && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	credit, sent := srv.windowCredit.Load(), srv.dataSent.Load()
	t.Logf("DATA payload sent (padding included)=%d, connection window credited=%d", sent, credit)
	if credit != sent {
		t.Errorf("the client credited %d bytes of connection window for %d bytes of DATA payload: flow control must count padding too", credit, sent)
	}
}

// TestH2ThroughputCountsEveryDATAFrame is #86 as the issue measured it:
// throughput_bps over a 4 x 16 KiB response must be the whole body per
// request, not a quarter of it.
func TestH2ThroughputCountsEveryDATAFrame(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		c.headers(sid, false, ":status", "200")
		for i := range 4 {
			c.data(sid, i == 3, 16<<10, 0)
		}
		return rawKeep
	})
	res := runBench(t, Config{URL: srv.url("/"), Duration: 150 * time.Millisecond, Workers: 1,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: 4}})
	t.Logf("requests=%d errors=%d rps=%.0f throughput_bps=%.0f bytes/response=%.1f",
		res.Requests, res.Errors, res.RequestsPerSec, res.ThroughputBPS, res.ThroughputBPS/res.RequestsPerSec)
	if res.Requests == 0 || res.Errors != 0 {
		t.Fatalf("requests=%d errors=%d", res.Requests, res.Errors)
	}
	if perReq := res.ThroughputBPS / res.RequestsPerSec; perReq < 65535.5 || perReq > 65536.5 {
		t.Errorf("throughput_bps / requests_per_sec = %.1f bytes per response, want 65536", perReq)
	}
}

// ---------------------------------------------------------------------------
// #88: no lifetime deadline

// TestH2ConnectionHasNoLifetimeDeadline runs one H2 connection past the
// connection-lifetime bound #88 put on it (5 minutes, warped to 200 ms). Like
// HTTP/1.1, which sets no deadline, the connection must keep serving: no
// errors, no second connection.
func TestH2ConnectionHasNoLifetimeDeadline(t *testing.T) {
	warp := warpLongDeadlines(t)
	srv := startRawH2(t, respondOK)
	res := runBench(t, Config{URL: srv.url("/"), Duration: 600 * time.Millisecond, Workers: 1, MaxRPS: 400,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: 4}})
	t.Logf("deadlines seen=%d long deadlines armed=%d requests=%d errors=%d connect_errors=%d connections=%d",
		warp.seen.Load(), warp.armed.Load(), res.Requests, res.Errors, res.ConnectErrors, srv.accepted.Load())
	requireWarped(t, warp, srv.accepted.Load())
	if n := warp.armed.Load(); n != 0 {
		t.Errorf("%d deadline(s) more than %v ahead were armed on the connection: an H2 connection must not carry a lifetime deadline", n, warpThreshold)
	}
	if res.Errors != 0 || res.ConnectErrors != 0 || srv.accepted.Load() != 1 {
		t.Errorf("errors=%d connect_errors=%d connections=%d, want 0, 0 and 1: the connection died at a lifetime deadline (5 min, warped here to %v)",
			res.Errors, res.ConnectErrors, srv.accepted.Load(), 5*time.Minute/warpFactor)
	}
	if res.Requests == 0 {
		t.Error("no request completed")
	}
}

// ---------------------------------------------------------------------------
// #89: a connection that dies without a GOAWAY is redialed

// TestH2RedialsAfterServerClosesWithoutGOAWAY: the server answers one request
// per connection and closes it, with no GOAWAY. The client must redial and
// keep going, count at most the one in-flight stream of each dead
// connection as an error, and not charge the workers queued for a stream.
func TestH2RedialsAfterServerClosesWithoutGOAWAY(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		c.ok(sid)
		return rawClose
	})
	// 8 workers queue for 1 stream: the ones waiting when the connection
	// dies never reached it, so they are moved to the redialed connection.
	res := runBench(t, Config{URL: srv.url("/"), Duration: 300 * time.Millisecond, Workers: 2,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: 1}})
	accepted := srv.accepted.Load()
	t.Logf("connections=%d requests=%d errors=%d connect_errors=%d", accepted, res.Requests, res.Errors, res.ConnectErrors)
	if accepted < 3 {
		t.Fatalf("the server accepted %d connection(s) in %v and requests=%d errors=%d connect_errors=%d: the client did not redial after the server closed without GOAWAY",
			accepted, 300*time.Millisecond, res.Requests, res.Errors, res.ConnectErrors)
	}
	if res.Requests < accepted-1 {
		t.Errorf("requests=%d over %d connections: each connection answers one request", res.Requests, accepted)
	}
	if res.Errors > accepted {
		t.Errorf("errors=%d over %d connections with 1 stream each: more than one error per dead connection means requests that never reached it were charged, or a worker spun",
			res.Errors, accepted)
	}
}

// TestH2ReadErrorMidStreamFailsEachInFlightStreamOnce: on each connection the
// server answers 3 requests, then starts 4 responses (HEADERS and part of the
// body) and ends the connection in the middle of the 4th response's DATA
// frame, so the client's read fails with an unexpected EOF. Each of those 4
// streams is exactly one error, the client redials, and the torn bodies are
// credited to nothing. (The server closes rather than resets: on Darwin 27 a
// reset was seen to reach the client's read ~334 ms late, a kernel delay
// that would make this test's count depend on the platform.)
func TestH2ReadErrorMidStreamFailsEachInFlightStreamOnce(t *testing.T) {
	const answered, inFlight = 3, 4
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		if c.requests <= answered {
			c.ok(sid)
			return rawKeep
		}
		c.headers(sid, false, ":status", "200")
		if c.held++; c.held == inFlight {
			c.tornData(sid, 100, 40) // the connection ends 40 bytes into a 100-byte frame
			return rawClose
		}
		c.data(sid, false, 100, 0) // the body starts and never ends
		return rawKeep
	})
	res := runBench(t, Config{URL: srv.url("/"), Duration: 400 * time.Millisecond, Workers: 1,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: inFlight}})
	accepted, killed := srv.accepted.Load(), srv.killed.Load()
	t.Logf("connections=%d torn=%d requests=%d errors=%d connect_errors=%d bytes/response=%.3f",
		accepted, killed, res.Requests, res.Errors, res.ConnectErrors, res.ThroughputBPS/res.RequestsPerSec)
	if accepted < 3 {
		t.Fatalf("the server accepted %d connection(s) in %v and requests=%d errors=%d: the client did not redial after a read error mid-stream",
			accepted, 400*time.Millisecond, res.Requests, res.Errors)
	}
	// The last torn connection can race the end of the run, which is not an error.
	if lo, hi := inFlight*(killed-1), inFlight*killed; res.Errors < lo || res.Errors > hi {
		t.Errorf("errors=%d after %d torn connections with %d streams in flight each: want %d..%d, one error per in-flight stream",
			res.Errors, killed, inFlight, lo, hi)
	}
	if perReq := res.ThroughputBPS / res.RequestsPerSec; perReq < float64(len(rawOKBody))-0.01 || perReq > float64(len(rawOKBody))+0.01 {
		t.Errorf("throughput_bps / requests_per_sec = %.3f bytes per response, want %d: the partial bodies of failed streams leaked into the count",
			perReq, len(rawOKBody))
	}
}

// TestH2ResponseWinsOverConnectionClose: a worker that reaches its wait
// after readLoop has both answered its stream and failed the connection finds
// the response and the closed connection ready at once, and, since round 1,
// both loops already returned (loopsDone). The response is the request's
// outcome; select alone would pick at random. (Found in review of the #89
// fix, which makes a close right after a response common.)
func TestH2ResponseWinsOverConnectionClose(t *testing.T) {
	hc := &h2Conn{done: make(chan struct{}), loopsDone: make(chan struct{}), streamSem: make(chan struct{}, 1)}
	close(hc.done)
	close(hc.loopsDone)
	for i := range 200 {
		ch := make(chan h2Response, 1)
		ch <- h2Response{status: 200, bytesRead: 2}
		n, err := hc.await(context.Background(), &ch, 0)
		<-hc.streamSem // the token await returned
		if err != nil || n != 2 {
			t.Fatalf("attempt %d: a delivered 200 response on a closed connection returned (%d, %v), want (2, nil)", i, n, err)
		}
	}
}

// failWritesConn fails every Write after the first okWrites on the
// connection with ECONNRESET, writing nothing, while reads go on: the
// connection's death shows on the write side only, as when the server is gone
// but its reset has not reached the client's read.
type failWritesConn struct {
	net.Conn
	okWrites int
	writes   atomic.Int64
}

func (c *failWritesConn) Write(p []byte) (int, error) {
	if c.writes.Add(1) > int64(c.okWrites) {
		return 0, &net.OpError{Op: "write", Net: "tcp", Err: syscall.ECONNRESET}
	}
	return c.Conn.Write(p)
}

// TestH2WriteErrorFailsTheConnection: each connection's writes fail from the
// 6th on (3 go to the handshake) while the server stays silent and keeps the
// connection open, so no read ever fails. The failed write alone must end the
// connection and be redialed. On main a failed flush left bufio's sticky
// error behind, and every later request failed at once on the same dead
// connection: a spin of errors that never redialed.
func TestH2WriteErrorFailsTheConnection(t *testing.T) {
	dial := dialTimeoutFunc
	t.Cleanup(func() { dialTimeoutFunc = dial })
	dialTimeoutFunc = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		c, err := dial(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		return &failWritesConn{Conn: c, okWrites: 5}, nil
	}
	srv := startRawH2(t, respondOK)
	const streams = 4
	res := runBench(t, Config{URL: srv.url("/"), Duration: 300 * time.Millisecond, Workers: 1,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: streams}})
	accepted := srv.accepted.Load()
	t.Logf("connections=%d requests=%d errors=%d connect_errors=%d", accepted, res.Requests, res.Errors, res.ConnectErrors)
	if accepted < 3 {
		t.Fatalf("the server accepted %d connection(s) in %v and requests=%d errors=%d: a connection whose writes fail was not ended and redialed",
			accepted, 300*time.Millisecond, res.Requests, res.Errors)
	}
	if res.Errors > streams*accepted {
		t.Errorf("errors=%d over %d connections with %d streams each: requests kept failing on a dead connection", res.Errors, accepted, streams)
	}
}

// TestH2InFlightStreamAnsweredOnceWhenConnectionDiesMidBody: a POST whose
// body is larger than the send window waits in writeBodyFlowControlled for a
// WINDOW_UPDATE the server never sends; the server reads the 65,535 bytes
// the window allowed and closes the connection. The dead connection must be
// torn down (on main its writer waited for window forever), and the stream
// must be answered exactly once, although both readLoop (the connection
// died) and writeLoop (its body write gave up) fail it.
func TestH2InFlightStreamAnsweredOnceWhenConnectionDiesMidBody(t *testing.T) {
	srv := startRawH2With(t, rawH2Opts{closeAfterData: 65535}, func(*rawH2Conn, uint32, string) rawH2Action {
		return rawKeep // take the request and its body, never answer, never grant window
	})
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/upload", testH2Cfg("POST", nil, make([]byte, 100<<10), 1, 10))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	hc := cl.conns[0].cur.Load()

	// The request, as DoRequest hands it to the connection, with a response
	// channel the test owns and never drains until the connection is gone.
	ch := make(chan h2Response, 1)
	hc.writeCh <- h2WriteReq{kind: h2WriteHeaders, block: cl.headerBlock, data: cl.dataPayload, hasBody: true, respCh: &ch}

	select {
	case <-hc.done:
	case <-time.After(3 * time.Second):
		t.Fatal("the server read the 65,535 body bytes the window allowed and closed the connection, and the client never tore it down: its writer still waits for window on a dead connection")
	}
	var first h2Response
	select {
	case first = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("the in-flight stream was never answered")
	}
	if first.err == nil {
		t.Fatalf("the in-flight stream on a dead connection was answered with success: %+v", first)
	}
	select {
	case second := <-ch:
		t.Errorf("the in-flight stream was answered twice: %v, then %v", first.err, second.err)
	case <-time.After(200 * time.Millisecond):
	}
	t.Logf("answered once: %v", first.err)
}

// TestH2GOAWAYConnectionIsReleased: after a GOAWAY the client redials (it
// always has) and must also close the connection it abandons, instead of
// leaking the socket and its writer goroutine.
func TestH2GOAWAYConnectionIsReleased(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		c.ok(sid)
		return rawGoAway // and leave the connection open for the client to close
	})
	res := runBench(t, Config{URL: srv.url("/"), Duration: 300 * time.Millisecond, Workers: 1,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: 1}})
	if srv.accepted.Load() < 2 {
		t.Fatalf("the server accepted %d connection(s), requests=%d errors=%d: the client did not redial after GOAWAY", srv.accepted.Load(), res.Requests, res.Errors)
	}
	deadline := time.Now().Add(2 * time.Second)
	for srv.clientClosed.Load() < srv.accepted.Load() && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	t.Logf("connections=%d closed by the client=%d requests=%d errors=%d", srv.accepted.Load(), srv.clientClosed.Load(), res.Requests, res.Errors)
	if closed, accepted := srv.clientClosed.Load(), srv.accepted.Load(); closed < accepted {
		t.Errorf("the client closed %d of the %d connections it opened, after Run returned: the ones the server GOAWAYed are leaked", closed, accepted)
	}
}

// ---------------------------------------------------------------------------
// #88 and #89 together

// TestH2ServerEndedConnectionIsRedialedWithoutLifetimeDeadline: with the
// lifetime deadline gone (#88), a connection ends only when the server or the
// network ends it, and that must still be redialed (#89). The server closes
// every connection, without GOAWAY, 300 ms after accepting it; the lifetime
// bound would have fired at 200 ms.
func TestH2ServerEndedConnectionIsRedialedWithoutLifetimeDeadline(t *testing.T) {
	warp := warpLongDeadlines(t)
	srv := startRawH2With(t, rawH2Opts{killAfter: 300 * time.Millisecond}, respondOK)
	const streams = 4
	res := runBench(t, Config{URL: srv.url("/"), Duration: 800 * time.Millisecond, Workers: 1, MaxRPS: 400,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: streams}})
	killed := srv.killed.Load()
	t.Logf("deadlines seen=%d long deadlines armed=%d connections=%d server closes=%d answered after a redial=%d closed by the client=%d requests=%d errors=%d connect_errors=%d",
		warp.seen.Load(), warp.armed.Load(), srv.accepted.Load(), killed, srv.answeredLate.Load(), srv.clientClosed.Load(), res.Requests, res.Errors, res.ConnectErrors)
	requireWarped(t, warp, srv.accepted.Load())
	if n := warp.armed.Load(); n != 0 {
		t.Errorf("%d deadline(s) more than %v ahead were armed: the connection still has a lifetime deadline", n, warpThreshold)
	}
	if killed == 0 || srv.accepted.Load() < 2 || srv.answeredLate.Load() == 0 {
		t.Fatalf("server closes=%d connections=%d requests answered after a redial=%d (requests=%d errors=%d): the client did not redial the connection the server ended",
			killed, srv.accepted.Load(), srv.answeredLate.Load(), res.Requests, res.Errors)
	}
	if res.Errors > streams*killed {
		t.Errorf("errors=%d after %d server closes with at most %d streams in flight each", res.Errors, killed, streams)
	}
	if srv.clientClosed.Load() > 1 { // the one Close at the end of the run
		t.Errorf("the client ended %d connections itself before the server did", srv.clientClosed.Load())
	}
}

// ---------------------------------------------------------------------------
// Review of the #85-#89 fixes (PR #91, round 1)

// TestH2StreamIDExhaustionRedials: with no lifetime deadline (#88) a
// connection lives as long as the server keeps it, so a long run can use up
// its 2^30 client stream IDs. RFC 9113 §5.1.1: a client that cannot open a
// new stream opens a new connection. The request that finds the IDs used up
// never reached the server, so it goes to a redialed connection and does not
// fail, and the used-up connection is closed. On 2930ded every request from
// then on failed at once on a connection nothing retired.
func TestH2StreamIDExhaustionRedials(t *testing.T) {
	srv := startRawH2(t, respondOK)
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 1))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	// Three stream IDs left on the first connection: 2^31-5, 2^31-3, 2^31-1.
	cl.conns[0].cur.Load().nextStreamID.Store(0x7FFFFFFB)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 8 {
		if _, err := cl.DoRequest(ctx, 0); err != nil {
			t.Fatalf("request %d: %v (connections=%d): a request that finds the stream IDs used up must go to a new connection, not fail",
				i+1, err, srv.accepted.Load())
		}
	}
	deadline := time.Now().Add(2 * time.Second)
	for srv.clientClosed.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("8 requests: connections=%d closed by the client=%d", srv.accepted.Load(), srv.clientClosed.Load())
	if n := srv.accepted.Load(); n != 2 {
		t.Errorf("connections=%d, want 2: one redial when the first connection's stream IDs ran out", n)
	}
	if srv.clientClosed.Load() < 1 {
		t.Error("the client never closed the connection whose stream IDs ran out")
	}
}

// uploadWindow is the SETTINGS_INITIAL_WINDOW_SIZE of the servers below: a
// request body of 2,000 bytes sends 1,000 and then waits for window that the
// server never grants.
var uploadWindow = []http2.Setting{{ID: http2.SettingInitialWindowSize, Val: 1000}}

// sendUpload hands one POST with a 2,000-byte body to the client's first
// connection, as DoRequest does, with a response channel the test owns.
func sendUpload(t *testing.T, srv *rawH2Server) (*h2Conn, chan h2Response) {
	t.Helper()
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/upload", testH2Cfg("POST", nil, make([]byte, 2000), 1, 10))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	hc := cl.conns[0].cur.Load()
	ch := make(chan h2Response, 1)
	hc.writeCh <- h2WriteReq{kind: h2WriteHeaders, block: cl.headerBlock, data: cl.dataPayload, hasBody: true, respCh: &ch}
	return hc, ch
}

// TestH2WriteErrorWhileBodyWaitsForWindow: a POST body waits for send window,
// and the flush it makes while waiting is the connection's first write to
// fail. The server stays silent and keeps the connection open, so no read
// fails. The failed flush must end the connection and answer the stream,
// once, as a failed write anywhere else does (#89). On 2930ded the wait
// discarded the flush error and polled for window until the run ended, with
// every worker of the connection stuck behind it.
func TestH2WriteErrorWhileBodyWaitsForWindow(t *testing.T) {
	dial := dialTimeoutFunc
	t.Cleanup(func() { dialTimeoutFunc = dial })
	dialTimeoutFunc = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		c, err := dial(network, addr, timeout)
		if err != nil {
			return nil, err
		}
		return &failWritesConn{Conn: c, okWrites: 3}, nil // the handshake's 3 writes succeed, every later one fails
	}
	srv := startRawH2With(t, rawH2Opts{settings: uploadWindow}, func(*rawH2Conn, uint32, string) rawH2Action {
		return rawKeep // never answer, never grant window
	})
	hc, ch := sendUpload(t, srv)

	select {
	case <-hc.done:
	case <-time.After(3 * time.Second):
		t.Fatal("the flush made while the body waited for window failed, and the client never ended the connection: its writer still polls for window on a connection it cannot write to")
	}
	var first h2Response
	select {
	case first = <-ch:
	case <-time.After(2 * time.Second):
		t.Fatal("the stream whose body could not be written was never answered")
	}
	if first.err == nil {
		t.Fatalf("the stream whose body could not be written was answered with success: %+v", first)
	}
	select {
	case second := <-ch:
		t.Errorf("the stream was answered twice: %v, then %v", first.err, second.err)
	case <-time.After(200 * time.Millisecond):
	}
	t.Logf("answered once: %v", first.err)
}

// TestH2WindowCreditSentWhileBodyWaitsForWindow: while a POST body waits for
// send window, the client must still return receive window to the server.
// The server answers the POST at once with 10,000 bytes of a body it does not
// end, and never grants window for the upload. The client must credit those
// bytes to the connection window although its writer is parked in the
// flow-control wait: a server that waits for that credit before it reads or
// grants more deadlocks with the client otherwise. On 2930ded the credit was
// sent only once the wait ended, which it never did.
func TestH2WindowCreditSentWhileBodyWaitsForWindow(t *testing.T) {
	const respBody = 10000
	srv := startRawH2With(t, rawH2Opts{settings: uploadWindow}, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		c.headers(sid, false, ":status", "200")
		c.data(sid, false, respBody, 0)
		return rawKeep // never end the response, never grant window
	})
	sendUpload(t, srv)

	deadline := time.Now().Add(2 * time.Second)
	for srv.windowCredit.Load() < respBody && time.Now().Before(deadline) {
		time.Sleep(2 * time.Millisecond)
	}
	t.Logf("response bytes received by the client=%d, connection window credited=%d", srv.dataSent.Load(), srv.windowCredit.Load())
	if got := srv.windowCredit.Load(); got < respBody {
		t.Errorf("the client credited %d of the %d response bytes it received while its request body waited for window: the writer must send WINDOW_UPDATE while it waits", got, respBody)
	}
}

// TestH2RedialBacksOffOnlyWhenTheServerLooksDown: how a slot redials, matched
// to h1client (h1client.go DoRequest). h1client redials a connection the
// server ended at once, and when the redial fails it counts a connect error
// and sleeps its backoff inside the request it returns as an error, so the
// sleep reaches no recorded latency; the next request dials at once.
//
// HTTP/2 does the same, and treats a connection that ended before it carried
// a response to any request as the server looking down too: the request that
// finds it fails and sleeps, and nothing is dialed for it, so a server that
// completes each handshake and then drops the connection is paced and shows
// as errors. The pace doubles while the server keeps looking down, and is
// reset once a connection has served. Every subtest drives DoRequest; the
// backoff is set far beyond each context where a sleep must not happen.
func TestH2RedialBacksOffOnlyWhenTheServerLooksDown(t *testing.T) {
	srv := startRawH2(t, respondOK)
	host, port := srv.hostPort()
	newClient := func(t *testing.T) *h2Client {
		cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 1))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		return cl
	}
	const longBackoff = 20 * time.Second // sleep() jitters it to 10-20 s
	// endConn ends the slot's connection as a server that closes it does.
	endConn := func(slot *h2ConnSlot, why string) {
		slot.cur.Load().failConn(errors.New("test: " + why))
	}
	// emptySlot leaves the slot as a failed redial leaves it.
	emptySlot := func(slot *h2ConnSlot) {
		slot.cur.Load().closeConn()
		slot.cur.Store(nil)
	}
	ctxFor := func(t *testing.T, d time.Duration) context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), d)
		t.Cleanup(cancel)
		return ctx
	}

	t.Run("answered", func(t *testing.T) {
		cl := newClient(t)
		dials := countDials(cl, 0)
		ctx := ctxFor(t, 3*time.Second)
		if _, err := cl.DoRequest(ctx, 0); err != nil {
			t.Fatal(err)
		}
		slot := cl.conns[0]
		endConn(slot, "the server ended the connection")
		slot.backoff.next = longBackoff
		start := time.Now()
		_, err := cl.DoRequest(ctx, 0)
		t.Logf("request after a connection that answered: %v, err=%v, dials=%d", time.Since(start), err, dials.Load())
		if err != nil {
			t.Fatalf("the request that redials a connection which had answered failed after %v (%v): h1client redials such a connection at once, without its backoff",
				time.Since(start), err)
		}
	})
	t.Run("never answered", func(t *testing.T) {
		cl := newClient(t)
		dials := countDials(cl, 0)
		slot := cl.conns[0]
		endConn(slot, "the server dropped the connection before it answered anything")
		slot.backoff.next = 200 * time.Millisecond // sleep() jitters it to 100-200 ms
		start := time.Now()
		_, err := cl.DoRequest(ctxFor(t, 3*time.Second), 0)
		elapsed := time.Since(start)
		t.Logf("request that found a connection which never answered: %v, err=%v, dials=%d", elapsed, err, dials.Load())
		if err == nil {
			t.Fatalf("the request that found a connection which ended before it answered anything succeeded, after %v and %d dial(s): the server looks down, so it must fail, sleep the backoff inside that failure and dial nothing, or the sleep lands in a recorded latency and the outage shows no error",
				elapsed, dials.Load())
		}
		if n := dials.Load(); n != 0 {
			t.Errorf("%d dial(s) for the request that found a connection which never answered: it fails, and the next request dials", n)
		}
		if elapsed < 100*time.Millisecond {
			t.Errorf("the request returned after %v: it did not sleep the backoff (100-200 ms), so a server that drops every connection is redialed in a loop", elapsed)
		}
	})
	t.Run("after a failed redial", func(t *testing.T) {
		cl := newClient(t)
		slot := cl.conns[0]
		emptySlot(slot)
		slot.backoff.next = longBackoff
		start := time.Now()
		_, err := cl.DoRequest(ctxFor(t, 3*time.Second), 0)
		t.Logf("request after a failed redial: %v, err=%v", time.Since(start), err)
		if err != nil {
			t.Fatalf("the request after a failed redial failed after %v (%v): it slept the backoff before it dialed, so the sleep lands in its latency when the dial succeeds; h1client sleeps only inside the request whose redial failed",
				time.Since(start), err)
		}
	})
	t.Run("a failed redial", func(t *testing.T) {
		cl := newClient(t)
		ctx := ctxFor(t, 3*time.Second)
		if _, err := cl.DoRequest(ctx, 0); err != nil {
			t.Fatal(err)
		}
		dials := countDials(cl, 1)
		slot := cl.conns[0]
		endConn(slot, "the server ended the connection")
		start := time.Now()
		_, err := cl.DoRequest(ctx, 0)
		elapsed := time.Since(start)
		t.Logf("request whose redial failed: %v, err=%v, dials=%d", elapsed, err, dials.Load())
		if err == nil || dials.Load() != 1 {
			t.Fatalf("err=%v dials=%d, want an error after 1 failed dial", err, dials.Load())
		}
		// The connection it replaces had served, so the pace starts over:
		// the first step sleeps 5-10 ms.
		if elapsed < reconnectBackoffMin/2 {
			t.Errorf("the request whose redial failed returned after %v without sleeping the backoff (at least %v): as in h1client it paces the server that looks down inside the request it fails",
				elapsed, reconnectBackoffMin/2)
		}
	})
	t.Run("the sleep holds no lock", func(t *testing.T) {
		cl := newClient(t)
		slot := cl.conns[0]
		emptySlot(slot)
		slot.backoff.next = longBackoff
		failed := make(chan struct{})
		var n atomic.Int64
		redial := cl.redial
		cl.redial = func() (*h2Conn, error) {
			if n.Add(1) == 1 {
				close(failed)
				return nil, errors.New("test: connection refused")
			}
			return redial()
		}
		ctxA, cancelA := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancelA()
		aDone := make(chan error, 1)
		go func() {
			_, err := cl.DoRequest(ctxA, 0)
			aDone <- err
		}()
		select {
		case <-failed:
		case <-time.After(3 * time.Second):
			t.Fatal("the request on an empty slot did not dial within 3 s: it slept the backoff before it dialed")
		}
		// A's redial failed and A sleeps 10-20 s. B, on the same slot, must
		// not wait for that sleep: it dials at once.
		ctxB, cancelB := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancelB()
		bDone := make(chan error, 1)
		start := time.Now()
		go func() {
			_, err := cl.DoRequest(ctxB, 1)
			bDone <- err
		}()
		select {
		case err := <-bDone:
			t.Logf("second request while the first sleeps its backoff: %v, err=%v", time.Since(start), err)
			if err != nil {
				t.Fatalf("the second request failed (%v): it waited for the backoff sleep of the request whose redial failed", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("the second request did not return within 5 s: the request whose redial failed sleeps its backoff holding the slot lock, and every worker of the slot waits for it")
		}
		aReturned := false
		select {
		case err := <-aDone:
			aReturned = true
			t.Errorf("the request whose redial failed returned (%v) before its backoff ended", err)
		default:
		}
		cancelA()
		if !aReturned {
			<-aDone
		}
	})
	t.Run("grows until a connection serves", func(t *testing.T) {
		// Every connection is dialed by DoRequest (reconnectSlot), as in a
		// run: while down, the server completes each handshake and drops the
		// connection at its first request, unanswered; up, it answers.
		var up atomic.Bool
		flaky := startRawH2(t, func(c *rawH2Conn, sid uint32, path string) rawH2Action {
			if up.Load() {
				return respondOK(c, sid, path)
			}
			return rawClose
		})
		fh, fp := flaky.hostPort()
		cl, err := newH2Client(fh, fp, "/", testH2Cfg("GET", nil, nil, 1, 1))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		dials := countDials(cl, 0)
		slot := cl.conns[0]
		ctx := ctxFor(t, 5*time.Second)
		for round := 1; round <= 3; round++ {
			// The request on the slot's connection (from round 2 on, one
			// DoRequest dialed after the round before): the server drops it.
			if _, err := cl.DoRequest(ctx, 0); err == nil {
				t.Fatalf("round %d: the request the server dropped unanswered succeeded", round)
			}
			// The next request finds a connection that never answered.
			if _, err := cl.DoRequest(ctx, 0); !errors.Is(err, errH2NeverServed) {
				t.Fatalf("round %d: the request that found a connection which never answered got %v, want errH2NeverServed", round, err)
			}
			want := min(reconnectBackoffMin<<round, reconnectBackoffMax)
			t.Logf("round %d: backoff.next=%v dials=%d", round, slot.backoff.next, dials.Load())
			if slot.backoff.next != want {
				t.Errorf("round %d: backoff.next=%v, want %v: while every connection ends before it answers, the pace must keep doubling, not restart at every dial that succeeds", round, slot.backoff.next, want)
			}
		}
		if n := dials.Load(); n != 2 {
			t.Errorf("%d dials in 3 rounds, want 2: rounds 2 and 3 each dial once, through DoRequest", n)
		}
		// The server is up again. The next request dials at once and is
		// answered: that connection has served. When it ends, the request
		// that finds it redials at once (the server drops that one again),
		// and the pace starts over: the next failure sleeps 5-10 ms.
		up.Store(true)
		if _, err := cl.DoRequest(ctx, 0); err != nil {
			t.Fatalf("the request after the server came back failed: %v", err)
		}
		up.Store(false)
		slot.backoff.next = longBackoff
		endConn(slot, "the server ended the connection")
		start := time.Now()
		if _, err := cl.DoRequest(ctx, 0); err == nil {
			t.Fatal("the request the server dropped unanswered succeeded")
		}
		_, err = cl.DoRequest(ctx, 0)
		t.Logf("first request to find a dropped connection after one served: %v, err=%v, backoff.next=%v", time.Since(start), err, slot.backoff.next)
		if !errors.Is(err, errH2NeverServed) {
			t.Fatalf("got %v, want errH2NeverServed", err)
		}
		if d := time.Since(start); d > time.Second || slot.backoff.next != reconnectBackoffMin<<1 {
			t.Errorf("the first backoff after a connection served took %v and left backoff.next=%v: the pace was not reset (want 5-10 ms and %v)",
				d, slot.backoff.next, reconnectBackoffMin<<1)
		}
	})
	t.Run("h2c: only the upgrade's own stream answered", func(t *testing.T) {
		// The server answers the upgrade request on stream 1 (RFC 7540 §3.2;
		// celeris writes it before it reads the preface) and refuses every
		// request of the run. The upgrade's response is a handshake artefact,
		// not an answer to a request: the connection has served nothing.
		up := startRawH2With(t, rawH2Opts{answerUpgrade: true}, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
			c.reset(sid, http2.ErrCodeRefusedStream)
			return rawKeep
		})
		uh, uport := up.hostPort()
		cl, err := newH2CUpgradeClient(uh, uport, "/", testH2Cfg("GET", nil, nil, 1, 1))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cl.Close)
		dials := countDials(cl, 0)
		slot := cl.conns[0]
		// The refusal arrives after the stream-1 response, which readLoop has
		// therefore read.
		var re *HTTP2ResetError
		if _, err := cl.DoRequest(ctxFor(t, 3*time.Second), 0); !errors.As(err, &re) || re.Code != uint32(http2.ErrCodeRefusedStream) {
			t.Fatalf("got %v, want the REFUSED_STREAM reset", err)
		}
		if slot.cur.Load().served.Load() {
			t.Error("the h2c upgrade's own stream-1 response marked the connection served, though no request of the run was answered on it")
		}
		endConn(slot, "the server dropped the connection")
		slot.backoff.next = longBackoff
		_, err = cl.DoRequest(ctxFor(t, 300*time.Millisecond), 0)
		t.Logf("request after an upgraded connection that answered only stream 1: err=%v, dials=%d", err, dials.Load())
		if n := dials.Load(); n != 0 {
			t.Errorf("the connection that answered only the upgrade's stream 1 was redialed at once (%d dial(s)): a server that answers the upgrade and then drops every connection is redialed in a loop", n)
		}
	})
	// Guard: with no sleep in the way, the redial must still not outlive
	// the run. Benchmarker.Run cancels the context before it calls Close, so
	// a connection dialed after that would be one Close never sees.
	t.Run("run over", func(t *testing.T) {
		cl := newClient(t)
		ctx, cancel := context.WithCancel(context.Background())
		if _, err := cl.DoRequest(ctx, 0); err != nil {
			t.Fatal(err)
		}
		dials := countDials(cl, 0)
		endConn(cl.conns[0], "the server ended the connection")
		cancel()
		if _, err := cl.DoRequest(ctx, 0); err == nil || dials.Load() != 0 {
			t.Fatalf("err=%v dials=%d after the run's context ended: a connection dialed now is one Close, which runs after the cancel, never closes", err, dials.Load())
		}
	})
}

// countDials routes the client's redials through a counter and fails the
// first failFirst of them, as a refused connection would.
func countDials(cl *h2Client, failFirst int64) *atomic.Int64 {
	var n atomic.Int64
	redial := cl.redial
	cl.redial = func() (*h2Conn, error) {
		if n.Add(1) <= failFirst {
			return nil, errors.New("test: connection refused")
		}
		return redial()
	}
	return &n
}

// TestH2ResponseWithoutAReadableStatusIsNotASuccess: a response counts as a
// success only when its :status says so, as an HTTP/1.1 response does only
// with a status line (h1client: "short status line" is an error). A :status
// sent as a literal with a literal name (RFC 7541 §6.2, name index 0) is a
// status like any other; a response with no :status, or a HEADERS or DATA
// frame whose pad length does not fit its payload (RFC 9113 §6.1, §6.2: a
// connection error), is an error. On 2930ded each of these was a success.
func TestH2ResponseWithoutAReadableStatusIsNotASuccess(t *testing.T) {
	literalName := func(first byte, name []byte, huffman bool, value string) []byte {
		n := byte(len(name))
		if huffman {
			n |= 0x80
		}
		b := append([]byte{first, n}, name...)
		return append(append(b, byte(len(value))), value...)
	}
	plain, huff := []byte(":status"), hpack.AppendHuffmanString(nil, ":status")
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, path string) rawH2Action {
		endAll := http2.FlagHeadersEndHeaders | http2.FlagHeadersEndStream
		switch path {
		case "/literal-name-404": // without indexing, literal name
			_ = c.fr.WriteRawFrame(http2.FrameHeaders, endAll, sid, literalName(0x00, plain, false, "404"))
		case "/literal-name-huffman-503": // never indexed, Huffman-coded literal name
			_ = c.fr.WriteRawFrame(http2.FrameHeaders, endAll, sid, literalName(0x10, huff, true, "503"))
		case "/literal-name-indexed-502": // incremental indexing, literal name
			_ = c.fr.WriteRawFrame(http2.FrameHeaders, endAll, sid, literalName(0x40, plain, false, "502"))
		case "/no-status": // a field block without :status
			c.headers(sid, true, "content-type", "text/plain")
		case "/headers-bad-padding": // Pad Length 200 in a 2-byte payload, then :status 200
			_ = c.fr.WriteRawFrame(http2.FrameHeaders, endAll|http2.FlagHeadersPadded, sid, []byte{200, 0x88})
		case "/data-bad-padding": // :status 200, then DATA whose Pad Length 50 exceeds its 11-byte payload
			c.headers(sid, false, ":status", "200")
			_ = c.fr.WriteRawFrame(http2.FrameData, http2.FlagDataEndStream|http2.FlagDataPadded, sid, append([]byte{50}, make([]byte, 10)...))
		default:
			c.ok(sid)
		}
		return rawKeep
	})
	host, port := srv.hostPort()

	for _, tc := range []struct {
		path   string
		status int // 0: a success; -1: an error other than a status
	}{
		{"/literal-name-404", 404},
		{"/literal-name-huffman-503", 503},
		{"/literal-name-indexed-502", 502},
		{"/no-status", -1},
		{"/headers-bad-padding", -1},
		{"/data-bad-padding", -1},
		{"/ok", 0},
	} {
		n, err := h2Once(t, host, port, tc.path)
		t.Logf("%-26s status=%d bytes=%d err=%v", tc.path, h2Status(err), n, err)
		if got := h2Status(err); got != tc.status {
			t.Errorf("%s: recorded status %d (err=%v), want %d (0: success, -1: an error): a response is a success only when its :status says so",
				tc.path, got, err, tc.status)
		}
	}
}

// TestH2StreamOutlivedByLaterStreamsGetsItsResponse: a connection keeps each
// stream's response channel in slot (streamID/2) mod 2*MaxStreams, so a
// stream still waiting when 2*MaxStreams-1 later streams have been opened
// shares its slot with the next one. With 2 streams (4 slots), the server
// holds the first stream while a second worker sends 4 more; the 5th stream
// on the connection would take the held stream's slot. The server then
// answers both. Each worker must get its own response. On main (a89f02d)
// and on 2930ded the 5th stream overwrote the held stream's channel: its
// response went to the other worker, and the held stream's worker waited
// for the rest of the run. (Found in round 1, when an h2c benchmark with no
// deadline hung for 11 minutes on a loaded machine.)
func TestH2StreamOutlivedByLaterStreamsGetsItsResponse(t *testing.T) {
	firstSeen := make(chan struct{})
	var held uint32
	srv := startRawH2With(t, rawH2Opts{settings: []http2.Setting{{ID: http2.SettingMaxConcurrentStreams, Val: 2}}},
		func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
			switch c.requests {
			case 1: // hold the first stream
				held = sid
				close(firstSeen)
			case 5: // the stream after 3 more: it would reuse the held stream's slot
				c.ok(sid)
				c.ok(held)
			default:
				c.ok(sid)
			}
			return rawKeep
		})
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 2))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	type result struct {
		n   int
		err error
	}
	first := make(chan result, 1)
	go func() {
		n, err := cl.DoRequest(ctx, 0)
		first <- result{n, err}
	}()
	select {
	case <-firstSeen:
	case <-time.After(3 * time.Second):
		t.Fatal("the first request never reached the server")
	}
	for i := range 4 {
		if n, err := cl.DoRequest(ctx, 0); err != nil || n != len(rawOKBody) {
			t.Fatalf("second worker, request %d: bytes=%d err=%v", i+1, n, err)
		}
	}
	r := <-first
	t.Logf("held stream: bytes=%d err=%v (server streams: held=%d)", r.n, r.err, held)
	if r.err != nil || r.n != len(rawOKBody) {
		t.Errorf("the server answered the held stream, but its worker got bytes=%d err=%v: a later stream took its slot and its response", r.n, r.err)
	}
}

// TestH2RequestsTheConnectionNeverWroteAreRetried: 4 workers share one
// connection with 4 streams and POST 100 KiB bodies; the server starts each
// response, reads the 65,535 bytes the connection window allows and closes
// the connection. The first request's body was being written, so it is in
// flight and is one error. The other three were still queued for the writer:
// they never reached the server, so, like a request still waiting for a
// stream, they go to the redialed connection and are not errors. Before the
// fix their workers woke on the closed connection and gave up before the
// writer answered, and the writer failed the ones it reached: 4 errors per
// connection. (The server starts each response so that its connections have
// served: since round 2 a connection that ends before it answered anything
// costs the request that finds it, which is TestH2RedialBacksOff...'s case.)
func TestH2RequestsTheConnectionNeverWroteAreRetried(t *testing.T) {
	srv := startRawH2With(t, rawH2Opts{closeAfterData: 65535}, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		c.headers(sid, false, ":status", "200") // start the response, never end it
		return rawKeep
	})
	const streams = 4
	res := runBench(t, Config{URL: srv.url("/upload"), Method: "POST", Body: make([]byte, 100<<10), Duration: 400 * time.Millisecond,
		Workers: streams, HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: streams}})
	killed := srv.killed.Load()
	t.Logf("connections=%d closed by the server=%d requests=%d errors=%d errors per closed connection=%.2f",
		srv.accepted.Load(), killed, res.Requests, res.Errors, float64(res.Errors)/float64(max(killed, 1)))
	if killed < 3 {
		t.Fatalf("the server closed %d connection(s): the client did not keep redialing", killed)
	}
	if res.Errors > killed {
		t.Errorf("errors=%d over %d connections the server closed with one body in flight each: requests the connection never wrote were charged as errors",
			res.Errors, killed)
	}
}

// TestH2ClosedConnectionWritesNoNewRequest: once a connection takes no new
// request, its writer writes none of those it still holds: the one it takes
// next, and the ones still queued when it stops. Each is answered
// errH2NotSent, so DoRequest takes it to the redialed connection.
func TestH2ClosedConnectionWritesNoNewRequest(t *testing.T) {
	var wire bytes.Buffer
	bw := bufio.NewWriter(&wire)
	hc := &h2Conn{bufWriter: bw, framer: newH2Framer(bw, nil), writeCh: make(chan h2WriteReq, 4), streamSlots: make([]h2StreamSlot, 4)}
	hc.nextStreamID.Store(1)
	hc.closed.Store(true)

	chans := make([]chan h2Response, 4)
	for i := range chans {
		chans[i] = make(chan h2Response, 1)
	}
	hc.processWriteReq(h2WriteReq{kind: h2WriteHeaders, block: []byte{0x82}, respCh: &chans[0]}) // the one it takes next
	for i := 1; i < len(chans); i++ {
		hc.writeCh <- h2WriteReq{kind: h2WriteHeaders, block: []byte{0x82}, respCh: &chans[i]} // still queued
	}
	hc.answerQueued()
	if err := bw.Flush(); err != nil {
		t.Fatal(err)
	}
	for i, ch := range chans {
		select {
		case r := <-ch:
			if r.err != errH2NotSent {
				t.Errorf("request %d on a closed connection was answered %+v, want errH2NotSent", i, r)
			}
		default:
			t.Errorf("request %d on a closed connection was never answered", i)
		}
	}
	if wire.Len() != 0 {
		t.Errorf("the writer wrote %d bytes for requests on a closed connection, want 0", wire.Len())
	}
}

// TestH2AwaitTakesAnAnswerThatArrivesAfterTheClose: a worker that sees its
// connection close may still be owed an answer: a response readLoop took
// before the close and is delivering, or writeLoop's errH2NotSent for a
// request it never wrote. await waits until both loops have returned
// (loopsDone), and only a request no loop answered is "connection closing".
// Deciding at once made the response an error (N1) and the unsent request
// an error instead of a retry (N2).
func TestH2AwaitTakesAnAnswerThatArrivesAfterTheClose(t *testing.T) {
	for _, tc := range []struct {
		name   string
		answer *h2Response // nil: no loop answers
	}{
		{"response in delivery", &h2Response{status: 200, bytesRead: 2}},
		{"request never written", &h2Response{err: errH2NotSent}},
		{"no answer", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hc := &h2Conn{done: make(chan struct{}), loopsDone: make(chan struct{}), streamSem: make(chan struct{}, 1)}
			close(hc.done)
			ch := make(chan h2Response, 1)
			go func() {
				time.Sleep(20 * time.Millisecond) // the loops are still at work when the worker sees done
				if tc.answer != nil {
					ch <- *tc.answer
				}
				close(hc.loopsDone)
			}()
			n, err := hc.await(context.Background(), &ch, 0)
			<-hc.streamSem // the token await returned
			t.Logf("await returned (%d, %v)", n, err)
			switch {
			case tc.answer == nil:
				if err == nil || errors.Is(err, errH2NotSent) {
					t.Errorf("no loop answered, await returned (%d, %v), want a connection-closing error", n, err)
				}
			case tc.answer.err != nil:
				if err != errH2NotSent {
					t.Errorf("writeLoop answered errH2NotSent after the close, await returned (%d, %v)", n, err)
				}
			default:
				if err != nil || n != 2 {
					t.Errorf("readLoop delivered a 200 after the close, await returned (%d, %v), want (2, nil)", n, err)
				}
			}
		})
	}
}

// TestH2ConnectionLoopsEndAfterClose: await relies on readLoop and writeLoop
// returning soon after a connection closes, and on loopsDone saying so. Close
// a live connection and require loopsDone within 2 s.
func TestH2ConnectionLoopsEndAfterClose(t *testing.T) {
	srv := startRawH2(t, respondOK)
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := cl.DoRequest(ctx, 0); err != nil {
		t.Fatal(err)
	}
	hc := cl.conns[0].cur.Load()
	hc.closeConn()
	select {
	case <-hc.loopsDone:
	case <-time.After(2 * time.Second):
		t.Fatal("2 s after the connection closed, its readLoop and writeLoop had not both returned: a worker waiting in await for them would wait until its context ends")
	}
}

// ---------------------------------------------------------------------------
// Review of the fixes, round 2

// TestH2CUpgradeServerThatAnswersOnlyTheUpgradeIsPaced: an h2c server that
// answers the upgrade request on stream 1, as RFC 7540 §3.2 requires, and
// then closes each connection when the first request of the run arrives,
// without answering it. The stream-1 response must not count as the
// connection having served: every connection ends before it answered a
// request, so the server looks down, and the redials are paced. On e37777e
// the stream-1 response marked each connection served, so each was redialed
// at once: a dial storm, as on a dead port before the backoff existed.
func TestH2CUpgradeServerThatAnswersOnlyTheUpgradeIsPaced(t *testing.T) {
	srv := startRawH2With(t, rawH2Opts{answerUpgrade: true}, func(*rawH2Conn, uint32, string) rawH2Action {
		return rawClose
	})
	res := runBench(t, Config{URL: srv.url("/"), Duration: 300 * time.Millisecond, Workers: 1,
		H2CUpgrade: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: 4}})
	accepted := srv.accepted.Load()
	t.Logf("connections=%d requests=%d errors=%d connect_errors=%d", accepted, res.Requests, res.Errors, res.ConnectErrors)
	if accepted > 60 {
		t.Errorf("%d connections in %v to a server that answered no request: they were redialed without backoff", accepted, 300*time.Millisecond)
	}
	if res.Requests != 0 || res.Errors == 0 {
		t.Errorf("requests=%d errors=%d, want 0 and at least 1: the server answered nothing", res.Requests, res.Errors)
	}
}

// TestH2ServerThatDropsEveryConnectionShowsErrors: the server completes each
// handshake, then sends GOAWAY before any request. No request reaches it, so
// none is in flight when a connection ends; each still costs the request that
// finds the connection gone, as h1client charges each request to a server
// that drops its connections, and the pace grows, so the run shows the outage
// as errors and a handful of connections. On e37777e the pace restarted at
// every dial that succeeded (44 and 46 connections in two runs), and whether
// a request was an error or retried silently depended on whether it was
// written before the GOAWAY was read (43 and 24 errors). The server never
// answers a request, so a client that ignored the GOAWAY would wait on its
// first connection for the whole run: 1 connection and no error, which the
// floor below fails (fixed code: 10 connections and 10-16 errors in 400 ms).
func TestH2ServerThatDropsEveryConnectionShowsErrors(t *testing.T) {
	srv := startRawH2With(t, rawH2Opts{goAwayAtOnce: true}, func(*rawH2Conn, uint32, string) rawH2Action {
		return rawKeep
	})
	res := runBench(t, Config{URL: srv.url("/"), Duration: 400 * time.Millisecond, Workers: 1,
		HTTP2: true, HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: 4}})
	accepted := srv.accepted.Load()
	t.Logf("connections=%d requests=%d errors=%d connect_errors=%d", accepted, res.Requests, res.Errors, res.ConnectErrors)
	if accepted < 3 || res.Errors == 0 {
		t.Errorf("connections=%d errors=%d, want at least 3 and 1: the server drops every connection at once, so the client must keep redialing it and count the outage, not wait on one connection (#89)",
			accepted, res.Errors)
	}
	if res.Errors < accepted-2 {
		t.Errorf("errors=%d over %d connections that each ended before answering anything: the outage is not counted", res.Errors, accepted)
	}
	if accepted > 25 {
		t.Errorf("%d connections in %v to a server that drops every one: the backoff does not grow while the server looks down", accepted, 400*time.Millisecond)
	}
}

// TestH2BodyWaitEndsWhenTheServerEndsTheStream: a POST body larger than the
// server's stream window waits for a WINDOW_UPDATE; the server ends the
// stream first and never grants more. RFC 9113 §8.1 lets a server answer
// before the body is complete and then RST_STREAM(NO_ERROR) (x/net's server
// does so when its handler returns without reading the body); a server may
// also refuse the stream. It sends no window for a stream it has ended, so
// the wait must end there, without failing the connection: the later requests
// of the connection are queued behind the writer. A response that ends
// without a reset leaves the client's half of the stream open, which the
// client closes with RST_STREAM(CANCEL); after a reset it sends none (§5.4.2).
// On e37777e the writer polled for window until the run ended, and every
// later request of the connection hung, uncounted.
func TestH2BodyWaitEndsWhenTheServerEndsTheStream(t *testing.T) {
	const requests = 3
	for _, tc := range []struct {
		name       string
		answer     func(c *rawH2Conn, sid uint32)
		success    bool  // the request is a success (else the reset)
		minCancels int64 // RST_STREAM(CANCEL) the client must send, at least
		maxResets  int64 // RST_STREAM the client may send, at most
	}{
		{"response then RST_STREAM(NO_ERROR)", func(c *rawH2Conn, sid uint32) { c.ok(sid); c.reset(sid, http2.ErrCodeNo) }, true, 0, requests},
		{"response only", func(c *rawH2Conn, sid uint32) { c.ok(sid) }, true, requests, requests},
		{"RST_STREAM(REFUSED_STREAM)", func(c *rawH2Conn, sid uint32) { c.reset(sid, http2.ErrCodeRefusedStream) }, false, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startRawH2With(t, rawH2Opts{settings: uploadWindow}, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
				tc.answer(c, sid)
				return rawKeep // never grant window
			})
			host, port := srv.hostPort()
			cl, err := newH2Client(host, port, "/upload", testH2Cfg("POST", nil, make([]byte, 2000), 1, 4))
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			for i := range requests {
				_, err := cl.DoRequest(ctx, 0)
				var re *HTTP2ResetError
				if tc.success && err != nil || !tc.success && !errors.As(err, &re) {
					t.Fatalf("request %d: %v (connections=%d): the server ended each stream at once, but the connection's writer still waits for window for a body the server will never take",
						i+1, err, srv.accepted.Load())
				}
			}
			deadline := time.Now().Add(2 * time.Second)
			for srv.clientCancel.Load() < tc.minCancels && time.Now().Before(deadline) {
				time.Sleep(2 * time.Millisecond)
			}
			time.Sleep(50 * time.Millisecond) // a reset the client should not send would be flushed by now
			t.Logf("connections=%d client RST_STREAM=%d (CANCEL %d)", srv.accepted.Load(), srv.clientResets.Load(), srv.clientCancel.Load())
			if n := srv.accepted.Load(); n != 1 {
				t.Errorf("connections=%d, want 1: a stream the server ended is not a connection error", n)
			}
			if got := srv.clientCancel.Load(); got < tc.minCancels {
				t.Errorf("the client sent %d RST_STREAM(CANCEL), want %d: a body it stops sending leaves its half of the stream open on the server", got, tc.minCancels)
			}
			if got := srv.clientResets.Load(); got > tc.maxResets {
				t.Errorf("the client sent %d RST_STREAM, want at most %d: never one for a stream the server reset", got, tc.maxResets)
			}
		})
	}
}

// TestH2AwaitAnswersARequestQueuedAfterTheWriterReturned: a worker's hand-off
// to writeCh and the connection's done can be ready together, and select
// picks either, so a request can be queued after writeLoop's final drain
// (answerQueued). Nobody wrote it, so it never reached the server: await,
// once both loops have returned, answers it errH2NotSent (DoRequest takes it
// to the redialed connection) instead of "connection closing", an error. On
// e37777e it was an error.
func TestH2AwaitAnswersARequestQueuedAfterTheWriterReturned(t *testing.T) {
	hc := &h2Conn{done: make(chan struct{}), loopsDone: make(chan struct{}), streamSem: make(chan struct{}, 1), writeCh: make(chan h2WriteReq, 4)}
	close(hc.done)
	close(hc.loopsDone)
	ch := make(chan h2Response, 1)
	hc.writeCh <- h2WriteReq{kind: h2WriteHeaders, block: []byte{0x82}, respCh: &ch} // queued after the writer returned
	n, err := hc.await(context.Background(), &ch, 0)
	<-hc.streamSem // the token await returned
	t.Logf("await returned (%d, %v)", n, err)
	if err != errH2NotSent {
		t.Errorf("a request still queued for a writer that has returned was answered (%d, %v), want errH2NotSent: it was never written", n, err)
	}
	if len(hc.writeCh) != 0 {
		t.Errorf("%d request(s) left in writeCh after await", len(hc.writeCh))
	}
}

// TestH2ResponseWithOnlyAnInterimStatusIsNotASuccess: a stream that ends
// while its only :status is an interim 1xx has no final response, which RFC
// 9113 §8.1 makes malformed: an error (errH2NoStatus), not a success. On
// e37777e both shapes were a success with a 1xx status.
func TestH2ResponseWithOnlyAnInterimStatusIsNotASuccess(t *testing.T) {
	srv := startRawH2(t, func(c *rawH2Conn, sid uint32, path string) rawH2Action {
		switch path {
		case "/interim-ends-stream": // HEADERS 103 with END_STREAM
			c.headers(sid, true, ":status", "103")
		case "/interim-then-data": // HEADERS 100, then the stream ends on DATA
			c.headers(sid, false, ":status", "100")
			c.data(sid, true, len(rawOKBody), 0)
		case "/interim-then-final": // HEADERS 103, then the final 200: a success
			c.headers(sid, false, ":status", "103")
			c.ok(sid)
		default:
			c.ok(sid)
		}
		return rawKeep
	})
	host, port := srv.hostPort()
	for _, tc := range []struct {
		path   string
		status int // 0: a success; -1: an error other than a status
	}{
		{"/interim-ends-stream", -1},
		{"/interim-then-data", -1},
		{"/interim-then-final", 0},
		{"/ok", 0},
	} {
		n, err := h2Once(t, host, port, tc.path)
		t.Logf("%-22s status=%d bytes=%d err=%v", tc.path, h2Status(err), n, err)
		if got := h2Status(err); got != tc.status {
			t.Errorf("%s: recorded status %d (err=%v), want %d (0: success, -1: an error): a response with no final status is not a success",
				tc.path, got, err, tc.status)
		}
	}
}

// TestH2HeadersTooShortForTheirPriorityFieldsEndTheConnection: a HEADERS
// frame flagged PRIORITY whose payload has no room for the 5 bytes of
// priority fields is a frame size error on a frame that carries a field
// block, so a connection error (RFC 9113 §4.2, §6.2), as a Pad Length that
// does not fit is since round 1. On e37777e it was read as an empty block:
// the stream failed with errH2NoStatus and the connection went on. The
// boundary is exact: 4 bytes are too short, while 5 bytes are the priority
// fields and an empty field block, which is only a response without a
// :status (a stream error), and 6 carry :status 200.
func TestH2HeadersTooShortForTheirPriorityFieldsEndTheConnection(t *testing.T) {
	priority := []byte{0, 0, 0, 0, 15} // stream dependency 0, weight 16
	for _, tc := range []struct {
		name      string
		payload   []byte
		connError bool // a connection error; else the connection goes on
		success   bool
	}{
		{"1 byte: :status 200 where the priority fields should be", []byte{0x88}, true, false},
		{"4 bytes", priority[:4], true, false},
		{"5 bytes: the priority fields, an empty field block", priority, false, false},
		{"6 bytes: the priority fields, :status 200", append(append([]byte{}, priority...), 0x88), false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := startRawH2(t, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
				_ = c.fr.WriteRawFrame(http2.FrameHeaders, http2.FlagHeadersEndHeaders|http2.FlagHeadersEndStream|http2.FlagHeadersPriority, sid, tc.payload)
				return rawKeep
			})
			host, port := srv.hostPort()
			cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 1))
			if err != nil {
				t.Fatal(err)
			}
			defer cl.Close()
			hc := cl.conns[0].cur.Load()
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			_, err = cl.DoRequest(ctx, 0)
			t.Logf("%d-byte payload: err=%v connection closed=%t", len(tc.payload), err, hc.closed.Load())
			if tc.success != (err == nil) {
				t.Errorf("err=%v, want a success=%t", err, tc.success)
			}
			if hc.closed.Load() != tc.connError {
				t.Errorf("connection closed=%t after a HEADERS frame flagged PRIORITY with a %d-byte payload (err=%v), want %t: only fewer than 5 bytes are a connection error",
					hc.closed.Load(), len(tc.payload), err, tc.connError)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// Review of the fixes, round 3 (at 147edb4)

// TestH2BodyStopsAtTheServersReset: once the server resets a stream, the
// client sends no more frames on it (RFC 9113 §6.4, §5.1 "closed"), however
// much send window the body still has. The server grants 128 MiB of window,
// resets a 32 MiB upload at its HEADERS and reads nothing for 300 ms, so the
// client's writer can be ahead of the reset by no more than the socket
// buffers. The second request's HEADERS reach the server after every DATA
// frame the client wrote for the first stream: the server counts those at
// that moment. On 147edb4 the writer checked for the reset only once the
// window ran out, so it sent the whole body into a stream that no longer
// existed (a Go server answers each such frame with RST_STREAM(STREAM_CLOSED)).
func TestH2BodyStopsAtTheServersReset(t *testing.T) {
	const body = 32 << 20
	var afterFirst atomic.Int64 // DATA bytes of the first stream the server read before the second stream's HEADERS
	srv := startRawH2With(t, rawH2Opts{
		settings:   []http2.Setting{{ID: http2.SettingInitialWindowSize, Val: 128 << 20}},
		connWindow: 128 << 20,
	}, func(c *rawH2Conn, sid uint32, _ string) rawH2Action {
		if c.requests == 2 {
			afterFirst.Store(c.srv.dataAfterReset.Load())
		}
		c.reset(sid, http2.ErrCodeRefusedStream)
		if c.requests == 1 {
			time.Sleep(300 * time.Millisecond) // let the client read the reset while its writer is still in the body
		}
		return rawKeep
	})
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/upload", testH2Cfg("POST", nil, make([]byte, body), 1, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	for i := range 2 {
		var re *HTTP2ResetError
		if _, err := cl.DoRequest(ctx, 0); !errors.As(err, &re) {
			t.Fatalf("request %d: got %v, want the server's RST_STREAM(REFUSED_STREAM)", i+1, err)
		}
	}
	sent := afterFirst.Load()
	t.Logf("DATA bytes the client sent on the first stream after the server reset it: %d of the %d-byte body; client RST_STREAM=%d connections=%d",
		sent, body, srv.clientResets.Load(), srv.accepted.Load())
	if sent >= body/2 {
		t.Errorf("the client sent %d of the body's %d bytes on a stream the server had reset: after RST_STREAM it must send no more frames on it (RFC 9113 §6.4)", sent, body)
	}
	if n := srv.clientResets.Load(); n != 0 {
		t.Errorf("the client sent %d RST_STREAM: never one for a stream the server reset (RFC 9113 §5.4.2)", n)
	}
	if n := srv.accepted.Load(); n != 1 {
		t.Errorf("connections=%d, want 1: a stream the server reset is not a connection error", n)
	}
}

// TestH2ConnectionOutOfStreamIDsIsFailedWhenItDies: a connection whose stream
// IDs have run out takes no new request (closed), but its last streams are
// still in flight. If the server then closes it, or a write to it fails, the
// connection has died: its streams must fail and done must close, as on any
// other connection. No request comes to redial it here (the requests are
// handed to the writer directly), so nothing else closes it. On 147edb4
// readLoop and writeFailed took closed for the client's own close and
// returned without failing it: the stream in flight waited for the rest of
// the run.
func TestH2ConnectionOutOfStreamIDsIsFailedWhenItDies(t *testing.T) {
	for _, tc := range []struct {
		name       string
		failWrites bool // a write fails; else the server closes the connection
	}{
		{"the server closes it", false},
		{"a write to it fails", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.failWrites {
				dial := dialTimeoutFunc
				t.Cleanup(func() { dialTimeoutFunc = dial })
				dialTimeoutFunc = func(network, addr string, timeout time.Duration) (net.Conn, error) {
					c, err := dial(network, addr, timeout)
					if err != nil {
						return nil, err
					}
					return &failWritesConn{Conn: c, okWrites: 4}, nil // the handshake's 3 writes and the last stream's HEADERS
				}
			}
			held := make(chan *rawH2Conn, 1)
			srv := startRawH2(t, func(c *rawH2Conn, _ uint32, _ string) rawH2Action {
				held <- c // never answer
				return rawKeep
			})
			host, port := srv.hostPort()
			cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 4))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(cl.Close)
			hc := cl.conns[0].cur.Load()
			hc.nextStreamID.Store(0x7FFFFFFF) // the last stream ID a connection may use
			last := make(chan h2Response, 1)
			hc.writeCh <- h2WriteReq{kind: h2WriteHeaders, block: cl.headerBlock, respCh: &last}
			var sc *rawH2Conn
			select {
			case sc = <-held:
			case <-time.After(3 * time.Second):
				t.Fatal("the stream with the last ID never reached the server")
			}
			next := make(chan h2Response, 1)
			hc.writeCh <- h2WriteReq{kind: h2WriteHeaders, block: cl.headerBlock, respCh: &next}
			select {
			case r := <-next:
				if r.err != errH2NotSent {
					t.Fatalf("the request after the last stream ID was answered %+v, want errH2NotSent", r)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the request after the last stream ID was never answered")
			}
			if !hc.closed.Load() {
				t.Fatal("a connection out of stream IDs still takes new requests")
			}
			if tc.failWrites {
				_ = sc.fr.WritePing(false, [8]byte{1}) // the client's PING ACK is its next write, which fails
			} else {
				_ = sc.nc.Close()
			}
			select {
			case r := <-last:
				t.Logf("the stream in flight was answered: %v", r.err)
				if r.err == nil {
					t.Errorf("the stream in flight on a connection that died was a success: %+v", r)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("the stream in flight was never answered after its connection died: a connection out of stream IDs was taken for one the client closed, and nothing failed it")
			}
			select {
			case <-hc.done:
			case <-time.After(time.Second):
				t.Error("done never closed on a connection that died")
			}
		})
	}
}

// heldReadConn holds each read, once armed, after the bytes are read from
// the socket and before the reader gets them, until release is closed: the
// reader then parses a response that had arrived before the connection
// ended.
type heldReadConn struct {
	net.Conn
	armed   atomic.Bool
	holding chan struct{} // closed when an armed read holds bytes
	once    sync.Once
	release chan struct{}
}

func (c *heldReadConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 && c.armed.Load() {
		c.once.Do(func() { close(c.holding) })
		<-c.release
	}
	return n, err
}

// TestH2RedialTakesTheResponseTheConnectionHadReceived: the writer ends a
// connection (a failed write: failConn) just after the server's first
// response reached the client, and before readLoop has parsed it. That
// connection has served, so the request that finds it gone redials at once.
// reconnectSlot must read served only once readLoop has returned, having
// parsed what it had read. On 147edb4 it read served at once: the connection
// looked as if it had never served, so the request failed (errH2NeverServed)
// and slept the backoff, and nothing was dialed.
func TestH2RedialTakesTheResponseTheConnectionHadReceived(t *testing.T) {
	dial := dialTimeoutFunc
	t.Cleanup(func() { dialTimeoutFunc = dial })
	gate := &heldReadConn{holding: make(chan struct{}), release: make(chan struct{})}
	var dialed atomic.Int64
	dialTimeoutFunc = func(network, addr string, timeout time.Duration) (net.Conn, error) {
		c, err := dial(network, addr, timeout)
		if err != nil || dialed.Add(1) > 1 {
			return c, err
		}
		gate.Conn = c
		return gate, nil
	}
	srv := startRawH2(t, respondOK)
	host, port := srv.hostPort()
	cl, err := newH2Client(host, port, "/", testH2Cfg("GET", nil, nil, 1, 4))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	hc := cl.conns[0].cur.Load()
	gate.armed.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	first := make(chan error, 1)
	go func() {
		_, err := cl.DoRequest(ctx, 0)
		first <- err
	}()
	select {
	case <-gate.holding:
	case <-time.After(3 * time.Second):
		close(gate.release)
		t.Fatal("the server's first response never reached the client")
	}
	hc.failConn(errors.New("test: a write to the connection failed")) // as writeFailed does
	t.Logf("the request whose response was still unparsed: %v", <-first)
	go func() {
		time.Sleep(100 * time.Millisecond)
		close(gate.release)
	}()
	dials := countDials(cl, 0)
	start := time.Now()
	_, err = cl.DoRequest(ctx, 0)
	t.Logf("the request that found the connection gone: %v, err=%v, dials=%d, served=%t", time.Since(start), err, dials.Load(), hc.served.Load())
	if err != nil || dials.Load() != 1 {
		t.Errorf("err=%v dials=%d, want a success after 1 dial: the connection had received the answer to a request, so it served, and the request that finds it gone redials at once", err, dials.Load())
	}
}

// TestH2AwaitAnswersEachQueuedRequestOnce: requests still queued for a
// connection whose writer has returned are answered by whichever await runs
// answerQueued first; several awaits on one dead connection drain the queue
// together. An await that finds the queue empty and its own channel empty
// must not decide that nobody answered it while another await has taken its
// request off the queue and not yet answered it: that request never reached
// the server, so it is errH2NotSent (retried), not "connection closing" (an
// error). 8 awaits per dead connection, 20000 connections. On
// 147edb4 a probe of this shape counted 913 of 160,000 such requests
// answered as errors under -race, and 99-104 without.
func TestH2AwaitAnswersEachQueuedRequestOnce(t *testing.T) {
	const trials, waiters = 20000, 8
	var miscounted, total int64
	for range trials {
		hc := &h2Conn{done: make(chan struct{}), loopsDone: make(chan struct{}), streamSem: make(chan struct{}, waiters),
			writeCh: make(chan h2WriteReq, waiters)}
		close(hc.done)
		close(hc.loopsDone)
		chans := make([]chan h2Response, waiters)
		for i := range chans {
			chans[i] = make(chan h2Response, 1)
			hc.writeCh <- h2WriteReq{kind: h2WriteHeaders, block: []byte{0x82}, respCh: &chans[i]}
		}
		var wg sync.WaitGroup
		var bad atomic.Int64
		start := make(chan struct{})
		for i := range chans {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if _, err := hc.await(context.Background(), &chans[i], 0); err != errH2NotSent {
					bad.Add(1)
				}
			}()
		}
		close(start)
		wg.Wait()
		miscounted += bad.Load()
		total += waiters
	}
	t.Logf("requests never written, answered as errors: %d of %d", miscounted, total)
	if miscounted != 0 {
		t.Errorf("%d of %d requests that were never written were answered \"connection closing\": an await gave up while another await held its request, taken off the queue and not yet answered", miscounted, total)
	}
}
