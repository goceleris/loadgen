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

	accepted     atomic.Int64 // connections accepted
	killed       atomic.Int64 // connections the server ended (rawClose or killAfter)
	clientClosed atomic.Int64 // connections the client ended first
	dataSent     atomic.Int64 // DATA frame payload bytes written, padding included
	windowCredit atomic.Int64 // connection WINDOW_UPDATE increments received, minus each handshake's own
	answeredLate atomic.Int64 // requests answered on a connection other than the first one

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
}

// rawH2Opts are the rawH2Server options of the same names.
type rawH2Opts struct {
	killAfter      time.Duration
	closeAfterData int
	settings       []http2.Setting
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
	s := &rawH2Server{ln: ln, handler: h, killAfter: opts.killAfter, closeAfterData: opts.closeAfterData, settings: opts.settings}
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
	if !rawH2Handshake(nc, br, fr, s.settings...) {
		_ = nc.Close()
		return
	}
	c := &rawH2Conn{srv: s, nc: nc, fr: fr, index: index}
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
// sends the server's SETTINGS.
func rawH2Handshake(nc net.Conn, br *bufio.Reader, fr *http2.Framer, settings ...http2.Setting) bool {
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
// the response and the closed connection ready at once. The response is the
// request's outcome; select alone would pick at random. (Found in review of
// the #89 fix, which makes a close right after a response common.)
func TestH2ResponseWinsOverConnectionClose(t *testing.T) {
	hc := &h2Conn{done: make(chan struct{}), streamSem: make(chan struct{}, 1)}
	close(hc.done)
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

// TestH2RedialBacksOffOnlyWhenTheServerLooksDown: h1client redials a
// connection the server ended at once, and backs off only when the redial
// fails (h1client.go DoRequest). HTTP/2 must record a redial the same way. A
// connection that answered a request and then ended is redialed without the
// backoff sleep, which would otherwise be charged to the latency of the
// request that redials; a connection that ended before it answered anything
// is paced, so a server that accepts and drops every connection is not
// redialed in a loop. The backoff is set far beyond each context, so a sleep
// shows as a failed redial.
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

	t.Run("answered", func(t *testing.T) {
		cl := newClient(t)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := cl.DoRequest(ctx, 0); err != nil {
			t.Fatal(err)
		}
		slot := cl.conns[0]
		slot.cur.Load().failConn(errors.New("test: the server ended the connection"))
		slot.backoff.next = longBackoff
		start := time.Now()
		hc := cl.reconnectSlot(ctx, slot)
		t.Logf("redial after a connection that answered: %v, connection=%t", time.Since(start), hc != nil)
		if hc == nil {
			t.Fatalf("no connection after %v: the redial slept the backoff although the connection it replaces had answered; h1client redials such a connection at once",
				time.Since(start))
		}
	})
	t.Run("never answered", func(t *testing.T) {
		cl := newClient(t)
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		slot := cl.conns[0]
		slot.cur.Load().failConn(errors.New("test: the server dropped the connection before answering"))
		slot.backoff.next = longBackoff
		start := time.Now()
		hc := cl.reconnectSlot(ctx, slot)
		t.Logf("redial after a connection that never answered: %v, connection=%t", time.Since(start), hc != nil)
		if hc != nil {
			t.Fatal("a connection that never answered was redialed without the backoff: a server that drops every connection would be redialed in a loop")
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
		slot := cl.conns[0]
		slot.cur.Load().failConn(errors.New("test: the server ended the connection"))
		cancel()
		if hc := cl.reconnectSlot(ctx, slot); hc != nil {
			t.Fatal("a connection was dialed after the run's context ended: Close, which runs after the cancel, never closes it")
		}
	})
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
// connection with 4 streams and POST 100 KiB bodies; the server reads the
// 65,535 bytes the connection window allows and closes the connection. The
// first request's body was being written, so it is in flight and is one
// error. The other three were still queued for the writer: they never
// reached the server, so, like a request still waiting for a stream, they go
// to the redialed connection and are not errors. Before the fix their workers
// woke on the closed connection and gave up before the writer answered, and
// the writer failed the ones it reached: 4 errors per connection.
func TestH2RequestsTheConnectionNeverWroteAreRetried(t *testing.T) {
	srv := startRawH2With(t, rawH2Opts{closeAfterData: 65535}, func(*rawH2Conn, uint32, string) rawH2Action {
		return rawKeep // take the request and its body, never answer
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
