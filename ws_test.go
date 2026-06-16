package loadgen

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

const wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// wsTestServer is an httptest server performing a real RFC 6455 upgrade, then
// serving the requested ?mode= (echo/large-echo/hub) so wsClient is exercised
// against a genuine server-side accept.
func wsTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Sec-WebSocket-Key")
		if key == "" || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "not a websocket request", http.StatusBadRequest)
			return
		}

		hj, ok := w.(http.Hijacker)
		if !ok {
			http.Error(w, "no hijack", http.StatusInternalServerError)
			return
		}
		conn, brw, err := hj.Hijack()
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()

		sum := sha1.Sum([]byte(key + wsGUID))
		accept := base64.StdEncoding.EncodeToString(sum[:])
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
		if _, err := brw.WriteString(resp); err != nil {
			return
		}
		if err := brw.Flush(); err != nil {
			return
		}

		switch r.URL.Query().Get("mode") {
		case wsModeHub:
			wsServeHub(conn)
		default: // echo + large-echo: echo verbatim
			wsServeEcho(conn, brw.Reader)
		}
	}))
}

func wsServeEcho(conn net.Conn, br *bufio.Reader) {
	for {
		op, data, err := wsReadFrame(br)
		if err != nil {
			return
		}
		switch op {
		case wsOpClose:
			return
		case wsOpPing:
			if _, err := conn.Write(wsServerFrameTest(wsOpPong, data)); err != nil {
				return
			}
		default:
			if _, err := conn.Write(wsServerFrameTest(op, data)); err != nil {
				return
			}
		}
	}
}

func wsServeHub(conn net.Conn) {
	t := time.NewTicker(time.Millisecond)
	defer t.Stop()
	for range t.C {
		if _, err := conn.Write(wsServerFrameTest(wsOpText, []byte("payload"))); err != nil {
			return
		}
	}
}

// wsServerFrameTest builds an unmasked server frame with full length encoding.
func wsServerFrameTest(opcode byte, payload []byte) []byte {
	n := len(payload)
	b0 := byte(0x80) | (opcode & 0x0f)
	header := make([]byte, 0, 10+n)
	switch {
	case n <= 125:
		header = append(header, b0, byte(n))
	case n <= 0xffff:
		header = append(header, b0, 126, byte(n>>8), byte(n))
	default:
		header = append(header, b0, 127,
			byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
			byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	return append(header, payload...)
}

// wsDialClient builds a wsClient directly against an httptest server.
func wsDialClient(t *testing.T, srv *httptest.Server, mode string) Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{URL: srv.URL + "/ws", Mode: mode, scheme: u.Scheme}
	c, err := newWSClient(u.Hostname(), u.Port(), "/ws", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestWSEchoRoundTrip(t *testing.T) {
	srv := wsTestServer(t)
	defer srv.Close()

	c := wsDialClient(t, srv, wsModeEcho)
	defer c.Close()

	for i := 0; i < 3; i++ {
		n, err := c.DoRequest(context.Background(), 0)
		if err != nil {
			t.Fatalf("DoRequest %d: %v", i, err)
		}
		if n != len("payload") {
			t.Errorf("echo bytes = %d, want %d", n, len("payload"))
		}
	}
}

func TestWSLargeEcho(t *testing.T) {
	srv := wsTestServer(t)
	defer srv.Close()

	c := wsDialClient(t, srv, wsModeLargeEcho)
	defer c.Close()

	n, err := c.DoRequest(context.Background(), 0)
	if err != nil {
		t.Fatalf("DoRequest: %v", err)
	}
	if n != wsLargePayloadSize {
		t.Errorf("large echo bytes = %d, want %d", n, wsLargePayloadSize)
	}
}

func TestWSHubReceives(t *testing.T) {
	srv := wsTestServer(t)
	defer srv.Close()

	c := wsDialClient(t, srv, wsModeHub)
	defer c.Close()

	n, err := c.DoRequest(context.Background(), 0)
	if err != nil {
		t.Fatalf("DoRequest: %v", err)
	}
	if n != len("payload") {
		t.Errorf("hub bytes = %d, want %d", n, len("payload"))
	}
}

func TestWSRunIntegration(t *testing.T) {
	srv := wsTestServer(t)
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL + "/ws",
		Mode:        wsModeEcho,
		Connections: 4,
		Workers:     4,
		Duration:    300 * time.Millisecond,
		Warmup:      0,
		CPUMonitor:  false,
		RecvQProbe:  false,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if result.Requests == 0 {
		t.Error("expected nonzero requests")
	}
	if result.ThroughputBPS == 0 {
		t.Error("expected nonzero throughput")
	}
}

func TestWSValidateRejectsCombos(t *testing.T) {
	base := Config{
		URL:         "http://x/ws",
		Method:      "GET",
		Connections: 1,
		Workers:     1,
		Duration:    time.Second,
		Mode:        wsModeEcho,
	}

	combos := []Config{
		withMod(base, func(c *Config) { c.HTTP2 = true }),
		withMod(base, func(c *Config) { c.Mix = &MixRatio{H1: 1} }),
		withMod(base, func(c *Config) { c.H2CUpgrade = true }),
	}
	for i, c := range combos {
		if err := c.Validate(); err == nil {
			t.Errorf("combo %d: expected validation error", i)
		}
	}

	bad := withMod(base, func(c *Config) { c.Mode = "ws-bogus" })
	if err := bad.Validate(); err == nil {
		t.Error("expected error for unknown mode")
	}

	if err := base.Validate(); err != nil {
		t.Errorf("valid ws-echo config rejected: %v", err)
	}
}

func withMod(c Config, f func(*Config)) Config {
	f(&c)
	return c
}

// refusedAddr returns host, port for an address that actively refuses
// connections (bound then immediately released).
func refusedAddr(t *testing.T) (string, string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	return host, port
}

// TestWSFailFastAbortsRun drives a full Benchmarker run against a refusing
// port: with no stream ever established, the run must abort with an
// ErrNeverConnected-wrapped dial error well before Duration elapses, and
// dial attempts must be backoff-paced rather than hot-looped.
func TestWSFailFastAbortsRun(t *testing.T) {
	host, port := refusedAddr(t)

	cfg := Config{
		URL:    "http://" + net.JoinHostPort(host, port) + "/ws",
		Mode:   wsModeEcho,
		scheme: "http",
	}
	raw, err := newWSClient(host, port, "/ws", cfg)
	if err != nil {
		t.Fatal(err)
	}
	// Shrink the fail-fast window so the test doesn't sit through the
	// production 5s default.
	raw.(*wsClient).failFast = newFailFastTracker(300 * time.Millisecond)

	before := connectErrorsCounter.Swap(0)
	defer connectErrorsCounter.Add(before)

	b, err := New(Config{
		URL:         cfg.URL,
		Duration:    30 * time.Second, // must NOT be waited out
		Connections: 2,
		Workers:     2,
		Warmup:      0,
		Client:      raw,
		CPUMonitor:  false,
		RecvQProbe:  false,
	})
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	result, err := b.Run(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected fatal error from never-connected run")
	}
	if !errors.Is(err, ErrNeverConnected) {
		t.Errorf("error must wrap ErrNeverConnected: %v", err)
	}
	if !strings.Contains(err.Error(), "loadgen: dial: ") ||
		!strings.Contains(err.Error(), "connection refused") {
		t.Errorf("fatal error shape mismatch (want h1client pre-dial shape): %q", err.Error())
	}
	if result != nil {
		t.Errorf("expected nil result on fatal abort, got %+v", result)
	}
	if elapsed > 10*time.Second {
		t.Errorf("abort took %v — run waited out the duration", elapsed)
	}
	// Hot-loop regression guard: v3.8 burned ~386k dials/sec. With 2
	// workers, a ~300ms window, and 10ms-doubling backoff, the attempt
	// count must stay in the tens.
	if n := b.errors.Load(); n == 0 || n > 200 {
		t.Errorf("dial attempts = %d, want a small nonzero count (backoff-paced)", n)
	}
}

// TestWSReconnectBackoffAfterServerDeath: a client that HAD a live stream
// must keep retrying with backoff after the server dies — never fail fast,
// never hot-loop.
func TestWSReconnectBackoffAfterServerDeath(t *testing.T) {
	srv := wsTestServer(t)

	c := wsDialClient(t, srv, wsModeEcho)
	defer c.Close()

	if _, err := c.DoRequest(context.Background(), 0); err != nil {
		t.Fatalf("priming request: %v", err)
	}

	// Server dies mid-cell. srv.Close() releases the port but not the
	// hijacked WS conn, so drop the client side too to force redials.
	srv.Close()
	c.(*wsClient).drop(0)

	deadline := time.Now().Add(700 * time.Millisecond)
	errCount := 0
	for time.Now().Before(deadline) {
		_, err := c.DoRequest(context.Background(), 0)
		if err == nil {
			continue
		}
		if errors.Is(err, ErrNeverConnected) {
			t.Fatalf("had a live stream — must never fail fast: %v", err)
		}
		errCount++
	}
	if errCount == 0 {
		t.Fatal("expected reconnect errors after server death")
	}
	// 10ms-doubling backoff allows roughly 7 attempts in 700ms (plus the
	// initial dead-conn read error); a hot loop would produce thousands.
	if errCount > 100 {
		t.Errorf("errCount = %d in 700ms — reconnects are not backoff-paced", errCount)
	}
	t.Logf("reconnect errors in 700ms: %d", errCount)
}

// TestWSCloseAbortsDial parks a dial inside the upgrade handshake (the
// server accepts but never responds) and verifies Close() aborts it
// promptly even though neither ctx deadline nor DialTimeout is near.
func TestWSCloseAbortsDial(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = ln.Close() }()
	var held []net.Conn
	defer func() {
		for _, conn := range held {
			_ = conn.Close()
		}
	}()
	acceptDone := make(chan struct{})
	go func() {
		defer close(acceptDone)
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, conn) // hold open, never answer the upgrade
		}
	}()

	host, port, err := net.SplitHostPort(ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		URL:         "http://" + ln.Addr().String() + "/ws",
		Mode:        wsModeEcho,
		scheme:      "http",
		DialTimeout: 30 * time.Second, // Close, not the timeout, must abort
	}
	c, err := newWSClient(host, port, "/ws", cfg)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, derr := c.DoRequest(context.Background(), 0)
		done <- derr
	}()

	time.Sleep(100 * time.Millisecond) // let the dial park in the handshake read
	start := time.Now()
	c.Close()

	select {
	case derr := <-done:
		if derr == nil {
			t.Error("expected an error from the aborted dial")
		}
		if elapsed := time.Since(start); elapsed > 2*time.Second {
			t.Errorf("Close took %v to abort the in-flight dial", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close() did not abort the in-flight dial")
	}
	_ = ln.Close()
	<-acceptDone
}

// TestWSConnectErrorsAccounting runs a short, non-fatal window against a
// refusing port and checks the new error-class plumbing end to end:
// Result.ConnectErrors, Result.Warmup, and bounded (non-hot-loop) totals.
func TestWSConnectErrorsAccounting(t *testing.T) {
	host, port := refusedAddr(t)

	before := connectErrorsCounter.Swap(0)
	defer connectErrorsCounter.Add(before)

	cfg := Config{
		URL:         "http://" + net.JoinHostPort(host, port) + "/ws",
		Mode:        wsModeEcho,
		Duration:    900 * time.Millisecond, // well under the 5s fail-fast window
		Connections: 2,
		Workers:     2,
		Warmup:      400 * time.Millisecond,
		CPUMonitor:  false,
		RecvQProbe:  false,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("run should not trip fail-fast inside the 5s window: %v", err)
	}

	if result.Errors == 0 {
		t.Error("expected errors against a refusing port")
	}
	if result.ConnectErrors == 0 {
		t.Error("expected ConnectErrors > 0 (dial failures must land in the connect class)")
	}
	if result.Warmup == nil {
		t.Fatal("expected warmup stats")
	}
	if result.Warmup.Requests != 0 {
		t.Errorf("warmup requests = %d, want 0 against a dead port", result.Warmup.Requests)
	}
	if result.Warmup.Errors == 0 || result.Warmup.ConnectErrors == 0 {
		t.Errorf("100%%-failing warmup must be visible: %+v", result.Warmup)
	}
	// Hot-loop regression guard (v3.8: ~386k dials/sec).
	if result.Errors > 500 {
		t.Errorf("errors = %d in ~1.3s — dials are not backoff-paced", result.Errors)
	}
	t.Logf("errors=%d connect=%d warmup=%+v", result.Errors, result.ConnectErrors, result.Warmup)
}
