package loadgen

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/base64"
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
		defer conn.Close()

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
	var header []byte
	switch {
	case n <= 125:
		header = []byte{b0, byte(n)}
	case n <= 0xffff:
		header = []byte{b0, 126, byte(n >> 8), byte(n)}
	default:
		header = []byte{b0, 127,
			byte(n >> 56), byte(n >> 48), byte(n >> 40), byte(n >> 32),
			byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)}
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
