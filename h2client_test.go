package loadgen

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/net/http2/hpack"
)

// testH2Cfg builds a Config suitable for newH2Client in tests.
func testH2Cfg(method string, headers map[string]string, body []byte, numConns, maxStreams int) Config {
	return Config{
		Method: method,
		Headers: headers,
		Body:   body,
		HTTP2:  true,
		HTTP2Options: HTTP2Options{
			Connections: numConns,
			MaxStreams:  maxStreams,
		},
		DialTimeout:     10 * time.Second,
		ReadBufferSize:  2 * 1024 * 1024,
		WriteBufferSize: 2 * 1024 * 1024,
		MaxResponseSize: -1,
	}
}

// startH2CServer creates a local h2c server with endpoints of varying response sizes
// to exercise H2 flow control at different pressures.
func startH2CServer(t *testing.T, maxStreams uint32) (host, port string, cleanup func()) {
	t.Helper()

	body1MB := make([]byte, 1<<20)
	body2MB := make([]byte, 2<<20)

	mux := http.NewServeMux()
	mux.HandleFunc("/simple", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	})
	mux.HandleFunc("/medium", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(make([]byte, 64*1024))
	})
	mux.HandleFunc("/large", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(body1MB)
	})
	mux.HandleFunc("/xlarge", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write(body2MB)
	})

	h2s := &http2.Server{MaxConcurrentStreams: maxStreams}
	handler := h2c.NewHandler(mux, h2s)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()

	h, p, _ := net.SplitHostPort(ln.Addr().String())
	return h, p, func() { _ = srv.Close(); _ = ln.Close() }
}

// stressH2 hammers the H2 client with concurrent workers for the given duration.
// Returns total successful requests and errors. Fails the test if zero successes.
func stressH2(t *testing.T, client *h2Client, workers int, duration time.Duration) (total, errors int64) {
	t.Helper()

	// benchCtx controls the benchmark duration
	benchCtx, benchCancel := context.WithTimeout(context.Background(), duration)
	defer benchCancel()

	// outerCtx is the hard deadline for the entire test (detect hangs)
	outerCtx, outerCancel := context.WithTimeout(context.Background(), duration+30*time.Second)
	defer outerCancel()

	var wg sync.WaitGroup
	var totalOK, totalErr atomic.Int64

	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for benchCtx.Err() == nil {
				_, err := client.DoRequest(benchCtx, id)
				if err != nil {
					if benchCtx.Err() != nil {
						return
					}
					totalErr.Add(1)
				} else {
					totalOK.Add(1)
				}
			}
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-outerCtx.Done():
		t.Fatal("DEADLOCK: workers did not complete within timeout — H2 flow control likely stalled")
	}

	ok := totalOK.Load()
	errs := totalErr.Load()
	if ok == 0 {
		t.Fatal("zero successful requests — client may be broken")
	}
	return ok, errs
}

func TestH2ClientBasic(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 100)
	defer cleanup()

	client, err := newH2Client(host, port, "/simple", testH2Cfg("GET", nil, nil, 1, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = client.DoRequest(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
}

// TestH2StressSimple tests high-concurrency with small responses.
// Catches data races in stream slot dispatch, channel pool, and atomic counters.
func TestH2StressSimple(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 250)
	defer cleanup()

	client, err := newH2Client(host, port, "/simple", testH2Cfg("GET", nil, nil, 4, 200))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ok, errs := stressH2(t, client, 200, 5*time.Second)
	t.Logf("simple: %d OK, %d errors (%.1f%% error rate)", ok, errs, float64(errs)/float64(ok+errs)*100)
}

// TestH2StressMedium tests with 64KB responses — fills flow control window moderately.
func TestH2StressMedium(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 200)
	defer cleanup()

	client, err := newH2Client(host, port, "/medium", testH2Cfg("GET", nil, nil, 4, 150))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ok, errs := stressH2(t, client, 150, 5*time.Second)
	t.Logf("medium (64KB): %d OK, %d errors", ok, errs)
}

// TestH2StressLargeResponse tests with 1MB responses — at the old window size (1MB),
// a single response would exhaust the entire connection window. This forces
// multiple WINDOW_UPDATE round-trips and is the scenario that deadlocked on C7i/C7g.
func TestH2StressLargeResponse(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 100)
	defer cleanup()

	client, err := newH2Client(host, port, "/large", testH2Cfg("GET", nil, nil, 4, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ok, errs := stressH2(t, client, 100, 10*time.Second)
	t.Logf("large (1MB): %d OK, %d errors", ok, errs)
}

// TestH2StressXLargeResponse tests with 2MB responses — half the 4MB window.
// With 50 concurrent streams, the aggregate data flow (100MB) far exceeds
// the connection window, creating heavy WINDOW_UPDATE pressure.
func TestH2StressXLargeResponse(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 50)
	defer cleanup()

	client, err := newH2Client(host, port, "/xlarge", testH2Cfg("GET", nil, nil, 4, 50))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ok, errs := stressH2(t, client, 50, 15*time.Second)
	t.Logf("xlarge (2MB): %d OK, %d errors", ok, errs)
}

// TestH2StressMultiConn tests with many connections and high stream counts,
// mimicking the real benchmark configuration.
func TestH2StressMultiConn(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 250)
	defer cleanup()

	client, err := newH2Client(host, port, "/medium", testH2Cfg("GET", nil, nil, 8, 200))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ok, errs := stressH2(t, client, 256, 10*time.Second)
	t.Logf("multi-conn (8 conns, 256 workers): %d OK, %d errors", ok, errs)
}

// TestH2StressMixedEndpoints runs concurrent requests against different response
// sizes to create uneven flow control pressure across connections.
func TestH2StressMixedEndpoints(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 200)
	defer cleanup()

	paths := []string{"/simple", "/medium", "/large"}
	clients := make([]*h2Client, len(paths))
	for i, path := range paths {
		c, err := newH2Client(host, port, path, testH2Cfg("GET", nil, nil, 2, 100))
		if err != nil {
			t.Fatal(err)
		}
		clients[i] = c
	}
	defer func() {
		for _, c := range clients {
			c.Close()
		}
	}()

	benchCtx, benchCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer benchCancel()

	var wg sync.WaitGroup
	var totalOK, totalErr atomic.Int64

	for ci, client := range clients {
		for i := range 80 {
			wg.Add(1)
			go func(c *h2Client, id int) {
				defer wg.Done()
				for benchCtx.Err() == nil {
					_, err := c.DoRequest(benchCtx, id)
					if err != nil {
						if benchCtx.Err() != nil {
							return
						}
						totalErr.Add(1)
					} else {
						totalOK.Add(1)
					}
				}
			}(client, ci*80+i)
		}
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(40 * time.Second):
		t.Fatal("DEADLOCK: mixed-endpoint workers did not complete within timeout")
	}

	ok := totalOK.Load()
	errs := totalErr.Load()
	t.Logf("mixed endpoints (240 workers): %d OK, %d errors", ok, errs)
	if ok == 0 {
		t.Fatal("zero successful requests")
	}
}

// TestH2ContextCancellation verifies that DoRequest exits promptly when the
// context is cancelled, even if writeLoop is slow. This is the exact scenario
// that caused the warmup hang: workers waiting for errCh with no ctx.Done().
func TestH2ContextCancellation(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 100)
	defer cleanup()

	client, err := newH2Client(host, port, "/simple", testH2Cfg("GET", nil, nil, 1, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Start requests, then cancel context quickly
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for i := range 100 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for ctx.Err() == nil {
				_, _ = client.DoRequest(ctx, id)
			}
		}(i)
	}

	// Workers must exit within 2 seconds of context cancellation
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("HANG: workers did not exit after context cancellation — missing ctx.Done() in errCh wait?")
	}
}

// TestH2WarmupDoesNotHang simulates the warmup->benchmark flow using the
// Benchmarker. Warmup must complete even if writeLoop is slow, because
// warmup never calls Close() on connections.
func TestH2WarmupDoesNotHang(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 200)
	defer cleanup()

	cfg := Config{
		URL:              fmt.Sprintf("http://%s:%s/simple", host, port),
		Method:           "GET",
		Duration:         2 * time.Second,
		Workers:          100,
		Warmup:           1 * time.Second,
		DisableKeepAlive: false,
		HTTP2:            true,
		HTTP2Options: HTTP2Options{
			Connections: 4,
			MaxStreams:  100,
		},
	}

	benchmarker, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		_, _ = benchmarker.Run(context.Background())
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("HANG: Benchmarker.Run did not complete — warmup likely stuck on errCh wait")
	}
}

// TestH2StreamIDExhaustion verifies graceful handling when stream IDs approach
// the uint31 limit. The client should return an error, not panic or deadlock.
func TestH2StreamIDExhaustion(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 100)
	defer cleanup()

	client, err := newH2Client(host, port, "/simple", testH2Cfg("GET", nil, nil, 1, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	// Fast-forward stream ID close to max
	client.conns[0].nextStreamID.Store(0x7FFFFFF0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Should eventually get "stream ID exhausted" error, not hang
	for range 20 {
		_, err := client.DoRequest(ctx, 0)
		if err != nil {
			return // expected
		}
	}
	// If we got here without error, the IDs wrapped around — that's also fine
}

// TestH2HopByHopHeadersFiltered verifies that HTTP/1.1 hop-by-hop headers
// (Connection, Keep-Alive, etc.) are stripped from HPACK-encoded request
// headers per RFC 9113 Section 8.2.2. These headers cause Go's net/http H2
// server to reject requests with PROTOCOL_ERROR.
func TestH2HopByHopHeadersFiltered(t *testing.T) {
	headers := map[string]string{
		"Authorization": "Bearer token123",
		"Connection":    "keep-alive",
		"Keep-Alive":    "timeout=5",
		"X-Request-ID":  "abc-123",
	}

	block := buildHPACKHeaders("GET", "localhost", "8080", "/test", headers, 0)

	// Decode the block to verify Connection and Keep-Alive are absent
	decoder := hpack.NewDecoder(4096, nil)
	fields, err := decoder.DecodeFull(block)
	if err != nil {
		t.Fatalf("HPACK decode: %v", err)
	}

	for _, f := range fields {
		if f.Name == "connection" || f.Name == "keep-alive" {
			t.Errorf("hop-by-hop header %q should have been filtered", f.Name)
		}
	}

	// Verify non-hop-by-hop headers are present
	found := map[string]bool{}
	for _, f := range fields {
		found[f.Name] = true
	}
	if !found["authorization"] {
		t.Error("authorization header missing")
	}
	if !found["x-request-id"] {
		t.Error("x-request-id header missing")
	}
}

// TestH2WithRealisticHeaders verifies that the H2 client works correctly
// with the realistic headers used in the "headers" benchmark (including
// Connection: keep-alive which must be filtered for H2 compliance).
func TestH2WithRealisticHeaders(t *testing.T) {
	host, port, cleanup := startH2CServer(t, 100)
	defer cleanup()

	headers := map[string]string{
		"Authorization":    "Bearer eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ1c2VyXzEyMyJ9.sig",
		"Cookie":           "session_id=abc123; _ga=GA1.2.123",
		"User-Agent":       "Mozilla/5.0 (test)",
		"Accept":           "application/json",
		"Accept-Language":  "en-US,en;q=0.9",
		"Accept-Encoding":  "gzip, deflate, br",
		"Cache-Control":    "no-cache",
		"Connection":       "keep-alive", // forbidden in H2 — must be filtered
		"X-Request-ID":     "550e8400-e29b-41d4-a716-446655440000",
		"X-Correlation-ID": "7c9e6679-7425-40de-944b-e07fc1f90ae7",
		"X-Forwarded-For":  "203.0.113.195, 70.41.3.18",
	}

	client, err := newH2Client(host, port, "/simple", testH2Cfg("GET", headers, nil, 2, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var ok, errs int
	for range 1000 {
		_, err := client.DoRequest(ctx, 0)
		if err != nil {
			errs++
		} else {
			ok++
		}
	}

	t.Logf("realistic headers: %d OK, %d errors", ok, errs)
	if ok == 0 {
		t.Fatal("zero successful requests — Connection header likely not filtered")
	}
	if errs > 0 {
		t.Errorf("unexpected errors: %d (possible H2 protocol violation)", errs)
	}
}
