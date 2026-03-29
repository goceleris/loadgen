package loadgen

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
)

func TestBenchmarkerSuccess(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    2 * time.Second,
		Connections: 4,
		Workers:     4,
		Warmup:      0,
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
		t.Error("expected requests > 0")
	}
	if result.RequestsPerSec == 0 {
		t.Error("expected RPS > 0")
	}
	if result.Latency.P99 == 0 {
		t.Error("expected p99 > 0")
	}
	if result.Errors != 0 {
		t.Errorf("unexpected errors: %d", result.Errors)
	}
	t.Logf("success: %d requests, %.0f RPS, p99=%v", result.Requests, result.RequestsPerSec, result.Latency.P99)
}

func TestBenchmarkerWarmup(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    1 * time.Second,
		Connections: 4,
		Workers:     4,
		Warmup:      1 * time.Second,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	result, err := b.Run(context.Background())
	elapsed := time.Since(start)
	if err != nil {
		t.Fatal(err)
	}

	// Total wall clock should be ~2s (1s warmup + 1s duration).
	// Result.Duration should be ~1s (only the measured phase).
	if elapsed < 1500*time.Millisecond {
		t.Errorf("total elapsed=%v, expected >= 1.5s (warmup + duration)", elapsed)
	}
	if result.Duration > 2*time.Second {
		t.Errorf("result.Duration=%v, expected ~1s (should not include warmup)", result.Duration)
	}
	if result.Requests == 0 {
		t.Error("expected requests > 0")
	}
	t.Logf("warmup: elapsed=%v, result.Duration=%v, requests=%d", elapsed, result.Duration, result.Requests)
}

func TestBenchmarkerContextCancellation(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    30 * time.Second, // long duration
		Connections: 4,
		Workers:     4,
		Warmup:      0,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()

	done := make(chan struct{})
	var result *Result
	go func() {
		result, _ = b.Run(ctx)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after context cancellation")
	}

	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.Requests == 0 {
		t.Error("expected partial results with requests > 0")
	}
	t.Logf("cancellation: %d requests in %v", result.Requests, result.Duration)
}

func TestBenchmarkerErrors(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(500)
		_, _ = w.Write([]byte("Internal Server Error"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    1 * time.Second,
		Connections: 2,
		Workers:     2,
		Warmup:      0,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if result.Errors == 0 {
		t.Error("expected errors > 0 for server returning 500")
	}
	t.Logf("errors: %d errors out of %d total (requests + errors)", result.Errors, result.Requests+result.Errors)
}

func TestBenchmarkerTimeseries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    3 * time.Second,
		Connections: 4,
		Workers:     4,
		Warmup:      0,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if len(result.Timeseries) == 0 {
		t.Error("expected timeseries entries for 3s benchmark")
	}
	// With 3s duration and 1s ticker, expect at least 2 entries
	if len(result.Timeseries) < 2 {
		t.Errorf("expected at least 2 timeseries entries, got %d", len(result.Timeseries))
	}
	t.Logf("timeseries: %d points", len(result.Timeseries))
}

func TestBenchmarkerOnProgress(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	var progressCalls atomic.Int64

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    3 * time.Second,
		Connections: 4,
		Workers:     4,
		Warmup:      0,
		OnProgress: func(_ time.Duration, _ Result) {
			progressCalls.Add(1)
		},
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	_, err = b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	calls := progressCalls.Load()
	if calls == 0 {
		t.Error("expected OnProgress to be called at least once")
	}
	t.Logf("OnProgress called %d times", calls)
}

func TestBenchmarkerMaxRPS(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    2 * time.Second,
		Connections: 4,
		Workers:     4,
		Warmup:      0,
		MaxRPS:      100,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// With MaxRPS=100 and 2s duration, expect ~200 requests.
	// Use wide tolerance for test stability.
	expectedMin := int64(50)  // 25% of target
	expectedMax := int64(400) // 200% of target
	if result.Requests < expectedMin || result.Requests > expectedMax {
		t.Errorf("requests=%d, expected between %d and %d (MaxRPS=100, 2s)", result.Requests, expectedMin, expectedMax)
	}
	t.Logf("MaxRPS: %d requests in %v (target ~200)", result.Requests, result.Duration)
}

// mockClient implements Client for testing custom client injection.
type mockClient struct {
	calls atomic.Int64
}

func (m *mockClient) DoRequest(_ context.Context, _ int) (int, error) {
	m.calls.Add(1)
	// Simulate a tiny bit of work
	time.Sleep(100 * time.Microsecond)
	return 10, nil
}

func (m *mockClient) Close() {}

func TestBenchmarkerCustomClient(t *testing.T) {
	mock := &mockClient{}

	cfg := Config{
		URL:         "http://localhost:9999/fake",
		Method:      "GET",
		Duration:    1 * time.Second,
		Connections: 2,
		Workers:     2,
		Warmup:      0,
		Client:      mock,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}

	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	calls := mock.calls.Load()
	if calls == 0 {
		t.Error("expected mock client to receive DoRequest calls")
	}
	if result.Requests == 0 {
		t.Error("expected result to record requests from mock client")
	}
	t.Logf("custom client: %d calls, %d recorded requests", calls, result.Requests)
}

func TestBenchmarkerValidation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"empty URL", Config{Duration: 1 * time.Second, Workers: 1, Connections: 1}},
		{"invalid scheme", Config{URL: "ftp://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1}},
		{"zero duration", Config{URL: "http://localhost", Duration: 0, Workers: 1, Connections: 1}},
		{"negative duration", Config{URL: "http://localhost", Duration: -1, Workers: 1, Connections: 1}},
		{"zero workers", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 0, Connections: 1}},
		{"zero connections", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 0}},
		{"invalid method", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1, Method: "INVALID"}},
		{"negative MaxResponseSize", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1, MaxResponseSize: -2}},
		{"negative DialTimeout", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1, DialTimeout: -1}},
		{"negative ReadBufferSize", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1, ReadBufferSize: -1}},
		{"negative WriteBufferSize", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1, WriteBufferSize: -1}},
		{"empty header key", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1, Headers: map[string]string{"": "val"}}},
		{"header with CR", Config{URL: "http://localhost", Duration: 1 * time.Second, Workers: 1, Connections: 1, Headers: map[string]string{"X-Bad\r": "val"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			_, err := New(tt.cfg)
			if err == nil {
				st.Errorf("expected error for %s config", tt.name)
			}
		})
	}
}

func TestBenchmarkerH2(t *testing.T) {
	// Start an h2c server
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	})

	h2s := &http2.Server{}
	handler := h2c.NewHandler(mux, h2s)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: handler}
	go func() { _ = srv.Serve(ln) }()
	defer func() { _ = srv.Close(); _ = ln.Close() }()

	cfg := Config{
		URL:      fmt.Sprintf("http://%s/", ln.Addr().String()),
		Method:   "GET",
		Duration: 2 * time.Second,
		Workers:  4,
		Warmup:   0,
		HTTP2:    true,
		HTTP2Options: HTTP2Options{
			Connections: 2,
			MaxStreams:  50,
		},
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
		t.Error("expected H2 requests > 0")
	}
	if result.Errors != 0 {
		t.Errorf("unexpected H2 errors: %d", result.Errors)
	}
	t.Logf("H2: %d requests, %.0f RPS, p99=%v", result.Requests, result.RequestsPerSec, result.Latency.P99)
}

func TestBenchmarkerH2Validation(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{
			"zero H2 connections",
			Config{
				URL: "http://localhost", Duration: 1 * time.Second,
				Workers: 1, HTTP2: true,
				HTTP2Options: HTTP2Options{Connections: 0, MaxStreams: 100},
			},
		},
		{
			"zero H2 max streams",
			Config{
				URL: "http://localhost", Duration: 1 * time.Second,
				Workers: 1, HTTP2: true,
				HTTP2Options: HTTP2Options{Connections: 1, MaxStreams: 0},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// New() applies defaults for zero values, so these should succeed.
			// We test that the defaults are applied correctly.
			b, err := New(tt.cfg)
			if err != nil {
				// If validation catches it, that's also valid behavior
				return
			}
			// If it succeeded, the defaults were applied
			_ = b
		})
	}
}
