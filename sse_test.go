package loadgen

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"
)

// sseTestServer flushes "data: hello\n\n" on a ticker until the request is
// cancelled, mirroring the celeris /events fan-out behaviour.
func sseTestServer(t *testing.T, interval time.Duration) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)
		flusher.Flush()

		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				if _, err := fmt.Fprint(w, "data: hello\n\n"); err != nil {
					return
				}
				flusher.Flush()
			}
		}
	}))
}

// sseDialClient builds an sseClient directly against an httptest server.
func sseDialClient(t *testing.T, srv *httptest.Server) Client {
	t.Helper()
	u, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{URL: srv.URL + "/events", Mode: sseMode, scheme: u.Scheme}
	c, err := newSSEClient(u.Hostname(), u.Port(), "/events", cfg)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func TestSSEConsumesEvents(t *testing.T) {
	srv := sseTestServer(t, time.Millisecond)
	defer srv.Close()

	c := sseDialClient(t, srv)
	defer c.Close()

	for i := 0; i < 5; i++ {
		n, err := c.DoRequest(context.Background(), 0)
		if err != nil {
			t.Fatalf("DoRequest %d: %v", i, err)
		}
		if n != len("hello") {
			t.Errorf("event %d: got %d bytes, want %d", i, n, len("hello"))
		}
	}
}

func TestSSERejectsNonStream(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("not a stream"))
	}))
	defer srv.Close()

	c := sseDialClient(t, srv)
	defer c.Close()

	if _, err := c.DoRequest(context.Background(), 0); err == nil {
		t.Fatal("expected DoRequest to fail against a non-event-stream response")
	}
}

func TestSSERunIntegration(t *testing.T) {
	srv := sseTestServer(t, time.Millisecond)
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL + "/events",
		Mode:        sseMode,
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
	// Each delivered event counts as exactly one request, and each carries a
	// 5-byte "hello" payload: throughput must equal requests * 5 / duration.
	wantBPS := float64(result.Requests*int64(len("hello"))) / result.Duration.Seconds()
	if delta := result.ThroughputBPS - wantBPS; delta < -1 || delta > 1 {
		t.Errorf("throughput=%.2f bps, want ~%.2f (requests=%d, one 5-byte event each)",
			result.ThroughputBPS, wantBPS, result.Requests)
	}
}
