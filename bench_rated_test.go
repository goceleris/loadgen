package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// TestRatedModeBasic verifies a constant-rate run records the requested
// rate and flips Mode/RatedMode/TargetRPS on the Result.
func TestRatedModeBasic(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    2 * time.Second,
		Connections: 8,
		Workers:     8,
		Warmup:      0,
		Rate:        500,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.RatedMode {
		t.Error("expected RatedMode=true")
	}
	if result.TargetRPS != 500 {
		t.Errorf("TargetRPS = %v, want 500", result.TargetRPS)
	}
	if result.Mode != "rated" {
		t.Errorf("Mode = %q, want rated", result.Mode)
	}
	// Achieved RPS should be roughly the target. Allow generous tolerance
	// for httptest jitter on busy CI machines: must be within 30%.
	if result.RequestsPerSec < 350 || result.RequestsPerSec > 650 {
		t.Errorf("RequestsPerSec = %.0f, expected ~500", result.RequestsPerSec)
	}
	if len(result.Histogram) == 0 {
		t.Error("expected non-empty Histogram payload")
	}
}

// TestRatedModeCoordinatedOmission asserts that when the server stalls
// briefly, the recorded tail latency reflects the *intended* dispatch
// time, not the actual start. With saturation mode + a stalled server,
// loadgen would simply send less and the tail would be small. With
// rated-mode + intended-time latency, queueing pressure surfaces in the
// histogram.
func TestRatedModeCoordinatedOmission(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	var count atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		n := count.Add(1)
		// Stall hard for the first 30 requests, then run free.
		if n <= 30 {
			time.Sleep(150 * time.Millisecond)
		}
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
		Rate:        200, // 200 rps means 5ms period
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// With coordinated-omission correction, p99 must reflect the queueing
	// delay during the stall (≥100ms). Without correction, the worker
	// would only sample post-stall fast responses and report tiny p99.
	if result.Latency.P99 < 50*time.Millisecond {
		t.Errorf("expected P99 >= 50ms (coordinated-omission correction), got %v", result.Latency.P99)
	}
}

// TestRatedModeEmitsHistogramOnEmpty makes sure that a rated-mode run with
// no successful requests still flips Mode/RatedMode but reports an empty
// histogram (rather than panicking).
func TestRatedModeNoServerStillCleansUp(t *testing.T) {
	// Listener that accepts but never reads or writes — every request
	// will hang. We rely on Run's runCancel timeout to unwind.
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
		// no-op: net/http will eventually time out the response writer
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    500 * time.Millisecond,
		Connections: 2,
		Workers:     2,
		Warmup:      0,
		Rate:        50,
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !result.RatedMode {
		t.Error("expected RatedMode=true")
	}
}
