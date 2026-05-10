package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

// TestFederationCoordinatorAndSidecar exercises the full primary/sidecar
// hand-off using two Benchmarker instances against the same httptest
// server.
func TestFederationCoordinatorAndSidecar(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	// Stand up a sidecar listener on an ephemeral port.
	sc, err := NewFederationSidecar("127.0.0.1:0")
	if err != nil {
		t.Fatalf("NewFederationSidecar: %v", err)
	}
	defer func() { _ = sc.Close() }()
	addr := sc.Addr()

	var wg sync.WaitGroup
	wg.Add(1)

	base := Config{
		Method:      "GET",
		Connections: 4,
		Workers:     4,
		Warmup:      0,
		HTTP2Options: HTTP2Options{
			Connections: 4,
			MaxStreams:  100,
		},
	}

	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		err := sc.Serve(ctx, base, func(rctx context.Context, cfg Config) (*Result, error) {
			b, berr := New(cfg)
			if berr != nil {
				return nil, berr
			}
			return b.Run(rctx)
		})
		if err != nil {
			t.Logf("sidecar Serve returned: %v", err)
		}
	}()

	// Primary points at the same target with -peer set to the sidecar.
	primaryCfg := Config{
		URL:         srv.URL,
		Method:      "GET",
		Duration:    1500 * time.Millisecond,
		Connections: 4,
		Workers:     4,
		Warmup:      0,
		Peer:        addr,
	}
	b, err := New(primaryCfg)
	if err != nil {
		t.Fatalf("New primary: %v", err)
	}
	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("primary Run: %v", err)
	}

	wg.Wait()

	if result.Federation == nil {
		t.Fatal("expected Federation stats")
	}
	if !result.Federation.MergeSucceeded {
		t.Fatalf("merge failed: %s", result.Federation.MergeError)
	}
	if result.Federation.PeerRequests <= 0 {
		t.Errorf("expected peer_requests > 0, got %d", result.Federation.PeerRequests)
	}
	if len(result.Histogram) == 0 {
		t.Error("expected merged Histogram payload on Result")
	}
}

// TestFederationDialFailureNonFatal asserts that an unreachable peer
// records the error in Federation but does not fail the overall run.
func TestFederationDialFailureNonFatal(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(200)
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()

	cfg := Config{
		URL:         srv.URL,
		Duration:    500 * time.Millisecond,
		Connections: 2,
		Workers:     2,
		Warmup:      0,
		Peer:        "127.0.0.1:1", // privileged port, very likely refused
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	result, err := b.Run(context.Background())
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
	if result.Federation == nil {
		t.Fatal("expected Federation stats even on dial failure")
	}
	if result.Federation.MergeSucceeded {
		t.Error("expected MergeSucceeded=false on dial failure")
	}
	if result.Federation.MergeError == "" {
		t.Error("expected MergeError to be populated")
	}
}
