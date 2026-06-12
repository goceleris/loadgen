package loadgen

import (
	"bufio"
	"context"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// goid returns the calling goroutine's id parsed from runtime.Stack.
// Test-only: goroutine ids are never reused by the runtime, which makes
// them a deterministic fingerprint for "did the benchmarker spawn a second
// worker set at the warmup→measure boundary".
func goid() int64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	// Format: "goroutine 123 [running]:..."
	const prefix = len("goroutine ")
	var id int64
	for _, c := range buf[prefix:n] {
		if c < '0' || c > '9' {
			break
		}
		id = id*10 + int64(c-'0')
	}
	return id
}

// handoffClient is a Client that fingerprints every DoRequest call:
// which goroutine issued it, which workerID, and when the worker was
// first seen relative to the run start.
type handoffClient struct {
	start time.Time

	mu         sync.Mutex
	goroutines map[int64]struct{}
	firstSeen  map[int]time.Duration

	calls atomic.Int64
}

func newHandoffClient() *handoffClient {
	return &handoffClient{
		start:      time.Now(),
		goroutines: make(map[int64]struct{}),
		firstSeen:  make(map[int]time.Duration),
	}
}

func (h *handoffClient) DoRequest(_ context.Context, workerID int) (int, error) {
	h.mu.Lock()
	h.goroutines[goid()] = struct{}{}
	if _, ok := h.firstSeen[workerID]; !ok {
		h.firstSeen[workerID] = time.Since(h.start)
	}
	h.mu.Unlock()
	h.calls.Add(1)
	time.Sleep(200 * time.Microsecond)
	return 2, nil
}

func (h *handoffClient) Close() {}

// TestSaturationHandoffContinuity pins the calibrated warmup→measure
// handoff in saturation mode: the commanded rate of an open benchmark is
// its closed-loop concurrency, so "no step discontinuity across the
// boundary" means (a) warmup already runs the full worker set and (b) the
// exact same worker goroutines carry across the boundary instead of being
// stopped and restarted. Before the fix, warmup ran 75% of the workers and
// the boundary respawned everything — the +25% concurrency step plus the
// phase-aligned restart burst put every error of a run into the first
// measured second (v3.9 chain-api repro).
func TestSaturationHandoffContinuity(t *testing.T) {
	const workers = 8
	hc := newHandoffClient()

	cfg := Config{
		URL:         "http://127.0.0.1:1/", // never dialled: custom Client
		Method:      "GET",
		Duration:    400 * time.Millisecond,
		Connections: workers,
		Workers:     workers,
		Warmup:      600 * time.Millisecond,
		Client:      hc,
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

	hc.mu.Lock()
	gor := len(hc.goroutines)
	seen := len(hc.firstSeen)
	var latestFirst time.Duration
	for _, d := range hc.firstSeen {
		if d > latestFirst {
			latestFirst = d
		}
	}
	hc.mu.Unlock()

	// (a) Full worker set participates in the warmup, and (b) no second
	// worker set is spawned at the boundary: exactly `workers` distinct
	// goroutines must have issued requests over the whole run. The
	// pre-fix behaviour would show warmup (6) + measured (8) = 14.
	if gor != workers {
		t.Errorf("distinct worker goroutines = %d, want %d (stop/restart at the warmup boundary?)", gor, workers)
	}
	if seen != workers {
		t.Errorf("distinct workerIDs = %d, want %d (warmup did not run the full worker set)", seen, workers)
	}
	// Every worker must be active early in the warmup — well before the
	// boundary at 600ms. 300ms is half the warmup: a worker first seen
	// later than that was started by the measured window, i.e. a step.
	if latestFirst > 300*time.Millisecond {
		t.Errorf("latest worker first seen at %v, want < 300ms (worker joined after warmup start)", latestFirst)
	}

	// Accounting across the handoff is exact: every successful request
	// lands in exactly one phase, including per-shard local counters
	// that were unflushed when the recorder swap happened.
	if result.Warmup == nil {
		t.Fatal("expected Result.Warmup to be populated")
	}
	total := hc.calls.Load()
	if got := result.Warmup.Requests + result.Requests; got != total {
		t.Errorf("warmup(%d) + measured(%d) = %d requests, want exactly %d (client total)",
			result.Warmup.Requests, result.Requests, got, total)
	}
	if result.Warmup.Requests == 0 {
		t.Error("expected warmup requests > 0")
	}
	if result.Requests == 0 {
		t.Error("expected measured requests > 0")
	}
	if result.Errors != 0 || result.Warmup.Errors != 0 {
		t.Errorf("unexpected errors: measured=%d warmup=%d", result.Errors, result.Warmup.Errors)
	}
	t.Logf("handoff: %d goroutines, warmup=%d measured=%d total=%d latestFirstSeen=%v",
		gor, result.Warmup.Requests, result.Requests, total, latestFirst)
}

// TestRatedHandoffKeepsStopStart pins that rated mode keeps its two-phase
// structure: the warmup's closed-loop workers drain at the boundary and a
// fresh ratedWorker set drives the measured window — so the constant-rate
// scheduler semantics are untouched by the saturation handoff change.
func TestRatedHandoffKeepsStopStart(t *testing.T) {
	const workers = 4
	hc := newHandoffClient()

	cfg := Config{
		URL:         "http://127.0.0.1:1/",
		Method:      "GET",
		Duration:    500 * time.Millisecond,
		Connections: workers,
		Workers:     workers,
		Warmup:      300 * time.Millisecond,
		Rate:        200,
		Client:      hc,
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

	hc.mu.Lock()
	gor := len(hc.goroutines)
	hc.mu.Unlock()

	// Warmup spawns `workers` closed-loop goroutines, the measured window
	// spawns `workers` ratedWorker goroutines: 2× distinct ids in total.
	if gor != 2*workers {
		t.Errorf("distinct worker goroutines = %d, want %d (rated warmup must stop/start at the boundary)", gor, 2*workers)
	}
	if !result.RatedMode {
		t.Error("expected RatedMode result")
	}
	if result.Warmup == nil || result.Warmup.Requests == 0 {
		t.Errorf("expected populated warmup stats, got %+v", result.Warmup)
	}
	if got := result.Warmup.Requests + result.Requests; got != hc.calls.Load() {
		t.Errorf("warmup(%d) + measured(%d) = %d requests, want exactly %d",
			result.Warmup.Requests, result.Requests, got, hc.calls.Load())
	}
	t.Logf("rated handoff: %d goroutines, warmup=%d measured=%d", gor, result.Warmup.Requests, result.Requests)
}

// sheddingServer is a raw-TCP HTTP/1.1 server that models the SUT
// behaviours behind the v3.9 t=0 error burst:
//
//   - a hard throughput limit: every response waits for a slot from a
//     zero-burst pacer (capacity 1), so sustained RPS cannot exceed
//     1/interval no matter how hard the client pushes;
//   - cold-start shedding: during the slow-start window after boot, every
//     3rd request gets its connection closed without a response (the
//     "server absorbing the initial burst" phase — these errors must land
//     in warmup, never in the measured window);
//   - idle-connection expiry: a connection idle longer than idleTimeout is
//     closed server-side. Before the fix, the 25% of conns that warmup
//     never exercised were idle-expired and turned into guaranteed
//     measured-window errors at t=0.
type sheddingServer struct {
	ln        net.Listener
	start     time.Time
	interval  int64 // pacer slot width, nanoseconds
	slowStart time.Duration
	idle      time.Duration

	nextSlot atomic.Int64
	reqCount atomic.Int64
	sheds    atomic.Int64
}

func startSheddingServer(t *testing.T, interval, slowStart, idle time.Duration) *sheddingServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &sheddingServer{
		ln:        ln,
		start:     time.Now(),
		interval:  int64(interval),
		slowStart: slowStart,
		idle:      idle,
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return s
}

// acquireSlot blocks until this request's paced slot arrives. Zero-burst:
// the next slot never lags behind "now", so quiet periods do not bank
// tokens that a later burst could spend.
func (s *sheddingServer) acquireSlot() {
	for {
		now := time.Now().UnixNano()
		cur := s.nextSlot.Load()
		slot := cur
		if now > slot {
			slot = now
		}
		if s.nextSlot.CompareAndSwap(cur, slot+s.interval) {
			if d := slot - now; d > 0 {
				time.Sleep(time.Duration(d))
			}
			return
		}
	}
}

var sheddingResponse = []byte("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nOK")

func (s *sheddingServer) serve(conn net.Conn) {
	defer func() { _ = conn.Close() }()
	br := bufio.NewReaderSize(conn, 4096)
	for {
		// Idle expiry: kill connections that sit quiet, like a server
		// keep-alive timeout would.
		_ = conn.SetReadDeadline(time.Now().Add(s.idle))
		// Read one request: header lines until the blank line (GETs only).
		seenRequestLine := false
		for {
			line, err := br.ReadSlice('\n')
			if err != nil {
				return // idle timeout, client close, or mid-request error
			}
			if len(line) <= 2 {
				break
			}
			seenRequestLine = true
		}
		if !seenRequestLine {
			return
		}
		// Cold-start shedding: while the server "boots", every 3rd
		// request gets its conn dropped instead of a response.
		if time.Since(s.start) < s.slowStart && s.reqCount.Add(1)%3 == 0 {
			s.sheds.Add(1)
			return
		}
		s.acquireSlot()
		if _, err := conn.Write(sheddingResponse); err != nil {
			return
		}
	}
}

// TestSaturationHandoffZeroMeasuredErrors is the loopback integration test
// for the calibrated handoff: against a rate-limited server that sheds
// during its cold start and expires idle connections, the warmup phase may
// record errors (and reports them on Result.Warmup — honest calibration),
// but the measured window must be structurally clean AND must converge to
// the server's throughput limit rather than understating it.
func TestSaturationHandoffZeroMeasuredErrors(t *testing.T) {
	const (
		workers      = 16
		slotInterval = 125 * time.Microsecond // 8000 req/s server-side limit
		slowStart    = 250 * time.Millisecond
		idleTimeout  = 400 * time.Millisecond
	)
	srv := startSheddingServer(t, slotInterval, slowStart, idleTimeout)

	cfg := Config{
		URL:         "http://" + srv.ln.Addr().String() + "/",
		Method:      "GET",
		Duration:    1200 * time.Millisecond,
		Connections: workers,
		Workers:     workers,
		Warmup:      1200 * time.Millisecond,
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
	if result.Warmup == nil {
		t.Fatal("expected warmup stats")
	}

	// The requirement: measured-window errors are structurally zero.
	// Cold-start sheds happen inside the warmup; no connection ever goes
	// idle long enough to be expired because the warmup exercises the
	// full pool and the handoff never stops the workers.
	if result.Errors != 0 {
		t.Errorf("measured-window errors = %d, want 0 (calibration burst leaked past the warmup boundary)", result.Errors)
	}
	for _, p := range result.Timeseries {
		if p.Errors != 0 {
			t.Errorf("timeseries bucket at t=%.1fs has %d errors, want 0", p.TimestampSec, p.Errors)
		}
	}

	// The shedding really happened — and landed in warmup where it is
	// reported, not discarded.
	if srv.sheds.Load() == 0 {
		t.Error("server shed no requests — slow-start window never exercised, test is vacuous")
	}
	if result.Warmup.Errors == 0 {
		t.Error("expected warmup errors > 0 from cold-start shedding (honest calibration reporting)")
	}

	// Convergence: the measured window must track the server's limit, not
	// re-discover it. Upper bound: the pacer is zero-burst, so measured
	// RPS cannot exceed the limit by more than scheduling noise. Lower
	// bound: half the limit is far above what a botched handoff that
	// understates the knee would produce, while staying robust to slow CI.
	limit := float64(time.Second) / float64(slotInterval)
	if result.RequestsPerSec > 1.15*limit {
		t.Errorf("measured RPS %.0f exceeds server limit %.0f — pacer bypassed?", result.RequestsPerSec, limit)
	}
	if result.RequestsPerSec < 0.5*limit {
		t.Errorf("measured RPS %.0f below 50%% of server limit %.0f — max understated", result.RequestsPerSec, limit)
	}

	// No understatement across the handoff: the measured window must be
	// at least as fast as the warmup average (which includes the slow
	// cold-start convergence).
	warmupAvg := float64(result.Warmup.Requests) / cfg.Warmup.Seconds()
	if result.RequestsPerSec < 0.9*warmupAvg {
		t.Errorf("measured RPS %.0f < 90%% of warmup average %.0f — handoff lost the calibrated rate", result.RequestsPerSec, warmupAvg)
	}

	t.Logf("shedding server: warmup=%d reqs / %d errors, measured=%.0f RPS (limit %.0f), sheds=%d",
		result.Warmup.Requests, result.Warmup.Errors, result.RequestsPerSec, limit, srv.sheds.Load())
}
