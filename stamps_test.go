package loadgen

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"runtime/metrics"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c" //nolint:staticcheck // same scaffold as h2client_test.go: h2c.NewHandler has no net/http replacement for prior-knowledge h2c.
)

// startH2CHandler serves handler over prior-knowledge h2c.
func startH2CHandler(t testing.TB, handler http.Handler) (host, port string) {
	t.Helper()
	h2s := &http2.Server{MaxConcurrentStreams: 1000} //nolint:staticcheck // see the import-line waiver
	h := h2c.NewHandler(handler, h2s)                //nolint:staticcheck // see the import-line waiver
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := &http.Server{Handler: h}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close(); _ = ln.Close() })
	host, port, _ = net.SplitHostPort(ln.Addr().String())
	return host, port
}

func stampsH2Cfg(host, port string, rate float64, d time.Duration, workers int) Config {
	return Config{
		URL:          fmt.Sprintf("http://%s:%s/x", host, port),
		Method:       "GET",
		Duration:     d,
		Workers:      workers,
		HTTP2:        true,
		HTTP2Options: HTTP2Options{Connections: 4, MaxStreams: 100},
		Rate:         rate,
	}
}

func segIdx(t *testing.T, cs *ClientStamps, name string) int {
	t.Helper()
	i := slices.Index(cs.SegmentOrder, name)
	if i < 0 {
		t.Fatalf("segment %q not in %v", name, cs.SegmentOrder)
	}
	return i
}

// ---- helpers ------------------------------------------------------------

func TestStampBuckets(t *testing.T) {
	prev := -1
	for _, ns := range []int64{0, 1, 500, 1023, 1024, 2047, 2048, 3000, 5000, 1e5, 1e6, 1e7, 1e8, 1e9, 6e10} {
		b := stampBucketOf(ns)
		if b < prev {
			t.Errorf("bucket(%d) = %d went backwards from %d", ns, b, prev)
		}
		prev = b
		if ns > 0 && stampBucketUpper(b) <= ns && b < stampBuckets-1 {
			t.Errorf("upper(bucket(%d)=%d) = %d <= value", ns, b, stampBucketUpper(b))
		}
		if b > 0 && b < stampBuckets-1 && ns >= 1024 && float64(stampBucketUpper(b)-ns) > 0.26*float64(ns)+1024 {
			t.Errorf("bucket(%d) upper %d is more than 25%% above the value", ns, stampBucketUpper(b))
		}
	}
	// Quantiles against the exact values: within one bucket (25 percent).
	var a segAgg
	var vals []int64
	for i := 1; i <= 10000; i++ {
		v := int64(i) * 3700
		vals = append(vals, v)
		a.add(v, 10_000_000)
	}
	for _, q := range []float64{0.5, 0.99, 0.999} {
		exact := vals[int(math.Ceil(q*float64(len(vals))))-1]
		got := a.quantile(q)
		if got < exact || float64(got) > 1.26*float64(exact) {
			t.Errorf("q%v: got %d, exact %d", q, got, exact)
		}
	}
	if a.over != int64(len(vals)-2702) { // values above 10 ms: i*3700 > 1e7 -> i >= 2703
		t.Errorf("over = %d, want %d", a.over, len(vals)-2702)
	}
}

func TestStampsEnvParsing(t *testing.T) {
	cases := []struct {
		env  string
		rate float64
		want bool
		err  bool
	}{
		{"", 300321, false, false},
		{"0", 300321, false, false},
		{"off", 300321, false, false},
		{"1", 300321, true, false},
		{"all", 300321, true, false},
		{"1", 0, false, false}, // a saturation run is never stamped
		{"300321", 300321, true, false},
		{"300321", 450481, false, false},
		{"300321,450481", 450481, true, false},
		{" 300321 , 450481 ", 300321, true, false},
		{"300321", 0, false, false},
		{"slot0", 300321, false, true},
		{"300321,x", 300321, false, true},
		{"-5", 300321, false, true},
	}
	for _, c := range cases {
		t.Setenv(ClientStampsEnv, c.env)
		cs, err := stampsFromEnv(c.rate)
		if (err != nil) != c.err {
			t.Errorf("env %q rate %v: err = %v, want error %v", c.env, c.rate, err, c.err)
			continue
		}
		if (cs != nil) != c.want {
			t.Errorf("env %q rate %v: stamps = %v, want %v", c.env, c.rate, cs != nil, c.want)
		}
	}
	// New refuses a value it cannot parse, whatever the rate.
	t.Setenv(ClientStampsEnv, "slot0")
	if _, err := New(Config{URL: "http://127.0.0.1:1/", Duration: time.Second, Connections: 1, Workers: 1}); err == nil || !strings.Contains(err.Error(), ClientStampsEnv) {
		t.Errorf("New with a bad %s: err = %v", ClientStampsEnv, err)
	}
}

func TestStampsStaleAttemptDropped(t *testing.T) {
	s := newStampSet(&ClientStampsConfig{}, 1, time.Now())
	r := s.rec(0)
	first := r.attempt()
	second := r.attempt() // a retry: the first attempt's writer is still working
	r.markFlush(first, 1, 2)
	r.markWdq(first)
	r.markHdr(first)
	if r.tFl.Load() != 0 || r.tWdq.Load() != 0 || r.tHdr.Load() != 0 {
		t.Fatal("a stamp of an earlier attempt was written into the new one")
	}
	r.markWdq(second)
	r.markHdr(second)
	h := r.tHdr.Load()
	r.markHdr(second)
	if r.tWdq.Load() == 0 || h == 0 || r.tHdr.Load() != h {
		t.Fatal("the current attempt's stamps were lost, or the first HEADERS stamp was overwritten")
	}
	if r.attempts != 2 {
		t.Fatalf("attempts = %d, want 2", r.attempts)
	}
}

func TestStampsSchedulerTick(t *testing.T) {
	base := time.Now()
	s := newStampSet(&ClientStampsConfig{}, 1, base)
	s.tick(base.Add(-50 * time.Millisecond)) // 50 ms late, intended before the window start
	s.tick(time.Now().Add(time.Millisecond)) // not yet due: clamped to 0
	if s.sched.agg.n != 2 || s.sched.agg.over != 1 || s.sched.agg.max < int64(50*time.Millisecond) {
		t.Fatalf("sched agg = n %d over %d max %d", s.sched.agg.n, s.sched.agg.over, s.sched.agg.max)
	}
	cs := s.result(time.Second, base)
	if cs.Scheduler.Ticks != 2 || cs.Scheduler.LateOver != 1 {
		t.Fatalf("scheduler = %+v", cs.Scheduler)
	}
}

func TestHistDelta(t *testing.T) {
	mk := func(c ...uint64) *metrics.Float64Histogram {
		return &metrics.Float64Histogram{
			Counts:  c,
			Buckets: []float64{0, 0.0001, 0.001, 0.01, math.Inf(1)},
		}
	}
	d := histDelta(mk(5, 5, 5, 5), mk(6, 7, 9, 12))
	if d == nil || d.Count != 1+2+4+7 || d.Over1ms != 4+7 || d.Over10ms != 7 || d.MaxLENS != int64(1e9*1e9) {
		t.Fatalf("histDelta = %+v", d)
	}
	if histDelta(mk(5, 5, 5, 5), mk(4, 5, 5, 5)) != nil {
		t.Error("a histogram that went backwards must give nil")
	}
	if histDelta(nil, mk(1, 1, 1, 1)) != nil {
		t.Error("nil start must give nil")
	}
	// The live metrics exist on this runtime and a GC shows in the delta.
	s := newStampSet(&ClientStampsConfig{}, 1, time.Now())
	s.runtimeStart()
	runtime.GC()
	rs := s.runtimeDelta()
	if rs == nil || rs.GCCycles < 1 || rs.SchedLatency == nil || rs.GCPause == nil {
		t.Fatalf("runtime delta = %+v", rs)
	}
}

// ---- off: nothing runs --------------------------------------------------

// TestStampsOffRunsNothing is the proof that OFF changes nothing observable: a
// rated H2 run without Config.ClientStamps and without the environment variable
// has no block, no stamp state on the client, the benchmarker or any stream slot, and
// its JSON has no client_stamps key.
func TestStampsOffRunsNothing(t *testing.T) {
	t.Setenv(ClientStampsEnv, "")
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	b, err := New(stampsH2Cfg(host, port, 2000, time.Second, 4))
	if err != nil {
		t.Fatal(err)
	}
	if b.config.ClientStamps != nil {
		t.Fatal("ClientStamps was set without a request")
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.Requests == 0 {
		t.Fatal("no requests")
	}
	if res.ClientStamps != nil {
		t.Fatal("Result.ClientStamps set with the stamps off")
	}
	raw, _ := json.Marshal(res)
	if strings.Contains(string(raw), "client_stamps") || strings.Contains(string(raw), "clientstamps") {
		t.Fatal("the JSON of an unstamped result mentions the stamps")
	}
	c := b.raw.(*h2Client)
	if b.stamps != nil || c.stamps != nil {
		t.Fatal("stamp state attached with the stamps off")
	}
	for _, slot := range c.conns {
		hc := slot.cur.Load()
		if hc == nil {
			continue
		}
		if len(hc.stampBatch) != 0 {
			t.Fatal("the writer batched stamps with the stamps off")
		}
		for i := range hc.streamSlots {
			if hc.streamSlots[i].stamp.Load() != nil || hc.streamSlots[i].stampSeq.Load() != 0 {
				t.Fatal("a stream slot carries a stamp with the stamps off")
			}
		}
	}
}

// TestStampsFile: the sidecar line is written for a stamped run, one per run,
// and never for an unstamped one.
func TestStampsFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer srv.Close()
	path := filepath.Join(t.TempDir(), "stamps.jsonl")
	t.Setenv(ClientStampsFileEnv, path)
	run := func(stamped bool) {
		cfg := Config{URL: srv.URL, Method: "GET", Duration: 500 * time.Millisecond, Connections: 2, Workers: 2, Rate: 300}
		if stamped {
			cfg.ClientStamps = &ClientStampsConfig{}
		}
		b, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := b.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	run(false)
	if _, err := os.Stat(path); err == nil {
		t.Fatal("an unstamped run wrote the stamps file")
	}
	run(true)
	run(true)
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) != 2 {
		t.Fatalf("%d lines, want 2", len(lines))
	}
	var l stampsFileLine
	if err := json.Unmarshal([]byte(lines[0]), &l); err != nil || l.ClientStamps.Schema != ClientStampsSchema || l.TargetRPS != 300 || l.Requests == 0 {
		t.Fatalf("line = %v %+v", err, l)
	}
}

// TestStampsOffAndOnSaturationIgnored: a saturation run never stamps, even when
// asked.
func TestStampsSaturationIgnored(t *testing.T) {
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	cfg := stampsH2Cfg(host, port, 0, 500*time.Millisecond, 2)
	cfg.ClientStamps = &ClientStampsConfig{}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.ClientStamps != nil {
		t.Fatal("a saturation run produced a client stamps block")
	}
}

// TestStampsOnAddsNoPerRequestAllocation compares the allocations per request of
// a sequential DoRequest loop with the stamps on and off against the same
// server. The stamps use one preallocated record per worker, so the difference
// is zero up to the noise of the in-process server.
func TestStampsOnAddsNoPerRequestAllocation(t *testing.T) {
	if raceEnabled {
		t.Skip("the race detector's shadow state makes allocation counts meaningless")
	}
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	c, err := newH2Client(host, port, "/x", testH2Cfg("GET", nil, nil, 1, 100))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx := context.Background()
	run := func() float64 {
		return testing.AllocsPerRun(3000, func() {
			if _, err := c.DoRequest(ctx, 0); err != nil {
				t.Fatal(err)
			}
		})
	}
	run() // warm the pools
	off := run()

	s := newStampSet(&ClientStampsConfig{}, 1, time.Now())
	s.h2 = true
	c.attachStamps(s)
	var on float64
	on = testing.AllocsPerRun(3000, func() {
		r := s.begin(0, time.Now(), 0)
		if _, err := c.DoRequest(ctx, 0); err != nil {
			t.Fatal(err)
		}
		s.finish(0, r, time.Since(s.base.Add(time.Duration(r.tInt))))
	})
	c.attachStamps(nil)
	t.Logf("allocs/request: off %.2f, on %.2f", off, on)
	if on > off+0.5 {
		t.Errorf("stamps on cost %.2f allocations per request over off (%.2f vs %.2f)", on-off, on, off)
	}
}

// ---- on: the segments ----------------------------------------------------

// TestStampsResponseWait: a handler that takes 30 ms puts the time into
// resp_wait, nowhere else, and the nine consecutive segments add up to the
// recorded latency of each kept vector.
func TestStampsResponseWait(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	var gcOnce atomic.Bool
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if gcOnce.CompareAndSwap(false, true) {
			go func() { time.Sleep(700 * time.Millisecond); runtime.GC() }()
		}
		time.Sleep(30 * time.Millisecond)
		_, _ = w.Write([]byte("ok"))
	}))
	cfg := stampsH2Cfg(host, port, 100, 2*time.Second, 8)
	cfg.ClientStamps = &ClientStampsConfig{}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs := res.ClientStamps
	if cs == nil {
		t.Fatal("no client stamps block")
	}
	if cs.Schema != ClientStampsSchema || !cs.H2Stamps || cs.Requests < 100 || cs.Requests != res.Requests {
		t.Fatalf("block header = schema %q h2 %v requests %d (result %d)", cs.Schema, cs.H2Stamps, cs.Requests, res.Requests)
	}
	if len(cs.SegmentOrder) != nSeg || len(cs.Segments) != nSeg {
		t.Fatalf("segment_order %d / segments %d, want %d", len(cs.SegmentOrder), len(cs.Segments), nSeg)
	}
	rw := cs.Segments[segIdx(t, cs, "resp_wait")]
	if rw.Over < cs.Requests*9/10 || rw.P50NS < int64(25*time.Millisecond) {
		t.Errorf("resp_wait = %+v, want nearly every request over 10 ms and p50 >= 25 ms (requests %d)", rw, cs.Requests)
	}
	for _, name := range []string{"write", "encode", "flush", "stream_wait", "wake"} {
		if s := cs.Segments[segIdx(t, cs, name)]; s.Over != 0 {
			t.Errorf("%s over threshold %d times with a server-side delay only: %+v", name, s.Over, s)
		}
	}
	svc := cs.Segments[segIdx(t, cs, "service")]
	if svc.Over < cs.Requests*9/10 {
		t.Errorf("service over = %d of %d", svc.Over, cs.Requests)
	}
	if cs.DominantSlowService["resp_wait"] < cs.Requests*9/10 {
		t.Errorf("dominant_slow_service = %v, want resp_wait for nearly all", cs.DominantSlowService)
	}
	if cs.Incomplete > cs.Requests/20 {
		t.Errorf("incomplete = %d of %d", cs.Incomplete, cs.Requests)
	}
	if len(cs.SlowService) == 0 {
		t.Fatal("no slow-service vectors")
	}
	clamped := int64(0)
	for _, c := range cs.Clamped {
		clamped += c
	}
	for _, v := range cs.SlowService {
		var sum int64
		for _, x := range v.SegsNS {
			sum += x
		}
		if sum < v.TotalNS-1000 {
			t.Errorf("segments sum %d < total %d: time went missing (vector %+v)", sum, v.TotalNS, v)
		}
		if clamped == 0 && sum != v.TotalNS {
			t.Errorf("nothing clamped, yet segments sum %d != total %d", sum, v.TotalNS)
		}
	}
	if len(cs.PerSecond) < 2 || len(cs.PerSecond[0].Over) != nSeg || len(cs.PerSecond[0].MaxNS) != nSeg {
		t.Fatalf("per_second = %d rows", len(cs.PerSecond))
	}
	var rows int64
	for _, r := range cs.PerSecond {
		rows += r.Requests
	}
	if rows != cs.Requests {
		t.Errorf("per_second requests sum %d != %d", rows, cs.Requests)
	}
	if cs.Runtime == nil || cs.Runtime.SchedLatency == nil {
		t.Error("runtime block missing")
	}
	var gcRows, depRows int64
	for _, r := range cs.PerSecond {
		gcRows += r.RtGCCycles
		depRows += r.DominantSlowService[segIdx(t, cs, "resp_wait")]
	}
	if gcRows < 1 {
		t.Errorf("a GC inside the window shows in no per-second runtime row (rows %d)", len(cs.PerSecond))
	}
	if depRows < cs.Requests*9/10 {
		t.Errorf("per-second dominant_slow_service[resp_wait] sums to %d of %d", depRows, cs.Requests)
	}
	if cs.Scheduler.Ticks < 100 {
		t.Errorf("scheduler ticks = %d", cs.Scheduler.Ticks)
	}
	raw, err := json.Marshal(res)
	if err != nil || !strings.Contains(string(raw), `"client_stamps":{"schema":"clientstamps/1"`) {
		t.Errorf("JSON block missing or unversioned: %v %.200s", err, raw)
	}
}

// TestStampsSlotQueue mirrors TestRatedModeCoordinatedOmission: a server that
// stalls at first makes the slots queue, which shows as worker_wait with every
// worker busy, while the segments that belong to the writer stay small.
func TestStampsSlotQueue(t *testing.T) {
	if testing.Short() {
		t.Skip("short mode")
	}
	var n atomic.Int64
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if n.Add(1) <= 40 {
			time.Sleep(200 * time.Millisecond)
		}
		_, _ = w.Write([]byte("ok"))
	}))
	cfg := stampsH2Cfg(host, port, 400, 2*time.Second, 2) // 2 workers x 4 = 8 in flight
	cfg.ClientStamps = &ClientStampsConfig{}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs := res.ClientStamps
	if cs == nil {
		t.Fatal("no block")
	}
	ww := cs.Segments[segIdx(t, cs, "worker_wait")]
	if ww.Over == 0 {
		t.Fatalf("worker_wait never exceeded the threshold: %+v", ww)
	}
	if cs.Segments[segIdx(t, cs, "resp_wait")].Over == 0 {
		t.Error("resp_wait never exceeded the threshold although the server stalled")
	}
	busy, q := 0, 0
	for _, r := range cs.PerSecond {
		busy, q = max(busy, r.BusyMax), max(q, r.SlotQMax)
	}
	if busy != 8 {
		t.Errorf("busy_max = %d, want 8 (every worker in flight)", busy)
	}
	if q == 0 {
		t.Error("slot queue never deepened")
	}
	if cs.Segments[segIdx(t, cs, "write")].Over != 0 {
		t.Error("the writer segment exceeded the threshold in a server stall")
	}
	if cs.DominantSlowTotal["worker_wait"] == 0 {
		t.Errorf("dominant_slow_total = %v, want worker_wait among them", cs.DominantSlowTotal)
	}
	if len(cs.FirstLate) == 0 || len(cs.SlowService) == 0 {
		t.Errorf("reservoirs: first_late %d slow_service %d", len(cs.FirstLate), len(cs.SlowService))
	}
}

// TestStampsWithWarmup: the rated warmup (closed loop, workers that exit
// before the measured window) runs unstamped, and the stamps start with the
// measured window; under -race this also covers the attach after the warmup.
func TestStampsWithWarmup(t *testing.T) {
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	cfg := stampsH2Cfg(host, port, 500, 1500*time.Millisecond, 4)
	cfg.Warmup = 500 * time.Millisecond
	cfg.ClientStamps = &ClientStampsConfig{}
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs := res.ClientStamps
	if cs == nil || cs.Requests != res.Requests || cs.Requests < 400 {
		t.Fatalf("block = %+v, result requests %d", cs, res.Requests)
	}
	if res.Warmup == nil || res.Warmup.Requests == 0 {
		t.Error("warmup did not run")
	}
	if cs.Incomplete > cs.Requests/20 {
		t.Errorf("incomplete %d of %d", cs.Incomplete, cs.Requests)
	}
}

// TestStampsNonH2: the HTTP/1.1 client has no writer or reader goroutine of its
// own to stamp; the worker-level segments are filled and the rest stay empty.
func TestStampsNonH2(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	defer srv.Close()
	t.Setenv(ClientStampsEnv, "500")
	b, err := New(Config{URL: srv.URL, Method: "GET", Duration: time.Second, Connections: 4, Workers: 4, Rate: 500})
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs := res.ClientStamps
	if cs == nil {
		t.Fatal("the env list matched the rate but no block was produced")
	}
	if cs.H2Stamps {
		t.Error("H2Stamps true for an HTTP/1.1 client")
	}
	for _, name := range []string{"worker_wait", "service", "total"} {
		if cs.Segments[segIdx(t, cs, name)].N != cs.Requests {
			t.Errorf("%s n = %d, want %d", name, cs.Segments[segIdx(t, cs, name)].N, cs.Requests)
		}
	}
	for _, name := range []string{"resp_wait", "flush", "write"} {
		if cs.Segments[segIdx(t, cs, name)].N != 0 {
			t.Errorf("%s has samples for an HTTP/1.1 client", name)
		}
	}
	// A rate not in the list is not stamped.
	t.Setenv(ClientStampsEnv, "999")
	b2, err := New(Config{URL: srv.URL, Method: "GET", Duration: 500 * time.Millisecond, Connections: 2, Workers: 2, Rate: 500})
	if err != nil {
		t.Fatal(err)
	}
	res2, err := b2.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res2.ClientStamps != nil {
		t.Error("a rate outside the list was stamped")
	}
}

// TestStampsRowsKeyedByDequeue: a request intended in second 0 that a worker
// took in second 2 belongs to row 2 (when it went to work), and the slot is
// counted as offered load in row 0.
func TestStampsRowsKeyedByDequeue(t *testing.T) {
	base := time.Now()
	s := newStampSet(&ClientStampsConfig{}, 1, base)
	r := s.begin(0, base.Add(200*time.Millisecond), 7)
	r.tDeq = int64(2500 * time.Millisecond)
	s.finish(0, r, 3*time.Second)
	cs := s.result(4*time.Second, base)
	if len(cs.PerSecond) != 3 {
		t.Fatalf("rows = %d, want 3", len(cs.PerSecond))
	}
	if cs.PerSecond[0].Intended != 1 || cs.PerSecond[0].Requests != 0 || cs.PerSecond[2].Requests != 1 || cs.PerSecond[2].Intended != 0 {
		t.Errorf("rows = %+v", cs.PerSecond)
	}
	if cs.PerSecond[2].Over[segIdx(t, cs, "total")] != 1 {
		t.Errorf("total over missing in row 2: %+v", cs.PerSecond[2])
	}
}

// TestStampsFinishAccounting drives finish directly: an error enters no
// segment, a retried request is counted, an incomplete H2 request enters the
// worker-level segments only, and clamping is counted.
func TestStampsFinishAccounting(t *testing.T) {
	base := time.Now()
	s := newStampSet(&ClientStampsConfig{Threshold: 5 * time.Millisecond}, 2, base)
	s.h2 = true
	ms := int64(time.Millisecond)

	// 1: complete, retried once, a late flush (tHdr before tFl: clamped).
	r := s.begin(0, base.Add(10*time.Millisecond), 3)
	r.tInt, r.tDeq = 0, 1*ms
	r.attempt()
	seq := r.attempt()
	_ = seq
	r.tSem = 2 * ms
	r.tWdq.Store(3 * ms)
	r.tFrm.Store(4 * ms)
	r.tFls.Store(5 * ms)
	r.tFl.Store(8 * ms)
	r.tHdr.Store(7 * ms) // earlier than tFl: resp_wait clamps to 0
	r.tEnd.Store(20 * ms)
	s.finish(0, r, 25*time.Millisecond)

	// 2: an error.
	s.begin(1, base.Add(30*time.Millisecond), 0)
	s.fail(1)

	// 3: H2 without its goroutine stamps.
	r = s.begin(1, base.Add(40*time.Millisecond), 0)
	r.tInt, r.tDeq = 40*ms, 40*ms
	r.attempt()
	s.finish(1, r, 2*time.Millisecond)

	if s.busy.Load() != 0 {
		t.Fatalf("busy = %d after every request ended", s.busy.Load())
	}
	cs := s.result(time.Second, base)
	if cs.Requests != 2 || cs.Errors != 1 || cs.Incomplete != 1 || cs.Retried != 1 {
		t.Fatalf("requests %d errors %d incomplete %d retried %d", cs.Requests, cs.Errors, cs.Incomplete, cs.Retried)
	}
	if cs.Clamped["resp_wait"] != 1 {
		t.Errorf("clamped = %v", cs.Clamped)
	}
	get := func(n string) SegmentStats { return cs.Segments[segIdx(t, cs, n)] }
	if get("resp_wait").N != 1 || get("resp_wait").MaxNS != 0 || get("body").MaxNS != 13*ms || get("wake").MaxNS != 5*ms {
		t.Errorf("segments resp_wait %+v body %+v wake %+v", get("resp_wait"), get("body"), get("wake"))
	}
	if get("sched_lag").MaxNS != 3*ms || get("write").MaxNS != 5*ms || get("service").MaxNS != 24*ms || get("total").N != 2 {
		t.Errorf("sched_lag %+v write %+v service %+v total %+v", get("sched_lag"), get("write"), get("service"), get("total"))
	}
	if get("body").Over != 1 || get("write").Over != 0 || get("wake").Over != 0 {
		t.Errorf("over: body %d write %d wake %d (threshold 5 ms)", get("body").Over, get("write").Over, get("wake").Over)
	}
	if cs.DominantSlowService["body"] != 1 || len(cs.SlowService) != 1 {
		t.Errorf("dominant %v slow %d", cs.DominantSlowService, len(cs.SlowService))
	}
	if cs.PerSecond[0].SlotQMax != 3 || cs.PerSecond[0].BusyMax != 1 {
		t.Errorf("per second 0 = %+v", cs.PerSecond[0])
	}
	if p := cs.PerSecond[0]; p.SumNS[segBody] != 13*ms || p.SumNS[segIdx(t, cs, "total")] != 27*ms || p.Incomplete != 1 {
		t.Errorf("per second sums: body %d total %d incomplete %d", p.SumNS[segBody], p.SumNS[segIdx(t, cs, "total")], p.Incomplete)
	}
	if cs.PerSecond[0].DominantSlowService[segBody] != 1 || cs.PerSecond[0].DominantSlowService[segWorkerWait] != 0 {
		t.Errorf("per second dominant_slow_service = %v", cs.PerSecond[0].DominantSlowService)
	}
}

// BenchmarkStampsH2DoRequest measures one sequential H2 request through the
// client with the stamps off (the default path) and on. Off must match a build
// without the stamps code.
func BenchmarkStampsH2DoRequest(b *testing.B) {
	for _, on := range []bool{false, true} {
		b.Run(map[bool]string{false: "off", true: "on"}[on], func(b *testing.B) {
			host, port := startH2CHandler(b, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
			c, err := newH2Client(host, port, "/x", testH2Cfg("GET", nil, nil, 1, 100))
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			ctx := context.Background()
			var s *stampSet
			if on {
				s = newStampSet(&ClientStampsConfig{}, 1, time.Now())
				s.h2 = true
				c.attachStamps(s)
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				var r *stampRec
				if on {
					r = s.begin(0, time.Now(), 0)
				}
				if _, err := c.DoRequest(ctx, 0); err != nil {
					b.Fatal(err)
				}
				if on {
					s.finish(0, r, time.Since(s.base.Add(time.Duration(r.tInt))))
				}
			}
		})
	}
}
