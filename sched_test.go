package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"

	"testing"
	"time"
)

func TestSchedulerShardsCount(t *testing.T) {
	cases := []struct {
		rate       float64
		configured int
		procs      int
		want       int
	}{
		{100, 0, 64, 1},
		{80000, 0, 64, 1},   // at the shard rate: still one goroutine
		{80000.5, 0, 64, 2}, // non-integer rate just above
		{80001, 0, 64, 2},
		{300321, 0, 64, 4}, // the rate verified on the cluster (D757-SCHED)
		{450481, 0, 64, 6},
		{450481, 0, 4, 4}, // capped at GOMAXPROCS
		{450481, 0, 0, 6}, // procs unknown: no cap
		{1e18, 0, 64, SchedulerShardMax},
		{300321, 1, 64, 1}, // explicit single goroutine
		{100, 4, 64, 4},    // explicit count at a low rate, not capped by procs
		{100, 4, 2, 4},
		{100, 1000, 64, SchedulerShardMax},
		{0.5, 0, 64, 1},
	}
	for _, c := range cases {
		if got := schedulerShards(c.rate, c.configured, c.procs); got != c.want {
			t.Errorf("schedulerShards(%v, %d, %d) = %d, want %d", c.rate, c.configured, c.procs, got, c.want)
		}
	}
}

func TestNewRejectsNegativeSchedulerShards(t *testing.T) {
	if _, err := New(Config{URL: "http://127.0.0.1:1/", Duration: time.Second, Connections: 1, Workers: 1, Rate: 10, SchedulerShards: -1}); err == nil {
		t.Error("New with SchedulerShards -1: want an error")
	}
}

// drain collects slots until the channel is closed or the timeout hits (a
// missing close is a failure, not a hang).
func drain(t *testing.T, slots <-chan time.Time) []time.Time {
	t.Helper()
	var got []time.Time
	deadline := time.After(5 * time.Second)
	for {
		select {
		case in, ok := <-slots:
			if !ok {
				return got
			}
			got = append(got, in)
		case <-deadline:
			t.Fatal("slot channel not closed after the scheduler returned")
		}
	}
}

// tickIndex maps an intended time to its tick number n (start + n*period) and
// reports whether it lies exactly on the schedule.
func tickIndex(in, start time.Time, period time.Duration) (int, bool) {
	d := in.Sub(start)
	n := int(d / period)
	return n, d%period == 0
}

// TestShardedSchedulerSameSchedule: k scheduler goroutines emit exactly the
// intended times of the single scheduler (start + n*period, n >= 1) at a rate
// whose period is truncated (300321/s: 3329 ns, not a divisor of a second), the
// same set the k=1 path emits: every tick below the lowest tick any shard has
// not reached is present exactly once, and every slot lies on the schedule.
func TestShardedSchedulerSameSchedule(t *testing.T) {
	rate := 300321.0
	period := time.Duration(float64(time.Second) / rate)
	for _, k := range []int{1, 2, 4, 7} {
		b := &Benchmarker{config: Config{Rate: rate}}
		b.running.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		slots := make(chan time.Time, 1<<18)
		done := make(chan struct{})
		start := time.Now()
		go b.rateSchedulerShards(ctx, start, slots, done, k)
		time.Sleep(300 * time.Millisecond)
		b.running.Store(false)
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("k=%d: scheduler did not return", k)
		}
		got := drain(t, slots)
		if len(got) < 10000 {
			t.Fatalf("k=%d: only %d slots in 300ms at %v/s", k, len(got), rate)
		}
		seen := make(map[int]int, len(got))
		perShard := make([][]int, k)
		maxN := 0
		for _, in := range got {
			n, exact := tickIndex(in, start, period)
			if !exact || n < 1 {
				t.Fatalf("k=%d: slot %v is not start + n*period (n=%d)", k, in.Sub(start), n)
			}
			seen[n]++
			if seen[n] > 1 {
				t.Fatalf("k=%d: tick %d emitted twice", k, n)
			}
			perShard[(n-1)%k] = append(perShard[(n-1)%k], n)
			if n > maxN {
				maxN = n
			}
		}
		// each shard's own sequence has no gap: ticks j+1, j+1+k, ... in order
		for j, seq := range perShard {
			for i, n := range seq {
				if want := j + 1 + i*k; n != want {
					t.Fatalf("k=%d shard %d: element %d is tick %d, want %d (a tick was skipped)", k, j, i, n, want)
				}
			}
		}
		// across shards the union is gapless up to the shortest shard's last tick
		low := maxN
		for _, seq := range perShard {
			if len(seq) > 0 && seq[len(seq)-1] < low {
				low = seq[len(seq)-1]
			}
		}
		for n := 1; n <= low; n++ {
			if seen[n] != 1 {
				t.Fatalf("k=%d: tick %d missing below %d", k, n, low)
			}
		}
	}
}

// TestShardedSchedulerKeepsLateSlots: a shard that wakes late still enqueues the
// intended time (no slot is skipped). The schedule starts 100 ms in the past, so
// every shard is late from its first tick; ticks 1..500 must all arrive, once.
func TestShardedSchedulerKeepsLateSlots(t *testing.T) {
	for _, k := range []int{1, 3} {
		b := &Benchmarker{config: Config{Rate: 5000}}
		b.running.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		slots := make(chan time.Time, 1<<14)
		done := make(chan struct{})
		start := time.Now().Add(-100 * time.Millisecond)
		period := time.Duration(float64(time.Second) / 5000)
		go b.rateSchedulerShards(ctx, start, slots, done, k)
		seen := map[int]bool{}
		deadline := time.After(2 * time.Second)
		have := 0 // ticks 1..500 seen so far
		for have < 500 {
			select {
			case in := <-slots:
				n, exact := tickIndex(in, start, period)
				if !exact || n < 1 {
					t.Fatalf("k=%d: slot not on the schedule: %v", k, in.Sub(start))
				}
				if seen[n] {
					t.Fatalf("k=%d: tick %d twice", k, n)
				}
				seen[n] = true
				if n <= 500 {
					have++
				}
			case <-deadline:
				t.Fatalf("k=%d: only %d of the first 500 late ticks arrived", k, have)
			}
		}
		for n := 1; n <= 500; n++ {
			if !seen[n] {
				t.Fatalf("k=%d: late tick %d was skipped", k, n)
			}
		}
		b.running.Store(false)
		cancel()
		<-done
	}
}

// TestShardedSchedulerStopsOnCancel: shards blocked on a full channel return when
// the context is cancelled, and the channel is then closed. Covers the legacy
// path's neighbour k=1 as well.
func TestShardedSchedulerStopsOnCancel(t *testing.T) {
	for _, k := range []int{1, 4} {
		b := &Benchmarker{config: Config{Rate: 100000}}
		b.running.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		slots := make(chan time.Time, 1) // nobody reads: every shard blocks on the send
		done := make(chan struct{})
		go b.rateSchedulerShards(ctx, time.Now(), slots, done, k)
		time.Sleep(50 * time.Millisecond)
		cancel()
		select {
		case <-done:
		case <-time.After(3 * time.Second):
			t.Fatalf("k=%d: scheduler did not stop after cancel", k)
		}
		if n := len(drain(t, slots)); n != 1 {
			t.Errorf("k=%d: %d slots left in a channel of capacity 1 that was full, want 1", k, n)
		}
	}
}

// TestRatedRunReportsSchedulerShards: the Result says how many goroutines
// scheduled the run (1 at a low rate, the configured count when forced), and is
// silent in saturation mode.
func TestRatedRunReportsSchedulerShards(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("OK"))
	}))
	defer srv.Close()
	for _, c := range []struct{ shards, want int }{{0, 1}, {1, 1}, {3, 3}} {
		b, err := New(Config{URL: srv.URL, Method: "GET", Duration: time.Second, Connections: 4, Workers: 4, Rate: 400, SchedulerShards: c.shards})
		if err != nil {
			t.Fatal(err)
		}
		res, err := b.Run(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if res.SchedulerShards != c.want {
			t.Errorf("SchedulerShards = %d for configured %d, want %d", res.SchedulerShards, c.shards, c.want)
		}
		if res.RequestsPerSec < 250 || res.RequestsPerSec > 550 {
			t.Errorf("configured %d: RequestsPerSec = %.0f, want about 400", c.shards, res.RequestsPerSec)
		}
	}
	b, err := New(Config{URL: srv.URL, Method: "GET", Duration: time.Second, Connections: 2, Workers: 2})
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.SchedulerShards != 0 {
		t.Errorf("saturation mode SchedulerShards = %d, want 0", res.SchedulerShards)
	}
}
