package loadgen

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"
)

func TestSchedulerShardsCount(t *testing.T) {
	cases := []struct {
		rate       float64
		configured int
		want       int
	}{
		{100, 0, 1},
		{80000, 0, 1},  // at the shard rate: still one goroutine
		{80001, 0, 2},  // just above
		{300321, 0, 4}, // the rate verified on the cluster (D757-SCHED)
		{450481, 0, 6},
		{1e9, 0, SchedulerShardMax},
		{300321, 1, 1}, // explicit single goroutine
		{100, 4, 4},    // explicit count at a low rate
		{100, 1000, SchedulerShardMax},
		{0.5, 0, 1},
	}
	for _, c := range cases {
		if got := schedulerShards(c.rate, c.configured); got != c.want {
			t.Errorf("schedulerShards(%v, %d) = %d, want %d", c.rate, c.configured, got, c.want)
		}
	}
}

func TestNewRejectsNegativeSchedulerShards(t *testing.T) {
	if _, err := New(Config{URL: "http://127.0.0.1:1/", Duration: time.Second, Connections: 1, Workers: 1, Rate: 10, SchedulerShards: -1}); err == nil {
		t.Error("New with SchedulerShards -1: want an error")
	}
}

// TestShardedSchedulerSameSchedule: k scheduler goroutines emit exactly the same
// set of intended times as the single scheduler would (start + n*period, n >= 1),
// and the channel is closed once all are done.
func TestShardedSchedulerSameSchedule(t *testing.T) {
	for _, k := range []int{1, 2, 4, 7} {
		b := &Benchmarker{config: Config{Rate: 20000}}
		b.running.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		slots := make(chan time.Time, 1<<16)
		done := make(chan struct{})
		start := time.Now()
		go b.rateSchedulerShards(ctx, start, slots, done, k)
		time.Sleep(300 * time.Millisecond)
		b.running.Store(false)
		cancel()
		<-done
		var got []time.Time
		for in := range slots { // ends only if the channel was closed
			got = append(got, in)
		}
		if len(got) < 3000 {
			t.Fatalf("k=%d: only %d slots in 300ms at 20000/s", k, len(got))
		}
		sort.Slice(got, func(i, j int) bool { return got[i].Before(got[j]) })
		period := time.Second / 20000
		// The sorted set must be start+period*(1..n) with no gap and no duplicate,
		// except a ragged tail of at most k slots (a shard stopped a few ticks
		// before another).
		n := len(got)
		for i := 0; i < n; i++ {
			want := start.Add(period * time.Duration(i+1))
			if !got[i].Equal(want) {
				if i >= n-k {
					break
				}
				t.Fatalf("k=%d: slot %d = %v want %v", k, i, got[i].Sub(start), want.Sub(start))
			}
		}
	}
}

// TestShardedSchedulerKeepsLateSlots: a shard that wakes late still enqueues the
// intended time (no slot is skipped), so the lateness is visible to the worker.
// The channel is unread for 200 ms; afterwards every intended time up to "now"
// is in it.
func TestShardedSchedulerKeepsLateSlots(t *testing.T) {
	b := &Benchmarker{config: Config{Rate: 5000}}
	b.running.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	slots := make(chan time.Time, 1<<14)
	done := make(chan struct{})
	start := time.Now()
	go b.rateSchedulerShards(ctx, start, slots, done, 3)
	time.Sleep(200 * time.Millisecond)
	b.running.Store(false)
	cancel()
	<-done
	if len(slots) < 900 {
		t.Errorf("%d slots after 200ms at 5000/s, want about 1000", len(slots))
	}
}

// TestShardedSchedulerStopsOnCancel: shards blocked on a full channel return when
// the context is cancelled, and the channel is then closed.
func TestShardedSchedulerStopsOnCancel(t *testing.T) {
	b := &Benchmarker{config: Config{Rate: 100000}}
	b.running.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	slots := make(chan time.Time, 1) // nobody reads: every shard blocks on the send
	done := make(chan struct{})
	go b.rateSchedulerShards(ctx, time.Now(), slots, done, 4)
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("scheduler did not stop after cancel")
	}
	n := 0
	for range slots {
		n++
	}
	if n > 1 {
		t.Errorf("%d slots in a channel of capacity 1", n)
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
