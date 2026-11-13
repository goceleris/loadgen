package loadgen

import (
	"context"
	"net/http"
	"sort"
	"testing"
	"time"
)

func TestParseSchedMode(t *testing.T) {
	ok := map[string]schedMode{"": {}, "off": {}, "0": {}, "self": {self: true}, "shard:4": {shards: 4}, "self, shard:2": {self: true, shards: 2}}
	for in, want := range ok {
		got, err := parseSchedMode(in)
		if err != nil || got != want {
			t.Errorf("parseSchedMode(%q) = %+v, %v; want %+v", in, got, err, want)
		}
	}
	for _, bad := range []string{"shard", "shard:1", "shard:17", "shard:x", "selff", "batch"} {
		if _, err := parseSchedMode(bad); err == nil {
			t.Errorf("parseSchedMode(%q): want an error", bad)
		}
	}
}

func TestNewRejectsBadSched(t *testing.T) {
	t.Setenv(SchedEnv, "shrad:4")
	if _, err := New(Config{URL: "http://127.0.0.1:1/", Duration: time.Second, Connections: 1, Workers: 1}); err == nil {
		t.Error("New with a bad LOADGEN_SCHED: want an error")
	}
}

// TestShardedSchedulerSameSchedule: K shard goroutines emit exactly the same set
// of intended times as the single scheduler would (start + i*period, i = 1..n).
func TestShardedSchedulerSameSchedule(t *testing.T) {
	for _, k := range []int{1, 2, 4, 7} {
		b := &Benchmarker{config: Config{Rate: 20000}}
		b.running.Store(true)
		ctx, cancel := context.WithCancel(context.Background())
		slots := make(chan time.Time, 1<<16)
		done := make(chan struct{})
		start := time.Now()
		go b.rateSchedulerShards(ctx, start, slots, done, schedMode{shards: k})
		time.Sleep(300 * time.Millisecond)
		b.running.Store(false)
		cancel()
		<-done
		var got []time.Time
		for in := range slots {
			got = append(got, in)
		}
		if len(got) < 3000 {
			t.Fatalf("k=%d: only %d slots in 300ms at 20000/s", k, len(got))
		}
		sort.Slice(got, func(i, j int) bool { return got[i].Before(got[j]) })
		period := time.Second / 20000
		// the first n of the sorted set must be exactly start+period*(1..n) with no gap
		// except possibly at the tail (a shard stopped a few ticks before another).
		n := len(got)
		for i := 0; i < n; i++ {
			want := start.Add(period * time.Duration(i+1))
			if !got[i].Equal(want) {
				// tolerate a ragged tail of at most k slots
				if i >= n-k {
					break
				}
				t.Fatalf("k=%d: slot %d = %v want %v", k, i, got[i].Sub(start), want.Sub(start))
			}
		}
	}
}

// TestSchedSelfBlock: with LOADGEN_SCHED=self,shard:2 and the stamps on, the result carries
// scheduler.self with ticks that match the scheduler stats and sends timed.
func TestSchedSelfBlock(t *testing.T) {
	t.Setenv(SchedEnv, "self,shard:2")
	t.Setenv(ClientStampsEnv, "1")
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	b, err := New(stampsH2Cfg(host, port, 4000, 1500*time.Millisecond, 8))
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cs := res.ClientStamps
	if cs == nil || cs.Scheduler.Self == nil {
		t.Fatal("no scheduler.self block")
	}
	self := cs.Scheduler.Self
	if self.Shards != 2 {
		t.Errorf("shards = %d", self.Shards)
	}
	var ticks, timers int64
	for _, s := range self.PerSecond {
		ticks += s.Ticks
		timers += s.Timers
	}
	if ticks < 3000 || ticks != cs.Scheduler.Ticks {
		t.Errorf("self ticks %d vs scheduler ticks %d", ticks, cs.Scheduler.Ticks)
	}
	if timers == 0 {
		t.Error("no timers counted")
	}
}

// TestSchedOffHasNoSelf: without the variable the block is absent.
func TestSchedOffHasNoSelf(t *testing.T) {
	t.Setenv(SchedEnv, "")
	t.Setenv(ClientStampsEnv, "1")
	host, port := startH2CHandler(t, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) }))
	b, err := New(stampsH2Cfg(host, port, 2000, time.Second, 4))
	if err != nil {
		t.Fatal(err)
	}
	res, err := b.Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if res.ClientStamps == nil || res.ClientStamps.Scheduler.Self != nil {
		t.Fatal("self block present with the variable unset")
	}
}
