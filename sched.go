package loadgen

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// SchedEnv selects the rate-scheduler diagnostics of a rated run without a code
// change (issue #757 lane D757-SCHED). Default (unset, "", "off"): the single
// scheduler goroutine, byte-for-byte the path of every earlier release.
// A comma-separated list of: "self" - the scheduler times its own work
// (per second: ticks, timers created, ns parked in timers, ns inside the slot
// send, sends slower than 1us) and the block is written as
// client_stamps.scheduler.self (needs the client stamps on); "shard:K" (K 2..16) -
// K scheduler goroutines emit the SAME intended times (goroutine j emits the
// ticks start+(i*K+j+1)*period) into the one slot channel, so the offered
// schedule is unchanged but the one-goroutine per-tick ceiling is lifted.
// Anything else is an error from New: a typo must not turn a pre-registered
// measurement off silently.
const SchedEnv = "LOADGEN_SCHED"

// schedMode is the parsed SchedEnv.
type schedMode struct {
	self   bool
	shards int // 0 or 1: the original single scheduler
}

func parseSchedMode(v string) (schedMode, error) {
	var m schedMode
	v = strings.ToLower(strings.TrimSpace(v))
	if v == "" || v == "off" || v == "0" {
		return m, nil
	}
	for _, p := range strings.Split(v, ",") {
		p = strings.TrimSpace(p)
		switch {
		case p == "self":
			m.self = true
		case strings.HasPrefix(p, "shard:"):
			k, err := strconv.Atoi(strings.TrimPrefix(p, "shard:"))
			if err != nil || k < 2 || k > 16 {
				return m, fmt.Errorf("%s=%q: shard:K needs K in 2..16", SchedEnv, v)
			}
			m.shards = k
		default:
			return m, fmt.Errorf("%s=%q: unknown item %q (want self, shard:K)", SchedEnv, v, p)
		}
	}
	return m, nil
}

// SchedSelfSecond is one second of one run's scheduler self-timing, summed over
// the scheduler goroutines.
type SchedSelfSecond struct {
	Ticks       int64 `json:"ticks"`
	Timers      int64 `json:"timers"`
	TimerParkNS int64 `json:"timer_park_ns"`
	SendNS      int64 `json:"send_ns"`
	SendMaxNS   int64 `json:"send_max_ns"`
	SendOver1us int64 `json:"send_over_1us"`
}

// SchedSelf is client_stamps.scheduler.self.
type SchedSelf struct {
	Mode      string            `json:"mode"`
	Shards    int               `json:"shards"`
	PerSecond []SchedSelfSecond `json:"per_second"`
}

// schedLocal is one scheduler goroutine's private counters.
type schedLocal struct {
	sec []SchedSelfSecond
}

func (l *schedLocal) at(i int) *SchedSelfSecond {
	if i < 0 {
		i = 0
	}
	if i >= len(l.sec) {
		i = len(l.sec) - 1
	}
	return &l.sec[i]
}

// rateSchedulerShards runs `shards` scheduler goroutines (shards >= 1) over the one
// slot channel and closes it when all are done. With shards == 1 and self on it
// is the original loop plus the timing; with shards > 1 goroutine j emits ticks
// j+1, j+1+K, ... of the same arithmetic schedule.
func (b *Benchmarker) rateSchedulerShards(ctx context.Context, start time.Time, slots chan<- time.Time, done chan<- struct{}, mode schedMode) {
	defer close(done)
	period := time.Duration(float64(time.Second) / b.config.Rate)
	if period <= 0 {
		closeChanIfOpen(slots)
		return
	}
	k := mode.shards
	if k < 1 {
		k = 1
	}
	maxSec := 120
	if b.stamps != nil {
		maxSec = b.stamps.cfg.MaxSeconds
	}
	locals := make([]*schedLocal, k)
	var tickMu sync.Mutex // stampSet.tick is single-writer; shards share it
	var wg sync.WaitGroup
	for j := 0; j < k; j++ {
		l := &schedLocal{sec: make([]SchedSelfSecond, maxSec)}
		locals[j] = l
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.schedShard(ctx, start, slots, period, j, k, mode.self, l, &tickMu)
		}()
	}
	wg.Wait()
	closeChanIfOpen(slots)
	if mode.self && b.stamps != nil {
		self := &SchedSelf{Mode: os.Getenv(SchedEnv), Shards: k, PerSecond: make([]SchedSelfSecond, maxSec)}
		for _, l := range locals {
			for i, s := range l.sec {
				d := &self.PerSecond[i]
				d.Ticks += s.Ticks
				d.Timers += s.Timers
				d.TimerParkNS += s.TimerParkNS
				d.SendNS += s.SendNS
				d.SendOver1us += s.SendOver1us
				if s.SendMaxNS > d.SendMaxNS {
					d.SendMaxNS = s.SendMaxNS
				}
			}
		}
		b.stamps.setSchedSelf(self)
	}
}

func (b *Benchmarker) schedShard(ctx context.Context, start time.Time, slots chan<- time.Time, period time.Duration, j, k int, self bool, l *schedLocal, tickMu *sync.Mutex) {
	stamps := b.stamps
	stride := period * time.Duration(k)
	next := start.Add(period * time.Duration(j+1))
	for b.running.Load() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		now := time.Now()
		if now.Before(next) {
			t := time.NewTimer(next.Sub(now))
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
			if self {
				now2 := time.Now()
				s := l.at(int(next.Sub(start) / time.Second))
				s.Timers++
				s.TimerParkNS += int64(now2.Sub(now))
			}
		}
		if stamps != nil {
			if k > 1 {
				tickMu.Lock()
				stamps.tick(next)
				tickMu.Unlock()
			} else {
				stamps.tick(next)
			}
		}
		var t0 time.Time
		if self {
			t0 = time.Now()
		}
		select {
		case <-ctx.Done():
			return
		case slots <- next:
		}
		if self {
			d := int64(time.Since(t0))
			s := l.at(int(next.Sub(start) / time.Second))
			s.Ticks++
			s.SendNS += d
			if d > s.SendMaxNS {
				s.SendMaxNS = d
			}
			if d > 1000 {
				s.SendOver1us++
			}
		}
		next = next.Add(stride)
	}
}
