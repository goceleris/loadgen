package loadgen

import (
	"context"
	"sync"
	"time"
)

// SchedulerShardRate is the highest intended-dispatch rate one scheduler
// goroutine is asked to carry. A single goroutine pays a wake-up of a parked
// worker on every tick (about 3.5 us measured on the amd64 bench client), so
// above roughly 280k ticks/s it cannot keep its own schedule: the lateness
// grows linearly and shows up as tail latency that the server did not cause
// (goceleris/celeris#757, D757-SCHED). 80k/s leaves a 3.5x margin and is the
// per-goroutine rate the verifying cluster run used (300321 rps over 4).
const SchedulerShardRate = 80000

// SchedulerShardMax bounds the number of scheduler goroutines.
const SchedulerShardMax = 32

// schedulerShards returns how many scheduler goroutines a rated run uses.
// configured == 0 selects the automatic count, ceil(rate / SchedulerShardRate)
// (1 at and below 80k rps, so low-rate runs are exactly the old single
// goroutine); configured >= 1 forces that count (1 is the legacy scheduler).
func schedulerShards(rate float64, configured int) int {
	k := configured
	if k <= 0 {
		k = int((rate + SchedulerShardRate - 1) / SchedulerShardRate)
	}
	if k < 1 {
		k = 1
	}
	if k > SchedulerShardMax {
		k = SchedulerShardMax
	}
	return k
}

// rateSchedulerShards runs k scheduler goroutines over the one slot channel
// and closes it when all have returned. Goroutine j (0-based) emits the
// intended times start + (j+1)*period + i*k*period for i = 0, 1, 2, ...: the
// union over j is exactly the single-goroutine schedule start + n*period,
// n >= 1, so no intended time is added, skipped or moved. As in the single
// goroutine version a late wake-up never skips a slot (coordinated-omission
// semantics): the intended time is enqueued and the lateness is measured by
// the worker. Slots reach the channel in an order that can differ from the
// intended order by up to k-1 periods; nothing downstream depends on that
// order (a worker measures time.Since(intended) of the slot it holds).
func (b *Benchmarker) rateSchedulerShards(ctx context.Context, start time.Time, slots chan<- time.Time, done chan<- struct{}, k int) {
	defer close(done)
	period := time.Duration(float64(time.Second) / b.config.Rate)
	if period <= 0 {
		closeChanIfOpen(slots)
		return
	}
	var wg sync.WaitGroup
	for j := 0; j < k; j++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			b.schedShard(ctx, start, slots, period, j, k)
		}()
	}
	wg.Wait()
	// All senders have returned: the one close.
	closeChanIfOpen(slots)
}

func (b *Benchmarker) schedShard(ctx context.Context, start time.Time, slots chan<- time.Time, period time.Duration, j, k int) {
	stride := period * time.Duration(k)
	next := start.Add(period * time.Duration(j+1))
	for b.running.Load() {
		select {
		case <-ctx.Done():
			return
		default:
		}
		if now := time.Now(); now.Before(next) {
			t := time.NewTimer(next.Sub(now))
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}
		select {
		case <-ctx.Done():
			return
		case slots <- next:
		}
		next = next.Add(stride)
	}
}
