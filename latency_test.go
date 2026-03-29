package loadgen

import (
	"sync"
	"testing"
	"time"
)

func TestShardedLatencyRecorder(t *testing.T) {
	r := NewShardedLatencyRecorder(4, defaultFlushInterval)

	// Record samples across shards
	r.RecordShard(0, 10*time.Millisecond)
	r.RecordShard(1, 20*time.Millisecond)
	r.RecordShard(2, 30*time.Millisecond)
	r.RecordShard(3, 40*time.Millisecond)

	p := r.Percentiles()
	if p.Min != 10*time.Millisecond {
		t.Errorf("min = %v, want 10ms", p.Min)
	}
	if p.Max != 40*time.Millisecond {
		t.Errorf("max = %v, want 40ms", p.Max)
	}
	if p.Avg != 25*time.Millisecond {
		t.Errorf("avg = %v, want 25ms", p.Avg)
	}
}

func TestRecordSuccessAndTotals(t *testing.T) {
	r := NewShardedLatencyRecorder(4, defaultFlushInterval)

	r.RecordSuccess(0, 10*time.Millisecond, 100)
	r.RecordSuccess(1, 20*time.Millisecond, 200)
	r.RecordSuccess(2, 30*time.Millisecond, 300)
	r.RecordSuccess(3, 40*time.Millisecond, 400)

	r.FlushLocal() // flush batched local counters before reading totals
	reqs, bytes := r.Totals()
	if reqs != 4 {
		t.Errorf("requests = %d, want 4", reqs)
	}
	if bytes != 1000 {
		t.Errorf("bytesRead = %d, want 1000", bytes)
	}

	// Latency percentiles should still work
	p := r.Percentiles()
	if p.Min != 10*time.Millisecond {
		t.Errorf("min = %v, want 10ms", p.Min)
	}
	if p.Max != 40*time.Millisecond {
		t.Errorf("max = %v, want 40ms", p.Max)
	}
}

func TestShardedLatencyRecorderReset(t *testing.T) {
	r := NewShardedLatencyRecorder(2, defaultFlushInterval)
	r.RecordShard(0, 10*time.Millisecond)
	r.RecordShard(1, 20*time.Millisecond)

	r.Reset()
	p := r.Percentiles()
	if p != (Percentiles{}) {
		t.Errorf("expected zero Percentiles after reset, got %+v", p)
	}

	// Record after reset should work
	r.RecordShard(0, 5*time.Millisecond)
	p = r.Percentiles()
	if p.Min != 5*time.Millisecond {
		t.Errorf("after reset: min = %v, want 5ms", p.Min)
	}
}

func TestTotalsResetClearsCounters(t *testing.T) {
	r := NewShardedLatencyRecorder(2, defaultFlushInterval)
	r.RecordSuccess(0, 10*time.Millisecond, 100)
	r.RecordSuccess(1, 20*time.Millisecond, 200)

	r.Reset()
	reqs, bytes := r.Totals()
	if reqs != 0 || bytes != 0 {
		t.Errorf("after reset: requests=%d bytesRead=%d, want 0/0", reqs, bytes)
	}

	// RecordSuccess after reset should work
	r.RecordSuccess(0, 5*time.Millisecond, 50)
	r.FlushLocal()
	reqs, bytes = r.Totals()
	if reqs != 1 || bytes != 50 {
		t.Errorf("after reset+record: requests=%d bytesRead=%d, want 1/50", reqs, bytes)
	}
}

func TestShardedLatencyRecorderModuloWrap(t *testing.T) {
	// Worker ID larger than shard count should wrap via modulo
	r := NewShardedLatencyRecorder(4, defaultFlushInterval)
	r.RecordShard(7, 10*time.Millisecond)  // 7 % 4 = shard 3
	r.RecordShard(12, 20*time.Millisecond) // 12 % 4 = shard 0

	p := r.Percentiles()
	if p.Min != 10*time.Millisecond || p.Max != 20*time.Millisecond {
		t.Errorf("modulo wrap: min=%v max=%v", p.Min, p.Max)
	}
}

// BenchmarkShardedLatencyRecorder measures throughput of per-shard recording
// (the hot path) with concurrent workers.
func BenchmarkShardedLatencyRecorder(b *testing.B) {
	for _, numWorkers := range []int{1, 8, 64, 256, 1024} {
		b.Run(itoa(numWorkers)+"workers", func(b *testing.B) {
			r := NewShardedLatencyRecorder(numWorkers, defaultFlushInterval)
			var wg sync.WaitGroup
			perWorker := b.N / numWorkers
			if perWorker < 1 {
				perWorker = 1
			}

			b.ResetTimer()
			for w := range numWorkers {
				wg.Add(1)
				go func(id int) {
					defer wg.Done()
					for range perWorker {
						r.RecordShard(id, 100*time.Microsecond)
					}
				}(w)
			}
			wg.Wait()
		})
	}
}
