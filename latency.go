package loadgen

import (
	"slices"
	"sync/atomic"
	"time"
)

const defaultMaxSamples = 1000000 // 1M samples = ~8MB memory

// defaultFlushInterval controls how often local counters are flushed to atomics.
// Batching reduces ARM64 atomic overhead (LDAXR/STLXR with memory barriers)
// from 2 per request to 2 per 256 requests. H2 uses a lower value (16) to
// avoid timeseries oscillation caused by correlated flush bursts at low per-worker rps.
const defaultFlushInterval int64 = 256

// latencyShard is a per-worker latency accumulator. Single-writer, no lock needed
// for samples/sum/count/min/max. The requests/bytesRead counters use atomics so
// that Totals() can be safely called concurrently (e.g., for timeseries snapshots).
//
// To minimize atomic overhead on ARM64, workers accumulate into plain localReqs/
// localBytes counters and flush to the atomic counters every flushInterval requests.
type latencyShard struct {
	samples       []time.Duration
	sum           time.Duration
	count         int64
	min           time.Duration
	max           time.Duration
	maxSamples    int
	flushInterval int64 // how often to flush local counters to atomics

	// Local (plain) counters — single-writer, flushed to atomics every flushInterval.
	localReqs  int64
	localBytes int64

	// Per-shard request/bytes counters — atomic for safe concurrent reads by Totals().
	requests  atomic.Int64
	bytesRead atomic.Int64
}

// ShardedLatencyRecorder eliminates lock contention by giving each worker its own shard.
// RecordShard is called from the hot path with zero synchronization.
// Shards are merged only once at the end for percentile calculation.
type ShardedLatencyRecorder struct {
	shards []latencyShard
}

// NewShardedLatencyRecorder creates a recorder with one shard per worker.
// flushInterval controls how often per-shard local counters are flushed to atomics.
// Use defaultFlushInterval (256) for H1, 16 for H2.
func NewShardedLatencyRecorder(numShards int, flushInterval int64) *ShardedLatencyRecorder {
	if numShards < 1 {
		numShards = 1
	}
	if flushInterval <= 0 {
		flushInterval = defaultFlushInterval
	}
	perShard := defaultMaxSamples / numShards
	if perShard < 1024 {
		perShard = 1024
	}
	shards := make([]latencyShard, numShards)
	for i := range shards {
		shards[i] = latencyShard{
			samples:       make([]time.Duration, 0, perShard),
			min:           time.Hour,
			maxSamples:    perShard,
			flushInterval: flushInterval,
		}
	}
	return &ShardedLatencyRecorder{shards: shards}
}

// RecordShard adds a latency sample to the worker's shard. No lock, no atomic.
func (s *ShardedLatencyRecorder) RecordShard(shardID int, d time.Duration) {
	sh := &s.shards[shardID%len(s.shards)]
	sh.sum += d
	sh.count++
	if d < sh.min {
		sh.min = d
	}
	if d > sh.max {
		sh.max = d
	}

	if len(sh.samples) < sh.maxSamples {
		sh.samples = append(sh.samples, d)
	} else {
		// Reservoir sampling per shard
		if int(sh.count%int64(sh.maxSamples)) == 0 {
			idx := int(sh.count/int64(sh.maxSamples)) % sh.maxSamples
			sh.samples[idx] = d
		}
	}
}

// RecordSuccess records a successful request: latency + request/bytes counters.
// Single call replaces separate RecordShard + atomic counter increments in the hot path.
// Counters are batched locally and flushed to atomics every flushInterval requests
// to avoid expensive memory barriers on ARM64.
func (s *ShardedLatencyRecorder) RecordSuccess(shardID int, d time.Duration, bytesRead int) {
	sh := &s.shards[shardID%len(s.shards)]
	sh.localReqs++
	sh.localBytes += int64(bytesRead)
	if sh.localReqs >= sh.flushInterval {
		sh.requests.Add(sh.localReqs)
		sh.bytesRead.Add(sh.localBytes)
		sh.localReqs = 0
		sh.localBytes = 0
	}
	sh.sum += d
	sh.count++
	if d < sh.min {
		sh.min = d
	}
	if d > sh.max {
		sh.max = d
	}

	if len(sh.samples) < sh.maxSamples {
		sh.samples = append(sh.samples, d)
	} else {
		if int(sh.count%int64(sh.maxSamples)) == 0 {
			idx := int(sh.count/int64(sh.maxSamples)) % sh.maxSamples
			sh.samples[idx] = d
		}
	}
}

// Totals sums request and bytes counters across all shards.
// Safe to call concurrently with RecordSuccess (reads only atomic counters).
// Mid-benchmark reads may lag by up to flushInterval requests per shard.
// Call FlushLocal() after workers stop for exact totals.
func (s *ShardedLatencyRecorder) Totals() (requests, bytesRead int64) {
	for i := range s.shards {
		requests += s.shards[i].requests.Load()
		bytesRead += s.shards[i].bytesRead.Load()
	}
	return
}

// FlushLocal flushes unflushed local counters to atomic counters.
// Must be called after all workers have stopped (single-threaded).
func (s *ShardedLatencyRecorder) FlushLocal() {
	for i := range s.shards {
		sh := &s.shards[i]
		if sh.localReqs > 0 {
			sh.requests.Add(sh.localReqs)
			sh.bytesRead.Add(sh.localBytes)
			sh.localReqs = 0
			sh.localBytes = 0
		}
	}
}

// Reset clears all shards (called between warmup and main benchmark).
func (s *ShardedLatencyRecorder) Reset() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.samples = sh.samples[:0]
		sh.sum = 0
		sh.count = 0
		sh.min = time.Hour
		sh.max = 0
		sh.localReqs = 0
		sh.localBytes = 0
		sh.requests.Store(0)
		sh.bytesRead.Store(0)
	}
}

// Percentiles merges all shards and calculates latency percentiles.
// Called once at the end of the benchmark — not on the hot path.
func (s *ShardedLatencyRecorder) Percentiles() Percentiles {
	// Merge stats across shards
	var totalSum time.Duration
	var totalCount int64
	globalMin := time.Hour
	var globalMax time.Duration
	totalSamples := 0

	for i := range s.shards {
		sh := &s.shards[i]
		totalSum += sh.sum
		totalCount += sh.count
		if sh.count > 0 && sh.min < globalMin {
			globalMin = sh.min
		}
		if sh.max > globalMax {
			globalMax = sh.max
		}
		totalSamples += len(sh.samples)
	}

	if totalCount == 0 {
		return Percentiles{}
	}

	// Merge all sample slices
	sorted := make([]time.Duration, 0, totalSamples)
	for i := range s.shards {
		sorted = append(sorted, s.shards[i].samples...)
	}
	slices.Sort(sorted)

	n := len(sorted)
	if n == 0 {
		return Percentiles{}
	}
	avg := totalSum / time.Duration(totalCount)

	return Percentiles{
		Avg:   avg,
		Min:   globalMin,
		Max:   globalMax,
		P50:   sorted[percentileIndex(n, 50)],
		P75:   sorted[percentileIndex(n, 75)],
		P90:   sorted[percentileIndex(n, 90)],
		P99:   sorted[percentileIndex(n, 99)],
		P999:  sorted[percentileIndex(n, 99.9)],
		P9999: sorted[percentileIndex(n, 99.99)],
	}
}

func percentileIndex(n int, percentile float64) int {
	idx := int(float64(n) * percentile / 100)
	if idx >= n {
		idx = n - 1
	}
	if idx < 0 {
		idx = 0
	}
	return idx
}
