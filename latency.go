package loadgen

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

const defaultMaxSamples = 1000000 // 1M samples = ~8MB memory

// defaultFlushInterval controls how often local counters are flushed to atomics.
// Batching reduces ARM64 atomic overhead (LDAXR/STLXR with memory barriers)
// from 2 per request to 2 per 256 requests. H2 uses a lower value (16) to
// avoid timeseries oscillation caused by correlated flush bursts at low per-worker rps.
const defaultFlushInterval int64 = 256

// HdrHistogram precision parameters — covers 1µs through 30s with 0.1% precision.
const (
	hdrLowestTrackable    int64 = 1
	hdrHighestTrackable   int64 = 30 * int64(time.Second) // 30s in ns
	hdrSignificantDigits  int   = 3
	hdrCorrectedThreshold int64 = 0 // 0 disables coordinated-omission correction at the histogram layer
)

// newHistogram returns a fresh hdrhistogram.Histogram tuned for HTTP latency.
// 1ns..30s with 3 significant digits — base buffer is ~37KB.
func newHistogram() *hdrhistogram.Histogram {
	return hdrhistogram.New(hdrLowestTrackable, hdrHighestTrackable, hdrSignificantDigits)
}

// latencyShard is a per-worker latency accumulator. Single-writer, no lock needed
// for samples/sum/count/min/max. The requests/bytesRead counters use atomics so
// that Totals() can be safely called concurrently (e.g., for timeseries snapshots).
//
// To minimize atomic overhead on ARM64, workers accumulate into plain localReqs/
// localBytes counters and flush to the atomic counters every flushInterval requests.
//
// Each shard owns its own HdrHistogram so RecordValue is contention-free in the
// hot path. Shards are merged at finalization to compute global percentiles and
// produce the V2-compressed histogram payload.
type latencyShard struct {
	hist          *hdrhistogram.Histogram
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

	// histWindow accumulates latencies for the current 1-second timeseries
	// interval only. Unlike the cumulative hist (lock-free single-writer), it is
	// read+reset by the ticker goroutine concurrently with the worker writer, so
	// it needs windowMu. The cumulative hot path stays untouched: windowMu
	// guards histWindow and nothing else.
	windowMu   sync.Mutex
	histWindow *hdrhistogram.Histogram
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
			hist:          newHistogram(),
			histWindow:    newHistogram(),
			samples:       make([]time.Duration, 0, perShard),
			min:           time.Hour,
			maxSamples:    perShard,
			flushInterval: flushInterval,
		}
	}
	return &ShardedLatencyRecorder{shards: shards}
}

// recordHist clamps to the histogram's tracked range and records. Out-of-range
// samples (e.g. >30s) are clipped to the highest trackable value rather than
// dropped, so the tail mass remains visible.
func recordHist(h *hdrhistogram.Histogram, d time.Duration) {
	v := int64(d)
	if v < hdrLowestTrackable {
		v = hdrLowestTrackable
	} else if v > hdrHighestTrackable {
		v = hdrHighestTrackable
	}
	_ = h.RecordValue(v)
}

// recordWindow records d into the shard's windowed histogram under windowMu so
// the ticker goroutine's SnapshotWindowP99Ms read+reset cannot race the worker
// writer. The cumulative hist is recorded separately and remains lock-free.
func (sh *latencyShard) recordWindow(d time.Duration) {
	sh.windowMu.Lock()
	recordHist(sh.histWindow, d)
	sh.windowMu.Unlock()
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
	recordHist(sh.hist, d)
	sh.recordWindow(d)

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
	recordHist(sh.hist, d)
	sh.recordWindow(d)

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

// SnapshotWindowP99Ms merges every shard's windowed histogram, reads the P99
// (in milliseconds), then resets each windowed histogram so the next 1-second
// interval starts fresh. Returns 0 when no samples were recorded in the window.
//
// Called from the timeseries ticker goroutine concurrently with worker writers.
// Each shard's windowed histogram is read and reset under that shard's windowMu,
// the same lock the hot path takes for its windowed RecordValue. The cumulative
// hist, Totals(), MergedHistogram(), and Percentiles() are not touched.
func (s *ShardedLatencyRecorder) SnapshotWindowP99Ms() float64 {
	merged := newHistogram()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.windowMu.Lock()
		merged.Merge(sh.histWindow)
		sh.histWindow.Reset()
		sh.windowMu.Unlock()
	}
	if merged.TotalCount() == 0 {
		return 0
	}
	return float64(merged.ValueAtQuantile(99)) / 1e6
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
		sh.hist.Reset()
		sh.windowMu.Lock()
		sh.histWindow.Reset()
		sh.windowMu.Unlock()
	}
}

// MergedHistogram returns a single Histogram containing the merged samples
// from every shard. Caller owns the result.
func (s *ShardedLatencyRecorder) MergedHistogram() *hdrhistogram.Histogram {
	merged := newHistogram()
	for i := range s.shards {
		merged.Merge(s.shards[i].hist)
	}
	return merged
}

// EncodeHistogram returns the V2-compressed encoding of the merged histogram.
// Empty payload when no samples were recorded.
func (s *ShardedLatencyRecorder) EncodeHistogram() ([]byte, error) {
	merged := s.MergedHistogram()
	if merged.TotalCount() == 0 {
		return nil, nil
	}
	return merged.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
}

// DecodeHistogram is a thin convenience wrapper around hdrhistogram.Decode
// for consumers that only depend on the loadgen module.
func DecodeHistogram(b []byte) (*hdrhistogram.Histogram, error) {
	return hdrhistogram.Decode(b)
}

// percentilesFromHistogram derives the legacy Percentiles struct from a
// merged HdrHistogram. The histogram tracks values in nanoseconds; the
// returned values are time.Duration. Returns the zero value when no samples
// have been recorded.
func percentilesFromHistogram(h *hdrhistogram.Histogram) Percentiles {
	if h == nil || h.TotalCount() == 0 {
		return Percentiles{}
	}
	mean := time.Duration(h.Mean())
	return Percentiles{
		Avg:   mean,
		Min:   time.Duration(h.Min()),
		Max:   time.Duration(h.Max()),
		P50:   time.Duration(h.ValueAtQuantile(50)),
		P75:   time.Duration(h.ValueAtQuantile(75)),
		P90:   time.Duration(h.ValueAtQuantile(90)),
		P99:   time.Duration(h.ValueAtQuantile(99)),
		P999:  time.Duration(h.ValueAtQuantile(99.9)),
		P9999: time.Duration(h.ValueAtQuantile(99.99)),
	}
}

// Percentiles merges all shards and calculates latency percentiles.
// Called once at the end of the benchmark — not on the hot path.
//
// Computation now flows through the merged HdrHistogram so percentiles are
// consistent with the V2-compressed payload exported on Result.Histogram.
// The legacy ring-buffer slice path is kept as a fallback for cases where
// HdrHistogram has not yet observed any value (count==0).
func (s *ShardedLatencyRecorder) Percentiles() Percentiles {
	merged := s.MergedHistogram()
	if merged.TotalCount() > 0 {
		return percentilesFromHistogram(merged)
	}

	// Legacy fallback path — preserved because tests inspect samples even
	// when zero values were recorded via RecordValue (the histogram does
	// not allow values < hdrLowestTrackable, but ring buffer does).
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
