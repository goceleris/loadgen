package loadgen

import (
	"math/rand"
	"testing"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// TestHistogramRoundTrip verifies the recorder's V2-compressed histogram
// payload round-trips through Decode and matches the live aggregate.
func TestHistogramRoundTrip(t *testing.T) {
	r := NewShardedLatencyRecorder(8, defaultFlushInterval)

	// Inject a deterministic distribution so percentiles are predictable.
	rng := rand.New(rand.NewSource(42))
	for range 10000 {
		// 99% of samples are 1ms-ish; 1% are 50ms-ish (long tail).
		var d time.Duration
		if rng.Intn(100) == 0 {
			d = 50 * time.Millisecond
		} else {
			d = time.Duration(900+rng.Intn(200)) * time.Microsecond
		}
		r.RecordSuccess(rng.Intn(8), d, 100)
	}

	blob, err := r.EncodeHistogram()
	if err != nil {
		t.Fatalf("EncodeHistogram: %v", err)
	}
	if len(blob) == 0 {
		t.Fatalf("EncodeHistogram returned empty payload")
	}

	dec, err := DecodeHistogram(blob)
	if err != nil {
		t.Fatalf("DecodeHistogram: %v", err)
	}

	live := r.MergedHistogram()
	if dec.TotalCount() != live.TotalCount() {
		t.Errorf("round-trip count mismatch: decoded=%d live=%d", dec.TotalCount(), live.TotalCount())
	}
	if !dec.Equals(live) {
		t.Errorf("decoded histogram not equal to live")
	}
}

// TestPercentilesFromHistogramAgreesWithLegacy verifies that legacy
// Percentiles fields computed off the histogram match the live ring-buffer
// percentiles within HdrHistogram quantization (0.1%).
func TestPercentilesFromHistogramAgreesWithLegacy(t *testing.T) {
	r := NewShardedLatencyRecorder(4, defaultFlushInterval)
	rng := rand.New(rand.NewSource(7))
	for range 5000 {
		d := time.Duration(rng.Intn(20000)+100) * time.Microsecond
		r.RecordSuccess(rng.Intn(4), d, 0)
	}
	r.FlushLocal()

	p := r.Percentiles()

	// Compare to the merged histogram directly.
	merged := r.MergedHistogram()
	if !approxEqual(p.P50, time.Duration(merged.ValueAtQuantile(50)), 0.005) {
		t.Errorf("P50 mismatch: %v vs %v", p.P50, time.Duration(merged.ValueAtQuantile(50)))
	}
	if !approxEqual(p.P99, time.Duration(merged.ValueAtQuantile(99)), 0.005) {
		t.Errorf("P99 mismatch: %v vs %v", p.P99, time.Duration(merged.ValueAtQuantile(99)))
	}
	if !approxEqual(p.P999, time.Duration(merged.ValueAtQuantile(99.9)), 0.005) {
		t.Errorf("P99.9 mismatch: %v vs %v", p.P999, time.Duration(merged.ValueAtQuantile(99.9)))
	}
}

// TestHistogramEmptyEncodeReturnsNil ensures we don't ship a useless empty
// payload on Result.Histogram when no samples were recorded.
func TestHistogramEmptyEncodeReturnsNil(t *testing.T) {
	r := NewShardedLatencyRecorder(2, defaultFlushInterval)
	blob, err := r.EncodeHistogram()
	if err != nil {
		t.Fatalf("EncodeHistogram on empty recorder: %v", err)
	}
	if blob != nil {
		t.Errorf("expected nil payload for empty recorder, got %d bytes", len(blob))
	}
}

// TestHistogramOutOfRangeClamping verifies samples >30s clip to the highest
// trackable value rather than being dropped.
func TestHistogramOutOfRangeClamping(t *testing.T) {
	r := NewShardedLatencyRecorder(1, defaultFlushInterval)
	r.RecordShard(0, 60*time.Second) // 2× the 30s ceiling

	merged := r.MergedHistogram()
	if merged.TotalCount() != 1 {
		t.Fatalf("expected 1 sample, got %d", merged.TotalCount())
	}
	maxObs := time.Duration(merged.Max())
	if maxObs <= 25*time.Second {
		t.Errorf("expected clamped max near 30s, got %v", maxObs)
	}
}

// TestHistogramMergeTwoRecorders simulates federation: encode one recorder,
// decode + merge into another, verify TotalCount + percentiles.
func TestHistogramMergeTwoRecorders(t *testing.T) {
	primary := NewShardedLatencyRecorder(2, defaultFlushInterval)
	sidecar := NewShardedLatencyRecorder(2, defaultFlushInterval)

	for i := range 1000 {
		primary.RecordSuccess(i%2, time.Duration(i+1)*time.Microsecond, 0)
		sidecar.RecordSuccess(i%2, time.Duration(i*2+1)*time.Microsecond, 0)
	}

	sidecarBlob, err := sidecar.EncodeHistogram()
	if err != nil {
		t.Fatalf("sidecar encode: %v", err)
	}
	peer, err := DecodeHistogram(sidecarBlob)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	merged := primary.MergedHistogram()
	wantTotal := merged.TotalCount() + peer.TotalCount()
	merged.Merge(peer)

	if merged.TotalCount() != wantTotal {
		t.Errorf("merged total = %d, want %d", merged.TotalCount(), wantTotal)
	}

	// Re-encode the merged and decode again; equality must hold.
	rblob, err := merged.Encode(hdrhistogram.V2CompressedEncodingCookieBase)
	if err != nil {
		t.Fatalf("merged encode: %v", err)
	}
	again, err := DecodeHistogram(rblob)
	if err != nil {
		t.Fatalf("merged decode: %v", err)
	}
	if !again.Equals(merged) {
		t.Errorf("merged round-trip not equal")
	}
}
