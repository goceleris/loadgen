package loadgen

import (
	"context"
	"math/rand/v2"
	"sync/atomic"
	"testing"
)

// BenchmarkParseMixRatio measures the CLI-time flag parse. Cold path —
// called once per loadgen invocation — but covered so unused allocations
// in the parser surface in benchmem reports.
func BenchmarkParseMixRatio(b *testing.B) {
	const s = "h1:h2:upgrade=4:4:1"
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ParseMixRatio(s); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkDrawMixProto isolates the weighted-draw inner loop. Not on the
// hot path (drawn once per worker at startup), but a cheap guard against
// accidental allocations landing in drawMixProto.
func BenchmarkDrawMixProto(b *testing.B) {
	m := MixRatio{H1: 4, H2: 4, Upgrade: 1}
	r := rand.New(rand.NewPCG(42, 43))
	b.ReportAllocs()
	for b.Loop() {
		_ = drawMixProto(m, r)
	}
}

// BenchmarkMixDispatch measures the per-request overhead of mixClient on
// top of a trivial sub-client. The workerMap cycles 1:1:1 across 12
// workers. noopClient is a zero-cost stub so the benchmark isolates the
// switch + atomic-counter cost.
//
// Target: under 100 ns/op overhead vs BenchmarkBaselineAtomicAdd — mix
// dispatch must never land on the H1/H2 critical path.
func BenchmarkMixDispatch(b *testing.B) {
	mc := &mixClient{
		h1:          noopClient{},
		h2:          noopClient{},
		upgrade:     noopClient{},
		workerMap:   []mixProto{mixProtoH1, mixProtoH2, mixProtoUpgrade, mixProtoH1, mixProtoH2, mixProtoUpgrade, mixProtoH1, mixProtoH2, mixProtoUpgrade, mixProtoH1, mixProtoH2, mixProtoUpgrade},
		subWorkerID: []int{0, 0, 0, 1, 1, 1, 2, 2, 2, 3, 3, 3},
	}
	ctx := context.Background()
	b.ReportAllocs()
	var i int
	for b.Loop() {
		_, _ = mc.DoRequest(ctx, i%12)
		i++
	}
}

// BenchmarkMixDispatchSingle forces a single-slot workerMap so the branch
// predictor hits the same case every iteration. Isolates the counter add
// cost from the switch-dispatch cost.
func BenchmarkMixDispatchSingle(b *testing.B) {
	mc := &mixClient{
		h1:          noopClient{},
		workerMap:   []mixProto{mixProtoH1},
		subWorkerID: []int{0},
	}
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		_, _ = mc.DoRequest(ctx, 0)
	}
}

// BenchmarkBaselineAtomicAdd is the floor for BenchmarkMixDispatchSingle:
// a bare atomic.Int64.Add(1) is the minimum cost per request. If mix
// dispatch > 2× this, the switch is suspect.
func BenchmarkBaselineAtomicAdd(b *testing.B) {
	var n atomic.Int64
	b.ReportAllocs()
	for b.Loop() {
		n.Add(1)
	}
}

// BenchmarkBuildH2CUpgradeRequest measures the per-dial cost of formatting
// the H2C upgrade HTTP/1.1 GET line. Fires once per TCP conn at dial
// time. Any B/op here is a candidate for pre-computation at package init.
func BenchmarkBuildH2CUpgradeRequest(b *testing.B) {
	payload := encodeH2SettingsPayload(h2HandshakeSettings)
	b.ReportAllocs()
	for b.Loop() {
		_ = buildH2CUpgradeRequest("/", "example.com", "8080", "http", payload)
	}
}

// BenchmarkEncodeH2SettingsPayload measures the per-dial SETTINGS payload
// serialisation. The result is byte-identical on every call, so
// non-trivial ns/op or any B/op makes this a package-init candidate.
func BenchmarkEncodeH2SettingsPayload(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		_ = encodeH2SettingsPayload(h2HandshakeSettings)
	}
}

// BenchmarkH2CUpgradeDial measures the full h2c upgrade handshake against
// a real golang.org/x/net/http2/h2c server. One dial per iteration —
// handshake cost, not steady-state request throughput.
//
// Skipped under -race: x/net/http2/hpack race (golang/go#57639) fires on
// SETTINGS_HEADER_TABLE_SIZE=0 in the upgrade path. Real perf runs are
// off -race anyway.
func BenchmarkH2CUpgradeDial(b *testing.B) {
	if raceEnabled {
		b.Skip("skipping under -race: x/net/http2/hpack race (golang/go#57639)")
	}
	host, port, cleanup := startH2CUpgradeServer(b)
	defer cleanup()

	cfg := testH2Cfg("GET", nil, nil, 1, 100)
	cfg.H2CUpgrade = true
	cfg.scheme = "http"

	b.ReportAllocs()
	for b.Loop() {
		client, err := newH2CUpgradeClient(host, port, "/", cfg)
		if err != nil {
			b.Fatalf("dial: %v", err)
		}
		client.Close()
	}
}

// BenchmarkH2CUpgradeRequest measures steady-state request throughput on
// an already-upgraded connection. Should approach the -h2 request
// throughput: anything more than 5% slower means the upgrade path is
// adding cost past the handshake.
func BenchmarkH2CUpgradeRequest(b *testing.B) {
	if raceEnabled {
		b.Skip("skipping under -race: x/net/http2/hpack race (golang/go#57639)")
	}
	host, port, cleanup := startH2CUpgradeServer(b)
	defer cleanup()

	cfg := testH2Cfg("GET", nil, nil, 2, 100)
	cfg.H2CUpgrade = true
	cfg.scheme = "http"

	client, err := newH2CUpgradeClient(host, port, "/", cfg)
	if err != nil {
		b.Fatalf("dial: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var w int
		for pb.Next() {
			if _, err := client.DoRequest(ctx, w); err != nil {
				b.Fatalf("req: %v", err)
			}
			w++
		}
	})
}

// noopClient is a zero-cost Client implementation used to isolate mix
// dispatch overhead from downstream request work.
type noopClient struct{}

func (noopClient) DoRequest(_ context.Context, _ int) (int, error) { return 0, nil }
func (noopClient) Close()                                          {}
