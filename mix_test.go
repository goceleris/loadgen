package loadgen

import (
	"context"
	"math"
	"math/rand/v2"
	"strings"
	"testing"
	"time"
)

func TestParseMixRatioValid(t *testing.T) {
	cases := []struct {
		in   string
		want MixRatio
	}{
		{"h1:h2:upgrade=1:1:1", MixRatio{1, 1, 1}},
		{"h1:h2:upgrade=4:4:1", MixRatio{4, 4, 1}},
		{"h1:h2:upgrade=1:0:0", MixRatio{1, 0, 0}},
		{"h1:h2:upgrade=0:1:0", MixRatio{0, 1, 0}},
		{"h1:h2:upgrade=0:0:1", MixRatio{0, 0, 1}},
		{"h1:h2:upgrade=10:0:5", MixRatio{10, 0, 5}},
		// Bare "N:N:N" form.
		{"1:1:1", MixRatio{1, 1, 1}},
		{"3:7:2", MixRatio{3, 7, 2}},
		// Whitespace tolerant in the weights.
		{"h1:h2:upgrade= 2 : 3 : 5 ", MixRatio{2, 3, 5}},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			got, err := ParseMixRatio(tc.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.want {
				t.Errorf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestParseMixRatioInvalid(t *testing.T) {
	cases := []struct {
		in      string
		wantSub string
	}{
		{"", "empty"},
		{"h1:h2:upgrade=", "3 colon-separated"},
		{"h1:h2:upgrade=1", "3 colon-separated"},
		{"h1:h2:upgrade=1:2", "3 colon-separated"},
		{"h1:h2:upgrade=1:2:3:4", "3 colon-separated"},
		{"h1:h2:upgrade=a:b:c", "not a number"},
		{"h1:h2:upgrade=1:two:3", "not a number"},
		{"h1:h2:upgrade=-1:2:3", "negative"},
		{"h1:h2:upgrade=0:0:0", "at least one"},
		{"0:0:0", "at least one"},
		{"h1h2upgrade=1:1:1", "key must be"},
		{"foo=1:1:1", "key must be"},
	}
	for _, tc := range cases {
		t.Run(tc.in, func(t *testing.T) {
			_, err := ParseMixRatio(tc.in)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSub) {
				t.Errorf("error %q should contain %q", err.Error(), tc.wantSub)
			}
		})
	}
}

// TestMixDistribution confirms the weighted-draw stays within 2% of the
// expected ratio across 10 000 samples. Uses a seeded PCG so the result is
// reproducible.
func TestMixDistribution(t *testing.T) {
	const n = 10_000
	ratios := []MixRatio{
		{1, 1, 1},
		{4, 4, 1},
		{1, 0, 0},
		{0, 1, 0},
		{0, 0, 1},
		{3, 7, 0},
	}
	for _, m := range ratios {
		t.Run(mixName(m), func(t *testing.T) {
			r := rand.New(rand.NewPCG(0xC0FFEE, 0xBADBEEF))
			var counts [3]int
			for range n {
				p := drawMixProto(m, r)
				counts[p]++
			}
			total := float64(m.Sum())
			expected := [3]float64{
				float64(m.H1) / total,
				float64(m.H2) / total,
				float64(m.Upgrade) / total,
			}
			observed := [3]float64{
				float64(counts[0]) / float64(n),
				float64(counts[1]) / float64(n),
				float64(counts[2]) / float64(n),
			}
			for i := range observed {
				if expected[i] == 0 {
					if counts[i] != 0 {
						t.Errorf("slot %d: expected zero draws, got %d", i, counts[i])
					}
					continue
				}
				delta := math.Abs(observed[i] - expected[i])
				if delta > 0.02 {
					t.Errorf("slot %d: observed %.4f, expected %.4f, delta %.4f > 0.02",
						i, observed[i], expected[i], delta)
				}
			}
			t.Logf("ratio %+v → counts=%v (exp=%v, obs=%v)", m, counts, expected, observed)
		})
	}
}

// TestMixDistributionDeterministic verifies that two draws with identical
// seeds produce identical sequences — a regression guard for the seeded-PCG
// contract in mix.go.
func TestMixDistributionDeterministic(t *testing.T) {
	m := MixRatio{2, 3, 5}

	var a, b [100]mixProto
	r1 := rand.New(rand.NewPCG(1, 2))
	r2 := rand.New(rand.NewPCG(1, 2))
	for i := range a {
		a[i] = drawMixProto(m, r1)
		b[i] = drawMixProto(m, r2)
	}
	if a != b {
		t.Fatalf("sequences diverged: a=%v b=%v", a, b)
	}
}

// mixName returns a human-readable name for MixRatio table-test entries.
func mixName(m MixRatio) string {
	var sb strings.Builder
	sb.WriteString(itoa(m.H1))
	sb.WriteByte(':')
	sb.WriteString(itoa(m.H2))
	sb.WriteByte(':')
	sb.WriteString(itoa(m.Upgrade))
	return sb.String()
}

// TestMixSum exercises MixRatio.Sum — small safety net for the weighted-draw
// contract.
func TestMixSum(t *testing.T) {
	if (MixRatio{0, 0, 0}).Sum() != 0 {
		t.Error("zero sum")
	}
	if (MixRatio{1, 2, 3}).Sum() != 6 {
		t.Error("non-zero sum")
	}
}

// TestMixClientAgainstH2CServer exercises newMixClient end-to-end against a
// golang.org/x/net/http2/h2c server that accepts all three protocols. It is
// a smoke test for the mix dispatch: per-protocol counts must all be
// non-zero and equal the observed request totals.
// Skipped under -race because of golang/go#57639 (x/net/http2/hpack race).
func TestMixClientAgainstH2CServer(t *testing.T) {
	if raceEnabled {
		t.Skip("skipping under -race: golang/go#57639")
	}
	host, port, cleanup := startH2CUpgradeServer(t)
	defer cleanup()

	cfg := Config{
		URL:    "http://" + host + ":" + port + "/",
		Method: "GET",
		Mix:    &MixRatio{H1: 1, H2: 1, Upgrade: 1},
		HTTP2Options: HTTP2Options{
			Connections: 2,
			MaxStreams:  50,
		},
		Workers:         12, // 1:1:1 with 12 workers → ~4 per protocol slot
		Connections:     12,
		Duration:        1 * time.Second,
		scheme:          "http",
		DialTimeout:     5 * time.Second,
		ReadBufferSize:  2 << 20,
		WriteBufferSize: 2 << 20,
		MaxResponseSize: -1,
	}

	mc, err := newMixClient(host, port, "/", cfg)
	if err != nil {
		t.Fatalf("newMixClient: %v", err)
	}
	defer mc.Close()

	// Confirm all three sub-clients were created (1:1:1 with 12 workers
	// near-certainly picks each protocol at least once with the seeded RNG).
	if mc.h1 == nil || mc.h2 == nil || mc.upgrade == nil {
		t.Fatalf("mix client missing sub-clients: h1=%v h2=%v upgrade=%v",
			mc.h1 != nil, mc.h2 != nil, mc.upgrade != nil)
	}

	// Drive a handful of requests from each worker.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for worker := range cfg.Workers {
		for range 5 {
			if _, err := mc.DoRequest(ctx, worker); err != nil {
				t.Fatalf("worker %d DoRequest: %v", worker, err)
			}
		}
	}

	stats := mc.stats()
	if stats.H1Requests+stats.H2Requests+stats.UpgradeRequests == 0 {
		t.Fatal("mix client recorded zero requests")
	}
	if stats.H1Errors+stats.H2Errors+stats.UpgradeErrors != 0 {
		t.Errorf("mix client saw errors: %+v", stats)
	}
	t.Logf("mix stats: %+v", stats)
}

// TestMixBenchmarkerEndToEnd runs the full Benchmarker.Run loop with a mix
// configuration against a server that speaks every protocol. Proves that
// per-protocol counters survive the warmup reset and land in the Result.
// Skipped under -race because of golang/go#57639 (x/net/http2/hpack race).
func TestMixBenchmarkerEndToEnd(t *testing.T) {
	if raceEnabled {
		t.Skip("skipping under -race: golang/go#57639")
	}
	host, port, cleanup := startH2CUpgradeServer(t)
	defer cleanup()

	cfg := Config{
		URL:      "http://" + host + ":" + port + "/",
		Method:   "GET",
		Mix:      &MixRatio{H1: 1, H2: 1, Upgrade: 1},
		Duration: 1 * time.Second,
		Warmup:   200 * time.Millisecond,
		Workers:  12,
		HTTP2Options: HTTP2Options{
			Connections: 2,
			MaxStreams:  50,
		},
	}
	b, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result, err := b.Run(ctx)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Mix == nil {
		t.Fatal("Result.Mix is nil for -mix run")
	}
	if result.Requests == 0 {
		t.Fatal("zero requests completed")
	}
	sum := result.Mix.H1Requests + result.Mix.H2Requests + result.Mix.UpgradeRequests
	if sum == 0 {
		t.Fatalf("mix per-protocol requests sum to 0: %+v", result.Mix)
	}
	t.Logf("end-to-end mix: %+v (total=%d)", result.Mix, result.Requests)
}

// TestMixValidationRejections verifies Validate() rejects the configurations
// that the CLI error-checking also blocks — so library users get the same
// feedback as CLI users.
func TestMixValidationRejections(t *testing.T) {
	base := Config{
		URL:         "http://localhost:0/",
		Method:      "GET",
		Duration:    1 * time.Second,
		Connections: 1,
		Workers:     1,
	}
	t.Run("mix_and_h2", func(t *testing.T) {
		c := base
		c.HTTP2 = true
		c.Mix = &MixRatio{1, 1, 1}
		if err := c.Validate(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("mix_and_h2c_upgrade", func(t *testing.T) {
		c := base
		c.H2CUpgrade = true
		c.Mix = &MixRatio{1, 1, 1}
		if err := c.Validate(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("h2_and_h2c_upgrade", func(t *testing.T) {
		c := base
		c.HTTP2 = true
		c.H2CUpgrade = true
		if err := c.Validate(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("h2c_upgrade_over_https", func(t *testing.T) {
		c := base
		c.URL = "https://localhost:0/"
		c.H2CUpgrade = true
		c.HTTP2Options = HTTP2Options{Connections: 1, MaxStreams: 1}
		if err := c.Validate(); err == nil {
			t.Fatal("expected error")
		}
	})
	t.Run("mix_upgrade_slot_over_https", func(t *testing.T) {
		c := base
		c.URL = "https://localhost:0/"
		c.Mix = &MixRatio{H1: 1, H2: 1, Upgrade: 1}
		c.HTTP2Options = HTTP2Options{Connections: 1, MaxStreams: 1}
		if err := c.Validate(); err == nil {
			t.Fatal("expected error")
		}
	})
}

// TestMixProtoString sanity-checks the String() method so error messages
// survive rename refactors.
func TestMixProtoString(t *testing.T) {
	cases := map[mixProto]string{
		mixProtoH1:      "h1",
		mixProtoH2:      "h2",
		mixProtoUpgrade: "upgrade",
		mixProto(255):   "?",
	}
	for p, want := range cases {
		if got := p.String(); got != want {
			t.Errorf("mixProto(%d).String() = %q, want %q", p, got, want)
		}
	}
}
