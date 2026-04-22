package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"
)

// MixRatio captures the three-protocol traffic ratio parsed from the `-mix`
// flag. Each field is a non-negative weight; the dispatcher normalises them
// when drawing per-connection protocol assignments.
type MixRatio struct {
	H1      int // weight for plain H1
	H2      int // weight for H2 prior-knowledge
	Upgrade int // weight for h2c upgrade
}

// Sum reports the total weight across the three slots. Returns 0 for the
// degenerate all-zero ratio; callers should reject that case before
// dispatching (weighted-draw over zeros is undefined).
func (m MixRatio) Sum() int { return m.H1 + m.H2 + m.Upgrade }

// ParseMixRatio parses a mix ratio string of the form "h1:h2:upgrade=N:N:N".
// The key prefix "h1:h2:upgrade=" is optional; if absent, the bare "N:N:N"
// form is accepted. All three weights must be non-negative integers and at
// least one must be positive (all-zero is rejected because weighted-draw
// over zeros is undefined).
func ParseMixRatio(s string) (MixRatio, error) {
	if s == "" {
		return MixRatio{}, errors.New("loadgen: empty mix ratio string")
	}
	// Accept "h1:h2:upgrade=N:N:N" or the bare "N:N:N" form.
	if eq := strings.IndexByte(s, '='); eq >= 0 {
		key := strings.TrimSpace(s[:eq])
		if key != "h1:h2:upgrade" {
			return MixRatio{}, fmt.Errorf("loadgen: mix ratio key must be \"h1:h2:upgrade\", got %q", key)
		}
		s = s[eq+1:]
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return MixRatio{}, fmt.Errorf("loadgen: mix ratio must have 3 colon-separated weights, got %d in %q", len(parts), s)
	}
	weights := make([]int, 3)
	for i, p := range parts {
		v, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil {
			return MixRatio{}, fmt.Errorf("loadgen: mix ratio weight %d is not a number: %q", i, p)
		}
		if v < 0 {
			return MixRatio{}, fmt.Errorf("loadgen: mix ratio weight %d is negative: %d", i, v)
		}
		weights[i] = v
	}
	m := MixRatio{H1: weights[0], H2: weights[1], Upgrade: weights[2]}
	if m.Sum() == 0 {
		return MixRatio{}, errors.New("loadgen: mix ratio is 0:0:0 — at least one weight must be positive")
	}
	return m, nil
}

// mixProto identifies the protocol chosen for a connection.
type mixProto uint8

const (
	mixProtoH1 mixProto = iota
	mixProtoH2
	mixProtoUpgrade
)

func (p mixProto) String() string {
	switch p {
	case mixProtoH1:
		return "h1"
	case mixProtoH2:
		return "h2"
	case mixProtoUpgrade:
		return "upgrade"
	default:
		return "?"
	}
}

// drawMixProto returns a mixProto chosen by weighted random draw from m using
// r. Panics if m.Sum() == 0; callers must validate before drawing.
func drawMixProto(m MixRatio, r *rand.Rand) mixProto {
	total := m.Sum()
	if total <= 0 {
		panic("loadgen: drawMixProto: empty ratio")
	}
	n := r.IntN(total)
	if n < m.H1 {
		return mixProtoH1
	}
	if n < m.H1+m.H2 {
		return mixProtoH2
	}
	return mixProtoUpgrade
}

// mixClient dispatches requests across three per-protocol sub-clients,
// assigning each worker to exactly one protocol by weighted random draw.
// Every worker stays on its chosen protocol for the entire benchmark.
//
// Per-protocol request/error counters are maintained so the final report
// can break down RPS + error rate by protocol; the shared
// ShardedLatencyRecorder tracks aggregate latency.
type mixClient struct {
	h1      Client
	h2      Client
	upgrade Client

	// workerMap[i] is the protocol assigned to worker i.
	workerMap []mixProto

	// Per-worker sub-client indices. For H1, each worker is assigned a
	// contiguous connection slice in the underlying h1Client. For H2
	// variants, workers are distributed round-robin across their dedicated
	// conn pool by modular indexing inside DoRequest.
	subWorkerID []int

	// Per-protocol request / error counters, atomic so workers update
	// without serialising.
	h1Requests      atomic.Int64
	h2Requests      atomic.Int64
	upgradeRequests atomic.Int64
	h1Errors        atomic.Int64
	h2Errors        atomic.Int64
	upgradeErrors   atomic.Int64

	// Per-protocol conn counts (informational, surfaced in Result).
	h1Conns      int
	h2Conns      int
	upgradeConns int
}

// newMixClient constructs a mixClient using the weights in cfg.Mix. Workers
// are partitioned across the three protocols by weighted draw; each
// protocol's sub-client is created if and only if at least one worker is
// assigned to it (avoiding phantom conn dials to the server).
func newMixClient(host, port, path string, cfg Config) (*mixClient, error) {
	if cfg.Mix == nil {
		return nil, errors.New("loadgen: mix client requires non-nil Config.Mix")
	}

	// Seed the draw RNG deterministically from the ratio so repeated runs
	// with the same config produce the same assignment. Benchmark variance
	// comes from timing, not dispatch.
	seed := uint64(cfg.Mix.H1)*1_000_003 + uint64(cfg.Mix.H2)*10_007 + uint64(cfg.Mix.Upgrade)*17
	r := rand.New(rand.NewPCG(seed, seed^0x9E3779B97F4A7C15))

	workerMap := make([]mixProto, cfg.Workers)
	counts := [3]int{}
	for i := range cfg.Workers {
		p := drawMixProto(*cfg.Mix, r)
		workerMap[i] = p
		counts[p]++
	}

	subWorkerID := make([]int, cfg.Workers)
	var h1Idx, h2Idx, upgradeIdx int
	for i, p := range workerMap {
		switch p {
		case mixProtoH1:
			subWorkerID[i] = h1Idx
			h1Idx++
		case mixProtoH2:
			subWorkerID[i] = h2Idx
			h2Idx++
		case mixProtoUpgrade:
			subWorkerID[i] = upgradeIdx
			upgradeIdx++
		}
	}

	mc := &mixClient{
		workerMap:   workerMap,
		subWorkerID: subWorkerID,
	}

	// H1 sub-client: one per worker that drew H1, matching the normal H1
	// client's 1:1 worker/conn mapping.
	if counts[mixProtoH1] > 0 {
		h1Cfg := cfg
		h1Cfg.HTTP2 = false
		h1Cfg.H2CUpgrade = false
		h1Cfg.Mix = nil
		h1Cfg.Workers = counts[mixProtoH1]
		// Connections is derived from Workers for H1 — see newH1Client.
		h1Cfg.Connections = counts[mixProtoH1]
		h1c, err := newH1Client(host, port, path, h1Cfg)
		if err != nil {
			return nil, fmt.Errorf("mix: h1 sub-client: %w", err)
		}
		mc.h1 = h1c
		mc.h1Conns = counts[mixProtoH1]
	}

	// H2 prior-knowledge sub-client.
	if counts[mixProtoH2] > 0 {
		h2Cfg := cfg
		h2Cfg.HTTP2 = true
		h2Cfg.H2CUpgrade = false
		h2Cfg.Mix = nil
		h2c, err := newH2Client(host, port, path, h2Cfg)
		if err != nil {
			return nil, fmt.Errorf("mix: h2 sub-client: %w", err)
		}
		mc.h2 = h2c
		mc.h2Conns = cfg.HTTP2Options.Connections
	}

	// h2c-upgrade sub-client.
	if counts[mixProtoUpgrade] > 0 {
		upCfg := cfg
		upCfg.HTTP2 = false
		upCfg.H2CUpgrade = true
		upCfg.Mix = nil
		uc, err := newH2CUpgradeClient(host, port, path, upCfg)
		if err != nil {
			// Clean up already-constructed sub-clients.
			if mc.h1 != nil {
				mc.h1.Close()
			}
			if mc.h2 != nil {
				mc.h2.Close()
			}
			return nil, fmt.Errorf("mix: upgrade sub-client: %w", err)
		}
		mc.upgrade = uc
		mc.upgradeConns = cfg.HTTP2Options.Connections
	}

	return mc, nil
}

// DoRequest dispatches the request to this worker's assigned sub-client,
// updating the per-protocol request / error counters.
func (c *mixClient) DoRequest(ctx context.Context, workerID int) (int, error) {
	p := c.workerMap[workerID%len(c.workerMap)]
	sub := c.subWorkerID[workerID%len(c.subWorkerID)]

	var (
		n   int
		err error
	)
	switch p {
	case mixProtoH1:
		n, err = c.h1.DoRequest(ctx, sub)
		if err != nil {
			c.h1Errors.Add(1)
		} else {
			c.h1Requests.Add(1)
		}
	case mixProtoH2:
		n, err = c.h2.DoRequest(ctx, sub)
		if err != nil {
			c.h2Errors.Add(1)
		} else {
			c.h2Requests.Add(1)
		}
	case mixProtoUpgrade:
		n, err = c.upgrade.DoRequest(ctx, sub)
		if err != nil {
			c.upgradeErrors.Add(1)
		} else {
			c.upgradeRequests.Add(1)
		}
	}
	return n, err
}

// Close shuts down every sub-client.
func (c *mixClient) Close() {
	if c.h1 != nil {
		c.h1.Close()
	}
	if c.h2 != nil {
		c.h2.Close()
	}
	if c.upgrade != nil {
		c.upgrade.Close()
	}
}

// stats returns the per-protocol counters as a MixStats snapshot.
func (c *mixClient) stats() MixStats {
	return MixStats{
		H1Conns:         c.h1Conns,
		H2Conns:         c.h2Conns,
		UpgradeConns:    c.upgradeConns,
		H1Requests:      c.h1Requests.Load(),
		H2Requests:      c.h2Requests.Load(),
		UpgradeRequests: c.upgradeRequests.Load(),
		H1Errors:        c.h1Errors.Load(),
		H2Errors:        c.h2Errors.Load(),
		UpgradeErrors:   c.upgradeErrors.Load(),
	}
}
