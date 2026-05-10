package loadgen

import (
	"time"
)

// TimeseriesPoint records a 1-second throughput/latency snapshot.
type TimeseriesPoint struct {
	TimestampSec   float64 `json:"t"`
	RequestsPerSec float64 `json:"rps"`
	P99Ms          float64 `json:"p99_ms,omitempty"`
}

// Result holds the benchmark results.
type Result struct {
	Requests       int64         `json:"requests"`
	Errors         int64         `json:"errors"`
	Duration       time.Duration `json:"duration"`
	RequestsPerSec float64       `json:"requests_per_sec"`
	ThroughputBPS  float64       `json:"throughput_bps"`
	Latency        Percentiles   `json:"latency"`

	ClientCPUPercent float64           `json:"client_cpu_percent,omitempty"`
	Timeseries       []TimeseriesPoint `json:"timeseries,omitempty"`

	// Upgrade, when non-nil, reports h2c-upgrade handshake outcomes for a
	// run using -h2c-upgrade. UpgradeSucceeded counts conns that completed
	// the 101 Switching Protocols handshake; UpgradeAttempted counts dialled
	// connections. For non-upgrade runs this field is omitted.
	Upgrade *UpgradeStats `json:"upgrade,omitempty"`

	// Mix, when non-nil, reports per-protocol request / error / conn counts
	// for a run using -mix. Omitted for single-protocol runs.
	Mix *MixStats `json:"mix,omitempty"`

	// DialRetries counts how many TCP SYN retries happened during the
	// run because the first SYN got an RST back (SO_REUSEPORT listener
	// replacement, adaptive engine active-engine switch, conntrack
	// collision, …). Zero in the normal case; a nonzero value is a
	// release-gate signal that the peer has a brief "no listener on
	// port" window and the run survived it only because loadgen
	// retried — the peer may still want to investigate.
	DialRetries uint64 `json:"dial_retries,omitempty"`

	// Histogram is the V2-compressed HdrHistogram payload covering the
	// full latency distribution recorded during the run. Range covers
	// 1µs through 30s with 3 significant digits (~0.1% precision).
	// Empty when no samples were recorded. Decode via
	// hdrhistogram.Decode after base64-decoding (or use the loadgen
	// helper DecodeHistogram).
	Histogram []byte `json:"histogram,omitempty"`

	// RatedMode is true when the run was driven by a constant-rate
	// scheduler (-rate) using Gil-Tene-style intended-time latency.
	// False for the default open-loop saturation mode.
	RatedMode bool `json:"rated_mode,omitempty"`

	// TargetRPS is the constant request rate requested via -rate.
	// Only meaningful when RatedMode is true; zero otherwise.
	TargetRPS float64 `json:"target_rps,omitempty"`

	// CPUPctP95 is the 95th-percentile of 1Hz CPU samples taken on the
	// loadgen process itself (user+sys ticks normalised by available
	// cores). Surfaces "loadgen CPU was the bottleneck" without
	// requiring the consumer to read /proc.
	CPUPctP95 float64 `json:"cpu_pct_p95,omitempty"`

	// RecvQHigh is true when the EPOLLIN backlog probe observed the
	// median per-socket receive queue exceed 64KB sustained for ≥10s.
	// Indicates the loadgen client could not drain responses fast
	// enough — observed latency is loadgen-side, not server-side.
	RecvQHigh bool `json:"recvq_high,omitempty"`

	// Mode is "saturation" or "rated", mirroring RatedMode in a
	// human-readable form. Set by Run() after the schedule resolves.
	Mode string `json:"mode,omitempty"`

	// Federation, when non-nil, reports the outcome of a -peer
	// coordinator/sidecar run. The coordinator's Histogram field
	// contains the merged distribution; Federation tracks how the
	// merge resolved.
	Federation *FederationStats `json:"federation,omitempty"`
}

// FederationStats captures the outcome of a -peer federated run.
// The primary instance fills this in after merging the sidecar's
// histogram into its own.
type FederationStats struct {
	Role           string `json:"role"`            // "primary" or "sidecar"
	Peer           string `json:"peer"`            // host:port of the remote
	PeerRequests   int64  `json:"peer_requests"`   // requests served by the sidecar
	PeerErrors     int64  `json:"peer_errors"`     // errors observed by the sidecar
	MergeSucceeded bool   `json:"merge_succeeded"` // true when the histograms merged cleanly
	MergeError     string `json:"merge_error,omitempty"`
}

// UpgradeStats summarises the outcome of h2c-upgrade handshakes across all
// dialled connections. UpgradeAttempted is the number of conns loadgen tried
// to dial; UpgradeSucceeded is the number that completed the 101 Switching
// Protocols handshake.
//
// In the current implementation these two fields are ALWAYS equal: the
// dialer fails the benchmark on the first conn that cannot upgrade, so a
// partial-success state never reaches a Result. The two fields are retained
// in the JSON shape as a forward-compatible slot for future best-effort
// dial semantics (one flaky conn shouldn't tank the run).
type UpgradeStats struct {
	UpgradeAttempted int `json:"attempted"`
	UpgradeSucceeded int `json:"succeeded"`
}

// MixStats breaks down request and error counts by protocol when a run uses
// the -mix flag. Conns counts report how many TCP connections were created
// per protocol slot; requests / errors are per-protocol tallies from the
// benchmark proper (post-warmup).
type MixStats struct {
	H1Conns         int   `json:"h1_conns"`
	H2Conns         int   `json:"h2_conns"`
	UpgradeConns    int   `json:"upgrade_conns"`
	H1Requests      int64 `json:"h1_requests"`
	H2Requests      int64 `json:"h2_requests"`
	UpgradeRequests int64 `json:"upgrade_requests"`
	H1Errors        int64 `json:"h1_errors"`
	H2Errors        int64 `json:"h2_errors"`
	UpgradeErrors   int64 `json:"upgrade_errors"`
}

// Percentiles holds latency percentile values.
type Percentiles struct {
	Avg   time.Duration `json:"avg"`
	Min   time.Duration `json:"min"`
	Max   time.Duration `json:"max"`
	P50   time.Duration `json:"p50"`
	P75   time.Duration `json:"p75"`
	P90   time.Duration `json:"p90"`
	P99   time.Duration `json:"p99"`
	P999  time.Duration `json:"p99_9"`
	P9999 time.Duration `json:"p99_99"`
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var b [20]byte
	pos := len(b)
	for i > 0 {
		pos--
		b[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		b[pos] = '-'
	}
	return string(b[pos:])
}
