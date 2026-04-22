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
}

// UpgradeStats summarises the outcome of h2c-upgrade handshakes across all
// dialled connections. UpgradeAttempted is the number of conns loadgen tried
// to dial; UpgradeSucceeded is the number that completed the 101 Switching
// Protocols handshake. They are equal in the happy path; divergence means
// some conns failed the upgrade (server misconfigured, refused upgrade, etc).
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
