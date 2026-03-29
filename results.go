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
