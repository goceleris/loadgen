// Package loadgen provides a zero-allocation HTTP/1.1 and HTTP/2 load
// generator for benchmarking web servers.
//
// The package implements custom protocol clients that bypass Go's standard
// net/http library for maximum throughput and minimal measurement overhead:
//
//   - HTTP/1.1: one persistent TCP connection per worker with pre-formatted
//     request bytes, auto-reconnect for Connection: close workloads via a
//     round-robin connection pool.
//
//   - HTTP/2: lock-free stream dispatch over multiplexed connections with
//     pre-encoded HPACK headers, batched WINDOW_UPDATE, and channel-pooled
//     response delivery.
//
// Latency is recorded per-worker in sharded recorders with zero contention
// on the hot path, producing accurate p50/p75/p90/p99/p99.9/p99.99
// percentiles via reservoir sampling.
//
// # Quick Start
//
//	cfg := loadgen.Config{
//	    URL:      "http://localhost:8080/",
//	    Duration: 15 * time.Second,
//	    Connections: 256,
//	}
//	b := loadgen.New(cfg)
//	result, err := b.Run(context.Background())
//	fmt.Printf("%.0f req/s, p99=%v\n", result.RequestsPerSec, result.Latency.P99)
//
// For HTTP/2:
//
//	cfg := loadgen.Config{
//	    URL:           "http://localhost:8080/",
//	    Duration:      15 * time.Second,
//	    H2C:           true,
//	    H2Connections: 16,
//	    H2MaxStreams:  100,
//	}
package loadgen
