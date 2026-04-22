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
//	    URL:      "http://localhost:8080/",
//	    Duration: 15 * time.Second,
//	    HTTP2:    true,
//	    HTTP2Options: loadgen.HTTP2Options{
//	        Connections: 16,
//	        MaxStreams:  100,
//	    },
//	}
//
// # HTTP/2 via h2c Upgrade (RFC 7540 §3.2)
//
// Set Config.H2CUpgrade to true instead of Config.HTTP2 to exercise the
// cleartext upgrade handshake: each new TCP connection opens with an H1 GET
// carrying Connection: Upgrade, HTTP2-Settings + Upgrade: h2c headers, then
// switches to H2 on the same socket after the server responds 101 Switching
// Protocols. Mutually exclusive with HTTP2; http scheme only (TLS uses ALPN).
// On success, Result.Upgrade reports how many conns completed the handshake.
//
//	cfg := loadgen.Config{
//	    URL:        "http://localhost:8080/",
//	    Duration:   15 * time.Second,
//	    H2CUpgrade: true,
//	    HTTP2Options: loadgen.HTTP2Options{Connections: 16, MaxStreams: 100},
//	}
//
// # Traffic mixes (-mix)
//
// Set Config.Mix to a non-nil MixRatio to drive a weighted blend of H1 +
// H2 prior-knowledge + h2c-upgrade on the same target. Each worker is
// assigned a single protocol for its lifetime; the dispatcher draws
// assignments from a seeded PCG so repeated runs are deterministic.
// Result.Mix reports per-protocol request / error / conn counts; if the
// mix includes a non-zero h2c-upgrade slot, Result.Upgrade is also
// populated.
//
//	cfg := loadgen.Config{
//	    URL:      "http://localhost:8080/",
//	    Duration: 15 * time.Second,
//	    Mix:      &loadgen.MixRatio{H1: 4, H2: 4, Upgrade: 1},
//	    HTTP2Options: loadgen.HTTP2Options{Connections: 8, MaxStreams: 100},
//	}
package loadgen
