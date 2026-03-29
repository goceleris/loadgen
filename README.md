# loadgen

[![Go Reference](https://pkg.go.dev/badge/github.com/goceleris/loadgen.svg)](https://pkg.go.dev/github.com/goceleris/loadgen)
[![License](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

Zero-allocation HTTP/1.1 and HTTP/2 load generator for benchmarking web servers.

Custom protocol clients bypass Go's standard `net/http` for maximum throughput and minimal measurement overhead. Latency is recorded per-worker in sharded recorders with zero contention on the hot path.

## Features

- **HTTP/1.1**: One persistent TCP connection per worker with pre-formatted request bytes. Auto-reconnect for `Connection: close` workloads via round-robin connection pool.
- **HTTP/2**: Lock-free stream dispatch over multiplexed connections with pre-encoded HPACK headers, batched WINDOW_UPDATE, and channel-pooled response delivery.
- **HTTPS/TLS**: Full TLS support with custom certificates, ALPN negotiation for H2.
- **Zero-allocation hot path**: No allocations during request/response cycle.
- **Sharded latency recording**: Per-worker histograms merged at completion -- no lock contention.
- **Accurate percentiles**: p50, p75, p90, p99, p99.9, p99.99 via reservoir sampling.
- **Rate limiting**: Optional `MaxRPS` for closed-loop benchmarking.
- **Progress callbacks**: Real-time `OnProgress` reporting during benchmarks.
- **Extensible**: Exported `Client` interface for custom protocol handlers (gRPC, QUIC, WebSocket).

## Installation

```
go get github.com/goceleris/loadgen
```

## Quick Start

### HTTP/1.1 Benchmark

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/goceleris/loadgen"
)

func main() {
    cfg := loadgen.Config{
        URL:         "http://localhost:8080/",
        Duration:    15 * time.Second,
        Connections: 256,
        Workers:     256,
    }
    b, err := loadgen.New(cfg)
    if err != nil {
        panic(err)
    }
    result, err := b.Run(context.Background())
    if err != nil {
        panic(err)
    }
    fmt.Printf("%.0f req/s, p99=%v\n", result.RequestsPerSec, result.Latency.P99)
}
```

### HTTP/2 Benchmark

```go
cfg := loadgen.Config{
    URL:      "http://localhost:8080/",
    Duration: 15 * time.Second,
    HTTP2:    true,
    HTTP2Options: loadgen.HTTP2Options{
        Connections: 16,
        MaxStreams:  100,
    },
    Workers: 64,
}
b, err := loadgen.New(cfg)
// ...
```

### HTTPS with Custom TLS

```go
cfg := loadgen.Config{
    URL:                "https://api.example.com/health",
    Duration:           30 * time.Second,
    Connections:        128,
    Workers:            128,
    InsecureSkipVerify: true, // for self-signed certs
}
```

### Rate-Limited Benchmark

```go
cfg := loadgen.Config{
    URL:      "http://localhost:8080/",
    Duration: 30 * time.Second,
    Workers:  64,
    MaxRPS:   10000, // cap at 10k req/s
}
```

### Progress Monitoring

```go
cfg := loadgen.Config{
    // ...
    OnProgress: func(elapsed time.Duration, snapshot loadgen.Result) {
        fmt.Printf("\r%s: %d req, %.0f req/s",
            elapsed.Round(time.Second),
            snapshot.Requests,
            snapshot.RequestsPerSec)
    },
}
```

### Custom Protocol Client

```go
type myClient struct { /* ... */ }
func (c *myClient) DoRequest(ctx context.Context, workerID int) (int, error) { /* ... */ }
func (c *myClient) Close() { /* ... */ }

cfg := loadgen.Config{
    Duration: 15 * time.Second,
    Workers:  64,
    Client:   &myClient{},
}
```

## CLI Usage

```
go install github.com/goceleris/loadgen/cmd/loadgen@latest
```

```
loadgen [flags] -url <target>

Flags:
  -url string          Target URL (required)
  -duration duration   Benchmark duration (default 15s)
  -warmup duration     Warmup duration (default 2s)
  -connections int     Number of H1 connections (default 256)
  -workers int         Number of workers (default: connections for H1, NumCPU*4 for H2)
  -method string       HTTP method (default "GET")
  -H string            Custom header "Key: Value" (repeatable)
  -body-file string    Read request body from file
  -close               Send Connection: close (H1 only)
  -h2                  Use HTTP/2 (h2c/h2)
  -h2-conns int        H2 connections (default 16)
  -h2-streams int      Max concurrent H2 streams per connection (default 100)
  -insecure            Skip TLS certificate verification
  -max-rps int         Max requests per second (0 = unlimited)
```

Example:

```bash
# H1 benchmark with custom headers
loadgen -url http://localhost:8080/api -duration 30s -connections 512 \
  -H "Authorization: Bearer token" -H "Content-Type: application/json"

# H2 benchmark
loadgen -url http://localhost:8080/ -h2 -h2-conns 16 -h2-streams 200 -duration 30s

# HTTPS with rate limiting
loadgen -url https://api.example.com/health -insecure -max-rps 5000 -duration 60s
```

Output is JSON with RPS, latency percentiles (p50-p99.99), errors, throughput, and timeseries data.

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Benchmarker                       │
│  ┌─────────┐  ┌─────────┐       ┌─────────┐       │
│  │ Worker 0 │  │ Worker 1 │  ...  │ Worker N │       │
│  └────┬─────┘  └────┬─────┘       └────┬─────┘       │
│       │              │                  │              │
│  ┌────▼──────────────▼──────────────────▼─────┐      │
│  │              Client Interface               │      │
│  │  ┌──────────┐    ┌──────────┐              │      │
│  │  │ H1Client │    │ H2Client │   (custom)   │      │
│  │  └──────────┘    └──────────┘              │      │
│  └────────────────────────────────────────────┘      │
│                                                       │
│  ┌────────────────────────────────────────────┐      │
│  │       ShardedLatencyRecorder               │      │
│  │  [Shard 0] [Shard 1] ... [Shard N]        │      │
│  │  (per-worker, zero contention)             │      │
│  └────────────────────────────────────────────┘      │
└─────────────────────────────────────────────────────┘
```

### H1 Worker Model

Each worker owns a dedicated TCP connection (keep-alive mode) or a round-robin pool of 16 connections (`Connection: close` mode). Pre-formatted request bytes are written in a single syscall. No synchronization between workers.

### H2 Multiplexed Model

N workers share M connections. Each connection has a write goroutine (serializes frame writes, allocates stream IDs) and a read goroutine (dispatches responses to waiting workers via lock-free stream slots). Workers acquire a stream semaphore, submit a write request, and wait on a pooled response channel.

### Latency Recording

Each worker writes to its own shard -- no locks, no atomics on the hot path. Request/byte counters use batched local counters flushed to atomics every 256 requests (H1) or 16 requests (H2) to minimize ARM64 memory barrier overhead. Shards are merged once at benchmark completion for percentile calculation.

## H2 Flow Control

The H2 client uses aggressive flow control settings tuned for benchmarking throughput rather than production fairness.

### Settings

| Parameter | Value | RFC 7540 Default | Rationale |
|-----------|-------|-------------------|-----------|
| Initial window size | 16 MB | 64 KB | Prevents stalls with large response bodies |
| Max frame size | 64 KB | 16 KB | Balances framing overhead against flow control granularity |
| Header table size | 0 | 4096 | Zero-allocation status code extraction from HPACK headers |

### WINDOW_UPDATE Batching

The read loop accumulates consumed bytes via an atomic counter. The write loop flushes a single WINDOW_UPDATE between processing request batches AND on a 1ms ticker when idle. This amortizes frame overhead while preventing flow control stalls.

### Dynamic Table Disabled

`SETTINGS_HEADER_TABLE_SIZE=0` allows zero-allocation status code extraction from HPACK-encoded headers. With the dynamic table disabled, there is no dynamic table state to track, so status codes can be parsed directly from the encoded header block without maintaining decoder state.

### Stream Concurrency

Controlled by a semaphore sized to `min(server's MAX_CONCURRENT_STREAMS, configured MaxStreams)`. Stream slots are allocated at 2x the semaphore size for wrap-around headroom, preventing slot reuse conflicts during high-throughput bursts.

### Tuning Guidance

- **CPU-bound**: Increase connections. More TCP sockets means more kernel parallelism across cores.
- **IO-bound**: Increase streams. More multiplexed requests per connection saturates network bandwidth.
- **Large responses (>1 MB)**: Fewer connections with more streams prevents flow control pressure. The 16 MB window can sustain ~16 in-flight 1 MB responses per connection before stalling.

## Comparison with Other Tools

| Feature | loadgen | wrk | h2load | k6 |
|---------|---------|-----|--------|-----|
| HTTP/1.1 | Zero-alloc custom client | Lua-scriptable | Limited | Full `net/http` |
| HTTP/2 | Zero-alloc multiplexed | Not supported | nghttp2-based | Full `net/http` |
| HTTPS/TLS | ALPN negotiation | Yes | Yes | Yes |
| Scripting | Go (compile-time) | Lua | None | JavaScript |
| Allocations | Zero on hot path | Low | Low | High |
| Rate limiting | Built-in `MaxRPS` | None | None | Built-in |
| Use case | Raw throughput measurement | H1 benchmarking | H2 benchmarking | Load testing |

- **wrk**: H1 only, no H2 support. loadgen supports both H1 and H2 with zero-alloc clients.
- **h2load**: H2 focused but limited H1. loadgen has both with equal optimization.
- **k6**: Full-featured but higher overhead (JavaScript scripting). loadgen is pure Go, zero-alloc, designed for maximum throughput measurement.
- **loadgen**: Focused on raw throughput measurement with minimal client overhead. Not a general-purpose load testing framework.

## License

Apache License 2.0 -- see [LICENSE](LICENSE).
