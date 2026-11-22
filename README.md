<p align="center">
  <img src=".github/cover.png" alt="goceleris / loadgen cover: loadgen, HTTP/1.1 and HTTP/2 load generator (http/1.1, http/2, tls, sharded-latency, p99.99)">
</p>

<p align="center">
  <a href="https://github.com/goceleris/loadgen/actions/workflows/ci.yml?query=branch%3Amain"><img src="https://github.com/goceleris/loadgen/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI status on main"></a>
  <a href="https://codecov.io/gh/goceleris/loadgen"><img src="https://codecov.io/gh/goceleris/loadgen/graph/badge.svg" alt="Codecov test coverage"></a>
  <a href="https://codspeed.io/goceleris/loadgen"><img src="https://img.shields.io/endpoint?url=https://codspeed.io/badge.json" alt="CodSpeed continuous benchmarks"></a>
  <a href="https://pkg.go.dev/github.com/goceleris/loadgen"><img src="https://pkg.go.dev/badge/github.com/goceleris/loadgen.svg" alt="Go Reference"></a>
  <a href="https://github.com/goceleris/loadgen/releases/latest"><img src="https://img.shields.io/github/v/release/goceleris/loadgen" alt="Latest release"></a>
  <a href="go.mod"><img src="https://img.shields.io/github/go-mod/go-version/goceleris/loadgen" alt="Go version from go.mod"></a>
  <a href="LICENSE"><img src="https://img.shields.io/github/license/goceleris/loadgen" alt="License: Apache-2.0"></a>
</p>

<p align="center">
  <strong>An HTTP/1.1 and HTTP/2 load generator for benchmarking web servers, with its own protocol clients instead of <code>net/http</code>, so that the numbers describe the server and not the client.</strong>
</p>

<p align="center">
  <a href="#why-loadgen">Why</a> ·
  <a href="#quick-start">Quick start</a> ·
  <a href="#cli-reference">CLI reference</a> ·
  <a href="#status-and-stability">Status and stability</a> ·
  <a href="https://pkg.go.dev/github.com/goceleris/loadgen">API reference</a> ·
  <a href="https://goceleris.dev/benchmarks/">Benchmarks it measures</a>
</p>

## Why loadgen

- **Its own H1 and H2 clients.** No `net/http` on the request path: request bytes are pre-formatted,
  HPACK headers are pre-encoded, and H2 status codes are read from the header block without decoding
  it.
- **Few allocations per request.** The goal is an allocation-free request/response cycle. Measured
  locally, the loadgen process made about 0.01 heap allocations per request over a 10 s run, nearly
  all of them per-run setup ([how it was measured](#allocations-measured-and-how-to-reproduce)). No test
  enforces it, and the WebSocket and SSE drivers allocate per message.
- **HTTP/1.1, HTTP/2 and more.** Prior-knowledge h2c or h2 over TLS (ALPN), the RFC 7540 §3.2 h2c
  upgrade, a weighted per-worker mix of all three, and built-in WebSocket and Server-Sent Events
  drivers.
- **Full latency distributions.** Every worker records into its own HdrHistogram (up to 30 s at three
  significant digits); p50 to p99.99 come from the merged histogram, and the full distribution is
  exported with every result for re-aggregation.
- **Coordinated-omission correction.** In rated mode (`-rate`), latency is measured from each
  request's *intended* send time, so a server stall shows up as tail latency instead of disappearing.
- **It tells you when it was the bottleneck.** A process-CPU sampler and a Linux receive-queue probe
  flag runs where the client, not the server, limited the result.

loadgen measures one server with minimal client overhead. It is not a general-purpose load-testing
framework; for scripted user journeys, use a tool built for that, such as k6. It is also young: read
[Status and stability](#status-and-stability) before relying on a number.

## Quick start

Install the CLI (Go 1.27.3 or newer, per [`go.mod`](go.mod)):

```bash
go install github.com/goceleris/loadgen/cmd/loadgen@latest
```

Run 10 seconds of HTTP/1.1 load with 64 connections against a server on port 8080:

```bash
loadgen -url http://localhost:8080/ -duration 10s -connections 64
```

Progress goes to stderr and the result goes to stdout as one JSON object: request and error counts,
requests per second, latency percentiles in nanoseconds, a per-second timeseries and the full
histogram. To pull out the headline numbers:

```bash
loadgen -url http://localhost:8080/ -duration 10s -connections 64 \
  | jq '{rps: .requests_per_sec, p50_ms: (.latency.p50 / 1e6), p99_ms: (.latency.p99 / 1e6), errors}'
```

Add `-h2` for HTTP/2, `-rate 20000` for a coordinated-omission-corrected run at 20k req/s, or
`-mode ws-echo` for WebSocket round trips. Pre-built binaries for Linux and macOS are attached to every
[release](https://github.com/goceleris/loadgen/releases/latest); see
[Pre-built binary releases](#pre-built-binary-releases).

### As a library

```bash
go get github.com/goceleris/loadgen
```

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/goceleris/loadgen"
)

func main() {
	b, err := loadgen.New(loadgen.Config{
		URL:         "http://localhost:8080/",
		Duration:    15 * time.Second,
		Connections: 256,
		Workers:     256,
	})
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

`Config.Warmup` is zero unless you set it; `DefaultConfig()` returns a 5 s warmup, and the CLI defaults
to 2 s.

More library examples:

<details>
<summary>HTTP/2, HTTPS, rated mode and progress callbacks</summary>

```go
// HTTP/2 prior knowledge: the workers share 16 connections, up to 100 streams each.
// New multiplies Workers by 4 in the HTTP/2, h2c-upgrade and mix modes, so this
// run has 256 workers.
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
```

```go
// HTTPS. The certificate and host name are verified against the system CA pool.
// For a private CA, client certificates or a pinned cipher list, set
// Config.TLSConfig (for example TLSConfig.RootCAs). InsecureSkipVerify (-insecure
// on the CLI) turns verification off; keep it for throwaway test certificates.
cfg := loadgen.Config{
	URL:         "https://api.example.com/health",
	Duration:    30 * time.Second,
	Connections: 128,
	Workers:     128,
}
```

```go
// Constant-rate (rated) run: 10k req/s, coordinated-omission corrected.
cfg := loadgen.Config{
	URL:         "http://localhost:8080/",
	Duration:    30 * time.Second,
	Connections: 64,
	Workers:     64,
	Rate:        10000,
}
```

```go
// Progress monitoring.
cfg := loadgen.Config{
	// ...
	OnProgress: func(elapsed time.Duration, snapshot loadgen.Result) {
		fmt.Printf("\r%s: %d req, %.0f req/s",
			elapsed.Round(time.Second), snapshot.Requests, snapshot.RequestsPerSec)
	},
}
```

</details>

## How the goceleris projects use it

- **[probatorium](https://github.com/goceleris/probatorium)**, the benchmark and validation harness,
  imports loadgen as a library: the load of its bench tier comes from loadgen. Its runner drives every
  cell with `loadgen.New(...).Run`, runs the rated latency-at-SLO passes through `Config.Rate`, records
  `loadgen_version` with each pass, and merges the exported histograms across runs before publishing
  (it decodes the V2 payload with the HdrHistogram library directly).
- **[celeris](https://github.com/goceleris/celeris)**: the numbers on the
  [goceleris.dev benchmark dashboard](https://goceleris.dev/benchmarks/) are probatorium runs, so the
  load behind them came from loadgen. In the other direction, loadgen's own integration matrix
  (`go test -tags integration`, run in CI) drives a live celeris server over H1, H2, h2c upgrade and
  mixed traffic through `internal/integrationtest/testserver`, a nested module that pins celeris
  v1.5.11.
- **The version behind the v1.6.0 numbers.** Every result records the loadgen version that produced it
  (`loadgen_version`); the v1.6.0 benches used
  [[PLACEHOLDER: loadgen version used by the v1.6.0 benches, run id]].

## Load modes

loadgen selects one driver per run. HTTP protocol selection (`-h2` / `-h2c-upgrade` / `-mix`) and the
streaming drivers (`-mode`) are mutually exclusive: a run is one or the other.

### HTTP protocols

| Mode | Flag | What it measures |
| --- | --- | --- |
| **HTTP/1.1** (default) | *(none)* | Baseline request/response throughput. One persistent TCP connection per worker, pre-formatted request bytes written with a single write call. With `-close`, every request carries `Connection: close` and travels on a connection of its own: after the response the server closes the connection (over TLS the client answers the server's `close_notify` with its own; a server that still keeps the TCP connection open is reset by the client after 50 ms, so the loadgen host keeps no `TIME_WAIT`), and the next request dials a fresh one, a dial that counts in that request's latency, including any SYN retransmission (1 s or more) when the server's accept queue is full. The expected close is never counted as an error. |
| **HTTP/2** | `-h2` | Multiplexed throughput. Prior-knowledge h2c, or h2 over TLS (ALPN offers `h2`): workers share connections and dispatch streams lock-free, with pre-encoded HPACK headers and batched `WINDOW_UPDATE`. |
| **h2c upgrade** | `-h2c-upgrade` | The RFC 7540 §3.2 cleartext upgrade path. Each connection starts as HTTP/1.1 carrying `Connection: Upgrade, HTTP2-Settings` + `Upgrade: h2c`, reads `101 Switching Protocols`, then switches to HTTP/2 on the same socket. Exercises the handshake that `-h2` skips. Cleartext only: TLS servers negotiate H2 via ALPN, and an `https` URL is rejected. The result includes an `upgrade` block and stderr prints `h2c upgrade: X/Y conns upgraded successfully`. |
| **Protocol mix** | `-mix h1:h2:upgrade=N:N:N` | A traffic blend. Each worker is assigned a protocol by a weighted draw (seeded from the weights, so repeated runs assign the same way) and keeps it for the whole run; the H2 and upgrade sub-clients each open `-h2-conns` connections. The `h1:h2:upgrade=` prefix is optional (a bare `N:N:N` parses too), weights are non-negative integers, `0:0:0` is rejected, and a non-zero upgrade weight needs an `http` URL. The `mix` block in the result and a stderr breakdown report per-protocol connection, request and error counts. |

`-h2`, `-h2c-upgrade` and `-mix` are mutually exclusive.

### Streaming drivers

Set `-mode` (or `Config.Mode`) to drive a non-HTTP protocol. One long-lived connection is held open per
worker; each unit of work flows through the same worker, latency and timeseries pipeline as an HTTP
request. `https` URLs use TLS and verify the certificate unless `-insecure` is set; `Config.TLSConfig`
is not used by these drivers.

| Mode | What it measures |
| --- | --- |
| `ws-echo` | RFC 6455 WebSocket echo round-trip latency (small text frame). |
| `ws-large-echo` | Echo round trip with a 64 KiB payload; exercises framing and buffering under large frames. |
| `ws-hub` | Broadcast fan-out: the worker only reads, and each unit is the wait for the server's next broadcast frame (frames carry no timestamp, so this is inter-frame time). No outbound write per unit. |
| `sse-fanout` | Server-Sent Events delivery. One `GET` stream per worker on the URL's path; each unit blocks until the next event arrives, so inter-event delivery time *is* the recorded latency. |

These drivers count frames and events without checking their content: a wrong or corrupted echo is
counted as a success ([#106](https://github.com/goceleris/loadgen/issues/106)).

## Rated mode (coordinated-omission correction)

By default every worker sends its next request as soon as the previous response arrives (a closed
loop), and loadgen reports throughput at saturation.

Pass `-rate <req/s>` (or set `Config.Rate`) to switch to a constant-rate schedule: one intended send
time every `1/rate` seconds, issued whether or not earlier requests have completed, and served by the
worker pool. Latency is measured from the request's *intended* dispatch time, not the moment
it actually left: Gil Tene's coordinated-omission correction. When the server stalls, the backlog of
intended-but-unsent requests accumulates and surfaces as honest tail latency, instead of being hidden by
a client that simply slowed down to match. Rated runs set `rated_mode: true`, `target_rps` and
`mode: "rated"` in the result. Compare `requests_per_sec` with `target_rps`: a run that fell short of
its rate carries the unsent backlog in its tail latency.

The intended times are emitted by several scheduler goroutines (`-sched-shards`,
`Config.SchedulerShards`; 0 = automatic, which is `ceil(rate / 80000)` capped at `GOMAXPROCS` and 32;
1 = a single goroutine): each goroutine emits
every k-th tick of the same arithmetic schedule, so the set of intended times is identical for every
count. One goroutine pays a wake-up of a parked worker per tick; on the amd64 bench client it could not
keep a 300k ticks/s schedule (measured on the bench cluster, see the
[v1.4.15 release notes](https://github.com/goceleris/loadgen/releases/tag/v1.4.15) and
[celeris#757](https://github.com/goceleris/celeris/issues/757); the ceiling depends on the CPU), and
its lateness then grew linearly and was recorded as latency the server did not cause.
Rated results carry `scheduler_shards`; compare rated results only between runs of the same
`loadgen_version` and shard rule (rated results above 80k req/s from v1.4.14 and earlier are
single-goroutine results). Slots reach the workers in an order that may differ from the intended
order (by up to k-1 periods while every goroutine is on time, by its lateness otherwise); each worker
measures `time.Since(intended)` of the slot it holds. In a federated run the sidecar takes the
coordinator's explicit `-sched-shards` and otherwise decides its own count from its rate.

`-rate` (constant rate, coordinated-omission corrected) and `-max-rps` are independent rate controls.
`-max-rps` only paces each closed-loop worker on its own to at most `max(1, max-rps / workers)` req/s
(no shared cap, so with more workers than `max-rps` the total exceeds it); latency is still measured
from the actual send, and it does not apply in rated mode. `-rate` is the right one for
latency-SLO measurement. One known limit of the rated path is tracked in
[#102](https://github.com/goceleris/loadgen/issues/102); see [Status and stability](#status-and-stability).

## Federation

When one host can't generate enough load to saturate the target, run loadgen on two boxes and merge their
measurements client-side.

- Start a **sidecar** on the second host: `loadgen -sidecar :9099`. It binds, waits for one
  coordinator, and takes the run's settings from the coordinator's start frame: URL, duration, rate,
  scheduler shards, connection and worker counts, and the HTTP/2 switch. Other settings (method,
  headers, body, `-close`, `-mode`, `-mix`, `-h2c-upgrade`, `-insecure`, `-max-rps`, warmup) are not
  forwarded, and the sidecar runs without a warmup.
- Start the **coordinator** with `-peer <sidecar-host>:9099` plus the usual flags. It dials the sidecar,
  both sides start against the target at an agreed time (so their clocks must agree), and at the end of
  the run the coordinator pulls the sidecar's V2-compressed HdrHistogram and merges it into its own. The
  latency percentiles come from the merged histogram; request and error counts stay local, with the
  sidecar's reported as `federation.peer_requests` and `federation.peer_errors`.

Each side runs the full workload, so the target sees the sum of the two. With `-rate R` each side
paces `R`: a local run of `-rate 5000 -duration 3s` with a sidecar gave 14,994 requests on the
coordinator and 14,975 on the sidecar. `requests`, `requests_per_sec` and the throughput fields in a
federated result describe the coordinator alone; add `federation.peer_requests` for the total.

The wire protocol is deliberately minimal: a `loadgen-fed-v1` magic handshake followed by
length-prefixed JSON frames (`start` / `ack` / `result` / `err`) over one TCP connection, with no
retries. It is best-effort: if any step fails, the coordinator records
`federation.merge_succeeded = false` (with `federation.merge_error`) and reports its local-only
histogram. Merge outcomes appear in the `federation` block of the result.

## HdrHistogram export

Every result carries the full latency distribution, not just the summary percentiles:

- `Result.Histogram` is a V2-compressed HdrHistogram (base64 in JSON) of nanosecond latencies, up to
  30 s at three significant digits. It is empty when no samples were recorded.
- `loadgen.DecodeHistogram([]byte)` (a thin wrapper around `hdrhistogram.Decode`) and the recorder's
  `EncodeHistogram()` are exported so downstream tools can decode, re-merge across cells and recompute
  percentiles without re-running the benchmark.

This is the same encoding federation uses on the wire, so a merged multi-host distribution round-trips
losslessly.

## Self-instrumentation

Two probes flag the runs where loadgen, not the server, was the limiting factor:

- **CPU sampler** (`-cpu-monitor`, on by default, Linux only). Samples the loadgen process's own CPU at
  1 Hz and reports the P95 as `cpu_pct_p95` (percentage of available cores). A high value means loadgen was
  compute-bound and the throughput number is a client ceiling, not a server ceiling.
- **Receive-queue probe** (`-recvq-probe`, on by default, Linux only). Reads per-socket receive-queue
  depths from `/proc/net/tcp` and `/proc/net/tcp6` every 5 s. If the median across loadgen's sockets
  stays above 64 KB for 10 s or more it latches `recvq_high: true`: the client couldn't drain responses
  fast enough, so the observed tail latency is loadgen-side. A no-op on other platforms.

## CLI reference

```text
loadgen [flags] -url <target>
```

### Flags

`loadgen -h` lists every flag with its default. The ones whose behaviour needs more than the help text:

| Flag | Default | Meaning |
| --- | --- | --- |
| `-connections int` | `256` | Default worker count for H1; each H1 worker holds one connection (or a `PoolSize` pool with `-close`). |
| `-workers int` | `0` | Concurrent workers. `0` → `-connections` for H1, `NumCPU*4` for multiplexed modes. Multiplexed modes (`-h2`, `-h2c-upgrade`, `-mix`) then run 4 workers per configured worker. |
| `-warmup duration` | `2s` | Warmup before measurement, excluded from the result's counts and latencies (reported under `warmup`). `0` to skip. |
| `-method string` | `GET` | One of `GET`, `HEAD`, `POST`, `PUT`, `DELETE`, `PATCH`, `OPTIONS`. |
| `-H "Key: Value"` | | Custom header, repeatable. The separator is a colon and one space. |
| `-mode string` | | Streaming driver: `ws-echo` \| `ws-large-echo` \| `ws-hub` \| `sse-fanout`. Mutually exclusive with `-h2` / `-h2c-upgrade` / `-mix`. |
| `-max-rps int` | `0` | Paces each worker on its own to `max(1, max-rps / workers)` req/s (integer division, `workers` after the 4x scaling of the multiplexed modes). There is no shared cap: with more workers than `max-rps` the total exceeds it. `0` = unlimited; ignored in rated mode. |
| `-rate float` | `0` | Constant request rate (req/s). `>0` enables rated mode with coordinated-omission correction. |
| `-sched-shards int` | `0` | Rated mode: number of goroutines that emit the intended times. `0` = automatic, `1` = a single goroutine. See [Rated mode](#rated-mode-coordinated-omission-correction). |
| `-peer host:port` / `-sidecar host:port` | | Federation coordinator and sidecar. A sidecar takes its workload settings from the coordinator. See [Federation](#federation). |
| `-out string` | | Also write the JSON result to this file (in addition to stdout). |
| `-cpu-monitor` / `-recvq-probe` | `true` | The two [self-instrumentation](#self-instrumentation) probes (Linux only). |

The remaining flags are `-url` (required, `http` or `https`), `-duration` (15s, excludes warmup),
`-body-file`, `-close` (H1 only), `-h2`, `-h2c-upgrade`, `-mix`, `-h2-conns` (16), `-h2-streams` (100,
per connection) and `-insecure`; [Load modes](#load-modes) covers their behaviour.

### Examples

```bash
# H1 with custom headers.
loadgen -url http://localhost:8080/api -duration 30s -connections 512 \
  -H "Authorization: Bearer token" -H "Content-Type: application/json"

# H2 prior knowledge, browser-style fan-out.
loadgen -url http://localhost:8080/ -h2 -h2-conns 16 -h2-streams 200 -duration 30s

# h2c upgrade (RFC 7540 §3.2).
loadgen -url http://localhost:8080/ -h2c-upgrade -duration 30s

# A traffic mix: H2-heavy, with a trickle of H1 and upgrade connections.
loadgen -url http://localhost:8080/ -mix h1:h2:upgrade=1:4:1 -duration 60s

# Rated latency measurement at 20k req/s.
loadgen -url http://localhost:8080/ -rate 20000 -duration 60s

# WebSocket echo latency.
loadgen -url http://localhost:8080/ws -mode ws-echo -duration 30s

# HTTPS with paced workers and a result file.
loadgen -url https://api.example.com/health -max-rps 5000 -duration 60s -out result.json

# Federation: a sidecar on the second host, then the coordinator.
loadgen -sidecar :9099
loadgen -url http://target:8080/ -peer second-host:9099 -rate 50000 -duration 60s
```

## Cluster-bench integration contract

`cmd/loadgen` is built to be invoked remotely (over SSH or by an orchestrator such as Ansible) on a
dedicated load-generation host. The contract:

| Aspect | Behavior |
| --- | --- |
| **stdout** | A single pretty-printed JSON `Result` object: no progress noise, no log lines. |
| **stderr** | Human-readable progress (`<elapsed>  <reqs>  <rps>`), warmup and mix/upgrade summaries, error traces. |
| **exit 0** | The benchmark ran (request errors are reported inside the JSON, not via the exit code). |
| **exit 2** | Conflicting `-h2` / `-h2c-upgrade` / `-mix`, or an unparseable `-mix`. No JSON. |
| **exit 1** | Any other configuration error (`-url` missing, a bad `-H`, an unreadable body file, an invalid config), or a run that could not start or failed outright, such as a target that refused every dial. No JSON. |
| **SIGINT / SIGTERM** | During the run, cancels it *gracefully*: the partial `Result` is still printed to stdout, exit 0. |

### JSON output schema

The shape is `loadgen.Result` in [`results.go`](results.go). Fields marked *(optional)* are omitted when
empty (`omitempty`).

| Field | Type | Meaning |
| --- | --- | --- |
| `requests` | int64 | Successful requests during the measurement window. |
| `errors` | int64 | Errors during the measurement window. |
| `duration` | duration (ns) | Wall-clock measurement duration (post-warmup). |
| `requests_per_sec` | float64 | `requests / duration.Seconds()`. |
| `throughput_bps` | float64 | Response-body bytes per second, over successful responses: the Content-Length or the chunk sizes for H1, the data of every DATA frame for H2 (padding excluded, as chunk framing is for H1). |
| `latency` | object | `{avg, min, max, p50, p75, p90, p99, p99_9, p99_99}`, each a duration in ns. |
| `loadgen_version` | string | loadgen build that produced the run. *(optional)* |
| `mode` | string | `"saturation"` or `"rated"`. *(optional)* |
| `rated_mode` | bool | True when driven by the constant-rate scheduler. *(optional)* |
| `target_rps` | float64 | Requested rate; meaningful only when `rated_mode`. *(optional)* |
| `scheduler_shards` | int | Number of goroutines that emitted a rated run's intended times. *(optional)* |
| `histogram` | base64 bytes | V2-compressed HdrHistogram of the full distribution (ns, up to 30 s, 3 significant digits). *(optional)* |
| `client_cpu_percent` | float64 | Whole-host CPU% over the run, from `/proc/stat` (Linux only). *(optional)* |
| `cpu_pct_p95` | float64 | P95 of the 1 Hz process-CPU sampler. *(optional)* |
| `recvq_high` | bool | True when the receive-queue probe latched (loadgen-side backpressure). *(optional)* |
| `dial_retries` | uint64 | TCP SYN retries after an RST (listener-replacement / engine-switch window). *(optional)* |
| `connect_errors` | uint64 | Dial/handshake failures (TCP, TLS, WS/SSE upgrade, H1 reconnect). *(optional)* |
| `close_aborts` | uint64 | H1 connections that were done (`-close`, or a response with `Connection: close`) and that the client reset because the server's FIN had not come 50 ms after the response: about one per request for a server that ignores `Connection: close`. *(optional)* |
| `timeseries` | array | 1-second snapshots: `{t, rps, p99_ms, errors, connect_errors}`. On a slow run `rps` is quantised by the 256-request per-worker flush and many seconds read 0 ([#107](https://github.com/goceleris/loadgen/issues/107)); use the top-level `requests_per_sec` there. *(optional)* |
| `warmup` | object | Warmup-phase `{requests, errors, connect_errors}`; a zero-request, nonzero-error warmup means the target was never healthy. *(optional)* |
| `upgrade` | object | h2c-upgrade handshake tally `{attempted, succeeded}` (`-h2c-upgrade`). *(optional)* |
| `mix` | object | Per-protocol connection/request/error counts (`-mix`). *(optional)* |
| `federation` | object | Federated-run outcome (`role`, `peer`, `peer_requests`, `peer_errors`, `merge_succeeded`, `merge_error`). *(optional)* |

### Pre-built binary releases

GitHub Releases ship platform tarballs (linux and darwin, amd64 and arm64) that orchestrators can fetch
directly:

```bash
TAG=v1.4.15
OS=linux
ARCH=amd64
curl -fsSL "https://github.com/goceleris/loadgen/releases/download/${TAG}/loadgen_${OS}_${ARCH}.tar.gz" \
  | tar xz -C /tmp/
chmod +x /tmp/loadgen-${OS}-${ARCH}
/tmp/loadgen-${OS}-${ARCH} -url http://target:8080/ -duration 10s
```

No separate `.sha256` sidecar ships. The release page lists each asset's SHA-256 digest, and
`gh release view <tag> --repo goceleris/loadgen --json assets` prints it as `digest`; compare it with
`shasum -a 256` of the file you downloaded.

### Verify a release

Every release tarball is built by the [release workflow](.github/workflows/release.yml) and, from the
release after v1.4.13 on, carries a signed [SLSA build provenance](https://slsa.dev/provenance/)
attestation, generated with `actions/attest-build-provenance` and recorded in GitHub's attestation
store. Before running a downloaded binary on a bench host, check that the asset really came from this
repository's release pipeline:

```bash
gh attestation verify loadgen_linux_amd64.tar.gz -R goceleris/loadgen
```

The command fails if the archive was tampered with after the build or was not produced by a
`goceleris/loadgen` workflow. Add `--format json` to inspect the attested source commit, workflow path
and builder. Releases published before the attestation step was added (v1.4.13 and earlier) have no
attestation and will fail verification.

## Architecture

```text
┌──────────────────────────────────────────────────────┐
│                     Benchmarker                      │
│   ┌──────────┐   ┌──────────┐         ┌──────────┐   │
│   │ Worker 0 │   │ Worker 1 │   ...   │ Worker N │   │
│   └────┬─────┘   └────┬─────┘         └────┬─────┘   │
│        │              │                    │         │
│   ┌────▼──────────────▼────────────────────▼─────┐   │
│   │               Client interface               │   │
│   │  ┌──────────┐  ┌──────────┐  ┌────────────┐  │   │
│   │  │ h1Client │  │ h2Client │  │ WS/SSE/mix │  │   │
│   │  └──────────┘  └──────────┘  └────────────┘  │   │
│   └──────────────────────────────────────────────┘   │
│                                                      │
│   ┌──────────────────────────────────────────────┐   │
│   │            ShardedLatencyRecorder            │   │
│   │     [Shard 0]  [Shard 1]  ...  [Shard N]     │   │
│   │   (per-worker HdrHistogram, merged at end)   │   │
│   └──────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────┘
```

### H1 worker model

Each worker owns a dedicated TCP connection (keep-alive). With `Connection: close`, each worker owns
`PoolSize` (16) connection slots, dialed up front, and every request travels on a connection of its own.
A connection the server closes, or announces it will close, never carries another request. The request bytes are
built once and written with a single write call. There is no synchronization between workers.

### H2 multiplexed model

N workers share M connections. Each connection has a write goroutine (serializes frame writes, allocates
stream IDs) and a read goroutine (dispatches responses to waiting workers via lock-free stream slots).
Workers acquire a stream semaphore, submit a write request, and wait on a pooled response channel.

### Latency recording

Each worker records into its own shard. The cumulative HdrHistogram is single-writer and lock-free; a
separate windowed histogram, used only for the 1-second `p99_ms` timeseries, takes a short per-shard lock
so the ticker goroutine can read and reset it without racing the writer. Request and byte counters are
batched into plain locals and flushed to atomics every 256 requests (H1, WebSocket, SSE) or 16 (the
HTTP/2 modes) to keep ARM64 memory-barrier overhead off the hot path. Shards merge once at completion for percentiles and the
exported histogram.

### Allocations: measured, and how to reproduce

The design goal is an allocation-free request/response cycle: request bytes are pre-formatted, HPACK
headers are pre-encoded, and H2 status codes are extracted from the encoded header block with no
allocation. It is a design target, not an invariant a test enforces. The Go benchmarks
(`go test -run '^$' -bench . -benchmem .`, in `bench_perf_test.go`, `h2status_test.go` and
`latency_test.go`) show 0 allocs/op for the mix dispatch and the HPACK status extraction, and cover the
h2c-upgrade request builder and the latency recorder, but not a full request cycle. The WebSocket and
SSE drivers allocate per message (a frame buffer per WebSocket write and read, a string per SSE line).

The whole-process figure in [Why loadgen](#why-loadgen) counts `runtime.MemStats.Mallocs` around `Run`,
against a server in another process. Measured on 22 November 2026 with v1.4.15, Go 1.27.3, an Apple M5
laptop (macOS, arm64) and a Go `net/http` server answering `ok` on the same host, with 64 H1
connections or 16 H2 workers (64 after the 4x scaling), a 2 s warmup and a 10 s run:
780,957 HTTP/1.1 requests with 12,251 mallocs (0.016 per request), and 1,021,955 HTTP/2 requests with
11,149 (0.011). The warmup's allocations are inside the count and its requests are not, and the host
was shared and loaded, so repeat runs vary: thirteen 10 s runs in all made 11,100 to 13,200 mallocs for
130,000 to 1,170,000 requests (0.01 to 0.09 per request), and four 2.5 s runs made 9,600 to 10,600. The
count is a roughly fixed setup cost, so the per-request ratio falls as throughput rises. To repeat it:

Take the [library example](#as-a-library), set `Warmup: 2 * time.Second`, `Connections: 64`,
`Workers: 64` and a `URL` served by another process, and wrap the `Run` call:

```go
var before, after runtime.MemStats
runtime.ReadMemStats(&before)
result, err := b.Run(context.Background())
runtime.ReadMemStats(&after)
if err != nil {
	panic(err)
}
fmt.Printf("requests=%d mallocs=%d\n", result.Requests, after.Mallocs-before.Mallocs)
```

For HTTP/2 add `HTTP2: true` and use `Workers: 16`.

## HTTP/2 flow control

The H2 client uses aggressive flow-control settings tuned for benchmark throughput rather than production
fairness.

### Settings

| Parameter | Value | RFC 7540 default | Rationale |
| --- | --- | --- | --- |
| Initial window size | 16 MB | 64 KB | Prevents stalls with large response bodies. |
| Max frame size | 64 KB | 16 KB | Balances framing overhead against flow-control granularity. |
| Header table size | 0 | 4096 | Allocation-free status-code extraction from HPACK headers. |

### Server-preface window

During the handshake the client captures the connection-level `WINDOW_UPDATE(stream 0)` a server sends in
its preface, before its `SETTINGS` ack, and seeds its connection send window with it. Servers such as
Kestrel/ASP.NET grow the connection flow-control window this way (65535 → their configured
`InitialConnectionWindowSize`, e.g. 1 MiB). Before v1.4.13 this update was dropped, stranding the send
window at the RFC 7540 §6.9.2 floor of 65535 and deadlocking sustained request-body sends (fixed in
[#69](https://github.com/goceleris/loadgen/pull/69)).

### `WINDOW_UPDATE` batching

The read loop accumulates consumed bytes in an atomic counter. The write loop flushes a single
`WINDOW_UPDATE` between request batches and on a 1 ms idle ticker, amortizing frame overhead while
preventing flow-control stalls.

### Stream concurrency

A semaphore sized to `min(server MAX_CONCURRENT_STREAMS, configured MaxStreams)` gates in-flight streams;
stream slots are allocated at 2× that size for wrap-around headroom during bursts. The server's value is
the one from its initial `SETTINGS`: a lower value it sends later is acknowledged but not applied
([#99](https://github.com/goceleris/loadgen/issues/99)).

### Tuning guidance

- **CPU-bound:** add connections. More TCP sockets means more kernel parallelism across cores.
- **IO-bound:** add streams. More multiplexed requests per connection saturates bandwidth.
- **Large responses:** the client refills the connection window continuously but never sends a
  stream-level `WINDOW_UPDATE`, so a single response body larger than the 16 MB initial window stalls
  its stream.

## Configuration reference

Beyond the flags above, the library `Config` exposes knobs the CLI leaves at their defaults:

| Field | Default | Notes |
| --- | --- | --- |
| `DialTimeout` | `10s` | TCP connect / reconnect timeout. |
| `ReadBufferSize` / `WriteBufferSize` | 256 KB (H1), 2 MB (H2) | Kernel socket buffer sizes (`SO_RCVBUF` / `SO_SNDBUF`) for the H1 and H2 clients. |
| `PoolSize` | `16` | Connection slots per worker in `Connection: close` mode, dialed up front. After a slot's first request, each request dials a fresh connection, so the pool does not hide the dial. |
| `MaxResponseSize` | 10 MB | Cap on response-body bytes read by the H1 client (a larger response is an error); `-1` for unlimited. |
| `TLSConfig` | | Client certs, custom CA pool, or cipher suites for the H1 and H2 clients over HTTPS (the WebSocket and SSE drivers do not use it). |

See the [package reference](https://pkg.go.dev/github.com/goceleris/loadgen) for the full `Config` and
`Result` types.

## Extending with a custom client

For protocols loadgen does not ship (gRPC, QUIC, …), implement the exported `Client` interface and inject
it via `Config.Client`. When set, the built-in H1/H2 client creation is skipped and your client drives
every worker.

```go
type Client interface {
	DoRequest(ctx context.Context, workerID int) (bytesRead int, err error)
	Close()
}

cfg := loadgen.Config{
	URL:         "http://localhost:50051/", // still required: Validate() enforces an http/https URL
	Duration:    15 * time.Second,
	Connections: 64, // required: New() rejects Connections < 1
	Workers:     64,
	Client:      &myGRPCClient{}, // implements loadgen.Client
}
```

Even with a custom `Client`, `Config.URL` must be a valid `http`/`https` URL: `Config.Validate()` enforces
it before your client runs, and your client is free to reinterpret the target however it needs.
WebSocket and SSE are built in; reach for `-mode` / `Config.Mode` rather than a custom client for those.

## Status and stability

loadgen is young: the first commit is from March 2026, the latest release is v1.4.15, and its main
user is the celeris benchmark cluster. Releases are `vX.Y.Z` tags; no compatibility policy is written
down beyond that, and the API has had little use outside goceleris.

- **Versioning and comparability.** Releases are `vX.Y.Z` tags with release notes. Every result carries
  `loadgen_version`, and rated results carry `scheduler_shards`: compare numbers only between runs of
  the same version, and of the same shard rule for rated runs.
- **What is checked.** CI runs `go test -race`, `govulncheck` and the celeris integration matrix on
  `ubuntu-latest`, and the root package's Go benchmarks run on
  [CodSpeed](https://codspeed.io/goceleris/loadgen) on every push to `main` and on pull requests that
  change the root package's Go files, `go.mod` or `go.sum`. The CodSpeed report is informational, and
  two of its benchmarks swing on identical code ([#109](https://github.com/goceleris/loadgen/issues/109)).
- **What is not covered.** CI runs on Linux only, and release binaries are built for Linux and macOS only (nothing
  for Windows); the CPU sampler and receive-queue probe work on Linux only. The H1 and H2 clients
  are written for benchmarking, not for general use: the open
  [bug issues](https://github.com/goceleris/loadgen/issues?q=is%3Aopen+label%3Abug) include edge cases of
  H1 response framing ([#93](https://github.com/goceleris/loadgen/issues/93)), H2 GOAWAY, late frames
  and late SETTINGS handling ([#92](https://github.com/goceleris/loadgen/issues/92),
  [#98](https://github.com/goceleris/loadgen/issues/98),
  [#99](https://github.com/goceleris/loadgen/issues/99)), no retry of an idempotent request on a
  connection the server closed while idle ([#94](https://github.com/goceleris/loadgen/issues/94)) and
  a dial that cannot be cancelled at the end of a run, which can deflate `requests_per_sec`
  ([#95](https://github.com/goceleris/loadgen/issues/95)). There is one URL per run (weighted paths are
  [#22](https://github.com/goceleris/loadgen/issues/22)), no gRPC
  ([#21](https://github.com/goceleris/loadgen/issues/21)) and no HTML report
  ([#23](https://github.com/goceleris/loadgen/issues/23)).
- **Known apparatus issue: rated mode ([#102](https://github.com/goceleris/loadgen/issues/102), open).**
  A rated run is paced by scheduler goroutines feeding one shared channel, which can cap the rate one
  process delivers, and its sub-millisecond waits are rounded up to about 1 ms by the Go runtime when the
  process is otherwise idle. v1.4.15 emits the schedule from several goroutines above 80k req/s
  (amd64 evidence only, see the release notes); the issue stays open, so read a rated result against
  its `target_rps` and `requests_per_sec` (probatorium reports the achieved ratio per pass).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the build, test and pull-request flow.

## Security

Report a vulnerability privately as described in [SECURITY.md](SECURITY.md); please do not open a public
issue for it.

## License

Apache License 2.0; see [LICENSE](LICENSE).

## Related projects

| Project | What it is |
| --- | --- |
| [celeris](https://github.com/goceleris/celeris) | The HTTP engine for Go whose published benchmarks loadgen measures |
| [probatorium](https://github.com/goceleris/probatorium) | The benchmark and validation harness that drives loadgen on the bench cluster |
| [docs](https://github.com/goceleris/docs) | The source of [goceleris.dev](https://goceleris.dev): documentation and the benchmark dashboard |
