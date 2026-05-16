# Changelog

All notable changes to `github.com/goceleris/loadgen` are documented in
this file. Versioning follows semver (MAJOR.MINOR.PATCH) with the same
release cadence as `github.com/goceleris/celeris`.

## v1.4.4 — 2026-05-16

This release lands the "wave 11" feature set that probatorium needs to
compute its headline metric `latency_at_slo` and to validate cross-arch
soak behaviour at sustained 10K+ RPS.

### Added

- **Rated mode** (`-rate <constant>`, `Config.RateRPS`) — scheduled-send
  benchmarker with Gil-Tene-style coordinated-omission correction. The
  latency interval starts at the *intended* dispatch deadline, so a slow
  server / GC / scheduler pause shows up in the histogram instead of
  being silently absorbed by closed-loop backpressure. Saturation mode
  (`-rate 0`) is unchanged and still the default.
  Closes #48.

- **HdrHistogram emission** — `Result.Latency.Histogram` now carries a
  V2-compressed payload of the merged per-worker hdrhistograms. Existing
  `P50/P75/P90/P99/P999/P9999` summary fields are preserved for back
  compat. Downstream readers (probatorium, perfmatrix) can re-merge
  across hosts / arches / git refs without re-running the benchmark.
  Closes #49.

- **EPOLLIN backlog probe** (`-recvq-probe`) — every 5s, sample
  `/proc/<pid>/net/sockstat` plus per-conn receive-queue depth. When the
  median recv-Q exceeds 64 KB sustained for 10 s, latch
  `Result.LoadgenRecvQHigh = true` so reports flag cells where loadgen
  itself is the bottleneck. Linux-only; the `recvq_other.go` stub keeps
  the field zero on Darwin/Windows.
  Closes #50.

- **1 Hz self-CPU sampler** (`-cpu-monitor`) — per-process CPU
  percentage sampled at 1 Hz, P95 emitted as `Result.CpuPctP95`.
  Probatorium uses this to fail cells where loadgen-side CPU breached
  70 % (the configured "loadgen-bound" floor).

- **Multi-host loadgen federation** (`-peer`, `-sidecar`) —
  coordinator / sidecar mode. The coordinator drives one host; one or
  more sidecars on peer hosts hit the same target in parallel; per-host
  HdrHistograms are streamed back and merged so downstream tools see
  one unified `Result` for the whole fleet. Required for high-RPS
  scenarios where a single loadgen host saturates its NIC before the
  server saturates its cores.
  Closes #51.

### Changed

- `cmd/loadgen/main.go` now exposes `-rate`, `-peer`, `-sidecar`,
  `-out`, `-cpu-monitor`, `-recvq-probe` flags directly. The previous
  reservation guard ("reserved for loadgen v2") is removed.

- `go.mod` bumped: `github.com/goceleris/celeris v1.4.1 → v1.4.3`,
  `golang.org/x/net v0.53.0 → v0.54.0` (plus indirect bumps from
  `go mod tidy`).

### Fixed

- (No bug fixes in this release; v1.4.3 fixes `h1.Unblock` via
  `close(conn)` carry forward unchanged.)

### Compatibility

- Wire-compatible with celeris ≥ v1.4.3. Probatorium v0.x consumes this
  release; the v1.4.4 cycle of celeris also bumps to this loadgen via
  goceleris/celeris#272.

## Older releases

For releases prior to v1.4.4, see the
[GitHub releases page](https://github.com/goceleris/loadgen/releases).
