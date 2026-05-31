// Package loadgen provides a high-performance HTTP benchmarking tool.
package loadgen

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// HTTP2Options holds HTTP/2-specific settings.
type HTTP2Options struct {
	// Connections is the number of HTTP/2 TCP connections to open.
	// HTTP/2 multiplexes streams, but multiple connections avoid
	// head-of-line blocking and utilize multi-core server architectures.
	// Must be >= 1 when HTTP2 is true. Default: 16.
	Connections int

	// MaxStreams is the maximum concurrent streams per HTTP/2 connection.
	// The actual limit is the minimum of this value and the server's
	// SETTINGS_MAX_CONCURRENT_STREAMS. Must be >= 1 when HTTP2 is true.
	// Default: 100.
	MaxStreams int
}

// Config holds benchmark configuration.
type Config struct {
	// URL is the target server URL (required).
	// Must include scheme (http:// or https://) and host.
	// Path defaults to "/" if not specified.
	URL string

	// Method is the HTTP method to use (default: "GET").
	// Supported: GET, HEAD, POST, PUT, DELETE, PATCH, OPTIONS.
	Method string

	// Body is the request body payload, sent with every request.
	// Typically used with POST/PUT methods. When non-empty, a
	// Content-Length header is automatically added.
	Body []byte

	// Headers are custom HTTP headers added to every request.
	// The Connection header is managed automatically based on
	// DisableKeepAlive and should not be set here. For HTTP/2,
	// hop-by-hop headers (Connection, Keep-Alive, etc.) are
	// automatically stripped per RFC 9113.
	Headers map[string]string

	// Duration is the benchmark run time (required, must be positive).
	// The warmup phase runs before this and is not included.
	Duration time.Duration

	// Connections is the number of TCP connections for HTTP/1.1 mode.
	// Each worker owns one connection in keep-alive mode. In close mode,
	// each worker owns PoolSize connections and round-robins through them.
	// Must be >= 1 when HTTP2 is false. Default: 256.
	Connections int

	// Workers is the number of concurrent goroutines sending requests.
	// For HTTP/1.1, this typically equals Connections.
	// For HTTP/2, workers are multiplied by 4 internally to saturate
	// multiplexed streams. Must be >= 1. Default: 64.
	Workers int

	// Warmup is the warmup duration before the measured benchmark begins.
	// During warmup, 75% of workers send requests to warm connection pools
	// and give the server a realistic load preview. Set to 0 to skip.
	// Default: 5s.
	Warmup time.Duration

	// DisableKeepAlive disables HTTP keep-alive (Connection: close mode).
	// When true, the server closes connections after each response and
	// the client round-robins through a pool of PoolSize connections per
	// worker to hide reconnection latency. Default: false (keep-alive on).
	DisableKeepAlive bool

	// HTTP2 enables HTTP/2 over cleartext (h2c) mode.
	// When true, the client uses HTTP/2 multiplexed streams over
	// HTTP2Options.Connections TCP connections. When false, plain
	// HTTP/1.1 is used with one request per connection at a time.
	HTTP2 bool

	// H2CUpgrade enables the RFC 7540 §3.2 h2c upgrade handshake: each new
	// connection starts as H1 carrying Connection: Upgrade, HTTP2-Settings
	// + Upgrade: h2c headers, reads a 101 Switching Protocols response, then
	// switches to H2 on the same TCP conn. Mutually exclusive with HTTP2:
	// Validate() returns an error if both are set. Over TLS this is undefined
	// and also rejected (servers negotiate H2 via ALPN, not h2c). Only meaningful
	// when the URL scheme is "http".
	H2CUpgrade bool

	// Mix, if non-nil, assigns each new connection to a protocol via a weighted
	// random draw across H1, H2 prior-knowledge, and h2c-upgrade. Mutually
	// exclusive with HTTP2 and H2CUpgrade. See ParseMixRatio for the string
	// format. HTTP2Options.Connections controls how many H2 connections are
	// dialled; the H1 pool scales with Connections as usual. A conn stays on
	// its chosen protocol for its entire lifetime.
	Mix *MixRatio

	// HTTP2Options holds HTTP/2-specific tuning parameters.
	// Only used when HTTP2 is true. HTTP/2 multiplexes streams over
	// a single connection, but multiple connections can improve
	// throughput by avoiding head-of-line blocking.
	HTTP2Options HTTP2Options

	// DialTimeout is the timeout for establishing TCP connections.
	// Applies to both initial connection setup and reconnects.
	// Default: 10s. Must be non-negative.
	DialTimeout time.Duration

	// ReadBufferSize is the TCP read buffer size in bytes.
	// Larger buffers reduce syscall overhead for high-throughput workloads.
	// Default: 256KB for HTTP/1.1, 2MB for HTTP/2. Must be non-negative.
	ReadBufferSize int

	// WriteBufferSize is the TCP write buffer size in bytes.
	// Larger buffers reduce syscall overhead for high-throughput workloads.
	// Default: 256KB for HTTP/1.1, 2MB for HTTP/2. Must be non-negative.
	WriteBufferSize int

	// PoolSize is the number of connections per worker in Connection: close mode.
	// Workers round-robin through the pool so reconnection latency is hidden.
	// Only used when DisableKeepAlive is true. Default: 16.
	PoolSize int

	// MaxResponseSize is the maximum bytes to read from a response body.
	// Responses exceeding this limit return an error. Set to -1 for unlimited.
	// Default: 10MB (10485760 bytes). Must be >= -1.
	MaxResponseSize int64

	// TLSConfig specifies custom TLS configuration for HTTPS connections.
	// When nil and the URL scheme is https, a default TLS config is used.
	// Use this for client certificates, custom CA pools, or cipher suites.
	TLSConfig *tls.Config

	// InsecureSkipVerify skips TLS certificate verification.
	// Only use for testing with self-signed certificates.
	InsecureSkipVerify bool

	// MaxRPS limits total requests per second across all workers.
	// When zero (default), runs in open-loop mode with no rate limit.
	// Rate is distributed evenly across workers using a token bucket.
	MaxRPS int

	// Rate, when > 0, switches the benchmark from saturation mode (dispatch
	// as fast as possible) to constant-rate mode. The scheduler emits one
	// request every (1/Rate) seconds. Latency is measured from the
	// *intended* dispatch time, not the actual one — this is Gil Tene's
	// coordinated-omission correction. When the server stalls, a backlog
	// of unsent intended timestamps accumulates and surfaces as honest
	// tail latency.
	Rate float64

	// Peer enables federation: the loadgen instance becomes the
	// coordinator for a sidecar instance running at this host:port. Both
	// instances run identical workloads against the target; at end-of-run
	// the coordinator pulls the sidecar's V2-compressed histogram and
	// merges it into its own. Empty disables federation.
	Peer string

	// CPUMonitor toggles the loadgen-process CPU sampler (1Hz, P95 emitted
	// on Result.CPUPctP95). True by default in DefaultConfig.
	CPUMonitor bool

	// RecvQProbe toggles the per-socket receive-queue probe (5s cadence on
	// Linux, no-op elsewhere). RecvQHigh on Result is true when median
	// recv-Q exceeds 64KB sustained ≥10s.
	RecvQProbe bool

	// OnProgress is called approximately every second during the benchmark
	// with a snapshot of current results. The snapshot is a copy safe for
	// concurrent access. Must not block — long-running callbacks delay
	// timeseries collection. If nil, no progress reporting occurs.
	OnProgress func(elapsed time.Duration, snapshot Result)

	// Client allows injecting a custom protocol client implementation.
	// When set, the built-in H1/H2 client creation is skipped entirely,
	// and URL, HTTP2, HTTP2Options, DialTimeout, buffer sizes, PoolSize,
	// MaxResponseSize, DisableKeepAlive, and Headers are not used for
	// client creation (though they remain available via the Config).
	Client Client

	// Mode selects a non-HTTP driver. "" = normal HTTP. WebSocket modes:
	// "ws-echo", "ws-large-echo", "ws-hub" use the WS client; "sse-fanout"
	// uses the SSE client (one GET /events stream per worker, one event per
	// request). When set, Mode is mutually exclusive with HTTP2/Mix/H2CUpgrade.
	Mode string

	// scheme is set internally by New() from the parsed URL.
	// Defaults to "http" when empty (e.g., in tests calling newH1Client/newH2Client directly).
	scheme string
}

// DefaultConfig returns sensible defaults for benchmarking.
// Note: The main benchmark runner now auto-scales workers and connections
// based on available CPUs. These defaults are fallbacks for direct library usage.
func DefaultConfig() Config {
	return Config{
		Method:           "GET",
		Duration:         30 * time.Second,
		Connections:      256,
		Workers:          64, // Higher default for direct library use
		Warmup:           5 * time.Second,
		DisableKeepAlive: false,
		HTTP2Options: HTTP2Options{
			Connections: 16,  // Multiple H2 connections for better throughput
			MaxStreams:  100, // Concurrent streams per connection
		},
		CPUMonitor: true,
		RecvQProbe: true,
	}
}

// Validate checks that the configuration is valid. It should be called after
// defaults have been applied. New() calls Validate() automatically.
func (c Config) Validate() error {
	if c.URL == "" {
		return errors.New("loadgen: URL is required")
	}
	u, err := url.Parse(c.URL)
	if err != nil {
		return fmt.Errorf("loadgen: invalid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("loadgen: unsupported URL scheme %q (must be http or https)", u.Scheme)
	}
	if c.HTTP2 && c.H2CUpgrade {
		return errors.New("loadgen: HTTP2 and H2CUpgrade are mutually exclusive")
	}
	if c.H2CUpgrade && u.Scheme == "https" {
		return errors.New("loadgen: H2CUpgrade requires http scheme (TLS uses ALPN, not h2c)")
	}
	if c.Mode != "" {
		switch c.Mode {
		case wsModeEcho, wsModeLargeEcho, wsModeHub, sseMode:
		default:
			return fmt.Errorf("loadgen: unknown mode %q", c.Mode)
		}
		if c.HTTP2 || c.Mix != nil || c.H2CUpgrade {
			return errors.New("loadgen: Mode is mutually exclusive with HTTP2, Mix, and H2CUpgrade")
		}
	}
	if c.Mix != nil {
		if c.HTTP2 || c.H2CUpgrade {
			return errors.New("loadgen: Mix is mutually exclusive with HTTP2 and H2CUpgrade")
		}
		if u.Scheme == "https" && c.Mix.Upgrade > 0 {
			return errors.New("loadgen: Mix with non-zero h2c-upgrade weight requires http scheme")
		}
		if c.Mix.H1+c.Mix.H2+c.Mix.Upgrade == 0 {
			return errors.New("loadgen: Mix must have at least one non-zero weight")
		}
	}
	if c.Duration <= 0 {
		return errors.New("loadgen: Duration must be positive")
	}
	if c.Rate < 0 {
		return errors.New("loadgen: Rate must be non-negative")
	}
	usesH2Pool := c.HTTP2 || c.H2CUpgrade || (c.Mix != nil && (c.Mix.H2 > 0 || c.Mix.Upgrade > 0))
	if !usesH2Pool && c.Connections < 1 {
		return errors.New("loadgen: Connections must be >= 1")
	}
	if c.Workers < 1 {
		return errors.New("loadgen: Workers must be >= 1")
	}
	if usesH2Pool && c.HTTP2Options.Connections < 1 {
		return errors.New("loadgen: HTTP2Options.Connections must be >= 1")
	}
	if usesH2Pool && c.HTTP2Options.MaxStreams < 1 {
		return errors.New("loadgen: HTTP2Options.MaxStreams must be >= 1")
	}
	switch c.Method {
	case "GET", "HEAD", "POST", "PUT", "DELETE", "PATCH", "OPTIONS":
	default:
		return fmt.Errorf("loadgen: unsupported HTTP method %q", c.Method)
	}
	for k, v := range c.Headers {
		if k == "" {
			return errors.New("loadgen: empty header key")
		}
		if strings.ContainsAny(k, "\r\n") || strings.ContainsAny(v, "\r\n") {
			return fmt.Errorf("loadgen: header %q contains invalid characters (CR/LF)", k)
		}
	}
	if c.MaxResponseSize < -1 {
		return errors.New("loadgen: MaxResponseSize must be >= -1")
	}
	if c.DialTimeout < 0 {
		return errors.New("loadgen: DialTimeout must be non-negative")
	}
	if c.ReadBufferSize < 0 {
		return errors.New("loadgen: ReadBufferSize must be non-negative")
	}
	if c.WriteBufferSize < 0 {
		return errors.New("loadgen: WriteBufferSize must be non-negative")
	}
	return nil
}

// Client is the interface for protocol-specific benchmark clients.
// Implement this interface to add support for custom protocols.
type Client interface {
	DoRequest(ctx context.Context, workerID int) (bytesRead int, err error)
	Close()
}

// Benchmarker runs HTTP benchmarks.
type Benchmarker struct {
	config Config
	raw    Client

	// mix is a cached pointer to raw when raw is a *mixClient. nil otherwise.
	// The worker hot path reads this once on shutdown-filter and avoids a
	// type assertion per request. Set at New() time, never mutated.
	mix *mixClient

	// Metrics — errors use atomic (rare, no contention).
	// Requests and bytesRead are tracked per-shard in latencies.
	errors atomic.Int64

	// Latency tracking (also holds per-shard request/bytes counters)
	latencies *ShardedLatencyRecorder

	// Control
	running atomic.Bool
	wg      sync.WaitGroup
}

// parseURL extracts host, port, path, and scheme from a raw URL.
func parseURL(rawURL string) (host, port, path, scheme string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", "", fmt.Errorf("parse URL: %w", err)
	}
	scheme = u.Scheme
	host = u.Hostname()
	port = u.Port()
	if port == "" {
		if scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	path = u.RequestURI()
	if path == "" {
		path = "/"
	}
	return host, port, path, scheme, nil
}

// New creates a new Benchmarker with the given configuration.
// Returns an error if the configuration is invalid or if the initial
// connection setup fails.
func New(cfg Config) (*Benchmarker, error) {
	// Apply defaults for zero-value fields
	if cfg.Method == "" {
		cfg.Method = "GET"
	}
	if cfg.HTTP2Options.Connections == 0 {
		cfg.HTTP2Options.Connections = 16
	}
	if cfg.HTTP2Options.MaxStreams == 0 {
		cfg.HTTP2Options.MaxStreams = 100
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 10 * time.Second
	}
	if cfg.MaxResponseSize == 0 {
		cfg.MaxResponseSize = 10 << 20 // 10MB
	}
	useH2Defaults := cfg.HTTP2 || cfg.H2CUpgrade || (cfg.Mix != nil && (cfg.Mix.H2 > 0 || cfg.Mix.Upgrade > 0))
	if cfg.ReadBufferSize == 0 {
		if useH2Defaults {
			cfg.ReadBufferSize = 2 * 1024 * 1024 // 2MB for H2
		} else {
			cfg.ReadBufferSize = 256 * 1024 // 256KB for H1
		}
	}
	if cfg.WriteBufferSize == 0 {
		if useH2Defaults {
			cfg.WriteBufferSize = 2 * 1024 * 1024 // 2MB for H2
		} else {
			cfg.WriteBufferSize = 256 * 1024 // 256KB for H1
		}
	}
	if cfg.PoolSize == 0 {
		cfg.PoolSize = 16
	}

	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	flushInterval := defaultFlushInterval // 256 for H1
	if useH2Defaults {
		flushInterval = 16 // ~4 flushes/sec/worker at ~69 rps/worker
	}

	// Use custom client if provided
	if cfg.Client != nil {
		return &Benchmarker{
			config:    cfg,
			raw:       cfg.Client,
			latencies: NewShardedLatencyRecorder(cfg.Workers, flushInterval),
		}, nil
	}

	host, port, path, scheme, err := parseURL(cfg.URL)
	if err != nil {
		return nil, fmt.Errorf("loadgen: %w", err)
	}
	cfg.scheme = scheme

	var raw Client

	switch {
	case cfg.Mode == sseMode:
		raw, err = newSSEClient(host, port, path, cfg)
	case cfg.Mode != "":
		raw, err = newWSClient(host, port, path, cfg)
	case cfg.Mix != nil:
		// H2/upgrade conns are multiplexed like normal H2 — bump workers for
		// the same reason as the H2 case below.
		cfg.Workers *= 4
		raw, err = newMixClient(host, port, path, cfg)
	case cfg.HTTP2:
		// H2 workers are I/O-bound (blocked on channel round-trips), not CPU-bound.
		// With multiplexed streams, workers need 4x headroom to keep the pipeline
		// saturated. Little's Law: throughput = workers / RTT.
		cfg.Workers *= 4
		raw, err = newH2Client(host, port, path, cfg)
	case cfg.H2CUpgrade:
		cfg.Workers *= 4
		raw, err = newH2CUpgradeClient(host, port, path, cfg)
	default:
		raw, err = newH1Client(host, port, path, cfg)
	}

	if err != nil {
		return nil, fmt.Errorf("loadgen: dial: %w", err)
	}

	b := &Benchmarker{
		config:    cfg,
		raw:       raw,
		latencies: NewShardedLatencyRecorder(cfg.Workers, flushInterval),
	}
	if mc, ok := raw.(*mixClient); ok {
		b.mix = mc
	}
	return b, nil
}

// Run executes the benchmark and returns results.
//
// When the Config supplies a non-empty Peer, Run additionally dials the
// peer sidecar over TCP, kicks it off in lockstep, and merges the
// sidecar's HdrHistogram into the local one before returning. The merged
// histogram lands on Result.Histogram; FederationStats reports the merge
// outcome. Federation is best-effort: failure to reach the peer is
// recorded on Result.Federation but does not fail the whole run.
func (b *Benchmarker) Run(ctx context.Context) (*Result, error) {
	// Warmup phase
	if b.config.Warmup > 0 {
		b.warmup(ctx)
	}

	// Reset metrics for actual benchmark
	b.errors.Store(0)
	b.latencies.Reset()
	if b.mix != nil {
		// Clear per-protocol counters accumulated during warmup.
		b.mix.h1Requests.Store(0)
		b.mix.h2Requests.Store(0)
		b.mix.upgradeRequests.Store(0)
		b.mix.h1Errors.Store(0)
		b.mix.h2Errors.Store(0)
		b.mix.upgradeErrors.Store(0)
	}

	// Federation: dial the peer before workers spin up so the start
	// signal lands at a well-defined moment relative to local launch.
	var fed *FederationCoordinator
	var fedStats *FederationStats
	if b.config.Peer != "" {
		fed = NewFederationCoordinator(b.config.Peer, 30*time.Second)
		fedStats = &FederationStats{Role: "primary", Peer: b.config.Peer}
		if err := fed.Dial(ctx); err != nil {
			fedStats.MergeError = err.Error()
			fed = nil // disable downstream collection
		} else {
			startAt := time.Now().Add(500 * time.Millisecond)
			if err := fed.Start(startAt, b.config); err != nil {
				fedStats.MergeError = err.Error()
				_ = fed.Close()
				fed = nil
			} else {
				// Sleep until the agreed launch instant so both sides start
				// near-simultaneously.
				wait := time.Until(startAt)
				if wait > 0 {
					t := time.NewTimer(wait)
					select {
					case <-ctx.Done():
						t.Stop()
					case <-t.C:
					}
				}
			}
		}
	}

	// Start client CPU monitor (whole-system) and the loadgen-self sampler.
	cpuMon := &CPUMonitor{}
	cpuMon.Start()

	var selfCPU *SelfCPUSampler
	if b.config.CPUMonitor {
		selfCPU = NewSelfCPUSampler(600)
		selfCPU.Start()
	}

	// Create a scoped context for this benchmark run. Workers use this context
	// so their in-flight HTTP requests are cancelled when the benchmark ends,
	// preventing wg.Wait() from hanging if the server stops responding.
	// Allow Duration + 60s for in-flight requests to drain.
	runCtx, runCancel := context.WithTimeout(ctx, b.config.Duration+60*time.Second)
	defer runCancel()

	// Per-socket recv-Q probe (Linux only). The probe is best-effort: when
	// it cannot read /proc/net/tcp it stays silent and RecvQHigh remains
	// false. Caller filters by loadgen's open socket inodes.
	var recvqProbe *recvQProbe
	if b.config.RecvQProbe {
		recvqProbe = newRecvQProbe()
		recvqProbe.Start(runCtx)
	}

	// Timeseries collection: 1-second snapshots
	var timeseries []TimeseriesPoint
	var prevReqs int64
	var prevErrors int64

	// Start workers — each gets a unique workerID for connection partitioning
	b.running.Store(true)
	start := time.Now()

	// Rated mode: a single scheduler goroutine emits intended-dispatch
	// timestamps into a buffered channel; workers pop slots and run requests.
	// When the server stalls, slots queue up (capacity = 4× rate) — the
	// queueing time itself becomes part of the recorded latency, which is
	// the coordinated-omission correction.
	var slots chan time.Time
	var schedDone chan struct{}
	if b.config.Rate > 0 {
		bufLen := int(b.config.Rate * 4)
		if bufLen < 1024 {
			bufLen = 1024
		}
		if bufLen > 1<<20 {
			bufLen = 1 << 20
		}
		slots = make(chan time.Time, bufLen)
		schedDone = make(chan struct{})
		go b.rateScheduler(runCtx, start, slots, schedDone)
	}

	for i := range b.config.Workers {
		workerID := i
		if slots != nil {
			b.wg.Go(func() { b.ratedWorker(runCtx, workerID, slots) })
		} else {
			b.wg.Go(func() { b.worker(runCtx, workerID) })
		}
	}

	// Timeseries ticker: collect 1-second throughput snapshots
	ticker := time.NewTicker(1 * time.Second)
	tickerStop := make(chan struct{})
	tickerDone := make(chan struct{})
	go func() {
		defer close(tickerDone)
		for {
			select {
			case <-ticker.C:
				reqs, _ := b.latencies.Totals()
				elapsed := time.Since(start).Seconds()
				deltaReqs := reqs - prevReqs
				prevReqs = reqs
				p99 := b.latencies.SnapshotWindowP99Ms()
				curErr := b.errors.Load()
				timeseries = append(timeseries, TimeseriesPoint{
					TimestampSec:   elapsed,
					RequestsPerSec: float64(deltaReqs), // 1-second window
					P99Ms:          p99,
					Errors:         curErr - prevErrors,
				})
				prevErrors = curErr
				if b.config.OnProgress != nil {
					snapshot := Result{
						Requests:       reqs,
						Duration:       time.Duration(elapsed * float64(time.Second)),
						RequestsPerSec: float64(deltaReqs),
					}
					b.config.OnProgress(time.Duration(elapsed*float64(time.Second)), snapshot)
				}
			case <-tickerStop:
				return
			}
		}
	}()

	// Wait for duration
	select {
	case <-ctx.Done():
	case <-time.After(b.config.Duration):
	}

	b.running.Store(false)
	ticker.Stop()
	close(tickerStop)

	// Cancel the run context to unblock any workers stuck in HTTP requests.
	// Workers check b.running first, but if they're mid-request, context
	// cancellation ensures they don't hang past the HTTP client timeout.
	runCancel()

	if schedDone != nil {
		// rateScheduler closes slots on its own way out. Wait for it so
		// ratedWorker goroutines can observe the close and exit.
		<-schedDone
	}

	// Close connections to interrupt any pending I/O, allowing workers to exit.
	b.raw.Close()

	b.wg.Wait()
	<-tickerDone

	elapsed := time.Since(start)

	// Flush unflushed local counters to atomics now that workers have stopped.
	b.latencies.FlushLocal()

	result := b.buildResult(elapsed)
	result.ClientCPUPercent = cpuMon.Stop()
	result.Timeseries = timeseries
	if selfCPU != nil {
		result.CPUPctP95 = selfCPU.Stop()
	}
	if recvqProbe != nil {
		result.RecvQHigh = recvqProbe.Stop()
	}

	// Collect peer histogram if federation is active.
	if fed != nil {
		local := b.latencies.MergedHistogram()
		peerReqs, peerErrs, merged, err := fed.CollectResult(local)
		_ = fed.Close()
		if err != nil {
			fedStats.MergeError = err.Error()
			fedStats.MergeSucceeded = false
		} else {
			fedStats.PeerRequests = peerReqs
			fedStats.PeerErrors = peerErrs
			fedStats.MergeSucceeded = true
			if merged != nil && merged.TotalCount() > 0 {
				if blob, encErr := merged.Encode(hdrhistogram.V2CompressedEncodingCookieBase); encErr == nil {
					result.Histogram = blob
					result.Latency = percentilesFromHistogram(merged)
				}
			}
		}
	}
	if fedStats != nil {
		result.Federation = fedStats
	}

	return result, nil
}

func (b *Benchmarker) warmup(ctx context.Context) {
	warmupCtx, cancel := context.WithTimeout(ctx, b.config.Warmup)
	defer cancel()

	b.running.Store(true)

	// Use 75% of workers for warmup to properly warm up connection pools
	// and give the server a realistic preview of the load
	warmupWorkers := max((b.config.Workers*3)/4, 4)
	// Don't exceed actual worker count
	if warmupWorkers > b.config.Workers {
		warmupWorkers = b.config.Workers
	}

	for i := range warmupWorkers {
		workerID := i
		b.wg.Go(func() { b.worker(warmupCtx, workerID) })
	}

	<-warmupCtx.Done()
	b.running.Store(false)
	b.wg.Wait()
}

// rateScheduler emits intended-dispatch timestamps at the configured Rate.
// It closes slots when the run context is cancelled or when running flips
// false, so ratedWorker goroutines can drop out cleanly.
//
// The scheduler does NOT skip past stalled wakeups — every slot is enqueued
// even if it lands in the past. Workers see the original intended timestamp
// and record latency relative to it. That's the coordinated-omission
// correction: queueing time at the loadgen side becomes visible tail
// latency, instead of being silently dropped because "the worker was busy."
func (b *Benchmarker) rateScheduler(ctx context.Context, start time.Time, slots chan<- time.Time, done chan<- struct{}) {
	defer close(done)
	defer func() {
		// Tell workers no more slots are coming. Channel is buffered, so
		// any pending slots remain consumable until drained.
		// Closing the channel here is racy with the ratedWorker send-side
		// of nothing — only sender closes, by design.
		// Use a recover to swallow a panic if a second close ever happens.
		defer func() { _ = recover() }()
		// We're the sole sender, so closing here is safe.
		// (Caller does not also close.)
		closeChanIfOpen(slots)
	}()

	period := time.Duration(float64(time.Second) / b.config.Rate)
	if period <= 0 {
		return
	}

	next := start.Add(period)
	for b.running.Load() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		now := time.Now()
		if now.Before(next) {
			t := time.NewTimer(next.Sub(now))
			select {
			case <-ctx.Done():
				t.Stop()
				return
			case <-t.C:
			}
		}

		// Enqueue the *intended* time, not the actual "now". If the buffer
		// is full the server is far behind — block briefly so the
		// schedule pressure shows up as latency at workers, then keep going.
		select {
		case <-ctx.Done():
			return
		case slots <- next:
		}
		next = next.Add(period)
	}
}

// closeChanIfOpen closes a chan time.Time, swallowing the "already closed"
// panic if the close races (rateScheduler is sole sender, so this is just
// defence-in-depth).
func closeChanIfOpen(ch chan<- time.Time) {
	defer func() { _ = recover() }()
	close(ch)
}

// ratedWorker pulls intended-time slots from the scheduler and dispatches.
// Latency is measured from the intended dispatch time, not from when the
// worker actually fired — this is the coordinated-omission correction.
func (b *Benchmarker) ratedWorker(ctx context.Context, workerID int, slots <-chan time.Time) {
	for {
		select {
		case <-ctx.Done():
			return
		case intended, ok := <-slots:
			if !ok {
				return
			}
			if !b.running.Load() {
				return
			}
			bytesRead, err := b.raw.DoRequest(ctx, workerID)
			latency := time.Since(intended)

			if err != nil {
				if !b.running.Load() && ctx.Err() != nil {
					return
				}
				b.errors.Add(1)
				if b.mix != nil {
					b.mix.recordError(workerID)
				}
				continue
			}
			b.latencies.RecordSuccess(workerID, latency, bytesRead)
		}
	}
}

func (b *Benchmarker) worker(ctx context.Context, workerID int) {
	var interval time.Duration
	if b.config.MaxRPS > 0 {
		perWorker := b.config.MaxRPS / b.config.Workers
		if perWorker < 1 {
			perWorker = 1
		}
		interval = time.Second / time.Duration(perWorker)
	}

	var lastRequest time.Time
	for b.running.Load() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if interval > 0 {
			elapsed := time.Since(lastRequest)
			if elapsed < interval {
				time.Sleep(interval - elapsed)
			}
			lastRequest = time.Now()
		}

		start := time.Now()
		bytesRead, err := b.raw.DoRequest(ctx, workerID)
		latency := time.Since(start)

		if err != nil {
			// Don't count errors from context cancellation at benchmark shutdown.
			// When the benchmark duration ends, runCancel() cancels in-flight
			// requests which is expected, not an actual server error.
			if !b.running.Load() && ctx.Err() != nil {
				return
			}
			b.errors.Add(1)
			if b.mix != nil {
				b.mix.recordError(workerID)
			}
		} else {
			b.latencies.RecordSuccess(workerID, latency, bytesRead)
		}
	}
}

func (b *Benchmarker) buildResult(elapsed time.Duration) *Result {
	reqs, bytesRead := b.latencies.Totals()
	errs := b.errors.Load()

	rps := float64(reqs) / elapsed.Seconds()
	throughput := float64(bytesRead) / elapsed.Seconds()

	res := &Result{
		LoadgenVersion: Version,
		Requests:       reqs,
		Errors:         errs,
		Duration:       elapsed,
		RequestsPerSec: rps,
		ThroughputBPS:  throughput,
		Latency:        b.latencies.Percentiles(),
		DialRetries:    snapshotDialRetries(),
	}

	if hist, err := b.latencies.EncodeHistogram(); err == nil {
		res.Histogram = hist
	}

	if b.config.Rate > 0 {
		res.RatedMode = true
		res.TargetRPS = b.config.Rate
		res.Mode = "rated"
	} else {
		res.Mode = "saturation"
	}

	switch c := b.raw.(type) {
	case *h2Client:
		if c.dialedViaUpgrade {
			res.Upgrade = &UpgradeStats{
				UpgradeAttempted: c.upgradeAttempted,
				UpgradeSucceeded: len(c.conns),
			}
		}
	case *mixClient:
		stats := c.stats()
		res.Mix = &stats
		// A mix run that included an upgrade slot wraps its own *h2Client with
		// dialedViaUpgrade=true. Unwrap and surface the same UpgradeStats the
		// dedicated -h2c-upgrade run would have reported, so CLI output and
		// JSON shape stay parity between the two paths.
		if uc, ok := c.upgrade.(*h2Client); ok && uc.dialedViaUpgrade {
			res.Upgrade = &UpgradeStats{
				UpgradeAttempted: uc.upgradeAttempted,
				UpgradeSucceeded: len(uc.conns),
			}
		}
	}

	return res
}
