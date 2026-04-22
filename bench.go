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

	return &Benchmarker{
		config:    cfg,
		raw:       raw,
		latencies: NewShardedLatencyRecorder(cfg.Workers, flushInterval),
	}, nil
}

// Run executes the benchmark and returns results.
func (b *Benchmarker) Run(ctx context.Context) (*Result, error) {
	// Warmup phase
	if b.config.Warmup > 0 {
		b.warmup(ctx)
	}

	// Reset metrics for actual benchmark
	b.errors.Store(0)
	b.latencies.Reset()
	if mc, ok := b.raw.(*mixClient); ok {
		// Clear per-protocol counters accumulated during warmup.
		mc.h1Requests.Store(0)
		mc.h2Requests.Store(0)
		mc.upgradeRequests.Store(0)
		mc.h1Errors.Store(0)
		mc.h2Errors.Store(0)
		mc.upgradeErrors.Store(0)
	}

	// Start client CPU monitor
	cpuMon := &CPUMonitor{}
	cpuMon.Start()

	// Create a scoped context for this benchmark run. Workers use this context
	// so their in-flight HTTP requests are cancelled when the benchmark ends,
	// preventing wg.Wait() from hanging if the server stops responding.
	// Allow Duration + 60s for in-flight requests to drain.
	runCtx, runCancel := context.WithTimeout(ctx, b.config.Duration+60*time.Second)
	defer runCancel()

	// Timeseries collection: 1-second snapshots
	var timeseries []TimeseriesPoint
	var prevReqs int64

	// Start workers — each gets a unique workerID for connection partitioning
	b.running.Store(true)
	start := time.Now()

	for i := range b.config.Workers {
		workerID := i
		b.wg.Go(func() { b.worker(runCtx, workerID) })
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
				timeseries = append(timeseries, TimeseriesPoint{
					TimestampSec:   elapsed,
					RequestsPerSec: float64(deltaReqs), // 1-second window
				})
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
		Requests:       reqs,
		Errors:         errs,
		Duration:       elapsed,
		RequestsPerSec: rps,
		ThroughputBPS:  throughput,
		Latency:        b.latencies.Percentiles(),
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
	}

	return res
}
