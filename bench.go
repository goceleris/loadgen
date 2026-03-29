// Package bench provides a high-performance HTTP benchmarking tool.
package loadgen

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"
)

// Config holds benchmark configuration.
type Config struct {
	URL         string
	Method      string
	Body        []byte
	Headers     map[string]string
	Duration    time.Duration
	Connections int
	Workers     int
	WarmupTime  time.Duration
	KeepAlive   bool
	H2C         bool

	// H2-specific settings for better performance
	// H2Connections is the number of HTTP/2 connections to use (default: 16)
	// HTTP/2 multiplexes streams over a single connection, but having multiple
	// connections can improve throughput by avoiding head-of-line blocking
	H2Connections int
	// H2MaxStreams is the max concurrent streams per connection (default: 100)
	H2MaxStreams int
}

// DefaultConfig returns sensible defaults for benchmarking.
// Note: The main benchmark runner now auto-scales workers and connections
// based on available CPUs. These defaults are fallbacks for direct library usage.
func DefaultConfig() Config {
	return Config{
		Method:        "GET",
		Duration:      30 * time.Second,
		Connections:   256,
		Workers:       64, // Higher default for direct library use
		WarmupTime:    5 * time.Second,
		KeepAlive:     true,
		H2Connections: 16,  // Multiple H2 connections for better throughput
		H2MaxStreams:  100, // Concurrent streams per connection
	}
}

// rawClient is the interface for zero-allocation HTTP clients used by the benchmarker.
// workerID enables per-worker connection partitioning to eliminate contention.
type rawClient interface {
	doRequest(ctx context.Context, workerID int) (bytesRead int, err error)
	close()
}

// Benchmarker runs HTTP benchmarks.
type Benchmarker struct {
	config Config
	raw    rawClient

	// Metrics — errors use atomic (rare, no contention).
	// Requests and bytesRead are tracked per-shard in latencies.
	errors atomic.Int64

	// Latency tracking (also holds per-shard request/bytes counters)
	latencies *ShardedLatencyRecorder

	// Control
	running atomic.Bool
	wg      sync.WaitGroup
}

// parseURL extracts host, port, and path from a raw URL.
func parseURL(rawURL string) (host, port, path string, err error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", "", "", fmt.Errorf("parse URL: %w", err)
	}
	host = u.Hostname()
	port = u.Port()
	if port == "" {
		if u.Scheme == "https" {
			port = "443"
		} else {
			port = "80"
		}
	}
	path = u.RequestURI()
	if path == "" {
		path = "/"
	}
	return host, port, path, nil
}

// New creates a new Benchmarker with the given configuration.
func New(cfg Config) *Benchmarker {
	// Set H2 defaults if not specified
	if cfg.H2Connections == 0 {
		cfg.H2Connections = 16
	}
	if cfg.H2MaxStreams == 0 {
		cfg.H2MaxStreams = 100
	}

	flushInterval := defaultFlushInterval // 256 for H1
	if cfg.H2C {
		flushInterval = 16 // ~4 flushes/sec/worker at ~69 rps/worker
	}

	host, port, path, err := parseURL(cfg.URL)
	if err != nil {
		return &Benchmarker{
			config:    cfg,
			raw:       &failClient{err: err},
			latencies: NewShardedLatencyRecorder(cfg.Workers, flushInterval),
		}
	}

	var raw rawClient

	if cfg.H2C {
		// H2 workers are I/O-bound (blocked on channel round-trips), not CPU-bound.
		// With multiplexed streams, workers need 4x headroom to keep the pipeline
		// saturated. Little's Law: throughput = workers / RTT.
		cfg.Workers *= 4
		raw, err = newH2Client(host, port, path, cfg.Method, cfg.Headers, cfg.Body, cfg.H2Connections, cfg.H2MaxStreams)
	} else {
		// For H1: keep-alive uses 1 connection per worker; Connection: close
		// uses connsPerWorkerClose connections per worker (round-robin pool).
		numWorkers := cfg.Workers
		if numWorkers < 1 {
			numWorkers = 64
		}
		raw, err = newH1Client(host, port, path, cfg.Method, cfg.Headers, cfg.Body, numWorkers, cfg.KeepAlive)
	}

	if err != nil {
		return &Benchmarker{
			config:    cfg,
			raw:       &failClient{err: fmt.Errorf("dial: %w", err)},
			latencies: NewShardedLatencyRecorder(cfg.Workers, flushInterval),
		}
	}

	return &Benchmarker{
		config:    cfg,
		raw:       raw,
		latencies: NewShardedLatencyRecorder(cfg.Workers, flushInterval),
	}
}

// failClient is a rawClient that always returns an error.
// Used when URL parsing or connection setup fails.
type failClient struct {
	err error
}

func (f *failClient) doRequest(context.Context, int) (int, error) { return 0, f.err }
func (f *failClient) close()                                      {}

// Run executes the benchmark and returns results.
func (b *Benchmarker) Run(ctx context.Context) (*Result, error) {
	// Warmup phase
	if b.config.WarmupTime > 0 {
		b.warmup(ctx)
	}

	// Reset metrics for actual benchmark
	b.errors.Store(0)
	b.latencies.Reset()

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
	b.raw.close()

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
	warmupCtx, cancel := context.WithTimeout(ctx, b.config.WarmupTime)
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
	for b.running.Load() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		start := time.Now()
		bytesRead, err := b.raw.doRequest(ctx, workerID)
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

	return &Result{
		Requests:       reqs,
		Errors:         errs,
		Duration:       elapsed,
		RequestsPerSec: rps,
		ThroughputBPS:  throughput,
		Latency:        b.latencies.Percentiles(),
	}
}
