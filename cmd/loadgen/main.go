// Package main provides a minimal standalone load generator that wraps the
// bench package's custom H1/H2 clients. Designed for use in the celeris mage
// engine analysis sweep where wrk/h2load are insufficient.
//
// Usage:
//
//	loadgen -url http://host:port/path -duration 15s -connections 256 [-h2] [-h2-conns 16] [-h2-streams 100] [-workers 64]
//
// Output is JSON with RPS, latency percentiles (p50-p99.99), errors, and client CPU%.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/goceleris/loadgen"
)

func main() {
	var (
		url         = flag.String("url", "", "target URL (required)")
		duration    = flag.Duration("duration", 15*time.Second, "benchmark duration")
		warmup      = flag.Duration("warmup", 2*time.Second, "warmup duration")
		connections = flag.Int("connections", 256, "number of H1 connections (= workers)")
		workers     = flag.Int("workers", 0, "number of workers (default: connections for H1, connections*4 for H2)")
		h2          = flag.Bool("h2", false, "use HTTP/2 (h2c)")
		h2Conns     = flag.Int("h2-conns", 16, "H2 connections")
		h2Streams   = flag.Int("h2-streams", 100, "max concurrent H2 streams per connection")
		method      = flag.String("method", "GET", "HTTP method")
		connClose   = flag.Bool("close", false, "send Connection: close header (H1 only)")
	)
	flag.Parse()

	if *url == "" {
		fmt.Fprintln(os.Stderr, "error: -url is required")
		flag.Usage()
		os.Exit(1)
	}

	w := *workers
	if w == 0 {
		if *h2 {
			w = runtime.NumCPU() * 4
		} else {
			w = *connections
		}
	}

	// For Connection: close, don't pass it as a custom header — the H1
	// client sets it based on KeepAlive. Remove from headers to avoid
	// confusion (buildH1Request skips Connection in custom headers anyway).
	var headers map[string]string

	cfg := loadgen.Config{
		URL:           *url,
		Method:        *method,
		Duration:      *duration,
		WarmupTime:    *warmup,
		Connections:   *connections,
		Workers:       w,
		KeepAlive:     !*connClose,
		H2C:           *h2,
		H2Connections: *h2Conns,
		H2MaxStreams:  *h2Streams,
		Headers:       headers,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	b := loadgen.New(cfg)
	result, err := b.Run(ctx)
	if err != nil {
		log.Fatalf("benchmark failed: %v", err)
	}

	// Output JSON result.
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		log.Fatalf("encode result: %v", err)
	}
}
