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
	"strings"
	"syscall"
	"time"

	"github.com/goceleris/loadgen"
)

type headerFlag []string

func (h *headerFlag) String() string { return "" }
func (h *headerFlag) Set(value string) error {
	*h = append(*h, value)
	return nil
}

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
		insecure    = flag.Bool("insecure", false, "skip TLS certificate verification")
		bodyFile    = flag.String("body-file", "", "path to file whose contents are sent as request body")
		maxRPS      = flag.Int("max-rps", 0, "max requests per second (0 = unlimited)")
		customHdrs  headerFlag
	)
	flag.Var(&customHdrs, "H", "custom header in 'Key: Value' format (repeatable)")
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

	// Parse custom headers from -H flags
	var headers map[string]string
	if len(customHdrs) > 0 {
		headers = make(map[string]string, len(customHdrs))
		for _, h := range customHdrs {
			key, value, ok := strings.Cut(h, ": ")
			if !ok {
				fmt.Fprintf(os.Stderr, "error: invalid header format %q (expected \"Key: Value\")\n", h)
				os.Exit(1)
			}
			headers[key] = value
		}
	}

	var body []byte
	if *bodyFile != "" {
		var err error
		body, err = os.ReadFile(*bodyFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: reading body file: %v\n", err)
			os.Exit(1)
		}
	}

	cfg := loadgen.Config{
		URL:              *url,
		Method:           *method,
		Body:             body,
		Duration:         *duration,
		Warmup:           *warmup,
		Connections:      *connections,
		Workers:          w,
		DisableKeepAlive: *connClose,
		HTTP2:            *h2,
		HTTP2Options: loadgen.HTTP2Options{
			Connections: *h2Conns,
			MaxStreams:  *h2Streams,
		},
		Headers:            headers,
		InsecureSkipVerify: *insecure,
		MaxRPS:             *maxRPS,
	}

	cfg.OnProgress = func(elapsed time.Duration, snapshot loadgen.Result) {
		fmt.Fprintf(os.Stderr, "\r  %s  %d req  %.0f req/s",
			elapsed.Round(time.Second), snapshot.Requests, snapshot.RequestsPerSec)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()

	b, err := loadgen.New(cfg)
	if err != nil {
		log.Fatalf("loadgen: %v", err)
	}
	result, err := b.Run(ctx)
	fmt.Fprintln(os.Stderr)
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
