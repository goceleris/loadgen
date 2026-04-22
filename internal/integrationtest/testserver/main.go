// Command testserver launches a celeris HTTP server for loadgen integration
// tests. It accepts -protocol and -h2-upgrade flags so every matrix cell in
// the upgrade + mix integration suite (issue #30) can be exercised against a
// live celeris server.
//
// On startup the server binds on the requested address (default 127.0.0.1:0),
// prints the bound address on the first line of stdout, and then blocks until
// it receives SIGTERM, SIGINT, or the parent closes its stdin. Blocking on
// stdin lets integration tests reap the subprocess reliably even when the
// parent test times out.
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/goceleris/celeris"
)

func main() {
	protoFlag := flag.String("protocol", "auto", "celeris protocol: auto, http1, h2c")
	upgradeFlag := flag.String("h2-upgrade", "default", "celeris EnableH2Upgrade: default, true, false")
	engineFlag := flag.String("engine", "auto", "celeris engine: auto (platform default), std, adaptive, epoll, iouring")
	addrFlag := flag.String("addr", "127.0.0.1:0", "listen address")
	flag.Parse()

	var proto celeris.Protocol
	switch *protoFlag {
	case "auto":
		proto = celeris.Auto
	case "http1", "h1":
		proto = celeris.HTTP1
	case "h2c":
		proto = celeris.H2C
	default:
		fmt.Fprintf(os.Stderr, "testserver: unknown protocol %q\n", *protoFlag)
		os.Exit(2)
	}

	eng, err := resolveEngine(*engineFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "testserver: %v\n", err)
		os.Exit(2)
	}

	var upgrade *bool
	switch *upgradeFlag {
	case "default":
		upgrade = nil
	case "true":
		t := true
		upgrade = &t
	case "false":
		f := false
		upgrade = &f
	default:
		fmt.Fprintf(os.Stderr, "testserver: unknown h2-upgrade %q\n", *upgradeFlag)
		os.Exit(2)
	}

	// Silence the default slog logger — integration tests only care about
	// stdout's first line (the bound address) and stderr on failure.
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	srv := celeris.New(celeris.Config{
		Addr:            *addrFlag,
		Protocol:        proto,
		Engine:          eng,
		EnableH2Upgrade: upgrade,
		Logger:          logger,
		ReadTimeout:     10 * time.Second,
		WriteTimeout:    10 * time.Second,
		IdleTimeout:     30 * time.Second,
		ShutdownTimeout: 2 * time.Second,
		DisableMetrics:  true,
	})

	// Minimal handlers: any registered path returns 200 "ok".
	srv.GET("/", func(c *celeris.Context) error {
		return c.String(200, "ok")
	})
	srv.GET("/bench", func(c *celeris.Context) error {
		return c.String(200, "ok")
	})
	srv.POST("/bench", func(c *celeris.Context) error {
		return c.String(200, "ok")
	})

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	// Start the server in a goroutine so main() can poll Addr() for the
	// bound port, print it, then block on the run goroutine.
	runErr := make(chan error, 1)
	go func() {
		runErr <- srv.StartWithContext(ctx)
	}()

	// Wait for the listener to bind. Std engine exposes Server.Addr() once
	// the listener is open; poll for up to 5s.
	bound := false
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if a := srv.Addr(); a != nil {
			// Print address on first line of stdout. Integration tests
			// parse this line to discover the bound port when -addr=:0.
			fmt.Println(a.String())
			_ = os.Stdout.Sync()
			bound = true
			break
		}
		select {
		case err := <-runErr:
			// Run returned before bind — startup failure.
			if err != nil {
				fmt.Fprintf(os.Stderr, "testserver: start failed: %v\n", err)
			} else {
				fmt.Fprintln(os.Stderr, "testserver: start returned nil before bind")
			}
			os.Exit(1)
		case <-time.After(20 * time.Millisecond):
		}
	}
	if !bound {
		fmt.Fprintln(os.Stderr, "testserver: server did not bind within deadline")
		cancel()
		os.Exit(1)
	}

	// Parent process death signal: once the parent closes its stdin pipe,
	// io.Copy returns and we trigger shutdown. This guards against orphaned
	// celeris instances when `go test` is killed mid-cell. Start this AFTER
	// the bind so a closed-stdin-before-bind race (tests piping /dev/null)
	// doesn't cause a spurious shutdown before main can even print the addr.
	go func() {
		_, _ = io.Copy(io.Discard, os.Stdin)
		cancel()
	}()

	// Block until StartWithContext returns (either clean shutdown or fatal).
	if err := <-runErr; err != nil {
		fmt.Fprintf(os.Stderr, "testserver: %v\n", err)
		os.Exit(1)
	}
}

// resolveEngine maps the -engine flag to a celeris.EngineType. "auto" picks
// the best engine available on the current platform — Adaptive on Linux
// (the native engines properly enforce Protocol + EnableH2Upgrade), Std
// elsewhere (the only cross-platform option).
func resolveEngine(name string) (celeris.EngineType, error) {
	switch name {
	case "std":
		return celeris.Std, nil
	case "adaptive":
		return celeris.Adaptive, nil
	case "epoll":
		return celeris.Epoll, nil
	case "iouring":
		return celeris.IOUring, nil
	case "auto":
		if runtime.GOOS == "linux" {
			return celeris.Adaptive, nil
		}
		return celeris.Std, nil
	default:
		return 0, fmt.Errorf("unknown engine %q", name)
	}
}
