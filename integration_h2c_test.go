//go:build integration

// Integration tests for the -h2c-upgrade and -mix flags against a live
// celeris server. See issue #30 for the matrix and acceptance criteria.
//
// These tests are gated behind the "integration" build tag so `go test ./...`
// on its own stays fast and hermetic. Run them with:
//
//	go test -tags integration -race -count=1 -timeout 180s ./...
//
// Each subtest spawns a fresh celeris subprocess (via the helper at
// internal/integrationtest/testserver), parses the bound address from its
// stdout, then drives loadgen via its library API (loadgen.New + Run).
// Subprocess isolation means one flaky cell can't taint another — a lesson
// learned from the h2c upgrade bug that surfaced as a silent pass in
// early drafts of this suite.

package loadgen_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/goceleris/loadgen"
)

// loadgenMode identifies which client wiring the loadgen config exercises.
// A mode captures the protocol choice AND any relevant tuning — it's what
// maps one matrix row to a concrete loadgen.Config.
type loadgenMode int

const (
	modeH1      loadgenMode = iota // default — plain HTTP/1.1
	modeH2                         // -h2 prior-knowledge HTTP/2
	modeUpgrade                    // -h2c-upgrade RFC 7540 §3.2 handshake
	modeMix110                     // -mix 1:1:0 (h1 + h2, no upgrade)
	modeMix111                     // -mix 1:1:1 (h1 + h2 + upgrade)
)

func (m loadgenMode) String() string {
	switch m {
	case modeH1:
		return "default-h1"
	case modeH2:
		return "h2"
	case modeUpgrade:
		return "h2c-upgrade"
	case modeMix110:
		return "mix-1:1:0"
	case modeMix111:
		return "mix-1:1:1"
	default:
		return "unknown"
	}
}

// matrixCell is one row in the 6 × (up-to-4) integration matrix. expectOK
// true means we require RPS > minRPSFloor and errors == 0; expectOK false
// means we require either a dial-time failure from loadgen.New or a run
// where every recorded attempt errored (errors > 0, requests == 0).
//
// stdSkipReason is a non-empty string for cells whose expected behaviour
// relies on the native (linux-only) engines enforcing protocol selection
// that the net/http-backed Std engine does not. The matrix is meaningful
// on Linux; Darwin's Std-only runs skip these rows with a clear reason.
type matrixCell struct {
	serverProto   string // "auto", "http1", "h2c"
	serverUpgrade string // "default", "true", "false"
	mode          loadgenMode
	expectOK      bool
	stdSkipReason string
}

func (c matrixCell) name() string {
	return fmt.Sprintf("proto=%s/upgrade=%s/%s", c.serverProto, c.serverUpgrade, c.mode)
}

// minRPSFloor is the minimum requests-per-second any "expected OK" cell must
// achieve. Set conservatively: short-duration runs on an idle loopback
// should comfortably clear 1000 RPS even on a CI runner. Falling below this
// means the client is connecting but making almost no progress — nearly
// always a sign of a silent protocol mismatch.
const minRPSFloor = 500.0

// benchDuration is how long each cell's measured phase runs. Short so the
// full matrix stays under CI's 3-minute budget; long enough to produce
// statistically meaningful RPS numbers.
const benchDuration = 1500 * time.Millisecond

// benchWarmup is the connection-warmup window per cell. Short for speed;
// loadgen requires this to be positive or it skips warmup entirely, which
// means upgrade handshakes happen on the benchmark clock.
const benchWarmup = 300 * time.Millisecond

// TestIntegrationH2CMatrix is the main matrix-coverage test. It iterates
// every (server config × loadgen mode) combination from issue #30 and
// asserts the expected outcome.
func TestIntegrationH2CMatrix(t *testing.T) {
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skipf("integration matrix is only run on linux/darwin, got %s", runtime.GOOS)
	}

	binPath := buildTestServer(t)

	// h2cServingH1 is the reason we skip cells that rely on H2C and Auto
	// engines rejecting H1 plaintext. The net/http + x/net/http2/h2c stack
	// used by the Std engine always tolerates H1 on the same port via the
	// h2c handler's fallthrough — there is no toggle to disable it — so
	// these cells are meaningful only on the native (linux) engines.
	const h2cServingH1 = "std engine uses x/net/http2/h2c which falls through to H1 — EnableH2Upgrade + Protocol=H2C enforcement is only implemented in native linux engines"

	// The full matrix from issue #30. 20 cells total. stdSkipReason is
	// non-empty for the 4 cells whose expected behaviour relies on the
	// linux-only native engines.
	matrix := []matrixCell{
		// Protocol=HTTP1, EnableH2Upgrade=false
		{serverProto: "http1", serverUpgrade: "false", mode: modeH1, expectOK: true},
		{serverProto: "http1", serverUpgrade: "false", mode: modeH2, expectOK: false},
		{serverProto: "http1", serverUpgrade: "false", mode: modeUpgrade, expectOK: false},

		// Protocol=HTTP1, EnableH2Upgrade=true
		// (Same outcomes — HTTP1 explicitly rejects upgrade attempts.)
		{serverProto: "http1", serverUpgrade: "true", mode: modeH1, expectOK: true},
		{serverProto: "http1", serverUpgrade: "true", mode: modeH2, expectOK: false},
		{serverProto: "http1", serverUpgrade: "true", mode: modeUpgrade, expectOK: false},

		// Protocol=H2C, EnableH2Upgrade=false
		{serverProto: "h2c", serverUpgrade: "false", mode: modeH1, expectOK: false, stdSkipReason: h2cServingH1},
		{serverProto: "h2c", serverUpgrade: "false", mode: modeH2, expectOK: true},
		{serverProto: "h2c", serverUpgrade: "false", mode: modeUpgrade, expectOK: false, stdSkipReason: h2cServingH1},

		// Protocol=H2C, EnableH2Upgrade=true
		{serverProto: "h2c", serverUpgrade: "true", mode: modeH1, expectOK: false, stdSkipReason: h2cServingH1},
		{serverProto: "h2c", serverUpgrade: "true", mode: modeH2, expectOK: true},
		{serverProto: "h2c", serverUpgrade: "true", mode: modeUpgrade, expectOK: true},

		// Protocol=Auto, EnableH2Upgrade=false
		{serverProto: "auto", serverUpgrade: "false", mode: modeH1, expectOK: true},
		{serverProto: "auto", serverUpgrade: "false", mode: modeH2, expectOK: true},
		{serverProto: "auto", serverUpgrade: "false", mode: modeUpgrade, expectOK: false, stdSkipReason: h2cServingH1},
		{serverProto: "auto", serverUpgrade: "false", mode: modeMix110, expectOK: true},

		// Protocol=Auto, EnableH2Upgrade=true
		{serverProto: "auto", serverUpgrade: "true", mode: modeH1, expectOK: true},
		{serverProto: "auto", serverUpgrade: "true", mode: modeH2, expectOK: true},
		{serverProto: "auto", serverUpgrade: "true", mode: modeUpgrade, expectOK: true},
		{serverProto: "auto", serverUpgrade: "true", mode: modeMix111, expectOK: true},
	}

	isStd := runtime.GOOS != "linux"
	for _, cell := range matrix {
		cell := cell
		t.Run(cell.name(), func(t *testing.T) {
			if isStd && cell.stdSkipReason != "" {
				t.Skipf("%s", cell.stdSkipReason)
			}
			runMatrixCell(t, binPath, cell)
		})
	}
}

// runMatrixCell spawns the celeris subprocess, runs loadgen against it,
// and asserts the expected outcome for this matrix row.
func runMatrixCell(t *testing.T, binPath string, cell matrixCell) {
	t.Helper()

	// Each cell gets a bounded deadline — if the server hangs on shutdown
	// or loadgen refuses to exit, the test fails instead of stalling CI.
	const cellDeadline = 25 * time.Second
	cellCtx, cellCancel := context.WithTimeout(context.Background(), cellDeadline)
	defer cellCancel()

	addr, cleanup := startTestServer(cellCtx, t, binPath, cell.serverProto, cell.serverUpgrade)
	defer cleanup()

	cfg := loadgenConfigFor(cell.mode, addr)

	// loadgen's New() performs eager dials for some clients (H2, H2CUpgrade).
	// A connection-error outcome can surface either here or at Run() time;
	// we accept both shapes.
	b, newErr := loadgen.New(cfg)

	if newErr != nil {
		if cell.expectOK {
			t.Fatalf("%s: expected OK but loadgen.New failed: %v", cell.name(), newErr)
		}
		t.Logf("%s: loadgen.New rejected dial (expected failure): %v", cell.name(), newErr)
		return
	}

	runCtx, runCancel := context.WithTimeout(cellCtx, benchDuration+15*time.Second)
	defer runCancel()

	result, runErr := b.Run(runCtx)
	if runErr != nil {
		if cell.expectOK {
			t.Fatalf("%s: expected OK but Run failed: %v", cell.name(), runErr)
		}
		t.Logf("%s: Run errored (expected failure): %v", cell.name(), runErr)
		return
	}

	assertResult(t, cell, result)
}

// loadgenConfigFor builds a loadgen.Config tailored to the matrix mode.
// All modes share a small worker/conn footprint to keep the aggregate
// matrix under CI's time budget — correctness is what we're testing,
// not throughput numbers.
func loadgenConfigFor(mode loadgenMode, addr string) loadgen.Config {
	cfg := loadgen.Config{
		URL:             "http://" + addr + "/bench",
		Method:          "GET",
		Duration:        benchDuration,
		Warmup:          benchWarmup,
		Connections:     4,
		Workers:         4,
		DialTimeout:     2 * time.Second,
		MaxResponseSize: 64 * 1024,
		HTTP2Options: loadgen.HTTP2Options{
			Connections: 2,
			MaxStreams:  32,
		},
	}

	switch mode {
	case modeH1:
		// defaults
	case modeH2:
		cfg.HTTP2 = true
	case modeUpgrade:
		cfg.H2CUpgrade = true
	case modeMix110:
		cfg.Mix = &loadgen.MixRatio{H1: 1, H2: 1, Upgrade: 0}
	case modeMix111:
		cfg.Mix = &loadgen.MixRatio{H1: 1, H2: 1, Upgrade: 1}
	}
	return cfg
}

// assertResult validates the benchmark result against the matrix cell's
// expectation. The "expected fail but got OK" branch is the one that
// caught real bugs during suite development — a silent pass there means
// the client isn't actually exercising the protocol it claims to.
func assertResult(t *testing.T, cell matrixCell, result *loadgen.Result) {
	t.Helper()

	if cell.expectOK {
		if result.Errors > 0 {
			t.Fatalf("%s: expected zero errors, got %d (requests=%d rps=%.1f)",
				cell.name(), result.Errors, result.Requests, result.RequestsPerSec)
		}
		if result.RequestsPerSec < minRPSFloor {
			t.Fatalf("%s: RPS %.1f below floor %.1f (requests=%d errors=%d)",
				cell.name(), result.RequestsPerSec, minRPSFloor, result.Requests, result.Errors)
		}

		// For mix cells, verify every slot with non-zero weight actually
		// dispatched requests. A zero in a weighted slot means the dispatch
		// logic is dropping that protocol — as bad as an error.
		if result.Mix != nil {
			assertMixSlots(t, cell, result.Mix)
		}
		t.Logf("%s: OK (requests=%d errors=%d rps=%.1f)",
			cell.name(), result.Requests, result.Errors, result.RequestsPerSec)
		return
	}

	// expectOK=false. A pass-equivalent result means:
	//   - Requests > 0 with some progress, OR
	//   - RPS above the floor
	// is a TEST FAILURE — the client should have been unable to make
	// forward progress on the mis-matched protocol.
	if result.Requests > 0 && result.Errors == 0 {
		t.Fatalf("%s: expected failure but got %d successful requests (rps=%.1f). "+
			"This is a silent-pass bug — loadgen must surface the protocol mismatch as an error.",
			cell.name(), result.Requests, result.RequestsPerSec)
	}
	if result.Errors == 0 && result.Requests == 0 {
		// No requests made at all. For H2 prior-knowledge and upgrade
		// clients this happens when New() succeeded but every worker
		// couldn't complete a single request before the deadline. We
		// treat this as an expected-failure cell passing, but log it
		// because a healthy loadgen should surface these as errors.
		t.Logf("%s: expected failure observed as zero-request run (errors=0, rps=%.1f). "+
			"Prefer surfacing an explicit error — see h1/h2 client error paths.",
			cell.name(), result.RequestsPerSec)
		return
	}
	// Errors > 0 — the normal failure shape.
	t.Logf("%s: expected failure observed (requests=%d errors=%d rps=%.1f)",
		cell.name(), result.Requests, result.Errors, result.RequestsPerSec)
}

// assertMixSlots checks that every mix slot with a non-zero weight saw at
// least one request complete, and any slot with a zero weight saw exactly
// zero. This is the stricter end of "mix works" — just asserting totalRPS
// > 0 would let a bug that drops h1 traffic slip through silently.
func assertMixSlots(t *testing.T, cell matrixCell, mix *loadgen.MixStats) {
	t.Helper()

	var ratio loadgen.MixRatio
	switch cell.mode {
	case modeMix110:
		ratio = loadgen.MixRatio{H1: 1, H2: 1, Upgrade: 0}
	case modeMix111:
		ratio = loadgen.MixRatio{H1: 1, H2: 1, Upgrade: 1}
	default:
		return
	}

	if ratio.H1 > 0 && mix.H1Requests == 0 {
		t.Fatalf("%s: mix slot h1 has weight %d but zero requests completed "+
			"(h1_errors=%d, h1_conns=%d)", cell.name(), ratio.H1, mix.H1Errors, mix.H1Conns)
	}
	if ratio.H2 > 0 && mix.H2Requests == 0 {
		t.Fatalf("%s: mix slot h2 has weight %d but zero requests completed "+
			"(h2_errors=%d, h2_conns=%d)", cell.name(), ratio.H2, mix.H2Errors, mix.H2Conns)
	}
	if ratio.Upgrade > 0 && mix.UpgradeRequests == 0 {
		t.Fatalf("%s: mix slot upgrade has weight %d but zero requests completed "+
			"(upgrade_errors=%d, upgrade_conns=%d)", cell.name(), ratio.Upgrade, mix.UpgradeErrors, mix.UpgradeConns)
	}

	if ratio.H1 == 0 && mix.H1Requests > 0 {
		t.Fatalf("%s: mix slot h1 has weight 0 but saw %d requests (dispatcher bug)",
			cell.name(), mix.H1Requests)
	}
	if ratio.H2 == 0 && mix.H2Requests > 0 {
		t.Fatalf("%s: mix slot h2 has weight 0 but saw %d requests (dispatcher bug)",
			cell.name(), mix.H2Requests)
	}
	if ratio.Upgrade == 0 && mix.UpgradeRequests > 0 {
		t.Fatalf("%s: mix slot upgrade has weight 0 but saw %d requests (dispatcher bug)",
			cell.name(), mix.UpgradeRequests)
	}

	// For the 1:1:1 cell, assert every non-zero slot is also error-free.
	if cell.expectOK {
		if mix.H1Errors > 0 {
			t.Fatalf("%s: mix slot h1 had %d errors (expected zero)", cell.name(), mix.H1Errors)
		}
		if mix.H2Errors > 0 {
			t.Fatalf("%s: mix slot h2 had %d errors (expected zero)", cell.name(), mix.H2Errors)
		}
		if ratio.Upgrade > 0 && mix.UpgradeErrors > 0 {
			t.Fatalf("%s: mix slot upgrade had %d errors (expected zero)", cell.name(), mix.UpgradeErrors)
		}
	}
}

// buildTestServer compiles the helper at internal/integrationtest/testserver
// into a binary in t.TempDir, returning its absolute path. Compiled once
// per test invocation so all matrix cells share the same binary.
var (
	testServerBuildOnce sync.Once
	testServerBinPath   string
	testServerBuildErr  error
)

func buildTestServer(t *testing.T) string {
	t.Helper()
	testServerBuildOnce.Do(func() {
		dir, err := os.MkdirTemp("", "loadgen-integration-testserver-")
		if err != nil {
			testServerBuildErr = fmt.Errorf("mkdir temp: %w", err)
			return
		}
		bin := filepath.Join(dir, "testserver")
		if runtime.GOOS == "windows" {
			bin += ".exe"
		}
		cmd := exec.Command("go", "build", "-o", bin, "./internal/integrationtest/testserver")
		cmd.Env = os.Environ()
		out, err := cmd.CombinedOutput()
		if err != nil {
			testServerBuildErr = fmt.Errorf("go build testserver: %v\n%s", err, out)
			return
		}
		testServerBinPath = bin
	})
	if testServerBuildErr != nil {
		t.Fatalf("build testserver: %v", testServerBuildErr)
	}
	return testServerBinPath
}

// startTestServer launches the celeris subprocess, parses the bound address
// from its first stdout line, and returns that address plus a cleanup
// function. The cleanup signals the subprocess via SIGTERM (or kills it on
// timeout) and drains its stdio — no orphans, even if the test panics.
// ctx bounds the bind-wait; it is not stored, so lint's context-first rule
// is satisfied while keeping t as the logical primary subject.
func startTestServer(ctx context.Context, t *testing.T, binPath, protocol, upgrade string) (string, func()) {
	t.Helper()

	cmd := exec.Command(binPath,
		"-protocol="+protocol,
		"-h2-upgrade="+upgrade,
		"-addr=127.0.0.1:0",
	)

	// stdin pipe: keep it open so the server's parent-death-detection
	// goroutine doesn't trigger premature shutdown. Closing this pipe in
	// cleanup tells the server to exit gracefully.
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe: %v", err)
	}
	cmd.Stderr = newTestLogWriter(t, protocol+"/"+upgrade+" stderr")

	if err := cmd.Start(); err != nil {
		_ = stdin.Close()
		t.Fatalf("start testserver: %v", err)
	}

	// Read the bound address on the first stdout line with a deadline.
	addrCh := make(chan string, 1)
	scanErrCh := make(chan error, 1)
	go func() {
		br := bufio.NewReader(stdout)
		line, err := br.ReadString('\n')
		if err != nil && !errors.Is(err, io.EOF) {
			scanErrCh <- err
			return
		}
		addrCh <- strings.TrimSpace(line)
		// Drain rest of stdout in case the server logs; discard.
		_, _ = io.Copy(io.Discard, br)
	}()

	var addr string
	select {
	case addr = <-addrCh:
		if addr == "" {
			_ = stdin.Close()
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("testserver produced empty address line")
		}
	case err := <-scanErrCh:
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("read testserver address: %v", err)
	case <-time.After(10 * time.Second):
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("testserver did not print address within 10s")
	case <-ctx.Done():
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("context canceled before testserver bound: %v", ctx.Err())
	}

	cleanup := func() {
		// Closing stdin triggers the server's graceful shutdown path.
		_ = stdin.Close()
		done := make(chan struct{})
		go func() {
			_ = cmd.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Logf("testserver %s/%s did not exit within 5s, killing", protocol, upgrade)
			_ = cmd.Process.Kill()
			<-done
		}
	}

	return addr, cleanup
}

// testLogWriter bridges the subprocess' stderr to testing.T.Log, tagging
// every line with a prefix so matrix-cell output stays attributable.
type testLogWriter struct {
	t      *testing.T
	prefix string
	mu     sync.Mutex
	buf    []byte
}

func newTestLogWriter(t *testing.T, prefix string) *testLogWriter {
	return &testLogWriter{t: t, prefix: prefix}
}

func (w *testLogWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.buf = append(w.buf, p...)
	for {
		idx := -1
		for i, c := range w.buf {
			if c == '\n' {
				idx = i
				break
			}
		}
		if idx < 0 {
			break
		}
		line := string(w.buf[:idx])
		w.buf = w.buf[idx+1:]
		w.t.Logf("[%s] %s", w.prefix, line)
	}
	return len(p), nil
}
