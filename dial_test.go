package loadgen

import (
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func TestIsTransientDialRST(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{syscall.ECONNRESET, true},
		{&net.OpError{Op: "dial", Err: syscall.ECONNRESET}, true},
		{errors.New("connection reset by peer"), true},
		{errors.New("dial tcp 127.0.0.1:1234: connect: connection reset by peer"), true},
		{errors.New("dial tcp 127.0.0.1:1234: connect: reset by peer"), true},
		{errors.New("i/o timeout"), false},
		{syscall.ETIMEDOUT, false},
		{errors.New("no route to host"), false},
	}
	for _, tt := range tests {
		if got := isTransientDialRST(tt.err); got != tt.want {
			t.Errorf("isTransientDialRST(%v) = %v, want %v", tt.err, got, tt.want)
		}
	}
}

// TestDialTCPRetryRecoversFromRST wires a test-only dial primitive
// that returns a transient-RST error for the first N attempts then
// succeeds. Lets the retry loop be validated portably instead of
// relying on a kernel no-listener-on-port race (ECONNREFUSED vs
// ECONNRESET is not portable across Linux and Darwin).
func TestDialTCPRetryRecoversFromRST(t *testing.T) {
	before := dialRetriesCounter.Swap(0)
	defer dialRetriesCounter.Add(before)

	var attempts atomic.Int32
	realDialTimeout := dialTimeoutFunc
	defer func() { dialTimeoutFunc = realDialTimeout }()
	dialTimeoutFunc = func(network, _ string, _ time.Duration) (net.Conn, error) {
		n := attempts.Add(1)
		if n == 1 {
			return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNRESET}
		}
		return &net.TCPConn{}, nil
	}

	conn, err := dialTCPRetry("127.0.0.1:65535", 500*time.Millisecond)
	if err != nil {
		t.Fatalf("dialTCPRetry: %v", err)
	}
	_ = conn.Close()

	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2 (1 RST + 1 success)", got)
	}
	if n := dialRetriesCounter.Load(); n != 1 {
		t.Fatalf("expected exactly 1 dial retry, got %d", n)
	}
}

// TestDialTCPRetryFailsFastOnNonTransient verifies that
// dial errors that are NOT connection-reset (e.g. EHOSTUNREACH,
// EADDRNOTAVAIL) are returned immediately without consuming the retry
// budget.
func TestDialTCPRetryFailsFastOnNonTransient(t *testing.T) {
	before := dialRetriesCounter.Swap(0)
	defer dialRetriesCounter.Add(before)

	// Dial a port we haven't bound. On Linux this gives connect:
	// connection refused (ECONNREFUSED), which is NOT a transient RST
	// for our purposes — should fail on first attempt.
	start := time.Now()
	_, err := dialTCPRetry("127.0.0.1:1", 200*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected dial to fail against unbound port")
	}
	if strings.Contains(err.Error(), "reset by peer") {
		t.Fatalf("unexpected transient RST classification: %v", err)
	}
	// Budget = single DialTimeout ≤ 200ms. If we retried, elapsed
	// would be ≥ 210ms (timeout + backoff). Give a generous 300ms
	// ceiling for CI noise.
	if elapsed > 300*time.Millisecond {
		t.Fatalf("dial took %v — should have failed fast, not retried", elapsed)
	}
	if n := dialRetriesCounter.Load(); n != 0 {
		t.Fatalf("expected 0 retries on non-transient error, got %d", n)
	}
}
