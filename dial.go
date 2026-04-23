package loadgen

import (
	"crypto/tls"
	"errors"
	"net"
	"sync/atomic"
	"syscall"
	"time"
)

// dialRetriesCounter is incremented once per retried dial. Call sites
// (h1client, h2client, h2c_upgrade) drain it into Result.DialRetries at
// the end of a run. The counter is process-global because dial helpers
// are called from many goroutines; the caller snapshots via Swap after
// the run completes.
var dialRetriesCounter atomic.Uint64

// maxDialAttempts is the total number of TCP SYN attempts per dial,
// including the first. Only transient SYN-RST responses trigger a
// retry; every other error fails fast.
//
// 3 matches the pragma most TCP stacks apply to their own SYN
// retransmit ladder before giving up on a connection state race, and
// covers the observed celeris-side rebind / adaptive-engine switch
// windows (<5 ms) without slowing the legitimate-failure path more
// than ~35 ms (10 + 25 ms backoff).
const maxDialAttempts = 3

// dialBackoffs controls the sleep between retry attempts. Index i is
// the sleep *before* attempt (i+1) — so [0] is the gap between
// attempt 1 (which failed) and attempt 2. Length must equal
// maxDialAttempts-1.
var dialBackoffs = [...]time.Duration{
	10 * time.Millisecond,
	25 * time.Millisecond,
}

// isTransientDialRST reports whether err came from a SYN getting an
// RST back during connect. That happens when the peer briefly has no
// listener on the port (SO_REUSEPORT listener replacement, adaptive
// engine active-engine switch, conntrack collision) and is entitled
// to a retry; every other dial error (ETIMEDOUT, EHOSTUNREACH, EMFILE,
// …) should fail fast.
func isTransientDialRST(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, syscall.ECONNRESET) {
		return true
	}
	// Fall back to string match — net.OpError sometimes wraps the
	// errno in a way that Is() does not unwrap on every platform.
	s := err.Error()
	return containsAny(s,
		"connection reset by peer",
		"connection reset",
		"reset by peer",
	)
}

func containsAny(s string, subs ...string) bool {
	for _, sub := range subs {
		if indexOf(s, sub) >= 0 {
			return true
		}
	}
	return false
}

func indexOf(s, sub string) int {
	n, m := len(s), len(sub)
	if m == 0 {
		return 0
	}
	if m > n {
		return -1
	}
	for i := 0; i <= n-m; i++ {
		if s[i:i+m] == sub {
			return i
		}
	}
	return -1
}

// dialTimeoutFunc is the primitive dialTCPRetry uses under the hood;
// production paths call net.DialTimeout. The variable is test-only —
// unit tests swap it out to inject transient-RST errors portably
// (ECONNRESET vs ECONNREFUSED semantics differ between Linux and
// Darwin, so a kernel-triggered reproducer would not be portable).
var dialTimeoutFunc = net.DialTimeout

// dialTCPRetry opens a plain TCP connection to addr with a bounded
// retry window when the first SYN lands on a momentarily closed
// listener (RST). Non-RST errors fail on the first attempt.
func dialTCPRetry(addr string, timeout time.Duration) (net.Conn, error) {
	var (
		conn net.Conn
		err  error
	)
	for attempt := 0; attempt < maxDialAttempts; attempt++ {
		conn, err = dialTimeoutFunc("tcp", addr, timeout)
		if err == nil {
			return conn, nil
		}
		if !isTransientDialRST(err) {
			return nil, err
		}
		if attempt < maxDialAttempts-1 {
			dialRetriesCounter.Add(1)
			time.Sleep(dialBackoffs[attempt])
		}
	}
	return nil, err
}

// dialTLSRetry dials TLS to addr with RST-retry semantics matching
// dialTCPRetry.
func dialTLSRetry(addr string, timeout time.Duration, cfg *tls.Config) (*tls.Conn, error) {
	var (
		conn *tls.Conn
		err  error
	)
	for attempt := 0; attempt < maxDialAttempts; attempt++ {
		conn, err = tls.DialWithDialer(&net.Dialer{Timeout: timeout}, "tcp", addr, cfg)
		if err == nil {
			return conn, nil
		}
		if !isTransientDialRST(err) {
			return nil, err
		}
		if attempt < maxDialAttempts-1 {
			dialRetriesCounter.Add(1)
			time.Sleep(dialBackoffs[attempt])
		}
	}
	return nil, err
}

// snapshotDialRetries returns the current dial-retry count and resets
// the global counter. Called once per Benchmarker.Run so the value
// reported on a Result covers only that run.
func snapshotDialRetries() uint64 {
	return dialRetriesCounter.Swap(0)
}
