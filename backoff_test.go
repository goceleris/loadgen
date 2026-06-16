package loadgen

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// TestConnectBackoffDoublesAndCaps walks the ladder with a pre-cancelled
// context so no real sleeping happens — only the interval progression is
// under test: 10ms doubling per call, capped at 1s.
func TestConnectBackoffDoublesAndCaps(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	var bo connectBackoff
	want := []time.Duration{
		20 * time.Millisecond, 40 * time.Millisecond, 80 * time.Millisecond,
		160 * time.Millisecond, 320 * time.Millisecond, 640 * time.Millisecond,
		time.Second, time.Second, time.Second,
	}
	for i, w := range want {
		if bo.sleep(ctx, nil) {
			t.Fatalf("call %d: sleep returned true with cancelled ctx", i)
		}
		if bo.next != w {
			t.Fatalf("call %d: next = %v, want %v", i, bo.next, w)
		}
	}

	bo.reset()
	if bo.next != 0 {
		t.Fatalf("after reset: next = %v, want 0", bo.next)
	}
}

// TestConnectBackoffSleepsJittered pins the interval and checks the actual
// sleep lands in the jitter band [d/2, d] (with CI-noise headroom above).
func TestConnectBackoffSleepsJittered(t *testing.T) {
	bo := connectBackoff{next: 80 * time.Millisecond}
	start := time.Now()
	if !bo.sleep(context.Background(), nil) {
		t.Fatal("sleep aborted without cancellation")
	}
	elapsed := time.Since(start)
	if elapsed < 40*time.Millisecond {
		t.Fatalf("slept %v, want >= 40ms (lower jitter bound)", elapsed)
	}
	if elapsed > 300*time.Millisecond {
		t.Fatalf("slept %v, want ~<= 80ms (upper jitter bound + CI noise)", elapsed)
	}
}

// TestConnectBackoffAbort verifies both abort paths — context cancellation
// and the abort channel (Close()) — cut a long sleep short.
func TestConnectBackoffAbort(t *testing.T) {
	t.Run("context", func(st *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		bo := connectBackoff{next: time.Second}
		start := time.Now()
		if bo.sleep(ctx, nil) {
			st.Fatal("sleep completed despite ctx cancellation")
		}
		if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
			st.Fatalf("aborted sleep took %v, want well under the 0.5-1s interval", elapsed)
		}
	})

	t.Run("abort channel", func(st *testing.T) {
		abort := make(chan struct{})
		go func() {
			time.Sleep(20 * time.Millisecond)
			close(abort)
		}()
		bo := connectBackoff{next: time.Second}
		start := time.Now()
		if bo.sleep(context.Background(), abort) {
			st.Fatal("sleep completed despite abort channel close")
		}
		if elapsed := time.Since(start); elapsed > 400*time.Millisecond {
			st.Fatalf("aborted sleep took %v, want well under the 0.5-1s interval", elapsed)
		}
	})
}

func TestFailFastTracker(t *testing.T) {
	t0 := time.Now()
	dialErr := errors.New("dial tcp 127.0.0.1:1: connect: connection refused")

	t.Run("trips after window with no success", func(st *testing.T) {
		f := newFailFastTracker(5 * time.Second)
		if err := f.failure(t0, dialErr); err != nil {
			st.Fatalf("first failure must arm, not trip: %v", err)
		}
		if err := f.failure(t0.Add(4*time.Second), dialErr); err != nil {
			st.Fatalf("failure inside window must not trip: %v", err)
		}
		err := f.failure(t0.Add(5*time.Second), dialErr)
		if err == nil {
			st.Fatal("expected fatal error once the streak spans the window")
		}
		if !errors.Is(err, ErrNeverConnected) {
			st.Errorf("fatal error must wrap ErrNeverConnected: %v", err)
		}
		if !errors.Is(err, dialErr) {
			st.Errorf("fatal error must wrap the last dial error: %v", err)
		}
		// Shape must match the h1client pre-dial failure so the harness
		// classifies both as dnf.
		if !strings.Contains(err.Error(), "loadgen: dial: ") ||
			!strings.Contains(err.Error(), "connection refused") {
			st.Errorf("fatal error shape mismatch: %q", err.Error())
		}
	})

	t.Run("any success disarms permanently", func(st *testing.T) {
		f := newFailFastTracker(5 * time.Second)
		if err := f.failure(t0, dialErr); err != nil {
			st.Fatal(err)
		}
		f.success()
		if err := f.failure(t0.Add(time.Hour), dialErr); err != nil {
			st.Fatalf("post-success failure streak must never trip: %v", err)
		}
		if err := f.failure(t0.Add(2*time.Hour), dialErr); err != nil {
			st.Fatalf("post-success failure streak must never trip: %v", err)
		}
	})
}
