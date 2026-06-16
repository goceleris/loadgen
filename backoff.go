package loadgen

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// Reconnect backoff bounds: the first retry waits ~reconnectBackoffMin,
// doubling per consecutive failure up to reconnectBackoffMax. Sleeps are
// jittered uniformly over [d/2, d] so a fleet of workers does not redial in
// lockstep against a recovering server. v3.8 post-mortem: without pacing,
// the ws/sse drivers redialled a dead port at ~386k dials/sec (34.7M errors
// per 90s cell).
const (
	reconnectBackoffMin = 10 * time.Millisecond
	reconnectBackoffMax = time.Second
)

// failFastWindow is how long a streaming client (ws/sse) tolerates
// nothing-but-connect-failures before declaring the target dead. It only
// applies while NO stream was ever established — a client that had live
// streams keeps retrying with backoff indefinitely (the server may come
// back mid-cell).
const failFastWindow = 5 * time.Second

// ErrNeverConnected marks the fatal fail-fast condition: every connect
// attempt failed and not a single stream was ever established within
// failFastWindow. Benchmarker.Run aborts the whole run when a driver
// surfaces an error wrapping this, so a harness can classify the cell as
// did-not-finish instead of burning the full duration against a dead port.
var ErrNeverConnected = errors.New("no stream ever established")

// connectBackoff is per-connection (or per-worker) retry pacing state.
// Not safe for concurrent use — each instance is owned by one goroutine.
type connectBackoff struct {
	next time.Duration
}

// sleep blocks for the current backoff interval (jittered down to half) and
// doubles the interval for the following call, capped at
// reconnectBackoffMax. It returns early — reporting false — when ctx is
// cancelled or abort is closed, so Close() and run shutdown are never stuck
// behind a sleep. A nil abort channel is valid and never fires.
func (b *connectBackoff) sleep(ctx context.Context, abort <-chan struct{}) bool {
	d := b.next
	if d <= 0 {
		d = reconnectBackoffMin
	}
	b.next = min(d*2, reconnectBackoffMax)

	d = d/2 + rand.N(d/2+1) // uniform in [d/2, d]
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-abort:
		return false
	case <-t.C:
		return true
	}
}

// reset returns the backoff to its initial interval after a successful
// connect.
func (b *connectBackoff) reset() { b.next = 0 }

// failFastTracker decides when a streaming client that has NEVER had a live
// stream should give up on the whole run. One instance is shared by all
// workers of a client; any single established stream disarms it permanently.
type failFastTracker struct {
	window time.Duration

	mu            sync.Mutex
	everConnected bool
	failingSince  time.Time // zero when there is no active failure streak
}

func newFailFastTracker(window time.Duration) *failFastTracker {
	return &failFastTracker{window: window}
}

// success records an established stream; fail-fast is permanently disarmed.
func (f *failFastTracker) success() {
	f.mu.Lock()
	f.everConnected = true
	f.failingSince = time.Time{}
	f.mu.Unlock()
}

// failure records a connect failure observed at now. When the client never
// had a single live stream and the failure streak spans the configured
// window, it returns a fatal error wrapping both err and ErrNeverConnected;
// otherwise nil (caller backs off and retries).
func (f *failFastTracker) failure(now time.Time, err error) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.everConnected {
		return nil
	}
	if f.failingSince.IsZero() {
		f.failingSince = now
		return nil
	}
	if now.Sub(f.failingSince) < f.window {
		return nil
	}
	// Message shape mirrors the h1client pre-dial failure
	// ("loadgen: dial: ...: connection refused") so harness-side
	// classification treats the two identically.
	return fmt.Errorf("loadgen: dial: %w (fail-fast: %w within %v)", err, ErrNeverConnected, f.window)
}
