package loadgen

import (
	"context"
	"runtime"
	"testing"
	"time"
)

// TestRecvQProbeSmoke is a smoke test: on Linux, the probe must Start and
// Stop without error. On non-Linux it's a no-op stub.
func TestRecvQProbeSmoke(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("recvq probe is Linux-only")
	}
	p := newRecvQProbe()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	p.Start(ctx)
	got := p.Stop()
	// Idle process → no socket should have a deep recv-Q. False is fine.
	if got {
		t.Logf("recvq probe latched true (unusual but not necessarily a failure on shared CI)")
	}
}

// TestRecvQProbeNonLinuxStubReturnsFalse is a tautology on non-Linux but
// pins the contract: Stop must return false in the absence of /proc.
func TestRecvQProbeNonLinuxStubReturnsFalse(t *testing.T) {
	if runtime.GOOS == "linux" {
		t.Skip("non-Linux contract test")
	}
	p := newRecvQProbe()
	if p.Stop() {
		t.Error("non-Linux stub returned true")
	}
}
