package loadgen

import (
	"runtime"
	"testing"
	"time"
)

// TestSelfCPUSamplerStartStop is a smoke test: sampler must Start/Stop
// without panic and return a non-negative P95.
func TestSelfCPUSamplerStartStop(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("self-CPU sampler is Linux-only")
	}
	s := NewSelfCPUSampler(60)
	s.Start()
	time.Sleep(50 * time.Millisecond)
	got := s.Stop()
	if got < 0 {
		t.Errorf("P95 < 0: %v", got)
	}
}

// TestSelfCPUSamplerObservesLoad spins a CPU-burning goroutine for ~3s and
// asserts the sampler's P95 reflects measurable CPU activity.
func TestSelfCPUSamplerObservesLoad(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("self-CPU sampler is Linux-only")
	}
	if testing.Short() {
		t.Skip("skipping in short mode")
	}
	s := NewSelfCPUSampler(60)
	s.Start()

	stop := make(chan struct{})
	for range 2 {
		go func() {
			x := 0
			for {
				select {
				case <-stop:
					return
				default:
					for i := range 1000 {
						x ^= i
					}
					_ = x
				}
			}
		}()
	}
	// Need >=3 ticks for a stable percentile.
	time.Sleep(3500 * time.Millisecond)
	close(stop)
	got := s.Stop()
	if got <= 0 {
		t.Errorf("expected non-zero P95 under load, got %v", got)
	}
	t.Logf("self-CPU P95 under load: %.2f%%", got)
}

// TestSelfCPUSamplerNoSamples returns 0 when stopped before any tick.
func TestSelfCPUSamplerNoSamples(t *testing.T) {
	s := NewSelfCPUSampler(10)
	got := s.P95()
	if got != 0 {
		t.Errorf("P95 on empty sampler = %v, want 0", got)
	}
}
