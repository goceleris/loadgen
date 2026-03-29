//go:build !linux

package loadgen

// CPUMonitor is a no-op on non-Linux platforms.
type CPUMonitor struct{}

// Start is a no-op on non-Linux platforms.
func (m *CPUMonitor) Start() {}

// Stop returns 0 on non-Linux platforms (no CPU monitoring available).
func (m *CPUMonitor) Stop() float64 { return 0 }
