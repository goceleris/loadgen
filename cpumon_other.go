//go:build !linux

package loadgen

// CPUMonitor is a no-op on non-Linux platforms.
type CPUMonitor struct{}

// Start is a no-op on non-Linux platforms.
func (m *CPUMonitor) Start() {}

// Stop returns 0 on non-Linux platforms (no CPU monitoring available).
func (m *CPUMonitor) Stop() float64 { return 0 }

// SelfCPUSampler is a no-op stub on platforms without /proc.
type SelfCPUSampler struct{}

// NewSelfCPUSampler returns a stub sampler. capacity is ignored.
func NewSelfCPUSampler(_ int) *SelfCPUSampler { return &SelfCPUSampler{} }

// Start is a no-op on non-Linux platforms.
func (s *SelfCPUSampler) Start() {}

// Stop returns 0 on non-Linux platforms.
func (s *SelfCPUSampler) Stop() float64 { return 0 }

// P95 returns 0 on non-Linux platforms.
func (s *SelfCPUSampler) P95() float64 { return 0 }
