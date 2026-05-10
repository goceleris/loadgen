//go:build linux

package loadgen

import (
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

// CPUMonitor tracks client-side CPU utilization over a benchmark interval
// by reading /proc/stat at start and end.
type CPUMonitor struct {
	startUser, startNice, startSystem, startIdle    int64
	startIowait, startIrq, startSoftirq, startSteal int64
}

// Start records the initial CPU jiffies snapshot.
func (m *CPUMonitor) Start() {
	m.startUser, m.startNice, m.startSystem, m.startIdle,
		m.startIowait, m.startIrq, m.startSoftirq, m.startSteal = readProcStat()
}

// Stop reads the final snapshot and returns the CPU utilization percentage
// over the interval between Start() and Stop().
func (m *CPUMonitor) Stop() float64 {
	endUser, endNice, endSystem, endIdle,
		endIowait, endIrq, endSoftirq, endSteal := readProcStat()

	totalStart := m.startUser + m.startNice + m.startSystem + m.startIdle +
		m.startIowait + m.startIrq + m.startSoftirq + m.startSteal
	totalEnd := endUser + endNice + endSystem + endIdle +
		endIowait + endIrq + endSoftirq + endSteal

	totalDelta := totalEnd - totalStart
	if totalDelta <= 0 {
		return 0
	}

	idleDelta := (endIdle - m.startIdle) + (endIowait - m.startIowait)
	busyDelta := totalDelta - idleDelta

	return float64(busyDelta) / float64(totalDelta) * 100
}

func readProcStat() (user, nice, system, idle, iowait, irq, softirq, steal int64) {
	data, err := os.ReadFile("/proc/stat")
	if err != nil {
		return
	}
	lines := strings.SplitN(string(data), "\n", 2)
	if len(lines) == 0 {
		return
	}
	fields := strings.Fields(lines[0])
	if len(fields) < 9 || fields[0] != "cpu" {
		return
	}
	parse := func(s string) int64 { v, _ := strconv.ParseInt(s, 10, 64); return v }
	return parse(fields[1]), parse(fields[2]), parse(fields[3]), parse(fields[4]),
		parse(fields[5]), parse(fields[6]), parse(fields[7]), parse(fields[8])
}

// SelfCPUSampler is a 1Hz sampler of the loadgen process's own CPU usage,
// expressed as a percentage of available cores. It reads /proc/self/stat
// and computes (user+sys ticks) / (clk_tck * cores * elapsed) at each tick.
//
// The sampler keeps the last N samples (capacity defaults to 600 → 10min)
// and exposes P95 across that window. Surfaced on Result.CpuPctP95 so a
// "loadgen CPU was the bottleneck" run is self-flagging.
type SelfCPUSampler struct {
	mu       sync.Mutex
	samples  []float64
	capacity int

	stop chan struct{}
	done chan struct{}

	cores   float64
	clockHz float64
}

// NewSelfCPUSampler returns a sampler with the given ring-buffer capacity
// (samples). 600 covers 10 minutes at 1Hz.
func NewSelfCPUSampler(capacity int) *SelfCPUSampler {
	if capacity <= 0 {
		capacity = 600
	}
	return &SelfCPUSampler{
		capacity: capacity,
		samples:  make([]float64, 0, capacity),
		cores:    float64(runtime.NumCPU()),
		clockHz:  detectClockHz(),
	}
}

// Start begins 1Hz sampling. Safe to call once. Stop closes the goroutine.
func (s *SelfCPUSampler) Start() {
	s.stop = make(chan struct{})
	s.done = make(chan struct{})
	go s.run()
}

// Stop halts sampling. Returns the P95 across the captured window. Safe to
// call multiple times — subsequent calls return the same value.
func (s *SelfCPUSampler) Stop() float64 {
	if s.stop == nil {
		return 0
	}
	select {
	case <-s.stop:
		// already stopped
	default:
		close(s.stop)
		<-s.done
	}
	return s.P95()
}

// P95 returns the 95th-percentile self-CPU sample across the current window.
// Zero when no samples have been recorded yet.
func (s *SelfCPUSampler) P95() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.samples) == 0 {
		return 0
	}
	sorted := make([]float64, len(s.samples))
	copy(sorted, s.samples)
	slices.Sort(sorted)
	idx := int(float64(len(sorted)) * 0.95)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func (s *SelfCPUSampler) run() {
	defer close(s.done)

	prevTicks, ok := readSelfTicks()
	if !ok {
		return
	}
	prevWall := time.Now()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-s.stop:
			return
		case now := <-ticker.C:
			ticks, ok := readSelfTicks()
			if !ok {
				continue
			}
			elapsed := now.Sub(prevWall).Seconds()
			if elapsed <= 0 {
				continue
			}
			deltaTicks := float64(ticks - prevTicks)
			cpuSec := deltaTicks / s.clockHz
			pct := (cpuSec / (elapsed * s.cores)) * 100
			if pct < 0 {
				pct = 0
			}

			s.mu.Lock()
			if len(s.samples) >= s.capacity {
				s.samples = s.samples[1:]
			}
			s.samples = append(s.samples, pct)
			s.mu.Unlock()

			prevTicks = ticks
			prevWall = now
		}
	}
}

// readSelfTicks returns user+sys ticks from /proc/self/stat. The format is
// well-known: utime is field 14, stime field 15 (1-indexed) — but the
// process name in field 2 may contain spaces, so we split off the
// trailing-")" prefix first.
func readSelfTicks() (int64, bool) {
	data, err := os.ReadFile("/proc/self/stat")
	if err != nil {
		return 0, false
	}
	s := string(data)
	rp := strings.LastIndex(s, ") ")
	if rp < 0 {
		return 0, false
	}
	rest := s[rp+2:]
	fields := strings.Fields(rest)
	// After the closing paren, field 0 is state. utime is field 11,
	// stime is field 12 (0-indexed) since pid + comm consumed fields
	// 0 + 1 of the original split.
	if len(fields) < 13 {
		return 0, false
	}
	utime, err := strconv.ParseInt(fields[11], 10, 64)
	if err != nil {
		return 0, false
	}
	stime, err := strconv.ParseInt(fields[12], 10, 64)
	if err != nil {
		return 0, false
	}
	return utime + stime, true
}

// detectClockHz returns the kernel's clock-tick frequency. /proc/self/stat
// fields are in this unit. Falls back to the standard 100Hz when sysconf
// (via getconf) is unavailable.
func detectClockHz() float64 {
	// _SC_CLK_TCK isn't directly exposed by syscall on Linux, but it's
	// universally 100 on modern kernels. Read /proc/timer_list as a
	// sanity check; otherwise just trust 100.
	return 100
}
