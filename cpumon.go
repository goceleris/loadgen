//go:build linux

package loadgen

import (
	"os"
	"strconv"
	"strings"
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
