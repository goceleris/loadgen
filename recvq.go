//go:build linux

package loadgen

import (
	"context"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// recvQHighThresholdBytes is the per-socket recv-queue size above which a
// sample is "high". 64KB matches the SLO discussion in probatorium#11.
const recvQHighThresholdBytes = 64 * 1024

// recvQSustainWindow is how long the median recv-Q must stay above
// recvQHighThresholdBytes before we flag the run.
const recvQSustainWindow = 10 * time.Second

// recvQSampleInterval is the cadence at which we read /proc/net/tcp.
const recvQSampleInterval = 5 * time.Second

// recvQProbe samples per-socket receive-queue depths from /proc/net/tcp(6)
// every 5s. When the median across loadgen-owned sockets exceeds 64KB
// sustained for ≥10s, the probe latches a "high" flag. The flag is
// reported on Result.RecvQHigh so consumers can disambiguate "tail
// latency from server" vs "tail latency because the loadgen process
// could not drain responses fast enough."
type recvQProbe struct {
	high atomic.Bool

	mu        sync.Mutex
	stop      chan struct{}
	done      chan struct{}
	highSince time.Time
}

func newRecvQProbe() *recvQProbe {
	return &recvQProbe{
		stop: make(chan struct{}),
		done: make(chan struct{}),
	}
}

// Start launches the sampler. ctx cancellation aborts the loop in addition
// to Stop().
func (p *recvQProbe) Start(ctx context.Context) {
	go p.run(ctx)
}

// Stop halts the sampler and returns the latched "high" flag. Idempotent.
func (p *recvQProbe) Stop() bool {
	p.mu.Lock()
	stop := p.stop
	p.mu.Unlock()
	select {
	case <-stop:
		// already stopped
	default:
		close(stop)
		<-p.done
	}
	return p.high.Load()
}

func (p *recvQProbe) run(ctx context.Context) {
	defer close(p.done)

	ticker := time.NewTicker(recvQSampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-p.stop:
			return
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			median, ok := p.sampleMedianRecvQ()
			if !ok {
				// Couldn't read /proc — stay silent.
				p.mu.Lock()
				p.highSince = time.Time{}
				p.mu.Unlock()
				continue
			}
			p.mu.Lock()
			if median > recvQHighThresholdBytes {
				if p.highSince.IsZero() {
					p.highSince = now
				} else if now.Sub(p.highSince) >= recvQSustainWindow {
					p.high.Store(true)
				}
			} else {
				p.highSince = time.Time{}
			}
			p.mu.Unlock()
		}
	}
}

// sampleMedianRecvQ reads /proc/net/tcp and /proc/net/tcp6, filters by
// our own socket inodes (from /proc/self/fd/), and returns the median
// rx_queue across surviving rows. Returns ok=false on parse failure.
func (p *recvQProbe) sampleMedianRecvQ() (int64, bool) {
	myInodes, ok := readSelfSocketInodes()
	if !ok || len(myInodes) == 0 {
		return 0, false
	}

	var queues []int64
	for _, path := range []string{"/proc/net/tcp", "/proc/net/tcp6"} {
		queues = appendRecvQueuesForInodes(queues, path, myInodes)
	}
	if len(queues) == 0 {
		return 0, false
	}
	// median
	sortInt64(queues)
	mid := len(queues) / 2
	if len(queues)%2 == 0 {
		return (queues[mid-1] + queues[mid]) / 2, true
	}
	return queues[mid], true
}

// readSelfSocketInodes walks /proc/self/fd and collects inode numbers from
// "socket:[N]" symlinks. Returned as a set (map[uint64]struct{}-like).
func readSelfSocketInodes() (map[uint64]struct{}, bool) {
	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		return nil, false
	}
	out := make(map[uint64]struct{}, 32)
	for _, e := range entries {
		target, err := os.Readlink(filepath.Join("/proc/self/fd", e.Name()))
		if err != nil {
			continue
		}
		const prefix = "socket:["
		if !strings.HasPrefix(target, prefix) {
			continue
		}
		rest := target[len(prefix):]
		end := strings.IndexByte(rest, ']')
		if end < 0 {
			continue
		}
		ino, err := strconv.ParseUint(rest[:end], 10, 64)
		if err != nil {
			continue
		}
		out[ino] = struct{}{}
	}
	return out, true
}

// appendRecvQueuesForInodes parses a /proc/net/tcp(6) file and appends
// rx_queue values for sockets whose inode is in `inodes`. The format:
//
//	sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ...
//
// rx_queue is the 5th field (0-indexed 4) of "tx_queue:rx_queue".
func appendRecvQueuesForInodes(out []int64, path string, inodes map[uint64]struct{}) []int64 {
	data, err := os.ReadFile(path)
	if err != nil {
		return out
	}
	lines := strings.Split(string(data), "\n")
	if len(lines) <= 1 {
		return out
	}
	for _, line := range lines[1:] {
		fields := strings.Fields(line)
		if len(fields) < 10 {
			continue
		}
		// fields[4] is "tx_queue:rx_queue"
		qpair := fields[4]
		colon := strings.IndexByte(qpair, ':')
		if colon < 0 {
			continue
		}
		rxHex := qpair[colon+1:]
		rxq, err := strconv.ParseInt(rxHex, 16, 64)
		if err != nil {
			continue
		}
		// inode is field 9
		inoStr := fields[9]
		ino, err := strconv.ParseUint(inoStr, 10, 64)
		if err != nil {
			continue
		}
		if _, ok := inodes[ino]; !ok {
			continue
		}
		out = append(out, rxq)
	}
	return out
}

func sortInt64(a []int64) {
	// Small slices; insertion sort is fine.
	for i := 1; i < len(a); i++ {
		v := a[i]
		j := i - 1
		for j >= 0 && a[j] > v {
			a[j+1] = a[j]
			j--
		}
		a[j+1] = v
	}
}
