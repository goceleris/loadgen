package loadgen

// Client-side request stamps for the rated (open-loop) path.
//
// OPT-IN and default OFF. When the stamps are not requested (Config.ClientStamps
// is nil and LOADGEN_CLIENT_STAMPS is unset) nothing in this file runs: no
// clock reads, no allocations, no Result block, and the rated workers are the
// same ratedWorker as before. The H2 client keeps one nil-checked pointer and
// one atomic load per response frame for the hooks, and nothing else.
//
// What it answers. Result.Latency in a rated run is measured from the
// INTENDED send time (Gil Tene's coordinated-omission correction) to the
// completion of the response. When that is slow the number alone cannot say
// whether the time went to waiting for a loadgen worker, to loadgen's own
// writer, to the wire and the server, to the client's receive side or to the
// Go runtime. The stamps split the client-observed latency of every
// request into the segments below, count how many requests exceeded a
// threshold (default 10 ms) in each, and keep the per-second structure and
// the joint segment vectors of the slow requests.
//
// Time line of one H2 request (every point a monotonic reading in ns since the
// start of the measured window):
//
//	t_int  intended send time (the scheduler's slot)
//	t_deq  a worker took the slot
//	t_sem  the worker got a stream token (streamSem) and hands the request to
//	       the connection's writer goroutine
//	t_wdq  the writer goroutine took the request off its queue
//	t_frm  the HEADERS (and body) frames are in the connection's bufio writer
//	t_fls  the Flush that covered the request is about to start
//	t_fl   that Flush returned (the bytes are in the kernel)
//	t_hdr  the read loop returned the first frame of the response (HEADERS)
//	t_end  the read loop saw END_STREAM and is about to hand it to the worker
//	t_wake the worker is running again (t_int + the recorded latency)
//
// Segments (SegmentOrder): the nine consecutive ones, then four sums.
//
//	worker_wait = t_deq - t_int      slot queue + scheduler lateness + all workers busy
//	stream_wait = t_sem - t_deq      waiting for a stream token
//	write_queue = t_wdq - t_sem      waiting for the connection's writer
//	encode      = t_frm - t_wdq      framing into the bufio writer
//	flush_wait  = t_fls - t_frm      waiting in the writer's batch for the next Flush
//	flush       = t_fl  - t_fls      the Flush (write syscall)
//	resp_wait   = t_hdr - t_fl       flush returned -> first response frame read
//	body        = t_end - t_hdr      first frame -> END_STREAM
//	wake        = t_wake - t_end     read loop -> worker running (goroutine wake)
//	sched_lag   = t_wdq - t_int      worker_wait + stream_wait + write_queue
//	write       = t_fl  - t_wdq      encode + flush_wait + flush
//	service     = t_wake - t_deq     everything after a worker took the slot
//	total       = t_wake - t_int     the latency the run records
//
// What it cannot see: the time a response spends in the kernel receive queue
// before the read loop runs is NOT separable from resp_wait in user space.
// The runtime block (Go scheduler latency and GC pause histograms, deltas over
// the window) bounds how long runnable goroutines of the whole process waited
// for a P and how long the process stood still, and RecvQProbe reports the
// socket backlog; neither is per request. No header or id is added to any
// request, so the frames on the wire are byte-identical and a join with
// server-side stamps is by time bucket and distribution, never per request.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/bits"
	"os"
	"runtime/metrics"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// ClientStampsEnv is the environment variable that turns the client stamps on
// for rated runs without a code change. Values: "" / "0" / "off": off.
// "1" / "on" / "all": every rated run of the process. Otherwise a
// comma-separated list of target rates (requests per second): only a rated run
// whose Config.Rate equals one of them (within 0.5) is stamped, which is how
// the first slot of an absolute rate plan is selected without loadgen knowing
// the slot index. Any other text is an error from New: a typo must not turn a
// pre-registered measurement off silently.
const ClientStampsEnv = "LOADGEN_CLIENT_STAMPS"

// ClientStampsFileEnv names a file; when it is set and a run produced a client
// stamps block, loadgen appends one JSON line {"loadgen_version", "target_rps",
// "duration_ns", "client_stamps"} to it. It lets a harness that copies only some
// fields of Result (the probatorium runner does) keep the block without a code
// change: the file travels with the cell's other logs. A write failure is
// reported on stderr and never fails the run.
const ClientStampsFileEnv = "LOADGEN_CLIENT_STAMPS_FILE"

// ClientStampsSchema is the version of the Result.ClientStamps block.
const ClientStampsSchema = "clientstamps/1"

// Defaults and caps of ClientStampsConfig.
const (
	defaultStampThreshold = 10 * time.Millisecond
	defaultStampVectors   = 512
	defaultStampSeconds   = 120
	maxStampVectors       = 1 << 16
	maxStampSeconds       = 3600
)

// ClientStampsConfig switches the client-side request stamps on. A nil
// Config.ClientStamps is OFF. It applies to a rated run (Config.Rate > 0)
// only; a saturation run ignores it.
type ClientStampsConfig struct {
	// Threshold is the per-segment "slow" line. Default 10ms.
	Threshold time.Duration
	// MaxVectors caps each of the two reservoirs of full segment vectors
	// (the first slow-service requests, and the first requests whose total
	// latency passed Threshold, both in completion order). Default 512.
	MaxVectors int
	// MaxSeconds caps the per-second table; later seconds fold into the last
	// row. Default 120.
	MaxSeconds int
}

// stampsFromEnv decides whether the environment asks for stamps for a run of
// the given rate. It returns an error for a value it cannot parse.
func stampsFromEnv(rate float64) (*ClientStampsConfig, error) {
	v := strings.TrimSpace(os.Getenv(ClientStampsEnv))
	switch strings.ToLower(v) {
	case "", "0", "off", "false":
		return nil, nil
	case "1", "on", "all", "true":
		if rate > 0 {
			return &ClientStampsConfig{}, nil
		}
		return nil, nil
	}
	match := false
	for _, tok := range strings.Split(v, ",") {
		tok = strings.TrimSpace(tok)
		r, err := strconv.ParseFloat(tok, 64)
		if err != nil || r <= 0 {
			return nil, fmt.Errorf("loadgen: %s=%q: want 0, 1, all or a comma-separated list of rates (requests per second), got %q", ClientStampsEnv, v, tok)
		}
		if rate > 0 && r-rate < 0.5 && rate-r < 0.5 {
			match = true
		}
	}
	if match {
		return &ClientStampsConfig{}, nil
	}
	return nil, nil
}

func (c *ClientStampsConfig) normalized() ClientStampsConfig {
	n := *c
	if n.Threshold <= 0 {
		n.Threshold = defaultStampThreshold
	}
	if n.MaxVectors <= 0 {
		n.MaxVectors = defaultStampVectors
	}
	if n.MaxVectors > maxStampVectors {
		n.MaxVectors = maxStampVectors
	}
	if n.MaxSeconds <= 0 {
		n.MaxSeconds = defaultStampSeconds
	}
	if n.MaxSeconds > maxStampSeconds {
		n.MaxSeconds = maxStampSeconds
	}
	return n
}

// Segment indexes. The first nBaseSeg are consecutive and additive.
const (
	segWorkerWait = iota
	segStreamWait
	segWriteQueue
	segEncode
	segFlushWait
	segFlush
	segRespWait
	segBody
	segWake
	nBaseSeg
)

// The four sums follow the consecutive segments.
const (
	segSchedLag = nBaseSeg + iota
	segWrite
	segService
	segTotal
	nSeg
)

var stampSegNames = [nSeg]string{
	"worker_wait", "stream_wait", "write_queue", "encode", "flush_wait",
	"flush", "resp_wait", "body", "wake",
	"sched_lag", "write", "service", "total",
}

// SegmentOrder is the order of every per-segment array in the block.
func stampSegmentOrder() []string { return slices.Clone(stampSegNames[:]) }

// stampPad keeps the per-worker records on their own cache lines.
type stampPad [64]byte

// stampRec is the stamp record of one worker. A rated worker has exactly one
// request in flight, so the record is reused for every request of that worker
// and nothing is allocated per request. The worker goroutine owns t_int,
// t_deq, t_sem and the counters; the H2 writer and reader goroutines write the
// six atomic stamps. seq names the current attempt: a stamp from a goroutine
// that still works for an earlier attempt (a retry through errH2NotSent) is
// dropped, never written into the new one.
type stampRec struct {
	seq atomic.Uint64
	tWdq,
	tFrm,
	tFls,
	tFl,
	tHdr,
	tEnd atomic.Int64

	tInt, tDeq, tSem int64
	attempts         int32
	h2               bool
	set              *stampSet
	_                stampPad
}

// attempt starts a new attempt (the first one, or a retry) and returns its
// sequence number. Worker goroutine only.
func (r *stampRec) attempt() uint64 {
	s := r.seq.Add(1)
	r.tWdq.Store(0)
	r.tFrm.Store(0)
	r.tFls.Store(0)
	r.tFl.Store(0)
	r.tHdr.Store(0)
	r.tEnd.Store(0)
	r.attempts++
	r.h2 = true
	return s
}

func (r *stampRec) markWdq(seq uint64) {
	if r.seq.Load() == seq {
		r.tWdq.Store(r.set.now())
	}
}

func (r *stampRec) markFrm(seq uint64) {
	if r.seq.Load() == seq {
		r.tFrm.Store(r.set.now())
	}
}

// markFlush stores the flush start and end of the batch that carried the
// request.
func (r *stampRec) markFlush(seq uint64, start, end int64) {
	if r.seq.Load() == seq {
		r.tFls.Store(start)
		r.tFl.Store(end)
	}
}

// markHdr keeps the first response frame only.
func (r *stampRec) markHdr(seq uint64) {
	if r.seq.Load() == seq && r.tHdr.Load() == 0 {
		r.tHdr.CompareAndSwap(0, r.set.now())
	}
}

func (r *stampRec) markEnd(seq uint64) {
	if r.seq.Load() == seq {
		r.tEnd.Store(r.set.now())
	}
}

// stampRef is one entry of a writer goroutine's pending-flush batch.
type stampRef struct {
	rec *stampRec
	seq uint64
}

// stampAttacher is implemented by a Client that can feed the stamps (the H2
// client). attachStamps(nil) detaches.
type stampAttacher interface {
	attachStamps(*stampSet)
}

// Log-linear bucket counts: four buckets per octave from 1.024 us up to
// about 70 s, so a bucket is at most 25 percent wide.
const stampBuckets = 104

func stampBucketOf(ns int64) int {
	if ns <= 0 {
		return 0
	}
	x := uint64(ns) >> 10
	if x == 0 {
		return 0
	}
	l := bits.Len64(x)
	var b int
	if l <= 2 {
		b = int(x)
	} else {
		b = 4*(l-2) + int((x>>(l-3))&3)
	}
	if b >= stampBuckets {
		b = stampBuckets - 1
	}
	return b
}

// stampBucketUpper is the exclusive upper bound of bucket b in ns.
func stampBucketUpper(b int) int64 {
	if b < 4 {
		return int64(b+1) << 10
	}
	l := b/4 + 2
	sub := b % 4
	return int64(5+sub) << (l - 3) << 10
}

type segAgg struct {
	n, sum, over, max int64
	b                 [stampBuckets]uint32
}

func (a *segAgg) add(ns, thr int64) {
	a.n++
	a.sum += ns
	if ns > a.max {
		a.max = ns
	}
	if ns > thr {
		a.over++
	}
	a.b[stampBucketOf(ns)]++
}

// quantile is the upper bound of the bucket holding the q-th value, capped at
// the exact maximum.
func (a *segAgg) quantile(q float64) int64 {
	if a.n == 0 {
		return 0
	}
	want := int64(float64(a.n)*q + 0.999999)
	if want < 1 {
		want = 1
	}
	var run int64
	for i, c := range a.b {
		run += int64(c)
		if run >= want {
			u := stampBucketUpper(i)
			if u > a.max {
				u = a.max
			}
			return u
		}
	}
	return a.max
}

func (a *segAgg) merge(o *segAgg) {
	a.n += o.n
	a.sum += o.sum
	a.over += o.over
	if o.max > a.max {
		a.max = o.max
	}
	for i := range a.b {
		a.b[i] += o.b[i]
	}
}

// secAgg is one row of the per-second table. Rows are keyed by the second in
// which the worker took the slot (t_deq): that is when the request went to
// work, so the row lines up with the wall clock of the server and the wire.
// Keyed by the intended time instead, a request of a backlogged run would sit
// up to the whole backlog (seconds) before the second it was served in.
// intended counts the slots by their intended second (the offered load).
type secAgg struct {
	n          uint32
	intended   uint32
	over       [nSeg]uint32
	max        [nSeg]int64
	sum        [nSeg]int64 // per-segment sum in ns: lets a reader take means and additive shares per second
	domSvc     [nBaseSeg]uint32
	incomplete uint32
	slotqMax   int32
	busyMax    int32
}

type stampShard struct {
	seg        [nSeg]segAgg
	sec        []secAgg
	domService [nBaseSeg]int64
	domTotal   [nBaseSeg]int64
	errors     int64
	incomplete int64
	retried    int64
	clamped    [nBaseSeg]int64
	_          stampPad
}

// stampSet is the stamp state of one rated run.
type stampSet struct {
	cfg  ClientStampsConfig
	thr  int64
	base time.Time // the measured window's start (monotonic)
	recs []stampRec
	sh   []stampShard
	_    stampPad
	busy atomic.Int32 // on its own cache line: every worker adds to it per request
	_    stampPad
	h2   bool

	sched schedStats

	mu      sync.Mutex
	slowSvc []StampVector
	firstLt []StampVector
	nSvc    atomic.Int64 // vectors kept in slowSvc, for the lock-free "full" check
	nLate   atomic.Int64

	rtStart rtSnapshot
	rt      []rtSec // per wall-clock second of the window; written by the sampler goroutine
	rtStop  chan struct{}
	rtDone  chan struct{}
}

func newStampSet(c *ClientStampsConfig, workers int, base time.Time) *stampSet {
	cfg := c.normalized()
	if workers < 1 {
		workers = 1
	}
	s := &stampSet{
		cfg:  cfg,
		thr:  int64(cfg.Threshold),
		base: base,
		recs: make([]stampRec, workers),
		sh:   make([]stampShard, workers),
	}
	for i := range s.recs {
		s.recs[i].set = s
	}
	for i := range s.sh {
		s.sh[i].sec = make([]secAgg, cfg.MaxSeconds)
	}
	s.sched.sec = make([]schedSec, cfg.MaxSeconds)
	return s
}

// now is the monotonic time in ns since the window start.
func (s *stampSet) now() int64 { return int64(time.Since(s.base)) }

// rec returns worker w's record, or nil when w is out of range.
func (s *stampSet) rec(w int) *stampRec {
	if w < 0 || w >= len(s.recs) {
		return nil
	}
	return &s.recs[w]
}

func (s *stampSet) secIdx(tNS int64) int {
	i := int(tNS / int64(time.Second))
	if i < 0 {
		i = 0
	}
	if i >= s.cfg.MaxSeconds {
		i = s.cfg.MaxSeconds - 1
	}
	return i
}

// begin is called by a worker right after it took a slot.
func (s *stampSet) begin(w int, intended time.Time, slotQ int) *stampRec {
	busy := s.busy.Add(1)
	r := s.rec(w)
	if r == nil {
		return nil
	}
	r.tInt = int64(intended.Sub(s.base))
	r.tDeq = s.now()
	r.tSem = 0
	r.attempts = 0
	r.h2 = false
	sh := &s.sh[w]
	sh.sec[s.secIdx(r.tInt)].intended++
	sec := &sh.sec[s.secIdx(r.tDeq)]
	if int32(slotQ) > sec.slotqMax {
		sec.slotqMax = int32(slotQ)
	}
	if busy > sec.busyMax {
		sec.busyMax = busy
	}
	return r
}

// fail records a request that ended in an error (it enters no segment).
func (s *stampSet) fail(w int) {
	s.busy.Add(-1)
	if w >= 0 && w < len(s.sh) {
		s.sh[w].errors++
	}
}

// finish turns the stamps of a successful request into segments. latency is
// the value the run recorded for it (time since the intended send).
func (s *stampSet) finish(w int, r *stampRec, latency time.Duration) {
	s.busy.Add(-1)
	if r == nil {
		return
	}
	sh := &s.sh[w]
	tWake := r.tInt + int64(latency)

	var seg [nSeg]int64
	var have [nSeg]bool
	diff := func(i int, a, b int64) {
		d := b - a
		if d < 0 {
			sh.clamped[i]++
			d = 0
		}
		seg[i] = d
		have[i] = true
	}
	diff(segWorkerWait, r.tInt, r.tDeq)
	diff(segService, r.tDeq, tWake)
	diff(segTotal, r.tInt, tWake)
	// segTotal is exactly the recorded latency (a clamp here would be a bug).
	seg[segTotal] = int64(latency)

	complete := false
	if r.h2 {
		if r.attempts > 1 {
			sh.retried++
		}
		tWdq, tFrm, tFls, tFl := r.tWdq.Load(), r.tFrm.Load(), r.tFls.Load(), r.tFl.Load()
		tHdr, tEnd := r.tHdr.Load(), r.tEnd.Load()
		complete = r.tSem != 0 && tWdq != 0 && tFrm != 0 && tFls != 0 && tFl != 0 && tHdr != 0 && tEnd != 0
		if complete {
			diff(segStreamWait, r.tDeq, r.tSem)
			diff(segWriteQueue, r.tSem, tWdq)
			diff(segEncode, tWdq, tFrm)
			diff(segFlushWait, tFrm, tFls)
			diff(segFlush, tFls, tFl)
			diff(segRespWait, tFl, tHdr)
			diff(segBody, tHdr, tEnd)
			diff(segWake, tEnd, tWake)
			seg[segSchedLag] = seg[segWorkerWait] + seg[segStreamWait] + seg[segWriteQueue]
			seg[segWrite] = seg[segEncode] + seg[segFlushWait] + seg[segFlush]
			have[segSchedLag], have[segWrite] = true, true
		} else {
			sh.incomplete++
			sh.sec[s.secIdx(r.tDeq)].incomplete++
		}
	}

	sec := &sh.sec[s.secIdx(r.tDeq)]
	sec.n++
	for i := range nSeg {
		if !have[i] {
			continue
		}
		sh.seg[i].add(seg[i], s.thr)
		sec.sum[i] += seg[i]
		if seg[i] > s.thr {
			sec.over[i]++
		}
		if seg[i] > sec.max[i] {
			sec.max[i] = seg[i]
		}
	}

	if !complete {
		return
	}
	slowSvc := seg[segService] > s.thr
	late := seg[segTotal] > s.thr
	if slowSvc {
		d := dominant(&seg, segStreamWait) // worker_wait is not part of service
		sh.domService[d]++
		sec.domSvc[d]++
	}
	if late {
		sh.domTotal[dominant(&seg, segWorkerWait)]++
	}
	if slowSvc && int(s.nSvc.Load()) < s.cfg.MaxVectors {
		s.keep(&s.slowSvc, &s.nSvc, w, r, &seg)
	}
	if late && int(s.nLate.Load()) < s.cfg.MaxVectors {
		s.keep(&s.firstLt, &s.nLate, w, r, &seg)
	}
}

// dominant is the consecutive segment, from index from on, with the largest
// duration (the first of equals).
func dominant(seg *[nSeg]int64, from int) int {
	best := from
	for i := from + 1; i < nBaseSeg; i++ {
		if seg[i] > seg[best] {
			best = i
		}
	}
	return best
}

func (s *stampSet) keep(dst *[]StampVector, n *atomic.Int64, w int, r *stampRec, seg *[nSeg]int64) {
	v := StampVector{TIntNS: r.tInt, Worker: w, TotalNS: seg[segTotal], Attempts: int(r.attempts)}
	v.SegsNS = make([]int64, nBaseSeg)
	copy(v.SegsNS, seg[:nBaseSeg])
	s.mu.Lock()
	if len(*dst) < s.cfg.MaxVectors {
		*dst = append(*dst, v)
		n.Store(int64(len(*dst)))
	}
	s.mu.Unlock()
}

// schedStats are the rate scheduler's own lateness figures: how far behind the
// intended time the scheduler goroutine was when it enqueued each slot.
// Written by the scheduler goroutine only, read after it exits.
type schedStats struct {
	agg segAgg
	sec []schedSec
}

type schedSec struct {
	ticks, over uint32
	max         int64
}

// tick records one slot's lateness. intended is the slot's intended time.
func (s *stampSet) tick(intended time.Time) {
	late := int64(time.Since(intended))
	if late < 0 {
		late = 0
	}
	s.sched.agg.add(late, s.thr)
	sec := &s.sched.sec[s.secIdx(int64(intended.Sub(s.base))+late)]
	sec.ticks++
	if late > s.thr {
		sec.over++
	}
	if late > sec.max {
		sec.max = late
	}
}

// ---- Result block -------------------------------------------------------

// ClientStamps is Result.ClientStamps: the optional, versioned block with the
// client-side split of the rated run's latency. Absent unless the stamps were
// requested for the run.
type ClientStamps struct {
	Schema string `json:"schema"`
	// ThresholdNS is the "slow" line every over count uses.
	ThresholdNS int64 `json:"threshold_ns"`
	// StartUnixNS is the wall clock of the measured window's start, to
	// align the per-second rows with server-side stamps.
	StartUnixNS int64 `json:"start_unix_ns"`
	DurationNS  int64 `json:"duration_ns"`
	// H2Stamps is false when the client is not the H2 client: only
	// worker_wait, service and total are then filled.
	H2Stamps bool  `json:"h2_stamps"`
	Workers  int   `json:"workers"`
	Requests int64 `json:"requests"`
	Errors   int64 `json:"errors"`
	// Incomplete counts H2 requests missing one of the six goroutine
	// stamps (a late flush stamp, an abandoned stream): they enter the
	// worker-level segments only.
	Incomplete int64 `json:"incomplete"`
	Retried    int64 `json:"retried"`
	// Clamped counts, per consecutive segment, requests whose later stamp
	// was earlier than the earlier one (a response read before the writer
	// stored its flush stamp is the common case); the segment is 0 then.
	Clamped      map[string]int64 `json:"clamped,omitempty"`
	SegmentOrder []string         `json:"segment_order"`
	Segments     []SegmentStats   `json:"segments"`
	PerSecond    []StampSecond    `json:"per_second"`
	Scheduler    SchedulerStats   `json:"scheduler"`
	Runtime      *RuntimeStats    `json:"runtime,omitempty"`
	// DominantSlowService / DominantSlowTotal count, over all complete
	// requests (not the reservoirs) whose service (total) exceeded the
	// threshold, the consecutive segment with the largest duration: among
	// the eight segments after worker_wait for service, among all nine for
	// total.
	DominantSlowService map[string]int64 `json:"dominant_slow_service,omitempty"`
	DominantSlowTotal   map[string]int64 `json:"dominant_slow_total,omitempty"`
	SlowService         []StampVector    `json:"slow_service,omitempty"`
	FirstLate           []StampVector    `json:"first_late,omitempty"`
	Notes               []string         `json:"notes"`
}

// SegmentStats is the distribution of one segment over the stamped requests.
// The quantiles are upper bounds of log-linear buckets (at most 25 percent
// wide), capped at Max.
type SegmentStats struct {
	N      int64 `json:"n"`
	SumNS  int64 `json:"sum_ns"`
	MaxNS  int64 `json:"max_ns"`
	Over   int64 `json:"over"`
	P50NS  int64 `json:"p50_ns"`
	P99NS  int64 `json:"p99_ns"`
	P999NS int64 `json:"p999_ns"`
}

// StampSecond is one row of the per-second table, keyed by the second (of the
// window, wall clock) in which a worker took the slot, t_deq. Over and MaxNS
// are aligned with SegmentOrder.
type StampSecond struct {
	T int `json:"t"`
	// Requests is the number of stamped requests whose worker took the slot
	// in this second; Intended the number of slots intended in it.
	Requests int64   `json:"requests"`
	Intended int64   `json:"intended"`
	Over     []int64 `json:"over"`
	MaxNS    []int64 `json:"max_ns"`
	// SumNS is the per-segment sum over the row's requests, aligned with
	// SegmentOrder: with Requests it gives per-second means, and over the
	// nine consecutive segments the additive decomposition of the service
	// time (a pure throughput deficit has no slow request to count).
	SumNS []int64 `json:"sum_ns"`
	// Incomplete counts the row's H2 requests that missed a goroutine stamp.
	Incomplete int64 `json:"incomplete"`
	// DominantSlowService is aligned with the first nine segments
	// (worker_wait is always 0): the requests of this second whose service
	// exceeded the threshold, by their largest consecutive segment.
	DominantSlowService []int64 `json:"dominant_slow_service"`
	// SlotQMax is the deepest slot queue a worker saw when taking a slot;
	// BusyMax the most workers busy at once (the in-flight window).
	SlotQMax int `json:"slot_queue_max"`
	BusyMax  int `json:"busy_max"`
	// SchedTicks, SchedLateOver and SchedLateMaxNS are the rate
	// scheduler's own lateness for the slots it enqueued in this second.
	SchedTicks     int64 `json:"sched_ticks"`
	SchedLateOver  int64 `json:"sched_late_over"`
	SchedLateMaxNS int64 `json:"sched_late_max_ns"`
	// The runtime fields are the deltas, over this wall-clock second of the
	// window, of the Go runtime's own histograms (process-wide; the
	// scheduler-latency histogram counts a sample of goroutine wakeups, not
	// requests): wakeups that waited >= 10 ms for a P, the upper bound of
	// the longest bucket that gained samples, GC pauses >= 1 ms, GC cycles.
	RtSchedOver10  int64 `json:"rt_sched_over_10ms"`
	RtSchedMaxLENS int64 `json:"rt_sched_max_le_ns"`
	RtGCOver1      int64 `json:"rt_gc_over_1ms"`
	RtGCCycles     int64 `json:"rt_gc_cycles"`
}

// SchedulerStats is the rate scheduler goroutine's lateness over the window.
type SchedulerStats struct {
	Ticks     int64 `json:"ticks"`
	LateOver  int64 `json:"late_over"`
	LateP99NS int64 `json:"late_p99_ns"`
	LateMaxNS int64 `json:"late_max_ns"`
}

// StampVector is the full segment vector of one slow request. SegsNS is aligned
// with the first nine entries of SegmentOrder.
type StampVector struct {
	TIntNS   int64   `json:"t_int_ns"`
	Worker   int     `json:"worker"`
	TotalNS  int64   `json:"total_ns"`
	Attempts int     `json:"attempts"`
	SegsNS   []int64 `json:"segs_ns"`
}

// RuntimeStats are the deltas of the Go runtime's own histograms over the
// window, process-wide.
type RuntimeStats struct {
	SchedLatency *HistDelta `json:"sched_latency,omitempty"`
	GCPause      *HistDelta `json:"gc_pause,omitempty"`
	GCCycles     uint64     `json:"gc_cycles"`
}

// HistDelta summarises the change of a runtime/metrics histogram. MaxLENS is
// the upper bound of the highest bucket that gained samples (0 when none).
type HistDelta struct {
	Count    uint64 `json:"count"`
	Over1ms  uint64 `json:"over_1ms"`
	Over10ms uint64 `json:"over_10ms"`
	MaxLENS  int64  `json:"max_le_ns"`
}

var stampNotes = []string{
	"segments are user-space monotonic stamps; time in the kernel receive queue before the read loop runs is inside resp_wait/body and is not separable per request",
	"worker_wait contains the slot queue, the scheduler's lateness and the wait for a free worker; read it with busy_max, slot_queue_max and scheduler",
	"a request counts in over when that segment alone exceeded threshold_ns; over counts of different segments are not additive and the distributions do not add up (use the vectors for the joint view)",
	"runtime is process-wide; its sched_latency counts every goroutine, not only the stamped ones",
	"frames on the wire are byte-identical to an unstamped run: a join with server-side stamps is by time bucket and distribution only",
}

// result builds the block after every worker and the scheduler have stopped.
func (s *stampSet) result(elapsed time.Duration, startWall time.Time) *ClientStamps {
	var agg [nSeg]segAgg
	var errs, incomplete, retried int64
	var clamped [nBaseSeg]int64
	var domS, domT [nBaseSeg]int64
	for i := range s.sh {
		sh := &s.sh[i]
		for j := range agg {
			agg[j].merge(&sh.seg[j])
		}
		errs += sh.errors
		incomplete += sh.incomplete
		retried += sh.retried
		for j := range nBaseSeg {
			clamped[j] += sh.clamped[j]
			domS[j] += sh.domService[j]
			domT[j] += sh.domTotal[j]
		}
	}
	cs := &ClientStamps{
		Schema:       ClientStampsSchema,
		ThresholdNS:  s.thr,
		StartUnixNS:  startWall.UnixNano(),
		DurationNS:   int64(elapsed),
		H2Stamps:     s.h2,
		Workers:      len(s.recs),
		Requests:     agg[segTotal].n,
		Errors:       errs,
		Incomplete:   incomplete,
		Retried:      retried,
		SegmentOrder: stampSegmentOrder(),
		Notes:        slices.Clone(stampNotes),
	}
	for j := range nBaseSeg {
		if clamped[j] > 0 {
			if cs.Clamped == nil {
				cs.Clamped = map[string]int64{}
			}
			cs.Clamped[stampSegNames[j]] = clamped[j]
		}
	}
	cs.Segments = make([]SegmentStats, nSeg)
	for j := range agg {
		a := &agg[j]
		cs.Segments[j] = SegmentStats{N: a.n, SumNS: a.sum, MaxNS: a.max, Over: a.over,
			P50NS: a.quantile(0.50), P99NS: a.quantile(0.99), P999NS: a.quantile(0.999)}
	}
	dom := func(d *[nBaseSeg]int64) map[string]int64 {
		var m map[string]int64
		for j, c := range d {
			if c > 0 {
				if m == nil {
					m = map[string]int64{}
				}
				m[stampSegNames[j]] = c
			}
		}
		return m
	}
	cs.DominantSlowService, cs.DominantSlowTotal = dom(&domS), dom(&domT)

	// Per-second rows up to the last second with any request or tick.
	last := -1
	for sec := range s.cfg.MaxSeconds {
		if s.sched.sec[sec].ticks > 0 {
			last = sec
		}
		for i := range s.sh {
			if s.sh[i].sec[sec].n > 0 || s.sh[i].sec[sec].intended > 0 {
				last = max(last, sec)
			}
		}
	}
	cs.PerSecond = make([]StampSecond, 0, last+1)
	for sec := 0; sec <= last; sec++ {
		row := StampSecond{T: sec, Over: make([]int64, nSeg), MaxNS: make([]int64, nSeg), SumNS: make([]int64, nSeg), DominantSlowService: make([]int64, nBaseSeg)}
		for i := range s.sh {
			x := &s.sh[i].sec[sec]
			row.Requests += int64(x.n)
			row.Intended += int64(x.intended)
			row.Incomplete += int64(x.incomplete)
			for j := range nBaseSeg {
				row.DominantSlowService[j] += int64(x.domSvc[j])
			}
			for j := range nSeg {
				row.Over[j] += int64(x.over[j])
				row.MaxNS[j] = max(row.MaxNS[j], x.max[j])
				row.SumNS[j] += x.sum[j]
			}
			row.SlotQMax = max(row.SlotQMax, int(x.slotqMax))
			row.BusyMax = max(row.BusyMax, int(x.busyMax))
		}
		sc := s.sched.sec[sec]
		row.SchedTicks, row.SchedLateOver, row.SchedLateMaxNS = int64(sc.ticks), int64(sc.over), sc.max
		if sec < len(s.rt) {
			rt := s.rt[sec]
			row.RtSchedOver10, row.RtSchedMaxLENS, row.RtGCOver1, row.RtGCCycles = rt.schedOver10, rt.schedMaxLE, rt.gcOver1, rt.gcCycles
		}
		cs.PerSecond = append(cs.PerSecond, row)
	}
	cs.Scheduler = SchedulerStats{
		Ticks: s.sched.agg.n, LateOver: s.sched.agg.over,
		LateP99NS: s.sched.agg.quantile(0.99), LateMaxNS: s.sched.agg.max,
	}
	cs.Runtime = s.runtimeDelta()

	s.mu.Lock()
	cs.SlowService = slices.Clone(s.slowSvc)
	cs.FirstLate = slices.Clone(s.firstLt)
	s.mu.Unlock()
	return cs
}

// stampsFileLine is one line of the ClientStampsFileEnv file.
type stampsFileLine struct {
	LoadgenVersion string  `json:"loadgen_version"`
	TargetRPS      float64 `json:"target_rps"`
	Requests       int64   `json:"requests"`
	DurationNS     int64   `json:"duration_ns"`
	// ClientCPUPercent and CPUPctP95 are the Result's own client-load
	// figures (whole system, and loadgen's process), for the observer-effect
	// comparison of a stamped pass with an unstamped one.
	ClientCPUPercent float64      `json:"client_cpu_percent"`
	CPUPctP95        float64      `json:"cpu_pct_p95"`
	ClientStamps     ClientStamps `json:"client_stamps"`
}

// appendStampsFile writes res.ClientStamps to the ClientStampsFileEnv file.
func appendStampsFile(res *Result) {
	path := os.Getenv(ClientStampsFileEnv)
	if path == "" || res == nil || res.ClientStamps == nil {
		return
	}
	line, err := json.Marshal(stampsFileLine{
		LoadgenVersion: res.LoadgenVersion, TargetRPS: res.TargetRPS, Requests: res.Requests,
		DurationNS: int64(res.Duration), ClientCPUPercent: res.ClientCPUPercent, CPUPctP95: res.CPUPctP95, ClientStamps: *res.ClientStamps,
	})
	if err == nil {
		var f *os.File
		if f, err = os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644); err == nil {
			_, err = f.Write(append(line, '\n'))
			if cerr := f.Close(); err == nil {
				err = cerr
			}
		}
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "loadgen: %s: %v\n", ClientStampsFileEnv, err)
	}
}

// ---- runtime/metrics ----------------------------------------------------

const (
	rtSchedLat = "/sched/latencies:seconds"
	rtGCPause  = "/sched/pauses/total/gc:seconds"
	rtGCOld    = "/gc/pauses:seconds"
	rtGCCycles = "/gc/cycles/total:gc-cycles"
)

type rtSnapshot struct {
	ok       bool
	sched    *metrics.Float64Histogram
	gc       *metrics.Float64Histogram
	gcCycles uint64
}

func rtRead() rtSnapshot {
	names := []string{rtSchedLat, rtGCPause, rtGCCycles}
	samples := make([]metrics.Sample, len(names))
	for i, n := range names {
		samples[i].Name = n
	}
	metrics.Read(samples)
	var s rtSnapshot
	hist := func(i int) *metrics.Float64Histogram {
		if samples[i].Value.Kind() != metrics.KindFloat64Histogram {
			return nil
		}
		h := samples[i].Value.Float64Histogram()
		return &metrics.Float64Histogram{Counts: slices.Clone(h.Counts), Buckets: slices.Clone(h.Buckets)}
	}
	s.sched = hist(0)
	s.gc = hist(1)
	if samples[2].Value.Kind() == metrics.KindUint64 {
		s.gcCycles = samples[2].Value.Uint64()
	}
	s.ok = true
	return s
}

func (s *stampSet) runtimeStart() {
	s.rtStart = rtRead()
	s.rtStop, s.rtDone = make(chan struct{}), make(chan struct{})
	go s.sampleRuntime()
}

// rtSec is the delta of the runtime histograms over one second of the window.
type rtSec struct {
	schedOver10, schedMaxLE, gcOver1, gcCycles int64
}

// sampleRuntime reads the runtime histograms at every second boundary of the
// window and keeps the per-second deltas, so a runtime stall can be placed in
// the same second as a slow stretch of requests. runtimeStop ends it.
func (s *stampSet) sampleRuntime() {
	defer close(s.rtDone)
	prev := s.rtStart
	row := func() {
		cur := rtRead()
		var r rtSec
		if d := histDelta(prev.sched, cur.sched); d != nil {
			r.schedOver10, r.schedMaxLE = int64(d.Over10ms), d.MaxLENS
		}
		if d := histDelta(prev.gc, cur.gc); d != nil {
			r.gcOver1 = int64(d.Over1ms)
		}
		r.gcCycles = int64(cur.gcCycles - prev.gcCycles)
		if len(s.rt) < s.cfg.MaxSeconds {
			s.rt = append(s.rt, r)
		}
		prev = cur
	}
	for k := 1; ; k++ {
		t := time.NewTimer(time.Until(s.base.Add(time.Duration(k) * time.Second)))
		select {
		case <-s.rtStop:
			t.Stop()
			row() // the last, partial second
			return
		case <-t.C:
			row()
		}
	}
}

// runtimeStop ends the sampler and waits for it.
func (s *stampSet) runtimeStop() {
	if s.rtStop != nil {
		close(s.rtStop)
		<-s.rtDone
		s.rtStop = nil
	}
}

func (s *stampSet) runtimeDelta() *RuntimeStats {
	if !s.rtStart.ok {
		return nil
	}
	end := rtRead()
	return &RuntimeStats{
		SchedLatency: histDelta(s.rtStart.sched, end.sched),
		GCPause:      histDelta(s.rtStart.gc, end.gc),
		GCCycles:     end.gcCycles - s.rtStart.gcCycles,
	}
}

// histDelta is end - start for two reads of the same histogram, or nil when
// either read or their layouts are unusable.
func histDelta(start, end *metrics.Float64Histogram) *HistDelta {
	if start == nil || end == nil || len(start.Counts) != len(end.Counts) || len(end.Buckets) != len(end.Counts)+1 {
		return nil
	}
	d := &HistDelta{}
	for i := range end.Counts {
		if end.Counts[i] < start.Counts[i] {
			return nil
		}
		c := end.Counts[i] - start.Counts[i]
		if c == 0 {
			continue
		}
		d.Count += c
		lo, hi := end.Buckets[i], end.Buckets[i+1]
		if lo >= 0.001 {
			d.Over1ms += c
		}
		if lo >= 0.010 {
			d.Over10ms += c
		}
		if hi > 1e9 { // +Inf or absurd: cap at 1000 s
			hi = 1e9
		}
		d.MaxLENS = max(d.MaxLENS, int64(hi*1e9))
	}
	return d
}

// ---- Benchmarker integration -------------------------------------------

// ratedWorkerStamped is ratedWorker with the stamps. It must stay the same as
// ratedWorker in every other respect: same slot loop, same latency, same error
// handling.
func (b *Benchmarker) ratedWorkerStamped(ctx context.Context, workerID int, slots <-chan time.Time, set *stampSet) {
	for {
		select {
		case <-ctx.Done():
			return
		case intended, ok := <-slots:
			if !ok {
				return
			}
			if !b.running.Load() {
				return
			}
			rec := set.begin(workerID, intended, len(slots))
			bytesRead, err := b.raw.DoRequest(ctx, workerID)
			latency := time.Since(intended)

			if err != nil {
				set.fail(workerID)
				if !b.running.Load() && ctx.Err() != nil {
					return
				}
				if errors.Is(err, ErrNeverConnected) {
					b.recordFatal(err)
					return
				}
				b.errors.Add(1)
				if b.mix != nil {
					b.mix.recordError(workerID)
				}
				continue
			}
			b.latencies.Load().RecordSuccess(workerID, latency, bytesRead)
			set.finish(workerID, rec, latency)
		}
	}
}
