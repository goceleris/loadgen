package loadgen

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync/atomic"
	"time"

	hdrhistogram "github.com/HdrHistogram/hdrhistogram-go"
)

// Federation roles: the coordinator dials the sidecar, sends a "start now"
// signal, then collects the sidecar's serialised result for client-side
// merging. The sidecar is a passive listener that runs an identical
// workload on its own host once it receives the signal.
//
// Wire protocol (deliberately minimal, no gRPC):
//
//	frame := uint32(big-endian length) || JSON payload
//	payloads:
//	  start:  {"op":"start","start_at_unix_nano":..., "duration_sec":..., "url":..., ...}
//	  result: {"op":"result","requests":...,"errors":...,"histogram":<base64>}
//	  ack:    {"op":"ack"}
//	  err:    {"op":"err","msg":...}
//
// One TCP conn carries the whole exchange. No retry. Best-effort: if any
// step fails, the primary records federation.merge_succeeded=false and
// proceeds with its local-only histogram.

const federationMagic = "loadgen-fed-v1\n"
const federationMaxFrame = 16 << 20 // 16MB cap; histograms are ~few KB

// fedFrame describes a single JSON message exchanged between primary
// and sidecar.
type fedFrame struct {
	Op            string  `json:"op"`
	StartAtUnixNS int64   `json:"start_at_unix_nano,omitempty"`
	DurationSec   float64 `json:"duration_sec,omitempty"`
	URL           string  `json:"url,omitempty"`
	Rate          float64 `json:"rate,omitempty"`
	Connections   int     `json:"connections,omitempty"`
	Workers       int     `json:"workers,omitempty"`
	HTTP2         bool    `json:"http2,omitempty"`

	// Result-side fields
	Requests  int64  `json:"requests,omitempty"`
	Errors    int64  `json:"errors,omitempty"`
	Histogram []byte `json:"histogram,omitempty"`

	// Generic error.
	Msg string `json:"msg,omitempty"`
}

// writeFrame writes a length-prefixed JSON frame to w. Returns the first I/O
// error encountered.
func writeFrame(w io.Writer, f fedFrame) error {
	buf, err := json.Marshal(f)
	if err != nil {
		return fmt.Errorf("federation: marshal: %w", err)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(buf)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(buf)
	return err
}

// readFrame reads a single length-prefixed JSON frame.
func readFrame(r io.Reader) (fedFrame, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return fedFrame{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n == 0 {
		return fedFrame{}, errors.New("federation: empty frame")
	}
	if n > federationMaxFrame {
		return fedFrame{}, fmt.Errorf("federation: frame too large (%d)", n)
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return fedFrame{}, err
	}
	var f fedFrame
	if err := json.Unmarshal(buf, &f); err != nil {
		return fedFrame{}, fmt.Errorf("federation: unmarshal: %w", err)
	}
	return f, nil
}

// FederationCoordinator dials a sidecar peer, kicks off a synchronised run,
// and collects the sidecar's histogram for client-side merging into the
// primary's Result.
//
// The coordinator does NOT mutate the run schedule on the local side; it
// just provides a hand-off point. The caller is responsible for actually
// starting the local benchmark at start_at_unix_nano.
type FederationCoordinator struct {
	peer    string
	timeout time.Duration

	mu     atomic.Pointer[net.TCPConn]
	closed atomic.Bool
}

// NewFederationCoordinator constructs a coordinator. Use Dial+Start before
// the local Run() begins; CollectResult after the local run returns.
func NewFederationCoordinator(peer string, timeout time.Duration) *FederationCoordinator {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &FederationCoordinator{peer: peer, timeout: timeout}
}

// Dial opens the TCP connection to the sidecar. Returns the wire-format
// magic-handshake error if the sidecar is not a loadgen instance.
func (c *FederationCoordinator) Dial(ctx context.Context) error {
	d := net.Dialer{Timeout: c.timeout}
	conn, err := d.DialContext(ctx, "tcp", c.peer)
	if err != nil {
		return fmt.Errorf("federation: dial %s: %w", c.peer, err)
	}
	tc, ok := conn.(*net.TCPConn)
	if !ok {
		_ = conn.Close()
		return errors.New("federation: dial returned non-TCP conn")
	}
	if _, err := tc.Write([]byte(federationMagic)); err != nil {
		_ = tc.Close()
		return fmt.Errorf("federation: handshake: %w", err)
	}
	c.mu.Store(tc)
	return nil
}

// Start sends the "start" frame to the sidecar so it can launch its local
// run synchronised with ours. startAt is when both sides should begin.
func (c *FederationCoordinator) Start(startAt time.Time, cfg Config) error {
	tc := c.mu.Load()
	if tc == nil {
		return errors.New("federation: not dialled")
	}
	_ = tc.SetWriteDeadline(time.Now().Add(c.timeout))
	frame := fedFrame{
		Op:            "start",
		StartAtUnixNS: startAt.UnixNano(),
		DurationSec:   cfg.Duration.Seconds(),
		URL:           cfg.URL,
		Rate:          cfg.Rate,
		Connections:   cfg.Connections,
		Workers:       cfg.Workers,
		HTTP2:         cfg.HTTP2,
	}
	if err := writeFrame(tc, frame); err != nil {
		return fmt.Errorf("federation: write start: %w", err)
	}
	_ = tc.SetWriteDeadline(time.Time{})
	// Read the ack.
	_ = tc.SetReadDeadline(time.Now().Add(c.timeout))
	ack, err := readFrame(tc)
	_ = tc.SetReadDeadline(time.Time{})
	if err != nil {
		return fmt.Errorf("federation: read ack: %w", err)
	}
	if ack.Op == "err" {
		return fmt.Errorf("federation: sidecar refused: %s", ack.Msg)
	}
	if ack.Op != "ack" {
		return fmt.Errorf("federation: unexpected op %q", ack.Op)
	}
	return nil
}

// CollectResult waits for the sidecar's "result" frame and returns the
// merged HdrHistogram alongside the sidecar's request/error counts. The
// merged histogram combines `local` with the sidecar's payload. Errors
// during merge are returned without stopping — caller can still emit a
// useful Result with merge_succeeded=false.
func (c *FederationCoordinator) CollectResult(local *hdrhistogram.Histogram) (peerReqs, peerErrs int64, merged *hdrhistogram.Histogram, err error) {
	tc := c.mu.Load()
	if tc == nil {
		return 0, 0, local, errors.New("federation: not dialled")
	}
	_ = tc.SetReadDeadline(time.Now().Add(c.timeout * 4))
	frame, err := readFrame(tc)
	_ = tc.SetReadDeadline(time.Time{})
	if err != nil {
		return 0, 0, local, fmt.Errorf("federation: read result: %w", err)
	}
	if frame.Op == "err" {
		return 0, 0, local, fmt.Errorf("federation: sidecar error: %s", frame.Msg)
	}
	if frame.Op != "result" {
		return 0, 0, local, fmt.Errorf("federation: unexpected op %q", frame.Op)
	}

	merged = local
	if len(frame.Histogram) > 0 {
		peerHist, decErr := hdrhistogram.Decode(frame.Histogram)
		if decErr != nil {
			return frame.Requests, frame.Errors, local, fmt.Errorf("federation: decode peer histogram: %w", decErr)
		}
		if local == nil {
			merged = peerHist
		} else {
			merged.Merge(peerHist)
		}
	}
	return frame.Requests, frame.Errors, merged, nil
}

// Close releases the underlying TCP connection. Idempotent.
func (c *FederationCoordinator) Close() error {
	if c.closed.Swap(true) {
		return nil
	}
	tc := c.mu.Load()
	if tc != nil {
		return tc.Close()
	}
	return nil
}

// FederationSidecar listens for an incoming coordinator on `addr`, runs the
// requested benchmark, and ships its histogram back. One-shot: serves
// exactly one peer and exits.
type FederationSidecar struct {
	addr string
	ln   net.Listener
}

// NewFederationSidecar binds to addr (e.g. ":9099"). Caller invokes Serve
// to handle a single coordinator connection.
func NewFederationSidecar(addr string) (*FederationSidecar, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("federation: listen %s: %w", addr, err)
	}
	return &FederationSidecar{addr: addr, ln: ln}, nil
}

// Addr returns the bound address (useful when addr was ":0").
func (s *FederationSidecar) Addr() string { return s.ln.Addr().String() }

// Close shuts down the listener.
func (s *FederationSidecar) Close() error { return s.ln.Close() }

// Serve accepts one connection, reads the "start" frame, and invokes
// runFn(cfg) which is expected to perform the local benchmark. It then
// ships the resulting histogram back to the coordinator. cfg passed to
// runFn carries fields from the start frame (URL, Duration, Rate, etc.)
// merged onto base.
func (s *FederationSidecar) Serve(ctx context.Context, base Config, runFn func(context.Context, Config) (*Result, error)) error {
	conn, err := s.ln.Accept()
	if err != nil {
		return fmt.Errorf("federation: accept: %w", err)
	}
	defer func() { _ = conn.Close() }()

	// Magic handshake.
	magic := make([]byte, len(federationMagic))
	_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
	if _, err := io.ReadFull(conn, magic); err != nil {
		return fmt.Errorf("federation: read magic: %w", err)
	}
	if string(magic) != federationMagic {
		_ = writeFrame(conn, fedFrame{Op: "err", Msg: "bad magic"})
		return errors.New("federation: bad magic")
	}
	_ = conn.SetReadDeadline(time.Time{})

	// Start frame.
	frame, err := readFrame(conn)
	if err != nil {
		return fmt.Errorf("federation: read start: %w", err)
	}
	if frame.Op != "start" {
		_ = writeFrame(conn, fedFrame{Op: "err", Msg: "expected start"})
		return fmt.Errorf("federation: expected start, got %q", frame.Op)
	}

	cfg := base
	if frame.URL != "" {
		cfg.URL = frame.URL
	}
	if frame.DurationSec > 0 {
		cfg.Duration = time.Duration(frame.DurationSec * float64(time.Second))
	}
	if frame.Rate > 0 {
		cfg.Rate = frame.Rate
	}
	if frame.Connections > 0 {
		cfg.Connections = frame.Connections
	}
	if frame.Workers > 0 {
		cfg.Workers = frame.Workers
	}
	cfg.HTTP2 = frame.HTTP2

	// Ack so the coordinator knows we accepted.
	if err := writeFrame(conn, fedFrame{Op: "ack"}); err != nil {
		return fmt.Errorf("federation: write ack: %w", err)
	}

	// Wait until the agreed start time, then run.
	if frame.StartAtUnixNS > 0 {
		wait := time.Until(time.Unix(0, frame.StartAtUnixNS))
		if wait > 0 {
			t := time.NewTimer(wait)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
	}

	res, err := runFn(ctx, cfg)
	if err != nil {
		_ = writeFrame(conn, fedFrame{Op: "err", Msg: err.Error()})
		return err
	}

	out := fedFrame{
		Op:        "result",
		Requests:  res.Requests,
		Errors:    res.Errors,
		Histogram: res.Histogram,
	}
	if err := writeFrame(conn, out); err != nil {
		return fmt.Errorf("federation: write result: %w", err)
	}
	return nil
}
