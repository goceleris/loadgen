//go:build !linux

package loadgen

import "context"

// recvQProbe is a no-op stub on non-Linux platforms.
type recvQProbe struct{}

func newRecvQProbe() *recvQProbe { return &recvQProbe{} }

// Start is a no-op on non-Linux platforms.
func (p *recvQProbe) Start(_ context.Context) {}

// Stop returns false on non-Linux platforms.
func (p *recvQProbe) Stop() bool { return false }
