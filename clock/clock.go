// Package clock supplies monotonic time to the node runtime.
//
// The interface lives outside the raft package because the core may not read a
// clock: it is told the time through raft.Tick and cannot tell whether that
// time is virtual or real. That is precisely what makes the simulator able to
// run an hour of cluster behaviour in a millisecond.
package clock

import (
	"time"

	"github.com/MiNam8/kvstore/raft"
)

// Clock reports elapsed monotonic time.
//
// Wall-clock time is never used, for any purpose. Raft's safety does not depend
// on time at all, and its liveness depends only on *elapsed* time; a wall clock
// can jump backwards on an NTP correction, which would corrupt election timeouts
// and — far worse — silently extend a leader lease past its true expiry.
type Clock interface {
	// Now returns nanoseconds elapsed since an arbitrary fixed origin.
	Now() raft.Time
}

// Real is a Clock backed by the host's monotonic clock.
//
// Go's time.Time carries a monotonic reading, and time.Since uses it, so this is
// immune to wall-clock adjustment.
type Real struct {
	origin time.Time
}

// NewReal returns a Clock whose origin is the moment of construction.
func NewReal() *Real { return &Real{origin: time.Now()} }

// Now implements [Clock].
func (r *Real) Now() raft.Time { return raft.Time(time.Since(r.origin)) }
