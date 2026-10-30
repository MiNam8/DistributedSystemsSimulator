package sim

import "github.com/MiNam8/kvstore/raft"

// Faults configures the simulated network. Every parameter is drawn from the
// run's seeded generator, so the faults a seed produces are part of what that
// seed reproduces.
//
// Rates are per mille (out of 1000) rather than floats. Floating point would
// work, but integer arithmetic guarantees that a seed replays identically
// regardless of platform or compiler, and reproducibility is the whole point.
type Faults struct {
	// DropPerMille is the chance a message is discarded outright.
	DropPerMille int
	// DuplicatePerMille is the chance a message is delivered twice, at
	// independently drawn delays. Duplication is what catches the blind
	// truncation bug: a stale AppendEntries arriving after the log moved on.
	DuplicatePerMille int
	// JitterPerMille varies each message's latency by up to this fraction,
	// which is what produces reordering — messages sent in one order arriving
	// in another.
	JitterPerMille int
	// SlowPerMille is the chance a message is delayed by SlowFactor times the
	// base latency. A heavy tail matters: uniform jitter never produces the
	// pathologically late message that arrives after an election has already
	// resolved, and that message is exactly the adversary Raft must survive.
	SlowPerMille int
	SlowFactor   int
}

// NoFaults is a perfect network.
func NoFaults() Faults { return Faults{} }

// DefaultFaults is a hostile but not absurd network: 5% loss, 2% duplication,
// ±50% jitter, and 3% of messages arriving 20x late.
func DefaultFaults() Faults {
	return Faults{
		DropPerMille:      50,
		DuplicatePerMille: 20,
		JitterPerMille:    500,
		SlowPerMille:      30,
		SlowFactor:        20,
	}
}

// link is a directed connection. Directed, not undirected, so that one-way
// partitions can be expressed: a node that can send but not receive is a real
// failure mode, and one that symmetric-only testing never produces.
type link struct{ from, to raft.NodeID }

// chaosAction is one adversarial move.
type chaosAction uint8

const (
	chaosPartition chaosAction = iota
	chaosOneWayPartition
	chaosHeal
	chaosCrash
	chaosRestart
	chaosStall
	chaosConfChange
	chaosActionCount
)

func (a chaosAction) String() string {
	switch a {
	case chaosPartition:
		return "partition"
	case chaosOneWayPartition:
		return "one-way-partition"
	case chaosHeal:
		return "heal"
	case chaosCrash:
		return "crash"
	case chaosRestart:
		return "restart"
	case chaosStall:
		return "stall"
	case chaosConfChange:
		return "conf-change"
	default:
		return "unknown"
	}
}
