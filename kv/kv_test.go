package kv_test

import (
	"testing"

	"github.com/MiNam8/kvstore/kv"
)

func put(c uint64, seq uint64, k, v string) kv.Command {
	return kv.Command{Op: kv.OpPut, Key: k, Value: []byte(v), ClientID: c, Seq: seq}
}

func TestBasicOperations(t *testing.T) {
	s := kv.New()
	s.ApplyCommand(put(1, 1, "a", "1"))

	if v, ok := s.Get("a"); !ok || string(v) != "1" {
		t.Errorf(`Get("a") = %q, %v; want "1", true`, v, ok)
	}
	if _, ok := s.Get("missing"); ok {
		t.Error("Get on an absent key reported found")
	}

	r := s.ApplyCommand(kv.Command{Op: kv.OpDelete, Key: "a", ClientID: 1, Seq: 2})
	if !r.Found {
		t.Error("Delete of an existing key reported not found")
	}
	if _, ok := s.Get("a"); ok {
		t.Error("key survived deletion")
	}
}

func TestCASSemantics(t *testing.T) {
	s := kv.New()

	// Expect-absent on an absent key succeeds.
	r := s.ApplyCommand(kv.Command{Op: kv.OpCAS, Key: "k", Value: []byte("v1"), ClientID: 1, Seq: 1})
	if !r.OK {
		t.Fatal("CAS expecting absence failed on an absent key")
	}
	// Expect-absent on a present key fails.
	r = s.ApplyCommand(kv.Command{Op: kv.OpCAS, Key: "k", Value: []byte("v2"), ClientID: 1, Seq: 2})
	if r.OK {
		t.Error("CAS expecting absence succeeded on a present key")
	}
	// Expect the current value succeeds.
	r = s.ApplyCommand(kv.Command{Op: kv.OpCAS, Key: "k", Value: []byte("v3"),
		Expected: []byte("v1"), HasExpected: true, ClientID: 1, Seq: 3})
	if !r.OK {
		t.Error("CAS with a matching expectation failed")
	}
	if v, _ := s.Get("k"); string(v) != "v3" {
		t.Errorf(`value = %q, want "v3"`, v)
	}
}

// TestDuplicateCommandIsNotReapplied is the reason the session table exists.
//
// A client whose write times out because the leader died mid-commit cannot tell
// whether it committed, so it retries against the new leader. Without
// deduplication that retry is a second, distinct log entry and gets applied
// again. For a counter increment or a CAS that is a lost update, and
// linearizability is broken however correct the consensus layer is.
func TestDuplicateCommandIsNotReapplied(t *testing.T) {
	s := kv.New()

	// Client 1 claims "k" by compare-and-set from absent.
	first := s.ApplyCommand(kv.Command{Op: kv.OpCAS, Key: "k", Value: []byte("mine"), ClientID: 1, Seq: 1})
	if !first.OK {
		t.Fatal("setup: initial CAS failed")
	}

	// Someone else changes it.
	s.ApplyCommand(put(2, 1, "k", "theirs"))

	// Client 1's original request is retried after a failover. Re-executing it
	// would now fail the comparison and report failure for a command that
	// actually succeeded — the client would be told its write was lost.
	retry := s.ApplyCommand(kv.Command{Op: kv.OpCAS, Key: "k", Value: []byte("mine"), ClientID: 1, Seq: 1})
	if !retry.Duplicate {
		t.Error("retry was not recognised as a duplicate")
	}
	if retry.OK != first.OK {
		t.Errorf("retry returned OK=%v but the original returned OK=%v — the client gets a different answer to the same request",
			retry.OK, first.OK)
	}
	if v, _ := s.Get("k"); string(v) != "theirs" {
		t.Errorf(`retry re-executed and clobbered the key: %q, want "theirs"`, v)
	}
}

func TestOutOfOrderAndStaleSequencesAreDeduplicated(t *testing.T) {
	s := kv.New()
	s.ApplyCommand(put(1, 5, "k", "five"))
	// Anything at or below the last applied sequence is a retry.
	r := s.ApplyCommand(put(1, 3, "k", "three"))
	if !r.Duplicate {
		t.Error("a stale sequence number was not treated as a duplicate")
	}
	if v, _ := s.Get("k"); string(v) != "five" {
		t.Errorf(`stale command was applied: %q, want "five"`, v)
	}
}

func TestReadsAreNeverDeduplicated(t *testing.T) {
	s := kv.New()
	s.ApplyCommand(put(1, 1, "k", "v1"))
	if r := s.ApplyCommand(kv.Command{Op: kv.OpGet, Key: "k", ClientID: 1, Seq: 1}); string(r.Value) != "v1" {
		t.Fatalf("read = %q, want v1", r.Value)
	}
	s.ApplyCommand(put(2, 1, "k", "v2"))
	// The same client and sequence, read again: a read at a later log position
	// must see the later value, so reads must not be served from the session table.
	if r := s.ApplyCommand(kv.Command{Op: kv.OpGet, Key: "k", ClientID: 1, Seq: 1}); string(r.Value) != "v2" {
		t.Errorf("read = %q, want v2 — reads were deduplicated and returned a stale value", r.Value)
	}
}

// TestSnapshotPreservesSessions guards the bug that only appears after a
// compaction, a restart and a retry — which is to say, in production.
//
// If Snapshot omits the session table, this test fails on the final assertion:
// the restored store has forgotten the client's sequence number and re-executes
// a command it already applied.
func TestSnapshotPreservesSessions(t *testing.T) {
	s := kv.New()
	s.ApplyCommand(put(1, 1, "a", "1"))
	s.ApplyCommand(put(1, 2, "b", "2"))
	first := s.ApplyCommand(kv.Command{Op: kv.OpCAS, Key: "c", Value: []byte("claimed"), ClientID: 7, Seq: 9})
	if !first.OK {
		t.Fatal("setup: CAS failed")
	}

	snap := s.Snapshot()

	restored := kv.New()
	if err := restored.Restore(snap); err != nil {
		t.Fatal(err)
	}
	if restored.Len() != 3 {
		t.Errorf("restored %d keys, want 3", restored.Len())
	}
	if v, ok := restored.Get("b"); !ok || string(v) != "2" {
		t.Errorf(`restored Get("b") = %q, %v`, v, ok)
	}

	// Someone else takes the key after the snapshot.
	restored.ApplyCommand(put(8, 1, "c", "someone else"))

	// Client 7 retries its original command against the restored replica.
	retry := restored.ApplyCommand(kv.Command{Op: kv.OpCAS, Key: "c", Value: []byte("claimed"), ClientID: 7, Seq: 9})
	if !retry.Duplicate {
		t.Fatal("the session table was not preserved across the snapshot: a retried command was re-executed")
	}
	if v, _ := restored.Get("c"); string(v) != "someone else" {
		t.Errorf(`retry clobbered the key after restore: %q`, v)
	}
}

func TestCommandCodecRoundTrip(t *testing.T) {
	cases := []kv.Command{
		{Op: kv.OpPut, Key: "k", Value: []byte("v"), ClientID: 3, Seq: 4},
		{Op: kv.OpDelete, Key: "", ClientID: 0, Seq: 0},
		{Op: kv.OpCAS, Key: "x", Value: nil, Expected: []byte{}, HasExpected: true},
		{Op: kv.OpPut, Key: "binary", Value: []byte{0, 1, 2, 0xff}},
	}
	for _, want := range cases {
		got, err := kv.DecodeCommand(kv.EncodeCommand(want))
		if err != nil {
			t.Fatalf("decode %+v: %v", want, err)
		}
		if got.Op != want.Op || got.Key != want.Key || got.ClientID != want.ClientID ||
			got.Seq != want.Seq || got.HasExpected != want.HasExpected ||
			string(got.Value) != string(want.Value) || string(got.Expected) != string(want.Expected) {
			t.Errorf("round trip: got %+v, want %+v", got, want)
		}
	}
}

func TestMalformedInputIsIgnoredNotFatal(t *testing.T) {
	s := kv.New()
	// Every replica sees the same bytes, so skipping is identical everywhere.
	// Panicking on one node and not another would be far worse than ignoring it.
	for _, bad := range [][]byte{nil, {}, {1}, {1, 2, 3}, make([]byte, 17)} {
		if r := s.Apply(bad); r.OK {
			t.Errorf("malformed input %v reported success", bad)
		}
	}
	if s.Len() != 0 {
		t.Error("malformed input mutated the store")
	}
}

// TestApplyIsDeterministic checks the property every replica depends on:
// the same command sequence produces the same state.
func TestApplyIsDeterministic(t *testing.T) {
	cmds := []kv.Command{
		put(1, 1, "a", "1"), put(2, 1, "b", "2"),
		{Op: kv.OpDelete, Key: "a", ClientID: 1, Seq: 2},
		{Op: kv.OpCAS, Key: "b", Value: []byte("3"), Expected: []byte("2"), HasExpected: true, ClientID: 2, Seq: 2},
		put(1, 1, "a", "duplicate"),
	}
	a, b := kv.New(), kv.New()
	for _, c := range cmds {
		a.ApplyCommand(c)
		b.Apply(kv.EncodeCommand(c)) // via the wire encoding
	}
	ka, kb := a.Keys(), b.Keys()
	if len(ka) != len(kb) {
		t.Fatalf("replicas hold %d and %d keys", len(ka), len(kb))
	}
	for k, v := range ka {
		if string(kb[k]) != string(v) {
			t.Errorf("replicas diverge at %q: %q vs %q", k, v, kb[k])
		}
	}
}
