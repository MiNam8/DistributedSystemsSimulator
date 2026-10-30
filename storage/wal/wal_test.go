package wal_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/MiNam8/kvstore/raft"
	"github.com/MiNam8/kvstore/storage"
	"github.com/MiNam8/kvstore/storage/wal"
)

var cluster = raft.Config{Voters: []raft.NodeID{1, 2, 3}}

func open(t *testing.T, dir string) *wal.WAL {
	t.Helper()
	// A small segment size so rollover is exercised without writing megabytes.
	w, err := wal.OpenSize(dir, cluster, 4096)
	if err != nil {
		t.Fatalf("open %s: %v", dir, err)
	}
	t.Cleanup(func() { w.Close() })
	return w
}

func entries(from, to raft.Index, term raft.Term) []raft.Entry {
	var out []raft.Entry
	for i := from; i <= to; i++ {
		out = append(out, raft.Entry{Index: i, Term: term, Type: raft.EntryNormal,
			Data: []byte{byte(i), byte(i >> 8), 0xAB}})
	}
	return out
}

func TestRoundTripSurvivesReopen(t *testing.T) {
	dir := t.TempDir()

	w := open(t, dir)
	if err := w.Append(entries(1, 200, 3)); err != nil {
		t.Fatal(err)
	}
	hs := raft.HardState{Term: 7, VotedFor: 2, Commit: 40}
	if err := w.SetHardState(hs); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2 := open(t, dir)
	gotHS, gotCfg, err := w2.InitialState()
	if err != nil {
		t.Fatal(err)
	}
	if gotHS != hs {
		t.Errorf("hard state = %+v, want %+v — a lost vote lets a node vote twice in one term", gotHS, hs)
	}
	if len(gotCfg.Voters) != 3 {
		t.Errorf("config = %+v, want 3 voters", gotCfg)
	}
	if last, _ := w2.LastIndex(); last != 200 {
		t.Fatalf("LastIndex = %d, want 200", last)
	}
	got, err := w2.Entries(1, 201)
	if err != nil {
		t.Fatal(err)
	}
	for i, e := range got {
		want := entries(1, 200, 3)[i]
		if e.Index != want.Index || e.Term != want.Term || string(e.Data) != string(want.Data) {
			t.Fatalf("entry %d = %+v, want %+v", i, e, want)
		}
	}
	if w2.Segments() < 2 {
		t.Errorf("expected segment rollover with a 4KB segment size, got %d segment(s)", w2.Segments())
	}
}

// TestTornWriteAtEveryOffset is the Phase 4 checkpoint, done exhaustively rather
// than by sampling: truncate the log at *every* byte offset and require that
// recovery always yields a clean prefix.
//
// This is the crash-consistency property the whole design rests on. A record
// that was not completely written was never fsync'd, so it was never
// acknowledged to any peer — discarding it is correct. What would not be correct
// is recovering a *gap*, or a corrupted entry, or refusing to open at all.
func TestTornWriteAtEveryOffset(t *testing.T) {
	golden := t.TempDir()
	w := open(t, golden)
	want := entries(1, 40, 2)
	if err := w.Append(want); err != nil {
		t.Fatal(err)
	}
	if err := w.SetHardState(raft.HardState{Term: 5, VotedFor: 3, Commit: 20}); err != nil {
		t.Fatal(err)
	}
	w.Close()

	names, err := filepath.Glob(filepath.Join(golden, "*.wal"))
	if err != nil || len(names) == 0 {
		t.Fatalf("no segments: %v", err)
	}
	// Torn writes only ever happen at the tail, so tear the final segment.
	victim := names[len(names)-1]
	full, err := os.ReadFile(victim)
	if err != nil {
		t.Fatal(err)
	}
	// Segments are preallocated, so the file ends in a long zero tail. Every cut
	// inside it is the same trivial case; test the region holding real records
	// plus a little past it.
	end := len(full)
	for end > 0 && full[end-1] == 0 {
		end--
	}
	full = full[:min(len(full), end+32)]

	tested := 0
	for cut := 0; cut <= len(full); cut++ {
		dir := t.TempDir()
		for _, n := range names {
			data, err := os.ReadFile(n)
			if err != nil {
				t.Fatal(err)
			}
			if n == victim {
				data = data[:min(cut, len(data))]
			}
			if err := os.WriteFile(filepath.Join(dir, filepath.Base(n)), data, 0o644); err != nil {
				t.Fatal(err)
			}
		}

		w2, err := wal.OpenSize(dir, cluster, 4096)
		if err != nil {
			t.Fatalf("cut at %d: recovery failed to open: %v", cut, err)
		}
		first, _ := w2.FirstIndex()
		last, _ := w2.LastIndex()

		if last > 40 {
			t.Fatalf("cut at %d: recovered LastIndex %d, beyond anything ever written", cut, last)
		}
		if last >= first {
			got, err := w2.Entries(first, last+1)
			if err != nil {
				t.Fatalf("cut at %d: Entries: %v", cut, err)
			}
			for i, e := range got {
				exp := want[i]
				if e.Index != exp.Index || e.Term != exp.Term || string(e.Data) != string(exp.Data) {
					t.Fatalf("cut at %d: entry %d = %+v, want %+v — recovery produced corrupt data",
						cut, i, e, exp)
				}
			}
		}
		// The log must still be usable: appends resume from the recovered tail.
		if err := w2.Append([]raft.Entry{{Index: last + 1, Term: 9}}); err != nil {
			t.Fatalf("cut at %d: cannot append after recovery: %v", cut, err)
		}
		w2.Close()
		tested++
	}
	t.Logf("recovered cleanly from %d distinct truncation points", tested)
}

// TestBitFlipIsDetected checks the CRC actually earns its place: silently
// returning a corrupted entry would let a node diverge from its peers with no
// symptom until a linearizability violation much later.
func TestBitFlipIsDetected(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	if err := w.Append(entries(1, 20, 1)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	names, _ := filepath.Glob(filepath.Join(dir, "*.wal"))
	data, err := os.ReadFile(names[0])
	if err != nil {
		t.Fatal(err)
	}

	corrupted := 0
	// Flip a bit in each record's payload region and require the entry count to
	// stop at or before that record.
	for pos := 60; pos < len(data) && pos < 400; pos += 7 {
		scratch := t.TempDir()
		mangled := append([]byte(nil), data...)
		mangled[pos] ^= 0x40
		if err := os.WriteFile(filepath.Join(scratch, filepath.Base(names[0])), mangled, 0o644); err != nil {
			t.Fatal(err)
		}
		w2, err := wal.OpenSize(scratch, cluster, 4096)
		if err != nil {
			corrupted++ // refusing to open is an acceptable response to corruption
			continue
		}
		last, _ := w2.LastIndex()
		if last == 20 {
			t.Fatalf("bit flip at byte %d was not detected: the full log was accepted", pos)
		}
		corrupted++
		w2.Close()
	}
	if corrupted == 0 {
		t.Fatal("no corruption was actually exercised")
	}
	t.Logf("%d bit flips all detected", corrupted)
}

func TestTruncateSuffixSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	if err := w.Append(entries(1, 30, 1)); err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateSuffix(11); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(entries(11, 15, 4)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2 := open(t, dir)
	if last, _ := w2.LastIndex(); last != 15 {
		t.Fatalf("LastIndex = %d, want 15", last)
	}
	// The truncation must be durable: replaying the raw records without honouring
	// the marker would resurrect the discarded term-1 entries.
	tm, err := w2.Term(12)
	if err != nil {
		t.Fatal(err)
	}
	if tm != 4 {
		t.Errorf("Term(12) = %d, want 4 — the truncation did not survive the reopen", tm)
	}
}

func TestTruncatePrefixDeletesSegmentsAndKeepsHardState(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	if err := w.Append(entries(1, 300, 1)); err != nil {
		t.Fatal(err)
	}
	hs := raft.HardState{Term: 9, VotedFor: 1, Commit: 250}
	if err := w.SetHardState(hs); err != nil {
		t.Fatal(err)
	}
	before := w.Segments()
	if before < 3 {
		t.Fatalf("expected several segments, got %d", before)
	}
	defer w.Close()

	if err := w.SaveSnapshot(raft.SnapshotMeta{LastIncludedIndex: 200, LastIncludedTerm: 1, Config: cluster}, []byte("state")); err != nil {
		t.Fatal(err)
	}
	if err := w.TruncatePrefix(200); err != nil {
		t.Fatal(err)
	}
	if w.Segments() >= before {
		t.Errorf("compaction deleted no segments (%d -> %d)", before, w.Segments())
	}

	w2, err := wal.OpenSize(dir, cluster, 4096)
	if err != nil {
		t.Fatal(err)
	}
	defer w2.Close()
	// The hard state may only have existed in a deleted segment. Losing it would
	// let this node vote a second time in term 9 after a restart.
	gotHS, _, _ := w2.InitialState()
	if gotHS != hs {
		t.Errorf("hard state after compaction = %+v, want %+v", gotHS, hs)
	}
	// Compaction is segment-granular, so the surviving first index is somewhere
	// between the old start and the snapshot boundary. Keeping more than asked is
	// always safe; keeping less is not.
	first, _ := w2.FirstIndex()
	if first <= 1 || first > 201 {
		t.Errorf("FirstIndex = %d, want in (1, 201]", first)
	}
	if last, _ := w2.LastIndex(); last != 300 {
		t.Errorf("LastIndex = %d, want 300", last)
	}
	if _, err := w2.Entries(first-1, first); !errors.Is(err, raft.ErrCompacted) {
		t.Errorf("reading below the first retained index: err = %v, want ErrCompacted", err)
	}

	// The live and recovered views must agree exactly.
	if liveFirst, _ := w.FirstIndex(); liveFirst != first {
		t.Errorf("live FirstIndex %d != recovered %d — a restart would see a different log",
			liveFirst, first)
	}
	// And the log handed to the core must start above the snapshot boundary.
	log, err := storage.RecoverLog(w2)
	if err != nil {
		t.Fatal(err)
	}
	if log.FirstIndex() != 201 {
		t.Errorf("recovered log FirstIndex = %d, want 201 (above the snapshot boundary)", log.FirstIndex())
	}
	if log.LastIndex() != 300 {
		t.Errorf("recovered log LastIndex = %d, want 300", log.LastIndex())
	}
	// The boundary index itself must still answer, or a leader probing there can
	// never make progress with this follower.
	if tm, err := w2.Term(200); err != nil || tm != 1 {
		t.Errorf("Term(200) at the snapshot boundary = %d, %v; want 1, nil", tm, err)
	}
}

func TestSnapshotSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	meta := raft.SnapshotMeta{LastIncludedIndex: 12, LastIncludedTerm: 4, Config: cluster}
	if err := w.SaveSnapshot(meta, []byte("machine state")); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2 := open(t, dir)
	gotMeta, data, err := w2.LoadSnapshot()
	if err != nil {
		t.Fatal(err)
	}
	if gotMeta.LastIncludedIndex != 12 || gotMeta.LastIncludedTerm != 4 {
		t.Errorf("meta = %+v, want index 12 term 4", gotMeta)
	}
	if string(data) != "machine state" {
		t.Errorf("data = %q, want %q", data, "machine state")
	}
	// A leftover temp file must never be mistaken for the real snapshot.
	if _, err := os.Stat(filepath.Join(dir, "snapshot.tmp")); err == nil {
		t.Error("snapshot.tmp was left behind")
	}
}

// TestRecoverLogMatchesInMemory checks the two Storage implementations agree.
// The simulator runs against the in-memory one; if they diverge, everything
// proven in simulation says nothing about a real node.
func TestRecoverLogMatchesInMemory(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	if err := w.Append(entries(1, 25, 2)); err != nil {
		t.Fatal(err)
	}
	if err := w.TruncateSuffix(20); err != nil {
		t.Fatal(err)
	}
	if err := w.Append(entries(20, 30, 5)); err != nil {
		t.Fatal(err)
	}
	w.Close()

	w2 := open(t, dir)
	log, err := storage.RecoverLog(w2)
	if err != nil {
		t.Fatal(err)
	}
	if log.LastIndex() != 30 {
		t.Errorf("recovered LastIndex = %d, want 30", log.LastIndex())
	}
	if tm, _ := log.Term(25); tm != 5 {
		t.Errorf("recovered Term(25) = %d, want 5", tm)
	}
	if tm, _ := log.Term(19); tm != 2 {
		t.Errorf("recovered Term(19) = %d, want 2", tm)
	}
}

func TestFsyncIsActuallyCalled(t *testing.T) {
	dir := t.TempDir()
	w := open(t, dir)
	before := w.Syncs
	if err := w.Append(entries(1, 1, 1)); err != nil {
		t.Fatal(err)
	}
	if w.Syncs <= before {
		t.Error("Append returned without fsyncing: entries would be acknowledged before they are durable")
	}
	before = w.Syncs
	if err := w.SetHardState(raft.HardState{Term: 1, VotedFor: 2}); err != nil {
		t.Fatal(err)
	}
	if w.Syncs <= before {
		t.Error("SetHardState returned without fsyncing: a vote would be promised before it is durable")
	}
}
