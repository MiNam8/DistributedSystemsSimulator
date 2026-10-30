package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"

	"github.com/MiNam8/kvstore/raft"
)

const snapshotFile = "snapshot"

// SaveSnapshot writes a snapshot atomically.
//
// Write to a temporary file, fsync it, rename over the old one, then fsync the
// directory. Rename is atomic within a directory, so a crash leaves either the
// old snapshot or the new one and never a mixture. That matters more than it
// does for the log: a half-written log record is discarded and the log is still
// a valid prefix, but a half-written snapshot is unrecoverable state.
func (w *WAL) SaveSnapshot(meta raft.SnapshotMeta, data []byte) error {
	buf := encodeSnapshotMeta(meta)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(data)))
	buf = append(buf, data...)
	framed := appendRecord(nil, recEntry, buf) // CRC-protected like every record

	tmp := filepath.Join(w.dir, snapshotFile+".tmp")
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(framed); err != nil {
		f.Close()
		return err
	}
	if !w.noSync {
		if err := f.Sync(); err != nil {
			f.Close()
			return err
		}
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(w.dir, snapshotFile)); err != nil {
		return err
	}
	if !w.noSync {
		if err := syncDir(w.dir); err != nil {
			return err
		}
		w.Syncs += 2
	}

	w.snapMeta, w.hasSnap = meta, true
	w.snapData = append([]byte(nil), data...)
	w.dropCovered()
	return nil
}

// dropCovered removes in-memory entries the snapshot now covers, so that
// firstIndex and lastIndex stay consistent with the boundary. The segment files
// may still hold them; replay filters them out the same way.
func (w *WAL) dropCovered() {
	for i, e := range w.entries {
		if e.Index > w.snapMeta.LastIncludedIndex {
			w.entries = w.entries[i:]
			return
		}
	}
	w.entries = nil
}

// LoadSnapshot returns the most recent durable snapshot.
func (w *WAL) LoadSnapshot() (raft.SnapshotMeta, []byte, error) {
	if !w.hasSnap {
		return raft.SnapshotMeta{}, nil, raft.ErrUnavailable
	}
	return w.snapMeta, w.snapData, nil
}

// loadSnapshot reads the snapshot file at Open time. A snapshot that fails its
// checksum is treated as absent: the log below it is still present, so the node
// recovers by replaying rather than by trusting a damaged image.
func (w *WAL) loadSnapshot() error {
	raw, err := os.ReadFile(filepath.Join(w.dir, snapshotFile))
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	_, payload, _, err := readRecord(raw)
	if err != nil {
		return nil // corrupt or torn: fall back to the log
	}
	meta, rest, err := decodeSnapshotMeta(payload)
	if err != nil || len(rest) < 4 {
		return nil
	}
	n := int(binary.LittleEndian.Uint32(rest[:4]))
	if 4+n != len(rest) {
		return nil
	}
	w.snapMeta, w.hasSnap = meta, true
	w.snapData = append([]byte(nil), rest[4:]...)
	return nil
}

func encodeSnapshotMeta(m raft.SnapshotMeta) []byte {
	buf := make([]byte, 0, 32)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(m.LastIncludedIndex))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(m.LastIncludedTerm))
	cfg := encodeConfig(m.Config)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(cfg)))
	return append(buf, cfg...)
}

func decodeSnapshotMeta(p []byte) (raft.SnapshotMeta, []byte, error) {
	if len(p) < 20 {
		return raft.SnapshotMeta{}, nil, errBadPayload
	}
	m := raft.SnapshotMeta{
		LastIncludedIndex: raft.Index(binary.LittleEndian.Uint64(p[0:8])),
		LastIncludedTerm:  raft.Term(binary.LittleEndian.Uint64(p[8:16])),
	}
	n := int(binary.LittleEndian.Uint32(p[16:20]))
	if 20+n > len(p) {
		return raft.SnapshotMeta{}, nil, errBadPayload
	}
	cfg, err := decodeConfig(p[20 : 20+n])
	if err != nil {
		return raft.SnapshotMeta{}, nil, err
	}
	m.Config = cfg
	return m, p[20+n:], nil
}
