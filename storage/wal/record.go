package wal

import (
	"encoding/binary"
	"errors"
	"hash/crc32"

	"github.com/MiNam8/kvstore/raft"
)

// Record framing.
//
//	crc32c(type || payload) : uint32
//	type                    : uint8
//	length of payload       : uint32
//	payload                 : length bytes
//
// The CRC covers the type byte as well as the payload, so a record whose type
// was corrupted into a different valid type is still caught. Length is *not*
// covered, deliberately: a corrupted length is caught by the CRC of the payload
// it causes to be read, and including it would not let recovery distinguish a
// torn tail from real corruption any better.
const recordHeaderSize = 9

type recordType uint8

const (
	recEntry recordType = iota + 1
	recHardState
	recConfig
)

// castagnoli, not IEEE: it is the CRC-32 variant with hardware acceleration on
// every CPU this will run on, and the WAL computes one per record on the write
// path where fsync latency already dominates but CPU should not add to it.
var crcTable = crc32.MakeTable(crc32.Castagnoli)

var (
	// errTornRecord means the record is incomplete or its checksum does not
	// match. During replay this is not corruption to report but the end of the
	// durable log: anything after an incomplete record was never fsync'd, so it
	// was never acknowledged to anyone.
	errTornRecord = errors.New("wal: torn or corrupt record")
	errBadPayload = errors.New("wal: malformed record payload")
)

func appendRecord(dst []byte, t recordType, payload []byte) []byte {
	var hdr [recordHeaderSize]byte

	crc := crc32.New(crcTable)
	_, _ = crc.Write([]byte{byte(t)})
	_, _ = crc.Write(payload)

	binary.LittleEndian.PutUint32(hdr[0:4], crc.Sum32())
	hdr[4] = byte(t)
	binary.LittleEndian.PutUint32(hdr[5:9], uint32(len(payload)))

	dst = append(dst, hdr[:]...)
	return append(dst, payload...)
}

// readRecord decodes one record from buf, returning it and the bytes consumed.
// It returns errTornRecord if buf does not hold a complete, checksum-valid record.
func readRecord(buf []byte) (recordType, []byte, int, error) {
	if len(buf) < recordHeaderSize {
		return 0, nil, 0, errTornRecord
	}
	want := binary.LittleEndian.Uint32(buf[0:4])
	t := recordType(buf[4])
	n := int(binary.LittleEndian.Uint32(buf[5:9]))

	// A corrupted length can be enormous; refuse it before allocating anything.
	if n < 0 || recordHeaderSize+n > len(buf) {
		return 0, nil, 0, errTornRecord
	}
	payload := buf[recordHeaderSize : recordHeaderSize+n]

	crc := crc32.New(crcTable)
	_, _ = crc.Write([]byte{byte(t)})
	_, _ = crc.Write(payload)
	if crc.Sum32() != want {
		return 0, nil, 0, errTornRecord
	}
	return t, payload, recordHeaderSize + n, nil
}

func encodeEntry(e raft.Entry) []byte {
	buf := make([]byte, 0, 21+len(e.Data))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Index))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(e.Term))
	buf = append(buf, byte(e.Type))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(e.Data)))
	return append(buf, e.Data...)
}

func decodeEntry(p []byte) (raft.Entry, error) {
	if len(p) < 21 {
		return raft.Entry{}, errBadPayload
	}
	e := raft.Entry{
		Index: raft.Index(binary.LittleEndian.Uint64(p[0:8])),
		Term:  raft.Term(binary.LittleEndian.Uint64(p[8:16])),
		Type:  raft.EntryType(p[16]),
	}
	n := int(binary.LittleEndian.Uint32(p[17:21]))
	if 21+n != len(p) {
		return raft.Entry{}, errBadPayload
	}
	if n > 0 {
		e.Data = make([]byte, n)
		copy(e.Data, p[21:])
	}
	return e, nil
}

func encodeHardState(hs raft.HardState) []byte {
	buf := make([]byte, 0, 24)
	buf = binary.LittleEndian.AppendUint64(buf, uint64(hs.Term))
	buf = binary.LittleEndian.AppendUint64(buf, uint64(hs.VotedFor))
	return binary.LittleEndian.AppendUint64(buf, uint64(hs.Commit))
}

func decodeHardState(p []byte) (raft.HardState, error) {
	if len(p) != 24 {
		return raft.HardState{}, errBadPayload
	}
	return raft.HardState{
		Term:     raft.Term(binary.LittleEndian.Uint64(p[0:8])),
		VotedFor: raft.NodeID(binary.LittleEndian.Uint64(p[8:16])),
		Commit:   raft.Index(binary.LittleEndian.Uint64(p[16:24])),
	}, nil
}

func encodeConfig(c raft.Config) []byte {
	buf := make([]byte, 0, 8+8*len(c.Voters)+8*len(c.Learners))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(c.Voters)))
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(c.Learners)))
	for _, v := range c.Voters {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(v))
	}
	for _, l := range c.Learners {
		buf = binary.LittleEndian.AppendUint64(buf, uint64(l))
	}
	return buf
}

func decodeConfig(p []byte) (raft.Config, error) {
	if len(p) < 8 {
		return raft.Config{}, errBadPayload
	}
	nv := int(binary.LittleEndian.Uint32(p[0:4]))
	nl := int(binary.LittleEndian.Uint32(p[4:8]))
	if len(p) != 8+8*(nv+nl) {
		return raft.Config{}, errBadPayload
	}
	c := raft.Config{}
	off := 8
	for range nv {
		c.Voters = append(c.Voters, raft.NodeID(binary.LittleEndian.Uint64(p[off:off+8])))
		off += 8
	}
	for range nl {
		c.Learners = append(c.Learners, raft.NodeID(binary.LittleEndian.Uint64(p[off:off+8])))
		off += 8
	}
	return c, nil
}
