package kv

import (
	"encoding/binary"
	"errors"
)

// Commands and snapshots use a self-contained binary encoding rather than
// protobuf. The state machine is what every replica must agree on byte for byte,
// so it is deliberately free of any dependency that could change its encoding
// out from under a running cluster during an upgrade.

var errMalformed = errors.New("kv: malformed encoding")

// EncodeCommand serialises a command for the Raft log.
func EncodeCommand(c Command) []byte {
	buf := make([]byte, 0, 32+len(c.Key)+len(c.Value)+len(c.Expected))
	buf = append(buf, byte(c.Op))
	flags := byte(0)
	if c.HasExpected {
		flags |= 1
	}
	buf = append(buf, flags)
	buf = binary.LittleEndian.AppendUint64(buf, c.ClientID)
	buf = binary.LittleEndian.AppendUint64(buf, c.Seq)
	buf = appendBytes(buf, []byte(c.Key))
	buf = appendBytes(buf, c.Value)
	return appendBytes(buf, c.Expected)
}

// DecodeCommand parses a command from the Raft log.
func DecodeCommand(b []byte) (Command, error) {
	if len(b) < 18 {
		return Command{}, errMalformed
	}
	c := Command{
		Op:          Op(b[0]),
		HasExpected: b[1]&1 != 0,
		ClientID:    binary.LittleEndian.Uint64(b[2:10]),
		Seq:         binary.LittleEndian.Uint64(b[10:18]),
	}
	rest := b[18:]

	key, rest, err := takeBytes(rest)
	if err != nil {
		return Command{}, err
	}
	c.Key = string(key)
	if c.Value, rest, err = takeBytes(rest); err != nil {
		return Command{}, err
	}
	if c.Expected, rest, err = takeBytes(rest); err != nil {
		return Command{}, err
	}
	if len(rest) != 0 {
		return Command{}, errMalformed
	}
	return c, nil
}

// Snapshot serialises the entire state machine.
//
// The session table is included, and that is the point. Snapshot the keys alone
// and everything works until a compaction discards the log entries the sessions
// were rebuilt from — after which a client retrying across a restart has its
// command applied a second time. The failure surfaces as a lost update long
// after the cause.
func (s *Store) Snapshot() []byte {
	buf := make([]byte, 0, 64+len(s.data)*32)
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s.data)))
	for k, v := range s.data {
		buf = appendBytes(buf, []byte(k))
		buf = appendBytes(buf, v)
	}
	buf = binary.LittleEndian.AppendUint32(buf, uint32(len(s.sessions)))
	for id, sess := range s.sessions {
		buf = binary.LittleEndian.AppendUint64(buf, id)
		buf = binary.LittleEndian.AppendUint64(buf, sess.Seq)
		buf = append(buf, boolByte(sess.Result.OK), boolByte(sess.Result.Found))
		buf = appendBytes(buf, sess.Result.Value)
	}
	return buf
}

// Restore replaces the store's contents with a snapshot.
//
// Map iteration order makes the encoding non-canonical, so two replicas can
// produce different *bytes* for the same state. That is fine: snapshots are
// transferred, never compared. Restoring is what must agree, and it does.
func (s *Store) Restore(b []byte) error {
	data := make(map[string][]byte)
	sessions := make(map[uint64]session)

	if len(b) < 4 {
		return errMalformed
	}
	n := int(binary.LittleEndian.Uint32(b[:4]))
	rest := b[4:]
	for range n {
		var k, v []byte
		var err error
		if k, rest, err = takeBytes(rest); err != nil {
			return err
		}
		if v, rest, err = takeBytes(rest); err != nil {
			return err
		}
		data[string(k)] = v
	}

	if len(rest) < 4 {
		return errMalformed
	}
	m := int(binary.LittleEndian.Uint32(rest[:4]))
	rest = rest[4:]
	for range m {
		if len(rest) < 18 {
			return errMalformed
		}
		id := binary.LittleEndian.Uint64(rest[0:8])
		sess := session{Seq: binary.LittleEndian.Uint64(rest[8:16])}
		sess.Result.OK = rest[16] != 0
		sess.Result.Found = rest[17] != 0
		rest = rest[18:]
		var val []byte
		var err error
		if val, rest, err = takeBytes(rest); err != nil {
			return err
		}
		sess.Result.Value = val
		sessions[id] = sess
	}
	if len(rest) != 0 {
		return errMalformed
	}

	s.data, s.sessions = data, sessions
	return nil
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.LittleEndian.AppendUint32(dst, uint32(len(b)))
	return append(dst, b...)
}

func takeBytes(b []byte) (val, rest []byte, err error) {
	if len(b) < 4 {
		return nil, nil, errMalformed
	}
	n := int(binary.LittleEndian.Uint32(b[:4]))
	if n < 0 || 4+n > len(b) {
		return nil, nil, errMalformed
	}
	if n == 0 {
		return nil, b[4:], nil
	}
	return append([]byte(nil), b[4:4+n]...), b[4+n:], nil
}

func boolByte(b bool) byte {
	if b {
		return 1
	}
	return 0
}
