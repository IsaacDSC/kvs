package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// walRecordMagic are the first four bytes of every WAL entry payload ("KVSW").
	walMagic0 = 'K'
	walMagic1 = 'V'
	walMagic2 = 'S'
	walMagic3 = 'W'

	// walFormatVersionV1 is the only supported WAL payload layout (after magic): Put, Del, Begin, Commit.
	walFormatVersionV1 uint16 = 1
)

// Op is the mutation kind stored in the WAL.
type Op byte

const (
	OpSet     Op = 1 // insert/overwrite value
	OpDel     Op = 2 // remove key
	OpBegin   Op = 3 // start transaction (ValueBytes = 8-byte txid BE)
	OpCommit  Op = 4 // commit transaction (ValueBytes = 8-byte txid BE)
	OpRaftLog Op = 5 // committed Raft log entry (ValueBytes = CBOR raftLogPayload)
	OpRaftMeta Op = 6 // Raft stable state: currentTerm + votedFor (ValueBytes = CBOR raftMetaPayload)
)

// Entry is one logical record in the WAL.
type Entry struct {
	Seq   uint64 // monotonic sequence; total order for replay
	Op    Op
	Table string // non-empty logical table name
	Key   string
	Fk    string
	// ValueBytes: CBOR memdb.Item for Put; empty for Del; 8-byte txid BE for Begin/Commit.
	ValueBytes []byte
}

var (
	ErrCorruptRecord = errors.New("wal: corrupt record")
	ErrTruncated     = errors.New("wal: truncated record")
)

func appendU32(dst []byte, v uint32) []byte {
	return binary.BigEndian.AppendUint32(dst, v)
}

func readU32(data []byte, off int) (uint32, int, error) {
	if off+4 > len(data) {
		return 0, off, ErrTruncated
	}
	return binary.BigEndian.Uint32(data[off:]), off + 4, nil
}

func appendString(dst []byte, s string) []byte {
	b := []byte(s)
	dst = appendU32(dst, uint32(len(b)))
	dst = append(dst, b...)
	return dst
}

func readString(data []byte, off int) (string, int, error) {
	n, noff, err := readU32(data, off)
	if err != nil {
		return "", off, err
	}
	if int(n) > len(data)-noff {
		return "", off, ErrTruncated
	}
	s := string(data[noff : noff+int(n)])
	return s, noff + int(n), nil
}

// MarshalBinary encodes one framed WAL record:
//
//	[payloadLen u32 BE][payload][crc32 IEEE of payload BE].
func (e *Entry) MarshalBinary() ([]byte, error) {
	if e.Table == "" {
		return nil, fmt.Errorf("wal: empty table name")
	}
	var payload []byte
	payload = append(payload, walMagic0, walMagic1, walMagic2, walMagic3)
	payload = binary.BigEndian.AppendUint16(payload, walFormatVersionV1)
	payload = binary.BigEndian.AppendUint64(payload, e.Seq)
	payload = append(payload, byte(e.Op))
	payload = appendString(payload, e.Table)
	payload = appendString(payload, e.Key)
	payload = appendString(payload, e.Fk)
	switch e.Op {
	case OpSet, OpRaftLog, OpRaftMeta:
		payload = appendU32(payload, uint32(len(e.ValueBytes)))
		payload = append(payload, e.ValueBytes...)
	case OpDel:
		payload = appendU32(payload, 0)
	case OpBegin, OpCommit:
		if len(e.ValueBytes) != 8 {
			return nil, fmt.Errorf("wal: begin/commit require 8-byte txid")
		}
		payload = appendU32(payload, 8)
		payload = append(payload, e.ValueBytes...)
	default:
		return nil, fmt.Errorf("wal: invalid op %d", e.Op)
	}
	crc := crc32.ChecksumIEEE(payload)
	out := appendU32(nil, uint32(len(payload)))
	out = append(out, payload...)
	out = appendU32(out, crc)
	return out, nil
}

// UnmarshalBinary decodes one full frame as produced by MarshalBinary.
func (e *Entry) UnmarshalBinary(data []byte) error {
	const frameLenU32 = 4
	const frameCrcU32 = 4
	minFrame := frameLenU32 + frameCrcU32
	if len(data) < minFrame {
		return ErrTruncated
	}

	payloadLen := binary.BigEndian.Uint32(data[0:4])
	totalFrame := int(payloadLen) + frameLenU32 + frameCrcU32
	if totalFrame > len(data) {
		return ErrTruncated
	}

	payload := data[frameLenU32 : frameLenU32+payloadLen]
	gotCRC := binary.BigEndian.Uint32(data[frameLenU32+payloadLen : frameLenU32+payloadLen+frameCrcU32])
	if crc32.ChecksumIEEE(payload) != gotCRC {
		return ErrCorruptRecord
	}

	const (
		magicBytes     = 4
		versionBytes   = 2
		seqBytes       = 8
		opBytes        = 1
		fixedHeaderLen = magicBytes + versionBytes + seqBytes + opBytes
	)
	if len(payload) < fixedHeaderLen {
		return ErrCorruptRecord
	}

	off := 0
	if payload[off] != walMagic0 || payload[off+1] != walMagic1 || payload[off+2] != walMagic2 || payload[off+3] != walMagic3 {
		return ErrCorruptRecord
	}
	off += magicBytes

	ver := binary.BigEndian.Uint16(payload[off:])
	off += versionBytes
	if ver != walFormatVersionV1 {
		return fmt.Errorf("wal: unsupported format version %d", ver)
	}

	e.Seq = binary.BigEndian.Uint64(payload[off:])
	off += seqBytes

	e.Op = Op(payload[off])
	off += opBytes

	var err error
	e.Table, off, err = readString(payload, off)
	if err != nil {
		return err
	}
	e.Key, off, err = readString(payload, off)
	if err != nil {
		return err
	}
	e.Fk, off, err = readString(payload, off)
	if err != nil {
		return err
	}

	var vlen uint32
	vlen, off, err = readU32(payload, off)
	if err != nil {
		return err
	}
	if off+int(vlen) != len(payload) {
		return ErrCorruptRecord
	}
	e.ValueBytes = append([]byte(nil), payload[off:off+int(vlen)]...)

	switch e.Op {
	case OpSet, OpRaftLog, OpRaftMeta:
		// variable-length ValueBytes; any length is valid
	case OpDel:
		if len(e.ValueBytes) != 0 {
			return ErrCorruptRecord
		}
	case OpBegin, OpCommit:
		if len(e.ValueBytes) != 8 {
			return ErrCorruptRecord
		}
	default:
		return fmt.Errorf("wal: invalid op %d", e.Op)
	}
	return nil
}

// TxIDFromValueBytes returns the txid stored in Begin/Commit ValueBytes.
func TxIDFromValueBytes(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, ErrCorruptRecord
	}
	return binary.BigEndian.Uint64(b), nil
}

// EncodeTxID encodes a txid for Entry.ValueBytes on Begin/Commit.
func EncodeTxID(id uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, id)
	return b
}
