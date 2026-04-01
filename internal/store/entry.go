package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
)

const (
	// walRecordMagic are the first four bytes of every WAL entry payload ("KVSW").
	// They tag records as ours and catch wrong offsets or garbage before we parse fields.
	walMagic0 = 'K'
	walMagic1 = 'V'
	walMagic2 = 'S'
	walMagic3 = 'W'

	// formatVersion is the on-disk layout revision after the magic (field order, types).
	// Bump when the binary schema changes; readers reject unknown versions.
	formatVersion uint16 = 1
)

// Op is the mutation kind stored in the WAL.
type Op byte

const (
	OpPut Op = 1 // insert/overwrite value
	OpDel Op = 2 // remove key
)

// Entry is one logical mutation in the WAL.
type Entry struct {
	Seq   uint64 // monotonic sequence; total order for replay
	Op    Op
	Table string // non-empty logical table name
	Key   string
	Fk    string
	// ValueBytes is CBOR-encoded db.Item for Put (legacy WAL may hold only Value); empty for Delete.
	ValueBytes []byte
}

var (
	// ErrCorruptRecord means CRC/magic/op/payload checks failed.
	ErrCorruptRecord = errors.New("store: corrupt wal record")
	// ErrTruncated means the byte slice ended before a full frame could be read.
	ErrTruncated = errors.New("store: truncated wal record")
)

// appendU32 appends big-endian uint32.
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
//   [payloadLen u32 BE][payload][crc32 IEEE of payload BE].
func (e *Entry) MarshalBinary() ([]byte, error) {
	if e.Table == "" {
		return nil, fmt.Errorf("store: empty table name")
	}
	var payload []byte
	payload = append(payload, walMagic0, walMagic1, walMagic2, walMagic3)
	payload = binary.BigEndian.AppendUint16(payload, formatVersion)
	payload = binary.BigEndian.AppendUint64(payload, e.Seq)
	payload = append(payload, byte(e.Op))
	payload = appendString(payload, e.Table)
	payload = appendString(payload, e.Key)
	payload = appendString(payload, e.Fk)
	if e.Op == OpPut {
		payload = appendU32(payload, uint32(len(e.ValueBytes)))
		payload = append(payload, e.ValueBytes...)
	} else if e.Op == OpDel {
		payload = appendU32(payload, 0)
	} else {
		return nil, fmt.Errorf("store: invalid op %d", e.Op)
	}
	crc := crc32.ChecksumIEEE(payload)
	out := appendU32(nil, uint32(len(payload)))
	out = append(out, payload...)
	out = appendU32(out, crc)
	return out, nil
}

// UnmarshalBinary decodes one full frame as produced by MarshalBinary
// (length prefix, payload, trailing CRC).
func (e *Entry) UnmarshalBinary(data []byte) error {
	// Frame externo: [tamanho_payload u32][payload][crc32 u32]
	// Tamanho mínimo = 4 (tamanho) + 0 (payload vazio teórico) + 4 (crc) = 8 bytes.
	const frameLenU32 = 4
	const frameCrcU32 = 4
	minFrame := frameLenU32 + frameCrcU32
	if len(data) < minFrame {
		return ErrTruncated
	}

	// Bytes [0:4]: comprimento do payload (não inclui estes 4 nem o CRC).
	payloadLen := binary.BigEndian.Uint32(data[0:4])
	// Byte total do frame = prefixo + payload + CRC (8 = 4 + 4).
	totalFrame := int(payloadLen) + frameLenU32 + frameCrcU32
	if totalFrame > len(data) {
		return ErrTruncated
	}

	payload := data[frameLenU32 : frameLenU32+payloadLen]
	// CRC ocupa sempre 4 bytes imediatamente após o payload.
	gotCRC := binary.BigEndian.Uint32(data[frameLenU32+payloadLen : frameLenU32+payloadLen+frameCrcU32])
	if crc32.ChecksumIEEE(payload) != gotCRC {
		return ErrCorruptRecord
	}

	// Cabeçalho fixo dentro do payload (antes das strings variáveis):
	// magic 4B + versão uint16 2B + Seq uint64 8B + Op uint8 1B = 15 bytes.
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
	if ver != formatVersion {
		return fmt.Errorf("store: unsupported format version %d", ver)
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

	// Bloco de valor: comprimento u32 + bytes (Put) ou comprimento 0 (Del).
	var vlen uint32
	vlen, off, err = readU32(payload, off)
	if err != nil {
		return err
	}
	// O cursor deve fechar exactamente no fim do payload (sem lixo a mais).
	if off+int(vlen) != len(payload) {
		return ErrCorruptRecord
	}
	e.ValueBytes = append([]byte(nil), payload[off:off+int(vlen)]...)

	switch e.Op {
	case OpPut:
	case OpDel:
		if len(e.ValueBytes) != 0 {
			return ErrCorruptRecord
		}
	default:
		return fmt.Errorf("store: invalid op %d", e.Op)
	}
	return nil
}
