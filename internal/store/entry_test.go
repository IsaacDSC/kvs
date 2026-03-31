package store

import (
	"bytes"
	"testing"
)

func TestEntryRoundTripPut(t *testing.T) {
	e := Entry{
		Seq:        7,
		Op:         OpPut,
		Table:      "users",
		Key:        "k1",
		Fk:         "f1",
		ValueBytes: []byte{0xa1, 0x64, 0x6e, 0x61, 0x6d, 0x65, 0x64, 0x41, 0x6e, 0x6e}, // CBOR-ish blob
	}
	b, err := e.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	if got.Seq != e.Seq || got.Op != e.Op || got.Table != e.Table || got.Key != e.Key || got.Fk != e.Fk || !bytes.Equal(got.ValueBytes, e.ValueBytes) {
		t.Fatalf("decode mismatch: %+v vs %+v", got, e)
	}
}

func TestEntryRoundTripDelete(t *testing.T) {
	e := Entry{Seq: 2, Op: OpDel, Table: "t", Key: "x", Fk: ""}
	b, err := e.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := got.UnmarshalBinary(b); err != nil {
		t.Fatal(err)
	}
	if got.Op != OpDel || len(got.ValueBytes) != 0 {
		t.Fatalf("got %+v", got)
	}
}

func TestEntryGoldenPutAndDelete(t *testing.T) {
	put := Entry{Seq: 1, Op: OpPut, Table: "t", Key: "a", Fk: "g", ValueBytes: []byte{0x01}}
	putBytes, err := put.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	// Stable size: magic+ver+seq+op+strings+value; assert non-zero fixed prefix
	if len(putBytes) < 40 {
		t.Fatalf("put record unexpectedly short: %d", len(putBytes))
	}
	del := Entry{Seq: 2, Op: OpDel, Table: "t", Key: "a"}
	delBytes, err := del.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	if len(delBytes) >= len(putBytes) {
		t.Fatalf("delete frame should be smaller than put")
	}
}

func TestEntryUnmarshalTruncated(t *testing.T) {
	e := Entry{Seq: 3, Op: OpPut, Table: "x", Key: "y", Fk: "", ValueBytes: []byte{2, 3}}
	b, err := e.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	var got Entry
	if err := got.UnmarshalBinary(b[:len(b)-3]); err == nil {
		t.Fatal("want error on truncated frame")
	}
}

func TestEntryBadCRC(t *testing.T) {
	e := Entry{Seq: 1, Op: OpPut, Table: "t", Key: "k", Fk: "f", ValueBytes: []byte{9}}
	b, err := e.MarshalBinary()
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-1] ^= 0xff
	var got Entry
	if err := got.UnmarshalBinary(b); err != ErrCorruptRecord {
		t.Fatalf("want ErrCorruptRecord, got %v", err)
	}
}
