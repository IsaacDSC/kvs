package durable

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/IsaacDSC/kvs/internal/memdb"
	"github.com/fxamacker/cbor/v2"
)

const CheckpointFileName = "checkpoint.cbor"

type checkpointFile struct {
	Version int                  `cbor:"v"`
	LastSeq uint64               `cbor:"seq"`
	Tables  map[string]tableSnap `cbor:"t"`
}

type tableSnap struct {
	Data map[string][]byte   `cbor:"d"`
	Fk   map[string][]string `cbor:"fk"`
}

// LoadCheckpoint restores DB state from checkpoint.cbor, returning the stored last sequence.
func LoadCheckpoint(dir string, database *memdb.DB) (lastSeq uint64, err error) {
	path := filepath.Join(dir, CheckpointFileName)
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	var cf checkpointFile
	if err := cbor.Unmarshal(raw, &cf); err != nil {
		return 0, err
	}
	if cf.Version != 1 {
		return 0, fmt.Errorf("durable: unsupported checkpoint version %d", cf.Version)
	}
	database.Lock.Lock()
	defer database.Lock.Unlock()
	for name, snap := range cf.Tables {
		database.Tables[name] = memdb.NewTableFromSnapshot(copyBytesMap(snap.Data), copyStringSliceMap(snap.Fk))
	}
	return cf.LastSeq, nil
}

// SaveCheckpoint writes the current DB state and last sequence atomically.
func SaveCheckpoint(dir string, database *memdb.DB, lastSeq uint64) error {
	cf := checkpointFile{
		Version: 1,
		LastSeq: lastSeq,
		Tables:  make(map[string]tableSnap),
	}
	database.Lock.RLock()
	for name, t := range database.Tables {
		d, f := t.ExportMaps()
		cf.Tables[name] = tableSnap{Data: d, Fk: f}
	}
	database.Lock.RUnlock()

	raw, err := cbor.Marshal(cf)
	if err != nil {
		return err
	}
	path := filepath.Join(dir, CheckpointFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

func copyBytesMap(m map[string][]byte) map[string][]byte {
	if m == nil {
		return nil
	}
	out := make(map[string][]byte, len(m))
	for k, v := range m {
		vv := make([]byte, len(v))
		copy(vv, v)
		out[k] = vv
	}
	return out
}

func copyStringSliceMap(m map[string][]string) map[string][]string {
	if m == nil {
		return nil
	}
	out := make(map[string][]string, len(m))
	for k, v := range m {
		sl := make([]string, len(v))
		copy(sl, v)
		out[k] = sl
	}
	return out
}
