package durable

import (
	"github.com/IsaacDSC/kvs/internal/old/memdb"
)

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
	cf, err := parseCheckpointDir(dir)
	if err != nil {
		return 0, err
	}
	if len(cf.Tables) == 0 {
		return cf.LastSeq, nil
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

	return writeCheckpointAtomic(dir, cf)
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
