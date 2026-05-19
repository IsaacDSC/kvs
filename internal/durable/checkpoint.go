package durable

// SaveCheckpointTableBlobs writes checkpoint.cbor with LastSeq and per-table CBOR-encoded row blobs
// (same layout as snapshots from the WAL checkpoint tooling). FK maps in blobs are omitted (nil).
func SaveCheckpointTableBlobs(dir string, lastSeq uint64, tables map[string]map[string][]byte) error {
	cf := checkpointFile{
		Version: 1,
		LastSeq: lastSeq,
		Tables:  make(map[string]tableSnap, len(tables)),
	}
	for name, data := range tables {
		cf.Tables[name] = tableSnap{Data: copyBytesMap(data), Fk: nil}
	}
	return writeCheckpointAtomic(dir, cf)
}
