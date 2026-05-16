package durable

// LoadLastSeq reads checkpoint.cbor in dir and returns the stored LastSeq without
// loading table snapshots. Missing file returns (0, nil).
func LoadLastSeq(dir string) (uint64, error) {
	cf, err := parseCheckpointDir(dir)
	if err != nil {
		return 0, err
	}
	return cf.LastSeq, nil
}

// SaveLastSeq writes only version and LastSeq (no table snapshot) atomically.
// Use after durable state on disk (e.g. fsdb) reflects all mutations through seq.
func SaveLastSeq(dir string, seq uint64) error {
	cf := checkpointFile{
		Version: 1,
		LastSeq: seq,
		Tables:  nil,
	}
	return writeCheckpointAtomic(dir, cf)
}
