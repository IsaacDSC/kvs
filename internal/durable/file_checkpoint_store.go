package durable

// FileCheckpointStore implements the wal.CheckpointStore interface for CBOR checkpoints on disk.
type FileCheckpointStore struct{}

// NewFileCheckpointStore returns a FileCheckpointStore for use with wal.Options.CheckpointStore.
func NewFileCheckpointStore() *FileCheckpointStore {
	return &FileCheckpointStore{}
}

func (*FileCheckpointStore) SaveLastSeq(dir string, seq uint64) error {
	return SaveLastSeq(dir, seq)
}

// ReadCheckpointTables implements the CheckpointStore contract expected by package wal.
func (*FileCheckpointStore) ReadCheckpointTables(dir string) (uint64, map[string]map[string][]byte, error) {
	cf, err := parseCheckpointDir(dir)
	if err != nil {
		return 0, nil, err
	}
	if len(cf.Tables) == 0 {
		return cf.LastSeq, nil, nil
	}
	out := make(map[string]map[string][]byte, len(cf.Tables))
	for name, snap := range cf.Tables {
		out[name] = copyBytesMap(snap.Data)
	}
	return cf.LastSeq, out, nil
}
