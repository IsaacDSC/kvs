package wal

// CheckpointStore reads and writes checkpoint.cbor (LastSeq and optional per-table encoded blobs).
// When Options.CheckpointDir is non-empty, Options.CheckpointStore must be set (e.g. durable.NewFileCheckpointStore).
type CheckpointStore interface {
	// ReadCheckpointTables returns LastSeq and table blobs (nil or empty if metadata-only checkpoint).
	ReadCheckpointTables(dir string) (lastSeq uint64, tables map[string]map[string][]byte, err error)
	SaveLastSeq(dir string, seq uint64) error
}
