package db

import "context"

// CheckpointBlobHydrator is implemented by stores that restore state from WAL checkpoint row blobs.
type CheckpointBlobHydrator interface {
	ReplaceWithCheckpointBlobs(ctx context.Context, tables map[string]map[string][]byte) error
}
