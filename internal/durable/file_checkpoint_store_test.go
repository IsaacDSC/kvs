package durable_test

import (
	"testing"

	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/wal"
)

func TestFileCheckpointStore_implementsWalCheckpointStore(t *testing.T) {
	var _ wal.CheckpointStore = durable.NewFileCheckpointStore()
}
