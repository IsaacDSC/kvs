package store

import (
	"github.com/IsaacDSC/kvs/internal/durable"
	"github.com/IsaacDSC/kvs/internal/memdb"
)

const CheckpointFileName = durable.CheckpointFileName

func loadCheckpoint(dir string, database *memdb.DB) (lastSeq uint64, err error) {
	return durable.LoadCheckpoint(dir, database)
}

func saveCheckpoint(dir string, database *memdb.DB, lastSeq uint64) error {
	return durable.SaveCheckpoint(dir, database, lastSeq)
}
