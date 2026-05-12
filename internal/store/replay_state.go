package store

import "github.com/IsaacDSC/kvs/internal/wal"

// replayState is kept as a compatibility alias while replay logic lives in internal/wal.
type replayState = wal.MemDBReplayer
