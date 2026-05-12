package store

import "github.com/IsaacDSC/kvs/internal/wal"

const WalFileName = wal.WalFileName

// WAL is an append-only log file.
type WAL = wal.WAL

func openWAL(path string, opts Options) (*WAL, error) {
	return wal.Open(path, opts)
}

// RepairTruncatesTail reads path and truncates the file to the last complete record (same rules as Replay).
func RepairTruncatesTail(path string) error {
	return wal.RepairTruncatesTail(path)
}
