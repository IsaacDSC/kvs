package raft

import "github.com/IsaacDSC/kvs/internal/commands"

// LogEntry holds a single entry in the replicated log.
type LogEntry struct {
	Term int           `json:"term"`
	Data commands.Data `json:"data"`
}
