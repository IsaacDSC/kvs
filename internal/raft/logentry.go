package raft

// LogEntry holds a single entry in the replicated log.
type LogEntry struct {
	Term    int    `json:"term"`
	Command string `json:"command"`
}
