package raftpb

// LogEntry is a single entry in the replicated log.
type LogEntry struct {
	Term    int32  `json:"term"`
	Command string `json:"command"`
}

// VoteRequest is sent by a Candidate to request a vote (§5.2).
type VoteRequest struct {
	Term         int32  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int32  `json:"last_log_index"`
	LastLogTerm  int32  `json:"last_log_term"`
}

// VoteResponse is the reply to VoteRequest.
type VoteResponse struct {
	Term        int32 `json:"term"`
	VoteGranted bool  `json:"vote_granted"`
}

// ReplicateRequest is sent by the Leader over the Replicate stream.
// When Entries is empty it acts as a heartbeat.
type ReplicateRequest struct {
	Term         int32      `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int32      `json:"prev_log_index"`
	PrevLogTerm  int32      `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int32      `json:"leader_commit"`
}

// ReplicateResponse is sent by the Follower back on the Replicate stream.
type ReplicateResponse struct {
	Term    int32 `json:"term"`
	Success bool  `json:"success"`
}
