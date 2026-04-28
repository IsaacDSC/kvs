package raft

// RequestVoteArgs is sent by a Candidate to collect votes.
type RequestVoteArgs struct {
	Term         int    `json:"term"`
	CandidateID  string `json:"candidateId"`
	LastLogIndex int    `json:"lastLogIndex"`
	LastLogTerm  int    `json:"lastLogTerm"`
}

// RequestVoteReply is the response to RequestVote.
type RequestVoteReply struct {
	Term        int  `json:"term"`
	VoteGranted bool `json:"voteGranted"`
}

// AppendEntriesArgs is sent by the Leader to replicate log entries
// and also serves as a heartbeat when Entries is empty.
type AppendEntriesArgs struct {
	Term         int        `json:"term"`
	LeaderID     string     `json:"leaderId"`
	PrevLogIndex int        `json:"prevLogIndex"`
	PrevLogTerm  int        `json:"prevLogTerm"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int        `json:"leaderCommit"`
}

// AppendEntriesReply is the response to AppendEntries.
type AppendEntriesReply struct {
	Term    int  `json:"term"`
	Success bool `json:"success"`
}
