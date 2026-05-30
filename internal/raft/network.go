package raft

import "context"

// Network is how a Raft node talks to peers (normally gRPC; see Transport).
type Network interface {
	RequestVote(ctx context.Context, peer string, args RequestVoteArgs, reply *RequestVoteReply) error
	AppendEntries(ctx context.Context, peer string, args AppendEntriesArgs, reply *AppendEntriesReply) error
	Close()
}
