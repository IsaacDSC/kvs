package raft

import (
	"context"
	"io"

	"github.com/IsaacDSC/kvs/internal/raftpb"
)

// GRPCServer bridges incoming gRPC RPCs to a Node.
type GRPCServer struct {
	node *Node
}

// NewGRPCServer creates a GRPCServer that delegates to node.
func NewGRPCServer(node *Node) *GRPCServer {
	return &GRPCServer{node: node}
}

func (s *GRPCServer) RequestVote(_ context.Context, req *raftpb.VoteRequest) (*raftpb.VoteResponse, error) {
	args := RequestVoteArgs{
		Term:         int(req.Term),
		CandidateID:  req.CandidateID,
		LastLogIndex: int(req.LastLogIndex),
		LastLogTerm:  int(req.LastLogTerm),
	}

	var reply RequestVoteReply
	s.node.HandleRequestVote(args, &reply)

	return &raftpb.VoteResponse{
		Term:        int32(reply.Term),
		VoteGranted: reply.VoteGranted,
	}, nil
}

func (s *GRPCServer) Replicate(stream *raftpb.FollowerStream) error {
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		entries := make([]LogEntry, len(req.Entries))
		for i, e := range req.Entries {
			entries[i] = LogEntry{Term: int(e.Term), Data: e.Data}
		}

		args := AppendEntriesArgs{
			Term:         int(req.Term),
			LeaderID:     req.LeaderID,
			PrevLogIndex: int(req.PrevLogIndex),
			PrevLogTerm:  int(req.PrevLogTerm),
			Entries:      entries,
			LeaderCommit: int(req.LeaderCommit),
		}

		var reply AppendEntriesReply
		s.node.HandleAppendEntries(args, &reply)

		if err := stream.Send(&raftpb.ReplicateResponse{
			Term:    int32(reply.Term),
			Success: reply.Success,
		}); err != nil {
			return err
		}
	}
}
