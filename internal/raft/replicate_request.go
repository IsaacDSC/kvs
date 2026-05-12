package raft

import "github.com/IsaacDSC/kvs/internal/raftpb"

func toReplicateRequest(args AppendEntriesArgs) *raftpb.ReplicateRequest {
	entries := make([]raftpb.LogEntry, len(args.Entries))
	for i, e := range args.Entries {
		entries[i] = raftpb.LogEntry{Term: int32(e.Term), Data: e.Data}
	}
	return &raftpb.ReplicateRequest{
		Term:         int32(args.Term),
		LeaderID:     args.LeaderID,
		PrevLogIndex: int32(args.PrevLogIndex),
		PrevLogTerm:  int32(args.PrevLogTerm),
		Entries:      entries,
		LeaderCommit: int32(args.LeaderCommit),
	}
}
