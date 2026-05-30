package api

import (
	"context"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/internal/node"
	"github.com/IsaacDSC/kvs/internal/raft"
)

type ReplicateDb interface {
	CreateTable(tableName string) error
	ApplyReplicated(ctx context.Context, tableName string, it dto.Item) error
	ApplyReplicatedDelete(ctx context.Context, tableName string, it dto.DeleteItem) error
}

type RaftNode interface {
	NextIndex() int
	State() node.State
}

type Log interface {
	Info(msg string, args ...any)
}

func GrpcHandle(l Log, database ReplicateDb, raftNode RaftNode) func(ctx context.Context, entry raft.LogEntry) error {
	return func(ctx context.Context, entry raft.LogEntry) error {
		promoteVersion, oldVersion := "EMPTY", "EMPTY"
		if entry.Data.Item.Version != nil {
			promoteVersion = entry.Data.Item.Version.PromoteVersion
			oldVersion = entry.Data.Item.Version.OldVersion
		}
		st := raftNode.State()
		l.Info("applied entry",
			"command", entry.Data.Cmd,
			"term", entry.Term,
			"raft_index", raftNode.NextIndex(),
			"state", st,
			"old_version", oldVersion,
			"promote_version", promoteVersion,
		)

		// KV mutations replicate only onto followers—the leader applied them eagerly on propose.
		if st.Role != raft.Follower.String() {
			return nil
		}

		switch entry.Data.Cmd {
		case commands.CreateTableCmd:
			return database.CreateTable(entry.Data.TableName)
		case commands.SetCmd, commands.OptimisticSetCmd:
			return database.ApplyReplicated(ctx, entry.Data.TableName, entry.Data.Item)
		case commands.DeleteCmd, commands.OptimisticDelCmd:
			return database.ApplyReplicatedDelete(ctx, entry.Data.TableName, entry.Data.Item.DelItem())
		}
		return nil
	}
}
