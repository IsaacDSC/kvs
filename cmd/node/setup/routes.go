package setup

import (
	"github.com/IsaacDSC/kvs/internal/api"
	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/raft"
	"github.com/IsaacDSC/kvs/pkg/www"
)

func GetRoutes(database *db.Adapter, raftNode *raft.Node) []www.Handler {
	return []www.Handler{
		api.PingHandler(),
		api.CreateTableHandler(database, raftNode),
		api.PutHandler(database, raftNode),
		api.BulkPutHandle(database, raftNode),
		api.DeleteHandler(database, raftNode),
		api.GetHandler(database),
		api.GetBySecondaryKeyHandler(database),
		api.StateHandler(raftNode),
	}
}
