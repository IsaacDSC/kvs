package api

import (
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type ReplicateNodes interface {
	ProposeCommand(command commands.Data) *dto.ErrProposeCmd
	PermittedProposeCmd() *dto.ErrProposeCmd
	FullClusterReplicationMinAcks() int
}

type Db interface {
	CreateTable(table string) error
}

type createTableInput struct {
	TableName   string `json:"table_name"`
	RaftMinAcks int    `json:"raft_min_acks,omitempty"`
}

type createTableOutput struct {
	TableName string `json:"table_name"`
}

func CreateTableHandler(db Db, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "POST /table",
		Fn: func(r *http.Request) *www.Response {
			var input createTableInput
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusBadRequest),
					www.RespErr(err),
				)
			}

			//validate if node is lead
			if rpcErr := replicateNodes.PermittedProposeCmd(); rpcErr != nil {
				var payload map[string]any
				if err := json.Unmarshal(rpcErr.RespJson(), &payload); err != nil {
					payload = map[string]any{"error": string(rpcErr.RespJson())}
				}
				return www.NewResponse(
					www.StatusCode(getStatusCode(rpcErr.Err())),
					www.Body(payload),
				)
			}

			if err := db.CreateTable(input.TableName); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusInternalServerError),
					www.RespErr(err),
				)
			}

			if rpcErr := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       commands.CreateTableCmd,
				TableName: input.TableName,
				MinAcks:   HTTPDefaultRaftMinAcks(replicateNodes, input.RaftMinAcks),
			}); rpcErr != nil {
				var payload map[string]any
				if err := json.Unmarshal(rpcErr.RespJson(), &payload); err != nil {
					payload = map[string]any{"error": string(rpcErr.RespJson())}
				}
				return www.NewResponse(
					www.StatusCode(getStatusCode(rpcErr.Err())),
					www.Body(payload),
				)
			}

			return www.NewResponse(
				www.StatusCode(http.StatusCreated),
				www.Body(createTableOutput{TableName: input.TableName}),
			)
		},
	}
}
