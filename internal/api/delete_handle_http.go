package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type DeleteDb interface {
	Delete(ctx context.Context, tableName string, it dto.DeleteItem) error
}

type deleteParams struct {
	TableName     string            `param:"tableName"`
	OperationType dto.OperationType `query:"operation"`
}

func DeleteHandler(db DeleteDb, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "DELETE /table/{tableName}",
		Fn: func(r *http.Request) *www.Response {
			var params deleteParams
			if err := www.DecodeParams(r, &params); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusBadRequest),
					www.RespErr(err),
				)
			}

			var it dto.DeleteItem
			if err := json.NewDecoder(r.Body).Decode(&it); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusBadRequest),
					www.RespErr(err),
				)
			}

			if fe := it.Validate(params.OperationType); fe != nil {
				var payload map[string]any
				if err := json.Unmarshal(fe.Json(), &payload); err != nil {
					payload = map[string]any{"error": string(fe.Json())}
				}
				return www.NewResponse(
					www.StatusCode(http.StatusUnprocessableEntity),
					www.Body(payload),
				)
			}

			cmd := commands.DeleteCmd
			if params.OperationType == dto.OperationTypeOptimisticLock {
				cmd = commands.OptimisticDelCmd
			}

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

			if err := db.Delete(r.Context(), params.TableName, it); err != nil {
				return www.NewResponse(
					www.StatusCode(getStatusCode(err)),
					www.RespErr(err),
				)
			}

			if rpcErr := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       cmd,
				TableName: params.TableName,
				Item:      it.Item(),
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

			return www.NewResponse(www.StatusCode(http.StatusNoContent))
		},
	}
}
