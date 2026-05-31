package api

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type BulkPutDb interface {
	BulkSet(ctx context.Context, tableName string, its dto.Items) error
}

func BulkPutHandle(db BulkPutDb, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "PUT /table/{tableName}/operation/bulk",
		Fn: func(r *http.Request) *www.Response {
			var params putParams
			if err := www.DecodeParams(r, &params); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusBadRequest),
					www.RespErr(err),
				)
			}

			if err := params.Validate(); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusBadRequest),
					www.RespErr(err),
				)
			}

			minAcks := HTTPDefaultRaftMinAcks(replicateNodes, params.RaftMinAcks)

			var its dto.Items
			if err := json.NewDecoder(r.Body).Decode(&its); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusBadRequest),
					www.RespErr(err),
				)
			}

			if fe := its.Validate(); fe != nil {
				var payload map[string]any
				if err := json.Unmarshal(fe.Json(), &payload); err != nil {
					payload = map[string]any{"error": string(fe.Json())}
				}
				return www.NewResponse(
					www.StatusCode(http.StatusUnprocessableEntity),
					www.Body(payload),
				)
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

			if err := db.BulkSet(r.Context(), params.TableName, its); err != nil {
				return www.NewResponse(
					www.StatusCode(getStatusCode(err)),
					www.RespErr(err),
				)
			}

			if rpcErr := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       commands.BulkPutCmd,
				TableName: params.TableName,
				Items:     its,
				MinAcks:   minAcks,
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

			return www.NewResponse(www.StatusCode(http.StatusOK))
		},
	}
}
