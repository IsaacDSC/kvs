package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type BulkDelDb interface {
	BulkDel(ctx context.Context, tableName string, its dto.DeleteItems) error
}

type bulkDelParams struct {
	TableName   string `param:"tableName"`
	RaftMinAcks int    `query:"raft_min_acks"`
}

func (p *bulkDelParams) Validate() error {
	if p.TableName == "" {
		return errors.New("table name is required")
	}
	if p.RaftMinAcks < 0 {
		return errors.New("invalid raft_min_acks")
	}
	return nil
}

func BulkDelHandle(db BulkDelDb, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "DELETE /table/{tableName}/operation/bulk",
		Fn: func(r *http.Request) *www.Response {
			var params bulkDelParams
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

			var its dto.DeleteItems
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

			if err := db.BulkDel(r.Context(), params.TableName, its); err != nil {
				return www.NewResponse(
					www.StatusCode(getStatusCode(err)),
					www.RespErr(err),
				)
			}

			if rpcErr := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       commands.BulkDelCmd,
				TableName: params.TableName,
				Items:     its.Items(),
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

			return www.NewResponse(www.StatusCode(http.StatusNoContent))
		},
	}
}
