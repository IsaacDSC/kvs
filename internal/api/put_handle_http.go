package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type PutDb interface {
	Set(ctx context.Context, tableName string, item dto.Item) error
}

type putParams struct {
	TableName     string            `param:"tableName"`
	OperationType dto.OperationType `query:"operation"`
	RaftMinAcks   int               `query:"raft_min_acks"`
}

func (p *putParams) Validate() error {
	if p.TableName == "" {
		return errors.New("table name is required")
	}

	if p.OperationType == "" {
		p.OperationType = dto.OperationTypeNormal
	}

	validOperationTypes := []dto.OperationType{dto.OperationTypeOptimisticLock, dto.OperationTypeNormal}
	if !slices.Contains(validOperationTypes, p.OperationType) {
		return errors.New("operation type is invalid")
	}

	if p.RaftMinAcks < 0 {
		return errors.New("invalid raft_min_acks")
	}

	return nil
}

func PutHandler(db PutDb, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "PUT /table/{tableName}",
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

			var it dto.Item
			if err := json.NewDecoder(r.Body).Decode(&it); err != nil {
				return www.NewResponse(
					www.StatusCode(http.StatusBadRequest),
					www.RespErr(err),
				)
			}

			if fe := it.Validate(); fe != nil {
				var payload map[string]any
				if err := json.Unmarshal(fe.Json(), &payload); err != nil {
					payload = map[string]any{"error": string(fe.Json())}
				}
				return www.NewResponse(
					www.StatusCode(http.StatusUnprocessableEntity),
					www.Body(payload),
				)
			}

			cmd := commands.SetCmd
			if params.OperationType == dto.OperationTypeOptimisticLock {
				cmd = commands.OptimisticSetCmd
			} else {
				it.Version = nil
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

			if err := db.Set(r.Context(), params.TableName, it); err != nil {
				return www.NewResponse(
					www.StatusCode(getStatusCode(err)),
					www.RespErr(err),
				)
			}

			if rpcErr := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       cmd,
				TableName: params.TableName,
				Item:      it,
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
