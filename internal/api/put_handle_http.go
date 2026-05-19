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

	return nil
}

func PutHandler(db PutDb, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "PUT /table/{tableName}",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var params putParams
			if err := www.DecodeParams(r, &params); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err := params.Validate(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var it dto.Item
			if err := json.NewDecoder(r.Body).Decode(&it); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err := it.Validate(); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write(err.Json())
				return
			}

			// Security validation only permit save version if use operation equal at OptimisticLock
			cmd := commands.SetCmd
			if params.OperationType == dto.OperationTypeOptimisticLock {
				cmd = commands.OptimisticSetCmd
			} else {
				it.Version = nil
			}

			if err := db.Set(r.Context(), params.TableName, it); err != nil {
				http.Error(w, err.Error(), getStatusCode(err))
				return
			}

			if err := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       cmd,
				TableName: params.TableName,
				Item:      it,
			}); err != nil {
				w.WriteHeader(getStatusCode(err.Err()))
				_ = json.NewEncoder(w).Encode(err.RespJson())
				return
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(it) //nolint:errcheck
		},
	}
}
