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

type operationType string

const (
	operationTypeOptimisticLock operationType = "optimistic_lock"
	operationTypeNormal         operationType = "normal"
)

type putParams struct {
	TableName     string        `param:"tableName"`
	OperationType operationType `query:"operation"`
}

func (p *putParams) Validate() error {
	if p.TableName == "" {
		return errors.New("table name is required")
	}

	if p.OperationType == "" {
		p.OperationType = operationTypeNormal
	}

	validOperationTypes := []operationType{operationTypeOptimisticLock, operationTypeNormal}
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
			if params.OperationType == operationTypeOptimisticLock {
				cmd = commands.OptimisticSetCmd
			} else {
				it.Version = nil
			}

			if err := db.Set(r.Context(), params.TableName, it); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       cmd,
				TableName: params.TableName,
				Item:      it,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(it) //nolint:errcheck
		},
	}
}
