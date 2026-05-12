package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"slices"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/item"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type PutDb interface {
	Set(ctx context.Context, tableName string, entity item.Entity) error
}

type putInput struct {
	SK    string         `json:"sk,omitempty"`
	Value map[string]any `json:"value"`
}

func (i *putInput) Validate() error {
	if len(i.Value) == 0 {
		return errors.New("value is required")
	}

	return nil
}

type operationType string

const (
	operationTypeOptimisticLock operationType = "optimistic_lock"
	operationTypeNormal         operationType = "normal"
)

type putParams struct {
	TableName     string        `param:"tableName"`
	Key           string        `param:"key"`
	OperationType operationType `query:"operation_type"`
	Version       string        `query:"version"`
}

func (p *putParams) Validate() error {
	if p.TableName == "" {
		return errors.New("table name is required")
	}

	if p.Key == "" {
		return errors.New("key is required")
	}

	if p.OperationType == "" {
		p.OperationType = operationTypeNormal
	}

	validOperationTypes := []operationType{operationTypeOptimisticLock, operationTypeNormal}
	if !slices.Contains(validOperationTypes, p.OperationType) {
		return errors.New("operation type is invalid")
	}

	if p.OperationType == operationTypeOptimisticLock && p.Version == "" {
		return errors.New("version is required for optimistic lock")
	}

	return nil
}

type putOutput struct {
	TableName string `json:"table_name"`
	SK        string `json:"sk,omitempty"`
	Key       string `json:"key"`
	Version   string `json:"version,omitempty"`
	Value     any    `json:"value"`
}

func PutHandler(db PutDb, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "PUT /table/{tableName}/{key}",
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

			var input putInput
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err := input.Validate(); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			entity := item.Entity{
				Key:   params.Key,
				SK:    input.SK,
				Value: input.Value,
			}

			if err := db.Set(r.Context(), params.TableName, entity); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       commands.SetCmd,
				TableName: params.TableName,
				Item:      entity,
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			json.NewEncoder(w).Encode(putOutput{
				TableName: params.TableName,
				Key:       params.Key,
				SK:        input.SK,
				Version:   params.Version,
				Value:     input.Value,
			}) //nolint:errcheck
		},
	}
}
