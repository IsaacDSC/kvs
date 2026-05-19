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
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var params deleteParams
			if err := www.DecodeParams(r, &params); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			var it dto.DeleteItem
			if err := json.NewDecoder(r.Body).Decode(&it); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err := it.Validate(params.OperationType); err != nil {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(http.StatusUnprocessableEntity)
				_, _ = w.Write(err.Json())
				return
			}

			cmd := commands.DeleteCmd
			if params.OperationType == dto.OperationTypeOptimisticLock {
				cmd = commands.OptimisticDelCmd
			}

			//validate if node is lead
			if rpcErr := replicateNodes.PermittedProposeCmd(); rpcErr != nil {
				writeErrProposeCmd(w, rpcErr)
				return
			}

			err := db.Delete(r.Context(), params.TableName, it)
			if err != nil {
				statusCode := getStatusCode(err)
				http.Error(w, err.Error(), statusCode)
				return
			}

			if rpcErr := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       cmd,
				TableName: params.TableName,
				Item:      it.Item(),
			}); rpcErr != nil {
				writeErrProposeCmd(w, rpcErr)
				return
			}

			w.WriteHeader(http.StatusNoContent)
		},
	}
}
