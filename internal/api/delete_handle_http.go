package api

import (
	"context"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/internal/dto"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type DeleteDb interface {
	Delete(ctx context.Context, tableName string, key string) error
}

type deleteParams struct {
	TableName string `param:"tableName"`
	Key       string `param:"key"`
}

func DeleteHandler(db DeleteDb, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "DELETE /table/{tableName}/{key}",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var params deleteParams
			if err := www.DecodeParams(r, &params); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err := db.Delete(r.Context(), params.TableName, params.Key); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if err := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       commands.DeleteCmd,
				TableName: params.TableName,
				Item: dto.Item{
					Key: params.Key,
				},
			}); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusNoContent)
		},
	}
}
