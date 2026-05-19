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
}

type Db interface {
	CreateTable(table string) error
}

type createTableInput struct {
	TableName string `json:"table_name"`
}

type createTableOutput struct {
	TableName string `json:"table_name"`
}

func CreateTableHandler(db Db, replicateNodes ReplicateNodes) www.Handler {
	return www.Handler{
		Pattern: "POST /table",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var input createTableInput
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			//validate if node is lead
			if rpcErr := replicateNodes.PermittedProposeCmd(); rpcErr != nil {
				writeErrProposeCmd(w, rpcErr)
				return
			}

			if err := db.CreateTable(input.TableName); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			if rpcErr := replicateNodes.ProposeCommand(commands.Data{
				Cmd:       commands.CreateTableCmd,
				TableName: input.TableName,
			}); rpcErr != nil {
				writeErrProposeCmd(w, rpcErr)
				return
			}

			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(createTableOutput{TableName: input.TableName}) //nolint:errcheck
		},
	}
}
