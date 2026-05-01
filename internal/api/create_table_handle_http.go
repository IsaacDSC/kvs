package api

import (
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/commands"
	"github.com/IsaacDSC/kvs/pkg/httphandler"
)

type CreateTableNode interface {
	ProposeCommand(command commands.Data) error
}

type createTableInput struct {
	TableName string `json:"table_name"`
}

type createTableOutput struct {
	TableName string `json:"table_name"`
}

func CreateTableHandler(node CreateTableNode) httphandler.Handler {
	return httphandler.Handler{
		Pattern: "POST /table",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var input createTableInput
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			cmdData := commands.Data{
				Cmd:       commands.CreateTableCmd,
				TableName: input.TableName,
			}

			if err := node.ProposeCommand(cmdData); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}

			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(createTableOutput{TableName: input.TableName}) //nolint:errcheck
		},
	}
}
