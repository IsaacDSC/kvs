package api

import (
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/pkg/www"
)

type Db interface {
	CreateTable(table string) error
}

type createTableInput struct {
	TableName string `json:"table_name"`
}

type createTableOutput struct {
	TableName string `json:"table_name"`
}

func CreateTableHandler(db Db) www.Handler {
	return www.Handler{
		Pattern: "POST /table",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var input createTableInput
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err := db.CreateTable(input.TableName); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusAccepted)
			json.NewEncoder(w).Encode(createTableOutput{TableName: input.TableName}) //nolint:errcheck
		},
	}
}
