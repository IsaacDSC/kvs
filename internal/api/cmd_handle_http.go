package api

import (
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/pkg/httphandler"
)

type CmdProposeNode interface {
	Propose(command string) error
}

type req struct {
	Command string `json:"command"`
}

// database == HOST
// tableName
// action/cmd - POST, PUT, DELETE, GET, GET by fk
/* example:

	POST /table
   {
		TableName string `json:"table_name"`
   }

   POST /session/{tableName}
   response	>> SessionId


   PUT /{tableName}/{key}?sessionId={sessionId}
   {
		Fk      string // optional
		Value   any // required
   }

   PUT /{tableName}/{key}?optimistic_lock=true&version=1
   {
		Fk      string // optional
		Value   any // required
   }


   DELETE /{tableName}/{key}
   GET /{tableName}/{key}
   GET /{tableName}/{fk}
*/
func CmdProposeHandler(node CmdProposeNode) httphandler.Handler {
	return httphandler.Handler{
		Pattern: "POST /cmd/propose",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			var input req
			if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}

			if err := node.Propose(input.Command); err != nil {
				http.Error(w, err.Error(), http.StatusConflict)
				return
			}

			w.WriteHeader(http.StatusAccepted)
		},
	}
}
