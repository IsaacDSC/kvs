package api

import (
	"net/http"

	"github.com/IsaacDSC/kvs/pkg/www"
)

func PingHandler() www.Handler {
	return www.Handler{
		Pattern: "GET /ping",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		},
	}
}
