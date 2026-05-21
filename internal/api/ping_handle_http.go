package api

import (
	"net/http"

	"github.com/IsaacDSC/kvs/pkg/www"
)

func PingHandler() www.Handler {
	return www.Handler{
		Pattern: "GET /ping",
		Fn: func(r *http.Request) *www.Response {
			return www.NewResponse(
				www.StatusCode(http.StatusOK),
				www.Body(map[string]any{"ok": true}),
			)
		},
	}
}
