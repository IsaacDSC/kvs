package api

import (
	"net/http"

	"github.com/IsaacDSC/kvs/internal/node"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type StateNode interface {
	State() node.State
}

func StateHandler(node StateNode) www.Handler {
	return www.Handler{
		Pattern: "GET /state",
		Fn: func(r *http.Request) *www.Response {
			return www.NewResponse(
				www.StatusCode(http.StatusOK),
				www.Body(node.State()),
			)
		},
	}
}
