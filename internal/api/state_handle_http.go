package api

import (
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/node"
	"github.com/IsaacDSC/kvs/pkg/httphandler"
)

type StateNode interface {
	State() node.State
}

type NodeStateResponse struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	Term        int    `json:"term"`
	CommitIndex int    `json:"commitIndex"`
	LogLen      int    `json:"logLen"`
}

func NewNodeStateResponse(state node.State) NodeStateResponse {
	return NodeStateResponse{
		ID:          state.ID,
		Role:        state.Role,
		Term:        state.Term,
		CommitIndex: state.CommitIndex,
		LogLen:      state.LogLen,
	}
}

func StateHandler(node StateNode) httphandler.Handler {
	return httphandler.Handler{
		Pattern: "GET /state",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(node.State()) //nolint:errcheck
		},
	}
}
