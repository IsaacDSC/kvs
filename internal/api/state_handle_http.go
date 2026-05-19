package api

import (
	"encoding/json"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/node"
	"github.com/IsaacDSC/kvs/pkg/www"
)

type StateNode interface {
	State() node.State
}

type NodeStateResponse struct {
	ID          string `json:"id"`
	Role        string `json:"role"`
	LeaderID    string `json:"leader_id,omitempty"`
	Term        int    `json:"term"`
	CommitIndex int    `json:"commitIndex"`
	LogLen      int    `json:"logLen"`
}

func StateHandler(node StateNode) www.Handler {
	return www.Handler{
		Pattern: "GET /state",
		Fn: func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(node.State()) //nolint:errcheck
		},
	}
}
