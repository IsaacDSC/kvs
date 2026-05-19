package api

import (
	"errors"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/db"
	"github.com/IsaacDSC/kvs/internal/dto"
)

func getStatusCode(err error) int {
	switch {
	case errors.Is(err, db.ErrNotCompatibleVersion) ||
		errors.Is(err, db.ErrFollowerRejectCmd) ||
		errors.Is(err, db.ErrNotFound) ||
		errors.Is(err, db.ErrNotFoundSk) ||
		errors.Is(err, db.ErrTableNotFound):
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}

func writeErrProposeCmd(w http.ResponseWriter, rpcErr *dto.ErrProposeCmd) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(getStatusCode(rpcErr.Err()))
	_, _ = w.Write(rpcErr.RespJson())
}
