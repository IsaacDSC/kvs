package api

import (
	"errors"
	"net/http"

	"github.com/IsaacDSC/kvs/internal/db"
)

func getStatusCode(err error) int {
	switch {
	case errors.Is(err, db.ErrNotCompatibleVersion) ||
		errors.Is(err, db.ErrNotFound) ||
		errors.Is(err, db.ErrNotFoundSk) ||
		errors.Is(err, db.ErrTableNotFound):
		return http.StatusUnprocessableEntity
	default:
		return http.StatusInternalServerError
	}
}
