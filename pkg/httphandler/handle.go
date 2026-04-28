package httphandler

import "net/http"

type Handler struct {
	Pattern string
	Fn      func(w http.ResponseWriter, r *http.Request)
}
