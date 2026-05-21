package www

import (
	"encoding/json"
	"log"
	"net/http"
)

type Response struct {
	statusCode int
	body       any
	headers    map[string]string
}

type Opts func(r *Response)

func NewResponse(opts ...Opts) *Response {
	r := &Response{
		headers: make(map[string]string),
	}

	r.headers["Content-Type"] = "application/json"

	for _, fn := range opts {
		fn(r)
	}

	return r
}

func StatusCode(s int) func(r *Response) {
	return func(r *Response) {
		r.statusCode = s
	}
}

func Body(b any) func(r *Response) {
	return func(r *Response) {
		r.body = b
	}
}

// RespErr sets the JSON body to the standard envelope {"error": "<err.Error()>"}.
// Passing a nil error is a no-op (body is unchanged).
func RespErr(err error) Opts {
	return func(r *Response) {
		if err == nil {
			return
		}
		r.body = map[string]any{"error": err.Error()}
	}
}

func Headers(headers map[string]string) func(r *Response) {
	return func(r *Response) {
		r.headers = headers
	}
}

type Handler struct {
	Pattern string
	Fn      func(r *http.Request) *Response
}

func HandlerHttp(fn func(r *http.Request) *Response) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		resp := fn(r)
		if resp == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Apply headers before WriteHeader; afterward they're locked for this response.
		for k, v := range resp.headers {
			w.Header().Set(k, v)
		}

		if resp.statusCode != 0 {
			w.WriteHeader(resp.statusCode)
		}

		var emptyBody any
		if resp.body != emptyBody {
			if err := json.NewEncoder(w).Encode(resp.body); err != nil {
				//TODO: add log
				log.Printf("[*] - LogLevel: ERROR - error on enconde body to response http: %v", err)
			}
		}
	}
}
