package www

import (
	"log/slog"
	"net/http"
	"path"
	"strings"
	"time"
)

type requestLatencyConfig struct {
	skipPaths map[string]struct{}
}

// RequestLatencyOption configures behaviour of RequestLatency middleware.
type RequestLatencyOption func(*requestLatencyConfig)

// WithSkipLogPaths adds routes for which completed requests are not logged.
// Paths are matched after cleanHTTPPath; use the same normalization as URLs (path.Clean, "" → "/" ).
// Routes listed here apply in addition to the default omission of /ping for any HTTP method.
func WithSkipLogPaths(paths ...string) RequestLatencyOption {
	return func(cfg *requestLatencyConfig) {
		for _, p := range paths {
			cfg.skipPaths[cleanHTTPPath(p)] = struct{}{}
		}
	}
}

func newRequestLatencyConfig(opts ...RequestLatencyOption) requestLatencyConfig {
	cfg := requestLatencyConfig{
		skipPaths: map[string]struct{}{
			cleanHTTPPath("/ping"): {},
		},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	return cfg
}

func cleanHTTPPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	cp := path.Clean(p)
	if cp == "." {
		return "/"
	}
	return cp
}

type responseStatusWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseStatusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}

func (w *responseStatusWriter) Flush() {
	if f, ok := w.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

// RequestLatency returns middleware that logs each completed request with duration in milliseconds.
// Fields: method, path, status, duration_ms. If the handler never calls WriteHeader, status is treated as 200.
// By default logs are not emitted for /ping for any HTTP method; use WithSkipLogPaths to omit additional routes.
func RequestLatency(log *slog.Logger, opts ...RequestLatencyOption) func(http.Handler) http.Handler {
	cfg := newRequestLatencyConfig(opts...)
	skip := make(map[string]struct{}, len(cfg.skipPaths))
	for p := range cfg.skipPaths {
		skip[p] = struct{}{}
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if _, omit := skip[cleanHTTPPath(r.URL.Path)]; omit {
				next.ServeHTTP(w, r)
				return
			}

			start := time.Now()
			rw := &responseStatusWriter{ResponseWriter: w, status: http.StatusOK}
			next.ServeHTTP(rw, r)
			log.Info("http",
				"method", r.Method,
				"path", r.URL.Path,
				"status", rw.status,
				"duration_ms", time.Since(start).Milliseconds(),
			)
		})
	}
}
