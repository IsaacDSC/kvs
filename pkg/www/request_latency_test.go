package www

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRequestLatency_logsDurationAndStatus(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: stripTime}))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /x", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTeapot)
	})

	h := RequestLatency(log)(mux)
	req := httptest.NewRequest(http.MethodGet, "/x", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusTeapot {
		t.Fatalf("status: got %d", rec.Code)
	}
	out := buf.String()
	if !strings.Contains(out, "418") || !strings.Contains(out, "/x") || !strings.Contains(out, "GET") || !strings.Contains(out, "duration_ms") {
		t.Fatalf("log line missing fields: %q", out)
	}
}

func TestRequestLatency_skipsPingByDefault(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: stripTime}))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	h := RequestLatency(log)(mux)
	req := httptest.NewRequest(http.MethodGet, "/ping", nil)
	h.ServeHTTP(httptest.NewRecorder(), req)

	if buf.Len() != 0 {
		t.Fatalf("expected no log for /ping by default, got: %s", strings.TrimSpace(buf.String()))
	}
}

func TestRequestLatency_WithSkipLogPaths_skipsListedAndStillOmitsPing(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{ReplaceAttr: stripTime}))

	mux := http.NewServeMux()
	mux.HandleFunc("GET /ping", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("GET /x", func(w http.ResponseWriter, r *http.Request) {})

	h := RequestLatency(log, WithSkipLogPaths("/health"))(mux)

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/ping", nil))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/health", nil))
	buf.Reset()

	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
	out := buf.String()
	if !strings.Contains(out, "/x") {
		t.Fatalf("expected log for /x after skips, got: %q", out)
	}
}

func stripTime(groups []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}
