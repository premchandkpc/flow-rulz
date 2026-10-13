package observability

import (
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// NewMetricsServer returns a private-by-default HTTP server for scraping the
// process-local Prometheus registry. allowedOrigin enables browser access only
// for one explicitly configured console origin; an empty value disables CORS.
func NewMetricsServer(allowedOrigin string) *http.Server {
	allowedOrigin = strings.TrimRight(strings.TrimSpace(allowedOrigin), "/")
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.Handler())
	mux.HandleFunc("OPTIONS /metrics", func(w http.ResponseWriter, r *http.Request) {
		if allowedOrigin == "" || r.Header.Get("Origin") != allowedOrigin {
			http.Error(w, "origin not allowed", http.StatusForbidden)
			return
		}
		w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "600")
		w.Header().Add("Vary", "Origin")
		w.WriteHeader(http.StatusNoContent)
	})
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != "" && allowedOrigin != "" && origin == allowedOrigin {
			w.Header().Set("Access-Control-Allow-Origin", allowedOrigin)
			w.Header().Add("Vary", "Origin")
		}
		mux.ServeHTTP(w, r)
	})
	return &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second}
}
