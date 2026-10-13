// Package http provides HTTP server readiness checks.
package http

import (
	"context"
	"encoding/json"
	"net/http"
	"time"
)

const readinessTimeout = 2 * time.Second

type dependencyStatus struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
}

// readinessHandler reports per-dependency state and returns 503 unless all probes pass.
func readinessHandler(checks map[string]func(context.Context) error) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), readinessTimeout)
		defer cancel()

		deps := make(map[string]dependencyStatus, len(checks))
		ready := true
		for name, check := range checks {
			if err := check(ctx); err != nil {
				ready = false
				deps[name] = dependencyStatus{Status: "down", Error: "unavailable"}
				continue
			}
			deps[name] = dependencyStatus{Status: "up"}
		}

		status, label := http.StatusOK, "ready"
		if !ready {
			status, label = http.StatusServiceUnavailable, "not_ready"
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{"status": label, "dependencies": deps}})
	}
}
