package ports

import (
	"context"
	"time"
)

// HealthChecker provides health check primitives
type HealthChecker interface {
	Check(ctx context.Context) HealthStatus
	Name() string
}

// HealthStatus represents the health status of a component.
type HealthStatus struct {
	Healthy   bool
	Message   string
	Details   map[string]interface{}
	Timestamp time.Time
}
