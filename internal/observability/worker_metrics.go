package observability

import (
	"context"
	"errors"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var workerDeliveryStageDuration = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Name:    "flowrulz_worker_delivery_stage_duration_seconds",
		Help:    "Duration of each queued event processing, acknowledgement, retry request, or quarantine attempt.",
		Buckets: prometheus.DefBuckets,
	},
	[]string{"stage", "result"},
)

// ObserveWorkerDeliveryStage records latency and the result of one queued
// delivery stage. Error labels are fixed values and exclude identifiers and messages.
func ObserveWorkerDeliveryStage(stage string, err error, elapsed time.Duration) {
	var result string
	switch {
	case err == nil:
		result = "ok"
	case errors.Is(err, context.Canceled):
		result = "canceled"
	case errors.Is(err, context.DeadlineExceeded), errors.Is(err, domain.ErrEvaluationTimeout):
		result = "timeout"
	case stage == "ack", stage == "retry":
		result = string(domain.ErrorClassBroker)
	case stage == "quarantine":
		result = string(domain.ErrorClassQuarantine)
	default:
		result = string(domain.ClassifyError(err))
	}
	workerDeliveryStageDuration.WithLabelValues(stage, result).Observe(elapsed.Seconds())
}
