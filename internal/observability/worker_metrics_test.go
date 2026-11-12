package observability

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestWorkerDeliveryStageMetricsAreScrapable(t *testing.T) {
	ObserveWorkerDeliveryStage("process", domain.ErrDatabaseError, 20*time.Millisecond)
	ObserveWorkerDeliveryStage("process", context.DeadlineExceeded, time.Second)
	ObserveWorkerDeliveryStage("retry", nil, time.Millisecond)
	ObserveWorkerDeliveryStage("ack", errors.New("raw broker error"), 2*time.Millisecond)

	req := httptest.NewRequest("GET", "/metrics", nil)
	res := httptest.NewRecorder()
	NewMetricsServer("").Handler.ServeHTTP(res, req)
	if res.Code != 200 {
		t.Fatalf("GET /metrics status = %d, want 200", res.Code)
	}
	body := res.Body.String()
	for _, labels := range []string{
		`result="database",stage="process"`,
		`result="timeout",stage="process"`,
		`result="ok",stage="retry"`,
		`result="broker",stage="ack"`,
	} {
		series := "flowrulz_worker_delivery_stage_duration_seconds_count{" + labels + "} "
		if !strings.Contains(body, series) {
			t.Errorf("missing metric %s", series)
		}
	}
	if strings.Contains(body, "raw broker error") {
		t.Error("raw broker error leaked in metrics")
	}
}
