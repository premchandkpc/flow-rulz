package broker

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	runtimebroker "github.com/flowrulz/flow-rulz/internal/runtime/broker"
)

func TestRejectsMalformedAndUnboundedRequests(t *testing.T) {
	b, err := runtimebroker.New(runtimebroker.Config{Partitions: 1, MaxMessages: 10, MaxBytes: 1 << 20, AckWait: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandler(b, "")
	for _, tc := range []struct{ path, body string }{
		{"consume", `{"topic":"events","group":"g","wait_ms":-1}`},
		{"consume", `{"topic":"events","group":"g","wait_ms":20001}`},
		{"consume", `{"topic":"events","group":"g","unknown":true}`},
		{"consume", `{"topic":"events","group":"g"} {}`},
		{"consume", strings.Repeat(" ", 1<<20) + `{}`},
		{"retry", `{"topic":"events","group":"g","receipt":"x","delay_ms":300001}`},
	} {
		req := httptest.NewRequest(http.MethodPost, "/v1/broker/"+tc.path, strings.NewReader(tc.body))
		req.Header.Set("X-Tenant-ID", "a")
		response := httptest.NewRecorder()
		h.ServeHTTP(response, req)
		if response.Code != http.StatusBadRequest {
			t.Fatalf("%s status=%d", tc.path, response.Code)
		}
	}
}
