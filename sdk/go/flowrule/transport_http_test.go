package flowrulz

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestHTTPTransportSendBatchDoesNotDeadlockWhenCloseWaits(t *testing.T) {
	firstRequest := make(chan struct{})
	releaseFirst := make(chan struct{})
	var requests atomic.Int32

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) == 1 {
			close(firstRequest)
			<-releaseFirst
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer server.Close()

	cfg := &TransportConfig{Config: &Config{
		HTTPAddr:    server.URL,
		HTTPTimeout: 2 * time.Second,
		SendTimeout: 2 * time.Second,
	}}
	transport := &httpTransport{
		cfg:     cfg,
		client:  server.Client(),
		baseURL: server.URL,
	}

	batchDone := make(chan error, 1)
	go func() {
		batchDone <- transport.SendBatch(context.Background(), []*Envelope{
			{ID: "one", Type: "order.created", TenantID: "tenant-a"},
			{ID: "two", Type: "order.created", TenantID: "tenant-a"},
		})
	}()

	select {
	case <-firstRequest:
	case <-time.After(time.Second):
		t.Fatal("first batch request did not reach the server")
	}

	closeStarted := make(chan struct{})
	closeDone := make(chan error, 1)
	go func() {
		close(closeStarted)
		closeDone <- transport.Close()
	}()
	<-closeStarted
	time.Sleep(20 * time.Millisecond)
	close(releaseFirst)

	select {
	case err := <-batchDone:
		if err != nil {
			t.Fatalf("SendBatch() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("SendBatch deadlocked while Close was waiting")
	}

	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not finish after the batch completed")
	}

	if got := requests.Load(); got != 2 {
		t.Fatalf("request count = %d, want 2", got)
	}
}
