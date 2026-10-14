package application

import (
	"testing"
	"time"
)

func TestNormalizeWorkerConfigPreservesPartialOverrides(t *testing.T) {
	got, err := normalizeWorkerConfig(WorkerConfig{
		FetchBatchSize:  17,
		KeyQueueWorkers: 4,
		RetryDelay:      2 * time.Second,
	})
	if err != nil {
		t.Fatalf("normalizeWorkerConfig() error = %v", err)
	}
	if got.MaxGlobalInFlight != 100 {
		t.Fatalf("MaxGlobalInFlight = %d, want 100", got.MaxGlobalInFlight)
	}
	if got.FetchBatchSize != 17 {
		t.Fatalf("FetchBatchSize = %d, want 17", got.FetchBatchSize)
	}
	if got.KeyQueueWorkers != 4 {
		t.Fatalf("KeyQueueWorkers = %d, want 4", got.KeyQueueWorkers)
	}
	if got.RetryDelay != 2*time.Second {
		t.Fatalf("RetryDelay = %s, want 2s", got.RetryDelay)
	}
}

func TestNormalizeWorkerConfigCapsDefaultFetchBatchAtGlobalLimit(t *testing.T) {
	got, err := normalizeWorkerConfig(WorkerConfig{MaxGlobalInFlight: 20})
	if err != nil {
		t.Fatalf("normalizeWorkerConfig() error = %v", err)
	}
	if got.FetchBatchSize != 20 {
		t.Fatalf("FetchBatchSize = %d, want 20", got.FetchBatchSize)
	}
}

func TestNormalizeWorkerConfigRejectsExplicitFetchBatchAboveGlobalLimit(t *testing.T) {
	_, err := normalizeWorkerConfig(WorkerConfig{
		MaxGlobalInFlight: 20,
		FetchBatchSize:    21,
	})
	if err == nil {
		t.Fatal("normalizeWorkerConfig() error = nil, want invalid batch size error")
	}
}
