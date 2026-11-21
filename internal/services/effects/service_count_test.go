package effects

import (
	"context"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestPublishBatchCountReportsClaims(t *testing.T) {
	repo := &testOutboxRepository{effects: []domain.OutboxEffect{{ID: "one"}, {ID: "two"}}}
	svc := NewService(repo, &testEffectSender{}, nil, nil, "worker")
	count, err := svc.PublishBatchCount(context.Background(), 10)
	if err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Fatalf("claimed %d, want 2", count)
	}
	if repo.deliveredCalls != 2 {
		t.Fatalf("delivered %d, want 2", repo.deliveredCalls)
	}
}

func TestPublishBatchCountZeroAndMissingDependencies(t *testing.T) {
	svc := NewService(&testOutboxRepository{}, &testEffectSender{}, nil, nil, "worker")
	count, err := svc.PublishBatchCount(context.Background(), 0)
	if err != nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
	missing := NewService(nil, &testEffectSender{}, nil, nil, "worker")
	count, err = missing.PublishBatchCount(context.Background(), 10)
	if err == nil || count != 0 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}
