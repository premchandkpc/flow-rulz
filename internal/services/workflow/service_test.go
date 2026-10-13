package workflow

import (
	"context"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type workflowRepositoryStub struct {
	ports.WorkflowRepository
	getTenant string
	getID     string
	getCalls  int
	workflow  *domain.WorkflowInstance
}

func (r *workflowRepositoryStub) Get(_ context.Context, tenantID, workflowID string) (*domain.WorkflowInstance, error) {
	r.getCalls++
	r.getTenant, r.getID = tenantID, workflowID
	return r.workflow, nil
}

func TestGetRequiresTenantScope(t *testing.T) {
	repo := &workflowRepositoryStub{
		workflow: &domain.WorkflowInstance{WorkflowID: "wf-1", TenantID: "tenant-a"},
	}
	service := NewService(repo, domain.SystemClock{})

	if _, err := service.Get(context.Background(), "", "wf-1"); err == nil {
		t.Fatal("Get with empty tenant returned no error")
	}
	if repo.getCalls != 0 {
		t.Fatalf("repository calls = %d, want 0 for missing tenant", repo.getCalls)
	}

	got, err := service.Get(context.Background(), "tenant-a", "wf-1")
	if err != nil {
		t.Fatalf("Get returned error: %v", err)
	}
	if got == nil || got.WorkflowID != "wf-1" {
		t.Fatalf("Get result = %#v, want workflow wf-1", got)
	}
	if repo.getTenant != "tenant-a" || repo.getID != "wf-1" {
		t.Fatalf("repository Get args = tenant %q workflow %q", repo.getTenant, repo.getID)
	}
}
