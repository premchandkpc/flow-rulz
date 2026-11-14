package postgres

import (
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestSameWorkflowCreateIdentity(t *testing.T) {
	tests := workflowIdentityCases()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sameWorkflowCreateIdentity(tt.tenant, tt.typeName, tt.requested); got != tt.want {
				t.Fatalf("sameWorkflowCreateIdentity() = %v, want %v", got, tt.want)
			}
		})
	}
}

type workflowIdentityCase struct {
	name      string
	tenant    string
	typeName  string
	requested *domain.WorkflowInstance
	want      bool
}

func workflowIdentityCases() []workflowIdentityCase {
	requested := &domain.WorkflowInstance{TenantID: "tenant-a", WorkflowType: "orders"}
	return []workflowIdentityCase{
		{name: "same identity", tenant: "tenant-a", typeName: "orders", requested: requested, want: true},
		{name: "different tenant", tenant: "tenant-b", typeName: "orders", requested: requested, want: false},
		{name: "different workflow type", tenant: "tenant-a", typeName: "payments", requested: requested, want: false},
		{name: "nil request", tenant: "tenant-a", typeName: "orders", requested: nil, want: false},
	}
}
