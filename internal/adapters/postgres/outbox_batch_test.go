package postgres

import (
	"strings"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
)

func TestBuildOutboxInsertBatchesParameterizedRows(t *testing.T) {
	query, args, err := buildOutboxInsert([]domain.OutboxEffect{
		{ID: "effect-one", TenantID: "tenant"},
		{ID: "effect-two", TenantID: "tenant"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 28 || !strings.Contains(query, "$1") || !strings.Contains(query, "$28") {
		t.Fatalf("unexpected placeholders or args: %d %s", len(args), query)
	}
	if strings.Contains(query, "effect-one") {
		t.Fatal("effect data must be passed as SQL arguments")
	}
}

func TestBuildOutboxInsertRejectsMissingTenant(t *testing.T) {
	_, _, err := buildOutboxInsert([]domain.OutboxEffect{{ID: "effect-one"}})
	if err == nil || !strings.Contains(err.Error(), "tenant_id") {
		t.Fatalf("expected tenant validation error: %v", err)
	}
}
