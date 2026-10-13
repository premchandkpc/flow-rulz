package mongo

import (
	"strings"
	"testing"
)

func TestBatchRunListFilterRequiresTenant(t *testing.T) {
	for _, tenantID := range []string{"", " ", "\t"} {
		if _, err := batchRunListFilter(tenantID, "orders"); err == nil {
			t.Fatalf("batchRunListFilter(%q) expected tenant validation error", tenantID)
		}
	}
}

func TestBatchRunListFilterAlwaysScopesTenant(t *testing.T) {
	filter, err := batchRunListFilter("tenant-a", "")
	if err != nil {
		t.Fatalf("batchRunListFilter returned error: %v", err)
	}
	if got := filter["tenant_id"]; got != "tenant-a" {
		t.Fatalf("tenant_id = %#v, want tenant-a", got)
	}
	if _, exists := filter["rule_set"]; exists {
		t.Fatal("rule_set filter should be omitted when empty")
	}

	filter, err = batchRunListFilter("tenant-b", "payments")
	if err != nil {
		t.Fatalf("batchRunListFilter returned error: %v", err)
	}
	if got := filter["tenant_id"]; got != "tenant-b" {
		t.Fatalf("tenant_id = %#v, want tenant-b", got)
	}
	if got := filter["rule_set"]; got != "payments" {
		t.Fatalf("rule_set = %#v, want payments", got)
	}
	if strings.TrimSpace(filter["tenant_id"].(string)) == "" {
		t.Fatal("tenant scope must never be blank")
	}
}
