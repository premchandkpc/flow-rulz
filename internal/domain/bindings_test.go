package domain

import "testing"

func TestContractBindingsAllowOneContractToFanOutToMultipleRulesets(t *testing.T) {
	ref := &ContractRef{Name: "orders.created", Version: "v1", SchemaHash: "sha256:abc"}
	bindings := []ContractBinding{
		{TenantID: "tenant-a", ContractName: "orders.created", ContractVersion: "v1", ContractHash: "sha256:abc", RuleSet: "fraud-check"},
		{TenantID: "tenant-a", ContractName: "orders.created", ContractVersion: "v1", ContractHash: "sha256:abc", RuleSet: "inventory-check"},
	}
	for _, binding := range bindings {
		if !binding.MatchesContract(ref) {
			t.Fatalf("binding for ruleset %q should match the same immutable contract", binding.RuleSet)
		}
	}
	if bindings[0].RuleSet == bindings[1].RuleSet {
		t.Fatal("test requires independent ruleset identities")
	}
}

func TestContractBindingRequiresExactVersionAndSchemaHash(t *testing.T) {
	binding := ContractBinding{
		ContractName:    "orders.created",
		ContractVersion: "v1",
		ContractHash:    "sha256:abc",
		RuleSet:         "orders-created",
	}
	tests := []struct {
		name string
		ref  ContractRef
	}{
		{name: "different contract", ref: ContractRef{Name: "orders.updated", Version: "v1", SchemaHash: "sha256:abc"}},
		{name: "different version", ref: ContractRef{Name: "orders.created", Version: "v2", SchemaHash: "sha256:abc"}},
		{name: "different hash", ref: ContractRef{Name: "orders.created", Version: "v1", SchemaHash: "sha256:def"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if binding.MatchesContract(&tt.ref) {
				t.Fatal("binding unexpectedly matched a different immutable contract")
			}
		})
	}
	if binding.MatchesContract(nil) {
		t.Fatal("nil contract must never match a binding")
	}
}
