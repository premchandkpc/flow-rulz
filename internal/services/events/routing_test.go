package events

import (
	"context"
	"errors"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type routingBindings struct {
	bindingStub
	bindings []*domain.ContractBinding
}

func (r routingBindings) ListByContract(context.Context, string, string, string) ([]*domain.ContractBinding, error) {
	return r.bindings, nil
}

func routingEnvelope() *domain.EventEnvelope {
	envelope := testEnvelope()
	envelope.Contract = &domain.ContractRef{Name: "orders", Version: "1", SchemaHash: "hash"}
	return envelope
}

func resolveRoutingFixture(t *testing.T, bindings ...*domain.ContractBinding) []rulesetRevision {
	t.Helper()
	service, _, _, _, _, _ := setupEventService(true)
	repos := ports.EventTxRepos{
		Bindings:    routingBindings{bindings: bindings},
		Activations: activationStub{active: true}, RuleRepo: ruleRepositoryStub{},
	}
	revisions, err := service.resolveRulesets(context.Background(), repos, routingEnvelope())
	if err != nil {
		t.Fatal(err)
	}
	return revisions
}

func TestResolveRulesetsOrdersMatchingBindingsByPriority(t *testing.T) {
	revisions := resolveRoutingFixture(t,
		&domain.ContractBinding{RuleSet: "low", ContractHash: "hash", Priority: 1},
		&domain.ContractBinding{RuleSet: "wrong-hash", ContractHash: "other", Priority: 100},
		&domain.ContractBinding{RuleSet: "high", ContractHash: "hash", Priority: 10},
	)
	if len(revisions) != 2 || revisions[0].RuleSet != "high" || revisions[1].RuleSet != "low" {
		t.Fatalf("revisions = %#v, want high then low", revisions)
	}
}

func TestResolveRulesetsFallsBackWhenBindingsDoNotMatch(t *testing.T) {
	revisions := resolveRoutingFixture(t, &domain.ContractBinding{RuleSet: "ignored", ContractHash: "other"})
	if len(revisions) != 1 || revisions[0].RuleSet != testEnvelope().Type {
		t.Fatalf("revisions = %#v, want event-type routing", revisions)
	}
}

type inputContractCase struct {
	name             string
	required, actual *domain.ContractRef
	want             error
}

func inputContractCases() []inputContractCase {
	ref := routingEnvelope().Contract
	return []inputContractCase{
		{"unbound", nil, nil, nil},
		{"unbound with contract", nil, ref, nil},
		{"exact match", ref, ref, nil},
		{"missing", ref, nil, domain.ErrContractRequired},
		{"name", ref, &domain.ContractRef{Name: "other", Version: "1", SchemaHash: "hash"}, domain.ErrContractMismatch},
		{"version", ref, &domain.ContractRef{Name: "orders", Version: "2", SchemaHash: "hash"}, domain.ErrContractMismatch},
		{"hash", ref, &domain.ContractRef{Name: "orders", Version: "1", SchemaHash: "other"}, domain.ErrContractMismatch},
	}
}

func TestValidateInputContractPreservesErrorClassification(t *testing.T) {
	for _, test := range inputContractCases() {
		t.Run(test.name, func(t *testing.T) {
			revision := &domain.RuleRevision{RuleID: "rule", InputContract: test.required}
			if err := validateInputContract(revision, test.actual); !errors.Is(err, test.want) {
				t.Fatalf("validateInputContract() = %v, want %v", err, test.want)
			}
			if got := matchesInputContract(test.required, test.actual); got != (test.want == nil) {
				t.Fatalf("matchesInputContract() = %t, want %t", got, test.want == nil)
			}
		})
	}
}
