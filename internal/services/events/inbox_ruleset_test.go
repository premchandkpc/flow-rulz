package events

import (
	"context"
	"errors"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type inboxKey struct{ tenant, event, ruleSet string }

type scopedInbox struct {
	ports.InboxRepository
	entries map[inboxKey]*domain.InboxEntry
}

func (r *scopedInbox) GetByRuleSet(_ context.Context, tenant, event, ruleSet string) (*domain.InboxEntry, error) {
	return r.entries[inboxKey{tenant, event, ruleSet}], nil
}

func (r *scopedInbox) Insert(_ context.Context, entry *domain.InboxEntry) (bool, error) {
	key := inboxKey{entry.TenantID, entry.EventID, entry.RuleSet}
	if r.entries[key] != nil {
		return false, nil
	}
	r.entries[key] = entry
	return true, nil
}

func (r *scopedInbox) MarkCommitted(_ context.Context, tenant, event, ruleSet, execution string) error {
	entry := r.entries[inboxKey{tenant, event, ruleSet}]
	if entry == nil || entry.Status != domain.InboxStatusProcessing {
		return domain.ErrInboxInvalidStatus
	}
	entry.Status, entry.ExecutionID = domain.InboxStatusCommitted, execution
	return nil
}

type scopedExecutions struct {
	ports.ExecutionRepository
	entries map[string]*domain.Execution
}

func (r *scopedExecutions) Save(_ context.Context, entry *domain.Execution) error {
	r.entries[entry.ID] = entry
	return nil
}

func (r *scopedExecutions) Get(_ context.Context, id string) (*domain.Execution, error) {
	return r.entries[id], nil
}

func multiRulesetFixture() (*Service, ports.EventTxRepos, []rulesetRevision) {
	service, _, _, _, subscribers, _ := setupEventService(true)
	repos := ports.EventTxRepos{
		Inbox:      &scopedInbox{entries: make(map[inboxKey]*domain.InboxEntry)},
		Executions: &scopedExecutions{entries: make(map[string]*domain.Execution)},
		Outbox:     &outboxStub{}, Subscribers: subscribers,
	}
	return service, repos, []rulesetRevision{
		{RuleSet: "fraud", Revision: &domain.RuleRevision{Revision: 1}},
		{RuleSet: "payment", Revision: &domain.RuleRevision{Revision: 1}},
	}
}

func assertRulesetInbox(t *testing.T, repos ports.EventTxRepos, ruleSet string) {
	t.Helper()
	entry, err := repos.Inbox.GetByRuleSet(context.Background(), "tenant-a", "event-1", ruleSet)
	if err != nil || entry == nil || entry.Status != domain.InboxStatusCommitted {
		t.Fatalf("inbox for %s: entry=%#v error=%v", ruleSet, entry, err)
	}
	execution, err := repos.Executions.Get(context.Background(), entry.ExecutionID)
	if err != nil || execution == nil || execution.RuleSet != ruleSet {
		t.Fatalf("execution for %s: execution=%#v error=%v", ruleSet, execution, err)
	}
}

func TestMultiRulesetExecutionAndRedelivery(t *testing.T) {
	service, repos, revisions := multiRulesetFixture()
	result, err := service.processRulesets(context.Background(), repos, testEnvelope(), revisions)
	if err != nil || !result.processed {
		t.Fatalf("initial processing: result=%#v error=%v", result, err)
	}
	assertRulesetInbox(t, repos, "fraud")
	assertRulesetInbox(t, repos, "payment")
	result, err = service.processRulesets(context.Background(), repos, testEnvelope(), revisions)
	if err != nil || result.processed || len(repos.Executions.(*scopedExecutions).entries) != 2 {
		t.Fatalf("redelivery: result=%#v error=%v", result, err)
	}
	if len(repos.Outbox.(*outboxStub).inserted) != 1 || repos.Subscribers.(*subscriberRepositoryStub).lookups != 1 {
		t.Fatal("redelivery or multiple rulesets duplicated subscriber routing")
	}
}

func TestLegacyInboxWithoutRuleSetFailsClosed(t *testing.T) {
	service, repos, revisions := multiRulesetFixture()
	inbox := repos.Inbox.(*scopedInbox)
	inbox.entries[inboxKey{"tenant-a", "event-1", ""}] = &domain.InboxEntry{Status: domain.InboxStatusCommitted}
	_, err := service.processRulesets(context.Background(), repos, testEnvelope(), revisions)
	if !errors.Is(err, domain.ErrInboxDuplicate) || len(repos.Executions.(*scopedExecutions).entries) != 0 {
		t.Fatalf("legacy inbox must block re-execution: %v", err)
	}
}

func TestInboxRejectsExecutionFromAnotherTenant(t *testing.T) {
	service, repos, revisions := multiRulesetFixture()
	inbox := repos.Inbox.(*scopedInbox)
	inbox.entries[inboxKey{"tenant-a", "event-1", "fraud"}] = &domain.InboxEntry{
		TenantID: "tenant-a", EventID: "event-1", RuleSet: "fraud", Status: domain.InboxStatusCommitted, ExecutionID: "wrong",
	}
	repos.Executions.(*scopedExecutions).entries["wrong"] = &domain.Execution{
		ID: "wrong", TenantID: "tenant-b", EventID: "event-1", RuleSet: "fraud",
	}
	_, err := service.processRulesets(context.Background(), repos, testEnvelope(), revisions)
	if !errors.Is(err, domain.ErrInboxDuplicate) {
		t.Fatalf("cross-tenant execution should block acknowledgement: %v", err)
	}
}
