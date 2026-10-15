package postgres

import (
	"fmt"

	"github.com/flowrulz/flow-rulz/internal/ports"
)

// NewEventTxRepositories constructs the narrow repository bundle used by event
// processing. The adapter owns the concrete transaction type assertion and
// driver-specific repository construction.
func NewEventTxRepositories(tx ports.Transaction) (ports.EventTxRepos, error) {
	sqlTx, ok := tx.(*Tx)
	if !ok {
		return ports.EventTxRepos{}, fmt.Errorf("postgres transaction has unexpected type %T", tx)
	}

	q := NewTxQuerier(sqlTx)
	return ports.EventTxRepos{
		Inbox:          NewInboxRepository(q),
		Activations:    NewActivationRepository(q),
		RuleRepo:       NewRuleRepository(q),
		Executions:     NewExecutionRepository(q),
		Outbox:         NewOutboxRepository(q),
		ShardLeases:    NewShardLeaseRepository(q),
		Subscribers:    NewDomainServiceRepository(q),
		Bindings:       NewBindingRepository(q),
	}, nil
}

// NewRuleTxRepositories builds only the repositories required for atomic rule activation.
func NewRuleTxRepositories(tx ports.Transaction) (ports.RuleTxRepos, error) {
	sqlTx, ok := tx.(*Tx)
	if !ok {
		return ports.RuleTxRepos{}, fmt.Errorf("postgres transaction has unexpected type %T", tx)
	}

	q := NewTxQuerier(sqlTx)
	return ports.RuleTxRepos{
		RuleRepo:    NewRuleRepository(q),
		Activations: NewActivationRepository(q),
	}, nil
}
