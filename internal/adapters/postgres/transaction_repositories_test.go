package postgres

import (
	"testing"
)

func TestEventTxRepositoriesRejectsForeignTransaction(t *testing.T) {
	if _, err := NewEventTxRepositories(nil); err == nil {
		t.Fatal("NewEventTxRepositories(nil) error = nil, want transaction type error")
	}
}

func TestRuleTxRepositoriesRejectsForeignTransaction(t *testing.T) {
	if _, err := NewRuleTxRepositories(nil); err == nil {
		t.Fatal("NewRuleTxRepositories(nil) error = nil, want transaction type error")
	}
}
