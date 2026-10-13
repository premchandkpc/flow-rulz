package domain

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsPermanentClassifiesDeterministicEvaluationFailures(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "evaluation error", err: ErrEvaluationError, want: true},
		{name: "wrapped evaluation error", err: fmt.Errorf("evaluate rule: %w", ErrEvaluationError), want: true},
		{name: "missing fact snapshot", err: ErrFactSnapshotRequired, want: true},
		{name: "invalid fact path", err: ErrInvalidFactPath, want: true},
		{name: "evaluation timeout is quarantined", err: ErrEvaluationTimeout, want: true},
		{name: "database error remains retryable", err: ErrDatabaseError, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := IsPermanent(tt.err); got != tt.want {
				t.Fatalf("IsPermanent(%v) = %t, want %t", tt.err, got, tt.want)
			}
		})
	}
}

func TestRuleNotReadyIsRetriedUntilActivation(t *testing.T) {
	err := fmt.Errorf("load active revision: %w", ErrRuleNotReady)
	if IsPermanent(err) {
		t.Fatalf("IsPermanent(%v) = true, want false", err)
	}
	if !IsRetryable(err) {
		t.Fatalf("IsRetryable(%v) = false, want true", err)
	}
}

func TestIsPermanentDoesNotTreatUnknownErrorsAsPermanent(t *testing.T) {
	if IsPermanent(errors.New("unknown transient failure")) {
		t.Fatal("IsPermanent(unknown error) = true, want false")
	}
}
