// Package domain defines domain errors and classifies failures.
package domain

import "errors"

var (
	// ErrEventIDRequired indicates that an event ID is required.
	ErrEventIDRequired = errors.New("event id is required")
	// ErrEventTypeRequired indicates that an event type is required.
	ErrEventTypeRequired = errors.New("event type is required")
	// ErrTenantIDRequired indicates that a tenant ID is required.
	ErrTenantIDRequired = errors.New("tenant id is required")
	// ErrPartitionKeyRequired indicates that a partition key is required.
	ErrPartitionKeyRequired = errors.New("partition key is required")
	// ErrOccurredAtRequired indicates that an event timestamp is required.
	ErrOccurredAtRequired = errors.New("occurred at is required")
	// ErrInvalidEventEnvelope indicates that an event envelope is invalid.
	ErrInvalidEventEnvelope = errors.New("invalid event envelope")

	// ErrRuleIDRequired indicates that a rule ID is required.
	ErrRuleIDRequired = errors.New("rule id is required")
	// ErrRevisionRequired indicates that a revision is required.
	ErrRevisionRequired = errors.New("revision is required")
	// ErrRuleSourceRequired indicates that a rule source is required.
	ErrRuleSourceRequired = errors.New("rule source is required")
	// ErrInvalidMatchMode indicates an invalid match mode was provided.
	ErrInvalidMatchMode = errors.New("invalid match mode")
	// ErrInvalidOperator indicates an invalid operator.
	ErrInvalidOperator = errors.New("invalid operator")
	// ErrInvalidPredicate indicates an invalid predicate.
	ErrInvalidPredicate = errors.New("invalid predicate")
	// ErrInvalidAction indicates an invalid action.
	ErrInvalidAction = errors.New("invalid action")
	// ErrDuplicateRuleID indicates a duplicate rule ID.
	ErrDuplicateRuleID = errors.New("duplicate rule id")
	// ErrAmbiguousPriority indicates an ambiguous priority.
	ErrAmbiguousPriority = errors.New("ambiguous priority")
	// ErrUnknownDestination indicates an unknown destination.
	ErrUnknownDestination = errors.New("unknown destination")
	// ErrDestinationNotAllowed indicates a destination is not allowed.
	ErrDestinationNotAllowed = errors.New("destination not allowed")
	// ErrRuleExceedsLimits indicates a rule exceeds limits.
	ErrRuleExceedsLimits = errors.New("rule exceeds limits")
	// ErrInvalidRuleSchema indicates an invalid rule schema.
	ErrInvalidRuleSchema = errors.New("invalid rule schema")
	// ErrRuleNotFound indicates a rule was not found.
	ErrRuleNotFound = errors.New("rule not found")
	// ErrRuleNotActive indicates a rule is not active.
	ErrRuleNotActive = errors.New("rule not active")
	// ErrRuleNotReady indicates a rule is not ready.
	ErrRuleNotReady = errors.New("rule not ready, retry later")

	// ErrEffectIDRequired indicates an effect ID is required.
	ErrEffectIDRequired = errors.New("effect id is required")
	// ErrExecutionIDRequired indicates an execution ID is required.
	ErrExecutionIDRequired = errors.New("execution id is required")
	// ErrInvalidEffectType indicates an invalid effect type.
	ErrInvalidEffectType = errors.New("invalid effect type")
	// ErrEffectNotFound indicates an effect was not found.
	ErrEffectNotFound = errors.New("effect not found")
	// ErrMaxAttemptsExceeded indicates max attempts were exceeded.
	ErrMaxAttemptsExceeded = errors.New("max attempts exceeded")
	// ErrOutboxClaimLost indicates an outbox claim was lost.
	ErrOutboxClaimLost = errors.New("outbox claim lost")

	// ErrExecutionNotFound indicates an execution was not found.
	ErrExecutionNotFound = errors.New("execution not found")
	// ErrInvalidExecutionStatus indicates an invalid execution status.
	ErrInvalidExecutionStatus = errors.New("invalid execution status")
	// ErrExecutionAlreadyExists indicates an execution already exists.
	ErrExecutionAlreadyExists = errors.New("execution already exists")

	// ErrInboxEntryNotFound indicates an inbox entry was not found.
	ErrInboxEntryNotFound = errors.New("inbox entry not found")
	// ErrInboxDuplicate indicates a duplicate event ID.
	ErrInboxDuplicate = errors.New("duplicate event id")
	// ErrInboxInvalidStatus indicates an invalid inbox status.
	ErrInboxInvalidStatus = errors.New("invalid inbox status")

	// ErrLeaseNotFound indicates a lease was not found.
	ErrLeaseNotFound = errors.New("lease not found")
	// ErrLeaseExpired indicates a lease has expired.
	ErrLeaseExpired = errors.New("lease expired")
	// ErrLeaseOwnedByOther indicates a lease is owned by another worker.
	ErrLeaseOwnedByOther = errors.New("lease owned by another worker")
	// ErrFencingTokenMismatch indicates a fencing token mismatch.
	ErrFencingTokenMismatch = errors.New("fencing token mismatch")

	// ErrQuarantineNotFound indicates a quarantine entry was not found.
	ErrQuarantineNotFound = errors.New("quarantine entry not found")
	// ErrQuarantineReplayFailed indicates a quarantine replay failed.
	ErrQuarantineReplayFailed = errors.New("quarantine replay failed")

	// ErrActivationNotFound indicates an activation was not found.
	ErrActivationNotFound = errors.New("activation not found")
	// ErrActivationConflict indicates an activation conflict.
	ErrActivationConflict = errors.New("activation conflict")

	// ErrWorkflowNotFound indicates a workflow was not found.
	ErrWorkflowNotFound = errors.New("workflow not found")
	// ErrWorkflowConflict indicates a workflow version conflict.
	ErrWorkflowConflict = errors.New("workflow version conflict")
	// ErrWorkflowInvalidTransition indicates an invalid workflow state transition.
	ErrWorkflowInvalidTransition = errors.New("invalid workflow state transition")
	// ErrBatchProcessingUnavailable indicates batch processing is not configured.
	ErrBatchProcessingUnavailable = errors.New("batch processing is not configured")
	// ErrDomainServiceConflict indicates a tenant domain or service already exists.
	ErrDomainServiceConflict = errors.New("tenant domain or service already exists")

	// ErrBrokerFetchFailed indicates a broker fetch failed.
	ErrBrokerFetchFailed = errors.New("broker fetch failed")
	// ErrBrokerAckFailed indicates a broker ack failed.
	ErrBrokerAckFailed = errors.New("broker ack failed")
	// ErrBrokerRetryFailed indicates a broker retry failed.
	ErrBrokerRetryFailed = errors.New("broker retry failed")

	// ErrEffectSendFailed indicates an effect send failed.
	ErrEffectSendFailed = errors.New("effect send failed")
	// ErrEffectDestinationDown indicates an effect destination is down.
	ErrEffectDestinationDown = errors.New("effect destination down")

	// ErrDatabaseError indicates a database error.
	ErrDatabaseError = errors.New("database error")
	// ErrTransactionFailed indicates a transaction failed.
	ErrTransactionFailed = errors.New("transaction failed")
	// ErrSerializationFailed indicates serialization failed.
	ErrSerializationFailed = errors.New("serialization failed")
	// ErrDeserializationFailed indicates deserialization failed.
	ErrDeserializationFailed = errors.New("deserialization failed")

	// ErrUnauthorized indicates unauthorized access.
	ErrUnauthorized = errors.New("unauthorized")
	// ErrForbidden indicates forbidden access.
	ErrForbidden = errors.New("forbidden")

	// ErrEvaluationTimeout indicates an evaluation timeout.
	ErrEvaluationTimeout = errors.New("evaluation timeout")
	// ErrEvaluationError indicates an evaluation error.
	ErrEvaluationError = errors.New("evaluation error")
	// ErrFactSnapshotRequired indicates a fact snapshot is required.
	ErrFactSnapshotRequired = errors.New("fact snapshot required")
	// ErrInvalidFactPath indicates an invalid fact path.
	ErrInvalidFactPath = errors.New("invalid fact path")
)

// ErrorClass contains error class settings.
type ErrorClass string

const (
	// ErrorClassValidation indicates a validation error.
	ErrorClassValidation ErrorClass = "validation"
	// ErrorClassRule indicates a rule error.
	ErrorClassRule ErrorClass = "rule"
	// ErrorClassEvaluation indicates an evaluation error.
	ErrorClassEvaluation ErrorClass = "evaluation"
	// ErrorClassExecution indicates an execution error.
	ErrorClassExecution ErrorClass = "execution"
	// ErrorClassEffect indicates an effect error.
	ErrorClassEffect ErrorClass = "effect"
	// ErrorClassBroker indicates a broker error.
	ErrorClassBroker ErrorClass = "broker"
	// ErrorClassDatabase indicates a database error.
	ErrorClassDatabase ErrorClass = "database"
	// ErrorClassAuthorization indicates an authorization error.
	ErrorClassAuthorization ErrorClass = "authorization"
	// ErrorClassQuarantine indicates a quarantine error.
	ErrorClassQuarantine ErrorClass = "quarantine"
	// ErrorClassInternal indicates an internal error.
	ErrorClassInternal ErrorClass = "internal"
)

// ClassifyError provides the classify error.
func ClassifyError(err error) ErrorClass {
	if err == nil {
		return ErrorClassInternal
	}
	switch {
	case errors.Is(err, ErrEventIDRequired),
		errors.Is(err, ErrEventTypeRequired),
		errors.Is(err, ErrTenantIDRequired),
		errors.Is(err, ErrPartitionKeyRequired),
		errors.Is(err, ErrOccurredAtRequired),
		errors.Is(err, ErrInvalidEventEnvelope):
		return ErrorClassValidation
	case errors.Is(err, ErrRuleIDRequired),
		errors.Is(err, ErrRevisionRequired),
		errors.Is(err, ErrRuleSourceRequired),
		errors.Is(err, ErrInvalidMatchMode),
		errors.Is(err, ErrInvalidOperator),
		errors.Is(err, ErrInvalidPredicate),
		errors.Is(err, ErrInvalidAction),
		errors.Is(err, ErrDuplicateRuleID),
		errors.Is(err, ErrAmbiguousPriority),
		errors.Is(err, ErrUnknownDestination),
		errors.Is(err, ErrDestinationNotAllowed),
		errors.Is(err, ErrRuleExceedsLimits),
		errors.Is(err, ErrInvalidRuleSchema),
		errors.Is(err, ErrRuleNotFound),
		errors.Is(err, ErrRuleNotActive):
		return ErrorClassRule
	case errors.Is(err, ErrRuleNotReady):
		return ErrorClassBroker
	case errors.Is(err, ErrEvaluationTimeout),
		errors.Is(err, ErrEvaluationError),
		errors.Is(err, ErrFactSnapshotRequired),
		errors.Is(err, ErrInvalidFactPath):
		return ErrorClassEvaluation
	case errors.Is(err, ErrExecutionNotFound),
		errors.Is(err, ErrInvalidExecutionStatus),
		errors.Is(err, ErrExecutionAlreadyExists):
		return ErrorClassExecution
	case errors.Is(err, ErrEffectIDRequired),
		errors.Is(err, ErrExecutionIDRequired),
		errors.Is(err, ErrInvalidEffectType),
		errors.Is(err, ErrEffectNotFound),
		errors.Is(err, ErrMaxAttemptsExceeded),
		errors.Is(err, ErrOutboxClaimLost):
		return ErrorClassEffect
	case errors.Is(err, ErrBrokerFetchFailed),
		errors.Is(err, ErrBrokerAckFailed),
		errors.Is(err, ErrBrokerRetryFailed):
		return ErrorClassBroker
	case errors.Is(err, ErrInboxEntryNotFound),
		errors.Is(err, ErrInboxDuplicate),
		errors.Is(err, ErrInboxInvalidStatus),
		errors.Is(err, ErrLeaseNotFound),
		errors.Is(err, ErrLeaseExpired),
		errors.Is(err, ErrLeaseOwnedByOther),
		errors.Is(err, ErrFencingTokenMismatch),
		errors.Is(err, ErrDatabaseError),
		errors.Is(err, ErrTransactionFailed):
		return ErrorClassDatabase
	case errors.Is(err, ErrUnauthorized),
		errors.Is(err, ErrForbidden):
		return ErrorClassAuthorization
	case errors.Is(err, ErrQuarantineNotFound),
		errors.Is(err, ErrQuarantineReplayFailed):
		return ErrorClassQuarantine
	default:
		return ErrorClassInternal
	}
}

// IsRetryable provides the is retryable.
func IsRetryable(err error) bool {
	cls := ClassifyError(err)
	return cls == ErrorClassBroker || cls == ErrorClassEffect || cls == ErrorClassDatabase
}

// IsPermanent provides the is permanent.
func IsPermanent(err error) bool {
	cls := ClassifyError(err)
	return cls == ErrorClassValidation ||
		cls == ErrorClassRule ||
		cls == ErrorClassEvaluation ||
		cls == ErrorClassAuthorization
}
