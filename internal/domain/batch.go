package domain

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// BatchMode selects how committed inbox entries are grouped into batch runs.
type BatchMode string

const (
	// BatchModeNone keeps the original behavior: every event is evaluated alone.
	BatchModeNone BatchMode = "none"
	// BatchModeAccumulate forms a batch once a partition reaches MaxBatch.
	BatchModeAccumulate BatchMode = "accumulate"
	// BatchModeWindow forms a batch once the oldest entry in a partition is
	// older than the configured window, the way a scheduler would fire.
	BatchModeWindow BatchMode = "window"
)

// BatchConfig is the runtime policy for the batch scheduler.
type BatchConfig struct {
	Mode     BatchMode
	MaxBatch int
	Window   time.Duration
}

// BatchRun records one synthesized batch evaluation.
type BatchRun struct {
	BatchID      string     `json:"batch_id"`
	TenantID     string     `json:"tenant_id"`
	PartitionKey string     `json:"partition_key"`
	RuleSet      string     `json:"rule_set"`
	Status       string     `json:"status"`
	MemberCount  int        `json:"member_count"`
	ExecutionID  string     `json:"execution_id,omitempty"`
	DecisionHash string     `json:"decision_hash,omitempty"`
	WindowFrom   *time.Time `json:"window_from,omitempty"`
	WindowTo     *time.Time `json:"window_to,omitempty"`
	CreatedAt    time.Time  `json:"created_at"`
}

const (
	// BatchRunStatusFormed means a batch run has been formed but not completed.
	BatchRunStatusFormed = "formed"
	// BatchRunStatusCompleted means a batch run has completed.
	BatchRunStatusCompleted = "completed"
)

// BatchMember is one source event carried inside a synthesized batch envelope.
type BatchMember struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// BatchData is the JSON payload of a synthesized batch event.
type BatchData struct {
	BatchID      string        `json:"batch_id"`
	TenantID     string        `json:"tenant_id"`
	PartitionKey string        `json:"partition_key"`
	RuleSet      string        `json:"rule_set"`
	WindowFrom   time.Time     `json:"window_from"`
	WindowTo     time.Time     `json:"window_to"`
	Events       []BatchMember `json:"events"`
}

// BatchRuleSet maps an event rule set onto the rule set that evaluates batches.
func BatchRuleSet(ruleSet string) string {
	return ruleSet + ".batch"
}

// ComputeBatchID derives a deterministic batch identity from the member event
// ids, so a retried tick after a crash reuses the same inbox key.
func ComputeBatchID(tenantID, partitionKey, ruleSet string, eventIDs []string) string {
	ids := append([]string(nil), eventIDs...)
	sort.Strings(ids)
	raw := tenantID + "|" + partitionKey + "|" + ruleSet + "|" + strings.Join(ids, ",")
	h := sha256.Sum256([]byte(raw))
	return fmt.Sprintf("%x", h)
}
