package domain

import (
	"time"
)

// BindingActivationPolicy defines how a binding activates its ruleset.
type BindingActivationPolicy string

const (
	// BindingPolicyAutomatic activates the ruleset when an active revision exists.
	BindingPolicyAutomatic BindingActivationPolicy = "automatic"
	// BindingPolicyManual requires explicit activation of the ruleset.
	BindingPolicyManual BindingActivationPolicy = "manual"
)

// ContractBinding connects an event contract to an eligible ruleset.
// Multiple bindings for the same contract enable fan-out to multiple rulesets.
type ContractBinding struct {
	ID               string                    `json:"id"`
	TenantID         string                    `json:"tenant_id"`
	ContractName     string                    `json:"contract_name"`
	ContractVersion  string                    `json:"contract_version"`
	ContractHash     string                    `json:"contract_hash"`
	RuleSet          string                    `json:"rule_set"`
	Priority         int                       `json:"priority"`
	ActivationPolicy BindingActivationPolicy   `json:"activation_policy"`
	CreatedAt        time.Time                 `json:"created_at"`
	UpdatedAt        time.Time                 `json:"updated_at"`
}

// MatchesContract reports whether this binding matches the given contract reference.
func (b *ContractBinding) MatchesContract(ref *ContractRef) bool {
	if ref == nil {
		return false
	}
	return b.ContractName == ref.Name &&
		b.ContractVersion == ref.Version &&
		b.ContractHash == ref.SchemaHash
}