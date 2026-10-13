// Package rules provides rule compilation.
package rules

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

var _ ports.RuleCompiler = (*Compiler)(nil)

const (
	// CompilerVersion is the version of the rule compiler.
	CompilerVersion = "1.0.0"
)

// Limits configures compiler validation limits.
type Limits struct {
	MaxRulesPerSet   int
	MaxPredicates    int
	MaxNestingDepth  int
	MaxActionsPerSet int
	MaxPayloadBytes  int
	MaxRuleIDLen     int
}

// DefaultLimits returns the default compiler limits.
func DefaultLimits() Limits {
	return Limits{
		MaxRulesPerSet:   100,
		MaxPredicates:    200,
		MaxNestingDepth:  10,
		MaxActionsPerSet: 10,
		MaxPayloadBytes:  256 * 1024,
		MaxRuleIDLen:     128,
	}
}

// Compiler is the PostgreSQL persistence adapter.
type Compiler struct {
	limits Limits
}

// NewCompiler creates a new Compiler with the given limits.
func NewCompiler(limits Limits) *Compiler {
	return &Compiler{limits: limits}
}

// Compile parses and validates a rule set source, returning a compiled RuleRevision.
func (c *Compiler) Compile(source json.RawMessage) (*domain.RuleRevision, error) {
	rawSet, err := c.parseRuleSet(source)
	if err != nil {
		return nil, err
	}

	rawRules, err := c.validateRuleSet(rawSet)
	if err != nil {
		return nil, err
	}

	compiled, err := c.compileRules(rawRules)
	if err != nil {
		return nil, err
	}

	if rawSet.Mode == string(domain.MatchModeFirstMatch) {
		for i := 0; i < len(compiled)-1; i++ {
			if len(compiled[i].Otherwise) > 0 {
				return nil, fmt.Errorf("first_match permits otherwise only on the lowest-priority rule: %w", domain.ErrInvalidAction)
			}
		}
	}

	revision := &domain.RuleRevision{
		RuleID:          rawSet.RuleSet,
		Revision:        rawSet.Revision,
		ContentHash:     domain.ComputeSourceHash(source),
		MatchMode:       domain.MatchMode(rawSet.Mode),
		Compiled:        compiled,
		Source:          source,
		CompilerVersion: CompilerVersion,
		InputContract:   rawSet.InputContract,
		CreatedAt:       domain.SystemClock{}.Now(),
	}
	return revision, nil
}

// rawRuleSet represents the raw JSON structure of a rule set.
type rawRuleSet struct {
	RuleSet       string              `json:"rule_set"`
	Revision      int64               `json:"revision"`
	Mode          string              `json:"mode"`
	Rules         json.RawMessage     `json:"rules"`
	InputContract *domain.ContractRef `json:"input_contract,omitempty"`
}

// rawRule represents a single raw rule from the JSON input.
type rawRule struct {
	ID          string           `json:"id"`
	Name        string           `json:"name,omitempty"`
	Description string           `json:"description,omitempty"`
	Tags        []string         `json:"tags,omitempty"`
	Priority    int              `json:"priority"`
	When        domain.Predicate `json:"when"`
	Then        []domain.Action  `json:"then"`
	Otherwise   []domain.Action  `json:"otherwise,omitempty"`
}

// parseRuleSet parses the raw JSON rule set into structured data.
func (c *Compiler) parseRuleSet(source json.RawMessage) (*rawRuleSet, error) {
	if len(source) == 0 {
		return nil, fmt.Errorf("empty rule source")
	}

	var rs rawRuleSet
	if err := json.Unmarshal(source, &rs); err != nil {
		return nil, fmt.Errorf("parse rule set: %w", err)
	}
	return &rs, nil
}

// validateRuleSet validates the rule set structure and rules.
func (c *Compiler) validateRuleSet(rs *rawRuleSet) ([]rawRule, error) {
	if err := validateRuleSetMetadata(rs); err != nil {
		return nil, err
	}
	rawRules, err := parseRawRules(rs.Rules)
	if err != nil {
		return nil, err
	}
	if err := c.validateRuleSetBudgets(rawRules); err != nil {
		return nil, err
	}
	return rawRules, nil
}

func validateRuleSetMetadata(rs *rawRuleSet) error {
	if rs.RuleSet == "" {
		return domain.ErrRuleIDRequired
	}
	if rs.Revision <= 0 {
		return domain.ErrRevisionRequired
	}
	if rs.Mode != "first_match" && rs.Mode != "all_matches" {
		return domain.ErrInvalidMatchMode
	}
	return nil
}

func parseRawRules(source json.RawMessage) ([]rawRule, error) {
	var rules []rawRule
	if err := json.Unmarshal(source, &rules); err != nil {
		return nil, fmt.Errorf("parse rules: %w", err)
	}
	return rules, nil
}

func (c *Compiler) validateRuleSetBudgets(rules []rawRule) error {
	if len(rules) == 0 || len(rules) > c.limits.MaxRulesPerSet {
		return domain.ErrRuleExceedsLimits
	}
	totalPredicates, totalActions := countRuleSetWork(rules)
	if totalPredicates > c.limits.MaxPredicates || totalActions > c.limits.MaxActionsPerSet {
		return domain.ErrRuleExceedsLimits
	}
	return nil
}

func countRuleSetWork(rules []rawRule) (predicates int, actions int) {
	for _, rule := range rules {
		predicates += countPredicates(rule.When)
		actions += len(rule.Then) + len(rule.Otherwise)
	}
	return predicates, actions
}

// compileRules compiles the validated raw rules into domain compiled rules.
func (c *Compiler) compileRules(rawRules []rawRule) ([]domain.CompiledRule, error) {
	seenIDs := make(map[string]bool)
	seenPriorities := make(map[int]bool)
	compiled := make([]domain.CompiledRule, 0, len(rawRules))
	for _, rule := range rawRules {
		compiledRule, err := c.compileRule(rule, seenIDs, seenPriorities)
		if err != nil {
			return nil, err
		}
		compiled = append(compiled, compiledRule)
	}
	sort.Slice(compiled, func(i, j int) bool {
		return compiled[i].Priority > compiled[j].Priority
	})
	return compiled, nil
}

func (c *Compiler) compileRule(
	rule rawRule,
	seenIDs map[string]bool,
	seenPriorities map[int]bool,
) (domain.CompiledRule, error) {
	if err := validateRuleIdentity(rule, c.limits, seenIDs, seenPriorities); err != nil {
		return domain.CompiledRule{}, err
	}
	if err := c.validateRuleActions(rule); err != nil {
		return domain.CompiledRule{}, err
	}
	return domain.CompiledRule{
		ID: rule.ID, Name: rule.Name, Description: rule.Description, Tags: rule.Tags,
		Priority: rule.Priority, When: rule.When, Then: rule.Then, Otherwise: rule.Otherwise,
	}, nil
}

func validateRuleIdentity(
	rule rawRule,
	limits Limits,
	seenIDs map[string]bool,
	seenPriorities map[int]bool,
) error {
	if rule.ID == "" {
		return domain.ErrRuleIDRequired
	}
	if len(rule.ID) > limits.MaxRuleIDLen {
		return domain.ErrRuleExceedsLimits
	}
	if seenIDs[rule.ID] {
		return domain.ErrDuplicateRuleID
	}
	seenIDs[rule.ID] = true
	if seenPriorities[rule.Priority] {
		return domain.ErrAmbiguousPriority
	}
	seenPriorities[rule.Priority] = true
	return nil
}

func (c *Compiler) validateRuleActions(rule rawRule) error {
	if err := c.validatePredicate(rule.When, 0); err != nil {
		return err
	}
	if countPredicates(rule.When) > c.limits.MaxPredicates {
		return domain.ErrRuleExceedsLimits
	}
	if len(rule.Then) == 0 || len(rule.Then) > c.limits.MaxActionsPerSet {
		return domain.ErrRuleExceedsLimits
	}
	for _, action := range rule.Then {
		if err := c.validateAction(action); err != nil {
			return err
		}
	}
	if len(rule.Otherwise) > c.limits.MaxActionsPerSet {
		return domain.ErrRuleExceedsLimits
	}
	for _, action := range rule.Otherwise {
		if err := c.validateAction(action); err != nil {
			return err
		}
	}
	return nil
}

func (c *Compiler) validatePredicate(p domain.Predicate, depth int) error {
	if depth > c.limits.MaxNestingDepth {
		return domain.ErrRuleExceedsLimits
	}
	hasLeaf := p.Path != ""
	hasComposite := p.All != nil || p.Any != nil || p.Not != nil
	if hasLeaf == hasComposite {
		return domain.ErrInvalidPredicate
	}
	if hasLeaf {
		if !domain.ValidOp(p.Op) {
			return domain.ErrInvalidOperator
		}
		return nil
	}
	if err := validateCompositePredicateShape(p); err != nil {
		return err
	}
	return c.validateCompositeChildren(p, depth)
}

func validateCompositePredicateShape(p domain.Predicate) error {
	compositeCount := 0
	if p.All != nil {
		compositeCount++
		if len(p.All) == 0 {
			return domain.ErrInvalidPredicate
		}
	}
	if p.Any != nil {
		compositeCount++
		if len(p.Any) == 0 {
			return domain.ErrInvalidPredicate
		}
	}
	if p.Not != nil {
		compositeCount++
	}
	if compositeCount != 1 {
		return domain.ErrInvalidPredicate
	}
	return nil
}

func (c *Compiler) validateCompositeChildren(p domain.Predicate, depth int) error {
	for _, children := range [][]*domain.Predicate{p.All, p.Any} {
		for _, child := range children {
			if child == nil {
				return domain.ErrInvalidPredicate
			}
			if err := c.validatePredicate(*child, depth+1); err != nil {
				return err
			}
		}
	}
	if p.Not != nil {
		return c.validatePredicate(*p.Not, depth+1)
	}
	return nil
}

func (c *Compiler) validateAction(action domain.Action) error {
	if err := validateActionShape(action); err != nil {
		return err
	}
	switch {
	case action.Emit != nil:
		return c.validateEmitAction(action.Emit)
	case action.EmitEvent != nil:
		return c.validateEmitEventAction(action.EmitEvent)
	case action.Command != nil:
		return c.validateCommandAction(action.Command)
	default:
		return domain.ErrInvalidAction
	}
}

func validateActionShape(action domain.Action) error {
	count := 0
	if action.Emit != nil {
		count++
	}
	if action.EmitEvent != nil {
		count++
	}
	if action.Command != nil {
		count++
	}
	if count != 1 {
		return domain.ErrInvalidAction
	}
	return nil
}

func (c *Compiler) validateEmitAction(action *domain.EmitAction) error {
	if action.Topic == "" {
		return domain.ErrInvalidAction
	}
	if payloadSize(action.Data) > c.limits.MaxPayloadBytes {
		return domain.ErrRuleExceedsLimits
	}
	return nil
}

func (c *Compiler) validateEmitEventAction(action *domain.EmitEventAction) error {
	policy := action.PartitionKeyPolicy
	if policy == "" {
		policy = domain.PartitionKeyPolicyExplicit
	}
	if !validPartitionKeyPolicy(policy) {
		return domain.ErrInvalidAction
	}
	if err := validatePartitionKeyPolicy(action, policy); err != nil {
		return err
	}
	if action.Type == "" || strings.ContainsAny(action.Type, "*>") || !json.Valid(action.Data) {
		return domain.ErrInvalidAction
	}
	if len(action.Data) > c.limits.MaxPayloadBytes {
		return domain.ErrRuleExceedsLimits
	}
	return nil
}

func validPartitionKeyPolicy(policy domain.PartitionKeyPolicy) bool {
	return policy == domain.PartitionKeyPolicyExplicit ||
		policy == domain.PartitionKeyPolicyInherit ||
		policy == domain.PartitionKeyPolicyFromData
}

func validatePartitionKeyPolicy(action *domain.EmitEventAction, policy domain.PartitionKeyPolicy) error {
	switch policy {
	case domain.PartitionKeyPolicyExplicit:
		if action.PartitionKey == "" {
			return domain.ErrInvalidAction
		}
	case domain.PartitionKeyPolicyInherit:
		// The key is inherited from the parent event.
	case domain.PartitionKeyPolicyFromData:
		if action.PartitionKeyPath == "" || !strings.HasPrefix(action.PartitionKeyPath, "$.") {
			return domain.ErrInvalidAction
		}
	}
	return nil
}

func (c *Compiler) validateCommandAction(action *domain.CommandAction) error {
	if action.Destination == "" || action.Name == "" {
		return domain.ErrInvalidAction
	}
	if payloadSize(action.Data) > c.limits.MaxPayloadBytes {
		return domain.ErrRuleExceedsLimits
	}
	return nil
}

func payloadSize(value interface{}) int {
	encoded, _ := json.Marshal(value)
	return len(encoded)
}

func countPredicates(p domain.Predicate) int {
	count := 0
	if p.All != nil {
		for _, sub := range p.All {
			if sub != nil {
				count += countPredicates(*sub)
			}
		}
	}
	if p.Any != nil {
		for _, sub := range p.Any {
			if sub != nil {
				count += countPredicates(*sub)
			}
		}
	}
	if p.Not != nil {
		count += countPredicates(*p.Not)
	}
	if p.Path != "" {
		count++
	}
	return count
}

// ValidatePathsAgainstContract checks that all paths referenced in a compiled
// rule set exist in the given contract schema. Returns an error for the first
// unknown path found, or nil if all paths are valid.
func ValidatePathsAgainstContract(compiled []domain.CompiledRule, schema *domain.ContractSchema) error {
	for _, rule := range compiled {
		if err := validatePredicatePaths(rule.When, schema); err != nil {
			return fmt.Errorf("rule %s: %w", rule.ID, err)
		}
	}
	return nil
}

func validatePredicatePaths(p domain.Predicate, schema *domain.ContractSchema) error {
	if p.All != nil {
		for _, sub := range p.All {
			if sub != nil {
				if err := validatePredicatePaths(*sub, schema); err != nil {
					return err
				}
			}
		}
	}
	if p.Any != nil {
		for _, sub := range p.Any {
			if sub != nil {
				if err := validatePredicatePaths(*sub, schema); err != nil {
					return err
				}
			}
		}
	}
	if p.Not != nil {
		if err := validatePredicatePaths(*p.Not, schema); err != nil {
			return err
		}
	}
	if p.Path != "" {
		field := strings.TrimPrefix(p.Path, "$.")
		if _, ok := schema.Fields[field]; !ok {
			return fmt.Errorf("path %q not found in contract %s@%s", p.Path, schema.Name, schema.Version)
		}
	}
	return nil
}
