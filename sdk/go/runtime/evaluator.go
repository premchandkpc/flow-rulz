package runtime

// Effect represents an effect to be emitted
type Effect struct {
	Type               string         `json:"type"`
	Payload            map[string]any `json:"payload"`
	PartitionKey       string         `json:"partition_key"`
	PartitionKeyPolicy string         `json:"partition_key_policy,omitempty"`
}

// CompiledRule represents a pre-compiled rule for fast evaluation
type CompiledRule struct {
	ID        string
	Priority  int
	Condition ConditionFunc
	Effects   []EffectTemplate
}

// ConditionFunc evaluates a condition against payload
type ConditionFunc func(payload map[string]any) bool

// EffectTemplate defines an effect to emit with template substitution
type EffectTemplate struct {
	EventType          string
	PartitionKeyPolicy string
	DataTemplate       map[string]string
}

// Evaluator evaluates rules against a payload
type Evaluator struct {
	rules []CompiledRule
}

// NewEvaluator creates a new evaluator with compiled rules
func NewEvaluator(rules []CompiledRule) *Evaluator {
	return &Evaluator{rules: rules}
}

// Evaluate runs all rules against the payload and returns matching effects
func (e *Evaluator) Evaluate(payload map[string]any) ([]Effect, error) {
	var effects []Effect
	for _, rule := range e.rules {
		if rule.Condition(payload) {
			for _, effTmpl := range rule.Effects {
				effect := Effect{
					Type:         effTmpl.EventType,
					PartitionKey: computePartitionKey(payload, effTmpl.PartitionKeyPolicy),
				}
				effect.Payload = substituteTemplate(effTmpl.DataTemplate, payload)
				effects = append(effects, effect)
			}
		}
	}
	return effects, nil
}

func getFloat64(m map[string]any, key string) (float64, bool) {
	switch v := m[key].(type) {
	case float64:
		return v, true
	case int:
		return float64(v), true
	case int64:
		return float64(v), true
	}
	return 0, false
}

func getString(m map[string]any, key string) (string, bool) {
	v, ok := m[key]
	if !ok {
		return "", false
	}
	switch v := v.(type) {
	case string:
		return v, true
	}
	return "", false
}

func computePartitionKey(payload map[string]any, policy string) string {
	switch policy {
	case "inherit":
		if pk, ok := getString(payload, "customer_id"); ok {
			return pk
		}
		if pk, ok := getString(payload, "partition_key"); ok {
			return pk
		}
		return "default"
	default:
		return policy
	}
}

func substituteTemplate(tmpl map[string]string, payload map[string]any) map[string]any {
	result := make(map[string]any)
	for k, v := range tmpl {
		result[k] = substituteValue(v, payload)
	}
	return result
}

func substituteValue(val string, payload map[string]any) any {
	// Simple template substitution - can be extended
	if val == "{{new_id}}" {
		return "generated-id" // Placeholder
	}
	if val == "{{now}}" {
		return "2026-01-01T00:00:00Z" // Placeholder
	}
	// Check for {{$.path}} or {{field}} patterns
	if len(val) >= 4 && val[:2] == "{{" && val[len(val)-2:] == "}}" {
		key := val[2 : len(val)-2]
		key = key[2:] // Remove "$." prefix if present
		if v, ok := payload[key]; ok {
			return v
		}
		return nil
	}
	return val
}
