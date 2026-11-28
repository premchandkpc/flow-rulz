package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"

	"github.com/flowrulz/flow-rulz/internal/adapters/agent"
	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

type agentSender struct {
	next  ports.EffectSender
	model *agent.Client
}

func brokerAgent(next ports.EffectSender) (ports.EffectSender, error) {
	endpoint, model := os.Getenv("BROKER_AGENT_URL"), os.Getenv("BROKER_AGENT_MODEL")
	if endpoint == "" && model == "" {
		return next, nil
	}
	client, err := agent.New(endpoint, model)
	if err != nil {
		return nil, err
	}
	return &agentSender{next, client}, nil
}

func (s *agentSender) Send(ctx context.Context, e *domain.Effect, key string) error {
	if e == nil || e.EffectType != domain.EffectTypeEmitEvent || e.Name != "agent.triaged" {
		return s.next.Send(ctx, e, key)
	}
	var child domain.EventEnvelope
	if err := json.Unmarshal(e.Payload, &child); err != nil {
		return err
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(child.Data, &fields); err != nil || fields == nil {
		return errors.New("agent.triaged requires object data")
	}
	if _, exists := fields["decision"]; exists {
		return errors.New("agent.triaged reserves decision")
	}
	decision, err := s.model.Decide(ctx, fields)
	if err != nil {
		return err
	}
	fields["decision"], _ = json.Marshal(decision)
	child.Data, err = json.Marshal(fields)
	if err != nil {
		return err
	}
	output := *e
	output.Payload, err = json.Marshal(child)
	if err != nil {
		return err
	}
	return s.next.Send(ctx, &output, key)
}
