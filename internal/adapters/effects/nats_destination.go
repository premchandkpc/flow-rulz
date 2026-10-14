// Package effects contains NATS-backed effect destinations.
package effects

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/flowrulz/flow-rulz/internal/domain"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

var subjectToken = regexp.MustCompile(`^[a-z][a-z0-9_-]{0,62}$`)

// NATSDestination publishes emitted effects and commands to durable NATS JetStream subjects.
// Receivers consume the subject with their own durable consumer and deduplicate
// using the stable effect ID/idempotency key in the message envelope.
type NATSDestination struct {
	publisher ports.SubjectPublisher
}

// NewNATSDestination creates a NATS-backed effect destination.
func NewNATSDestination(publisher ports.SubjectPublisher) *NATSDestination {
	return &NATSDestination{publisher: publisher}
}

// Send publishes an emitted topic to effects.<topic> or a command to commands.<destination>.<name>.
func (s *NATSDestination) Send(ctx context.Context, effect *domain.Effect, idempotencyKey string) error {
	if effect == nil {
		return errors.New("publish effect: effect is nil")
	}
	if s.publisher == nil {
		return errors.New("publish effect: NATS publisher is not configured")
	}
	if effect.ID == "" || idempotencyKey == "" {
		return errors.New("publish effect: stable effect ID and idempotency key are required")
	}
	var subject string
	switch effect.EffectType {
	case domain.EffectTypeEmit:
		if err := validateSubjectPath(effect.Destination); err != nil {
			return fmt.Errorf("publish emitted effect: %w", err)
		}
		subject = "effects." + effect.Destination
	case domain.EffectTypeCommand:
		if effect.Destination == "" || effect.Name == "" {
			return errors.New("publish command: destination and name are required")
		}
		if !subjectToken.MatchString(effect.Destination) {
			return fmt.Errorf("publish command: invalid destination %q", effect.Destination)
		}
		if err := validateSubjectPath(effect.Name); err != nil {
			return err
		}
		subject = "commands." + effect.Destination + "." + effect.Name
	default:
		return fmt.Errorf("publish command: unsupported effect type %q", effect.EffectType)
	}
	message := *effect
	message.IdempotencyKey = idempotencyKey
	payload, err := json.Marshal(&message)
	if err != nil {
		return fmt.Errorf("encode command effect: %w", err)
	}
	if err := s.publisher.Publish(ctx, subject, payload); err != nil {
		return fmt.Errorf("publish effect to %q: %w", subject, err)
	}
	return nil
}

func validateSubjectPath(name string) error {
	if len(name) == 0 || len(name) > 128 {
		return errors.New("publish effect: subject path must be between 1 and 128 characters")
	}
	for _, token := range strings.Split(name, ".") {
		if !subjectToken.MatchString(token) {
			return fmt.Errorf("publish effect: invalid subject path %q", name)
		}
	}
	return nil
}

var _ ports.EffectSender = (*NATSDestination)(nil)
