package flowrulz

import (
	"context"
)

// localTransport is a stub - local transport requires internal rules engine
type localTransport struct {
	cfg    *TransportConfig
	closed bool
}

func newLocalTransport(cfg *TransportConfig) (Transport, error) {
	return nil, ErrConfig("local transport not available in SDK: requires internal rules engine. Use NATS or HTTP transport to send to flow-rulz-server.")
}

func (t *localTransport) Send(ctx context.Context, env *Envelope) error {
	return ErrConfig("local transport not available")
}

func (t *localTransport) SendBatch(ctx context.Context, envs []*Envelope) error {
	return ErrConfig("local transport not available")
}

func (t *localTransport) Close() error {
	return nil
}

func (t *localTransport) Name() string {
	return "local"
}
