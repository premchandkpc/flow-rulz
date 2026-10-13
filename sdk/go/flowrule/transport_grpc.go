package flowrulz

import (
	"context"
)

// grpcTransport is a stub - gRPC transport requires protobuf definitions
type grpcTransport struct {
	cfg    *TransportConfig
	closed bool
}

func newGRPCTransport(cfg *TransportConfig) (Transport, error) {
	return nil, ErrConfig("gRPC transport not available: protobuf definitions not found. Use NATS, HTTP, or Local transport instead.")
}

func (t *grpcTransport) Send(ctx context.Context, env *Envelope) error {
	return ErrConfig("gRPC transport not available")
}

func (t *grpcTransport) SendBatch(ctx context.Context, envs []*Envelope) error {
	return ErrConfig("gRPC transport not available")
}

func (t *grpcTransport) Close() error {
	return nil
}

func (t *grpcTransport) Name() string {
	return "grpc"
}
