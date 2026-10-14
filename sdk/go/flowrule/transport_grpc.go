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
	return nil, ErrConfig("gRPC transport not available: no gRPC server is implemented. Use NATS to publish events; HTTP event ingestion and local evaluation are not supported.")
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
