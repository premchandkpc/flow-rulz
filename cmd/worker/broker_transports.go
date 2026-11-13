package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/embedded"
	brokerhttp "github.com/flowrulz/flow-rulz/internal/adapters/http/broker"
	"github.com/flowrulz/flow-rulz/internal/adapters/nats"
	"github.com/flowrulz/flow-rulz/internal/adapters/remote"
	brokertcp "github.com/flowrulz/flow-rulz/internal/adapters/tcp"
	"github.com/flowrulz/flow-rulz/internal/config"
	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
)

type brokerPublisher interface {
	ports.EventPublisher
	ports.SubjectPublisher
}

func newBrokerPublisher(cfg *config.Config, b *broker.Broker) (brokerPublisher, error) {
	var client ports.BrokerCaller
	var err error
	switch cfg.Broker.OutputTransport {
	case "", "local":
		return &embedded.Publisher{Broker: b, Partitions: cfg.Worker.NumShards}, nil
	case "http":
		client, err = brokerhttp.NewClient(cfg.Broker.OutputURL, cfg.Broker.OutputToken)
	case "tcp":
		client = &brokertcp.Client{Addr: cfg.Broker.OutputAddr, Token: cfg.Broker.OutputToken}
	case "nats":
		url := cfg.Broker.OutputNATSURL
		if url == "" {
			url = cfg.NATS.URL
		}
		client, err = nats.NewBrokerClient(url, cfg.Broker.OutputNATSSubject, cfg.Broker.OutputToken)
	default:
		return nil, errors.New("unsupported broker output transport")
	}
	if err != nil {
		return nil, err
	}
	return &remote.Publisher{Client: client, Partitions: cfg.Worker.NumShards}, nil
}

type brokerTransport struct {
	name  string
	run   func(context.Context) error
	close func(context.Context) error
}

func newBrokerTransports(ctx context.Context, cfg *config.Config, protocol ports.BrokerProtocol) ([]brokerTransport, error) {
	var endpoints []brokerTransport
	for _, name := range cfg.Broker.Transports {
		var endpoint brokerTransport
		var err error
		switch name {
		case "none":
			continue
		case "http":
			endpoint, err = newBrokerHTTP(ctx, cfg.Broker.Addr, protocol)
		case "tcp":
			endpoint, err = newBrokerTCP(cfg.Broker.TCPAddr, protocol)
		case "nats":
			var server *nats.BrokerEndpoint
			server, err = nats.NewBrokerEndpoint(ctx, cfg.NATS.URL, cfg.Broker.NATSSubject, protocol)
			if err == nil {
				endpoint = brokerTransport{name: "nats", run: server.Serve, close: func(context.Context) error { return server.Close() }}
			}
		default:
			err = errors.New("unsupported broker ingress transport")
		}
		if err != nil {
			return endpoints, fmt.Errorf("initialize broker %s: %w", name, err)
		}
		endpoints = append(endpoints, endpoint)
	}
	return endpoints, nil
}

func closeBrokerTransports(ctx context.Context, endpoints []brokerTransport) error {
	var result error
	for i := len(endpoints) - 1; i >= 0; i-- {
		if err := endpoints[i].close(ctx); err != nil {
			result = errors.Join(result, fmt.Errorf("close broker %s: %w", endpoints[i].name, err))
		}
	}
	return result
}

func closeListener(listener net.Listener) error {
	err := listener.Close()
	if errors.Is(err, net.ErrClosed) {
		return nil
	}
	return err
}

func newBrokerHTTP(ctx context.Context, address string, protocol ports.BrokerProtocol) (brokerTransport, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return brokerTransport{}, err
	}
	server := &http.Server{Handler: brokerhttp.NewProtocolHandler(protocol), BaseContext: func(net.Listener) context.Context { return ctx }, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 25 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 32 << 10}
	log.Printf("broker HTTP listening on %s", listener.Addr())
	return brokerTransport{name: "http", run: func(context.Context) error {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}, close: func(ctx context.Context) error {
		err := server.Shutdown(ctx)
		if err != nil {
			err = errors.Join(err, server.Close())
		}
		return errors.Join(err, closeListener(listener))
	}}, nil
}

func newBrokerTCP(address string, protocol ports.BrokerProtocol) (brokerTransport, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return brokerTransport{}, err
	}
	server := &brokertcp.Server{Protocol: protocol}
	log.Printf("broker TCP listening on %s", listener.Addr())
	return brokerTransport{name: "tcp", run: func(ctx context.Context) error { return server.Serve(ctx, listener) }, close: func(context.Context) error { return closeListener(listener) }}, nil
}
