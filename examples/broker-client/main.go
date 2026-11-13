// The broker-client example executes one shared broker request over HTTP, TCP or NATS.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	brokerhttp "github.com/flowrulz/flow-rulz/internal/adapters/http/broker"
	"github.com/flowrulz/flow-rulz/internal/adapters/nats"
	brokertcp "github.com/flowrulz/flow-rulz/internal/adapters/tcp"
	"github.com/flowrulz/flow-rulz/internal/ports"
)

func main() {
	if err := run(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func run() (runErr error) {
	transport := flag.String("transport", "tcp", "http, tcp or nats")
	address := flag.String("address", "127.0.0.1:8091", "TCP address, HTTP base URL or NATS URL")
	subject := flag.String("subject", "flowrulz.broker", "NATS RPC subject")
	tenant := flag.String("tenant", "demo", "request tenant")
	flag.Parse()
	var request ports.BrokerRequest
	decoder := json.NewDecoder(io.LimitReader(os.Stdin, (1<<20)+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return fmt.Errorf("read broker request from stdin: %w", err)
	}
	if err := decoder.Decode(new(any)); !errors.Is(err, io.EOF) {
		return errors.New("expected one broker JSON request")
	}
	if request.TenantID != "" && request.TenantID != *tenant {
		return errors.New("request tenant differs from -tenant")
	}
	request.TenantID = *tenant
	request.Version = ports.BrokerProtocolVersion
	token := os.Getenv("BROKER_TOKEN")
	var client ports.BrokerCaller
	var err error
	switch *transport {
	case "tcp":
		client = &brokertcp.Client{Addr: *address, Token: token}
	case "http":
		client, err = brokerhttp.NewClient(*address, token)
	case "nats":
		client, err = nats.NewBrokerClient(*address, *subject, token)
	default:
		return errors.New("-transport must be http, tcp or nats")
	}
	if err != nil {
		return err
	}
	defer func() { runErr = errors.Join(runErr, client.Close()) }()
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()
	response, err := client.Call(ctx, request)
	if err != nil {
		return err
	}
	if err := json.NewEncoder(os.Stdout).Encode(response); err != nil {
		return err
	}
	switch response.Code {
	case "accepted", "message", "empty", "ok":
		return nil
	default:
		return errors.New("broker rejected request; see response code")
	}
}
