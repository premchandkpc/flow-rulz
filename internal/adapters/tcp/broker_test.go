package tcp

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/ports"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
	brokerservice "github.com/flowrulz/flow-rulz/internal/services/broker"
)

func startTCPTest(t *testing.T) string {
	t.Helper()
	b, err := broker.New(broker.Config{Partitions: 1, MaxMessages: 10, MaxBytes: 1 << 20, AckWait: time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- (&Server{Protocol: brokerservice.New(b, "token")}).Serve(ctx, listener) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Error(err)
			}
		case <-time.After(2 * time.Second):
			t.Error("TCP server failed to stop")
		}
	})
	return listener.Addr().String()
}

func TestRejectsOversizedFramesBeforeAllocating(t *testing.T) {
	addr := startTCPTest(t)
	conn, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	var header [4]byte
	binary.BigEndian.PutUint32(header[:], maxRequestBytes+1)
	if _, err := conn.Write(header[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := readFrame(conn, maxResponseBytes); err == nil {
		t.Fatal("oversized frame was accepted")
	}
}

func TestAuthenticationAndCancellationOnReusableConnection(t *testing.T) {
	client := &Client{Addr: startTCPTest(t), Token: "wrong"}
	t.Cleanup(func() {
		if err := client.Close(); err != nil {
			t.Error(err)
		}
	})
	request := ports.BrokerRequest{Operation: "consume", TenantID: "a", Topic: "events", Group: "g"}
	response, err := client.Call(t.Context(), request)
	if err != nil || response.Code != "unauthorized" {
		t.Fatalf("unauthorized response=%+v err=%v", response, err)
	}
	client.Token = "token"
	request.WaitMS = 20000
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Call(ctx, request); err == nil {
		t.Fatal("cancelled call succeeded")
	}
	request.WaitMS = 0
	response, err = client.Call(t.Context(), request)
	if err != nil || response.Code != "empty" {
		t.Fatalf("reconnect response=%+v err=%v", response, err)
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("cancelled deadline leaked to next request")
	}
}
