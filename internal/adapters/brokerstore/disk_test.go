package brokerstore_test

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/adapters/brokerstore"
	"github.com/flowrulz/flow-rulz/internal/runtime/broker"
)

func TestDiskRecoveryAfterProcessExit(t *testing.T) {
	cfg := broker.Config{Partitions: 1, MaxMessages: 10, MaxBytes: 1024, AckWait: time.Second}
	if dir := os.Getenv("FLOWRULZ_BROKER_TEST_DIR"); dir != "" {
		disk, err := brokerstore.Open(dir)
		if err != nil {
			t.Fatal(err)
		}
		b, err := broker.New(cfg, disk)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			if _, err := b.Publish("events", "a", "k", []byte(`{}`)); err != nil {
				t.Fatal(err)
			}
		}
		c, err := b.Next(context.Background(), "a", "events", "g", 0)
		if err != nil || c == nil {
			t.Fatal("claim failed")
		}
		if err := b.Settle("a", "events", "g", 0, c.Receipt, false, 0); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Next(context.Background(), "a", "events", "g", 0); err != nil {
			t.Fatal(err)
		}
		os.Exit(0) // Abrupt process exit intentionally skips releasing the file lock.
	}
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestDiskRecoveryAfterProcessExit$")
	cmd.Env = append(os.Environ(), "FLOWRULZ_BROKER_TEST_DIR="+dir)
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child failed: %v %s", err, output)
	}
	disk, err := brokerstore.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	if duplicate, err := brokerstore.Open(dir); err == nil {
		_ = duplicate.Close()
		t.Fatal("directory lock was not enforced")
	}
	b, err := broker.New(cfg, disk)
	if err != nil {
		t.Fatal(err)
	}
	c, err := b.Next(context.Background(), "a", "events", "g", 0)
	if err != nil || c == nil || c.Record.Offset != 1 {
		t.Fatalf("recovery lost offset or pending record: %+v %v", c, err)
	}
	_, err = broker.New(broker.Config{Partitions: 2, MaxMessages: 10, MaxBytes: 1024, AckWait: time.Second}, disk)
	if !errors.Is(err, broker.ErrInvalid) {
		t.Fatal("partition count changed on existing storage")
	}
}

func TestDiskRejectsCorruptSnapshot(t *testing.T) {
	dir := t.TempDir()
	disk, err := brokerstore.Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := disk.Close(); err != nil {
			t.Error(err)
		}
	})
	if err := disk.Save([]byte(`{"version":1,"partitions":1,"topics":null,"groups":{}}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := broker.New(broker.Config{Partitions: 1, MaxMessages: 10, MaxBytes: 1024, AckWait: time.Second}, disk); err == nil {
		t.Fatal("corrupt snapshot accepted")
	}
}
