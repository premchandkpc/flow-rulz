package nats

import (
	"testing"
	"time"

	"github.com/nats-io/nats.go/jetstream"
)

func TestShardConsumerPreservesDeliveryOrderOnRetry(t *testing.T) {
	cfg := shardConsumerConfig(Config{MaxDeliver: -1, AckWait: 30 * time.Second}, "events-shard-1", "events.shard.1")
	if cfg.AckPolicy != jetstream.AckExplicitPolicy || cfg.MaxAckPending != 1 {
		t.Fatalf("per-shard consumer ordering disabled: %+v", cfg)
	}
	if cfg.MaxDeliver != -1 || cfg.FilterSubject != "events.shard.1" {
		t.Fatalf("consumer routing or retry policy changed: %+v", cfg)
	}
}
