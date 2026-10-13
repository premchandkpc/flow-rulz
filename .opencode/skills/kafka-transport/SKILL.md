# Kafka Transport — Full Semantics

Replace the in-memory transport stubs with real Kafka integration using `confluent-kafka-go`. Implement consumer groups, partition assignment, offset commit, batch poll, backpressure, poison messages, rebalance handling, and dead letter queues.

**Files affected:**
- `go/internal/transport/consumer.go`
- `go/internal/transport/producer.go`
- `go/internal/transport/consumer_group.go` (new)
- `go/internal/transport/dlx.go` (new)
- `go.mod`

**Verification:** Integration tests with a real Kafka broker (or test container) verify consumer group rebalancing, offset commits, and poison message routing to DLQ.
