package logfirst

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"
)

type memoryLogStore struct {
	records     []LogRecord
	checkpoints map[string][]LogPosition
	beginErr    error
	commitErr   error
}

func newMemoryLogStore() *memoryLogStore {
	return &memoryLogStore{checkpoints: make(map[string][]LogPosition)}
}

func (s *memoryLogStore) Begin(context.Context) (LogTransaction, error) {
	if s.beginErr != nil {
		return nil, s.beginErr
	}
	return &memoryLogTx{store: s}, nil
}

func (s *memoryLogStore) Append(ctx context.Context, record LogRecord) (LogPosition, error) {
	tx, err := s.Begin(ctx)
	if err != nil {
		return LogPosition{}, err
	}
	if err := tx.Append(ctx, record); err != nil {
		_ = tx.Abort(context.Background())
		return LogPosition{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		_ = tx.Abort(context.Background())
		return LogPosition{}, err
	}
	return LogPosition{Stream: record.Kind, Partition: record.PartitionKey, Sequence: int64(len(s.records))}, nil
}

func (s *memoryLogStore) ReadFrom(_ context.Context, stream, partition string, from int64, limit int) ([]LogRecord, error) {
	var result []LogRecord
	for i, record := range s.records {
		if record.Kind == stream && record.PartitionKey == partition && int64(i) >= from {
			result = append(result, record)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

func (s *memoryLogStore) Health(context.Context) error { return nil }
func (s *memoryLogStore) Close() error                { return nil }

type memoryLogTx struct {
	store     *memoryLogStore
	records   []LogRecord
	group     string
	positions []LogPosition
	closed    bool
}

func (tx *memoryLogTx) Append(_ context.Context, record LogRecord) error {
	if tx.closed {
		return errors.New("transaction is closed")
	}
	if record.ID == "" || record.TenantID == "" || record.Kind == "" {
		return errors.New("record identity, tenant, and kind are required")
	}
	tx.records = append(tx.records, record)
	return nil
}

func (tx *memoryLogTx) CommitInputPosition(_ context.Context, group string, positions []LogPosition) error {
	if tx.closed {
		return errors.New("transaction is closed")
	}
	if group == "" {
		return errors.New("consumer group is required")
	}
	tx.group = group
	tx.positions = append([]LogPosition(nil), positions...)
	return nil
}

func (tx *memoryLogTx) Commit(context.Context) error {
	if tx.closed {
		return errors.New("transaction is closed")
	}
	tx.closed = true
	if tx.store.commitErr != nil {
		return tx.store.commitErr
	}
	tx.store.records = append(tx.store.records, tx.records...)
	if tx.group != "" {
		tx.store.checkpoints[tx.group] = append([]LogPosition(nil), tx.positions...)
	}
	return nil
}

func (tx *memoryLogTx) Abort(context.Context) error {
	if tx.closed {
		return nil
	}
	tx.closed = true
	tx.records = nil
	tx.positions = nil
	return nil
}

type jsonCodec struct{}

func (jsonCodec) DecodeEvent(value []byte) (EventEnvelope, error) {
	var event EventEnvelope
	err := json.Unmarshal(value, &event)
	return event, err
}
func (jsonCodec) EncodeEffect(effect Effect) ([]byte, error) { return json.Marshal(effect) }
func (jsonCodec) EncodeAudit(audit ExecutionAudit) ([]byte, error) {
	return json.Marshal(audit)
}
func (jsonCodec) EncodeEvent(event EventEnvelope) ([]byte, error) { return json.Marshal(event) }

type fixedEvaluator struct {
	effects []Effect
	audits  []ExecutionAudit
	err     error
}

func (e fixedEvaluator) Evaluate(context.Context, EventEnvelope) ([]Effect, []ExecutionAudit, error) {
	return e.effects, e.audits, e.err
}

func TestWorkerCommitsOutputsAndInputPositionsTogether(t *testing.T) {
	store := newMemoryLogStore()
	effect := Effect{
		ID: "effect-1", IdempotencyKey: "effect-1", TenantID: "tenant-1",
		Destination: "flow.commands", PartitionKey: "order-1", Payload: json.RawMessage(`{"ok":true}`),
	}
	audit := ExecutionAudit{TenantID: "tenant-1", EventID: "event-1", RulesetID: "rules", Revision: 3}
	worker := Worker{
		GroupID: "worker-group", AuditTopic: "flow.audit", Store: store,
		Evaluator: fixedEvaluator{effects: []Effect{effect}, audits: []ExecutionAudit{audit}},
		Codec: jsonCodec{},
	}
	input, err := json.Marshal(EventEnvelope{
		SchemaVersion: 1, ID: "event-1", TenantID: "tenant-1",
		EventType: "orders.created", PartitionKey: "order-1", OccurredAt: time.Unix(1, 0).UTC(),
	})
	if err != nil {
		t.Fatal(err)
	}
	batch := Batch{
		Records: []Record{{Topic: "flow.events", Partition: 2, Offset: 10, Value: input}},
		Offsets: []Offset{{Topic: "flow.events", Partition: 2, Next: 11}},
	}
	if err := worker.ProcessBatch(context.Background(), batch); err != nil {
		t.Fatalf("ProcessBatch() error = %v", err)
	}
	if len(store.records) != 2 {
		t.Fatalf("expected effect + audit records, got %d", len(store.records))
	}
	wantPositions := []LogPosition{{Stream: "flow.events", Partition: "2", Sequence: 11}}
	if got := store.checkpoints["worker-group"]; !reflect.DeepEqual(got, wantPositions) {
		t.Fatalf("checkpoint = %#v, want %#v", got, wantPositions)
	}
}

func TestWorkerAbortsOnDecodeFailure(t *testing.T) {
	store := newMemoryLogStore()
	worker := Worker{GroupID: "group", Store: store, Evaluator: fixedEvaluator{}, Codec: jsonCodec{}}
	err := worker.ProcessBatch(context.Background(), Batch{
		Records: []Record{{Topic: "flow.events", Partition: 0, Offset: 0, Value: []byte("{")}},
		Offsets: []Offset{{Topic: "flow.events", Partition: 0, Next: 1}},
	})
	if err == nil {
		t.Fatal("expected decode error")
	}
	if len(store.records) != 0 {
		t.Fatalf("records committed after decode error: %d", len(store.records))
	}
	if _, ok := store.checkpoints["group"]; ok {
		t.Fatal("input checkpoint committed after decode error")
	}
}

func TestWorkerAbortsOnCommitFailure(t *testing.T) {
	store := newMemoryLogStore()
	store.commitErr = errors.New("simulated durable commit failure")
	worker := Worker{
		GroupID: "group", Store: store, Evaluator: fixedEvaluator{}, Codec: jsonCodec{},
	}
	input, _ := json.Marshal(EventEnvelope{
		SchemaVersion: 1, ID: "event", TenantID: "tenant", EventType: "test", PartitionKey: "key",
	})
	err := worker.ProcessBatch(context.Background(), Batch{
		Records: []Record{{Topic: "flow.events", Partition: 0, Offset: 0, Value: input}},
		Offsets: []Offset{{Topic: "flow.events", Partition: 0, Next: 1}},
	})
	if err == nil {
		t.Fatal("expected commit error")
	}
	if len(store.records) != 0 {
		t.Fatalf("records committed after failed commit: %d", len(store.records))
	}
	if _, ok := store.checkpoints["group"]; ok {
		t.Fatal("checkpoint committed after failed transaction")
	}
}

func TestSchedulerWritesEventAndTombstoneInOneTransaction(t *testing.T) {
	store := newMemoryLogStore()
	scheduler := Scheduler{Store: store, Codec: jsonCodec{}, EventsTopic: "flow.events", SchedulesTopic: "flow.schedules", GroupID: "scheduler-group"}
	due := DueSchedule{
		Key: []byte("tenant-1:schedule-1"),
		Record: ScheduleRecord{
			SchemaVersion: 1, TenantID: "tenant-1", ScheduleID: "schedule-1", Revision: 2,
			DueAt: time.Unix(100, 0).UTC(),
			Event: EventEnvelope{EventType: "timer.fired", PartitionKey: "order-1"},
		},
		Occurrence: OccurrenceID("schedule-1", 2, time.Unix(100, 0).UTC()),
		InputPosition: LogPosition{Stream: "flow.schedules", Partition: "0", Sequence: 7},
	}
	if err := scheduler.FireDue(context.Background(), due); err != nil {
		t.Fatalf("FireDue() error = %v", err)
	}
	if len(store.records) != 2 {
		t.Fatalf("expected event and tombstone, got %d records", len(store.records))
	}
	if store.records[0].Kind != "flow.events" || store.records[0].ID != due.Occurrence {
		t.Fatalf("unexpected event record: %#v", store.records[0])
	}
	if store.records[1].Kind != "flow.schedules" || store.records[1].Value != nil {
		t.Fatalf("expected schedule tombstone, got %#v", store.records[1])
	}
	wantPositions := []LogPosition{{Stream: "flow.schedules", Partition: "0", Sequence: 7}}
	if got := store.checkpoints["scheduler-group"]; !reflect.DeepEqual(got, wantPositions) {
		t.Fatalf("scheduler checkpoint = %#v, want %#v", got, wantPositions)
	}
}

func TestSchedulerDoesNotPublishPartialTransaction(t *testing.T) {
	store := newMemoryLogStore()
	store.commitErr = errors.New("simulated commit failure")
	scheduler := Scheduler{Store: store, Codec: jsonCodec{}, EventsTopic: "flow.events", SchedulesTopic: "flow.schedules", GroupID: "scheduler-group"}
	due := DueSchedule{
		Key: []byte("tenant:schedule"),
		Record: ScheduleRecord{
			SchemaVersion: 1, TenantID: "tenant", ScheduleID: "schedule", Revision: 1,
			Event: EventEnvelope{EventType: "timer.fired", PartitionKey: "key"},
		},
		Occurrence: "schedule:1",
		InputPosition: LogPosition{Stream: "flow.schedules", Partition: "0", Sequence: 1},
	}
	if err := scheduler.FireDue(context.Background(), due); err == nil {
		t.Fatal("expected commit error")
	}
	if len(store.records) != 0 {
		t.Fatalf("partial schedule transaction committed: %d records", len(store.records))
	}
	if _, ok := store.checkpoints["scheduler-group"]; ok {
		t.Fatal("scheduler checkpoint committed after failed transaction")
	}
}

func TestWorkerRequiresInputOffsets(t *testing.T) {
	store := newMemoryLogStore()
	worker := Worker{GroupID: "group", Store: store, Evaluator: fixedEvaluator{}, Codec: jsonCodec{}}
	input, err := json.Marshal(EventEnvelope{
		SchemaVersion: 1, ID: "event", TenantID: "tenant", EventType: "test", PartitionKey: "key",
	})
	if err != nil {
		t.Fatal(err)
	}
	err = worker.ProcessBatch(context.Background(), Batch{
		Records: []Record{{Topic: "flow.events", Partition: 0, Offset: 0, Value: input}},
	})
	if err == nil {
		t.Fatal("expected missing-offset error")
	}
	if len(store.records) != 0 {
		t.Fatalf("records committed without input checkpoint: %d", len(store.records))
	}
}
