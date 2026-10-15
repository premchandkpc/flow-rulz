// Package logfirst contains the scheduler transaction boundary for the
// Kafka/Redpanda log-first proposal. Timer heap and partition replay are omitted.
package logfirst

import (
	"context"
	"errors"
	"strconv"
	"time"
)

type ScheduleRecord struct {
	SchemaVersion int           `json:"schema_version"`
	TenantID      string        `json:"tenant_id"`
	ScheduleID    string        `json:"schedule_id"`
	Revision      int64         `json:"revision"`
	DueAt         time.Time     `json:"due_at"`
	Event         EventEnvelope `json:"event"`
	Status        string        `json:"status"`
}

type DueSchedule struct {
	Key         []byte
	Record      ScheduleRecord
	Occurrence string
}

// OccurrenceID must be deterministic for a schedule revision and due occurrence.
// Production code should use a documented stable hash/UUID algorithm.
func OccurrenceID(scheduleID string, revision int64, dueAt time.Time) string {
	return scheduleID + ":" + time.Unix(dueAt.Unix(), 0).UTC().Format(time.RFC3339) + ":" + strconv.FormatInt(revision, 10)
}

type ScheduleCodec interface {
	EncodeEvent(EventEnvelope) ([]byte, error)
}

type Scheduler struct {
	Transactions TransactionFactory
	Codec        ScheduleCodec
	EventsTopic  string
	SchedulesTopic string
	GroupID      string
}

// FireDue atomically publishes the due event and tombstones the compacted schedule
// key. If the transaction aborts, read-committed consumers see neither output.
// The caller must only call this for a due record in a partition currently assigned
// to this scheduler; on assignment loss it must stop and discard local heap entries.
func (s *Scheduler) FireDue(ctx context.Context, due DueSchedule) error {
	if s.Transactions == nil || s.Codec == nil || s.EventsTopic == "" || s.SchedulesTopic == "" {
		return errors.New("scheduler dependencies are not configured")
	}
	if due.Record.ScheduleID == "" || due.Record.TenantID == "" || due.Occurrence == "" {
		return errors.New("schedule identity and deterministic occurrence are required")
	}
	if due.Record.SchemaVersion != 1 {
		return ErrUnsupportedSchema
	}

	event := due.Record.Event
	event.SchemaVersion = 1
	event.ID = due.Occurrence
	event.TenantID = due.Record.TenantID
	if event.Metadata.CausationID == "" {
		event.Metadata.CausationID = "schedule:" + due.Record.ScheduleID
	}
	payload, err := s.Codec.EncodeEvent(event)
	if err != nil {
		return err
	}

	tx, err := s.Transactions.Begin(ctx)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Abort(context.Background())
		}
	}()

	if err := tx.Produce(ctx, s.EventsTopic, []byte(event.TenantID+":"+event.PartitionKey), payload); err != nil {
		return err
	}
	// A nil value is a Kafka tombstone for this compacted schedule key.
	if err := tx.Produce(ctx, s.SchedulesTopic, due.Key, nil); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	committed = true
	return nil
}

