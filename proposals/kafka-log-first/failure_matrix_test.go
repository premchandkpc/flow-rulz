package logfirst

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"
)

type failingCodec struct {
	effectErr error
	auditErr error
}

func (f failingCodec) DecodeEvent(value []byte) (EventEnvelope, error) {
	return (jsonCodec{}).DecodeEvent(value)
}
func (f failingCodec) EncodeEffect(effect Effect) ([]byte, error) {
	if f.effectErr != nil { return nil, f.effectErr }
	return json.Marshal(effect)
}
func (f failingCodec) EncodeAudit(audit ExecutionAudit) ([]byte, error) {
	if f.auditErr != nil { return nil, f.auditErr }
	return json.Marshal(audit)
}

func workerTestBatch(t *testing.T) Batch {
	t.Helper()
	input, err := json.Marshal(EventEnvelope{
		SchemaVersion: 1, ID: "event-1", TenantID: "tenant-a",
		EventType: "orders.created", PartitionKey: "order-1",
		Metadata: EventMetadata{TargetServiceID: "domain/orders", DeploymentRevision: 2},
	})
	if err != nil { t.Fatal(err) }
	return Batch{
		Records: []Record{{Topic: "flow.events", Partition: 0, Offset: 10, Value: input}},
		Offsets: []Offset{{Topic: "flow.events", Partition: 0, Next: 11}},
	}
}

func workerTest(t *testing.T, store *memoryLogStore, evaluator Evaluator, codec Codec) Worker {
	t.Helper()
	return Worker{
		DeploymentID: "domain/orders", DeploymentRevision: 2, GroupID: "orders-workers",
		AuditTopic: "flow.audit", Store: store, Evaluator: evaluator, Codec: codec,
	}
}

func validWorkerEffect() Effect {
	return Effect{ID:"effect-1", IdempotencyKey:"effect-1", TenantID:"tenant-a", Destination:"flow.commands", PartitionKey:"order-1"}
}
func validWorkerAudit() ExecutionAudit {
	return ExecutionAudit{TenantID:"tenant-a", EventID:"event-1", RulesetID:"orders-created", Revision:1}
}

func TestWorkerDoesNotAdvanceCheckpointOnAnyTransactionalFailure(t *testing.T) {
	evalErr := errors.New("evaluation failed")
	encodeEffectErr := errors.New("effect encoding failed")
	encodeAuditErr := errors.New("audit encoding failed")
	appendErr := errors.New("log append failed")
	checkpointErr := errors.New("checkpoint write failed")
	beginErr := errors.New("transaction begin failed")
	tests := []struct {
		name string
		store func() *memoryLogStore
		evaluator Evaluator
		codec Codec
	}{
		{name:"begin fails", store:func()*memoryLogStore{s:=newMemoryLogStore();s.beginErr=beginErr;return s}, evaluator:fixedEvaluator{}, codec:jsonCodec{}},
		{name:"evaluation fails", store:newMemoryLogStore, evaluator:fixedEvaluator{err:evalErr}, codec:jsonCodec{}},
		{name:"effect encoding fails", store:newMemoryLogStore, evaluator:fixedEvaluator{effects:[]Effect{validWorkerEffect()}}, codec:failingCodec{effectErr:encodeEffectErr}},
		{name:"audit encoding fails", store:newMemoryLogStore, evaluator:fixedEvaluator{audits:[]ExecutionAudit{validWorkerAudit()}}, codec:failingCodec{auditErr:encodeAuditErr}},
		{name:"append fails", store:func()*memoryLogStore{s:=newMemoryLogStore();s.appendErr=appendErr;return s}, evaluator:fixedEvaluator{effects:[]Effect{validWorkerEffect()}}, codec:jsonCodec{}},
		{name:"checkpoint write fails", store:func()*memoryLogStore{s:=newMemoryLogStore();s.positionErr=checkpointErr;return s}, evaluator:fixedEvaluator{effects:[]Effect{validWorkerEffect()}}, codec:jsonCodec{}},
	}
	for _, tt := range tests {
		t.Run(tt.name,func(t *testing.T){
			store:=tt.store()
			worker:=workerTest(t,store,tt.evaluator,tt.codec)
			if err:=worker.ProcessBatch(context.Background(),workerTestBatch(t));err==nil{t.Fatal("ProcessBatch() error = nil, want failure")}
			if len(store.records)!=0{t.Fatalf("records committed despite failure: %#v",store.records)}
			if _,ok:=store.checkpoints["orders-workers"];ok{t.Fatal("checkpoint advanced despite failure")}
		})
	}
}

func TestWorkerRejectsCrossTenantEffectAndAudit(t *testing.T) {
	tests:=[]struct{name string;evaluator Evaluator}{
		{name:"effect tenant mismatch",evaluator:fixedEvaluator{effects:[]Effect{{ID:"e",IdempotencyKey:"e",TenantID:"tenant-b",Destination:"commands",PartitionKey:"key"}}}},
		{name:"audit tenant mismatch",evaluator:fixedEvaluator{audits:[]ExecutionAudit{{TenantID:"tenant-b",EventID:"event-1",RulesetID:"rules",Revision:1}}}},
		{name:"audit event mismatch",evaluator:fixedEvaluator{audits:[]ExecutionAudit{{TenantID:"tenant-a",EventID:"other-event",RulesetID:"rules",Revision:1}}}},
		{name:"audit revision invalid",evaluator:fixedEvaluator{audits:[]ExecutionAudit{{TenantID:"tenant-a",EventID:"event-1",RulesetID:"rules",Revision:0}}}},
	}
	for _,tt:=range tests{t.Run(tt.name,func(t *testing.T){
		store:=newMemoryLogStore()
		worker := workerTest(t, store, tt.evaluator, jsonCodec{})
		if err := worker.ProcessBatch(context.Background(), workerTestBatch(t)); err == nil { t.Fatal("expected identity validation error") }
		if len(store.records)!=0{t.Fatalf("records committed: %#v",store.records)}
		if _,ok:=store.checkpoints["orders-workers"];ok{t.Fatal("checkpoint advanced")}
	})}
}

func TestWorkerRejectsCheckpointThatCouldSkipInput(t *testing.T) {
	tests:=[]struct{name string;offsets []Offset}{
		{name:"checkpoint skips offsets",offsets:[]Offset{{Topic:"flow.events",Partition:0,Next:12}}},
		{name:"checkpoint for unused partition",offsets:[]Offset{{Topic:"flow.events",Partition:0,Next:11},{Topic:"flow.events",Partition:1,Next:1}}},
		{name:"duplicate partition checkpoint",offsets:[]Offset{{Topic:"flow.events",Partition:0,Next:11},{Topic:"flow.events",Partition:0,Next:11}}},
		{name:"negative partition",offsets:[]Offset{{Topic:"flow.events",Partition:-1,Next:11}}},
		{name:"empty topic",offsets:[]Offset{{Topic:"",Partition:0,Next:11}}},
	}
	for _,tt:=range tests{t.Run(tt.name,func(t *testing.T){
		store:=newMemoryLogStore()
		batch:=workerTestBatch(t);batch.Offsets=tt.offsets
		worker := workerTest(t, store, fixedEvaluator{}, jsonCodec{})
		if err := worker.ProcessBatch(context.Background(), batch); err == nil { t.Fatal("expected invalid checkpoint error") }
		if len(store.records)!=0{t.Fatalf("records committed: %#v",store.records)}
	})}
}

func TestSchedulerRejectsInvalidDueScheduleBeforeOpeningTransaction(t *testing.T) {
	validDue:=DueSchedule{
		Key:[]byte("tenant-a:schedule-1"),
		Record:ScheduleRecord{SchemaVersion:1,TenantID:"tenant-a",ScheduleID:"schedule-1",Revision:2,DueAt:time.Unix(100,0).UTC(),Event:EventEnvelope{EventType:"timer.fired",PartitionKey:"order-1"}},
		Occurrence:OccurrenceID("schedule-1",2,time.Unix(100,0).UTC()),
		InputPosition:InputCheckpoint{Stream:"flow.schedules",Partition:"0",NextSequence:5},
	}
	tests:=[]struct{name string;mutate func(*DueSchedule)}{
		{"missing key",func(d *DueSchedule){d.Key=nil}},
		{"missing tenant",func(d *DueSchedule){d.Record.TenantID=""}},
		{"missing schedule id",func(d *DueSchedule){d.Record.ScheduleID=""}},
		{"non-positive revision",func(d *DueSchedule){d.Record.Revision=0}},
		{"zero due time",func(d *DueSchedule){d.Record.DueAt=time.Time{}}},
		{"missing event type",func(d *DueSchedule){d.Record.Event.EventType=""}},
		{"missing partition key",func(d *DueSchedule){d.Record.Event.PartitionKey=""}},
		{"nondeterministic occurrence",func(d *DueSchedule){d.Occurrence="caller-controlled"}},
		{"missing input stream",func(d *DueSchedule){d.InputPosition.Stream=""}},
		{"missing input partition",func(d *DueSchedule){d.InputPosition.Partition=""}},
		{"negative input position",func(d *DueSchedule){d.InputPosition.NextSequence=-1}},
		{"unsupported schema",func(d *DueSchedule){d.Record.SchemaVersion=2}},
	}
	for _,tt:=range tests{t.Run(tt.name,func(t *testing.T){
		store:=newMemoryLogStore();due:=validDue;tt.mutate(&due)
		scheduler:=Scheduler{Store:store,Codec:jsonCodec{},EventsTopic:"flow.events",SchedulesTopic:"flow.schedules",GroupID:"scheduler"}
		if err:=scheduler.FireDue(context.Background(),due);err==nil{t.Fatal("FireDue() error = nil, want validation error")}
		if len(store.records)!=0{t.Fatalf("records committed: %#v",store.records)}
		if _,ok:=store.checkpoints["scheduler"];ok{t.Fatal("scheduler checkpoint advanced")}
	})}
}

func TestWorkerRejectsMisroutedEventBeforeEvaluation(t *testing.T) {
	batch := workerTestBatch(t)
	var event EventEnvelope
	if err := json.Unmarshal(batch.Records[0].Value, &event); err != nil { t.Fatal(err) }
	event.Metadata.TargetServiceID = "domain/billing"
	batch.Records[0].Value, _ = json.Marshal(event)
	store := newMemoryLogStore()
	worker := workerTest(t, store, fixedEvaluator{effects: []Effect{validWorkerEffect()}}, jsonCodec{})
	if err := worker.ProcessBatch(context.Background(), batch); err == nil {
		t.Fatal("expected mismatched service deployment to be rejected")
	}
	if len(store.records) != 0 { t.Fatalf("records committed for misrouted event: %#v", store.records) }
	if _, ok := store.checkpoints["orders-workers"]; ok { t.Fatal("checkpoint advanced for misrouted event") }
}
