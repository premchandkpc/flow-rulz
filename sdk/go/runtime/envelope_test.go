package runtime

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeUsesExtensibleMetaAndPreservesUnknownFields(t *testing.T) {
	event := &Envelope{
		ID:"evt-1", Type:"orders.created", TenantID:"merchant-42", PartitionKey:"order-1",
		OccurredAt:time.Date(2026,10,15,7,30,0,0,time.UTC), Data:json.RawMessage(`{"order_id":"order-1"}`),
		Meta:map[string]json.RawMessage{"producer":json.RawMessage(`"checkout-api"`),"future":json.RawMessage(`{"enabled":true}`)},
		Headers:map[string]string{"traceparent":"trace-1"},
	}
	encoded,err:=json.Marshal(event);if err!=nil{t.Fatal(err)}
	var wire map[string]json.RawMessage
	if err:=json.Unmarshal(encoded,&wire);err!=nil{t.Fatal(err)}
	if _,ok:=wire["tenant_id"];ok{t.Fatal("tenant_id must be inside meta")}
	var meta map[string]json.RawMessage
	if err:=json.Unmarshal(wire["meta"],&meta);err!=nil{t.Fatal(err)}
	for _,key:=range []string{"tenant_id","partition_key","producer","future","headers"}{if _,ok:=meta[key];!ok{t.Errorf("meta.%s missing",key)}}
	var decoded Envelope
	if err:=json.Unmarshal(encoded,&decoded);err!=nil{t.Fatal(err)}
	if decoded.TenantID!="merchant-42"||decoded.PartitionKey!="order-1"{t.Fatalf("decoded identity = %#v",decoded)}
	if string(decoded.Meta["future"])!=`{"enabled":true}`{t.Fatalf("unknown metadata lost: %s",decoded.Meta["future"])}
}

func TestEnvelopeRejectsLegacyTopLevelIdentity(t *testing.T) {
	var event Envelope
	err:=json.Unmarshal([]byte(`{"id":"e","type":"orders.created","tenant_id":"t","partition_key":"p","occurred_at":"2026-10-15T07:30:00Z","data":{},"meta":{}}`),&event)
	if err==nil{t.Fatal("expected top-level runtime identity to be rejected")}
}
