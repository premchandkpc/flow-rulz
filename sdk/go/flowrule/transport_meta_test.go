package flowrulz

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeMarshalNestsAndPreservesMetadata(t *testing.T) {
	env:=Envelope{
		ID:"evt-1",Type:"orders.created",TenantID:"merchant-42",PartitionKey:"order-1007",
		OccurredAt:time.Date(2026,10,15,7,30,0,0,time.UTC),Data:json.RawMessage(`{"order_id":"order-1007"}`),
		Meta:map[string]json.RawMessage{"source":json.RawMessage(`"checkout-api"`),"producer_sequence":json.RawMessage(`1042`)},
		CorrelationID:"corr-1",Headers:map[string]string{"traceparent":"trace-value"},
	}
	payload,err:=json.Marshal(env);if err!=nil{t.Fatal(err)}
	var wire map[string]json.RawMessage
	if err:=json.Unmarshal(payload,&wire);err!=nil{t.Fatal(err)}
	for _,key:=range []string{"tenant_id","partition_key","correlation_id","headers"}{if _,ok:=wire[key];ok{t.Fatalf("%s must be nested: %s",key,payload)}}
	var meta map[string]json.RawMessage
	if err:=json.Unmarshal(wire["meta"],&meta);err!=nil{t.Fatal(err)}
	var tenant,partition,correlation,source string
	_ = json.Unmarshal(meta["tenant_id"],&tenant);_ = json.Unmarshal(meta["partition_key"],&partition)
	_ = json.Unmarshal(meta["correlation_id"],&correlation);_ = json.Unmarshal(meta["source"],&source)
	if tenant!="merchant-42"||partition!="order-1007"||correlation!="corr-1"||source!="checkout-api"{t.Fatalf("meta mismatch: %s",payload)}
	var decoded Envelope
	if err:=json.Unmarshal(payload,&decoded);err!=nil{t.Fatal(err)}
	if decoded.TenantID!=tenant||decoded.PartitionKey!=partition||decoded.Meta["source"]==nil{t.Fatalf("round trip lost metadata: %#v",decoded)}
}
