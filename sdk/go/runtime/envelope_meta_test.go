package runtime

import (
	"encoding/json"
	"testing"
	"time"
)

func TestEnvelopeMarshalNestsAndPreservesMetadata(t *testing.T) {
	env, err := NewEnvelope("orders.created", "merchant-42", "order-1007", map[string]any{"order_id":"order-1007"})
	if err != nil { t.Fatal(err) }
	env.Meta = map[string]json.RawMessage{"source":json.RawMessage(`"checkout-api"`),"producer_sequence":json.RawMessage(`1042`)}
	env.WorkflowID = "workflow-1"
	env.Headers = map[string]string{"traceparent":"trace-value"}
	payload, err := env.Marshal()
	if err != nil { t.Fatal(err) }
	var wire map[string]json.RawMessage
	if err := json.Unmarshal(payload,&wire); err != nil { t.Fatal(err) }
	if _,ok:=wire["tenant_id"];ok { t.Fatalf("tenant_id must be nested: %s",payload) }
	var meta map[string]json.RawMessage
	if err:=json.Unmarshal(wire["meta"],&meta);err!=nil{t.Fatal(err)}
	var tenant,partition,workflow,source string
	_ = json.Unmarshal(meta["tenant_id"],&tenant); _ = json.Unmarshal(meta["partition_key"],&partition)
	_ = json.Unmarshal(meta["workflow_id"],&workflow); _ = json.Unmarshal(meta["source"],&source)
	if tenant!="merchant-42"||partition!="order-1007"||workflow!="workflow-1"||source!="checkout-api"{t.Fatalf("metadata not preserved: %s",payload)}
	var seq int
	if err:=json.Unmarshal(meta["producer_sequence"],&seq);err!=nil||seq!=1042{t.Fatalf("sequence=%d err=%v",seq,err)}
	var decoded Envelope
	if err:=json.Unmarshal(payload,&decoded);err!=nil{t.Fatal(err)}
	if decoded.TenantID!=tenant||decoded.PartitionKey!=partition||decoded.Meta["source"]==nil{t.Fatalf("decoded metadata lost: %#v",decoded)}
}
