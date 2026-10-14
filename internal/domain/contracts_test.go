package domain

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestContractSchemaHashStableAndVersioned(t *testing.T) {
	s := &ContractSchema{Name:"orders.created", Version:"1.0.0", Kind:ContractKindEvent, Fields:map[string]ContractField{
		"order_id": {Type:"string", Required:true},
		"amount": {Type:"integer", Required:true},
	}}
	if err := s.NormalizeAndValidate(); err != nil { t.Fatal(err) }
	first := s.SchemaHash
	s.Description = "description changes must not alter contract identity"
	again, err := s.ComputeSchemaHash()
	if err != nil { t.Fatal(err) }
	if first != again { t.Fatalf("description changed schema hash: %s != %s", first, again) }
	s.Fields["amount"] = ContractField{Type:"number", Required:true}
	changed, err := s.ComputeSchemaHash()
	if err != nil { t.Fatal(err) }
	if first == changed { t.Fatal("semantic schema change must change hash") }
}

func TestValidatePayloadRejectsMissingUnknownAndWrongType(t *testing.T) {
	s := &ContractSchema{Name:"orders.created", Version:"1.0.0", Fields:map[string]ContractField{
		"order_id": {Type:"string", Required:true},
		"amount": {Type:"integer", Required:true},
	}}
	cases := []struct{name, payload, want string}{
		{"valid", `{"order_id":"o-1","amount":3}`, ""},
		{"missing", `{"order_id":"o-1"}`, "amount is required"},
		{"unknown", `{"order_id":"o-1","amount":3,"surprise":true}`, "not declared"},
		{"wrong type", `{"order_id":"o-1","amount":"3"}`, "must be integer"},
		{"fraction", `{"order_id":"o-1","amount":3.5}`, "must be integer"},
		{"not object", `[]`, "JSON object"},
	}
	for _, tc := range cases { t.Run(tc.name, func(t *testing.T) {
		err := s.ValidatePayload(json.RawMessage(tc.payload))
		if tc.want == "" && err != nil { t.Fatal(err) }
		if tc.want != "" && (err == nil || !strings.Contains(err.Error(), tc.want)) { t.Fatalf("got %v, want error containing %q", err, tc.want) }
	}) }
}

func TestParentOnlyAllowsExplicitChildContract(t *testing.T) {
	parent := &ContractSchema{AllowedChildren: []ContractRef{{Name:"orders.created", Version:"1.0.0", SchemaHash:"abc"}}}
	if !parent.AllowsChild(ContractRef{Name:"orders.created", Version:"1.0.0", SchemaHash:"abc"}) { t.Fatal("expected explicitly allowed child") }
	if parent.AllowsChild(ContractRef{Name:"orders.cancelled", Version:"1.0.0", SchemaHash:"abc"}) { t.Fatal("unexpected child allowed") }
	if parent.AllowsChild(ContractRef{Name:"orders.created", Version:"1.0.0", SchemaHash:"different"}) { t.Fatal("schema hash mismatch must not be allowed") }
}
