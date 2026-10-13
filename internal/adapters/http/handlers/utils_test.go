package handlers

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestDecodeRequestBody(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		wantErr     bool
	}{
		{name: "valid JSON", contentType: "application/json", body: `{"name":"rule"}`},
		{name: "valid YAML", contentType: "application/yaml", body: "name: rule\n"},
		{name: "reject trailing JSON value", contentType: "application/json", body: `{"name":"rule"} {"name":"other"}`, wantErr: true},
		{name: "reject malformed trailing JSON", contentType: "application/json", body: `{"name":"rule"} {`, wantErr: true},
		{name: "reject multiple YAML documents", contentType: "application/yaml", body: "name: rule\n---\nname: other\n", wantErr: true},
		{name: "reject malformed trailing YAML", contentType: "application/yaml", body: "name: rule\n---\n: invalid\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/items", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			var got map[string]string
			err := decodeRequestBody(req, &got)
			if (err != nil) != tt.wantErr {
				t.Fatalf("decodeRequestBody() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && got["name"] != "rule" {
				t.Fatalf("decoded name = %q, want %q", got["name"], "rule")
			}
		})
	}
}

func TestDecodeRawRequestBody(t *testing.T) {
	tests := []struct {
		name        string
		contentType string
		body        string
		want        string
		wantErr     bool
	}{
		{name: "JSON preserved", contentType: "application/json", body: `{"name":"rule"}`, want: `{"name":"rule"}`},
		{name: "YAML normalized", contentType: "application/yaml", body: "name: rule\n", want: `{"name":"rule"}`},
		{name: "reject trailing JSON value", contentType: "application/json", body: `{"name":"rule"} {}`, wantErr: true},
		{name: "reject multiple YAML documents", contentType: "application/yaml", body: "name: rule\n---\nname: other\n", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("POST", "/items", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			got, err := decodeRawRequestBody(req)
			if (err != nil) != tt.wantErr {
				t.Fatalf("decodeRawRequestBody() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if tt.contentType == "application/json" && string(got) != tt.want {
				t.Fatalf("JSON body = %s, want %s", got, tt.want)
			}
			if tt.contentType == "application/yaml" {
				var value map[string]string
				if err := json.Unmarshal(got, &value); err != nil {
					t.Fatalf("normalized YAML is not JSON: %v", err)
				}
				if value["name"] != "rule" {
					t.Fatalf("normalized name = %q, want %q", value["name"], "rule")
				}
			}
		})
	}
}
