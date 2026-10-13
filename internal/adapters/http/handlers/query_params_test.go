package handlers

import (
	"net/http/httptest"
	"testing"
)

func TestParsePagination(t *testing.T) {
	tests := []struct {
		name       string
		query      string
		defaultLim int
		maxLim     int
		maxOffset  int
		want       Pagination
		wantErr    bool
	}{
		{name: "defaults", query: "", defaultLim: 100, maxLim: 500, maxOffset: 10000, want: Pagination{Limit: 100, Offset: 0}},
		{name: "valid values", query: "?limit=25&offset=40", defaultLim: 100, maxLim: 500, maxOffset: 10000, want: Pagination{Limit: 25, Offset: 40}},
		{name: "limit too small", query: "?limit=0", defaultLim: 100, maxLim: 500, maxOffset: 10000, wantErr: true},
		{name: "limit too large", query: "?limit=501", defaultLim: 100, maxLim: 500, maxOffset: 10000, wantErr: true},
		{name: "invalid limit", query: "?limit=abc", defaultLim: 100, maxLim: 500, maxOffset: 10000, wantErr: true},
		{name: "negative offset", query: "?offset=-1", defaultLim: 100, maxLim: 500, maxOffset: 10000, wantErr: true},
		{name: "offset too large", query: "?offset=10001", defaultLim: 100, maxLim: 500, maxOffset: 10000, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/items"+tt.query, nil)
			got, err := parsePagination(req, tt.defaultLim, tt.maxLim, tt.maxOffset)
			if (err != nil) != tt.wantErr {
				t.Fatalf("parsePagination() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && got != tt.want {
				t.Fatalf("parsePagination() = %#v, want %#v", got, tt.want)
			}
		})
	}
}
