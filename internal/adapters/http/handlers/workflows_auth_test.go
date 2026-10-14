package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/flowrulz/flow-rulz/internal/auth"
)

func TestRequireWorkflowDefinitionAdminAllowsDevelopment(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/definitions", nil)
	rec := httptest.NewRecorder()

	if !requireWorkflowDefinitionAdmin(rec, req) {
		t.Fatalf("development write rejected: status=%d body=%s", rec.Code, rec.Body.String())
	}
}

func TestRequireWorkflowDefinitionAdminFailsClosedOutsideDevelopment(t *testing.T) {
	for _, env := range []string{"", "test", "production"} {
		t.Run(env, func(t *testing.T) {
			t.Setenv("APP_ENV", env)
			req := httptest.NewRequest(http.MethodPost, "/v1/workflows/definitions", nil)
			rec := httptest.NewRecorder()

			if requireWorkflowDefinitionAdmin(rec, req) {
				t.Fatal("write accepted without platform admin scope")
			}
			if rec.Code != http.StatusForbidden {
				t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
			}
		})
	}
}

func TestRequireWorkflowDefinitionAdminAcceptsPlatformAdminScope(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	ctx := auth.ContextWithClaims(httptest.NewRequest(http.MethodPost, "/v1/workflows/definitions", nil).Context(), &auth.Claims{
		TenantID: "tenant-a",
		Scopes:   []string{"flowrulz:admin"},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/workflows/definitions", nil).WithContext(ctx)
	rec := httptest.NewRecorder()

	if !requireWorkflowDefinitionAdmin(rec, req) {
		t.Fatalf("admin write rejected: status=%d body=%s", rec.Code, rec.Body.String())
	}
}
