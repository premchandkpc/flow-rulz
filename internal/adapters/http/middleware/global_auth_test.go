package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/flowrulz/flow-rulz/internal/auth"
	"github.com/golang-jwt/jwt/v5"
)

func TestGlobalConfigurationRequiresResourceSpecificJWTScope(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	tests := []struct {
		path, scope string
		want        int
	}{
		{"/v1/rules/orders/revisions", "rules:admin", http.StatusNoContent},
		{"/v1/rules/orders/revisions", "contracts:admin", http.StatusForbidden},
		{"/v1/domains", "rules:admin", http.StatusForbidden},
		{"/v1/contracts", "contracts:admin", http.StatusNoContent},
		{"/v1/subscriptions", "subscriptions:admin", http.StatusNoContent},
		{"/v1/workflows/definitions", "domains:admin", http.StatusForbidden},
		{"/v1/workflows/definitions", "flowrulz:admin", http.StatusNoContent},
		{"/v1/domains", "*", http.StatusNoContent},
	}
	for _, tc := range tests {
		t.Run(tc.path+"/"+tc.scope, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, tc.path, nil)
			claims := &auth.Claims{TenantID: "principal", Scopes: []string{tc.scope}}
			req = req.WithContext(auth.ContextWithClaims(req.Context(), claims))
			req.Header.Set(trustedProxyTokenHeader, "proxy-secret")
			req.Header.Set(trustedScopesHeader, "admin")
			assertGlobalStatus(t, req, tc.want)
		})
	}
}

func TestSignedJWTScopesAreEnforcedAcrossMiddleware(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	secret := []byte("this-is-a-long-test-secret-value")
	claims := &auth.Claims{TenantID: "caller", Scopes: []string{"rules:admin"},
		RegisteredClaims: jwt.RegisteredClaims{ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour))}}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodHS256, claims).SignedString(secret)
	if err != nil {
		t.Fatal(err)
	}
	protected := auth.Middleware(auth.NewValidator(secret, "", ""))(TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})))
	for path, want := range map[string]int{"/v1/rules/orders/revisions": http.StatusNoContent, "/v1/domains": http.StatusForbidden} {
		req := httptest.NewRequest(http.MethodPost, path, nil)
		req.Header.Set("Authorization", "Bearer "+signed)
		req.Header.Set(trustedProxyTokenHeader, "proxy-secret")
		req.Header.Set(trustedScopesHeader, "admin")
		rec := httptest.NewRecorder()
		protected.ServeHTTP(rec, req)
		if rec.Code != want {
			t.Fatalf("%s: status=%d want=%d", path, rec.Code, want)
		}
	}
}

func TestGlobalConfigurationRequiresAuthenticatedProxyScope(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	for _, tc := range []struct {
		name, token, scopes string
		want                int
	}{
		{"authorized", "proxy-secret", "domains:admin", http.StatusNoContent},
		{"platform admin", "proxy-secret", "admin", http.StatusNoContent},
		{"wrong resource", "proxy-secret", "rules:admin", http.StatusForbidden},
		{"missing scope", "proxy-secret", "", http.StatusForbidden},
		{"spoofed scope", "wrong", "admin", http.StatusUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/v1/domains", nil)
			req.Header.Set(trustedProxyTokenHeader, tc.token)
			req.Header.Set(trustedScopesHeader, tc.scopes)
			assertGlobalStatus(t, req, tc.want)
		})
	}
}

func TestAuthenticatedProxyScopesReachTheHandler(t *testing.T) {
	t.Setenv("APP_ENV", "production")
	t.Setenv(trustedProxyTokenEnv, "proxy-secret")
	req := httptest.NewRequest(http.MethodPost, "/v1/contracts", nil)
	req.Header.Set(trustedProxyTokenHeader, "proxy-secret")
	req.Header.Set(trustedScopesHeader, "contracts:admin")
	rec := httptest.NewRecorder()
	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !auth.HasScope(r.Context(), "contracts:admin") || auth.HasScope(r.Context(), "admin") {
			t.Error("handler received incorrect proxy scopes")
		}
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)
	if rec.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNoContent)
	}
}

func assertGlobalStatus(t *testing.T, req *http.Request, want int) {
	t.Helper()
	rec := httptest.NewRecorder()
	TenantExtractor(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})).ServeHTTP(rec, req)
	if rec.Code != want {
		t.Fatalf("status = %d, want %d", rec.Code, want)
	}
}
