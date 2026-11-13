package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang-jwt/jwt/v5"
)

func TestContextWithClaimsPreservesTenantAndScopes(t *testing.T) {
	claims := &Claims{TenantID: "tenant-a", Scopes: []string{"contracts:admin"}}
	ctx := ContextWithClaims(context.Background(), claims)

	if tenant, ok := TenantFromContext(ctx); !ok || tenant != "tenant-a" {
		t.Fatalf("TenantFromContext() = (%q, %v), want (tenant-a, true)", tenant, ok)
	}
	if !HasScope(ctx, "contracts:admin") {
		t.Fatal("HasScope() = false, want contracts:admin")
	}
	if HasScope(ctx, "rules:write") {
		t.Fatal("HasScope() = true for an ungranted scope")
	}
}

func TestHasScopeSupportsWildcard(t *testing.T) {
	ctx := ContextWithClaims(context.Background(), &Claims{TenantID: "tenant-a", Scopes: []string{"*"}})
	if !HasScope(ctx, "contracts:admin") {
		t.Fatal("wildcard scope should authorize the requested scope")
	}
}

func TestContextWithNilClaimsDoesNotInventIdentity(t *testing.T) {
	ctx := ContextWithClaims(context.Background(), nil)
	if _, ok := TenantFromContext(ctx); ok {
		t.Fatal("nil claims must not establish tenant identity")
	}
	if HasScope(ctx, "admin") {
		t.Fatal("nil claims must not grant scopes")
	}
}

func TestValidatorAcceptsSingleStringAudienceClaim(t *testing.T) {
	v := NewValidator([]byte("secret"), "flowrulz", "flowrulz-api")
	tokenString := signedAudienceToken(t, "secret")
	claims, err := v.Validate(context.Background(), tokenString)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if claims.TenantID != "tenant-a" {
		t.Fatalf("Validate().TenantID = %q, want tenant-a", claims.TenantID)
	}
}

// signedAudienceToken builds and signs a token whose audience claim is encoded
// as a single string rather than a list.
func signedAudienceToken(t *testing.T, secret string) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, &Claims{
		TenantID: "tenant-a",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:   "flowrulz",
			Audience: jwt.ClaimStrings{"flowrulz-api"},
		},
	})
	tokenString, err := token.SignedString([]byte(secret))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}
	return tokenString
}

func TestValidatorReturnsNoClaimsWhenIssuerIsRejected(t *testing.T) {
	validator := NewValidator([]byte("secret"), "other-issuer", "flowrulz-api")
	claims, err := validator.Validate(context.Background(), signedAudienceToken(t, "secret"))
	if !errors.Is(err, ErrInvalidIssuer) || claims != nil {
		t.Fatalf("Validate() = (%#v, %v), want nil claims and invalid issuer", claims, err)
	}
}

func TestMiddlewareRejectsAnUngrantedEmptyScope(t *testing.T) {
	validator := NewValidator([]byte("secret"), "flowrulz", "flowrulz-api")
	request := httptest.NewRequest(http.MethodGet, "/", nil)
	request.Header.Set("Authorization", "Bearer "+signedAudienceToken(t, "secret"))
	response := httptest.NewRecorder()
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) { t.Fatal("ungranted scope reached handler") })
	Middleware(validator, "")(handler).ServeHTTP(response, request)
	if response.Code != http.StatusForbidden {
		t.Fatalf("response status = %d, want %d", response.Code, http.StatusForbidden)
	}
}
