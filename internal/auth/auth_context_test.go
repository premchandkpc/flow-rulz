package auth

import (
	"context"
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
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, &Claims{
		TenantID: "tenant-a",
		RegisteredClaims: jwt.RegisteredClaims{
			Issuer:   "flowrulz",
			Audience: jwt.ClaimStrings{"flowrulz-api"},
		},
	})

	tokenString, err := token.SignedString([]byte("secret"))
	if err != nil {
		t.Fatalf("SignedString() error = %v", err)
	}

	claims, err := v.Validate(context.Background(), tokenString)
	if err != nil {
		t.Fatalf("Validate() error = %v", err)
	}
	if claims.TenantID != "tenant-a" {
		t.Fatalf("Validate().TenantID = %q, want tenant-a", claims.TenantID)
	}
}
