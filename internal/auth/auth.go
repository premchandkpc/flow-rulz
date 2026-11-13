// Package auth validates authentication tokens and exposes authentication errors.
package auth

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	// ErrNoCredentials indicates that authentication credentials were not provided.
	ErrNoCredentials = errors.New("no credentials provided")
	// ErrInvalidToken indicates that a supplied token is invalid.
	ErrInvalidToken = errors.New("invalid token")
	// ErrTokenExpired indicates that a supplied token has expired.
	ErrTokenExpired = errors.New("token expired")
	// ErrInvalidIssuer indicates that the token issuer is not trusted.
	ErrInvalidIssuer = errors.New("invalid issuer")
	// ErrInvalidAudience indicates that the token audience is not trusted.
	ErrInvalidAudience = errors.New("invalid audience")
	// ErrMissingTenantID indicates that the token does not identify a tenant.
	ErrMissingTenantID = errors.New("tenant ID not found in token")
)

// Claims represents the JWT claims for FlowRulz
type Claims struct {
	TenantID string   `json:"tenant_id"`
	Scopes   []string `json:"scopes,omitempty"`
	jwt.RegisteredClaims
}

// Validator validates JWT tokens
type Validator struct {
	SigningKey       []byte
	ExpectedIssuer   string
	ExpectedAudience string
	Clock            func() time.Time
}

// NewValidator creates a new validator
func NewValidator(signingKey []byte, issuer, audience string) *Validator {
	return &Validator{
		SigningKey:       signingKey,
		ExpectedIssuer:   issuer,
		ExpectedAudience: audience,
		Clock:            time.Now,
	}
}

// Validate validates a JWT token string and returns claims
func (v *Validator) Validate(_ context.Context, tokenString string) (*Claims, error) {
	if tokenString == "" {
		return nil, ErrNoCredentials
	}
	claims, err := v.parseClaims(tokenString)
	if err != nil {
		return nil, err
	}
	if err := v.validateClaimValues(claims); err != nil {
		return nil, err
	}
	return claims, nil
}

// parseClaims parses the signed token and classifies parse failures as either an
// expired token or an invalid token.
func (v *Validator) parseClaims(tokenString string) (*Claims, error) {
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, v.signingKeyFunc, jwt.WithTimeFunc(v.Clock))
	if err != nil {
		if errors.Is(err, jwt.ErrTokenExpired) {
			return nil, ErrTokenExpired
		}
		return nil, ErrInvalidToken
	}
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}
	return claims, nil
}

// signingKeyFunc rejects tokens that are not HMAC-signed and otherwise supplies
// the validator key.
func (v *Validator) signingKeyFunc(token *jwt.Token) (interface{}, error) {
	if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
		return nil, ErrInvalidToken
	}
	return v.SigningKey, nil
}

// validateClaimValues enforces the issuer, audience and tenant requirements on
// already-verified claims.
func (v *Validator) validateClaimValues(claims *Claims) error {
	if v.ExpectedIssuer != "" && claims.Issuer != v.ExpectedIssuer {
		return ErrInvalidIssuer
	}
	if v.ExpectedAudience != "" && !audienceContains(claims.Audience, v.ExpectedAudience) {
		return ErrInvalidAudience
	}
	if claims.TenantID == "" {
		return ErrMissingTenantID
	}
	return nil
}

// audienceContains reports whether the expected audience appears in the claim list.
func audienceContains(audience jwt.ClaimStrings, expected string) bool {
	for _, aud := range audience {
		if aud == expected {
			return true
		}
	}
	return false
}

// ExtractToken extracts Bearer token from Authorization header
func ExtractToken(authHeader string) string {
	if authHeader == "" {
		return ""
	}
	if len(authHeader) > 7 && strings.EqualFold(authHeader[:7], "Bearer ") {
		return authHeader[7:]
	}
	return ""
}

// TenantFromContext extracts tenant ID from context
func TenantFromContext(ctx context.Context) (string, bool) {
	tenantID, ok := ctx.Value(tenantKey{}).(string)
	return tenantID, ok
}

// ContextWithTenant returns a new context with tenant ID.
func ContextWithTenant(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, tenantKey{}, tenantID)
}

// ContextWithClaims stores validated claims for downstream authorization checks.
// Call this only after Validator.Validate succeeds.
func ContextWithClaims(ctx context.Context, claims *Claims) context.Context {
	if claims == nil {
		return ctx
	}
	ctx = ContextWithTenant(ctx, claims.TenantID)
	return context.WithValue(ctx, claimsKey{}, claims)
}

// HasScope reports whether validated claims in ctx contain the requested scope.
func HasScope(ctx context.Context, scope string) bool {
	claims, ok := ctx.Value(claimsKey{}).(*Claims)
	return ok && claims != nil && claims.HasScope(scope)
}

type tenantKey struct{}
type claimsKey struct{}

// HasScope checks if claims have a specific scope
func (c *Claims) HasScope(scope string) bool {
	for _, s := range c.Scopes {
		if s == scope || s == "*" {
			return true
		}
	}
	return false
}

// IsAdmin checks if claims have admin scope
func (c *Claims) IsAdmin() bool {
	return c.HasScope("admin") || c.HasScope("flowrulz:admin")
}
