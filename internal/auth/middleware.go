package auth

import (
	"net/http"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
)

// Middleware creates authentication middleware
func Middleware(validator *Validator, requiredScopes ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			claims, err := authorize(validator, w, r)
			if err != nil {
				return
			}
			// Check required scopes
			if scope, missing := missingScope(claims, requiredScopes); missing {
				responses.WriteError(w, http.StatusForbidden, "insufficient scope: "+scope, responses.CodeForbidden)
				return
			}
			next.ServeHTTP(w, r.WithContext(ContextWithClaims(r.Context(), claims)))
		})
	}
}

// authorize extracts and validates the bearer token, writing the 401 response
// when credentials are missing or rejected.
func authorize(validator *Validator, w http.ResponseWriter, r *http.Request) (*Claims, error) {
	tokenString := ExtractToken(r.Header.Get("Authorization"))
	if tokenString == "" {
		responses.WriteError(w, http.StatusUnauthorized, ErrNoCredentials.Error(), responses.CodeUnauthorized)
		return nil, ErrNoCredentials
	}
	claims, err := validator.Validate(r.Context(), tokenString)
	if err != nil {
		responses.WriteError(w, http.StatusUnauthorized, err.Error(), responses.CodeUnauthorized)
		return nil, err
	}
	return claims, nil
}

// missingScope returns the first ungranted required scope and whether one exists.
func missingScope(claims *Claims, requiredScopes []string) (string, bool) {
	for _, scope := range requiredScopes {
		if !claims.HasScope(scope) {
			return scope, true
		}
	}
	return "", false
}

// OptionalAuthMiddleware creates optional authentication middleware (doesn't require auth but extracts if present)
func OptionalAuthMiddleware(validator *Validator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			tokenString := ExtractToken(authHeader)

			if tokenString != "" {
				claims, err := validator.Validate(r.Context(), tokenString)
				if err == nil {
					ctx := ContextWithClaims(r.Context(), claims)
					r = r.WithContext(ctx)
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
