package auth

import (
	"net/http"

	"github.com/flowrulz/flow-rulz/internal/adapters/http/responses"
)

// Middleware creates authentication middleware
func Middleware(validator *Validator, requiredScopes ...string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			authHeader := r.Header.Get("Authorization")
			tokenString := ExtractToken(authHeader)

			if tokenString == "" {
				responses.WriteError(w, http.StatusUnauthorized, ErrNoCredentials.Error(), responses.CodeUnauthorized)
				return
			}

			claims, err := validator.Validate(r.Context(), tokenString)
			if err != nil {
				responses.WriteError(w, http.StatusUnauthorized, err.Error(), responses.CodeUnauthorized)
				return
			}

			// Check required scopes
			for _, scope := range requiredScopes {
				if !claims.HasScope(scope) {
					responses.WriteError(w, http.StatusForbidden, "insufficient scope: "+scope, responses.CodeForbidden)
					return
				}
			}

			ctx := ContextWithClaims(r.Context(), claims)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
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
