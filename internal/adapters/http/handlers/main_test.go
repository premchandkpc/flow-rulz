package handlers

import (
	"os"
	"testing"
)

// HTTP handler tests exercise local requests by default. Individual security
// tests override APP_ENV to production with t.Setenv when needed.
func TestMain(m *testing.M) {
	if os.Getenv("APP_ENV") == "" {
		_ = os.Setenv("APP_ENV", "development")
	}
	os.Exit(m.Run())
}
