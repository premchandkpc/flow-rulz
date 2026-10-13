package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func validConfigForTest() *Config {
	return &Config{
		App:      AppConfig{Env: "development", HTTPAddr: "127.0.0.1:8080", ShutdownTimeout: time.Second},
		Database: DatabaseConfig{StorageBackend: "postgres", DSN: "postgres://localhost/flowrulz"},
		NATS:     NATSConfig{URL: "nats://localhost:4222"},
		Worker: WorkerConfig{
			NumShards:               1,
			WorkerReplicas:          1,
			MaxGlobalInFlight:       1,
			FetchBatchSize:          1,
			EffectPublishIntervalMS: 1,
			EffectPublishBatchSize:  1,
			RetryDelayMS:            1,
			MaxPerKeyQueue:          1,
			KeyQueueWorkers:         1,
			BatchMax:                1,
			BatchWindowMS:           1,
			SchedulerPollMS:         1,
			SchedulerBatchSize:      1,
			LeaseTTLMS:              3,
			LeaseRenewalIntervalMS:  1,
			NATSAckWaitMS:           1500,
			NATSMaxDeliver:          -1,
			HotKeyThreshold:         1,
			HotKeyCheckIntervalMS:   2500,
		},
	}
}

func TestValidateRejectsDuplicateNATSRulesets(t *testing.T) {
	cfg := validConfigForTest()
	cfg.NATS.RuleSets = []string{"orders.created", "orders.created"}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "duplicate ruleset") {
		t.Fatalf("Validate() error = %v, want duplicate ruleset error", err)
	}
}

func TestValidateRejectsOversizedNATSRuleset(t *testing.T) {
	cfg := validConfigForTest()
	cfg.NATS.RuleSets = []string{strings.Repeat("x", 129)}
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "at most 128 bytes") {
		t.Fatalf("Validate() error = %v, want ruleset length error", err)
	}
}

func TestValidateRejectsInvalidWorkerRuntimeSettings(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*Config)
	}{
		{name: "fetch batch exceeds in-flight limit", mutate: func(c *Config) { c.Worker.FetchBatchSize = 2 }},
		{name: "worker replicas must be positive", mutate: func(c *Config) { c.Worker.WorkerReplicas = 0 }},
		{name: "lease renewal exceeds safe interval", mutate: func(c *Config) { c.Worker.LeaseRenewalIntervalMS = 2 }},
		{name: "NATS ack wait", mutate: func(c *Config) { c.Worker.NATSAckWaitMS = 0 }},
		{name: "NATS max deliver", mutate: func(c *Config) { c.Worker.NATSMaxDeliver = 0 }},
		{name: "hot key threshold", mutate: func(c *Config) { c.Worker.HotKeyThreshold = 0 }},
		{name: "hot key check interval", mutate: func(c *Config) { c.Worker.HotKeyCheckIntervalMS = 0 }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := validConfigForTest()
			tt.mutate(cfg)
			if err := cfg.Validate(); err == nil {
				t.Fatal("Validate() error = nil, want invalid setting error")
			}
		})
	}
}

func TestWorkerRuntimeDurationHelpers(t *testing.T) {
	cfg := validConfigForTest()

	if got, want := cfg.NATSAckWait(), 1500*time.Millisecond; got != want {
		t.Fatalf("NATSAckWait() = %s, want %s", got, want)
	}
	if got, want := cfg.HotKeyCheckInterval(), 2500*time.Millisecond; got != want {
		t.Fatalf("HotKeyCheckInterval() = %s, want %s", got, want)
	}
}

func TestLoadRejectsMalformedWorkerRuntimeSetting(t *testing.T) {
	t.Setenv("NATS_ACK_WAIT_MS", "not-a-duration")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want malformed integer error")
	}
}

func TestLoadRejectsMalformedWorkerReplicas(t *testing.T) {
	t.Setenv("WORKER_REPLICAS", "many")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "WORKER_REPLICAS") {
		t.Fatalf("Load() error = %v, want malformed WORKER_REPLICAS error", err)
	}
}

func TestLoadRejectsMalformedShutdownTimeout(t *testing.T) {
	t.Setenv("SHUTDOWN_TIMEOUT", "later-ish")
	if _, err := Load(); err == nil {
		t.Fatal("Load() error = nil, want malformed duration error")
	}
}

func TestLoadDefaultsHTTPAddrToLoopback(t *testing.T) {
	t.Setenv("APP_ENV", "development")
	previous, wasSet := os.LookupEnv("HTTP_ADDR")
	if err := os.Unsetenv("HTTP_ADDR"); err != nil {
		t.Fatalf("Unsetenv(HTTP_ADDR): %v", err)
	}
	t.Cleanup(func() {
		if wasSet {
			_ = os.Setenv("HTTP_ADDR", previous)
			return
		}
		_ = os.Unsetenv("HTTP_ADDR")
	})

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.App.HTTPAddr, "127.0.0.1:8080"; got != want {
		t.Fatalf("HTTPAddr = %q, want %q", got, want)
	}
}

func TestValidateRejectsBatchModeWithoutLeaseAndQueueIntegration(t *testing.T) {
	cfg := validConfigForTest()
	cfg.Worker.BatchMode = "accumulate"
	err := cfg.Validate()
	if err == nil {
		t.Fatal("Validate() error = nil, want unsupported batch-mode error")
	}
	if !strings.Contains(err.Error(), "shard leases and the key queue") {
		t.Fatalf("Validate() error = %v, want explanation of missing runtime integration", err)
	}
}

func TestValidateRejectsProductionWithoutAuthentication(t *testing.T) {
	t.Setenv("FLOWRULE_TRUSTED_PROXY_TOKEN", "")
	cfg := validConfigForTest()
	cfg.App.Env = "production"
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "production authentication is not configured") {
		t.Fatalf("Validate() error = %v, want production authentication configuration error", err)
	}
}

func TestValidateAllowsProductionTrustedProxyAuthentication(t *testing.T) {
	t.Setenv("FLOWRULE_TRUSTED_PROXY_TOKEN", "test-proxy-secret")
	cfg := validConfigForTest()
	cfg.App.Env = "production"
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want trusted-proxy configuration to be accepted", err)
	}
}

func TestValidateAllowsProductionJWTAuthentication(t *testing.T) {
	t.Setenv("FLOWRULE_TRUSTED_PROXY_TOKEN", "")
	cfg := validConfigForTest()
	cfg.App.Env = "production"
	cfg.Auth.RequireAuth = true
	cfg.Auth.JWTSecret = strings.Repeat("s", 32)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want valid JWT configuration to be accepted", err)
	}
}

func TestLoadRejectsMalformedRequireAuth(t *testing.T) {
	t.Setenv("REQUIRE_AUTH", "definitely-not-a-boolean")
	if _, err := Load(); err == nil || !strings.Contains(err.Error(), "REQUIRE_AUTH must be a boolean") {
		t.Fatalf("Load() error = %v, want invalid REQUIRE_AUTH boolean error", err)
	}
}

func TestLoadDefaultsEnvironmentToProduction(t *testing.T) {
	t.Setenv("APP_ENV", "")
	t.Setenv("FLOWRULE_TRUSTED_PROXY_TOKEN", "test-proxy-secret")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load() error = %v", err)
	}
	if got, want := cfg.App.Env, "production"; got != want {
		t.Fatalf("App.Env = %q, want %q", got, want)
	}
}

func TestValidateRejectsFiniteNATSMaxDeliverUntilRecoveryExists(t *testing.T) {
	cfg := validConfigForTest()
	cfg.Worker.NATSMaxDeliver = 10
	err := cfg.Validate()
	if err == nil || !strings.Contains(err.Error(), "NATS_MAX_DELIVER must be -1 (unlimited)") {
		t.Fatalf("Validate() error = %v, want finite NATS delivery limit rejection", err)
	}
}

func TestValidateAllowsUnlimitedNATSMaxDeliver(t *testing.T) {
	cfg := validConfigForTest()
	cfg.Worker.NATSMaxDeliver = -1
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() error = %v, want unlimited NATS delivery limit to be accepted", err)
	}
}
