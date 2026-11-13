package config

import "testing"

func TestLoadDirectWorkerWithoutDatabaseOrAPIAuth(t *testing.T) {
	t.Setenv("WORKER_MODE", "direct")
	t.Setenv("WORKER_RULES_DIR", "rules")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("APP_ENV", "production")
	t.Setenv("REQUIRE_AUTH", "false")
	t.Setenv("NATS_RULESETS", "")
	t.Setenv("FLOW_SERVICE_ID", "")
	t.Setenv("FLOW_DEPLOYMENT_REVISION", "0")
	if _, err := LoadWorker(); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(); err == nil {
		t.Fatal("API validation must still require database configuration")
	}
}

// assertDirectWorkerRejected requires validateWorker to reject a direct-mode
// configuration after mutate invalidates it.
func assertDirectWorkerRejected(t *testing.T, mutate func(*Config)) {
	t.Helper()
	cfg := validConfigForTest()
	cfg.Worker.Mode = "direct"
	cfg.Worker.RulesDir = "rules"
	cfg.Worker.InputSubject = "events.shard.*"
	mutate(cfg)
	if err := cfg.validateWorker(); err == nil {
		t.Fatal("accepted invalid direct configuration")
	}
}

func TestDirectWorkerRejectsInvalidSettings(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Worker.Mode = "typo" },
		func(c *Config) { c.Worker.RulesDir = "" },
		func(c *Config) { c.Worker.InputSubject = "" },
		func(c *Config) { c.Worker.NATSAckWaitMS = 0 },
		func(c *Config) { c.Worker.RetryDelayMS = 0 },
		func(c *Config) { c.NATS.RuleSets = []string{"a"} },
		func(c *Config) { c.Worker.ServiceID = "b" },
	} {
		assertDirectWorkerRejected(t, mutate)
	}
}
