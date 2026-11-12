package config

import "testing"

func TestBrokerModeNeedsOnlyLocalRulesAndBrokerSettings(t *testing.T) {
	t.Setenv("WORKER_MODE", "broker")
	t.Setenv("WORKER_RULES_DIR", "rules")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("NATS_URL", "")
	t.Setenv("APP_ENV", "production")
	t.Setenv("REQUIRE_AUTH", "false")
	if _, err := LoadWorker(); err != nil {
		t.Fatal(err)
	}
}

func TestBrokerRejectsInvalidSettings(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Broker.Addr = "0.0.0.0:8090" },
		func(c *Config) { c.Broker.Storage = "invalid" },
		func(c *Config) { c.Broker.Storage = "file"; c.Broker.DataDir = "" },
		func(c *Config) { c.Broker.MaxBytes = 0 },
		func(c *Config) { c.Broker.MaxMessages = 10001 },
		func(c *Config) { c.Broker.AckWaitMS = 0 },
		func(c *Config) { c.Worker.RulesDir = "" },
	} {
		cfg := validConfigForTest()
		cfg.Worker.Mode = "broker"
		cfg.Worker.RulesDir = "rules"
		cfg.Broker = BrokerConfig{Transports: []string{"http"}, Addr: "127.0.0.1:8090", Storage: "memory", MaxMessages: 100, MaxBytes: 1 << 20, AckWaitMS: 1000}
		mutate(cfg)
		if err := cfg.validateBroker(); err == nil {
			t.Fatal("invalid broker settings accepted")
		}
	}
}

func TestBrokerTransportsAreIndependentlyOptional(t *testing.T) {
	for _, transport := range []string{"none", "http", "tcp", "nats"} {
		t.Run(transport, func(t *testing.T) {
			cfg := validConfigForTest()
			cfg.Worker.RulesDir = "rules"
			cfg.Broker = BrokerConfig{Transports: []string{transport}, Addr: "unused", TCPAddr: "unused", Storage: "memory", MaxMessages: 100, MaxBytes: 1 << 20, AckWaitMS: 1000, OutputTransport: "local"}
			cfg.NATS.URL = ""
			switch transport {
			case "http":
				cfg.Broker.Addr = "127.0.0.1:0"
			case "tcp":
				cfg.Broker.TCPAddr = "127.0.0.1:0"
			case "nats":
				cfg.NATS.URL = "nats://localhost:4222"
				cfg.Broker.NATSSubject = "broker.unique"
			}
			if err := cfg.validateBroker(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBrokerRejectsInvalidTransportSelections(t *testing.T) {
	for _, mutate := range []func(*Config){
		func(c *Config) { c.Broker.Transports = []string{"none", "http"} },
		func(c *Config) { c.Broker.Transports = []string{"http", "http"} },
		func(c *Config) { c.Broker.Transports = []string{"invalid"} },
		func(c *Config) { c.Broker.Transports = []string{"tcp"}; c.Broker.TCPAddr = "0.0.0.0:8091" },
		func(c *Config) { c.Broker.Transports = []string{"nats"}; c.NATS.URL = "" },
		func(c *Config) { c.Broker.Transports = []string{"nats"}; c.Broker.NATSSubject = "broker.*" },
		func(c *Config) { c.Broker.OutputTransport = "http"; c.Broker.OutputURL = "file:///tmp/broker" },
		func(c *Config) { c.Broker.OutputTransport = "tcp"; c.Broker.OutputAddr = "missing-port" },
		func(c *Config) { c.Broker.OutputTransport = "nats"; c.Broker.OutputNATSSubject = "broker.>" },
	} {
		cfg := validConfigForTest()
		cfg.Worker.RulesDir = "rules"
		cfg.Broker = BrokerConfig{Transports: []string{"none"}, Storage: "memory", MaxMessages: 100, MaxBytes: 1 << 20, AckWaitMS: 1000, OutputTransport: "local", NATSSubject: "broker.rpc", OutputNATSSubject: "peer.rpc"}
		mutate(cfg)
		if err := cfg.validateBroker(); err == nil {
			t.Fatal("invalid transport selection accepted")
		}
	}
}
