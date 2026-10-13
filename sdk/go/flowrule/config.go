package flowrulz

import (
	"os"
	"strconv"
	"time"
)

// TransportType defines the transport mechanism
type TransportType string

const (
	TransportNATS  TransportType = "nats"
	TransportGRPC  TransportType = "grpc"
	TransportHTTP  TransportType = "http"
	TransportLocal TransportType = "local"
)

// Config holds all client configuration
// All fields can be set via environment variables (see LoadConfig)
type Config struct {
	// Transport selection
	Transport TransportType `env:"FLOWRULE_TRANSPORT,default=nats"`

	// Tenant identification (required for all transports)
	TenantID string `env:"FLOWRULE_TENANT_ID"`

	// NATS configuration
	NATSURL      string        `env:"NATS_URL,default=nats://localhost:4222"`
	NATSStream   string        `env:"NATS_STREAM,default=flowrulz"`
	NATSSubjects []string      `env:"NATS_SUBJECTS,default=events"`
	NATSRuleSets []string      `env:"NATS_RULESETS"`
	NATSAckWait  time.Duration `env:"NATS_ACK_WAIT,default=30s"`
	NATSMaxDeliv int           `env:"NATS_MAX_DELIVER,default=10"`

	// gRPC configuration
	GRPCAddr     string        `env:"FLOWRULE_GRPC_ADDR,default=localhost:9090"`
	GRPCInsecure bool          `env:"FLOWRULE_GRPC_INSECURE,default=true"`
	GRPCTimeout  time.Duration `env:"FLOWRULE_GRPC_TIMEOUT,default=10s"`

	// HTTP configuration
	HTTPAddr    string        `env:"FLOWRULE_HTTP_ADDR,default=http://localhost:8080"`
	HTTPTimeout time.Duration `env:"FLOWRULE_HTTP_TIMEOUT,default=10s"`

	// Local (embedded) configuration
	RuleSet        string `env:"FLOWRULE_RULESET"`
	RuleSource     string `env:"FLOWRULE_RULE_SOURCE"`
	RuleSourcePath string `env:"FLOWRULE_RULE_SOURCE_PATH"` // file path to rule source

	// Common
	NumShards    uint32        `env:"NUM_SHARDS,default=4096"`
	SendTimeout  time.Duration `env:"FLOWRULE_SEND_TIMEOUT,default=5s"`
	MaxRetries   int           `env:"FLOWRULE_MAX_RETRIES,default=3"`
	RetryBackoff time.Duration `env:"FLOWRULE_RETRY_BACKOFF,default=100ms"`

	// Contract validation (opt-in)
	ValidateContracts bool   `env:"FLOWRULE_VALIDATE_CONTRACTS,default=false"`
	ContractRegistry  string `env:"FLOWRULE_CONTRACT_REGISTRY"` // path to contract definitions

	// Partition key extraction
	PartitionKeyExtractor string `env:"FLOWRULE_PARTITION_KEY_EXTRACTOR"` // e.g., "CustomerID", "OrderID", or JSONPath "$.customer.id"

	// Observability
	EnableTracing bool   `env:"FLOWRULE_ENABLE_TRACING,default=false"`
	ServiceName   string `env:"FLOWRULE_SERVICE_NAME,default=flowrulz-client"`
}

// LoadConfig loads configuration from environment variables
func LoadConfig() (*Config, error) {
	cfg := &Config{
		Transport:             TransportType(getEnv("FLOWRULE_TRANSPORT", "nats")),
		TenantID:              getEnv("FLOWRULE_TENANT_ID", ""),
		NATSURL:               getEnv("NATS_URL", "nats://localhost:4222"),
		NATSStream:            getEnv("NATS_STREAM", "flowrulz"),
		NATSAckWait:           getDurationEnv("NATS_ACK_WAIT", 30*time.Second),
		NATSMaxDeliv:          getIntEnv("NATS_MAX_DELIVER", 10),
		GRPCAddr:              getEnv("FLOWRULE_GRPC_ADDR", "localhost:9090"),
		GRPCInsecure:          getBoolEnv("FLOWRULE_GRPC_INSECURE", true),
		GRPCTimeout:           getDurationEnv("FLOWRULE_GRPC_TIMEOUT", 10*time.Second),
		HTTPAddr:              getEnv("FLOWRULE_HTTP_ADDR", "http://localhost:8080"),
		HTTPTimeout:           getDurationEnv("FLOWRULE_HTTP_TIMEOUT", 10*time.Second),
		RuleSet:               getEnv("FLOWRULE_RULESET", ""),
		RuleSource:            getEnv("FLOWRULE_RULE_SOURCE", ""),
		RuleSourcePath:        getEnv("FLOWRULE_RULE_SOURCE_PATH", ""),
		NumShards:             uint32(getIntEnv("NUM_SHARDS", 4096)),
		SendTimeout:           getDurationEnv("FLOWRULE_SEND_TIMEOUT", 5*time.Second),
		MaxRetries:            getIntEnv("FLOWRULE_MAX_RETRIES", 3),
		RetryBackoff:          getDurationEnv("FLOWRULE_RETRY_BACKOFF", 100*time.Millisecond),
		ValidateContracts:     getBoolEnv("FLOWRULE_VALIDATE_CONTRACTS", false),
		ContractRegistry:      getEnv("FLOWRULE_CONTRACT_REGISTRY", ""),
		PartitionKeyExtractor: getEnv("FLOWRULE_PARTITION_KEY_EXTRACTOR", ""),
		EnableTracing:         getBoolEnv("FLOWRULE_ENABLE_TRACING", false),
		ServiceName:           getEnv("FLOWRULE_SERVICE_NAME", "flowrulz-client"),
	}

	// Parse NATS subjects and optional ruleset-specific routing.
	if subjects := getEnv("NATS_SUBJECTS", ""); subjects != "" {
		cfg.NATSSubjects = splitCSV(subjects)
	} else {
		cfg.NATSSubjects = []string{"events"}
	}
	if ruleSets := getEnv("NATS_RULESETS", ""); ruleSets != "" {
		cfg.NATSRuleSets = splitCSV(ruleSets)
	}

	// Validate required fields
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

func (c *Config) Validate() error {
	if c.TenantID == "" {
		return ErrConfig("FLOWRULE_TENANT_ID is required")
	}
	if c.NumShards == 0 {
		return ErrConfig("NUM_SHARDS must be > 0")
	}
	if c.Transport == TransportLocal {
		if c.RuleSet == "" {
			return ErrConfig("FLOWRULE_RULESET is required for local transport")
		}
		if c.RuleSource == "" && c.RuleSourcePath == "" {
			return ErrConfig("FLOWRULE_RULE_SOURCE or FLOWRULE_RULE_SOURCE_PATH is required for local transport")
		}
	}
	return nil
}

func (c *Config) WithDefaults() *Config {
	if c.NumShards == 0 {
		c.NumShards = 4096
	}
	if c.SendTimeout == 0 {
		c.SendTimeout = 5 * time.Second
	}
	if c.MaxRetries == 0 {
		c.MaxRetries = 3
	}
	if c.RetryBackoff == 0 {
		c.RetryBackoff = 100 * time.Millisecond
	}
	if len(c.NATSSubjects) == 0 {
		c.NATSSubjects = []string{"events"}
	}
	return c
}

// ErrConfig represents a configuration error
type ErrConfig string

func (e ErrConfig) Error() string {
	return "flowrulz config: " + string(e)
}

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if i, err := strconv.Atoi(v); err == nil {
			return i
		}
	}
	return fallback
}

func getBoolEnv(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		if b, err := strconv.ParseBool(v); err == nil {
			return b
		}
	}
	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return fallback
}

func splitCSV(s string) []string {
	var result []string
	for _, part := range splitString(s, ",") {
		if trimmed := trimSpace(part); trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}

// Simple string utilities to avoid importing strings
func splitString(s, sep string) []string {
	var result []string
	start := 0
	for i := 0; i <= len(s)-len(sep); i++ {
		if s[i:i+len(sep)] == sep {
			result = append(result, s[start:i])
			start = i + len(sep)
			i += len(sep) - 1
		}
	}
	result = append(result, s[start:])
	return result
}

func trimSpace(s string) string {
	start := 0
	for start < len(s) && isSpace(s[start]) {
		start++
	}
	end := len(s)
	for end > start && isSpace(s[end-1]) {
		end--
	}
	return s[start:end]
}

func isSpace(b byte) bool {
	return b == ' ' || b == '\t' || b == '\n' || b == '\r'
}
