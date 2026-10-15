// Package config loads and validates application configuration.
package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// AppConfig contains app config settings.
type AppConfig struct {
	Env             string
	LogLevel        string
	HTTPAddr        string
	ShutdownTimeout time.Duration
}

// DatabaseConfig contains database config settings.
type DatabaseConfig struct {
	StorageBackend  string
	DSN             string
	MigrationsDir   string
	MongoDBURI      string
	MongoDBDatabase string
}

// NATSConfig contains natsconfig settings.
type NATSConfig struct {
	URL      string
	Stream   string
	Consumer string
	RuleSets []string
	MaxAge   time.Duration
	MaxBytes int64
	MaxMsgs  int64
}

// RedisConfig contains redis config settings.
type RedisConfig struct {
	Addr     string
	Password string
	DB       int
}

// WorkerConfig contains worker config settings.
type WorkerConfig struct {
	NumShards               uint32
	WorkerReplicas          uint32
	WorkerID                string
	ServiceID               string
	DeploymentRevision      int64
	MaxGlobalInFlight       int
	FetchBatchSize          int
	EffectPublishIntervalMS int
	EffectPublishBatchSize  int
	RetryDelayMS            int
	MaxPerKeyQueue          int
	KeyQueueWorkers         int
	BatchMode               string
	BatchMax                int
	BatchWindowMS           int
	SchedulerPollMS         int
	SchedulerBatchSize      int
	LeaseTTLMS              int
	LeaseRenewalIntervalMS  int
	NATSAckWaitMS           int
	NATSMaxDeliver          int
	HotKeyThreshold         int
	HotKeyCheckIntervalMS   int
}

// GenerationConfig contains generation config settings.
type GenerationConfig struct {
	ProtocPath          string
	ProtocGenGoPath     string
	ProtocGenGoGRPCPath string
	GenerationOutputDir string
}

// AuthConfig contains auth config settings.
type AuthConfig struct {
	JWTSecret      string   `env:"JWT_SECRET"`
	JWTIssuer      string   `env:"JWT_ISSUER"`
	JWTAudience    string   `env:"JWT_AUDIENCE"`
	RequireAuth    bool     `env:"REQUIRE_AUTH"`
	RequiredScopes []string `env:"REQUIRED_SCOPES"`
}

// Config contains config settings.
type Config struct {
	App        AppConfig
	Database   DatabaseConfig
	NATS       NATSConfig
	Redis      RedisConfig
	Worker     WorkerConfig
	Generation GenerationConfig
	Auth       AuthConfig
}

// Load provides the load.
func Load() (*Config, error) {
	if err := validateRawEnv(); err != nil {
		return nil, err
	}
	cfg := &Config{
		App:        loadAppConfig(),
		Database:   loadDatabaseConfig(),
		NATS:       loadNATSConfig(),
		Redis:      loadRedisConfig(),
		Worker:     loadWorkerConfig(),
		Generation: loadGenerationConfig(),
		Auth:       loadAuthConfig(),
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

func loadAppConfig() AppConfig {
	return AppConfig{
		Env:             getEnvNonEmpty("APP_ENV", "production"),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
		HTTPAddr:        getEnv("HTTP_ADDR", "127.0.0.1:8080"),
		ShutdownTimeout: getDurationEnv("SHUTDOWN_TIMEOUT", 30*time.Second),
	}
}

func loadDatabaseConfig() DatabaseConfig {
	return DatabaseConfig{
		StorageBackend:  getEnv("STORAGE_BACKEND", "postgres"),
		DSN:             getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/flowrulz?sslmode=disable"),
		MigrationsDir:   getEnv("MIGRATIONS_DIR", "migrations"),
		MongoDBURI:      getEnv("MONGODB_URI", ""),
		MongoDBDatabase: getEnv("MONGODB_DATABASE", ""),
	}
}

func loadNATSConfig() NATSConfig {
	return NATSConfig{
		URL:      getEnv("NATS_URL", "nats://localhost:4222"),
		Stream:   getEnv("NATS_STREAM", "flowrulz"),
		Consumer: getEnv("NATS_CONSUMER", ""),
		RuleSets: splitCSV(getEnv("NATS_RULESETS", "")),
		MaxAge:   getDurationEnv("NATS_STREAM_MAX_AGE", 0),
		MaxBytes: int64(getIntEnv("NATS_STREAM_MAX_BYTES", -1)),
		MaxMsgs:  int64(getIntEnv("NATS_STREAM_MAX_MSGS", -1)),
	}
}

func loadRedisConfig() RedisConfig {
	return RedisConfig{
		Addr:     getEnv("REDIS_ADDR", "localhost:6379"),
		Password: getEnv("REDIS_PASSWORD", ""),
		DB:       getIntEnv("REDIS_DB", 0),
	}
}

func loadWorkerConfig() WorkerConfig {
	return WorkerConfig{
		NumShards:               uint32(toUint32(getIntEnv("NUM_SHARDS", int(DefaultNumShards)))),
		WorkerReplicas:          uint32(toUint32(getIntEnv("WORKER_REPLICAS", 1))),
		WorkerID:                getEnv("WORKER_ID", ""),
		ServiceID:               strings.TrimSpace(getEnv("FLOW_SERVICE_ID", "")),
		DeploymentRevision:      int64(getIntEnv("FLOW_DEPLOYMENT_REVISION", 0)),
		MaxGlobalInFlight:       toInt(getIntEnv("MAX_GLOBAL_IN_FLIGHT", 100)),
		FetchBatchSize:          toInt(getIntEnv("FETCH_BATCH_SIZE", 100)),
		EffectPublishIntervalMS: toInt(getIntEnv("EFFECT_PUBLISH_INTERVAL_MS", 5000)),
		EffectPublishBatchSize:  toInt(getIntEnv("EFFECT_PUBLISH_BATCH_SIZE", 10)),
		RetryDelayMS:            toInt(getIntEnv("EVENT_RETRY_DELAY_MS", 5000)),
		MaxPerKeyQueue:          toInt(getIntEnv("MAX_PER_KEY_QUEUE", 100)),
		KeyQueueWorkers:         toInt(getIntEnv("KEY_QUEUE_WORKERS", 1)),
		BatchMode:               getEnv("BATCH_MODE", "none"),
		BatchMax:                toInt(getIntEnv("BATCH_MAX", 100)),
		BatchWindowMS:           toInt(getIntEnv("BATCH_WINDOW_MS", 30000)),
		SchedulerPollMS:         toInt(getIntEnv("SCHEDULER_POLL_MS", 5000)),
		SchedulerBatchSize:      toInt(getIntEnv("SCHEDULER_BATCH_SIZE", 100)),
		LeaseTTLMS:              toInt(getIntEnv("LEASE_TTL_MS", 30000)),
		LeaseRenewalIntervalMS:  toInt(getIntEnv("LEASE_RENEWAL_INTERVAL_MS", 10000)),
		NATSAckWaitMS:           toInt(getIntEnv("NATS_ACK_WAIT_MS", 30000)),
		NATSMaxDeliver:          getIntEnv("NATS_MAX_DELIVER", -1),
		HotKeyThreshold:         toInt(getIntEnv("HOT_KEY_THRESHOLD", 1000)),
		HotKeyCheckIntervalMS:   toInt(getIntEnv("HOT_KEY_CHECK_INTERVAL_MS", 10000)),
	}
}

func loadGenerationConfig() GenerationConfig {
	return GenerationConfig{
		ProtocPath:          getEnv("PROTOC_PATH", "protoc"),
		ProtocGenGoPath:     getEnv("PROTOC_GEN_GO_PATH", "protoc-gen-go"),
		ProtocGenGoGRPCPath: getEnv("PROTOC_GEN_GO_GRPC_PATH", "protoc-gen-go-grpc"),
		GenerationOutputDir: getEnv("GENERATION_OUTPUT_DIR", ""),
	}
}

func loadAuthConfig() AuthConfig {
	return AuthConfig{
		JWTSecret:      getEnv("JWT_SECRET", ""),
		JWTIssuer:      getEnv("JWT_ISSUER", "flowrulz"),
		JWTAudience:    getEnv("JWT_AUDIENCE", "flowrulz-api"),
		RequireAuth:    getBoolEnv("REQUIRE_AUTH", false),
		RequiredScopes: splitCSV(getEnv("REQUIRED_SCOPES", "")),
	}
}

// Validate provides the validate.
func (c *Config) Validate() error {
	if err := c.validateApp(); err != nil {
		return err
	}
	if err := c.validateDatabase(); err != nil {
		return err
	}
	if err := c.validateNATS(); err != nil {
		return err
	}
	if err := c.validateAuth(); err != nil {
		return err
	}
	if err := c.validateEnvironmentAuth(); err != nil {
		return err
	}
	return c.validateWorker()
}

func (c *Config) validateApp() error {
	if c.App.HTTPAddr == "" {
		return fmt.Errorf("HTTP_ADDR is required")
	}
	if c.App.ShutdownTimeout <= 0 {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be > 0")
	}
	return nil
}

func (c *Config) validateDatabase() error {
	backend := strings.ToLower(strings.TrimSpace(c.Database.StorageBackend))
	if backend == "" {
		backend = "postgres"
	}
	switch backend {
	case "postgres":
		if c.Database.DSN == "" {
			return fmt.Errorf("DATABASE_URL is required when STORAGE_BACKEND=postgres")
		}
	case "mongodb":
		if c.Database.MongoDBURI == "" {
			return fmt.Errorf("MONGODB_URI is required when STORAGE_BACKEND=mongodb")
		}
		if c.Database.MongoDBDatabase == "" {
			return fmt.Errorf("MONGODB_DATABASE is required when STORAGE_BACKEND=mongodb")
		}
	default:
		return fmt.Errorf("STORAGE_BACKEND must be either postgres or mongodb")
	}
	return nil
}

func (c *Config) validateNATS() error {
	if c.NATS.URL == "" {
		return fmt.Errorf("NATS_URL is required")
	}
	if c.NATS.MaxAge < 0 {
		return fmt.Errorf("NATS_STREAM_MAX_AGE must be >= 0")
	}
	if c.NATS.MaxBytes < -1 {
		return fmt.Errorf("NATS_STREAM_MAX_BYTES must be -1 (unlimited) or >= 1")
	}
	if c.NATS.MaxBytes == 0 {
		return fmt.Errorf("NATS_STREAM_MAX_BYTES must be -1 (unlimited) or >= 1")
	}
	if c.NATS.MaxMsgs < -1 || c.NATS.MaxMsgs == 0 {
		return fmt.Errorf("NATS_STREAM_MAX_MSGS must be -1 (unlimited) or >= 1")
	}
	seenRuleSets := make(map[string]struct{}, len(c.NATS.RuleSets))
	for _, ruleSet := range c.NATS.RuleSets {
		ruleSet = strings.TrimSpace(ruleSet)
		if ruleSet == "" {
			return fmt.Errorf("NATS_RULESETS must not contain empty values")
		}
		if len(ruleSet) > 128 {
			return fmt.Errorf("NATS_RULESETS entries must be at most 128 bytes")
		}
		if _, exists := seenRuleSets[ruleSet]; exists {
			return fmt.Errorf("NATS_RULESETS contains duplicate ruleset %q", ruleSet)
		}
		seenRuleSets[ruleSet] = struct{}{}
	}
	return nil
}

func (c *Config) validateAuth() error {
	if !c.Auth.RequireAuth {
		return nil
	}
	if c.Auth.JWTSecret == "" {
		return fmt.Errorf("JWT_SECRET is required when REQUIRE_AUTH=true")
	}
	if len(c.Auth.JWTSecret) < 32 {
		return fmt.Errorf("JWT_SECRET must be at least 32 characters")
	}
	return nil
}

func (c *Config) validateEnvironmentAuth() error {
	// Non-local deployments must establish tenant identity through either
	// first-party JWT authentication or an explicitly configured trusted proxy.
	// TenantExtractor independently validates the proxy token on every request.
	env := strings.ToLower(strings.TrimSpace(c.App.Env))
	localEnv := env == "development" || env == "dev" || env == "test"
	if !localEnv && !c.Auth.RequireAuth && strings.TrimSpace(os.Getenv("FLOWRULZ_TRUSTED_PROXY_TOKEN")) == "" {
		return fmt.Errorf("production authentication is not configured: set REQUIRE_AUTH=true with JWT_SECRET or configure FLOWRULZ_TRUSTED_PROXY_TOKEN for a trusted proxy")
	}
	return nil
}

func (c *Config) validateWorker() error {
	if c.Worker.ServiceID == "" && c.Worker.DeploymentRevision != 0 {
		return fmt.Errorf("FLOW_SERVICE_ID is required when FLOW_DEPLOYMENT_REVISION is set")
	}
	if c.Worker.ServiceID != "" && c.Worker.DeploymentRevision <= 0 {
		return fmt.Errorf("FLOW_DEPLOYMENT_REVISION must be > 0 when FLOW_SERVICE_ID is set")
	}
	if err := c.validateWorkerConcurrency(); err != nil {
		return err
	}
	if err := c.validateBatchSettings(); err != nil {
		return err
	}
	if err := c.validateSchedulerSettings(); err != nil {
		return err
	}
	if err := c.validateLeaseSettings(); err != nil {
		return err
	}
	return c.validateDeliverySettings()
}

func (c *Config) validateWorkerConcurrency() error {
	if c.Worker.NumShards == 0 {
		return fmt.Errorf("NUM_SHARDS must be > 0")
	}
	if c.Worker.WorkerReplicas == 0 {
		return fmt.Errorf("WORKER_REPLICAS must be > 0")
	}
	if c.Worker.MaxGlobalInFlight <= 0 {
		return fmt.Errorf("MAX_GLOBAL_IN_FLIGHT must be > 0")
	}
	if c.Worker.FetchBatchSize <= 0 {
		return fmt.Errorf("FETCH_BATCH_SIZE must be > 0")
	}
	if c.Worker.FetchBatchSize > c.Worker.MaxGlobalInFlight {
		return fmt.Errorf("FETCH_BATCH_SIZE must be <= MAX_GLOBAL_IN_FLIGHT")
	}
	if c.Worker.EffectPublishIntervalMS <= 0 {
		return fmt.Errorf("EFFECT_PUBLISH_INTERVAL_MS must be > 0")
	}
	if c.Worker.EffectPublishBatchSize <= 0 {
		return fmt.Errorf("EFFECT_PUBLISH_BATCH_SIZE must be > 0")
	}
	if c.Worker.RetryDelayMS <= 0 {
		return fmt.Errorf("EVENT_RETRY_DELAY_MS must be > 0")
	}
	if c.Worker.MaxPerKeyQueue <= 0 {
		return fmt.Errorf("MAX_PER_KEY_QUEUE must be > 0")
	}
	if c.Worker.KeyQueueWorkers <= 0 {
		return fmt.Errorf("KEY_QUEUE_WORKERS must be > 0")
	}
	return nil
}

func (c *Config) validateBatchSettings() error {
	batchMode := strings.ToLower(strings.TrimSpace(c.Worker.BatchMode))
	if batchMode != "" && batchMode != "none" {
		return fmt.Errorf("BATCH_MODE=%q is not supported by the worker runtime yet; batch execution must be integrated with shard leases and the key queue first", batchMode)
	}
	if c.Worker.BatchMax <= 0 {
		return fmt.Errorf("BATCH_MAX must be > 0")
	}
	if c.Worker.BatchWindowMS <= 0 {
		return fmt.Errorf("BATCH_WINDOW_MS must be > 0")
	}
	return nil
}

func (c *Config) validateSchedulerSettings() error {
	if c.Worker.SchedulerPollMS <= 0 {
		return fmt.Errorf("SCHEDULER_POLL_MS must be > 0")
	}
	if c.Worker.SchedulerBatchSize <= 0 {
		return fmt.Errorf("SCHEDULER_BATCH_SIZE must be > 0")
	}
	return nil
}

func (c *Config) validateLeaseSettings() error {
	if c.Worker.LeaseTTLMS <= 0 {
		return fmt.Errorf("LEASE_TTL_MS must be > 0")
	}
	if c.Worker.LeaseRenewalIntervalMS <= 0 {
		return fmt.Errorf("LEASE_RENEWAL_INTERVAL_MS must be > 0")
	}
	if c.Worker.LeaseRenewalIntervalMS > c.Worker.LeaseTTLMS/3 {
		return fmt.Errorf("LEASE_RENEWAL_INTERVAL_MS * 3 must be <= LEASE_TTL_MS")
	}
	return nil
}

func (c *Config) validateDeliverySettings() error {
	if c.Worker.NATSAckWaitMS <= 0 {
		return fmt.Errorf("NATS_ACK_WAIT_MS must be > 0")
	}
	if c.Worker.NATSMaxDeliver != -1 {
		return fmt.Errorf("NATS_MAX_DELIVER must be -1 (unlimited) until durable exhausted-delivery recovery is implemented")
	}
	if c.Worker.HotKeyThreshold <= 0 {
		return fmt.Errorf("HOT_KEY_THRESHOLD must be > 0")
	}
	if c.Worker.HotKeyCheckIntervalMS <= 0 {
		return fmt.Errorf("HOT_KEY_CHECK_INTERVAL_MS must be > 0")
	}
	return nil
}

func validateRawEnv() error {
	if err := validateIntegerEnvironment(); err != nil {
		return err
	}
	if err := validateShutdownTimeoutEnvironment(); err != nil {
		return err
	}
	if err := validateRequireAuthEnvironment(); err != nil {
		return err
	}
	return validateNumShardsEnvironment()
}

func validateIntegerEnvironment() error {
	keys := []string{
		"REDIS_DB", "NUM_SHARDS", "WORKER_REPLICAS", "MAX_GLOBAL_IN_FLIGHT",
		"FETCH_BATCH_SIZE", "EFFECT_PUBLISH_INTERVAL_MS", "EFFECT_PUBLISH_BATCH_SIZE",
		"EVENT_RETRY_DELAY_MS", "MAX_PER_KEY_QUEUE", "KEY_QUEUE_WORKERS",
		"BATCH_MAX", "BATCH_WINDOW_MS", "SCHEDULER_POLL_MS", "SCHEDULER_BATCH_SIZE",
		"LEASE_TTL_MS", "LEASE_RENEWAL_INTERVAL_MS", "NATS_ACK_WAIT_MS", "NATS_MAX_DELIVER",
		"HOT_KEY_THRESHOLD", "HOT_KEY_CHECK_INTERVAL_MS",
	}
	for _, key := range keys {
		value, exists := os.LookupEnv(key)
		if !exists {
			continue
		}
		if _, err := strconv.Atoi(strings.TrimSpace(value)); err != nil {
			return fmt.Errorf("%s must be an integer: %w", key, err)
		}
	}
	return nil
}

func validateShutdownTimeoutEnvironment() error {
	value, exists := os.LookupEnv("SHUTDOWN_TIMEOUT")
	if !exists {
		return nil
	}
	if _, err := time.ParseDuration(strings.TrimSpace(value)); err != nil {
		return fmt.Errorf("SHUTDOWN_TIMEOUT must be a valid duration: %w", err)
	}
	return nil
}

func validateRequireAuthEnvironment() error {
	value, exists := os.LookupEnv("REQUIRE_AUTH")
	if !exists {
		return nil
	}
	if _, err := strconv.ParseBool(strings.TrimSpace(value)); err != nil {
		return fmt.Errorf("REQUIRE_AUTH must be a boolean: %w", err)
	}
	return nil
}

func validateNumShardsEnvironment() error {
	value, exists := os.LookupEnv("NUM_SHARDS")
	if !exists {
		return nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(value))
	if err != nil {
		return fmt.Errorf("NUM_SHARDS must be an integer: %w", err)
	}
	if n <= 0 {
		return fmt.Errorf("NUM_SHARDS must be > 0")
	}
	return nil
}

func getEnvNonEmpty(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getIntEnv(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if i, err := strconv.Atoi(strings.TrimSpace(value)); err == nil {
			return i
		}
	}
	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(strings.TrimSpace(value)); err == nil {
			return d
		}
	}
	return fallback
}

func getBoolEnv(key string, fallback bool) bool {
	if value, ok := os.LookupEnv(key); ok {
		if b, err := strconv.ParseBool(strings.TrimSpace(value)); err == nil {
			return b
		}
	}
	return fallback
}

func toUint32(v int) uint32 {
	if v < 0 {
		return 0
	}
	if v > 1<<31-1 {
		return 1<<31 - 1
	}
	return uint32(v)
}

func toInt(v int) int {
	if v < 0 {
		return 0
	}
	return v
}

// WorkerID provides the worker id.
func (c *Config) WorkerID() string {
	if c.Worker.WorkerID != "" {
		return c.Worker.WorkerID
	}
	hostname, _ := os.Hostname()
	return fmt.Sprintf("%s-%d", hostname, os.Getpid())
}

// BatchWindow provides the batch window.
func (c *Config) BatchWindow() time.Duration {
	return time.Duration(c.Worker.BatchWindowMS) * time.Millisecond
}

// SchedulerPollInterval provides the scheduler poll interval.
func (c *Config) SchedulerPollInterval() time.Duration {
	return time.Duration(c.Worker.SchedulerPollMS) * time.Millisecond
}

// LeaseTTL provides the lease ttl.
func (c *Config) LeaseTTL() time.Duration {
	return time.Duration(c.Worker.LeaseTTLMS) * time.Millisecond
}

// LeaseRenewalInterval provides the lease renewal interval.
func (c *Config) LeaseRenewalInterval() time.Duration {
	return time.Duration(c.Worker.LeaseRenewalIntervalMS) * time.Millisecond
}

// NATSAckWait provides the natsack wait.
func (c *Config) NATSAckWait() time.Duration {
	return time.Duration(c.Worker.NATSAckWaitMS) * time.Millisecond
}

// HotKeyCheckInterval provides the hot key check interval.
func (c *Config) HotKeyCheckInterval() time.Duration {
	return time.Duration(c.Worker.HotKeyCheckIntervalMS) * time.Millisecond
}

// EffectPublishInterval provides the effect publish interval.
func (c *Config) EffectPublishInterval() time.Duration {
	return time.Duration(c.Worker.EffectPublishIntervalMS) * time.Millisecond
}

// EventRetryDelay provides the event retry delay.
func (c *Config) EventRetryDelay() time.Duration {
	return time.Duration(c.Worker.RetryDelayMS) * time.Millisecond
}

// ShutdownTimeoutDuration provides the shutdown timeout duration.
func (c *Config) ShutdownTimeoutDuration() time.Duration {
	return c.App.ShutdownTimeout
}

func splitCSV(s string) []string {
	if s == "" {
		return nil
	}
	var result []string
	for _, part := range strings.Split(s, ",") {
		trimmed := strings.TrimSpace(part)
		if trimmed != "" {
			result = append(result, trimmed)
		}
	}
	return result
}
