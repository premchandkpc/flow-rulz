// Package config loads and validates application configuration.
package config

import (
	"fmt"
	"net"
	"net/url"
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
	Mode                    string
	RulesDir                string
	InputSubject            string
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

// BrokerConfig configures the broker hosted inside the worker process.
type BrokerConfig struct {
	Transports        []string
	TCPAddr           string
	NATSSubject       string
	OutputTransport   string
	OutputURL         string
	OutputAddr        string
	OutputNATSURL     string
	OutputNATSSubject string
	OutputToken       string
	Addr              string
	Token             string
	Storage           string
	DataDir           string
	MaxMessages       int
	MaxBytes          int64
	AckWaitMS         int
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
	Broker     BrokerConfig
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
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return cfg, nil
}

// LoadWorker validates dependencies required by the selected worker mode.
// Load retains full validation for the API and other database-backed commands.
func LoadWorker() (*Config, error) {
	cfg, err := loadConfig()
	if err != nil {
		return nil, err
	}
	if err := validateWorkerModeDependencies(cfg); err != nil {
		return nil, err
	}
	return cfg, nil
}

// validateWorkerModeDependencies runs only the validation the selected worker
// mode actually depends on.
func validateWorkerModeDependencies(cfg *Config) error {
	switch cfg.Worker.Mode {
	case "broker":
		return cfg.validateBroker()
	case "direct":
		if err := cfg.validateNATS(); err != nil {
			return err
		}
		return cfg.validateWorker()
	default:
		return cfg.Validate()
	}
}

func loadConfig() (*Config, error) {
	if err := validateRawEnv(); err != nil {
		return nil, err
	}
	return &Config{
		Broker:     loadBrokerConfig(),
		App:        loadAppConfig(),
		Database:   loadDatabaseConfig(),
		NATS:       loadNATSConfig(),
		Redis:      loadRedisConfig(),
		Worker:     loadWorkerConfig(),
		Generation: loadGenerationConfig(),
		Auth:       loadAuthConfig(),
	}, nil
}

// loadBrokerConfig reads the broker listener, storage and output settings.
func loadBrokerConfig() BrokerConfig {
	cfg := loadBrokerIngressConfig()
	cfg.OutputTransport = getEnv("BROKER_OUTPUT_TRANSPORT", "local")
	cfg.OutputURL = getEnv("BROKER_OUTPUT_URL", "")
	cfg.OutputAddr = getEnv("BROKER_OUTPUT_ADDR", "")
	cfg.OutputNATSURL = getEnv("BROKER_OUTPUT_NATS_URL", "")
	cfg.OutputNATSSubject = getEnv("BROKER_OUTPUT_NATS_SUBJECT", "flowrulz.broker")
	cfg.OutputToken = getEnv("BROKER_OUTPUT_TOKEN", "")
	return cfg
}

// loadBrokerIngressConfig reads the broker ingress transport, address and
// retention settings.
func loadBrokerIngressConfig() BrokerConfig {
	return BrokerConfig{
		Transports:  splitCSV(getEnv("BROKER_TRANSPORTS", "http")),
		TCPAddr:     getEnv("BROKER_TCP_ADDR", "127.0.0.1:8091"),
		NATSSubject: getEnv("BROKER_NATS_SUBJECT", "flowrulz.broker"),
		Addr:        getEnv("BROKER_ADDR", "127.0.0.1:8090"),
		Token:       getEnv("BROKER_TOKEN", ""),
		Storage:     getEnv("BROKER_STORAGE", "memory"),
		DataDir:     getEnv("BROKER_DATA_DIR", "data/broker"),
		MaxMessages: toInt(getIntEnv("BROKER_MAX_MESSAGES", 10000)),
		MaxBytes:    int64(getIntEnv("BROKER_MAX_BYTES", 64<<20)),
		AckWaitMS:   toInt(getIntEnv("BROKER_ACK_WAIT_MS", 30000)),
	}
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

// loadWorkerConfig reads worker mode, routing and deployment identity settings.
func loadWorkerConfig() WorkerConfig {
	cfg := loadWorkerCapacityConfig()
	cfg.Mode = getEnv("WORKER_MODE", "durable")
	cfg.RulesDir = getEnv("WORKER_RULES_DIR", "")
	cfg.InputSubject = getEnv("WORKER_INPUT_SUBJECT", "events.shard.*")
	cfg.NumShards = uint32(toUint32(getIntEnv("NUM_SHARDS", int(DefaultNumShards))))
	cfg.WorkerReplicas = uint32(toUint32(getIntEnv("WORKER_REPLICAS", 1)))
	cfg.WorkerID = getEnv("WORKER_ID", "")
	cfg.ServiceID = strings.TrimSpace(getEnv("FLOW_SERVICE_ID", ""))
	cfg.DeploymentRevision = int64(getIntEnv("FLOW_DEPLOYMENT_REVISION", 0))
	return cfg
}

// loadWorkerCapacityConfig reads concurrency, queue and effect publication settings.
func loadWorkerCapacityConfig() WorkerConfig {
	cfg := loadWorkerScheduleConfig()
	cfg.MaxGlobalInFlight = toInt(getIntEnv("MAX_GLOBAL_IN_FLIGHT", 100))
	cfg.FetchBatchSize = toInt(getIntEnv("FETCH_BATCH_SIZE", 100))
	cfg.EffectPublishIntervalMS = toInt(getIntEnv("EFFECT_PUBLISH_INTERVAL_MS", 5000))
	cfg.EffectPublishBatchSize = toInt(getIntEnv("EFFECT_PUBLISH_BATCH_SIZE", 10))
	cfg.RetryDelayMS = toInt(getIntEnv("EVENT_RETRY_DELAY_MS", 5000))
	cfg.MaxPerKeyQueue = toInt(getIntEnv("MAX_PER_KEY_QUEUE", 100))
	cfg.KeyQueueWorkers = toInt(getIntEnv("KEY_QUEUE_WORKERS", 1))
	return cfg
}

// loadWorkerScheduleConfig reads batch, scheduler, lease and delivery settings.
func loadWorkerScheduleConfig() WorkerConfig {
	return WorkerConfig{
		BatchMode:              getEnv("BATCH_MODE", "none"),
		BatchMax:               toInt(getIntEnv("BATCH_MAX", 100)),
		BatchWindowMS:          toInt(getIntEnv("BATCH_WINDOW_MS", 30000)),
		SchedulerPollMS:        toInt(getIntEnv("SCHEDULER_POLL_MS", 5000)),
		SchedulerBatchSize:     toInt(getIntEnv("SCHEDULER_BATCH_SIZE", 100)),
		LeaseTTLMS:             toInt(getIntEnv("LEASE_TTL_MS", 30000)),
		LeaseRenewalIntervalMS: toInt(getIntEnv("LEASE_RENEWAL_INTERVAL_MS", 10000)),
		NATSAckWaitMS:          toInt(getIntEnv("NATS_ACK_WAIT_MS", 30000)),
		NATSMaxDeliver:         getIntEnv("NATS_MAX_DELIVER", -1),
		HotKeyThreshold:        toInt(getIntEnv("HOT_KEY_THRESHOLD", 50)),
		HotKeyCheckIntervalMS:  toInt(getIntEnv("HOT_KEY_CHECK_INTERVAL_MS", 10000)),
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
	if err := c.validateAuthAndEnvironment(); err != nil {
		return err
	}
	return c.validateWorker()
}

// validateAuthAndEnvironment checks JWT settings and then the deployment's
// authentication posture.
func (c *Config) validateAuthAndEnvironment() error {
	if err := c.validateAuth(); err != nil {
		return err
	}
	return c.validateEnvironmentAuth()
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
		return validatePostgresSettings(c.Database)
	case "mongodb":
		return validateMongoSettings(c.Database)
	default:
		return fmt.Errorf("STORAGE_BACKEND must be either postgres or mongodb")
	}
}

// validatePostgresSettings requires a DSN for the postgres backend.
func validatePostgresSettings(db DatabaseConfig) error {
	if db.DSN == "" {
		return fmt.Errorf("DATABASE_URL is required when STORAGE_BACKEND=postgres")
	}
	return nil
}

// validateMongoSettings requires a URI and database name for the mongodb backend.
func validateMongoSettings(db DatabaseConfig) error {
	if db.MongoDBURI == "" {
		return fmt.Errorf("MONGODB_URI is required when STORAGE_BACKEND=mongodb")
	}
	if db.MongoDBDatabase == "" {
		return fmt.Errorf("MONGODB_DATABASE is required when STORAGE_BACKEND=mongodb")
	}
	return nil
}

func (c *Config) validateNATS() error {
	if err := validateNATSSettings(c.NATS); err != nil {
		return err
	}
	return validateNATSRulesets(c.NATS.RuleSets)
}

// validateNATSSettings checks the broker URL and stream retention bounds.
func validateNATSSettings(nats NATSConfig) error {
	if nats.URL == "" {
		return fmt.Errorf("NATS_URL is required")
	}
	if nats.MaxAge < 0 {
		return fmt.Errorf("NATS_STREAM_MAX_AGE must be >= 0")
	}
	if nats.MaxBytes < -1 || nats.MaxBytes == 0 {
		return fmt.Errorf("NATS_STREAM_MAX_BYTES must be -1 (unlimited) or >= 1")
	}
	if nats.MaxMsgs < -1 || nats.MaxMsgs == 0 {
		return fmt.Errorf("NATS_STREAM_MAX_MSGS must be -1 (unlimited) or >= 1")
	}
	return nil
}

// validateNATSRulesets rejects empty, oversized and duplicate ruleset names.
func validateNATSRulesets(ruleSets []string) error {
	seenRuleSets := make(map[string]struct{}, len(ruleSets))
	for _, ruleSet := range ruleSets {
		if err := validateNATSRulesetEntry(ruleSet, seenRuleSets); err != nil {
			return err
		}
	}
	return nil
}

// validateNATSRulesetEntry trims and checks one ruleset name against the set of
// names already accepted.
func validateNATSRulesetEntry(ruleSet string, seen map[string]struct{}) error {
	ruleSet = strings.TrimSpace(ruleSet)
	if ruleSet == "" {
		return fmt.Errorf("NATS_RULESETS must not contain empty values")
	}
	if len(ruleSet) > 128 {
		return fmt.Errorf("NATS_RULESETS entries must be at most 128 bytes")
	}
	if _, exists := seen[ruleSet]; exists {
		return fmt.Errorf("NATS_RULESETS contains duplicate ruleset %q", ruleSet)
	}
	seen[ruleSet] = struct{}{}
	return nil
}

func (c *Config) validateAuth() error {
	if c.Auth.RequireAuth && c.Auth.JWTSecret == "" {
		return fmt.Errorf("JWT_SECRET is required when REQUIRE_AUTH=true")
	}
	if c.Auth.JWTSecret != "" && (len(c.Auth.JWTSecret) < 32 || strings.TrimSpace(c.Auth.JWTSecret) != c.Auth.JWTSecret) {
		return fmt.Errorf("JWT_SECRET must be at least 32 characters without surrounding whitespace")
	}
	return nil
}

func (c *Config) validateEnvironmentAuth() error {
	// Non-local deployments must establish tenant identity through either
	// first-party JWT authentication or an explicitly configured trusted proxy.
	// TenantExtractor independently validates the proxy token on every request.
	env := strings.ToLower(strings.TrimSpace(c.App.Env))
	localEnv := env == "development" || env == "dev" || env == "test"
	proxyToken := os.Getenv("FLOWRULZ_TRUSTED_PROXY_TOKEN")
	if !localEnv && proxyToken != "" && (len(proxyToken) < 32 || strings.TrimSpace(proxyToken) != proxyToken) {
		return fmt.Errorf("FLOWRULZ_TRUSTED_PROXY_TOKEN must be at least 32 characters without surrounding whitespace")
	}
	if !localEnv && !c.Auth.RequireAuth && proxyToken == "" {
		return fmt.Errorf("production authentication is not configured: set REQUIRE_AUTH=true with JWT_SECRET or configure FLOWRULZ_TRUSTED_PROXY_TOKEN for a trusted proxy")
	}
	return nil
}

func (c *Config) validateWorker() error {
	if err := validateWorkerMode(c.Worker.Mode); err != nil {
		return err
	}
	if c.Worker.Mode == "direct" {
		return c.validateDirectWorker()
	}
	if err := c.validateDeploymentIdentity(); err != nil {
		return err
	}
	return c.validateWorkerRuntimeSettings()
}

// validateWorkerMode rejects modes the worker runtime does not implement.
func validateWorkerMode(mode string) error {
	if mode != "" && mode != "durable" && mode != "direct" && mode != "broker" {
		return fmt.Errorf("WORKER_MODE must be durable, direct or broker")
	}
	return nil
}

// validateDirectWorker applies the direct-mode constraints: local rules only,
// no deployment identity, no NATS rule sets and unlimited delivery.
func (c *Config) validateDirectWorker() error {
	if c.Worker.RulesDir == "" || c.Worker.InputSubject == "" {
		return fmt.Errorf("direct mode requires WORKER_RULES_DIR and WORKER_INPUT_SUBJECT")
	}
	if c.Worker.NumShards == 0 || c.Worker.NATSAckWaitMS <= 0 || c.Worker.RetryDelayMS <= 0 || c.Worker.MaxGlobalInFlight <= 0 {
		return fmt.Errorf("direct mode requires positive NUM_SHARDS, NATS_ACK_WAIT_MS, EVENT_RETRY_DELAY_MS and MAX_GLOBAL_IN_FLIGHT")
	}
	if c.Worker.ServiceID != "" || c.Worker.DeploymentRevision != 0 || len(c.NATS.RuleSets) > 0 {
		return fmt.Errorf("direct mode uses local rules and WORKER_INPUT_SUBJECT; deployment identity and NATS_RULESETS are unsupported")
	}
	if c.Worker.NATSMaxDeliver != -1 {
		return fmt.Errorf("direct mode requires unlimited NATS_MAX_DELIVER=-1")
	}
	return nil
}

// validateDeploymentIdentity prevents startup with unsupported service isolation.
func (c *Config) validateDeploymentIdentity() error {
	if c.Worker.ServiceID != "" || c.Worker.DeploymentRevision != 0 {
		return fmt.Errorf("FLOW_SERVICE_ID and FLOW_DEPLOYMENT_REVISION are unsupported until service-specific routing and ruleset isolation are implemented")
	}
	return nil
}

// validateWorkerRuntimeSettings checks concurrency, batch, scheduler, lease and
// delivery settings for every non-direct mode.
func (c *Config) validateWorkerRuntimeSettings() error {
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

func (c *Config) validateBroker() error {
	if c.Worker.RulesDir == "" || c.Worker.NumShards == 0 || c.Worker.NumShards > 4096 || c.Worker.RetryDelayMS <= 0 {
		return fmt.Errorf("broker mode requires WORKER_RULES_DIR, NUM_SHARDS between 1 and 4096 and positive EVENT_RETRY_DELAY_MS")
	}
	if err := validateBrokerLimits(c.Broker); err != nil {
		return err
	}
	if err := c.validateBrokerTransports(); err != nil {
		return err
	}
	if c.Worker.ServiceID != "" || c.Worker.DeploymentRevision != 0 {
		return fmt.Errorf("broker mode uses local rules without service deployment identity")
	}
	return nil
}

// validateBrokerLimits checks the broker storage mode, data directory and
// retention and ACK bounds.
func validateBrokerLimits(broker BrokerConfig) error {
	if broker.Storage != "memory" && broker.Storage != "file" {
		return fmt.Errorf("BROKER_STORAGE must be memory or file")
	}
	if broker.Storage == "file" && broker.DataDir == "" {
		return fmt.Errorf("BROKER_DATA_DIR is required for file storage")
	}
	if broker.MaxMessages < 2 || broker.MaxMessages > 10000 || broker.MaxBytes <= 0 || broker.MaxBytes > 64<<20 || broker.AckWaitMS < 100 || broker.AckWaitMS > 300000 {
		return fmt.Errorf("broker limits require 2..10000 messages, 1..67108864 bytes and 100..300000 ms ACK wait")
	}
	return nil
}

func validateBrokerAddress(address string) (string, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return "", err
	}
	number, err := strconv.Atoi(port)
	if err != nil || number < 0 || number > 65535 {
		return "", fmt.Errorf("port must be between 0 and 65535")
	}
	return host, nil
}

func validBrokerRPCSubject(subject string) bool {
	if subject == "" || len(subject) > 128 {
		return false
	}
	for _, part := range strings.Split(subject, ".") {
		if part == "" || strings.ContainsAny(part, "*> \t\r\n") {
			return false
		}
	}
	return true
}

// validateBrokerTransports checks the configured ingress transports and then
// the outbound publication transport.
func (c *Config) validateBrokerTransports() error {
	if err := c.validateBrokerIngressTransports(); err != nil {
		return err
	}
	return c.validateBrokerOutputTransport()
}

// validateBrokerIngressTransports checks every ingress transport for duplicate
// selection and for its own address, token and subject requirements.
func (c *Config) validateBrokerIngressTransports() error {
	seen := make(map[string]bool)
	for _, transport := range c.Broker.Transports {
		if seen[transport] {
			return fmt.Errorf("BROKER_TRANSPORTS contains duplicate %q", transport)
		}
		seen[transport] = true
		if err := c.validateBrokerTransport(transport); err != nil {
			return err
		}
	}
	return nil
}

// validateBrokerTransport checks one ingress transport selection.
func (c *Config) validateBrokerTransport(transport string) error {
	switch transport {
	case "none":
		if len(c.Broker.Transports) != 1 {
			return fmt.Errorf("BROKER_TRANSPORTS=none must be used alone")
		}
	case "http", "tcp":
		return validateBrokerListener(transport, c.Broker)
	case "nats":
		return validateBrokerNATSIngress(c.NATS.URL, c.Broker.NATSSubject)
	default:
		return fmt.Errorf("BROKER_TRANSPORTS accepts http, tcp, nats or none")
	}
	return nil
}

// validateBrokerListener checks the bind address and token requirements for the
// http and tcp ingress listeners.
func validateBrokerListener(transport string, broker BrokerConfig) error {
	address := broker.Addr
	if transport == "tcp" {
		address = broker.TCPAddr
	}
	host, err := validateBrokerAddress(address)
	if err != nil {
		return fmt.Errorf("invalid broker %s address: %w", transport, err)
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) && len(broker.Token) < 16 {
		return fmt.Errorf("non-loopback broker listeners require BROKER_TOKEN of at least 16 characters")
	}
	return nil
}

// validateBrokerNATSIngress requires a broker URL and a literal RPC subject.
func validateBrokerNATSIngress(natsURL, subject string) error {
	if natsURL == "" || !validBrokerRPCSubject(subject) {
		return fmt.Errorf("NATS ingress requires NATS_URL and a literal BROKER_NATS_SUBJECT")
	}
	return nil
}

// validateBrokerOutputTransport checks the configured outbound transport.
func (c *Config) validateBrokerOutputTransport() error {
	switch c.Broker.OutputTransport {
	case "", "local":
		return nil
	case "http":
		return validateBrokerHTTPOutput(c.Broker.OutputURL)
	case "tcp":
		return validateBrokerTCPOutput(c.Broker.OutputAddr)
	case "nats":
		return validateBrokerNATSOutput(c.Broker.OutputNATSURL, c.NATS.URL, c.Broker.OutputNATSSubject)
	default:
		return fmt.Errorf("BROKER_OUTPUT_TRANSPORT must be local, http, tcp or nats")
	}
}

// validateBrokerHTTPOutput requires an absolute URL without credentials, query
// or fragment.
func validateBrokerHTTPOutput(rawURL string) error {
	peer, err := url.Parse(rawURL)
	if err != nil || (peer.Scheme != "http" && peer.Scheme != "https") || peer.Host == "" || peer.User != nil || peer.RawQuery != "" || peer.Fragment != "" {
		return fmt.Errorf("HTTP output requires an absolute BROKER_OUTPUT_URL without credentials, query or fragment")
	}
	return nil
}

// validateBrokerTCPOutput requires a resolvable output address.
func validateBrokerTCPOutput(address string) error {
	if _, err := validateBrokerAddress(address); err != nil {
		return fmt.Errorf("invalid BROKER_OUTPUT_ADDR: %w", err)
	}
	return nil
}

// validateBrokerNATSOutput requires a NATS URL and a literal output subject.
func validateBrokerNATSOutput(outputURL, natsURL, subject string) error {
	if (outputURL == "" && natsURL == "") || !validBrokerRPCSubject(subject) {
		return fmt.Errorf("NATS output requires a NATS URL and literal BROKER_OUTPUT_NATS_SUBJECT")
	}
	return nil
}

// validateWorkerConcurrency checks shard, replica and in-flight capacity, then
// the fetch batch bounds, then effect publication and key-queue settings.
func (c *Config) validateWorkerConcurrency() error {
	if err := c.validateWorkerCapacity(); err != nil {
		return err
	}
	if err := c.validateFetchBatchSettings(); err != nil {
		return err
	}
	if err := c.validateEffectPublishSettings(); err != nil {
		return err
	}
	return c.validateKeyQueueSettings()
}

// validateWorkerCapacity checks shard, replica and global in-flight limits.
func (c *Config) validateWorkerCapacity() error {
	if c.Worker.NumShards == 0 {
		return fmt.Errorf("NUM_SHARDS must be > 0")
	}
	if c.Worker.WorkerReplicas == 0 {
		return fmt.Errorf("WORKER_REPLICAS must be > 0")
	}
	if c.Worker.MaxGlobalInFlight <= 0 {
		return fmt.Errorf("MAX_GLOBAL_IN_FLIGHT must be > 0")
	}
	return nil
}

// validateFetchBatchSettings checks the fetch batch size against its in-flight ceiling.
func (c *Config) validateFetchBatchSettings() error {
	if c.Worker.FetchBatchSize <= 0 {
		return fmt.Errorf("FETCH_BATCH_SIZE must be > 0")
	}
	if c.Worker.FetchBatchSize > c.Worker.MaxGlobalInFlight {
		return fmt.Errorf("FETCH_BATCH_SIZE must be <= MAX_GLOBAL_IN_FLIGHT")
	}
	return nil
}

// validateEffectPublishSettings checks effect publication cadence and retry delay.
func (c *Config) validateEffectPublishSettings() error {
	if c.Worker.EffectPublishIntervalMS <= 0 {
		return fmt.Errorf("EFFECT_PUBLISH_INTERVAL_MS must be > 0")
	}
	if c.Worker.EffectPublishBatchSize <= 0 {
		return fmt.Errorf("EFFECT_PUBLISH_BATCH_SIZE must be > 0")
	}
	if c.Worker.RetryDelayMS <= 0 {
		return fmt.Errorf("EVENT_RETRY_DELAY_MS must be > 0")
	}
	return nil
}

// validateKeyQueueSettings checks the per-key queue bounds.
func (c *Config) validateKeyQueueSettings() error {
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
	for _, key := range integerEnvironmentKeys() {
		if err := validateIntegerEnvVar(key); err != nil {
			return err
		}
	}
	return nil
}

// integerEnvironmentKeys lists, in validation order, the environment variables
// that must parse as integers.
func integerEnvironmentKeys() []string {
	return []string{
		"BROKER_MAX_MESSAGES", "BROKER_MAX_BYTES", "BROKER_ACK_WAIT_MS",
		"REDIS_DB", "NUM_SHARDS", "WORKER_REPLICAS", "MAX_GLOBAL_IN_FLIGHT",
		"FETCH_BATCH_SIZE", "EFFECT_PUBLISH_INTERVAL_MS", "EFFECT_PUBLISH_BATCH_SIZE",
		"EVENT_RETRY_DELAY_MS", "MAX_PER_KEY_QUEUE", "KEY_QUEUE_WORKERS",
		"BATCH_MAX", "BATCH_WINDOW_MS", "SCHEDULER_POLL_MS", "SCHEDULER_BATCH_SIZE",
		"LEASE_TTL_MS", "LEASE_RENEWAL_INTERVAL_MS", "NATS_ACK_WAIT_MS", "NATS_MAX_DELIVER",
		"HOT_KEY_THRESHOLD", "HOT_KEY_CHECK_INTERVAL_MS",
	}
}

// validateIntegerEnvVar rejects a set but non-integer environment value.
func validateIntegerEnvVar(key string) error {
	value, exists := os.LookupEnv(key)
	if !exists {
		return nil
	}
	if _, err := strconv.Atoi(strings.TrimSpace(value)); err != nil {
		return fmt.Errorf("%s must be an integer: %w", key, err)
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
