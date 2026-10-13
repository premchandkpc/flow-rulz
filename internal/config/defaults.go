package config

// DefaultNumShards is the shared fallback for the worker and HTTP publisher.
// Production deployments should configure NUM_SHARDS explicitly and keep it
// identical across publishers, schedulers, and consumers.
const DefaultNumShards uint32 = 4096
