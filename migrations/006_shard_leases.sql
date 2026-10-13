CREATE TABLE IF NOT EXISTS shard_leases (
    virtual_shard INT PRIMARY KEY,
    owner TEXT NOT NULL,
    fencing_token BIGINT NOT NULL DEFAULT 0,
    expires_at TIMESTAMPTZ NOT NULL,
    routing_epoch BIGINT NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_shard_leases_owner ON shard_leases (owner);
CREATE INDEX IF NOT EXISTS idx_shard_leases_expires ON shard_leases (expires_at);
