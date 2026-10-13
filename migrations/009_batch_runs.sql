-- Batching support: inbox carries grouping keys and payload so a scheduler
-- can synthesize batch events without going back to the broker.
ALTER TABLE inbox
    ADD COLUMN IF NOT EXISTS partition_key TEXT,
    ADD COLUMN IF NOT EXISTS rule_set TEXT,
    ADD COLUMN IF NOT EXISTS payload JSONB,
    ADD COLUMN IF NOT EXISTS batch_id TEXT;

CREATE INDEX IF NOT EXISTS idx_inbox_unbatched
    ON inbox (tenant_id, partition_key, first_seen_at)
    WHERE status = 'committed' AND batch_id IS NULL;

CREATE TABLE IF NOT EXISTS batch_runs (
    batch_id TEXT PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    partition_key TEXT NOT NULL,
    rule_set TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'formed',
    member_count INT NOT NULL DEFAULT 0,
    execution_id TEXT,
    decision_hash TEXT,
    window_from TIMESTAMPTZ,
    window_to TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_batch_runs_status
    ON batch_runs (status, created_at);
