CREATE TABLE IF NOT EXISTS inbox (
    tenant_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'processing',
    execution_id TEXT,
    first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    committed_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, event_id)
);
