CREATE TABLE IF NOT EXISTS executions (
    execution_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    tenant_id TEXT NOT NULL,
    rule_set TEXT NOT NULL,
    revision BIGINT NOT NULL,
    decision_hash TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    trace_id TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_executions_tenant_event ON executions (tenant_id, event_id);
CREATE INDEX IF NOT EXISTS idx_executions_status ON executions (status);
