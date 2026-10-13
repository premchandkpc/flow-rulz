CREATE TABLE IF NOT EXISTS quarantine (
    quarantine_id TEXT PRIMARY KEY,
    source_type TEXT NOT NULL,
    source_id TEXT NOT NULL,
    error_class TEXT NOT NULL,
    payload_ref TEXT,
    event_id TEXT,
    tenant_id TEXT,
    revision BIGINT,
    decision_hash TEXT,
    error_message TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_quarantine_tenant ON quarantine (tenant_id);
CREATE INDEX IF NOT EXISTS idx_quarantine_created ON quarantine (created_at);
