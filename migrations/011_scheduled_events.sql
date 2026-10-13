-- Scheduled events for delayed execution
CREATE TABLE IF NOT EXISTS scheduled_events (
    event_id        TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    partition_key   TEXT NOT NULL,
    workflow_id     TEXT,
    payload         JSONB NOT NULL,
    headers         JSONB,
    scheduled_at    TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending | released | failed
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    released_at     TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_scheduled_events_due
    ON scheduled_events (status, scheduled_at)
    WHERE status = 'pending';