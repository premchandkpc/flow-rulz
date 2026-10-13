-- Persist bounded scheduler retry state without changing the original scheduled_at.
ALTER TABLE scheduled_events
    ADD COLUMN IF NOT EXISTS retry_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_scheduled_events_retry_due
    ON scheduled_events (status, retry_at, scheduled_at)
    WHERE status = 'pending';
