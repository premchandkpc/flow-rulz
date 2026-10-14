-- Add the claim/settlement fields used by the scheduled-event scheduler.
-- Existing pending events remain eligible; expired claims can be reclaimed after
-- a worker crash without editing the already-applied 011 migration.
ALTER TABLE scheduled_events
    ADD COLUMN IF NOT EXISTS claimant TEXT,
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS last_error TEXT;

CREATE INDEX IF NOT EXISTS idx_scheduled_events_expired_claims
    ON scheduled_events (status, claim_expires_at)
    WHERE status = 'claimed';

CREATE INDEX IF NOT EXISTS idx_scheduled_events_tenant_schedule
    ON scheduled_events (tenant_id, scheduled_at DESC, created_at DESC);
