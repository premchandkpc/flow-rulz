ALTER TABLE outbox_effects
    ADD COLUMN IF NOT EXISTS claimed_by TEXT,
    ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS claim_expires_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_outbox_effects_claim_expiry
    ON outbox_effects (status, claim_expires_at)
    WHERE status = 'claimed';
