CREATE TABLE IF NOT EXISTS outbox_effects (
    effect_id TEXT PRIMARY KEY,
    execution_id TEXT NOT NULL,
    destination TEXT NOT NULL,
    name TEXT NOT NULL,
    payload JSONB NOT NULL,
    effect_type TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    attempts INT NOT NULL DEFAULT 0,
    max_attempts INT NOT NULL DEFAULT 5,
    available_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    FOREIGN KEY (execution_id) REFERENCES executions(execution_id)
);

CREATE INDEX IF NOT EXISTS idx_outbox_effects_status ON outbox_effects (status, available_at);
CREATE INDEX IF NOT EXISTS idx_outbox_effects_execution ON outbox_effects (execution_id);
