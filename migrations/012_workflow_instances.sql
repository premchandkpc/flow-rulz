-- Workflow instances for long-running business processes
CREATE TABLE IF NOT EXISTS workflow_instances (
    workflow_id     TEXT PRIMARY KEY,
    tenant_id       TEXT NOT NULL,
    workflow_type   TEXT NOT NULL,
    state           TEXT NOT NULL,
    version         BIGINT NOT NULL DEFAULT 1,
    current_revision BIGINT,
    context         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_workflow_tenant_state ON workflow_instances (tenant_id, state);
CREATE INDEX IF NOT EXISTS idx_workflow_updated ON workflow_instances (updated_at);