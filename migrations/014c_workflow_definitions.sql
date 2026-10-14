-- Workflow definitions for versioned workflow templates
CREATE TABLE IF NOT EXISTS workflow_definitions (
    workflow_type TEXT NOT NULL,
    version       BIGINT NOT NULL,
    states        JSONB NOT NULL,
    transitions   JSONB NOT NULL,
    rules         JSONB,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (workflow_type, version)
);

CREATE INDEX IF NOT EXISTS idx_workflow_definitions_type ON workflow_definitions (workflow_type);