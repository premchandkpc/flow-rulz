CREATE TABLE IF NOT EXISTS rule_revisions (
    tenant_scope TEXT NOT NULL,
    rule_id TEXT NOT NULL,
    revision BIGINT NOT NULL,
    source JSONB NOT NULL,
    compiled JSONB NOT NULL,
    content_hash TEXT NOT NULL,
    compiler_version TEXT NOT NULL,
    match_mode TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_scope, rule_id, revision)
);

CREATE INDEX IF NOT EXISTS idx_rule_revisions_content_hash ON rule_revisions (content_hash);
