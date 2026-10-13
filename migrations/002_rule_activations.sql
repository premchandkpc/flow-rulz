CREATE TABLE IF NOT EXISTS rule_activations (
    tenant_scope TEXT NOT NULL,
    rule_set TEXT NOT NULL,
    revision BIGINT NOT NULL,
    version BIGINT NOT NULL DEFAULT 1,
    actor TEXT NOT NULL DEFAULT 'system',
    activated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_scope, rule_set),
    FOREIGN KEY (tenant_scope, rule_set, revision) REFERENCES rule_revisions(tenant_scope, rule_id, revision)
);
