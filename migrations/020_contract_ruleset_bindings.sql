-- Contract-to-Ruleset Bindings
-- Explicit routing from event contracts to eligible rulesets
-- Allows one event contract to fan out to multiple independently versioned rulesets

CREATE TABLE IF NOT EXISTS contract_bindings (
    id                  UUID NOT NULL DEFAULT gen_random_uuid(),
    tenant_id           TEXT NOT NULL,
    contract_name       TEXT NOT NULL,
    contract_version    TEXT NOT NULL,
    contract_hash       TEXT NOT NULL,
    rule_set            TEXT NOT NULL,
    priority            INT NOT NULL DEFAULT 0,
    activation_policy   TEXT NOT NULL DEFAULT 'automatic',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (id)
);

CREATE INDEX IF NOT EXISTS idx_contract_bindings_tenant_contract
    ON contract_bindings (tenant_id, contract_name, contract_version);

CREATE INDEX IF NOT EXISTS idx_contract_bindings_tenant_ruleset
    ON contract_bindings (tenant_id, rule_set);

-- Activation policy values: 'automatic' (activate when rule exists), 'manual' (require explicit activation)