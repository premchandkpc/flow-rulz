-- Contract schemas for event/command/effect definitions
CREATE TABLE IF NOT EXISTS contract_schemas (
    name          TEXT NOT NULL,
    version       TEXT NOT NULL,
    namespace     TEXT,
    description   TEXT,
    fields        JSONB NOT NULL,
    compatibility TEXT NOT NULL DEFAULT 'BACKWARD',
    owner         TEXT,
    deprecated    BOOLEAN NOT NULL DEFAULT FALSE,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (name, version)
);

CREATE INDEX IF NOT EXISTS idx_contract_schemas_name ON contract_schemas (name);