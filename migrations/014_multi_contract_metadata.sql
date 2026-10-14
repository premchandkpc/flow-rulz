-- Multi-contract metadata. Contract identity is (name, version); schema_hash pins the
-- immutable schema definition used by producers, rule revisions, and receivers.
ALTER TABLE contract_schemas
    ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'event',
    ADD COLUMN IF NOT EXISTS parent_contract_name TEXT,
    ADD COLUMN IF NOT EXISTS parent_contract_version TEXT,
    ADD COLUMN IF NOT EXISTS parent_contract_schema_hash TEXT,
    ADD COLUMN IF NOT EXISTS allowed_children JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS schema_hash TEXT;

CREATE INDEX IF NOT EXISTS idx_contract_schemas_parent
    ON contract_schemas (parent_contract_name, parent_contract_version);

-- Existing schemas are backfilled by the application on read/register. Do not invent
-- hashes in SQL because JSONB normalization must match the canonical Go representation.
