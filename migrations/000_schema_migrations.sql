-- Schema migration tracking table
-- This must be the first migration applied to enable version tracking for subsequent migrations
CREATE TABLE IF NOT EXISTS schema_migrations (
    version     TEXT NOT NULL PRIMARY KEY,  -- e.g., "001", "019"
    name        TEXT NOT NULL,              -- e.g., "rule_revisions"
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    checksum    TEXT NOT NULL               -- SHA256 of migration file content
);

CREATE INDEX IF NOT EXISTS idx_schema_migrations_applied_at ON schema_migrations (applied_at);