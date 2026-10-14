-- Alter inbox table to support per-ruleset deduplication
-- Change primary key from (tenant_id, event_id) to (tenant_id, event_id, rule_set)

-- First, drop the existing primary key
ALTER TABLE inbox DROP CONSTRAINT IF EXISTS inbox_pkey;

-- Add rule_set column if not exists (should already exist from previous migrations)
ALTER TABLE inbox ADD COLUMN IF NOT EXISTS rule_set TEXT;

-- Update existing rows to have a default rule_set based on event type
-- This is a best-effort migration for existing data
UPDATE inbox SET rule_set = '' WHERE rule_set IS NULL;

-- Add new primary key
ALTER TABLE inbox ADD PRIMARY KEY (tenant_id, event_id, rule_set);

-- Create index for lookups by tenant_id, event_id (for backward compatibility)
CREATE INDEX IF NOT EXISTS idx_inbox_tenant_event ON inbox (tenant_id, event_id);