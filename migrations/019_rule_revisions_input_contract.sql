-- Add input_contract column to rule_revisions for contract enforcement
-- Stores the ContractRef as JSONB: {"name": "...", "version": "...", "schema_hash": "..."}
ALTER TABLE rule_revisions
ADD COLUMN IF NOT EXISTS input_contract JSONB;

COMMENT ON COLUMN rule_revisions.input_contract IS 'ContractRef JSON: {name, version, schema_hash} that this rule revision requires for input events';