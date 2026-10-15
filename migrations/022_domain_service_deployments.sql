ALTER TABLE tenant_domains
    ADD COLUMN IF NOT EXISTS deployments JSONB NOT NULL DEFAULT '[]'::jsonb;

ALTER TABLE tenant_domains
    ADD CONSTRAINT tenant_domains_deployments_array_check
    CHECK (jsonb_typeof(deployments) = 'array');
