-- Generic subscriber filters are explicit event types. Domain configuration is
-- reserved for execution Services and their ruleset assignments.
ALTER TABLE tenant_domain_services
    ADD COLUMN IF NOT EXISTS event_types TEXT[] NOT NULL DEFAULT '{}'::text[];

CREATE INDEX IF NOT EXISTS idx_tenant_domain_services_event_types
    ON tenant_domain_services USING GIN (event_types);
