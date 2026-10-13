CREATE TABLE IF NOT EXISTS tenant_domains (
    tenant_id TEXT NOT NULL,
    domain_id TEXT NOT NULL,
    name TEXT NOT NULL,
    event_types TEXT[] NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, domain_id),
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS tenant_domain_services (
    tenant_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    name TEXT NOT NULL,
    endpoint_url TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, service_id),
    UNIQUE (tenant_id, name)
);

CREATE TABLE IF NOT EXISTS tenant_domain_subscriptions (
    tenant_id TEXT NOT NULL,
    service_id TEXT NOT NULL,
    domain_id TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, service_id, domain_id),
    FOREIGN KEY (tenant_id, service_id)
        REFERENCES tenant_domain_services (tenant_id, service_id) ON DELETE CASCADE,
    FOREIGN KEY (tenant_id, domain_id)
        REFERENCES tenant_domains (tenant_id, domain_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_tenant_domain_services_active
    ON tenant_domain_services (tenant_id, active);
CREATE INDEX IF NOT EXISTS idx_tenant_domain_subscriptions_domain
    ON tenant_domain_subscriptions (tenant_id, domain_id, service_id);

CREATE INDEX IF NOT EXISTS idx_tenant_domains_event_types
    ON tenant_domains USING GIN (event_types);
