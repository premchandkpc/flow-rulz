//! Environment-backed configuration for the Rust worker runtime.
use anyhow::{bail, Result};
use std::{env, time::Duration};

#[derive(Debug, Clone)]
pub(crate) struct Config {
    pub(crate) nats_url: String,
    pub(crate) stream: String,
    pub(crate) durable: String,
    pub(crate) database_url: String,
    pub(crate) worker_id: String,
    pub(crate) service_id: Option<String>,
    pub(crate) deployment_revision: Option<i64>,
    pub(crate) num_shards: u32,
    pub(crate) worker_replicas: u32,
    pub(crate) lease_ttl: Duration,
    pub(crate) lease_renew_interval: Duration,
    pub(crate) ack_wait: Duration,
    pub(crate) retry_delay: Duration,
    pub(crate) fetch_batch_size: usize,
    pub(crate) rulesets: Vec<String>,
    pub(crate) effect_poll_interval: Duration,
    pub(crate) scheduler_poll_interval: Duration,
    pub(crate) scheduler_batch_size: i64,
}

struct ConnectionSettings {
    nats_url: String,
    stream: String,
    durable: String,
    database_url: String,
    worker_id: String,
    service_id: Option<String>,
    deployment_revision: Option<i64>,
}

struct LeaseSettings {
    num_shards: u32,
    worker_replicas: u32,
    lease_ttl: Duration,
    lease_renew_interval: Duration,
}

struct DeliverySettings {
    ack_wait: Duration,
    retry_delay: Duration,
    fetch_batch_size: usize,
    rulesets: Vec<String>,
    effect_poll_interval: Duration,
    scheduler_poll_interval: Duration,
    scheduler_batch_size: i64,
}

impl Config {
    pub(crate) fn from_env() -> Result<Self> {
        let connection = load_connection_settings()?;
        let leases = load_lease_settings()?;
        let delivery = load_delivery_settings()?;

        let config = Self {
            nats_url: connection.nats_url,
            stream: connection.stream,
            durable: connection.durable,
            database_url: connection.database_url,
            worker_id: connection.worker_id,
            service_id: connection.service_id,
            deployment_revision: connection.deployment_revision,
            num_shards: leases.num_shards,
            worker_replicas: leases.worker_replicas,
            lease_ttl: leases.lease_ttl,
            lease_renew_interval: leases.lease_renew_interval,
            ack_wait: delivery.ack_wait,
            retry_delay: delivery.retry_delay,
            fetch_batch_size: delivery.fetch_batch_size,
            rulesets: delivery.rulesets,
            effect_poll_interval: delivery.effect_poll_interval,
            scheduler_poll_interval: delivery.scheduler_poll_interval,
            scheduler_batch_size: delivery.scheduler_batch_size,
        };
        validate_config(&config)?;
        Ok(config)
    }
}

fn load_connection_settings() -> Result<ConnectionSettings> {
    let storage_backend = env::var("STORAGE_BACKEND").unwrap_or_else(|_| "postgres".into());
    if !storage_backend.eq_ignore_ascii_case("postgres") {
        bail!("Rust worker supports STORAGE_BACKEND=postgres only; MongoDB parity is not implemented");
    }

    Ok(ConnectionSettings {
        nats_url: env::var("NATS_URL").unwrap_or_else(|_| "nats://127.0.0.1:4222".into()),
        stream: env::var("NATS_STREAM").unwrap_or_else(|_| "flowrulz".into()),
        durable: env::var("NATS_CONSUMER").unwrap_or_else(|_| "flowrulz-worker".into()),
        database_url: env::var("DATABASE_URL").unwrap_or_else(|_| {
            "postgres://postgres:postgres@127.0.0.1:5432/flowrulz?sslmode=disable".into()
        }),
        worker_id: env::var("WORKER_ID").unwrap_or_else(|_| {
            format!("rust-{}", env::var("HOSTNAME").unwrap_or_else(|_| "worker".into()))
        }),
        service_id: optional_env("FLOW_SERVICE_ID"),
        deployment_revision: optional_env("FLOW_DEPLOYMENT_REVISION")
            .map(|value| value.parse::<i64>().map_err(|error| anyhow::anyhow!("FLOW_DEPLOYMENT_REVISION must be a positive integer: {error}")))
            .transpose()?,
    })
}

fn load_lease_settings() -> Result<LeaseSettings> {
    Ok(LeaseSettings {
        num_shards: parse_u32("NUM_SHARDS", 4096)?,
        worker_replicas: parse_u32("WORKER_REPLICAS", 1)?,
        lease_ttl: Duration::from_millis(parse_u64("LEASE_TTL_MS", 30_000)?),
        lease_renew_interval: Duration::from_millis(parse_u64("LEASE_RENEWAL_INTERVAL_MS", 10_000)?),
    })
}

fn load_delivery_settings() -> Result<DeliverySettings> {
    let rulesets = env::var("NATS_RULESETS").unwrap_or_default().split(',')
        .map(str::trim).filter(|value| !value.is_empty()).map(str::to_owned).collect();

    Ok(DeliverySettings {
        ack_wait: Duration::from_millis(parse_u64("NATS_ACK_WAIT_MS", 30_000)?),
        retry_delay: Duration::from_millis(parse_u64("EVENT_RETRY_DELAY_MS", 5_000)?),
        fetch_batch_size: usize::try_from(parse_u64("FETCH_BATCH_SIZE", 100)?)?,
        rulesets,
        effect_poll_interval: Duration::from_millis(parse_u64("EFFECT_PUBLISH_INTERVAL_MS", 5_000)?),
        scheduler_poll_interval: Duration::from_millis(parse_u64("SCHEDULER_POLL_MS", 5_000)?),
        scheduler_batch_size: i64::try_from(parse_u64("SCHEDULER_BATCH_SIZE", 100)?)?,
    })
}

fn validate_config(config: &Config) -> Result<()> {
    if config.stream.trim().is_empty() || config.durable.trim().is_empty()
        || config.worker_id.trim().is_empty()
    {
        bail!("NATS_STREAM, NATS_CONSUMER and WORKER_ID cannot be empty");
    }
    if config.database_url.trim().is_empty() {
        bail!("DATABASE_URL cannot be empty");
    }
    if config.nats_url.trim().is_empty() {
        bail!("NATS_URL cannot be empty");
    }

    validate_rulesets(&config.rulesets)?;
    match (&config.service_id, config.deployment_revision) {
        (Some(service_id), Some(revision)) if !service_id.trim().is_empty() && revision > 0 => {}
        (None, None) => {}
        _ => bail!("FLOW_SERVICE_ID and positive FLOW_DEPLOYMENT_REVISION must be configured together"),
    }
    validate_lease_settings(config)?;
    validate_delivery_settings(config)
}

fn validate_rulesets(rulesets: &[String]) -> Result<()> {
    let mut seen = std::collections::HashSet::new();
    for ruleset in rulesets {
        if ruleset.len() > 128 {
            bail!("NATS_RULESETS entries must be at most 128 bytes");
        }
        if !seen.insert(ruleset) {
            bail!("NATS_RULESETS contains duplicate rule set {ruleset:?}");
        }
    }
    Ok(())
}

fn validate_lease_settings(config: &Config) -> Result<()> {
    if config.num_shards == 0 || config.worker_replicas == 0 {
        bail!("NUM_SHARDS and WORKER_REPLICAS must be > 0");
    }
    if config.lease_ttl.is_zero() || config.lease_renew_interval.is_zero()
        || config.lease_renew_interval * 3 > config.lease_ttl
    {
        bail!("LEASE_RENEWAL_INTERVAL_MS must be positive and no greater than one third of LEASE_TTL_MS");
    }
    Ok(())
}

fn validate_delivery_settings(config: &Config) -> Result<()> {
    if config.ack_wait.is_zero() || config.retry_delay.is_zero() || config.fetch_batch_size == 0 {
        bail!("NATS_ACK_WAIT_MS, EVENT_RETRY_DELAY_MS and FETCH_BATCH_SIZE must be > 0");
    }
    if config.effect_poll_interval.is_zero() || config.scheduler_poll_interval.is_zero()
        || config.scheduler_batch_size <= 0
    {
        bail!("effect/scheduler polling intervals and batch size must be > 0");
    }
    Ok(())
}

fn parse_u64(name: &str, default: u64) -> Result<u64> {
    let value = env::var(name).unwrap_or_else(|_| default.to_string());
    let parsed = value.parse::<u64>().map_err(|e| anyhow::anyhow!("{name} must be an unsigned integer: {e}"))?;
    if parsed == 0 { bail!("{name} must be greater than zero"); }
    Ok(parsed)
}
fn parse_u32(name: &str, default: u32) -> Result<u32> {
    let value = parse_u64(name, u64::from(default))?;
    u32::try_from(value).map_err(|_| anyhow::anyhow!("{name} exceeds u32"))
}

fn optional_env(name: &str) -> Option<String> {
    env::var(name).ok().map(|value| value.trim().to_owned()).filter(|value| !value.is_empty())
}


#[cfg(test)]
mod tests {
    use super::*;

    fn valid_config() -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            stream: "flowrulz".into(),
            durable: "worker".into(),
            database_url: "postgres://localhost/flowrulz".into(),
            worker_id: "worker-1".into(),
            service_id: None,
            deployment_revision: None,
            num_shards: 16,
            worker_replicas: 1,
            lease_ttl: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(5),
            ack_wait: Duration::from_secs(30),
            retry_delay: Duration::from_secs(5),
            fetch_batch_size: 10,
            rulesets: vec!["orders-created".into()],
            effect_poll_interval: Duration::from_secs(5),
            scheduler_poll_interval: Duration::from_secs(5),
            scheduler_batch_size: 10,
        }
    }

    #[test]
    fn accepts_legacy_and_pinned_deployment_configurations() {
        assert!(validate_config(&valid_config()).is_ok());
        let mut config = valid_config();
        config.service_id = Some("domain-a/orders".into());
        config.deployment_revision = Some(3);
        assert!(validate_config(&config).is_ok());
    }

    #[test]
    fn rejects_incomplete_or_invalid_deployment_identity() {
        let cases = [
            ("service without revision", Some("domain-a/orders"), None),
            ("revision without service", None, Some(3)),
            ("blank service", Some("  "), Some(3)),
            ("zero revision", Some("domain-a/orders"), Some(0)),
            ("negative revision", Some("domain-a/orders"), Some(-1)),
        ];
        for (name, service, revision) in cases {
            let mut config = valid_config();
            config.service_id = service.map(str::to_owned);
            config.deployment_revision = revision;
            assert!(validate_config(&config).is_err(), "case {name} should fail");
        }
    }

    #[test]
    fn rejects_duplicate_or_oversized_ruleset_assignments() {
        let mut config = valid_config();
        config.rulesets = vec!["orders-created".into(), "orders-created".into()];
        assert!(validate_config(&config).is_err());
        config.rulesets = vec!["x".repeat(129)];
        assert!(validate_config(&config).is_err());
    }

    #[test]
    fn rejects_invalid_lease_and_delivery_settings() {
        let mut config = valid_config();
        config.num_shards = 0;
        assert!(validate_config(&config).is_err());

        config = valid_config();
        config.lease_renew_interval = Duration::from_secs(11);
        assert!(validate_config(&config).is_err());

        config = valid_config();
        config.fetch_batch_size = 0;
        assert!(validate_config(&config).is_err());

        config = valid_config();
        config.scheduler_batch_size = 0;
        assert!(validate_config(&config).is_err());
    }
}
