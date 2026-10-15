//! JetStream event consumption and delivery settlement.
use crate::{config::Config, engine::{self, EventEnvelope, ProcessOutcome}, runtime::quarantine};
use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream::{self, consumer::pull};
use futures_util::StreamExt;
use sqlx::PgPool;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

pub(super) fn subjects_for_shard(shard: u32, config: &Config) -> Vec<(String, String)> {
    let mut subjects = vec![(format!("events.shard.{shard}"),
        format!("{}-legacy-shard-{shard}", config.durable))];
    for rule_set in &config.rulesets {
        let token = engine::sha256_hex(rule_set.as_bytes());
        subjects.push((format!("events.ruleset.{}.shard.{shard}", token),
            format!("{}-rs-{}-shard-{shard}", config.durable, &token[..32])));
    }
    subjects
}

pub(super) async fn consume_shard_subject(
    pool: PgPool,
    js: jetstream::Context,
    config: Config,
    shard: u32,
    fencing_token: i64,
    subject: String,
    durable: String,
) -> Result<()> {
    let stream = js.get_stream(&config.stream).await?;
    let consumer = stream.get_or_create_consumer(&durable, pull::Config {
        durable_name: Some(durable.clone()),
        filter_subject: subject.clone(),
        ack_wait: config.ack_wait,
        max_deliver: -1,
        ..Default::default()
    }).await.with_context(|| format!("create durable {durable} for {subject}"))?;
    let mut messages = consumer.messages().await.context("open pull consumer message stream")?;
    let mut renew = interval(config.lease_renew_interval);
    renew.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = renew.tick() => {
                renew_shard_lease(&pool, &config, shard, fencing_token).await?;
            }
            maybe_message = messages.next() => {
                let Some(message_result) = maybe_message else { bail!("JetStream consumer stream ended"); };
                let message = message_result.context("receive JetStream message")?;
                handle_message(&pool, message, &config, shard, fencing_token).await?;
            }
        }
    }
}

async fn renew_shard_lease(
    pool: &PgPool,
    config: &Config,
    shard: u32,
    fencing_token: i64,
) -> Result<()> {
    let ttl_ms = i64::try_from(config.lease_ttl.as_millis())?;
    let renewed = engine::renew_lease(
        pool, shard, &config.worker_id, fencing_token, ttl_ms,
    ).await?;
    if !renewed {
        bail!("shard lease lost; consumer must stop");
    }
    Ok(())
}

async fn handle_message(
    pool: &PgPool,
    message: jetstream::Message,
    config: &Config,
    shard: u32,
    fencing_token: i64,
) -> Result<()> {
    let Some(envelope) = decode_or_quarantine(pool, &message).await? else {
        return Ok(());
    };
    if let Err(error) = validate_message_routing(&envelope, config, shard) {
        quarantine_and_ack(pool, &message, &envelope.id, Some(&envelope.tenant_id), &error.to_string()).await?;
        return Ok(());
    }

    process_and_settle_event(pool, message, config, shard, fencing_token, envelope).await
}

async fn decode_or_quarantine(
    pool: &PgPool,
    message: &jetstream::Message,
) -> Result<Option<EventEnvelope>> {
    let envelope = match serde_json::from_slice::<EventEnvelope>(&message.payload) {
        Ok(envelope) => envelope,
        Err(error) => {
            quarantine_and_ack(pool, message, "unknown", None, &error.to_string()).await?;
            return Ok(None);
        }
    };
    if let Err(error) = envelope.validate() {
        quarantine_and_ack(
            pool, message, &envelope.id, Some(&envelope.tenant_id), &error.to_string(),
        ).await?;
        return Ok(None);
    }
    Ok(Some(envelope))
}

fn validate_message_routing(envelope: &EventEnvelope, config: &Config, shard: u32) -> Result<()> {
    let computed_shard = envelope.virtual_shard(config.num_shards)?;
    if computed_shard != shard {
        bail!("message routed to shard {shard} but envelope hashes to {computed_shard}");
    }
    if let Some(expected_service_id) = &config.service_id {
        let target_service_id = envelope.meta.get("flow.service_id").and_then(|value| value.as_str())
            .ok_or_else(|| anyhow!("service deployment routing header flow.service_id is required"))?;
        if target_service_id != expected_service_id.as_str() {
            bail!("message targets service deployment {target_service_id}, not this worker's {expected_service_id}");
        }
        let expected_revision = config.deployment_revision
            .ok_or_else(|| anyhow!("worker deployment revision is not configured"))?;
        let target_revision = match envelope.meta.get("flow.deployment_revision") {
            Some(serde_json::Value::Number(value)) => value.as_i64(),
            Some(serde_json::Value::String(value)) => value.parse::<i64>().ok(),
            _ => None,
        }.ok_or_else(|| anyhow!("meta.flow.deployment_revision must be an integer or integer string"))?;
        if target_revision != expected_revision {
            bail!("message targets deployment revision {target_revision}, not this worker's {expected_revision}");
        }
    }
    Ok(())
}

async fn process_and_settle_event(
    pool: &PgPool,
    message: jetstream::Message,
    config: &Config,
    shard: u32,
    fencing_token: i64,
    envelope: EventEnvelope,
) -> Result<()> {
    let outcome = engine::process_event(
        pool, &envelope, &config.worker_id, fencing_token, config.num_shards,
    ).await;

    match outcome {
        Ok(ProcessOutcome::Processed { execution_id, decision_hash, matched_rules }) => {
            settle_processed_event(&message, shard, &envelope, &execution_id, &decision_hash, &matched_rules).await
        }
        Ok(ProcessOutcome::Duplicate { execution_id }) => {
            settle_duplicate_event(&message, shard, &envelope, &execution_id).await
        }
        Err(error) if is_permanent_event_error(&error) => {
            quarantine_and_ack(
                pool, &message, &envelope.id, Some(&envelope.tenant_id), &error.to_string(),
            ).await
        }
        Err(error) => retry_event_later(&message, config, shard, &envelope, &error).await,
    }
}

async fn settle_processed_event(
    message: &jetstream::Message,
    shard: u32,
    envelope: &EventEnvelope,
    execution_id: &str,
    decision_hash: &str,
    matched_rules: &[String],
) -> Result<()> {
    log_processed_event(shard, envelope, execution_id, decision_hash, matched_rules);
    acknowledge_message(message, "ack committed event").await
}

async fn settle_duplicate_event(
    message: &jetstream::Message,
    shard: u32,
    envelope: &EventEnvelope,
    execution_id: &str,
) -> Result<()> {
    info!(shard, event_id=%envelope.id, execution_id=%execution_id, "duplicate event confirmed");
    acknowledge_message(message, "ack confirmed duplicate").await
}

async fn retry_event_later(
    message: &jetstream::Message,
    config: &Config,
    shard: u32,
    envelope: &EventEnvelope,
    error: &anyhow::Error,
) -> Result<()> {
    warn!(shard, event_id=%envelope.id, error=%error, "event processing failed; scheduling retry");
    message.ack_with(jetstream::AckKind::Nak(Some(config.retry_delay)))
        .await.map_err(|error| anyhow!("nak failed event for retry: {error}"))?;
    Ok(())
}

fn log_processed_event(
    shard: u32,
    envelope: &EventEnvelope,
    execution_id: &str,
    decision_hash: &str,
    matched_rules: &[String],
) {
    info!(
        shard, event_id=%envelope.id, execution_id=%execution_id, decision_hash=%decision_hash,
        matched_rules=?matched_rules, "event committed"
    );
}

async fn acknowledge_message(message: &jetstream::Message, context: &str) -> Result<()> {
    message.ack().await.map_err(|error| anyhow!("{context}: {error}"))
}

async fn quarantine_and_ack(
    pool: &PgPool,
    message: &jetstream::Message,
    event_id: &str,
    tenant_id: Option<&str>,
    reason: &str,
) -> Result<()> {
    quarantine::persist_quarantine(pool, "event", event_id, tenant_id, &message.payload, reason).await?;
    message.ack().await.map_err(|err| anyhow!("ack quarantined event: {err}"))?;
    Ok(())
}

fn is_permanent_event_error(error: &anyhow::Error) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    ["event id is required", "event type is required", "tenant_id is required",
     "partition_key is required", "unsupported predicate operator", "unsupported partition key policy",
     "emit topic is required", "resolved an empty partition key",
     "action must contain exactly one supported action", "predicate path missing",
     "inbox duplicate has no committed execution", "inbox points to missing execution",
     "exceeds the 100-service fan-out limit", "command destination and name are required"].iter().any(|needle| message.contains(needle))
}


#[cfg(test)]
mod deployment_routing_tests {
    use super::*;
    use crate::engine::{ContractRef, EventEnvelope};
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use std::{collections::HashMap, time::Duration};

    fn config(service_id: Option<&str>, revision: Option<i64>) -> Config {
        Config {
            nats_url: "nats://localhost:4222".into(),
            stream: "flowrulz".into(),
            durable: "worker".into(),
            database_url: "postgres://localhost/flowrulz".into(),
            worker_id: "worker-1".into(),
            service_id: service_id.map(str::to_owned),
            deployment_revision: revision,
            num_shards: 16,
            worker_replicas: 1,
            lease_ttl: Duration::from_secs(30),
            lease_renew_interval: Duration::from_secs(5),
            ack_wait: Duration::from_secs(30),
            retry_delay: Duration::from_secs(5),
            fetch_batch_size: 10,
            rulesets: vec![],
            effect_poll_interval: Duration::from_secs(5),
            scheduler_poll_interval: Duration::from_secs(5),
            scheduler_batch_size: 10,
        }
    }

    fn event(meta: HashMap<String, serde_json::Value>) -> EventEnvelope {
        EventEnvelope {
            id: "event-1".into(),
            event_type: "orders.created".into(),
            tenant_id: "tenant-a".into(),
            partition_key: "order-1".into(),
            workflow_id: None,
            occurred_at: "2026-10-15T00:00:00Z".parse::<DateTime<Utc>>().unwrap(),
            data: json!({"order_id":"order-1"}),
            headers: HashMap::new(),
            meta,
            contract: Some(ContractRef {
                name: "orders.created".into(),
                version: "v1".into(),
                schema_hash: Some("sha256:abc".into()),
            }),
            message_kind: None,
            correlation_id: None,
            causation_id: None,
        }
    }

    #[test]
    fn accepts_exact_service_and_deployment_revision() {
        let cfg = config(Some("domain-a/orders"), Some(3));
        let env = event(HashMap::from([
            ("flow.service_id".into(), json!("domain-a/orders")),
            ("flow.deployment_revision".into(), json!(3)),
        ]));
        let shard = env.virtual_shard(cfg.num_shards).unwrap();
        assert!(validate_message_routing(&env, &cfg, shard).is_ok());
    }

    #[test]
    fn rejects_missing_or_mismatched_service_identity() {
        let cfg = config(Some("domain-a/orders"), Some(3));
        let shard = event(HashMap::new()).virtual_shard(cfg.num_shards).unwrap();
        assert!(validate_message_routing(&event(HashMap::new()), &cfg, shard).is_err());

        let env = event(HashMap::from([
            ("flow.service_id".into(), json!("domain-a/billing")),
            ("flow.deployment_revision".into(), json!("3")),
        ]));
        let shard = env.virtual_shard(cfg.num_shards).unwrap();
        assert!(validate_message_routing(&env, &cfg, shard).is_err());
    }

    #[test]
    fn rejects_missing_malformed_and_stale_deployment_revision() {
        let cfg = config(Some("domain-a/orders"), Some(3));
        for revision in [None, Some("not-a-number"), Some("2")] {
            let mut meta = HashMap::from([("flow.service_id".into(), json!("domain-a/orders"))]);
            if let Some(value) = revision {
                meta.insert("flow.deployment_revision".into(), json!(value));
            }
            let env = event(meta);
            let shard = env.virtual_shard(cfg.num_shards).unwrap();
            assert!(validate_message_routing(&env, &cfg, shard).is_err(), "revision case {revision:?} should fail");
        }
    }

    #[test]
    fn rejects_wrong_shard_even_when_service_matches() {
        let cfg = config(Some("domain-a/orders"), Some(3));
        let env = event(HashMap::from([
            ("flow.service_id".into(), json!("domain-a/orders")),
            ("flow.deployment_revision".into(), json!("3")),
        ]));
        let correct = env.virtual_shard(cfg.num_shards).unwrap();
        let wrong = (correct + 1) % cfg.num_shards;
        assert!(validate_message_routing(&env, &cfg, wrong).is_err());
    }

    #[test]
    fn permits_legacy_routing_when_deployment_identity_is_unset() {
        let cfg = config(None, None);
        let env = event(HashMap::new());
        let shard = env.virtual_shard(cfg.num_shards).unwrap();
        assert!(validate_message_routing(&env, &cfg, shard).is_ok());
    }
}
