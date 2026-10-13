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
