use crate::{config::Config, engine::{EventEnvelope, sha256_hex}, runtime::{scheduler::event_subject, uuid_token, validate_subject}};
use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

pub(super) async fn outbox_loop(pool: PgPool, js: jetstream::Context, config: Config) -> Result<()> {
    let mut ticker = interval(config.effect_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        process_outbox_batch(&pool, &js, &config).await?;
    }
}

async fn process_outbox_batch(
    pool: &PgPool,
    js: &jetstream::Context,
    config: &Config,
) -> Result<()> {
    let claim = format!("{}-{}", config.worker_id, uuid_token());
    let rows = claim_outbox_batch(pool, &claim, config.fetch_batch_size).await?;
    process_claimed_rows(pool, js, config, &claim, rows).await
}

async fn process_claimed_rows(
    pool: &PgPool,
    js: &jetstream::Context,
    http: &reqwest::Client,
    config: &Config,
    claim: &str,
    rows: Vec<sqlx::postgres::PgRow>,
) -> Result<()> {
    for row in rows {
        process_claimed_effect(pool, js, config, claim, row).await?;
    }
    Ok(())
}

struct ClaimedEffect {
    id: String,
    execution_id: String,
    tenant_id: String,
    destination: String,
    name: String,
    payload: Value,
    effect_type: String,
    attempts: i32,
    max_attempts: i32,
}

async fn claim_outbox_batch(pool: &PgPool, claim: &str, batch_size: usize) -> Result<Vec<sqlx::postgres::PgRow>> {
    sqlx::query(
        "WITH candidates AS ( \
           SELECT effect_id FROM outbox_effects \
           WHERE (status='pending' AND available_at<=NOW()) OR (status='claimed' AND claim_expires_at<NOW()) \
           ORDER BY available_at,created_at LIMIT $1 FOR UPDATE SKIP LOCKED \
         ) UPDATE outbox_effects o SET status='claimed',claimed_by=$2,claimed_at=NOW(), \
           claim_expires_at=NOW()+INTERVAL '2 minutes',updated_at=NOW() \
         FROM candidates c WHERE o.effect_id=c.effect_id \
         RETURNING o.effect_id,o.execution_id,o.tenant_id,o.destination,o.name,o.payload,o.effect_type,o.attempts,o.max_attempts"
    ).bind(i64::try_from(batch_size)?).bind(claim).fetch_all(pool).await.context("claim outbox effects")
}

fn decode_claimed_effect(row: &sqlx::postgres::PgRow) -> Result<ClaimedEffect> {
    Ok(ClaimedEffect {
        id: row.try_get("effect_id")?,
        execution_id: row.try_get("execution_id")?,
        tenant_id: row.try_get("tenant_id")?,
        destination: row.try_get("destination")?,
        name: row.try_get("name")?,
        payload: row.try_get("payload")?,
        effect_type: row.try_get("effect_type")?,
        attempts: row.try_get("attempts")?,
        max_attempts: row.try_get("max_attempts")?,
    })
}

async fn process_claimed_effect(
    pool: &PgPool,
    js: &jetstream::Context,
    http: &reqwest::Client,
    config: &Config,
    claim: &str,
    row: sqlx::postgres::PgRow,
) -> Result<()> {
    let effect = decode_claimed_effect(&row)?;
    match send_effect(pool, js, config, &effect).await {
        Ok(()) => mark_effect_delivered(pool, claim, &effect.id).await,
        Err(error) => record_effect_failure(pool, claim, &effect, &error.to_string()).await,
    }
}

async fn mark_effect_delivered(pool: &PgPool, claim: &str, effect_id: &str) -> Result<()> {
    let result = sqlx::query(
        "UPDATE outbox_effects SET status='delivered',claimed_by=NULL,claimed_at=NULL,claim_expires_at=NULL,updated_at=NOW() \
         WHERE effect_id=$1 AND status='claimed' AND claimed_by=$2 AND claim_expires_at>NOW()"
    ).bind(effect_id).bind(claim).execute(pool).await.context("mark outbox effect delivered")?;
    if result.rows_affected() != 1 {
        warn!(effect_id, "effect delivered but claim settlement was lost");
    }
    Ok(())
}

async fn record_effect_failure(pool: &PgPool, claim: &str, effect: &ClaimedEffect, error: &str) -> Result<()> {
    let next_attempt = effect.attempts + 1;
    if next_attempt >= effect.max_attempts {
        quarantine_effect_atomically(pool, claim, effect, next_attempt, error).await
    } else {
        schedule_effect_retry(pool, claim, &effect.id, next_attempt, error).await
    }
}

// Persisting the quarantine record and changing the outbox status must share a
// transaction. Otherwise a stale publisher can leave a quarantine record behind
// even after it has lost its claim to another worker.
async fn quarantine_effect_atomically(
    pool: &PgPool,
    claim: &str,
    effect: &ClaimedEffect,
    attempt: i32,
    error: &str,
) -> Result<()> {
    let mut tx = pool.begin().await.context("begin exhausted effect quarantine")?;
    let raw = effect.payload.to_string();
    let quarantine_id = sha256_hex(
        format!("effect|{}|{}", effect.id, sha256_hex(raw.as_bytes())).as_bytes(),
    );

    sqlx::query(
        "INSERT INTO quarantine (quarantine_id,source_type,source_id,error_class,payload_ref,event_id,tenant_id,error_message,created_at,updated_at) \
         VALUES ($1,'effect',$2,'permanent','nats:flowrulz',NULL,$3,$4,NOW(),NOW()) \
         ON CONFLICT (quarantine_id) DO NOTHING"
    )
    .bind(quarantine_id)
    .bind(&effect.id)
    .bind(&effect.tenant_id)
    .bind(error)
    .execute(&mut *tx)
    .await
    .context("persist exhausted effect quarantine")?;

    let result = sqlx::query(
        "UPDATE outbox_effects SET status='quarantined',attempts=$3,last_error=$4,claimed_by=NULL,claimed_at=NULL,claim_expires_at=NULL,updated_at=NOW() \
         WHERE effect_id=$1 AND status='claimed' AND claimed_by=$2 AND claim_expires_at>NOW()"
    )
    .bind(&effect.id)
    .bind(claim)
    .bind(attempt)
    .bind(error)
    .execute(&mut *tx)
    .await
    .context("quarantine exhausted outbox effect")?;

    ensure_claim_update(result.rows_affected(), "quarantining", &effect.id)?;
    tx.commit().await.context("commit exhausted effect quarantine")?;
    Ok(())
}

fn ensure_claim_update(rows_affected: u64, action: &str, effect_id: &str) -> Result<()> {
    if rows_affected != 1 {
        bail!("outbox claim lost while {action} effect {effect_id}");
    }
    Ok(())
}

async fn schedule_effect_retry(pool: &PgPool, claim: &str, effect_id: &str, attempt: i32, error: &str) -> Result<()> {
    let delay = (1_i64 << (attempt - 1).clamp(0, 5)).min(60);
    let result = sqlx::query(
        "UPDATE outbox_effects SET status='pending',attempts=$3,last_error=$4,available_at=NOW()+($5 * INTERVAL '1 second'), \
         claimed_by=NULL,claimed_at=NULL,claim_expires_at=NULL,updated_at=NOW() \
         WHERE effect_id=$1 AND status='claimed' AND claimed_by=$2 AND claim_expires_at>NOW()"
    ).bind(effect_id).bind(claim).bind(attempt).bind(error).bind(delay).execute(pool).await
        .context("schedule outbox effect retry")?;
    ensure_claim_update(result.rows_affected(), "scheduling retry for", effect_id)
}

async fn send_effect(
    pool: &PgPool,
    js: &jetstream::Context,
    config: &Config,
    effect: &ClaimedEffect,
) -> Result<()> {
    let subject = match effect.effect_type.as_str() {
        "emit" => {
            validate_subject_path(&effect.destination)?;
            format!("effects.{}", effect.destination)
        }
        "command" => {
            validate_subject_token(&effect.destination, "command destination")?;
            validate_subject_path(&effect.name)?;
            format!("commands.{}.{}", effect.destination, effect.name)
        }
        "emit_event" => {
            let event: EventEnvelope =
                serde_json::from_value(effect.payload.clone()).context("decode child event")?;
            event.validate().context("validate child event")?;
            let shard = event.virtual_shard(config.num_shards)?;
            let subject = event_subject(&event.event_type, shard, &config.rulesets);
            publish(js, &subject, serde_json::to_vec(&event)?, "publish child event").await?;
            return Ok(());
        }
        "service_event" => {
            let event: EventEnvelope =
                serde_json::from_value(effect.payload.clone()).context("decode service event")?;
            event.validate().context("validate service event")?;
            if event.tenant_id != effect.tenant_id || event.event_type != effect.name {
                bail!("service event identity does not match its outbox effect");
            }
            let active: bool = sqlx::query_scalar(
                "SELECT EXISTS(SELECT 1 FROM tenant_domain_services WHERE tenant_id=$1 AND service_id=$2 AND active=TRUE)"
            )
            .bind(&effect.tenant_id)
            .bind(&effect.destination)
            .fetch_one(pool)
            .await
            .context("verify active tenant service")?;
            if !active {
                bail!("active tenant service {:?} was not found", effect.destination);
            }
            format!(
                "services.{}.{}",
                &sha256_hex(effect.tenant_id.as_bytes())[..24],
                &sha256_hex(effect.destination.as_bytes())[..24]
            )
        }
        other => bail!("unsupported outbox effect type {other:?}"),
    };

    validate_concrete_subject(&subject)?;
    let envelope = json!({
        "id": effect.id,
        "execution_id": effect.execution_id,
        "tenant_id": effect.tenant_id,
        "destination": effect.destination,
        "name": effect.name,
        "payload": effect.payload,
        "effect_type": effect.effect_type,
        "idempotency_key": format!("effect-{}", effect.id),
    });
    publish(js, &subject, serde_json::to_vec(&envelope)?, "publish outbox effect").await
}

async fn publish(
    js: &jetstream::Context,
    subject: &str,
    payload: Vec<u8>,
    context: &str,
) -> Result<()> {
    js.publish(subject.to_owned(), payload.into())
        .await
        .with_context(|| format!("{context} to {subject}"))?;
    Ok(())
}

fn validate_subject_token(value: &str, label: &str) -> Result<()> {
    let valid = !value.is_empty()
        && value.len() <= 63
        && value.as_bytes()[0].is_ascii_lowercase()
        && value.bytes().all(|byte| byte.is_ascii_lowercase()
            || byte.is_ascii_digit() || byte == b'_' || byte == b'-');
    if !valid {
        bail!("invalid {label} {value:?}");
    }
    Ok(())
}

fn validate_subject_path(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 128 {
        bail!("effect subject path must contain 1..=128 bytes");
    }
    for token in value.split('.') {
        validate_subject_token(token, "subject token")?;
    }
    Ok(())
}

fn validate_concrete_subject(subject: &str) -> Result<()> {
    if subject.is_empty()
        || subject.split('.').any(|token| token.is_empty() || token.contains('*') || token.contains('>'))
    {
        bail!("invalid concrete NATS subject {subject:?}");
    }
    Ok(())
}

#[cfg(test)]
mod outbox_claim_tests {
    use super::*;

    #[test]
    fn claim_settlement_requires_exactly_one_updated_row() {
        assert!(ensure_claim_update(1, "scheduling retry for", "effect-1").is_ok());
        assert!(ensure_claim_update(0, "scheduling retry for", "effect-1").is_err());
        assert!(ensure_claim_update(2, "quarantining", "effect-1").is_err());
    }
}
