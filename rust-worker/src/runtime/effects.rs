use crate::{config::Config, engine::{EventEnvelope, sha256_hex}, runtime::{scheduler::event_subject, uuid_token, validate_subject}};
use anyhow::{anyhow, bail, Context, Result};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::{json, Value};
use sqlx::{PgPool, Row};
use std::time::Duration;
use tokio::time::{interval, MissedTickBehavior};
use tracing::{info, warn};

pub(super) async fn outbox_loop(pool: PgPool, js: jetstream::Context, config: Config) -> Result<()> {
    let http = build_http_client()?;
    let mut ticker = interval(config.effect_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        process_outbox_batch(&pool, &js, &http, &config).await?;
    }
}

async fn process_outbox_batch(
    pool: &PgPool,
    js: &jetstream::Context,
    http: &reqwest::Client,
    config: &Config,
) -> Result<()> {
    let claim = format!("{}-{}", config.worker_id, uuid_token());
    let rows = claim_outbox_batch(pool, &claim, config.fetch_batch_size).await?;
    process_claimed_rows(pool, js, http, config, &claim, rows).await
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
        process_claimed_effect(pool, js, http, config, claim, row).await?;
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

fn build_http_client() -> Result<reqwest::Client> {
    Ok(reqwest::Client::builder().no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10)).build()?)
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
    match send_effect(pool, js, http, config, &effect).await {
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
    http: &reqwest::Client,
    config: &Config,
    effect: &ClaimedEffect,
) -> Result<()> {
    match effect.effect_type.as_str() {
        "emit" => {
            validate_subject(&effect.destination)?;
            js.publish(effect.destination.clone(), serde_json::to_vec(&effect.payload)?.into())
                .await
                .context("publish effect to JetStream")?;
            Ok(())
        }
        "command" => send_external_effect(http, config, effect).await,
        "emit_event" => {
            let event: EventEnvelope =
                serde_json::from_value(effect.payload.clone()).context("decode child event")?;
            let shard = event.virtual_shard(config.num_shards)?;
            let subject = event_subject(&event.event_type, shard, &config.rulesets);
            js.publish(subject, serde_json::to_vec(&event)?.into())
                .await
                .context("publish child event to JetStream")?;
            Ok(())
        }
        "service_event" => send_domain_service_effect(pool, http, config, effect).await,
        other => bail!("unsupported outbox effect type {other:?}"),
    }
}

async fn send_external_effect(
    http: &reqwest::Client,
    config: &Config,
    effect: &ClaimedEffect,
) -> Result<()> {
    if accept_development_effect(config, &effect.id, &effect.destination, &effect.name)? {
        return Ok(());
    }

    let url = build_external_destination_url(config, &effect.destination, &effect.name)?;
    let payload = build_external_effect_payload(
        &effect.id,
        &effect.execution_id,
        &effect.tenant_id,
        &effect.destination,
        &effect.name,
        &effect.payload,
        &effect.effect_type,
    );
    post_external_effect(http, url, &effect.id, &effect.execution_id, &payload).await
}

fn accept_development_effect(config: &Config, id: &str, destination: &str, name: &str) -> Result<bool> {
    if !config.effect_destination_url.trim().is_empty() {
        return Ok(false);
    }
    if is_local_environment(&config.app_env) {
        info!(effect_id = id, destination, name, "development fake destination accepted effect");
        return Ok(true);
    }
    bail!("EFFECT_DESTINATION_URL is required for external effects")
}

fn build_external_destination_url(config: &Config, destination: &str, name: &str) -> Result<reqwest::Url> {
    let mut url = reqwest::Url::parse(&config.effect_destination_url)
        .context("parse EFFECT_DESTINATION_URL")?;
    let mut segments = url.path_segments_mut()
        .map_err(|_| anyhow!("EFFECT_DESTINATION_URL cannot be a base URL"))?;
    segments.pop_if_empty();
    segments.push(destination);
    segments.push(name);
    drop(segments);
    Ok(url)
}

fn build_external_effect_payload(
    id: &str,
    execution_id: &str,
    tenant_id: &str,
    destination: &str,
    name: &str,
    payload: &Value,
    effect_type: &str,
) -> Value {
    json!({
        "id": id,
        "execution_id": execution_id,
        "tenant_id": tenant_id,
        "destination": destination,
        "name": name,
        "payload": payload,
        "effect_type": effect_type,
        "created_at": Utc::now()
    })
}

async fn post_external_effect(
    http: &reqwest::Client,
    url: reqwest::Url,
    id: &str,
    execution_id: &str,
    effect: &Value,
) -> Result<()> {
    let response = http.post(url)
        .header("Content-Type", "application/json")
        .header("X-Effect-ID", id)
        .header("X-Execution-ID", execution_id)
        .header("Idempotency-Key", format!("effect-{id}"))
        .json(effect)
        .send()
        .await
        .context("send external effect")?;
    if !response.status().is_success() {
        bail!("external effect endpoint returned {}", response.status());
    }
    Ok(())
}

async fn send_domain_service_effect(
    pool: &PgPool,
    http: &reqwest::Client,
    config: &Config,
    effect: &ClaimedEffect,
) -> Result<()> {
    let endpoint = load_active_service_endpoint(pool, &effect.tenant_id, &effect.destination).await?;
    let url = validate_service_endpoint(&endpoint, config)?;
    post_service_callback(http, url, effect).await
}

async fn load_active_service_endpoint(pool: &PgPool, tenant_id: &str, service_id: &str) -> Result<String> {
    sqlx::query_scalar(
        "SELECT endpoint_url FROM tenant_domain_services WHERE tenant_id=$1 AND service_id=$2 AND active=TRUE"
    )
    .bind(tenant_id)
    .bind(service_id)
    .fetch_optional(pool)
    .await?
    .ok_or_else(|| anyhow!("active tenant domain service {service_id:?} was not found"))
}

fn validate_service_endpoint(endpoint: &str, config: &Config) -> Result<reqwest::Url> {
    let url = reqwest::Url::parse(endpoint).context("parse tenant service endpoint URL")?;
    let local_env = is_local_environment(&config.app_env);

    validate_service_url_components(&url, local_env)?;
    validate_service_ip_literal(&url, local_env)?;
    Ok(url)
}

fn is_local_environment(app_env: &str) -> bool {
    matches!(app_env.to_ascii_lowercase().as_str(), "dev" | "development" | "test")
}

fn validate_service_url_components(url: &reqwest::Url, local_env: bool) -> Result<()> {
    if url.scheme() != "https" && !local_env {
        bail!("tenant service endpoint must use HTTPS outside development/test");
    }
    if url.host_str().is_none() || !url.username().is_empty()
        || url.password().is_some() || url.fragment().is_some()
    {
        bail!("tenant service endpoint URL contains invalid authority components");
    }
    Ok(())
}

fn validate_service_ip_literal(url: &reqwest::Url, local_env: bool) -> Result<()> {
    if local_env {
        return Ok(());
    }
    let Some(host) = url.host_str() else {
        bail!("tenant service endpoint must contain a host");
    };
    let Ok(ip) = host.parse::<std::net::IpAddr>() else {
        // DNS names are not resolved here; validating all resolved IPs remains a known security gap.
        return Ok(());
    };
    let private = is_non_public_ip(ip);
    if private {
        bail!("tenant service endpoint resolves to a non-public IP literal");
    }
    Ok(())
}

fn is_non_public_ip(ip: std::net::IpAddr) -> bool {
    match ip {
        std::net::IpAddr::V4(ip) => {
            ip.is_private() || ip.is_loopback() || ip.is_link_local() || ip.is_unspecified()
        }
        std::net::IpAddr::V6(ip) => {
            ip.is_loopback() || ip.is_unspecified() || ip.is_unique_local() || ip.is_unicast_link_local()
        }
    }
}

async fn post_service_callback(
    http: &reqwest::Client,
    url: reqwest::Url,
    effect: &ClaimedEffect,
) -> Result<()> {
    let event_id = effect.payload.get("id").and_then(Value::as_str).unwrap_or_default();
    let request = build_service_callback_request(http, url, effect, event_id);
    let response = request
        .send()
        .await
        .context("send tenant service event callback")?;
    ensure_successful_callback(response.status())
}

fn build_service_callback_request(
    http: &reqwest::Client,
    url: reqwest::Url,
    effect: &ClaimedEffect,
    event_id: &str,
) -> reqwest::RequestBuilder {
    http.post(url)
        .header("Content-Type", "application/json")
        .header("Idempotency-Key", format!("effect-{}", effect.id))
        .header("X-FlowRulz-Tenant-ID", &effect.tenant_id)
        .header("X-FlowRulz-Service-ID", &effect.destination)
        .header("X-FlowRulz-Effect-ID", &effect.id)
        .header("X-FlowRulz-Event-ID", event_id)
        .header("X-Effect-ID", &effect.id)
        .header("X-Execution-ID", &effect.execution_id)
        .header("X-FlowRulz-Event-Type", &effect.name)
        .json(&effect.payload)
}

fn ensure_successful_callback(status: reqwest::StatusCode) -> Result<()> {
    if !status.is_success() {
        bail!("tenant service callback returned {status}");
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
