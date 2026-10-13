//! Scheduled-event claiming, retry, and JetStream publication.
use crate::{config::Config, engine::{self, EventEnvelope}, runtime::uuid_token};
use anyhow::{Context, Result};
use async_nats::jetstream;
use chrono::Utc;
use serde_json::Value;
use sqlx::{postgres::PgRow, PgPool, Row};
use std::collections::HashMap;
use tokio::time::{interval, MissedTickBehavior};

pub(super) async fn scheduler_loop(pool: PgPool, js: jetstream::Context, config: Config) -> Result<()> {
    let mut ticker = interval(config.scheduler_poll_interval);
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);

    loop {
        ticker.tick().await;
        let claimant = format!("scheduler-{}", uuid_token());
        let rows = claim_due_events(&pool, &claimant, config.scheduler_batch_size).await?;
        for row in rows {
            process_scheduled_event(&pool, &js, &config, &claimant, row).await?;
        }
    }
}

async fn claim_due_events(pool: &PgPool, claimant: &str, batch_size: i64) -> Result<Vec<PgRow>> {
    sqlx::query(
        "WITH due AS (SELECT event_id FROM scheduled_events \
         WHERE (status='pending' AND COALESCE(retry_at,scheduled_at)<=NOW()) OR (status='claimed' AND claim_expires_at<NOW()) \
         ORDER BY scheduled_at LIMIT $1 FOR UPDATE SKIP LOCKED) \
         UPDATE scheduled_events s SET status='claimed',claimant=$2,claimed_at=NOW(),claim_expires_at=NOW()+INTERVAL '5 minutes' \
         FROM due WHERE s.event_id=due.event_id \
         RETURNING s.event_id,s.tenant_id,s.event_type,s.partition_key,s.workflow_id,s.payload,s.headers,s.retry_count"
    ).bind(batch_size).bind(claimant).fetch_all(pool).await.context("claim due scheduled events")
}

async fn process_scheduled_event(
    pool: &PgPool,
    js: &jetstream::Context,
    config: &Config,
    claimant: &str,
    row: PgRow,
) -> Result<()> {
    let (event, retry_count) = decode_scheduled_event(&row)?;
    let event_id = event.id.clone();
    match publish_scheduled_event(js, config, &event).await {
        Ok(()) => mark_released(pool, claimant, &event_id).await,
        Err(error) => record_publish_failure(pool, claimant, &event_id, retry_count, &error.to_string()).await,
    }
}

fn decode_scheduled_event(row: &PgRow) -> Result<(EventEnvelope, i32)> {
    let event_id: String = row.try_get("event_id")?;
    let tenant_id: String = row.try_get("tenant_id")?;
    let event_type: String = row.try_get("event_type")?;
    let partition_key: String = row.try_get("partition_key")?;
    let workflow_id: Option<String> = row.try_get("workflow_id")?;
    let payload: Value = row.try_get("payload")?;
    let headers: Option<Value> = row.try_get("headers")?;
    let retry_count: i32 = row.try_get("retry_count")?;

    let headers = scheduled_event_headers(headers, &event_id);
    let event = EventEnvelope {
        id: event_id,
        event_type,
        tenant_id,
        partition_key,
        workflow_id,
        occurred_at: Utc::now(),
        data: payload,
        headers,
    };
    Ok((event, retry_count))
}

fn scheduled_event_headers(headers: Option<Value>, event_id: &str) -> HashMap<String, String> {
    let mut headers = headers
        .and_then(|value| serde_json::from_value::<HashMap<String, String>>(value).ok())
        .unwrap_or_default();
    headers.insert("Idempotency-Key".to_owned(), format!("sched-{event_id}"));
    headers
}

async fn publish_scheduled_event(js: &jetstream::Context, config: &Config, event: &EventEnvelope) -> Result<()> {
    let shard = event.virtual_shard(config.num_shards)?;
    let subject = event_subject(&event.event_type, shard, &config.rulesets);
    js.publish(subject, serde_json::to_vec(event)?.into()).await
        .context("publish scheduled event to JetStream")?;
    Ok(())
}

async fn mark_released(pool: &PgPool, claimant: &str, event_id: &str) -> Result<()> {
    sqlx::query(
        "UPDATE scheduled_events SET status='released',released_at=NOW(),claimant=NULL,claimed_at=NULL,claim_expires_at=NULL \
         WHERE event_id=$1 AND claimant=$2 AND claim_expires_at>NOW()"
    ).bind(event_id).bind(claimant).execute(pool).await.context("mark scheduled event released")?;
    Ok(())
}

async fn record_publish_failure(
    pool: &PgPool,
    claimant: &str,
    event_id: &str,
    retry_count: i32,
    error: &str,
) -> Result<()> {
    let next_attempt = retry_count + 1;
    if next_attempt >= 8 {
        mark_failed(pool, claimant, event_id, next_attempt, error).await
    } else {
        schedule_retry(pool, claimant, event_id, next_attempt, error).await
    }
}

async fn mark_failed(pool: &PgPool, claimant: &str, event_id: &str, attempt: i32, error: &str) -> Result<()> {
    sqlx::query(
        "UPDATE scheduled_events SET status='failed',retry_count=$3,last_error=$4,claimant=NULL,claimed_at=NULL,claim_expires_at=NULL \
         WHERE event_id=$1 AND claimant=$2 AND claim_expires_at>NOW()"
    ).bind(event_id).bind(claimant).bind(attempt).bind(error).execute(pool).await
        .context("mark scheduled event permanently failed")?;
    Ok(())
}

async fn schedule_retry(pool: &PgPool, claimant: &str, event_id: &str, attempt: i32, error: &str) -> Result<()> {
    let delay = (1_i64 << (attempt - 1).clamp(0, 5)).min(60);
    sqlx::query(
        "UPDATE scheduled_events SET status='pending',retry_count=$3,retry_at=NOW()+($4 * INTERVAL '1 second'),last_error=$5,claimant=NULL,claimed_at=NULL,claim_expires_at=NULL \
         WHERE event_id=$1 AND claimant=$2 AND claim_expires_at>NOW()"
    ).bind(event_id).bind(claimant).bind(attempt).bind(delay).bind(error).execute(pool).await
        .context("schedule scheduled-event retry")?;
    Ok(())
}

pub(super) fn event_subject(event_type: &str, shard: u32, rulesets: &[String]) -> String {
    if rulesets.iter().any(|rule_set| rule_set == event_type) {
        format!("events.ruleset.{}.shard.{shard}", engine::sha256_hex(event_type.as_bytes()))
    } else {
        format!("events.shard.{shard}")
    }
}
