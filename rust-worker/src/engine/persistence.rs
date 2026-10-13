//! Transactional persistence for event processing.
use super::{sha256_hex, EventEnvelope};
use super::rules::{Effect, RuleRevision};
use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
use sqlx::{Postgres, Transaction, Row};

pub(super) async fn assert_lease(tx: &mut Transaction<'_, Postgres>, shard: u32, owner: &str, token: i64) -> Result<()> {
    let row = sqlx::query(
        "SELECT fencing_token, expires_at > NOW() AS active FROM shard_leases \
         WHERE virtual_shard=$1 AND owner=$2 FOR SHARE"
    ).bind(i32::try_from(shard)?).bind(owner).fetch_optional(&mut **tx).await?
     .ok_or_else(|| anyhow!("shard lease is not held by this worker"))?;
    let current: i64 = row.try_get("fencing_token")?;
    let active: bool = row.try_get("active")?;
    if !active || token <= 0 || current != token { bail!("shard lease expired or fencing token mismatch"); }
    Ok(())
}

pub(super) async fn insert_inbox(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<bool> {
    let affected = sqlx::query(
        "INSERT INTO inbox (tenant_id,event_id,status,first_seen_at,partition_key,rule_set,payload) \
         VALUES ($1,$2,'processing',NOW(),$3,$4,$5) ON CONFLICT (tenant_id,event_id) DO NOTHING"
    ).bind(&event.tenant_id).bind(&event.id).bind(&event.partition_key)
     .bind(&event.event_type).bind(&event.data).execute(&mut **tx).await
     .context("insert inbox row")?.rows_affected();
    Ok(affected == 1)
}

pub(super) async fn load_duplicate_execution(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<String> {
    let execution_id: Option<String> = sqlx::query_scalar(
        "SELECT execution_id FROM inbox WHERE tenant_id=$1 AND event_id=$2"
    ).bind(&event.tenant_id).bind(&event.id).fetch_optional(&mut **tx).await?.flatten();
    let Some(execution_id) = execution_id else {
        bail!("inbox duplicate has no committed execution; refusing unsafe acknowledgement");
    };
    let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM executions WHERE execution_id=$1)")
        .bind(&execution_id).fetch_one(&mut **tx).await?;
    if !exists { bail!("inbox points to missing execution; refusing unsafe acknowledgement"); }
    Ok(execution_id)
}

pub(super) async fn load_active_revision(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<RuleRevision> {
    let row = sqlx::query(
        "SELECT r.tenant_scope,r.rule_id,r.revision,r.content_hash,r.match_mode,r.compiled \
         FROM rule_revisions r JOIN rule_activations a \
         ON a.tenant_scope=r.tenant_scope AND a.rule_set=r.rule_id AND a.revision=r.revision \
         WHERE a.tenant_scope=$1 AND a.rule_set=$2"
    ).bind(&event.tenant_id).bind(&event.event_type).fetch_optional(&mut **tx).await
     .context("load active rule revision")?.ok_or_else(|| anyhow!(
        "active rule revision not found for tenant={} rule_set={}", event.tenant_id, event.event_type
     ))?;
    Ok(RuleRevision {
        tenant_scope: row.try_get("tenant_scope")?,
        rule_id: row.try_get("rule_id")?,
        revision: row.try_get("revision")?,
        content_hash: row.try_get("content_hash")?,
        match_mode: row.try_get("match_mode")?,
        compiled: serde_json::from_value(row.try_get::<Value, _>("compiled")?).context("decode compiled rule set")?,
    })
}

pub(super) async fn load_service_subscriber_effects(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope) -> Result<Vec<Effect>> {
    let rows = sqlx::query("SELECT s.service_id FROM tenant_domain_services s JOIN tenant_domain_subscriptions sub ON sub.tenant_id=s.tenant_id AND sub.service_id=s.service_id JOIN tenant_domains d ON d.tenant_id=sub.tenant_id AND d.domain_id=sub.domain_id WHERE s.tenant_id=$1 AND s.active=TRUE AND $2=ANY(d.event_types) ORDER BY s.service_id LIMIT 101")
        .bind(&event.tenant_id).bind(&event.event_type).fetch_all(&mut **tx).await
        .context("load tenant domain-service subscribers")?;
    if rows.len() > 100 { bail!("event type {} exceeds the 100-service fan-out limit", event.event_type); }
    let payload = serde_json::to_value(event).context("serialize subscribed event envelope")?;
    let mut effects = Vec::with_capacity(rows.len());
    for row in rows {
        let service_id: String = row.try_get("service_id")?;
        let effect_id = sha256_hex(format!("{}|{}|service-subscription|1|{}|0", event.tenant_id, event.id, service_id).as_bytes());
        effects.push(Effect { id: effect_id, tenant_id: event.tenant_id.clone(), destination: service_id,
            name: event.event_type.clone(), payload: payload.clone(), effect_type: "service_event" });
    }
    Ok(effects)
}

pub(super) async fn persist_execution(
    tx: &mut Transaction<'_, Postgres>, execution_id: &str, event: &EventEnvelope,
    revision: &RuleRevision, decision_hash: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO executions (execution_id,event_id,tenant_id,rule_set,revision,decision_hash,status,created_at,completed_at) \
         VALUES ($1,$2,$3,$4,$5,$6,'completed',NOW(),NOW())"
    ).bind(execution_id).bind(&event.id).bind(&event.tenant_id).bind(&event.event_type)
     .bind(revision.revision).bind(decision_hash).execute(&mut **tx).await.context("persist execution")?;
    Ok(())
}

pub(super) async fn persist_outbox_effects(tx: &mut Transaction<'_, Postgres>, execution_id: &str, effects: &[Effect]) -> Result<()> {
    for effect in effects {
        sqlx::query(
            "INSERT INTO outbox_effects (effect_id,execution_id,tenant_id,destination,name,payload,effect_type,status,attempts,max_attempts,available_at,created_at,updated_at) \
             VALUES ($1,$2,$3,$4,$5,$6,$7,'pending',0,5,NOW(),NOW(),NOW()) ON CONFLICT (effect_id) DO NOTHING"
        ).bind(&effect.id).bind(execution_id).bind(&effect.tenant_id).bind(&effect.destination)
         .bind(&effect.name).bind(&effect.payload).bind(effect.effect_type).execute(&mut **tx).await
         .context("persist outbox effect")?;
    }
    Ok(())
}

pub(super) async fn mark_inbox_committed(tx: &mut Transaction<'_, Postgres>, event: &EventEnvelope, execution_id: &str) -> Result<()> {
    let affected = sqlx::query(
        "UPDATE inbox SET status='committed',execution_id=$3,committed_at=NOW() WHERE tenant_id=$1 AND event_id=$2 AND status='processing'"
    ).bind(&event.tenant_id).bind(&event.id).bind(execution_id).execute(&mut **tx).await
     .context("mark inbox committed")?.rows_affected();
    if affected != 1 { bail!("inbox commit transition affected {affected} rows"); }
    Ok(())
}
