//! Durable quarantine persistence for poison messages and permanent failures.
use crate::engine;
use anyhow::{Context, Result};
use sqlx::PgPool;

pub(super) async fn persist_quarantine(
    pool: &PgPool,
    source_type: &str,
    source_id: &str,
    tenant_id: Option<&str>,
    raw: &[u8],
    error: &str,
) -> Result<()> {
    let quarantine_id = engine::sha256_hex(format!("{source_type}|{source_id}|{}", engine::sha256_hex(raw)).as_bytes());
    sqlx::query(
        "INSERT INTO quarantine (quarantine_id,source_type,source_id,error_class,payload_ref,event_id,tenant_id,error_message,created_at,updated_at) \
         VALUES ($1,$2,$3,'permanent','nats:flowrulz',CASE WHEN $2='event' THEN $3 ELSE NULL END,$4,$5,NOW(),NOW()) ON CONFLICT (quarantine_id) DO NOTHING"
    ).bind(quarantine_id).bind(source_type).bind(source_id).bind(tenant_id).bind(error)
     .execute(pool).await.context("persist quarantine record")?;
    Ok(())
}
