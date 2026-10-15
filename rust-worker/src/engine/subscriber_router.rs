//! Generic subscription matching for event deliveries.
//!
//! This module resolves tenant-domain subscriptions inside the event transaction.
//! It creates durable delivery intents only; NATS publication is handled by the
//! outbox dispatcher after commit, independently of rule evaluation.
use super::{sha256_hex, EventEnvelope};
use super::rules::Effect;
use anyhow::{bail, Context, Result};
use sqlx::{Postgres, Row, Transaction};

const MAX_SUBSCRIBER_FANOUT: usize = 100;

pub(super) async fn route_subscribers(
    tx: &mut Transaction<'_, Postgres>,
    event: &EventEnvelope,
) -> Result<Vec<Effect>> {
    let rows = sqlx::query(
        "SELECT s.service_id FROM tenant_domain_services s \
         JOIN tenant_domain_subscriptions sub ON sub.tenant_id=s.tenant_id AND sub.service_id=s.service_id \
         JOIN tenant_domains d ON d.tenant_id=sub.tenant_id AND d.domain_id=sub.domain_id \
         WHERE s.tenant_id=$1 AND s.active=TRUE AND $2=ANY(d.event_types) \
         ORDER BY s.service_id LIMIT 101",
    )
    .bind(&event.tenant_id)
    .bind(&event.event_type)
    .fetch_all(&mut **tx)
    .await
    .context("load tenant domain-service subscribers")?;

    if rows.len() > MAX_SUBSCRIBER_FANOUT {
        bail!(
            "event type {} exceeds the {}-service fan-out limit",
            event.event_type,
            MAX_SUBSCRIBER_FANOUT
        );
    }

    let payload = serde_json::to_value(event).context("serialize subscribed event envelope")?;
    let mut effects = Vec::with_capacity(rows.len());
    for row in rows {
        let service_id: String = row.try_get("service_id")?;
        let effect_id = sha256_hex(
            format!(
                "{}|{}|service-subscription|1|{}|0",
                event.tenant_id, event.id, service_id
            )
            .as_bytes(),
        );
        effects.push(Effect {
            id: effect_id,
            tenant_id: event.tenant_id.clone(),
            destination: service_id,
            name: event.event_type.clone(),
            payload: payload.clone(),
            effect_type: "service_event",
        });
    }
    Ok(effects)
}
