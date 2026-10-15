//! Shared-domain Rust implementation for FlowRulz business-event processing.
//! This module intentionally uses the existing PostgreSQL schema and event contract.
use anyhow::{bail, Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use self::rules::{Effect, RuleRevision};

mod lease;
mod persistence;
mod rules;
mod subscriber_router;
pub(crate) use lease::{acquire_lease, release_lease, renew_lease};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::PgPool;

#[derive(Debug, Clone)]
pub struct EventEnvelope {
    pub id: String,
    pub event_type: String,
    pub tenant_id: String,
    pub partition_key: String,
    pub workflow_id: Option<String>,
    pub occurred_at: DateTime<Utc>,
    pub data: Value,
    pub headers: HashMap<String, String>,
    pub contract: Option<ContractRef>,
    pub message_kind: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    /// Unknown event metadata is preserved across deserialize/serialize cycles.
    pub meta: HashMap<String, Value>,
}

#[derive(Serialize)]
struct EventEnvelopeWireRef<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    event_type: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    workflow_id: &'a Option<String>,
    occurred_at: DateTime<Utc>,
    data: &'a Value,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    headers: &'a HashMap<String, String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    contract: &'a Option<ContractRef>,
    #[serde(skip_serializing_if = "Option::is_none")]
    message_kind: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: &'a Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    causation_id: &'a Option<String>,
    meta: HashMap<String, Value>,
}

impl Serialize for EventEnvelope {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where S: Serializer {
        let mut meta = self.meta.clone();
        meta.insert("tenant_id".into(), Value::String(self.tenant_id.clone()));
        meta.insert("partition_key".into(), Value::String(self.partition_key.clone()));
        EventEnvelopeWireRef {
            id: &self.id,
            event_type: &self.event_type,
            workflow_id: &self.workflow_id,
            occurred_at: self.occurred_at,
            data: &self.data,
            headers: &self.headers,
            contract: &self.contract,
            message_kind: &self.message_kind,
            correlation_id: &self.correlation_id,
            causation_id: &self.causation_id,
            meta,
        }.serialize(serializer)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EventEnvelopeWire {
    id: String,
    #[serde(rename = "type")]
    event_type: String,
    #[serde(default)]
    tenant_id: Option<String>,
    #[serde(default)]
    partition_key: Option<String>,
    #[serde(default)]
    workflow_id: Option<String>,
    occurred_at: DateTime<Utc>,
    #[serde(default)]
    data: Value,
    #[serde(default)]
    headers: HashMap<String, String>,
    #[serde(default)]
    contract: Option<ContractRef>,
    #[serde(default)]
    message_kind: Option<String>,
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    causation_id: Option<String>,
    #[serde(default)]
    meta: HashMap<String, Value>,
}

impl<'de> Deserialize<'de> for EventEnvelope {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where D: Deserializer<'de> {
        let wire = EventEnvelopeWire::deserialize(deserializer)?;
        let tenant_id = wire.meta.get("tenant_id").and_then(Value::as_str)
            .map(str::to_owned).or(wire.tenant_id)
            .ok_or_else(|| serde::de::Error::custom("meta.tenant_id is required"))?;
        let partition_key = wire.meta.get("partition_key").and_then(Value::as_str)
            .map(str::to_owned).or(wire.partition_key)
            .ok_or_else(|| serde::de::Error::custom("meta.partition_key is required"))?;
        if wire.meta.get("tenant_id").is_some_and(|v| !v.is_string()) {
            return Err(serde::de::Error::custom("meta.tenant_id must be a string"));
        }
        if wire.meta.get("partition_key").is_some_and(|v| !v.is_string()) {
            return Err(serde::de::Error::custom("meta.partition_key must be a string"));
        }
        Ok(Self {
            id: wire.id, event_type: wire.event_type, tenant_id, partition_key,
            workflow_id: wire.workflow_id, occurred_at: wire.occurred_at, data: wire.data,
            headers: wire.headers, contract: wire.contract, message_kind: wire.message_kind,
            correlation_id: wire.correlation_id, causation_id: wire.causation_id, meta: wire.meta,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ContractRef {
    pub name: String,
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
}

impl EventEnvelope {
    pub fn validate(&self) -> Result<()> {
        if self.id.trim().is_empty() { bail!("event id is required"); }
        if self.event_type.trim().is_empty() { bail!("event type is required"); }
        if self.tenant_id.trim().is_empty() { bail!("tenant_id is required"); }
        if self.partition_key.trim().is_empty() { bail!("partition_key is required"); }
        Ok(())
    }

    pub fn virtual_shard(&self, num_shards: u32) -> Result<u32> {
        if num_shards == 0 { bail!("NUM_SHARDS must be greater than zero"); }
        let key = format!("{}:{}", self.tenant_id, self.partition_key);
        Ok(fnv1a32(key.as_bytes()) % num_shards)
    }
}

pub fn fnv1a32(bytes: &[u8]) -> u32 {
    let mut hash = 2_166_136_261_u32;
    for byte in bytes { hash ^= u32::from(*byte); hash = hash.wrapping_mul(16_777_619); }
    hash
}

pub enum ProcessOutcome {
    Processed { execution_id: String, decision_hash: String, matched_rules: Vec<String> },
    Duplicate { execution_id: String },
}

pub async fn process_event(
    pool: &PgPool,
    envelope: &EventEnvelope,
    worker_id: &str,
    fencing_token: i64,
    num_shards: u32,
) -> Result<ProcessOutcome> {
    validate_event_for_processing(envelope, worker_id, fencing_token)?;
    let shard = envelope.virtual_shard(num_shards)?;
    let mut tx = pool.begin().await.context("begin event transaction")?;
    persistence::assert_lease(&mut tx, shard, worker_id, fencing_token).await?;

    if !persistence::insert_inbox(&mut tx, envelope).await? {
        let execution_id = load_duplicate_execution(&mut tx, envelope).await?;
        tx.commit().await.context("commit duplicate acknowledgement transaction")?;
        return Ok(ProcessOutcome::Duplicate { execution_id });
    }

    let outcome = persist_new_event(&mut tx, envelope).await?;
    tx.commit().await.context("commit event transaction")?;
    Ok(outcome)
}

fn validate_event_for_processing(
    envelope: &EventEnvelope,
    worker_id: &str,
    fencing_token: i64,
) -> Result<()> {
    envelope.validate()?;
    validate_worker_lease(worker_id, fencing_token)
}

async fn load_duplicate_execution(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    envelope: &EventEnvelope,
) -> Result<String> {
    persistence::load_duplicate_execution(tx, envelope).await
}

async fn persist_new_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    envelope: &EventEnvelope,
) -> Result<ProcessOutcome> {
    let revision = persistence::load_active_revision(tx, envelope).await?;
    validate_contract_support(&revision, envelope)?;
    let (matched_rules, mut effects) = rules::evaluate(&revision, envelope)?;
    // Keep the decision hash scoped to rule evaluation. Subscription routing is
    // a separate platform policy that contributes durable delivery intents.
    let decision_hash = calculate_decision_hash(&revision, envelope, &matched_rules, &effects);
    effects.extend(subscriber_router::route_subscribers(tx, envelope).await?);
    let execution_id = calculate_execution_id(envelope, revision.revision);

    persist_execution_result(tx, &execution_id, envelope, &revision, &decision_hash, &effects).await?;

    Ok(ProcessOutcome::Processed { execution_id, decision_hash, matched_rules })
}

async fn persist_execution_result(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    execution_id: &str,
    envelope: &EventEnvelope,
    revision: &RuleRevision,
    decision_hash: &str,
    effects: &[Effect],
) -> Result<()> {
    persistence::persist_execution(tx, execution_id, envelope, revision, decision_hash).await?;
    persistence::persist_outbox_effects(tx, execution_id, effects).await?;
    persistence::mark_inbox_committed(tx, envelope, execution_id).await
}

fn validate_contract_support(revision: &RuleRevision, envelope: &EventEnvelope) -> Result<()> {
    if let Some(required) = &revision.input_contract {
        if envelope.contract.is_none() {
            bail!("rule set {} requires contract {}@{}", revision.rule_id, required.name, required.version);
        }
        bail!("Rust worker cannot safely evaluate contract-bound rule set {} yet; route this event to the Go worker", revision.rule_id);
    }
    if envelope.contract.is_some() {
        bail!("Rust worker cannot safely validate event contracts yet; route this event to the Go worker");
    }
    Ok(())
}

fn validate_worker_lease(worker_id: &str, fencing_token: i64) -> Result<()> {
    if worker_id.trim().is_empty() || fencing_token <= 0 {
        bail!("worker id and positive fencing token are required");
    }
    Ok(())
}

fn calculate_decision_hash(
    revision: &RuleRevision,
    envelope: &EventEnvelope,
    matched_rules: &[String],
    effects: &[Effect],
) -> String {
    let mut material = revision.content_hash.clone();
    for id in matched_rules { material.push_str(id); }
    for effect in effects { material.push_str(&effect.id); }
    material.push_str(&envelope.id);
    sha256_hex(material.as_bytes())
}

fn calculate_execution_id(envelope: &EventEnvelope, revision: i64) -> String {
    sha256_hex(format!(
        "execution|{}:{}|{}:{}|{}:{}|{}",
        envelope.tenant_id.len(), envelope.tenant_id,
        envelope.id.len(), envelope.id,
        envelope.event_type.len(), envelope.event_type,
        revision
    ).as_bytes())
}

pub fn sha256_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut digest = Sha256::new();
    digest.update(bytes);
    let output = digest.finalize();
    let mut encoded = String::with_capacity(output.len() * 2);
    for byte in output {
        encoded.push(HEX[usize::from(byte >> 4)] as char);
        encoded.push(HEX[usize::from(byte & 0x0f)] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use serde_json::json;
    use super::rules::{Action, Predicate, build_effect, compare_eq, eval_predicate, expand_template};

    #[test]
    fn contract_bound_events_fail_closed_until_contract_parity_exists() {
        let revision = rules::RuleRevision {
            tenant_scope: "tenant-a".into(),
            rule_id: "orders.created".into(),
            revision: 1,
            content_hash: "hash".into(),
            match_mode: "all_matches".into(),
            input_contract: Some(rules::ContractRef {
                name: "OrderEvent".into(),
                version: "1".into(),
            }),
            compiled: vec![],
        };
        let event = EventEnvelope {
            id: "e".into(), event_type: "orders.created".into(), tenant_id: "tenant-a".into(),
            partition_key: "p".into(), workflow_id: None, occurred_at: Utc::now(),
            data: json!({}), headers: Default::default(), contract: None, message_kind: None,
            correlation_id: None, causation_id: None, meta: Default::default(),
        };
        assert!(validate_contract_support(&revision, &event).is_err());
    }

    #[test]
    fn sha256_hex_matches_standard_sha256_vector() {
        assert_eq!(
            sha256_hex(b"abc"),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn shard_hash_matches_fnv1a_contract() {
        assert_eq!(fnv1a32(b""), 2_166_136_261);
        assert_eq!(fnv1a32(b"a"), 3_826_002_220);
    }

    #[test]
    fn evaluates_nested_predicates_and_paths() {
        let predicate: Predicate = serde_json::from_value(json!({
            "all": [
                {"path":"$.amount","op":"gte","value":10},
                {"path":"$.status","op":"eq","value":"ready"}
            ]
        })).unwrap();
        assert!(eval_predicate(&predicate, &json!({"amount":10,"status":"ready"})).unwrap());
        assert!(!eval_predicate(&predicate, &json!({"amount":9,"status":"ready"})).unwrap());
    }

    #[test]
    fn rejects_empty_partition_key_for_child_event() {
        let revision = RuleRevision { tenant_scope:"t".into(), rule_id:"r".into(), revision:1, content_hash:"h".into(), match_mode:"all_matches".into(), input_contract: None, compiled:vec![] };
        let event = EventEnvelope { id:"e".into(), event_type:"r".into(), tenant_id:"t".into(), partition_key:"p".into(), workflow_id:None, occurred_at:Utc::now(), data:json!({}), headers:Default::default(), contract:None, message_kind:None, correlation_id:None, causation_id:None, meta:Default::default() };
        let action: Action = serde_json::from_value(json!({"emit_event":{"type":"child","partition_key_policy":"from_data","partition_key_path":"$.missing","data":{}}})).unwrap();
        assert!(build_effect(&revision, &event, "rule", 0, &action).is_err());
    }
    #[test]
    fn templates_preserve_single_placeholder_type_and_expand_nested_strings() {
        let event = EventEnvelope { id: "e".into(), event_type: "orders".into(), tenant_id: "t".into(),
            partition_key: "p".into(), workflow_id: None, occurred_at: Utc::now(),
            data: json!({"amount": 12, "customer": {"name": "Ada"}}), headers: Default::default(),
            contract: None, message_kind: None, correlation_id: None, causation_id: None, meta: Default::default() };
        assert_eq!(expand_template("{{$.amount}}", &event, "child-1"), json!(12));
        assert_eq!(expand_template("amount={{$.amount}} for {{$.customer.name}}", &event, "child-1"), json!("amount=12 for Ada"));
        assert_eq!(expand_template("{{new_id}}", &event, "child-1"), json!("child-1"));
    }

    #[test]
    fn numeric_string_comparison_matches_go_rules() {
        assert!(compare_eq(&json!(10), Some(&json!("10"))));
        assert!(!eval_predicate(&Predicate { all: None, any: None, not: None, path: Some("$.missing".into()), op: Some("neq".into()), value: Some(json!(1)) }, &json!({})).unwrap());
    }

    #[test]
    fn event_envelope_rejects_invalid_or_missing_meta_identity() {
        for input in [
            json!({"id":"e","type":"orders.created","occurred_at":"2026-10-14T00:00:00Z","data":{},"meta":{"partition_key":"p"}}),
            json!({"id":"e","type":"orders.created","occurred_at":"2026-10-14T00:00:00Z","data":{},"meta":{"tenant_id":42,"partition_key":"p"}}),
            json!({"id":"e","type":"orders.created","occurred_at":"2026-10-14T00:00:00Z","data":{},"meta":{"tenant_id":"t","partition_key":false}}),
            json!({"id":"e","type":"orders.created","occurred_at":"2026-10-14T00:00:00Z","data":{},"meta":{"tenant_id":"t","partition_key":"p"},"unknown_top_level":true}),
        ] {
            assert!(serde_json::from_value::<EventEnvelope>(input).is_err());
        }
    }

    #[test]
    fn event_envelope_reads_legacy_identity_but_serializes_canonical_meta() {
        let input = json!({
            "id":"e","type":"orders.created","tenant_id":"t","partition_key":"p",
            "occurred_at":"2026-10-14T00:00:00Z","data":{}
        });
        let event: EventEnvelope = serde_json::from_value(input).unwrap();
        let encoded = serde_json::to_value(event).unwrap();
        assert_eq!(encoded["meta"]["tenant_id"], "t");
        assert_eq!(encoded["meta"]["partition_key"], "p");
        assert!(encoded.get("tenant_id").is_none());
        assert!(encoded.get("partition_key").is_none());
    }

    #[test]
    fn event_envelope_round_trip_preserves_go_metadata() {
        let input = json!({
            "id": "event-1",
            "type": "orders.created",
            "meta": {"tenant_id": "tenant-a", "partition_key": "order-1", "region": "ap-south-1"},
            "occurred_at": "2026-10-14T00:00:00Z",
            "data": {"amount": 12},
            "contract": {"name": "orders.created", "version": "1", "schema_hash": "schema-hash"},
            "message_kind": "event",
            "correlation_id": "correlation-1",
            "causation_id": "causation-1",
            "headers": {"source": "test"}
        });
        let event: EventEnvelope = serde_json::from_value(input).unwrap();
        let encoded = serde_json::to_value(event).unwrap();

        assert_eq!(encoded["contract"]["name"], "orders.created");
        assert_eq!(encoded["contract"]["version"], "1");
        assert_eq!(encoded["contract"]["schema_hash"], "schema-hash");
        assert_eq!(encoded["message_kind"], "event");
        assert_eq!(encoded["correlation_id"], "correlation-1");
        assert_eq!(encoded["causation_id"], "causation-1");
        assert_eq!(encoded["headers"]["source"], "test");
        assert_eq!(encoded["meta"]["tenant_id"], "tenant-a");
        assert_eq!(encoded["meta"]["partition_key"], "order-1");
        assert_eq!(encoded["meta"]["region"], "ap-south-1");
        assert!(encoded.get("tenant_id").is_none());
        assert!(encoded.get("partition_key").is_none());
    }

}
