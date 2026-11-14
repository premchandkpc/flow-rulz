use super::{ContractRef, EventIdentity};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    Async,
    RequestReply,
    Batch,
    Scheduled,
    Replay,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventEnvelope {
    pub identity: EventIdentity,
    pub event_type: String,
    pub occurred_at: DateTime<Utc>,
    pub input_contract: ContractRef,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
    pub mode: ExecutionMode,
    pub payload: Bytes,
    pub headers: BTreeMap<String, String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
}

impl EventEnvelope {
    pub fn validate(&self, max_payload_bytes: usize) -> Result<(), String> {
        self.validate_identity()?;
        self.validate_contract()?;
        if self.payload.len() > max_payload_bytes {
            return Err("event payload exceeds configured limit".into());
        }
        serde_json::from_slice::<serde_json::Value>(&self.payload)
            .map_err(|_| "event payload must be valid JSON".to_string())?;
        Ok(())
    }

    fn validate_identity(&self) -> Result<(), String> {
        for (name, value) in [
            ("tenant_id", &self.identity.tenant_id),
            ("event_id", &self.identity.event_id),
            ("partition_key", &self.identity.partition_key),
            ("event_type", &self.event_type),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{name} is required"));
            }
        }
        Ok(())
    }

    fn validate_contract(&self) -> Result<(), String> {
        if self.input_contract.name.trim().is_empty()
            || self.input_contract.version.trim().is_empty()
            || self.input_contract.content_hash.trim().is_empty()
        {
            return Err("input contract must be immutable and versioned".into());
        }
        Ok(())
    }
}
