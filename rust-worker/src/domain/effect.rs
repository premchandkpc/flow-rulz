use super::{ContractRef, OutputRoute};
use crate::domain::identity::{deterministic_id, encode_hex};
use bytes::Bytes;
use sha2::{Digest, Sha256};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EffectKind {
    PublishEvent,
    InvokeCommand,
    Reply,
    Schedule,
}

#[derive(Debug, Clone)]
pub struct EffectIntent {
    pub effect_id: String,
    pub effect_key: String,
    pub tenant_id: String,
    pub source_event_id: String,
    pub execution_id: String,
    pub output_contract: ContractRef,
    pub route: OutputRoute,
    pub kind: EffectKind,
    pub payload: Bytes,
    pub payload_sha256: String,
    pub partition_key: String,
}

impl EffectIntent {
    pub fn validate(&self) -> Result<(), String> {
        if self.effect_id.is_empty()
            || self.effect_key.trim().is_empty()
            || self.execution_id.is_empty()
        {
            return Err("effect_id, effect_key and execution_id are required".into());
        }
        if self.tenant_id.is_empty() || self.source_event_id.is_empty() {
            return Err("effect tenant and source event are required".into());
        }
        self.route.validate()?;
        if self.output_contract.name.trim().is_empty()
            || self.output_contract.version.trim().is_empty()
            || !is_sha256_hex(&self.output_contract.content_hash)
        {
            return Err("output contract must be immutable and versioned".into());
        }
        if self.partition_key.trim().is_empty() {
            return Err("effect partition key is required".into());
        }
        let expected_id = deterministic_id(
            "effect",
            &[
                &self.tenant_id,
                &self.partition_key,
                &self.source_event_id,
                &self.execution_id,
                &self.effect_key,
            ],
        );
        if self.effect_id != expected_id {
            return Err("effect_id does not match its deterministic identity".into());
        }
        if self.payload.is_empty() {
            return Err("effect payload is required".into());
        }
        let parsed_payload: serde_json::Value = serde_json::from_slice(&self.payload)
            .map_err(|_| "effect payload must be valid JSON".to_string())?;
        let canonical_payload = serde_json::to_vec(&parsed_payload)
            .map_err(|_| "effect payload could not be canonicalized".to_string())?;
        let actual_hash = encode_hex(&Sha256::digest(&canonical_payload));
        if self.payload_sha256 != actual_hash {
            return Err("effect payload_sha256 does not match canonical JSON payload".into());
        }
        Ok(())
    }
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
