use super::{ContractRef, OutputRoute};
use crate::domain::identity::encode_hex;
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
        if self.effect_id.is_empty() || self.execution_id.is_empty() {
            return Err("effect_id and execution_id are required".into());
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
        if self.payload.is_empty() {
            return Err("effect payload is required".into());
        }
        serde_json::from_slice::<serde_json::Value>(&self.payload)
            .map_err(|_| "effect payload must be valid JSON".to_string())?;
        let actual_hash = encode_hex(&Sha256::digest(&self.payload));
        if self.payload_sha256 != actual_hash {
            return Err("effect payload_sha256 does not match payload".into());
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
