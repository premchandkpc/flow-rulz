use crate::{
    domain::{
        deterministic_id, identity::encode_hex, EventEnvelope, ExecutionMode, LogPosition,
        LogRecordKind, PartitionId,
    },
    ports::{
        event_log::{EventLog, LogError, NewLogRecord},
        registries::{ContractRegistry, RegistryError},
    },
};
use async_trait::async_trait;
use bytes::Bytes;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum AcceptError {
    #[error("invalid event: {0}")]
    Invalid(String),
    #[error(transparent)]
    Log(#[from] LogError),
    #[error(transparent)]
    Registry(#[from] RegistryError),
}

#[async_trait]
pub trait AcceptEvent: Send + Sync {
    async fn accept(
        &self,
        event: &EventEnvelope,
        partition: PartitionId,
    ) -> Result<LogPosition, AcceptError>;
}

pub struct AcceptEventUseCase {
    log: Arc<dyn EventLog>,
    contracts: Arc<dyn ContractRegistry>,
    max_payload_bytes: usize,
}

impl AcceptEventUseCase {
    pub fn new(
        log: Arc<dyn EventLog>,
        contracts: Arc<dyn ContractRegistry>,
        max_payload_bytes: usize,
    ) -> Self {
        Self {
            log,
            contracts,
            max_payload_bytes,
        }
    }
}

#[async_trait]
impl AcceptEvent for AcceptEventUseCase {
    async fn accept(
        &self,
        event: &EventEnvelope,
        partition: PartitionId,
    ) -> Result<LogPosition, AcceptError> {
        event
            .validate(self.max_payload_bytes)
            .map_err(AcceptError::Invalid)?;
        self.contracts.get_contract(&event.input_contract).await?;

        let payload_hash = encode_hex(&Sha256::digest(&event.payload));
        let record_id = deterministic_id(
            "event.accepted",
            &[
                &event.identity.tenant_id,
                &event.identity.partition_key,
                &event.identity.event_id,
                &event.input_contract.content_hash,
                &payload_hash,
            ],
        );
        let mode = match event.mode {
            ExecutionMode::Async => "async",
            ExecutionMode::RequestReply => "request_reply",
            ExecutionMode::Batch => "batch",
            ExecutionMode::Scheduled => "scheduled",
            ExecutionMode::Replay => "replay",
        };

        // Persist the entire immutable envelope, not only the business payload.
        // payload_hex preserves the original bytes for deterministic replay.
        let accepted_payload = json!({
            "identity": {
                "tenant_id": event.identity.tenant_id,
                "event_id": event.identity.event_id,
                "partition_key": event.identity.partition_key,
            },
            "event_type": event.event_type,
            "occurred_at": event.occurred_at,
            "input_contract": event.input_contract,
            "correlation_id": event.correlation_id,
            "causation_id": event.causation_id,
            "mode": mode,
            "headers": event.headers,
            "payload_hex": encode_hex(&event.payload),
            "payload_sha256": payload_hash,
        });
        let accepted_payload = serde_json::to_vec(&accepted_payload)
            .map_err(|error| AcceptError::Invalid(error.to_string()))?;
        let checksum_sha256 = encode_hex(&Sha256::digest(&accepted_payload));

        Ok(self
            .log
            .append(NewLogRecord {
                record_id,
                partition,
                kind: LogRecordKind::EventAccepted,
                tenant_id: event.identity.tenant_id.clone(),
                event_id: event.identity.event_id.clone(),
                payload: Bytes::from(accepted_payload),
                checksum_sha256,
            })
            .await?)
    }
}
