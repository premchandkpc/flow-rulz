use crate::{domain::{deterministic_id, identity::encode_hex, EventEnvelope, LogPosition, PartitionId, LogRecordKind}, ports::{event_log::{EventLog, LogError, NewLogRecord}, registries::{ContractRegistry, RegistryError}}};
use async_trait::async_trait; use sha2::{Digest,Sha256}; use std::sync::Arc; use thiserror::Error;
#[derive(Debug,Error)] pub enum AcceptError {#[error("invalid event: {0}")] Invalid(String),#[error(transparent)] Log(#[from] LogError),#[error(transparent)] Registry(#[from] RegistryError)}
#[async_trait] pub trait AcceptEvent:Send+Sync {async fn accept(&self,event:&EventEnvelope,partition:PartitionId)->Result<LogPosition,AcceptError>;}
pub struct AcceptEventUseCase {log:Arc<dyn EventLog>,contracts:Arc<dyn ContractRegistry>,max_payload_bytes:usize}
impl AcceptEventUseCase {pub fn new(log:Arc<dyn EventLog>,contracts:Arc<dyn ContractRegistry>,max_payload_bytes:usize)->Self{Self{log,contracts,max_payload_bytes}}}
#[async_trait] impl AcceptEvent for AcceptEventUseCase {async fn accept(&self,event:&EventEnvelope,partition:PartitionId)->Result<LogPosition,AcceptError>{
event.validate(self.max_payload_bytes).map_err(AcceptError::Invalid)?;
let _contract=self.contracts.get_contract(&event.input_contract).await?;
let payload_hash=encode_hex(&Sha256::digest(&event.payload));
let record_id=deterministic_id("event.accepted",&[&event.identity.tenant_id,&event.identity.partition_key,&event.identity.event_id,&event.input_contract.content_hash,&payload_hash]);
Ok(self.log.append(NewLogRecord{record_id,partition,kind:LogRecordKind::EventAccepted,tenant_id:event.identity.tenant_id.clone(),event_id:event.identity.event_id.clone(),payload:event.payload.clone(),checksum_sha256:payload_hash}).await?)
}}