use bytes::Bytes; use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)] pub struct PartitionId(pub u32);
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)] pub struct LogPosition(pub u64);
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum LogRecordKind {
    #[serde(rename = "event.accepted")]
    EventAccepted,
    #[serde(rename = "execution.committed")]
    ExecutionCommitted,
    #[serde(rename = "effect.settled")]
    EffectSettled,
    #[serde(rename = "schedule.requested")]
    ScheduleRequested,
    #[serde(rename = "schedule.cancelled")]
    ScheduleCancelled,
    #[serde(rename = "schedule.fired")]
    ScheduleFired,
    #[serde(rename = "event.quarantined")]
    EventQuarantined,
    #[serde(rename = "consumer.progressed")]
    ConsumerProgressed,
    #[serde(rename = "fence.advanced")]
    FenceAdvanced,
}
#[derive(Debug, Clone)] pub struct LogRecord { pub record_id: String, pub partition: PartitionId, pub position: LogPosition, pub kind: LogRecordKind, pub tenant_id: String, pub event_id: String, pub payload: Bytes, pub checksum_sha256: String }
impl LogRecord { pub fn validate(&self) -> Result<(), String> { if self.record_id.is_empty() || self.tenant_id.is_empty() || self.event_id.is_empty() { return Err("log record identity is incomplete".into()); } if self.position.0 == 0 { return Err("log position must be positive".into()); } if self.payload.is_empty() { return Err("log record payload is required".into()); } if self.checksum_sha256.is_empty() { return Err("log record checksum is required".into()); } Ok(()) } }