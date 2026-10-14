use crate::domain::{ExecutionCommit, LogPosition, LogRecord, LogRecordKind, PartitionId};
use async_trait::async_trait;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum LogError {
    #[error("log unavailable: {0}")]
    Unavailable(String),
    #[error("WAL storage is full: {0}")]
    StorageFull(String),
    #[error("record identity conflict: {0}")]
    IdentityConflict(String),
    #[error("partition fenced: attempted {attempted}, current {current}")]
    Fenced { attempted: u64, current: u64 },
    #[error("invalid request: {0}")]
    Invalid(String),
    #[error("integrity failure: {0}")]
    Integrity(String),
}

#[derive(Debug, Clone)]
pub struct NewLogRecord {
    pub record_id: String,
    pub partition: PartitionId,
    pub kind: LogRecordKind,
    pub tenant_id: String,
    pub event_id: String,
    pub payload: bytes::Bytes,
    pub checksum_sha256: String,
}

#[async_trait]
pub trait EventLog: Send + Sync {
    async fn append(&self, record: NewLogRecord) -> Result<LogPosition, LogError>;
    async fn commit_execution(&self, commit: ExecutionCommit) -> Result<LogPosition, LogError>;
    async fn read(
        &self,
        partition: PartitionId,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<LogRecord>, LogError>;
    async fn current_position(&self, partition: PartitionId) -> Result<LogPosition, LogError>;
    async fn advance_fence(
        &self,
        partition: PartitionId,
        fencing_epoch: u64,
    ) -> Result<LogPosition, LogError>;
}
