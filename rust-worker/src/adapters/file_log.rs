//! Single-node append-only WAL adapter.
//!
//! This adapter provides durable local append, replay and idempotency. It is
//! deliberately not described as a replicated/distributed broker: it has no
//! quorum protocol or cross-node fencing authority.
use crate::domain::{
    deterministic_id, encode_hex, EffectKind, ExecutionCommit, ExecutionOutcome, LogPosition,
    LogRecord, LogRecordKind, PartitionId,
};
use crate::ports::event_log::{EventLog, LogError, NewLogRecord};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct FrameBody {
    record_id: String,
    partition: u32,
    position: u64,
    kind: LogRecordKind,
    tenant_id: String,
    event_id: String,
    payload_hex: String,
    payload_sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Frame {
    body: FrameBody,
    frame_sha256: String,
}

struct PartitionState {
    file: tokio::fs::File,
    next_position: u64,
    records: Vec<LogRecord>,
    by_id: HashMap<String, (LogRecord, LogPosition)>,
    highest_fencing_epoch: u64,
}

pub struct FileEventLog {
    root: PathBuf,
    partitions: Mutex<HashMap<PartitionId, PartitionState>>,
    record_index: Mutex<HashMap<String, (PartitionId, LogRecord, LogPosition)>>,
}

impl FileEventLog {
    /// Open a WAL directory, replaying all complete frames and truncating only
    /// an incomplete final frame. A complete frame with a bad checksum fails
    /// closed instead of silently discarding potentially committed data.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, LogError> {
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(storage_error)?;
        let mut partitions = HashMap::new();
        let mut record_index = HashMap::new();
        let entries = fs::read_dir(&root).map_err(storage_error)?;

        for entry in entries {
            let entry = entry.map_err(storage_error)?;
            let name = entry.file_name().to_string_lossy().to_string();
            let Some(number) = name
                .strip_prefix("partition-")
                .and_then(|name| name.strip_suffix(".wal"))
            else {
                continue;
            };
            let partition_number = number.parse::<u32>().map_err(|_| {
                LogError::Integrity(format!("invalid WAL filename {name}"))
            })?;
            let partition = PartitionId(partition_number);
            let (file, records, next_position, by_id, highest_fencing_epoch) =
                recover_partition(entry.path(), partition)?;
            for (record_id, (record, position)) in &by_id {
                if record_index
                    .insert(record_id.clone(), (partition, record.clone(), *position))
                    .is_some()
                {
                    return Err(LogError::Integrity(format!(
                        "record ID {record_id} appears in more than one partition"
                    )));
                }
            }
            partitions.insert(
                partition,
                PartitionState {
                    file: tokio::fs::File::from_std(file),
                    next_position,
                    records,
                    by_id,
                    highest_fencing_epoch,
                },
            );
        }

        Ok(Self {
            root,
            partitions: Mutex::new(partitions),
            record_index: Mutex::new(record_index),
        })
    }

    async fn ensure_partition<'a>(
        &'a self,
        partitions: &'a mut HashMap<PartitionId, PartitionState>,
        partition: PartitionId,
    ) -> Result<&'a mut PartitionState, LogError> {
        if !partitions.contains_key(&partition) {
            let path = partition_path(&self.root, partition);
            let existed = tokio::fs::try_exists(&path).await.map_err(storage_error)?;
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(path)
                .await
                .map_err(storage_error)?;
            if !existed {
                // Persist the new directory entry as well as later frame bytes.
                File::open(&self.root)
                    .and_then(|directory| directory.sync_all())
                    .map_err(storage_error)?;
            }
            partitions.insert(
                partition,
                PartitionState {
                    file,
                    next_position: 0,
                    records: Vec::new(),
                    by_id: HashMap::new(),
                    highest_fencing_epoch: 0,
                },
            );
        }
        partitions
            .get_mut(&partition)
            .ok_or_else(|| LogError::Integrity("partition state disappeared".into()))
    }
}

#[async_trait]
impl EventLog for FileEventLog {
    async fn append(&self, record: NewLogRecord) -> Result<LogPosition, LogError> {
        if record.record_id.trim().is_empty()
            || record.tenant_id.trim().is_empty()
            || record.event_id.trim().is_empty()
            || record.payload.is_empty()
        {
            return Err(LogError::Invalid(
                "record ID, tenant, event ID and payload are required".into(),
            ));
        }

        let actual_payload_hash = sha256_hex(&record.payload);
        if record.checksum_sha256 != actual_payload_hash {
            return Err(LogError::Integrity(
                "payload checksum does not match supplied checksum".into(),
            ));
        }

        // Lock ordering is always record index, then partition map.
        let mut record_index = self.record_index.lock().await;
        if let Some((existing_partition, existing, position)) =
            record_index.get(&record.record_id)
        {
            let identical = *existing_partition == record.partition
                && existing.kind == record.kind
                && existing.tenant_id == record.tenant_id
                && existing.event_id == record.event_id
                && existing.payload == record.payload
                && existing.checksum_sha256 == record.checksum_sha256;
            return if identical {
                Ok(*position)
            } else {
                Err(LogError::IdentityConflict(record.record_id))
            };
        }

        let mut partitions = self.partitions.lock().await;
        let state = self.ensure_partition(&mut partitions, record.partition).await?;

        match record.kind {
            LogRecordKind::FenceAdvanced => {
                let value: serde_json::Value =
                    serde_json::from_slice(&record.payload).map_err(|error| {
                        LogError::Invalid(format!("invalid fencing payload: {error}"))
                    })?;
                let attempted = value["fencing_epoch"].as_u64().ok_or_else(|| {
                    LogError::Invalid("fencing payload requires fencing_epoch".into())
                })?;
                if value["partition"].as_u64() != Some(u64::from(record.partition.0))
                    || record.tenant_id != "_system"
                    || record.event_id != format!("fence:{}:{attempted}", record.partition.0)
                {
                    return Err(LogError::Invalid("fencing fact identity does not match payload".into()));
                }
                let partition_text = record.partition.0.to_string();
                let expected_id = deterministic_id(
                    "fence.advanced",
                    &["_system", &partition_text, &record.event_id, ""],
                );
                if record.record_id != expected_id {
                    return Err(LogError::Invalid("fencing fact has a non-canonical record ID".into()));
                }
                if attempted <= state.highest_fencing_epoch {
                    return Err(LogError::Fenced {
                        attempted,
                        current: state.highest_fencing_epoch,
                    });
                }
            }
            LogRecordKind::ExecutionCommitted => {
                let value: serde_json::Value =
                    serde_json::from_slice(&record.payload).map_err(|error| {
                        LogError::Invalid(format!("invalid execution commit payload: {error}"))
                    })?;
                let attempted = value["fencing_epoch"].as_u64().ok_or_else(|| {
                    LogError::Invalid("execution commit requires fencing_epoch".into())
                })?;
                let partition_key = value["event"]["partition_key"].as_str().ok_or_else(|| {
                    LogError::Invalid("execution commit requires event.partition_key".into())
                })?;
                let execution_id = value["execution_id"].as_str().ok_or_else(|| {
                    LogError::Invalid("execution commit requires execution_id".into())
                })?;
                if value["event"]["tenant_id"].as_str() != Some(record.tenant_id.as_str())
                    || value["event"]["event_id"].as_str() != Some(record.event_id.as_str())
                {
                    return Err(LogError::Invalid("execution commit identity does not match record".into()));
                }
                let expected_execution_id = deterministic_id(
                    "execution.committed",
                    &[&record.tenant_id, partition_key, &record.event_id, "execution"],
                );
                let expected_record_id = deterministic_id(
                    "execution.committed",
                    &[&record.tenant_id, partition_key, &record.event_id, execution_id],
                );
                if execution_id != expected_execution_id || record.record_id != expected_record_id {
                    return Err(LogError::Invalid("execution commit has non-canonical identity".into()));
                }
                if attempted != state.highest_fencing_epoch {
                    return Err(LogError::Fenced {
                        attempted,
                        current: state.highest_fencing_epoch,
                    });
                }
            }
            _ => {}
        }

        let position = LogPosition(state.next_position + 1);
        let stored = LogRecord {
            record_id: record.record_id.clone(),
            partition: record.partition,
            position,
            kind: record.kind,
            tenant_id: record.tenant_id.clone(),
            event_id: record.event_id.clone(),
            payload: record.payload.clone(),
            checksum_sha256: record.checksum_sha256.clone(),
        };
        stored.validate().map_err(LogError::Invalid)?;

        let body = FrameBody {
            record_id: stored.record_id.clone(),
            partition: stored.partition.0,
            position: stored.position.0,
            kind: stored.kind,
            tenant_id: stored.tenant_id.clone(),
            event_id: stored.event_id.clone(),
            payload_hex: encode_hex(&stored.payload),
            payload_sha256: stored.checksum_sha256.clone(),
        };
        let frame_hash = sha256_hex(&serde_json::to_vec(&body).map_err(storage_error)?);
        let bytes = serde_json::to_vec(&Frame {
            body,
            frame_sha256: frame_hash,
        })
        .map_err(storage_error)?;
        if bytes.len() > MAX_FRAME_BYTES {
            return Err(LogError::Invalid("WAL frame exceeds 64 MiB".into()));
        }
        let frame_len = u32::try_from(bytes.len())
            .map_err(|_| LogError::Invalid("WAL frame length overflow".into()))?;
        let mut encoded = Vec::with_capacity(4 + bytes.len());
        encoded.extend_from_slice(&frame_len.to_be_bytes());
        encoded.extend_from_slice(&bytes);

        let old_len = state.file.metadata().await.map_err(storage_error)?.len();
        if let Err(error) = state.file.write_all(&encoded).await {
            let _ = state.file.set_len(old_len).await;
            let _ = state.file.sync_data().await;
            return Err(storage_error(error));
        }
        if let Err(error) = state.file.sync_data().await {
            let _ = state.file.set_len(old_len).await;
            let _ = state.file.sync_data().await;
            return Err(storage_error(error));
        }

        if stored.kind == LogRecordKind::FenceAdvanced {
            let value: serde_json::Value =
                serde_json::from_slice(&stored.payload).map_err(|error| {
                    LogError::Integrity(format!("invalid durable fencing payload: {error}"))
                })?;
            state.highest_fencing_epoch = value["fencing_epoch"]
                .as_u64()
                .ok_or_else(|| LogError::Integrity("fencing epoch missing after append".into()))?;
        }
        state.next_position = position.0;
        state.records.push(stored.clone());
        state
            .by_id
            .insert(stored.record_id.clone(), (stored.clone(), position));
        record_index.insert(
            stored.record_id.clone(),
            (stored.partition, stored, position),
        );
        Ok(position)
    }

    async fn commit_execution(&self, commit: ExecutionCommit) -> Result<LogPosition, LogError> {
        commit.validate().map_err(LogError::Invalid)?;
        let expected_execution_id = deterministic_id(
            "execution.committed",
            &[
                &commit.event.tenant_id,
                &commit.event.partition_key,
                &commit.event.event_id,
                "execution",
            ],
        );
        if commit.execution_id != expected_execution_id {
            return Err(LogError::Invalid(
                "execution_id must be deterministic for tenant, partition key and event".into(),
            ));
        }

        let outcome = match &commit.outcome {
            ExecutionOutcome::Completed => json!({"kind": "completed"}),
            ExecutionOutcome::NoMatch => json!({"kind": "no_match"}),
            ExecutionOutcome::Quarantined { category, message } => {
                json!({"kind": "quarantined", "category": category, "message": message})
            }
        };
        let mut effects = Vec::with_capacity(commit.effects.len());
        for effect in &commit.effects {
            let kind = match effect.kind {
                EffectKind::PublishEvent => "publish_event",
                EffectKind::InvokeCommand => "invoke_command",
                EffectKind::Reply => "reply",
                EffectKind::Schedule => "schedule",
            };
            effects.push(json!({
                "effect_id": effect.effect_id,
                "tenant_id": effect.tenant_id,
                "source_event_id": effect.source_event_id,
                "execution_id": effect.execution_id,
                "output_contract": effect.output_contract,
                "route": effect.route,
                "kind": kind,
                "payload_hex": encode_hex(&effect.payload),
                "payload_sha256": effect.payload_sha256,
                "partition_key": effect.partition_key,
            }));
        }
        let payload = json!({
            "execution_id": commit.execution_id,
            "event": {
                "tenant_id": commit.event.tenant_id,
                "event_id": commit.event.event_id,
                "partition_key": commit.event.partition_key,
            },
            "rule_revisions": commit.rule_revisions,
            "outcome": outcome,
            "decision_hash": commit.decision_hash,
            "matched_rule_ids": commit.matched_rule_ids,
            "result_payload_hex": encode_hex(&commit.result_payload),
            "effects": effects,
            "fencing_epoch": commit.fencing_epoch,
        });
        let payload = serde_json::to_vec(&payload).map_err(storage_error)?;
        let payload = Bytes::from(payload);
        let record_id = deterministic_id(
            "execution.committed",
            &[
                &commit.event.tenant_id,
                &commit.event.partition_key,
                &commit.event.event_id,
                &commit.execution_id,
            ],
        );
        self.append(NewLogRecord {
            record_id,
            partition: partition_for_key(&commit.event.tenant_id, &commit.event.partition_key),
            kind: LogRecordKind::ExecutionCommitted,
            tenant_id: commit.event.tenant_id,
            event_id: commit.event.event_id,
            checksum_sha256: sha256_hex(&payload),
            payload,
        })
        .await
    }

    async fn read(
        &self,
        partition: PartitionId,
        after: LogPosition,
        limit: usize,
    ) -> Result<Vec<LogRecord>, LogError> {
        if limit == 0 {
            return Err(LogError::Invalid("read limit must be positive".into()));
        }
        let partitions = self.partitions.lock().await;
        let Some(state) = partitions.get(&partition) else {
            if after.0 > 0 {
                return Err(LogError::Invalid("cursor is beyond an empty partition".into()));
            }
            return Ok(Vec::new());
        };
        if after.0 > state.next_position {
            return Err(LogError::Invalid("cursor is beyond current log position".into()));
        }
        Ok(state
            .records
            .iter()
            .filter(|record| record.position.0 > after.0)
            .take(limit)
            .cloned()
            .collect())
    }

    async fn current_position(&self, partition: PartitionId) -> Result<LogPosition, LogError> {
        let partitions = self.partitions.lock().await;
        Ok(LogPosition(
            partitions
                .get(&partition)
                .map(|state| state.next_position)
                .unwrap_or(0),
        ))
    }

    async fn advance_fence(
        &self,
        partition: PartitionId,
        fencing_epoch: u64,
    ) -> Result<LogPosition, LogError> {
        if fencing_epoch == 0 {
            return Err(LogError::Invalid("fencing epoch must be positive".into()));
        }
        let partition_text = partition.0.to_string();
        let epoch_text = fencing_epoch.to_string();
        let payload = serde_json::to_vec(&json!({
            "partition": partition.0,
            "fencing_epoch": fencing_epoch,
        }))
        .map_err(storage_error)?;
        let payload = Bytes::from(payload);
        self.append(NewLogRecord {
            record_id: deterministic_id(
                "fence.advanced",
                &["_system", &partition_text, &format!("fence:{partition_text}:{epoch_text}"), ""],
            ),
            partition,
            kind: LogRecordKind::FenceAdvanced,
            tenant_id: "_system".into(),
            event_id: format!("fence:{partition_text}:{epoch_text}"),
            checksum_sha256: sha256_hex(&payload),
            payload,
        })
        .await
    }
}

fn recover_partition(
    path: PathBuf,
    partition: PartitionId,
) -> Result<
    (
        File,
        Vec<LogRecord>,
        u64,
        HashMap<String, (LogRecord, LogPosition)>,
        u64,
    ),
    LogError,
> {
    let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(storage_error)?;
    let file_len = file.metadata().map_err(storage_error)?.len();
    let mut offset = 0_u64;
    let mut records = Vec::new();
    let mut by_id = HashMap::new();
    let mut expected_position = 1_u64;
    let mut highest_fencing_epoch = 0_u64;

    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < 4 {
            file.set_len(offset).map_err(storage_error)?;
            break;
        }
        file.seek(SeekFrom::Start(offset)).map_err(storage_error)?;
        let mut length_bytes = [0_u8; 4];
        file.read_exact(&mut length_bytes).map_err(storage_error)?;
        let frame_len = u32::from_be_bytes(length_bytes) as usize;
        if frame_len == 0 || frame_len > MAX_FRAME_BYTES {
            return Err(LogError::Integrity(format!(
                "invalid frame length at byte offset {offset}"
            )));
        }
        if remaining - 4 < frame_len as u64 {
            file.set_len(offset).map_err(storage_error)?;
            break;
        }
        let mut bytes = vec![0_u8; frame_len];
        file.read_exact(&mut bytes).map_err(storage_error)?;
        let frame: Frame = serde_json::from_slice(&bytes).map_err(|error| {
            LogError::Integrity(format!("invalid complete frame at offset {offset}: {error}"))
        })?;
        let body_bytes = serde_json::to_vec(&frame.body).map_err(storage_error)?;
        if sha256_hex(&body_bytes) != frame.frame_sha256 {
            return Err(LogError::Integrity(format!(
                "frame checksum mismatch at byte offset {offset}"
            )));
        }
        if frame.body.partition != partition.0 || frame.body.position != expected_position {
            return Err(LogError::Integrity(format!(
                "partition/position sequence mismatch at byte offset {offset}"
            )));
        }
        let payload = decode_hex(&frame.body.payload_hex)?;
        if sha256_hex(&payload) != frame.body.payload_sha256 {
            return Err(LogError::Integrity(format!(
                "payload checksum mismatch at byte offset {offset}"
            )));
        }
        let record = LogRecord {
            record_id: frame.body.record_id,
            partition,
            position: LogPosition(frame.body.position),
            kind: frame.body.kind,
            tenant_id: frame.body.tenant_id,
            event_id: frame.body.event_id,
            payload: Bytes::from(payload),
            checksum_sha256: frame.body.payload_sha256,
        };
        record.validate().map_err(LogError::Integrity)?;
        match record.kind {
            LogRecordKind::FenceAdvanced => {
                let value: serde_json::Value = serde_json::from_slice(&record.payload)
                    .map_err(|error| LogError::Integrity(error.to_string()))?;
                if value["partition"].as_u64() != Some(u64::from(partition.0)) {
                    return Err(LogError::Integrity("fencing partition mismatch".into()));
                }
                let epoch = value["fencing_epoch"]
                    .as_u64()
                    .ok_or_else(|| LogError::Integrity("fencing epoch missing".into()))?;
                if epoch <= highest_fencing_epoch {
                    return Err(LogError::Integrity("fencing epochs must increase monotonically".into()));
                }
                highest_fencing_epoch = epoch;
            }
            LogRecordKind::ExecutionCommitted => {
                let value: serde_json::Value = serde_json::from_slice(&record.payload)
                    .map_err(|error| LogError::Integrity(error.to_string()))?;
                if value["fencing_epoch"].as_u64() != Some(highest_fencing_epoch) {
                    return Err(LogError::Integrity("execution commit has stale fencing epoch".into()));
                }
            }
            _ => {}
        }
        if by_id.contains_key(&record.record_id) {
            return Err(LogError::Integrity(format!(
                "duplicate record ID {} exists in WAL",
                record.record_id
            )));
        }
        by_id.insert(
            record.record_id.clone(),
            (record.clone(), record.position),
        );
        records.push(record);
        expected_position += 1;
        offset += 4 + frame_len as u64;
    }

    file.seek(SeekFrom::End(0)).map_err(storage_error)?;
    Ok((file, records, expected_position - 1, by_id, highest_fencing_epoch))
}

fn partition_path(root: &Path, partition: PartitionId) -> PathBuf {
    root.join(format!("partition-{:010}.wal", partition.0))
}

fn partition_for_key(tenant_id: &str, key: &str) -> PartitionId {
    // Keep this identical to the Go worker's hash(tenant:key) virtual-shard mapping.
    let combined = format!("{tenant_id}:{key}");
    let mut hash = 2_166_136_261_u32;
    for byte in combined.as_bytes() {
        hash ^= u32::from(*byte);
        hash = hash.wrapping_mul(16_777_619);
    }
    PartitionId(hash % 4096)
}

fn sha256_hex(bytes: &[u8]) -> String {
    encode_hex(&Sha256::digest(bytes))
}

fn decode_hex(value: &str) -> Result<Vec<u8>, LogError> {
    if value.len() % 2 != 0 {
        return Err(LogError::Integrity("hex payload has odd length".into()));
    }
    let mut decoded = Vec::with_capacity(value.len() / 2);
    for pair in value.as_bytes().chunks_exact(2) {
        let high = hex_nibble(pair[0])?;
        let low = hex_nibble(pair[1])?;
        decoded.push((high << 4) | low);
    }
    Ok(decoded)
}

fn hex_nibble(value: u8) -> Result<u8, LogError> {
    match value {
        b'0'..=b'9' => Ok(value - b'0'),
        b'a'..=b'f' => Ok(value - b'a' + 10),
        _ => Err(LogError::Integrity("invalid hex payload".into())),
    }
}

fn storage_error(error: impl std::fmt::Display) -> LogError {
    LogError::Unavailable(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT_TEMP: AtomicU64 = AtomicU64::new(1);

    fn temp_dir() -> PathBuf {
        let id = NEXT_TEMP.fetch_add(1, Ordering::Relaxed);
        std::env::temp_dir().join(format!(
            "flow-rulz-wal-{}-{id}",
            std::process::id()
        ))
    }

    fn new_record(record_id: &str, payload: &[u8]) -> NewLogRecord {
        NewLogRecord {
            record_id: record_id.to_string(),
            partition: PartitionId(7),
            kind: LogRecordKind::EventAccepted,
            tenant_id: "tenant-a".to_string(),
            event_id: record_id.to_string(),
            payload: Bytes::copy_from_slice(payload),
            checksum_sha256: sha256_hex(payload),
        }
    }

    #[tokio::test]
    async fn append_is_idempotent_and_survives_restart() {
        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        let record = new_record("event-1", br#"{"value":1}"#);
        let first = log.append(record.clone()).await.unwrap();
        let retry = log.append(record).await.unwrap();
        assert_eq!(first, retry);
        drop(log);

        let reopened = FileEventLog::open(&dir).await.unwrap();
        assert_eq!(
            reopened.current_position(PartitionId(7)).await.unwrap(),
            LogPosition(1)
        );
        let records = reopened
            .read(PartitionId(7), LogPosition(0), 10)
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].payload.as_ref(), br#"{"value":1}"#);
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn same_record_id_with_different_content_conflicts() {
        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        log.append(new_record("event-1", br#"{"value":1}"#))
            .await
            .unwrap();
        let error = log
            .append(new_record("event-1", br#"{"value":2}"#))
            .await
            .unwrap_err();
        assert!(matches!(error, LogError::IdentityConflict(_)));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn recovery_truncates_only_an_incomplete_tail() {
        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        log.append(new_record("event-1", br#"{"value":1}"#))
            .await
            .unwrap();
        drop(log);

        let path = partition_path(&dir, PartitionId(7));
        let mut file = OpenOptions::new().append(true).open(path).unwrap();
        file.write_all(&[0, 0]).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let reopened = FileEventLog::open(&dir).await.unwrap();
        assert_eq!(
            reopened.current_position(PartitionId(7)).await.unwrap(),
            LogPosition(1)
        );
        reopened
            .append(new_record("event-2", br#"{"value":2}"#))
            .await
            .unwrap();
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn complete_frame_corruption_fails_closed() {
        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        log.append(new_record("event-1", br#"{"value":1}"#))
            .await
            .unwrap();
        drop(log);

        let path = partition_path(&dir, PartitionId(7));
        let mut file = OpenOptions::new().read(true).write(true).open(path).unwrap();
        file.seek(SeekFrom::End(-2)).unwrap();
        file.write_all(b"xx").unwrap();
        file.sync_all().unwrap();
        drop(file);

        let error = FileEventLog::open(&dir).await.err().unwrap();
        assert!(matches!(error, LogError::Integrity(_)));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn execution_and_all_effects_are_one_committed_record() {
        use crate::domain::{ContractRef, EventIdentity, OutputRoute};

        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        let tenant_id = "tenant-a";
        let event_id = "event-1";
        let partition_key = "order-1";
        let execution_id =
            deterministic_id("execution", &[tenant_id, partition_key, event_id]);
        let effect_payload = Bytes::from_static(br#"{"type":"orders.approved"}"#);
        let effect = crate::domain::EffectIntent {
            effect_id: "effect-1".to_string(),
            tenant_id: tenant_id.to_string(),
            source_event_id: event_id.to_string(),
            execution_id: execution_id.clone(),
            output_contract: ContractRef {
                name: "orders.approved".to_string(),
                version: "1".to_string(),
                content_hash: sha256_hex(b"contract-v1"),
            },
            route: OutputRoute::Consumer {
                destination: "orders.approved".to_string(),
            },
            kind: EffectKind::PublishEvent,
            payload: effect_payload.clone(),
            payload_sha256: sha256_hex(&effect_payload),
            partition_key: partition_key.to_string(),
        };
        let commit = ExecutionCommit {
            execution_id,
            event: EventIdentity {
                tenant_id: tenant_id.to_string(),
                event_id: event_id.to_string(),
                partition_key: partition_key.to_string(),
            },
            rule_revisions: Vec::new(),
            outcome: ExecutionOutcome::Completed,
            decision_hash: sha256_hex(b"decision"),
            matched_rule_ids: vec!["approve-order".to_string()],
            result_payload: Bytes::from_static(br#"{"approved":true}"#),
            effects: vec![effect],
            fencing_epoch: 1,
        };

        let partition = partition_for_key(tenant_id, partition_key);
        assert_eq!(log.advance_fence(partition, 1).await.unwrap(), LogPosition(1));
        let position = log.commit_execution(commit).await.unwrap();
        assert_eq!(position, LogPosition(2));
        let records = log.read(partition, LogPosition(0), 10).await.unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].kind, LogRecordKind::FenceAdvanced);
        assert_eq!(records[1].kind, LogRecordKind::ExecutionCommitted);
        let payload: serde_json::Value = serde_json::from_slice(&records[1].payload).unwrap();
        assert_eq!(payload["effects"].as_array().unwrap().len(), 1);
        assert_eq!(payload["effects"][0]["effect_id"], "effect-1");
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn partition_mapping_matches_fnv1a_virtual_shards() {
        let key = "order-1";
        let combined = format!("tenant-a:{key}");
        let mut expected = 2_166_136_261_u32;
        for byte in combined.as_bytes() {
            expected ^= u32::from(*byte);
            expected = expected.wrapping_mul(16_777_619);
        }
        assert_eq!(partition_for_key("tenant-a", key), PartitionId(expected % 4096));
    }


    #[tokio::test]
    async fn record_id_cannot_be_reused_in_another_partition() {
        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        log.append(new_record("event-1", br#"{"value":1}"#))
            .await
            .unwrap();
        let mut duplicate = new_record("event-1", br#"{"value":1}"#);
        duplicate.partition = PartitionId(8);
        let error = log.append(duplicate).await.unwrap_err();
        assert!(matches!(error, LogError::IdentityConflict(_)));
        let _ = fs::remove_dir_all(dir);
    }

}
