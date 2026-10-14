//! Single-node append-only WAL adapter.
//!
//! This adapter provides durable local append, replay and idempotency. It is
//! deliberately not described as a replicated/distributed broker: it has no
//! quorum protocol or cross-node fencing authority.
use crate::domain::{
    deterministic_id, EffectKind, ExecutionCommit, ExecutionOutcome, LogPosition, LogRecord,
    LogRecordKind, PartitionId,
};
use crate::domain::identity::encode_hex;
use crate::ports::event_log::{EventLog, LogError, NewLogRecord};
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;
const DEFAULT_SEGMENT_BYTES: u64 = 256 * 1024 * 1024;
type RecoveredPartition = (File, Vec<LogRecord>, u64, u64);

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
    segment_number: u64,
    next_position: u64,
    records: Vec<LogRecord>,
    highest_fencing_epoch: u64,
}

pub struct FileEventLog {
    root: PathBuf,
    segment_max_bytes: u64,
    // Holding this file keeps the OS advisory lock for the adapter lifetime.
    _lock_file: File,
    partitions: Mutex<HashMap<PartitionId, PartitionState>>,
    record_index: Mutex<HashMap<String, (PartitionId, LogPosition)>>,
}

impl FileEventLog {
    /// Open a WAL directory, replaying all complete frames and truncating only
    /// an incomplete final frame. A complete frame with a bad checksum fails
    /// closed instead of silently discarding potentially committed data.
    pub async fn open(root: impl AsRef<Path>) -> Result<Self, LogError> {
        Self::open_with_segment_bytes(root, DEFAULT_SEGMENT_BYTES).await
    }

    /// Open a segmented WAL. Segment size is configurable for deterministic
    /// rotation tests; production defaults to 256 MiB per partition segment.
    pub async fn open_with_segment_bytes(
        root: impl AsRef<Path>,
        segment_max_bytes: u64,
    ) -> Result<Self, LogError> {
        if segment_max_bytes == 0 {
            return Err(LogError::Invalid("WAL segment size must be positive".into()));
        }
        let root = root.as_ref().to_path_buf();
        fs::create_dir_all(&root).map_err(storage_error)?;
        let lock_file = acquire_process_lock(&root)?;
        let mut partitions = HashMap::new();
        let mut record_index = HashMap::new();
        let mut files: BTreeMap<PartitionId, Vec<(u64, PathBuf)>> = BTreeMap::new();

        for entry in fs::read_dir(&root).map_err(storage_error)? {
            let entry = entry.map_err(storage_error)?;
            let name = entry.file_name().to_string_lossy().to_string();
            if let Some((partition, segment)) = parse_partition_segment(&name)? {
                files.entry(PartitionId(partition)).or_default().push((segment, entry.path()));
            }
        }

        for (partition, mut segments) in files {
            segments.sort_by_key(|(segment, _)| *segment);
            if segments.first().map(|(segment, _)| *segment) != Some(0) {
                return Err(LogError::Integrity(format!(
                    "partition {} has a segment gap before its first segment",
                    partition.0
                )));
            }

            let mut records = Vec::new();
            let mut next_position = 0_u64;
            let mut highest_fencing_epoch = 0_u64;
            let mut current_file = None;
            let mut expected_segment = 0_u64;

            for (segment_number, path) in segments.iter() {
                if *segment_number != expected_segment {
                    return Err(LogError::Integrity(format!(
                        "partition {} has a WAL segment gap at segment {}",
                        partition.0, expected_segment
                    )));
                }
                let (file, segment_records, segment_position, segment_epoch) =
                    recover_partition(
                        path.clone(),
                        partition,
                        next_position + 1,
                        highest_fencing_epoch,
                    )?;

                for record in &segment_records {
                    if record_index
                        .insert(record.record_id.clone(), (partition, record.position))
                        .is_some()
                    {
                        return Err(LogError::Integrity(format!(
                            "record ID {} appears in more than one partition or segment",
                            record.record_id
                        )));
                    }
                }
                records.extend(segment_records);
                next_position = segment_position;
                highest_fencing_epoch = segment_epoch;
                current_file = Some(file);
                expected_segment += 1;
            }

            let file = current_file.ok_or_else(|| {
                LogError::Integrity(format!("partition {} has no WAL segments", partition.0))
            })?;
            partitions.insert(
                partition,
                PartitionState {
                    file: tokio::fs::File::from_std(file),
                    segment_number: expected_segment - 1,
                    next_position,
                    records,
                    highest_fencing_epoch,
                },
            );
        }

        Ok(Self {
            root,
            segment_max_bytes,
            _lock_file: lock_file,
            partitions: Mutex::new(partitions),
            record_index: Mutex::new(record_index),
        })
    }

    async fn ensure_partition<'a>(
        &'a self,
        partitions: &'a mut HashMap<PartitionId, PartitionState>,
        partition: PartitionId,
    ) -> Result<&'a mut PartitionState, LogError> {
        if let Some(state) = partitions.get_mut(&partition) {
            return Ok(state);
        }

        let path = partition_path(&self.root, partition);
        let existed = tokio::fs::try_exists(&path).await.map_err(storage_error)?;
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)
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
                segment_number: 0,
                next_position: 0,
                records: Vec::new(),
                highest_fencing_epoch: 0,
            },
        );
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
        if let Some((existing_partition, position)) = record_index.get(&record.record_id) {
            if *existing_partition != record.partition {
                return Err(LogError::IdentityConflict(record.record_id));
            }
            let partitions = self.partitions.lock().await;
            let state = partitions.get(existing_partition).ok_or_else(|| {
                LogError::Integrity("record index references a missing partition".into())
            })?;
            let record_index = usize::try_from(position.0.checked_sub(1).ok_or_else(|| {
                LogError::Integrity("record index contains a zero position".into())
            })?)
            .map_err(|error| LogError::Integrity(error.to_string()))?;
            let existing = state.records.get(record_index).ok_or_else(|| {
                LogError::Integrity("record index references a missing log record".into())
            })?;
            let identical = existing.kind == record.kind
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
            LogRecordKind::EventAccepted => {
                let value: serde_json::Value = serde_json::from_slice(&record.payload)
                    .map_err(|error| LogError::Integrity(error.to_string()))?;
                let tenant_id = value["identity"]["tenant_id"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event tenant ID missing".into()))?;
                let event_id = value["identity"]["event_id"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event ID missing".into()))?;
                let partition_key = value["identity"]["partition_key"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event partition key missing".into()))?;
                let payload_hex = value["payload_hex"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event payload bytes missing".into()))?;
                let payload_hash = value["payload_sha256"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event payload hash missing".into()))?;
                let original_payload = decode_hex(payload_hex)?;
                let expected_record_id = deterministic_id(
                    "event.accepted",
                    &[tenant_id, partition_key, event_id, ""],
                );
                if tenant_id != record.tenant_id
                    || event_id != record.event_id
                    || record.record_id != expected_record_id
                    || partition_for_key(tenant_id, partition_key) != record.partition
                    || sha256_hex(&original_payload) != payload_hash
                {
                    return Err(LogError::Integrity("accepted event identity or payload hash is invalid".into()));
                }
            }
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
                    "execution",
                    &[&record.tenant_id, partition_key, &record.event_id],
                );
                let expected_record_id = deterministic_id(
                    "execution.committed",
                    &[&record.tenant_id, partition_key, &record.event_id, execution_id],
                );
                if execution_id != expected_execution_id
                    || record.record_id != expected_record_id
                    || partition_for_key(&record.tenant_id, partition_key) != record.partition
                {
                    return Err(LogError::Invalid("execution commit has non-canonical identity or partition".into()));
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

        let current_len = state.file.metadata().await.map_err(storage_error)?.len();
        if current_len > 0
            && current_len.saturating_add(encoded.len() as u64) > self.segment_max_bytes
        {
            let next_segment = state.segment_number + 1;
            let path = segment_path(&self.root, record.partition, next_segment);
            let next_file = OpenOptions::new()
                .create_new(true)
                .read(true)
                .append(true)
                .open(path)
                .map_err(storage_error)?;
            File::open(&self.root)
                .and_then(|directory| directory.sync_all())
                .map_err(storage_error)?;
            state.file = tokio::fs::File::from_std(next_file);
            state.segment_number = next_segment;
        }

        let old_len = state.file.metadata().await.map_err(storage_error)?.len();
        if let Err(error) = state.file.write_all(&encoded).await {
            let _ = state.file.set_len(old_len).await;
            let _ = state.file.sync_data().await;
            return Err(storage_io_error(error));
        }
        if let Err(error) = state.file.sync_data().await {
            let _ = state.file.set_len(old_len).await;
            let _ = state.file.sync_data().await;
            return Err(storage_io_error(error));
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
        record_index.insert(stored.record_id.clone(), (stored.partition, position));
        Ok(position)
    }

    async fn commit_execution(&self, commit: ExecutionCommit) -> Result<LogPosition, LogError> {
        commit.validate().map_err(LogError::Invalid)?;
        let expected_execution_id = deterministic_id(
            "execution",
            &[
                &commit.event.tenant_id,
                &commit.event.partition_key,
                &commit.event.event_id,
            ],
        );
        if commit.execution_id != expected_execution_id {
            return Err(LogError::Invalid(
                "execution_id must be deterministic for tenant, partition key and event".into(),
            ));
        }

        let outcome = match &commit.outcome {
            ExecutionOutcome::Completed => "completed",
            ExecutionOutcome::NoMatch => "no_match",
            ExecutionOutcome::Quarantined { .. } => "quarantined",
        };
        let result_payload: serde_json::Value =
            serde_json::from_slice(&commit.result_payload)
                .map_err(|error| LogError::Invalid(format!("invalid result payload: {error}")))?;
        let mut effects = Vec::with_capacity(commit.effects.len());
        for effect in &commit.effects {
            let kind = match effect.kind {
                EffectKind::PublishEvent => "publish_event",
                EffectKind::InvokeCommand => "invoke_command",
                EffectKind::Reply => "reply",
                EffectKind::Schedule => "schedule",
            };
            let effect_payload: serde_json::Value =
                serde_json::from_slice(&effect.payload).map_err(|error| {
                    LogError::Invalid(format!("invalid effect payload: {error}"))
                })?;
            effects.push(json!({
                "effect_id": effect.effect_id,
                "effect_key": effect.effect_key,
                "tenant_id": effect.tenant_id,
                "source_event_id": effect.source_event_id,
                "execution_id": effect.execution_id,
                "output_contract": effect.output_contract,
                "route": effect.route,
                "kind": kind,
                "payload": effect_payload,
                "payload_sha256": effect.payload_sha256,
                "partition_key": effect.partition_key,
            }));
        }
        let mut payload = json!({
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
            "result_payload": result_payload,
            "effects": effects,
            "fencing_epoch": commit.fencing_epoch,
        });
        if let ExecutionOutcome::Quarantined { category, message } = &commit.outcome {
            payload["outcome_details"] = json!({"category": category, "message": message});
        }
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
        let current = self
            .partitions
            .lock()
            .await
            .get(&partition)
            .map(|state| state.highest_fencing_epoch)
            .unwrap_or(0);
        if fencing_epoch < current {
            return Err(LogError::Fenced {
                attempted: fencing_epoch,
                current,
            });
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
    first_position: u64,
    initial_fencing_epoch: u64,
) -> Result<RecoveredPartition, LogError> {
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(storage_error)?;
    let file_len = file.metadata().map_err(storage_error)?.len();
    let mut offset = 0_u64;
    let mut records = Vec::new();
    let mut seen_ids = HashSet::new();
    let mut expected_position = first_position;
    let mut highest_fencing_epoch = initial_fencing_epoch;

    while offset < file_len {
        let remaining = file_len - offset;
        if remaining < 4 {
            truncate_and_sync(&mut file, offset)?;
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
            truncate_and_sync(&mut file, offset)?;
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
            LogRecordKind::EventAccepted => {
                let value: serde_json::Value = serde_json::from_slice(&record.payload)
                    .map_err(|error| LogError::Integrity(error.to_string()))?;
                let tenant_id = value["identity"]["tenant_id"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event tenant ID missing".into()))?;
                let event_id = value["identity"]["event_id"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event ID missing".into()))?;
                let partition_key = value["identity"]["partition_key"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event partition key missing".into()))?;
                let payload_hex = value["payload_hex"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event payload bytes missing".into()))?;
                let payload_hash = value["payload_sha256"].as_str()
                    .ok_or_else(|| LogError::Integrity("accepted event payload hash missing".into()))?;
                let original_payload = decode_hex(payload_hex)?;
                let expected_record_id = deterministic_id(
                    "event.accepted",
                    &[tenant_id, partition_key, event_id, ""],
                );
                if tenant_id != record.tenant_id
                    || event_id != record.event_id
                    || record.record_id != expected_record_id
                    || partition_for_key(tenant_id, partition_key) != partition
                    || sha256_hex(&original_payload) != payload_hash
                {
                    return Err(LogError::Integrity("accepted event identity or payload hash is invalid".into()));
                }
            }
            LogRecordKind::FenceAdvanced => {
                let value: serde_json::Value = serde_json::from_slice(&record.payload)
                    .map_err(|error| LogError::Integrity(error.to_string()))?;
                if value["partition"].as_u64() != Some(u64::from(partition.0)) {
                    return Err(LogError::Integrity("fencing partition mismatch".into()));
                }
                let epoch = value["fencing_epoch"]
                    .as_u64()
                    .ok_or_else(|| LogError::Integrity("fencing epoch missing".into()))?;
                let expected_event_id = format!("fence:{}:{epoch}", partition.0);
                let partition_text = partition.0.to_string();
                let expected_record_id = deterministic_id(
                    "fence.advanced",
                    &["_system", &partition_text, &expected_event_id, ""],
                );
                if record.tenant_id != "_system"
                    || record.event_id != expected_event_id
                    || record.record_id != expected_record_id
                {
                    return Err(LogError::Integrity("fencing record identity is not canonical".into()));
                }
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
                let tenant_id = value["event"]["tenant_id"].as_str()
                    .ok_or_else(|| LogError::Integrity("execution tenant ID missing".into()))?;
                let event_id = value["event"]["event_id"].as_str()
                    .ok_or_else(|| LogError::Integrity("execution event ID missing".into()))?;
                let partition_key = value["event"]["partition_key"].as_str()
                    .ok_or_else(|| LogError::Integrity("execution partition key missing".into()))?;
                let execution_id = value["execution_id"].as_str()
                    .ok_or_else(|| LogError::Integrity("execution ID missing".into()))?;
                let expected_execution_id = deterministic_id("execution", &[tenant_id, partition_key, event_id]);
                let expected_record_id = deterministic_id(
                    "execution.committed",
                    &[tenant_id, partition_key, event_id, execution_id],
                );
                if tenant_id != record.tenant_id
                    || event_id != record.event_id
                    || execution_id != expected_execution_id
                    || record.record_id != expected_record_id
                    || partition_for_key(tenant_id, partition_key) != partition
                {
                    return Err(LogError::Integrity("execution record identity is not canonical".into()));
                }
            }
            _ => {}
        }
        if !seen_ids.insert(record.record_id.clone()) {
            return Err(LogError::Integrity(format!(
                "duplicate record ID {} exists in WAL segment",
                record.record_id
            )));
        }
        records.push(record);
        expected_position += 1;
        offset += 4 + frame_len as u64;
    }

    file.seek(SeekFrom::End(0)).map_err(storage_error)?;
    Ok((file, records, expected_position - 1, highest_fencing_epoch))
}


#[cfg(unix)]
unsafe extern "C" {
    fn flock(fd: std::os::raw::c_int, operation: std::os::raw::c_int) -> std::os::raw::c_int;
}

fn truncate_and_sync(file: &mut File, offset: u64) -> Result<(), LogError> {
    file.set_len(offset).map_err(storage_io_error)?;
    // Persist recovery before accepting new appends, so a later crash cannot
    // resurrect bytes from the discarded incomplete tail.
    file.sync_all().map_err(storage_io_error)
}

fn acquire_process_lock(root: &Path) -> Result<File, LogError> {
    let path = root.join(".flow-rulz-wal.lock");
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)
        .map_err(storage_error)?;

    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;
        // LOCK_EX | LOCK_NB: fail instead of allowing two writers to interleave frames.
        let result = unsafe { flock(file.as_raw_fd(), 2 | 4) };
        if result != 0 {
            return Err(LogError::Unavailable(format!(
                "WAL directory is already locked: {}",
                std::io::Error::last_os_error()
            )));
        }
    }

    #[cfg(not(unix))]
    {
        return Err(LogError::Unavailable(
            "FileEventLog currently requires Unix advisory file locks".into(),
        ));
    }

    Ok(file)
}

fn partition_path(root: &Path, partition: PartitionId) -> PathBuf {
    root.join(format!("partition-{:010}.wal", partition.0))
}

fn segment_path(root: &Path, partition: PartitionId, segment: u64) -> PathBuf {
    root.join(format!("partition-{:010}-segment-{:020}.wal", partition.0, segment))
}

fn parse_partition_segment(name: &str) -> Result<Option<(u32, u64)>, LogError> {
    let Some(name) = name.strip_prefix("partition-").and_then(|value| value.strip_suffix(".wal")) else {
        return Ok(None);
    };
    if let Some((partition, segment)) = name.split_once("-segment-") {
        let partition = partition.parse::<u32>().map_err(|_| {
            LogError::Integrity(format!("invalid WAL partition filename {name}"))
        })?;
        let segment = segment.parse::<u64>().map_err(|_| {
            LogError::Integrity(format!("invalid WAL segment filename {name}"))
        })?;
        Ok(Some((partition, segment)))
    } else {
        let partition = name.parse::<u32>().map_err(|_| {
            LogError::Integrity(format!("invalid WAL partition filename {name}"))
        })?;
        Ok(Some((partition, 0)))
    }
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
    if !value.len().is_multiple_of(2) {
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

fn storage_io_error(error: std::io::Error) -> LogError {
    if error.kind() == std::io::ErrorKind::StorageFull {
        LogError::StorageFull(error.to_string())
    } else {
        LogError::Unavailable(error.to_string())
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
            kind: LogRecordKind::ConsumerProgressed,
            tenant_id: "tenant-a".to_string(),
            event_id: record_id.to_string(),
            payload: Bytes::copy_from_slice(payload),
            checksum_sha256: sha256_hex(payload),
        }
    }

    #[tokio::test]
    async fn recovery_fails_closed_when_a_segment_is_missing() {
        let dir = temp_dir();
        let log = FileEventLog::open_with_segment_bytes(&dir, 1).await.unwrap();
        for index in 1_u32..=3 {
            log.append(new_record(
                &format!("progress-{index}"),
                format!(r#"{{"cursor":{index}}}"#).as_bytes(),
            ))
            .await
            .unwrap();
        }
        drop(log);

        fs::remove_file(segment_path(&dir, PartitionId(7), 1)).unwrap();
        let error = FileEventLog::open(&dir).await.err().unwrap();
        assert!(matches!(error, LogError::Integrity(_)));
        let _ = fs::remove_dir_all(dir);
    }

    #[test]
    fn storage_exhaustion_is_reported_as_a_durability_failure() {
        let error = storage_io_error(std::io::Error::from(std::io::ErrorKind::StorageFull));
        assert!(matches!(error, LogError::StorageFull(_)));
    }

    #[tokio::test]
    async fn segment_rotation_preserves_global_positions_after_restart() {
        let dir = temp_dir();
        let log = FileEventLog::open_with_segment_bytes(&dir, 1).await.unwrap();
        for index in 1_u32..=3 {
            let record = new_record(
                &format!("progress-{index}"),
                format!(r#"{{"cursor":{index}}}"#).as_bytes(),
            );
            assert_eq!(
                log.append(record).await.unwrap(),
                LogPosition(u64::from(index))
            );
        }
        drop(log);

        let segment_count = fs::read_dir(&dir)
            .unwrap()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().ends_with(".wal"))
            .count();
        assert_eq!(segment_count, 3);

        let reopened = FileEventLog::open(&dir).await.unwrap();
        assert_eq!(
            reopened.current_position(PartitionId(7)).await.unwrap(),
            LogPosition(3)
        );
        let records = reopened.read(PartitionId(7), LogPosition(0), 10).await.unwrap();
        assert_eq!(records.len(), 3);
        assert_eq!(
            records.iter().map(|record| record.position.0).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn accepted_event_validates_partition_identity_and_recovers() {
        let dir = temp_dir();
        let tenant_id = "tenant-a";
        let event_id = "event-accepted-1";
        let partition_key = "order-1";
        let partition = partition_for_key(tenant_id, partition_key);
        let original_payload = br#"{"value":1}"#;
        let accepted = json!({
            "identity": {
                "tenant_id": tenant_id,
                "event_id": event_id,
                "partition_key": partition_key
            },
            "event_type": "orders.created",
            "occurred_at": "2026-10-14T00:00:00Z",
            "input_contract": {
                "name": "orders.created",
                "version": "1",
                "content_hash": sha256_hex(b"contract-v1")
            },
            "correlation_id": null,
            "causation_id": null,
            "mode": "async",
            "headers": {},
            "payload_hex": encode_hex(original_payload),
            "payload_sha256": sha256_hex(original_payload)
        });
        let payload = Bytes::from(serde_json::to_vec(&accepted).unwrap());
        let record = NewLogRecord {
            record_id: deterministic_id("event.accepted", &[tenant_id, partition_key, event_id, ""]),
            partition,
            kind: LogRecordKind::EventAccepted,
            tenant_id: tenant_id.to_string(),
            event_id: event_id.to_string(),
            checksum_sha256: sha256_hex(&payload),
            payload,
        };

        let log = FileEventLog::open(&dir).await.unwrap();
        assert_eq!(log.append(record.clone()).await.unwrap(), LogPosition(1));
        let mut wrong_partition = record.clone();
        wrong_partition.record_id = "different-record-id".to_string();
        wrong_partition.partition = PartitionId((partition.0 + 1) % 4096);
        let error = log.append(wrong_partition).await.err().unwrap();
        assert!(matches!(error, LogError::Integrity(_)));
        drop(log);

        let reopened = FileEventLog::open(&dir).await.unwrap();
        let records = reopened.read(partition, LogPosition(0), 10).await.unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].kind, LogRecordKind::EventAccepted);
        let _ = fs::remove_dir_all(dir);
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
        let execution_id = deterministic_id(
            "execution",
            &[tenant_id, partition_key, event_id],
        );
        let effect_key = "orders-approved";
        let effect_id = deterministic_id(
            "effect",
            &[tenant_id, partition_key, event_id, &execution_id, effect_key],
        );
        let effect_payload = Bytes::from_static(br#"{"type":"orders.approved"}"#);
        let effect = crate::domain::EffectIntent {
            effect_id: effect_id.clone(),
            effect_key: effect_key.to_string(),
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
        assert_eq!(payload["effects"][0]["effect_id"], effect_id);
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


    #[tokio::test]
    async fn fencing_epochs_are_monotonic_and_stale_retries_are_rejected() {
        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        assert_eq!(
            log.advance_fence(PartitionId(7), 1).await.unwrap(),
            LogPosition(1)
        );
        assert_eq!(
            log.advance_fence(PartitionId(7), 2).await.unwrap(),
            LogPosition(2)
        );
        let error = log.advance_fence(PartitionId(7), 1).await.unwrap_err();
        assert!(matches!(error, LogError::Fenced { attempted: 1, current: 2 }));
        let _ = fs::remove_dir_all(dir);
    }


    #[tokio::test]
    async fn recovery_rejects_semantically_invalid_fencing_identity_even_with_valid_checksum() {
        let dir = temp_dir();
        let log = FileEventLog::open(&dir).await.unwrap();
        log.advance_fence(PartitionId(7), 1).await.unwrap();
        drop(log);

        let path = partition_path(&dir, PartitionId(7));
        let mut file = OpenOptions::new().read(true).write(true).open(&path).unwrap();
        let mut length = [0_u8; 4];
        file.read_exact(&mut length).unwrap();
        let frame_len = u32::from_be_bytes(length) as usize;
        let mut bytes = vec![0_u8; frame_len];
        file.read_exact(&mut bytes).unwrap();
        let mut frame: Frame = serde_json::from_slice(&bytes).unwrap();

        // Keep the frame structurally valid and recompute its checksum. Recovery
        // must validate semantic identities, not merely accept self-consistent bytes.
        frame.body.event_id = "fence:7:9".to_string();
        frame.frame_sha256 = sha256_hex(&serde_json::to_vec(&frame.body).unwrap());
        let encoded = serde_json::to_vec(&frame).unwrap();
        assert_eq!(encoded.len(), frame_len);
        file.seek(SeekFrom::Start(0)).unwrap();
        file.write_all(&u32::try_from(encoded.len()).unwrap().to_be_bytes()).unwrap();
        file.write_all(&encoded).unwrap();
        file.sync_all().unwrap();
        drop(file);

        let error = FileEventLog::open(&dir).await.err().unwrap();
        assert!(matches!(error, LogError::Integrity(_)));
        let _ = fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn wal_directory_rejects_a_second_writer() {
        let dir = temp_dir();
        let first = FileEventLog::open(&dir).await.unwrap();
        let second = FileEventLog::open(&dir).await;
        assert!(matches!(second, Err(LogError::Unavailable(_))));
        drop(first);
        assert!(FileEventLog::open(&dir).await.is_ok());
        let _ = fs::remove_dir_all(dir);
    }

}
