//! Bounded retained topics, group offsets and fenced delivery receipts.
use crate::{engine, ports::broker::Store};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Record {
    pub topic: String,
    pub partition: u32,
    pub offset: u64,
    pub tenant_id: String,
    pub key: String,
    pub payload: Value,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(crate) struct Claim {
    pub record: Record,
    pub group: String,
    pub receipt: String,
    pub expires_at: DateTime<Utc>,
}

#[derive(Default, Deserialize, Serialize)]
struct Partition {
    base: u64,
    records: Vec<Record>,
}

struct Lease {
    offset: u64,
    receipt: String,
    expires_at: DateTime<Utc>,
}

#[derive(Deserialize, Serialize)]
struct Group {
    tenant_id: String,
    topic: String,
    name: String,
    offsets: BTreeMap<u32, u64>,
    #[serde(skip)]
    leases: BTreeMap<u32, Lease>,
    #[serde(skip)]
    cursor: u32,
}

#[derive(Deserialize, Serialize)]
struct Snapshot {
    version: u32,
    partitions: u32,
    topics: BTreeMap<String, BTreeMap<u32, Partition>>,
    groups: BTreeMap<String, Group>,
}

struct State {
    snapshot: Snapshot,
    count: usize,
    bytes: usize,
    failed: bool,
}

pub(crate) struct Broker {
    state: Mutex<State>,
    changed: Notify,
    config: Limits,
    store: Option<Arc<dyn Store>>,
}

#[derive(Clone)]
pub(crate) struct Limits {
    pub partitions: u32,
    pub max_messages: usize,
    pub max_bytes: usize,
    pub ack_wait: Duration,
}

pub(crate) fn valid_name(name: &str) -> bool {
    !name.is_empty() && name.len() <= 128
}
fn group_key(tenant: &str, topic: &str, group: &str) -> String {
    serde_json::to_string(&[tenant, topic, group]).expect("string array serialization")
}

impl Broker {
    pub fn partition_count(&self) -> u32 {
        self.config.partitions
    }
    pub fn new(config: Limits, store: Option<Arc<dyn Store>>) -> Result<Self> {
        if !(1..=4096).contains(&config.partitions)
            || !(2..=10000).contains(&config.max_messages)
            || !(1..=64 * 1024 * 1024).contains(&config.max_bytes)
            || config.ack_wait.is_zero()
            || config.ack_wait > Duration::from_secs(300)
        {
            bail!("invalid broker limits");
        }
        let snapshot = match store
            .as_ref()
            .map(|store| store.load())
            .transpose()?
            .flatten()
        {
            Some(data) => {
                serde_json::from_slice::<Snapshot>(&data).context("decode broker snapshot")?
            }
            None => Snapshot {
                version: 1,
                partitions: config.partitions,
                topics: BTreeMap::new(),
                groups: BTreeMap::new(),
            },
        };
        let mut state = State {
            snapshot,
            count: 0,
            bytes: 0,
            failed: false,
        };
        validate_snapshot(&mut state, &config)?;
        Ok(Self {
            state: Mutex::new(state),
            changed: Notify::new(),
            config,
            store,
        })
    }

    fn persist(&self, state: &mut State) -> Result<()> {
        if let Some(store) = &self.store {
            // No await separates the mutation from its atomic snapshot. Cancelling a
            // request cannot let a later snapshot overtake this durable write.
            let result = serde_json::to_vec(&state.snapshot)
                .map_err(anyhow::Error::from)
                .and_then(|data| store.save(&data));
            if let Err(error) = result {
                state.failed = true;
                self.changed.notify_waiters();
                return Err(error.context("persist broker state; restart required"));
            }
        }
        Ok(())
    }

    fn register_locked(
        &self,
        state: &mut State,
        tenant: &str,
        topic: &str,
        name: &str,
    ) -> Result<()> {
        if state.failed {
            bail!("broker storage unavailable");
        }
        let key = group_key(tenant, topic, name);
        if !state.snapshot.groups.contains_key(&key) {
            if state.snapshot.groups.len() >= 1024 {
                bail!("broker group capacity reached");
            }
            state.snapshot.groups.insert(
                key,
                Group {
                    tenant_id: tenant.into(),
                    topic: topic.into(),
                    name: name.into(),
                    offsets: BTreeMap::new(),
                    leases: BTreeMap::new(),
                    cursor: 0,
                },
            );
            self.persist(state)?;
        }
        Ok(())
    }

    pub async fn register_rules(&self) -> Result<()> {
        self.register_locked(&mut *self.state.lock().await, "", "events", "__rules")
    }

    pub async fn append(
        &self,
        topic: &str,
        tenant: &str,
        key: &str,
        payload: Value,
        input: bool,
    ) -> Result<Record> {
        let size = serde_json::to_vec(&payload)?.len();
        if !valid_name(topic)
            || !valid_name(tenant)
            || key.is_empty()
            || key.len() > 1024
            || size > 1 << 20
        {
            bail!("invalid broker record");
        }
        let partition =
            engine::fnv1a32(format!("{tenant}:{key}").as_bytes()) % self.config.partitions;
        let mut state = self.state.lock().await;
        if state.failed {
            bail!("broker storage unavailable");
        }
        let message_limit = self.config.max_messages
            - if input {
                (self.config.max_messages / 4).max(1)
            } else {
                0
            };
        let byte_limit = self.config.max_bytes - if input { self.config.max_bytes / 4 } else { 0 };
        if state.count >= message_limit || state.bytes + size > byte_limit {
            compact(&mut state)?;
        }
        if state.count >= message_limit || state.bytes + size > byte_limit {
            bail!("broker retention capacity reached");
        }
        if !state.snapshot.topics.contains_key(topic) && state.snapshot.topics.len() >= 128 {
            bail!("broker topic capacity reached");
        }
        let log = state
            .snapshot
            .topics
            .entry(topic.into())
            .or_default()
            .entry(partition)
            .or_default();
        let record = Record {
            topic: topic.into(),
            partition,
            offset: log.base + log.records.len() as u64,
            tenant_id: tenant.into(),
            key: key.into(),
            payload,
        };
        log.records.push(record.clone());
        state.count += 1;
        state.bytes += size;
        self.persist(&mut state)?;
        self.changed.notify_waiters();
        Ok(record)
    }

    pub async fn next(
        &self,
        tenant: &str,
        topic: &str,
        name: &str,
        wait: Duration,
    ) -> Result<Option<Claim>> {
        if !valid_name(topic)
            || !valid_name(name)
            || (!tenant.is_empty() && !valid_name(tenant))
            || wait > Duration::from_secs(20)
        {
            bail!("invalid consumer request");
        }
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (claim, wake) = {
                let mut state = self.state.lock().await;
                self.register_locked(&mut state, tenant, topic, name)?;
                claim_locked(&mut state, &self.config, tenant, topic, name, deadline)
            };
            if claim.is_some() || wait.is_zero() || tokio::time::Instant::now() >= deadline {
                return Ok(claim);
            }
            tokio::select! {_ = notified => (), _ = tokio::time::sleep_until(wake) => ()}
        }
    }

    pub async fn settle(
        &self,
        tenant: &str,
        topic: &str,
        name: &str,
        partition: u32,
        receipt: &str,
        retry: Option<Duration>,
    ) -> Result<bool> {
        if !valid_name(topic)
            || !valid_name(name)
            || partition >= self.config.partitions
            || receipt.is_empty()
            || retry.is_some_and(|delay| delay > Duration::from_secs(300))
        {
            bail!("invalid settlement");
        }
        let mut state = self.state.lock().await;
        if state.failed {
            bail!("broker storage unavailable");
        }
        let Some(group) = state
            .snapshot
            .groups
            .get_mut(&group_key(tenant, topic, name))
        else {
            return Ok(false);
        };
        let Some(lease) = group.leases.get_mut(&partition) else {
            return Ok(false);
        };
        if lease.receipt != receipt || Utc::now() >= lease.expires_at {
            return Ok(false);
        }
        if let Some(delay) = retry {
            lease.receipt.clear();
            lease.expires_at = Utc::now() + chrono::Duration::from_std(delay)?;
        } else {
            group.offsets.insert(partition, lease.offset + 1);
            group.leases.remove(&partition);
            self.persist(&mut state)?;
        }
        self.changed.notify_waiters();
        Ok(true)
    }
}

fn claim_locked(
    state: &mut State,
    config: &Limits,
    tenant: &str,
    topic: &str,
    name: &str,
    deadline: tokio::time::Instant,
) -> (Option<Claim>, tokio::time::Instant) {
    let group = state
        .snapshot
        .groups
        .get_mut(&group_key(tenant, topic, name))
        .expect("registered consumer");
    let mut wake = deadline;
    let now = Utc::now();
    for index in 0..config.partitions {
        let partition = (group.cursor + index) % config.partitions;
        if let Some(lease) = group.leases.get(&partition)
            && now < lease.expires_at
        {
            if let Ok(delay) = (lease.expires_at - now).to_std() {
                wake = wake.min(tokio::time::Instant::now() + delay);
            }
            continue;
        }
        let Some(log) = state
            .snapshot
            .topics
            .get(topic)
            .and_then(|parts| parts.get(&partition))
        else {
            continue;
        };
        let first = group
            .offsets
            .get(&partition)
            .copied()
            .unwrap_or_default()
            .max(log.base);
        for record in log.records.iter().skip((first - log.base) as usize) {
            if !tenant.is_empty() && record.tenant_id != tenant {
                continue;
            }
            let claim = Claim {
                record: record.clone(),
                group: name.into(),
                receipt: Uuid::new_v4().to_string(),
                expires_at: now
                    + chrono::Duration::from_std(config.ack_wait).expect("validated ACK wait"),
            };
            group.leases.insert(
                partition,
                Lease {
                    offset: record.offset,
                    receipt: claim.receipt.clone(),
                    expires_at: claim.expires_at,
                },
            );
            group.cursor = (partition + 1) % config.partitions;
            return (Some(claim), wake);
        }
    }
    (None, wake)
}

fn compact(state: &mut State) -> Result<()> {
    let groups = &state.snapshot.groups;
    for partitions in state.snapshot.topics.values_mut() {
        for log in partitions.values_mut() {
            let mut removed = 0;
            for record in &log.records {
                let matching: Vec<_> = groups
                    .values()
                    .filter(|g| {
                        g.topic == record.topic
                            && (g.tenant_id.is_empty() || g.tenant_id == record.tenant_id)
                    })
                    .collect();
                if matching.is_empty()
                    || matching.iter().any(|g| {
                        g.offsets
                            .get(&record.partition)
                            .copied()
                            .unwrap_or_default()
                            <= record.offset
                    })
                {
                    break;
                }
                removed += 1;
                state.count -= 1;
                state.bytes -= serde_json::to_vec(&record.payload)?.len();
            }
            log.records.drain(..removed);
            log.base += removed as u64;
        }
    }
    Ok(())
}

fn validate_snapshot(state: &mut State, config: &Limits) -> Result<()> {
    let snapshot = &state.snapshot;
    if snapshot.version != 1
        || snapshot.partitions != config.partitions
        || snapshot.topics.len() > 128
        || snapshot.groups.len() > 1024
    {
        bail!("incompatible broker snapshot");
    }
    for (topic, partitions) in &snapshot.topics {
        if !valid_name(topic) {
            bail!("invalid snapshot topic");
        }
        for (partition, log) in partitions {
            if *partition >= config.partitions {
                bail!("invalid snapshot partition");
            }
            for (index, record) in log.records.iter().enumerate() {
                let bytes = serde_json::to_vec(&record.payload)?.len();
                if record.topic != *topic
                    || record.partition != *partition
                    || record.offset != log.base + index as u64
                    || !valid_name(&record.tenant_id)
                    || record.key.is_empty()
                    || record.key.len() > 1024
                    || bytes > 1 << 20
                {
                    bail!("invalid snapshot record");
                }
                state.count += 1;
                state.bytes += bytes;
            }
        }
    }
    for (key, group) in &snapshot.groups {
        if !valid_name(&group.topic)
            || !valid_name(&group.name)
            || *key != group_key(&group.tenant_id, &group.topic, &group.name)
        {
            bail!("invalid snapshot group");
        }
        for (partition, offset) in &group.offsets {
            let log = snapshot
                .topics
                .get(&group.topic)
                .and_then(|parts| parts.get(partition))
                .context("snapshot group references missing partition")?;
            if *partition >= config.partitions || *offset > log.base + log.records.len() as u64 {
                bail!("invalid snapshot offset");
            }
        }
    }
    if state.count > config.max_messages || state.bytes > config.max_bytes {
        bail!("snapshot exceeds configured capacity");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    fn limits(count: usize) -> Limits {
        Limits {
            partitions: 1,
            max_messages: count,
            max_bytes: 1 << 20,
            ack_wait: Duration::from_secs(1),
        }
    }
    #[tokio::test]
    async fn groups_order_receipt_fencing_and_tenant_isolation() {
        let broker = Broker::new(limits(10), None).unwrap();
        for tenant in ["a", "a", "b"] {
            broker
                .append("events", tenant, "key", json!({}), false)
                .await
                .unwrap();
        }
        let first = broker
            .next("a", "events", "g", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert!(
            broker
                .next("a", "events", "g", Duration::ZERO)
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            !broker
                .settle("b", "events", "g", 0, &first.receipt, None)
                .await
                .unwrap()
        );
        broker
            .settle("a", "events", "g", 0, &first.receipt, Some(Duration::ZERO))
            .await
            .unwrap();
        let replacement = broker
            .next("a", "events", "g", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_ne!(replacement.receipt, first.receipt);
        assert!(
            !broker
                .settle("a", "events", "g", 0, &first.receipt, None)
                .await
                .unwrap()
        );
        assert!(
            broker
                .settle("a", "events", "g", 0, &replacement.receipt, None)
                .await
                .unwrap()
        );
        let second = broker
            .next("a", "events", "g", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second.record.offset, 1);
        let replay = broker
            .next("a", "events", "replay", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.record.offset, 0);
        let other = broker
            .next("b", "events", "g", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(other.record.tenant_id, "b");
    }
    #[tokio::test]
    async fn capacity_reclaims_only_acknowledged_prefixes() {
        let broker = Broker::new(limits(2), None).unwrap();
        broker
            .append("events", "a", "k", json!({}), false)
            .await
            .unwrap();
        broker
            .append("events", "a", "k", json!({}), false)
            .await
            .unwrap();
        assert!(
            broker
                .append("events", "a", "k", json!({}), false)
                .await
                .is_err()
        );
        let claim = broker
            .next("a", "events", "g", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        broker
            .settle("a", "events", "g", 0, &claim.receipt, None)
            .await
            .unwrap();
        let record = broker
            .append("events", "a", "k", json!({}), false)
            .await
            .unwrap();
        assert_eq!(record.offset, 2);
        let replay = broker
            .next("a", "events", "new", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(replay.record.offset, 1);
    }
    struct FailedStore;
    impl Store for FailedStore {
        fn load(&self) -> Result<Option<Vec<u8>>> {
            Ok(None)
        }
        fn save(&self, _: &[u8]) -> Result<()> {
            bail!("disk unavailable")
        }
    }
    #[tokio::test]
    async fn persistence_failure_stops_acceptance_and_delivery() {
        let broker = Broker::new(limits(10), Some(Arc::new(FailedStore))).unwrap();
        assert!(
            broker
                .append("events", "a", "k", json!({}), false)
                .await
                .is_err()
        );
        assert!(
            broker
                .next("a", "events", "g", Duration::ZERO)
                .await
                .is_err()
        );
    }
    #[tokio::test]
    async fn disk_restores_offsets_and_rejects_second_writer() {
        use crate::adapters::broker_store::DiskStore;
        let dir = std::env::temp_dir().join(format!("flowrulz-rust-broker-{}", Uuid::new_v4()));
        let store = Arc::new(DiskStore::open(&dir).unwrap());
        assert!(DiskStore::open(&dir).is_err());
        let broker = Broker::new(limits(10), Some(store.clone())).unwrap();
        for _ in 0..2 {
            broker
                .append("events", "a", "k", json!({}), false)
                .await
                .unwrap();
        }
        let claim = broker
            .next("a", "events", "g", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        broker
            .settle("a", "events", "g", 0, &claim.receipt, None)
            .await
            .unwrap();
        drop(broker);
        drop(store);
        let broker =
            Broker::new(limits(10), Some(Arc::new(DiskStore::open(&dir).unwrap()))).unwrap();
        let claim = broker
            .next("a", "events", "g", Duration::ZERO)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(claim.record.offset, 1);
        drop(broker);
        std::fs::remove_dir_all(dir).unwrap();
    }
}
