//! Storage- and transport-independent domain model.
pub mod contract;
pub mod event;
pub mod identity;
pub mod log_record;
pub use contract::{ContractRef, OutputRoute, RuleRevisionRef};
pub use event::{EventEnvelope, ExecutionMode};
pub use identity::{deterministic_id, EventIdentity};
pub use log_record::{LogPosition, LogRecord, LogRecordKind, PartitionId};