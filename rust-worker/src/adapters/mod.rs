//! Infrastructure adapters. The file WAL is single-node durability, not a replicated broker.
pub(crate) mod broker_http;
pub(crate) mod broker_nats;
pub(crate) mod broker_output;
pub(crate) mod broker_store;
pub(crate) mod broker_tcp;
pub mod file_log;
