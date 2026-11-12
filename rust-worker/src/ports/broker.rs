//! Shared broker protocol version 1, matching the Go worker's transport contract.
use crate::engine::EventEnvelope;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub(crate) struct Request {
    pub version: u32,
    pub id: String,
    pub operation: String,
    pub tenant_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event: Option<EventEnvelope>,
    pub topic: String,
    pub key: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(deserialize_with = "present_payload")]
    pub payload: Option<Value>,
    pub group: String,
    pub partition: u32,
    pub receipt: String,
    pub wait_ms: i64,
    pub delay_ms: i64,
}

fn present_payload<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Response {
    pub version: u32,
    pub id: String,
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}

impl Response {
    pub fn new(id: &str, code: &str, data: Option<Value>) -> Self {
        Self {
            version: 1,
            id: id.into(),
            code: code.into(),
            data,
        }
    }
}

/// Snapshot persistence is optional and independent of transport selection.
pub(crate) trait Store: Send + Sync {
    fn load(&self) -> anyhow::Result<Option<Vec<u8>>>;
    fn save(&self, data: &[u8]) -> anyhow::Result<()>;
}

pub(crate) struct Publication {
    pub topic: String,
    pub tenant: String,
    pub key: String,
    pub payload: Value,
}

#[async_trait::async_trait]
pub(crate) trait Publisher: Send + Sync {
    async fn publish(&self, output: Publication) -> anyhow::Result<()>;
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn null_payload_and_missing_payload_remain_distinct() {
        let present: Request = serde_json::from_str(r#"{"version":1,"payload":null}"#).unwrap();
        let absent: Request = serde_json::from_str(r#"{"version":1}"#).unwrap();
        assert_eq!(present.payload, Some(Value::Null));
        assert_eq!(absent.payload, None);
    }
}
