use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct EventIdentity {
    pub tenant_id: String,
    pub event_id: String,
    pub partition_key: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub meta: Option<BTreeMap<String, serde_json::Value>>,
}
pub fn deterministic_id(namespace: &str, parts: &[&str]) -> String {
    let mut h = Sha256::new();
    field(&mut h, namespace.as_bytes());
    for part in parts {
        field(&mut h, part.as_bytes());
    }
    encode_hex(&h.finalize())
}
pub fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }
    output
}
fn field(h: &mut Sha256, value: &[u8]) {
    h.update((value.len() as u64).to_be_bytes());
    h.update(value);
}

pub(super) fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn stable_and_namespaced() {
        assert_eq!(
            deterministic_id("event", &["t", "e"]),
            deterministic_id("event", &["t", "e"])
        );
        assert_ne!(
            deterministic_id("event", &["t", "e"]),
            deterministic_id("effect", &["t", "e"])
        );
    }
    #[test]
    fn boundaries_are_unambiguous() {
        assert_ne!(
            deterministic_id("x", &["ab", "c"]),
            deterministic_id("x", &["a", "bc"])
        );
    }
}
