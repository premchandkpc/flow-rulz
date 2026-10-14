use sha2::{Digest, Sha256};
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EventIdentity { pub tenant_id: String, pub event_id: String, pub partition_key: String }
pub fn deterministic_id(namespace: &str, parts: &[&str]) -> String {
 let mut h = Sha256::new(); field(&mut h, namespace.as_bytes());
 for part in parts { field(&mut h, part.as_bytes()); }
 hex::encode(h.finalize())
}
fn field(h: &mut Sha256, value: &[u8]) { h.update((value.len() as u64).to_be_bytes()); h.update(value); }
#[cfg(test)] mod tests { use super::*;
 #[test] fn stable_and_namespaced() { assert_eq!(deterministic_id("event", &["t","e"]), deterministic_id("event", &["t","e"])); assert_ne!(deterministic_id("event", &["t","e"]), deterministic_id("effect", &["t","e"])); }
 #[test] fn boundaries_are_unambiguous() { assert_ne!(deterministic_id("x", &["ab","c"]), deterministic_id("x", &["a","bc"])); }
}