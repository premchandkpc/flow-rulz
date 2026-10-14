use serde::{Deserialize, Serialize};
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ContractRef { pub name: String, pub version: String, pub content_hash: String }
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct RuleRevisionRef { pub tenant_id: String, pub rule_set: String, pub revision: i64, pub content_hash: String, pub compiler_version: String }
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum OutputRoute { Consumer { destination: String }, RuleSet { rule_set: String }, Workflow { workflow_id: String }, Schedule { schedule_id: String }, Reply { correlation_id: String }, Internal { processor: String } }
impl OutputRoute { pub fn validate(&self) -> Result<(), String> { let target = match self { Self::Consumer { destination } => destination, Self::RuleSet { rule_set } => rule_set, Self::Workflow { workflow_id } => workflow_id, Self::Schedule { schedule_id } => schedule_id, Self::Reply { correlation_id } => correlation_id, Self::Internal { processor } => processor }; if target.trim().is_empty() { Err("output route target is required".into()) } else { Ok(()) } } }