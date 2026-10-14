use super::{EffectIntent, EventIdentity, RuleRevisionRef};
use bytes::Bytes;

#[derive(Debug, Clone)]
pub enum ExecutionOutcome {
    Completed,
    NoMatch,
    Quarantined { category: String, message: String },
}

#[derive(Debug, Clone)]
pub struct ExecutionCommit {
    pub execution_id: String,
    pub event: EventIdentity,
    pub rule_revisions: Vec<RuleRevisionRef>,
    pub outcome: ExecutionOutcome,
    pub decision_hash: String,
    pub matched_rule_ids: Vec<String>,
    pub result_payload: Bytes,
    pub effects: Vec<EffectIntent>,
    pub fencing_epoch: u64,
}

impl ExecutionCommit {
    pub fn validate(&self) -> Result<(), String> {
        if self.execution_id.is_empty() || self.decision_hash.is_empty() {
            return Err("execution_id and decision_hash are required".into());
        }
        if self.event.tenant_id.is_empty()
            || self.event.event_id.is_empty()
            || self.event.partition_key.is_empty()
        {
            return Err("execution event identity is incomplete".into());
        }
        if self.fencing_epoch == 0 {
            return Err("fencing epoch must be positive".into());
        }
        if self.result_payload.is_empty() {
            return Err("execution result payload is required".into());
        }
        serde_json::from_slice::<serde_json::Value>(&self.result_payload)
            .map_err(|_| "execution result payload must be valid JSON".to_string())?;
        if matches!(self.outcome, ExecutionOutcome::Quarantined { .. }) && !self.effects.is_empty() {
            return Err("quarantined execution cannot emit effects".into());
        }
        for revision in &self.rule_revisions {
            if revision.tenant_id != self.event.tenant_id
                || revision.rule_set.trim().is_empty()
                || revision.revision <= 0
                || revision.content_hash.trim().is_empty()
                || revision.compiler_version.trim().is_empty()
            {
                return Err("rule revision is incomplete or belongs to another tenant".into());
            }
        }
        for effect in &self.effects {
            effect.validate()?;
            if effect.execution_id != self.execution_id {
                return Err("effect execution_id does not match execution commit".into());
            }
            if effect.tenant_id != self.event.tenant_id {
                return Err("cross-tenant effect is not permitted".into());
            }
            if effect.source_event_id != self.event.event_id {
                return Err("effect source_event_id does not match execution event".into());
            }
        }
        Ok(())
    }
}
