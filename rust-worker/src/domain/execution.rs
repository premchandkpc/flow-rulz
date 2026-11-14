use super::identity::is_sha256_hex;
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
        self.validate_identity()?;
        self.validate_result()?;
        self.validate_outcome()?;
        self.validate_revisions()?;
        self.validate_effects()
    }

    fn validate_identity(&self) -> Result<(), String> {
        if self.execution_id.is_empty() || self.decision_hash.is_empty() {
            return Err("execution_id and decision_hash are required".into());
        }
        if self.event.tenant_id.is_empty()
            || self.event.event_id.is_empty()
            || self.event.partition_key.is_empty()
        {
            return Err("execution event identity is incomplete".into());
        }
        Ok(())
    }

    fn validate_result(&self) -> Result<(), String> {
        if self.fencing_epoch == 0 {
            return Err("fencing epoch must be positive".into());
        }
        if self.result_payload.is_empty() {
            return Err("execution result payload is required".into());
        }
        serde_json::from_slice::<serde_json::Value>(&self.result_payload)
            .map_err(|_| "execution result payload must be valid JSON".to_string())?;
        Ok(())
    }

    fn validate_outcome(&self) -> Result<(), String> {
        if matches!(
            &self.outcome,
            ExecutionOutcome::Quarantined { .. } | ExecutionOutcome::NoMatch
        ) && !self.effects.is_empty()
        {
            return Err("no-match or quarantined execution cannot emit effects".into());
        }
        if !is_sha256_hex(&self.decision_hash) {
            return Err("decision_hash must be a SHA-256 hex digest".into());
        }
        Ok(())
    }

    fn validate_revisions(&self) -> Result<(), String> {
        for revision in &self.rule_revisions {
            if revision.tenant_id != self.event.tenant_id
                || revision.rule_set.trim().is_empty()
                || revision.revision <= 0
                || !is_sha256_hex(&revision.content_hash)
                || revision.compiler_version.trim().is_empty()
            {
                return Err("rule revision is incomplete or belongs to another tenant".into());
            }
        }
        Ok(())
    }

    fn validate_effects(&self) -> Result<(), String> {
        let mut effect_ids = std::collections::HashSet::with_capacity(self.effects.len());
        for effect in &self.effects {
            effect.validate()?;
            if !effect_ids.insert(&effect.effect_id) {
                return Err("duplicate effect_id in execution commit".into());
            }
            self.validate_effect_identity(effect)?;
        }
        Ok(())
    }

    fn validate_effect_identity(&self, effect: &EffectIntent) -> Result<(), String> {
        if effect.execution_id != self.execution_id {
            return Err("effect execution_id does not match execution commit".into());
        }
        if effect.tenant_id != self.event.tenant_id {
            return Err("cross-tenant effect is not permitted".into());
        }
        if effect.source_event_id != self.event.event_id {
            return Err("effect source_event_id does not match execution event".into());
        }
        Ok(())
    }
}
