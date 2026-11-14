//! Local JSON rule loading reuses the existing pure evaluator and canonical event.
use super::{EventEnvelope, rules, sha256_hex};
use crate::ports::broker::Publication as Output;
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};

pub(crate) struct LocalRuleSet {
    revision: rules::RuleRevision,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Source {
    rule_set: String,
    revision: i64,
    mode: String,
    rules: Value,
    #[serde(default)]
    input_contract: Option<Value>,
}

impl LocalRuleSet {
    pub fn compile(source: &[u8]) -> Result<Self> {
        if source.len() > 256 * 1024 {
            bail!("local rule file exceeds 256 KiB");
        }
        let content_hash = sha256_hex(source);
        let source: Source = serde_json::from_slice(source).context("parse local rules")?;
        source.validate()?;
        validate_rules(&source.rules)?;
        Ok(Self {
            revision: source.into_revision(content_hash)?,
        })
    }

    pub fn name(&self) -> &str {
        &self.revision.rule_id
    }

    pub fn evaluate(&self, event: &EventEnvelope) -> Result<Vec<Output>> {
        event.validate()?;
        if event.contract.is_some() {
            bail!("local rules cannot validate registry contracts");
        }
        let (_, effects) = rules::evaluate(&self.revision, event)?;
        let execution_id = local_execution_id(event, &self.revision);
        effects
            .into_iter()
            .map(|effect| effect_output(effect, event, &execution_id))
            .collect()
    }
}

fn validate_predicate(value: &Value, depth: usize) -> Result<()> {
    if depth > 10 {
        bail!("predicate nesting exceeds ten levels");
    }
    let object = value.as_object().context("predicate must be an object")?;
    if let Some(child) = object.get("not") {
        return validate_predicate(child, depth + 1);
    }
    for name in ["all", "any"] {
        if let Some(items) = object.get(name) {
            return validate_predicate_group(items, depth);
        }
    }
    validate_leaf_predicate(value)
}

fn validate_subject(subject: &str) -> Result<()> {
    if subject.is_empty() || subject.len() > 128 {
        bail!("invalid output topic");
    }
    for token in subject.split('.') {
        validate_subject_token(token)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn event(tenant: &str) -> EventEnvelope {
        serde_json::from_value(json!({"id":"event-1","type":"a.created","occurred_at":"2026-11-12T12:00:00Z","meta":{"tenant_id":tenant,"partition_key":"key-1"},"data":{"id":"123"}})).unwrap()
    }
    #[test]
    fn canonical_sample_chain_and_tenant_scoped_child_ids() {
        let first = sample_rules("first");
        let output = first.evaluate(&event("tenant-a")).unwrap().remove(0);
        assert_eq!(output.topic, "events");
        assert!(output.payload.get("tenant_id").is_none());
        let child: EventEnvelope = serde_json::from_value(output.payload).unwrap();
        assert_sample_child(&child);
        assert_sample_command(&child);
        let other = first.evaluate(&event("tenant-b")).unwrap().remove(0);
        assert_ne!(other.payload["id"], child.id);
    }
    #[test]
    fn unsupported_scheduled_actions_and_contracts_fail_at_startup() {
        let mut source: Value = serde_json::from_slice(include_bytes!(
            "../../../examples/direct-worker/rules/a.json"
        ))
        .unwrap();
        source["rules"][0]["then"] = json!([{"schedule_event":{"type":"later"}}]);
        assert!(LocalRuleSet::compile(&serde_json::to_vec(&source).unwrap()).is_err());
        source["input_contract"] = json!({"name":"contract","version":"1"});
        assert!(LocalRuleSet::compile(&serde_json::to_vec(&source).unwrap()).is_err());
    }
    fn sample_rules(stage: &str) -> LocalRuleSet {
        let source: &[u8] = match stage {
            "first" => include_bytes!("../../../examples/direct-worker/rules/a.json"),
            _ => include_bytes!("../../../examples/direct-worker/rules/processed.json"),
        };
        LocalRuleSet::compile(source).unwrap()
    }
    fn assert_sample_child(child: &EventEnvelope) {
        // Byte lengths must match the Go child ID contract.
        let name = "forward-to-next-worker";
        let expected =
            sha256_hex(format!("child|8:tenant-a|7:event-1|{}:{}|0", name.len(), name).as_bytes());
        assert_eq!(child.id, expected);
        assert_eq!(child.data["id"], "123");
    }
    fn assert_sample_command(child: &EventEnvelope) {
        let command = sample_rules("last").evaluate(child).unwrap().remove(0);
        assert_eq!(command.topic, "commands.b.handle");
        assert_eq!(command.payload["idempotency_key"], command.payload["id"]);
        assert_eq!(command.payload["tenant_id"], "tenant-a");
    }
}

impl Source {
    fn validate(&self) -> Result<()> {
        if self.rule_set.is_empty()
            || self.rule_set.len() > 128
            || self.revision <= 0
            || !matches!(self.mode.as_str(), "first_match" | "all_matches")
            || self.input_contract.is_some()
        {
            bail!(
                "local rule requires a name, positive revision, supported mode and no registry contract"
            );
        }
        Ok(())
    }
    fn into_revision(self, content_hash: String) -> Result<rules::RuleRevision> {
        Ok(rules::RuleRevision {
            tenant_scope: "__domain_configuration__".into(),
            rule_id: self.rule_set,
            revision: self.revision,
            content_hash,
            match_mode: self.mode,
            input_contract: None,
            compiled: serde_json::from_value(self.rules)?,
        })
    }
}

fn validate_rules(value: &Value) -> Result<()> {
    let items = value.as_array().context("rules must be an array")?;
    if items.is_empty() || items.len() > 100 {
        bail!("local rules require 1..100 rules");
    }
    let mut actions = 0;
    let mut ids = std::collections::HashSet::new();
    for rule in items {
        actions += validate_rule(rule, &mut ids)?;
    }
    if actions > 10 {
        bail!("local rules exceed the ten-action limit");
    }
    Ok(())
}

fn validate_rule<'a>(
    rule: &'a Value,
    ids: &mut std::collections::HashSet<&'a str>,
) -> Result<usize> {
    let id = rule["id"].as_str().context("rule id is required")?;
    if id.is_empty() || id.len() > 128 || !ids.insert(id) {
        bail!("invalid or duplicate local rule id");
    }
    validate_predicate(&rule["when"], 0)?;
    let mut count = 0;
    for name in ["then", "otherwise"] {
        if let Some(items) = rule.get(name) {
            count += validate_actions(items)?;
        }
    }
    Ok(count)
}

fn validate_actions(value: &Value) -> Result<usize> {
    let items = value.as_array().context("actions must be an array")?;
    for action in items {
        let object = action.as_object().context("action must be an object")?;
        if object.len() != 1
            || !object
                .keys()
                .all(|name| matches!(name.as_str(), "emit" | "command" | "emit_event"))
        {
            bail!("exactly one supported action is required");
        }
        if action.get("schedule_event").is_some() {
            bail!("scheduled actions require the durable runtime");
        }
    }
    Ok(items.len())
}

fn local_execution_id(event: &EventEnvelope, revision: &rules::RuleRevision) -> String {
    let tenant = &event.tenant_id;
    let id = &event.id;
    let rule = &revision.rule_id;
    sha256_hex(
        format!(
            "execution|{}:{}|{}:{}|{}:{}|{}",
            tenant.len(),
            tenant,
            id.len(),
            id,
            rule.len(),
            rule,
            revision.revision
        )
        .as_bytes(),
    )
}

fn effect_output(
    effect: rules::Effect,
    event: &EventEnvelope,
    execution_id: &str,
) -> Result<Output> {
    if effect.effect_type == "emit_event" {
        return child_output(effect);
    }
    let topic = effect_topic(&effect)?;
    let payload = effect_payload(&effect, execution_id);
    Ok(Output {
        topic,
        tenant: event.tenant_id.clone(),
        key: effect.id,
        payload,
    })
}

fn child_output(effect: rules::Effect) -> Result<Output> {
    let child: EventEnvelope = serde_json::from_value(effect.payload.clone())?;
    Ok(Output {
        topic: "events".into(),
        tenant: child.tenant_id,
        key: child.partition_key,
        payload: effect.payload,
    })
}

fn effect_topic(effect: &rules::Effect) -> Result<String> {
    match effect.effect_type {
        "emit" => {
            validate_subject(&effect.destination)?;
            Ok(format!("effects.{}", effect.destination))
        }
        "command" => command_topic(&effect.destination, &effect.name),
        _ => bail!("unsupported local effect"),
    }
}

fn command_topic(destination: &str, name: &str) -> Result<String> {
    validate_subject(destination)?;
    if destination.contains('.') {
        bail!("command destination must be one token");
    }
    validate_subject(name)?;
    Ok(format!("commands.{destination}.{name}"))
}

fn effect_payload(effect: &rules::Effect, execution_id: &str) -> Value {
    json!({"id": effect.id, "execution_id": execution_id,
        "tenant_id": effect.tenant_id, "destination": effect.destination,
        "name": effect.name, "payload": effect.payload, "effect_type": effect.effect_type,
        "idempotency_key": effect.id, "created_at": chrono::Utc::now()})
}

fn validate_predicate_group(value: &Value, depth: usize) -> Result<()> {
    let items = value
        .as_array()
        .context("predicate group must be an array")?;
    if items.is_empty() || items.len() > 200 {
        bail!("invalid predicate group size");
    }
    for child in items {
        validate_predicate(child, depth + 1)?;
    }
    Ok(())
}

fn validate_leaf_predicate(value: &Value) -> Result<()> {
    let path = value["path"]
        .as_str()
        .context("predicate path is required")?;
    if !path.starts_with("$.") || path.contains(['[', ']']) {
        bail!("local Rust predicates require object paths");
    }
    if !matches!(
        value["op"].as_str(),
        Some(
            "exists" | "not_exists" | "eq" | "neq" | "gt" | "gte" | "lt" | "lte" | "in" | "not_in"
        )
    ) {
        bail!("unsupported local predicate operator");
    }
    Ok(())
}

fn validate_subject_token(token: &str) -> Result<()> {
    if token.is_empty()
        || token.len() > 63
        || !token.as_bytes()[0].is_ascii_lowercase()
        || !token
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-')
    {
        bail!("invalid output topic token");
    }
    Ok(())
}
