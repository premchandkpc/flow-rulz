//! Compiled rule model, predicate evaluation, and effect construction.
use super::{EventEnvelope, sha256_hex};
use anyhow::{Result, anyhow, bail};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
pub(super) struct RuleRevision {
    pub(super) tenant_scope: String,
    pub(super) rule_id: String,
    pub(super) revision: i64,
    pub(super) content_hash: String,
    pub(super) match_mode: String,
    #[serde(default)]
    pub(super) input_contract: Option<ContractRef>,
    pub(super) compiled: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
pub(super) struct ContractRef {
    pub(super) name: String,
    pub(super) version: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct Rule {
    pub(super) id: String,
    #[serde(default)]
    pub(super) priority: i64,
    #[serde(rename = "when")]
    pub(super) predicate: Predicate,
    #[serde(default)]
    pub(super) then: Vec<Action>,
    #[serde(default)]
    pub(super) otherwise: Vec<Action>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Predicate {
    #[serde(default)]
    pub(super) all: Option<Vec<Predicate>>,
    #[serde(default)]
    pub(super) any: Option<Vec<Predicate>>,
    #[serde(default)]
    pub(super) not: Option<Box<Predicate>>,
    #[serde(default)]
    pub(super) path: Option<String>,
    #[serde(default)]
    pub(super) op: Option<String>,
    #[serde(default)]
    pub(super) value: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Action {
    #[serde(default)]
    pub(super) emit: Option<EmitAction>,
    #[serde(default)]
    pub(super) emit_event: Option<EmitEventAction>,
    #[serde(default)]
    pub(super) command: Option<CommandAction>,
}

#[derive(Debug, Deserialize)]
pub(super) struct EmitAction {
    topic: String,
    data: Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct EmitEventAction {
    #[serde(rename = "type")]
    pub(super) event_type: String,
    #[serde(default)]
    pub(super) partition_key: String,
    #[serde(default)]
    pub(super) partition_key_policy: String,
    #[serde(default)]
    pub(super) partition_key_path: String,
    pub(super) data: Value,
}

#[derive(Debug, Deserialize)]
pub(super) struct CommandAction {
    destination: String,
    name: String,
    data: Value,
}

#[derive(Debug)]
pub(super) struct Effect {
    pub(super) id: String,
    pub(super) tenant_id: String,
    pub(super) destination: String,
    pub(super) name: String,
    pub(super) payload: Value,
    pub(super) effect_type: &'static str,
}

pub(super) fn evaluate(
    revision: &RuleRevision,
    event: &EventEnvelope,
) -> Result<(Vec<String>, Vec<Effect>)> {
    if revision.tenant_scope != "__domain_configuration__" {
        bail!("rule revision is not in the global configuration scope");
    }
    Evaluation::new(revision, event).run()
}

pub(super) fn eval_predicate(predicate: &Predicate, data: &Value) -> Result<bool> {
    if let Some(result) = evaluate_composite_predicate(predicate, data)? {
        return Ok(result);
    }
    evaluate_leaf_predicate(predicate, data)
}

fn evaluate_composite_predicate(predicate: &Predicate, data: &Value) -> Result<Option<bool>> {
    if let Some(children) = &predicate.all {
        return evaluate_children(children, data, false).map(Some);
    }
    if let Some(children) = &predicate.any {
        return evaluate_children(children, data, true).map(Some);
    }
    if let Some(child) = &predicate.not {
        return Ok(Some(!eval_predicate(child, data)?));
    }
    Ok(None)
}

fn evaluate_leaf_predicate(predicate: &Predicate, data: &Value) -> Result<bool> {
    let found = predicate_value(predicate, data)?;
    let operator = predicate.op.as_deref().unwrap_or("");
    match operator {
        "exists" => Ok(found.is_some()),
        "not_exists" => Ok(found.is_none()),
        _ => match found {
            Some(actual) => evaluate_comparison(operator, actual, predicate.value.as_ref()),
            None => Ok(false),
        },
    }
}

fn evaluate_comparison(operator: &str, actual: &Value, expected: Option<&Value>) -> Result<bool> {
    match operator {
        "eq" => Ok(compare_eq(actual, expected)),
        "neq" => Ok(!compare_eq(actual, expected)),
        "gt" => Ok(compare(Some(actual), expected, |a, b| a > b)),
        "gte" => Ok(compare(Some(actual), expected, |a, b| a >= b)),
        "lt" => Ok(compare(Some(actual), expected, |a, b| a < b)),
        "lte" => Ok(compare(Some(actual), expected, |a, b| a <= b)),
        "in" => Ok(contains_value(expected, actual)),
        "not_in" => Ok(!contains_value(expected, actual)),
        _ => bail!("unsupported predicate operator {operator:?}"),
    }
}

pub(super) fn compare_eq(actual: &Value, expected: Option<&Value>) -> bool {
    let Some(expected) = expected else {
        return false;
    };
    match (number_value(actual), number_value(expected)) {
        (Some(a), Some(b)) => a == b,
        _ => actual == expected,
    }
}

fn number_value(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn compare(
    found: Option<&Value>,
    expected: Option<&Value>,
    predicate: impl Fn(f64, f64) -> bool,
) -> bool {
    match (
        found.and_then(number_value),
        expected.and_then(number_value),
    ) {
        (Some(a), Some(b)) => predicate(a, b),
        _ => false,
    }
}

fn resolve_path<'a>(path: &str, root: &'a Value) -> Option<&'a Value> {
    let trimmed = path.strip_prefix("$.").unwrap_or(path);
    if trimmed.is_empty() || trimmed == "facts" || trimmed.starts_with("facts.") {
        return None;
    }
    let resolved = trimmed
        .split('.')
        .filter(|part| !part.is_empty())
        .try_fold(root, |value, key| value.as_object()?.get(key));
    resolved.filter(|value| !value.is_null())
}

fn interpolate_templates(value: &Value, event: &EventEnvelope, child_id: &str) -> Value {
    match value {
        Value::String(text) => expand_template(text, event, child_id),
        Value::Array(items) => Value::Array(
            items
                .iter()
                .map(|item| interpolate_templates(item, event, child_id))
                .collect(),
        ),
        Value::Object(object) => interpolate_object(object, event, child_id),
        other => other.clone(),
    }
}

pub(super) fn expand_template(text: &str, event: &EventEnvelope, child_id: &str) -> Value {
    if !text.contains("{{") {
        return Value::String(text.to_owned());
    }
    if let Some(captures) = PLACEHOLDER.captures(text)
        && captures.get(0).is_some_and(|whole| whole.as_str() == text)
    {
        return lookup_capture(&captures, event, child_id).unwrap_or(Value::String(String::new()));
    }
    render_template(text, event, child_id)
}

fn render_template(text: &str, event: &EventEnvelope, child_id: &str) -> Value {
    let rendered = PLACEHOLDER.replace_all(text, |captures: &regex::Captures<'_>| {
        lookup_capture(captures, event, child_id)
            .map(value_text)
            .unwrap_or_default()
    });
    Value::String(rendered.into_owned())
}

fn template_lookup(expression: &str, event: &EventEnvelope, child_id: &str) -> Option<Value> {
    match expression.trim() {
        "new_id" => Some(Value::String(child_id.to_owned())),
        "now" => Some(Value::String(
            Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true),
        )),
        path => resolve_path(path, &event.data).cloned(),
    }
}

pub(super) fn build_effect(
    revision: &RuleRevision,
    event: &EventEnvelope,
    rule_id: &str,
    index: usize,
    action: &Action,
) -> Result<Effect> {
    let id = effect_id(revision, event, rule_id, index);
    action_effect(id, event, rule_id, index, action)
}

fn effect_id(
    revision: &RuleRevision,
    event: &EventEnvelope,
    rule_id: &str,
    index: usize,
) -> String {
    sha256_hex(
        format!(
            "{}|{}|{}|{}|{}|{}",
            event.tenant_id, event.id, revision.rule_id, revision.revision, rule_id, index
        )
        .as_bytes(),
    )
}

fn build_emit_effect(id: String, event: &EventEnvelope, emit: &EmitAction) -> Result<Effect> {
    if emit.topic.trim().is_empty() {
        bail!("emit topic is required");
    }
    Ok(Effect {
        id,
        tenant_id: event.tenant_id.clone(),
        destination: emit.topic.clone(),
        name: emit.topic.clone(),
        payload: emit.data.clone(),
        effect_type: "emit",
    })
}

struct ChildEvent<'a> {
    event: &'a EventEnvelope,
    rule_id: &'a str,
    index: usize,
    action: &'a EmitEventAction,
}

impl ChildEvent<'_> {
    fn effect(&self, id: String) -> Result<Effect> {
        let child_id = self.child_id();
        let key = resolve_child_partition_key(self.action, self.event)?;
        Ok(Effect {
            id,
            tenant_id: self.event.tenant_id.clone(),
            destination: format!("events.{}", self.action.event_type),
            name: self.action.event_type.clone(),
            payload: build_child_event_payload(self.action, self.event, &child_id, &key),
            effect_type: "emit_event",
        })
    }

    fn child_id(&self) -> String {
        let event = self.event;
        let identity = format!(
            "child|{}:{}|{}:{}|{}:{}|{}",
            event.tenant_id.len(),
            event.tenant_id,
            event.id.len(),
            event.id,
            self.rule_id.len(),
            self.rule_id,
            self.index
        );
        sha256_hex(identity.as_bytes())
    }
}

fn resolve_child_partition_key(action: &EmitEventAction, event: &EventEnvelope) -> Result<String> {
    let key = match action.partition_key_policy.as_str() {
        "" | "explicit" => action.partition_key.clone(),
        "inherit" => event.partition_key.clone(),
        "from_data" => resolve_path(&action.partition_key_path, &event.data)
            .cloned()
            .map(value_text)
            .unwrap_or_default(),
        policy => bail!("unsupported partition key policy {policy:?}"),
    };
    if key.is_empty() {
        bail!("emit_event resolved an empty partition key");
    }
    Ok(key)
}

fn build_child_event_payload(
    action: &EmitEventAction,
    event: &EventEnvelope,
    child_id: &str,
    partition_key: &str,
) -> Value {
    let mut payload = child_payload(action, event, child_id, partition_key);
    copy_lineage(event, &mut payload);
    payload
}

fn copy_lineage(event: &EventEnvelope, payload: &mut Value) {
    if let Some(workflow_id) = &event.workflow_id {
        payload["workflow_id"] = json!(workflow_id);
    }
    if let Some(correlation_id) = &event.correlation_id {
        payload["correlation_id"] = json!(correlation_id);
    }
    payload["causation_id"] = json!(event.id);
}

fn build_command_effect(
    id: String,
    event: &EventEnvelope,
    action: &CommandAction,
) -> Result<Effect> {
    if action.destination.trim().is_empty() || action.name.trim().is_empty() {
        bail!("command destination and name are required");
    }
    Ok(Effect {
        id,
        tenant_id: event.tenant_id.clone(),
        destination: action.destination.clone(),
        name: action.name.clone(),
        payload: action.data.clone(),
        effect_type: "command",
    })
}

struct Evaluation<'a> {
    revision: &'a RuleRevision,
    event: &'a EventEnvelope,
    matched: Vec<String>,
    effects: Vec<Effect>,
}

impl<'a> Evaluation<'a> {
    fn new(revision: &'a RuleRevision, event: &'a EventEnvelope) -> Self {
        Self {
            revision,
            event,
            matched: Vec::new(),
            effects: Vec::new(),
        }
    }

    fn run(mut self) -> Result<(Vec<String>, Vec<Effect>)> {
        for rule in ordered_rules(self.revision) {
            let matched = self.apply(rule)?;
            if self.revision.match_mode == "first_match" && matched {
                break;
            }
        }
        Ok((self.matched, self.effects))
    }

    fn apply(&mut self, rule: &Rule) -> Result<bool> {
        let matched = eval_predicate(&rule.predicate, &self.event.data)?;
        let actions = if matched { &rule.then } else { &rule.otherwise };
        if matched {
            self.matched.push(rule.id.clone());
        }
        self.apply_actions(rule, actions)?;
        Ok(matched)
    }

    fn apply_actions(&mut self, rule: &Rule, actions: &[Action]) -> Result<()> {
        for (index, action) in actions.iter().enumerate() {
            self.effects.push(build_effect(
                self.revision,
                self.event,
                &rule.id,
                index,
                action,
            )?);
        }
        Ok(())
    }
}

fn ordered_rules(revision: &RuleRevision) -> Vec<&Rule> {
    let mut rules: Vec<&Rule> = revision.compiled.iter().collect();
    rules.sort_by(|left, right| {
        right
            .priority
            .cmp(&left.priority)
            .then_with(|| left.id.cmp(&right.id))
    });
    rules
}

fn evaluate_children(children: &[Predicate], data: &Value, stop_on: bool) -> Result<bool> {
    for child in children {
        if eval_predicate(child, data)? == stop_on {
            return Ok(stop_on);
        }
    }
    Ok(!stop_on)
}

fn contains_value(expected: Option<&Value>, actual: &Value) -> bool {
    expected
        .and_then(Value::as_array)
        .is_some_and(|values| values.iter().any(|value| compare_eq(actual, Some(value))))
}

fn interpolate_object(
    object: &serde_json::Map<String, Value>,
    event: &EventEnvelope,
    child_id: &str,
) -> Value {
    let mut result = serde_json::Map::new();
    for (key, item) in object {
        result.insert(key.clone(), interpolate_templates(item, event, child_id));
    }
    Value::Object(result)
}

static PLACEHOLDER: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| {
    regex::Regex::new(r"\{\{\s*([^{}]+?)\s*\}\}").expect("static template regex")
});

fn lookup_capture(
    captures: &regex::Captures<'_>,
    event: &EventEnvelope,
    id: &str,
) -> Option<Value> {
    template_lookup(
        captures.get(1).map(|m| m.as_str()).unwrap_or_default(),
        event,
        id,
    )
}

fn value_text(value: Value) -> String {
    match value {
        Value::String(text) => text,
        value => value.to_string(),
    }
}

fn action_effect(
    id: String,
    event: &EventEnvelope,
    rule_id: &str,
    index: usize,
    action: &Action,
) -> Result<Effect> {
    match (&action.emit, &action.emit_event, &action.command) {
        (Some(emit), None, None) => build_emit_effect(id, event, emit),
        (None, Some(child), None) => child_effect(id, event, rule_id, index, child),
        (None, None, Some(command)) => build_command_effect(id, event, command),
        _ => bail!("action must contain exactly one supported action"),
    }
}

fn child_payload(action: &EmitEventAction, event: &EventEnvelope, id: &str, key: &str) -> Value {
    json!({
        "id": id,
        "type": action.event_type,
        "meta": child_meta(event, key),
        "occurred_at": Utc::now(),
        "data": interpolate_templates(&action.data, event, id),
        "headers": {"flowrulz_parent_event_id": event.id, "flowrulz_child_event_id": id}
    })
}

fn child_meta(event: &EventEnvelope, partition_key: &str) -> HashMap<String, Value> {
    let mut meta = event.meta.clone();
    meta.insert("tenant_id".into(), json!(event.tenant_id));
    meta.insert("partition_key".into(), json!(partition_key));
    meta
}

fn predicate_value<'a>(predicate: &Predicate, data: &'a Value) -> Result<Option<&'a Value>> {
    let path = predicate
        .path
        .as_deref()
        .ok_or_else(|| anyhow!("predicate path missing"))?;
    Ok(resolve_path(path, data))
}

fn child_effect(
    id: String,
    event: &EventEnvelope,
    rule_id: &str,
    index: usize,
    action: &EmitEventAction,
) -> Result<Effect> {
    ChildEvent {
        event,
        rule_id,
        index,
        action,
    }
    .effect(id)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(tenant_id: &str) -> EventEnvelope {
        serde_json::from_value(json!({
            "id": "event-1", "type": "orders.created", "occurred_at": Utc::now(),
            "meta": {"tenant_id": tenant_id, "partition_key": "order-1"},
            "data": {"approved": true}
        }))
        .unwrap()
    }

    fn matching_rule(id: &str, priority: i64) -> Rule {
        Rule {
            id: id.into(),
            priority,
            predicate: approved_predicate(),
            then: Vec::new(),
            otherwise: Vec::new(),
        }
    }

    fn approved_predicate() -> Predicate {
        serde_json::from_value(json!({"path": "$.approved", "op": "eq", "value": true})).unwrap()
    }

    fn revision(tenant_scope: &str, match_mode: &str, compiled: Vec<Rule>) -> RuleRevision {
        RuleRevision {
            tenant_scope: tenant_scope.into(),
            rule_id: "orders.created".into(),
            revision: 1,
            content_hash: "contract-hash".into(),
            match_mode: match_mode.into(),
            input_contract: None,
            compiled,
        }
    }

    #[test]
    fn first_match_uses_descending_priority_with_deterministic_ties() {
        let revision = revision(
            "__domain_configuration__",
            "first_match",
            vec![matching_rule("low", 1), matching_rule("high", 100)],
        );
        let (matched, effects) = evaluate(&revision, &event("tenant-a")).unwrap();
        assert_eq!(matched, vec!["high"]);
        assert!(effects.is_empty());
    }

    #[test]
    fn global_rule_revision_can_process_events_from_different_tenants() {
        let revision = revision(
            "__domain_configuration__",
            "all_matches",
            vec![matching_rule("rule-1", 1)],
        );
        assert!(evaluate(&revision, &event("tenant-a")).is_ok());
        assert!(evaluate(&revision, &event("tenant-b")).is_ok());
    }

    #[test]
    fn evaluation_rejects_non_global_configuration_scope() {
        let revision = revision("tenant-a", "all_matches", vec![matching_rule("rule-1", 1)]);
        assert!(evaluate(&revision, &event("tenant-a")).is_err());
    }
    #[test]
    fn child_event_preserves_workflow_correlation_and_causation() {
        let mut parent = event("tenant-a");
        parent.workflow_id = Some("workflow-1".into());
        parent.correlation_id = Some("correlation-1".into());
        let action = approved_child_action();
        let payload = build_child_event_payload(&action, &parent, "child-1", "order-1");
        assert_eq!(payload["workflow_id"], "workflow-1");
        assert_eq!(payload["correlation_id"], "correlation-1");
        assert_eq!(payload["causation_id"], "event-1");
        assert_eq!(payload["headers"]["flowrulz_parent_event_id"], "event-1");
    }

    fn approved_child_action() -> EmitEventAction {
        serde_json::from_value(json!({
            "type": "orders.approved",
            "partition_key_policy": "inherit",
            "data": {"order_id": "{{$.order_id}}"}
        }))
        .unwrap()
    }
}
