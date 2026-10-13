//! Compiled rule model, predicate evaluation, and effect construction.
use super::{sha256_hex, EventEnvelope};
use anyhow::{anyhow, bail, Result};
use chrono::Utc;
use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
pub(super) struct RuleRevision {
    pub(super) tenant_scope: String,
    pub(super) rule_id: String,
    pub(super) revision: i64,
    pub(super) content_hash: String,
    pub(super) match_mode: String,
    pub(super) compiled: Vec<Rule>,
}

#[derive(Debug, Deserialize)]
pub(super) struct Rule {
    pub(super) id: String,
    #[serde(default)]
    pub(super) name: String,
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
pub(super) struct Action {
    #[serde(default)]
    pub(super) emit: Option<EmitAction>,
    #[serde(default)]
    pub(super) emit_event: Option<EmitEventAction>,
    #[serde(default)]
    pub(super) command: Option<CommandAction>,
}

#[derive(Debug, Deserialize)]
pub(super) struct EmitAction { topic: String, data: Value }

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
pub(super) struct CommandAction { destination: String, name: String, data: Value }

#[derive(Debug)]
pub(super) struct Effect {
    pub(super) id: String,
    pub(super) tenant_id: String,
    pub(super) destination: String,
    pub(super) name: String,
    pub(super) payload: Value,
    pub(super) effect_type: &'static str,
}

pub(super) fn evaluate(revision: &RuleRevision, event: &EventEnvelope) -> Result<(Vec<String>, Vec<Effect>)> {
    let mut matched = Vec::new();
    let mut effects = Vec::new();
    for rule in &revision.compiled {
        let is_match = eval_predicate(&rule.predicate, &event.data)?;
        let actions = if is_match { &rule.then } else { &rule.otherwise };
        if is_match { matched.push(rule.id.clone()); }
        for (index, action) in actions.iter().enumerate() {
            effects.push(build_effect(revision, event, &rule.id, index, action)?);
        }
        if revision.match_mode == "first_match" && is_match { break; }
    }
    Ok((matched, effects))
}

pub(super) fn eval_predicate(predicate: &Predicate, data: &Value) -> Result<bool> {
    if let Some(result) = evaluate_composite_predicate(predicate, data)? {
        return Ok(result);
    }
    evaluate_leaf_predicate(predicate, data)
}

fn evaluate_composite_predicate(predicate: &Predicate, data: &Value) -> Result<Option<bool>> {
    if let Some(children) = &predicate.all {
        for child in children {
            if !eval_predicate(child, data)? {
                return Ok(Some(false));
            }
        }
        return Ok(Some(true));
    }
    if let Some(children) = &predicate.any {
        for child in children {
            if eval_predicate(child, data)? {
                return Ok(Some(true));
            }
        }
        return Ok(Some(false));
    }
    if let Some(child) = &predicate.not {
        return Ok(Some(!eval_predicate(child, data)?));
    }
    Ok(None)
}

fn evaluate_leaf_predicate(predicate: &Predicate, data: &Value) -> Result<bool> {
    let path = predicate.path.as_deref().ok_or_else(|| anyhow!("predicate path missing"))?;
    let found = resolve_path(path, data);
    let operator = predicate.op.as_deref().unwrap_or("");
    let expected = predicate.value.as_ref();

    match operator {
        "exists" => Ok(found.is_some()),
        "not_exists" => Ok(found.is_none()),
        _ => match found {
            Some(actual) => evaluate_comparison(operator, actual, expected),
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
        "in" => Ok(expected.and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| compare_eq(actual, Some(value))))),
        "not_in" => Ok(!expected.and_then(Value::as_array)
            .is_some_and(|values| values.iter().any(|value| compare_eq(actual, Some(value))))),
        _ => bail!("unsupported predicate operator {operator:?}"),
    }
}

pub(super) fn compare_eq(actual: &Value, expected: Option<&Value>) -> bool {
    let Some(expected) = expected else { return false; };
    match (number_value(actual), number_value(expected)) {
        (Some(a), Some(b)) => a == b,
        _ => actual == expected,
    }
}

fn number_value(value: &Value) -> Option<f64> {
    value.as_f64().or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn compare(found: Option<&Value>, expected: Option<&Value>, predicate: impl Fn(f64, f64) -> bool) -> bool {
    match (found.and_then(number_value), expected.and_then(number_value)) {
        (Some(a), Some(b)) => predicate(a, b),
        _ => false,
    }
}

fn resolve_path<'a>(path: &str, root: &'a Value) -> Option<&'a Value> {
    let trimmed = path.strip_prefix("$.").unwrap_or(path);
    if trimmed.is_empty() || trimmed == "facts" || trimmed.starts_with("facts.") { return None; }
    let resolved = trimmed.split('.').filter(|part| !part.is_empty()).try_fold(root, |value, key| {
        value.as_object()?.get(key)
    });
    resolved.filter(|value| !value.is_null())
}

fn interpolate_templates(value: &Value, event: &EventEnvelope, child_id: &str) -> Value {
    match value {
        Value::String(text) => expand_template(text, event, child_id),
        Value::Array(items) => Value::Array(items.iter().map(|item| interpolate_templates(item, event, child_id)).collect()),
        Value::Object(object) => {
            let mut result = serde_json::Map::new();
            for (key, item) in object { result.insert(key.clone(), interpolate_templates(item, event, child_id)); }
            Value::Object(result)
        }
        other => other.clone(),
    }
}

pub(super) fn expand_template(text: &str, event: &EventEnvelope, child_id: &str) -> Value {
    static PLACEHOLDER: std::sync::LazyLock<regex::Regex> = std::sync::LazyLock::new(|| regex::Regex::new(r"\{\{\s*([^{}]+?)\s*\}\}").expect("static template regex"));
    if !text.contains("{{") { return Value::String(text.to_owned()); }
    if let Some(captures) = PLACEHOLDER.captures(text) {
        if captures.get(0).is_some_and(|whole| whole.as_str() == text) {
            return template_lookup(captures.get(1).map(|m| m.as_str()).unwrap_or_default(), event, child_id).unwrap_or(Value::String(String::new()));
        }
    }
    let rendered = PLACEHOLDER.replace_all(text, |captures: &regex::Captures<'_>| {
        match template_lookup(captures.get(1).map(|m| m.as_str()).unwrap_or_default(), event, child_id) {
            Some(Value::String(value)) => value,
            Some(value) => value.to_string(),
            None => String::new(),
        }
    });
    Value::String(rendered.into_owned())
}

fn template_lookup(expression: &str, event: &EventEnvelope, child_id: &str) -> Option<Value> {
    match expression.trim() {
        "new_id" => return Some(Value::String(child_id.to_owned())),
        "now" => return Some(Value::String(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true))),
        _ => {},
    }
    let path = expression.trim();
    let path = if path.starts_with("$.") { path.to_owned() } else { format!("$.{path}") };
    resolve_path(&path, &event.data).cloned()
}

pub(super) fn build_effect(
    revision: &RuleRevision,
    event: &EventEnvelope,
    rule_id: &str,
    index: usize,
    action: &Action,
) -> Result<Effect> {
    let effect_id = effect_id(revision, event, rule_id, index);

    match (&action.emit, &action.emit_event, &action.command) {
        (Some(emit), None, None) => build_emit_effect(effect_id, event, emit),
        (None, Some(child), None) => build_child_event_effect(effect_id, event, rule_id, index, child),
        (None, None, Some(command)) => build_command_effect(effect_id, event, command),
        _ => bail!("action must contain exactly one supported action"),
    }
}

fn effect_id(revision: &RuleRevision, event: &EventEnvelope, rule_id: &str, index: usize) -> String {
    sha256_hex(format!(
        "{}|{}|{}|{}|{}|{}",
        event.tenant_id, event.id, revision.rule_id, revision.revision, rule_id, index
    ).as_bytes())
}

fn build_emit_effect(effect_id: String, event: &EventEnvelope, emit: &EmitAction) -> Result<Effect> {
    if emit.topic.trim().is_empty() {
        bail!("emit topic is required");
    }
    Ok(Effect {
        id: effect_id,
        tenant_id: event.tenant_id.clone(),
        destination: emit.topic.clone(),
        name: emit.topic.clone(),
        payload: emit.data.clone(),
        effect_type: "emit",
    })
}

fn build_child_event_effect(
    effect_id: String,
    event: &EventEnvelope,
    rule_id: &str,
    index: usize,
    action: &EmitEventAction,
) -> Result<Effect> {
    let child_id = sha256_hex(format!("child|{}|{}|{}", event.id, rule_id, index).as_bytes());
    let partition_key = resolve_child_partition_key(action, event)?;
    let payload = build_child_event_payload(action, event, &child_id, &partition_key);

    Ok(Effect {
        id: effect_id,
        tenant_id: event.tenant_id.clone(),
        destination: format!("events.{}", action.event_type),
        name: action.event_type.clone(),
        payload,
        effect_type: "emit_event",
    })
}

fn resolve_child_partition_key(action: &EmitEventAction, event: &EventEnvelope) -> Result<String> {
    let policy = if action.partition_key_policy.is_empty() {
        "explicit"
    } else {
        action.partition_key_policy.as_str()
    };
    let key = match policy {
        "explicit" => action.partition_key.clone(),
        "inherit" => event.partition_key.clone(),
        "from_data" => resolve_path(&action.partition_key_path, &event.data)
            .map(|value| value.as_str().map(str::to_owned).unwrap_or_else(|| value.to_string()))
            .unwrap_or_default(),
        _ => bail!("unsupported partition key policy {policy:?}"),
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
    json!({
        "id": child_id,
        "type": action.event_type,
        "tenant_id": event.tenant_id,
        "partition_key": partition_key,
        "occurred_at": Utc::now(),
        "data": interpolate_templates(&action.data, event, child_id),
        "headers": {
            "flowrulz_parent_event_id": event.id,
            "flowrulz_child_event_id": child_id
        }
    })
}

fn build_command_effect(effect_id: String, event: &EventEnvelope, command: &CommandAction) -> Result<Effect> {
    if command.destination.trim().is_empty() || command.name.trim().is_empty() {
        bail!("command destination and name are required");
    }
    Ok(Effect {
        id: effect_id,
        tenant_id: event.tenant_id.clone(),
        destination: command.destination.clone(),
        name: command.name.clone(),
        payload: command.data.clone(),
        effect_type: "command",
    })
}
