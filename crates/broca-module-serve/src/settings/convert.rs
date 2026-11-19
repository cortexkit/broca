//! Turns a `broca.jsonc` written in the shapes broca has accepted until now into the shapes of
//! broca's settings schema (`schema.json`).
//!
//! Pure: a document in, a document out, plus a note for each key it removed or rewrote. A
//! document already in the schema's shapes comes back unchanged with no notes, so running it
//! twice changes nothing. A value it can't map refuses by key, and the document is left as it
//! was.

use std::collections::BTreeMap;

use serde_json::{json, Map, Value};

use crate::auth_routing::AuthMethod;

/// A converted document and what the conversion did to it.
#[derive(Debug, Clone, PartialEq)]
pub struct Converted {
    pub document: Map<String, Value>,
    /// One line per key removed or rewritten, naming the key.
    pub notes: Vec<String>,
}

/// Keys no setting reads any more. Each is removed, with the reason as its note.
const REMOVED: &[(&str, &str)] = &[
    (
        "media",
        "media input follows the frozen model's capabilities",
    ),
    (
        "wal_archive_idle_retention_days",
        "sessions keep no logs to archive; the new storage seals history itself",
    ),
    (
        "session_conversion",
        "every session is in the new storage, so nothing is left to convert",
    ),
    ("vault", "broca finds the vault through the daemon"),
    ("account_limit_pause", "max_same_account_wait replaced it"),
    (
        "refusal_fallback",
        "anthropic_fallbacks.refusal_resend replaced it",
    ),
    (
        "anthropic_refusal_fallback",
        "anthropic_fallbacks.refusal_resend replaced it",
    ),
    (
        "anthropic_server_side_fallback",
        "anthropic_fallbacks.server_side replaced it",
    ),
    (
        "charge_basis_mapping_version",
        "the charge basis version is always the shipped one",
    ),
];

/// `document` in the schema's shapes.
pub fn convert(document: &Map<String, Value>) -> Result<Converted, String> {
    let mut document = document.clone();
    let mut notes = Vec::new();
    for (key, why) in REMOVED {
        if document.remove(*key).is_some() {
            notes.push(format!("removed {key}: {why}"));
        }
    }
    if let Some(value) = document.get_mut("max_same_account_wait") {
        if let Some(wait) = same_account_wait(value)? {
            *value = wait;
            notes.push("rewrote max_same_account_wait in its tagged form".into());
        }
    }
    if let Some(routes) = document.get_mut("tool_name_aliases") {
        for (route, aliases) in object_mut(routes, "tool_name_aliases")? {
            if let Some(tagged) = alias_route(aliases, route)? {
                *aliases = tagged;
                notes.push(format!(
                    "rewrote tool_name_aliases.{route} in its tagged form"
                ));
            }
        }
    }
    if let Some(rows) = document
        .get_mut("cache")
        .and_then(|cache| cache.get_mut("provider_ttl"))
    {
        for (provider, row) in object_mut(rows, "cache.provider_ttl")? {
            if let Some(tagged) = cache_row(row, provider)? {
                *row = tagged;
                notes.push(format!(
                    "rewrote cache.provider_ttl.{provider} in its tagged form"
                ));
            }
        }
    }
    charge_basis(&mut document, &mut notes)?;
    context_management(&mut document, &mut notes)?;
    Ok(Converted { document, notes })
}

fn object_mut<'a>(value: &'a mut Value, key: &str) -> Result<&'a mut Map<String, Value>, String> {
    value
        .as_object_mut()
        .ok_or_else(|| format!("{key} must be an object"))
}

/// Whether `value` is already in a tagged form.
fn tagged(value: &Value) -> bool {
    value.get("kind").is_some()
}

/// The tagged form of a `max_same_account_wait` written as minutes or `"max"`; `None` when
/// it is tagged already.
fn same_account_wait(value: &Value) -> Result<Option<Value>, String> {
    match value {
        value if tagged(value) => Ok(None),
        Value::String(max) if max == "max" => Ok(Some(json!({"kind": "max"}))),
        value => match value.as_u64() {
            Some(minutes) => Ok(Some(json!({"kind": "minutes", "minutes": minutes}))),
            None => Err(format!(
                "max_same_account_wait must be a number of minutes or \"max\", got {value}"
            )),
        },
    }
}

/// The tagged form of one route's tool name aliases.
fn alias_route(value: &Value, route: &str) -> Result<Option<Value>, String> {
    if tagged(value) {
        return Ok(None);
    }
    let object = value
        .as_object()
        .filter(|object| object.len() == 1)
        .ok_or_else(|| {
            format!("tool_name_aliases.{route} must be {{\"names\": …}} or {{\"prefix_all\": …}}")
        })?;
    match (object.get("names"), object.get("prefix_all")) {
        (Some(names), None) => Ok(Some(json!({"kind": "names", "names": names}))),
        (None, Some(prefix)) => Ok(Some(json!({"kind": "prefix_all", "prefix": prefix}))),
        _ => Err(format!(
            "tool_name_aliases.{route} must be {{\"names\": …}} or {{\"prefix_all\": …}}"
        )),
    }
}

/// The tagged form of one provider's cache row: marker lifetimes, or a retention hint (an
/// empty one when the row named none).
fn cache_row(value: &Value, provider: &str) -> Result<Option<Value>, String> {
    if tagged(value) {
        return Ok(None);
    }
    let shape = || {
        format!(
            "cache.provider_ttl.{provider} must be either {{primary, ephemeral}} or {{retention}}"
        )
    };
    let object = value.as_object().ok_or_else(shape)?;
    if object
        .keys()
        .any(|key| !matches!(key.as_str(), "primary" | "ephemeral" | "retention"))
    {
        return Err(shape());
    }
    match (
        object.get("primary"),
        object.get("ephemeral"),
        object.get("retention"),
    ) {
        (Some(primary), Some(ephemeral), None) => Ok(Some(
            json!({"kind": "markers", "primary": primary, "ephemeral": ephemeral}),
        )),
        (None, None, retention) => Ok(Some(json!({
            "kind": "retention",
            "retention": retention.cloned().unwrap_or_else(|| json!("")),
        }))),
        _ => Err(shape()),
    }
}

/// Every charge basis row, however it was spelled, as `billing.charge_basis.<provider>.<method>`.
///
/// Rows were accepted under `billing.charge_basis` (or its alias `charge_basis_mapping`) and
/// at the root as `charge_basis`, each keyed by provider (holding a method map) or by
/// `<method>:<provider>` or `<provider>:<method>`. They apply as broca applied them: in key
/// order, the root's over `billing`'s for one key, and a later row over an earlier one for
/// the same provider and method.
fn charge_basis(document: &mut Map<String, Value>, notes: &mut Vec<String>) -> Result<(), String> {
    let mut billing = match document.remove("billing") {
        None => Map::new(),
        Some(Value::Object(billing)) => billing,
        Some(_) => return Err("billing must be an object".into()),
    };
    for alias in [
        "version",
        "charge_basis_mapping_version",
        "charge_basis_version",
    ] {
        if billing.remove(alias).is_some() {
            notes.push(format!(
                "removed billing.{alias}: the charge basis version is always the shipped one"
            ));
        }
    }
    let mut keyed: BTreeMap<String, Value> = BTreeMap::new();
    let mut respelled = false;
    for key in ["charge_basis", "charge_basis_mapping"] {
        if let Some(rows) = billing.remove(key) {
            respelled |= key != "charge_basis";
            keyed.extend(object_rows(rows, &format!("billing.{key}"))?);
        }
    }
    if let Some(rows) = document.remove("charge_basis") {
        respelled = true;
        keyed.extend(object_rows(rows, "charge_basis")?);
    }
    if let Some(key) = billing.keys().next() {
        return Err(format!("billing.{key} is not a billing setting"));
    }
    let mut rows: Map<String, Value> = Map::new();
    for (key, value) in keyed {
        let provider_rows = match value {
            Value::Object(methods) => {
                let mut provider_rows = Vec::new();
                for (method, basis) in methods {
                    let token = method_token(&method, &key)?;
                    respelled |= token != method;
                    provider_rows.push((key.clone(), token, basis));
                }
                provider_rows
            }
            basis => {
                respelled = true;
                let (provider, method) = split_pair(&key).ok_or_else(|| {
                    format!(
                        "charge-basis key '{key}' must be '<method>:<provider>' or \
                         '<provider>:<method>'"
                    )
                })?;
                vec![(provider, method, basis)]
            }
        };
        for (provider, method, basis) in provider_rows {
            rows.entry(provider)
                .or_insert_with(|| Value::Object(Map::new()))
                .as_object_mut()
                .expect("a provider's rows are an object")
                .insert(method, basis);
        }
    }
    if !rows.is_empty() {
        document.insert("billing".into(), json!({"charge_basis": rows}));
        if respelled {
            notes.push(
                "rewrote the charge basis rows as billing.charge_basis.<provider>.<method>".into(),
            );
        }
    } else if respelled {
        notes.push("removed empty charge basis rows".into());
    }
    Ok(())
}

fn object_rows(value: Value, key: &str) -> Result<Map<String, Value>, String> {
    match value {
        Value::Object(rows) => Ok(rows),
        _ => Err(format!("{key} must be an object")),
    }
}

/// `method` as its canonical sign-in method token.
fn method_token(method: &str, row: &str) -> Result<String, String> {
    AuthMethod::from_token(method)
        .map(|method| method.as_token().to_owned())
        .ok_or_else(|| format!("unknown auth method '{method}' in charge-basis row '{row}'"))
}

/// `(provider, method)` from a `<method>:<provider>` or `<provider>:<method>` key.
fn split_pair(key: &str) -> Option<(String, String)> {
    let (first, second) = key.split_once(':')?;
    if first.is_empty() || second.is_empty() {
        return None;
    }
    match (
        AuthMethod::from_token(first),
        AuthMethod::from_token(second),
    ) {
        (Some(method), _) => Some((second.to_owned(), method.as_token().to_owned())),
        (None, Some(method)) => Some((first.to_owned(), method.as_token().to_owned())),
        (None, None) => None,
    }
}

/// `context_management`'s default and per-project values in their tagged forms.
fn context_management(
    document: &mut Map<String, Value>,
    notes: &mut Vec<String>,
) -> Result<(), String> {
    let Some(section) = document.get_mut("context_management") else {
        return Ok(());
    };
    let section = object_mut(section, "context_management")?;
    if let Some(value) = section.get_mut("default") {
        if let Some(converted) = context_value(value, "context_management.default")? {
            *value = converted;
            notes.push("rewrote context_management.default in its tagged form".into());
        }
    }
    if let Some(projects) = section.get_mut("projects") {
        for (root, value) in object_mut(projects, "context_management.projects")? {
            let key = format!("context_management.projects[{root:?}]");
            if let Some(converted) = context_value(value, &key)? {
                *value = converted;
                notes.push(format!("rewrote {key} in its tagged form"));
            }
        }
    }
    Ok(())
}

fn context_value(value: &Value, key: &str) -> Result<Option<Value>, String> {
    match value {
        value if tagged(value) => Ok(None),
        Value::String(mode) if mode == "none" || mode == "compaction" => {
            Ok(Some(json!({"kind": mode})))
        }
        Value::Object(object) if object.len() == 1 && object.contains_key("transform") => Ok(Some(
            json!({"kind": "transform", "provider": object["transform"]}),
        )),
        value => Err(format!(
            "{key} must be \"none\", \"compaction\" or {{\"transform\": …}}, got {value}"
        )),
    }
}

#[cfg(test)]
#[path = "convert_tests.rs"]
mod tests;
