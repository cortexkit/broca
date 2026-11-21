//! broca's review of a proposed settings view, before the daemon approves it.
//!
//! The schema's own rules run first ([`subc_config::Schema::review_tuple`]). Then every reader
//! that builds part of broca at start reads the effective document, so a view that would stop
//! broca at its next start is refused now, naming the key it fails at. A refusal carries the
//! key, never the value.

use std::collections::BTreeMap;

use serde_json::Value;
use subc_config::{
    ApplyKind, FieldChange, Layer, LayerValue, RecordedValue, Refusal, RefusalCode, ReviewedTuple,
    Schema, ValidationContext,
};
use subc_protocol::config::{
    ConfigReviewRefusal, ConfigReviewReply, ConfigReviewRequest, ConfigView,
};

use super::Settings;

/// A reader that builds part of broca from the settings, and the top-level keys it reads.
struct Reader {
    keys: &'static [&'static str],
    check: fn(&Settings) -> Result<(), String>,
}

macro_rules! reader {
    ([$($key:literal),+], $reader:ty) => {
        Reader {
            keys: &[$($key),+],
            check: |settings| <$reader>::from_settings(settings).map(drop),
        }
    };
}

/// Every reader built at start. `chatgpt_force_https` has no reader here: it is a plain
/// boolean, and the schema already checks that.
const READERS: &[Reader] = &[
    reader!(
        ["account_selection"],
        crate::account_routing::AccountSelectionConfig
    ),
    reader!(
        ["anthropic_fallbacks"],
        crate::config::AnthropicFallbackConfig
    ),
    reader!(["backup_classes"], crate::session_class::BackupClasses),
    reader!(["billing"], crate::billing_routing::ChargeBasisTable),
    reader!(["bus_run_events_enabled"], crate::bus_events::Config),
    reader!(["cache"], crate::CacheTtlTable),
    reader!(
        ["classifier_routing"],
        crate::config::ClassifierRoutingConfig
    ),
    reader!(
        ["compaction_signals"],
        crate::config::CompactionSignalsConfig
    ),
    reader!(
        ["context_management"],
        crate::context_management::ContextManagementConfig
    ),
    reader!(["import"], crate::config::ImportConfig),
    reader!(
        ["llm_tools", "private_content_providers"],
        crate::config::LlmToolsConfig
    ),
    reader!(
        ["max_same_account_wait"],
        crate::config::AccountLimitPauseConfig
    ),
    reader!(["providers"], crate::provider_overrides::ProviderOverrides),
    reader!(["service_tier"], crate::config::ServiceTierConfig),
    reader!(["session_storage"], crate::config::SessionStorageConfig),
    reader!(["storage_sync"], crate::config::StorageSyncConfig),
    reader!(["tool_name_aliases"], crate::config::ToolNameAliasesConfig),
];

/// Keys the schema alone checks. Only the coverage test reads this list.
#[cfg(test)]
const SCHEMA_ONLY: &[&str] = &["chatgpt_force_https"];

/// Review the user layer `user`: the effective view and where each value came from, or the
/// first refusal.
pub fn review(schema: &Schema, user: &Value) -> Result<ReviewedTuple, Refusal> {
    let context = ValidationContext::new("broca", None);
    let tuple = schema.review_tuple(&[LayerValue::new(Layer::User, user.clone())], &context)?;
    let settings = Settings::from_view(&tuple.effective, "managed settings");
    match refused_key(&settings, user) {
        Some(path) => Err(Refusal::new(RefusalCode::ValueInvalid, path, None)),
        None => Ok(tuple),
    }
}

/// The pointer to the first top-level key broca would fail to build from in `settings`, or
/// `None` when every reader accepts it. A reader that reads several keys is named by the first
/// of them `user` sets.
pub(crate) fn refused_key(settings: &Settings, user: &Value) -> Option<String> {
    READERS.iter().find_map(|reader| {
        let error = (reader.check)(settings).err()?;
        // The reader's message can quote the value, so it stays in broca's own log.
        tracing::warn!(target: "config", keys = ?reader.keys, %error, "settings view refused");
        let key = reader
            .keys
            .iter()
            .find(|key| user.get(**key).is_some())
            .unwrap_or(&reader.keys[0]);
        Some(format!("/{key}"))
    })
}

/// Every field of `schema`, with how a change to it is applied.
pub(crate) fn fields(schema: &Schema) -> Vec<(String, ApplyKind)> {
    leaf_fields(schema.normalized(), String::new())
}

/// broca's answer to the daemon's `config.review`: the fields the proposed user layer changes
/// from the request's base view, or why broca refuses it. broca's settings have only a user
/// layer, so a snapshot of any other layer that carries bytes is refused.
pub fn review_request(schema: &Schema, request: &ConfigReviewRequest) -> ConfigReviewReply {
    let reviewed = proposed_user_layer(request).and_then(|user| {
        review(schema, &user).map_err(|refusal| {
            let code = serde_json::to_value(refusal.code)
                .ok()
                .and_then(|code| code.as_str().map(str::to_owned))
                .unwrap_or_else(|| "value_invalid".into());
            ConfigReviewRefusal::new(code, vec![refusal.path], Vec::new())
        })
    });
    match reviewed {
        Ok(tuple) => {
            ConfigReviewReply::new(changes(schema, request.base.as_ref(), &tuple), Vec::new())
        }
        Err(refusal) => ConfigReviewReply::new(Vec::new(), vec![refusal]),
    }
}

/// The proposed user file as JSON: its bytes as broca reads `broca.jsonc` (comments and
/// trailing commas allowed), or the empty document when the file is absent.
fn proposed_user_layer(request: &ConfigReviewRequest) -> Result<Value, ConfigReviewRefusal> {
    let mut user = Value::Object(Default::default());
    for snapshot in &request.layers {
        let Some(bytes) = &snapshot.bytes else {
            continue;
        };
        if snapshot.layer.known_layer() != Some(Layer::User) {
            return Err(ConfigReviewRefusal::new(
                "layer_forbidden",
                vec![String::new()],
                vec![format!("broca has no {} settings", snapshot.layer.as_str())],
            ));
        }
        // Parsed as written, never converted from an older shape: the daemon checks the same
        // bytes against the schema, so a shape the schema refuses must be refused here too.
        let parsed = std::str::from_utf8(bytes)
            .ok()
            .and_then(|text| {
                let text = crate::cache_routing::strip_trailing_commas(
                    &crate::cache_routing::strip_jsonc_comments(text),
                );
                serde_json::from_str::<Value>(&text).ok()
            })
            .ok_or_else(|| {
                ConfigReviewRefusal::new(
                    "value_invalid",
                    vec![String::new()],
                    vec!["the user file is not readable JSON".into()],
                )
            })?;
        user = parsed;
    }
    Ok(user)
}

/// The fields `tuple` changes from `base`, the last approved view the daemon supplies. With no
/// base nothing was approved before, so every field's old value is null from the default
/// layer. Old values come only from `base`, never from this build's own defaults:
/// `config.review` is pure, and the daemon checks the reply against the base it sent.
///
/// A field is a leaf of either view's settings ([`leaves`]). The daemon checks a reply against
/// exactly these paths, so a field is never an object above other fields. One counts as changed
/// when its value or the layer it came from differs; with no base, every leaf of the proposal
/// does. A leaf only the base has is reported with a null new value.
fn changes(schema: &Schema, base: Option<&ConfigView>, tuple: &ReviewedTuple) -> Vec<FieldChange> {
    let nothing = Value::Null;
    let no_sources = BTreeMap::new();
    let (old, old_sources) = match base {
        Some(view) => (&view.effective, &view.sources),
        None => (&nothing, &no_sources),
    };
    let recorded = |value: &Value| schema.record(value).unwrap_or(Value::Null);
    let (old, new) = (recorded(old), recorded(&tuple.effective));
    let mut paths = std::collections::BTreeSet::new();
    if base.is_some() {
        leaves(schema.normalized(), &old, String::new(), &mut paths);
    }
    leaves(schema.normalized(), &new, String::new(), &mut paths);
    let mut fields = Vec::new();
    for path in &paths {
        let old_value = old.pointer(path).cloned().unwrap_or(Value::Null);
        let new_value = new.pointer(path).cloned().unwrap_or(Value::Null);
        let old_source = source(old_sources, path);
        let new_source = source(&tuple.sources, path);
        // With no base nothing was approved before, so every leaf of the proposal is new.
        if base.is_some() && old_value == new_value && old_source == new_source {
            continue;
        }
        // The branch of a tagged choice follows the value that names it: the new one, or the
        // old one for a field the new value no longer has.
        let chosen = if new.pointer(path).is_some() {
            &new
        } else {
            &old
        };
        let Some(apply) = apply_at(schema.normalized(), chosen, path) else {
            continue;
        };
        let mut field = FieldChange::new(
            "broca",
            path.clone(),
            RecordedValue::new(old_value),
            RecordedValue::new(new_value),
            apply,
        );
        field.old_source = old_source;
        field.new_source = new_source;
        fields.push(field);
    }
    fields
}

/// The layer `path` came from: its own entry in `sources`, or that of its nearest ancestor with
/// one, as the daemon reads a view's sources; the default layer when none has one.
fn source(sources: &BTreeMap<String, Layer>, path: &str) -> Layer {
    let mut path = path;
    loop {
        if let Some(layer) = sources.get(path) {
            return *layer;
        }
        match path.rfind('/') {
            Some(cut) => path = &path[..cut],
            None => return Layer::Default,
        }
    }
}

/// The leaves of `value` under `node`, as the daemon walks a view to check a review: every
/// value that isn't an object (an array is one leaf, whole), a vault reference or an opaque
/// `json` value whole, and the members of the `oneOf` branch the value's discriminator selects.
/// An empty object is a leaf of its own, since nothing below it can carry the change.
fn leaves(node: &Value, value: &Value, path: String, out: &mut std::collections::BTreeSet<String>) {
    if node.get("x-ck-kind").and_then(Value::as_str) == Some("json") {
        out.insert(path);
        return;
    }
    let node = variant(node, value).unwrap_or(node);
    match value.as_object() {
        Some(object) if !object.is_empty() && !subc_config::is_vault_reference(value) => {
            for (key, member) in object {
                let child = child_schema(node, key).unwrap_or(&Value::Null);
                let escaped = key.replace('~', "~0").replace('/', "~1");
                leaves(child, member, format!("{path}/{escaped}"), out);
            }
        }
        _ => {
            out.insert(path);
        }
    }
}

/// The schema of `node`'s member `key`: a named property, else the first pattern property
/// whose pattern matches it.
fn child_schema<'a>(node: &'a Value, key: &str) -> Option<&'a Value> {
    node.get("properties")
        .and_then(|properties| properties.get(key))
        .or_else(|| {
            node.get("patternProperties")?
                .as_object()?
                .iter()
                .find(|(pattern, _)| {
                    regex::Regex::new(pattern).is_ok_and(|pattern| pattern.is_match(key))
                })
                .map(|(_, child)| child)
        })
}

/// How a change at `path` in `value` is applied: the `x-ck-apply` of the deepest schema node on
/// the way to it. The way is walked as `Schema::review_tuple` walks values: named properties,
/// then pattern properties, inside the `oneOf` branch whose discriminator the value names.
fn apply_at(schema: &Value, value: &Value, path: &str) -> Option<ApplyKind> {
    let apply = |node: &Value| {
        node.get("x-ck-apply")
            .and_then(|apply| serde_json::from_value(apply.clone()).ok())
    };
    let mut node = schema;
    let mut here = Some(value);
    let mut found = apply(node);
    for segment in path.split('/').skip(1) {
        let key = segment.replace("~1", "/").replace("~0", "~");
        if let Some(branch) = here.and_then(|value| variant(node, value)) {
            node = branch;
            found = apply(node).or(found);
        }
        // Below a key the schema doesn't name, the value's own leaves keep the apply kind found
        // above them.
        let Some(child) = child_schema(node, &key) else {
            break;
        };
        node = child;
        here = here.and_then(|value| value.get(&key));
        found = apply(node).or(found);
    }
    found
}

/// The `oneOf` branch of `node` that `value` selects through the branches' discriminator: the
/// one property every branch requires and fixes to a `const` of its own, as subc-config picks it.
fn variant<'a>(node: &'a Value, value: &Value) -> Option<&'a Value> {
    if node.get("x-ck-kind").and_then(Value::as_str) == Some("json") {
        return None;
    }
    let branches = node.get("oneOf")?.as_array()?;
    let first = branches.first()?.get("properties")?.as_object()?;
    let mut candidates = first.iter().filter(|(key, property)| {
        let required = |branch: &Value| {
            branch
                .get("required")
                .and_then(Value::as_array)
                .is_some_and(|required| required.iter().any(|name| name == key.as_str()))
        };
        let constants: Vec<&Value> = branches
            .iter()
            .filter_map(|branch| branch.get("properties")?.get(key.as_str())?.get("const"))
            .collect();
        property.get("const").is_some()
            && branches.iter().all(required)
            && constants.len() == branches.len()
            && constants
                .iter()
                .enumerate()
                .all(|(i, constant)| !constants[..i].contains(constant))
    });
    let (key, _) = candidates.next()?;
    if candidates.next().is_some() {
        return None;
    }
    branches.iter().find(|branch| {
        value
            .get(key)
            .is_some_and(|v| branch["properties"][key]["const"] == *v)
    })
}

/// Every field under `node`: each property with an apply kind and no such property inside it.
fn leaf_fields(node: &Value, path: String) -> Vec<(String, ApplyKind)> {
    let children: Vec<(String, ApplyKind)> = node
        .get("properties")
        .and_then(Value::as_object)
        .into_iter()
        .flatten()
        .flat_map(|(key, child)| leaf_fields(child, format!("{path}/{key}")))
        .collect();
    if !children.is_empty() {
        return children;
    }
    node.get("x-ck-apply")
        .and_then(|apply| serde_json::from_value(apply.clone()).ok())
        .map(|apply| vec![(path, apply)])
        .unwrap_or_default()
}

#[cfg(test)]
#[path = "review_tests.rs"]
mod tests;
