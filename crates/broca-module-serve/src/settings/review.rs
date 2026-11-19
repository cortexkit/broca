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
    reader!(["wal_recovery"], crate::config::WalRecoveryConfig),
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
/// `config.review` is pure, and the daemon checks the reply against the base it sent. A field
/// is a schema property with an apply kind and no such properties inside it, and it counts as
/// changed when its value or the layer it came from differs.
fn changes(schema: &Schema, base: Option<&ConfigView>, tuple: &ReviewedTuple) -> Vec<FieldChange> {
    let nothing = Value::Null;
    let no_sources = BTreeMap::new();
    let (old, old_sources) = match base {
        Some(view) => (&view.effective, &view.sources),
        None => (&nothing, &no_sources),
    };
    let recorded = |value: &Value| schema.record(value).unwrap_or(Value::Null);
    let (old, new) = (recorded(old), recorded(&tuple.effective));
    let mut fields = Vec::new();
    for (path, apply) in leaf_fields(schema.normalized(), String::new()) {
        let old_value = old.pointer(&path).cloned().unwrap_or(Value::Null);
        let new_value = new.pointer(&path).cloned().unwrap_or(Value::Null);
        let old_source = old_sources.get(&path).copied().unwrap_or(Layer::Default);
        let new_source = tuple.sources.get(&path).copied().unwrap_or(Layer::Default);
        if old_value == new_value && old_source == new_source {
            continue;
        }
        let mut field = FieldChange::new(
            "broca",
            path,
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
