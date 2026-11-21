//! Billing record form rebuilt from session-store records.
//!
//! The usage export is built by the billing builder (`broca_core::billing`) from
//! `broca_wal::WalRecord` records. A run kept in the session store is billed by rebuilding
//! those records from its session-store records and calling the same builder.
//!
//! Each function here rebuilds one kind of record as JSON. The conversion of old sessions used
//! these same functions, and checked every converted log against them.
//!
//! The JSON leaves out members whose values are null or absent, as the forward conversion
//! does. A run start's `session` is left out too: the session store keeps the session's
//! identity once, in its head, and a caller that needs the whole record adds it.

use broca_session_store::codec::Digest;
use broca_session_store::record::{
    Assistant, AuthRefrozen, ConfigRefrozen, Input, PartKind, PartRef, Pause, RunEnd, RunStart,
    StepEnd, StepRefused, Warmed,
};
use serde::Serialize;
use serde_json::{Map, Value};

pub fn json(value: impl Serialize) -> Value {
    serde_json::to_value(value).expect("record values serialize")
}

/// Sets `key` to `value` when there is one.
pub fn set<T: Serialize>(object: &mut Map<String, Value>, key: &str, value: Option<T>) {
    if let Some(value) = value {
        object.insert(key.into(), json(value));
    }
}

/// An owned payload's members, as the object a record was rebuilt on.
pub fn members(payload: Option<&Value>) -> Result<Map<String, Value>, String> {
    match payload {
        None => Ok(Map::new()),
        Some(Value::Object(object)) => Ok(object.clone()),
        Some(_) => Err("a carried payload is not an object".into()),
    }
}

/// [`members`] of a payload a record holds as JSON text.
pub fn text_members(
    payload: Option<&broca_protocol::json::Json>,
) -> Result<Map<String, Value>, String> {
    members(payload.map(broca_protocol::json::Json::parse).as_ref())
}

/// A digest's text form: 64 lowercase hex characters.
pub fn hex(digest: &Digest) -> String {
    digest.0.iter().map(|b| format!("{b:02x}")).collect()
}

/// Puts a plan manifest's setup part back into the manifest object.
pub fn fill_manifest(manifest: &mut Map<String, Value>, kind: PartKind, value: Value) {
    match kind {
        PartKind::Plan => {
            manifest.insert("plan".into(), value);
        }
        PartKind::ProviderAnswer => {
            if let Some(Value::Array(fetches)) = manifest.get_mut("fetches") {
                fetches.push(value);
            } else {
                manifest.insert("fetches".into(), Value::Array(vec![value]));
            }
        }
        PartKind::Tools => {
            manifest.insert("tools".into(), value);
        }
        PartKind::SystemText => {
            manifest.insert("system_text".into(), value);
        }
        PartKind::Declarations => {
            manifest.insert("declarations".into(), value);
        }
        PartKind::ResponseFormat | PartKind::GlobalSystem | PartKind::RenderConfig => {}
    }
}

/// A manifest always lists its provider answers, even when there are none.
pub fn finish_manifest(mut manifest: Map<String, Value>) -> Value {
    manifest
        .entry("fetches")
        .or_insert_with(|| Value::Array(Vec::new()));
    Value::Object(manifest)
}

/// A record's v1 tag list, one entry per message: absent when no message has tags.
pub fn tags(entries: impl Iterator<Item = Option<Value>>) -> Option<Value> {
    let entries: Vec<Option<Value>> = entries.collect();
    entries.iter().any(Option::is_some).then(|| {
        Value::Array(
            entries
                .into_iter()
                .map(|t| t.unwrap_or(Value::Null))
                .collect(),
        )
    })
}

/// Builds `run_started` from a run's start, the input entries that follow it, and the setup
/// parts it names, which `part` reads.
pub fn run_started(
    start: &RunStart,
    inputs: &[&Input],
    part: impl FnMut(&PartRef) -> Result<Value, String>,
) -> Result<Value, String> {
    let choices = members(start.run_choices.as_ref())?;
    let mut object = members(choices.get("record"))?;
    let mut manifest = choices
        .get("manifest")
        .map(|m| members(Some(m)))
        .transpose()?;

    object.insert("type".into(), json("run_started"));
    object.insert("run_id".into(), json(&start.run_id));
    set(&mut object, "session_class", start.session_class.as_ref());
    set(&mut object, "submission_id", start.submission_id.as_ref());
    set(&mut object, "send_id", start.send_id.as_ref());
    set(
        &mut object,
        "request_identity_digest",
        start.request_identity_digest.as_ref(),
    );
    set(&mut object, "ts_ms", start.ts_ms);
    set(&mut object, "written_at_ms", start.written_at_ms);
    set(&mut object, "retention", start.retention_secs);
    set(&mut object, "title", start.title.as_ref());
    set(
        &mut object,
        "compaction_plan_version",
        start.compaction_plan_version,
    );
    set(&mut object, "origin", start.origin.as_ref());
    set(&mut object, "principal", start.principal.as_ref());
    set(&mut object, "scope", start.scope.as_ref());
    set(&mut object, "hooks", start.hooks.as_ref());
    set(&mut object, "keep_warm", start.keep_warm.as_ref());
    set(&mut object, "on_restart", start.on_restart.as_ref());
    set(&mut object, "message_marks", start.message_marks.as_ref());

    // The config's members and setup parts are read as the serve executor reads them.
    let (config, manifest_parts) =
        crate::run::run_config_members(start, crate::run::SetupParts::All, part, || {
            "a carried payload is not an object".to_string()
        })?;
    for (kind, value) in manifest_parts {
        fill_manifest(manifest.get_or_insert_with(Map::new), kind, value);
    }
    object.insert("config".into(), Value::Object(config));
    if let Some(manifest) = manifest {
        object.insert("manifest".into(), finish_manifest(manifest));
    }

    object.insert(
        "input".into(),
        Value::Array(inputs.iter().map(|i| json(&i.message)).collect()),
    );
    set(
        &mut object,
        "tags",
        tags(inputs.iter().map(|i| i.tags_v1.clone())),
    );
    Ok(Value::Object(object))
}

/// Builds the `model_step_finished` event from a step's end and its assistant message.
pub fn model_step_finished(assistant: &Assistant, end: &StepEnd) -> Result<Value, String> {
    let mut object = text_members(end.fallback.as_ref())?;
    object.insert("type".into(), json("model_step_finished"));
    object.insert("step_id".into(), json(end.step_id));
    let mut message = Map::new();
    message.insert("message_id".into(), json(&assistant.provider_message_id));
    message.insert("content".into(), json(&assistant.message.content));
    object.insert("assistant_message".into(), Value::Object(message));
    set(&mut object, "usage", assistant.usage.as_ref());
    set(&mut object, "written_at_ms", assistant.written_at_ms);
    set(&mut object, "tags", assistant.tags_v1.as_ref());
    set(&mut object, "finish_reason", end.finish_reason.as_ref());
    object.insert("retries_used".into(), json(end.retries_used));
    set(
        &mut object,
        "service_tier_used",
        end.service_tier_used.as_ref(),
    );
    set(&mut object, "speed_used", end.speed_used.as_ref());
    set(&mut object, "hooks", end.hooks.as_ref());
    Ok(Value::Object(object))
}

/// Builds the `run_finished` event.
pub fn run_finished(end: &RunEnd) -> Value {
    let mut object = Map::new();
    object.insert("type".into(), json("run_finished"));
    object.insert("reason".into(), json(end.reason));
    set(&mut object, "usage", end.usage.as_ref());
    set(&mut object, "ts_ms", end.ts_ms);
    object.insert("retries_used".into(), json(end.retries_used));
    set(
        &mut object,
        "interrupt_reason",
        end.interrupt_reason.as_ref(),
    );
    set(&mut object, "error", end.error.as_ref());
    Value::Object(object)
}

/// Builds the `run_paused` event.
pub fn run_paused(pause: &Pause) -> Value {
    let mut object = Map::new();
    object.insert("type".into(), json("run_paused"));
    object.insert("run_id".into(), json(&pause.run_id));
    object.insert("reason".into(), json(pause.reason));
    set(&mut object, "ts_ms", pause.ts_ms);
    set(&mut object, "error", pause.error.as_ref());
    set(&mut object, "account_limit", pause.account_limit.as_ref());
    Value::Object(object)
}

/// Builds the `model_step_refused` event.
pub fn model_step_refused(refused: &StepRefused) -> Value {
    let mut object = Map::new();
    object.insert("type".into(), json("model_step_refused"));
    object.insert("step_id".into(), json(refused.step_id));
    object.insert("usage".into(), json(refused.usage));
    set(&mut object, "served_model", refused.served_model.as_ref());
    set(
        &mut object,
        "usage_iterations",
        refused.usage_iterations.as_ref(),
    );
    object.insert("discarded_blocks".into(), json(refused.discarded_blocks));
    object.insert("schedule".into(), refused.schedule.clone());
    set(&mut object, "ts_ms", refused.ts_ms);
    Value::Object(object)
}

/// Builds the `run_config_refrozen` event.
pub fn run_config_refrozen(refrozen: &ConfigRefrozen) -> Value {
    let mut object = Map::new();
    object.insert("type".into(), json("run_config_refrozen"));
    set(&mut object, "ts_ms", refrozen.ts_ms);
    object.insert("renderer_version".into(), json(refrozen.renderer_version));
    object.insert(
        "frozen_render_config".into(),
        refrozen.render_config.clone(),
    );
    Value::Object(object)
}

/// Builds the `run_auth_refrozen` event.
pub fn run_auth_refrozen(refrozen: &AuthRefrozen) -> Value {
    let mut object = Map::new();
    object.insert("type".into(), json("run_auth_refrozen"));
    set(&mut object, "ts_ms", refrozen.ts_ms);
    object.insert("auth_selection".into(), refrozen.auth_selection.clone());
    Value::Object(object)
}

/// Builds the `cache_warmed` event.
pub fn cache_warmed(warmed: &Warmed) -> Result<Value, String> {
    let mut object = text_members(warmed.billing.as_ref())?;
    object.insert("type".into(), json("cache_warmed"));
    set(&mut object, "attempt_id", warmed.attempt_id.as_ref());
    set(&mut object, "trigger", warmed.trigger.as_ref());
    object.insert("run_id".into(), json(&warmed.run_id));
    object.insert("step_id".into(), json(warmed.step_id));
    object.insert(
        "request_body_sha256".into(),
        json(hex(&warmed.request_body_sha256)),
    );
    object.insert(
        "warm_body_sha256".into(),
        json(hex(&warmed.warm_body_sha256)),
    );
    object.insert("usage".into(), json(warmed.usage));
    set(
        &mut object,
        "served_account_ref",
        warmed.served_account_ref.as_ref(),
    );
    set(
        &mut object,
        "ratelimit_headers",
        warmed.ratelimit_headers.as_ref(),
    );
    object.insert("sent_at_ms".into(), json(warmed.sent_at_ms));
    set(&mut object, "ttl_ms", warmed.ttl_ms);
    set(&mut object, "expires_at_ms", warmed.expires_at_ms);
    Ok(Value::Object(object))
}
