//! Structured output on the wire: which routes can enforce a schema, which schemas each route
//! can enforce (a named subset profile per family), and how each family encodes the schema.
//!
//! [`decide`] runs once, at admission, and its answer is frozen into the run's render config
//! ([`FrozenStructuredOutput`]). Every request of the run is then rendered from that frozen
//! answer by [`prepare`] (before a family shapes its body) and [`finish`] (after), so a resumed
//! or retried request renders byte-identically, and a run without structured output renders
//! exactly as before: both are no-ops when the frozen config carries none.
//!
//! The encodings and profiles follow broca's per-family probes (October 2026). The ChatGPT/Codex
//! backend (`gpt-6-luna`) and Anthropic Messages (Claude Sonnet 5.5 and Opus 5.5) were measured
//! with live requests; "measured" below refers to those requests. Every other family rests on
//! its provider's documentation.

use std::borrow::Cow;
use std::collections::BTreeSet;
use std::sync::Arc;

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::request::{CallOptions, Tool};
use broca_protocol::structured_output::{
    enforcement, reasons, schema_jcs_text, FrozenStructuredOutput, Mode, SchemaDelivery,
    StructuredOutput, SUBMIT_RESULT_DESCRIPTION, SUBMIT_RESULT_TOOL,
};
use serde_json::{json, Map, Value};

use crate::spec::{CapabilityFlag, WireFamilyId};
use crate::wire::FrozenRenderConfig;

/// When a final-text schema falls outside a Responses route's strict profile, send it in
/// `text.format` without `strict` (`true`), or only as instructions in the system prompt
/// (`false`).
///
/// Off, because the backend validates `text.format` whatever `strict` says. Measured on the
/// ChatGPT/Codex backend with a production schema whose root is an array: sent without
/// `strict`, it was refused 400 `invalid_json_schema` ("'additionalProperties' is required to
/// be supplied and to be false"); with `strict: false` it was still refused ("schema must be
/// a JSON Schema of 'type: \"object\"'"). A request the provider refuses ends the run, so a
/// schema outside the profile goes in the instructions only and is reported `best_effort`.
pub const RESPONSES_OUTSIDE_PROFILE_SENDS_SCHEMA: bool = false;

/// A family's named subset profile: the schema features a route of that family can enforce.
/// The name is frozen with the run, so a resumed run is judged exactly as it was admitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Profile {
    /// OpenAI strict structured outputs (Responses API and the ChatGPT/Codex backend). The root
    /// is an object; every object is closed and requires all of its properties; `oneOf` is
    /// refused (measured on ChatGPT), as are the composition and conditional keywords strict
    /// mode does not list.
    OpenAiStrict,
    /// Meta's Responses API: `openai_strict` without `allOf` or recursive `$ref`, and with
    /// `anyOf` allowed only below the root.
    MetaStrict,
    /// Anthropic structured outputs: `anyOf`, `$ref`/`$defs`, closed objects, `pattern` and
    /// `minLength` (all measured enforced on Claude Sonnet 5.5 and Opus 5.5). `oneOf` is refused
    /// with an HTTP 400, measured, so it is outside; `maxLength` was not measured and stays
    /// outside.
    AnthropicSo,
    /// xAI's OpenAI Chat structured outputs: broad, with `oneOf` read as `anyOf`; recursive
    /// `$ref` is outside.
    Xai,
    /// Gemini `responseJsonSchema`: `pattern` and length bounds may be ignored, so they are
    /// outside.
    GeminiJsonSchema,
}

impl Profile {
    pub fn name(self) -> &'static str {
        match self {
            Self::OpenAiStrict => "openai_strict",
            Self::MetaStrict => "meta_strict",
            Self::AnthropicSo => "anthropic_so",
            Self::Xai => "xai",
            Self::GeminiJsonSchema => "gemini_json_schema",
        }
    }
}

/// What a route can do with a schema: its profile, and whether its family has an encoding for
/// each mode. A route with no profile has no schema encoding at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Route {
    profile: Option<Profile>,
    final_text: bool,
    submit_tool: bool,
}

/// Classify a route by its wire family and, where one family serves providers with different
/// support, by the provider id or the API host.
fn route(family: WireFamilyId, provider_id: &str, base_url: Option<&str>) -> Route {
    let host = base_url.unwrap_or_default();
    let named = |needle: &str| provider_id.contains(needle) || host.contains(needle);
    let none = Route {
        profile: None,
        final_text: false,
        submit_tool: false,
    };
    match family {
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => Route {
            profile: Some(if provider_id == "meta" || host.contains("meta.ai") {
                Profile::MetaStrict
            } else {
                Profile::OpenAiStrict
            }),
            final_text: true,
            submit_tool: true,
        },
        WireFamilyId::AnthropicMessages => Route {
            profile: Some(Profile::AnthropicSo),
            final_text: true,
            submit_tool: true,
        },
        // xAI documents `response_format: json_schema`, and its tool arguments are always
        // strict. Kimi documents only JSON mode, and the OpenAI-compatible long tail has no
        // documented schema encoding broca can rely on.
        WireFamilyId::OpenAiChat if named("xai") || named("x.ai") => Route {
            profile: Some(Profile::Xai),
            final_text: true,
            submit_tool: true,
        },
        WireFamilyId::OpenAiChat => none,
        // Gemini shapes final text with `responseJsonSchema`; function declarations carry no
        // strict flag, so tool input is not enforced.
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => Route {
            profile: Some(Profile::GeminiJsonSchema),
            final_text: true,
            submit_tool: false,
        },
        WireFamilyId::BedrockConverse => none,
    }
}

/// Decide, at admission, how a run's structured output reaches the provider and what it
/// reports. `enforced` needs all three: the catalog says the route supports schemas, the
/// family has an encoding for the requested mode, and the schema fits the family's profile.
///
/// The `best_effort` reasons are checked in this order: a route whose family has no schema
/// encoding (`route_unsupported`), a catalog that does not say (`catalog_unknown`) or says no
/// (`route_unsupported`), a mode the family cannot enforce (`mode_not_enforceable`), and a
/// schema outside the profile (`schema_outside_subset`). Only a route the catalog vouches for
/// is sent the family's schema encoding at all; anything the provider was not asked to
/// enforce is still validated by the runner when the run ends.
pub fn decide(
    family: WireFamilyId,
    provider_id: &str,
    base_url: Option<&str>,
    catalog: CapabilityFlag,
    request: &StructuredOutput,
) -> Result<FrozenStructuredOutput, String> {
    let route = route(family, provider_id, base_url);
    let mode_encodable = match request.mode {
        Mode::FinalText => route.final_text,
        Mode::SubmitTool => route.submit_tool,
        _ => false,
    };
    let fits = route
        .profile
        .is_some_and(|profile| fits_profile(profile, &request.schema));
    let (reason, delivery) = if route.profile.is_none() {
        (
            Some(reasons::ROUTE_UNSUPPORTED),
            SchemaDelivery::Unconstrained,
        )
    } else {
        match catalog {
            CapabilityFlag::Absent | CapabilityFlag::Unknown => (
                Some(reasons::CATALOG_UNKNOWN),
                SchemaDelivery::Unconstrained,
            ),
            CapabilityFlag::Known(false) => (
                Some(reasons::ROUTE_UNSUPPORTED),
                SchemaDelivery::Unconstrained,
            ),
            CapabilityFlag::Known(true) if !mode_encodable => (
                Some(reasons::MODE_NOT_ENFORCEABLE),
                SchemaDelivery::Unconstrained,
            ),
            CapabilityFlag::Known(true) if !fits => (
                Some(reasons::SCHEMA_OUTSIDE_SUBSET),
                outside_profile_delivery(family, request.mode),
            ),
            CapabilityFlag::Known(true) => (None, SchemaDelivery::Strict),
        }
    };
    Ok(FrozenStructuredOutput {
        mode: request.mode,
        name: request.effective_name().to_owned(),
        schema_jcs: schema_jcs_text(request)?,
        enforcement: if reason.is_none() {
            enforcement::ENFORCED
        } else {
            enforcement::BEST_EFFORT
        }
        .to_owned(),
        reason: reason.map(str::to_owned),
        profile: route.profile.map(|profile| profile.name().to_owned()),
        delivery,
    })
}

/// How a schema outside the route's profile is sent. A family whose encoding has a
/// non-strict form uses it; one whose encoding is always enforced (Anthropic's
/// `output_config.format`, Gemini's `responseJsonSchema`) falls back to instructions for final
/// text, since asking it to enforce an unsupported schema could be refused. A submit tool is
/// always sent, without a strict flag.
fn outside_profile_delivery(family: WireFamilyId, mode: Mode) -> SchemaDelivery {
    match (family, mode) {
        (WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt, Mode::FinalText) => {
            if RESPONSES_OUTSIDE_PROFILE_SENDS_SCHEMA {
                SchemaDelivery::NonStrict
            } else {
                SchemaDelivery::Unconstrained
            }
        }
        (WireFamilyId::OpenAiChat, Mode::FinalText) => SchemaDelivery::NonStrict,
        (_, Mode::FinalText) => SchemaDelivery::Unconstrained,
        _ => SchemaDelivery::NonStrict,
    }
}

// ───────────────────────── subset profiles ─────────────────────────

/// Keywords no profile can enforce: dynamic references resolve against the evaluation path,
/// which no provider grammar compiler follows.
const NEVER: &[&str] = &["$dynamicRef", "$dynamicAnchor", "$recursiveRef"];

/// Keywords OpenAI's strict mode does not list.
const OPENAI_OUTSIDE: &[&str] = &[
    "oneOf",
    "allOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "dependentRequired",
    "patternProperties",
    "unevaluatedProperties",
    "unevaluatedItems",
    "propertyNames",
    "contains",
    "prefixItems",
];

/// Keywords Anthropic's structured outputs refuse (`oneOf`, measured) or do not list.
const ANTHROPIC_OUTSIDE: &[&str] = &[
    "oneOf",
    "allOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "dependentRequired",
    "patternProperties",
    "unevaluatedProperties",
    "unevaluatedItems",
    "propertyNames",
    "maxLength",
];

/// Keywords xAI's structured outputs do not list.
const XAI_OUTSIDE: &[&str] = &[
    "allOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "unevaluatedProperties",
    "unevaluatedItems",
];

/// Keywords Gemini's `responseJsonSchema` may ignore or does not list.
const GEMINI_OUTSIDE: &[&str] = &[
    "pattern",
    "minLength",
    "maxLength",
    "allOf",
    "not",
    "if",
    "then",
    "else",
    "dependentSchemas",
    "dependentRequired",
    "patternProperties",
    "unevaluatedProperties",
    "unevaluatedItems",
];

/// Draft 2020-12 keywords whose value is one schema.
const SCHEMA_KEYWORDS: &[&str] = &[
    "items",
    "contains",
    "additionalProperties",
    "propertyNames",
    "if",
    "then",
    "else",
    "not",
    "unevaluatedItems",
    "unevaluatedProperties",
    "contentSchema",
];
/// Draft 2020-12 keywords whose value is an array of schemas.
const SCHEMA_ARRAY_KEYWORDS: &[&str] = &["allOf", "anyOf", "oneOf", "prefixItems"];
/// Draft 2020-12 keywords whose value maps names to schemas.
const SCHEMA_MAP_KEYWORDS: &[&str] = &[
    "properties",
    "patternProperties",
    "$defs",
    "dependentSchemas",
];

/// Whether `schema` uses only features `profile` can enforce.
pub fn fits_profile(profile: Profile, schema: &Value) -> bool {
    let outside: &[&str] = match profile {
        Profile::OpenAiStrict | Profile::MetaStrict => OPENAI_OUTSIDE,
        Profile::AnthropicSo => ANTHROPIC_OUTSIDE,
        Profile::Xai => XAI_OUTSIDE,
        Profile::GeminiJsonSchema => GEMINI_OUTSIDE,
    };
    let mut ok = true;
    let mut subschemas = Vec::new();
    collect_subschemas(schema, true, &mut subschemas);
    for (subschema, is_root) in &subschemas {
        let Some(members) = subschema.as_object() else {
            continue;
        };
        if members
            .keys()
            .any(|key| NEVER.contains(&key.as_str()) || outside.contains(&key.as_str()))
        {
            ok = false;
        }
        let closed_objects = matches!(
            profile,
            Profile::OpenAiStrict | Profile::MetaStrict | Profile::AnthropicSo
        );
        if closed_objects && is_object_schema(members) && !object_is_closed(members, profile) {
            ok = false;
        }
        if profile == Profile::MetaStrict && *is_root && members.contains_key("anyOf") {
            ok = false;
        }
        if profile == Profile::AnthropicSo
            && members
                .get("minItems")
                .and_then(Value::as_u64)
                .is_some_and(|min| min > 1)
        {
            ok = false;
        }
        if profile == Profile::Xai
            && members
                .get("minLength")
                .and_then(Value::as_u64)
                .is_some_and(|min| min > 2048)
        {
            ok = false;
        }
    }
    if matches!(profile, Profile::OpenAiStrict | Profile::MetaStrict)
        && schema.get("type").and_then(Value::as_str) != Some("object")
    {
        ok = false;
    }
    let recursion_outside = !matches!(profile, Profile::OpenAiStrict);
    if recursion_outside && has_recursive_reference(schema) {
        ok = false;
    }
    ok
}

fn is_object_schema(members: &Map<String, Value>) -> bool {
    members.get("type").and_then(Value::as_str) == Some("object")
        || members.contains_key("properties")
}

/// An object is closed when it forbids unlisted members. OpenAI's strict mode also requires
/// every listed property to be required.
fn object_is_closed(members: &Map<String, Value>, profile: Profile) -> bool {
    if members.get("additionalProperties") != Some(&Value::Bool(false)) {
        return false;
    }
    if !matches!(profile, Profile::OpenAiStrict | Profile::MetaStrict) {
        return true;
    }
    let properties: BTreeSet<&str> = members
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| properties.keys().map(String::as_str).collect())
        .unwrap_or_default();
    let required: BTreeSet<&str> = members
        .get("required")
        .and_then(Value::as_array)
        .map(|required| required.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default();
    properties == required
}

/// Every subschema of `schema` in a draft 2020-12 schema position, with whether it is the root.
fn collect_subschemas<'a>(schema: &'a Value, is_root: bool, out: &mut Vec<(&'a Value, bool)>) {
    out.push((schema, is_root));
    let Some(members) = schema.as_object() else {
        return;
    };
    for (key, value) in members {
        let key = key.as_str();
        if SCHEMA_KEYWORDS.contains(&key) {
            collect_subschemas(value, false, out);
        } else if SCHEMA_ARRAY_KEYWORDS.contains(&key) {
            for item in value.as_array().into_iter().flatten() {
                collect_subschemas(item, false, out);
            }
        } else if SCHEMA_MAP_KEYWORDS.contains(&key) {
            for item in value.as_object().into_iter().flat_map(Map::values) {
                collect_subschemas(item, false, out);
            }
        }
    }
}

/// Whether a `$ref` can lead back to a schema that contains it, directly or through other
/// references. A reference that is not a JSON Pointer into the document (an anchor, or an
/// `$id`) cannot be followed here and counts as possibly recursive.
fn has_recursive_reference(schema: &Value) -> bool {
    let mut safe = BTreeSet::new();
    walk_references(schema, schema, String::new(), &mut Vec::new(), &mut safe)
}

/// `pointer` is `node`'s location in `root`; `expanding` holds the reference targets being
/// followed to reach it; `safe` holds targets already followed to the end without recursion.
fn walk_references(
    root: &Value,
    node: &Value,
    pointer: String,
    expanding: &mut Vec<String>,
    safe: &mut BTreeSet<String>,
) -> bool {
    let Some(members) = node.as_object() else {
        return false;
    };
    let contains =
        |outer: &str, inner: &str| inner == outer || inner.starts_with(&format!("{outer}/"));
    if let Some(reference) = members.get("$ref").and_then(Value::as_str) {
        let Some(target) = reference
            .strip_prefix('#')
            .filter(|target| target.is_empty() || target.starts_with('/'))
        else {
            return true;
        };
        if contains(target, &pointer) || expanding.iter().any(|open| contains(target, open)) {
            return true;
        }
        if !safe.contains(target) {
            if let Some(resolved) = root.pointer(target) {
                expanding.push(target.to_owned());
                let recursive = walk_references(root, resolved, target.to_owned(), expanding, safe);
                expanding.pop();
                if recursive {
                    return true;
                }
            }
            safe.insert(target.to_owned());
        }
    }
    let escape = |key: &str| key.replace('~', "~0").replace('/', "~1");
    for (key, value) in members {
        let key_str = key.as_str();
        let at = format!("{pointer}/{}", escape(key_str));
        let recursive = if SCHEMA_KEYWORDS.contains(&key_str) {
            walk_references(root, value, at, expanding, safe)
        } else if SCHEMA_ARRAY_KEYWORDS.contains(&key_str) {
            value
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
                .any(|(index, item)| {
                    walk_references(root, item, format!("{at}/{index}"), expanding, safe)
                })
        } else if SCHEMA_MAP_KEYWORDS.contains(&key_str) {
            value.as_object().into_iter().flatten().any(|(name, item)| {
                walk_references(
                    root,
                    item,
                    format!("{at}/{}", escape(name)),
                    expanding,
                    safe,
                )
            })
        } else {
            false
        };
        if recursive {
            return true;
        }
    }
    false
}

// ───────────────────────── rendering ─────────────────────────

/// The instructions a `final_text` run without a provider-side schema constraint carries in
/// its system prompt.
fn instructions(frozen: &FrozenStructuredOutput) -> String {
    format!(
        "Your final answer must be a single JSON value, with no other text before or after \
         it, that validates against this JSON Schema (draft 2020-12) named \"{}\":\n{}",
        frozen.name, frozen.schema_jcs
    )
}

/// The request a family shapes for a run with structured output: the `submit_result` tool
/// appended to the run's tools, or for an unconstrained `final_text` run, the schema
/// instructions appended to the system prompt. Borrowed, unchanged, for a run without.
///
/// The instructions join the first system message as a final text block. A prompt with no
/// system message gets one at the front, and every renderer-only position into the prompt
/// (declared cache markers, episode starts) moves with it.
pub fn prepare<'a>(frozen: &FrozenRenderConfig, opts: &'a CallOptions) -> Cow<'a, CallOptions> {
    let Some(so) = &frozen.structured_output else {
        return Cow::Borrowed(opts);
    };
    match so.mode {
        Mode::SubmitTool => {
            let mut view = opts.clone();
            let mut tools = Vec::clone(&opts.tools);
            tools.push(Tool {
                name: SUBMIT_RESULT_TOOL.to_owned(),
                description: SUBMIT_RESULT_DESCRIPTION.to_owned(),
                input_schema: so.schema(),
                module: None,
                execution_mode: Default::default(),
            });
            view.tools = Arc::new(tools);
            Cow::Owned(view)
        }
        Mode::FinalText if so.delivery == SchemaDelivery::Unconstrained => {
            let mut view = opts.clone();
            let block = ContentBlock::Text {
                text: instructions(so),
            };
            match view.prompt.iter_mut().find(|m| m.role == Role::System) {
                Some(system) => system.content.push(block),
                None => {
                    view.prompt
                        .insert(0, Message::new(Role::System, vec![block]));
                    for marker in &mut view.declared_markers {
                        marker.message_index += 1;
                    }
                    let mut history = (*view.render_episodes).clone();
                    for episode in &mut history.episodes {
                        episode.prompt_start += 1;
                        episode.first_user = episode.first_user.map(|index| index + 1);
                    }
                    view.render_episodes = Arc::new(history);
                }
            }
            Cow::Owned(view)
        }
        _ => Cow::Borrowed(opts),
    }
}

/// Add the family's schema encoding to a shaped body, and mark the submit tool strict or not.
/// `body` is the family's own request body: for Code Assist, the inner Gemini body.
/// `submit_wire_name` is the submit tool's name as the body spells it.
pub fn finish(frozen: &FrozenRenderConfig, body: &mut Value, submit_wire_name: &str) {
    let Some(so) = &frozen.structured_output else {
        return;
    };
    let family = frozen.family_id;
    let strict = so.delivery == SchemaDelivery::Strict;
    match so.mode {
        Mode::FinalText if so.delivery != SchemaDelivery::Unconstrained => {
            let schema = so.schema();
            match family {
                WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => {
                    let mut format = json!({
                        "type": "json_schema",
                        "name": so.name,
                        "schema": schema,
                    });
                    if strict {
                        format["strict"] = json!(true);
                    }
                    object_member(body, "text").insert("format".into(), format);
                }
                WireFamilyId::AnthropicMessages => {
                    // Merged into the `output_config` that may already carry the variant's
                    // `effort`. Structured outputs are generally available: no beta header.
                    object_member(body, "output_config").insert(
                        "format".into(),
                        json!({ "type": "json_schema", "schema": schema }),
                    );
                }
                WireFamilyId::OpenAiChat => {
                    let mut json_schema = json!({ "name": so.name, "schema": schema });
                    if strict {
                        json_schema["strict"] = json!(true);
                    }
                    body["response_format"] =
                        json!({ "type": "json_schema", "json_schema": json_schema });
                }
                WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist => {
                    let config = object_member(body, "generationConfig");
                    config.insert("responseMimeType".into(), json!("application/json"));
                    config.insert("responseJsonSchema".into(), schema);
                }
                WireFamilyId::BedrockConverse => {}
            }
        }
        Mode::SubmitTool => {
            let Some(tools) = body.get_mut("tools").and_then(Value::as_array_mut) else {
                return;
            };
            match family {
                // Responses tools are strict unless told otherwise on the ChatGPT backend, so
                // the submit tool always states its strictness explicitly.
                WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => {
                    if let Some(tool) = tools
                        .iter_mut()
                        .find(|tool| tool["name"] == submit_wire_name)
                    {
                        tool["strict"] = json!(strict);
                    }
                }
                WireFamilyId::AnthropicMessages => {
                    if strict {
                        if let Some(tool) = tools
                            .iter_mut()
                            .find(|tool| tool["name"] == submit_wire_name)
                        {
                            tool["strict"] = json!(true);
                        }
                    }
                    // Claude Sonnet 5.5 and Opus 5.5 refuse any forcing `tool_choice` (`any`
                    // or `tool`) with an HTTP 400, with or without thinking, measured. A
                    // `submit_tool` run never forces: a caller's forcing choice is sent as
                    // `auto`, so the run still reaches the model with the submit tool offered.
                    let forcing =
                        matches!(body["tool_choice"]["type"].as_str(), Some("any" | "tool"));
                    if forcing {
                        body["tool_choice"] = json!({ "type": "auto" });
                    }
                }
                // xAI's tool arguments are always strict; Gemini and Bedrock have no flag.
                _ => {}
            }
        }
        _ => {}
    }
}

/// `body[key]` as an object, created when absent and replaced when it is not an object.
fn object_member<'a>(body: &'a mut Value, key: &str) -> &'a mut Map<String, Value> {
    let entry = body
        .as_object_mut()
        .expect("a family request body is a JSON object")
        .entry(key)
        .or_insert_with(|| json!({}));
    if !entry.is_object() {
        *entry = json!({});
    }
    entry.as_object_mut().expect("just made an object")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn closed() -> Value {
        json!({
            "type": "object",
            "properties": {"city": {"type": "string"}, "n": {"type": "number"}},
            "required": ["city", "n"],
            "additionalProperties": false
        })
    }

    #[test]
    fn openai_strict_needs_closed_objects_with_every_property_required() {
        assert!(fits_profile(Profile::OpenAiStrict, &closed()));
        let mut open = closed();
        open.as_object_mut().unwrap().remove("additionalProperties");
        assert!(!fits_profile(Profile::OpenAiStrict, &open));
        let mut optional = closed();
        optional["required"] = json!(["city"]);
        assert!(!fits_profile(Profile::OpenAiStrict, &optional));
        assert!(!fits_profile(
            Profile::OpenAiStrict,
            &json!({"type": "string"})
        ));
    }

    #[test]
    fn one_of_is_outside_openai_and_anthropic_and_inside_xai() {
        let mut one_of = closed();
        one_of["properties"]["shape"] = json!({"oneOf": [{"type": "string"}, {"type": "number"}]});
        one_of["required"] = json!(["city", "n", "shape"]);
        assert!(!fits_profile(Profile::OpenAiStrict, &one_of));
        assert!(!fits_profile(Profile::AnthropicSo, &one_of));
        assert!(fits_profile(Profile::Xai, &one_of));
    }

    #[test]
    fn meta_refuses_root_any_of_and_recursion_that_openai_allows() {
        let recursive = json!({
            "type": "object",
            "properties": {"child": {"$ref": "#"}},
            "required": ["child"],
            "additionalProperties": false
        });
        assert!(fits_profile(Profile::OpenAiStrict, &recursive));
        assert!(!fits_profile(Profile::MetaStrict, &recursive));
        let defs = json!({
            "type": "object",
            "$defs": {"node": {"type": "object", "properties": {"next": {"$ref": "#/$defs/node"}},
                "required": ["next"], "additionalProperties": false}},
            "properties": {"head": {"$ref": "#/$defs/node"}},
            "required": ["head"],
            "additionalProperties": false
        });
        assert!(!fits_profile(Profile::MetaStrict, &defs));
        let flat = json!({
            "type": "object",
            "$defs": {"leaf": {"type": "string"}},
            "properties": {"a": {"$ref": "#/$defs/leaf"}, "b": {"$ref": "#/$defs/leaf"}},
            "required": ["a", "b"],
            "additionalProperties": false
        });
        assert!(fits_profile(Profile::MetaStrict, &flat));
        let mut root_any_of = closed();
        root_any_of["anyOf"] = json!([{"required": ["city"]}]);
        assert!(!fits_profile(Profile::MetaStrict, &root_any_of));
    }

    #[test]
    fn anthropic_enforces_measured_string_bounds_and_gemini_does_not() {
        let mut bounded = closed();
        bounded["properties"]["city"]["pattern"] = json!("^[A-Z]");
        bounded["properties"]["city"]["minLength"] = json!(3);
        assert!(fits_profile(Profile::AnthropicSo, &bounded));
        assert!(fits_profile(Profile::AnthropicSo, &closed()));
        assert!(!fits_profile(Profile::GeminiJsonSchema, &bounded));
        let mut unmeasured = closed();
        unmeasured["properties"]["city"]["maxLength"] = json!(30);
        assert!(!fits_profile(Profile::AnthropicSo, &unmeasured));
    }

    fn request(mode: Mode, schema: Value) -> StructuredOutput {
        StructuredOutput::new(mode, schema)
    }

    #[test]
    fn enforcement_needs_catalog_support_an_encoding_and_a_fitting_schema() {
        let known = CapabilityFlag::Known(true);
        let enforced = decide(
            WireFamilyId::OpenAiResponsesChatgpt,
            "openai",
            None,
            known,
            &request(Mode::FinalText, closed()),
        )
        .unwrap();
        assert_eq!(enforced.enforcement, "enforced");
        assert_eq!(enforced.reason, None);
        assert_eq!(enforced.delivery, SchemaDelivery::Strict);
        assert_eq!(enforced.profile.as_deref(), Some("openai_strict"));
        let cases = [
            (
                WireFamilyId::OpenAiResponses,
                CapabilityFlag::Absent,
                Mode::FinalText,
                closed(),
                "catalog_unknown",
                SchemaDelivery::Unconstrained,
            ),
            (
                WireFamilyId::OpenAiResponses,
                CapabilityFlag::Unknown,
                Mode::FinalText,
                closed(),
                "catalog_unknown",
                SchemaDelivery::Unconstrained,
            ),
            (
                WireFamilyId::OpenAiResponses,
                CapabilityFlag::Known(false),
                Mode::SubmitTool,
                closed(),
                "route_unsupported",
                SchemaDelivery::Unconstrained,
            ),
            (
                WireFamilyId::OpenAiResponses,
                known,
                Mode::FinalText,
                json!({"type": "object"}),
                "schema_outside_subset",
                // The backend validates `text.format` even without `strict`, so a schema outside
                // the profile goes in the instructions only.
                SchemaDelivery::Unconstrained,
            ),
            (
                WireFamilyId::AnthropicMessages,
                known,
                Mode::FinalText,
                json!({"type": "object"}),
                "schema_outside_subset",
                SchemaDelivery::Unconstrained,
            ),
            (
                WireFamilyId::Gemini,
                known,
                Mode::SubmitTool,
                closed(),
                "mode_not_enforceable",
                SchemaDelivery::Unconstrained,
            ),
            (
                WireFamilyId::BedrockConverse,
                known,
                Mode::FinalText,
                closed(),
                "route_unsupported",
                SchemaDelivery::Unconstrained,
            ),
        ];
        for (family, catalog, mode, schema, reason, delivery) in cases {
            let decided = decide(family, "p", None, catalog, &request(mode, schema)).unwrap();
            assert_eq!(decided.enforcement, "best_effort", "{family:?} {catalog:?}");
            assert_eq!(
                decided.reason.as_deref(),
                Some(reason),
                "{family:?} {catalog:?}"
            );
            assert_eq!(decided.delivery, delivery, "{family:?} {catalog:?}");
        }
    }

    #[test]
    fn openai_chat_enforces_on_xai_and_not_on_kimi() {
        let schema = json!({"type": "object", "properties": {"answer": {"type": "string"}}, "required": ["answer"]});
        let known = CapabilityFlag::Known(true);
        let xai = decide(
            WireFamilyId::OpenAiChat,
            "xai",
            None,
            known,
            &request(Mode::SubmitTool, schema.clone()),
        )
        .unwrap();
        assert_eq!(
            (xai.enforcement.as_str(), xai.profile.as_deref()),
            ("enforced", Some("xai"))
        );
        let kimi = decide(
            WireFamilyId::OpenAiChat,
            "moonshotai",
            Some("https://api.moonshot.ai/v1"),
            known,
            &request(Mode::FinalText, schema),
        )
        .unwrap();
        assert_eq!(kimi.reason.as_deref(), Some("route_unsupported"));
        assert_eq!(kimi.profile, None);
        let meta = decide(
            WireFamilyId::OpenAiResponses,
            "meta",
            None,
            known,
            &request(Mode::FinalText, closed()),
        )
        .unwrap();
        assert_eq!(meta.profile.as_deref(), Some("meta_strict"));
    }
}
