//! The Google Gemini wire family — `generateContent` / `streamGenerateContent`.
//!
//! Wire grammar confirmed against:
//! - https://ai.google.dev/api/generate-content
//! - https://ai.google.dev/gemini-api/docs/text-generation
//!
//! Request: `contents[{role, parts}]` with roles `user` / `model`; text `{text}`;
//! tool calls `{functionCall:{name, args}}` (args object); results
//! `{functionResponse:{name, response}}`. `systemInstruction` is a separate top-level
//! field. Tools: `tools[{functionDeclarations}]`. Limits in `generationConfig`.
//! Tool choice: `toolConfig.functionCallingConfig`.
//!
//! Streaming: POST `.../models/{model}:streamGenerateContent?alt=sse` — SSE `data:`
//! lines are `GenerateContentResponse` chunks with `candidates[0].content.parts[]`
//! (text + functionCall) and optional `usageMetadata` / `finishReason` on later chunks.
//! Gemini does not echo tool-call ids; inbound ids are assigned deterministically by
//! call index within the assembled model turn (`ToolIdPolicy::SyntheticIndex`).
//!
//! Auth: `x-goog-api-key` header. Vertex AI (OAuth + different base URL) is catalog
//! data only — follow-up when OAuth plumbing exists.

use std::collections::BTreeMap;

use broca_protocol::{
    content::{
        AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall, ToolOutput,
        ToolResult,
    },
    display::DisplayEvent,
    ids::{MessageId, ToolCallId},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, ModelHandle, Tool, ToolChoice},
    usage::Usage,
    StepId,
};
use serde_json::{json, Map, Value};

use crate::families::{apply_generation_controls, merge_inclusive_input_tokens};
use crate::policy::{SystemPromptPolicy, ToolIdPolicy};
use crate::sse::SseDecoder;
use crate::wire::{
    AuthHandle, FrozenRenderConfig, HttpMethod, HttpRequest, NormalizeError, StreamNormalizer,
    WireFamily,
};

/// The default API base (no `base_url` override). The family's
/// `/models/<model>:streamGenerateContent` path is appended; a base_url override (a
/// Gemini-compatible host, or a mock in tests) replaces only the base. base_url lives
/// in the endpoint, not the rendered body, so it's outside the cache/resume hash —
/// overriding it never changes a golden.
const DEFAULT_BASE: &str = "https://generativelanguage.googleapis.com/v1beta";

pub struct Gemini;

impl WireFamily for Gemini {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        build_request_body(frozen, opts)
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        let base = frozen.base_url.as_deref().unwrap_or(DEFAULT_BASE);
        let url = format!(
            "{}/models/{}:streamGenerateContent?alt=sse",
            base.trim_end_matches('/'),
            frozen.wire_model_id
        );
        let mut headers = vec![
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "text/event-stream".to_string()),
        ];
        match auth {
            AuthHandle::Bearer(token) => {
                headers.push(("authorization".to_string(), format!("Bearer {token}")));
            }
            AuthHandle::ApiKeyHeader { header, value } => {
                headers.push((header.clone(), value.clone()));
            }
            AuthHandle::SigV4 { signed_headers, .. } => {
                headers.extend(signed_headers.iter().cloned());
            }
            AuthHandle::Headers(hs) => headers.extend(hs.iter().cloned()),
            AuthHandle::None => {}
        }
        HttpRequest {
            method: HttpMethod::Post,
            url,
            headers,
            body,
        }
    }

    fn normalizer(
        &self,
        frozen: &FrozenRenderConfig,
        step_id: StepId,
    ) -> Box<dyn StreamNormalizer> {
        Box::new(GeminiNormalizer::new(step_id, frozen.tool_id).with_text_state(frozen))
    }
}

/// Build the Gemini `generateContent` request body as a JSON value. Shared with the
/// Google Antigravity / Code-Assist family, which nests this exact body inside its
/// `cloudcode-pa` envelope's `request` field — so the inner body is byte-identical to a
/// direct Gemini request by construction.
pub(crate) fn build_request_body(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    // A run with structured output adds its submit tool or schema instructions here; a run
    // without renders from `opts` unchanged.
    let prepared = crate::structured_output::prepare(frozen, opts);
    let opts = prepared.as_ref();
    let view = crate::families::compaction_view(frozen.family_id, opts);
    let opts = view.as_ref();
    let name_map = crate::tool_name_alias::ToolNameMap::build(&opts.tools, &opts.prompt);
    let contents = convert_contents(&opts.prompt, frozen, opts.target_model.as_ref(), &name_map);

    let mut body = json!({ "contents": contents });

    if let SystemPromptPolicy::SystemInstructionField = frozen.system_prompt {
        let parts = system_instruction_parts(&opts.prompt);
        if !parts.is_empty() {
            body["systemInstruction"] = json!({ "parts": parts });
        }
    }

    let mut generation_config = Map::new();
    if frozen.token_limit_field.in_generation_config() {
        generation_config.insert(
            frozen.token_limit_field.key().to_string(),
            json!(frozen.max_output_tokens),
        );
    } else {
        body[frozen.token_limit_field.key()] = json!(frozen.max_output_tokens);
    }
    // Gemini keeps sampling controls under `generationConfig`, and Code-Assist reuses
    // this exact inner body under its envelope.
    apply_generation_controls(
        &mut generation_config,
        opts,
        "temperature",
        "topP",
        Some("stopSequences"),
    );
    // A caller's reasoning variant, resolved at admission from the model's catalog options.
    if let Some(crate::variant::FrozenVariant {
        request: crate::variant::VariantRequest::Gemini { thinking_config },
        ..
    }) = &frozen.variant
    {
        generation_config.insert("thinkingConfig".into(), thinking_config.clone());
    }
    if !generation_config.is_empty() {
        body["generationConfig"] = Value::Object(generation_config);
    }

    if !opts.tools.is_empty() {
        body["tools"] = json!([{
            "functionDeclarations": opts
                .tools
                .iter()
                .map(|tool| convert_tool(tool, &name_map))
                .collect::<Vec<_>>()
        }]);
        body["toolConfig"] = convert_tool_config(&opts.tool_choice, &opts.tools, &name_map);
    }

    // The caller's service tier (`flex` or `priority`; the default sends nothing). Google
    // documents the field as `service_tier`
    // (https://ai.google.dev/gemini-api/docs/flex-inference). Code Assist reuses this body but
    // is never admitted with a tier, so its envelope is unaffected.
    if let Some(service_tier) = &frozen.service_tier {
        body["service_tier"] = Value::String(service_tier.clone());
    }

    merge_static_options(&mut body, &frozen.static_options);
    crate::structured_output::finish(
        frozen,
        &mut body,
        name_map.wire_name(broca_protocol::structured_output::SUBMIT_RESULT_TOOL),
    );
    body
}

fn merge_static_options(body: &mut Value, static_options: &Value) {
    if static_options.is_null() {
        return;
    }
    let Some(obj) = static_options.as_object() else {
        return;
    };
    let Some(body_obj) = body.as_object_mut() else {
        return;
    };
    for (k, v) in obj {
        body_obj.insert(k.clone(), v.clone());
    }
}

fn system_instruction_parts(prompt: &[Message]) -> Vec<Value> {
    let mut out = Vec::new();
    for m in prompt {
        if m.role != Role::System {
            continue;
        }
        for b in &m.content {
            // Drop an empty text part — Gemini rejects an empty `{text:""}` part. Defensive
            // against foreign input; the owned normalizers never emit empty text.
            if let ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } = b {
                if !text.is_empty() {
                    out.push(json!({ "text": text }));
                }
            }
        }
    }
    out
}

fn convert_contents(
    prompt: &[Message],
    frozen: &FrozenRenderConfig,
    target_model: Option<&ModelHandle>,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Vec<Value> {
    let mut out: Vec<Value> = Vec::new();
    let non_system: Vec<&Message> = prompt.iter().filter(|m| m.role != Role::System).collect();
    let mut current_tool_call_names = BTreeMap::new();

    let mut i = 0;
    while i < non_system.len() {
        let m = non_system[i];
        match m.role {
            Role::User => {
                let parts = convert_user_parts(&m.content, &current_tool_call_names);
                current_tool_call_names.clear();
                // Gemini rejects turns left empty after unsupported blocks are stripped.
                if !parts.is_empty() {
                    out.push(json!({
                        "role": "user",
                        "parts": parts,
                    }));
                }
                i += 1;
            }
            Role::Assistant => {
                current_tool_call_names = tool_call_names_in_message(m, name_map);
                let echo_safe =
                    crate::families::signed_reasoning_is_echo_safe(m.origin.as_ref(), target_model);
                let parts = convert_model_parts(
                    &m.content,
                    frozen.tool_id,
                    echo_safe,
                    name_map,
                    |format| {
                        frozen.provider_text_state
                            && super::text_state_is_echo_safe(
                                m,
                                target_model,
                                format,
                                frozen.family_id,
                            )
                    },
                );
                // A foreign model's signed reasoning can be the turn's only block.
                if !parts.is_empty() {
                    out.push(json!({
                        "role": "model",
                        "parts": parts,
                    }));
                }
                i += 1;
            }
            Role::Tool => {
                let mut parts: Vec<Value> = Vec::new();
                while i < non_system.len() && non_system[i].role == Role::Tool {
                    parts.extend(convert_tool_result_parts(
                        non_system[i],
                        &current_tool_call_names,
                    ));
                    i += 1;
                }
                current_tool_call_names.clear();
                if !parts.is_empty() {
                    out.push(json!({ "role": "user", "parts": parts }));
                }
            }
            Role::System => {
                current_tool_call_names.clear();
                i += 1;
            }
        }
    }
    out
}

fn convert_user_parts(
    blocks: &[ContentBlock],
    id_to_name: &BTreeMap<String, String>,
) -> Vec<Value> {
    blocks
        .iter()
        .filter_map(|block| match block {
            // Drop an empty text part (Gemini rejects `text:""`). Defensive — see above.
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
                if text.is_empty() =>
            {
                None
            }
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                Some(json!({ "text": text }))
            }
            ContentBlock::ToolResult(result) => Some(convert_tool_result_part(result, id_to_name)),
            _ => None,
        })
        .collect()
}

fn convert_model_parts(
    blocks: &[ContentBlock],
    tool_id: ToolIdPolicy,
    signed_calls_are_echo_safe: bool,
    name_map: &crate::tool_name_alias::ToolNameMap,
    text_state_is_echo_safe: impl Fn(broca_protocol::content::TextProviderFormat) -> bool,
) -> Vec<Value> {
    use crate::reasoning::{decide_reasoning, ReasoningDecision, SerializerProfile};

    let mut parts: Vec<Value> = Vec::new();
    let mut call_index: u32 = 0;
    for (idx, b) in blocks.iter().enumerate() {
        match b {
            // Drop an empty text part (Gemini rejects `text:""`). Defensive — see above.
            ContentBlock::Text { text } if text.is_empty() => {}
            ContentBlock::Text { text } => parts.push(json!({ "text": text })),
            ContentBlock::TextWithState {
                text,
                provider_state,
            } => {
                let mut part = json!({"text":text});
                if text_state_is_echo_safe(provider_state.format) {
                    if let Ok(state) = serde_json::from_str::<Value>(&provider_state.data) {
                        if let Some(signature) =
                            state.get("thoughtSignature").and_then(Value::as_str)
                        {
                            part["thoughtSignature"] = json!(signature);
                        }
                    }
                }
                if !text.is_empty() || part.get("thoughtSignature").is_some() {
                    parts.push(part);
                }
            }
            ContentBlock::Reasoning { text, signature } => {
                let keep = decide_reasoning(
                    signed_calls_are_echo_safe,
                    signature.is_some(),
                    idx,
                    blocks,
                    SerializerProfile::SourceOrder,
                ) == ReasoningDecision::Keep;
                if keep {
                    if let Some(signature) = signature {
                        parts.push(json!({
                            "thought": true,
                            "text": text,
                            "thoughtSignature": signature,
                        }));
                    }
                }
            }
            ContentBlock::RedactedReasoning {
                data,
                provider_format,
            } => {
                let keep = provider_format.is_none()
                    && decide_reasoning(
                        signed_calls_are_echo_safe,
                        !data.is_empty(),
                        idx,
                        blocks,
                        SerializerProfile::SourceOrder,
                    ) == ReasoningDecision::Keep;
                if keep && !data.is_empty() {
                    parts.push(json!({
                        "thought": true,
                        "thoughtSignature": data,
                    }));
                }
            }
            ContentBlock::ToolCall(tc) => {
                let wire_id = wire_tool_call_id(&tc.tool_call_id, tool_id, call_index);
                call_index = call_index.saturating_add(1);
                let mut fc = json!({
                    "name": name_map.wire_name(&tc.tool_name),
                    "args": tc.input,
                });
                if tool_id == ToolIdPolicy::Passthrough {
                    fc["id"] = json!(wire_id);
                }
                let mut part = json!({ "functionCall": fc });
                if signed_calls_are_echo_safe {
                    if let Some(signature) = &tc.thought_signature {
                        part["thoughtSignature"] = json!(signature);
                    }
                }
                parts.push(part);
            }
            ContentBlock::ToolResult(_) => {}
        }
    }
    parts
}

fn wire_tool_call_id(id: &ToolCallId, policy: ToolIdPolicy, index: u32) -> String {
    match policy {
        ToolIdPolicy::Passthrough => id.0.clone(),
        ToolIdPolicy::SyntheticIndex => format!("call_{index}"),
    }
}

fn tool_call_names_in_message(
    message: &Message,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> BTreeMap<String, String> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some((
                call.tool_call_id.0.clone(),
                name_map.wire_name(&call.tool_name).to_owned(),
            )),
            _ => None,
        })
        .collect()
}

fn convert_tool_result_parts(m: &Message, id_to_name: &BTreeMap<String, String>) -> Vec<Value> {
    m.content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(convert_tool_result_part(result, id_to_name)),
            _ => None,
        })
        .collect()
}

fn convert_tool_result_part(result: &ToolResult, id_to_name: &BTreeMap<String, String>) -> Value {
    let name = id_to_name
        .get(&result.tool_call_id.0)
        .cloned()
        .unwrap_or_default();
    json!({
        "functionResponse": {
            "name": name,
            "response": function_response_payload(&result.output),
        }
    })
}

fn function_response_payload(out: &ToolOutput) -> Value {
    match out {
        ToolOutput::Text { text } => json!({ "result": text }),
        // Gemini models FunctionResponse.response as a protobuf Struct, so it must be a JSON
        // object. Keep objects unchanged and give every other JSON value the same `result`
        // envelope used for text output.
        ToolOutput::Json { value } if value.is_object() => value.clone(),
        ToolOutput::Json { value } => json!({ "result": value }),
        ToolOutput::Denied { reason } => json!({ "error": format!("denied: {reason}") }),
        ToolOutput::Error { message } => json!({ "error": message }),
    }
}

fn convert_tool(t: &Tool, name_map: &crate::tool_name_alias::ToolNameMap) -> Value {
    // Keep the canonical tool schema intact; only the Gemini wire declaration needs its
    // restricted OpenAPI subset.
    json!({
        "name": name_map.wire_name(&t.name),
        "description": t.description,
        "parameters": sanitize_gemini_schema(&t.input_schema),
    })
}

fn sanitize_gemini_schema(schema: &Value) -> Value {
    let Value::Object(input) = schema else {
        return schema.clone();
    };

    if input.contains_key("$ref") {
        // Gemini cannot resolve JSON Schema references. Dereferencing would require the
        // surrounding schema, so degrade this node to a valid object declaration instead.
        let mut output = Map::new();
        output.insert("type".into(), Value::String("object".into()));
        if let Some(description) = input.get("description") {
            output.insert("description".into(), description.clone());
        }
        return Value::Object(output);
    }

    let (normalized_type, nullable_from_type) = normalize_gemini_type(input.get("type"));
    let type_name = normalized_type.as_ref().and_then(Value::as_str);
    let mut output = Map::new();

    // `serde_json/preserve_order` is not enabled in this workspace, but sorting explicitly
    // keeps this pure transformation deterministic if the map implementation changes.
    let mut keys: Vec<&String> = input.keys().collect();
    keys.sort_unstable();
    for key in keys {
        let value = &input[key];
        let sanitized = match key.as_str() {
            "type" => normalized_type.clone(),
            "format" if gemini_format_is_supported(type_name, value) => Some(value.clone()),
            "format" => None,
            "nullable" if nullable_from_type => Some(Value::Bool(true)),
            "nullable" | "description" | "enum" | "required" | "minItems" | "maxItems"
            | "minimum" | "maximum" | "minLength" | "maxLength" | "pattern" => Some(value.clone()),
            "properties" => Some(sanitize_gemini_properties(value)),
            "items" => Some(sanitize_gemini_schema(value)),
            "anyOf" | "oneOf" => Some(sanitize_gemini_schema_arms(value)),
            _ => None,
        };
        if let Some(value) = sanitized {
            output.insert(key.clone(), value);
        }
    }

    if nullable_from_type && !input.contains_key("nullable") {
        output.insert("nullable".into(), Value::Bool(true));
    }

    Value::Object(output)
}

fn normalize_gemini_type(schema_type: Option<&Value>) -> (Option<Value>, bool) {
    let Some(schema_type) = schema_type else {
        return (None, false);
    };
    let Value::Array(types) = schema_type else {
        return (Some(schema_type.clone()), false);
    };

    if types.len() == 2 {
        if let Some(non_null) = types
            .iter()
            .find(|value| value.as_str().is_some_and(|name| name != "null"))
        {
            if types
                .iter()
                .any(|value| value.as_str().is_some_and(|name| name == "null"))
            {
                return (Some(non_null.clone()), true);
            }
        }
    }

    (types.first().cloned(), false)
}

fn gemini_format_is_supported(type_name: Option<&str>, format: &Value) -> bool {
    let Some(type_name) = type_name else {
        return false;
    };
    let Some(format) = format.as_str() else {
        return false;
    };

    if type_name.eq_ignore_ascii_case("string") {
        matches!(format, "enum" | "date-time")
    } else if type_name.eq_ignore_ascii_case("integer") {
        matches!(format, "int32" | "int64")
    } else if type_name.eq_ignore_ascii_case("number") {
        matches!(format, "float" | "double")
    } else {
        false
    }
}

fn sanitize_gemini_properties(properties: &Value) -> Value {
    let Value::Object(properties) = properties else {
        return properties.clone();
    };
    let mut output = Map::new();
    let mut keys: Vec<&String> = properties.keys().collect();
    keys.sort_unstable();
    for key in keys {
        output.insert(key.clone(), sanitize_gemini_schema(&properties[key]));
    }
    Value::Object(output)
}

fn sanitize_gemini_schema_arms(arms: &Value) -> Value {
    let Value::Array(arms) = arms else {
        return arms.clone();
    };
    Value::Array(arms.iter().map(sanitize_gemini_schema).collect())
}

fn convert_tool_config(
    tc: &ToolChoice,
    tools: &[Tool],
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Value {
    let (mode, allowed): (&str, Option<Vec<String>>) = match tc {
        ToolChoice::Auto => ("AUTO", None),
        ToolChoice::None => ("NONE", None),
        ToolChoice::Required => ("ANY", None),
        ToolChoice::Tool { name } => ("ANY", Some(vec![name_map.wire_name(name).into()])),
    };
    let mut cfg = json!({ "mode": mode });
    if let Some(names) = allowed {
        cfg["allowedFunctionNames"] = json!(names);
    } else if matches!(tc, ToolChoice::Required) && !tools.is_empty() {
        cfg["allowedFunctionNames"] = json!(tools
            .iter()
            .map(|tool| name_map.wire_name(&tool.name))
            .collect::<Vec<_>>());
    }
    json!({ "functionCallingConfig": cfg })
}

struct FunctionCallBuf {
    index: u32,
    name: String,
    args: Value,
    thought_signature: Option<String>,
}

enum GeminiPartBuf {
    TextWithState {
        text: String,
        provider_state: broca_protocol::content::TextProviderState,
    },
    Text {
        text: String,
        coalescible: bool,
    },
    Reasoning {
        text: String,
        signature: Option<String>,
    },
    RedactedReasoning {
        data: String,
    },
    FunctionCall(FunctionCallBuf),
}

pub(crate) struct GeminiNormalizer {
    step_id: StepId,
    tool_id_policy: ToolIdPolicy,
    text_state_format: Option<broca_protocol::content::TextProviderFormat>,
    sse: SseDecoder,
    parts: Vec<GeminiPartBuf>,
    next_call_index: u32,
    usage: Usage,
    finish_reason: FinishReason,
    started: bool,
    terminal_observed: bool,
    emitted_final: bool,
    /// The `x-gemini-service-tier` response header, verbatim: where Google documents the tier a
    /// request was served at. `None` when the response carried no such header.
    header_service_tier: Option<String>,
    /// `usageMetadata.serviceTier` from the body, verbatim. Not in Google's tier docs, but a
    /// recorded gemini-2.5-flash stream carries it (`"standard"`), so it is read as a second
    /// report of the same fact. The header wins when both are present, being the documented one.
    body_service_tier: Option<String>,
}

/// The response header in which the Gemini API reports the service tier it actually served
/// (a priority request downgraded under congestion reports `standard` here, and bills it).
pub(crate) const GEMINI_SERVICE_TIER_HEADER: &str = "x-gemini-service-tier";

impl GeminiNormalizer {
    pub(crate) fn new(step_id: StepId, tool_id_policy: ToolIdPolicy) -> Self {
        Self {
            step_id,
            tool_id_policy,
            text_state_format: None,
            sse: SseDecoder::for_content_family(crate::content_progress::ContentFamily::Gemini),
            parts: Vec::new(),
            next_call_index: 0,
            usage: Usage::default(),
            finish_reason: FinishReason::Other,
            started: false,
            terminal_observed: false,
            emitted_final: false,
            header_service_tier: None,
            body_service_tier: None,
        }
    }

    pub(crate) fn with_text_state(mut self, frozen: &FrozenRenderConfig) -> Self {
        use broca_protocol::content::TextProviderFormat as F;
        if frozen.provider_text_state {
            self.text_state_format = Some(match frozen.family_id {
                crate::spec::WireFamilyId::GoogleCodeAssist => F::GoogleCodeAssist,
                _ => F::Gemini,
            });
        }
        self
    }

    fn push_part(&mut self, part: GeminiPartBuf) {
        match (self.parts.last_mut(), &part) {
            (
                Some(GeminiPartBuf::Text {
                    text: buffered,
                    coalescible: true,
                }),
                GeminiPartBuf::Text {
                    text,
                    coalescible: true,
                },
            ) => {
                buffered.push_str(text);
                return;
            }
            (
                Some(GeminiPartBuf::Reasoning {
                    text: buffered,
                    signature: None,
                }),
                GeminiPartBuf::Reasoning {
                    text,
                    signature: None,
                },
            ) => {
                buffered.push_str(text);
                return;
            }
            _ => {}
        }
        self.parts.push(part);
    }

    fn handle_chunk(
        &mut self,
        payload: &str,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        let v: Value =
            serde_json::from_str(payload).map_err(|e| NormalizeError::Invalid(e.to_string()))?;
        self.handle_response_value(&v, out)
    }

    /// Decode one already-parsed `GenerateContentResponse` chunk. Code-Assist unwraps its
    /// `{response: ...}` envelope and feeds the inner value through this same Gemini decoder.
    pub(crate) fn handle_response_value(
        &mut self,
        v: &Value,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        if let Some(error) = v.get("error").filter(|error| error.is_object()) {
            let code = error.get("status").and_then(Value::as_str);
            let message = error.get("message").and_then(Value::as_str).unwrap_or("");
            let status = error
                .get("code")
                .and_then(Value::as_u64)
                .and_then(|status| u16::try_from(status).ok());
            let overflow_code = crate::normalize::context_overflow_code(
                crate::spec::WireFamilyId::Gemini,
                code,
                message,
                status,
            );
            let class = if overflow_code.is_some() {
                broca_protocol::ErrorClass::ContextOverflow
            } else {
                crate::normalize::classify_error_with_status_defaults(code, status)
            };
            // Error envelopes are terminal even when HTTP succeeded; EOF must not replace
            // the provider's diagnosis with a generic stream-truncation error.
            self.emitted_final = true;
            out.push(NormalizedEvent::Control(ProviderControl::Error {
                error: broca_protocol::ProviderError {
                    class,
                    message: message.to_owned(),
                    status,
                    provider_code: overflow_code.or(code).map(str::to_owned),
                    retry_after_secs: None,
                    resets_at_ms: None,
                    resets_at_source: None,
                    limit_window: None,
                    account_ref: None,
                    credential_id: None,
                    refusal: None,
                },
                retries_used: 0,
            }));
            return Ok(());
        }
        if !self.started {
            self.started = true;
            out.push(NormalizedEvent::Control(ProviderControl::StreamStart {
                warnings: Vec::new(),
            }));
            out.push(NormalizedEvent::Control(ProviderControl::StepStarted {
                step_id: self.step_id,
            }));
        }

        if let Some(u) = v.get("usageMetadata").filter(|u| !u.is_null()) {
            self.merge_usage(u);
            if let Some(tier) = u.get("serviceTier").and_then(Value::as_str) {
                self.body_service_tier = Some(tier.to_owned());
            }
        }

        let candidate = match v
            .get("candidates")
            .and_then(|c| c.as_array())
            .and_then(|a| a.first())
        {
            Some(c) => c,
            None => {
                if let Some(reason) = v
                    .get("promptFeedback")
                    .and_then(|feedback| feedback.get("blockReason"))
                    .and_then(Value::as_str)
                {
                    self.finish_reason = map_finish_reason(reason);
                    self.terminal_observed = true;
                }
                return Ok(());
            }
        };

        if let Some(fr) = candidate.get("finishReason").and_then(Value::as_str) {
            self.finish_reason = map_finish_reason(fr);
            self.terminal_observed = true;
        }

        let parts = match candidate
            .get("content")
            .and_then(|c| c.get("parts"))
            .and_then(|p| p.as_array())
        {
            Some(p) => p,
            None => return Ok(()),
        };

        let message_id = MessageId(String::new());

        for part in parts {
            let thought_signature = part
                .get("thoughtSignature")
                .and_then(Value::as_str)
                .map(str::to_owned);

            // A Gemini Part has one data payload. Check functionCall first so a malformed part
            // carrying extra text cannot surface that text as either private reasoning or an
            // answer in addition to the authoritative tool call.
            if let Some(fc) = part.get("functionCall") {
                let index = self.next_call_index;
                self.next_call_index = self.next_call_index.saturating_add(1);
                let name = fc
                    .get("name")
                    .and_then(Value::as_str)
                    .unwrap_or("")
                    .to_owned();
                let args = fc.get("args").cloned().unwrap_or_else(|| json!({}));
                let display_delta = args.to_string();
                self.push_part(GeminiPartBuf::FunctionCall(FunctionCallBuf {
                    index,
                    name,
                    args,
                    thought_signature,
                }));
                let tcid = synthetic_inbound_id(index, self.tool_id_policy);
                out.push(NormalizedEvent::Display(DisplayEvent::ToolInputDelta {
                    tool_call_id: ToolCallId(tcid),
                    delta: display_delta,
                }));
                continue;
            }

            let is_thought = part.get("thought").and_then(Value::as_bool) == Some(true);
            let text = part.get("text").and_then(Value::as_str);

            // Thought text is a distinct provider lane, not assistant prose. Classifying it at
            // the wire part prevents model summaries from being persisted and replayed as answers.
            if is_thought {
                if let Some(text) = text.filter(|text| !text.is_empty()) {
                    self.push_part(GeminiPartBuf::Reasoning {
                        text: text.to_owned(),
                        signature: thought_signature,
                    });
                    out.push(NormalizedEvent::Display(DisplayEvent::ReasoningDelta {
                        message_id: message_id.clone(),
                        delta: text.to_owned(),
                    }));
                } else if let Some(signature) = thought_signature.filter(|sig| !sig.is_empty()) {
                    self.push_part(GeminiPartBuf::RedactedReasoning { data: signature });
                }
                continue;
            }

            if let (Some(format), Some(signature)) =
                (self.text_state_format, thought_signature.as_ref())
            {
                // A signature belongs to the exact part that carried it, including empty text.
                self.push_part(GeminiPartBuf::TextWithState {
                    text: text.unwrap_or("").to_owned(),
                    provider_state: broca_protocol::content::TextProviderState {
                        format,
                        data: json!({"thoughtSignature":signature}).to_string(),
                    },
                });
            } else if let Some(text) = text.filter(|text| !text.is_empty()) {
                let coalescible = thought_signature.is_none();
                self.push_part(GeminiPartBuf::Text {
                    text: text.to_owned(),
                    coalescible,
                });
            }
            if let Some(text) = text.filter(|text| !text.is_empty()) {
                out.push(NormalizedEvent::Display(DisplayEvent::TextDelta {
                    message_id: message_id.clone(),
                    tool_call_id: None,
                    delta: text.to_owned(),
                }));
            }
        }

        Ok(())
    }

    fn merge_usage(&mut self, u: &Value) {
        let g = |k: &str| u.get(k).and_then(Value::as_u64);
        merge_inclusive_input_tokens(
            &mut self.usage,
            g("promptTokenCount"),
            g("cachedContentTokenCount"),
        );
        // Gemini's `candidatesTokenCount` is the VISIBLE-ONLY output (answer text + tool
        // calls); `thoughtsTokenCount` is a SIBLING — the thinking tokens are ADDITIONAL,
        // not included in candidatesTokenCount. Our canonical `output_tokens` is the
        // inclusive billing total (the subset invariant: reasoning_tokens is a subset of
        // output_tokens), so we sum candidates + thoughts into it. This corrects a prior
        // undercount for thinking models, which stored visible-only output. Absent
        // thoughtsTokenCount (non-thinking models, or any non-terminal chunk) leaves
        // output = candidates alone and reasoning unreported. A lone sibling is a
        // reported lower bound; when neither is present, retain the prior measurement.
        // Code-Assist inherits this via its delegation to the Gemini decode.
        let candidates = g("candidatesTokenCount");
        let thoughts = g("thoughtsTokenCount");
        if let Some(thoughts) = thoughts {
            self.usage.reasoning_tokens = Some(thoughts);
        }
        if candidates.is_some() || thoughts.is_some() {
            self.usage.output_tokens =
                broca_protocol::usage::sum_reported(candidates, self.usage.reasoning_tokens);
        }
    }

    fn assemble_message(&mut self) -> AssistantMessage {
        let content = std::mem::take(&mut self.parts)
            .into_iter()
            .map(|part| match part {
                GeminiPartBuf::TextWithState {
                    text,
                    provider_state,
                } => ContentBlock::TextWithState {
                    text,
                    provider_state,
                },
                GeminiPartBuf::Text { text, .. } => ContentBlock::Text { text },
                GeminiPartBuf::Reasoning { text, signature } => {
                    ContentBlock::Reasoning { text, signature }
                }
                GeminiPartBuf::RedactedReasoning { data } => ContentBlock::RedactedReasoning {
                    data,
                    provider_format: None,
                },
                GeminiPartBuf::FunctionCall(buf) => {
                    let tcid = synthetic_inbound_id(buf.index, self.tool_id_policy);
                    ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId(tcid),
                        tool_name: buf.name,
                        input: buf.args,
                        thought_signature: buf.thought_signature,
                    })
                }
            })
            .collect();
        AssistantMessage {
            message_id: MessageId(String::new()),
            content,
        }
    }
}

fn synthetic_inbound_id(index: u32, policy: ToolIdPolicy) -> String {
    match policy {
        ToolIdPolicy::Passthrough => format!("gemini_call_{index}"),
        ToolIdPolicy::SyntheticIndex => format!("call_{index}"),
    }
}

impl StreamNormalizer for GeminiNormalizer {
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.sse.content_progress()
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.push(chunk)? {
            if payload.trim() == "[DONE]" {
                continue;
            }
            self.handle_chunk(&payload, &mut out)?;
        }
        Ok(out)
    }

    fn progress_marker(&self) -> u64 {
        self.sse.data_records()
    }

    fn last_event_type(&self) -> Option<&str> {
        self.sse.last_event_type()
    }

    fn keepalives_after_content(&self) -> Option<u64> {
        Some(self.sse.keepalives_after_content())
    }

    fn event_type_counts(&self) -> Option<&[(String, u64)]> {
        Some(self.sse.event_type_counts())
    }

    fn liveness_marker(&self) -> Option<u64> {
        Some(self.sse.liveness_records())
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.flush()? {
            if payload.trim() == "[DONE]" {
                continue;
            }
            self.handle_chunk(&payload, &mut out)?;
        }
        if self.emitted_final || !self.started {
            return Ok(out);
        }
        if !self.terminal_observed {
            return Err(NormalizeError::Invalid(
                "Gemini stream truncated: ended before a terminal chunk".into(),
            ));
        }
        self.emitted_final = true;
        let message = self.assemble_message();
        out.extend([
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }),
            NormalizedEvent::Control(ProviderControl::StepFinished {
                step_id: self.step_id,
                usage: self.usage,
                finish_reason: self.finish_reason,
                service_tier_used: self
                    .header_service_tier
                    .clone()
                    .or_else(|| self.body_service_tier.clone()),
                speed_used: None,
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: self.usage,
                finish_reason: self.finish_reason,
                retries_used: 0,
            }),
        ]);
        Ok(out)
    }

    fn observe_response_headers(&mut self, headers: &crate::wire::ResponseHeaders) {
        self.header_service_tier = headers.get(GEMINI_SERVICE_TIER_HEADER).map(str::to_owned);
    }
}

fn map_finish_reason(fr: &str) -> FinishReason {
    match fr {
        "STOP" => FinishReason::Stop,
        "MAX_TOKENS" => FinishReason::Length,
        "MALFORMED_FUNCTION_CALL" | "UNEXPECTED_TOOL_CALL" => FinishReason::ToolCalls,
        "SAFETY" | "RECITATION" | "BLOCKLIST" | "PROHIBITED_CONTENT" => FinishReason::ContentFilter,
        _ => FinishReason::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn normalize_response_values(values: &[Value]) -> Vec<NormalizedEvent> {
        let mut normalizer = GeminiNormalizer::new(StepId(1), ToolIdPolicy::SyntheticIndex);
        let mut events = Vec::new();
        for value in values {
            let record = format!("data: {value}\n\n");
            events.extend(normalizer.push(record.as_bytes()).unwrap());
        }
        events.extend(normalizer.finish().unwrap());
        events
    }

    fn terminal_finish_reasons(events: &[NormalizedEvent]) -> Vec<FinishReason> {
        events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    finish_reason, ..
                })
                | NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) => {
                    Some(*finish_reason)
                }
                _ => None,
            })
            .collect()
    }

    fn legacy_convert_user_parts(blocks: &[ContentBlock]) -> Vec<Value> {
        blocks
            .iter()
            .filter_map(|block| match block {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }
                    if text.is_empty() =>
                {
                    None
                }
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    Some(json!({ "text": text }))
                }
                _ => None,
            })
            .collect()
    }

    fn assistant_tool_call(id: &str, name: &str) -> Message {
        Message::new(
            Role::Assistant,
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(id.to_string()),
                tool_name: name.to_string(),
                input: json!({"query": "broca"}),
                thought_signature: None,
            })],
        )
    }

    fn user_tool_result(id: &str, output: ToolOutput) -> ContentBlock {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.to_string()),
            output,
            is_error: false,
        })
    }

    #[test]
    fn mixed_user_tool_result_and_text_render_in_source_order() {
        let mixed = Message::new(
            Role::User,
            vec![
                user_tool_result(
                    "call_lookup",
                    ToolOutput::Json {
                        value: json!([{"row": 1}, {"row": 2}]),
                    },
                ),
                ContentBlock::Text {
                    text: "Now summarize".into(),
                },
            ],
        );
        let prompt = vec![assistant_tool_call("call_lookup", "lookup"), mixed];
        let name_map = crate::tool_name_alias::ToolNameMap::build(&[], &prompt);

        let contents = convert_contents(
            &prompt,
            &super::super::gemini_frozen_config("model"),
            None,
            &name_map,
        );
        let legacy = legacy_convert_user_parts(&prompt[1].content);

        assert_eq!(legacy, vec![json!({"text": "Now summarize"})]);
        assert_eq!(
            contents[1]["parts"],
            json!([
                {
                    "functionResponse": {
                        "name": "lookup",
                        "response": {"result": [{"row": 1}, {"row": 2}]}
                    }
                },
                {"text": "Now summarize"}
            ])
        );
        assert_ne!(contents[1]["parts"], Value::Array(legacy));
    }

    #[test]
    fn pure_text_user_parts_match_the_legacy_converter_bytes() {
        let blocks = vec![
            ContentBlock::Text {
                text: "first".into(),
            },
            ContentBlock::Text {
                text: String::new(),
            },
            ContentBlock::Text {
                text: "second".into(),
            },
        ];

        assert_eq!(
            serde_json::to_vec(&convert_user_parts(&blocks, &BTreeMap::new())).unwrap(),
            serde_json::to_vec(&legacy_convert_user_parts(&blocks)).unwrap()
        );
    }

    #[test]
    fn mixed_user_tool_result_with_unknown_id_keeps_the_existing_empty_name_fallback() {
        let mixed = Message::new(
            Role::User,
            vec![user_tool_result(
                "unknown_call",
                ToolOutput::Text { text: "ok".into() },
            )],
        );
        let prompt = vec![assistant_tool_call("known_call", "lookup"), mixed];
        let name_map = crate::tool_name_alias::ToolNameMap::build(&[], &prompt);

        let contents = convert_contents(
            &prompt,
            &super::super::gemini_frozen_config("model"),
            None,
            &name_map,
        );

        assert_eq!(
            contents[1]["parts"][0]["functionResponse"],
            json!({"name": "", "response": {"result": "ok"}})
        );
    }

    #[test]
    fn prompt_feedback_safety_without_candidates_is_content_filtered() {
        let events = normalize_response_values(&[json!({
            "promptFeedback": {"blockReason": "SAFETY"}
        })]);

        let message = events.iter().find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                Some(message)
            }
            _ => None,
        });
        assert!(message.is_some_and(|message| message.content.is_empty()));
        assert_eq!(
            terminal_finish_reasons(&events),
            vec![FinishReason::ContentFilter, FinishReason::ContentFilter]
        );
    }

    #[test]
    fn candidate_finish_reason_overrides_earlier_prompt_feedback() {
        let events = normalize_response_values(&[
            json!({"promptFeedback": {"blockReason": "SAFETY"}}),
            json!({
                "candidates": [{
                    "finishReason": "STOP",
                    "content": {"parts": [{"text": "answer"}]}
                }]
            }),
        ]);

        assert_eq!(
            terminal_finish_reasons(&events),
            vec![FinishReason::Stop, FinishReason::Stop]
        );
    }

    #[test]
    fn schema_sanitizer_strips_unsupported_root_keywords() {
        let schema = json!({
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Lookup input",
            "type": "object",
            "description": "Lookup parameters",
            "additionalProperties": false,
            "default": {},
            "examples": [{"query": "rust"}]
        });

        assert_eq!(
            sanitize_gemini_schema(&schema),
            json!({
                "type": "object",
                "description": "Lookup parameters"
            })
        );
    }

    #[test]
    fn schema_sanitizer_recurses_through_nested_schema_containers() {
        let schema = json!({
            "type": "object",
            "properties": {
                "entries": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "title": "Entry",
                        "additionalProperties": false,
                        "properties": {
                            "url": {
                                "type": "string",
                                "format": "uri",
                                "default": "https://example.com"
                            }
                        }
                    }
                },
                "choice": {
                    "anyOf": [
                        {"type": "string", "title": "Text"},
                        {"type": "number", "format": "double", "exclusiveMinimum": true}
                    ]
                },
                "timestamp": {
                    "oneOf": [
                        {"type": "string", "format": "date-time", "examples": []}
                    ]
                }
            }
        });

        assert_eq!(
            sanitize_gemini_schema(&schema),
            json!({
                "type": "object",
                "properties": {
                    "entries": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "url": {"type": "string"}
                            }
                        }
                    },
                    "choice": {
                        "anyOf": [
                            {"type": "string"},
                            {"type": "number", "format": "double"}
                        ]
                    },
                    "timestamp": {
                        "oneOf": [
                            {"type": "string", "format": "date-time"}
                        ]
                    }
                }
            })
        );
    }

    #[test]
    fn schema_sanitizer_maps_nullable_type_arrays() {
        let schema = json!({
            "type": ["string", "null"],
            "nullable": false,
            "format": "enum",
            "enum": ["ready", "done"]
        });

        assert_eq!(
            sanitize_gemini_schema(&schema),
            json!({
                "type": "string",
                "nullable": true,
                "format": "enum",
                "enum": ["ready", "done"]
            })
        );
    }

    #[test]
    fn schema_sanitizer_degrades_references_without_dereferencing() {
        let schema = json!({
            "$ref": "#/$defs/address",
            "description": "A postal address",
            "$defs": {
                "address": {"type": "string"}
            }
        });

        assert_eq!(
            sanitize_gemini_schema(&schema),
            json!({
                "type": "object",
                "description": "A postal address"
            })
        );
    }

    #[test]
    fn schema_sanitizer_preserves_clean_schema_bytes() {
        let schema = json!({
            "type": "object",
            "description": "Search input",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Words to find",
                    "minLength": 1,
                    "maxLength": 100,
                    "pattern": "^[a-z]+$"
                },
                "limit": {
                    "type": "integer",
                    "format": "int32",
                    "minimum": 1,
                    "maximum": 20
                },
                "tags": {
                    "type": "array",
                    "items": {"type": "string"},
                    "minItems": 1,
                    "maxItems": 5
                }
            },
            "required": ["query"]
        });

        assert_eq!(
            serde_json::to_vec(&sanitize_gemini_schema(&schema)).unwrap(),
            serde_json::to_vec(&schema).unwrap()
        );
    }
}
