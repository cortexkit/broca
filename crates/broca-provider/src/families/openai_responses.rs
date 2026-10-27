//! The OpenAI Responses wire family — `/v1/responses`.
//!
//! Grammar confirmed against OpenAI OpenAPI-derived SDK types (openai-python
//! `response_create_params`, `response_input_param`, `function_tool_param`) and the
//! streaming event union (`response_stream_event`):
//!  - request: `model`, `input` (typed items: `message`, `function_call`,
//!    `function_call_output`), `stream`, `max_output_tokens`, flat `tools`
//!    `{type:"function", name, description, parameters, strict}`, `tool_choice`
//!    (`auto`/`none`/`required` or `{type:"function", name}`), optional top-level
//!    `instructions` and `reasoning` (o-series / gpt-5 reasoning controls);
//!  - streaming SSE: each `data:` JSON object has a `type` discriminator, including
//!    `response.created`, `response.output_item.added`, `response.output_text.delta`,
//!    `response.function_call_arguments.delta`, terminal `response.completed`, and
//!    in-band failures (`error` / `response.failed`) (usage on `response.usage` with
//!    `input_tokens` / `output_tokens` and
//!    `input_tokens_details.cached_tokens` / `cache_write_tokens`, both inside
//!    `input_tokens`). Sources: OpenAI API reference / streaming guide / prompt-caching
//!    guide, openai-python `responses/*` types.

use std::borrow::Cow;
use std::collections::BTreeMap;

use broca_protocol::{
    content::{
        AssistantMessage, ContentBlock, EncryptedReasoningFormat, FinishReason, Message, Role,
        ToolCall, ToolOutput,
    },
    display::DisplayEvent,
    error::{ErrorClass, ProviderError},
    ids::{MessageId, ToolCallId},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, Tool, ToolChoice},
    usage::Usage,
    StepId,
};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Map, Value};

use crate::families::{apply_generation_controls, merge_inclusive_input_tokens_with_writes};
use crate::policy::{ReasoningPolicy, SystemPromptPolicy};
use crate::spec::WireFamilyId;
use crate::sse::SseDecoder;
use crate::wire::{
    AuthHandle, CacheClass, FrozenRenderConfig, HttpMethod, HttpRequest, NormalizeError,
    StreamNormalizer, WireFamily,
};

/// The default API base (no `base_url` override). The family's `/responses` path is
/// appended; a base_url override (an OpenAI-compatible host, or a mock in tests)
/// replaces only the base. base_url lives in the endpoint, not the rendered body, so
/// it's outside the cache/resume hash — overriding it never changes a golden.
const DEFAULT_BASE: &str = "https://api.openai.com/v1";

pub struct OpenAiResponses;

impl WireFamily for OpenAiResponses {
    fn warm_body(&self, _frozen: &FrozenRenderConfig, base: &[u8]) -> Result<Vec<u8>, String> {
        crate::cache_warm::responses_warm_body(base)
    }

    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        finish_platform_body(frozen, build_request_body(frozen, opts))
    }

    fn shape_with_media(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        payloads: &[crate::media_render::InlineMedia<'_>],
    ) -> Value {
        let media = crate::media_render::MediaRender { frozen, payloads };
        let body = build_prepared_request_body_with_media(
            frozen,
            opts,
            frozen.media.as_ref().map(|_| &media),
        )
        .body;
        finish_platform_body(frozen, body)
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        let base = frozen.base_url.as_deref().unwrap_or(DEFAULT_BASE);
        let url = format!("{}/responses", base.trim_end_matches('/'));
        let mut headers = crate::opencode_go::responses_request_headers(frozen);
        headers.extend([
            ("content-type".to_string(), "application/json".to_string()),
            ("accept".to_string(), "application/json".to_string()),
        ]);
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
        Box::new(
            OpenAiResponsesNormalizer::new(step_id)
                .with_text_state(frozen.provider_text_state)
                // The body renders `max_output_tokens` from this same value.
                .with_max_output_tokens(frozen.max_output_tokens),
        )
    }
}

/// Build the base OpenAI `/responses` request body. The ChatGPT (codex) family
/// starts from this base body too, then applies its backend-specific fields; platform
/// cache hints are applied by `OpenAiResponses::shape` so the codex backend can keep
/// its own run-session cache key as the single cache-key authority.
pub(crate) fn build_request_body(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    build_prepared_request_body(frozen, opts).body
}

fn finish_platform_body(frozen: &FrozenRenderConfig, mut body: Value) -> Value {
    apply_prompt_cache_hints(&mut body, frozen);
    if let Some(cache_key) = &frozen.prompt_cache_key {
        body["prompt_cache_key"] = Value::String(cache_key.clone());
    }
    body
}

/// The shared Responses body together with the inputs it was rendered from, so the ChatGPT
/// wrapper can finish the body without preparing the request or planning effort a second
/// time: each of those clones the prompt or parses every episode's frozen config.
pub(crate) struct PreparedRequestBody<'a> {
    pub body: Value,
    /// The request as structured output prepares it.
    pub prepared: Cow<'a, CallOptions>,
    /// The ChatGPT effort anchor planned over `prepared`; `None` when no plan applies.
    pub effort_anchor: Option<String>,
}

pub(crate) fn build_prepared_request_body<'a>(
    frozen: &FrozenRenderConfig,
    opts: &'a CallOptions,
) -> PreparedRequestBody<'a> {
    build_prepared_request_body_with_media(frozen, opts, None)
}

pub(crate) fn build_prepared_request_body_with_media<'a>(
    frozen: &FrozenRenderConfig,
    opts: &'a CallOptions,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> PreparedRequestBody<'a> {
    // A run with structured output adds its submit tool or schema instructions here; a run
    // without renders from `opts` unchanged.
    let prepared = crate::structured_output::prepare(frozen, opts);
    // The plan reads only the episode history, which the compaction view below leaves as it
    // is, so planning over the prepared request gives the same plan as over that view.
    let (effort_anchor, updates) =
        match super::openai_responses_chatgpt::effort_plan(frozen, prepared.as_ref()) {
            Some((anchor, updates)) => (Some(anchor), updates),
            None => (None, Vec::new()),
        };
    let body = render_prepared_body(frozen, prepared.as_ref(), &updates, media);
    PreparedRequestBody {
        body,
        prepared,
        effort_anchor,
    }
}

fn render_prepared_body(
    frozen: &FrozenRenderConfig,
    prepared: &CallOptions,
    updates: &[super::openai_responses_chatgpt::EffortUpdate],
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Value {
    let view = crate::families::compaction_view(frozen.family_id, prepared);
    let opts = view.as_ref();
    let name_map = crate::tool_name_alias::ToolNameMap::build(&opts.tools, &opts.prompt);
    let input = convert_input_items(
        &opts.prompt,
        frozen.system_prompt.clone(),
        &name_map,
        updates,
        &ReplayPolicy {
            target_model: opts.target_model.as_ref(),
            target_family: frozen.family_id,
            provider_text_state: frozen.provider_text_state,
            stateless: frozen.static_options.get("store") == Some(&Value::Bool(false)),
        },
        media,
    );

    let mut body = json!({
        "model": frozen.wire_model_id,
        "stream": true,
        "input": input,
    });
    body[frozen.token_limit_field.key()] = json!(frozen.max_output_tokens);

    if let Some(instructions) = instructions_from_policy(&opts.prompt, frozen.system_prompt.clone())
    {
        body["instructions"] = Value::String(instructions);
    }

    if let ReasoningPolicy::OpenAiResponsesReasoning { effort, summary } = &frozen.reasoning {
        let mut reasoning = Map::new();
        reasoning.insert("effort".into(), Value::String(effort.clone()));
        if let Some(s) = summary {
            reasoning.insert("summary".into(), Value::String(s.clone()));
        }
        body["reasoning"] = Value::Object(reasoning);
    }
    // A stateless continuation needs the encrypted reasoning item returned by the provider.
    // We request it only when reasoning is configured, so request bytes without reasoning stay
    // unchanged. (The Codex CLI sends this include on every request, unconditionally.) On the
    // ChatGPT backend the gap doesn't lose anything: in 3,375 production steps without a
    // variant, every step that reasoned still came back with its encrypted reasoning item.
    // ChatGPT's optional `reasoning_effort` configuration is applied by the wrapper after this
    // shared body is built and must enable the same include field.
    if matches!(
        &frozen.reasoning,
        ReasoningPolicy::OpenAiResponsesReasoning { .. }
    ) || (frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt
        && frozen.reasoning_effort.is_some())
    {
        body["include"] = json!(["reasoning.encrypted_content"]);
    }

    if !opts.tools.is_empty() {
        body["tools"] = Value::Array(
            opts.tools
                .iter()
                .map(|tool| convert_tool(tool, &name_map, frozen.responses_tools_non_strict))
                .collect(),
        );
        body["tool_choice"] = convert_tool_choice(&opts.tool_choice, &name_map);
    }
    // `/v1/responses` accepts `temperature` + `top_p`, but no stop field, so
    // `stop_sequences` stay absent on this wire shape.
    apply_generation_controls(
        body.as_object_mut()
            .expect("OpenAI Responses request body must stay a JSON object"),
        opts,
        "temperature",
        "top_p",
        None,
    );
    // The caller's service tier, as the admitted provider maps it. Absent (no tier requested,
    // or one that needs no field) leaves the body exactly as before the field existed.
    if let Some(service_tier) = &frozen.service_tier {
        body["service_tier"] = Value::String(service_tier.clone());
    }
    crate::structured_output::finish(
        frozen,
        &mut body,
        name_map.wire_name(broca_protocol::structured_output::SUBMIT_RESULT_TOOL),
    );
    // Provider-required request flags are frozen at admission, so enabling stateless reasoning
    // for a host does not change the bytes of runs admitted before that policy existed.
    if let Some(options) = frozen.static_options.as_object() {
        for (key, value) in options {
            body[key] = value.clone();
        }
    }
    body
}

fn apply_prompt_cache_hints(body: &mut Value, frozen: &FrozenRenderConfig) {
    let Some(cache) = &frozen.cache else {
        return;
    };
    let CacheClass::Hints {
        cache_key,
        retention,
    } = &cache.class
    else {
        return;
    };
    body["prompt_cache_key"] = Value::String(cache_key.clone());
    if let Some(retention) = retention {
        body["prompt_cache_retention"] = Value::String(
            crate::cache_hints::openai_prompt_cache_retention_wire_value(retention).to_string(),
        );
    }
}

fn instructions_from_policy(prompt: &[Message], policy: SystemPromptPolicy) -> Option<String> {
    if !matches!(policy, SystemPromptPolicy::InstructionsField) {
        return None;
    }
    let mut parts = Vec::new();
    for m in prompt {
        if m.role == Role::System {
            let t = text_content(&m.content);
            if !t.is_empty() {
                parts.push(t);
            }
        }
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join("\n"))
    }
}

/// `target_model` and `target_family` fence provider-owned state to its compatible destination.
/// The recorded `provider_text_state` switch enables assistant phase metadata; `stateless`
/// keeps complete encrypted reasoning items instead of the legacy continuation shape.
struct ReplayPolicy<'a> {
    target_model: Option<&'a broca_protocol::ModelHandle>,
    target_family: WireFamilyId,
    provider_text_state: bool,
    stateless: bool,
}

/// Convert a completed assistant message into the same input items used by full-history
/// requests, so continuation compares those items without rendering the large prompt again.
#[cfg(feature = "live")]
pub(crate) fn completed_output_as_input(
    frozen: &FrozenRenderConfig,
    target_model: Option<&broca_protocol::ModelHandle>,
    name_map: &crate::tool_name_alias::ToolNameMap,
    assistant: &broca_protocol::AssistantMessage,
) -> Vec<Value> {
    let mut message = Message::new(Role::Assistant, assistant.content.clone());
    message.origin = target_model.cloned();
    let stateless = frozen.static_options.get("store") == Some(&Value::Bool(false));
    let mut items = convert_assistant_items(
        &message,
        name_map,
        &ReplayPolicy {
            target_model,
            target_family: frozen.family_id,
            provider_text_state: frozen.provider_text_state,
            stateless,
        },
    );
    if frozen.chatgpt_wire.is_some() {
        crate::chatgpt_wire::typed_message_content(&mut items);
    }
    items
}

fn convert_input_items(
    prompt: &[Message],
    system_policy: SystemPromptPolicy,
    name_map: &crate::tool_name_alias::ToolNameMap,
    updates: &[super::openai_responses_chatgpt::EffortUpdate],
    replay: &ReplayPolicy<'_>,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Vec<Value> {
    let skip_system = matches!(system_policy, SystemPromptPolicy::InstructionsField);
    let mut out = Vec::new();
    let mut updates = updates.iter().peekable();
    let has_ordinals = prompt.iter().any(|message| message.ordinal.is_some());
    for (index, message) in prompt.iter().enumerate() {
        // Synthetic transform rows have no durable position and must not move an episode update.
        let ordinal = message
            .ordinal
            .or_else(|| (!has_ordinals).then_some(index as u64));
        if let Some(ordinal) = ordinal {
            while updates
                .peek()
                .is_some_and(|update| update.before <= ordinal as usize)
            {
                let update = updates.next().expect("peeked update");
                append_effort_update(
                    &mut out,
                    update,
                    message.role == Role::User && ordinal as usize == update.before,
                );
            }
        }
        if skip_system && message.role == Role::System {
            continue;
        }
        match message.role {
            Role::System | Role::User => out.push(convert_easy_message(message, media)),
            Role::Assistant => out.extend(convert_assistant_items(message, name_map, replay)),
            Role::Tool => out.extend(convert_tool_output_items(message, media)),
        }
    }
    for update in updates {
        append_effort_update(&mut out, update, false);
    }
    out
}

fn append_effort_update(
    out: &mut Vec<Value>,
    update: &super::openai_responses_chatgpt::EffortUpdate,
    user_follows: bool,
) {
    out.push(json!({"type": "configuration_update", "reasoning": {"effort": update.effort}}));
    if update.synthetic_user || !user_follows {
        out.push(json!({"type": "message", "role": "user", "content": "Continue with the updated reasoning effort."}));
    }
}

fn reasoning_format_matches_family(format: EncryptedReasoningFormat, family: WireFamilyId) -> bool {
    matches!(
        (format, family),
        (
            EncryptedReasoningFormat::OpenAiResponses,
            WireFamilyId::OpenAiResponses
        ) | (
            EncryptedReasoningFormat::OpenAiResponsesChatgpt,
            WireFamilyId::OpenAiResponsesChatgpt
        )
    )
}

/// A frozen `store:false` request echoes the complete reasoning item, as xAI requires. Configs
/// without that flag retain the previous shape: response ids and metadata are removed, summary
/// and ciphertext are preserved, and `content:null` is added. Keeping that branch reproduces
/// the input of runs recorded before stateless continuation was enabled.
fn reasoning_request_item(raw_response_item: &str, stateless: bool) -> Option<Value> {
    let response: Value = serde_json::from_str(raw_response_item).ok()?;
    if response.get("type").and_then(Value::as_str) != Some("reasoning") {
        return None;
    }
    let encrypted_content = response.get("encrypted_content")?.as_str()?;
    if stateless {
        return Some(response);
    }
    let summary = response
        .get("summary")
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    Some(json!({
        "type": "reasoning",
        "summary": summary,
        "content": Value::Null,
        "encrypted_content": encrypted_content,
    }))
}

// Preserve user block boundaries as input_text parts, retaining scalars for single text blocks.
fn user_text_content(
    blocks: &[ContentBlock],
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Value {
    if blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::Media(_)))
    {
        return Value::Array(
            blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                        Some(json!({"type":"input_text", "text":text}))
                    }
                    ContentBlock::Media(reference) => {
                        Some(responses_media_block(reference, media, false))
                    }
                    ContentBlock::Reasoning { .. }
                    | ContentBlock::RedactedReasoning { .. }
                    | ContentBlock::ToolCall(_)
                    | ContentBlock::ToolResult(_) => None,
                    // Anthropic's server-side fallback handoff means nothing to this family's API: dropped.
                    ContentBlock::ProviderFallback { .. } => None,
                })
                .collect(),
        );
    }
    let texts: Vec<&str> = blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect();
    if texts.len() > 1 {
        Value::Array(
            texts
                .into_iter()
                .map(|text| json!({"type": "input_text", "text": text}))
                .collect(),
        )
    } else {
        Value::String(text_content(blocks))
    }
}

fn convert_easy_message(
    m: &Message,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Value {
    let role = match m.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "user",
    };
    json!({
        "type": "message",
        "role": role,
        "content": if m.role == Role::User {
            user_text_content(&m.content, media)
        } else {
            Value::String(text_content(&m.content))
        },
    })
}

fn convert_assistant_items(
    message: &Message,
    name_map: &crate::tool_name_alias::ToolNameMap,
    replay: &ReplayPolicy<'_>,
) -> Vec<Value> {
    fn flush_text(out: &mut Vec<Value>, text: &mut String) {
        if text.is_empty() {
            return;
        }
        out.push(json!({
            "type": "message",
            "role": "assistant",
            "content": std::mem::take(text),
        }));
    }

    let echo_safe = crate::families::signed_reasoning_is_echo_safe(
        message.origin.as_ref(),
        replay.target_model,
    );
    let mut out = Vec::new();
    let mut text = String::new();
    for block in &message.content {
        match block {
            ContentBlock::Media(media) => {
                text.push_str(&broca_protocol::content::media_not_shown_marker(
                    media,
                    broca_protocol::MediaMarkerReason::Carrier,
                ))
            }
            ContentBlock::Text { text: part } => text.push_str(part),
            ContentBlock::TextWithState {
                text: part,
                provider_state,
            } => {
                if replay.provider_text_state {
                    flush_text(&mut out, &mut text);
                    let mut item = json!({"type":"message", "role":"assistant", "content":part});
                    if super::text_state_is_echo_safe(
                        message,
                        replay.target_model,
                        provider_state.format,
                        replay.target_family,
                    ) {
                        if let Ok(state) = serde_json::from_str::<Value>(&provider_state.data) {
                            if let Some(phase) = state.get("phase") {
                                item["phase"] = phase.clone();
                            }
                        }
                    }
                    // Each block is one output item; never merge adjacent items with equal phases.
                    if !part.is_empty() || item.get("phase").is_some() {
                        out.push(item);
                    }
                } else {
                    text.push_str(part);
                }
            }
            ContentBlock::RedactedReasoning {
                data,
                provider_format: Some(provider_format),
            } if echo_safe
                && reasoning_format_matches_family(*provider_format, replay.target_family) =>
            {
                flush_text(&mut out, &mut text);
                if let Some(item) = reasoning_request_item(data, replay.stateless) {
                    out.push(item);
                }
            }
            ContentBlock::ToolCall(call) => {
                flush_text(&mut out, &mut text);
                out.push(json!({
                    "type": "function_call",
                    "call_id": call.tool_call_id.0,
                    "name": name_map.wire_name(&call.tool_name),
                    "arguments": serde_json::to_string(&call.input)
                        .unwrap_or_else(|_| "{}".into()),
                }));
            }
            ContentBlock::Reasoning { .. }
            | ContentBlock::RedactedReasoning { .. }
            | ContentBlock::ToolResult(_) => {}
            // Anthropic's server-side fallback handoff means nothing to this family's API: dropped.
            ContentBlock::ProviderFallback { .. } => {}
        }
    }
    flush_text(&mut out, &mut text);
    out
}

fn convert_tool_output_items(
    m: &Message,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Vec<Value> {
    let mut out = Vec::new();
    for b in &m.content {
        if let ContentBlock::ToolResult(tr) = b {
            out.push(json!({
                "type": "function_call_output",
                "call_id": tr.tool_call_id.0,
                "output": tool_output_content(&tr.output, media),
            }));
        }
    }
    out
}

fn tool_output_content(
    out: &ToolOutput,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Value {
    if let (ToolOutput::Parts { parts }, Some(media)) = (out, media) {
        return Value::Array(
            parts
                .iter()
                .map(|part| match part {
                    broca_protocol::ToolOutputPart::Text { text } => {
                        super::openai_media::Grammar::Responses.text(text)
                    }
                    broca_protocol::ToolOutputPart::Media { media: reference } => {
                        responses_media_block(reference, Some(media), true)
                    }
                })
                .collect(),
        );
    }
    Value::String(tool_output_text(out))
}

fn responses_media_block(
    reference: &broca_protocol::MediaRef,
    media: Option<&crate::media_render::MediaRender<'_>>,
    tool: bool,
) -> Value {
    let grammar = super::openai_media::Grammar::Responses;
    grammar
        .block(
            reference,
            media,
            tool,
            crate::media_profile::MediaCarrier::Native,
        )
        .unwrap_or_else(|reason| grammar.marker(reference, reason))
}

fn text_content(blocks: &[ContentBlock]) -> String {
    let mut s = String::new();
    for b in blocks {
        if let ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } = b {
            s.push_str(text);
        }
    }
    s
}

fn convert_tool(
    t: &Tool,
    name_map: &crate::tool_name_alias::ToolNameMap,
    non_strict: bool,
) -> Value {
    // Live ChatGPT/Codex probes with read and zoom schemas showed that omitting `strict`
    // made every property required, including optional parameters. OpenAI's function-calling
    // guide says: "To opt out of strict mode in Responses and keep non-strict, best-effort
    // function calling, explicitly set `strict: false`." The admission-time flag keeps
    // pre-flag runs rendering without the field rather than changing their resumed bytes.
    let mut tool = json!({
        "type": "function",
        "name": name_map.wire_name(&t.name),
        "description": t.description,
        "parameters": t.input_schema,
    });
    if non_strict {
        tool["strict"] = json!(false);
    }
    tool
}

pub(crate) fn convert_tool_choice(
    tc: &ToolChoice,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Value {
    match tc {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Required => json!("required"),
        ToolChoice::Tool { name } => {
            json!({ "type": "function", "name": name_map.wire_name(name) })
        }
    }
}

fn tool_output_text(out: &ToolOutput) -> String {
    match out {
        ToolOutput::Parts { parts } => broca_protocol::content::media_parts_text(
            parts,
            broca_protocol::MediaMarkerReason::Carrier,
        ),
        ToolOutput::Text { text } => text.clone(),
        ToolOutput::Json { value } => value.to_string(),
        ToolOutput::Denied { reason } => format!("denied: {reason}"),
        ToolOutput::Error { message } => message.clone(),
    }
}

#[derive(Default)]
struct ToolCallBuf {
    call_id: String,
    name: String,
    arguments: String,
}

#[derive(Deserialize)]
struct ResponsesTerminalEvent {
    #[serde(rename = "type")]
    kind: Option<ResponsesTerminalKind>,
    response: Option<ResponsesTerminalResponse>,
}

impl ResponsesTerminalEvent {
    fn response(self) -> Option<ResponsesTerminalResponse> {
        matches!(
            self.kind,
            Some(ResponsesTerminalKind::Completed | ResponsesTerminalKind::Incomplete)
        )
        .then(|| self.response.unwrap_or_default())
    }
}

#[derive(Deserialize)]
enum ResponsesTerminalKind {
    #[serde(rename = "response.completed")]
    Completed,
    #[serde(rename = "response.incomplete")]
    Incomplete,
    #[serde(other)]
    Other,
}

#[derive(Default, Deserialize)]
struct ResponsesTerminalResponse {
    id: Option<String>,
    status: Option<String>,
    usage: Option<ResponsesTerminalUsage>,
    incomplete_details: Option<ResponsesIncompleteDetails>,
    service_tier: Option<String>,
}

#[derive(Deserialize)]
struct ResponsesIncompleteDetails {
    reason: Option<String>,
}

#[derive(Deserialize)]
struct ResponsesTerminalUsage {
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    input_tokens_details: Option<ResponsesInputTokenDetails>,
    output_tokens_details: Option<ResponsesOutputTokenDetails>,
}

#[derive(Deserialize)]
struct ResponsesInputTokenDetails {
    cached_tokens: Option<u64>,
    /// Absent on older responses (the July 2026 Codex captures carry only `cached_tokens`);
    /// a missing key must stay `None` rather than become a reported zero.
    cache_write_tokens: Option<u64>,
}

#[derive(Deserialize)]
struct ResponsesOutputTokenDetails {
    reasoning_tokens: Option<u64>,
}

pub struct OpenAiResponsesNormalizer {
    display_parts: crate::display_parts::DisplayParts,
    arguments_guard: crate::wire::ToolArgsGuard,
    completed_items: std::collections::HashSet<u64>,
    completed_text_parts: std::collections::HashSet<(u64, u64)>,
    refusal_completed: bool,
    private_error_diagnostics: bool,
    step_id: StepId,
    reasoning_format: EncryptedReasoningFormat,
    sse: SseDecoder,
    response_id: Option<MessageId>,
    /// Keyed by (output_index, content_index). ONE OUTPUT ITEM MAY CARRY SEVERAL TEXT
    /// PARTS: `content_index` identifies a part within a message and a `done` event
    /// finalises exactly that part. Keying on output_index alone made each `done`
    /// overwrite the whole item, so a two-part message kept only the LAST part while the
    /// display lane had already shown both -- a durable answer disagreeing with what the
    /// user watched arrive.
    text_items: BTreeMap<(u64, u64), String>,
    provider_text_state: bool,
    text_states: BTreeMap<u64, String>,
    refusal: String,
    refusal_output_index: Option<u64>,
    saw_refusal: bool,
    reasoning_items: BTreeMap<u64, String>,
    /// The (output_index, summary_index) of the last reasoning-summary part shown on the display
    /// lane, so the first delta of the next part starts after a blank line instead of running on.
    last_summary_part: Option<(u64, u64)>,
    tool_calls: BTreeMap<u64, ToolCallBuf>,
    usage: Usage,
    finish_reason: FinishReason,
    /// The terminal `incomplete_details.reason`, verbatim. Only `max_output_tokens` proves an
    /// output-ceiling cut; a bare `status: incomplete` does not say why.
    incomplete_reason: Option<String>,
    /// The output cap the request was rendered with, when the body carries one.
    max_output_tokens: Option<u32>,
    started: bool,
    completed: bool,
    /// `response.service_tier` from the TERMINAL response (completed or incomplete), verbatim.
    /// Only the terminal one is read: `response.created` and `response.in_progress` echo the
    /// requested value (`auto` in recorded codex streams) before the tier is decided, while
    /// the terminal response reports the tier that served and billed the request.
    service_tier_used: Option<String>,
}

impl OpenAiResponsesNormalizer {
    pub fn new(step_id: StepId) -> Self {
        Self::new_for_format(step_id, EncryptedReasoningFormat::OpenAiResponses)
    }

    pub(crate) fn new_for_format(
        step_id: StepId,
        reasoning_format: EncryptedReasoningFormat,
    ) -> Self {
        Self {
            display_parts: Default::default(),
            arguments_guard: Default::default(),
            completed_items: Default::default(),
            completed_text_parts: Default::default(),
            refusal_completed: false,
            step_id,
            private_error_diagnostics: false,
            reasoning_format,
            sse: SseDecoder::for_content_family(crate::content_progress::ContentFamily::Responses),
            response_id: None,
            text_items: BTreeMap::new(),
            provider_text_state: false,
            text_states: BTreeMap::new(),
            refusal: String::new(),
            refusal_output_index: None,
            saw_refusal: false,
            reasoning_items: BTreeMap::new(),
            last_summary_part: None,
            tool_calls: BTreeMap::new(),
            usage: Usage::default(),
            finish_reason: FinishReason::Stop,
            incomplete_reason: None,
            max_output_tokens: None,
            started: false,
            completed: false,
            service_tier_used: None,
        }
    }

    /// Record the rendered output cap so an output-ceiling error can name it. The ChatGPT codex
    /// backend strips the field from its body, so that family leaves it unknown.
    pub fn with_max_output_tokens(mut self, max_output_tokens: u32) -> Self {
        self.max_output_tokens = Some(max_output_tokens);
        self
    }

    pub(crate) fn with_text_state(mut self, enabled: bool) -> Self {
        self.provider_text_state = enabled;
        self
    }

    fn capture_text_state(&mut self, index: u64, item: &Value) {
        if self.provider_text_state
            && item.get("type").and_then(Value::as_str) == Some("message")
            && item.get("phase").is_some()
        {
            let mut state = serde_json::Map::new();
            for key in ["id", "phase"] {
                if let Some(value) = item.get(key) {
                    state.insert(key.into(), value.clone());
                }
            }
            self.text_states
                .insert(index, Value::Object(state).to_string());
        }
    }

    fn handle_event(
        &mut self,
        payload: &str,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        if self.completed {
            return Ok(());
        }
        // Terminal Responses envelopes repeat large request and output fields. A cheap byte scan
        // selects their typed parser without making every small delta pay for a second JSON parse;
        // unusual valid discriminator spellings fall through to the existing generic path.
        //
        // The lifecycle events response.created and response.in_progress echo the same bulk and do
        // NOT take this path: they fall through and build a full owned DOM, of which created keeps
        // only response.id and in_progress keeps nothing. On a real Codex turn that is ~38 KB each,
        // so ~76 KB parsed and dropped per model step. Left as-is because it costs well under a
        // millisecond against a step measured in seconds; extending the typed path to cover them
        // is the obvious change if this file is open anyway, and must keep the created-before-delta
        // control ordering and fall back to the generic parser rather than skipping malformed JSON.
        if payload.contains("response.completed") || payload.contains("response.incomplete") {
            let response = serde_json::from_str::<ResponsesTerminalEvent>(payload)
                .ok()
                .and_then(ResponsesTerminalEvent::response);
            if let Some(response) = response {
                return self.handle_terminal_response(response, out);
            }
        }

        let v: Value =
            serde_json::from_str(payload).map_err(|e| NormalizeError::Invalid(e.to_string()))?;
        let event_type = v.get("type").and_then(Value::as_str).unwrap_or_default();

        if !self.started && event_type == "response.created" {
            self.started = true;
            if let Some(id) = v
                .get("response")
                .and_then(|r| r.get("id"))
                .and_then(Value::as_str)
            {
                self.response_id = Some(MessageId(id.to_owned()));
            }
            out.push(NormalizedEvent::Control(ProviderControl::StreamStart {
                warnings: Vec::new(),
            }));
            out.push(NormalizedEvent::Control(ProviderControl::StepStarted {
                step_id: self.step_id,
            }));
        }

        match event_type {
            "error" => {
                out.push(NormalizedEvent::Control(ProviderControl::Error {
                    error: response_stream_error(&v, false, self.private_error_diagnostics),
                    retries_used: 0,
                }));
            }
            "response.failed" => {
                out.push(NormalizedEvent::Control(ProviderControl::Error {
                    error: response_stream_error(&v, true, self.private_error_diagnostics),
                    retries_used: 0,
                }));
            }
            "response.output_text.delta" => {
                let index = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                let delta = v.get("delta").and_then(Value::as_str).unwrap_or("");
                if !delta.is_empty() {
                    let part = v.get("content_index").and_then(Value::as_u64).unwrap_or(0);
                    self.text_items
                        .entry((index, part))
                        .or_default()
                        .push_str(delta);
                    let mid = self
                        .response_id
                        .clone()
                        .unwrap_or_else(|| MessageId(String::new()));
                    self.display_parts.delta(
                        (index, 1),
                        DisplayEvent::TextDelta {
                            mid: None,
                            part: None,
                            message_id: mid,
                            tool_call_id: None,
                            delta: delta.to_owned(),
                        },
                        "openai_responses",
                        self.step_id,
                        out,
                    );
                }
            }
            // The finalized text is authoritative over streamed fragments. Do not re-emit it
            // as display progress because clients have already received the delta lane.
            "response.output_text.done" => {
                let index = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                let part = v.get("content_index").and_then(Value::as_u64).unwrap_or(0);
                self.completed_text_parts.insert((index, part));
                if let Some(text) = v.get("text").and_then(Value::as_str) {
                    self.text_items.insert((index, part), text.to_owned());
                    if !text.is_empty() {
                        self.display_parts.reserve(
                            (index, 1),
                            "openai_responses",
                            self.step_id,
                            out,
                        );
                    }
                }
            }
            // Reasoning summaries are shown live as reasoning text, the way Anthropic thinking
            // deltas are, so a long reasoning phase visibly progresses before the answer. This is
            // display only: the durable record of the summary is the complete reasoning item kept
            // from `response.output_item.done`, which is echoed back to the provider unchanged.
            // `session.read` returns only stored blocks, so it never shows a readable copy.
            "response.reasoning_summary_text.delta" => {
                let delta = v.get("delta").and_then(Value::as_str).unwrap_or("");
                if !delta.is_empty() {
                    let part = (
                        v.get("output_index").and_then(Value::as_u64).unwrap_or(0),
                        v.get("summary_index").and_then(Value::as_u64).unwrap_or(0),
                    );
                    let mut text = String::with_capacity(delta.len() + 2);
                    if self.last_summary_part.is_some_and(|last| last != part) {
                        text.push_str("\n\n");
                    }
                    self.last_summary_part = Some(part);
                    text.push_str(delta);
                    let mid = self
                        .response_id
                        .clone()
                        .unwrap_or_else(|| MessageId(String::new()));
                    self.display_parts.delta(
                        (part.0, 0),
                        DisplayEvent::ReasoningDelta {
                            mid: None,
                            part: None,
                            message_id: mid,
                            delta: text,
                        },
                        "openai_responses",
                        self.step_id,
                        out,
                    );
                }
            }
            "response.refusal.delta" => {
                self.saw_refusal = true;
                if let Some(index) = v.get("output_index").and_then(Value::as_u64) {
                    self.refusal_output_index = Some(index);
                }
                let delta = v.get("delta").and_then(Value::as_str).unwrap_or("");
                if !delta.is_empty() {
                    self.refusal.push_str(delta);
                    let mid = self
                        .response_id
                        .clone()
                        .unwrap_or_else(|| MessageId(String::new()));
                    self.display_parts.delta(
                        (self.refusal_output_index.unwrap_or(0), 2),
                        DisplayEvent::TextDelta {
                            mid: None,
                            part: None,
                            message_id: mid,
                            tool_call_id: None,
                            delta: delta.to_owned(),
                        },
                        "openai_responses",
                        self.step_id,
                        out,
                    );
                }
            }
            // Refusal deltas are display progress; the done event is the authoritative reply.
            // Replacing here prevents duplicated or damaged deltas from corrupting durable text.
            "response.refusal.done" => {
                self.refusal_completed = true;
                self.saw_refusal = true;
                if let Some(index) = v.get("output_index").and_then(Value::as_u64) {
                    self.refusal_output_index = Some(index);
                }
                if let Some(refusal) = v.get("refusal").and_then(Value::as_str) {
                    self.refusal = refusal.to_owned();
                    if !refusal.is_empty() {
                        self.display_parts.reserve(
                            (self.refusal_output_index.unwrap_or(0), 2),
                            "openai_responses",
                            self.step_id,
                            out,
                        );
                    }
                }
            }
            "response.output_item.added" => {
                if let Some(item) = v.get("item") {
                    if let Some(kind) = match item.get("type").and_then(Value::as_str) {
                        Some("reasoning") => Some(0),
                        Some("function_call") => Some(3),
                        _ => None,
                    } {
                        let index = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                        self.display_parts.reserve(
                            (index, kind),
                            "openai_responses",
                            self.step_id,
                            out,
                        );
                    }
                    self.capture_text_state(
                        v.get("output_index").and_then(Value::as_u64).unwrap_or(0),
                        item,
                    );
                    let index = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                    if self.text_states.contains_key(&index) {
                        self.display_parts.reserve(
                            (index, 1),
                            "openai_responses",
                            self.step_id,
                            out,
                        );
                    }
                    if item.get("type").and_then(Value::as_str) == Some("function_call") {
                        let idx = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                        let buf = self.tool_calls.entry(idx).or_default();
                        if let Some(id) = item.get("call_id").and_then(Value::as_str) {
                            buf.call_id = id.to_owned();
                        }
                        if let Some(name) = item.get("name").and_then(Value::as_str) {
                            buf.name = name.to_owned();
                        }
                        if let Some(args) = item.get("arguments").and_then(Value::as_str) {
                            buf.arguments = args.to_owned();
                            self.arguments_guard.replace(
                                idx,
                                &buf.call_id,
                                &buf.name,
                                &buf.arguments,
                            );
                        }
                    }
                }
            }
            "response.function_call_arguments.delta" => {
                let delta = v.get("delta").and_then(Value::as_str).unwrap_or("");
                let idx = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                let buf = self.tool_calls.entry(idx).or_default();
                buf.arguments.push_str(delta);
                self.arguments_guard
                    .stream(idx, &buf.call_id, &buf.name, &buf.arguments, delta);
                if !buf.call_id.is_empty() && !delta.is_empty() {
                    self.display_parts.delta(
                        (idx, 3),
                        DisplayEvent::ToolInputDelta {
                            mid: None,
                            part: None,
                            tool_call_id: ToolCallId(buf.call_id.clone()),
                            delta: delta.to_owned(),
                        },
                        "openai_responses",
                        self.step_id,
                        out,
                    );
                }
            }
            // The authoritative complete-arguments event. Some backends/models (observed
            // live: gpt-5.3-codex-spark on the chatgpt codex backend) send NO delta events
            // at all — `output_item.added` carries `arguments:""` and the full string
            // arrives ONLY here. Replace, never append: a stream that DID send deltas has
            // already accumulated this same string, and replacing with the authoritative
            // form also self-heals any lost/duplicated delta.
            "response.function_call_arguments.done" => {
                let idx = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                if let Some(args) = v.get("arguments").and_then(Value::as_str) {
                    let buf = self.tool_calls.entry(idx).or_default();
                    buf.arguments = args.to_owned();
                    self.arguments_guard
                        .replace(idx, &buf.call_id, &buf.name, &buf.arguments);
                }
            }
            // The completed item also carries the full final state; treat it as one more
            // authoritative source for the same fields (idempotent with `added`/`done`,
            // and covers a hypothetical stream that skips the arguments.done event).
            "response.output_item.done" => {
                if let Some(item) = v.get("item") {
                    let idx = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                    self.completed_items.insert(idx);
                    self.capture_text_state(idx, item);
                    let kind = match item.get("type").and_then(Value::as_str) {
                        Some("reasoning") => Some(0),
                        Some("function_call") => Some(3),
                        _ if message_item_output_text_parts(item)
                            .iter()
                            .any(|(_, text)| !text.is_empty())
                            || self.text_states.contains_key(&idx) =>
                        {
                            Some(1)
                        }
                        _ => None,
                    };
                    if let Some(kind) = kind {
                        self.display_parts.reserve(
                            (idx, kind),
                            "openai_responses",
                            self.step_id,
                            out,
                        );
                    }
                    match item.get("type").and_then(Value::as_str) {
                        Some("function_call") => {
                            let buf = self.tool_calls.entry(idx).or_default();
                            if let Some(id) = item.get("call_id").and_then(Value::as_str) {
                                buf.call_id = id.to_owned();
                            }
                            if let Some(name) = item.get("name").and_then(Value::as_str) {
                                buf.name = name.to_owned();
                            }
                            // Presence is authoritative even when the final value is empty:
                            // `decode_tool_arguments` maps an empty string to `{}` for a
                            // zero-argument call. Absence alone preserves accumulated deltas.
                            if let Some(args) = item.get("arguments").and_then(Value::as_str) {
                                buf.arguments = args.to_owned();
                                self.arguments_guard.replace(
                                    idx,
                                    &buf.call_id,
                                    &buf.name,
                                    &buf.arguments,
                                );
                            }
                        }
                        Some("reasoning") => {
                            // Preserve the provider's complete item token rather than rebuilding
                            // it: id, summary, encrypted_content, metadata, and key order all
                            // survive persistence. Request rendering later extracts the fields
                            // needed for continuation and removes response-only fields.
                            let raw_item = raw_output_item(payload)?;
                            self.reasoning_items.insert(idx, raw_item);
                        }
                        _ => {
                            // A completed message item repeats its full content array. Recover any
                            // output_text parts as a fallback when granular output_text.delta /
                            // output_text.done events were omitted. Fallback-only (entry / or_insert):
                            // a present entry means a granular event already supplied this part index,
                            // so a part that arrived through streamed events is never double-counted.
                            for (part_idx, text) in message_item_output_text_parts(item) {
                                self.text_items
                                    .entry((idx, part_idx))
                                    .or_insert_with(|| text.to_owned());
                            }
                            if let Some(refusal) = message_item_refusal(item) {
                                self.refusal_completed = true;
                                // A completed message item repeats the full refusal content. Use
                                // it as the authoritative fallback if refusal delta/done events
                                // were omitted, matching the function-call completion behavior.
                                self.saw_refusal = true;
                                self.refusal_output_index = Some(idx);
                                self.refusal = refusal;
                                if !self.refusal.is_empty() {
                                    self.display_parts.reserve(
                                        (idx, 2),
                                        "openai_responses",
                                        self.step_id,
                                        out,
                                    );
                                }
                            }
                        }
                    }
                }
            }
            // Escaped or unusually ordered type discriminators miss the terminal prefilter but
            // still retain the previous permissive Value-based behavior.
            "response.completed" | "response.incomplete" => {
                if let Some(resp) = v.get("response") {
                    if let Some(id) = resp.get("id").and_then(Value::as_str) {
                        self.response_id = Some(MessageId(id.to_owned()));
                    }
                    if let Some(u) = resp.get("usage") {
                        self.merge_usage(u);
                    }
                    if let Some(status) = resp.get("status").and_then(Value::as_str) {
                        self.finish_reason = map_response_status(status);
                    }
                    if let Some(tier) = resp.get("service_tier").and_then(Value::as_str) {
                        self.service_tier_used = Some(tier.to_owned());
                    }
                    if let Some(details) = resp.get("incomplete_details") {
                        if let Some(reason) = details.get("reason").and_then(Value::as_str) {
                            self.finish_reason = map_incomplete_reason(reason);
                            self.incomplete_reason = Some(reason.to_owned());
                        }
                    }
                }
                self.finish_terminal(out)?;
            }
            _ => {}
        }
        if self.arguments_guard.fired() && !self.completed {
            self.finish_reason = FinishReason::ToolCalls;
            self.finish_terminal(out)?;
        }
        Ok(())
    }

    fn handle_terminal_response(
        &mut self,
        response: ResponsesTerminalResponse,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        if let Some(id) = response.id {
            self.response_id = Some(MessageId(id));
        }
        if let Some(usage) = response.usage {
            let (cached, written) = usage
                .input_tokens_details
                .map(|details| (details.cached_tokens, details.cache_write_tokens))
                .unwrap_or_default();
            merge_inclusive_input_tokens_with_writes(
                &mut self.usage,
                usage.input_tokens,
                cached,
                written,
            );
            if let Some(output_tokens) = usage.output_tokens {
                self.usage.output_tokens = Some(output_tokens);
            }
            if let Some(reasoning_tokens) = usage
                .output_tokens_details
                .and_then(|details| details.reasoning_tokens)
            {
                self.usage.reasoning_tokens = Some(reasoning_tokens);
            }
        }
        if let Some(status) = response.status {
            self.finish_reason = map_response_status(&status);
        }
        if let Some(tier) = response.service_tier {
            self.service_tier_used = Some(tier);
        }
        if let Some(reason) = response
            .incomplete_details
            .and_then(|details| details.reason)
        {
            self.finish_reason = map_incomplete_reason(&reason);
            self.incomplete_reason = Some(reason);
        }
        self.finish_terminal(out)
    }

    fn finish_terminal(&mut self, out: &mut Vec<NormalizedEvent>) -> Result<(), NormalizeError> {
        // `completed` maps to the generic Stop reason, but an observed refusal is the
        // more specific policy terminal. Explicit non-Stop statuses/details still win.
        if self.saw_refusal && self.finish_reason == FinishReason::Stop {
            self.finish_reason = FinishReason::ContentFilter;
        }
        if !self.completed {
            self.completed = true;
            let message = self.assemble_message(out)?;
            out.extend(self.arguments_guard.event());
            out.push(NormalizedEvent::Control(
                ProviderControl::AssistantMessage { message },
            ));
            out.push(NormalizedEvent::Control(ProviderControl::StepFinished {
                step_id: self.step_id,
                usage: self.usage,
                finish_reason: self.finish_reason,
                service_tier_used: self.service_tier_used.clone(),
                speed_used: None,
                // No non-Anthropic family reports a server-side fallback or per-model usage.
                served_model: None,
                usage_iterations: None,
            }));
            out.push(NormalizedEvent::Control(ProviderControl::Finished {
                usage: self.usage,
                finish_reason: self.finish_reason,
                retries_used: 0,
            }));
        }
        Ok(())
    }

    fn merge_usage(&mut self, u: &Value) {
        let detail = |key: &str| {
            u.get("input_tokens_details")
                .and_then(|d| d.get(key))
                .and_then(Value::as_u64)
        };
        merge_inclusive_input_tokens_with_writes(
            &mut self.usage,
            u.get("input_tokens").and_then(Value::as_u64),
            detail("cached_tokens"),
            detail("cache_write_tokens"),
        );
        if let Some(x) = u.get("output_tokens").and_then(Value::as_u64) {
            self.usage.output_tokens = Some(x);
        }
        // The Responses API reports the reasoning-token subset of output under
        // `output_tokens_details.reasoning_tokens`. It is a SUBSET of `output_tokens`
        // (the provider's billing total), so we surface it without subtracting —
        // consumers treat `output_tokens` as the total and `reasoning_tokens` as the
        // provider-reported split of it. Absent details (long-tail OpenAI-compatible
        // servers) leaves the default None untouched.
        if let Some(x) = u
            .get("output_tokens_details")
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(Value::as_u64)
        {
            self.usage.reasoning_tokens = Some(x);
        }
    }

    fn assemble_message(
        &mut self,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<AssistantMessage, NormalizeError> {
        for buf in self.tool_calls.values() {
            self.arguments_guard
                .terminal(&buf.call_id, &buf.name, &buf.arguments);
        }
        if self.arguments_guard.fired() {
            self.finish_reason = FinishReason::ToolCalls;
        }
        // One assistant message can represent several Responses output items. Sort canonical
        // blocks by output_index so reasoning remains immediately before the function/message
        // item it accompanied. The secondary key makes events without output_index deterministic.
        let mut ordered = Vec::<(u64, u8, ContentBlock)>::new();
        for (index, raw_item) in std::mem::take(&mut self.reasoning_items) {
            ordered.push((
                index,
                0,
                ContentBlock::RedactedReasoning {
                    data: raw_item,
                    provider_format: Some(self.reasoning_format),
                },
            ));
        }
        // A BTreeMap over (output_index, content_index) yields one item's parts adjacent and
        // in part order, so folding consecutive equal output_index values rebuilds the message
        // text in wire order without a second sort.
        let mut parts: Vec<(u64, String)> = Vec::new();
        for index in self.text_states.keys() {
            self.text_items.entry((*index, 0)).or_default();
        }
        for ((index, part), text) in std::mem::take(&mut self.text_items) {
            if self.arguments_guard.fired()
                && !self.completed_items.contains(&index)
                && !self.completed_text_parts.contains(&(index, part))
            {
                continue;
            }
            match parts.last_mut() {
                Some((prev, acc)) if *prev == index => acc.push_str(&text),
                _ => parts.push((index, text)),
            }
        }
        for (index, text) in parts {
            if let Some(data) = self.text_states.remove(&index) {
                ordered.push((index, 1, ContentBlock::TextWithState {
                    text,
                    provider_state: broca_protocol::content::TextProviderState {
                        format: match self.reasoning_format {
                            EncryptedReasoningFormat::OpenAiResponses => broca_protocol::content::TextProviderFormat::OpenAiResponses,
                            EncryptedReasoningFormat::OpenAiResponsesChatgpt => broca_protocol::content::TextProviderFormat::OpenAiResponsesChatgpt,
                            EncryptedReasoningFormat::OpenRouter => return Err(NormalizeError::Invalid("OpenRouter reasoning cannot label Responses text state".into())),
                        },
                        data,
                    },
                }));
            } else if !text.is_empty() {
                ordered.push((index, 1, ContentBlock::Text { text }));
            }
        }
        if !self.refusal.is_empty() && (!self.arguments_guard.fired() || self.refusal_completed) {
            ordered.push((
                self.refusal_output_index.take().unwrap_or(0),
                2,
                ContentBlock::Text {
                    text: std::mem::take(&mut self.refusal),
                },
            ));
        }
        // Tool arguments are parsed here, when the terminal `response.completed` or
        // `response.incomplete` event arrives; that same event carries `incomplete_details`, and
        // it has been read above before this runs. So a `max_output_tokens` reason is known when
        // a buffer fails to parse, and it proves the ceiling cut the call.
        let ceiling = self
            .incomplete_reason
            .as_deref()
            .filter(|reason| *reason == "max_output_tokens")
            .map(|provider_stop_reason| crate::wire::CeilingStop {
                provider_stop_reason,
                max_output_tokens: self.max_output_tokens,
                output_tokens: self.usage.output_tokens,
            });
        for (index, buf) in std::mem::take(&mut self.tool_calls) {
            let Some(input) = self.arguments_guard.decode(
                &buf.arguments,
                &buf.name,
                ceiling,
                self.completed_items.contains(&index),
            )?
            else {
                continue;
            };
            ordered.push((
                index,
                3,
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(buf.call_id),
                    tool_name: buf.name,
                    input,
                    thought_signature: None,
                }),
            ));
        }
        ordered.sort_by_key(|(index, kind_order, _)| (*index, *kind_order));
        let keys: Vec<_> = ordered
            .iter()
            .map(|(index, kind, _)| (*index, u64::from(*kind)))
            .collect();
        self.display_parts
            .retain(&keys, "openai_responses", self.step_id, out);
        Ok(AssistantMessage {
            message_id: self.response_id.clone().unwrap_or(MessageId(String::new())),
            content: ordered.into_iter().map(|(_, _, block)| block).collect(),
        })
    }
}

impl StreamNormalizer for OpenAiResponsesNormalizer {
    fn websocket_telemetry(&mut self) {
        self.sse.websocket_telemetry();
    }
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.sse.content_progress()
    }
    fn private_error_diagnostics(&mut self) {
        self.private_error_diagnostics = true;
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.push(chunk)? {
            self.handle_event(&payload, &mut out)?;
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
            self.handle_event(&payload, &mut out)?;
        }
        Ok(out)
    }
}

#[derive(Deserialize)]
struct RawOutputItemEnvelope {
    item: Box<RawValue>,
}

fn raw_output_item(payload: &str) -> Result<String, NormalizeError> {
    serde_json::from_str::<RawOutputItemEnvelope>(payload)
        .map(|event| event.item.get().to_owned())
        .map_err(|error| NormalizeError::Invalid(error.to_string()))
}

fn message_item_refusal(item: &Value) -> Option<String> {
    if item.get("type").and_then(Value::as_str) != Some("message") {
        return None;
    }
    let refusals: Vec<&str> = item
        .get("content")?
        .as_array()?
        .iter()
        .filter(|part| part.get("type").and_then(Value::as_str) == Some("refusal"))
        .filter_map(|part| part.get("refusal").and_then(Value::as_str))
        .collect();
    (!refusals.is_empty()).then(|| refusals.concat())
}

/// The `output_text` content parts of a completed `message` item, with their part indices.
/// The completed item repeats the full content array, providing an authoritative source
/// for any text parts where granular streamed events were omitted.
fn message_item_output_text_parts(item: &Value) -> Vec<(u64, &str)> {
    if item.get("type").and_then(Value::as_str) != Some("message") {
        return Vec::new();
    }
    let Some(content) = item.get("content").and_then(Value::as_array) else {
        return Vec::new();
    };
    content
        .iter()
        .enumerate()
        .filter_map(|(idx, part)| {
            if part.get("type").and_then(Value::as_str) == Some("output_text") {
                part.get("text")
                    .and_then(Value::as_str)
                    .map(|text| (idx as u64, text))
            } else {
                None
            }
        })
        .collect()
}

fn response_stream_error(event: &Value, nested: bool, private_diagnostics: bool) -> ProviderError {
    let error = if nested {
        event.pointer("/response/error").unwrap_or(&Value::Null)
    } else {
        event
            .get("error")
            .filter(|error| error.is_object())
            .unwrap_or(event)
    };
    let event_type = event
        .get("type")
        .and_then(Value::as_str)
        .unwrap_or("unknown");
    let code = error.get("code").and_then(Value::as_str);
    let error_type = error.get("type").and_then(Value::as_str);
    // An exhausted prepaid balance is about the credential, not this request: relabel it so the
    // run's terminal names the credential and routing stops using it.
    let billing_exhausted = crate::normalize::openai_billing_exhausted(code, error_type);
    let message = if private_diagnostics {
        "Responses websocket backend reported an error; response body omitted".to_owned()
    } else {
        error
        .get("message")
        .and_then(Value::as_str)
        .map(str::to_owned)
        .unwrap_or_else(|| {
            let code_description = code.map_or_else(|| "no code".to_owned(), |code| format!("code {code:?}"));
            format!("OpenAI Responses stream reported an error (event {event_type:?}, {code_description})")
        })
    };
    if private_diagnostics {
        // Error messages, codes and even event discriminators are arbitrary backend strings.
        tracing::warn!(
            transport = "websocket",
            "OpenAI Responses stream error; response body omitted"
        );
    } else {
        tracing::warn!(
            event_type,
            code,
            error_type,
            message = %message.chars().take(300).collect::<String>(),
            "OpenAI Responses stream error"
        );
    }
    ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: if billing_exhausted {
            ErrorClass::Permanent
        } else {
            response_error_class(event, code)
        },
        message,
        status: None,
        // Only `response.failed` carries a retry time Codex reads; it ignores a bare `error`
        // event's fields (`codex-api/src/sse/responses.rs:349-352`). The value is read from the
        // provider's own message even when `message` above is replaced.
        retry_after_secs: if nested {
            crate::codex_retry_after::failed_response_retry_after(
                error,
                std::time::SystemTime::now(),
            )
        } else {
            None
        },
        provider_code: if billing_exhausted {
            Some(broca_protocol::error::BILLING_EXHAUSTED.to_owned())
        } else {
            code.map(str::to_owned)
        },
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

fn response_error_class(event: &Value, code: Option<&str>) -> ErrorClass {
    // A used-up plan allowance, or a plan without the requested use, cannot be granted by
    // re-sending, so the shared ChatGPT policy's account-wall and not-included stream classes
    // end the step. Its rate-limit and overload classes are transient, as the table below
    // already makes every code it does not list.
    if matches!(
        cortexkit_chatgpt_policy::classify_stream_failure(event),
        cortexkit_chatgpt_policy::Classification::AccountLimitExhausted { .. }
            | cortexkit_chatgpt_policy::Classification::UsageNotIncluded
    ) {
        return ErrorClass::Permanent;
    }
    match code {
        Some("context_length_exceeded" | "context_window_exceeded") => ErrorClass::ContextOverflow,
        Some(
            "billing_exhausted"
            | "insufficient_quota"
            | "credit_balance_exhausted"
            | "organization_spend_limit_exceeded"
            | "project_spend_limit_exceeded"
            | "invalid_prompt"
            | "content_filter"
            | "cyber_policy"
            | "bio_policy"
            | "misalignment_policy_violation",
        ) => ErrorClass::Permanent,
        // The Responses backend uses unknown codes for transient failures. Model requests
        // have no side effects, so only codes known to mean the request cannot succeed as
        // sent are permanent. Keep exhausted account allowances permanent as well.
        Some(_) | None => ErrorClass::Transient,
    }
}

fn map_response_status(status: &str) -> FinishReason {
    match status {
        "completed" => FinishReason::Stop,
        "incomplete" => FinishReason::Length,
        "failed" | "cancelled" => FinishReason::Other,
        _ => FinishReason::Other,
    }
}

fn map_incomplete_reason(reason: &str) -> FinishReason {
    match reason {
        "max_output_tokens" => FinishReason::Length,
        "content_filter" => FinishReason::ContentFilter,
        _ => FinishReason::Other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The ChatGPT/Codex stream-failure codes, in the `response.failed` shape and both bare
    /// `error` shapes: the plan-allowance codes end the step, the throttle and overload codes
    /// retry, and only `code` is read, so a `type` naming the same value changes nothing.
    #[test]
    fn chatgpt_stream_failure_codes_keep_their_classes() {
        for (code, class) in [
            ("usage_limit_reached", ErrorClass::Permanent),
            ("usage_not_included", ErrorClass::Permanent),
            ("rate_limit_exceeded", ErrorClass::Transient),
            ("slow_down", ErrorClass::Transient),
            ("server_is_overloaded", ErrorClass::Transient),
        ] {
            for event in [
                serde_json::json!({"type":"response.failed","response":{"error":{"code":code}}}),
                serde_json::json!({"type":"error","error":{"code":code}}),
                serde_json::json!({"type":"error","code":code}),
            ] {
                let nested = event["type"] == "response.failed";
                let error = response_stream_error(&event, nested, false);
                assert_eq!(error.class, class, "{event}");
                assert_eq!(error.provider_code.as_deref(), Some(code), "{event}");
                assert_eq!(error.status, None);
            }
        }
        let typed = serde_json::json!({"type":"response.failed","response":{"error":{"type":"usage_not_included"}}});
        let error = response_stream_error(&typed, true, false);
        assert_eq!(error.class, ErrorClass::Transient);
        assert_eq!(error.provider_code, None);
    }

    /// A filtered stream must not be retried: `content_filter` ends the step in every
    /// error shape the classifier reads, while an unlisted failure code still retries.
    #[test]
    fn content_filter_stream_error_is_permanent() {
        for event in [
            serde_json::json!({"type":"response.failed","response":{"error":{"code":"content_filter"}}}),
            serde_json::json!({"type":"error","error":{"code":"content_filter"}}),
            serde_json::json!({"type":"error","code":"content_filter"}),
        ] {
            let nested = event["type"] == "response.failed";
            let error = response_stream_error(&event, nested, false);
            assert_eq!(error.class, ErrorClass::Permanent, "{event}");
            assert_eq!(
                error.provider_code.as_deref(),
                Some("content_filter"),
                "{event}"
            );
            assert_eq!(error.status, None);
        }
        for event in [
            serde_json::json!({"type":"response.failed","response":{"error":{"code":"internal_error"}}}),
            serde_json::json!({"type":"error","error":{"code":"internal_error"}}),
            serde_json::json!({"type":"error","code":"internal_error"}),
        ] {
            let nested = event["type"] == "response.failed";
            let error = response_stream_error(&event, nested, false);
            assert_eq!(error.class, ErrorClass::Transient, "{event}");
            assert_eq!(
                error.provider_code.as_deref(),
                Some("internal_error"),
                "{event}"
            );
        }
    }

    fn extract_assistant_text(events: &[NormalizedEvent]) -> String {
        for event in events {
            if let NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) = event {
                let mut text = String::new();
                for block in &message.content {
                    if let ContentBlock::Text { text: t } = block {
                        text.push_str(t);
                    }
                }
                return text;
            }
        }
        panic!("no AssistantMessage found in events: {events:?}");
    }

    #[test]
    fn responses_fallback_mixed_case_recovers_part_1() {
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        let chunks: &[&[u8]] = &[
            b"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
            b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"part 0 granular \"}\n\n",
            b"data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"text\":\"part 0 granular \"}\n\n",
            b"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"part 0 granular \"},{\"type\":\"output_text\",\"text\":\"part 1 from completed item\"}]}}\n\n",
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
        ];
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(normalizer.push(chunk).unwrap());
        }
        events.extend(normalizer.finish().unwrap());
        let text = extract_assistant_text(&events);
        assert_eq!(text, "part 0 granular part 1 from completed item");
    }

    #[test]
    fn responses_fallback_all_granular_case_is_unchanged() {
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        let chunks: &[&[u8]] = &[
            b"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
            b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"part 0 granular \"}\n\n",
            b"data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"text\":\"part 0 granular \"}\n\n",
            b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":1,\"delta\":\"part 1 granular\"}\n\n",
            b"data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":1,\"text\":\"part 1 granular\"}\n\n",
            b"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"part 0 granular \"},{\"type\":\"output_text\",\"text\":\"part 1 granular\"}]}}\n\n",
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
        ];
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(normalizer.push(chunk).unwrap());
        }
        events.extend(normalizer.finish().unwrap());
        let text = extract_assistant_text(&events);
        assert_eq!(text, "part 0 granular part 1 granular");
    }

    #[test]
    fn responses_fallback_all_omitted_case_recovers_everything() {
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        let chunks: &[&[u8]] = &[
            b"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
            b"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"part 0 omitted \"},{\"type\":\"output_text\",\"text\":\"part 1 omitted\"}]}}\n\n",
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
        ];
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(normalizer.push(chunk).unwrap());
        }
        events.extend(normalizer.finish().unwrap());
        let text = extract_assistant_text(&events);
        assert_eq!(text, "part 0 omitted part 1 omitted");
    }

    /// The event shape a Meta Muse stream sent with `reasoning.summary: "auto"` and
    /// `store: false`: two summary parts, each a whole line in one delta and arriving seconds
    /// apart, then the answer. Each summary delta must reach the display lane as reasoning text,
    /// in order and before the first answer delta, so a long reasoning phase is visible progress.
    /// The assembled message must still carry only the provider's raw reasoning item, so the
    /// durable record and the request echoed back are unchanged.
    #[test]
    fn reasoning_summary_deltas_reach_the_display_lane_before_the_answer() {
        const REASONING_ITEM: &str = r#"{"id":"rs_1","type":"reasoning","summary":[{"type":"summary_text","text":"**Planning** I will check the inputs."},{"type":"summary_text","text":"**Answering** The inputs are fine."}],"encrypted_content":"synthetic-cipher"}"#;
        let events_in = [
            r#"{"type":"response.created","response":{"id":"resp_1"}}"#.to_owned(),
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"rs_1","type":"reasoning","summary":[]}}"#.to_owned(),
            r#"{"type":"response.reasoning_summary_part.added","item_id":"rs_1","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":""}}"#.to_owned(),
            r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_1","output_index":0,"summary_index":0,"delta":"**Planning** I will check the inputs."}"#.to_owned(),
            r#"{"type":"response.reasoning_summary_text.done","item_id":"rs_1","output_index":0,"summary_index":0,"text":"**Planning** I will check the inputs."}"#.to_owned(),
            r#"{"type":"response.reasoning_summary_part.done","item_id":"rs_1","output_index":0,"summary_index":0,"part":{"type":"summary_text","text":"**Planning** I will check the inputs."}}"#.to_owned(),
            r#"{"type":"response.reasoning_summary_part.added","item_id":"rs_1","output_index":0,"summary_index":1,"part":{"type":"summary_text","text":""}}"#.to_owned(),
            r#"{"type":"response.reasoning_summary_text.delta","item_id":"rs_1","output_index":0,"summary_index":1,"delta":"**Answering** The inputs are fine."}"#.to_owned(),
            r#"{"type":"response.reasoning_summary_text.done","item_id":"rs_1","output_index":0,"summary_index":1,"text":"**Answering** The inputs are fine."}"#.to_owned(),
            format!(r#"{{"type":"response.output_item.done","output_index":0,"item":{REASONING_ITEM}}}"#),
            r#"{"type":"response.output_item.added","output_index":1,"item":{"id":"msg_1","type":"message","role":"assistant","content":[]}}"#.to_owned(),
            r#"{"type":"response.output_text.delta","output_index":1,"content_index":0,"delta":"All good."}"#.to_owned(),
            r#"{"type":"response.output_text.done","output_index":1,"content_index":0,"text":"All good."}"#.to_owned(),
            r#"{"type":"response.completed","response":{"id":"resp_1","status":"completed"}}"#.to_owned(),
        ];
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        let mut events = Vec::new();
        // One push per event, as the events arrived seconds apart on the measured stream.
        for event in &events_in {
            events.extend(
                normalizer
                    .push(format!("data: {event}\n\n").as_bytes())
                    .unwrap(),
            );
        }
        events.extend(normalizer.finish().unwrap());

        let display: Vec<(&str, &str)> = events
            .iter()
            .filter_map(|event| match event {
                NormalizedEvent::Display(DisplayEvent::ReasoningDelta {
                    message_id,
                    delta,
                    ..
                }) => {
                    assert_eq!(message_id.0, "resp_1");
                    Some(("reasoning", delta.as_str()))
                }
                NormalizedEvent::Display(DisplayEvent::TextDelta { delta, .. }) => {
                    Some(("text", delta.as_str()))
                }
                _ => None,
            })
            .collect();
        assert_eq!(
            display,
            [
                ("reasoning", "**Planning** I will check the inputs."),
                ("reasoning", "\n\n**Answering** The inputs are fine."),
                ("text", "All good."),
            ]
        );

        let message = events
            .iter()
            .find_map(|event| match event {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message)
                }
                _ => None,
            })
            .expect("assembled message");
        assert_eq!(
            message.content,
            [
                ContentBlock::RedactedReasoning {
                    data: REASONING_ITEM.into(),
                    provider_format: Some(EncryptedReasoningFormat::OpenAiResponses),
                },
                ContentBlock::Text {
                    text: "All good.".into()
                },
            ]
        );
    }
}
