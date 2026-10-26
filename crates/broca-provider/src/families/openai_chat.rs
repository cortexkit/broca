//! The OpenAI Chat Completions wire family — `/v1/chat/completions`.
//!
//! The big bucket and the long-tail unlocker: most "OpenAI-compatible" providers
//! speak this grammar with only config differences. It is deliberately the FIRST
//! family after Anthropic, because it is the MOST different grammar — proving the
//! `WireFamily`/`ProviderSpec`/`Policies` seam generalizes here means it generalizes.
//!
//! Differences from Anthropic Messages (all handled as data/policy, no provider
//! branches):
//!  - the system prompt is a LEADING `role:"system"` message, not a top-level field;
//!  - a tool call lives in `assistant.tool_calls[]` as
//!    `{id, type:"function", function:{name, arguments}}`, and `arguments` is a
//!    JSON-encoded STRING, not an object;
//!  - a tool result is a `role:"tool"` message with `tool_call_id` + string content;
//!  - tools are `{type:"function", function:{name, description, parameters}}` (the
//!    schema key is `parameters`, not `input_schema`);
//!  - the output-token limit field is `max_tokens` or `max_completion_tokens`
//!    (a typed policy);
//!  - the stream is delta-accumulated (`choices[].delta`) with index-keyed
//!    tool-call argument fragments, ends with a `[DONE]` sentinel, and has no
//!    in-band terminal event — the assembled message is emitted on `finish()`.

use broca_protocol::{
    content::{
        AssistantMessage, ContentBlock, EncryptedReasoningFormat, FinishReason, Message, Role,
        ToolCall, ToolOutput,
    },
    display::DisplayEvent,
    error::ProviderError,
    ids::{MessageId, ToolCallId},
    provider::{NormalizedEvent, ProviderControl},
    request::{CallOptions, Tool, ToolChoice},
    usage::Usage,
    StepId,
};
use serde_json::{json, Value};

use crate::families::{
    apply_generation_controls, merge_inclusive_input_tokens,
    merge_inclusive_input_tokens_with_writes,
};
use crate::normalize::classify_error_with_status_defaults;
use crate::policy::{ReasoningEchoFormat, ReasoningEchoPolicy, ReasoningEchoTurns, UsagePolicy};
use crate::sse::SseDecoder;
use crate::wire::{
    AuthHandle, CacheClass, FrozenRenderConfig, HttpMethod, HttpRequest, NormalizeError,
    StreamNormalizer, WireFamily,
};

/// The default base URL (no `base_url` override). The `/chat/completions` path is
/// appended; an OpenAI-compatible provider supplies its own base (e.g.
/// `https://api.deepseek.com/v1`) via `FrozenRenderConfig.base_url`.
const DEFAULT_BASE_URL: &str = "https://api.openai.com/v1";

/// The OpenAI Chat Completions family. Stateless — per-run config is in the frozen
/// render config passed to each method.
pub struct OpenAiChat;

impl WireFamily for OpenAiChat {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        build_request_body(frozen, opts)
    }

    fn shape_with_media(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        payloads: &[crate::media_render::InlineMedia<'_>],
    ) -> Value {
        let media = crate::media_render::MediaRender { frozen, payloads };
        build_request_body_with_media(frozen, opts, frozen.media.as_ref().map(|_| &media))
    }

    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        let mut headers = crate::opencode_go::request_headers(frozen);
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
        // The base URL is config (an OpenAI-compatible provider points the same
        // family at its own host); the `/chat/completions` path is the family's.
        let base = frozen.base_url.as_deref().unwrap_or(DEFAULT_BASE_URL);
        let url = format!("{}/chat/completions", base.trim_end_matches('/'));
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
            OpenAiChatNormalizer::new(step_id, frozen.usage)
                .with_reasoning_echo(frozen.reasoning_echo)
                // The body renders the token-limit field from this same value.
                .with_max_output_tokens(frozen.max_output_tokens),
        )
    }
}

// ───────────────────────── request body ─────────────────────────

fn build_request_body(frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
    build_request_body_with_media(frozen, opts, None)
}

fn build_request_body_with_media(
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Value {
    // A run with structured output adds its submit tool or schema instructions here; a run
    // without renders from `opts` unchanged.
    let prepared = crate::structured_output::prepare(frozen, opts);
    let opts = prepared.as_ref();
    let view = crate::families::compaction_view(frozen.family_id, opts);
    let opts = view.as_ref();
    let name_map = crate::tool_name_alias::ToolNameMap::build(&opts.tools, &opts.prompt);
    let messages = convert_messages(
        &opts.prompt,
        frozen.reasoning_echo.or(frozen
            .reasoning_content_required
            .then_some(ReasoningEchoPolicy::DEEPSEEK)),
        frozen.tool_call_content_empty_string,
        frozen.opencode_go_reasoning_signature,
        &name_map,
        media,
    );

    let mut body = json!({
        "model": frozen.wire_model_id,
        "stream": true,
        // OpenAI-compatible servers only append the final usage-only chunk when the
        // request opts in. Without this, some providers stream a clean answer but report
        // zero tokens at step/run finish.
        "stream_options": { "include_usage": true },
        "messages": messages,
    });
    // The output-token limit field name is a typed policy (max_tokens vs
    // max_completion_tokens for reasoning models).
    body[frozen.token_limit_field.key()] = json!(frozen.max_output_tokens);
    // A caller's reasoning variant, resolved at admission from the model's catalog options.
    if let Some(crate::variant::FrozenVariant {
        request: crate::variant::VariantRequest::OpenAiChat { effort },
        ..
    }) = &frozen.variant
    {
        body["reasoning_effort"] = json!(effort);
    }
    if frozen.opencode_go_qwen_thinking {
        // Pi's Go Qwen routes send enable_thinking even when reasoning is turned off.
        body["enable_thinking"] = json!(frozen.variant.is_some());
    }
    if frozen.opencode_go_deepseek_thinking {
        // DeepSeek Go routes require an explicit thinking mode on every turn.
        body["thinking"] = json!({
            "type": if frozen.variant.is_some() { "enabled" } else { "disabled" }
        });
    }

    if !opts.tools.is_empty() {
        body["tools"] = Value::Array(
            opts.tools
                .iter()
                .map(|tool| convert_tool(tool, &name_map))
                .collect(),
        );
        body["tool_choice"] = convert_tool_choice(&opts.tool_choice, &name_map);
    }
    apply_generation_controls(
        body.as_object_mut()
            .expect("OpenAI Chat request body must stay a JSON object"),
        opts,
        "temperature",
        "top_p",
        Some("stop"),
    );
    apply_prompt_cache_hints(&mut body, frozen);
    if let Some(cache_key) = &frozen.prompt_cache_key {
        body["prompt_cache_key"] = Value::String(cache_key.clone());
    }
    // The caller's service tier, set only where the admitted provider maps a tier to this field
    // (first-party OpenAI and xAI); never for the OpenAI-compatible long tail. Absent leaves
    // the body exactly as before the field existed.
    if let Some(service_tier) = &frozen.service_tier {
        body["service_tier"] = Value::String(service_tier.clone());
    }
    crate::structured_output::finish(
        frozen,
        &mut body,
        name_map.wire_name(broca_protocol::structured_output::SUBMIT_RESULT_TOOL),
    );
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
    // On OpenCode Go's Kimi K2.6 route, omit the 24-hour prompt-cache hint and its cache key.
    if frozen.opencode_go_long_cache_retention_unsupported && retention.as_deref() == Some("24h") {
        return;
    }
    body["prompt_cache_key"] = Value::String(cache_key.clone());
    if let Some(retention) = retention {
        body["prompt_cache_retention"] = Value::String(
            crate::cache_hints::openai_prompt_cache_retention_wire_value(retention).to_string(),
        );
    }
}

/// Convert the canonical prompt to OpenAI chat messages. The system prompt is a
/// leading `system` message; an assistant turn carries any tool calls in
/// `tool_calls`; a tool turn becomes one `role:"tool"` message PER result (OpenAI
/// requires a separate message per tool_call_id).
fn convert_messages(
    prompt: &[Message],
    reasoning_echo: Option<ReasoningEchoPolicy>,
    tool_call_content_empty_string: bool,
    opencode_go: bool,
    name_map: &crate::tool_name_alias::ToolNameMap,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Vec<Value> {
    let mut out = Vec::new();
    let mut follow_up = Vec::new();
    for m in prompt {
        // Finish every result in a parallel tool round before inserting a synthetic user turn.
        if m.role != Role::Tool && !follow_up.is_empty() {
            out.push(json!({"role":"user", "content":std::mem::take(&mut follow_up)}));
        }
        match m.role {
            Role::System => {
                out.push(json!({ "role": "system", "content": text_content(&m.content) }))
            }
            Role::User => {
                out.push(json!({ "role": "user", "content": user_text_content(&m.content, media) }))
            }
            Role::Assistant => out.push(convert_assistant(
                m,
                reasoning_echo,
                tool_call_content_empty_string,
                opencode_go,
                name_map,
            )),
            Role::Tool => {
                for b in &m.content {
                    if let ContentBlock::ToolResult(tr) = b {
                        out.push(json!({
                            "role": "tool",
                            "tool_call_id": tr.tool_call_id.0,
                            "content": tool_output_with_media(tr, media, &mut follow_up),
                        }));
                    }
                }
            }
        }
    }
    if !follow_up.is_empty() {
        out.push(json!({"role":"user", "content":follow_up}));
    }
    out
}

fn tool_output_with_media(
    result: &broca_protocol::ToolResult,
    media: Option<&crate::media_render::MediaRender<'_>>,
    follow_up: &mut Vec<Value>,
) -> String {
    let (ToolOutput::Parts { parts }, Some(media)) = (&result.output, media) else {
        return tool_output_text(&result.output);
    };
    let grammar = super::openai_media::Grammar::Chat;
    let mut text = Vec::new();
    let mut blocks = Vec::new();
    for part in parts {
        match part {
            broca_protocol::ToolOutputPart::Text { text: part } => text.push(part.clone()),
            broca_protocol::ToolOutputPart::Media { media: reference } => {
                let reason = match grammar.block(
                    reference,
                    Some(media),
                    true,
                    crate::media_profile::MediaCarrier::FollowUpUserMessage,
                ) {
                    Ok(block) => {
                        blocks.push(block);
                        broca_protocol::MediaMarkerReason::Carrier
                    }
                    Err(reason) => reason,
                };
                text.push(broca_protocol::content::media_not_shown_marker(
                    reference, reason,
                ));
            }
        }
    }
    if !blocks.is_empty() {
        follow_up.push(grammar.text(&format!("Media from tool call {}:", result.tool_call_id.0)));
        follow_up.extend(blocks);
    }
    text.join("\n")
}

/// An assistant turn: text content (or null) + any tool calls. Reasoning blocks are
/// dropped for the default OpenAI-Chat shape. Under DeepSeek's echo policy,
/// EVERY assistant turn carries `reasoning_content`: the stored reasoning text verbatim, or `""`
/// when the turn has none -- with tool calls or without.
///
/// DeepSeek's thinking-mode guide says a request with `tools` must pass `reasoning_content` back
/// for all previous assistant turns, including turns without a tool call. Leaving it out does not
/// fail immediately, which is why this was once believed optional, but measured on a production
/// DeepSeek session it costs the model its reasoning:
///  - a recorded request with no `reasoning_content` counted about 37k more prompt tokens than
///    its bytes hold, with the prompt cache warm and with the first byte changed (cache cold)
///    alike: DeepSeek restored each turn's thinking from its own store;
///  - the same request with every tool-call id renamed was refused HTTP 400 "The
///    `reasoning_content` in the thinking mode must be passed back to the API.": the store is
///    keyed by DeepSeek's own tool-call ids, and without a match the request is invalid;
///  - the step after a new user message counted fewer tokens than a smaller earlier body: the
///    thinking from before that message was no longer restored.
///
/// So without the echo every turn boundary drops the model's earlier reasoning, and a run resumed
/// after DeepSeek's store expires is refused. Sending the text ourselves makes the request
/// complete on its own.
///
/// No renderer_version bump: resumed runs restore their recorded policy or legacy scalar flag,
/// including an absent flag. New provider defaults therefore cannot move old request bytes.
///
/// When `tool_call_content_empty_string` is set (ollama OpenAI-compat), a text-less turn uses
/// `"content": ""` instead of `null`.
fn convert_assistant(
    m: &Message,
    reasoning_echo: Option<ReasoningEchoPolicy>,
    tool_call_content_empty_string: bool,
    opencode_go: bool,
    name_map: &crate::tool_name_alias::ToolNameMap,
) -> Value {
    let mut text = String::new();
    let mut reasoning = String::new();
    let mut opencode_go_reasoning = false;
    let mut tool_calls: Vec<Value> = Vec::new();
    for b in &m.content {
        match b {
            ContentBlock::Text { text: t } | ContentBlock::TextWithState { text: t, .. } => {
                text.push_str(t)
            }
            ContentBlock::Reasoning { text, .. }
                if reasoning_echo
                    .is_some_and(|p| p.format == ReasoningEchoFormat::ReasoningContent) =>
            {
                reasoning.push_str(text);
            }
            ContentBlock::Reasoning { text, signature }
                if opencode_go && signature.as_deref() == Some("reasoning") =>
            {
                reasoning.push_str(text);
                opencode_go_reasoning = true;
            }
            ContentBlock::ToolCall(tc) => tool_calls.push(json!({
                "id": tc.tool_call_id.0,
                "type": "function",
                "function": {
                    "name": name_map.wire_name(&tc.tool_name),
                    // OpenAI wants the arguments as a JSON-encoded STRING.
                    "arguments": serde_json::to_string(&tc.input).unwrap_or_else(|_| "{}".into()),
                },
            })),
            // Structured continuation state is handled separately under its own format tag;
            // tool results never belong in an assistant message.
            _ => {}
        }
    }
    let mut msg = json!({ "role": "assistant" });
    // OpenAI requires `content` present; it is null when the turn is only tool calls (unless
    // the provider opts into an empty string for that case).
    msg["content"] = if text.is_empty() {
        // The quirk is NOT scoped to tool-call turns, though it was written that way. What the
        // opted-in providers reject is `content: null` at all, and an assistant turn can be
        // empty with no tool calls: a step that bills output tokens and assembles neither text
        // nor a call. That turn is durable, replay carries it into the next episode, and the
        // continuation was rendered with a bare `{"role":"assistant","content":null}` --
        // rejected 400 "invalid message content type: <nil>", so the whole next episode died
        // on a turn the model produced. Found in production: 32 completed OpenAI-Chat episodes
        // hold a genuinely empty assistant turn, and all 5 that were ever continued failed
        // with exactly that 400.
        //
        // No renderer_version bump. Bytes move only for a provider that opted into the quirk,
        // and only for the empty-turn shape whose every previous render was refused by the
        // provider -- so no run that ever worked renders differently, and the failure mode of
        // not bumping is a resumed run that now succeeds where it used to 400.
        if tool_call_content_empty_string {
            Value::String(String::new())
        } else {
            Value::Null
        }
    } else {
        Value::String(text)
    };
    if !tool_calls.is_empty() {
        msg["tool_calls"] = Value::Array(tool_calls);
    }
    // Coverage belongs to the frozen policy, not the current turn's contents. Scalar APIs
    // receive an empty string when no reasoning was stored; OpenRouter omits missing details.
    if let Some(policy) = reasoning_echo.filter(|p| {
        p.turns == ReasoningEchoTurns::EveryAssistant || msg.get("tool_calls").is_some()
    }) {
        match policy.format {
            ReasoningEchoFormat::ReasoningContent => {
                msg["reasoning_content"] = Value::String(reasoning);
            }
            ReasoningEchoFormat::OpenRouterDetails => {
                echo_openrouter_details(m, &mut msg);
            }
        }
    } else if opencode_go_reasoning {
        // OpenCode Go's OpenAI-compatible endpoint expects Pi's `reasoning` signature as
        // `reasoning_content` on the next request, rather than under the original field name.
        msg["reasoning_content"] = Value::String(reasoning);
    }
    msg
}

/// OpenRouter: "pass the reasoning_details array back ... without any modifications".
/// https://openrouter.ai/docs/guides/best-practices/reasoning-tokens#preserving-reasoning-blocks
/// The format tag prevents encrypted gateway state from becoming another API's signed thinking.
fn echo_openrouter_details(message: &Message, wire: &mut Value) {
    let mut details = Vec::new();
    let mut reasoning = String::new();
    for block in &message.content {
        if let ContentBlock::RedactedReasoning {
            data,
            provider_format: Some(EncryptedReasoningFormat::OpenRouter),
        } = block
        {
            if let Ok(envelope) = serde_json::from_str::<Value>(data) {
                if let Some(text) = envelope.get("reasoning").and_then(Value::as_str) {
                    reasoning.push_str(text);
                }
                if let Some(items) = envelope.get("reasoning_details").and_then(Value::as_array) {
                    details.extend(items.iter().cloned());
                }
            }
        }
    }
    if !details.is_empty() {
        wire["reasoning_details"] = Value::Array(details);
    } else if !reasoning.is_empty() {
        // OpenRouter also documents the scalar field for models returning only raw strings.
        wire["reasoning"] = Value::String(reasoning);
    }
}

// Preserve user block boundaries without changing the legacy scalar form for one text block.
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
                        Some(json!({"type":"text", "text":text}))
                    }
                    ContentBlock::Media(reference) => {
                        let grammar = super::openai_media::Grammar::Chat;
                        Some(
                            grammar
                                .block(
                                    reference,
                                    media,
                                    false,
                                    crate::media_profile::MediaCarrier::Native,
                                )
                                .unwrap_or_else(|reason| grammar.marker(reference, reason)),
                        )
                    }
                    ContentBlock::Reasoning { .. }
                    | ContentBlock::RedactedReasoning { .. }
                    | ContentBlock::ToolCall(_)
                    | ContentBlock::ToolResult(_) => None,
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
                .map(|text| json!({"type": "text", "text": text}))
                .collect(),
        )
    } else {
        Value::String(text_content(blocks))
    }
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

fn convert_tool(t: &Tool, name_map: &crate::tool_name_alias::ToolNameMap) -> Value {
    json!({
        "type": "function",
        "function": {
            "name": name_map.wire_name(&t.name),
            "description": t.description,
            "parameters": t.input_schema,
        },
    })
}

fn convert_tool_choice(tc: &ToolChoice, name_map: &crate::tool_name_alias::ToolNameMap) -> Value {
    match tc {
        ToolChoice::Auto => json!("auto"),
        ToolChoice::None => json!("none"),
        ToolChoice::Required => json!("required"),
        ToolChoice::Tool { name } => {
            json!({ "type": "function", "function": { "name": name_map.wire_name(name) } })
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

// ───────────────────────── stream normalizer ─────────────────────────

/// An in-progress tool call assembled from streamed deltas. The provider's `index`
/// identifies the current call when an id is absent; a non-empty id can open another
/// ordered entry when it conflicts with the latest call at that index.
#[derive(Default)]
struct ToolCallBuf {
    index: u64,
    id: String,
    name: String,
    arguments: String,
}

/// Streaming normalizer for one OpenAI chat completion (= one step). OpenAI streams
/// `choices[0].delta` fragments; content and refusal text accumulate in source order,
/// and tool-call argument fragments normally accumulate by index (id+name arrive in the
/// first fragment, `arguments`
/// stream as string fragments). Some OpenAI-compatible providers reuse an index for
/// parallel calls, so non-empty ids take precedence over that index when they differ.
/// The assembled message is emitted on `finish()` (EOF), after either a non-null
/// finish reason or the `[DONE]` sentinel confirms the response completed. Usage is
/// taken from the optional trailing usage chunk.
pub struct OpenAiChatNormalizer {
    display_parts: crate::display_parts::DisplayParts,
    arguments_guard: crate::wire::ToolArgsGuard,
    step_id: StepId,
    sse: SseDecoder,
    message_id: Option<MessageId>,
    reasoning: String,
    openrouter: bool,
    reasoning_details: Vec<Value>,
    text: String,
    saw_refusal: bool,
    tool_calls: Vec<ToolCallBuf>,
    usage: Usage,
    usage_policy: UsagePolicy,
    finish_reason: Option<FinishReason>,
    /// The provider's `finish_reason`, verbatim, for naming an output-ceiling cut.
    raw_finish_reason: Option<String>,
    /// The output cap the request was rendered with, when known.
    max_output_tokens: Option<u32>,
    started: bool,
    terminal_observed: bool,
    emitted_final: bool,
    /// The chunks' top-level `service_tier`, verbatim; the last non-null value wins. OpenAI and
    /// xAI put it on every chunk; servers that don't report one (or send `null`) leave it `None`.
    service_tier_used: Option<String>,
}

impl OpenAiChatNormalizer {
    pub fn new(step_id: StepId, usage_policy: UsagePolicy) -> Self {
        Self {
            display_parts: Default::default(),
            arguments_guard: Default::default(),
            step_id,
            sse: SseDecoder::for_content_family(crate::content_progress::ContentFamily::Chat),
            message_id: None,
            reasoning: String::new(),
            openrouter: false,
            reasoning_details: Vec::new(),
            text: String::new(),
            saw_refusal: false,
            tool_calls: Vec::new(),
            usage: Usage::default(),
            usage_policy,
            finish_reason: None,
            raw_finish_reason: None,
            max_output_tokens: None,
            started: false,
            terminal_observed: false,
            emitted_final: false,
            service_tier_used: None,
        }
    }

    /// Record the rendered output cap so an output-ceiling error can name it.
    pub fn with_max_output_tokens(mut self, max_output_tokens: u32) -> Self {
        self.max_output_tokens = Some(max_output_tokens);
        self
    }

    pub fn with_reasoning_echo(mut self, policy: Option<ReasoningEchoPolicy>) -> Self {
        self.openrouter =
            policy.is_some_and(|p| p.format == ReasoningEchoFormat::OpenRouterDetails);
        self
    }

    fn handle_chunk(
        &mut self,
        payload: &str,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        let v: Value =
            serde_json::from_str(payload).map_err(|e| NormalizeError::Invalid(e.to_string()))?;

        if let Some(error) = openai_chat_stream_error(&v) {
            out.push(NormalizedEvent::Control(ProviderControl::Error {
                error,
                retries_used: 0,
            }));
            return Ok(());
        }

        if !self.started {
            self.started = true;
            if let Some(id) = v.get("id").and_then(Value::as_str) {
                self.message_id = Some(MessageId(id.to_owned()));
            }
            out.push(NormalizedEvent::Control(ProviderControl::StreamStart {
                warnings: Vec::new(),
            }));
            out.push(NormalizedEvent::Control(ProviderControl::StepStarted {
                step_id: self.step_id,
            }));
        }

        if let Some(tier) = v.get("service_tier").and_then(Value::as_str) {
            self.service_tier_used = Some(tier.to_owned());
        }

        // A trailing usage-only chunk has `choices: []` + a `usage` object.
        if let Some(u) = v.get("usage").filter(|u| !u.is_null()) {
            self.merge_usage(u);
        }

        let choice = match v
            .get("choices")
            .and_then(|c| c.as_array())
            .and_then(|a| a.first())
        {
            Some(c) => c,
            None => return Ok(()),
        };

        if let Some(fr) = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish_reason = Some(map_finish_reason(fr));
            self.raw_finish_reason = Some(fr.to_owned());
            self.terminal_observed = true;
        }

        let delta = match choice.get("delta") {
            Some(d) => d,
            None => return Ok(()),
        };
        let mid = self
            .message_id
            .clone()
            .unwrap_or_else(|| MessageId(String::new()));

        if self.openrouter {
            if let Some(details) = delta.get("reasoning_details").and_then(Value::as_array) {
                self.reasoning_details.extend(details.iter().cloned());
                if !details.is_empty() {
                    self.display_parts
                        .reserve((0, 0), "openai_chat", self.step_id, out);
                }
            }
        }
        let reasoning_key = if self.openrouter {
            "reasoning"
        } else {
            "reasoning_content"
        };
        if let Some(reasoning) = delta.get(reasoning_key).and_then(Value::as_str) {
            if !reasoning.is_empty() {
                self.reasoning.push_str(reasoning);
                self.display_parts.delta(
                    (0, 0),
                    DisplayEvent::ReasoningDelta {
                        mid: None,
                        part: None,
                        message_id: mid.clone(),
                        delta: reasoning.to_owned(),
                    },
                    "openai_chat",
                    self.step_id,
                    out,
                );
            }
        }

        if let Some(content) = delta.get("content").and_then(Value::as_str) {
            if !content.is_empty() {
                self.text.push_str(content);
                self.display_parts.delta(
                    (1, 0),
                    DisplayEvent::TextDelta {
                        mid: None,
                        part: None,
                        message_id: mid.clone(),
                        tool_call_id: None,
                        delta: content.to_owned(),
                    },
                    "openai_chat",
                    self.step_id,
                    out,
                );
            }
        }

        if let Some(refusal) = delta.get("refusal").and_then(Value::as_str) {
            self.saw_refusal = true;
            if !refusal.is_empty() {
                self.text.push_str(refusal);
                self.display_parts.delta(
                    (1, 0),
                    DisplayEvent::TextDelta {
                        mid: None,
                        part: None,
                        message_id: mid.clone(),
                        tool_call_id: None,
                        delta: refusal.to_owned(),
                    },
                    "openai_chat",
                    self.step_id,
                    out,
                );
            }
        }

        if let Some(tcs) = delta.get("tool_calls").and_then(|t| t.as_array()) {
            for tc in tcs {
                let index = tc.get("index").and_then(Value::as_u64).unwrap_or(0);
                let id = tc
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty());
                let slot = self.tool_call_slot(index, id);
                self.display_parts
                    .reserve((2, slot as u64), "openai_chat", self.step_id, out);
                let buf = &mut self.tool_calls[slot];
                if let Some(id) = id {
                    buf.id = id.to_owned();
                }
                if let Some(func) = tc.get("function") {
                    if let Some(name) = func.get("name").and_then(Value::as_str) {
                        if !name.is_empty() {
                            buf.name = name.to_owned();
                        }
                    }
                    if let Some(args) = func.get("arguments").and_then(Value::as_str) {
                        buf.arguments.push_str(args);
                        self.arguments_guard.stream(
                            slot as u64,
                            &buf.id,
                            &buf.name,
                            &buf.arguments,
                            args,
                        );
                        let tcid = ToolCallId(buf.id.clone());
                        self.display_parts.delta(
                            (2, slot as u64),
                            DisplayEvent::ToolInputDelta {
                                mid: None,
                                part: None,
                                tool_call_id: tcid,
                                delta: args.to_owned(),
                            },
                            "openai_chat",
                            self.step_id,
                            out,
                        );
                    }
                }
            }
        }

        Ok(())
    }

    fn tool_call_slot(&mut self, index: u64, id: Option<&str>) -> usize {
        if let Some(id) = id {
            if let Some(slot) = self
                .tool_calls
                .iter()
                .position(|buf| !buf.id.is_empty() && buf.id == id)
            {
                return slot;
            }
        }
        let current = self.tool_calls.iter().rposition(|buf| buf.index == index);
        if let Some(id) = id {
            if let Some(slot) = current {
                // An absent current id is still compatible with the first id that arrives;
                // only two concrete, different ids identify separate calls.
                if !self.tool_calls[slot].id.is_empty() && self.tool_calls[slot].id != id {
                    self.tool_calls.push(ToolCallBuf {
                        index,
                        id: id.to_owned(),
                        ..ToolCallBuf::default()
                    });
                    return self.tool_calls.len() - 1;
                }
            }
        }
        if let Some(slot) = current {
            return slot;
        }
        self.tool_calls.push(ToolCallBuf {
            index,
            ..ToolCallBuf::default()
        });
        self.tool_calls.len() - 1
    }

    fn merge_usage(&mut self, u: &Value) {
        let g = |k: &str| u.get(k).and_then(Value::as_u64);
        let prompt_details = u.get("prompt_tokens_details");
        let cached = prompt_details
            .and_then(|d| d.get("cached_tokens"))
            .and_then(Value::as_u64)
            // DeepSeek's prompt total already includes its top-level cache-hit count.
            //
            // PROVENANCE: this field name comes from a bug report, not from a wire capture taken
            // here. No fixture in tests/fixtures/ carries it; the coverage below is an inline
            // test whose JSON was written from the same report that motivated the parse. So the
            // parser and its test share one belief and cannot disagree with each other -- if the
            // real field is spelled differently, or appears only in some response class, both are
            // wrong together and nothing in this repo would say so.
            //
            // Production has barely exercised it either: 6 deepseek facts total at the time of
            // writing. Closing this needs a captured response, not a better-constructed fixture,
            // which would only restate the belief more confidently.
            .or_else(|| g("prompt_cache_hit_tokens"));
        // PROVENANCE, same caveat as the cache-hit field above: `cache_write_tokens`
        // (OpenRouter) has no captured fixture here and is covered only by an inline test
        // written from the same report that motivated the parse. `cache_creation_input_tokens`
        // (Alibaba) does appear in one fixture, so it is the better-evidenced of the two.
        //
        // Reading a zero here is also uninformative on its own: broca's whole corpus shows
        // cache-write present only under EXPLICIT markers and absent under automatic prefix
        // caching, so absence is the normal shape for most providers rather than a signal.
        //
        // MEASURED NEGATIVE RESULT, and it is why the absent arm below stays an `Option` rather
        // than collapsing to 0. An external contributor mined 288,327 assistant messages from a
        // foreign harness corpus looking for the MISSING-KEY shape -- a response omitting the
        // cache-write key entirely while reporting cache reads -- and did not find it. Every
        // provider in that corpus either reports the class or omits the whole details object.
        //
        // So the branch distinguishing "key absent" from "key present and zero" is currently
        // exercised by NO observed traffic, here or there. It is kept because the distinction is
        // free and the cost of being wrong is asymmetric: collapsing absent to 0 would make an
        // unreported class indistinguishable from a reported zero, permanently and silently,
        // across every provider added later. Recorded rather than removed so the next reader
        // knows it is unexercised BY MEASUREMENT rather than by oversight.
        //
        // Reopen if a provider is seen reporting cache reads with the write key absent: that is
        // the observation this note says has never been made.
        // PLACEMENT, and it is why this looks in two places rather than one.
        //
        // `cache_creation_input_tokens` is ANTHROPIC'S spelling, and anthropic reports it at
        // the TOP LEVEL of its usage object rather than inside a prompt-details sub-object.
        // A gateway speaking openai-chat shape can carry it either way, and looking only
        // under `prompt_tokens_details` would miss the top-level form SILENTLY -- no error,
        // no zero to notice, just a counter that never appears.
        //
        // Downstream evidence that this is the live shape rather than a hypothetical:
        // alibaba-token-plan shows 110,711,808 cached-read tokens across 743 events and has
        // never once reported a write. The cache is demonstrably being populated and nothing
        // counts what filled it, which is what a wrong-placement lookup produces.
        //
        // Widening is safe in the direction that matters: a nested key still wins, and both
        // names are unambiguous cache-write counters wherever they appear. If the count stays
        // zero after this, the placement hypothesis is dead and "the provider does not report
        // it" is what remains -- which turns an unanswerable question into a measurement.
        let at = |k: &str| {
            prompt_details
                .and_then(|d| d.get(k))
                .and_then(Value::as_u64)
                .or_else(|| u.get(k).and_then(Value::as_u64))
        };
        // INCLUSIVE, SO SUBTRACTED: OpenAI's `cache_write_tokens` is part of `prompt_tokens`,
        // exactly like `cached_tokens`. Measured live on 2026-09-28 against metered chat
        // completions with `gpt-6-luna` and one ~3.2k-token prompt: the writing call reported
        // prompt_tokens 3173 with cache_write_tokens 3170 and total_tokens 3177 (prompt plus 4
        // completion), and the reading call prompt_tokens 3173 with cached_tokens 3170. The
        // openai-python type lists it in the prompt-token breakdown ("The unadjusted number of
        // prompt tokens written to cache"), and OpenRouter's documented example (prompt 194,
        // write 100, total 196) fits the same reading. Leaving it inside fresh input counted
        // every written token twice.
        //
        // NOT SUBTRACTED: `cache_creation_input_tokens`. No captured response in this repo shows
        // whether a gateway using that spelling (Alibaba) counts it inside `prompt_tokens`, so
        // it is recorded without touching fresh input. Reopen this with an Alibaba capture.
        match at("cache_write_tokens") {
            Some(written) => merge_inclusive_input_tokens_with_writes(
                &mut self.usage,
                g("prompt_tokens"),
                cached,
                Some(written),
            ),
            None => {
                merge_inclusive_input_tokens(&mut self.usage, g("prompt_tokens"), cached);
                if let Some(x) = at("cache_creation_input_tokens") {
                    self.usage.cache_write_tokens = Some(x);
                }
            }
        }
        let completion = g("completion_tokens");
        let reasoning = u
            .get("completion_tokens_details")
            .and_then(|d| d.get("reasoning_tokens"))
            .and_then(Value::as_u64);
        if let Some(x) = reasoning {
            self.usage.reasoning_tokens = Some(x);
        }
        match self.usage_policy {
            UsagePolicy::OpenAiUsageReasoningSeparate => {
                // Live xAI traffic falsifies inclusive completion semantics in one turn: the
                // prompt "Think briefly, then reply with exactly: ok" produced
                // `completion_tokens = 1` beside `reasoning_tokens = 21`. Canonical reasoning
                // is a subset of output, so xAI's sibling counts must be added; otherwise
                // reasoning exceeds output immediately.
                if completion.is_some() || reasoning.is_some() {
                    self.usage.output_tokens = broca_protocol::usage::sum_reported(
                        completion,
                        self.usage.reasoning_tokens,
                    );
                }
            }
            _ => {
                if let Some(x) = completion {
                    self.usage.output_tokens = Some(x);
                }
            }
        }
    }

    fn assemble_message(
        &mut self,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<AssistantMessage, NormalizeError> {
        for buf in &self.tool_calls {
            self.arguments_guard
                .terminal(&buf.id, &buf.name, &buf.arguments);
        }
        let mut content = Vec::new();
        let mut keys = Vec::new();
        if !self.reasoning.is_empty() || (self.openrouter && !self.reasoning_details.is_empty()) {
            keys.push((0, 0));
        }
        if !self.text.is_empty() {
            keys.push((1, 0));
        }
        if self.openrouter && (!self.reasoning.is_empty() || !self.reasoning_details.is_empty()) {
            content.push(ContentBlock::RedactedReasoning {
                data: serde_json::to_string(&json!({
                    "reasoning": std::mem::take(&mut self.reasoning),
                    "reasoning_details": std::mem::take(&mut self.reasoning_details),
                }))
                .expect("reasoning envelope is JSON"),
                provider_format: Some(EncryptedReasoningFormat::OpenRouter),
            });
        } else if !self.reasoning.is_empty() {
            content.push(ContentBlock::Reasoning {
                text: std::mem::take(&mut self.reasoning),
                signature: None,
            });
        }
        if !self.text.is_empty() {
            content.push(ContentBlock::Text {
                text: std::mem::take(&mut self.text),
            });
        }
        // Tool arguments are parsed here, from `finish()` at EOF, which is after the chunk that
        // carried `finish_reason`. So a `length` finish is already known when a buffer fails to
        // parse, and it proves the ceiling cut the call rather than the stream being malformed.
        let ceiling = (self.finish_reason == Some(FinishReason::Length))
            .then_some(self.raw_finish_reason.as_deref())
            .flatten()
            .map(|provider_stop_reason| crate::wire::CeilingStop {
                provider_stop_reason,
                max_output_tokens: self.max_output_tokens,
                output_tokens: self.usage.output_tokens,
            });
        for (slot, buf) in std::mem::take(&mut self.tool_calls).into_iter().enumerate() {
            let Some(input) =
                self.arguments_guard
                    .decode(&buf.arguments, &buf.name, ceiling, false)?
            else {
                continue;
            };
            content.push(ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(buf.id),
                tool_name: buf.name,
                input,
                thought_signature: None,
            }));
            keys.push((2, slot as u64));
        }
        self.display_parts
            .retain(&keys, "openai_chat", self.step_id, out);
        Ok(AssistantMessage {
            message_id: self.message_id.clone().unwrap_or(MessageId(String::new())),
            content,
        })
    }
}

impl StreamNormalizer for OpenAiChatNormalizer {
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.sse.content_progress()
    }
    fn push(&mut self, chunk: &[u8]) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        if self.emitted_final {
            return Ok(Vec::new());
        }
        let mut out = Vec::new();
        let payloads = self.sse.push(chunk)?;
        if self.sse.saw_done() {
            self.terminal_observed = true;
        }
        for payload in payloads {
            self.handle_chunk(&payload, &mut out)?;
            if self.arguments_guard.fired() {
                self.terminal_observed = true;
                self.finish_reason = Some(FinishReason::ToolCalls);
                out.extend(self.finish()?);
                break;
            }
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
        if !self.arguments_guard.fired() {
            for payload in self.sse.flush()? {
                self.handle_chunk(&payload, &mut out)?;
            }
        }
        if self.sse.saw_done() {
            self.terminal_observed = true;
        }
        if self.emitted_final || !self.started {
            return Ok(out);
        }
        if !self.terminal_observed {
            return Err(NormalizeError::Invalid(
                "OpenAI chat stream truncated: ended before a terminal chunk or [DONE]".into(),
            ));
        }
        self.emitted_final = true;
        let finish_reason = match self.finish_reason {
            Some(FinishReason::Stop) | None if self.saw_refusal => FinishReason::ContentFilter,
            Some(finish_reason) => finish_reason,
            None => FinishReason::Other,
        };
        let message = self.assemble_message(&mut out)?;
        let finish_reason = if self.arguments_guard.fired() {
            FinishReason::ToolCalls
        } else {
            finish_reason
        };
        out.extend(self.arguments_guard.event());
        out.extend([
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }),
            NormalizedEvent::Control(ProviderControl::StepFinished {
                step_id: self.step_id,
                usage: self.usage,
                finish_reason,
                service_tier_used: self.service_tier_used.clone(),
                speed_used: None,
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: self.usage,
                finish_reason,
                retries_used: 0,
            }),
        ]);
        Ok(out)
    }
}

fn openai_chat_stream_error(envelope: &Value) -> Option<ProviderError> {
    let error = envelope.get("error")?.as_object()?;
    let code = error.get("code").and_then(Value::as_str);
    let error_type = error.get("type").and_then(Value::as_str);
    // An exhausted prepaid balance is about the credential, not this request: relabel it so the
    // run's terminal names the credential and routing stops using it.
    let provider_code = if crate::normalize::openai_billing_exhausted(code, error_type) {
        Some(broca_protocol::error::BILLING_EXHAUSTED)
    } else {
        code.or(error_type)
    };
    let status = error_status(error, envelope);
    let retry_after_secs = retry_after_secs(error, envelope);
    let message = error
        .get("message")
        .and_then(Value::as_str)
        .unwrap_or("OpenAI Chat stream reported an error")
        .to_owned();

    Some(ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: classify_error_with_status_defaults(provider_code, status),
        message,
        status,
        retry_after_secs,
        provider_code: provider_code.map(str::to_owned),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    })
}

fn error_status(error: &serde_json::Map<String, Value>, envelope: &Value) -> Option<u16> {
    ["status", "status_code"].into_iter().find_map(|key| {
        error
            .get(key)
            .or_else(|| envelope.get(key))
            .and_then(|value| {
                value
                    .as_u64()
                    .and_then(|status| u16::try_from(status).ok())
                    .or_else(|| value.as_str()?.parse::<u16>().ok())
            })
    })
}

fn retry_after_secs(error: &serde_json::Map<String, Value>, envelope: &Value) -> Option<f64> {
    for source in [Some(error), envelope.as_object()].into_iter().flatten() {
        for key in ["retry_after_secs", "retry_after", "retryAfter"] {
            if let Some(seconds) = source.get(key).and_then(nonnegative_number_or_string) {
                return Some(seconds);
            }
        }
        for key in ["retry_after_ms", "retryAfterMs"] {
            if let Some(milliseconds) = source.get(key).and_then(nonnegative_number_or_string) {
                return Some(milliseconds / 1_000.0);
            }
        }
    }
    None
}

fn number_or_string(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str()?.parse::<f64>().ok())
        .filter(|value| value.is_finite())
}

fn nonnegative_number_or_string(value: &Value) -> Option<f64> {
    number_or_string(value).filter(|value| *value >= 0.0)
}

fn map_finish_reason(fr: &str) -> FinishReason {
    match fr {
        "stop" => FinishReason::Stop,
        "length" => FinishReason::Length,
        "tool_calls" | "function_call" => FinishReason::ToolCalls,
        "content_filter" => FinishReason::ContentFilter,
        _ => FinishReason::Other,
    }
}

#[cfg(test)]
mod openai_chat_cache_write_tests {
    use super::OpenAiChatNormalizer;
    use serde_json::json;

    /// EXERCISE THE CACHE-WRITE EXTRACTION, WHICH PRODUCTION HAS NEVER EXERCISED.
    ///
    /// This family reads two cache-write spellings, and downstream those have produced a
    /// nonzero measurement EXACTLY ZERO TIMES: 21,432 openai rows, 1,625 alibaba-token-plan
    /// and 6 openrouter -- the two gateways the extra spelling was added for -- all zero.
    /// So "the parser reads both spellings" was a claim about SOURCE. A defect that read a
    /// key into a local which never reached the output would produce the same all-zero
    /// table and survive the same inspection, because its only evidence is an absence.
    ///
    /// Input-determined, so no provider capture is needed to assert what the parser does
    /// with a body it is handed.
    /// Both SPELLINGS and both PLACEMENTS. The anthropic-spelled key is reported by anthropic
    /// at the top level of the usage object, so a gateway can carry it either way and a
    /// nested-only lookup misses the top-level form silently.
    #[test]
    fn cache_write_extraction_fires_at_the_top_level_as_well_as_nested() {
        for key in ["cache_write_tokens", "cache_creation_input_tokens"] {
            let mut n =
                OpenAiChatNormalizer::new(super::StepId(1), crate::policy::UsagePolicy::default());
            n.merge_usage(&json!({
                "prompt_tokens": 100,
                "completion_tokens": 5,
                key: 23,
                "prompt_tokens_details": { "cached_tokens": 80 },
            }));
            assert_eq!(
                n.usage.cache_write_tokens,
                Some(23),
                "the top-level `{key}` placement was not read"
            );
        }
    }

    /// A NESTED key still wins over a top-level one, so widening cannot change what a
    /// provider that already reported correctly produces.
    #[test]
    fn a_nested_cache_write_still_takes_precedence_over_a_top_level_one() {
        let mut n =
            OpenAiChatNormalizer::new(super::StepId(1), crate::policy::UsagePolicy::default());
        n.merge_usage(&json!({
            "prompt_tokens": 100,
            "completion_tokens": 5,
            "cache_write_tokens": 99,
            "prompt_tokens_details": { "cached_tokens": 80, "cache_write_tokens": 17 },
        }));
        assert_eq!(
            n.usage.cache_write_tokens,
            Some(17),
            "the nested value must win"
        );
    }

    #[test]
    fn cache_write_extraction_fires_for_each_spelling_it_claims_to_read() {
        for key in ["cache_write_tokens", "cache_creation_input_tokens"] {
            let mut n =
                OpenAiChatNormalizer::new(super::StepId(1), crate::policy::UsagePolicy::default());
            n.merge_usage(&json!({
                "prompt_tokens": 100,
                "completion_tokens": 5,
                "prompt_tokens_details": { "cached_tokens": 80, key: 17 },
            }));
            assert_eq!(
                n.usage.cache_write_tokens,
                Some(17),
                "the `{key}` spelling is read at source but did not reach the output"
            );
        }
    }
}
