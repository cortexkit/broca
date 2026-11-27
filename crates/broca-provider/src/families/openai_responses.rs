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

use broca_protocol::wire_node::Node;
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

use crate::families::{
    apply_generation_controls, merge_inclusive_input_tokens_with_writes, TextRun,
};
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
        self.shape_with_provenance(frozen, opts, None)
    }

    fn rendered_len(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> usize {
        with_platform_body(frozen, None, opts, None, None, |body| body.canonical_len())
    }

    fn shape_with_provenance(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Value {
        with_platform_body(frozen, None, opts, None, provenance, |body| body.to_value())
    }

    fn render_with_provenance(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Vec<u8> {
        with_platform_body(frozen, None, opts, None, provenance, |body| {
            body.canonical_bytes()
        })
    }

    fn render_with_shared_name_map(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
        name_map: &crate::tool_name_alias::ToolNameMap,
    ) -> Vec<u8> {
        with_platform_body(frozen, Some(name_map), opts, None, provenance, |body| {
            body.canonical_bytes()
        })
    }

    fn shape_with_media(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        payloads: &[crate::media_render::InlineMedia<'_>],
    ) -> Value {
        self.shape_with_media_and_provenance(frozen, opts, payloads, None)
    }

    fn shape_with_media_and_provenance(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        payloads: &[crate::media_render::InlineMedia<'_>],
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Value {
        let media = crate::media_render::MediaRender { frozen, payloads };
        with_platform_body(
            frozen,
            None,
            opts,
            frozen.media.as_ref().map(|_| &media),
            provenance,
            |body| body.to_value(),
        )
    }

    fn render_with_media_and_provenance(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        payloads: &[crate::media_render::InlineMedia<'_>],
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Vec<u8> {
        let media = crate::media_render::MediaRender { frozen, payloads };
        with_platform_body(
            frozen,
            None,
            opts,
            frozen.media.as_ref().map(|_| &media),
            provenance,
            |body| body.canonical_bytes(),
        )
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

/// A `/responses` body with nodes that borrow message and schema text. The other members are
/// small, so the platform and the ChatGPT backend shape them in place as JSON.
pub(crate) struct ResponsesBody<'a> {
    /// The body without `input` and the rendered tool nodes, as a JSON object.
    pub rest: Value,
    pub input: Vec<Node<'a>>,
    pub tools: Vec<Node<'a>>,
}

impl<'a> ResponsesBody<'a> {
    /// The whole body as one node.
    pub fn into_node(self) -> Node<'a> {
        let Value::Object(rest) = self.rest else {
            unreachable!("the Responses body is built as an object");
        };
        let mut members: Vec<(Cow<'a, str>, Node<'a>)> = rest
            .into_iter()
            .map(|(key, value)| (Cow::Owned(key), Node::value(value)))
            .collect();
        members.push(("input".into(), Node::Array(self.input)));
        if !self.tools.is_empty() {
            members.push(("tools".into(), Node::Array(self.tools)));
        }
        Node::Object(members)
    }
}

/// Hands `use_body` the platform request body: the shared body with the platform's cache
/// hints.
fn with_platform_body<R>(
    frozen: &FrozenRenderConfig,
    shared_name_map: Option<&crate::tool_name_alias::ToolNameMap>,
    opts: &CallOptions,
    media: Option<&crate::media_render::MediaRender<'_>>,
    provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    use_body: impl FnOnce(&Node<'_>) -> R,
) -> R {
    with_prepared_request_body(
        frozen,
        shared_name_map,
        opts,
        media,
        provenance,
        |prepared, _| {
            let mut body = prepared.body;
            finish_platform_body(frozen, &mut body.rest);
            // A session whose effort changed sends the first run's effort here; the later runs'
            // efforts went into the input as configuration updates.
            if let Some(anchor) = prepared.effort_anchor {
                if body.rest["reasoning"].is_object() {
                    body.rest["reasoning"]["effort"] = Value::String(anchor);
                }
            }
            use_body(&body.into_node())
        },
    )
}

fn finish_platform_body(frozen: &FrozenRenderConfig, body: &mut Value) {
    apply_prompt_cache_hints(body, frozen);
    if let Some(cache_key) = &frozen.prompt_cache_key {
        body["prompt_cache_key"] = Value::String(cache_key.clone());
    }
}

/// The shared Responses body together with the inputs it was rendered from, so the ChatGPT
/// wrapper can finish the body without preparing the request or planning effort a second
/// time: each of those clones the prompt or parses every episode's frozen config.
pub(crate) struct PreparedRequestBody<'a> {
    pub body: ResponsesBody<'a>,
    /// The request as structured output prepares it.
    pub prepared: &'a CallOptions,
    /// The effort the request's top level keeps when later runs' efforts go in as input
    /// configuration updates; `None` when no plan applies.
    pub effort_anchor: Option<String>,
}

/// Hands `use_body` the shared Responses body. When the caller asks for `provenance` (where
/// each rendered input item came from), it is filled in while the body is built and handed to
/// `use_body` complete. The body borrows the prompt's content, and the request prepared from
/// `opts` lives only in this call, so the body is used here rather than returned.
pub(crate) fn with_prepared_request_body<R>(
    frozen: &FrozenRenderConfig,
    shared_name_map: Option<&crate::tool_name_alias::ToolNameMap>,
    opts: &CallOptions,
    media: Option<&crate::media_render::MediaRender<'_>>,
    mut provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    use_body: impl FnOnce(
        PreparedRequestBody<'_>,
        Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> R,
) -> R {
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
    let view = crate::families::compaction_view(frozen.family_id, prepared.as_ref());
    let opts = view.as_ref();
    let owned_name_map;
    let name_map = match shared_name_map {
        Some(name_map) => name_map,
        None => {
            owned_name_map = crate::tool_name_alias::map_for(frozen, &opts.tools, &opts.prompt);
            &owned_name_map
        }
    };
    let body = render_prepared_body(
        frozen,
        opts,
        name_map,
        &updates,
        media,
        provenance.as_deref_mut(),
    );
    use_body(
        PreparedRequestBody {
            body,
            prepared: prepared.as_ref(),
            effort_anchor,
        },
        provenance,
    )
}

fn render_prepared_body<'a>(
    frozen: &FrozenRenderConfig,
    opts: &'a CallOptions,
    name_map: &'a crate::tool_name_alias::ToolNameMap,
    updates: &[super::openai_responses_chatgpt::EffortUpdate],
    media: Option<&crate::media_render::MediaRender<'_>>,
    provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
) -> ResponsesBody<'a> {
    let instructions = instructions_from_policy(&opts.prompt, frozen.system_prompt.clone());
    let developer_message = frozen.system_prompt == SystemPromptPolicy::DeveloperMessage;
    let mut provenance = provenance;
    // A fork's ask that lowers its effort sends it as an update before its question, the
    // prompt's last message, so only its answer uses it (see `effort_change::answer_effort`).
    let answer_update = opts
        .render_episodes
        .answer_effort
        .as_ref()
        .filter(|_| super::openai_responses_chatgpt::takes_effort_updates(frozen))
        .map(|effort| super::openai_responses_chatgpt::EffortUpdate {
            before: 0,
            effort: effort.clone(),
            synthetic_user: false,
        });
    let mut input = convert_input_items_with_provenance(
        &opts.prompt,
        frozen.system_prompt.clone(),
        name_map,
        EffortUpdates {
            per_run: updates,
            answer: answer_update.as_ref(),
        },
        &ReplayPolicy {
            target_model: opts.target_model.as_ref(),
            target_family: frozen.family_id,
            provider_text_state: frozen.provider_text_state,
            stateless: frozen.static_options.get("store") == Some(&Value::Bool(false)),
            removed_reasoning: Some(&opts.render_episodes.removed_responses_reasoning),
        },
        media,
        provenance.as_deref_mut(),
    );

    let mut body = json!({
        "model": frozen.wire_model_id,
        "stream": true,
    });
    body[frozen.token_limit_field.key()] = json!(frozen.max_output_tokens);
    match instructions {
        Some(text) if developer_message => {
            input.insert(
                0,
                Node::value(json!({
                    "type": "message", "role": "developer",
                    "content": [{"type": "input_text", "text": text}],
                })),
            );
            if let Some(sink) = provenance {
                sink.prepend_synthetic_message();
            }
        }
        Some(text) => body["instructions"] = Value::String(text),
        None => {}
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

    let mut tools: Vec<_> = opts
        .tools
        .iter()
        .map(|tool| convert_tool(tool, name_map, frozen.responses_tools_non_strict))
        .collect();
    if !tools.is_empty() {
        body["tool_choice"] = convert_tool_choice(&opts.tool_choice, name_map);
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
        &mut tools,
        name_map.wire_name(broca_protocol::structured_output::SUBMIT_RESULT_TOOL),
    );
    // Provider-required request flags are frozen at admission, so enabling stateless reasoning
    // for a host does not change the bytes of runs admitted before that policy existed.
    if let Some(options) = frozen.static_options.as_object() {
        for (key, value) in options {
            body[key] = value.clone();
        }
    }
    // An explicit static replacement takes precedence over the rendered declarations.
    if body.get("tools").is_some() {
        tools.clear();
    }
    ResponsesBody {
        rest: body,
        input,
        tools,
    }
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

/// The system text a Responses request carries outside its conversation items: as the
/// top-level `instructions` string, or as one leading `developer` message (see
/// [`SystemPromptPolicy::DeveloperMessage`]). Either way it is every non-turn system message's
/// text, joined by newlines.
fn instructions_from_policy(prompt: &[Message], policy: SystemPromptPolicy) -> Option<String> {
    if !matches!(
        policy,
        SystemPromptPolicy::InstructionsField | SystemPromptPolicy::DeveloperMessage
    ) {
        return None;
    }
    let mut parts = Vec::new();
    for m in prompt {
        if m.role == Role::System && !m.turn_system {
            let t = text_string(&m.content);
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
    removed_reasoning: Option<&'a std::collections::HashSet<String>>,
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
    let mut items: Vec<Value> = convert_assistant_items(
        &message,
        name_map,
        &ReplayPolicy {
            target_model,
            target_family: frozen.family_id,
            provider_text_state: frozen.provider_text_state,
            stateless,
            removed_reasoning: None,
        },
    )
    .iter()
    .map(Node::to_value)
    .collect();
    if frozen.chatgpt_wire.is_some() {
        crate::chatgpt_wire::typed_message_content(&mut items);
    }
    items
}

fn convert_input_items_with_provenance<'a>(
    prompt: &'a [Message],
    system_policy: SystemPromptPolicy,
    name_map: &crate::tool_name_alias::ToolNameMap,
    updates: EffortUpdates<'_>,
    replay: &ReplayPolicy<'_>,
    media: Option<&crate::media_render::MediaRender<'_>>,
    mut provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
) -> Vec<Node<'a>> {
    let skip_system = matches!(
        system_policy,
        SystemPromptPolicy::InstructionsField | SystemPromptPolicy::DeveloperMessage
    );
    let mut out = Vec::new();
    let answer_update = updates.answer;
    let mut updates = updates.per_run.iter().peekable();
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
                let start = provenance.as_ref().map(|_| out.len());
                append_effort_update(
                    &mut out,
                    update,
                    message.role == Role::User && ordinal as usize == update.before,
                );
                if let Some((sink, start)) = provenance.as_deref_mut().zip(start) {
                    sink.record_messages(start, out.len(), None);
                }
            }
        }
        if skip_system && message.role == Role::System && !message.turn_system {
            continue;
        }
        if let Some(update) =
            answer_update.filter(|_| index + 1 == prompt.len() && message.role == Role::User)
        {
            let start = provenance.as_ref().map(|_| out.len());
            append_effort_update(&mut out, update, true);
            if let Some((sink, start)) = provenance.as_deref_mut().zip(start) {
                sink.record_messages(start, out.len(), None);
            }
        }
        let start = provenance.as_ref().map(|_| out.len());
        match message.role {
            Role::System | Role::User => out.push(convert_easy_message(message, media)),
            Role::Assistant => out.extend(convert_assistant_items(message, name_map, replay)),
            Role::Tool => out.extend(convert_tool_output_items(message, media)),
        }
        if let Some((sink, start)) = provenance.as_deref_mut().zip(start) {
            sink.record_messages(start, out.len(), message.ordinal);
        }
    }
    for update in updates {
        let start = provenance.as_ref().map(|_| out.len());
        append_effort_update(&mut out, update, false);
        if let Some((sink, start)) = provenance.as_deref_mut().zip(start) {
            sink.record_messages(start, out.len(), None);
        }
    }
    out
}

/// The effort updates a request's input carries: one per run whose effort changed while
/// keeping the cache, each before its run's first user message, and a fork ask's lowered
/// effort for its answer alone, right before its question.
struct EffortUpdates<'u> {
    per_run: &'u [super::openai_responses_chatgpt::EffortUpdate],
    answer: Option<&'u super::openai_responses_chatgpt::EffortUpdate>,
}

fn append_effort_update(
    out: &mut Vec<Node<'_>>,
    update: &super::openai_responses_chatgpt::EffortUpdate,
    user_follows: bool,
) {
    out.push(Node::value(
        json!({"type": "configuration_update", "reasoning": {"effort": update.effort}}),
    ));
    if update.synthetic_user || !user_follows {
        out.push(Node::object([
            ("type", Node::str("message")),
            ("role", Node::str("user")),
            (
                "content",
                Node::str("Continue with the updated reasoning effort."),
            ),
        ]));
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
fn reasoning_request_item(raw_response_item: &str, replay: &ReplayPolicy<'_>) -> Option<Value> {
    let response: Value = serde_json::from_str(raw_response_item).ok()?;
    if response.get("type").and_then(Value::as_str) != Some("reasoning") {
        return None;
    }
    // The provider stripped this item before the model saw the prompt, so omitting its echo
    // leaves the model's input unchanged. Keep the text flush at the call site: dropping a
    // reasoning item must not join the text items on either side of it.
    if replay.removed_reasoning.is_some_and(|removed| {
        response["id"]
            .as_str()
            .is_some_and(|id| removed.contains(id))
    }) {
        return None;
    }
    let encrypted_content = response.get("encrypted_content")?.as_str()?;
    if replay.stateless {
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
fn user_text_content<'a>(
    blocks: &'a [ContentBlock],
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Node<'a> {
    let input_text = |text: &'a str| {
        Node::object([("type", Node::str("input_text")), ("text", Node::str(text))])
    };
    if blocks
        .iter()
        .any(|block| matches!(block, ContentBlock::Media(_)))
    {
        return Node::Array(
            blocks
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                        Some(input_text(text))
                    }
                    ContentBlock::Media(reference) => {
                        Some(Node::value(responses_media_block(reference, media, false)))
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
    let texts: Vec<&'a str> = blocks
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                Some(text.as_str())
            }
            _ => None,
        })
        .collect();
    if texts.len() > 1 {
        Node::Array(texts.into_iter().map(input_text).collect())
    } else {
        text_content(blocks)
    }
}

fn convert_easy_message<'a>(
    m: &'a Message,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Node<'a> {
    if m.turn_system {
        return Node::object([
            ("role", Node::str("developer")),
            (
                "content",
                Node::Array(vec![Node::object([
                    ("type", Node::str("input_text")),
                    ("text", Node::string(crate::turn_system::text(m))),
                ])]),
            ),
        ]);
    }
    let role = match m.role {
        Role::System => "system",
        Role::User => "user",
        Role::Assistant => "assistant",
        Role::Tool => "user",
    };
    Node::object([
        ("type", Node::str("message")),
        ("role", Node::str(role)),
        (
            "content",
            if m.role == Role::User {
                user_text_content(&m.content, media)
            } else {
                text_content(&m.content)
            },
        ),
    ])
}

fn convert_assistant_items<'a>(
    message: &'a Message,
    name_map: &crate::tool_name_alias::ToolNameMap,
    replay: &ReplayPolicy<'_>,
) -> Vec<Node<'a>> {
    fn flush_text<'a>(out: &mut Vec<Node<'a>>, text: &mut TextRun<'a>) {
        if text.is_empty() {
            return;
        }
        out.push(Node::object([
            ("type", Node::str("message")),
            ("role", Node::str("assistant")),
            ("content", text.take()),
        ]));
    }

    let echo_safe = crate::families::signed_reasoning_is_echo_safe(
        message.origin.as_ref(),
        replay.target_model,
    );
    let mut out = Vec::new();
    let mut text = TextRun::default();
    for block in &message.content {
        match block {
            ContentBlock::Media(media) => {
                text.push(Cow::Owned(broca_protocol::content::media_not_shown_marker(
                    media,
                    broca_protocol::MediaMarkerReason::Carrier,
                )))
            }
            ContentBlock::Text { text: part } => text.push(Cow::Borrowed(part)),
            ContentBlock::TextWithState {
                text: part,
                provider_state,
            } => {
                if replay.provider_text_state {
                    flush_text(&mut out, &mut text);
                    let mut item = Node::object([
                        ("type", Node::str("message")),
                        ("role", Node::str("assistant")),
                        ("content", Node::str(part)),
                    ]);
                    let mut has_phase = false;
                    if super::text_state_is_echo_safe(
                        message,
                        replay.target_model,
                        provider_state.format,
                        replay.target_family,
                    ) {
                        if let Ok(mut state) = serde_json::from_str::<Value>(&provider_state.data) {
                            if let Some(phase) = state.get_mut("phase") {
                                item.insert("phase", Node::value(phase.take()));
                                has_phase = true;
                            }
                        }
                    }
                    // Each block is one output item; never merge adjacent items with equal phases.
                    if !part.is_empty() || has_phase {
                        out.push(item);
                    }
                } else {
                    text.push(Cow::Borrowed(part));
                }
            }
            ContentBlock::RedactedReasoning {
                data,
                provider_format: Some(provider_format),
            } if echo_safe
                && reasoning_format_matches_family(*provider_format, replay.target_family) =>
            {
                flush_text(&mut out, &mut text);
                if let Some(item) = reasoning_request_item(data, replay) {
                    out.push(Node::value(item));
                }
            }
            ContentBlock::ToolCall(call) => {
                flush_text(&mut out, &mut text);
                // The arguments are the input's JSON text: the text `Json` holds is what
                // serializing the input writes.
                let mut item = Node::object([
                    ("type", Node::str("function_call")),
                    ("call_id", Node::str(&call.tool_call_id.0)),
                    (
                        "name",
                        Node::string(name_map.wire_name(&call.tool_name).to_owned()),
                    ),
                    ("arguments", Node::str(call.input.text())),
                ]);
                if replay.provider_text_state {
                    replay_call_state(&mut item, message, call, replay);
                }
                out.push(item);
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

/// Puts back the provider's own fields of a call it wrote, so the call matches the provider's
/// copy: the Responses API drops every later reasoning item when a call before it differs
/// from that copy, which happens to every earlier call on a request that resends the history
/// (a restart, a retry on a new socket, a compaction view). The call's identity stays the
/// canonical one, and the model's own `arguments` text is used only while it still says what
/// the stored input says, so a view that edited the input sends the edit.
fn replay_call_state<'a>(
    item: &mut Node<'a>,
    message: &Message,
    call: &broca_protocol::content::ToolCall,
    replay: &ReplayPolicy<'_>,
) {
    let Some(state) = call.provider_state.as_ref().filter(|state| {
        super::text_state_is_echo_safe(
            message,
            replay.target_model,
            state.format,
            replay.target_family,
        )
    }) else {
        return;
    };
    let Ok(Value::Object(fields)) = serde_json::from_str::<Value>(&state.data) else {
        return;
    };
    for (key, value) in fields {
        match (key.as_str(), value) {
            ("type" | "id" | "call_id" | "name", _) => {}
            ("arguments", Value::String(raw)) => {
                if broca_protocol::json::Json::from_text(&raw)
                    .is_ok_and(|canonical| canonical.text() == call.input.text())
                {
                    item.insert("arguments", Node::string(raw));
                }
            }
            ("arguments", _) => {}
            (_, value) => item.insert(key, Node::value(value)),
        }
    }
}

fn convert_tool_output_items<'a>(
    m: &'a Message,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Vec<Node<'a>> {
    let mut out = Vec::new();
    for b in &m.content {
        if let ContentBlock::ToolResult(tr) = b {
            out.push(Node::object([
                ("type", Node::str("function_call_output")),
                ("call_id", Node::str(&tr.tool_call_id.0)),
                ("output", tool_output_content(&tr.output, media)),
            ]));
        }
    }
    out
}

fn tool_output_content<'a>(
    out: &'a ToolOutput,
    media: Option<&crate::media_render::MediaRender<'_>>,
) -> Node<'a> {
    match (out, media) {
        (ToolOutput::Parts { parts }, Some(media)) => Node::Array(
            parts
                .iter()
                .map(|part| match part {
                    broca_protocol::ToolOutputPart::Text { text } => {
                        Node::value(super::openai_media::Grammar::Responses.text(text))
                    }
                    broca_protocol::ToolOutputPart::Media { media: reference } => {
                        Node::value(responses_media_block(reference, Some(media), true))
                    }
                })
                .collect(),
        ),
        // The two outputs that carry their text as it is: written from the message, not
        // copied.
        (ToolOutput::Text { text }, _) => Node::str(text),
        (ToolOutput::Json { value }, _) => Node::str(value.text()),
        _ => Node::string(out.model_text()),
    }
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

/// The text of `blocks`' text blocks, joined into a string.
fn text_string(blocks: &[ContentBlock]) -> String {
    let mut s = String::new();
    for b in blocks {
        if let ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } = b {
            s.push_str(text);
        }
    }
    s
}

/// The text of `blocks`' text blocks, joined: borrowed when a single block holds it all.
fn text_content(blocks: &[ContentBlock]) -> Node<'_> {
    let mut run = TextRun::default();
    for b in blocks {
        if let ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } = b {
            run.push(Cow::Borrowed(text));
        }
    }
    if run.is_empty() {
        return Node::str("");
    }
    run.take()
}

fn convert_tool<'a>(
    t: &'a Tool,
    name_map: &'a crate::tool_name_alias::ToolNameMap,
    non_strict: bool,
) -> Node<'a> {
    // Live ChatGPT/Codex probes with read and zoom schemas showed that omitting `strict`
    // made every property required, including optional parameters. OpenAI's function-calling
    // guide says: "To opt out of strict mode in Responses and keep non-strict, best-effort
    // function calling, explicitly set `strict: false`." The admission-time flag keeps
    // pre-flag runs rendering without the field rather than changing their resumed bytes.
    let mut tool = Node::object([
        ("type", Node::str("function")),
        ("name", Node::str(name_map.wire_name(&t.name))),
        ("description", Node::str(&t.description)),
        ("parameters", Node::Json(&t.input_schema)),
    ]);
    if non_strict {
        tool.insert("strict", Node::value(Value::Bool(false)));
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

#[derive(Default)]
struct ToolCallBuf {
    call_id: String,
    name: String,
    arguments: String,
    /// The completed item's other fields (`status`, and any the API adds), kept when the
    /// endpoint replays provider state; see `replay_call_state`.
    state: Option<serde_json::Map<String, Value>>,
    /// The completed item said `status: incomplete`: the provider stopped generating the call
    /// before it was whole, so its arguments (often still `""`) are not the call's arguments.
    incomplete: bool,
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
    /// Kept as a raw value, so a report of an unexpected shape can't fail this typed parse.
    input_transformations: Option<Value>,
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
    raw_sse_observer: Option<crate::wire::RawSseObserver>,
    response_evidence: Option<crate::normalize::SharedResponseEvidence>,
    display_parts: crate::display_parts::DisplayParts,
    arguments_guard: crate::wire::ToolArgsGuard,
    completed_items: std::collections::HashSet<u64>,
    completed_text_parts: std::collections::HashSet<(u64, u64)>,
    /// Output indices of function calls whose complete arguments arrived in their own
    /// `function_call_arguments.done` event.
    finalized_calls: std::collections::HashSet<u64>,
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
    raw_finish_reason: Option<String>,
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
    /// The terminal response's `input_transformations`, verbatim: the reasoning items sent back
    /// in `input` that the API removed before the model ran, and why (`prefix_mismatch` when
    /// something before the item changed). `None` when the response carried no report.
    input_transformations: Option<Vec<broca_protocol::InputTransformation>>,
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
            response_evidence: None,
            raw_sse_observer: None,
            display_parts: Default::default(),
            arguments_guard: Default::default(),
            completed_items: Default::default(),
            completed_text_parts: Default::default(),
            finalized_calls: Default::default(),
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
            raw_finish_reason: None,
            incomplete_reason: None,
            max_output_tokens: None,
            started: false,
            completed: false,
            service_tier_used: None,
            input_transformations: None,
        }
    }

    /// Keeps a terminal response's `input_transformations` report, when it carries one.
    fn observe_input_transformations(&mut self, report: Option<&Value>) {
        if let Some(entries) = report.and_then(broca_protocol::InputTransformation::from_report) {
            self.input_transformations = Some(entries);
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

    /// The provider state a completed call keeps: its other item fields, plus the model's own
    /// `arguments` text when it is spelled differently from the canonical input text, which
    /// is what a replay writes otherwise. Most calls keep only `status`.
    fn call_state(
        &self,
        fields: Option<serde_json::Map<String, Value>>,
        arguments: &str,
        input: &broca_protocol::json::Json,
    ) -> Option<broca_protocol::content::TextProviderState> {
        let mut fields = fields?;
        if arguments != input.text() {
            fields.insert("arguments".into(), Value::String(arguments.to_owned()));
        }
        if fields.is_empty() {
            return None;
        }
        let format = match self.reasoning_format {
            EncryptedReasoningFormat::OpenAiResponses => {
                broca_protocol::content::TextProviderFormat::OpenAiResponses
            }
            EncryptedReasoningFormat::OpenAiResponsesChatgpt => {
                broca_protocol::content::TextProviderFormat::OpenAiResponsesChatgpt
            }
            EncryptedReasoningFormat::OpenRouter => return None,
        };
        Some(broca_protocol::content::TextProviderState {
            format,
            data: Value::Object(fields).to_string(),
        })
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
        event: &crate::sse::SsePayload,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        let payload = event.raw();
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
        if self.response_evidence.is_none()
            && (payload.contains("response.completed") || payload.contains("response.incomplete"))
        {
            let response = serde_json::from_str::<ResponsesTerminalEvent>(payload)
                .ok()
                .and_then(ResponsesTerminalEvent::response);
            if let Some(response) = response {
                self.complete_from_terminal(payload, out)?;
                return self.handle_terminal_response(response, out);
            }
        }

        let v = event.value()?;
        let event_type = v.get("type").and_then(Value::as_str).unwrap_or_default();
        if event_type == "response.created" {
            if let (Some(evidence), Some(response)) = (&self.response_evidence, v.get("response")) {
                evidence.lock().unwrap().observe_response(response);
            }
        }
        if event_type == "response.failed" {
            self.observe_input_transformations(
                v.get("response")
                    .and_then(|response| response.get("input_transformations")),
            );
        }
        if self.response_evidence.is_some() && event_type == "response.failed" {
            if let Some(response) = v.get("response") {
                if let Some(usage) = response.get("usage") {
                    self.merge_usage(usage);
                }
                self.response_evidence
                    .as_ref()
                    .unwrap()
                    .lock()
                    .unwrap()
                    .observe_response(response);
            }
        }

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
                    error: response_stream_error(v, false, self.private_error_diagnostics),
                    retries_used: 0,
                }));
            }
            "response.failed" => {
                out.push(NormalizedEvent::Control(ProviderControl::Error {
                    error: response_stream_error(v, true, self.private_error_diagnostics),
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
                            tool_name: crate::normalize::known_name(&buf.name),
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
                    self.finalized_calls.insert(idx);
                    let buf = self.tool_calls.entry(idx).or_default();
                    buf.arguments = args.to_owned();
                    self.arguments_guard
                        .terminal(&buf.call_id, &buf.name, &buf.arguments);
                }
            }
            // The completed item also carries the full final state; treat it as one more
            // authoritative source for the same fields (idempotent with `added`/`done`,
            // and covers a hypothetical stream that skips the arguments.done event).
            "response.output_item.done" => {
                if let Some(item) = v.get("item") {
                    let idx = v.get("output_index").and_then(Value::as_u64).unwrap_or(0);
                    self.complete_item(idx, item, RawItem::Envelope(payload), out)?;
                }
            }
            // Escaped or unusually ordered type discriminators miss the terminal prefilter but
            // still retain the previous permissive Value-based behavior.
            "response.completed" | "response.incomplete" => {
                if let Some(resp) = v.get("response") {
                    if let Some(evidence) = &self.response_evidence {
                        evidence.lock().unwrap().observe_response(resp);
                    }
                    if let Some(id) = resp.get("id").and_then(Value::as_str) {
                        self.response_id = Some(MessageId(id.to_owned()));
                    }
                    if let Some(u) = resp.get("usage") {
                        self.merge_usage(u);
                    }
                    if let Some(status) = resp.get("status").and_then(Value::as_str) {
                        self.raw_finish_reason = Some(
                            match (
                                status,
                                resp.pointer("/incomplete_details/reason")
                                    .and_then(Value::as_str),
                            ) {
                                ("incomplete", Some(reason)) => format!("incomplete:{reason}"),
                                _ => status.to_owned(),
                            },
                        );
                        self.finish_reason = map_response_status(status);
                    }
                    if let Some(tier) = resp.get("service_tier").and_then(Value::as_str) {
                        self.service_tier_used = Some(tier.to_owned());
                    }
                    self.observe_input_transformations(resp.get("input_transformations"));
                    if let Some(details) = resp.get("incomplete_details") {
                        if let Some(reason) = details.get("reason").and_then(Value::as_str) {
                            self.finish_reason = map_incomplete_reason(reason);
                            self.incomplete_reason = Some(reason.to_owned());
                        }
                    }
                    if let Some(evidence) = &self.response_evidence {
                        let reason = resp
                            .pointer("/incomplete_details/reason")
                            .and_then(Value::as_str)
                            .map(map_incomplete_reason)
                            .or_else(|| {
                                resp.get("status")
                                    .and_then(Value::as_str)
                                    .map(map_response_status)
                            });
                        if reason.is_some() {
                            evidence.lock().unwrap().finish_reason = reason;
                        }
                    }
                }
                self.complete_from_terminal(payload, out)?;
                self.finish_terminal(out)?;
            }
            _ => {}
        }
        if self.arguments_guard.fired() && !self.completed {
            self.finish_reason = FinishReason::Length;
            self.finish_terminal(out)?;
        }
        Ok(())
    }

    /// Takes a completed output item at `idx` as the authoritative final state of what its
    /// streamed events built: from its `response.output_item.done` event, or from the terminal
    /// response's `output` when the stream never sent that event.
    fn complete_item(
        &mut self,
        idx: u64,
        item: &Value,
        raw: RawItem<'_>,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
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
            self.display_parts
                .reserve((idx, kind), "openai_responses", self.step_id, out);
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
                buf.incomplete = item.get("status").and_then(Value::as_str) == Some("incomplete");
                // Presence is authoritative even when the final value is empty:
                // `decode_tool_arguments` maps an empty string to `{}` for a
                // zero-argument call. Absence alone preserves accumulated deltas.
                if let Some(args) = item.get("arguments").and_then(Value::as_str) {
                    buf.arguments = args.to_owned();
                    self.arguments_guard
                        .terminal(&buf.call_id, &buf.name, &buf.arguments);
                }
                if self.provider_text_state {
                    buf.state = item.as_object().map(|fields| {
                        fields
                            .iter()
                            .filter(|(key, _)| {
                                !matches!(
                                    key.as_str(),
                                    "type" | "id" | "call_id" | "name" | "arguments"
                                )
                            })
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect()
                    });
                }
            }
            Some("reasoning") => {
                // Preserve the provider's complete item token rather than rebuilding
                // it: id, summary, encrypted_content, metadata, and key order all
                // survive persistence. Request rendering later extracts the fields
                // needed for continuation and removes response-only fields.
                let raw_item = match raw {
                    RawItem::Envelope(payload) => raw_output_item(payload)?,
                    RawItem::Item(item) => item.to_owned(),
                };
                self.reasoning_items.insert(idx, raw_item);
            }
            _ => {
                self.complete_text(idx, message_item_output_text_parts(item));
                if let Some(refusal) = message_item_refusal(item) {
                    self.refusal_completed = true;
                    // A completed message item repeats the full refusal content. Use
                    // it as the authoritative fallback if refusal delta/done events
                    // were omitted, matching the function-call completion behavior.
                    self.saw_refusal = true;
                    self.refusal_output_index = Some(idx);
                    self.refusal = refusal;
                    if !self.refusal.is_empty() {
                        self.display_parts
                            .reserve((idx, 2), "openai_responses", self.step_id, out);
                    }
                }
            }
        }
        Ok(())
    }

    /// Takes the `output_text` parts of a completed message item at `idx`. A part no streamed
    /// event supplied is filled in. When no part of the item was finalized by its own
    /// `output_text.done` and the streamed text is a strict prefix of the completed text, the
    /// stream was cut short and the completed parts replace the streamed ones. Parts are
    /// compared joined, since a completed item may carry as one part text that streamed as
    /// several: replacing per part would repeat it. When some part was finalized, the stream's
    /// part boundaries are the provider's own, so each part that was not finalized is repaired
    /// on its own: its streamed text, when a strict prefix of the same completed part, is
    /// replaced by it.
    fn complete_text(&mut self, idx: u64, parts: Vec<(u64, &str)>) {
        let item = (idx, 0)..=(idx, u64::MAX);
        let finalized = self
            .completed_text_parts
            .iter()
            .any(|(index, _)| *index == idx);
        let streamed: String = self
            .text_items
            .range(item.clone())
            .map(|(_, text)| text.as_str())
            .collect();
        let complete: String = parts.iter().map(|(_, text)| *text).collect();
        if !finalized
            && !streamed.is_empty()
            && complete.len() > streamed.len()
            && complete.starts_with(&streamed)
        {
            let streamed_parts: Vec<_> = self.text_items.range(item).map(|(key, _)| *key).collect();
            for key in streamed_parts {
                self.text_items.remove(&key);
            }
            for (part_idx, text) in parts {
                self.text_items.insert((idx, part_idx), text.to_owned());
            }
            return;
        }
        for (part_idx, text) in parts {
            let key = (idx, part_idx);
            match self.text_items.get_mut(&key) {
                Some(streamed)
                    if finalized
                        && !self.completed_text_parts.contains(&key)
                        && text.len() > streamed.len()
                        && text.starts_with(streamed.as_str()) =>
                {
                    *streamed = text.to_owned();
                }
                Some(_) => {}
                None => {
                    self.text_items.insert(key, text.to_owned());
                }
            }
        }
    }

    /// Takes the terminal response's `output` items the stream never completed with their own
    /// `response.output_item.done` event, so content that arrived only in the terminal response
    /// isn't lost. A function call whose `function_call_arguments.done` already arrived keeps
    /// those arguments. A terminal without an `output` array, or one that doesn't decode, adds
    /// nothing: its other members were already read.
    fn complete_from_terminal(
        &mut self,
        payload: &str,
        out: &mut Vec<NormalizedEvent>,
    ) -> Result<(), NormalizeError> {
        let Ok(TerminalOutputEnvelope {
            response: Some(TerminalOutput {
                output: Some(items),
            }),
        }) = serde_json::from_str::<TerminalOutputEnvelope>(payload)
        else {
            return Ok(());
        };
        for (idx, raw) in items.iter().enumerate() {
            let idx = idx as u64;
            if self.completed_items.contains(&idx) {
                continue;
            }
            let Ok(mut item) = serde_json::from_str::<Value>(raw.get()) else {
                continue;
            };
            if self.finalized_calls.contains(&idx) {
                if let Some(item) = item.as_object_mut() {
                    item.remove("arguments");
                }
            }
            self.complete_item(idx, &item, RawItem::Item(raw.get()), out)?;
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
            self.raw_finish_reason = Some(
                match (
                    status.as_str(),
                    response
                        .incomplete_details
                        .as_ref()
                        .and_then(|details| details.reason.as_deref()),
                ) {
                    ("incomplete", Some(reason)) => format!("incomplete:{reason}"),
                    _ => status,
                },
            );
        }
        if let Some(tier) = response.service_tier {
            self.service_tier_used = Some(tier);
        }
        self.observe_input_transformations(response.input_transformations.as_ref());
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
            out.extend(self.arguments_guard.events());
            out.push(NormalizedEvent::Control(
                ProviderControl::AssistantMessage { message },
            ));
            out.push(NormalizedEvent::Control(ProviderControl::StepFinished {
                raw_finish_reason: self.raw_finish_reason.take(),
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
        if let Some(evidence) = &self.response_evidence {
            evidence.lock().unwrap().observe_usage(u, self.usage);
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
            self.finish_reason = FinishReason::Length;
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
                    data: raw_item.into(),
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
                    text: text.into(),
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
                ordered.push((index, 1, ContentBlock::Text { text: text.into() }));
            }
        }
        if !self.refusal.is_empty() && (!self.arguments_guard.fired() || self.refusal_completed) {
            ordered.push((
                self.refusal_output_index.take().unwrap_or(0),
                2,
                ContentBlock::Text {
                    text: std::mem::take(&mut self.refusal).into(),
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
            // Only a length cut gives a provider-incomplete call a not-run result. Other
            // provider-incomplete items stay out of the answer as before.
            if buf.incomplete && ceiling.is_none() && !self.arguments_guard.fired() {
                continue;
            }
            let input = if buf.incomplete {
                self.arguments_guard.refuse(&buf.call_id, &buf.name)
            } else {
                self.arguments_guard.decode(
                    &buf.call_id,
                    &buf.arguments,
                    &buf.name,
                    ceiling,
                    self.completed_items.contains(&index),
                )?
            };
            let input: broca_protocol::json::Json = input.into();
            let provider_state = if self.arguments_guard.refused_call(&buf.call_id) {
                None
            } else {
                self.call_state(buf.state, &buf.arguments, &input)
            };
            ordered.push((
                index,
                3,
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId(buf.call_id),
                    tool_name: buf.name,
                    input,
                    thought_signature: None,
                    provider_state,
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
    fn set_raw_sse_observer(&mut self, observer: crate::wire::RawSseObserver) {
        self.raw_sse_observer = Some(observer);
    }
    fn observe_error_response(&mut self, response: &Value) {
        let response = response.get("response").unwrap_or(response);
        if let Some(usage) = response.get("usage") {
            self.merge_usage(usage);
        }
        if let Some(evidence) = &self.response_evidence {
            evidence.lock().unwrap().observe_response(response);
        }
    }
    fn set_response_evidence(&mut self, evidence: crate::normalize::SharedResponseEvidence) {
        self.response_evidence = Some(evidence);
    }
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
        for payload in self.sse.push_parsed(chunk)? {
            if let Some(observer) = &self.raw_sse_observer {
                observer(payload.raw());
            }
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

    fn input_transformations(&self) -> Option<Vec<broca_protocol::InputTransformation>> {
        self.input_transformations.clone()
    }

    fn finish(&mut self) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut out = Vec::new();
        for payload in self.sse.flush_parsed()? {
            if let Some(observer) = &self.raw_sse_observer {
                observer(payload.raw());
            }
            self.handle_event(&payload, &mut out)?;
        }
        Ok(out)
    }
}

#[derive(Deserialize)]
struct RawOutputItemEnvelope {
    item: Box<RawValue>,
}

/// Where a completed output item's own bytes come from: the `response.output_item.done` event
/// that carries it, or the item itself, taken from a terminal response's `output`.
enum RawItem<'a> {
    Envelope(&'a str),
    Item(&'a str),
}

/// The `output` items of a terminal `response.completed` or `response.incomplete` event, each
/// kept as its own bytes.
#[derive(Deserialize)]
struct TerminalOutputEnvelope {
    response: Option<TerminalOutput>,
}

#[derive(Deserialize)]
struct TerminalOutput {
    output: Option<Vec<Box<RawValue>>>,
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
    let now = std::time::SystemTime::now();
    // Only `response.failed` carries a retry time Codex reads; it ignores a bare `error`
    // event's fields (`codex-api/src/sse/responses.rs:349-352`). The value is read from the
    // provider's own message even when `message` above is replaced.
    let retry_after_secs = if nested {
        crate::codex_retry_after::failed_response_retry_after(error, now)
    } else {
        None
    };
    // A used-up plan allowance that states when it refills is the same account wall a ChatGPT
    // HTTP 429 reports: transient for routing, with its reset, and the transport ends the step
    // without re-sending to this account. One without a reset keeps ending permanent below.
    if !billing_exhausted
        && matches!(
            cortexkit_chatgpt_policy::classify_stream_failure(event),
            cortexkit_chatgpt_policy::Classification::AccountLimitExhausted { .. }
        )
    {
        if let Some((resets_at_ms, resets_at_source)) =
            account_limit_reset(error, now, retry_after_secs)
        {
            return ProviderError {
                provider_details: None,
                cause: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Transient,
                message,
                status: None,
                retry_after_secs,
                provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.to_owned()),
                resets_at_ms: Some(resets_at_ms),
                resets_at_source: Some(resets_at_source.to_owned()),
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
                account_continue: None,
            };
        }
    }
    ProviderError {
        provider_details: None,
        cause: None,
        provider_subcode: None,
        refusal: None,
        class: if billing_exhausted {
            ErrorClass::Permanent
        } else {
            response_error_class(event, code)
        },
        message,
        status: None,
        retry_after_secs,
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
        account_continue: None,
    }
}

/// When a stream failure's used-up plan allowance refills, read the way the ChatGPT policy reads
/// an HTTP 429's body: the error's `resets_at` (epoch seconds, source `body`), else `now` plus its
/// `resets_in_seconds`, else `now` plus the failure's retry time (both source `retry_after`).
fn account_limit_reset(
    error: &Value,
    now: std::time::SystemTime,
    retry_after_secs: Option<f64>,
) -> Option<(u64, &'static str)> {
    let now_ms = u64::try_from(
        now.duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis(),
    )
    .unwrap_or(u64::MAX);
    error
        .get("resets_at")
        .and_then(Value::as_u64)
        .map(|seconds| (seconds.saturating_mul(1_000), "body"))
        .or_else(|| {
            error
                .get("resets_in_seconds")
                .and_then(Value::as_u64)
                .map(|seconds| {
                    (
                        now_ms.saturating_add(seconds.saturating_mul(1_000)),
                        "retry_after",
                    )
                })
        })
        .or_else(|| {
            retry_after_secs
                .filter(|seconds| seconds.is_finite() && *seconds >= 0.0)
                .map(|seconds| {
                    (
                        now_ms.saturating_add((seconds * 1_000.0) as u64),
                        "retry_after",
                    )
                })
        })
}

fn response_error_class(event: &Value, code: Option<&str>) -> ErrorClass {
    // A used-up plan allowance with no stated reset, or a plan without the requested use, cannot
    // be granted by re-sending, so the shared ChatGPT policy's account-wall and not-included
    // stream classes end the step. Its rate-limit and overload classes are transient, as
    // `openai_stream_error_class` already makes every code it does not list.
    if matches!(
        cortexkit_chatgpt_policy::classify_stream_failure(event),
        cortexkit_chatgpt_policy::Classification::AccountLimitExhausted { .. }
            | cortexkit_chatgpt_policy::Classification::UsageNotIncluded
    ) {
        return ErrorClass::Permanent;
    }
    // The Responses backend uses unknown codes for transient failures, so only the codes known
    // to mean the request cannot succeed as sent are permanent.
    crate::normalize::openai_stream_error_class(code)
}

pub(crate) fn map_response_status(status: &str) -> FinishReason {
    match status {
        "completed" => FinishReason::Stop,
        "incomplete" => FinishReason::Length,
        "failed" | "cancelled" => FinishReason::Other,
        _ => FinishReason::Other,
    }
}

pub(crate) fn map_incomplete_reason(reason: &str) -> FinishReason {
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
    fn developer_message_policy_moves_the_system_text_into_input_and_changes_nothing_else() {
        let opts: CallOptions = serde_json::from_value(json!({"prompt":[
            {"role":"system","content":[{"type":"text","text":"rules"}]},
            {"role":"system","content":[{"type":"text","text":"more rules"}]},
            {"role":"user","content":[{"type":"text","text":"hi"}]}
        ]}))
        .unwrap();
        let instructions = crate::families::openai_responses_frozen_config("gpt-6.2-sol");
        assert_eq!(
            instructions.system_prompt,
            SystemPromptPolicy::InstructionsField
        );
        let mut developer = instructions.clone();
        developer.system_prompt = SystemPromptPolicy::DeveloperMessage;
        let mut before = OpenAiResponses.shape(&instructions, &opts);
        let mut after = OpenAiResponses.shape(&developer, &opts);
        assert_eq!(before["instructions"], "rules\nmore rules");
        assert!(after.get("instructions").is_none());
        let input = after["input"].as_array_mut().unwrap();
        assert_eq!(
            input.remove(0),
            json!({"type":"message","role":"developer",
                   "content":[{"type":"input_text","text":"rules\nmore rules"}]})
        );
        before.as_object_mut().unwrap().remove("instructions");
        assert_eq!(before, after, "only the system text moves");
    }

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

    /// A used-up plan allowance that states its reset is the account wall a ChatGPT HTTP 429
    /// reports: transient, relabelled `account_limit_exhausted`, with the reset kept, from an
    /// absolute `resets_at` or a relative `resets_in_seconds`, in either failure shape.
    #[test]
    fn a_timed_chatgpt_stream_account_wall_keeps_its_reset() {
        let resets_at = 4_102_444_800u64; // 2100-01-01, always in the future
        for event in [
            serde_json::json!({"type":"response.failed","response":{"error":{"code":"usage_limit_reached","resets_at":resets_at}}}),
            serde_json::json!({"type":"error","error":{"code":"usage_limit_reached","resets_at":resets_at}}),
        ] {
            let nested = event["type"] == "response.failed";
            let error = response_stream_error(&event, nested, false);
            assert_eq!(error.class, ErrorClass::Transient, "{event}");
            assert!(error.is_account_limit_exhausted(), "{event}");
            assert_eq!(error.resets_at_ms, Some(resets_at * 1_000), "{event}");
            assert_eq!(error.resets_at_source.as_deref(), Some("body"), "{event}");
            assert_eq!(error.status, None);
        }
        let before = crate::transport::now_millis();
        let relative = serde_json::json!({"type":"response.failed","response":{"error":{"code":"usage_limit_reached","resets_in_seconds":600}}});
        let error = response_stream_error(&relative, true, false);
        let after = crate::transport::now_millis();
        assert!(error.is_account_limit_exhausted());
        let resets_at_ms = error.resets_at_ms.expect("reset from the relative delay");
        assert!((before + 600_000..=after + 600_000).contains(&resets_at_ms));
        assert_eq!(error.resets_at_source.as_deref(), Some("retry_after"));
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
    fn ordinary_delta_uses_one_json_tree() {
        crate::json_parse::reset_value_parses();
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));

        normalizer
            .push(
                br#"data: {"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"hello"}

"#,
            )
            .unwrap();

        assert_eq!(
            crate::json_parse::value_parses(),
            1,
            "telemetry, content observation, and normalization must share the decoded delta"
        );
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

    /// Part 0 is finalized by its own `output_text.done`; part 1 streamed only its start. The
    /// completed item carries part 1 whole, and it replaces the cut-short stream of that part.
    #[test]
    fn responses_fallback_repairs_a_partial_part_beside_a_finalized_one() {
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        let chunks: &[&[u8]] = &[
            b"data: {\"type\":\"response.output_item.added\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[]}}\n\n",
            b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":0,\"delta\":\"A\"}\n\n",
            b"data: {\"type\":\"response.output_text.done\",\"output_index\":0,\"content_index\":0,\"text\":\"A\"}\n\n",
            b"data: {\"type\":\"response.output_text.delta\",\"output_index\":0,\"content_index\":1,\"delta\":\"he\"}\n\n",
            b"data: {\"type\":\"response.output_item.done\",\"output_index\":0,\"item\":{\"id\":\"msg_1\",\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"A\"},{\"type\":\"output_text\",\"text\":\"hello\"}]}}\n\n",
            b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_1\",\"status\":\"completed\"}}\n\n",
        ];
        let mut events = Vec::new();
        for chunk in chunks {
            events.extend(normalizer.push(chunk).unwrap());
        }
        events.extend(normalizer.finish().unwrap());
        assert_eq!(extract_assistant_text(&events), "Ahello");
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

    /// A function call added with empty arguments whose completed item says
    /// `status: incomplete`, delivered either in its own `output_item.done` or only in the
    /// terminal response's `output`, ending with `incomplete_details.reason = reason`.
    fn incomplete_call(
        format: EncryptedReasoningFormat,
        in_terminal_output: bool,
        reason: &str,
    ) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let item = r#"{"type":"function_call","id":"fc_1","call_id":"call_1","name":"refresh","arguments":"","status":"incomplete"}"#;
        let added = r#"{"type":"response.output_item.added","output_index":0,"item":{"type":"function_call","id":"fc_1","call_id":"call_1","name":"refresh","arguments":"","status":"in_progress"}}"#;
        let done =
            format!(r#"{{"type":"response.output_item.done","output_index":0,"item":{item}}}"#);
        let output = if in_terminal_output { item } else { "" };
        let terminal = format!(
            r#"{{"type":"response.incomplete","response":{{"id":"resp_1","status":"incomplete","incomplete_details":{{"reason":"{reason}"}},"output":[{output}],"usage":{{"input_tokens":10,"output_tokens":64}}}}}}"#
        );
        let mut records = vec![
            r#"{"type":"response.created","response":{"id":"resp_1"}}"#.to_owned(),
            added.to_owned(),
        ];
        if !in_terminal_output {
            records.push(done);
        }
        records.push(terminal);
        let mut normalizer =
            OpenAiResponsesNormalizer::new_for_format(StepId(1), format).with_max_output_tokens(64);
        let mut events = Vec::new();
        for record in records {
            events.extend(normalizer.push(format!("data: {record}\n\n").as_bytes())?);
        }
        events.extend(normalizer.finish()?);
        Ok(events)
    }

    const FORMATS: [EncryptedReasoningFormat; 2] = [
        EncryptedReasoningFormat::OpenAiResponses,
        EncryptedReasoningFormat::OpenAiResponsesChatgpt,
    ];

    #[test]
    fn a_call_the_provider_marked_incomplete_is_not_a_call() {
        for format in FORMATS {
            for in_terminal_output in [false, true] {
                let events = incomplete_call(format, in_terminal_output, "content_filter")
                    .expect("an incomplete call outside the ceiling is left out, not an error");
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
                    message.tool_calls().count(),
                    0,
                    "{format:?} in_terminal_output={in_terminal_output}: {:?}",
                    message.content
                );
            }
        }
    }

    #[test]
    fn a_call_marked_incomplete_at_the_output_ceiling_is_refused() {
        for format in FORMATS {
            for in_terminal_output in [false, true] {
                let events =
                    incomplete_call(format, in_terminal_output, "max_output_tokens").unwrap();
                assert!(
                    events.iter().any(|event| matches!(
                        event,
                        NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { .. })
                    )),
                    "{format:?} in_terminal_output={in_terminal_output}: {events:?}"
                );
            }
        }
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

    /// One stream, pushed event by event, with `terminal` as its last event.
    fn stream_ending_with(normalizer: &mut OpenAiResponsesNormalizer, terminal: &str) {
        let events_in = [
            r#"{"type":"response.created","response":{"id":"resp_1"}}"#,
            r#"{"type":"response.output_item.added","output_index":0,"item":{"id":"msg_1","type":"message","role":"assistant","content":[]}}"#,
            r#"{"type":"response.output_text.delta","output_index":0,"content_index":0,"delta":"Done."}"#,
            terminal,
        ];
        for event in events_in {
            normalizer
                .push(format!("data: {event}\n\n").as_bytes())
                .unwrap();
        }
        normalizer.finish().unwrap();
    }

    /// The shape OpenAI documents for a reasoning item it removed before the model ran.
    fn removed(id: &str, reason: &str) -> broca_protocol::InputTransformation {
        broca_protocol::InputTransformation {
            kind: Some("reasoning".into()),
            path: None,
            id: Some(id.into()),
            action: Some("removed".into()),
            reason: Some(reason.into()),
        }
    }

    /// A terminal response's `input_transformations` report is kept verbatim, in input order, on
    /// both the typed terminal parse and the permissive one an unusual discriminator falls to. An
    /// entry of an unexpected shape still counts, with its fields unknown.
    #[test]
    fn a_terminal_report_of_removed_reasoning_is_kept_verbatim() {
        let report = r#"[{"type":"reasoning","id":"rs_1","action":"removed","reason":"prefix_mismatch"},{"type":"reasoning","id":"rs_2","action":"removed","reason":"model_mismatch"},7]"#;
        let unknown = broca_protocol::InputTransformation {
            kind: None,
            path: None,
            id: None,
            action: None,
            reason: None,
        };
        let expected = vec![
            removed("rs_1", "prefix_mismatch"),
            removed("rs_2", "model_mismatch"),
            unknown,
        ];
        for terminal in [
            format!(
                r#"{{"type":"response.completed","response":{{"id":"resp_1","status":"completed","input_transformations":{report}}}}}"#
            ),
            // An escaped discriminator misses the typed parse and takes the permissive one.
            format!(
                r#"{{"type":"response.complet\u0065d","response":{{"id":"resp_1","status":"completed","input_transformations":{report}}}}}"#
            ),
        ] {
            let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
            stream_ending_with(&mut normalizer, &terminal);
            assert_eq!(
                normalizer.input_transformations(),
                Some(expected.clone()),
                "{terminal}"
            );
        }
    }

    /// A response that carries no report reads as no report, never as an empty one, and an empty
    /// report reads as empty: the two say different things.
    #[test]
    fn no_report_and_an_empty_report_stay_apart() {
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        stream_ending_with(
            &mut normalizer,
            r#"{"type":"response.completed","response":{"id":"resp_1","status":"completed"}}"#,
        );
        assert_eq!(normalizer.input_transformations(), None);

        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        stream_ending_with(
            &mut normalizer,
            r#"{"type":"response.completed","response":{"id":"resp_1","status":"completed","input_transformations":[]}}"#,
        );
        assert_eq!(normalizer.input_transformations(), Some(Vec::new()));
    }

    /// A failed response's report is kept too: the removal happened before the model ran.
    #[test]
    fn a_failed_response_keeps_its_report() {
        let mut normalizer = OpenAiResponsesNormalizer::new(StepId(1));
        normalizer
            .push(
                concat!(
                    r#"data: {"type":"response.created","response":{"id":"resp_1"}}"#,
                    "\n\n",
                    r#"data: {"type":"response.failed","response":{"id":"resp_1","status":"failed","error":{"code":"server_error","message":"x"},"input_transformations":[{"type":"reasoning","id":"rs_9","action":"removed","reason":"prefix_mismatch"}]}}"#,
                    "\n\n",
                )
                .as_bytes(),
            )
            .ok();
        assert_eq!(
            normalizer.input_transformations(),
            Some(vec![removed("rs_9", "prefix_mismatch")])
        );
    }
}
