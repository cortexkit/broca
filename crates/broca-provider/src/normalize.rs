//! Anthropic Messages SSE → provider→runner stream-parts.
//!
//! The provider module owns vendor parsing and emits BOTH lanes: per-delta
//! [`DisplayEvent`]s (lossy display lane) AND the ASSEMBLED [`ProviderControl`]
//! units (reliable control lane). The runner consumes only the assembled units; it
//! never re-assembles deltas.
//!
//! This is a pure state machine over the decoded Anthropic event stream — no IO,
//! so it is driven identically by a live SSE connection (transport leg) and by a
//! captured fixture (tests). Anthropic streaming event schema:
//! `message_start` → (`content_block_start` → `content_block_delta`* →
//! `content_block_stop`)* → `message_delta` → `message_stop`, plus `ping` and
//! `error`.

use std::collections::BTreeMap;

use broca_protocol::{
    content::{AssistantMessage, ContentBlock, FinishReason, ToolCall},
    display::DisplayEvent,
    error::{ErrorClass, ProviderError},
    ids::{MessageId, StepId, ToolCallId},
    provider::{NormalizedEvent, ProviderControl},
    usage::Usage,
};

// NOTE: `redacted_thinking` is mapped to a DISTINCT `ContentBlock::RedactedReasoning`
// (NOT `Reasoning.signature`); its encrypted `data` blob is preserved BYTE-VERBATIM
// so it can be echoed back exactly on the next turn (Anthropic cache/multi-turn
// requirement). See the content.rs TODO for the request-side round-trip.
use serde_json::Value;

// `NormalizedEvent` is the canonical type from broca-protocol (re-exported by this
// crate's lib.rs); the normalizer produces it directly.

#[derive(Debug)]
pub enum NormalizeError {
    Json(serde_json::Error),
    ToolArguments(serde_json::Error),
    /// A tool call's arguments did not parse and `message_delta` reported
    /// `stop_reason: max_tokens`: the output ceiling cut the call, the stream is not malformed.
    OutputCeiling(crate::wire::OutputCeilingCut),
    /// An event arrived before `message_start` established the message id.
    NoActiveMessage,
    /// The SSE framing of a captured buffer could not be decoded (a record over the size
    /// cap, or a record that is not valid UTF-8), so its events could not be extracted.
    Framing(crate::wire::NormalizeError),
}

impl NormalizeError {
    /// The grammar-generic form the transport classifies. A ceiling cut keeps its own variant
    /// so it is classed permanent; every other failure stays an ordinary invalid stream.
    pub(crate) fn into_wire(self) -> crate::wire::NormalizeError {
        match self {
            NormalizeError::OutputCeiling(cut) => crate::wire::NormalizeError::OutputCeiling(cut),
            // Already in the transport's form; passing it through keeps a size-cap failure
            // reported as a stream limit rather than as a malformed stream.
            NormalizeError::Framing(error) => error,
            other => crate::wire::NormalizeError::Invalid(other.to_string()),
        }
    }
}

impl std::fmt::Display for NormalizeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NormalizeError::Json(e) => write!(f, "decode anthropic event: {e}"),
            NormalizeError::ToolArguments(e) => {
                write!(f, "invalid terminal tool arguments: {e}")
            }
            NormalizeError::OutputCeiling(cut) => write!(f, "{cut}"),
            NormalizeError::NoActiveMessage => {
                write!(f, "content event before message_start")
            }
            NormalizeError::Framing(e) => write!(f, "decode anthropic SSE framing: {e}"),
        }
    }
}

impl std::error::Error for NormalizeError {}

/// An in-progress content block, keyed by its stream index.
#[derive(Debug)]
enum BlockBuf {
    Text {
        text: String,
    },
    Reasoning {
        text: String,
        signature: Option<String>,
    },
    /// Encrypted reasoning blob, preserved verbatim (Anthropic redacted_thinking).
    RedactedReasoning {
        data: String,
    },
    ToolUse {
        id: String,
        name: String,
        input_json: String,
        start_input: Value,
    },
}

/// Streaming normalizer for ONE Anthropic message (= one step). Construct with
/// the runner-assigned `step_id`; `push` each decoded event in order.
pub struct AnthropicNormalizer {
    arguments_guard: crate::wire::ToolArgsGuard,
    completed: bool,
    completed_blocks: std::collections::HashSet<usize>,
    step_id: StepId,
    message_id: Option<MessageId>,
    blocks: BTreeMap<usize, BlockBuf>,
    usage: Usage,
    finish_reason: FinishReason,
    /// The provider's `stop_reason`, verbatim, for naming an output-ceiling cut.
    stop_reason: Option<String>,
    /// The `max_tokens` the request was rendered with, when the caller supplied it.
    max_output_tokens: Option<u32>,
    started: bool,
    /// `usage.service_tier` as the response reported it; `None` until a usage object names one.
    service_tier_used: Option<String>,
    /// `usage.speed` as the response reported it (where fast mode is reported).
    speed_used: Option<String>,
    /// `message.input_transformations` from `message_start`, verbatim; `None` when the
    /// response did not carry the field (it is present only under the thinking-binding beta).
    input_transformations: Option<Vec<broca_protocol::InputTransformation>>,
}

impl AnthropicNormalizer {
    pub fn new(step_id: StepId) -> Self {
        Self {
            arguments_guard: Default::default(),
            completed: false,
            completed_blocks: Default::default(),
            step_id,
            message_id: None,
            blocks: BTreeMap::new(),
            usage: Usage::default(),
            finish_reason: FinishReason::Other,
            stop_reason: None,
            max_output_tokens: None,
            started: false,
            service_tier_used: None,
            speed_used: None,
            input_transformations: None,
        }
    }

    /// The provider's report of how it altered the request, as read from `message_start`.
    pub fn input_transformations(&self) -> Option<Vec<broca_protocol::InputTransformation>> {
        self.input_transformations.clone()
    }

    /// Record the rendered output cap so an output-ceiling error can name it.
    pub fn with_max_output_tokens(mut self, max_output_tokens: u32) -> Self {
        self.max_output_tokens = Some(max_output_tokens);
        self
    }

    /// Convenience: normalize a whole captured SSE buffer to the full event
    /// sequence (skips `ping`/`[DONE]`). Errors on the first malformed payload, and on a
    /// buffer whose SSE framing cannot be decoded.
    pub fn normalize_sse(
        step_id: StepId,
        raw: &str,
    ) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let mut n = Self::new(step_id);
        let mut out = Vec::new();
        for payload in crate::sse::parse_data_events(raw).map_err(NormalizeError::Framing)? {
            out.extend(n.push_json(&payload)?);
        }
        Ok(out)
    }

    /// Push one raw event JSON payload.
    pub fn push_json(&mut self, json: &str) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        if self.completed {
            return Ok(Vec::new());
        }
        let v: Value = serde_json::from_str(json).map_err(NormalizeError::Json)?;
        self.push(&v)
    }

    /// Push one decoded event. Unknown event types are ignored (forward-compat).
    pub fn push(&mut self, v: &Value) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let ty = v.get("type").and_then(Value::as_str).unwrap_or("");
        let mut out = Vec::new();
        match ty {
            "message_start" => {
                let msg = v.get("message");
                if let Some(id) = msg.and_then(|m| m.get("id")).and_then(Value::as_str) {
                    self.message_id = Some(MessageId(id.to_owned()));
                }
                if let Some(u) = msg.and_then(|m| m.get("usage")) {
                    self.merge_usage(u);
                }
                // Each entry is kept even when its shape is unexpected: a report that cannot be
                // read field by field still counts as a report.
                if let Some(entries) = msg
                    .and_then(|m| m.get("input_transformations"))
                    .and_then(Value::as_array)
                {
                    self.input_transformations = Some(
                        entries
                            .iter()
                            .map(|entry| {
                                serde_json::from_value(entry.clone()).unwrap_or(
                                    broca_protocol::InputTransformation {
                                        kind: None,
                                        path: None,
                                        reason: None,
                                    },
                                )
                            })
                            .collect(),
                    );
                }
                self.started = true;
                out.push(NormalizedEvent::Control(ProviderControl::StreamStart {
                    warnings: Vec::new(),
                }));
                out.push(NormalizedEvent::Control(ProviderControl::StepStarted {
                    step_id: self.step_id,
                }));
            }
            "content_block_start" => {
                let idx = block_index(v);
                if let Some(cb) = v.get("content_block") {
                    let buf = match cb.get("type").and_then(Value::as_str) {
                        Some("text") => BlockBuf::Text {
                            text: cb
                                .get("text")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_owned(),
                        },
                        Some("thinking") => BlockBuf::Reasoning {
                            text: cb
                                .get("thinking")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_owned(),
                            signature: cb
                                .get("signature")
                                .and_then(Value::as_str)
                                .filter(|s| !s.is_empty())
                                .map(str::to_owned),
                        },
                        Some("redacted_thinking") => BlockBuf::RedactedReasoning {
                            // Preserve the encrypted blob BYTE-VERBATIM and distinct
                            // from a normal signature (echo-back fidelity, see above).
                            data: cb
                                .get("data")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_owned(),
                        },
                        Some("tool_use") => BlockBuf::ToolUse {
                            id: cb
                                .get("id")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_owned(),
                            name: cb
                                .get("name")
                                .and_then(Value::as_str)
                                .unwrap_or("")
                                .to_owned(),
                            input_json: String::new(),
                            start_input: cb.get("input").cloned().unwrap_or(Value::Null),
                        },
                        _ => return Ok(out), // unknown block type: ignore
                    };
                    self.blocks.insert(idx, buf);
                }
            }
            "content_block_delta" => {
                let idx = block_index(v);
                let delta = v.get("delta");
                let dty = delta
                    .and_then(|d| d.get("type"))
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let mid = self
                    .message_id
                    .clone()
                    .ok_or(NormalizeError::NoActiveMessage)?;
                match dty {
                    "text_delta" => {
                        let t = delta
                            .and_then(|d| d.get("text"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        if let Some(BlockBuf::Text { text }) = self.blocks.get_mut(&idx) {
                            text.push_str(t);
                        }
                        out.push(NormalizedEvent::Display(DisplayEvent::TextDelta {
                            message_id: mid,
                            tool_call_id: None,
                            delta: t.to_owned(),
                        }));
                    }
                    "thinking_delta" => {
                        let t = delta
                            .and_then(|d| d.get("thinking"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        if let Some(BlockBuf::Reasoning { text, .. }) = self.blocks.get_mut(&idx) {
                            text.push_str(t);
                        }
                        out.push(NormalizedEvent::Display(DisplayEvent::ReasoningDelta {
                            message_id: mid,
                            delta: t.to_owned(),
                        }));
                    }
                    "signature_delta" => {
                        let s = delta
                            .and_then(|d| d.get("signature"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        if let Some(BlockBuf::Reasoning { signature, .. }) =
                            self.blocks.get_mut(&idx)
                        {
                            signature.get_or_insert_with(String::new).push_str(s);
                        }
                        // signature is not display content — no display event.
                    }
                    "input_json_delta" => {
                        let p = delta
                            .and_then(|d| d.get("partial_json"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        let tcid = match self.blocks.get_mut(&idx) {
                            Some(BlockBuf::ToolUse {
                                input_json,
                                id,
                                name,
                                ..
                            }) => {
                                input_json.push_str(p);
                                self.arguments_guard
                                    .stream(idx as u64, id, name, input_json, p);
                                ToolCallId(id.clone())
                            }
                            _ => return Ok(out),
                        };
                        out.push(NormalizedEvent::Display(DisplayEvent::ToolInputDelta {
                            tool_call_id: tcid,
                            delta: p.to_owned(),
                        }));
                    }
                    _ => {}
                }
            }
            "content_block_stop" => {
                if let Some(index) = v.get("index").and_then(Value::as_u64) {
                    self.completed_blocks.insert(index as usize);
                }
            }
            "message_delta" => {
                if let Some(sr) = v
                    .get("delta")
                    .and_then(|d| d.get("stop_reason"))
                    .and_then(Value::as_str)
                {
                    self.finish_reason = map_stop_reason(sr);
                    self.stop_reason = Some(sr.to_owned());
                }
                if let Some(u) = v.get("usage") {
                    self.merge_usage(u);
                }
            }
            "message_stop" => {
                if !self.started {
                    return Err(NormalizeError::NoActiveMessage);
                }
                let message = self.assemble_message()?;
                self.completed = true;
                out.extend(self.arguments_guard.event());
                out.push(NormalizedEvent::Control(
                    ProviderControl::AssistantMessage { message },
                ));
                out.push(NormalizedEvent::Control(ProviderControl::StepFinished {
                    step_id: self.step_id,
                    usage: self.usage,
                    finish_reason: self.finish_reason,
                    service_tier_used: self.service_tier_used.clone(),
                    speed_used: self.speed_used.clone(),
                }));
                out.push(NormalizedEvent::Control(ProviderControl::Finished {
                    usage: self.usage,
                    finish_reason: self.finish_reason,
                    retries_used: 0,
                }));
            }
            "error" => {
                let err = v.get("error");
                let message = err
                    .and_then(|e| e.get("message"))
                    .and_then(Value::as_str)
                    .unwrap_or("anthropic stream error")
                    .to_owned();
                let provider_code = err
                    .and_then(|e| e.get("type"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                let overflow_code = context_overflow_code(
                    crate::spec::WireFamilyId::AnthropicMessages,
                    provider_code.as_deref(),
                    &message,
                    None,
                );
                let class = overflow_code
                    .map(|_| ErrorClass::ContextOverflow)
                    .unwrap_or_else(|| {
                        classify_error(provider_code.as_deref(), None)
                            .unwrap_or(ErrorClass::Permanent)
                    });
                let provider_code = overflow_code.map(str::to_owned).or(provider_code);
                out.push(NormalizedEvent::Control(ProviderControl::Error {
                    error: ProviderError {
                        refusal: None,
                        // An unrecognised in-band code lands on Permanent, which is the less
                        // forgiving of the two directions and is chosen deliberately. Unknown
                        // failures generally default to Transient, since a needless retry costs
                        // attempts while a wrong Permanent kills a run that would have succeeded.
                        // The reasoning inverts here: every error type this provider documents is
                        // already in the table above, and an error event carrying no HTTP status is
                        // the provider deliberately reporting something about the request rather
                        // than a transport fault. Retrying re-sends byte-identical bytes, so a
                        // request fault reproduces exactly, three times, having possibly already
                        // billed partial output.
                        //
                        // This only fires on a code added after the table was last updated, so the
                        // first real instance is the evidence for revisiting it. Moving to
                        // Transient means accepting repeated identical sends for faults that can
                        // never succeed; keeping Permanent means a genuinely retryable new code
                        // fails a healthy run until the table learns it. Prefer adding the code to
                        // the table over changing this default.
                        class,
                        message,
                        status: None,
                        retry_after_secs: None,
                        provider_code,
                        resets_at_ms: None,
                        resets_at_source: None,
                        limit_window: None,
                        account_ref: None,
                        credential_id: None,
                    },
                    retries_used: 0,
                }));
            }
            _ => {} // ping, unknown: ignore
        }
        if self.arguments_guard.fired() && !self.completed {
            self.finish_reason = FinishReason::ToolCalls;
            out.extend(self.push_json(r#"{"type":"message_stop"}"#)?);
        }
        Ok(out)
    }

    fn merge_usage(&mut self, u: &Value) {
        // The tier and speed the provider served, read verbatim from whichever usage object
        // carries them (message_start and message_delta both may). Last reported value wins,
        // like the token counts below; an event that omits the key leaves the earlier value.
        if let Some(tier) = u.get("service_tier").and_then(Value::as_str) {
            self.service_tier_used = Some(tier.to_owned());
        }
        if let Some(speed) = u.get("speed").and_then(Value::as_str) {
            self.speed_used = Some(speed.to_owned());
        }
        let g = |k: &str| u.get(k).and_then(Value::as_u64);
        if let Some(x) = g("input_tokens") {
            self.usage.input_tokens = Some(x);
        }
        if let Some(x) = g("cache_read_input_tokens") {
            self.usage.cached_input_tokens = Some(x);
        }
        // Anthropic ALSO emits `cache_creation` as an object beside this flat total,
        // splitting the write into `ephemeral_5m_input_tokens` and
        // `ephemeral_1h_input_tokens` — which bill at DIFFERENT rates, 1h higher. We
        // collapse to the flat total, so the split is lost at this boundary and cannot be
        // recovered downstream; it is already present in captured SSE and simply unread.
        //
        // Deliberate for now because `Usage` has one field for it and the consuming ledger
        // has one column, so preserving it here would store a number nothing can spend.
        // The exposure today is nil rather than small: anthropic traffic on this fleet is
        // subscription-included and never priced. A METERED 1h workload would be
        // understated at whichever single rate the catalog holds, with no downstream
        // instrument able to notice — the information stops existing here.
        //
        // Reverse this when a metered anthropic route appears or the ledger grows a second
        // column; the field to read is already on the wire.
        if let Some(x) = g("cache_creation_input_tokens") {
            self.usage.cache_write_tokens = Some(x);
        }
        // MERGING ACROSS EVENTS IS LOAD-BEARING, NOT INCIDENTAL. Anthropic reports usage
        // TWICE per response, and the two are not the same measurement: `message_start`
        // carries a PLACEHOLDER output count while `message_delta` carries the real one.
        // Measured over 183 real responses by the gateway team: 2/3/2 at message_start
        // against 496/301/229 at message_delta, different on 183 of 183, with input
        // byte-identical across both and thinking_tokens present only on the terminal
        // event.
        //
        // So last-write-wins here is the correctness property. Reading a SINGLE event
        // instead — the obvious optimization, since the first one already looks like a
        // complete usage object — understates every output count by roughly two orders of
        // magnitude, uniformly and silently. Nothing downstream would flag it: the numbers
        // stay plausible, just small.
        if let Some(x) = g("output_tokens") {
            self.usage.output_tokens = Some(x);
        }
        // Newer Anthropic API responses carry `output_tokens_details.thinking_tokens`
        // — the reasoning subset of output (subset semantics, like OpenAI's reasoning
        // split). Surface it as `reasoning_tokens` without subtracting from
        // `output_tokens` (the provider's billing total). Absent on older responses
        // leaves the default None. We read only the provider-reported field; we never
        // synthesize it from thinking-block sizes.
        if let Some(x) = u
            .get("output_tokens_details")
            .and_then(|d| d.get("thinking_tokens"))
            .and_then(Value::as_u64)
        {
            self.usage.reasoning_tokens = Some(x);
        }
    }

    fn assemble_message(&mut self) -> Result<AssistantMessage, NormalizeError> {
        for buf in self.blocks.values() {
            if let BlockBuf::ToolUse {
                id,
                name,
                input_json,
                ..
            } = buf
            {
                self.arguments_guard.terminal(id, name, input_json);
            }
        }
        if self.arguments_guard.fired() {
            self.finish_reason = FinishReason::ToolCalls;
        }
        // Tool arguments are parsed here, at `message_stop`, which Anthropic sends after the
        // `message_delta` carrying `stop_reason`. So when a buffer does not parse, the stop
        // reason is already known, and `max_tokens` proves the ceiling cut the call.
        let ceiling = (self.finish_reason == FinishReason::Length)
            .then_some(self.stop_reason.as_deref())
            .flatten()
            .map(|provider_stop_reason| crate::wire::CeilingStop {
                provider_stop_reason,
                max_output_tokens: self.max_output_tokens,
                output_tokens: self.usage.output_tokens,
            });
        let mut content = Vec::new();
        for (idx, buf) in std::mem::take(&mut self.blocks) {
            if self.arguments_guard.fired()
                && !self.completed_blocks.contains(&idx)
                && !matches!(buf, BlockBuf::ToolUse { .. })
            {
                continue;
            }
            match buf {
                BlockBuf::Text { text } => content.push(ContentBlock::Text { text }),
                BlockBuf::Reasoning { text, signature } => {
                    content.push(ContentBlock::Reasoning { text, signature })
                }
                BlockBuf::RedactedReasoning { data } => {
                    content.push(ContentBlock::RedactedReasoning {
                        data,
                        provider_format: None,
                    })
                }
                BlockBuf::ToolUse {
                    id,
                    name,
                    input_json,
                    start_input,
                } => {
                    if self.arguments_guard.fired()
                        && ((input_json.is_empty() && !self.completed_blocks.contains(&idx))
                            || (!input_json.is_empty()
                                && serde_json::from_str::<Value>(&input_json).is_err()))
                    {
                        self.arguments_guard.omit_tool();
                        continue;
                    }
                    let input = if input_json.is_empty() {
                        if start_input.is_null() {
                            Value::Object(Default::default())
                        } else {
                            start_input
                        }
                    } else {
                        serde_json::from_str(&input_json).map_err(|error| match ceiling {
                            Some(stop) => {
                                NormalizeError::OutputCeiling(stop.cut(&name, &input_json))
                            }
                            None => NormalizeError::ToolArguments(error),
                        })?
                    };
                    content.push(ContentBlock::ToolCall(ToolCall {
                        tool_call_id: ToolCallId(id),
                        tool_name: name,
                        input,
                        thought_signature: None,
                    }));
                }
            }
        }
        Ok(AssistantMessage {
            message_id: self.message_id.clone().unwrap_or(MessageId(String::new())),
            content,
        })
    }
}

fn block_index(v: &Value) -> usize {
    v.get("index").and_then(Value::as_u64).unwrap_or(0) as usize
}

fn map_stop_reason(sr: &str) -> FinishReason {
    match sr {
        "end_turn" | "stop_sequence" => FinishReason::Stop,
        "max_tokens" => FinishReason::Length,
        "tool_use" => FinishReason::ToolCalls,
        "refusal" => FinishReason::ContentFilter,
        _ => FinishReason::Other,
    }
}

/// Recognize request-size refusals only within their wire family. A missing status denotes an
/// in-band provider error, not a failed HTTP request; other HTTP statuses keep their usual meaning.
pub(crate) fn context_overflow_code<'a>(
    family: crate::spec::WireFamilyId,
    code: Option<&'a str>,
    message: &str,
    status: Option<u16>,
) -> Option<&'a str> {
    use crate::spec::WireFamilyId;
    if status.is_some_and(|status| status != 400) {
        return None;
    }
    match family {
        WireFamilyId::AnthropicMessages
            if matches!(code, None | Some("invalid_request_error"))
                && message.starts_with("prompt is too long:") =>
        {
            // Anthropic supplies only the generic invalid_request_error type, not an overflow
            // code. Reuse OpenAI's stable code so callers can recognize the same refusal.
            Some("context_length_exceeded")
        }
        WireFamilyId::Gemini | WireFamilyId::GoogleCodeAssist
            if code == Some("INVALID_ARGUMENT")
                && (message.starts_with("The input token count ")
                    || message.starts_with("input token count "))
                && message.contains(" exceeds the maximum number of tokens allowed") =>
        {
            code
        }
        WireFamilyId::BedrockConverse
            if matches!(
                code,
                None | Some("ValidationException" | "validationException")
            ) && (message.starts_with("Input is too long")
                || message.starts_with("Too many input tokens")
                || message.starts_with("too many input tokens")) =>
        {
            Some(code.unwrap_or("context_length_exceeded"))
        }
        _ => None,
    }
}

pub(crate) fn classify_error(code: Option<&str>, status: Option<u16>) -> Option<ErrorClass> {
    match code {
        Some("context_length_exceeded") | Some("context_window_exceeded") => {
            Some(ErrorClass::ContextOverflow)
        }
        Some("billing_exhausted") | Some("usage_not_included") => Some(ErrorClass::Permanent),
        // A 429 is permanent only for the billing and entitlement codes above. Every other
        // provider code defaults to transient for this status, even if that code normally
        // maps differently on another status.
        _ if status == Some(429) => Some(ErrorClass::Transient),
        // `api_error` is a catch-all type rather than a diagnosis. Providers attach it to
        // everything from a briefly overloaded backend to a model that was retired months ago,
        // so it should only carry the retryable reading when nothing more specific contradicts
        // it. A status whose own meaning is permanent is more specific, and wins.
        //
        // Without this, a 410 Gone for a retired model reports as retryable: the step spends
        // its whole attempt budget re-asking a target that cannot answer, and the class that
        // reaches a caller invites it to retry the run for the same reason.
        Some("api_error")
            if status
                .map(status_default_class)
                .is_some_and(|class| class == ErrorClass::Permanent) =>
        {
            Some(ErrorClass::Permanent)
        }
        Some("overloaded_error" | "api_error" | "rate_limit_error" | "rate_limit_exceeded") => {
            Some(ErrorClass::Transient)
        }
        Some("authentication_error" | "invalid_api_key") => Some(ErrorClass::Auth),
        Some("permission_error") => Some(ErrorClass::Permanent),
        Some("invalid_request_error") | Some("not_found_error") => Some(ErrorClass::Permanent),
        _ => None,
    }
}

/// Apply the provider-code table before generic HTTP-status defaults. An in-band error with no
/// status therefore fails permanent when its code is unknown; the generic transient quota rule
/// applies only when the provider actually identifies the error as status 429.
pub(crate) fn classify_error_with_status_defaults(
    code: Option<&str>,
    status: Option<u16>,
) -> ErrorClass {
    classify_error(code, status)
        .unwrap_or_else(|| status.map_or(ErrorClass::Permanent, status_default_class))
}

/// What an HTTP status alone implies, absent a provider code that says otherwise. Kept as one
/// function because the code table consults it too, and a second copy would let the two drift
/// into disagreeing about the same status.
fn status_default_class(status: u16) -> ErrorClass {
    match status {
        401 => ErrorClass::Auth,
        408 | 429 | 529 | 500..=599 => ErrorClass::Transient,
        _ => ErrorClass::Permanent,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A retired model answers 410 with the generic `api_error` type and a null code. Retrying
    /// cannot help, so the status has to outrank the catch-all type.
    #[test]
    fn a_permanent_status_outranks_the_generic_api_error_type() {
        assert_eq!(
            classify_error_with_status_defaults(Some("api_error"), Some(410)),
            ErrorClass::Permanent,
        );
        assert_eq!(
            classify_error_with_status_defaults(Some("api_error"), Some(404)),
            ErrorClass::Permanent,
        );
    }

    /// The narrowing must not reach the statuses where `api_error` is genuinely retryable: an
    /// overloaded backend reports one, and so does a rate limiter.
    #[test]
    fn api_error_stays_transient_where_retrying_can_work() {
        for status in [500u16, 503, 529, 408] {
            assert_eq!(
                classify_error_with_status_defaults(Some("api_error"), Some(status)),
                ErrorClass::Transient,
                "api_error on {status} is a retryable server-side condition",
            );
        }
        assert_eq!(
            classify_error_with_status_defaults(Some("api_error"), Some(429)),
            ErrorClass::Transient,
        );
        // No status at all: an in-band stream error keeps the type's own reading.
        assert_eq!(
            classify_error_with_status_defaults(Some("api_error"), None),
            ErrorClass::Transient,
        );
    }

    /// Covers the thinking-TEXT delta path (the real fixture streamed a
    /// signature-only thinking block, so this complements it): a thinking_delta
    /// emits a display ReasoningDelta AND accumulates into the assembled block;
    /// the assembled message preserves reasoning text + signature, then text.
    #[test]
    fn assembles_thinking_text_then_text_block() {
        let events = [
            r#"{"type":"message_start","message":{"id":"msg_x","usage":{"input_tokens":10,"output_tokens":1,"cache_read_input_tokens":4,"cache_creation_input_tokens":6}}}"#,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"","signature":""}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"Let me "}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"reason."}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"sig123"}}"#,
            r#"{"type":"content_block_stop","index":0}"#,
            r#"{"type":"content_block_start","index":1,"content_block":{"type":"text","text":""}}"#,
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"text_delta","text":"Answer."}}"#,
            r#"{"type":"content_block_stop","index":1}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":7}}"#,
            r#"{"type":"message_stop"}"#,
        ];
        let mut n = AnthropicNormalizer::new(StepId(2));
        let mut all = Vec::new();
        for e in events {
            all.extend(n.push_json(e).expect("push"));
        }

        // Display reasoning deltas reassemble.
        let reasoning: String = all
            .iter()
            .filter_map(|e| match e {
                NormalizedEvent::Display(DisplayEvent::ReasoningDelta { delta, .. }) => {
                    Some(delta.as_str())
                }
                _ => None,
            })
            .collect();
        assert_eq!(reasoning, "Let me reason.");

        let msg = all
            .iter()
            .find_map(|e| match e {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message)
                }
                _ => None,
            })
            .expect("assembled message");
        match &msg.content[0] {
            ContentBlock::Reasoning { text, signature } => {
                assert_eq!(text, "Let me reason.");
                assert_eq!(signature.as_deref(), Some("sig123"));
            }
            other => panic!("expected reasoning, got {other:?}"),
        }
        assert!(matches!(&msg.content[1], ContentBlock::Text { text } if text == "Answer."));

        // end_turn → Stop; usage merged across message_start + message_delta.
        let fin = all.iter().find_map(|e| match e {
            NormalizedEvent::Control(ProviderControl::StepFinished {
                usage,
                finish_reason,
                ..
            }) => Some((*usage, *finish_reason)),
            _ => None,
        });
        let (usage, reason) = fin.expect("step finished");
        assert_eq!(reason, FinishReason::Stop);
        assert_eq!(usage.input_tokens, Some(10));
        assert_eq!(usage.cached_input_tokens, Some(4));
        assert_eq!(usage.cache_write_tokens, Some(6));
        assert_eq!(usage.output_tokens, Some(7));
    }

    #[test]
    fn error_event_maps_to_typed_control() {
        let mut n = AnthropicNormalizer::new(StepId(0));
        let out = n
            .push_json(
                r#"{"type":"error","error":{"type":"overloaded_error","message":"overloaded"}}"#,
            )
            .expect("push");
        match &out[0] {
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                assert_eq!(error.class, ErrorClass::Transient);
                assert_eq!(error.provider_code.as_deref(), Some("overloaded_error"));
            }
            other => panic!("expected typed error, got {other:?}"),
        }
    }

    #[test]
    fn redacted_thinking_preserved_verbatim_and_distinct() {
        let blob = "ErUBCkYIBBg...ENCRYPTED...verbatim==";
        let start = format!(
            r#"{{"type":"content_block_start","index":0,"content_block":{{"type":"redacted_thinking","data":"{blob}"}}}}"#
        );
        let events = [
            r#"{"type":"message_start","message":{"id":"msg_r","usage":{"input_tokens":5}}}"#,
            &start,
            r#"{"type":"content_block_stop","index":0}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}"#,
            r#"{"type":"message_stop"}"#,
        ];
        let mut n = AnthropicNormalizer::new(StepId(9));
        let mut all = Vec::new();
        for e in events {
            all.extend(n.push_json(e).expect("push"));
        }
        let msg = all
            .iter()
            .find_map(|e| match e {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message)
                }
                _ => None,
            })
            .expect("assembled message");
        assert_eq!(msg.content.len(), 1);
        match &msg.content[0] {
            // DISTINCT variant (not Reasoning), blob byte-verbatim.
            ContentBlock::RedactedReasoning {
                data,
                provider_format: None,
            } => assert_eq!(data, blob),
            other => panic!("expected RedactedReasoning, got {other:?}"),
        }
    }

    #[test]
    fn content_event_before_message_start_errors() {
        let mut n = AnthropicNormalizer::new(StepId(0));
        let err = n
            .push_json(r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"x"}}"#)
            .unwrap_err();
        assert!(matches!(err, NormalizeError::NoActiveMessage));
    }

    #[test]
    fn over_cap_capture_is_a_framing_error_not_an_empty_stream() {
        let mut raw = String::from("data: ");
        raw.push_str(&"x".repeat(crate::sse::MAX_SSE_RECORD_BYTES + 1));
        raw.push_str("\n\n");
        let err = AnthropicNormalizer::normalize_sse(StepId(0), &raw)
            .expect_err("an over-cap record must fail the capture, not yield no events");
        assert!(matches!(&err, NormalizeError::Framing(inner) if inner.is_stream_limit()));
        // The transport form keeps the stream-limit kind rather than becoming a generic
        // malformed-stream error.
        assert!(err.into_wire().is_stream_limit());
    }
}
