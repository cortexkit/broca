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

use crate::wire::UsageIteration;
use serde_json::Value;

/// Response-only billing evidence. Allocated and installed only for journaled attempts;
/// ordinary normalization keeps its existing usage and event representation.
#[derive(Debug, Default)]
pub struct ResponseEvidence {
    pub(crate) usage: Option<Usage>,
    pub(crate) usage_raw: Option<Value>,
    pub(crate) model_used: Option<String>,
    pub(crate) finish_reason: Option<FinishReason>,
    pub(crate) service_tier_used: Option<String>,
    pub(crate) speed_used: Option<String>,
}

pub type SharedResponseEvidence = std::sync::Arc<std::sync::Mutex<ResponseEvidence>>;

impl ResponseEvidence {
    pub(crate) fn observe_response(&mut self, response: &Value) {
        if let Some(model) = response
            .get("model")
            .or_else(|| response.get("modelVersion"))
            .and_then(Value::as_str)
        {
            self.model_used = Some(model.to_owned());
        }
        if let Some(tier) = response.get("service_tier").and_then(Value::as_str) {
            self.service_tier_used = Some(tier.to_owned());
        }
        if let Some(speed) = response.get("speed").and_then(Value::as_str) {
            self.speed_used = Some(speed.to_owned());
        }
    }

    pub(crate) fn observe_usage(&mut self, raw: &Value, canonical: Usage) {
        // Usage snapshots can be partial, notably Anthropic's start/delta pair. Merge
        // reported keys recursively so a terminal counter cannot erase earlier evidence.
        fn merge(target: &mut Value, source: &Value) {
            if let (Value::Object(target), Value::Object(source)) = (&mut *target, source) {
                for (key, value) in source {
                    merge(target.entry(key).or_insert(Value::Null), value);
                }
            } else {
                *target = source.clone();
            }
        }
        if raw.is_object() {
            merge(self.usage_raw.get_or_insert(Value::Null), raw);
            self.usage = Some(canonical);
            self.observe_response(raw);
        }
    }
}

// NOTE: `redacted_thinking` is mapped to a DISTINCT `ContentBlock::RedactedReasoning`
// (NOT `Reasoning.signature`); its encrypted `data` blob is preserved BYTE-VERBATIM
// so it can be echoed back exactly on the next turn (Anthropic cache/multi-turn
// requirement). See the content.rs TODO for the request-side round-trip.

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
    /// Anthropic's server-side fallback handoff: the runtime switched the response from one
    /// model to another here. Neither signed nor reasoning, so it is kept as reported and
    /// rendered back only to a target that opted in.
    Fallback {
        from_model: String,
        to_model: String,
        raw: Option<String>,
    },
}

/// Streaming normalizer for ONE Anthropic message (= one step). Construct with
/// the runner-assigned `step_id`; `push` each decoded event in order.
pub struct AnthropicNormalizer {
    response_evidence: Option<SharedResponseEvidence>,
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
    /// `message.model` from `message_start`: the model the provider says it served. Stays
    /// `None` when the response names no model.
    reported_model: Option<String>,
    /// `usage.iterations` from the last event that reported them, verbatim. A server-side
    /// fallback splits a response into one iteration per model it ran.
    usage_iterations: Option<Vec<UsageIteration>>,
    /// The wire model id the caller's request asked for. A response that names a different
    /// model was served by a server-side fallback, and only then is `served_model` reported.
    requested_model: Option<String>,
    /// Drop blocks the stream never finished when the response is a refusal that came after
    /// at least one completed tool call; see [`Self::with_refusal_tool_use_trim`].
    trim_refusal_after_tool_use: bool,
}

impl AnthropicNormalizer {
    pub(crate) fn set_response_evidence(&mut self, evidence: SharedResponseEvidence) {
        self.response_evidence = Some(evidence);
    }

    pub(crate) fn observe_error_response(&mut self, response: &Value) {
        if let Some(usage) = response.get("usage") {
            self.merge_usage(usage);
        }
        if let Some(evidence) = &self.response_evidence {
            evidence.lock().unwrap().observe_response(response);
        }
    }

    pub fn new(step_id: StepId) -> Self {
        Self {
            response_evidence: None,
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
            reported_model: None,
            usage_iterations: None,
            requested_model: None,
            trim_refusal_after_tool_use: false,
        }
    }

    /// Record the wire model id this step's request asked for, so a response naming another
    /// model can be reported as served by a fallback. Absent means the id is unknown, and then
    /// no response is reported as served by a fallback.
    pub fn with_requested_model(mut self, requested_model: impl Into<String>) -> Self {
        self.requested_model = Some(requested_model.into());
        self
    }

    /// For a request whose episode froze a client-side refusal fallback: when the response
    /// ends `stop_reason: "refusal"` after at least one tool call completed (got its
    /// `content_block_stop`) and none is still open, keep only the blocks that completed. The
    /// run commits such a step as a tool step, and a block the refusal cut off mid-stream (a
    /// partial trailing text) must not enter the transcript. Off by default, which assembles
    /// every refusal exactly as before.
    pub fn with_refusal_tool_use_trim(mut self, trim: bool) -> Self {
        self.trim_refusal_after_tool_use = trim;
        self
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
        let out = self.push(&v)?;
        if v.get("type").and_then(Value::as_str) == Some("content_block_start") {
            if let Some(BlockBuf::Fallback { raw, .. }) = self.blocks.get_mut(&block_index(&v)) {
                #[derive(serde::Deserialize)]
                struct Start<'a> {
                    #[serde(borrow)]
                    content_block: &'a serde_json::value::RawValue,
                }
                let start: Start<'_> = serde_json::from_str(json).map_err(NormalizeError::Json)?;
                *raw = Some(start.content_block.get().to_owned());
            }
        }
        Ok(out)
    }

    /// Push one decoded event. Unknown event types are ignored (forward-compat).
    pub fn push(&mut self, v: &Value) -> Result<Vec<NormalizedEvent>, NormalizeError> {
        let ty = v.get("type").and_then(Value::as_str).unwrap_or("");
        let mut out = Vec::new();
        match ty {
            "message_start" => {
                let msg = v.get("message");
                if let (Some(evidence), Some(message)) = (&self.response_evidence, msg) {
                    evidence.lock().unwrap().observe_response(message);
                }
                if let Some(id) = msg.and_then(|m| m.get("id")).and_then(Value::as_str) {
                    self.message_id = Some(MessageId(id.to_owned()));
                }
                // The model the provider says produced this response. It is recorded for
                // attribution only and never rendered; a later `message_delta` that names no
                // model leaves it standing, like the token counts below.
                if let Some(model) = msg.and_then(|m| m.get("model")).and_then(Value::as_str) {
                    self.reported_model = Some(model.to_owned());
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
                        // Anthropic's server-side fallback handoff. Its model ids must match
                        // `^claude-[a-z0-9-]+$` and be at most 128 characters because they are
                        // rendered back into a later request; a block whose ids do not is
                        // dropped with a WARN instead of being stored, so nothing unvalidated
                        // can reach a request body.
                        Some("fallback") => {
                            let model = |side: &str| {
                                cb.get(side)
                                    .and_then(|s| s.get("model"))
                                    .and_then(Value::as_str)
                            };
                            match (model("from"), model("to")) {
                                (Some(from_model), Some(to_model))
                                    if is_fallback_wire_model(from_model)
                                        && is_fallback_wire_model(to_model) =>
                                {
                                    BlockBuf::Fallback {
                                        from_model: from_model.to_owned(),
                                        to_model: to_model.to_owned(),
                                        raw: None,
                                    }
                                }
                                _ => {
                                    tracing::warn!(
                                        target: "anthropic_normalize",
                                        from = model("from"),
                                        to = model("to"),
                                        "dropped a server-side fallback block whose model ids are \
                                         missing or malformed"
                                    );
                                    return Ok(out);
                                }
                            }
                        }
                        _ => return Ok(out), // unknown block type: ignore
                    };
                    self.blocks.insert(idx, buf);
                }
            }
            "content_block_delta" => {
                let idx = block_index(v);
                let part = self.blocks.range(..idx).count() as u64;
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
                            mid: None,
                            part: Some(part),
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
                            mid: None,
                            part: Some(part),
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
                            mid: None,
                            part: Some(part),
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
                    if let Some(evidence) = &self.response_evidence {
                        evidence.lock().unwrap().finish_reason = Some(self.finish_reason);
                    }
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
                // Assembly drains the block buffer, including the strongest served-model signal.
                let served_model = self.served_model();
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
                    served_model,
                    usage_iterations: self.usage_iterations.clone(),
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
                        provider_details: None,
                        provider_subcode: None,
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
                        affordable_max_tokens: None,
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
        // A response that ran more than one model reports one entry per model here. They are
        // recorded verbatim, not summed: the top-level counts above stay the provider's own
        // total, and deciding later whether that total is the sum of these entries or only the
        // last one needs no re-read of the wire.
        if let Some(entries) = u.get("iterations").and_then(Value::as_array) {
            self.usage_iterations = Some(
                entries
                    .iter()
                    .map(|entry| {
                        serde_json::from_value::<UsageIteration>(entry.clone()).unwrap_or(
                            UsageIteration {
                                kind: None,
                                model: None,
                                input_tokens: None,
                                output_tokens: None,
                                cache_creation_input_tokens: None,
                                cache_read_input_tokens: None,
                                other: Default::default(),
                            },
                        )
                    })
                    .collect(),
            );
        }
        let g = |k: &str| u.get(k).and_then(Value::as_u64);
        if let Some(x) = g("input_tokens") {
            self.usage.input_tokens = Some(x);
        }
        if let Some(x) = g("cache_read_input_tokens") {
            self.usage.cached_input_tokens = Some(x);
        }
        // Replay usage keeps its flat total. Journaled attempts separately retain the
        // provider's TTL split and raw usage without changing the legacy step record.
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
        if let Some(evidence) = &self.response_evidence {
            evidence.lock().unwrap().observe_usage(u, self.usage);
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
        let is_tool = |buf: &BlockBuf| matches!(buf, BlockBuf::ToolUse { .. });
        let refusal_after_tool_use = self.trim_refusal_after_tool_use
            && self.finish_reason == FinishReason::ContentFilter
            && self
                .blocks
                .iter()
                .any(|(idx, buf)| is_tool(buf) && self.completed_blocks.contains(idx))
            && self
                .blocks
                .iter()
                .all(|(idx, buf)| !is_tool(buf) || self.completed_blocks.contains(idx));
        let mut content = Vec::new();
        for (idx, buf) in std::mem::take(&mut self.blocks) {
            if (self.arguments_guard.fired() || refusal_after_tool_use)
                && !self.completed_blocks.contains(&idx)
                && !is_tool(&buf)
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
                BlockBuf::Fallback {
                    from_model,
                    to_model,
                    raw,
                } => content.push(ContentBlock::ProviderFallback {
                    from_model,
                    to_model,
                    raw,
                }),
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

    /// The wire model that produced this response, when the runtime handed the request on to a
    /// different model (server-side fallback). `None` means the requested model answered, which
    /// is the ordinary case and what keeps an ordinary step's record byte-identical.
    ///
    /// The handoff block is the strongest report, then the last fallback iteration, then a
    /// `message.model` that names another model. A `message.model` that is only a dated alias of
    /// the requested id is the same model, so it does not count.
    fn served_model(&self) -> Option<String> {
        let handoff = self.blocks.values().rev().find_map(|buf| match buf {
            BlockBuf::Fallback { to_model, .. } => Some(to_model.clone()),
            _ => None,
        });
        let iteration = self
            .usage_iterations
            .iter()
            .flatten()
            .rev()
            .find(|entry| entry.kind.as_deref() == Some("fallback_message"))
            .and_then(|entry| entry.model.clone());
        let served = handoff.or(iteration).or_else(|| {
            self.requested_model.as_ref()?;
            self.reported_model.clone()
        })?;
        if self
            .requested_model
            .as_deref()
            .is_some_and(|requested| is_alias_of(&served, requested))
        {
            None
        } else {
            Some(served)
        }
    }
}

/// Whether `model` names the same model as `requested`: equal, or the requested id followed by
/// a dated suffix (`-20260101`) that the provider appends to report the exact snapshot it ran.
/// Anything else is a different model.
fn is_alias_of(model: &str, requested: &str) -> bool {
    match model.strip_prefix(requested) {
        Some("") => true,
        Some(suffix) => {
            let suffix = suffix.trim_start_matches('-');
            suffix.len() == 8 && suffix.bytes().all(|byte| byte.is_ascii_digit())
        }
        None => false,
    }
}

/// Whether `model` is a well-formed Anthropic model id: `^claude-[a-z0-9-]+$`, at most 128
/// characters — the grammar the handoff block's model ids must satisfy to be rendered back.
fn is_fallback_wire_model(model: &str) -> bool {
    let Some(rest) = model.strip_prefix("claude-") else {
        return false;
    };
    !rest.is_empty()
        && model.len() <= 128
        && rest
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn block_index(v: &Value) -> usize {
    v.get("index").and_then(Value::as_u64).unwrap_or(0) as usize
}

pub(crate) fn map_stop_reason(sr: &str) -> FinishReason {
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

/// Whether an OpenAI error says the organization's prepaid credit or quota is used up, so the
/// credential that sent it cannot be served until someone pays. OpenAI says so in three ways: the
/// older `insufficient_quota` code, the `credit_balance_exhausted` code its API sends when a
/// prepaid balance reaches zero, or an error whose `type` is `insufficient_quota` whatever its
/// code. `code` and `error_type` are the error object's own `code` and `type` fields, from an HTTP
/// error body or from an in-band stream error event alike. Callers relabel a match
/// [`broca_protocol::error::BILLING_EXHAUSTED`], which is permanent and names the credential.
pub(crate) fn openai_billing_exhausted(code: Option<&str>, error_type: Option<&str>) -> bool {
    matches!(
        code,
        Some("insufficient_quota" | "credit_balance_exhausted")
    ) || error_type == Some("insufficient_quota")
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

    /// A refusal after a completed tool call: text block 0 and tool block 1 completed, text
    /// block 2 was cut off by the refusal.
    fn refusal_after_tool_use(trim: bool) -> (Vec<ContentBlock>, FinishReason) {
        let events = [
            r#"{"type":"message_start","message":{"id":"msg_t","usage":{"input_tokens":5}}}"#,
            r#"{"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}"#,
            r#"{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"looking"}}"#,
            r#"{"type":"content_block_stop","index":0}"#,
            r#"{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"t1","name":"read","input":{}}}"#,
            r#"{"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"f\":\"a\"}"}}"#,
            r#"{"type":"content_block_stop","index":1}"#,
            r#"{"type":"content_block_start","index":2,"content_block":{"type":"text","text":""}}"#,
            r#"{"type":"content_block_delta","index":2,"delta":{"type":"text_delta","text":"half a sen"}}"#,
            r#"{"type":"message_delta","delta":{"stop_reason":"refusal"},"usage":{"output_tokens":3}}"#,
            r#"{"type":"message_stop"}"#,
        ];
        let mut n = AnthropicNormalizer::new(StepId(1)).with_refusal_tool_use_trim(trim);
        let mut all = Vec::new();
        for e in events {
            all.extend(n.push_json(e).expect("push"));
        }
        let message = all
            .iter()
            .find_map(|e| match e {
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    Some(message.content.clone())
                }
                _ => None,
            })
            .expect("assembled message");
        let reason = all
            .iter()
            .find_map(|e| match e {
                NormalizedEvent::Control(ProviderControl::Finished { finish_reason, .. }) => {
                    Some(*finish_reason)
                }
                _ => None,
            })
            .expect("finished");
        (message, reason)
    }

    #[test]
    fn a_refusal_after_a_completed_tool_call_keeps_only_completed_blocks_when_asked() {
        let (trimmed, reason) = refusal_after_tool_use(true);
        assert_eq!(reason, FinishReason::ContentFilter);
        assert_eq!(trimmed.len(), 2, "{trimmed:?}");
        assert!(matches!(&trimmed[0], ContentBlock::Text { text } if text == "looking"));
        assert!(matches!(&trimmed[1], ContentBlock::ToolCall(call) if call.tool_call_id.0 == "t1"));
        // Without the episode's fallback the refusal assembles as it always did.
        let (untouched, reason) = refusal_after_tool_use(false);
        assert_eq!(reason, FinishReason::ContentFilter);
        assert_eq!(untouched.len(), 3, "{untouched:?}");
        assert!(matches!(&untouched[2], ContentBlock::Text { text } if text == "half a sen"));
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
