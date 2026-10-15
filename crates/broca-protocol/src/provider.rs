//! CONTROL payload, provider module → runner: the ASSEMBLED units (the per-delta
//! parts ride the display lane as [`DisplayEvent`]). Sent on the reliable,
//! reserved-capacity priority lane so it is never starved by a display flood. The
//! runner validates/persists these; it never re-assembles.
//!
//! Note: tool calls are content blocks of the assembled [`AssistantMessage`]
//! (single source of truth), so there is deliberately NO separate `ToolCall`
//! control unit on this lane — the runner extracts the per-step tool batch from
//! the assembled message in source order.

use serde::{Deserialize, Serialize};

use crate::{
    content::{AssistantMessage, FinishReason},
    display::DisplayEvent,
    error::ProviderError,
    ids::StepId,
    usage::Usage,
};

/// A normalized provider output tagged with its destination lane — what a
/// provider module's SSE→normalize stage emits (one parse, two lanes). The runner
/// forwards `Display` to the caller untouched and consumes only the assembled
/// `Control` units.
#[derive(Debug, Clone, PartialEq)]
pub enum NormalizedEvent {
    /// Display lane (lossy, coalesced, passthrough).
    Display(DisplayEvent),
    /// Control lane (assembled, reliable).
    Control(ProviderControl),
}

/// Argument-free evidence of a locally rejected call. This is not a WAL field or record.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolArgsTrailingGarbage {
    pub tool_call_id: crate::ToolCallId,
    pub tool_name: String,
    pub json_bytes: usize,
    pub whitespace_bytes: usize,
    pub omitted_tool_calls: usize,
}

impl ToolArgsTrailingGarbage {
    pub fn error_text(&self) -> String {
        format!(
            "tool_args_trailing_garbage: tool arguments contain complete JSON followed by runaway whitespace (json_bytes={}, whitespace_bytes={}). Re-issue the call with valid, properly escaped arguments. Omitted {} other incomplete tool call(s) from this step.",
            self.json_bytes, self.whitespace_bytes, self.omitted_tool_calls,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProviderControl {
    /// First control unit of a stream; carries any provider warnings.
    StreamStart {
        #[serde(default)]
        warnings: Vec<String>,
    },
    /// Step boundary opened.
    StepStarted { step_id: StepId },
    /// The assembled assistant message (text + reasoning + tool calls).
    AssistantMessage { message: AssistantMessage },
    /// A locally cut attempt completed the step, but this call must receive an error,
    /// never dispatch. Transient control metadata; persisted using the existing ToolResult.
    ToolArgsTrailingGarbage { cut: ToolArgsTrailingGarbage },
    /// Step boundary closed, with authoritative usage + finish reason.
    StepFinished {
        step_id: StepId,
        usage: Usage,
        finish_reason: FinishReason,
        /// The service tier the provider says it served this step at, verbatim as the
        /// provider spelled it (`default`, `priority`, `standard`, ...). Taken only from
        /// the response (body or response header), never from the request: providers
        /// downgrade a requested tier and bill the one they used. Absent when the response
        /// did not report one; never filled with the requested tier or a default.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        service_tier_used: Option<String>,
        /// Anthropic's `usage.speed`, verbatim, which is where its fast mode is reported
        /// (fast mode is not a service tier there). Absent when the response had none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        speed_used: Option<String>,
    },
    /// Provider stream finished (terminal, non-error).
    Finished {
        usage: Usage,
        finish_reason: FinishReason,
        #[serde(default)]
        retries_used: u32,
    },
    /// Terminal typed error. `ProviderUnavailable` is synthesized by the runner,
    /// not sent here.
    Error {
        error: ProviderError,
        #[serde(default)]
        retries_used: u32,
    },
    /// Evidence for ONE HTTP attempt of the current step, sent as that attempt ends and
    /// before the step's terminal `Finished`/`Error`. NOT a terminal and not part of the
    /// assembled step: the runner only records it (as a WAL `model_attempt_finished`), so a
    /// provider that never sends one changes nothing about the step.
    AttemptFinished { attempt: ModelAttempt },
}

/// What one provider attempt sent and received, as byte lengths and SHA-256 digests.
/// Every attempt of a step produces one, including an attempt that never got a status line,
/// so `attempt` ordinals within a step have no gaps.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ModelAttempt {
    /// 0-based ordinal of this attempt within its step.
    pub attempt: u32,
    /// Length of the canonical rendered request body, independent of transport framing.
    pub request_body_len: u64,
    /// Lowercase hex SHA-256 of that canonical body. Socket framing is recorded separately.
    pub request_body_sha256: String,
    /// The HTTP status, absent when no status line was received.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    /// The received `Content-Type` header, verbatim. Absent without a status line or when the
    /// response carried no such header.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_type: Option<String>,
    /// Length of the raw response bytes before normalization: HTTP body bytes or socket text
    /// payloads before SSE adaptation. For a cut stream this is the prefix that arrived.
    pub response_body_len: u64,
    /// Lowercase hex SHA-256 of exactly the `response_body_len` raw response bytes received.
    pub response_body_sha256: String,
    /// Present only when a non-2xx error body read stopped at its size cap: the value is that
    /// cap in bytes, and the length and digest above then cover only the first `cap` bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response_body_cap: Option<u64>,
    /// How the attempt ended.
    pub outcome: AttemptOutcome,
    /// Wall-clock time the attempt ended, in Unix milliseconds. INFORMATIONAL ONLY, NEVER A
    /// JOIN KEY: clocks on the two sides of an HTTP exchange disagree. Match attempts to the
    /// provider's own log by body digest plus order instead.
    pub ts_ms: u64,
    /// Optional wire measurements; provider and model are joined through the run's `run_started`.
    #[serde(flatten)]
    pub content_progress: Box<ContentProgress>,
    /// Wall-clock time the request was handed to the HTTP client, in Unix milliseconds: the
    /// moment a provider's prompt-cache lifetime can start, which `ts_ms` (the end of a
    /// response that may have streamed for minutes) cannot stand in for. Absent when the attempt
    /// stopped before the request was handed over, and on every record written before this
    /// field existed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at_ms: Option<u64>,
    /// Pipe carrying this attempt: `websocket` or `https`. Older records omit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<String>,
    /// Socket framing differs from the canonical rendered body by its `response.create` tag
    /// and optional routing metadata. The canonical body digest above remains the cache domain.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_request: Option<Box<WebSocketRequestDigest>>,
    /// Server close code, when a socket ended this attempt with a close frame.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub websocket_close_code: Option<u16>,
    /// What the provider said it changed in the request before the model saw it, verbatim, when
    /// the response reported that at all. Anthropic lists here every signed thinking block it
    /// dropped or let through despite failing its prefix-binding check, which happens when
    /// anything sent before that block differs from the request that produced it. Absent when
    /// the response did not report the list; an empty list means the provider reported none.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_transformations: Option<Vec<InputTransformation>>,
    /// Present only on an attempt sent with a token refreshed inside the step, either after
    /// the previous attempt of the same step was rejected with an HTTP 401 or because the held
    /// token was about to expire: what broca asked the credential source for, why, and which
    /// version of the SAME credential it was served. Never the token. Absent on every other
    /// attempt, which is also how records written before this field existed read back.
    ///
    /// Boxed because it is rarely present and every attempt record travels inside
    /// [`ProviderControl`]; inline, it would make every event on the stream that much larger.
    /// Serde writes a box exactly as its contents, so the bytes do not change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_refresh: Option<Box<CredentialRefresh>>,
    /// Account of the token actually sent on this attempt, when the vault supplied one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_account_ref: Option<String>,
    /// What this attempt saw about its provider account, for the run's reports to the routing
    /// module. NEVER RECORDED: it is skipped by serde, so the WAL record's bytes do not depend
    /// on it, and an attempt read back from the WAL always carries it empty.
    /// Boxed because it is empty on most attempts and every provider event carries this type.
    #[serde(skip)]
    pub account_observation: Box<AttemptAccountObservation>,
}

/// Measured complete wire records, not transport chunks or assembled output. Provider and model
/// are supplied by the run's `run_started`, not duplicated on each attempt. Absent values were
/// not observed; in particular a stream with no content has no content timestamps or counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ContentProgress {
    /// Unix milliseconds when the first real-content record was decoded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_content_at_ms: Option<u64>,
    /// Unix milliseconds when the most recent real-content record was decoded.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_content_at_ms: Option<u64>,
    /// UTF-8 data payload bytes of content-bearing records, including their JSON envelopes,
    /// excluding SSE framing. Provider and model come from the run's `run_started`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content_bytes: Option<u64>,
    /// UTF-8 data payload bytes of non-content records, or comment bytes for keepalives,
    /// excluding SSE framing. Provider and model come from the run's `run_started`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_content_bytes: Option<u64>,
    /// Top sixteen event types by count (ties ordered by name), then an observed `other` bucket.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event_types: Option<Vec<EventTypeTally>>,
    /// Non-content record bytes following the last content, absent without content; zero when
    /// content was observed and no following non-content record was observed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub non_content_bytes_after_last_content: Option<u64>,
    /// Complete records following the last content, absent without content; zero for no suffix.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub events_after_last_content: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventTypeTally {
    pub event_type: String,
    pub count: u64,
    /// Absent if the transport did not expose payload lengths, as with WebSocket ping/pong.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
    /// Present only for event names not recognized by this wire family's explicit mapping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unknown: Option<bool>,
}

/// Live advisory status, never reconstructed from attempt records after a restart.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InFlightStep {
    pub step_id: StepId,
    pub started_at_ms: u64,
    pub attempt: u32,
    pub attempt_started_at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_content_at_ms: Option<u64>,
}

/// Exact UTF-8 text payload sent in a Responses WebSocket request, before frame compression.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WebSocketRequestDigest {
    pub len: u64,
    pub sha256: String,
}

/// The account evidence of one HTTP attempt, carried in memory from the transport to the run's
/// account reporter. Values are kept exactly as the provider sent them; nothing is parsed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AttemptAccountObservation {
    /// Every response header whose lowercase name starts with
    /// [`RATELIMIT_UNIFIED_HEADER_PREFIX`], keyed by that lowercase name, its value as received.
    /// Empty when no status line arrived or the provider sent none of them.
    pub ratelimit_headers: std::collections::BTreeMap<String, String>,
    /// Present only when this attempt ended in an account-limit refusal: the reset and window
    /// fields the refusal recorded.
    pub limit: Option<AttemptAccountLimit>,
}

/// The prefix of Anthropic's subscription rate-limit response headers.
pub use cortexkit_anthropic_policy::RATELIMIT_UNIFIED_HEADER_PREFIX;

/// The account-limit fields of an attempt that ended `account_limit_exhausted`, copied from the
/// refusal the transport classified (see `ProviderError::resets_at_ms` and its siblings).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AttemptAccountLimit {
    pub resets_at_ms: Option<u64>,
    pub resets_at_source: Option<String>,
    pub limit_window: Option<String>,
}

/// A token refresh performed inside one step: either after the provider rejected the token the
/// step was sent with, or just before an attempt because the held token was about to expire.
/// The credential itself is the one frozen at admission and never changes; only the token served
/// for it does. Holds request parameters and a version counter, never secret material.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialRefresh {
    /// The credential-vault operation the refresh was sent as: `get_scoped` today, `get` on
    /// records written while broca still read credentials by capability handle.
    pub vault_op: String,
    /// The minimum remaining validity demanded of the refreshed token, if any.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_ttl_ms: Option<i64>,
    /// Whether the read itself demanded a refresh regardless of the token's remaining validity.
    /// Always written. True on older `get` records, which forced the refresh; false on a
    /// `get_scoped` refresh, which has no such flag and refreshes because the failure report
    /// sent just before it marked the rejected version stale.
    pub force_refresh: bool,
    /// The vault's record version of the token served by the refresh, when it reported one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub record_version: Option<u64>,
    /// Why the token was refreshed: [`CredentialRefresh::REASON_REJECTED`] after a 401, or
    /// [`CredentialRefresh::REASON_EXPIRING`] before a send whose held token was close to its
    /// expiry. Absent on records written before the field existed, which were all rejections.
    /// Kept as a string so a reason a later build writes still decodes, verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

impl CredentialRefresh {
    /// The provider answered 401 to the token the previous attempt was sent with.
    pub const REASON_REJECTED: &'static str = "rejected";
    /// The held token had too little life left to carry the next request, so it was replaced
    /// before sending. No provider rejected it and no failure was reported to the vault.
    pub const REASON_EXPIRING: &'static str = "expiring";
}

/// One entry of a provider's report of how it altered the request input, kept as the provider
/// wrote it: every field is optional and an unrecognised `type` or `reason` is carried verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InputTransformation {
    /// The kind of change, for example `thinking_dropped`.
    #[serde(rename = "type", default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    /// Where in the request the changed block sat, for example `messages.1.content.0`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Why, for example `prefix_binding_mismatch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

/// How one provider HTTP attempt ended. Serialized as a bare string.
///
/// THIS SITS ON A WAL RECORD, so it must decode values a later build writes: production rolls
/// back, and a rolled-back build that refused an outcome name it did not know would fail to
/// decode the record. Any string therefore decodes, and an unrecognised one is KEPT verbatim in
/// [`AttemptOutcome::Unknown`], so re-serializing it writes back exactly the string that was
/// read. (`#[serde(other)]` would also decode it, but would forget the string.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AttemptOutcome {
    /// The response streamed to a complete, validated step.
    Complete,
    /// Broca stopped the attempt itself after complete argument JSON was followed by runaway
    /// whitespace. The step is complete and validated; this is not a provider failure.
    ToolArgsTrailingGarbage,
    /// A 2xx body ended cleanly before the provider sent the step's completion.
    EofBeforeCompletion,
    /// Reading a 2xx body failed mid-stream (a transport error on a chunk).
    ChunkError,
    /// No complete event arrived within the idle deadline while streaming a 2xx body.
    IdleTimeout,
    /// Complete keepalive records kept arriving on a 2xx body, but no content did, for the
    /// whole content deadline: the peer was alive and never produced anything.
    ContentTimeout,
    /// A 2xx body exceeded the per-step response size limit.
    ByteLimit,
    /// The 2xx body could not be parsed as the provider's stream grammar.
    DecodeError,
    /// The 2xx body streamed content below the configured minimum rate.
    RateFloor,
    /// The caller's cancellation (or the run's watchdog) stopped the attempt in flight.
    Cancelled,
    /// The module's shutdown seal stopped the attempt in flight; the run ends Interrupted.
    SealedInFlight,
    /// The provider sent a non-2xx status.
    HttpError,
    /// A 2xx body parsed fine but carried an in-band provider error event (for example an
    /// Anthropic `event: error`): not an HTTP failure, and not a parse failure.
    ProviderError,
    /// The connection could not be established, so no request bytes reached a server.
    ConnectFailed,
    /// Connected and sent, but no status line arrived within the header deadline.
    HeaderTimeout,
    /// Any other failure before a status line. Whether the request body was fully written is
    /// unknown and is deliberately not guessed.
    NoStatus,
    /// An outcome name this build does not know, written by a later build. Holds the name
    /// exactly as stored. This build never writes one of its own.
    Unknown(String),
}

impl AttemptOutcome {
    /// The wire spelling.
    pub fn as_str(&self) -> &str {
        match self {
            AttemptOutcome::Complete => "complete",
            AttemptOutcome::ToolArgsTrailingGarbage => "tool_args_trailing_garbage",
            AttemptOutcome::EofBeforeCompletion => "eof_before_completion",
            AttemptOutcome::ChunkError => "chunk_error",
            AttemptOutcome::IdleTimeout => "idle_timeout",
            AttemptOutcome::ContentTimeout => "content_timeout",
            AttemptOutcome::ByteLimit => "byte_limit",
            AttemptOutcome::DecodeError => "decode_error",
            AttemptOutcome::RateFloor => "rate_floor",
            AttemptOutcome::Cancelled => "cancelled",
            AttemptOutcome::SealedInFlight => "sealed_in_flight",
            AttemptOutcome::HttpError => "http_error",
            AttemptOutcome::ProviderError => "provider_error",
            AttemptOutcome::ConnectFailed => "connect_failed",
            AttemptOutcome::HeaderTimeout => "header_timeout",
            AttemptOutcome::NoStatus => "no_status",
            AttemptOutcome::Unknown(name) => name,
        }
    }
}

impl Serialize for AttemptOutcome {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for AttemptOutcome {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(match name.as_str() {
            "complete" => AttemptOutcome::Complete,
            "tool_args_trailing_garbage" => AttemptOutcome::ToolArgsTrailingGarbage,
            "eof_before_completion" => AttemptOutcome::EofBeforeCompletion,
            "chunk_error" => AttemptOutcome::ChunkError,
            "idle_timeout" => AttemptOutcome::IdleTimeout,
            "content_timeout" => AttemptOutcome::ContentTimeout,
            "byte_limit" => AttemptOutcome::ByteLimit,
            "decode_error" => AttemptOutcome::DecodeError,
            "rate_floor" => AttemptOutcome::RateFloor,
            "cancelled" => AttemptOutcome::Cancelled,
            "sealed_in_flight" => AttemptOutcome::SealedInFlight,
            "http_error" => AttemptOutcome::HttpError,
            "provider_error" => AttemptOutcome::ProviderError,
            "connect_failed" => AttemptOutcome::ConnectFailed,
            "header_timeout" => AttemptOutcome::HeaderTimeout,
            "no_status" => AttemptOutcome::NoStatus,
            _ => AttemptOutcome::Unknown(name),
        })
    }
}
