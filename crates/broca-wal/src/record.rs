//! The durable WAL record set (the authoritative semantics are in
//! `docs/durability-contract.md`). These are the assembled UNITS the runner
//! persists — never token deltas. Bodies reuse `broca-protocol` canonical types.

use std::collections::BTreeMap;
use std::sync::Arc;

use broca_protocol::{
    caller::{PauseReason, RunFinishReason},
    content::{AssistantMessage, FinishReason, Message, ToolResult},
    error::ProviderError,
    request::{
        BudgetConfig, BudgetKind, ContextManagement, GenerationConfig, ModelHandle, ResponseFormat,
        StopCondition, SubmissionRequest, Tool, ToolChoice,
    },
    tags::MessageTags,
    AttemptOutcome, BatchId, BindIdentity, BindPrincipal, CredentialRefresh, Delivery,
    InputTransformation, ModelAttempt, RunId, SessionScope, StepId, SubmissionId, ToolCallId,
    Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

mod plan_manifest;
pub use plan_manifest::{
    FetchOutcome, FrozenDeclaration, FrozenHoldArgument, FrozenProviderDeclarations,
    FrozenReferenceSlot, FrozenReply, FrozenResultFormat, OpaqueJson, PlanManifest, ProviderFetch,
};
mod compaction;
pub use compaction::{
    compaction_replacement_sha256, CompactionCursor, CompactionDisposition, CompactionMessageKind,
    CompactionRange, CompactionStabilityRank, CompactionStrip, CompactionViewChoice,
    CompactionWait, CompactionWaitExit, RejectedCompaction, MAX_REPLACEMENT_BYTES,
};
mod fallback;
pub use fallback::{
    RefusalFallbackPhase, RefusalFallbackSchedule, StepFallback, StepRoute, StepRouteReason,
    UsageIteration,
};

fn is_false(value: &bool) -> bool {
    !*value
}

/// The terminal attached to a queued-submission tombstone when admission permanently fails.
/// Keeping it on the single tombstone frame makes queue consumption and the caller-visible
/// failure atomic across a crash.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct QueuedAdmissionFailure {
    pub run_id: RunId,
    pub error: ProviderError,
}

/// Durable hook attribution. Rendered record fields already hold the final subject, except
/// queued inputs and intentless pre-tool terminals, which keep it in `final` instead. The
/// original/final JSON uses the subject's own wire type (messages, assistant message, tool
/// input or result); it is heterogeneous because a ToolResult can also close a pre-tool deny.
/// Operations are evidence only: readers never reconstruct a subject by applying them.
/// Records box this optional metadata so hook-free records do not pay its in-memory size.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Hooks {
    #[serde(
        default,
        deserialize_with = "present_hook_subject",
        skip_serializing_if = "Option::is_none"
    )]
    pub original: Option<Value>,
    #[serde(
        default,
        rename = "final",
        deserialize_with = "present_hook_subject",
        skip_serializing_if = "Option::is_none"
    )]
    pub final_value: Option<Value>,
    pub ops: Vec<HookOp>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unavailable: Option<Vec<String>>,
}

// A tool's input can itself be JSON null. Keep a present null distinct from an omitted
// subject; Option's usual decoder would erase that original/final value on a round trip.
fn present_hook_subject<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<Value>, D::Error> {
    Value::deserialize(deserializer).map(Some)
}

/// One accepted operation's provenance, not the operation payload. Names decode open so a
/// reader retains attribution from later writers without inventing new executable behavior.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HookOp {
    pub module: String,
    pub hook: String,
    /// Only pre-tool hooks have a phase; other hook sites write null.
    pub phase: Option<String>,
    pub op: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub block: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub note: Option<String>,
}

/// The billing join key: exactly 128 random bits encoded as 32 lowercase hex characters.
/// Positions, run ids and attempt ordinals are not billing identities.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct AttemptId(String);

impl AttemptId {
    /// Encode bytes supplied by the OS random source without reserving version or variant bits.
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        use std::fmt::Write;
        let mut hex = String::with_capacity(32);
        for byte in bytes {
            write!(hex, "{byte:02x}").expect("writing to a String cannot fail");
        }
        Self(hex)
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<'de> Deserialize<'de> for AttemptId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let value = String::deserialize(deserializer).map_err(|error| {
            serde::de::Error::custom(format!(
                "attempt_id must be a lowercase hexadecimal string: {error}"
            ))
        })?;
        if value.len() != 32
            || !value
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(serde::de::Error::custom(
                "attempt_id must be exactly 32 lowercase hexadecimal characters",
            ));
        }
        Ok(Self(value))
    }
}

fn present_attempt_id<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<AttemptId>, D::Error> {
    AttemptId::deserialize(deserializer).map(Some)
}

/// Identity of one correction, independent of the physical attempt it corrects.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize)]
#[serde(transparent)]
pub struct CorrectionId(AttemptId);

impl CorrectionId {
    pub fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(AttemptId::from_bytes(bytes))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }
}

impl<'de> Deserialize<'de> for CorrectionId {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        AttemptId::deserialize(deserializer)
            .map(Self)
            .map_err(|error| {
                serde::de::Error::custom(format!(
                    "correction_id must be exactly 32 lowercase hexadecimal characters: {error}"
                ))
            })
    }
}

/// Closed reasons for replacing an attempt's evidence. Corrections never add another send.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AttemptCorrectionReason {
    /// Later provider usage or a repaired normalization replaces previously reported usage.
    UsageReconciled,
    /// Correct response evidence, including model_used, status, timing or outcome;
    /// never change the reservation's request identity.
    ProviderEvidenceCorrected,
    /// Correct only reservation.credential, including its account attribution, without
    /// inferring a billing model. Every other reservation field remains unchanged.
    CredentialAttributionCorrected,
}

/// Whether a physical send happened. Unknown is explicit rather than a missing/false value.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AttemptSent {
    Yes,
    No,
    #[default]
    Unknown,
}

impl Serialize for AttemptSent {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self {
            Self::Yes => serializer.serialize_bool(true),
            Self::No => serializer.serialize_bool(false),
            Self::Unknown => serializer.serialize_str("unknown"),
        }
    }
}

impl<'de> Deserialize<'de> for AttemptSent {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        match Value::deserialize(deserializer)? {
            Value::Bool(true) => Ok(Self::Yes),
            Value::Bool(false) => Ok(Self::No),
            Value::String(value) if value == "unknown" => Ok(Self::Unknown),
            _ => Err(serde::de::Error::custom(
                "sent must be true, false or \"unknown\"",
            )),
        }
    }
}

/// Closed billing outcomes: `complete`, `tool_args_trailing_garbage`, `eof_before_completion`,
/// `chunk_error`, `idle_timeout`, `content_timeout`, `byte_limit`, `decode_error`, `rate_floor`,
/// `cancelled`, `sealed_in_flight`, `http_error`, `compression_refused`, `provider_error`, `connect_failed`,
/// `header_timeout`, `no_status`, `unknown`. Transport names retain the meanings of the
/// legacy forensic [`AttemptOutcome`], whose independently open vocabulary is unchanged.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BillingAttemptOutcome {
    Complete,
    ToolArgsTrailingGarbage,
    EofBeforeCompletion,
    ChunkError,
    IdleTimeout,
    ContentTimeout,
    ByteLimit,
    DecodeError,
    RateFloor,
    Cancelled,
    SealedInFlight,
    HttpError,
    /// A compressed request the provider refused, retried uncompressed as a second attempt.
    CompressionRefused,
    ProviderError,
    ConnectFailed,
    HeaderTimeout,
    NoStatus,
    /// Recovery has no definitive outcome. It is never a usage measurement.
    #[default]
    Unknown,
}

/// Credential facts recorded verbatim as returned by the vault, not a classification
/// of whether the call is metered. Absent plan_tier means unknown, never a default tier.
///
/// Quota joins prefer account_ref for identified accounts. credential_ref identifies account-less
/// vault credentials such as OpenCode Go and Kimi for Coding. The insula quota provider may have
/// no credentialRef for non-vault lanes (Codex local login, Antigravity editor cache). For an account
/// with two lanes, insula reports the vault ref when the vault lane supplies the quota reading,
/// or no credentialRef when a local non-vault login supplies it; never another credential's ref.
///
/// Metadata vocabularies are open strings: known auth_method examples are `apikey`, `oauth`,
/// `chatgpt`, `antigravity`; plan_tier examples are `max_5x`, `max_20x`, `pro_200`;
/// plan_tier_source examples are `operator`, `detected`. Readers preserve unfamiliar values.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptCredential {
    /// Vault id of the credential actually served, from that token's VaultToken.resolved_credential_id
    /// (the nonempty credential_id in a credential.get/get_scoped reply), unchanged. Absent without
    /// a vault witness. It need not equal auth_selection.credential_id, a frozen resume lookup key,
    /// nor the admission-time resolved_credential_id after a different token has been served.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_selection: Option<AuthSelection>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider_ids: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_tier: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan_tier_source: Option<String>,
}

/// The bind identity and opaque session reference at reservation time.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptSession {
    #[serde(flatten)]
    pub bind: BindIdentity,
    pub session_ref: String,
}

/// Price-affecting request facts, retained even when the outcome is lost.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AttemptRun {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flow_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_class: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier_requested: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_requested: Option<String>,
}

/// Complete pre-send facts, repeated on settlement. Only `attempt_id` joins records;
/// the 0-based attempt ordinal and send index are informational and may repeat after restore.
/// Corrections repeat these fields unchanged under bill-attempts/v1, except credential for
/// CredentialAttributionCorrected. Any other reservation change requires a new feature name.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptReservation {
    pub run_id: RunId,
    pub step_id: StepId,
    pub attempt: u32,
    /// One for the initial send, two for its uncompressed resend.
    pub send_index: u32,
    pub warm: bool,
    pub reserved_at_ms: u64,
    /// Catalog provider slug, not a module address or a model prefix.
    pub provider: String,
    pub endpoint_host: String,
    /// Bare model id written in the request body after alias resolution, without a provider prefix.
    pub model_sent: String,
    pub credential: AttemptCredential,
    pub session: AttemptSession,
    pub run: AttemptRun,
    pub request_body_bytes: u64,
    pub compressed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens_estimate: Option<u64>,
}

/// Usage for one serving model, in first-provider-occurrence order. Repeated iterations of
/// the same model are coalesced. Input tokens exclude cache reads and cache writes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AttemptModelUsage {
    /// Bare vendor-reported model id, without a provider prefix.
    pub model_used: String,
    pub usage: Usage,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_5m_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_1h_tokens: Option<u64>,
}

/// Complete terminal evidence, never a correction delta. Input tokens exclude cache
/// reads/writes; unreported optional values stay absent, not zero/false/default tier.
/// Billing-envelope writers must include `sent`, `outcome` and `response_bytes`; readers reject
/// an envelope missing any of them. Legacy terminal records may omit the envelope altogether.
/// Writers must encode multi-model usage in `usage_by_model`, coalescing repeated models in
/// provider order, rather than recording only the last iteration's usage.
///
/// The decoder requires the sum of both reported cache-write splits to equal cache_write_tokens,
/// and reasoning_included_in_output whenever any normalized usage has reasoning_tokens (even zero).
/// It cannot infer whether a provider's input count was normalized correctly or verify provider
/// order after the fact; those are writer obligations. When reported usage is absent, billing
/// readers, not broca, apply this rule: a 4xx with zero response bytes means zero usage;
/// any response bytes or any 200 means unknown usage. A recovery outcome unknown is never a measurement.
/// service_tier_used and speed_used are open verbatim provider strings, not closed enums:
/// known tier examples include `default`, `flex`, `priority`, `standard`; speed examples include
/// `fast`, `standard`. Readers preserve unknown values, and never infer them from requested values.
#[derive(Debug, Clone, Default, PartialEq, Serialize)]
pub struct AttemptEnvelope {
    pub sent: AttemptSent,
    /// Bytes received and evidenced by this terminal, zero when none were observed durably.
    pub response_bytes: u64,
    pub outcome: BillingAttemptOutcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sent_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub http_status: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub finish_reason: Option<FinishReason>,
    /// Bare vendor-reported model id for a single-model reply; never inferred from model_sent.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model_used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_by_model: Option<Vec<AttemptModelUsage>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_5m_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cache_write_1h_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reasoning_included_in_output: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub usage_raw: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vendor_request_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier_used: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speed_used: Option<String>,
}

impl<'de> Deserialize<'de> for AttemptEnvelope {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Fields {
            sent: AttemptSent,
            response_bytes: u64,
            outcome: BillingAttemptOutcome,
            sent_at_ms: Option<u64>,
            finished_at_ms: Option<u64>,
            http_status: Option<u16>,
            finish_reason: Option<FinishReason>,
            model_used: Option<String>,
            usage: Option<Usage>,
            usage_by_model: Option<Vec<AttemptModelUsage>>,
            cache_write_5m_tokens: Option<u64>,
            cache_write_1h_tokens: Option<u64>,
            reasoning_included_in_output: Option<bool>,
            usage_raw: Option<Value>,
            vendor_request_id: Option<String>,
            service_tier_used: Option<String>,
            speed_used: Option<String>,
        }
        let fields = Fields::deserialize(deserializer)?;
        let envelope = Self {
            sent: fields.sent,
            response_bytes: fields.response_bytes,
            outcome: fields.outcome,
            sent_at_ms: fields.sent_at_ms,
            finished_at_ms: fields.finished_at_ms,
            http_status: fields.http_status,
            finish_reason: fields.finish_reason,
            model_used: fields.model_used,
            usage: fields.usage,
            usage_by_model: fields.usage_by_model,
            cache_write_5m_tokens: fields.cache_write_5m_tokens,
            cache_write_1h_tokens: fields.cache_write_1h_tokens,
            reasoning_included_in_output: fields.reasoning_included_in_output,
            usage_raw: fields.usage_raw,
            vendor_request_id: fields.vendor_request_id,
            service_tier_used: fields.service_tier_used,
            speed_used: fields.speed_used,
        };
        envelope.validate().map_err(serde::de::Error::custom)?;
        Ok(envelope)
    }
}

impl AttemptEnvelope {
    fn validate(&self) -> Result<(), &'static str> {
        fn splits(
            usage: Option<Usage>,
            five: Option<u64>,
            hour: Option<u64>,
        ) -> Result<(), &'static str> {
            if let (Some(five), Some(hour)) = (five, hour) {
                if five.checked_add(hour).is_none()
                    || five.checked_add(hour) != usage.and_then(|u| u.cache_write_tokens)
                {
                    return Err("cache_write_5m_tokens + cache_write_1h_tokens must equal cache_write_tokens");
                }
            }
            Ok(())
        }
        splits(
            self.usage,
            self.cache_write_5m_tokens,
            self.cache_write_1h_tokens,
        )?;
        let mut reasoning = self.usage.is_some_and(|u| u.reasoning_tokens.is_some());
        if let Some(models) = &self.usage_by_model {
            if models.len() < 2
                || self.usage.is_some()
                || self.model_used.is_some()
                || self.cache_write_5m_tokens.is_some()
                || self.cache_write_1h_tokens.is_some()
            {
                return Err("usage_by_model requires at least two models and no top-level model_used, usage or cache-write splits");
            }
            let mut seen = std::collections::BTreeSet::new();
            for model in models {
                if model.model_used.is_empty() || !seen.insert(&model.model_used) {
                    return Err("usage_by_model must coalesce repeated model_used entries");
                }
                splits(
                    Some(model.usage),
                    model.cache_write_5m_tokens,
                    model.cache_write_1h_tokens,
                )?;
                reasoning |= model.usage.reasoning_tokens.is_some();
            }
        }
        if reasoning && self.reasoning_included_in_output.is_none() {
            return Err(
                "reasoning_included_in_output is required when reasoning_tokens is present",
            );
        }
        if self.sent == AttemptSent::No
            && (self.sent_at_ms.is_some() || self.response_bytes != 0 || self.http_status.is_some())
        {
            return Err("sent false cannot carry a sent timestamp, HTTP status or response bytes");
        }
        if self.outcome == BillingAttemptOutcome::Unknown
            && (self.usage.is_some()
                || self.usage_by_model.is_some()
                || self.usage_raw.is_some()
                || self.cache_write_5m_tokens.is_some()
                || self.cache_write_1h_tokens.is_some())
        {
            return Err("unknown outcome must not carry usage");
        }
        Ok(())
    }
}

fn recovery_envelope<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Box<AttemptEnvelope>, D::Error> {
    let envelope = AttemptEnvelope::deserialize(deserializer)?;
    if envelope.outcome != BillingAttemptOutcome::Unknown {
        return Err(serde::de::Error::custom(
            "attempt_finished recovery terminal requires outcome unknown",
        ));
    }
    Ok(Box::new(envelope))
}

fn released_envelope<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Box<AttemptEnvelope>, D::Error> {
    let envelope = AttemptEnvelope::deserialize(deserializer)?;
    if envelope.sent != AttemptSent::No
        || envelope.response_bytes != 0
        || envelope.sent_at_ms.is_some()
        || envelope.http_status.is_some()
        || envelope.usage.is_some()
        || envelope.usage_by_model.is_some()
        || envelope.usage_raw.is_some()
        || envelope.cache_write_5m_tokens.is_some()
        || envelope.cache_write_1h_tokens.is_some()
    {
        return Err(serde::de::Error::custom(
            "attempt_released requires provably unsent evidence and no response or usage",
        ));
    }
    Ok(Box::new(envelope))
}

/// One durable record. Internally tagged (`{"type": …}`) for a self-describing,
/// debuggable log.
///
/// # Adding a record type: the skip contract
///
/// A build that reads a record whose `type` it does not know decodes it as
/// [`WalRecord::Unknown`] and SKIPS it everywhere: it adds nothing to replay state, the
/// reconstructed prompt, store projection, the control stream, billing or export. That is
/// what lets a newer build write a new record type while an older build it may be rolled
/// back to can still read the session.
///
/// An UNGATED NEW RECORD TYPE MAY ONLY BE ADDED IF AN OLDER BUILD CAN SAFELY SKIP IT: it must be
/// purely informational and change no replay, projection or terminal semantics. Reading a
/// WAL with the record removed must give the same session as reading it with the record
/// present. A record that changes what a session means (a new terminal, a new prompt
/// contribution, a new queue or pause transition, anything replay has to act on) is not
/// skippable: an older build would silently compute the wrong session from it. Such a record
/// requires a must-understand feature through `required_features`, shipped in a reader
/// release before any writer uses it, never reliance on the `Unknown` arm to hide it.
/// `media-content/v1` is a same-release reader/writer exception: its writer raises the
/// machine's feature floor before the first media frame. Placement and WAL open then refuse
/// older builds by feature name, so rollback past that release is blocked after media is written.
///
/// The EXHAUSTIVE matches on this enum name `Unknown` explicitly, so adding a variant still
/// fails to compile at each of them until someone decides what it means there. Many other
/// sites pick out a few variants and end in `_ =>`; those skip `Unknown` by construction and
/// will also say nothing when a variant is added. Do not add new wildcard arms to silence
/// the compiler at an exhaustive site: name the variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalRecord {
    /// A new writer has durably taken responsibility for this log's recovered prefix.
    WriterOpened {
        writer_id: [u8; 16],
        #[serde(default, skip_serializing_if = "Option::is_none")]
        previous_writer_id: Option<[u8; 16]>,
        recovered_through_seq: u64,
        cause: String,
    },
    /// This writer will make no further appends, including after a disconnected handle.
    WriterClosed {
        writer_id: [u8; 16],
    },

    /// A recently stamped publication boundary protects an older synced batch
    /// from being exposed without advancing the log's last recorded write.
    VisibilityLate {
        through_seq: u64,
        oldest_appended_ms: u64,
    },
    /// Episode boundary open — ONE ATOMIC record: the run_id, session, the
    /// AUTHORITATIVE request config, AND the user input, all in a single framed
    /// record. Atomic-by-construction so a torn crash leaves EITHER nothing OR a
    /// fully-started run — never a partial start (config-or-input missing), which
    /// would make resume reconstruct a divergent (e.g. empty-prompt) request.
    /// Synced BEFORE the first provider call.
    ///
    /// In a lineage WAL this marks the start of one EPISODE (turn). Replay walks the
    /// WAL as a sequence of episodes: each `RunStarted` closes the previous episode
    /// into the immutable conversation prefix and opens a new one. It is
    /// run_id-authoritative.
    RunStarted {
        /// Admission time of the input, preserved across queue drain and restart.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        written_at_ms: Option<u64>,
        /// Graceful restart policy frozen at admission. Absent means interrupt.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        on_restart: Option<broca_protocol::request::OnRestart>,
        /// Per-episode maintenance policy; absent records retain the historical off behavior.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keep_warm: Option<broca_protocol::request::KeepWarm>,
        run_id: RunId,
        session: BindIdentity,
        /// The registered project id the producer sent on the bind that admitted this
        /// episode, if any. Observation only: nothing keys on it yet. `None` is the
        /// legacy and the "no stable id" case alike, and serializes to no bytes.
        /// Alternating Some/None binds are visible only after the fact in the WAL;
        /// this observation does not detect or reject inconsistent producers at admission.
        ///
        /// RECORDED VERBATIM, INCLUDING VALUES THAT ARE NOT PROJECT IDS. This field is
        /// client-supplied and unattested — `subc-protocol` states that plainly, and the
        /// daemon relays it without minting, defaulting or rewriting. The normative grammar
        /// is entorhinal's `pj-…`; the daemon does not enforce it.
        ///
        /// MEASURED 2026-09-23: NO BIND HAS EVER CARRIED ONE. Across 12,934 RunStarted records
        /// this field is absent on every one. It stays observation-only because there is
        /// nothing yet to key on, not because a producer is sending bad values.
        ///
        /// A CORRECTION, KEPT HERE BECAUSE THIS COMMENT CARRIED THE ERROR FOR THREE DAYS. An
        /// earlier version claimed a producer was sending `encouraging-env-qwp21` across
        /// 1,809 WALs and 168 project roots, and gave that as the reason for this gate. It was
        /// my measurement matching a DIFFERENT FIELD WITH THE SAME NAME: the Google Code
        /// Assist render config carries its GCP project as `config.frozen_render_config
        /// .project_id`, and a byte search for `"project_id":"..."` matches that nested key on
        /// every Antigravity run. It follows the credential, not the project, which is why it
        /// spanned 168 roots. Count the FIELD, by decoding the record, never the string: the
        /// same key name appears at two depths of this one record.
        ///
        /// THE MIGRATION GATE STANDS, on its own reasoning rather than on that measurement: it
        /// is a property of the values, not the presence of the field. Two different project
        /// roots must never present the same id -- check it against the WAL corpus, by
        /// decoded field, before any keying change. A `pj-` shape check is a cheap pre-filter
        /// for whoever writes that code, but it is not the gate: a conforming-looking value
        /// that repeats across roots is just as fatal.
        ///
        /// The verbatim record is deliberate, so that whatever producers do send is visible
        /// as sent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        project_id: Option<String>,
        /// The principal the subc daemon stamped on the bind whose send started this episode,
        /// exactly as stamped. For an episode drained from the queue it is the principal that
        /// QUEUED the submission (carried on its `Queued` record), not whoever's send happened
        /// to be current when the queue drained.
        ///
        /// A session recorded without a scope may later adopt one only if the scope's owner
        /// equals the principal on every earlier episode, so this is what that check reads.
        /// `None` means the bind carried no principal, or the episode predates this field; it
        /// serializes to no bytes and never stands for any principal.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        principal: Option<BindPrincipal>,
        /// Wall-clock admission time, when written by a metering-aware producer.
        /// `None` preserves compatibility with pre-metering WAL records.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        /// Retention in seconds, frozen by the lineage's first run. A later send may
        /// record a shorter value; absence leaves the lineage's policy unchanged.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retention: Option<u64>,
        /// Session label frozen at the first send and repeated on later episodes.
        /// Readers must understand it before admitting another send into the session.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        session_class: Option<String>,
        /// Boxed — `RunConfig` is large and dwarfs every other variant; boxing
        /// keeps `WalRecord` small (this record is once-per-run, off the hot path).
        config: Box<RunConfig>,
        input: Arc<Vec<Message>>,
        /// Where this episode's conversation came from. `Fresh` (the default) = a
        /// new lineage or a follow-on turn in the same lineage. `SeededFrom` is the
        /// fork seam (reserved on the wire; v2 only constructs `Fresh`).
        #[serde(default)]
        origin: RunOrigin,
        /// The submission this run consumed, if it was started from the durable
        /// queue (so replay can tell which queued prompt became which run, and never
        /// double-consume one across a crash). `None` for a directly-started run.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        submission_id: Option<SubmissionId>,
        /// The caller's `send_id` for the send that carried this episode's message: the send
        /// itself when the lineage was idle, or the queued send this episode drained. `None`
        /// for a send without one, an internal re-drive, or a record written before this field.
        ///
        /// Written here, in the record that carries the message, so the WAL alone can answer a
        /// keyed retry: the run index is only a projection and can lose the row. Informational
        /// for replay (it changes nothing a session renders), so an older build may ignore it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        send_id: Option<String>,
        /// The canonical identity of the caller's request (see `submission_identity_digest` in
        /// broca-core), written beside `send_id` so a retry with the same key can be compared
        /// against what was actually sent and a changed payload refused by field name. `None`
        /// for a send whose run id cannot repeat, or a record written before this field; a
        /// reader then falls back to the run index's copy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_identity_digest: Option<String>,
        /// The human-readable title the caller's `session.send` carried, for dashboards and logs
        /// (for an episode drained from the queue, the title its `Queued` record carried). `None`
        /// (no bytes) for a send without one, an internal re-drive, or a record written before
        /// this field.
        ///
        /// INFORMATIONAL ONLY, so ungated: it is never rendered into a provider request and is
        /// not part of routing, cache keys or the request identity digest, so an older build that
        /// ignores it reads the same session.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// The scope this session was admitted under, frozen at its first scoped episode and
        /// repeated on every later episode, because a fold can take the first `RunStarted` out
        /// of the live WAL and each episode must still say which identity owns the session.
        ///
        /// MUST-UNDERSTAND. A reader that ignored it would admit a send from anyone into a
        /// session that only its scope's owner may drive, so a record carrying it is written
        /// only in a gated frame that requires [`crate::framing::SCOPE_FEATURE`] (see
        /// [`WalRecord::required_features`]), which a build without that name refuses as a newer
        /// writer. `None` is an unscoped episode: no bytes, an ordinary frame, readable by every
        /// build.
        /// Agent-run scopes additionally freeze `agent_id` and the stamped `run_id` on this
        /// scope (distinct from this record's broca episode `run_id`). They require the
        /// `agent-run-scopes/v1` reader feature. Builds with agent-run writing disabled refuse
        /// to start or resume work under these scopes; ordinary and flow scopes omit the fields.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        scope: Option<SessionScope>,
        /// Cache marker priority is fixed at session admission and copied to later episodes.
        /// Absent preserves the original order. No reader feature gate is required: older builds
        /// can ignore this optional field and continue rendering in their original order.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        marker_order_version: Option<u32>,
        /// The tool catalogs and system text the session's first send fetched from its
        /// providers, frozen and repeated on every later episode, because a fold can take the
        /// first `RunStarted` out of the live WAL and every episode must render from the same
        /// tools and text without fetching again.
        ///
        /// MUST-UNDERSTAND. A reader that ignored it would render the session without its
        /// frozen tools and system text, so a record carrying it is written only in a gated
        /// frame that requires [`crate::framing::PLAN_MANIFEST_FEATURE`] (see
        /// [`WalRecord::required_features`]), which a build without that name refuses as a newer
        /// writer. `None` is a session admitted without a plan: no bytes, and no feature needed.
        /// Boxed because it is large and present on few records.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        manifest: Option<Box<PlanManifest>>,
        /// Pre-user output; `input` holds the final messages. Absent when all providers pass.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<Box<Hooks>>,
        /// Runner tags (feature `runner-tags/v1`): the per-part tags of the message(s)
        /// this record carries, assigned by the session's writer as it appends the
        /// record. Absent on every record of a session admitted before tags existed, which keeps
        /// its exact bytes.
        /// One entry per message of `input`, in order.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Arc<Vec<MessageTags>>>,
    },
    /// The assembled assistant unit for a completed model step.
    ModelStepFinished {
        /// End of the successful model attempt, before any post-assistant hooks.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        written_at_ms: Option<u64>,
        step_id: StepId,
        assistant_message: Arc<AssistantMessage>,
        usage: Usage,
        /// The provider's terminal finish reason for this step (classification /
        /// observability only — replay's prompt reconstruction never reads it).
        /// Optional so an older-shaped record still deserializes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finish_reason: Option<FinishReason>,
        #[serde(default)]
        retries_used: u32,
        /// The service tier the provider REPORTED serving this step at, verbatim as it spelled
        /// it (OpenAI `default`/`flex`/`priority`, Anthropic `standard`/`priority`, Gemini's
        /// `x-gemini-service-tier` header value, ...). It comes from the response only, because
        /// providers downgrade a requested tier and bill the tier they used. ABSENT when the
        /// response reported none: it is never filled from the requested tier or a default, so
        /// absence means "unknown", not "standard". Absent serializes to no bytes, so a step
        /// without it keeps the bytes it had before this field existed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        service_tier_used: Option<String>,
        /// Anthropic's `usage.speed`, verbatim; fast mode is reported there rather than as a
        /// service tier. Absent when the response did not report one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        speed_used: Option<String>,
        /// Post-assistant output; `assistant_message` holds the final message.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<Box<Hooks>>,
        /// Runner tags (feature `runner-tags/v1`): the per-part tags of the message(s)
        /// this record carries, assigned by the session's writer as it appends the
        /// record. Absent on every record of a session admitted before tags existed, which keeps
        /// its exact bytes.
        /// One tag per block of `assistant_message`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Box<MessageTags>>,
        /// Server-side and client-side fallback facts about this step, flattened into the
        /// record as top-level keys. All absent on an ordinary step, which then keeps its
        /// exact bytes. Each present member is must-understand (see [`StepFallback`]). Boxed
        /// so the common step pays one small allocation, not the record's size.
        #[serde(flatten)]
        fallback: Box<StepFallback>,
    },
    /// The source model refused a step (`stop_reason: "refusal"`) and the step is being re-sent
    /// to the client-side fallback model under the same step id, with no new `StepStarted`.
    /// Synced before the re-send, so a crash after it re-sends the step on the fallback model
    /// and a crash before it re-sends the step on the source.
    ///
    /// The refused content is deliberately not stored: it must never reach a prompt or a
    /// reader. The refused attempt's `ModelAttemptFinished` digests remain the forensic link.
    /// Contributes nothing to the prompt; its `usage` counts toward the run's totals because the
    /// call was billed. At most one per step. Requires `refusal-fallback/v1`.
    ModelStepRefused {
        /// The step being re-sent.
        step_id: StepId,
        /// The refused response's billed usage.
        usage: Usage,
        /// As on `ModelStepFinished`: the wire model a server-side fallback served the refused
        /// response with, when it was not the requested one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        served_model: Option<String>,
        /// Per-serving-model usage when the refused response itself used a server handoff.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        usage_iterations: Option<Vec<UsageIteration>>,
        /// How many content blocks the refused response carried; only the count is kept.
        discarded_blocks: u32,
        /// The schedule after this refusal.
        schedule: RefusalFallbackSchedule,
        /// Wall-clock time the refusal was recorded, Unix milliseconds.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
    },
    /// Evidence for ONE provider HTTP attempt of a model step: what was sent and received, as
    /// byte lengths and SHA-256 digests, and how the attempt ended. One per attempt, failed
    /// and cut attempts included, each written before the step's `ModelStepFinished` (or the
    /// run's terminal when the step failed). Appended without an fsync; it becomes durable at
    /// the next existing barrier.
    ///
    /// Legacy records are purely informational under the skip contract: replay, the reconstructed
    /// prompt, projection, the control stream, billing and export all skip it exactly as they
    /// skip [`WalRecord::Unknown`], so a WAL with these records removed reads as the same
    /// session. `retries_used` on `ModelStepFinished` stays the authoritative retry count.
    ModelAttemptFinished {
        /// The only billing join key; absent on legacy, non-billing records.
        #[serde(
            default,
            deserialize_with = "present_attempt_id",
            skip_serializing_if = "Option::is_none"
        )]
        attempt_id: Option<AttemptId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reservation: Option<Box<AttemptReservation>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        envelope: Option<Box<AttemptEnvelope>>,
        /// The same step id the step's `ModelStepFinished` carries.
        step_id: StepId,
        /// 0-based attempt ordinal within the step, informational, never a billing join key.
        attempt: u32,
        request_body_len: u64,
        /// Lowercase hex SHA-256 of the canonical rendered body, excluding socket framing.
        request_body_sha256: String,
        /// Absent when no status line was received.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        http_status: Option<u16>,
        /// The received `Content-Type` header, verbatim; absent without a status line.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        content_type: Option<String>,
        /// Length of the raw response bytes received, before SSE parsing (for a cut stream,
        /// the prefix that arrived).
        response_body_len: u64,
        /// Lowercase hex SHA-256 of exactly those `response_body_len` raw bytes.
        response_body_sha256: String,
        /// Present only when a non-2xx error body read stopped at its size cap; the length and
        /// digest then cover only the first `response_body_cap` bytes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response_body_cap: Option<u64>,
        /// How the attempt ended. Decodes open: an outcome a later build writes is kept verbatim.
        outcome: AttemptOutcome,
        /// Wall-clock attempt end, Unix milliseconds. INFORMATIONAL ONLY, NEVER A JOIN KEY:
        /// match attempts to a provider's log by body digest plus order.
        ts_ms: u64,
        /// Optional timestamps and byte counts for text, reasoning/summary, function-argument
        /// and refusal deltas, plus the longest gap between consecutive real-content records and
        /// when that gap ended, non-content bytes, a bounded event tally and the suffix following
        /// the last content. Legacy attempts take provider/model context from run_started;
        /// billing attempts repeat the physical send's identity in reservation/envelope.
        #[serde(flatten)]
        content_progress: Box<broca_protocol::provider::ContentProgress>,
        /// Wall-clock time the request was handed to the HTTP client, Unix milliseconds: when the
        /// provider's prompt-cache lifetime can start. After a restart it is what dates the
        /// session's cache, since `ts_ms` is the end of a response that may have run for minutes.
        /// Absent when the attempt stopped before sending, and on every record written before this
        /// field existed, which then read exactly as they did.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sent_at_ms: Option<u64>,
        /// Transport selected at runtime, never a frozen rendering input. Absent on older records.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transport: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        websocket_request: Option<Box<broca_protocol::provider::WebSocketRequestDigest>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        websocket_close_code: Option<u16>,
        /// The provider's own report of what it changed in the request, verbatim (Anthropic's
        /// `input_transformations`: signed thinking blocks dropped or let through after failing
        /// the prefix-binding check). Absent when the response did not carry the report, which
        /// is also how every record written before this field existed reads back.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input_transformations: Option<Vec<InputTransformation>>,
        /// The request did not ask for the report above, so its absence means "not measured",
        /// not "nothing was transformed" (see `ModelAttempt::input_transformations_unmeasured`).
        /// Omitted when false, which is also how every older record reads back.
        #[serde(default, skip_serializing_if = "is_false")]
        input_transformations_unmeasured: bool,
        /// Present only on an attempt sent with a token refreshed inside the step, either after
        /// the step's previous attempt got an HTTP 401 or because the held token was about to
        /// expire (its `reason` says which): the refresh request's parameters and the served
        /// credential version, never the token. Absent otherwise and on every older record.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        credential_refresh: Option<CredentialRefresh>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        served_account_ref: Option<String>,
    },
    /// Complete pre-send evidence, durably recorded before any request bytes are sent.
    /// Exactly one terminal closes each attempt_id: ModelAttemptFinished, CacheWarmed,
    /// AttemptReleased or the recovery-only AttemptFinished. Corrections are not terminals.
    /// Requires `bill-attempts/v1`; contributes no session or prompt state.
    AttemptReserved {
        attempt_id: AttemptId,
        reservation: Box<AttemptReservation>,
    },
    /// Recovery terminal for an open reservation after a crash: outcome unknown, no usage,
    /// and sent true, false or unknown according to durable evidence. response_bytes counts
    /// only durably observed bytes, not a claim that a remote server received nothing.
    /// Do not write this terminal as ModelAttemptFinished: that record's body lengths can be
    /// read as measured sizes, which recovery cannot establish.
    AttemptFinished {
        attempt_id: AttemptId,
        reservation: Box<AttemptReservation>,
        #[serde(deserialize_with = "recovery_envelope")]
        envelope: Box<AttemptEnvelope>,
    },
    /// Settlement only for a provably unsent reservation (sent false), never an uncertain crash.
    AttemptReleased {
        attempt_id: AttemptId,
        reservation: Box<AttemptReservation>,
        #[serde(deserialize_with = "released_envelope")]
        envelope: Box<AttemptEnvelope>,
    },
    /// Later evidence about an already reported attempt, not a second terminal or another send.
    /// attempt_id identifies the corrected attempt; correction_id deduplicates this correction.
    /// The envelope is a complete replacement of the previous evidence, never a delta.
    /// With several corrections, the latest in log order (frame seq) is the evidence in force.
    /// appended_ms is for the watermark only, not correction precedence: it can tie across writers.
    ///
    /// Under bill-attempts/v1 every reservation field stays unchanged, including provider,
    /// model_sent, run_id and step_id. Only the envelope may change, plus reservation.credential
    /// when reason is CredentialAttributionCorrected. Any other reservation change requires a
    /// new feature name. Readers retaining the original reservation must not apply a correction
    /// that violates this rule; a single-record decoder has no earlier reservation to compare.
    AttemptCorrected {
        attempt_id: AttemptId,
        correction_id: CorrectionId,
        reason: AttemptCorrectionReason,
        reservation: Box<AttemptReservation>,
        envelope: Box<AttemptEnvelope>,
    },
    /// Step boundary open marker — written when a model step begins. Pure
    /// replay/projection observability (a `StepStarted`/`StepFinished` bracket for
    /// the control stream); never an input to prompt reconstruction.
    StepStarted {
        step_id: StepId,
    },
    /// Opens a tool batch; `planned_calls` is the durable plan — every call gets
    /// exactly one downstream terminal (intent→result, or no_dispatch).
    ToolBatchStarted {
        batch_id: BatchId,
        step_id: StepId,
        planned_calls: Vec<ToolCallId>,
    },
    /// The barrier's "before" half — ONLY for a call planned dispatchable.
    ToolDispatchIntent {
        /// Unix milliseconds observed by broca when it queued this durable dispatch record.
        /// Absent for records written before tool timing was recorded.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dispatched_at_ms: Option<u64>,
        batch_id: BatchId,
        tool_call_id: ToolCallId,
        /// The tool name (replay/projection DISPLAY only — the reconstructed prompt
        /// takes the name from the assistant message's tool_calls, NEVER from here).
        /// Optional so an older-shaped record still deserializes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_name: Option<String>,
        args: Arc<Value>,
        /// The tool provider module the call was dispatched to. Must-understand (feature
        /// `dispatch-module/v1`): `session.read` reports it as the call's `dispatched_to`, and a
        /// reader that skipped it would report a call as never dispatched. Absent on every
        /// intent written before broca recorded it, and then never guessed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        module_id: Option<String>,
        /// The tool schema the call was checked against when it was dispatched, when the
        /// session's frozen tools carry one. Must-understand under the same feature as
        /// `module_id`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_pin: Option<DispatchSchemaPin>,
        /// Pre-tool output; `args` holds the final dispatched input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<Box<Hooks>>,
        /// The approval this dispatch spends, for a call a human approved first: the
        /// `approval_digest` of its `approval_recorded {approved}`. It remains durable history;
        /// consent-based provider requests carry the saved arguments and deferred call key, not
        /// this digest. Must-understand (feature `deferred-call/v1`): a reader that skipped it
        /// would not know the approval was spent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        approval_digest: Option<String>,
        /// The key of the approved call's `tool_call_deferred` record (`<lineage_id>:<wal_seq>` of
        /// that record's frame), for a call a human approved first. When present it is the
        /// call's key, instead of the one derived from this intent's own frame, so the key the
        /// tool provider sees equals the key the question was filed under. Must-understand under
        /// `deferred-call/v1`: a reader that derived the key from this frame would lose the link.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_key: Option<String>,
    },
    /// A durable terminal for a call that NEVER left the process: denied /
    /// validation-failed / unavailable-before-send / policy-blocked. Resume NEVER
    /// re-dispatches these.
    ToolNoDispatch {
        /// Unix milliseconds observed when broca recorded the no-dispatch decision.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        dispatched_at_ms: Option<u64>,
        batch_id: BatchId,
        tool_call_id: ToolCallId,
        reason: NoDispatchReason,
        /// Pre-tool output for a mutated input rejected before dispatch; `final` holds it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<Box<Hooks>>,
        /// Runner tags (feature `runner-tags/v1`): the per-part tags of the message(s)
        /// this record carries, assigned by the session's writer as it appends the
        /// record. Absent on every record of a session admitted before tags existed, which keeps
        /// its exact bytes.
        /// One tag, for the result replay builds from `reason`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Box<MessageTags>>,
    },
    /// The barrier's "after" half. `outcome` tags whether this is a real result, an
    /// ambiguous "sent but no result" outcome, or a synthetic interrupt/max-steps
    /// repair. The embedded `ToolResult` is always provider-valid (so the
    /// reconstructed prompt is valid).
    ToolResult {
        /// Unix milliseconds observed when broca recorded the result, not a replay-time clock.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        finished_at_ms: Option<u64>,
        batch_id: BatchId,
        result: Arc<ToolResult>,
        outcome: DispatchOutcome,
        /// Post-tool output, or pre-tool attribution on an intentless deny/unavailable result.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<Box<Hooks>>,
        /// A typed reason name for a result broca wrote in place of a provider's, so a reader
        /// can tell why without parsing the result text. Today it is set only when the route a call
        /// was to use, opened under the run's scope, closed. It then names what the daemon said
        /// about the scope when asked (`scope.describe`): `scope_ended`, `scope_not_live`,
        /// `scope_parent_ended`, `scope_agent_changed` (the scope is now bound to another agent)
        /// or `scope_unverifiable` (the scope's state could not be confirmed). It can also be the
        /// refusal code the daemon gave when broca tried to re-open the route. With `outcome: completed` the call was never sent (`route_drained`); with
        /// `outcome: unknown` it was sent and its result never came back.
        ///
        /// Optional, and not a must-understand field (one an older reader would misread by
        /// skipping): the outcome already carries everything replay acts on, so a reader that
        /// skips this field computes the same session. An open string, not
        /// an enum, so a reason a later build adds never makes this build's reader fail the WAL.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
        /// Set when this result closed a call that was waiting for a human's approval without
        /// dispatching it (a cancel, a stop that does not resume, or a seal): the question filed
        /// for the call is to be withdrawn. Must-understand (feature `deferred-call/v1`): a
        /// reader that skipped it would lose the obligation to withdraw the question.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        withdraw_elicitation: Option<WithdrawElicitation>,
        /// Set when the tool provider holds the call past its reply: a blocking ask still in
        /// flight, or a `decision_pending` result naming the question it filed. Replay renders
        /// `result` unchanged. Must-understand (feature `deferred-call/v1`): a reader that
        /// skipped it would lose the obligation to withdraw the call from the provider.
        ///
        /// Superseded by [`WalRecord::ToolCallProviderHeld`] and never written: a provider-held
        /// call is to be recorded with that record instead, whose interim reply never reaches the
        /// model. Always `None` on every record broca writes; kept so the reader still decodes it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        provider_held: Option<ProviderHeld>,
        /// Runner tags (feature `runner-tags/v1`): the per-part tags of the message(s)
        /// this record carries, assigned by the session's writer as it appends the
        /// record. Absent on every record of a session admitted before tags existed, which keeps
        /// its exact bytes.
        /// One tag, for `result`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Box<MessageTags>>,
    },
    /// Batch commit marker — written only after EXACTLY one terminal per planned
    /// call.
    ToolBatchFinished {
        batch_id: BatchId,
    },
    /// Turn boundary — synced after the turn's batch commits/repairs.
    TurnFinished {
        step_id: StepId,
    },
    /// A caller-supplied budget message was injected at this durable prompt position.
    BudgetFired {
        run_id: RunId,
        kind: BudgetKind,
        threshold: u64,
        ordinal: u64,
        /// The exact caller text injected when this firing became durable. This replay-only
        /// observation is intentionally absent from request and metering identities.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        caller_text: Option<String>,
        /// Runner tags (feature `runner-tags/v1`): the per-part tags of the message(s)
        /// this record carries, assigned by the session's writer as it appends the
        /// record. Absent on every record of a session admitted before tags existed, which keeps
        /// its exact bytes.
        /// One tag, for the budget message's text block.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Box<MessageTags>>,
    },
    /// Session policy, frozen at admission even when the session was previously unlimited.
    InputTokenCapFrozen {
        limit: u64,
    },
    /// Written in the terminal barrier, including any never-dispatched tool results.
    InputTokenBudgetExhausted {
        detail: broca_protocol::BudgetExhaustedDetail,
    },
    /// A step's bounded ceiling wait. Must-understand: `context-ceiling/v1`.
    ContextCeilingWait {
        step_id: StepId,
        deadline_ms: u64,
        /// Once the ride completes, remember its availability so recovery does not call it
        /// again or change the eventual refusal's cause.
        unavailable: Option<bool>,
    },
    /// The durable decision of a ceiling check, before any model I/O. `None` permits this
    /// step to send; a detail seals the run without sending. Must-understand: `context-ceiling/v1`.
    ContextCeilingDecided {
        step_id: StepId,
        detail: Option<broca_protocol::ContextCeilingDetail>,
    },
    /// A step's output-token cap, lowered below the run's frozen cap before any model I/O so
    /// that the estimated input plus the output reservation stays inside the run's context
    /// limit. Written (and synced) only for a step whose cap was lowered; a step sent with the
    /// frozen cap writes nothing, so its WAL bytes are as before. The step's request, and every
    /// re-render of it (a resume, a cache warm, a cache dump), renders `max_output_tokens`
    /// instead of the frozen cap. Must-understand: `output-reservation/v1`, because a build that
    /// skipped the record would resume the step with the frozen cap, a different request from
    /// the one recorded.
    OutputReservationClamped {
        step_id: StepId,
        /// The cap the step's request carries.
        max_output_tokens: u32,
        /// The input-token estimate the cap was derived from, kept for diagnosis only.
        estimated_input_tokens: u64,
        /// The run's frozen context limit the estimate was checked against.
        context_limit: u64,
    },
    /// A rewrite ride (compaction-provider 0.5.1 §8) whose answer has been pending for
    /// `COMPACTION_WAIT_REPORT_AFTER_MS` (llm-runner 0.5.2 §6): the step waits for it until
    /// `deadline_ms`, the absolute time the ride's deadline ends, and the run reports
    /// `compaction_wait` with that deadline until a `CompactionRideWaitEnded` follows. Written
    /// only for a session with a compaction provider, in a gated frame requiring
    /// `compaction-ride-wait/v1`, so no older build reads it as something it understands.
    CompactionRideWait {
        step_id: StepId,
        deadline_ms: u64,
    },
    /// The ride a `CompactionRideWait` reported resolved: it answered, was refused, or its
    /// deadline passed. Gated like the wait record.
    CompactionRideWaitEnded {
        step_id: StepId,
    },
    /// Anthropic refused the strict schema's compiled grammar. From this barrier onward,
    /// the run renders non-strict and reports `grammar_too_large`, including after a crash.
    StructuredOutputDowngraded {
        run_id: RunId,
    },
    /// What a completed run admitted with structured output produced, as `run.result` and
    /// `run.status` report it (the role crate's `StructuredOutputResult`). Written in the same
    /// commit as the run's `RunFinished`, so the value, its validation and the skipped siblings
    /// are decided once, when the run ends, and never recomputed on read.
    StructuredOutputResult {
        run_id: RunId,
        result: broca_protocol::structured_output::StructuredOutputResult,
    },
    /// Episode close — synced BEFORE terminal success + lease release. Marks THIS
    /// episode terminal; it does NOT seal the lineage (a later `RunStarted` may open
    /// the next episode). `error` is populated only when `reason == Error`.
    RunFinished {
        /// The interrupt reason recorded with this terminal; never inferred for older records.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        interrupt_reason: Option<String>,
        reason: RunFinishReason,
        /// Wall-clock terminal time, when available. Older WALs omit this metadata.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        usage: Usage,
        /// The typed terminal error when the run ended in `Error` (classification /
        /// `run.status` observability only — never an input to prompt rebuild).
        /// Optional so an older-shaped record still deserializes.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<ProviderError>,
        #[serde(default)]
        retries_used: u32,
    },
    /// An accepted cancellation, synced before its acknowledgement. This record does not seal
    /// the episode: recovery still repairs the current tool batch using its dispatch evidence.
    /// Older readers must refuse the feature rather than start a model step after cancellation.
    RunCancelRequested {
        run_id: RunId,
        at: RunCancelAt,
    },
    /// Episode PAUSE — a NON-terminal durable record (distinct from `RunFinished`).
    /// Written when a run cannot proceed but is RESUMABLE: a mid-run auth failure
    /// (`needs_reauth`/revoke) pauses the episode rather than ending it, so replay
    /// continues the SAME run_id after the cause is resolved. Replay treats this as
    /// `AuthRequired`, never terminal (a terminal would seal a resumable run). Always
    /// the last record of its episode (no new episode starts until the pause resolves).
    RunPaused {
        run_id: RunId,
        /// Wall-clock pause time, used as the terminal time when this is the final record.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        reason: PauseReason,
        /// The error detail behind the pause, persisted for `run.status`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<ProviderError>,
    },
    /// Replaces the episode's initially frozen provider render config after an auth-at-start
    /// pause. This is legal only before the first provider call: the initial credential lookup
    /// could not supply credential-bound render fields, while the successful re-auth lookup can.
    /// Synced before that first call so every later crash resume uses the complete config.
    RunConfigRefrozen {
        renderer_version: u32,
        /// Wall-clock time at which the refreeze became durable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        frozen_render_config: Value,
    },
    /// Replaces the episode's frozen credential selection after successful re-authentication.
    /// `RunConfigRefrozen` is limited to once before the first provider call because render config
    /// affects request bytes. Authentication metadata does not, so this record may appear after
    /// provider calls and more than once. Synced before the resumed provider call so billing can
    /// label each side of the boundary.
    RunAuthRefrozen {
        /// Wall-clock time at which the refreeze became durable.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        auth_selection: AuthSelection,
    },

    // ── durable submission queue (survives a crash, unlike a runtime-only queue) ──
    /// A prompt durably queued while a run was active. Carries the submission
    /// IDENTITY plus the caller's full REQUEST (not just the message) so a queued-then-
    /// drained submission runs with the SAME config it would have had if sent when idle
    /// (model, tools, generation, etc.) — the queue is a pure deferral, never a config-
    /// dropping path. Consume/retract is unambiguous across a crash: the pending set is
    /// `Queued − Retracted − consumed-by-a-RunStarted`.
    Queued {
        /// Admission time carried into the input record when this send is delivered.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        written_at_ms: Option<u64>,
        /// A shortened lineage retention in seconds, when this send changes it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retention: Option<u64>,
        /// Admission time in milliseconds on a retention lineage, even if its policy
        /// is unchanged. Absence preserves records written before retention support.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        /// The queued send's `keep_warm` setting is kept when the queue drains into a run;
        /// it is not part of the session's frozen render settings.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        keep_warm: Option<broca_protocol::request::KeepWarm>,
        /// Routing input retained for admission after a crash, not copied into run data.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        conversation_key: Option<String>,
        submission_id: SubmissionId,
        /// Boxed so the queued request does not determine the size of every WAL record variant.
        /// Serde treats the indirection transparently, preserving the existing WAL bytes.
        request: Box<SubmissionRequest>,
        /// Canonical identity for a caller-supplied send id. `None` means the server minted a
        /// nonce run id, or this is an older record written before durable identity tracking.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_identity_digest: Option<String>,
        /// The principal the daemon stamped on the bind that queued this submission. The
        /// `RunStarted` that later drains it records this value, because the drain can happen
        /// after a different caller's send and must not attribute this submission to them.
        /// `None` (no bytes) when that bind carried none or the record predates this field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        principal: Option<BindPrincipal>,
        /// The caller's `send_id` for this queued send. After a restart the queue is rebuilt
        /// from this record alone, so the `RunStarted` that drains it can record the key only if
        /// it is here. `None` (no bytes) for a send without one or a record written before this
        /// field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        send_id: Option<String>,
        /// The title the queued send carried. The queue is rebuilt from this record alone after a
        /// restart, so the `RunStarted` that drains it can record the title only if it is here.
        /// Informational only and ungated, like `RunStarted.title`: never rendered and not part of
        /// the request identity. `None` (no bytes) for a send without one or an older record.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// How the send asked to be delivered. `Some(Interrupt)` is the message an interrupting
        /// send queued before cancelling the run it interrupted; the rebuilt queue drains every
        /// pending interrupt ahead of the plain entries. `None` (no bytes) is a plain queued
        /// send, and every record written before this field.
        ///
        /// UNGATED ON PURPOSE. A build that does not know the field ignores it and drains the
        /// message as a plain queued send, in WAL order. That keeps delivery safe (the message
        /// runs exactly once) but not order: after a rollback onto such a build an interrupt
        /// runs behind the plain sends queued before it instead of ahead of them.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery: Option<Delivery>,
        /// Pre-user output. `request` stays original; `hooks.final` holds changed input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<Box<Hooks>>,
    },
    /// A message a caller added to the RUNNING turn (`session.send` with `delivery: steer`).
    ///
    /// Its place in the conversation is derived from its place in the WAL: replay appends the
    /// message, as a user message, at the first step boundary after this record. A step
    /// boundary is the episode's next `StepStarted` (the message is rendered by that step), or
    /// the episode's `RunFinished` when no further step comes. So a steer written while a tool
    /// call runs lands after that call's result (and after any `BudgetFired` message the turn
    /// added), and a steer written during a model step lands after that step's turn: never
    /// between a tool call and its result, and nothing earlier in the prompt changes. The live
    /// run loop takes pending steers and writes `StepStarted` while holding the same lock the
    /// writer of this record holds while appending it, so a steer is either before that
    /// `StepStarted` in the WAL and in the step's prompt, or after it and in neither; the live
    /// prompt and every replay therefore agree byte for byte.
    ///
    /// MUST-UNDERSTAND. An older build that skipped it would drop a user message from the
    /// transcript and render a different prompt, so this record is only ever written in a
    /// gated frame requiring [`crate::framing::STEER_QUEUE_FEATURE`] (see
    /// [`WalRecord::required_features`]), which such a build refuses as a newer writer.
    Steered {
        /// Admission time of this input, not the later boundary that places it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        written_at_ms: Option<u64>,
        /// A shortened lineage retention in seconds, when this send changes it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        retention: Option<u64>,
        /// Admission time in milliseconds on a retention lineage, even if its policy
        /// is unchanged. Absence preserves records written before retention support.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        /// The caller's `send_id`, carried in the same frame as the message so a retry is
        /// answered from the WAL alone. `None` (no bytes) for a send without one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        send_id: Option<String>,
        /// The request identity digest of the steering send (`submission_identity_digest` in
        /// broca-core). A retry with the same `send_id` is compared against it, and one whose
        /// payload or delivery mode differs is refused `send_id_reuse` naming the field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_identity_digest: Option<String>,
        /// The delivery mode of the send that wrote the record. Always `Some(Steer)` in this
        /// build; recorded so the record says which mode produced it without inferring that from
        /// its type.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        delivery: Option<Delivery>,
        /// The title the steering send carried. Informational only, like `RunStarted.title`: never
        /// rendered and not part of the request identity. `None` (no bytes) for a send without one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        title: Option<String>,
        /// The user message(s) the steer adds to the conversation, byte for byte as a model
        /// step renders them.
        input: Arc<Vec<Message>>,
        /// Pre-user output. Replay renders `input` as written and never runs hooks over a
        /// durable record, including records written before hooks existed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        hooks: Option<Box<Hooks>>,
        /// Runner tags (feature `runner-tags/v1`): the per-part tags of the message(s)
        /// this record carries, assigned by the session's writer as it appends the
        /// record. Absent on every record of a session admitted before tags existed, which keeps
        /// its exact bytes.
        /// One entry per message of `input`, in order.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Arc<Vec<MessageTags>>>,
    },
    /// A tombstone removing a still-pending submission. An explicit caller retract has no
    /// failure; a permanently unadmittable queue head carries its atomic caller-visible terminal.
    Retracted {
        submission_id: SubmissionId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        failure: Option<QueuedAdmissionFailure>,
    },

    // ── durable runtime state ──
    ApprovalRequested {
        tool_call_id: ToolCallId,
    },
    ApprovalResolved {
        tool_call_id: ToolCallId,
        decision: ApprovalDecision,
    },

    /// Written once by each resume of an episode that has no durable terminal, before the
    /// resume writes anything else or calls anyone: which episode it resumed, from which
    /// durable record, what it rebuilt from the log without calling a provider or a tool, what
    /// it is about to do again, and which tool calls it found with no recorded outcome.
    ///
    /// PURELY INFORMATIONAL under the skip contract on this enum: replay, the reconstructed
    /// prompt, projection, the control stream, billing and export all skip it exactly as they
    /// skip [`WalRecord::Unknown`], so an older build that does not know the kind reads the
    /// same session. It never enters a rendered request. The tool results a resume writes for
    /// the calls listed in `indeterminate` are ordinary `ToolResult` records of their own.
    ResumeStarted {
        /// The run id of the episode being resumed.
        episode: RunId,
        /// The sequence number of the last durable record the resume read.
        resumed_from_seq: u64,
        /// What the resume rebuilt from the log without calling anyone.
        replayed: ResumeReplayed,
        /// What the resume does again because no durable record of it survived.
        #[serde(default)]
        redone: Vec<ResumeRedo>,
        /// Tool calls with a durable dispatch intent and no recorded result, or a recorded
        /// `Unknown` outcome: each may or may not have run. The resume never sends them again.
        #[serde(default)]
        indeterminate: Vec<ToolCallId>,
    },

    /// One warm of the session's provider prompt cache: broca re-sent the session's most recent
    /// model request shaped so the provider generates nothing, which reads the cached prefix and
    /// restarts its lifetime. Written by the session's own writer, once per warm the provider
    /// answered successfully.
    ///
    /// PURELY INFORMATIONAL under the skip contract on this enum: replay, the reconstructed
    /// prompt, projection and the control stream skip it exactly as they skip
    /// [`WalRecord::Unknown`]. It is never a transcript message. Records carrying billing
    /// evidence require `bill-attempts/v1`; legacy records remain ungated.
    /// Billing reads it: each warm becomes its own export fact, marked `warm`, on the run whose
    /// request it re-sent. An older build skips it and so exports no fact for it.
    CacheWarmed {
        /// The only billing join key; absent on legacy warms.
        #[serde(
            default,
            deserialize_with = "present_attempt_id",
            skip_serializing_if = "Option::is_none"
        )]
        attempt_id: Option<AttemptId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reservation: Option<Box<AttemptReservation>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        envelope: Option<Box<AttemptEnvelope>>,
        /// `trigger` is `keep_warm` for warms sent by the tool-wait scheduler; absent for warms
        /// requested through `session.warm`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        trigger: Option<String>,
        /// The run whose request was re-sent.
        run_id: RunId,
        /// The step that request belongs to.
        step_id: StepId,
        /// Lowercase hex SHA-256 of the request whose prefix was warmed, the digest that
        /// request's own attempt records carry.
        request_body_sha256: String,
        /// Lowercase hex SHA-256 of the bytes the warm actually sent.
        warm_body_sha256: String,
        /// Legacy export usage only. Billing readers use envelope.usage (or its usage_by_model)
        /// and ignore this field whenever a billing envelope is present.
        usage: Usage,
        /// The account of the credential that sent the warm, when the credential source names
        /// one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        served_account_ref: Option<String>,
        /// The provider's rate-limit headers on the warm's response, verbatim, by lowercase name.
        #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
        ratelimit_headers: BTreeMap<String, String>,
        /// When sending started, Unix milliseconds. The cache's lifetime restarts here.
        sent_at_ms: u64,
        /// The cache lifetime the session's policy declares.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ttl_ms: Option<u64>,
        /// `sent_at_ms + ttl_ms`: an estimate, since no provider reports the expiry itself.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        expires_at_ms: Option<u64>,
    },

    /// The issued high-water, durable before a compaction step call. Reader-only for now.
    CompactionRequest {
        provider: String,
        request_id: String,
    },
    /// The complete applied view, inline so replay never needs the provider. Setup freezes
    /// the declarations and has no request id or cursor; an applied step carries both.
    CompactionMessage {
        provider: String,
        kind: CompactionMessageKind,
        compaction_id: String,
        version: u64,
        range: CompactionRange,
        replacement: Arc<Vec<Message>>,
        /// Lowercase SHA-256 over the compact native replacement array; see
        /// `compaction_replacement_sha256` for the exact encoding domain.
        sha256: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        request_id: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cursor: Option<CompactionCursor>,
        /// Verbatim JSON text, like the manifest's opaque plan. Unknown condition kinds
        /// and number/escape spellings survive a decode unchanged.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_when: Option<OpaqueJson>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stability: Option<Vec<CompactionStabilityRank>>,
        /// Setup's ceiling policy (compaction-provider 0.3 `CeilingPolicy`), verbatim JSON,
        /// frozen with the session. Present only on a Setup record of an asynchronous
        /// session, and then must-understand (feature `compaction-view/v1`): a reader that
        /// dropped it would let the session run past its ceiling.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ceiling: Option<OpaqueJson>,
        /// The Setup answer's tag checkpoint, on a Setup record of a session the provider moved
        /// from standalone Magic Context. Must-understand (feature `runner-tags/v2`): a reader
        /// that dropped it would number the session from the wrong anchor.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tag_checkpoint: Option<SetupTagCheckpoint>,
        /// The Setup answer's `ride_wait_ms` (compaction-provider 0.5.1 §4): how long a rewrite
        /// ride may hold a step, frozen with the session. Present only on a Setup record whose
        /// answer carried one, and then must-understand (feature `compaction-ride-wait/v1`): a
        /// reader that dropped it would give the session's rides its own short deadline.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ride_wait_ms: Option<u64>,
    },
    /// Every other step outcome and the cursor it moves, in one durable frame. A wait
    /// answer is itself the wait entry, with the reason and bound in the same record.
    CompactionAnswer {
        provider: String,
        request_id: String,
        cursor: CompactionCursor,
        disposition: CompactionDisposition,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        rejected: Option<RejectedCompaction>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        wait: Option<CompactionWait>,
    },
    /// A durable wait exit. Terminal-intent exits must survive a crash before the terminal.
    CompactionWait {
        exited: CompactionWaitExit,
        reason: String,
    },
    /// A compaction provider's view (compaction-provider/v1 §6), applied at the step boundary
    /// before the step that first renders it. Every later request of the lineage renders it,
    /// until a newer view applies, including after a restart, without asking the provider
    /// again. Must-understand (feature `compaction-view/v1`).
    CompactionViewApplied {
        /// The session's compaction provider module.
        provider: String,
        /// The view's `view_version`, repeated here so readers need not decode `view`.
        view_version: u64,
        /// The view exactly as `view.get` returned it, verbatim JSON, digest included.
        view: OpaqueJson,
        /// How it was applied: as published, or with the companion edit the model provider
        /// requires. Recorded so a resumed request strips exactly what the live one did.
        choice: CompactionViewChoice,
    },
    /// One advance notice (compaction-provider/v1 §5), recorded before it is published. The
    /// publisher projects it from the durable WAL, so it never publishes a notice that is not
    /// durable, and a republish after a crash carries the same `seq` and headers.
    /// Must-understand (feature `compaction-view/v1`): a reader that skipped one would reuse
    /// its `seq`, and the bus would drop the later notice as a duplicate.
    CompactionNotice {
        /// The session's notice sequence, from 1.
        seq: u64,
        /// The notice's headers, as the contract names and encodes them.
        headers: std::collections::BTreeMap<String, String>,
    },

    // ── calls held for a human's approval (feature `deferred-call/v1`) ──
    //
    // Every record in this group except `QuestionWithdrawalAcknowledged` is must-understand and is
    // only ever written in a gated frame requiring `crate::framing::DEFERRED_CALL_FEATURE` (see
    // `WalRecord::required_features`): an older build that skipped one would dispatch a call no
    // human approved, dispatch an approved one twice, or forget a question it owes a withdrawal.
    /// A tool call an approve hook held for a human's answer, written and synced before the
    /// question is filed. Its own frame is the call's anchor: the call's key is
    /// `<lineage_id>:<wal_seq>` of this record, derived by readers rather than stored, because the
    /// sequence number is reserved only after the payload is built. The question's
    /// `elicitation_id` is not stored either: it comes back from the filing, and after a crash
    /// re-filing under the same key returns it again.
    ToolCallDeferred {
        /// Unix milliseconds observed when broca recorded the call's approval deferral.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        deferred_at_ms: Option<u64>,
        batch_id: BatchId,
        call_id: ToolCallId,
        /// Who executes the call once approved. Always `broca` in this build.
        executor: DeferredExecutor,
        /// The tool provider module the call goes to; absent when the tool plane names none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        module: Option<String>,
        tool: String,
        /// The schema the call will be dispatched against, when the session's frozen tools carry
        /// one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema_pin: Option<DispatchSchemaPin>,
        /// The call's final argument bytes, after every rewrite, as a JSON string. These exact
        /// bytes are what the human approves and what is dispatched; they are never re-serialized.
        args_jcs: String,
        /// SHA-256 over exactly the bytes of `args_jcs`.
        args_hash: String,
        /// The session's frozen scope: the identity every route reopen and a late execution use.
        scope: SessionScope,
        /// What the approve hook asked, verbatim.
        ask: DeferredAsk,
    },
    /// A human's answer to a held call's question, or broca closing the question itself. With
    /// `approved` it authorizes the call's dispatch and is not a terminal. With `declined`,
    /// `expired` or `unavailable` it carries `closing_result` and IS the call's terminal: replay
    /// renders `closing_result` as the call's result, and no `ToolNoDispatch` or `ToolResult` is
    /// written for the call.
    ApprovalRecorded {
        /// Unix milliseconds observed when a closing result was recorded. An approval that
        /// permits dispatch has no closing result and leaves this absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        closed_at_ms: Option<u64>,
        call_key: String,
        call_id: ToolCallId,
        /// Absent only for an `unavailable` call whose question was never filed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elicitation_id: Option<String>,
        decision: DeferredDecision,
        /// Present with `approved`: the digest the dispatch intent carries to the tool provider.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        approval_digest: Option<String>,
        /// Present with `declined`, `expired` and `unavailable`: the provider-valid result the
        /// call closes with.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        closing_result: Option<Arc<ToolResult>>,
        /// Runner tags (feature `runner-tags/v1`): the per-part tags of the message(s)
        /// this record carries, assigned by the session's writer as it appends the
        /// record. Absent on every record of a session admitted before tags existed, which keeps
        /// its exact bytes.
        /// One tag, for `closing_result`; absent with `approved`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Box<MessageTags>>,
    },
    /// Which of a batch's held calls are still waiting for an answer, and the question each was
    /// filed as. Re-written whenever a filing returns or a question is answered. NOT
    /// authoritative for membership: a crash can keep a call's `ApprovalRecorded` and an older
    /// marker, so readers take the waiting set from the calls' own records and use this only for
    /// the latest `elicitation_id` of each.
    AwaitingApproval {
        batch_id: BatchId,
        pending: Vec<AwaitingApprovalEntry>,
    },
    /// An answer read after its call was closed `ApprovalRecorded {expired}`. Exactly one per
    /// expired call: `none` is written only once the question is terminal with no answer.
    /// Never enters a prompt.
    LateApprovalRecorded {
        call_key: String,
        elicitation_id: String,
        decision: LateDecision,
        /// Present with `approved`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        approval_digest: Option<String>,
        /// The ask's `late_execution`, present with `approved`: whether the late approval
        /// executes the call.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        late_execution: Option<LateExecution>,
    },
    /// The before half of a late approved call's dispatch. One without a `LateResult` resumes as
    /// `LateResult {unknown}` and is never sent again. Never enters a prompt.
    LateDispatchIntent {
        call_key: String,
        approval_digest: String,
        /// The isolated batch the call is dispatched through (`late:<call_key>`).
        batch_id: BatchId,
    },
    /// The terminal of a late approved call, in every outcome. Never enters a prompt.
    LateResult {
        call_key: String,
        outcome: LateOutcome,
        result: Arc<ToolResult>,
    },
    /// How a withdrawal of a held call's question ended. INFORMATIONAL and ungated: an older
    /// build skips it, under the skip contract on this enum, and computes the same session.
    /// It is written in an ordinary (version 1) frame and adds no required feature.
    QuestionWithdrawalAcknowledged {
        call_key: String,
        /// Absent when the question's id was never recovered.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        elicitation_id: Option<String>,
        answer: WithdrawalAnswer,
        /// With `not_sent`, why nothing was withdrawn: the reply code, `expired`, or
        /// `withdrawal_retry_exhausted` when broca stopped retrying. With `acknowledged`,
        /// absent when the withdrawal was confirmed, or the code saying the card was already
        /// settled or not ours (`elicitation_not_found`, `elicitation_not_owner`).
        #[serde(default, skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },

    // ── calls a tool provider holds past its reply (feature `provider-held/v1`) ──
    //
    // Must-understand: written only in a gated frame requiring
    // `crate::framing::PROVIDER_HELD_FEATURE`, as is a `RunPaused` with `PauseReason::ToolHeld`.
    // An older build that skipped this record would see a dispatched call without a result,
    // class it indeterminate and seal the run Interrupted, losing a result the provider still
    // owes.
    /// A tool provider accepted the call and answered with a `held` marker (tool-provider
    /// contract §4.1, `held-reply/v1`): the real result arrives later as a `late_results` entry
    /// for `call_key` (§7). The writer must append and sync it before acting on anything else
    /// in the reply. No production path writes it yet (see `framing::PROVIDER_HELD_FEATURE`).
    ///
    /// The call's `ToolDispatchIntent` precedes this record, and no terminal does: the call is
    /// open but not indeterminate, because the provider confirmed it holds the call. Its later
    /// terminal is an ordinary `ToolResult` under the same `batch_id`. The held reply's interim
    /// content is never stored and never enters a prompt; this record adds nothing to the
    /// transcript.
    ToolCallProviderHeld {
        batch_id: BatchId,
        tool_call_id: ToolCallId,
        /// The key the call was sent with, which the provider's late entry names.
        call_key: String,
        /// The tool provider module the call was dispatched to; its feed delivers the result.
        module: String,
        /// The `held.reason` the provider gave, verbatim. An open snake_case string: reasons a
        /// later contract adds are kept as sent. Informational only, never a decision input.
        reason: String,
        /// Lowercase hex SHA-256 of the held marker's `detail` object as serialized JSON, when
        /// the provider sent one. The detail itself is provider-owned and not stored.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        detail_digest: Option<String>,
        /// Wall-clock time the held reply was accepted, in milliseconds.
        held_at_ms: u64,
        /// True when the batch dispatches its calls one after another
        /// (`ExecutionMode::Sequential`): calls planned after this one have their intents but
        /// are not sent before this call has its terminal.
        sequential: bool,
    },

    // ── sends with an ordered `delivery` list (feature `delivery-order/v1`) ──
    //
    // Every record in this group is must-understand and is only ever written in a gated frame
    // requiring `crate::framing::DELIVERY_ORDER_FEATURE`: an older build that skipped one would
    // start a run from a send that must never start one, deliver a replaced or expired send, or
    // render a prompt without a delivered send. Sessions whose sends use no ordered list, dedupe
    // key or expiry never carry these records.
    /// Records the delivery order of the `Queued` record for the same `submission_id` that
    /// follows it.
    /// Both are written in one barrier, this one first, so a durable `Queued` always has its
    /// order before it (a running turn's own records may lie between the two). The queued send
    /// then waits until a request boundary where one of its modes is possible; it starts a run
    /// only if its order lists `idle_start`.
    ///
    /// `sent_by` is the attested principal of a plan-named sender other than the session's
    /// owner. Such a send is queued (and any run it starts recorded) under the session's frozen
    /// principal, never the sender's, so the sender is kept only here: it is whose dedupe key
    /// the send carries. Absent for a send from the owner.
    DeliveryOrdered {
        submission_id: SubmissionId,
        order: broca_protocol::DeliveryOrder,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sent_by: Option<BindPrincipal>,
    },
    /// A queued, undelivered send that will never be delivered: a newer send from the same
    /// sender with the same `dedupe_key` replaced it, or it expired. Removes the send from the
    /// pending queue like `Retracted`, and says why.
    SendEnded {
        submission_id: SubmissionId,
        reason: SendEndReason,
    },
    /// A queued send delivered into run `run_id` in `mode`, consuming it from the pending queue.
    ///
    /// With `steer` it carries the send's user message(s) in `input`, placed exactly like a
    /// [`WalRecord::Steered`] record's input: at the episode's next `StepStarted` (written under
    /// the same lock, right after this record), or at its `RunFinished` when no step follows.
    ///
    /// With `tool_result` it names the tool result it rides on and the exact text appended to that
    /// result's output. The stored `ToolResult` record is never changed: a reader renders the
    /// result followed by `appended_text`. It is written between the batch's `ToolBatchFinished`
    /// and its `TurnFinished`, so a projection boundary never separates the result from it.
    ///
    /// A send delivered with `idle_start` has no record of this kind: the `RunStarted` that
    /// consumes it (naming its `submission_id`) is its delivery.
    SendDelivered {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        written_at_ms: Option<u64>,
        submission_id: SubmissionId,
        run_id: RunId,
        mode: broca_protocol::DeliveryMode,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        input: Option<Arc<Vec<Message>>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tool_call_id: Option<ToolCallId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        appended_text: Option<String>,
        /// Runner tags (feature `runner-tags/v1`) of a `steer` delivery's `input`, one entry per
        /// message, like a `Steered` record's. A `tool_result` delivery's `appended_text` is part
        /// of the tool result it rides on and has no tag of its own.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tags: Option<Arc<Vec<MessageTags>>>,
    },

    /// A record whose `type` this build does not know: written by a newer build, and
    /// skipped by every reader under the contract on this enum.
    ///
    /// DELIBERATELY LOSSY. It keeps no fields, because nothing needs them: no path decodes a
    /// `WalRecord` and writes it back anywhere durable. The WAL writer only encodes records
    /// the running engine constructs; archive fold and unfold copy whole WAL files as
    /// verbatim, digest-checked bytes; store projection, export segments, billing and the
    /// control stream write values derived from records, never the records themselves; and
    /// `wal-verify` only counts frames. The Python readers under `scripts/` read the WAL
    /// bytes themselves and never go through this type.
    ///
    /// `skip_serializing` is the guard for that property: serializing this variant is an
    /// error, so `framing::encode` refuses it instead of writing `{"type":"unknown"}` where
    /// the original record's bytes used to be. If a path that re-encodes decoded records is
    /// ever added, it must copy the original frame bytes, or this variant must first become
    /// one that keeps the raw record.
    #[serde(other, skip_serializing)]
    Unknown,
}

impl WalRecord {
    /// The WAL record for one provider attempt of `step_id`.
    pub fn model_attempt_finished(step_id: StepId, attempt: ModelAttempt) -> Self {
        let ModelAttempt {
            attempt,
            request_body_len,
            request_body_sha256,
            http_status,
            content_type,
            response_body_len,
            response_body_sha256,
            response_body_cap,
            outcome,
            ts_ms,
            content_progress,
            sent_at_ms,
            transport,
            websocket_request,
            websocket_close_code,
            input_transformations,
            input_transformations_unmeasured,
            credential_refresh,
            served_account_ref,
            // In-memory account evidence for the routing reports; never part of the record.
            account_observation: _,
        } = attempt;
        WalRecord::ModelAttemptFinished {
            attempt_id: None,
            reservation: None,
            envelope: None,
            step_id,
            attempt,
            request_body_len,
            request_body_sha256,
            http_status,
            content_type,
            response_body_len,
            response_body_sha256,
            response_body_cap,
            outcome,
            ts_ms,
            content_progress,
            sent_at_ms,
            transport,
            websocket_request,
            websocket_close_code,
            input_transformations,
            input_transformations_unmeasured,
            credential_refresh: credential_refresh.map(|refresh| *refresh),
            served_account_ref,
        }
    }

    /// Whether every session reader skips this record: a type from a newer build
    /// ([`WalRecord::Unknown`]) or one this build knows but that carries no session semantics
    /// ([`WalRecord::ModelAttemptFinished`], [`WalRecord::ResumeStarted`]). Sites that pick
    /// "the first/next record that matters" filter on this so an informational record can
    /// never move their decision.
    pub fn is_informational(&self) -> bool {
        matches!(
            self,
            WalRecord::Unknown
                | WalRecord::WriterOpened { .. }
                | WalRecord::WriterClosed { .. }
                | WalRecord::VisibilityLate { .. }
                | WalRecord::ModelAttemptFinished { .. }
                | WalRecord::AttemptReserved { .. }
                | WalRecord::AttemptFinished { .. }
                | WalRecord::AttemptReleased { .. }
                | WalRecord::AttemptCorrected { .. }
                | WalRecord::ResumeStarted { .. }
                | WalRecord::CacheWarmed { .. }
                | WalRecord::QuestionWithdrawalAcknowledged { .. }
        )
    }

    /// The caller-supplied title this record carries: set only on the records a `session.send`
    /// writes (`RunStarted`, `Queued` and `Steered`), and only when the send carried one.
    pub fn title(&self) -> Option<&str> {
        match self {
            WalRecord::RunStarted { title, .. }
            | WalRecord::Queued { title, .. }
            | WalRecord::Steered { title, .. } => title.as_deref(),
            _ => None,
        }
    }

    /// The WAL features a reader must know before it may interpret this record, sorted.
    ///
    /// Empty for every record that uses no must-understand value, which is then written in an
    /// ordinary frame, byte for byte as before. The framing adds these to a frame's `requires`
    /// on its own, so no caller can write a must-understand value into a frame an older build
    /// would read without checking.
    pub fn required_features(&self) -> &'static [&'static str] {
        if matches!(self, Self::VisibilityLate { .. }) {
            return &[crate::head_proof::VISIBILITY_LATE_FEATURE];
        }
        if matches!(self, Self::WriterOpened { .. } | Self::WriterClosed { .. }) {
            return &["writer-id/v1"];
        }
        let base = self.base_required_features();
        // The agent-run, session-class and input-cap unions below are precomputed tables that
        // cover only what `base_required_features` returns, plus the names the earlier tables
        // add. Every other feature is added after them, by `with_feature`, so a new feature
        // added here never reaches a table it was not listed in. A name a table does not list
        // still gets a correct answer, through `FeatureUnionTable::lookup`'s fallback.
        let scope = match self {
            WalRecord::RunStarted { scope, .. } => scope.as_ref(),
            WalRecord::ToolCallDeferred { scope, .. } => Some(scope),
            _ => None,
        };
        let base = if scope.is_some_and(|scope| scope.run_id.is_some() || scope.agent_id.is_some())
        {
            agent_run_features(base)
        } else {
            base
        };
        let base = if matches!(
            self,
            WalRecord::RunStarted {
                session_class: Some(_),
                ..
            }
        ) {
            session_class_features(base)
        } else {
            base
        };
        let capped = match self {
            WalRecord::RunStarted { config, .. } => config
                .budget
                .as_ref()
                .is_some_and(|budget| budget.max_input_tokens.is_some()),
            WalRecord::Queued { request, .. } => request
                .budget
                .as_ref()
                .is_some_and(|budget| budget.max_input_tokens.is_some()),
            WalRecord::InputTokenCapFrozen { .. } | WalRecord::InputTokenBudgetExhausted { .. } => {
                true
            }
            WalRecord::RunFinished {
                reason: RunFinishReason::BudgetExhausted,
                ..
            } => true,
            WalRecord::ToolResult {
                reason: Some(reason),
                ..
            } if reason == "budget_exhausted" => true,
            _ => false,
        };
        let base = if capped {
            input_cap_features(base)
        } else {
            base
        };
        let base = if self.carries_media_content() {
            with_feature(base, crate::framing::MEDIA_CONTENT_FEATURE)
        } else {
            base
        };
        let base = if matches!(
            self,
            WalRecord::RunFinished {
                interrupt_reason: Some(_),
                ..
            }
        ) {
            with_feature(base, crate::framing::INTERRUPT_REASON_FEATURE)
        } else {
            base
        };
        let has_directives = match self {
            WalRecord::RunStarted { config, .. } => config.turn_directives_v1.is_some(),
            WalRecord::Queued { request, .. } => request.directives.is_some(),
            _ => false,
        };
        let base = if has_directives {
            with_feature(base, crate::framing::TURN_DIRECTIVES_FEATURE)
        } else {
            base
        };
        // A send's client_message_id lives in its identity digest (see
        // `SubmissionRequest::client_message_id`). An older build would read the digest without
        // it and answer a re-send under the same `send_id` with another id as a plain retry,
        // where this build refuses it `send_id_reuse`, so the record is must-understand.
        // A manifest that froze any provider declaration is must-understand: an older reader
        // would skip the fields and read replies by a catalog that may have changed since.
        let base = if matches!(
            self,
            WalRecord::RunStarted {
                manifest: Some(manifest),
                ..
            } if manifest.fetches.iter().any(|fetch| {
                matches!(
                    &fetch.outcome,
                    FetchOutcome::Answered {
                        declarations_frozen: Some(declarations),
                        ..
                    } if !declarations.is_empty()
                )
            })
        ) {
            with_feature(base, crate::framing::PROVIDER_DECLARATIONS_FEATURE)
        } else {
            base
        };
        let base = if matches!(self, WalRecord::RunStarted { manifest: Some(manifest), .. }
            if manifest.fetches.iter().any(|fetch| matches!(&fetch.outcome,
                FetchOutcome::Answered { declarations_frozen: Some(declarations), .. }
                    if declarations.schema_pins.is_some())))
        {
            with_feature(base, crate::framing::TOOL_SCHEMA_PINS_FEATURE)
        } else {
            base
        };
        let identity_digest = match self {
            WalRecord::RunStarted {
                request_identity_digest,
                ..
            }
            | WalRecord::Queued {
                request_identity_digest,
                ..
            }
            | WalRecord::Steered {
                request_identity_digest,
                ..
            } => request_identity_digest.as_deref(),
            _ => None,
        };
        let base = if identity_digest.is_some_and(digest_carries_client_message_id) {
            with_feature(base, crate::framing::CLIENT_MESSAGE_ID_FEATURE)
        } else {
            base
        };
        let codex_wire = match self {
            Self::RunStarted { config, .. } => config.frozen_render_config.get("chatgpt_wire"),
            Self::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => frozen_render_config.get("chatgpt_wire"),
            _ => None,
        };
        let codex_attempt = matches!(self, Self::ModelAttemptFinished { websocket_request: Some(frame), .. } if frame.codex.is_some());
        let base = if codex_attempt || codex_wire.is_some_and(|value| !value.is_null()) {
            with_feature(base, crate::framing::CHATGPT_CODEX_WIRE_FEATURE)
        } else {
            base
        };
        let base = if self.carries_provider_text_state() {
            with_feature(base, crate::framing::PROVIDER_TEXT_STATE_FEATURE)
        } else {
            base
        };
        let base = if self.carries_provider_fallback() {
            with_feature(base, crate::framing::PROVIDER_FALLBACK_BLOCK_FEATURE)
        } else {
            base
        };
        let base = if self.carries_refusal_fallback() {
            with_feature(base, crate::framing::REFUSAL_FALLBACK_FEATURE)
        } else {
            base
        };
        let base = if self.carries_openrouter_reasoning() {
            with_feature(base, crate::framing::OPENROUTER_REASONING_FEATURE)
        } else {
            base
        };
        let base = if self.carries_structured_output() {
            with_feature(base, crate::framing::STRUCTURED_OUTPUT_FEATURE)
        } else {
            base
        };
        let base = if self.carries_structured_output_paragraph_break() {
            with_feature(base, crate::framing::STRUCTURED_OUTPUT_PARAGRAPH_FEATURE)
        } else {
            base
        };
        let base = if matches!(
            self,
            WalRecord::CompactionMessage {
                ceiling: Some(_),
                ..
            }
        ) {
            with_feature(base, crate::framing::COMPACTION_VIEW_FEATURE)
        } else {
            base
        };
        let base = if matches!(
            self,
            WalRecord::CompactionMessage {
                ride_wait_ms: Some(_),
                ..
            }
        ) {
            with_feature(base, crate::framing::COMPACTION_RIDE_WAIT_FEATURE)
        } else {
            base
        };
        let base = if self.carries_runner_tags() {
            with_feature(base, crate::framing::RUNNER_TAGS_FEATURE)
        } else {
            base
        };
        let computed_tags = match self {
            WalRecord::RunStarted { config, .. } => config.computed_tags.is_some(),
            WalRecord::CompactionMessage { tag_checkpoint, .. } => tag_checkpoint.is_some(),
            _ => false,
        };
        let base = if computed_tags {
            with_feature(base, crate::framing::RUNNER_TAGS_V2_FEATURE)
        } else {
            base
        };
        let base = if self.carries_tool_reply() {
            with_feature(base, crate::framing::TOOL_REPLY_FEATURE)
        } else {
            base
        };
        let base = if self.carries_auth_selection_method() {
            with_feature(base, crate::framing::AUTH_SELECTION_METHOD_FEATURE)
        } else {
            base
        };
        let base = if self.carries_reference_slots() {
            with_feature(base, crate::framing::PLAN_REFERENCE_SLOTS_FEATURE)
        } else {
            base
        };
        let base = if self.carries_google_rpc_error_details() {
            with_feature(base, crate::framing::GOOGLE_RPC_ERROR_DETAILS_FEATURE)
        } else {
            base
        };
        // Like every feature added after the precomputed unions above, message times never reach
        // their tables.
        let base = if matches!(
            self,
            Self::RunStarted {
                written_at_ms: Some(_),
                ..
            } | Self::ModelStepFinished {
                written_at_ms: Some(_),
                ..
            } | Self::Steered {
                written_at_ms: Some(_),
                ..
            } | Self::Queued {
                written_at_ms: Some(_),
                ..
            } | Self::SendDelivered {
                written_at_ms: Some(_),
                ..
            }
        ) {
            with_feature(base, crate::framing::MESSAGE_TIMES_FEATURE)
        } else {
            base
        };
        let base = if self.carries_billing_attempt() {
            with_feature(base, crate::framing::BILL_ATTEMPTS_FEATURE)
        } else {
            base
        };
        if matches!(
            self,
            Self::ToolDispatchIntent {
                dispatched_at_ms: Some(_),
                ..
            } | Self::ToolNoDispatch {
                dispatched_at_ms: Some(_),
                ..
            } | Self::ToolResult {
                finished_at_ms: Some(_),
                ..
            } | Self::ToolCallDeferred {
                deferred_at_ms: Some(_),
                ..
            } | Self::ApprovalRecorded {
                closed_at_ms: Some(_),
                ..
            }
        ) {
            with_feature(base, crate::framing::TOOL_TIMES_FEATURE)
        } else {
            base
        }
    }

    fn carries_billing_attempt(&self) -> bool {
        match self {
            Self::AttemptReserved { .. }
            | Self::AttemptFinished { .. }
            | Self::AttemptReleased { .. }
            | Self::AttemptCorrected { .. } => true,
            Self::ModelAttemptFinished {
                attempt_id,
                reservation,
                envelope,
                ..
            }
            | Self::CacheWarmed {
                attempt_id,
                reservation,
                envelope,
                ..
            } => attempt_id.is_some() || reservation.is_some() || envelope.is_some(),
            _ => false,
        }
    }

    /// A `RunStarted` whose plan manifest froze a provider's catalog reference slots.
    fn carries_reference_slots(&self) -> bool {
        matches!(self, WalRecord::RunStarted { manifest: Some(manifest), .. }
        if manifest.fetches.iter().any(|fetch| matches!(
            fetch.outcome,
            FetchOutcome::Answered { reference_slots: Some(_), .. }
        )))
    }

    /// Whether a tool this record freezes or queues declares its own reply deadline, which an
    /// older reader would drop while decoding the tool.
    fn carries_tool_reply(&self) -> bool {
        let declares = |tools: &[Tool]| tools.iter().any(|tool| tool.reply.is_some());
        match self {
            WalRecord::RunStarted { config, .. } => declares(&config.tools),
            WalRecord::Queued { request, .. } => request.tools.as_deref().is_some_and(declares),
            _ => false,
        }
    }

    /// Whether a credential selection this record freezes names its auth method. Its
    /// `credential_id` is then the vault's opaque id, which an older reader would parse for a
    /// method, so the record is must-understand.
    fn carries_auth_selection_method(&self) -> bool {
        let selection = match self {
            WalRecord::RunStarted { config, .. } => config.auth_selection.as_ref(),
            WalRecord::RunAuthRefrozen { auth_selection, .. } => Some(auth_selection),
            _ => None,
        };
        selection.is_some_and(|selection| selection.auth_method.is_some())
    }

    fn carries_media_content(&self) -> bool {
        use broca_protocol::{ContentBlock, ToolOutput};
        let result = |result: &ToolResult| matches!(result.output, ToolOutput::Parts { .. });
        let blocks = |content: &[ContentBlock]| {
            content.iter().any(|block| match block {
                ContentBlock::Media(_) => true,
                ContentBlock::ToolResult(value) => result(value),
                ContentBlock::Text { .. }
                | ContentBlock::TextWithState { .. }
                | ContentBlock::Reasoning { .. }
                | ContentBlock::RedactedReasoning { .. }
                | ContentBlock::ProviderFallback { .. }
                | ContentBlock::ToolCall(_) => false,
            })
        };
        let messages = |messages: &[Message]| messages.iter().any(|m| blocks(&m.content));
        let hooks = |hooks: &Option<Box<Hooks>>, subject| {
            hooks.as_ref().is_some_and(|hooks| {
                hooks
                    .original
                    .iter()
                    .chain(hooks.final_value.iter())
                    .any(|value| hook_subject_carries_media(value, subject))
            })
        };
        match self {
            Self::RunStarted {
                config,
                input,
                hooks: h,
                ..
            } => {
                config
                    .frozen_render_config
                    .get("media")
                    .is_some_and(|media| !media.is_null())
                    || messages(input)
                    || hooks(h, MediaHookSubject::Messages)
            }
            Self::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => frozen_render_config
                .get("media")
                .is_some_and(|media| !media.is_null()),
            Self::Steered {
                input, hooks: h, ..
            } => messages(input) || hooks(h, MediaHookSubject::Messages),
            Self::Queued {
                request, hooks: h, ..
            } => messages(&request.input) || hooks(h, MediaHookSubject::Messages),
            Self::ModelStepFinished {
                assistant_message,
                hooks: h,
                ..
            } => blocks(&assistant_message.content) || hooks(h, MediaHookSubject::Assistant),
            Self::ToolResult {
                result: value,
                hooks: h,
                reason,
                ..
            } => {
                // Intentless pre-tool refusals store arguments, not a post-tool result.
                let pre_tool = matches!(
                    reason.as_deref(),
                    Some("pre_tool_denied" | "pre_tool_unavailable")
                ) || h
                    .as_ref()
                    .is_some_and(|hooks| hooks.ops.iter().any(|op| op.hook == "pre_tool"));
                let subject = if pre_tool {
                    MediaHookSubject::ToolInput
                } else {
                    MediaHookSubject::Result
                };
                result(value) || hooks(h, subject)
            }
            Self::LateResult { result: value, .. } => result(value),
            Self::ApprovalRecorded { closing_result, .. } => {
                closing_result.as_deref().is_some_and(result)
            }
            Self::SendDelivered { input, .. } => {
                input.as_deref().is_some_and(|input| messages(input))
            }
            Self::ToolNoDispatch { hooks: h, .. } | Self::ToolDispatchIntent { hooks: h, .. } => {
                hooks(h, MediaHookSubject::ToolInput)
            }
            Self::CompactionMessage { replacement, .. } => messages(replacement),
            Self::CompactionViewApplied { view, .. } => view_carries_media_content(view),
            Self::ModelAttemptFinished { .. }
            | Self::AttemptReserved { .. }
            | Self::AttemptFinished { .. }
            | Self::AttemptReleased { .. }
            | Self::AttemptCorrected { .. }
            | Self::ModelStepRefused { .. }
            | Self::StepStarted { .. }
            | Self::ToolBatchStarted { .. }
            | Self::ToolBatchFinished { .. }
            | Self::TurnFinished { .. }
            | Self::BudgetFired { .. }
            | Self::InputTokenCapFrozen { .. }
            | Self::InputTokenBudgetExhausted { .. }
            | Self::ContextCeilingWait { .. }
            | Self::ContextCeilingDecided { .. }
            | Self::OutputReservationClamped { .. }
            | Self::StructuredOutputDowngraded { .. }
            | Self::StructuredOutputResult { .. }
            | Self::RunFinished { .. }
            | Self::RunCancelRequested { .. }
            | Self::RunPaused { .. }
            | Self::RunAuthRefrozen { .. }
            | Self::Retracted { .. }
            | Self::ApprovalRequested { .. }
            | Self::ApprovalResolved { .. }
            | Self::ResumeStarted { .. }
            | Self::CacheWarmed { .. }
            | Self::CompactionRequest { .. }
            | Self::CompactionAnswer { .. }
            | Self::CompactionWait { .. }
            | Self::CompactionRideWait { .. }
            | Self::CompactionRideWaitEnded { .. }
            | Self::CompactionNotice { .. }
            | Self::ToolCallDeferred { .. }
            | Self::AwaitingApproval { .. }
            | Self::LateApprovalRecorded { .. }
            | Self::LateDispatchIntent { .. }
            | Self::QuestionWithdrawalAcknowledged { .. }
            | Self::ToolCallProviderHeld { .. }
            | Self::DeliveryOrdered { .. }
            | Self::SendEnded { .. }
            | Self::WriterOpened { .. }
            | Self::WriterClosed { .. }
            | Self::VisibilityLate { .. }
            | Self::Unknown => false,
        }
    }

    fn carries_google_rpc_error_details(&self) -> bool {
        match self {
            WalRecord::RunFinished {
                error: Some(error), ..
            }
            | WalRecord::RunPaused {
                error: Some(error), ..
            } => error.provider_details.is_some(),
            WalRecord::Retracted {
                failure: Some(failure),
                ..
            } => failure.error.provider_details.is_some(),
            _ => false,
        }
    }

    /// Whether this record carries runner tags, or is a run start frozen to render them. A
    /// reader that skipped either would serve the session without its tags or render requests
    /// without the tags the run was admitted to show, so both are must-understand.
    fn carries_runner_tags(&self) -> bool {
        match self {
            WalRecord::RunStarted { tags, config, .. } => tags.is_some() || config.render_tags,
            WalRecord::Steered { tags, .. } | WalRecord::SendDelivered { tags, .. } => {
                tags.is_some()
            }
            WalRecord::ModelStepFinished { tags, .. }
            | WalRecord::ToolNoDispatch { tags, .. }
            | WalRecord::ToolResult { tags, .. }
            | WalRecord::BudgetFired { tags, .. }
            | WalRecord::ApprovalRecorded { tags, .. } => tags.is_some(),
            _ => false,
        }
    }

    /// The runner tags this record carries, one entry per message, in the order the record
    /// holds the messages. Empty for a record without tags.
    pub fn runner_tags(&self) -> &[MessageTags] {
        match self {
            WalRecord::RunStarted {
                tags: Some(tags), ..
            }
            | WalRecord::Steered {
                tags: Some(tags), ..
            }
            | WalRecord::SendDelivered {
                tags: Some(tags), ..
            } => tags.as_slice(),
            WalRecord::ModelStepFinished {
                tags: Some(tags), ..
            }
            | WalRecord::ToolNoDispatch {
                tags: Some(tags), ..
            }
            | WalRecord::ToolResult {
                tags: Some(tags), ..
            }
            | WalRecord::BudgetFired {
                tags: Some(tags), ..
            }
            | WalRecord::ApprovalRecorded {
                tags: Some(tags), ..
            } => std::slice::from_ref(tags.as_ref()),
            _ => &[],
        }
    }

    /// The OpenRouter format is a closed-enum addition that older content decoders reject.
    /// The frozen `reasoning_echo` policy selects OpenRouter's continuation representation and
    /// assistant-turn coverage. Gate that policy before any response arrives too, so rollback
    /// refuses by feature name instead of silently ignoring the run's echo requirements.
    fn carries_openrouter_reasoning(&self) -> bool {
        use broca_protocol::content::{ContentBlock, EncryptedReasoningFormat};
        let blocks = |content: &[ContentBlock]| {
            content.iter().any(|block| {
                matches!(
                    block,
                    ContentBlock::RedactedReasoning {
                        provider_format: Some(EncryptedReasoningFormat::OpenRouter),
                        ..
                    }
                )
            })
        };
        let messages = |input: &[Message]| input.iter().any(|message| blocks(&message.content));
        let frozen = |config: &Value| {
            config
                .get("reasoning_echo")
                .and_then(|policy| policy.get("format"))
                .and_then(Value::as_str)
                == Some("open_router_details")
        };
        match self {
            Self::RunStarted { config, input, .. } => {
                frozen(&config.frozen_render_config) || messages(input)
            }
            Self::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => frozen(frozen_render_config),
            Self::ModelStepFinished {
                assistant_message, ..
            } => blocks(&assistant_message.content),
            Self::Queued { request, .. } => messages(&request.input),
            Self::Steered { input, .. } => messages(input),
            Self::CompactionMessage { replacement, .. } => messages(replacement),
            _ => false,
        }
    }

    /// Whether this record carries Anthropic's server-side fallback: the `provider_fallback`
    /// content part in any message it holds, or a step's `served_model` or `usage_iterations`.
    fn carries_provider_fallback(&self) -> bool {
        let blocks = |content: &[broca_protocol::content::ContentBlock]| {
            content.iter().any(|block| {
                matches!(
                    block,
                    broca_protocol::content::ContentBlock::ProviderFallback { .. }
                )
            })
        };
        let messages = |input: &[Message]| input.iter().any(|message| blocks(&message.content));
        match self {
            Self::RunStarted { input, .. } => messages(input),
            Self::ModelStepFinished {
                assistant_message,
                fallback,
                ..
            } => blocks(&assistant_message.content) || fallback.carries_server_side_fallback(),
            Self::Queued { request, .. } => messages(&request.input),
            Self::Steered { input, .. } => messages(input),
            Self::SendDelivered { input, .. } => {
                input.as_deref().is_some_and(|input| messages(input))
            }
            Self::CompactionMessage { replacement, .. } => messages(replacement),
            _ => false,
        }
    }

    /// Whether this record carries client-side refusal-fallback state: a refused step, or a
    /// step's route, schedule or refusal-after-tool-use flag.
    fn carries_refusal_fallback(&self) -> bool {
        match self {
            Self::ModelStepRefused { .. } => true,
            Self::ModelStepFinished { fallback, .. } => fallback.carries_refusal_fallback(),
            _ => false,
        }
    }

    fn carries_provider_text_state(&self) -> bool {
        let blocks = |content: &[broca_protocol::content::ContentBlock]| {
            content.iter().any(|block| {
                matches!(
                    block,
                    broca_protocol::content::ContentBlock::TextWithState { .. }
                )
            })
        };
        let messages = |input: &[Message]| input.iter().any(|message| blocks(&message.content));
        let frozen = |config: &Value| {
            config.get("provider_text_state").and_then(Value::as_bool) == Some(true)
        };
        match self {
            Self::RunStarted { config, input, .. } => {
                frozen(&config.frozen_render_config) || messages(input)
            }
            Self::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => frozen(frozen_render_config),
            Self::ModelStepFinished {
                assistant_message, ..
            } => blocks(&assistant_message.content),
            Self::Queued { request, .. } => messages(&request.input),
            Self::Steered { input, .. } => messages(input),
            Self::CompactionMessage { replacement, .. } => messages(replacement),
            _ => false,
        }
    }

    /// Whether this record holds structured-output state a reader must understand: a run start
    /// or refreeze whose frozen render config carries the run's frozen schema, a queued send
    /// that asks for one, the result of such a run, or a call skipped because `submit_result`
    /// ended its run.
    fn carries_structured_output(&self) -> bool {
        let frozen = |config: Option<&Value>| {
            config.is_some_and(|config| config.get("structured_output").is_some())
        };
        match self {
            WalRecord::RunStarted { config, .. } => frozen(Some(&config.frozen_render_config)),
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => frozen(Some(frozen_render_config)),
            WalRecord::Queued { request, .. } => request.structured_output.is_some(),
            WalRecord::StructuredOutputResult { .. } => true,
            WalRecord::ToolResult {
                reason: Some(reason),
                ..
            } => reason == broca_protocol::structured_output::SKIPPED_AFTER_SUBMIT,
            _ => false,
        }
    }

    /// A frozen render config whose structured output starts its schema instructions on a new
    /// paragraph (`structured_output.instructions_paragraph_break: true`).
    fn carries_structured_output_paragraph_break(&self) -> bool {
        let frozen = |config: &Value| {
            config
                .pointer("/structured_output/instructions_paragraph_break")
                .and_then(Value::as_bool)
                == Some(true)
        };
        match self {
            WalRecord::RunStarted { config, .. } => frozen(&config.frozen_render_config),
            WalRecord::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => frozen(frozen_render_config),
            _ => false,
        }
    }

    fn base_required_features(&self) -> &'static [&'static str] {
        use crate::framing::{
            DEFERRED_CALL_FEATURE, DISPATCH_MODULE_FEATURE, FLOW_SCOPE_FEATURE,
            PLAN_MANIFEST_FEATURE, RESTART_PAUSE_FEATURE, RETENTION_FEATURE, SCOPE_FEATURE,
            STEER_QUEUE_FEATURE, STEP_TRANSFORMS_FEATURE,
        };
        match self {
            WalRecord::CompactionRideWait { .. } | WalRecord::CompactionRideWaitEnded { .. } => {
                &[crate::framing::COMPACTION_RIDE_WAIT_FEATURE]
            }
            WalRecord::ContextCeilingWait { .. }
            | WalRecord::ContextCeilingDecided { .. }
            | WalRecord::RunFinished {
                reason: RunFinishReason::ContextCeiling,
                ..
            } => &[crate::framing::CONTEXT_CEILING_FEATURE],
            WalRecord::StructuredOutputDowngraded { .. } => {
                &[crate::framing::STRUCTURED_OUTPUT_DOWNGRADE_FEATURE]
            }
            WalRecord::OutputReservationClamped { .. } => {
                &[crate::framing::OUTPUT_RESERVATION_FEATURE]
            }
            WalRecord::RunCancelRequested { .. } => &[crate::framing::RUN_CANCEL_AT_FEATURE],
            WalRecord::CompactionRequest { .. }
            | WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionAnswer { .. } => &[crate::framing::COMPACTION_FEATURE],
            WalRecord::CompactionWait { .. } => &[crate::framing::COMPACTION_WAIT_FEATURE],
            WalRecord::CompactionViewApplied { .. } | WalRecord::CompactionNotice { .. } => {
                &[crate::framing::COMPACTION_VIEW_FEATURE]
            }
            WalRecord::RunPaused {
                reason: PauseReason::Restart,
                ..
            } => &[RESTART_PAUSE_FEATURE],
            WalRecord::RunPaused {
                reason: PauseReason::ToolHeld,
                ..
            }
            | WalRecord::ToolCallProviderHeld { .. } => &[crate::framing::PROVIDER_HELD_FEATURE],
            WalRecord::RunStarted {
                scope,
                manifest,
                hooks,
                retention,
                ..
            } => {
                let flow = scope.as_ref().is_some_and(|scope| {
                    scope.flow_id.is_some()
                        || (scope.sender.is_some()
                            && scope.run_id.is_none()
                            && scope.agent_id.is_none())
                });
                let transforms = hooks.is_some()
                    || manifest
                        .as_ref()
                        .is_some_and(|manifest| !manifest.declarations.is_empty());
                match (
                    scope.is_some(),
                    flow,
                    manifest.is_some(),
                    transforms,
                    retention.is_some(),
                ) {
                    (false, false, false, false, false) => &[],
                    (false, false, false, true, false) => &[STEP_TRANSFORMS_FEATURE],
                    (false, false, true, false, false) => &[PLAN_MANIFEST_FEATURE],
                    (false, false, true, true, false) => {
                        &[PLAN_MANIFEST_FEATURE, STEP_TRANSFORMS_FEATURE]
                    }
                    (true, false, false, false, false) => &[SCOPE_FEATURE],
                    (true, false, false, true, false) => &[SCOPE_FEATURE, STEP_TRANSFORMS_FEATURE],
                    (true, false, true, false, false) => &[PLAN_MANIFEST_FEATURE, SCOPE_FEATURE],
                    (true, false, true, true, false) => &[
                        PLAN_MANIFEST_FEATURE,
                        SCOPE_FEATURE,
                        STEP_TRANSFORMS_FEATURE,
                    ],
                    (_, true, false, false, false) => &[FLOW_SCOPE_FEATURE, SCOPE_FEATURE],
                    (_, true, false, true, false) => {
                        &[FLOW_SCOPE_FEATURE, SCOPE_FEATURE, STEP_TRANSFORMS_FEATURE]
                    }
                    (_, true, true, false, false) => {
                        &[FLOW_SCOPE_FEATURE, PLAN_MANIFEST_FEATURE, SCOPE_FEATURE]
                    }
                    (_, true, true, true, false) => &[
                        FLOW_SCOPE_FEATURE,
                        PLAN_MANIFEST_FEATURE,
                        SCOPE_FEATURE,
                        STEP_TRANSFORMS_FEATURE,
                    ],
                    (false, false, false, false, true) => &[RETENTION_FEATURE],
                    (false, false, false, true, true) => {
                        &[RETENTION_FEATURE, STEP_TRANSFORMS_FEATURE]
                    }
                    (false, false, true, false, true) => {
                        &[PLAN_MANIFEST_FEATURE, RETENTION_FEATURE]
                    }
                    (false, false, true, true, true) => &[
                        PLAN_MANIFEST_FEATURE,
                        RETENTION_FEATURE,
                        STEP_TRANSFORMS_FEATURE,
                    ],
                    (true, false, false, false, true) => &[RETENTION_FEATURE, SCOPE_FEATURE],
                    (true, false, false, true, true) => {
                        &[RETENTION_FEATURE, SCOPE_FEATURE, STEP_TRANSFORMS_FEATURE]
                    }
                    (true, false, true, false, true) => {
                        &[PLAN_MANIFEST_FEATURE, RETENTION_FEATURE, SCOPE_FEATURE]
                    }
                    (true, false, true, true, true) => &[
                        PLAN_MANIFEST_FEATURE,
                        RETENTION_FEATURE,
                        SCOPE_FEATURE,
                        STEP_TRANSFORMS_FEATURE,
                    ],
                    (_, true, false, false, true) => {
                        &[FLOW_SCOPE_FEATURE, RETENTION_FEATURE, SCOPE_FEATURE]
                    }
                    (_, true, false, true, true) => &[
                        FLOW_SCOPE_FEATURE,
                        RETENTION_FEATURE,
                        SCOPE_FEATURE,
                        STEP_TRANSFORMS_FEATURE,
                    ],
                    (_, true, true, false, true) => &[
                        FLOW_SCOPE_FEATURE,
                        PLAN_MANIFEST_FEATURE,
                        RETENTION_FEATURE,
                        SCOPE_FEATURE,
                    ],
                    (_, true, true, true, true) => &[
                        FLOW_SCOPE_FEATURE,
                        PLAN_MANIFEST_FEATURE,
                        RETENTION_FEATURE,
                        SCOPE_FEATURE,
                        STEP_TRANSFORMS_FEATURE,
                    ],
                }
            }
            WalRecord::Steered {
                hooks,
                retention,
                ts_ms,
                ..
            } => match (retention.is_some() || ts_ms.is_some(), hooks.is_some()) {
                (false, false) => &[STEER_QUEUE_FEATURE],
                (false, true) => &[STEER_QUEUE_FEATURE, STEP_TRANSFORMS_FEATURE],
                (true, false) => &[RETENTION_FEATURE, STEER_QUEUE_FEATURE],
                (true, true) => &[
                    RETENTION_FEATURE,
                    STEER_QUEUE_FEATURE,
                    STEP_TRANSFORMS_FEATURE,
                ],
            },
            WalRecord::Queued {
                hooks,
                retention,
                ts_ms,
                ..
            } => match (retention.is_some() || ts_ms.is_some(), hooks.is_some()) {
                (false, false) => &[],
                (false, true) => &[STEP_TRANSFORMS_FEATURE],
                (true, false) => &[RETENTION_FEATURE],
                (true, true) => &[RETENTION_FEATURE, STEP_TRANSFORMS_FEATURE],
            },
            WalRecord::ToolDispatchIntent {
                module_id,
                schema_pin,
                hooks,
                approval_digest,
                call_key,
                ..
            } => match (
                approval_digest.is_some() || call_key.is_some(),
                module_id.is_some() || schema_pin.is_some(),
                hooks.is_some(),
            ) {
                // (deferred call, dispatch module, step transforms); each list is in the sorted
                // order the framing writes `requires` in.
                (false, false, false) => &[],
                (false, false, true) => &[STEP_TRANSFORMS_FEATURE],
                (false, true, false) => &[DISPATCH_MODULE_FEATURE],
                (false, true, true) => &[DISPATCH_MODULE_FEATURE, STEP_TRANSFORMS_FEATURE],
                (true, false, false) => &[DEFERRED_CALL_FEATURE],
                (true, false, true) => &[DEFERRED_CALL_FEATURE, STEP_TRANSFORMS_FEATURE],
                (true, true, false) => &[DEFERRED_CALL_FEATURE, DISPATCH_MODULE_FEATURE],
                (true, true, true) => &[
                    DEFERRED_CALL_FEATURE,
                    DISPATCH_MODULE_FEATURE,
                    STEP_TRANSFORMS_FEATURE,
                ],
            },
            WalRecord::ToolResult {
                hooks,
                withdraw_elicitation,
                provider_held,
                ..
            } if withdraw_elicitation.is_some() || provider_held.is_some() => {
                if hooks.is_some() {
                    &[DEFERRED_CALL_FEATURE, STEP_TRANSFORMS_FEATURE]
                } else {
                    &[DEFERRED_CALL_FEATURE]
                }
            }
            WalRecord::ModelStepFinished { hooks: Some(_), .. }
            | WalRecord::ToolNoDispatch { hooks: Some(_), .. }
            | WalRecord::ToolResult { hooks: Some(_), .. } => &[STEP_TRANSFORMS_FEATURE],
            WalRecord::ToolCallDeferred { .. }
            | WalRecord::ApprovalRecorded { .. }
            | WalRecord::AwaitingApproval { .. }
            | WalRecord::LateApprovalRecorded { .. }
            | WalRecord::LateDispatchIntent { .. }
            | WalRecord::LateResult { .. } => &[DEFERRED_CALL_FEATURE],
            WalRecord::DeliveryOrdered { .. }
            | WalRecord::SendEnded { .. }
            | WalRecord::SendDelivered { .. } => &[crate::framing::DELIVERY_ORDER_FEATURE],
            // Informational: an older build may skip it, so it needs no feature.
            WalRecord::QuestionWithdrawalAcknowledged { .. } => &[],
            _ => &[],
        }
    }
}

#[derive(Clone, Copy)]
enum MediaHookSubject {
    Messages,
    Assistant,
    Result,
    ToolInput,
}

/// Decode the phase's known subject shape once, borrowing its fields. Tool input is opaque:
/// a media-looking object in arguments is not native content and must not raise the floor.
fn hook_subject_carries_media(value: &Value, subject: MediaHookSubject) -> bool {
    match subject {
        MediaHookSubject::Messages => Vec::<MediaMessage>::deserialize(value)
            .is_ok_and(|messages| messages.iter().any(MediaMessage::carries_media)),
        MediaHookSubject::Assistant => {
            MediaMessage::deserialize(value).is_ok_and(|message| message.carries_media())
        }
        MediaHookSubject::Result => {
            MediaToolResult::deserialize(value).is_ok_and(|result| result.output.kind == "parts")
        }
        MediaHookSubject::ToolInput => false,
    }
}

// These borrowed projections read only native grammar discriminators. Unrelated fields,
// notably tool-call input and JSON result values, are ignored without copying their trees.
#[derive(Deserialize)]
struct MediaMessage<'a> {
    #[serde(borrow)]
    content: Vec<MediaBlock<'a>>,
}

impl MediaMessage<'_> {
    fn carries_media(&self) -> bool {
        self.content.iter().any(MediaBlock::carries_media)
    }
}

#[derive(Deserialize)]
struct MediaBlock<'a> {
    #[serde(rename = "type", borrow)]
    kind: std::borrow::Cow<'a, str>,
    #[serde(default, borrow)]
    output: Option<MediaOutput<'a>>,
}

impl MediaBlock<'_> {
    fn carries_media(&self) -> bool {
        self.kind == "media"
            || (self.kind == "tool_result"
                && self
                    .output
                    .as_ref()
                    .is_some_and(|output| output.kind == "parts"))
    }
}

#[derive(Deserialize)]
struct MediaToolResult<'a> {
    #[serde(borrow)]
    output: MediaOutput<'a>,
}

#[derive(Deserialize)]
struct MediaOutput<'a> {
    #[serde(borrow)]
    kind: std::borrow::Cow<'a, str>,
}

#[derive(Deserialize)]
struct MediaView<'a> {
    #[serde(borrow)]
    ranges: Vec<MediaViewRange<'a>>,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum MediaViewRange<'a> {
    Keep {
        #[serde(default, borrow)]
        edits: Vec<MediaViewEdit<'a>>,
    },
    Replace {
        #[serde(borrow)]
        bytes: std::borrow::Cow<'a, str>,
    },
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct MediaViewEdit<'a> {
    #[serde(default, borrow)]
    bytes: Option<std::borrow::Cow<'a, str>>,
}

fn view_carries_media_content(view: &OpaqueJson) -> bool {
    serde_json::from_str::<MediaView>(view.as_json_text()).is_ok_and(|view| {
        view.ranges.iter().any(|range| match range {
            MediaViewRange::Keep { edits } => edits.iter().any(|edit| {
                edit.bytes.as_ref().is_some_and(|bytes| {
                    serde_json::from_str::<MediaBlock>(bytes)
                        .is_ok_and(|block| block.carries_media())
                })
            }),
            MediaViewRange::Replace { bytes } => serde_json::from_str::<Vec<MediaMessage>>(bytes)
                .is_ok_and(|messages| messages.iter().any(MediaMessage::carries_media)),
            MediaViewRange::Other => false,
        })
    })
}

/// Cancellation points recorded in the WAL. The run.cancel endpoint rejects all other strings
/// before writing, rather than interpreting an unknown point as immediate cancellation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunCancelAt {
    Now,
    StepBoundary,
}

/// Why a queued send ended undelivered ([`WalRecord::SendEnded`]).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SendEndReason {
    /// A newer send from the same sender with the same `dedupe_key` replaced it.
    Superseded { by: SubmissionId },
    /// Its `expires_at_ms` passed before any request boundary could deliver it.
    Expired,
}

impl SendEndReason {
    /// The outcome name a caller sees for the send: `superseded` or `expired`.
    pub fn as_str(&self) -> &'static str {
        match self {
            SendEndReason::Superseded { .. } => "superseded",
            SendEndReason::Expired => "expired",
        }
    }
}

// `WalRecord::required_features` adds the agent-run, session-class and input-cap features
// through precomputed tables, so the common records find their sorted feature list without
// taking a lock or allocating. All three tables are generated from one list, and a name a
// table does not list falls back to the cached sorted union `with_feature` computes, so a
// feature added later can make a lookup slower but can never make it panic or answer wrongly.

/// Every feature name `base_required_features` returns for a record that reaches one of the
/// tables below, sorted: `RunStarted` (all three tables), `ToolCallDeferred` (agent-run) and
/// `Queued`, `ToolResult`, `RunFinished`, `InputTokenCapFrozen` and
/// `InputTokenBudgetExhausted` (input cap). Each later table also lists the features the
/// earlier tables add. `required_features` adds every other feature after the tables, so none
/// of them needs listing here.
const UNION_TABLE_BASE_FEATURES: [&str; 6] = [
    crate::framing::DEFERRED_CALL_FEATURE,
    crate::framing::FLOW_SCOPE_FEATURE,
    crate::framing::PLAN_MANIFEST_FEATURE,
    crate::framing::RETENTION_FEATURE,
    crate::framing::SCOPE_FEATURE,
    crate::framing::STEP_TRANSFORMS_FEATURE,
];
/// What a classed `RunStarted` can carry when it reaches the session-class table: the shared
/// list plus the agent-run feature that table runs after.
const CLASS_BASE_FEATURES: [&str; 7] = insert_sorted(
    UNION_TABLE_BASE_FEATURES,
    crate::framing::AGENT_RUN_SCOPE_FEATURE,
);
/// What a capped record can carry when it reaches the input-cap table: the session-class
/// table's list plus the session-class feature.
const INPUT_CAP_BASE_FEATURES: [&str; 8] =
    insert_sorted(CLASS_BASE_FEATURES, crate::framing::SESSION_CLASS_FEATURE);

static AGENT_RUN_FEATURE_SETS: FeatureUnionTable<6, 7, 64> = FeatureUnionTable::new(
    UNION_TABLE_BASE_FEATURES,
    crate::framing::AGENT_RUN_SCOPE_FEATURE,
);
static CLASS_FEATURE_SETS: FeatureUnionTable<7, 8, 128> =
    FeatureUnionTable::new(CLASS_BASE_FEATURES, crate::framing::SESSION_CLASS_FEATURE);
static INPUT_CAP_FEATURE_SETS: FeatureUnionTable<8, 9, 256> = FeatureUnionTable::new(
    INPUT_CAP_BASE_FEATURES,
    crate::framing::INPUT_TOKEN_CAP_FEATURE,
);

fn agent_run_features(base: &'static [&'static str]) -> &'static [&'static str] {
    AGENT_RUN_FEATURE_SETS.lookup(base)
}

fn session_class_features(base: &'static [&'static str]) -> &'static [&'static str] {
    CLASS_FEATURE_SETS.lookup(base)
}

fn input_cap_features(base: &'static [&'static str]) -> &'static [&'static str] {
    INPUT_CAP_FEATURE_SETS.lookup(base)
}

/// One sorted feature list of a [`FeatureUnionTable`]; only the first `len` names are used.
#[derive(Clone, Copy)]
struct FeatureUnion<const W: usize> {
    names: [&'static str; W],
    len: usize,
}

/// `added` united with every subset of the `N` sorted names in `listed`, indexed by the subset's
/// bitmask (bit `i` set means `listed[i]` is present). `W` is `N + 1` and `S` is `2^N`; stable
/// Rust cannot compute either from `N`, so [`FeatureUnionTable::new`] checks them at compile
/// time.
struct FeatureUnionTable<const N: usize, const W: usize, const S: usize> {
    listed: [&'static str; N],
    added: &'static str,
    sets: [FeatureUnion<W>; S],
}

impl<const N: usize, const W: usize, const S: usize> FeatureUnionTable<N, W, S> {
    const fn new(listed: [&'static str; N], added: &'static str) -> Self {
        assert!(
            W == N + 1 && S == 1 << N,
            "a table needs N + 1 names and 2^N sets"
        );
        let mut i = 1;
        while i < N {
            assert!(
                str_lt(listed[i - 1], listed[i]),
                "listed names must be sorted"
            );
            i += 1;
        }
        let mut i = 0;
        while i < N {
            assert!(
                str_lt(listed[i], added) || str_lt(added, listed[i]),
                "the added name cannot also be listed"
            );
            i += 1;
        }
        let mut sets = [FeatureUnion {
            names: [""; W],
            len: 0,
        }; S];
        let mut mask = 0;
        while mask < S {
            let mut len = 0;
            let mut placed = false;
            let mut bit = 0;
            while bit < N {
                // `added` goes before the first listed name that sorts after it, so every set
                // comes out sorted whichever listed names it holds.
                if !placed && str_lt(added, listed[bit]) {
                    sets[mask].names[len] = added;
                    len += 1;
                    placed = true;
                }
                if mask & (1 << bit) != 0 {
                    sets[mask].names[len] = listed[bit];
                    len += 1;
                }
                bit += 1;
            }
            if !placed {
                sets[mask].names[len] = added;
                len += 1;
            }
            sets[mask].len = len;
            mask += 1;
        }
        Self {
            listed,
            added,
            sets,
        }
    }

    /// The sorted union of `base` (itself sorted and duplicate-free, as every list
    /// `required_features` builds is) and this table's added name.
    ///
    /// A name this table does not list is answered by `with_feature`, which builds that union
    /// once and caches it, instead of panicking: a must-understand feature added after this
    /// table was written must still let the record be framed. The coverage test in
    /// `feature_union_tests` fails when one of its records brings an unlisted name here, so
    /// the slower path is reported rather than left in place silently.
    fn lookup(&'static self, base: &'static [&'static str]) -> &'static [&'static str] {
        #[cfg(test)]
        feature_union_tests::note_lookup(self.added, base);
        let mut mask = 0;
        for name in base {
            let Some(bit) = self.listed.iter().position(|known| known == name) else {
                return with_feature(base, self.added);
            };
            mask |= 1 << bit;
        }
        let set = &self.sets[mask];
        &set.names[..set.len]
    }
}

/// `a < b` in the byte order `str`'s `Ord` uses, for sorting in a `const` context.
const fn str_lt(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    let mut i = 0;
    while i < a.len() && i < b.len() {
        if a[i] != b[i] {
            return a[i] < b[i];
        }
        i += 1;
    }
    a.len() < b.len()
}

/// `list` with `name` inserted in sorted position; `M` must be `N + 1`.
const fn insert_sorted<const N: usize, const M: usize>(
    list: [&'static str; N],
    name: &'static str,
) -> [&'static str; M] {
    assert!(M == N + 1, "inserting one name needs one more slot");
    let mut out = [""; M];
    let mut from = 0;
    let mut to = 0;
    let mut placed = false;
    while to < M {
        if !placed && (from == N || str_lt(name, list[from])) {
            out[to] = name;
            placed = true;
        } else {
            out[to] = list[from];
            from += 1;
        }
        to += 1;
    }
    out
}

/// Whether a stored request identity digest (a JSON object of per-field digests written by
/// broca-core) carries the send's `client_message_id`. A digest that does not parse carries
/// none: it is compared as an opaque string, and this build never writes such a digest with
/// the member.
fn digest_carries_client_message_id(digest: &str) -> bool {
    #[derive(Deserialize)]
    struct Probe {
        #[serde(default)]
        client_message_id: Option<serde::de::IgnoredAny>,
    }
    serde_json::from_str::<Probe>(digest).is_ok_and(|probe| probe.client_message_id.is_some())
}

/// `base` with `feature` added in sorted position. The feature sets a record can require are a
/// small fixed family, so each distinct union is built once and kept for the process lifetime;
/// the cache is keyed by the address of `base`, which is always a static slice.
fn with_feature(base: &'static [&'static str], feature: &'static str) -> &'static [&'static str] {
    use std::collections::HashMap;
    use std::sync::{Mutex, OnceLock};
    type Unions = HashMap<(usize, usize, &'static str), &'static [&'static str]>;
    static UNIONS: OnceLock<Mutex<Unions>> = OnceLock::new();
    if base.contains(&feature) {
        return base;
    }
    let key = (base.as_ptr() as usize, base.len(), feature);
    let mut unions = UNIONS
        .get_or_init(Default::default)
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    unions.entry(key).or_insert_with(|| {
        let mut names = base.to_vec();
        let at = names.partition_point(|name| *name < feature);
        names.insert(at, feature);
        Box::leak(names.into_boxed_slice())
    })
}

/// The schema a tool call was dispatched against: the tool's `schema_digest` and `semantics`
/// version as its provider answered them when the session's tools were frozen. A provider that
/// has since changed the tool refuses the call instead of running it against a different schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchSchemaPin {
    /// The provider's digest of the tool's structural schema.
    pub schema_digest: String,
    /// The provider's version of the tool's behaviour where its schema did not change.
    pub semantics: u64,
}

/// Who executes a held call once a human approves it ([`WalRecord::ToolCallDeferred`]).
///
/// Serialized as a bare string. Only `broca` is written today; other values are reserved for
/// calls a tool provider executes itself, which share the `deferred-call/v1` feature. So a value
/// this build does not know still decodes, is KEPT verbatim and re-serializes to the string it
/// was read from, instead of failing the WAL as corrupt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeferredExecutor {
    /// broca re-validates the saved arguments and dispatches them itself.
    Broca,
    /// A value this build does not know. Holds the string exactly as stored; never written by
    /// this build.
    Other(String),
}

impl DeferredExecutor {
    /// The wire spelling.
    pub fn as_str(&self) -> &str {
        match self {
            DeferredExecutor::Broca => "broca",
            DeferredExecutor::Other(name) => name,
        }
    }
}

impl Serialize for DeferredExecutor {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for DeferredExecutor {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(match name.as_str() {
            "broca" => DeferredExecutor::Broca,
            _ => DeferredExecutor::Other(name),
        })
    }
}

/// The question an approve hook asked about a held call, as the WAL keeps it: the step-transform
/// role's `ApprovalAsk`, field for field and in its wire order. Owned here rather than borrowed
/// from the role crate so a change to that crate can never change the bytes of a record already
/// on disk.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DeferredAsk {
    /// The question shown to the human.
    pub prompt: String,
    /// The answers offered beyond allow and decline. Omitted when empty, as the role omits it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub options: Vec<String>,
    /// When the question expires: absolute Unix milliseconds, not a duration.
    pub expires_at_ms: u64,
    /// What happens when it expires unanswered.
    pub on_expiry: OnExpiry,
    /// Whether the call can cause damage that cannot be undone.
    pub material_damage: bool,
    /// What an approval read after the call expired does.
    pub late_execution: LateExecution,
}

/// What happens when a held call's question expires unanswered. Closed: the role defines no
/// other value, and a policy this build does not understand must not be read as one it does.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnExpiry {
    /// The call is closed `ApprovalRecorded {expired}` with `pre_tool_expired`.
    Deny,
}

/// What an approval read after its call expired does. Closed, like [`OnExpiry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateExecution {
    /// The call executes at the session's next admission.
    Execute,
    /// Nothing executes; the late answer is only recorded.
    NotifyOnly,
}

/// The decision an [`WalRecord::ApprovalRecorded`] records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeferredDecision {
    /// The human approved; the call may be validated again and dispatched.
    Approved,
    /// The human declined (`pre_tool_declined`).
    Declined,
    /// The question expired unanswered (`pre_tool_expired`).
    Expired,
    /// The question could not be filed (`pre_tool_unavailable`).
    Unavailable,
}

/// One held call in an [`WalRecord::AwaitingApproval`] marker.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AwaitingApprovalEntry {
    pub call_key: String,
    pub call_id: ToolCallId,
    /// Absent until the filing has returned the question's id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elicitation_id: Option<String>,
    pub expires_at_ms: u64,
}

/// The decision a [`WalRecord::LateApprovalRecorded`] records.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateDecision {
    Approved,
    Declined,
    /// The question became terminal with no answer.
    None,
}

/// How a late approved call ended ([`WalRecord::LateResult`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LateOutcome {
    /// The call ran and its result came back.
    Completed,
    /// The call was never sent: a late validation refused it, or the tool plane reported it
    /// not sent.
    NotSent,
    /// The call may have run and its result is lost (a timeout, or an intent found without a
    /// result after a restart).
    Unknown,
}

/// How a question withdrawal ended ([`WalRecord::QuestionWithdrawalAcknowledged`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WithdrawalAnswer {
    /// The question was withdrawn.
    Acknowledged,
    /// Nothing was withdrawn because the question was no longer open, the scope refused for
    /// good, or it expired; `reason` says which.
    NotSent,
}

/// The question a [`WalRecord::ToolResult`] closing a held call obliges broca to withdraw.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WithdrawElicitation {
    pub call_key: String,
    /// Absent when the filing had not returned (including a filing the provider accepted whose
    /// response was lost); the withdrawal then re-files the same key to recover it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elicitation_id: Option<String>,
}

/// A call a tool provider holds past its reply ([`WalRecord::ToolResult`]).
///
/// Superseded by [`WalRecord::ToolCallProviderHeld`] and never written; the reader still
/// decodes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProviderHeld {
    pub call_key: String,
    /// The module the call was dispatched to, which a withdrawal is sent to.
    pub module: String,
    /// Present for a `decision_pending` result; absent while a blocking ask is in flight.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub elicitation_id: Option<String>,
}

/// Where an episode's conversation originated. `SeededFrom` is shared by imports
/// and lineage forks; optional fork metadata is reserved structurally so future writers
/// never need to encode it in `source_ref`.
#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RunOrigin {
    #[default]
    Fresh,
    SeededFrom {
        #[serde(default)]
        origin_kind: SeedOriginKind,
        #[serde(default)]
        source_ref: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        origin_device_pseudonym: Option<[u8; 16]>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        base_seq: Option<u64>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SeedOriginKind {
    #[default]
    Import,
    Fork,
}

/// The authoritative request config, frozen at run start. On resume this wins over
/// whatever the caller passes, so the rendered request bytes are byte-identical
/// across a run and its resume.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunConfig {
    /// Opts a run into the role crate's stable wake-directive grammar. Replay always uses
    /// that grammar with `parse_trailing`; a grammar change needs a new versioned field/name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub turn_directives_v1: Option<Vec<broca_protocol::request::DirectiveName>>,
    pub model: ModelHandle,
    /// Tools WITH ORDER preserved (a reorder would change the rendered bytes).
    pub tools: Arc<Vec<Tool>>,
    pub tool_choice: ToolChoice,
    pub generation: GenerationConfig,
    pub response_format: ResponseFormat,
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub provider_options: Value,
    /// Cache-breakpoint tier markers. Opaque until the cache-tier work lands.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub cache_tiers: Value,
    /// Pinned at run start; bumped only when the context is rewritten.
    pub context_epoch: u64,
    /// The deterministic provider-request renderer version — a renderer change
    /// means a resume can't reproduce the original bytes.
    pub renderer_version: u32,
    /// The provider's frozen render config, stored as an OPAQUE blob. The WAL and
    /// the loop never interpret it — only the provider deserializes it on resume to
    /// rebuild the exact renderer that produced this run's request bytes. Kept
    /// opaque so the durability core stays provider-agnostic; it only needs to
    /// round-trip losslessly (the cache hash is over the rendered bytes, not this
    /// blob's serialization). Null on a run with no provider framework wired.
    #[serde(default, skip_serializing_if = "Value::is_null")]
    pub frozen_render_config: Value,
    /// The auth-selection resolved at run start, frozen so a resume re-uses the SAME
    /// credential + method instead of re-resolving from config/router (which could have
    /// drifted between run start and resume → a different method → a different wire family
    /// → different rendered bytes → a cache bust and a provider-invalid history). This is
    /// NOT a render input: the loop never feeds it to the renderer, and the wire family it
    /// implies is already captured in `frozen_render_config`. `None` on a run with no vault
    /// token (for example, the mock path), so a pre-existing record replays unchanged.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_selection: Option<AuthSelection>,
    /// Whether the caller omitted the tool surface that resolved to `tools`. This is admission
    /// identity metadata, not a render input; it distinguishes an inherited surface from an
    /// explicitly re-sent identical surface when validating `send_id` retries.
    #[serde(default, skip_serializing_if = "is_false")]
    pub tools_were_omitted: bool,
    /// Caller-supplied tool-call budget, frozen at admission for deterministic resume.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub budget: Option<BudgetConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_limit: Option<u64>,
    /// Opaque caller-declared workload class, frozen at admission and surfaced on usage export.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub work_class: Option<String>,
    /// The reasoning variant the caller requested (a models.dev variant name), recorded so the
    /// run shows what was asked for. Absent when the send carried none. Until the catalog
    /// supplies per-model reasoning options this is recorded only, never rendered.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub variant: Option<String>,
    /// The service tier the caller requested, as admitted: checked against what the model's
    /// provider family and catalog offer, then frozen here and in the render config. A resume
    /// keeps this value and never takes the resuming send's. Absent when the send carried none,
    /// which serializes to no bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub service_tier: Option<broca_protocol::request::ServiceTier>,
    /// Runner-side stop predicates, frozen so automatic crash recovery preserves the admitted
    /// execution boundary. Older records omit this additive field and therefore retain the
    /// historical empty-policy behavior, which the loop maps to its default safety cap.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub stop_when: Vec<StopCondition>,
    /// The session's context management, frozen when the episode was admitted. Resume renders
    /// from this value, never from process configuration. `None` only on a record written
    /// before the field existed; such a session followed the process-wide transform setting.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_management: Option<ContextManagement>,
    /// Whether this run shows runner tags to the model, frozen at admission from the plan's
    /// `render_tags`. With it on, every tagged user text block and tool-result text output is
    /// rendered with its `§N§ ` prefix; stored records never change. Omitted when false, which
    /// renders exactly as before the field existed. Must-understand when true (feature
    /// `runner-tags/v1`): a reader that ignored it would render different request bytes.
    #[serde(default, skip_serializing_if = "is_false")]
    pub render_tags: bool,
    /// Present when the run was frozen under the computed tag rules (feature `runner-tags/v2`):
    /// no record of the run stores a tag number; numbers are computed from the transcript, and
    /// only when `render_tags` is on. Absent on a run frozen under the stored-ledger rules
    /// (`runner-tags/v1`), whose records keep stamping a number per part for the rest of the
    /// run. Must-understand when present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub computed_tags: Option<ComputedTagRules>,
}

/// A run's frozen computed tag rules (`runner-tags/v2`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComputedTagRules {
    /// The highest number the session had shown, or handed out under the stored ledger, when
    /// the run was admitted: the run's first counted part is numbered after it. On the first
    /// run under these rules this is the stored ledger's high-water, so no number a reader
    /// has seen is reused or changed.
    pub checkpoint: u64,
}

/// A compaction provider's Setup checkpoint (llm-runner `tags::SetupCheckpoint`), recorded with
/// the Setup answer it came on: the message `cut` (`m<ordinal>`) and every later one are
/// numbered after `tag_checkpoint`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupTagCheckpoint {
    pub tag_checkpoint: u64,
    pub cut: String,
}

/// The credential a run resolved at start, frozen for resume. A struct (not a bare string)
/// so a later frozen field (e.g. a display name) has a home without another `RunConfig`
/// field. Carries NO secret — the credential id is an opaque routing key; the bearer handle
/// it maps to lives only in the 0600 handle map, never in the WAL.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuthSelection {
    /// The credential id to re-`get` on resume. Without [`AuthSelection::auth_method`] it is a
    /// broca key (`<method>:<provider>[:<account>]`) whose method, provider and account parse
    /// back out of it; with it, it is the vault's opaque credential id (see that field).
    pub credential_id: String,
    /// The vault-served account id (a provider-side account UUID) the credential resolved to
    /// at admission, frozen verbatim so usage export can label spend with the SAME account the
    /// run authenticated with — never re-derived from the credential id's components and never
    /// the JWT-claim account. `None` when the run has no vault identity that carries an account
    /// (the OpenCode/mock path, or a vault record without an account id), so a pre-existing
    /// record replays unchanged and the exported fact stays unlabeled.
    ///
    /// # It is EMPTY FOR WHOLE AUTH METHODS, not merely sometimes absent
    ///
    /// The vault parses this from a token, and an api key is not a token with an account in it.
    /// So `apikey` and `antigravity` credentials carry NO account here BY CONSTRUCTION — not as a
    /// gap, and not something a better handle file would fix. Measured over production export
    /// facts on 2026-09-11:
    ///
    /// ```text
    /// chatgpt       18,729 facts    14,806 with account_ref
    /// apikey         6,082 facts         0
    /// oauth          4,875 facts     2,220
    /// antigravity    4,441 facts         0
    /// ```
    ///
    /// A CONSUMER KEYING SPEND BY `account_ref` SILENTLY EXCLUDES TWO AUTH METHODS. That is the
    /// expensive shape, because it looks like the runs never happened rather than like a field
    /// that does not apply — a peer built a read-back query on exactly this and would have
    /// concluded their credential pin never reached broca.
    ///
    /// Key on `credential_id` instead: it is always present, it is the caller's pin verbatim, and
    /// on a selection without `auth_method` method, provider and account parse back out of it
    /// (with `auth_method`, it is the vault's opaque id and the method is that field). Use
    /// `account_ref` only when you
    /// specifically need the provider-side account UUID, and expect it on chatgpt and oauth only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    /// The id the vault resolved the handle to, frozen verbatim at admission (or explicit
    /// auth refreeze), NEVER re-derived from the routing key or overwritten on ordinary resume.
    /// A different id is legitimate many-to-one routing, not an authentication failure.
    /// None means no vault witness is available, including OpenCode/mock and older records.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resolved_credential_id: Option<String>,
    /// The parameters broca sent on the vault `credential.get` that resolved this selection,
    /// recorded from the call itself so the vault's own record of that request (served,
    /// refreshed, refused) can be joined to what broca ASKED for. Recorded per run rather than
    /// read from today's build: a later change to broca's defaults must not silently rewrite
    /// what an older run asked.
    ///
    /// PRESENT IFF THE VAULT'S REPLY WAS RECEIVED -- a token, or any answer the vault itself
    /// sent (a refusal, a locked vault, a body broca could not decode), since each means the
    /// vault received the request and has its own record to join against.
    ///
    /// None means one of: broca made no vault request for this run (the OpenCode and mock
    /// paths); no reply from the vault arrived (a route open the daemon refused or that
    /// failed, a daemon rejection of the request before delivery, a transport failure, a bad
    /// frame on the connection -- in these broca either knows the vault never saw the request
    /// or cannot tell, so it claims nothing, even when a cached token was then served); or the
    /// record predates this field. It is never synthesised for a request the vault may not
    /// have seen.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault_request: Option<VaultRequest>,
    /// How this run's provider account was chosen: by the routing module (which chooses among
    /// a provider's subscription accounts), from the caller's pin, or statically because routing
    /// was switched off, unreachable or bypassed. Frozen with
    /// the rest of the selection so a resume neither asks again nor changes the answer. Absent on
    /// records written before broca asked routing, and on runs with no vault account to choose
    /// (the OpenCode and mock paths), so those keep their bytes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_decision: Option<AccountDecision>,
    /// How the credential authenticates (`oauth`, `chatgpt`, `apikey`, `antigravity`), frozen
    /// at admission from the vault's own metadata for a credential routing chose.
    ///
    /// PRESENT CHANGES WHAT `credential_id` IS. With it, `credential_id` is the vault's own
    /// credential id, an opaque record name: a resume reads that credential verbatim and takes
    /// the method from here, never from the id's spelling and never through a
    /// `vault.credential_ids` alias. Without it (every record written before this field, and
    /// every selection broca resolved from its own configuration) `credential_id` is a broca
    /// key `<method>:<provider>[:<account>]`, read as before.
    ///
    /// Kept as the written string so a method a later build adds still decodes. A record
    /// carrying it requires the must-understand feature `auth-selection-method/v1`: an older
    /// build would parse the id for its method, so it must refuse such a log instead.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth_method: Option<String>,
}

/// Which path decided a run's provider account, with the admission and decision ids routing
/// knows it by.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccountDecision {
    /// The routing affinity frozen at admission, retained so later episodes can inherit it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_key: Option<String>,
    /// Whether routing affinity came from the caller, an earlier episode, or the session identity.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conversation_key_source: Option<ConversationKeySource>,
    /// The path, as one of the `ACCOUNT_DECISION_*` strings below. Kept as the written string
    /// rather than an enum so
    /// a record from a later build that names a path this build does not know still decodes and
    /// re-serializes to its stored bytes.
    pub path: String,
    /// The id broca asked routing about this run under, and names it by in its account reports
    /// to routing. Absent when broca reports nothing for the run (the `legacy_static` path older
    /// builds wrote).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admission_id: Option<String>,
    /// Routing's id for the decision that chose the account, when routing chose it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub decision_id: Option<String>,
}

/// Origin of an admission's routing affinity. Unknown strings survive WAL replay.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(from = "String", into = "String")]
pub enum ConversationKeySource {
    Caller,
    Session,
    Inherited,
    Other(String),
}

impl From<String> for ConversationKeySource {
    fn from(value: String) -> Self {
        match value.as_str() {
            "caller" => Self::Caller,
            "session" => Self::Session,
            "inherited" => Self::Inherited,
            _ => Self::Other(value),
        }
    }
}

impl From<ConversationKeySource> for String {
    fn from(value: ConversationKeySource) -> Self {
        match value {
            ConversationKeySource::Caller => "caller".into(),
            ConversationKeySource::Session => "session".into(),
            ConversationKeySource::Inherited => "inherited".into(),
            ConversationKeySource::Other(value) => value,
        }
    }
}

/// Routing chose the account.
pub const ACCOUNT_DECISION_ROUTED: &str = "routed";
/// Routing answered that account selection is switched off; the static pick was used.
pub const ACCOUNT_DECISION_DISABLED: &str = "disabled";
/// Routing gave no answer in time (route refused, timeout, malformed reply); the static pick
/// was used. Written only by builds that admitted such a send: a send routing cannot answer
/// is now refused before anything is written. Kept so those older runs still read and resume.
pub const ACCOUNT_DECISION_UNREACHABLE: &str = "unreachable";
/// Routing does not select accounts for this provider; the static pick was used.
pub const ACCOUNT_DECISION_UNSUPPORTED: &str = "unsupported";
/// The caller named the account; routing was not asked.
pub const ACCOUNT_DECISION_PINNED: &str = "pinned";
/// The provider had at most one candidate account, so there was nothing to ask.
pub const ACCOUNT_DECISION_SINGLE_CANDIDATE: &str = "single_candidate";
/// The operator's former `account_selection.legacy_static` setting in `broca.jsonc`, an
/// emergency switch, bypassed routing entirely. The setting no longer exists, so this path is
/// no longer written; it is kept so runs recorded under it still read and resume.
pub const ACCOUNT_DECISION_LEGACY_STATIC: &str = "legacy_static";
/// broca had no vault listing to ask routing with in time; the static pick was used.
pub const ACCOUNT_DECISION_CANDIDATES_UNKNOWN: &str = "candidates_unknown";
/// Routing answered that every remaining candidate needs a fresh login; the run admits and
/// pauses for re-authentication on the static pick.
pub const ACCOUNT_DECISION_AUTH_REQUIRED: &str = "auth_required";

/// What broca asked the credential vault for on one credential read: which operation it sent
/// and with which parameters. It records what was SENT, so a parameter the operation does not
/// carry is absent rather than defaulted.
///
/// Not `Copy`: [`VaultReadOp::Unknown`] keeps the operation name it could not recognise.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VaultRequest {
    /// The minimum remaining validity demanded of the served token; the vault refreshes an
    /// OAuth credential that would expire sooner. None means no floor was requested. Always
    /// serialized, as `null` when no floor was requested.
    pub min_ttl_ms: Option<i64>,
    /// Whether broca demanded a refresh regardless of the token's remaining validity.
    ///
    /// `Some(false)` on a `credential.get`, where broca always decides it: false is an answer
    /// and is written out, which keeps every record made before this field became optional
    /// byte-identical. Absent on a `credential.get_scoped`, which has no such parameter -- the
    /// vault never refreshes on demand for a grant-addressed read, so there was nothing to send.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub force_refresh: Option<bool>,
    /// The vault operation this request was sent as. A consumer joining against the vault's
    /// `auth_events` looks for this operation. Written on every request recorded since the
    /// operation name was added; absent on older records, which were all `credential.get`
    /// (see [`VaultRequest::op_or_legacy`]).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<VaultReadOp>,
}

impl VaultRequest {
    /// The operation this request was sent as, reading an absent `op` as `credential.get`: the
    /// only operation broca sent before the field existed. An operation name this build does
    /// not know comes back as [`VaultReadOp::Unknown`], never as `Get`: a later build wrote it,
    /// and this build cannot say what was sent.
    pub fn op_or_legacy(&self) -> VaultReadOp {
        self.op.clone().unwrap_or(VaultReadOp::Get)
    }
}

/// The vault read operation a [`VaultRequest`] was sent as. Serialized as a bare string:
/// `"get"`, `"get_scoped"`, or, for [`VaultReadOp::Unknown`], the string it was read from.
///
/// THIS SITS ON A WAL RECORD (`RunStarted`, through `AuthSelection.vault_request`), so it must
/// decode values a later build writes. Production rolls back, and a rolled-back build that
/// refused an operation name it did not know would fail to decode `RunStarted` and leave the
/// whole session unreadable. So any string decodes, and an unrecognised one is KEPT verbatim:
/// re-serializing it writes back exactly the string that was read, and a stored record keeps
/// its bytes. (`#[serde(other)]` would also decode it, but would forget the string and
/// rewrite the record.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VaultReadOp {
    /// `credential.get`, addressed by a bearer handle from the handle file.
    Get,
    /// `credential.get_scoped`, addressed by credential id and authorized by the vault's read
    /// grant to this process's supervised principal.
    GetScoped,
    /// An operation name this build does not know, written by a later build. Holds the name
    /// exactly as stored. This build never writes one of its own.
    Unknown(String),
}

impl VaultReadOp {
    /// The wire spelling.
    pub fn as_str(&self) -> &str {
        match self {
            VaultReadOp::Get => "get",
            VaultReadOp::GetScoped => "get_scoped",
            VaultReadOp::Unknown(name) => name,
        }
    }
}

impl Serialize for VaultReadOp {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for VaultReadOp {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let name = String::deserialize(deserializer)?;
        Ok(match name.as_str() {
            "get" => VaultReadOp::Get,
            "get_scoped" => VaultReadOp::GetScoped,
            _ => VaultReadOp::Unknown(name),
        })
    }
}

/// Why a call never left the process.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum NoDispatchReason {
    Denied {
        reason: String,
    },
    ValidationFailed {
        detail: String,
    },
    /// The tool plane was unavailable. `detail` is the stable matchable message
    /// (`"tool plane unavailable"`); a refusal the plane attributed to a module outage
    /// additionally names the module, the daemon's refusal code, and its detail, with
    /// `nothing_sent` implied. The optional fields default for WALs written before they
    /// existed, and old readers ignore the unknown fields, so both directions decode.
    Unavailable {
        detail: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        module: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        code: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        refusal_detail: Option<String>,
    },
    PolicyBlocked {
        detail: String,
    },
}

/// What KIND of terminal a `tool_result` record is — drives the replay policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatchOutcome {
    /// A definite result (real tool output, or a "never sent" error result).
    Completed,
    /// The request was sent but no result came back — the side effect MAY have
    /// applied, so the run stops for review rather than auto-resuming.
    Unknown,
    /// Synthetic repair: the call was abandoned by a cancel/interrupt.
    Interrupted,
    /// Synthetic repair: a stop/max-steps hard-stop fired with this call pending.
    MaxSteps,
}

impl DispatchOutcome {
    /// Whether this terminal's outcome is ambiguous — i.e. the request may have run
    /// with its result lost, so resume must stop for review rather than re-dispatch.
    /// Only `Unknown` qualifies.
    pub fn is_indeterminate(self) -> bool {
        matches!(self, DispatchOutcome::Unknown)
    }
}

/// Counts of what a resume rebuilt from the log without calling a provider or a tool.
///
/// Every field reads as zero when absent, and unknown fields are ignored, so a later build can
/// add a count without making this record undecodable here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ResumeReplayed {
    /// Messages of the reconstructed conversation, every earlier episode included.
    #[serde(default)]
    pub messages: u64,
    /// Model steps of the resumed episode whose output is durable and is not requested again.
    #[serde(default)]
    pub model_steps: u64,
    /// Durable tool terminals of the episode's open tool batch, reused as recorded.
    #[serde(default)]
    pub tool_results: u64,
}

/// One thing a resume does again because no durable record of it survived the crash.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ResumeRedo {
    /// The model step with this id is requested again: no output of it was durable.
    ModelStep { step_id: StepId },
    /// This planned tool call is dispatched: it has no durable intent, so it never left the
    /// process before the crash.
    ToolDispatch { tool_call_id: ToolCallId },
    /// A kind a later build writes. Decoding it here keeps the whole record readable instead
    /// of failing the WAL as corrupt; it is never written.
    #[serde(other, skip_serializing)]
    Unknown,
}

/// A durable approval decision (mirrors `ApprovalResponse` without the redundant
/// id — the record carries `tool_call_id`).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum ApprovalDecision {
    Allow,
    AllowNarrowed { input: Value },
    Deny { reason: String },
}

#[cfg(test)]
mod schema_tests {
    //! Schema v2 shape tests: every new/changed record serializes AND deserializes
    //! back to itself, and an absent optional field deserializes to the intended
    //! default — so the enrichment writes are verified here in Phase 1, not left
    //! unchecked until a later phase's projection reads them, and a forward/back-compat
    //! read of an older-shaped record is sound.

    use super::*;
    use broca_protocol::{
        content::{AssistantMessage, ContentBlock, FinishReason, Message, Role, ToolCall},
        error::{ErrorClass, ProviderError},
        MessageId,
    };
    use serde_json::json;

    fn round_trips(rec: &WalRecord) {
        let bytes = serde_json::to_vec(rec).expect("serialize");
        let back: WalRecord = serde_json::from_slice(&bytes).expect("deserialize");
        assert_eq!(rec, &back, "record did not round-trip: {rec:?}");
    }

    #[test]
    fn model_step_finished_arc_keeps_existing_wal_bytes() {
        let record = WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: Arc::new(AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::Text {
                    text: "done".into(),
                }],
            }),
            // Pin the old all-zero record, not the new unreported default.
            usage: Usage {
                input_tokens: Some(0),
                cached_input_tokens: Some(0),
                cache_write_tokens: Some(0),
                output_tokens: Some(0),
                reasoning_tokens: Some(0),
            },
            finish_reason: Some(FinishReason::Stop),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize shared assistant message");
        assert_eq!(
            bytes,
            br#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"text","text":"done"}]},"usage":{"input_tokens":0,"cached_input_tokens":0,"cache_write_tokens":0,"output_tokens":0,"reasoning_tokens":0},"finish_reason":"stop","retries_used":0}"#,
            "Arc must serialize its pointee without changing existing WAL bytes"
        );
    }

    #[test]
    fn tool_dispatch_intent_arc_keeps_existing_wal_bytes() {
        let record = WalRecord::ToolDispatchIntent {
            dispatched_at_ms: None,
            hooks: None,
            batch_id: BatchId("b1".into()),
            tool_call_id: ToolCallId("t1".into()),
            tool_name: Some("edit".into()),
            args: Arc::new(json!({"path": "file"})),
            module_id: None,
            schema_pin: None,
            approval_digest: None,
            call_key: None,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize shared tool arguments");
        assert_eq!(
            bytes,
            br#"{"type":"tool_dispatch_intent","batch_id":"b1","tool_call_id":"t1","tool_name":"edit","args":{"path":"file"}}"#,
            "Arc must serialize its pointee without changing existing WAL bytes"
        );
    }

    #[test]
    fn run_config_tools_arc_keeps_existing_wal_bytes() {
        let config = RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("p", "m"),
            tools: Arc::new(vec![Tool {
                name: "edit".into(),
                description: "edit a file".into(),
                input_schema: json!({"type": "object"}),
                module: Some("aft".into()),
                execution_mode: broca_protocol::request::ExecutionMode::Parallel,
                reply: None,
            }]),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        };

        let bytes = serde_json::to_vec(&config).expect("serialize config with shared tools");
        assert_eq!(
            bytes,
            br#"{"model":{"provider_module_id":"p","model_id":"m"},"tools":[{"name":"edit","description":"edit a file","input_schema":{"type":"object"},"module":"aft","execution_mode":"parallel"}],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1}"#,
            "Arc must serialize its pointee without changing existing RunConfig WAL bytes"
        );
    }

    #[test]
    fn tool_result_arc_keeps_existing_wal_bytes() {
        let record = WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: BatchId("b1".into()),
            result: Arc::new(ToolResult {
                tool_call_id: ToolCallId("t1".into()),
                output: broca_protocol::content::ToolOutput::Text {
                    text: "done".into(),
                },
                is_error: false,
            }),
            outcome: DispatchOutcome::Completed,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize shared tool result");
        assert_eq!(
            bytes,
            br#"{"type":"tool_result","batch_id":"b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}"#,
            "Arc must serialize its pointee without changing existing WAL bytes"
        );
    }

    /// Today a call refused before send is written as a `tool_result` with outcome `completed`
    /// and `is_error`, and a call sent with no result as one with outcome `unknown`. A result that names no
    /// typed reason must keep exactly these bytes, so a session written before any reason field
    /// existed replays and re-serializes unchanged, and an older build reads a newer build's
    /// results the same way.
    #[test]
    fn pre_reason_tool_result_bytes_are_pinned() {
        const REFUSED: &str = r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool not dispatched: route gone"},"is_error":true},"outcome":"completed"}"#;
        const UNKNOWN: &str = r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"error","message":"tool outcome unknown (interrupted for review): route closed"},"is_error":true},"outcome":"unknown"}"#;
        for (pinned, outcome) in [
            (REFUSED, DispatchOutcome::Completed),
            (UNKNOWN, DispatchOutcome::Unknown),
        ] {
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            let WalRecord::ToolResult {
                outcome: decoded_outcome,
                ref result,
                ..
            } = decoded
            else {
                panic!("expected a tool_result, got {decoded:?}");
            };
            assert_eq!(decoded_outcome, outcome);
            assert!(result.is_error);
            assert!(
                matches!(decoded, WalRecord::ToolResult { reason: None, .. }),
                "a result written before the reason field reads as naming no reason"
            );
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), pinned.as_bytes());
        }
    }

    fn user_msg(text: &str) -> Message {
        Message {
            cache_prefix_blocks: None,
            role: Role::User,
            content: vec![ContentBlock::Text { text: text.into() }],
            origin: None,
            ordinal: None,
            tags: Default::default(),
        }
    }

    fn min_config() -> Box<RunConfig> {
        Box::new(RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("p", "m"),
            tools: vec![].into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: ResponseFormat::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        })
    }

    #[test]
    fn pre_resolved_credential_run_config_bytes_are_unchanged() {
        let mut config = min_config();
        config.auth_selection = Some(AuthSelection {
            auth_method: None,
            credential_id: "apikey:kimi-code-plan-global".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });
        // Captured from the serializer before resolved_credential_id was added.
        const LEGACY: &str = r#"{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"apikey:kimi-code-plan-global"}}"#;
        assert_eq!(serde_json::to_vec(&config).unwrap(), LEGACY.as_bytes());
        let decoded: RunConfig = serde_json::from_str(LEGACY).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), LEGACY.as_bytes());
    }

    #[test]
    fn pre_vault_request_run_started_bytes_are_unchanged() {
        let mut config = min_config();
        config.auth_selection = Some(AuthSelection {
            auth_method: None,
            credential_id: "oauth:anthropic:work".into(),
            account_ref: Some("acct-1".into()),
            resolved_credential_id: Some("oauth:anthropic:shared".into()),
            vault_request: None,
            account_decision: None,
        });
        let record = WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        // Captured from the serializer before AuthSelection recorded the vault request
        // parameters. A run that made no vault request must keep exactly these bytes.
        const LEGACY: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","resolved_credential_id":"oauth:anthropic:shared"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
            LEGACY
        );
        let decoded: WalRecord = serde_json::from_str(LEGACY).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), LEGACY.as_bytes());
    }

    /// A RunStarted carrying every auth-selection field this build writes: credential id,
    /// account, resolved credential and a vault request with its operation name. Captured from
    /// the serializer before the selection gained `account_decision`; a selection without one
    /// must keep exactly these bytes, on write and on read-back, so records written before it
    /// replay unchanged.
    #[test]
    fn pre_account_decision_run_started_bytes_are_unchanged() {
        let mut config = min_config();
        config.auth_selection = Some(AuthSelection {
            auth_method: None,
            credential_id: "oauth:anthropic:work".into(),
            account_ref: Some("acct-1".into()),
            resolved_credential_id: Some("oauth:anthropic:work".into()),
            vault_request: Some(VaultRequest {
                min_ttl_ms: Some(600_000),
                force_refresh: None,
                op: Some(VaultReadOp::GetScoped),
            }),
            account_decision: None,
        });
        let record = WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("selection-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const LEGACY: &str = r#"{"type":"run_started","run_id":"selection-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","resolved_credential_id":"oauth:anthropic:work","vault_request":{"min_ttl_ms":600000,"op":"get_scoped"}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
            LEGACY
        );
        let decoded: WalRecord = serde_json::from_str(LEGACY).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), LEGACY.as_bytes());
    }

    /// A selection that records how its account was chosen adds exactly the one key, after
    /// every older one, and reads back to the same bytes; a path this build does not name (a
    /// later build's) decodes and keeps its spelling.
    #[test]
    fn an_account_decision_adds_one_key_and_keeps_its_bytes() {
        const STORED: &str = r#"{"credential_id":"oauth:anthropic:work","account_ref":"acct-1","vault_request":{"min_ttl_ms":600000,"op":"get_scoped"},"account_decision":{"path":"routed","admission_id":"run-1","decision_id":"acd-1"}}"#;
        let selection = AuthSelection {
            auth_method: None,
            credential_id: "oauth:anthropic:work".into(),
            account_ref: Some("acct-1".into()),
            resolved_credential_id: None,
            vault_request: Some(VaultRequest {
                min_ttl_ms: Some(600_000),
                force_refresh: None,
                op: Some(VaultReadOp::GetScoped),
            }),
            account_decision: Some(AccountDecision {
                conversation_key: None,
                conversation_key_source: None,
                path: ACCOUNT_DECISION_ROUTED.into(),
                admission_id: Some("run-1".into()),
                decision_id: Some("acd-1".into()),
            }),
        };
        assert_eq!(serde_json::to_string(&selection).unwrap(), STORED);
        let decoded: AuthSelection = serde_json::from_str(STORED).unwrap();
        assert_eq!(decoded, selection);

        const FUTURE: &str =
            r#"{"credential_id":"oauth:anthropic","account_decision":{"path":"some_future_path"}}"#;
        let decoded: AuthSelection = serde_json::from_str(FUTURE).unwrap();
        assert_eq!(
            decoded.account_decision.as_ref().map(|d| d.path.as_str()),
            Some("some_future_path")
        );
        assert_eq!(serde_json::to_string(&decoded).unwrap(), FUTURE);
    }

    #[test]
    fn model_step_finished_bytes_are_pinned() {
        // Captured from the serializer before steps recorded the service tier the provider
        // reported. A step whose response reported no tier must keep exactly these bytes, and
        // these bytes must keep decoding to the same record.
        let full = WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(3),
            assistant_message: (AssistantMessage {
                message_id: MessageId("msg-3".into()),
                content: vec![ContentBlock::Text { text: "ok".into() }],
            })
            .into(),
            usage: Usage {
                input_tokens: Some(120),
                cached_input_tokens: Some(0),
                cache_write_tokens: Some(40),
                output_tokens: Some(9),
                reasoning_tokens: Some(2),
            },
            finish_reason: Some(FinishReason::Stop),
            retries_used: 1,
            service_tier_used: None,
            speed_used: None,
        };
        let bare = WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(0),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };
        const FULL: &str = r#"{"type":"model_step_finished","step_id":3,"assistant_message":{"message_id":"msg-3","content":[{"type":"text","text":"ok"}]},"usage":{"input_tokens":120,"cached_input_tokens":0,"cache_write_tokens":40,"output_tokens":9,"reasoning_tokens":2},"finish_reason":"stop","retries_used":1}"#;
        const BARE: &str = r#"{"type":"model_step_finished","step_id":0,"assistant_message":{"message_id":"m","content":[]},"usage":{},"retries_used":0}"#;
        for (record, pinned) in [(full, FULL), (bare, BARE)] {
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
                pinned
            );
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
            assert_eq!(serde_json::to_vec(&decoded).unwrap(), pinned.as_bytes());
        }
    }

    #[test]
    fn old_records_default_retries_used_to_zero() {
        let model = WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m".into()),
                content: vec![ContentBlock::Text { text: "ok".into() }],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 7,
            service_tier_used: None,
            speed_used: None,
        };
        let terminal = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 9,
        };
        for record in [model, terminal] {
            let mut value = serde_json::to_value(record).expect("serialize current record");
            value
                .as_object_mut()
                .expect("tagged record")
                .remove("retries_used");
            let decoded: WalRecord = serde_json::from_value(value).expect("old record decodes");
            match decoded {
                WalRecord::ModelStepFinished { retries_used, .. }
                | WalRecord::RunFinished { retries_used, .. } => assert_eq!(retries_used, 0),
                other => panic!("unexpected record: {other:?}"),
            }
        }
    }

    #[test]
    fn pre_fix_run_started_omits_ts_ms_and_decodes_as_none() {
        let record = WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config: min_config(),
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        // Captured from the pre-project-id serializer, not generated from the new shape.
        const LEGACY: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(serde_json::to_vec(&record).unwrap(), LEGACY.as_bytes());
        let mut observed = record.clone();
        if let WalRecord::RunStarted { project_id, .. } = &mut observed {
            *project_id = Some("pj-a".into());
        }
        let bytes = serde_json::to_vec(&observed).unwrap();
        assert_eq!(bytes.len() - LEGACY.len(), br#","project_id":"pj-a""#.len());
        assert_eq!(
            serde_json::from_slice::<WalRecord>(&bytes).unwrap(),
            observed
        );
        let mut pre_fix = serde_json::to_value(&record).expect("serialize legacy shape");
        pre_fix
            .as_object_mut()
            .expect("tagged record")
            .remove("ts_ms");

        let decoded: WalRecord = serde_json::from_value(pre_fix.clone()).expect("decode old WAL");
        assert!(matches!(decoded, WalRecord::RunStarted { ts_ms: None, .. }));
        let reencoded = serde_json::to_value(decoded).expect("re-encode old WAL");
        assert_eq!(
            reencoded, pre_fix,
            "old record shape must remain byte-compatible"
        );
    }

    #[test]
    fn pre_work_class_run_started_bytes_are_pinned() {
        let record = WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config: min_config(),
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        // Captured before RunConfig gained work_class; an omitted value must remain byte-identical.
        const PRE_WORK_CLASS: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            serde_json::to_vec(&record).unwrap(),
            PRE_WORK_CLASS.as_bytes()
        );
    }

    /// Captured from the serializer before `RunConfig` gained `context_management`. Both a
    /// minimal config and one carrying every other optional field must keep exactly these
    /// bytes, so a session admitted before the field existed replays and re-serializes
    /// unchanged, and an absent value costs no bytes on any record.
    #[test]
    fn pre_context_management_run_started_bytes_are_pinned() {
        let record = |config: Box<RunConfig>| WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const MINIMAL: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let minimal = record(min_config());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&minimal).unwrap()).unwrap(),
            MINIMAL
        );
        let decoded: WalRecord = serde_json::from_str(MINIMAL).unwrap();
        assert_eq!(decoded, minimal);

        let mut populated = min_config();
        populated.tools_were_omitted = true;
        populated.context_limit = Some(200_000);
        populated.work_class = Some("mason".into());
        populated.stop_when = vec![StopCondition::StepCount { n: 7 }];
        let populated = record(populated);
        const POPULATED: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"tools_were_omitted":true,"context_limit":200000,"work_class":"mason","stop_when":[{"type":"step_count","n":7}]},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&populated).unwrap()).unwrap(),
            POPULATED
        );
        let decoded: WalRecord = serde_json::from_str(POPULATED).unwrap();
        assert_eq!(decoded, populated);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), POPULATED.as_bytes());
    }

    /// Captured from the serializer before `RunConfig` gained the caller's requested reasoning
    /// `variant`. A record without a variant, minimal or carrying every other optional field,
    /// must keep exactly these bytes, so a session admitted before the field existed replays and
    /// re-serializes unchanged and a send without a variant costs no bytes on any record.
    #[test]
    fn pre_variant_run_started_bytes_are_pinned() {
        let record = |config: Box<RunConfig>| WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: None,
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const MINIMAL: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let minimal = record(min_config());
        assert_eq!(serde_json::to_vec(&minimal).unwrap(), MINIMAL.as_bytes());
        let decoded: WalRecord = serde_json::from_str(MINIMAL).unwrap();
        assert_eq!(decoded, minimal);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), MINIMAL.as_bytes());

        let mut populated = min_config();
        populated.tools_were_omitted = true;
        populated.context_limit = Some(200_000);
        populated.work_class = Some("mason".into());
        populated.stop_when = vec![StopCondition::StepCount { n: 7 }];
        // Absent context management costs no bytes; stated so the pin covers every optional
        // field that existed before `variant`.
        populated.context_management = None;
        populated.frozen_render_config =
            serde_json::json!({"family_id": "openai_responses_chatgpt", "reasoning_effort": "max"});
        let populated = record(populated);
        const POPULATED: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"family_id":"openai_responses_chatgpt","reasoning_effort":"max"},"tools_were_omitted":true,"context_limit":200000,"work_class":"mason","stop_when":[{"type":"step_count","n":7}]},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            serde_json::to_vec(&populated).unwrap(),
            POPULATED.as_bytes()
        );
        let decoded: WalRecord = serde_json::from_str(POPULATED).unwrap();
        assert_eq!(decoded, populated);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), POPULATED.as_bytes());
    }

    /// Captured from the serializer before `RunConfig` gained the caller's requested
    /// `service_tier`. A record without a tier, minimal or carrying every other optional field
    /// (a variant and context management included), must keep exactly these bytes: a session
    /// admitted before the field existed replays and re-serializes unchanged, and a send without
    /// a tier costs no bytes on any record.
    #[test]
    fn pre_service_tier_run_started_bytes_are_pinned() {
        let record = |config: Box<RunConfig>| WalRecord::RunStarted {
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: RunId("legacy-run".into()),
            session: BindIdentity::new("/tmp/project", "runner", "session"),
            ts_ms: Some(1_790_000_000_000),
            config,
            input: vec![user_msg("hello")].into(),
            origin: RunOrigin::Fresh,
            submission_id: None,
        };
        const MINIMAL: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let minimal = record(min_config());
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&minimal).unwrap()).unwrap(),
            MINIMAL
        );
        let decoded: WalRecord = serde_json::from_str(MINIMAL).unwrap();
        assert_eq!(decoded, minimal);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), MINIMAL.as_bytes());

        let mut populated = min_config();
        populated.tools_were_omitted = true;
        populated.context_limit = Some(200_000);
        populated.work_class = Some("mason".into());
        populated.variant = Some("high".into());
        populated.stop_when = vec![StopCondition::StepCount { n: 7 }];
        populated.context_management = Some(ContextManagement::None);
        populated.frozen_render_config = serde_json::json!({
            "family_id": "openai_responses_chatgpt",
            "service_tier": "priority"
        });
        let populated = record(populated);
        const POPULATED: &str = r#"{"type":"run_started","run_id":"legacy-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"family_id":"openai_responses_chatgpt","service_tier":"priority"},"tools_were_omitted":true,"context_limit":200000,"work_class":"mason","variant":"high","stop_when":[{"type":"step_count","n":7}],"context_management":"none"},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        assert_eq!(
            String::from_utf8(serde_json::to_vec(&populated).unwrap()).unwrap(),
            POPULATED
        );
        let decoded: WalRecord = serde_json::from_str(POPULATED).unwrap();
        assert_eq!(decoded, populated);
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), POPULATED.as_bytes());
    }

    /// Records this build reads but never writes: a `run_finished` with reason
    /// `transform_unavailable`, and `{"transform": ...}` as `context_management` on a
    /// `RunStarted` config and on a queued request. Older WALs carry these bytes. Each must keep
    /// decoding to the same value and re-encoding to the same bytes; a reader that refused them
    /// would make the whole session unreadable.
    #[test]
    fn records_from_the_removed_transform_route_still_decode_and_keep_their_bytes() {
        const FINISHED: &str = r#"{"type":"run_finished","reason":"transform_unavailable","ts_ms":1790000000000,"usage":{},"error":{"class":"permanent","message":"transform unavailable: route closed"},"retries_used":0}"#;
        const STARTED: &str = r#"{"type":"run_started","run_id":"mc-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"context_limit":200000,"context_management":{"transform":"magic-context"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        const QUEUED: &str = r#"{"type":"queued","submission_id":"sub-mc","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[],"context_management":{"transform":"magic-context"}}}"#;

        for pinned in [FINISHED, STARTED, QUEUED] {
            let decoded: WalRecord = serde_json::from_str(pinned)
                .unwrap_or_else(|error| panic!("{pinned} must still decode: {error}"));
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }

        // The values really came through, rather than decoding into something that happens to
        // re-encode the same way.
        let WalRecord::RunFinished { reason, error, .. } =
            serde_json::from_str::<WalRecord>(FINISHED).unwrap()
        else {
            panic!("expected RunFinished");
        };
        assert_eq!(reason, RunFinishReason::TransformUnavailable);
        assert_eq!(error.map(|error| error.class), Some(ErrorClass::Permanent));
        let WalRecord::RunStarted { config, .. } =
            serde_json::from_str::<WalRecord>(STARTED).unwrap()
        else {
            panic!("expected RunStarted");
        };
        assert_eq!(
            config.context_management,
            Some(ContextManagement::Transform("magic-context".into()))
        );
        let WalRecord::Queued { request, .. } = serde_json::from_str::<WalRecord>(QUEUED).unwrap()
        else {
            panic!("expected Queued");
        };
        assert_eq!(
            request.context_management,
            Some(ContextManagement::Transform("magic-context".into()))
        );
    }

    /// A `RunStarted` whose frozen render config carries a prompt-cache policy, captured before a
    /// session could warm its cache between requests. Warming adds no field to the episode
    /// record: an Anthropic 1h marker policy and an OpenAI 24h hint policy both keep these bytes.
    #[test]
    fn pre_cache_warm_run_started_bytes_are_pinned() {
        let record = |frozen_render_config: serde_json::Value| {
            let mut config = min_config();
            config.frozen_render_config = frozen_render_config;
            WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: Some(1),
                manifest: None,
                scope: None,
                send_id: Some("send-1".into()),
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId("cache-run".into()),
                session: BindIdentity::new("/tmp/project", "runner", "session"),
                ts_ms: Some(1_790_000_000_000),
                config,
                input: vec![user_msg("hello")].into(),
                origin: RunOrigin::Fresh,
                submission_id: None,
            }
        };
        for (frozen, pinned) in [
            (
                serde_json::json!({
                    "family_id": "anthropic_messages",
                    "cache": {"version": 1, "class": "markers", "syntax": "anthropic_cache_control",
                              "ttl": "1h", "max_breakpoints": 4, "lookback_blocks": 20}
                }),
                r#"{"type":"run_started","run_id":"cache-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"cache":{"class":"markers","lookback_blocks":20,"max_breakpoints":4,"syntax":"anthropic_cache_control","ttl":"1h","version":1},"family_id":"anthropic_messages"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"send_id":"send-1","marker_order_version":1}"#,
            ),
            (
                serde_json::json!({
                    "family_id": "openai_responses",
                    "cache": {"version": 1, "class": "hints", "cache_key": "k", "retention": "24h"}
                }),
                r#"{"type":"run_started","run_id":"cache-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"frozen_render_config":{"cache":{"cache_key":"k","class":"hints","retention":"24h","version":1},"family_id":"openai_responses"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"send_id":"send-1","marker_order_version":1}"#,
            ),
        ] {
            let record = record(frozen);
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&record).unwrap()).unwrap(),
                pinned
            );
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
        }
    }

    /// A requested tier is recorded under `service_tier` in its wire spelling and reads back as
    /// the same tier; absent it costs nothing (pinned above).
    #[test]
    fn a_recorded_service_tier_round_trips() {
        let mut config = min_config();
        config.service_tier = Some(broca_protocol::request::ServiceTier::Priority);
        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["service_tier"], "priority");
        let decoded: RunConfig = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, *config);
    }

    /// A RunStarted stored with a recorded vault request must read back and re-serialize to
    /// exactly the bytes it was stored with. Captured from the serializer while
    /// `VaultRequest` was `{ min_ttl_ms, force_refresh: bool }`, before the request gained an
    /// operation name: a later field on the request has to stay absent on a record that
    /// predates it rather than acquiring a default value.
    #[test]
    fn a_stored_run_started_with_a_vault_request_reserializes_to_its_stored_bytes() {
        const STORED: &str = r#"{"type":"run_started","run_id":"vault-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"apikey:deepseek","resolved_credential_id":"apikey:deepseek","vault_request":{"min_ttl_ms":600000,"force_refresh":false}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let decoded: WalRecord = serde_json::from_str(STORED).expect("decode stored RunStarted");
        match &decoded {
            WalRecord::RunStarted { config, .. } => assert!(
                config
                    .auth_selection
                    .as_ref()
                    .is_some_and(|selection| selection.vault_request.is_some()),
                "the pin must carry a vault request, or it proves nothing about one"
            ),
            other => panic!("expected RunStarted, got {other:?}"),
        }
        assert_eq!(serde_json::to_string(&decoded).unwrap(), STORED);
    }

    /// A RunStarted written by a LATER build, whose vault request names an operation this
    /// build does not know, still decodes -- a rollback must not make the session unreadable --
    /// and re-serializes to exactly its stored bytes, the unknown name included. Reading it
    /// never claims the request was a `credential.get`.
    #[test]
    fn a_run_started_with_a_future_vault_op_decodes_and_keeps_its_bytes() {
        const STORED: &str = r#"{"type":"run_started","run_id":"vault-run","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"apikey:deepseek","vault_request":{"min_ttl_ms":600000,"op":"some_future_op"}}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        let decoded: WalRecord =
            serde_json::from_str(STORED).expect("a future op must not make RunStarted undecodable");
        let WalRecord::RunStarted { config, .. } = &decoded else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        let request = config
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.vault_request.clone())
            .expect("the stored request survives decoding");
        assert_eq!(
            request.op,
            Some(VaultReadOp::Unknown("some_future_op".into()))
        );
        assert_eq!(
            request.op_or_legacy(),
            VaultReadOp::Unknown("some_future_op".into()),
            "an unknown op must not be read as credential.get"
        );
        assert_eq!(serde_json::to_string(&decoded).unwrap(), STORED);
    }

    /// A request records what was sent: a handle read spells out its false `force_refresh`
    /// and names `get`; a scoped read, which has no refresh parameter, omits it and names
    /// `get_scoped`; a record from before the operation name existed reads as `get`.
    #[test]
    fn vault_request_records_the_operation_and_only_the_parameters_it_sent() {
        let handle_read = VaultRequest {
            min_ttl_ms: Some(600_000),
            force_refresh: Some(false),
            op: Some(VaultReadOp::Get),
        };
        assert_eq!(
            serde_json::to_string(&handle_read).unwrap(),
            r#"{"min_ttl_ms":600000,"force_refresh":false,"op":"get"}"#
        );
        let scoped_read = VaultRequest {
            min_ttl_ms: Some(600_000),
            force_refresh: None,
            op: Some(VaultReadOp::GetScoped),
        };
        assert_eq!(
            serde_json::to_string(&scoped_read).unwrap(),
            r#"{"min_ttl_ms":600000,"op":"get_scoped"}"#
        );
        let legacy: VaultRequest =
            serde_json::from_str(r#"{"min_ttl_ms":600000,"force_refresh":false}"#).unwrap();
        assert_eq!(legacy.op, None);
        assert_eq!(legacy.op_or_legacy(), VaultReadOp::Get);
        assert_eq!(scoped_read.op_or_legacy(), VaultReadOp::GetScoped);
    }

    #[test]
    fn every_v2_record_round_trips() {
        let provider_err = ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Auth,
            message: "needs reauth".into(),
            status: Some(401),
            retry_after_secs: None,
            provider_code: Some("invalid_token".into()),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: None,
            credential_id: None,
            affordable_max_tokens: None,
        };
        let records = vec![
            WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId("r1".into()),
                session: BindIdentity::new("/root", "runner", "s1"),
                ts_ms: None,
                config: min_config(),
                input: vec![user_msg("hi")].into(),
                origin: RunOrigin::Fresh,
                submission_id: Some(SubmissionId("sub1".into())),
            },
            WalRecord::ModelStepFinished {
                fallback: Default::default(),
                written_at_ms: None,
                tags: None,
                hooks: None,
                step_id: StepId(1),
                assistant_message: AssistantMessage {
                    message_id: MessageId("m1".into()),
                    content: vec![ContentBlock::Text { text: "ok".into() }],
                }
                .into(),
                usage: Usage::default(),
                finish_reason: Some(FinishReason::Stop),
                retries_used: 0,
                service_tier_used: None,
                speed_used: None,
            },
            WalRecord::StepStarted { step_id: StepId(1) },
            WalRecord::ToolDispatchIntent {
                dispatched_at_ms: None,
                hooks: None,
                batch_id: BatchId("b1".into()),
                tool_call_id: ToolCallId("t1".into()),
                tool_name: Some("read".into()),
                args: json!({"path": "x"}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            },
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: RunFinishReason::Error,
                usage: Usage::default(),
                error: Some(provider_err.clone()),
                retries_used: 0,
            },
            WalRecord::RunPaused {
                run_id: RunId("r1".into()),
                ts_ms: None,
                reason: PauseReason::AuthRequired,
                error: Some(provider_err),
            },
            WalRecord::RunConfigRefrozen {
                renderer_version: 2,
                ts_ms: None,
                frozen_render_config: json!({"project_id": "project-1"}),
            },
            WalRecord::RunAuthRefrozen {
                ts_ms: Some(123),
                auth_selection: AuthSelection {
                    auth_method: None,
                    credential_id: "chatgpt:openai".into(),
                    account_ref: Some("account-1".into()),
                    resolved_credential_id: None,
                    vault_request: None,
                    account_decision: None,
                },
            },
            WalRecord::BudgetFired {
                tags: None,
                run_id: RunId("r1".into()),
                kind: BudgetKind::Nudge,
                threshold: 20,
                ordinal: 5,
                caller_text: Some("nudge text".into()),
            },
            WalRecord::Queued {
                written_at_ms: None,
                retention: None,
                ts_ms: None,
                hooks: None,
                title: None,
                keep_warm: None,
                conversation_key: None,
                send_id: None,
                delivery: None,
                submission_id: SubmissionId("sub2".into()),
                request: Box::new(SubmissionRequest {
                    staged_uploads: None,
                    content: Vec::new(),
                    directives: None,
                    on_restart: None,
                    keep_warm: None,
                    scope: Default::default(),
                    project_id: None,
                    principal: None,
                    model: ModelHandle::new("anthropic", "claude-opus-4-8"),
                    input: vec![user_msg("queued prompt")],
                    tools: Some(vec![]),
                    policy_allowed: Default::default(),
                    tool_choice: ToolChoice::Auto,
                    generation: GenerationConfig::default(),
                    cache: None,
                    budget: None,
                    auth: None,
                    context_limit: None,
                    work_class: None,
                    variant: None,
                    service_tier: None,
                    stop_when: vec![],
                    context_management: None,
                    inherit_frozen: Default::default(),
                    structured_output: None,
                    client_message_id: None,
                }),
                request_identity_digest: None,
                principal: None,
            },
            WalRecord::Retracted {
                submission_id: SubmissionId("sub2".into()),
                failure: None,
            },
            WalRecord::Retracted {
                submission_id: SubmissionId("sub3".into()),
                failure: Some(QueuedAdmissionFailure {
                    run_id: RunId("r3".into()),
                    error: ProviderError {
                        provider_details: None,
                        provider_subcode: None,
                        refusal: None,
                        class: ErrorClass::Permanent,
                        message: "unknown model".into(),
                        status: None,
                        retry_after_secs: None,
                        provider_code: Some("queued_admission_failed".into()),
                        resets_at_ms: None,
                        resets_at_source: None,
                        limit_window: None,
                        account_ref: None,
                        credential_id: None,
                        affordable_max_tokens: None,
                    },
                }),
            },
        ];
        for rec in &records {
            round_trips(rec);
        }
    }

    /// The exact bytes a terminal `RunFinished` and a `RunPaused` carrying a typed error were
    /// written with before any account-limit fields existed on `ProviderError`. Old WALs hold
    /// these bytes, so both records must still decode from them and re-encode to them
    /// unchanged: a new optional error field that serialized when absent would change every
    /// terminal record broca writes and every consumer pinned to them.
    #[test]
    fn terminal_error_records_keep_their_pinned_bytes() {
        const FINISHED: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000000,"usage":{},"error":{"class":"transient","message":"429: rate limited","status":429,"retry_after_secs":353271.0,"provider_code":"rate_limit_error"},"retries_used":1}"#;
        const PAUSED: &str = r#"{"type":"run_paused","run_id":"r1","ts_ms":1790000000001,"reason":"auth_required","error":{"class":"auth_required","message":"needs reauth","status":401,"provider_code":"authentication_error"}}"#;
        let finished = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: Some(1_790_000_000_000),
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Transient,
                message: "429: rate limited".into(),
                status: Some(429),
                retry_after_secs: Some(353271.0),
                provider_code: Some("rate_limit_error".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
            retries_used: 1,
        };
        let paused = WalRecord::RunPaused {
            run_id: RunId("r1".into()),
            ts_ms: Some(1_790_000_000_001),
            reason: PauseReason::AuthRequired,
            error: Some(ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::Auth,
                message: "needs reauth".into(),
                status: Some(401),
                retry_after_secs: None,
                provider_code: Some("authentication_error".into()),
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
        };
        for (record, pinned) in [(finished, FINISHED), (paused, PAUSED)] {
            assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
            assert_eq!(serde_json::to_string(&decoded).unwrap(), pinned);
        }
    }

    /// The exact bytes of an account-limit terminal. Routing decodes these keys to wall one
    /// provider account until `resets_at_ms`, so their names, order and number forms are a
    /// contract with that decoder.
    #[test]
    fn account_limit_terminal_error_has_pinned_bytes() {
        const FINISHED: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790387965000,"usage":{},"error":{"class":"transient","message":"429: {\"type\":\"error\",\"error\":{\"type\":\"rate_limit_error\",\"message\":\"This request would exceed your account's rate limit. Please try again later.\"},\"request_id\":\"req_011CfRDPYNrfveajQccKCJ7o\"}","status":429,"retry_after_secs":352835.0,"provider_code":"account_limit_exhausted","resets_at_ms":1790740800000,"limit_window":"seven_day","account_ref":"d4bc9dbc"},"retries_used":0}"#;
        let record = WalRecord::RunFinished { interrupt_reason: None, ts_ms: Some(1_790_387_965_000),
        reason: RunFinishReason::Error,
        usage: Usage::default(),
        error: Some(ProviderError {
        provider_details: None,
            provider_subcode: None,
            refusal: None,
            class: ErrorClass::Transient,
            message: r#"429: {"type":"error","error":{"type":"rate_limit_error","message":"This request would exceed your account's rate limit. Please try again later."},"request_id":"req_011CfRDPYNrfveajQccKCJ7o"}"#.into(),
            status: Some(429),
            retry_after_secs: Some(352_835.0),
            provider_code: Some(broca_protocol::error::ACCOUNT_LIMIT_EXHAUSTED.into()),
            resets_at_ms: Some(1_790_740_800_000),
            resets_at_source: None,
            limit_window: Some("seven_day".into()),
            account_ref: Some("d4bc9dbc".into()),
            credential_id: None,
            affordable_max_tokens: None,
        }),
        retries_used: 0, };
        assert_eq!(serde_json::to_string(&record).unwrap(), FINISHED);
        assert_eq!(serde_json::from_str::<WalRecord>(FINISHED).unwrap(), record);
    }

    /// The exact bytes of the terminal errors that name a credential: a billing refusal with
    /// and without the run's `account_ref`, an HTTP 429 rate-limit terminal with no provider
    /// code, and an auth pause that carries an `account_ref`. Old WALs hold these bytes, and a
    /// later optional error field must leave every one of them unchanged when it is absent.
    #[test]
    fn credential_scoped_terminal_error_bytes_are_pinned() {
        const BILLING_WITH_ACCOUNT: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000002,"usage":{},"error":{"class":"permanent","message":"403: spending limit","status":403,"provider_code":"billing_exhausted","account_ref":"acct-x"},"retries_used":0}"#;
        const BILLING_WITHOUT_ACCOUNT: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000003,"usage":{},"error":{"class":"permanent","message":"403: spending limit","status":403,"provider_code":"billing_exhausted"},"retries_used":0}"#;
        const RATE_LIMITED: &str = r#"{"type":"run_finished","reason":"error","ts_ms":1790000000004,"usage":{},"error":{"class":"transient","message":"429: slow down","status":429,"retry_after_secs":30.0},"retries_used":3}"#;
        const PAUSED_WITH_ACCOUNT: &str = r#"{"type":"run_paused","run_id":"r2","ts_ms":1790000000005,"reason":"auth_required","error":{"class":"auth_required","message":"needs reauth","status":401,"account_ref":"acct-y"}}"#;
        let error = |class,
                     message: &str,
                     status,
                     retry_after_secs,
                     code: Option<&str>,
                     account: Option<&str>| ProviderError {
            provider_details: None,
            provider_subcode: None,
            refusal: None,
            class,
            message: message.into(),
            status: Some(status),
            retry_after_secs,
            provider_code: code.map(str::to_owned),
            resets_at_ms: None,
            resets_at_source: None,
            limit_window: None,
            account_ref: account.map(str::to_owned),
            credential_id: None,
            affordable_max_tokens: None,
        };
        let finished = |ts_ms, error, retries_used| WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: Some(ts_ms),
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(error),
            retries_used,
        };
        let cases = [
            (
                finished(
                    1_790_000_000_002,
                    error(
                        ErrorClass::Permanent,
                        "403: spending limit",
                        403,
                        None,
                        Some("billing_exhausted"),
                        Some("acct-x"),
                    ),
                    0,
                ),
                BILLING_WITH_ACCOUNT,
            ),
            (
                finished(
                    1_790_000_000_003,
                    error(
                        ErrorClass::Permanent,
                        "403: spending limit",
                        403,
                        None,
                        Some("billing_exhausted"),
                        None,
                    ),
                    0,
                ),
                BILLING_WITHOUT_ACCOUNT,
            ),
            (
                finished(
                    1_790_000_000_004,
                    error(
                        ErrorClass::Transient,
                        "429: slow down",
                        429,
                        Some(30.0),
                        None,
                        None,
                    ),
                    3,
                ),
                RATE_LIMITED,
            ),
            (
                WalRecord::RunPaused {
                    run_id: RunId("r2".into()),
                    ts_ms: Some(1_790_000_000_005),
                    reason: PauseReason::AuthRequired,
                    error: Some(error(
                        ErrorClass::Auth,
                        "needs reauth",
                        401,
                        None,
                        None,
                        Some("acct-y"),
                    )),
                },
                PAUSED_WITH_ACCOUNT,
            ),
        ];
        for (record, pinned) in cases {
            assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
            let decoded: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(decoded, record);
            assert_eq!(serde_json::to_string(&decoded).unwrap(), pinned);
        }
    }

    #[test]
    fn run_finished_round_trip_preserves_provider_unavailable_error_class() {
        let record = WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Error,
            usage: Usage::default(),
            error: Some(ProviderError {
                provider_details: None,
                provider_subcode: None,
                refusal: None,
                class: ErrorClass::ProviderUnavailable,
                message: "provider stream disconnected".into(),
                status: None,
                retry_after_secs: None,
                provider_code: None,
                resets_at_ms: None,
                resets_at_source: None,
                limit_window: None,
                account_ref: None,
                credential_id: None,
                affordable_max_tokens: None,
            }),
            retries_used: 0,
        };

        let bytes = serde_json::to_vec(&record).expect("serialize WAL record");
        assert_eq!(
            serde_json::from_slice::<Value>(&bytes).unwrap()["error"]["class"],
            "provider_unavailable",
            "the durable record retains the producer-local diagnostic class"
        );

        let decoded: WalRecord = serde_json::from_slice(&bytes).expect("deserialize WAL record");
        let WalRecord::RunFinished {
            error: Some(error), ..
        } = decoded
        else {
            panic!("expected terminal error in RunFinished");
        };
        assert_eq!(error.class, ErrorClass::ProviderUnavailable);
    }

    #[test]
    fn legacy_retracted_record_defaults_to_no_failure() {
        let record: WalRecord = serde_json::from_value(json!({
            "type": "retracted",
            "submission_id": "sub-old"
        }))
        .expect("decode legacy retract");
        assert_eq!(
            record,
            WalRecord::Retracted {
                submission_id: SubmissionId("sub-old".into()),
                failure: None,
            }
        );
    }

    #[test]
    fn run_config_stop_when_round_trips_and_defaults_when_absent() {
        let mut config = *min_config();
        config.stop_when = vec![
            StopCondition::StepCount { n: 3 },
            StopCondition::HasToolCall {
                name: Some("edit".into()),
            },
            StopCondition::LoopFinished,
        ];
        let encoded = serde_json::to_value(&config).expect("serialize stop predicates");
        assert!(encoded.get("stop_when").is_some());
        let decoded: RunConfig =
            serde_json::from_value(encoded.clone()).expect("deserialize stop predicates");
        assert_eq!(decoded.stop_when, config.stop_when);

        let mut legacy = encoded;
        legacy
            .as_object_mut()
            .expect("run config object")
            .remove("stop_when");
        let decoded: RunConfig =
            serde_json::from_value(legacy).expect("deserialize pre-stop-policy config");
        assert!(
            decoded.stop_when.is_empty(),
            "an old record keeps the historical empty policy and default safety cap"
        );
    }

    #[test]
    fn run_started_origin_defaults_to_fresh_when_absent() {
        // An older-shaped RunStarted (no `origin`, no `submission_id`) must read back
        // with origin = Fresh and submission_id = None.
        let older = json!({
            "type": "run_started",
            "run_id": "r1",
            "session": BindIdentity::new("/root", "runner", "s1"),
            "config": *min_config(),
            "input": [user_msg("hi")],
        });
        let rec: WalRecord = serde_json::from_value(older).expect("deserialize older shape");
        match rec {
            WalRecord::RunStarted {
                origin,
                submission_id,
                ..
            } => {
                assert_eq!(origin, RunOrigin::Fresh, "absent origin defaults to Fresh");
                assert_eq!(submission_id, None, "absent submission_id defaults to None");
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
    }

    #[test]
    fn seeded_from_origins_round_trip_and_bare_payload_is_compatible() {
        let import = RunOrigin::SeededFrom {
            origin_kind: SeedOriginKind::Import,
            source_ref: "cc:session-1".into(),
            origin_device_pseudonym: None,
            base_seq: None,
        };
        let fork = RunOrigin::SeededFrom {
            origin_kind: SeedOriginKind::Fork,
            source_ref: "ck:lineage-1".into(),
            origin_device_pseudonym: Some([7; 16]),
            base_seq: Some(42),
        };
        for origin in [&import, &fork] {
            let bytes = serde_json::to_vec(origin).expect("serialize origin");
            let back: RunOrigin = serde_json::from_slice(&bytes).expect("deserialize origin");
            assert_eq!(origin, &back);
        }

        let bare: RunOrigin = serde_json::from_value(json!({"kind": "seeded_from"}))
            .expect("deserialize reserved bare SeededFrom shape");
        assert_eq!(
            bare,
            RunOrigin::SeededFrom {
                origin_kind: SeedOriginKind::Import,
                source_ref: String::new(),
                origin_device_pseudonym: None,
                base_seq: None,
            }
        );
    }

    #[test]
    fn run_config_auth_selection_defaults_to_none_when_absent() {
        // A RunConfig written before `auth_selection` was added must read back with
        // auth_selection = None. This proves the field can be optional-with-default (no
        // schema version bump needed): a pre-existing record stays valid, and the path that
        // never sets it is unchanged.
        // Serialize a current config, then REMOVE the auth_selection key to simulate a
        // record written before the field existed.
        let mut config_json = serde_json::to_value(&*min_config()).unwrap();
        config_json
            .as_object_mut()
            .unwrap()
            .remove("auth_selection");
        assert!(
            config_json.get("auth_selection").is_none(),
            "precondition: the field is absent in the pre-auth shape"
        );
        let older = json!({
            "type": "run_started",
            "run_id": "r1",
            "session": BindIdentity::new("/root", "runner", "s1"),
            "config": config_json,
            "input": [user_msg("hi")],
        });
        let rec: WalRecord = serde_json::from_value(older).expect("deserialize pre-auth shape");
        match rec {
            WalRecord::RunStarted { config, .. } => {
                assert_eq!(
                    config.auth_selection, None,
                    "absent auth_selection defaults to None"
                );
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
    }

    #[test]
    fn run_config_freezes_only_the_resolved_auth_pin_effect() {
        let mut config = *min_config();
        config.auth_selection = Some(AuthSelection {
            auth_method: None,
            credential_id: "apikey:openai:astro".into(),
            account_ref: None,
            resolved_credential_id: None,
            vault_request: None,
            account_decision: None,
        });

        let frozen_bytes = serde_json::to_vec(&config).unwrap();
        let frozen_value: Value = serde_json::from_slice(&frozen_bytes).unwrap();
        assert_eq!(
            frozen_value.get("auth_selection"),
            Some(&json!({"credential_id": "apikey:openai:astro"}))
        );
        assert!(
            frozen_value.get("auth").is_none(),
            "RunConfig freezes the credential id, not the caller's admission-only pin"
        );

        let decoded: RunConfig = serde_json::from_slice(&frozen_bytes).unwrap();
        assert_eq!(serde_json::to_vec(&decoded).unwrap(), frozen_bytes);
    }

    #[test]
    fn run_config_budget_round_trips() {
        let mut config = *min_config();
        config.budget = Some(BudgetConfig {
            max_input_tokens: None,
            nudges: vec![broca_protocol::BudgetNudge {
                at_tool_calls: 20,
                text: "slow down".into(),
            }],
            finalize_at_tool_calls: 30,
            finalize_text: "answer now".into(),
        });
        let decoded: RunConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(decoded, config);
    }

    #[test]
    fn run_config_budget_defaults_to_none_when_absent() {
        let mut config_json = serde_json::to_value(&*min_config()).unwrap();
        config_json.as_object_mut().unwrap().remove("budget");
        let config: RunConfig = serde_json::from_value(config_json).unwrap();
        assert_eq!(config.budget, None);
    }

    #[test]
    fn run_config_context_limit_round_trips() {
        let mut config = *min_config();
        config.context_limit = Some(200_000);
        let decoded: RunConfig =
            serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        assert_eq!(decoded, config);
    }

    #[test]
    fn run_config_context_limit_defaults_to_none_and_preserves_legacy_bytes() {
        let legacy_bytes = serde_json::to_vec(&*min_config()).unwrap();
        let legacy_value: Value = serde_json::from_slice(&legacy_bytes).unwrap();
        assert!(
            legacy_value.get("context_limit").is_none(),
            "a None context limit must retain the pre-field WAL shape"
        );

        let decoded: RunConfig = serde_json::from_slice(&legacy_bytes).unwrap();
        assert_eq!(decoded.context_limit, None);
        assert_eq!(
            serde_json::to_vec(&decoded).unwrap(),
            legacy_bytes,
            "decoding and re-encoding an older RunConfig must preserve its bytes"
        );
    }

    #[test]
    fn run_config_tool_module_is_optional_and_round_trips() {
        let mut config = *min_config();
        config.tools = Arc::new(vec![Tool {
            name: "edit".into(),
            description: "edit files".into(),
            input_schema: json!({"type": "object"}),
            module: Some("aft".into()),
            execution_mode: broca_protocol::request::ExecutionMode::Parallel,
            reply: None,
        }]);

        let encoded = serde_json::to_value(&config).unwrap();
        assert_eq!(encoded["tools"][0]["module"], "aft");
        let decoded: RunConfig = serde_json::from_value(encoded.clone()).unwrap();
        assert_eq!(decoded, config);

        let mut older = encoded;
        older["tools"][0].as_object_mut().unwrap().remove("module");
        let decoded: RunConfig = serde_json::from_value(older).unwrap();
        assert_eq!(decoded.tools[0].module, None);
    }

    #[test]
    fn model_step_finished_preserves_signed_tool_call() {
        let rec = WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId("c1".into()),
                    tool_name: "read".into(),
                    input: json!({"path": "x"}),
                    thought_signature: Some("gemini-signed-call".into()),
                })],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::ToolCalls),
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };

        round_trips(&rec);
        let encoded = serde_json::to_string(&rec).expect("serialize signed record");
        assert!(encoded.contains("\"thought_signature\":\"gemini-signed-call\""));
    }

    #[test]
    fn unsigned_tool_call_keeps_pre_signature_bytes_and_reads_as_none() {
        let call = ToolCall {
            tool_call_id: ToolCallId("c1".into()),
            tool_name: "read".into(),
            input: json!({"path": "x"}),
            thought_signature: None,
        };
        let encoded_call = serde_json::to_vec(&call).expect("serialize unsigned call");
        assert_eq!(
            encoded_call, br#"{"tool_call_id":"c1","tool_name":"read","input":{"path":"x"}}"#,
            "an absent optional signature must not change pre-field bytes"
        );
        let decoded_call: ToolCall =
            serde_json::from_slice(&encoded_call).expect("deserialize pre-field call");
        assert_eq!(decoded_call, call);

        let rec = WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: (AssistantMessage {
                message_id: MessageId("m1".into()),
                content: vec![ContentBlock::ToolCall(call)],
            })
            .into(),
            usage: Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        };
        round_trips(&rec);
        assert!(
            !serde_json::to_string(&rec)
                .expect("serialize unsigned record")
                .contains("thought_signature"),
            "unsigned WAL records must retain their pre-field shape"
        );
    }

    #[test]
    fn budget_fired_caller_text_is_additive_and_defaults_to_none() {
        let legacy = serde_json::json!({
            "type": "budget_fired",
            "run_id": "r1",
            "kind": "nudge",
            "threshold": 2,
            "ordinal": 4,
        });
        let decoded: WalRecord =
            serde_json::from_value(legacy.clone()).expect("legacy budget firing");
        assert!(matches!(
            decoded,
            WalRecord::BudgetFired {
                caller_text: None,
                ..
            }
        ));
        assert_eq!(
            serde_json::to_value(decoded).expect("reencode legacy budget firing"),
            legacy,
            "an absent replay field must preserve the prior record shape"
        );
    }

    #[test]
    fn enrichment_optionals_default_to_none_when_absent() {
        // ModelStepFinished without finish_reason, ToolDispatchIntent without
        // tool_name, RunFinished without error — all read back as None.
        let step: WalRecord = serde_json::from_value(json!({
            "type": "model_step_finished",
            "step_id": 1,
            "assistant_message": { "message_id": "m1", "content": [] },
            "usage": Usage::default(),
        }))
        .unwrap();
        assert!(
            matches!(
                step,
                WalRecord::ModelStepFinished {
                    finish_reason: None,
                    ..
                }
            ),
            "absent finish_reason defaults to None"
        );

        let intent: WalRecord = serde_json::from_value(json!({
            "type": "tool_dispatch_intent",
            "batch_id": "b1",
            "tool_call_id": "t1",
            "args": {},
        }))
        .unwrap();
        assert!(
            matches!(
                intent,
                WalRecord::ToolDispatchIntent {
                    tool_name: None,
                    ..
                }
            ),
            "absent tool_name defaults to None"
        );

        let finished: WalRecord = serde_json::from_value(json!({
            "type": "run_finished",
            "reason": "completed",
            "usage": Usage::default(),
        }))
        .unwrap();
        assert!(
            matches!(finished, WalRecord::RunFinished { error: None, .. }),
            "absent error defaults to None"
        );
    }

    #[test]
    fn auth_selection_account_ref_is_additive_and_defaults_to_none() {
        // A selection written before `account_ref` was added (only a credential id) reads back
        // with account_ref = None and re-encodes byte-identically — the field is serde-skipped
        // when absent, so a legacy frozen selection keeps its exact shape (the same additive
        // contract as BudgetFired's caller_text).
        let legacy_selection = json!({ "credential_id": "oauth:anthropic" });
        let decoded: AuthSelection =
            serde_json::from_value(legacy_selection.clone()).expect("decode legacy selection");
        assert_eq!(decoded.credential_id, "oauth:anthropic");
        assert_eq!(
            decoded.account_ref, None,
            "absent account_ref defaults to None"
        );
        assert_eq!(
            serde_json::to_value(&decoded).expect("reencode legacy selection"),
            legacy_selection,
            "an absent account_ref must preserve the prior selection shape"
        );

        // A vault-served account id round-trips verbatim (never derived, never hashed).
        let stamped = json!({ "credential_id": "chatgpt:openai", "account_ref": "acct-uuid-123" });
        let decoded: AuthSelection =
            serde_json::from_value(stamped.clone()).expect("decode stamped selection");
        assert_eq!(decoded.account_ref.as_deref(), Some("acct-uuid-123"));
        assert_eq!(
            serde_json::to_value(&decoded).expect("reencode stamped selection"),
            stamped
        );
    }

    #[test]
    fn legacy_run_started_selection_decodes_without_account_ref() {
        // A RunStarted frozen before account_ref existed (selection carries only credential_id)
        // decodes with account_ref None, so resume/replay of an older WAL is unaffected.
        let mut config_json = serde_json::to_value(&*min_config()).unwrap();
        config_json.as_object_mut().unwrap().insert(
            "auth_selection".into(),
            json!({ "credential_id": "oauth:anthropic" }),
        );
        let legacy_run = json!({
            "type": "run_started",
            "run_id": "r1",
            "session": BindIdentity::new("/root", "runner", "s1"),
            "config": config_json,
            "input": [user_msg("hi")],
        });
        let rec: WalRecord = serde_json::from_value(legacy_run).expect("decode legacy run_started");
        match &rec {
            WalRecord::RunStarted { config, .. } => {
                let selection = config.auth_selection.as_ref().expect("selection present");
                assert_eq!(selection.credential_id, "oauth:anthropic");
                assert_eq!(
                    selection.account_ref, None,
                    "a legacy frozen selection has no account_ref"
                );
            }
            other => panic!("expected RunStarted, got {other:?}"),
        }
    }

    /// A record type written by a newer build decodes instead of failing the whole WAL. The
    /// payload deliberately reuses field names known records carry (`step_id`, `usage`,
    /// `reason`), so a decoder that fell through to a known variant would show up here.
    #[test]
    fn a_record_type_this_build_does_not_know_decodes_as_unknown() {
        let newer = br#"{"type":"attempt_digest_from_a_newer_build","step_id":3,"attempt":2,"status":529,"reason":"completed","usage":{"output_tokens":7}}"#;
        let record: WalRecord = serde_json::from_slice(newer).expect("unknown type decodes");
        assert_eq!(record, WalRecord::Unknown);

        // A payload with no tag at all is still corruption, not an unknown record.
        assert!(serde_json::from_slice::<WalRecord>(br#"{"step_id":3}"#).is_err());
    }

    #[test]
    fn pre_account_stamp_attempt_bytes_remain_unchanged() {
        let bytes = r#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_str(bytes).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), bytes);
    }

    #[test]
    fn contentful_attempt_progress_bytes_are_pinned_before_gap_metrics() {
        // Keep the full content-bearing shape stable when optional measurements are added.
        let bytes = br#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":50,"first_content_at_ms":10,"last_content_at_ms":20,"content_bytes":3,"non_content_bytes":4,"non_content_bytes_after_last_content":0,"events_after_last_content":0}"#;
        let record: WalRecord = serde_json::from_slice(bytes).unwrap();
        match &record {
            WalRecord::ModelAttemptFinished {
                content_progress, ..
            } => {
                assert_eq!(content_progress.max_content_gap_ms, None);
                assert_eq!(content_progress.max_content_gap_ended_at_ms, None);
            }
            other => panic!("expected model attempt record, got {other:?}"),
        }
        assert_eq!(serde_json::to_vec(&record).unwrap(), bytes.to_vec());
    }

    /// The attempt record's bytes are pinned, and every outcome this build writes round-trips.
    #[test]
    fn model_attempt_finished_bytes_are_pinned_and_every_outcome_round_trips() {
        let stored = br#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":503,"content_type":"text/plain","response_body_len":1048576,"response_body_sha256":"bb","response_body_cap":1048576,"outcome":"http_error","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_slice(stored).unwrap();
        assert!(record.is_informational());
        assert_eq!(serde_json::to_vec(&record).unwrap(), stored.to_vec());

        let interior_gap = br#"{"type":"model_attempt_finished","step_id":2,"attempt":1,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"bb","outcome":"complete","ts_ms":50,"first_content_at_ms":10,"last_content_at_ms":50,"max_content_gap_ms":30,"max_content_gap_ended_at_ms":40}"#;
        let record: WalRecord = serde_json::from_slice(interior_gap).unwrap();
        match &record {
            WalRecord::ModelAttemptFinished {
                content_progress, ..
            } => {
                assert_eq!(content_progress.max_content_gap_ms, Some(30));
                assert_eq!(content_progress.max_content_gap_ended_at_ms, Some(40));
            }
            other => panic!("expected model attempt record, got {other:?}"),
        }
        assert_eq!(serde_json::to_vec(&record).unwrap(), interior_gap.to_vec());

        // Absent optionals write no bytes: an attempt that never received a status line.
        let no_status = br#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":0,"response_body_sha256":"e3b0","outcome":"connect_failed","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_slice(no_status).unwrap();
        assert_eq!(serde_json::to_vec(&record).unwrap(), no_status.to_vec());

        for name in [
            "complete",
            "eof_before_completion",
            "chunk_error",
            "idle_timeout",
            "content_timeout",
            "byte_limit",
            "decode_error",
            "rate_floor",
            "cancelled",
            "sealed_in_flight",
            "http_error",
            "provider_error",
            "connect_failed",
            "header_timeout",
            "no_status",
        ] {
            let outcome: AttemptOutcome = serde_json::from_value(json!(name)).unwrap();
            assert!(
                !matches!(outcome, AttemptOutcome::Unknown(_)),
                "{name} is known"
            );
            assert_eq!(serde_json::to_value(&outcome).unwrap(), json!(name));
        }
    }

    /// An outcome a later build writes decodes, and keeps its exact string: re-serializing the
    /// record writes back the bytes that were read, as `VaultReadOp` does for operations.
    #[test]
    fn an_attempt_outcome_this_build_does_not_know_decodes_and_keeps_its_string() {
        let stored = br#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","http_status":200,"response_body_len":3,"response_body_sha256":"cc","outcome":"tls_renegotiation_refused","ts_ms":5}"#;
        let record: WalRecord = serde_json::from_slice(stored).expect("unknown outcome decodes");
        match &record {
            WalRecord::ModelAttemptFinished { outcome, .. } => assert_eq!(
                outcome,
                &AttemptOutcome::Unknown("tls_renegotiation_refused".into())
            ),
            other => panic!("expected the attempt record, got {other:?}"),
        }
        assert_eq!(serde_json::to_vec(&record).unwrap(), stored.to_vec());
    }

    /// `Unknown` holds none of the original record, so it must never be written back as if
    /// it were one: serializing it is an error rather than `{"type":"unknown"}`.
    #[test]
    fn an_unknown_record_refuses_to_serialize() {
        assert!(serde_json::to_vec(&WalRecord::Unknown).is_err());
        assert!(serde_json::to_value(&WalRecord::Unknown).is_err());
    }

    #[test]
    fn pre_conversation_key_bytes_are_pinned() {
        let queued = r#"{"type":"queued","submission_id":"sub-r","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","send_id":"turn","delivery":"interrupt"}"#;
        let record: WalRecord = serde_json::from_str(queued).unwrap();
        assert!(matches!(record, WalRecord::Queued { .. }));
        assert_eq!(serde_json::to_string(&record).unwrap(), queued);
        let decision = r#"{"path":"routed","admission_id":"a","decision_id":"d"}"#;
        let record: AccountDecision = serde_json::from_str(decision).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), decision);
    }

    /// Optional fields must not change the bytes of records that omit them.
    /// Decode literal bytes so the fixture cannot move together with the encoder.
    #[test]
    fn run_started_and_queued_bytes_are_pinned() {
        const RUN_STARTED_POPULATED: &str = r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-7"}"#;
        const RUN_STARTED_MINIMAL: &str = r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        const QUEUED_POPULATED: &str = r#"{"type":"queued","submission_id":"sub-run-8","request":{"project_id":"pj-a","model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc"}"#;
        const QUEUED_MINIMAL: &str = r#"{"type":"queued","submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;

        for pinned in [
            RUN_STARTED_POPULATED,
            RUN_STARTED_MINIMAL,
            QUEUED_POPULATED,
            QUEUED_MINIMAL,
        ] {
            let decoded: WalRecord = serde_json::from_str(pinned).expect("pinned record decodes");
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }

        // The populated shapes really carried their optional fields through the decode, so
        // the byte comparison above covered them rather than a record that dropped them.
        let decoded: WalRecord = serde_json::from_str(RUN_STARTED_POPULATED).unwrap();
        let WalRecord::RunStarted {
            project_id,
            ts_ms,
            submission_id,
            ..
        } = decoded
        else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert_eq!(project_id.as_deref(), Some("pj-a"));
        assert_eq!(ts_ms, Some(1_790_000_000_000));
        assert_eq!(submission_id, Some(SubmissionId("sub-run-7".into())));
        let decoded: WalRecord = serde_json::from_str(QUEUED_POPULATED).unwrap();
        let WalRecord::Queued {
            request,
            request_identity_digest,
            ..
        } = decoded
        else {
            panic!("expected Queued, got {decoded:?}");
        };
        assert_eq!(request.project_id.as_deref(), Some("pj-a"));
        assert_eq!(request_identity_digest.as_deref(), Some("v1:abc"));
    }

    /// Every optional field `RunStarted` and `Queued` carried before the send key was written on
    /// them, populated at once (the principal included), plus the shapes with none of them. A
    /// session written by an older build holds exactly these bytes, so recording the send key
    /// must leave them decoding and re-encoding to themselves: an absent key costs no bytes.
    ///
    /// Built only by decoding, never by a struct literal, so adding a field to either record
    /// does not change this test's source.
    #[test]
    fn run_started_and_queued_bytes_before_the_send_key_are_pinned() {
        const RUN_STARTED_ALL_OPTIONAL: &str = r#"{"type":"run_started","run_id":"run-sid-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-sid-7"}"#;
        const RUN_STARTED_NO_OPTIONAL: &str = r#"{"type":"run_started","run_id":"run-sid-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;
        const QUEUED_ALL_OPTIONAL: &str = r#"{"type":"queued","submission_id":"sub-run-sid-8","request":{"project_id":"pj-a","model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","principal":{"kind":"direct"}}"#;
        const QUEUED_NO_OPTIONAL: &str = r#"{"type":"queued","submission_id":"sub-run-sid-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;

        for pinned in [
            RUN_STARTED_ALL_OPTIONAL,
            RUN_STARTED_NO_OPTIONAL,
            QUEUED_ALL_OPTIONAL,
            QUEUED_NO_OPTIONAL,
        ] {
            let decoded: WalRecord = serde_json::from_str(pinned).expect("pinned record decodes");
            assert!(
                !matches!(decoded, WalRecord::Unknown),
                "a pinned record must decode as itself: {pinned}"
            );
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }

        // The principal really came through the decode on both records, so the byte
        // comparison above covered it rather than a record that dropped it.
        let decoded: WalRecord = serde_json::from_str(RUN_STARTED_ALL_OPTIONAL).unwrap();
        let WalRecord::RunStarted { principal, .. } = decoded else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert!(principal.is_some());
        let decoded: WalRecord = serde_json::from_str(QUEUED_ALL_OPTIONAL).unwrap();
        let WalRecord::Queued { principal, .. } = decoded else {
            panic!("expected Queued, got {decoded:?}");
        };
        assert!(principal.is_some());
    }

    /// The bytes `RunStarted` and `Queued` are written with when they carry the send key: the
    /// key fields come last, after every field an older build already knows. An older reader
    /// ignores them (serde skips unknown fields), so these records stay frame version 1.
    #[test]
    fn run_started_and_queued_bytes_with_the_send_key_are_pinned() {
        const RUN_STARTED_KEYED: &str = r#"{"type":"run_started","run_id":"run-sid-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-sid-7","send_id":"turn-7","request_identity_digest":"v1:abc"}"#;
        const QUEUED_KEYED: &str = r#"{"type":"queued","submission_id":"sub-run-sid-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]},"request_identity_digest":"v1:abc","send_id":"turn-8"}"#;
        for pinned in [RUN_STARTED_KEYED, QUEUED_KEYED] {
            let decoded: WalRecord = serde_json::from_str(pinned).expect("pinned record decodes");
            assert_eq!(
                String::from_utf8(serde_json::to_vec(&decoded).unwrap()).unwrap(),
                pinned,
                "a pinned record must re-encode to exactly the bytes it was read from"
            );
        }
        let decoded: WalRecord = serde_json::from_str(RUN_STARTED_KEYED).unwrap();
        let WalRecord::RunStarted {
            send_id,
            request_identity_digest,
            ..
        } = decoded
        else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert_eq!(send_id.as_deref(), Some("turn-7"));
        assert_eq!(request_identity_digest.as_deref(), Some("v1:abc"));
        let decoded: WalRecord = serde_json::from_str(QUEUED_KEYED).unwrap();
        let WalRecord::Queued { send_id, .. } = decoded else {
            panic!("expected Queued, got {decoded:?}");
        };
        assert_eq!(send_id.as_deref(), Some("turn-8"));
    }

    /// `RunStarted` with every field it has today populated at once, as the complete framed
    /// bytes an ordinary (version 1) frame holds. The tests above pin each optional field, but
    /// in separate records; this one also pins their relative order in one record and the frame
    /// around it, so a field added later can be shown to leave a record that does not use it
    /// byte-identical, frame included.
    ///
    /// The digest is a literal computed outside this crate (SHA-256 over the version byte 1,
    /// seq 1 and fence 1 as u64 LE, then the payload), so it cannot move together with the
    /// encoder. Built only by decoding, so adding a field does not change this test's source.
    #[test]
    fn pre_marker_order_run_started_frame_bytes_are_pinned() {
        const RUN_STARTED_EVERY_FIELD: &str = r#"{"type":"run_started","run_id":"run-all-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-all-7","send_id":"turn-7","request_identity_digest":"v1:abc"}"#;
        const EXPECTED_DIGEST: [u8; 32] = [
            0x05, 0x3c, 0xba, 0x5b, 0xbc, 0x0b, 0xc3, 0x50, 0x44, 0xa5, 0xb5, 0x65, 0xdd, 0x34,
            0xc7, 0x8e, 0xef, 0x60, 0xea, 0xb3, 0x32, 0xb0, 0x87, 0xc1, 0x64, 0x7f, 0xb7, 0x07,
            0x43, 0x6c, 0xbe, 0xf0,
        ];

        let decoded: WalRecord =
            serde_json::from_str(RUN_STARTED_EVERY_FIELD).expect("pinned record decodes");
        let WalRecord::RunStarted {
            project_id,
            principal,
            ts_ms,
            submission_id,
            send_id,
            request_identity_digest,
            marker_order_version,
            ..
        } = &decoded
        else {
            panic!("expected RunStarted, got {decoded:?}");
        };
        assert_eq!(*marker_order_version, None);
        // Every optional field really came through the decode, so the byte comparison below
        // covers it rather than a record that dropped it.
        assert!(project_id.is_some() && principal.is_some() && ts_ms.is_some());
        assert!(submission_id.is_some() && send_id.is_some());
        assert!(request_identity_digest.is_some());

        let bytes = crate::framing::encode(1, 1, &decoded).unwrap();
        let payload = RUN_STARTED_EVERY_FIELD.as_bytes();
        assert_eq!(
            &bytes[0..4],
            &(payload.len() as u32).to_le_bytes(),
            "length"
        );
        assert_eq!(bytes[4], 1, "an ordinary record frame");
        assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
        assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(
            String::from_utf8(bytes[53..].to_vec()).unwrap(),
            RUN_STARTED_EVERY_FIELD,
            "payload"
        );
        let mut versioned = decoded.clone();
        if let WalRecord::RunStarted {
            marker_order_version,
            ..
        } = &mut versioned
        {
            *marker_order_version = Some(1);
        }
        assert!(versioned.required_features().is_empty());
        let versioned_bytes = crate::framing::encode(1, 1, &versioned).unwrap();
        assert_eq!(versioned_bytes[4], 1);
        let roundtrip: WalRecord = serde_json::from_slice(&versioned_bytes[53..]).unwrap();
        assert_eq!(roundtrip, versioned);
        assert!(String::from_utf8(versioned_bytes[53..].to_vec())
            .unwrap()
            .ends_with("\"marker_order_version\":1}"));
    }

    /// `RunStarted` with every field it has before the frozen plan manifest existed, as the
    /// complete framed bytes: once with every field that needs no reader feature (an ordinary
    /// version-1 frame), and once also carrying a frozen scope (a version-3 frame that requires
    /// `scope/v1`). A field added later must leave both byte-identical when it is absent, frame
    /// and digest included.
    ///
    /// The digests are literals computed outside this crate (SHA-256 over the version byte, seq 1
    /// and fence 1 as u64 LE, then the payload), so they cannot move together with the encoder.
    /// Both records are built only by decoding, so adding a field does not change this source.
    #[test]
    fn pre_manifest_run_started_frame_bytes_are_pinned() {
        const UNGATED: &str = r#"{"type":"run_started","run_id":"run-all-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-all-7","send_id":"turn-7","request_identity_digest":"v1:abc","marker_order_version":1}"#;
        const UNGATED_DIGEST: [u8; 32] = [
            0x71, 0x9b, 0x20, 0xa3, 0x62, 0x09, 0xbe, 0xf1, 0x45, 0x3f, 0xab, 0x46, 0x20, 0xd9,
            0xa3, 0x3d, 0x6e, 0xd9, 0x64, 0x3c, 0x2a, 0x43, 0x58, 0x49, 0xdb, 0xd9, 0x5b, 0x58,
            0x8a, 0x29, 0x0d, 0x99,
        ];
        const SCOPED_PAYLOAD: &str = r#"{"requires":["scope/v1"],"record":{"type":"run_started","run_id":"run-all-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"project_id":"pj-a","principal":{"kind":"reserved","module_id":"prefrontal-core"},"ts_ms":1790000000000,"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"submission_id":"sub-run-all-7","send_id":"turn-7","request_identity_digest":"v1:abc","scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3},"marker_order_version":1}}"#;
        const SCOPED_DIGEST: [u8; 32] = [
            0x47, 0x88, 0xe5, 0xba, 0xc3, 0xd1, 0xb3, 0x88, 0xb1, 0xa1, 0xa1, 0x12, 0x72, 0xa3,
            0x52, 0x5b, 0x5b, 0xc3, 0x7f, 0x0e, 0x2f, 0xa8, 0x10, 0xa5, 0xb0, 0xe6, 0xb4, 0x79,
            0x8b, 0x9d, 0xb2, 0xb6,
        ];

        let ungated: WalRecord = serde_json::from_str(UNGATED).expect("pinned record decodes");
        let scoped_record = SCOPED_PAYLOAD
            .strip_prefix(r#"{"requires":["scope/v1"],"record":"#)
            .and_then(|rest| rest.strip_suffix('}'))
            .expect("the gated payload wraps the record");
        let scoped: WalRecord =
            serde_json::from_str(scoped_record).expect("pinned scoped record decodes");
        for (record, ver, payload, digest) in [
            (&ungated, 1u8, UNGATED, UNGATED_DIGEST),
            (&scoped, 3u8, SCOPED_PAYLOAD, SCOPED_DIGEST),
        ] {
            let WalRecord::RunStarted {
                project_id,
                principal,
                ts_ms,
                submission_id,
                send_id,
                request_identity_digest,
                marker_order_version,
                ..
            } = record
            else {
                panic!("expected RunStarted, got {record:?}");
            };
            // Every optional field really came through the decode, so the byte comparison
            // below covers it rather than a record that dropped it.
            assert!(project_id.is_some() && principal.is_some() && ts_ms.is_some());
            assert!(submission_id.is_some() && send_id.is_some());
            assert!(request_identity_digest.is_some() && marker_order_version.is_some());

            let bytes = crate::framing::encode(1, 1, record).unwrap();
            assert_eq!(
                &bytes[0..4],
                &(payload.len() as u32).to_le_bytes(),
                "length"
            );
            assert_eq!(bytes[4], ver, "frame version");
            assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
            assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
            assert_eq!(&bytes[21..53], &digest[..], "digest");
            assert_eq!(
                String::from_utf8(bytes[53..].to_vec()).unwrap(),
                payload,
                "payload"
            );
        }
        let WalRecord::RunStarted { scope, .. } = &scoped else {
            unreachable!()
        };
        assert!(scope.is_some(), "the scoped record kept its scope");
    }

    /// Existing scoped sessions must retain their exact WAL encoding when optional fields
    /// for an automation's sender and flow identifier are absent. The fixture and digest are
    /// independent literals so the encoder cannot silently move the expected bytes with it.
    #[test]
    fn pre_flow_scoped_run_started_bytes_are_pinned() {
        let payload = include_str!("../tests/fixtures/pre_flow_scoped_run_started.json").trim_end();
        let envelope: serde_json::Value = serde_json::from_str(payload).unwrap();
        let record: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
        let bytes = crate::framing::encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], 3);
        assert_eq!(&bytes[5..13], &1u64.to_le_bytes());
        assert_eq!(&bytes[13..21], &1u64.to_le_bytes());
        assert_eq!(
            &bytes[21..53],
            &[
                0x47, 0x88, 0xe5, 0xba, 0xc3, 0xd1, 0xb3, 0x88, 0xb1, 0xa1, 0xa1, 0x12, 0x72, 0xa3,
                0x52, 0x5b, 0x5b, 0xc3, 0x7f, 0x0e, 0x2f, 0xa8, 0x10, 0xa5, 0xb0, 0xe6, 0xb4, 0x79,
                0x8b, 0x9d, 0xb2, 0xb6,
            ]
        );
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        assert_eq!(&bytes[53..], payload.as_bytes());
    }

    /// `ToolDispatchIntent` with every field it has before it recorded the module a call was
    /// dispatched to, as complete framed bytes: once with every optional field present and once
    /// with none. Both are ordinary version-1 frames. A field added later must leave both
    /// byte-identical when it is absent, frame and digest included.
    ///
    /// The digests are literals computed outside this crate (SHA-256 over the version byte, seq 1
    /// and fence 1 as u64 LE, then the payload), so they cannot move together with the encoder.
    /// Both records are built only by decoding, so adding a field does not change this source.
    #[test]
    fn pre_dispatch_module_tool_dispatch_intent_frame_bytes_are_pinned() {
        const EVERY_FIELD: &str = r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"}}"#;
        const EVERY_FIELD_DIGEST: [u8; 32] = [
            0x02, 0x47, 0x46, 0x47, 0x3f, 0x70, 0x65, 0x47, 0x41, 0xc3, 0x98, 0xd6, 0x5f, 0x82,
            0xc4, 0x9a, 0x10, 0x74, 0xb9, 0x85, 0x1e, 0x86, 0x0a, 0x1d, 0x82, 0x37, 0x22, 0x59,
            0xfc, 0x2c, 0x6b, 0x01,
        ];
        const NO_OPTIONAL: &str =
            r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t2","args":null}"#;
        const NO_OPTIONAL_DIGEST: [u8; 32] = [
            0xbe, 0x34, 0xc1, 0xb3, 0x3d, 0x68, 0x23, 0x3f, 0x8a, 0x56, 0x7d, 0x9f, 0x5a, 0xa2,
            0xc3, 0xd5, 0xe9, 0xd8, 0x1b, 0xce, 0x6b, 0x57, 0x55, 0x84, 0x5a, 0xa7, 0x88, 0x18,
            0x28, 0x09, 0x81, 0xa7,
        ];

        for (payload, digest, named) in [
            (EVERY_FIELD, EVERY_FIELD_DIGEST, true),
            (NO_OPTIONAL, NO_OPTIONAL_DIGEST, false),
        ] {
            let record: WalRecord = serde_json::from_str(payload).expect("pinned record decodes");
            let WalRecord::ToolDispatchIntent { tool_name, .. } = &record else {
                panic!("expected ToolDispatchIntent, got {record:?}");
            };
            // The optional name really came through the decode (or really stayed absent), so
            // the byte comparison below covers it.
            assert_eq!(tool_name.is_some(), named);
            assert!(record.required_features().is_empty());

            let bytes = crate::framing::encode(1, 1, &record).unwrap();
            assert_eq!(
                &bytes[0..4],
                &(payload.len() as u32).to_le_bytes(),
                "length"
            );
            assert_eq!(bytes[4], 1, "frame version");
            assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
            assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
            assert_eq!(&bytes[21..53], &digest[..], "digest");
            assert_eq!(
                String::from_utf8(bytes[53..].to_vec()).unwrap(),
                payload,
                "payload"
            );
        }
    }
}

#[cfg(test)]
mod pinned_bytes_tests;

#[cfg(test)]
mod feature_union_tests;

#[cfg(test)]
mod step_transform_bytes_tests;

#[cfg(test)]
mod affinity_source_tests {
    #[test]
    fn conversation_key_source_preserves_future_strings() {
        let source: super::ConversationKeySource =
            serde_json::from_str("\"future_affinity\"").unwrap();
        assert_eq!(
            serde_json::to_string(&source).unwrap(),
            "\"future_affinity\""
        );
    }
}

#[cfg(test)]
mod step_transform_tests;

#[cfg(test)]
mod bill_attempt_tests {
    use super::*;

    const ID: &str = "00112233445566778899aabbccddeeff";
    const RESERVATION: &str = r#"{"run_id":"r","step_id":1,"attempt":0,"send_index":1,"warm":false,"reserved_at_ms":100,"provider":"opencode-go","endpoint_host":"example.com","model_sent":"model","credential":{"credential_ref":"apikey:opencode-go","account_ref":"account","auth_selection":{"credential_id":"apikey:opencode-go","resolved_credential_id":"apikey:opencode-go"},"auth_method":"apikey","provider_ids":["opencode-go","kimi-for-coding"],"plan_tier":"max_5x","plan_tier_source":"operator"},"session":{"project_root":"/project","harness":"runner","session":"s","session_ref":"ref"},"run":{"flow_id":"flow","work_class":"interactive","context_limit":200000,"service_tier_requested":"priority","speed_requested":"fast"},"request_body_bytes":123,"compressed":true,"input_tokens_estimate":20}"#;
    const ENVELOPE: &str = r#"{"sent":true,"response_bytes":99,"outcome":"complete","sent_at_ms":101,"finished_at_ms":200,"http_status":200,"finish_reason":"stop","model_used":"model","usage":{"input_tokens":10,"cached_input_tokens":20,"cache_write_tokens":30,"output_tokens":40,"reasoning_tokens":5},"cache_write_5m_tokens":0,"cache_write_1h_tokens":30,"reasoning_included_in_output":true,"usage_raw":{"future_vendor_key":{"count":9}},"vendor_request_id":"vendor-id","service_tier_used":"default","speed_used":"standard"}"#;
    const MULTI_ENVELOPE: &str = r#"{"sent":true,"response_bytes":99,"outcome":"complete","sent_at_ms":101,"finished_at_ms":200,"http_status":200,"finish_reason":"stop","usage_by_model":[{"model_used":"source-model","usage":{"input_tokens":10,"cache_write_tokens":30,"output_tokens":40,"reasoning_tokens":5},"cache_write_5m_tokens":0,"cache_write_1h_tokens":30},{"model_used":"fallback-model","usage":{"input_tokens":7,"cached_input_tokens":11,"output_tokens":9}}],"reasoning_included_in_output":true,"usage_raw":{"iterations":[{"model":"source-model"},{"model":"fallback-model"}]},"vendor_request_id":"vendor-id","service_tier_used":"default","speed_used":"standard"}"#;
    const LEGACY_ATTEMPT: &str = r#"{"type":"model_attempt_finished","step_id":2,"attempt":0,"request_body_len":12,"request_body_sha256":"aa","response_body_len":0,"response_body_sha256":"e3b0","outcome":"connect_failed","ts_ms":5}"#;
    const LEGACY_WARM: &str = r#"{"type":"cache_warmed","run_id":"r","step_id":2,"request_body_sha256":"aa","warm_body_sha256":"bb","usage":{},"sent_at_ms":1790000000000}"#;

    #[test]
    fn bill_attempt_compression_refused_pinned_bytes() {
        use sha2::{Digest, Sha256};
        const ENVELOPE: &str = r#"{"sent":true,"response_bytes":0,"outcome":"compression_refused","sent_at_ms":101,"finished_at_ms":200,"http_status":415}"#;
        let envelope: AttemptEnvelope = serde_json::from_str(ENVELOPE).unwrap();
        assert_eq!(envelope.outcome, BillingAttemptOutcome::CompressionRefused);
        assert_eq!(serde_json::to_string(&envelope).unwrap(), ENVELOPE);
        let json = format!(
            r#"{{"type":"model_attempt_finished","attempt_id":"{ID}","reservation":{RESERVATION},"envelope":{ENVELOPE},"step_id":1,"attempt":0,"request_body_len":123,"request_body_sha256":"aa","http_status":415,"response_body_len":0,"response_body_sha256":"e3b0","outcome":"http_error","ts_ms":200}}"#
        );
        let record: WalRecord = serde_json::from_str(&json).unwrap();
        let payload = serde_json::to_vec(&record).unwrap();
        assert_eq!(payload, json.as_bytes());
        let frame = crate::framing::encode_v4(
            broca_wal_reader::frame::Metadata {
                seq: 1,
                fence: 1,
                appended_ms: 200,
                writer_id: [1; 16],
            },
            &[],
            &record,
        )
        .unwrap();
        let crate::framing::DecodeStep::Record { framed, consumed } =
            crate::framing::decode_next(&frame)
        else {
            panic!("compression refusal frame must decode");
        };
        assert_eq!(consumed, frame.len());
        assert_eq!(framed.record, record);
        if let Some(output) = std::env::var_os("BROCA_COMPRESSION_SAMPLE_DIR") {
            let output = std::path::PathBuf::from(output);
            std::fs::create_dir_all(&output).unwrap();
            std::fs::write(output.join("compression_refused.json"), &payload).unwrap();
            std::fs::write(output.join("compression_refused.frame"), &frame).unwrap();
            std::fs::write(
                output.join("SHA256SUMS"),
                format!(
                    "{:x}  compression_refused.json\n{:x}  compression_refused.frame\n",
                    Sha256::digest(&payload),
                    Sha256::digest(&frame),
                ),
            )
            .unwrap();
        }
    }

    pub(super) fn golden_records() -> Vec<String> {
        let mut records = vec![
            format!(
                r#"{{"type":"attempt_reserved","attempt_id":"{ID}","reservation":{RESERVATION}}}"#
            ),
            format!(
                r#"{{"type":"attempt_released","attempt_id":"{ID}","reservation":{RESERVATION},"envelope":{{"sent":false,"response_bytes":0,"outcome":"cancelled","finished_at_ms":200}}}}"#
            ),
            format!(
                r#"{{"type":"attempt_corrected","attempt_id":"{ID}","correction_id":"ffeeddccbbaa99887766554433221100","reason":"usage_reconciled","reservation":{RESERVATION},"envelope":{ENVELOPE}}}"#
            ),
            format!(
                r#"{{"type":"attempt_finished","attempt_id":"{ID}","reservation":{RESERVATION},"envelope":{{"sent":"unknown","response_bytes":0,"outcome":"unknown","finished_at_ms":200}}}}"#
            ),
        ];
        for (kind, legacy) in [
            ("model_attempt_finished", LEGACY_ATTEMPT),
            ("cache_warmed", LEGACY_WARM),
        ] {
            let current = if kind == "model_attempt_finished" {
                legacy
                    .replace("connect_failed", "complete")
                    .replace("\"step_id\":2", "\"step_id\":1")
                    .replace("\"ts_ms\":5", "\"ts_ms\":200")
                    .replace("\"request_body_len\":12", "\"request_body_len\":123")
                    .replace("\"response_body_len\":0", "\"response_body_len\":99")
            } else {
                legacy
                    .replace("\"step_id\":2", "\"step_id\":1")
                    .replace("1790000000000", "101")
            };
            let reservation = if kind == "cache_warmed" {
                RESERVATION.replace("\"warm\":false", "\"warm\":true")
            } else {
                RESERVATION.to_owned()
            };
            let suffix = current
                .strip_prefix(&format!(r#"{{"type":"{kind}","#))
                .unwrap();
            let envelope = if kind == "model_attempt_finished" {
                MULTI_ENVELOPE
            } else {
                ENVELOPE
            };
            records.push(format!(r#"{{"type":"{kind}","attempt_id":"{ID}","reservation":{reservation},"envelope":{envelope},{suffix}"#));
        }
        records
    }

    #[test]
    fn bill_attempt_golden_records_cover_every_field() {
        for golden in golden_records() {
            let record: WalRecord = serde_json::from_str(&golden).unwrap();
            assert_ne!(record, WalRecord::Unknown);
            assert_eq!(serde_json::to_string(&record).unwrap(), golden);
            assert_eq!(
                record.required_features(),
                &[crate::framing::BILL_ATTEMPTS_FEATURE]
            );
            assert!(record.is_informational());
            let value = serde_json::to_value(&record).unwrap();
            assert_eq!(value["reservation"]["run"]["speed_requested"], "fast");
            assert_eq!(value["reservation"]["credential"]["auth_method"], "apikey");
            assert!(value["reservation"]["credential"].get("kind").is_none());
            assert!(value["reservation"]["credential"]
                .get("billing_model")
                .is_none());
            if let WalRecord::ModelAttemptFinished {
                attempt,
                reservation: Some(reservation),
                ..
            } = record
            {
                assert_eq!(attempt, reservation.attempt);
            }
        }
    }

    #[test]
    fn bill_attempt_decoder_enforces_usage_arithmetic_and_model_attribution() {
        let single: Value = serde_json::from_str(ENVELOPE).unwrap();
        let multi: Value = serde_json::from_str(MULTI_ENVELOPE).unwrap();
        let rejected = |value: Value, expected: &str| {
            let error = serde_json::from_value::<AttemptEnvelope>(value)
                .unwrap_err()
                .to_string();
            assert!(error.contains(expected), "{error}");
        };
        for field in ["response_bytes", "sent", "outcome"] {
            let mut value = single.clone();
            value.as_object_mut().unwrap().remove(field);
            rejected(value, field);
        }
        let mut value = single.clone();
        value["cache_write_1h_tokens"] = serde_json::json!(31);
        rejected(value, "cache_write_tokens");
        let mut value = single.clone();
        value["cache_write_5m_tokens"] = serde_json::json!(u64::MAX);
        rejected(value, "cache_write_tokens");
        let mut value = single.clone();
        value["usage"]
            .as_object_mut()
            .unwrap()
            .remove("cache_write_tokens");
        rejected(value, "cache_write_tokens");
        for mut value in [single.clone(), multi.clone()] {
            value
                .as_object_mut()
                .unwrap()
                .remove("reasoning_included_in_output");
            rejected(value, "reasoning_included_in_output");
        }
        let mut value = multi.clone();
        value["usage"] = serde_json::json!({"output_tokens":9});
        rejected(value, "no top-level");
        let mut value = multi.clone();
        value["usage_by_model"][1]["model_used"] = serde_json::json!("source-model");
        rejected(value, "coalesce");
        let mut value = multi.clone();
        value["usage_by_model"].as_array_mut().unwrap().pop();
        rejected(value, "at least two models");
        let mut value = multi.clone();
        value["usage_by_model"][0]["cache_write_1h_tokens"] = serde_json::json!(31);
        rejected(value, "cache_write_tokens");
        let mut value = single.clone();
        value["outcome"] = serde_json::json!("unknown");
        rejected(value, "must not carry usage");
        let mut value = single.clone();
        value["sent"] = serde_json::json!(false);
        rejected(value, "sent false");
        let mut value = single.clone();
        value["outcome"] = serde_json::json!("future-outcome");
        rejected(value, "unknown variant");
        let decoded: AttemptEnvelope = serde_json::from_value(multi).unwrap();
        let models = decoded.usage_by_model.unwrap();
        assert_eq!(
            models
                .iter()
                .map(|m| m.model_used.as_str())
                .collect::<Vec<_>>(),
            ["source-model", "fallback-model"]
        );
        assert!(decoded.usage.is_none());
        assert_eq!(models[0].usage.input_tokens, Some(10));
    }

    #[test]
    fn bill_attempt_recovery_terminal_is_unknown_and_never_released_when_uncertain() {
        let records = golden_records();
        let recovery: Value = serde_json::from_str(&records[3]).unwrap();
        for sent in [
            serde_json::json!(true),
            serde_json::json!(false),
            serde_json::json!("unknown"),
        ] {
            let mut value = recovery.clone();
            value["envelope"]["sent"] = sent;
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert!(matches!(record, WalRecord::AttemptFinished { .. }));
            assert!(record.is_informational());
        }
        let mut value = recovery.clone();
        value["envelope"]["outcome"] = serde_json::json!("complete");
        assert!(serde_json::from_value::<WalRecord>(value)
            .unwrap_err()
            .to_string()
            .contains("requires outcome unknown"));
        let mut value = recovery.clone();
        value["envelope"]["usage"] = serde_json::json!({});
        assert!(serde_json::from_value::<WalRecord>(value)
            .unwrap_err()
            .to_string()
            .contains("must not carry usage"));
        let mut value = recovery;
        value["type"] = serde_json::json!("attempt_released");
        assert!(serde_json::from_value::<WalRecord>(value)
            .unwrap_err()
            .to_string()
            .contains("provably unsent"));
    }

    #[test]
    fn bill_attempt_corrections_have_independent_ids_and_closed_reasons() {
        let value: Value = serde_json::from_str(&golden_records()[2]).unwrap();
        assert!(value.get("corrects").is_none());
        assert_ne!(value["attempt_id"], value["correction_id"]);
        for reason in [
            "usage_reconciled",
            "provider_evidence_corrected",
            "credential_attribution_corrected",
        ] {
            let mut corrected = value.clone();
            corrected["reason"] = serde_json::json!(reason);
            assert!(serde_json::from_value::<WalRecord>(corrected).is_ok());
        }
        for field in ["correction_id", "reason"] {
            let mut corrected = value.clone();
            corrected.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<WalRecord>(corrected)
                .unwrap_err()
                .to_string()
                .contains(field));
        }
        let mut delta = value.clone();
        delta["envelope"] = serde_json::json!({"usage":{"output_tokens":5}});
        assert!(serde_json::from_value::<WalRecord>(delta).is_err());
        let mut corrected = value;
        corrected["reason"] = serde_json::json!("arbitrary");
        assert!(serde_json::from_value::<WalRecord>(corrected)
            .unwrap_err()
            .to_string()
            .contains("unknown variant"));
    }

    #[test]
    fn bill_attempt_absent_fields_stay_absent_and_reported_zero_stays_zero() {
        for (json, expected) in [
            (
                r#"{"sent":"unknown","response_bytes":0,"outcome":"unknown"}"#,
                r#"{"sent":"unknown","response_bytes":0,"outcome":"unknown"}"#,
            ),
            (
                r#"{"sent":true,"response_bytes":1,"outcome":"complete","usage":{}}"#,
                r#"{"sent":true,"response_bytes":1,"outcome":"complete","usage":{}}"#,
            ),
            (
                r#"{"sent":true,"response_bytes":0,"outcome":"http_error","usage":{"input_tokens":0},"cache_write_5m_tokens":0,"reasoning_included_in_output":false}"#,
                r#"{"sent":true,"response_bytes":0,"outcome":"http_error","usage":{"input_tokens":0},"cache_write_5m_tokens":0,"reasoning_included_in_output":false}"#,
            ),
        ] {
            let envelope: AttemptEnvelope = serde_json::from_str(json).unwrap();
            assert_eq!(serde_json::to_string(&envelope).unwrap(), expected);
        }
        let run: AttemptRun = serde_json::from_str("{}").unwrap();
        assert_eq!(serde_json::to_string(&run).unwrap(), "{}");
        let credential: AttemptCredential = serde_json::from_str("{}").unwrap();
        assert_eq!(serde_json::to_string(&credential).unwrap(), "{}");
        let mut reservation: Value = serde_json::from_str(RESERVATION).unwrap();
        reservation
            .as_object_mut()
            .unwrap()
            .remove("input_tokens_estimate");
        let decoded: AttemptReservation = serde_json::from_value(reservation.clone()).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), reservation);
    }

    #[test]
    fn bill_attempt_vault_and_provider_metadata_vocabularies_remain_open() {
        let credential_json = serde_json::json!({
            "credential_ref":"opaque-served-id", "auth_method":"future-auth",
            "provider_ids":["future-provider"], "plan_tier":"future-plan",
            "plan_tier_source":"future-source"
        });
        let credential: AttemptCredential =
            serde_json::from_value(credential_json.clone()).unwrap();
        assert_eq!(serde_json::to_value(credential).unwrap(), credential_json);
        let credential: AttemptCredential = serde_json::from_value(serde_json::json!({})).unwrap();
        assert_eq!(credential.plan_tier, None);
        assert!(serde_json::to_value(credential)
            .unwrap()
            .get("plan_tier")
            .is_none());
        let mut value: Value = serde_json::from_str(ENVELOPE).unwrap();
        value["service_tier_used"] = serde_json::json!("future-tier");
        value["speed_used"] = serde_json::json!("future-speed");
        let envelope: AttemptEnvelope = serde_json::from_value(value.clone()).unwrap();
        assert_eq!(serde_json::to_value(envelope).unwrap(), value);
    }

    #[test]
    fn bill_attempt_legacy_attempt_and_warm_bytes_are_unchanged() {
        for pinned in [LEGACY_ATTEMPT, LEGACY_WARM] {
            let record: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(serde_json::to_string(&record).unwrap(), pinned);
            assert!(record.required_features().is_empty());
            let frame = crate::framing::encode(1, 1, &record).unwrap();
            assert_eq!(&frame[crate::framing::HEADER_LEN..], pinned.as_bytes());
        }
    }

    #[test]
    fn bill_attempt_and_correction_ids_are_validated_by_name() {
        let bytes = [0xab; 16];
        assert_eq!(
            AttemptId::from_bytes(bytes).as_str(),
            "abababababababababababababababab"
        );
        for invalid in [
            "",
            "0011",
            "00112233445566778899AABBCCDDEEFF",
            "g0112233445566778899aabbccddeeff",
            "00112233445566778899aabbccddeeff00",
        ] {
            for kind in golden_records() {
                let mut value: Value = serde_json::from_str(&kind).unwrap();
                value["attempt_id"] = Value::String(invalid.into());
                let err = serde_json::from_value::<WalRecord>(value).unwrap_err();
                assert!(err.to_string().contains("attempt_id"), "{err}");
            }
            let mut value: Value = serde_json::from_str(&golden_records()[2]).unwrap();
            value["correction_id"] = Value::String(invalid.into());
            let err = serde_json::from_value::<WalRecord>(value).unwrap_err();
            assert!(err.to_string().contains("correction_id"), "{err}");
        }
        for invalid in [Value::Null, serde_json::json!(123), serde_json::json!(true)] {
            for golden in golden_records() {
                let mut value: Value = serde_json::from_str(&golden).unwrap();
                value["attempt_id"] = invalid.clone();
                let err = serde_json::from_value::<WalRecord>(value).unwrap_err();
                assert!(err.to_string().contains("attempt_id"), "{err}");
            }
        }
    }

    #[test]
    fn bill_attempt_each_optional_billing_field_requires_the_feature() {
        for legacy in [LEGACY_ATTEMPT, LEGACY_WARM] {
            for (field, value) in [
                ("attempt_id", serde_json::json!(ID)),
                ("reservation", serde_json::from_str(RESERVATION).unwrap()),
                ("envelope", serde_json::from_str(ENVELOPE).unwrap()),
            ] {
                let mut record: Value = serde_json::from_str(legacy).unwrap();
                record[field] = value;
                let record: WalRecord = serde_json::from_value(record).unwrap();
                assert_eq!(
                    record.required_features(),
                    &[crate::framing::BILL_ATTEMPTS_FEATURE]
                );
            }
        }
    }

    #[test]
    fn bill_attempt_samples_use_the_writer_and_reader_codecs() {
        use broca_wal_reader::{
            features::Policy,
            frame::{decode_next_with_policy, DecodeStep, Metadata},
        };
        use sha2::{Digest, Sha256};
        let output = std::env::var_os("BROCA_ATTEMPT_SAMPLE_DIR").map(std::path::PathBuf::from);
        let mut sums = String::new();
        for golden in golden_records() {
            let record: WalRecord = serde_json::from_str(&golden).unwrap();
            let bytes = crate::framing::encode_v4(
                Metadata {
                    seq: 1,
                    fence: 1,
                    appended_ms: 200,
                    writer_id: [1; 16],
                },
                &[],
                &record,
            )
            .unwrap();
            let DecodeStep::Frame(frame) =
                decode_next_with_policy(&bytes, crate::framing::KNOWN_FEATURES, Policy::All)
                    .unwrap()
            else {
                panic!("reader did not decode billing sample");
            };
            assert_eq!(frame.consumed, bytes.len());
            assert_eq!(frame.payload, golden.as_bytes());
            assert_eq!(frame.header.metadata.appended_ms, 200);
            assert_eq!(
                serde_json::from_slice::<WalRecord>(frame.payload).unwrap(),
                record
            );
            assert_eq!(
                decode_next_with_policy(&bytes, &[], Policy::All).unwrap_err(),
                broca_wal_reader::Error::NewerWriter {
                    features: vec![crate::framing::BILL_ATTEMPTS_FEATURE.into()],
                }
            );
            let kind = serde_json::from_str::<Value>(&golden).unwrap()["type"]
                .as_str()
                .unwrap()
                .to_owned();
            if let Some(output) = &output {
                std::fs::create_dir_all(output).unwrap();
                let decoded = serde_json::to_vec_pretty(&serde_json::json!({
                    "seq":frame.header.metadata.seq,
                    "fence":frame.header.metadata.fence,
                    "appended_ms":frame.header.metadata.appended_ms,
                    "writer_id":frame.header.metadata.writer_id,
                    "requires":frame.header.requires,
                    "record":serde_json::from_slice::<Value>(frame.payload).unwrap(),
                }))
                .unwrap();
                for (name, contents) in [
                    (format!("{kind}.json"), frame.payload),
                    (format!("{kind}.wal"), bytes.as_slice()),
                    (format!("{kind}.decoded.json"), decoded.as_slice()),
                ] {
                    std::fs::write(output.join(&name), contents).unwrap();
                    sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(contents)));
                }
            }
        }
        if let Some(output) = output {
            std::fs::write(output.join("SHA256SUMS"), sums).unwrap();
        }
    }
}
