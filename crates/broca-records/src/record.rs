//! The durable log record set (the authoritative semantics are in
//! `docs/durability-contract.md`). These are the assembled UNITS the runner
//! persists — never token deltas. Bodies reuse `broca-protocol` canonical types.

use std::sync::Arc;

use broca_protocol::{
    content::{FinishReason, Message},
    error::ProviderError,
    request::{
        BudgetConfig, ContextManagement, GenerationConfig, ModelHandle, ResponseFormat,
        StopCondition, Tool, ToolChoice,
    },
    BindIdentity, RunId, StepId, SubmissionId, ToolCallId, Usage,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub use crate::vault_facts::{
    ObservedVaultFacts, VaultBillingFacts, VaultFactsAttribution, VaultFactsIdentity,
};

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

/// A compaction view the session accepted: the edits, keyed by message and part, that its
/// compaction provider published for the requests broca renders. Edits stay raw JSON so
/// provider extensions, key order and number spellings survive a reader round trip without
/// depending on the compaction library.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionViewV2 {
    pub version: u64,
    pub checksum: String,
    pub edits: Vec<StoredViewEdit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub built_through: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_signed: Option<String>,
    /// The module that applied the view, as the daemon attested it on the provider's route;
    /// never taken from the request.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_by: Option<String>,
}

/// One edit in rebuilt view order. A previous index names the latest earlier accepted view,
/// whether or not it was applied. The writer may reference only byte-identical edits.
#[derive(Debug, Clone, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StoredViewEdit {
    Full {
        edit: Box<serde_json::value::RawValue>,
    },
    Previous {
        index: u32,
    },
}

impl PartialEq for StoredViewEdit {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Full { edit: a }, Self::Full { edit: b }) => a.get() == b.get(),
            (Self::Previous { index: a }, Self::Previous { index: b }) => a == b,
            _ => false,
        }
    }
}

impl<'de> Deserialize<'de> for StoredViewEdit {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        // Internally tagged enum buffering cannot retain RawValue bytes. Parse the outer
        // object directly from its raw JSON instead of buffering the edit into serde values.
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        #[derive(Deserialize)]
        struct Kind {
            kind: String,
        }
        let kind: Kind = serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        match kind.kind.as_str() {
            "full" => {
                #[derive(Deserialize)]
                struct Full {
                    edit: Box<serde_json::value::RawValue>,
                }
                let full: Full =
                    serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
                Ok(Self::Full { edit: full.edit })
            }
            "previous" => {
                #[derive(Deserialize)]
                struct Previous {
                    index: u32,
                }
                let previous: Previous =
                    serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
                Ok(Self::Previous {
                    index: previous.index,
                })
            }
            other => Err(serde::de::Error::unknown_variant(
                other,
                &["full", "previous"],
            )),
        }
    }
}

/// Cancellation points recorded in the log. The run.cancel endpoint rejects all other strings
/// before writing, rather than interpreting an unknown point as immediate cancellation.
/// The failed request boundary and selection retained by an account-limit pause. The log
/// sequence identifies its failed physical attempt even when no billing reservation is recorded.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AccountLimitPause {
    pub failed_step: StepId,
    pub failed_attempt_seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub attempt_id: Option<AttemptId>,
    pub attempt: u32,
    pub paused_at_ms: u64,
    pub observed_at_ms: u64,
    pub auth_method: String,
    pub credential_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub credential_ref: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_ref: Option<String>,
    pub disposition: broca_protocol::provider::AccountLimitDisposition,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub resets_at_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_after_secs: Option<f64>,
    pub resume_not_before_ms: u64,
    pub request_body_len: u64,
    pub request_body_sha256: String,
    pub selection_round: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_routing_check_ms: Option<u64>,
}

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

/// The schema a tool call was dispatched against: the tool's `schema_digest`, `semantics` and,
/// for a versioned tool, its major version, as its provider answered them when the session's
/// tools were frozen. A provider that has since changed the tool refuses the call instead of
/// running it against a different schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DispatchSchemaPin {
    /// The provider's digest of the tool's structural schema.
    pub schema_digest: String,
    /// The provider's version of the tool's behaviour where its schema did not change. Calls
    /// to a tool without a version name it in their `tp1` pin.
    pub semantics: u64,
    /// The major of the tool's version, when its catalog answered one. Calls then carry a
    /// `tp2` pin naming this major, and the provider runs any call of the same major whose
    /// arguments validate. Absent for an unversioned tool and for every pin written before
    /// broca read versions; a build that skips the field sends the `tp1` pin, which a
    /// versioned provider still maps to the version it shipped it under.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub major: Option<u64>,
}

/// Who executes a held call once a human approves it ([`WalRecord::ToolCallDeferred`])?
///
/// Serialized as a bare string. Only `broca` is written today; other values are reserved for
/// calls a tool provider executes itself, which share the `deferred-call/v1` feature. So a value
/// this build does not know still decodes, is KEPT verbatim and re-serializes to the string it
/// was read from, instead of failing the log as corrupt.
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

/// The question an approve hook asked about a held call, as the log keeps it: the step-transform
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
    /// Caller-owned decision fixed for the run; AuthSelection holds routing's own decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_decision_id: Option<String>,
    /// The catalog provider whose rows a run borrowed, when its model's provider is one the
    /// catalog doesn't carry (`anthropic-platform`, a company gateway): the provider its vault
    /// credentials serve. The run's facts and render config came from that provider's row, and
    /// it is billed and reported as it; `model` keeps the provider the caller named. Absent for
    /// a provider the catalog carries. A resume reads it from here and never resolves it again.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub served_provider: Option<String>,
    /// Routing's account namespace, frozen independently of the model-call names.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub account_route: Option<broca_protocol::request::AccountRoute>,
    /// Frozen operator consent to automatic same-selection recovery. Omitted on legacy runs.
    #[serde(default, skip_serializing_if = "is_false")]
    pub account_limit_pause: bool,
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
    /// The provider's frozen render config, stored as an OPAQUE blob. The session's log and
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
    /// Frozen global instructions for a mode-3 run without a fetch plan. Planned
    /// runs use their own manifest.system_text; legacy runs keep their input layout.
    /// The compaction-v2 feature protects rollback execution while older readers
    /// ignore this optional member and continue to read only transcript input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_system: Option<Arc<Vec<Message>>>,
}

impl RunConfig {
    /// The provider a run is billed and reported as: the catalog provider whose rows it
    /// borrowed (`served_provider`), else its model's provider.
    pub fn billed_provider(&self) -> &str {
        self.served_provider
            .as_deref()
            .unwrap_or(self.model.provider_module_id.as_str())
    }
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

/// Setup's immutable claim check for a view required before the first model request.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SetupViewDue {
    pub version: u64,
    pub digest: String,
}

/// The credential a run resolved at start, frozen for resume. A struct (not a bare string)
/// so a later frozen field (e.g. a display name) has a home without another `RunConfig`
/// field. Carries NO secret — the credential id is an opaque routing key; the bearer handle
/// it maps to lives only in the 0600 handle map, never in the log.
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
    /// read from the current build: a later change to broca's defaults must not silently rewrite
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
    /// Billing-only metadata, frozen at admission or an auth-recovery credential change.
    /// Ordinary resume and same-credential refresh retain it without backfilling.
    /// Requires `bill-vault-facts/v1` so an older writer cannot discard the snapshot on rollback.
    /// Boxed to keep the no-snapshot cost to one pointer in run and replay state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub billing_vault_facts: Option<Box<VaultBillingFacts>>,
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
    /// The content-free reference to the account and credential routing admitted the run on
    /// (llm-runner's `step_events::account_ref`), which the run's step events carry. Present only
    /// when routing chose the account; never the raw ids, which can carry account labels.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub routed_ref: Option<String>,
}

/// Origin of an admission's routing affinity. Unknown strings survive log replay.
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
    /// operation name was added; absent on older records, which were all `credential.get`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub op: Option<VaultReadOp>,
}

/// The vault read operation a [`VaultRequest`] was sent as. Serialized as a bare string:
/// `"get"`, `"get_scoped"`, or, for [`VaultReadOp::Unknown`], the string it was read from.
///
/// THIS SITS ON A LOG RECORD (`RunStarted`, through `AuthSelection.vault_request`), so it must
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
    /// Whether this dispatch outcome is ambiguous — i.e. the request may have run
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
    /// of failing the log as corrupt; it is never written.
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

/// Rendering authority frozen on an individual riding send. Legacy deliveries omit the
/// envelope and retain their historical renderer, even in a newly started run.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RideMetadata {
    pub call_key: String,
    pub send_id: String,
    pub sender: String,
    /// Agent attribution travels with the ride so bounded reads need no earlier queue record.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub speaks_for: Option<broca_protocol::SpeaksFor>,
    pub forms: Vec<String>,
    pub form: RideForm,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RideForm {
    Append,
}
