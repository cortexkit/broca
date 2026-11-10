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

pub use broca_wal_reader::vault_facts::{
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

/// An accepted mode-3 view. Edits stay raw JSON so provider extensions, key order and number
/// spellings survive a reader round trip without depending on the compaction library.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CompactionViewV2 {
    pub version: u64,
    pub checksum: String,
    pub edits: Vec<StoredViewEdit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub built_through: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub new_signed: Option<String>,
    /// The applying module attested for the provider's route, never request data.
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
// The gap between the largest variant (`RunStarted`, with its frozen config and scope) and the
// small ones exceeds clippy's threshold on the Windows target, where some of those fields are
// larger. Boxing `RunStarted` would touch every constructor of the most-written record; the
// cost it would save is the full enum size held per record during a whole-log replay, which
// predates this allow and belongs to the replay redesign, not to a field-by-field patch.
#[allow(clippy::large_enum_variant)]
// cost it would save is the full enum size held per record during a whole-log replay. That
// memory tradeoff needs a replay-wide change, not a field-by-field patch.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(remote = "Self", tag = "type", rename_all = "snake_case")]
pub enum WalRecord {
    /// Durable attribution of data removed by overhaul recovery. Readers must
    /// retain these call tombstones even when a later episode starts.
    RecoveryCut {
        #[serde(flatten)]
        recovery: Box<crate::recovery::RecoveryCut>,
    },
    /// A prompt-free Unknown terminal for an attributed call whose model turn
    /// was removed. Ownership comes from preserved recovery evidence, never a
    /// reconstructed RunStarted or the episode current when this is appended.
    /// Unknown batch/step identities remain separate entries. When their exact
    /// effect cannot be proven, duplicate Unknown results are safer than losing
    /// an attributable call by merging it with another step's same-ID terminal.
    RecoveryCallClosed {
        cut_id: String,
        run_id: RunId,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        batch_id: Option<BatchId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step_id: Option<StepId>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        call_key: Option<String>,
        tool_call_id: ToolCallId,
        outcome: crate::recovery::RecoveryOutcome,
        /// A plain outcome_unknown error, never restored media or successful output.
        #[serde(
            serialize_with = "serialize_recovery_unknown",
            deserialize_with = "deserialize_recovery_unknown"
        )]
        result: Arc<ToolResult>,
    },
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
        /// Display-only marks, absent on older sends and outside their stored bodies.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_marks: Option<broca_protocol::MessageMarks>,
        /// Attested route sender for display only, independent of the execution principal.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sent_by: Option<BindPrincipal>,
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
        /// only in a gated frame that requires [`crate::features::SCOPE_FEATURE`] (see
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
        /// The compaction render-plan version frozen for a mode-3 run. Reader support ships
        /// before writing: this release decodes but does not apply it. When present it requires
        /// `compaction-v2/v1`, so a build without the reader refuses the log by feature name.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        compaction_plan_version: Option<u32>,
        /// The tool catalogs and system text the session's first send fetched from its
        /// providers, frozen and repeated on every later episode, because a fold can take the
        /// first `RunStarted` out of the live WAL and every episode must render from the same
        /// tools and text without fetching again.
        ///
        /// MUST-UNDERSTAND. A reader that ignored it would render the session without its
        /// frozen tools and system text, so a record carrying it is written only in a gated
        /// frame that requires [`crate::features::PLAN_MANIFEST_FEATURE`] (see
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
        /// Provider-native terminal token for display only; absent on older records.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        raw_finish_reason: Option<String>,
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
        /// Source classification retained only on runs admitted with timed account recovery.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        account_limit_disposition: Option<broca_protocol::provider::AccountLimitDisposition>,
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
        /// Informational request-prefix evidence. Unknown members remain safe to ignore.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        cache_evidence: Option<Box<serde_json::Value>>,
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
    /// Written when a run cannot proceed but remains resumable: authentication, restart,
    /// held tools or a timed account limit. Replay continues the same run_id after the cause
    /// clears; no new episode starts until the pause resolves or is cancelled.
    RunPaused {
        run_id: RunId,
        /// Wall-clock pause time, used as the terminal time when this is the final record.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        ts_ms: Option<u64>,
        reason: PauseReason,
        /// The error detail behind the pause, persisted for `run.status`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        error: Option<ProviderError>,
        /// Recovery evidence for a timed account pause; absent on every older pause.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        account_limit: Option<Box<AccountLimitPause>>,
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
    /// A mid-session plan change the owner accepted, durable before its reply. The next run
    /// that reaches its rung's point freezes this manifest on its `RunStarted` and applies it;
    /// earlier episodes keep resuming from their own manifests, byte-identical. An identical
    /// repeat (same generation, plan identity and policy) is answered from the durable record
    /// without fetching again.
    ///
    /// MUST-UNDERSTAND (feature `session-change/v1`). A reader without it would lose the
    /// pending generation, apply the wrong tools on the next run, or answer a repeat as stale.
    /// The actor syncs this record before returning the original reply flags.
    SessionRefreshAccepted {
        /// The owner's generation, strictly increasing per session.
        generation: u64,
        /// One rung name from the runner role's `rungs::ALL`.
        policy: String,
        /// Whether a run was active when this change was accepted. Kept for identical replies.
        run_in_progress: bool,
        /// Whether the next run could apply this change at acceptance, kept for identical replies.
        applies_from_next_run: bool,
        /// The refreshed plan's fetched manifest, frozen as at admission.
        manifest: Box<PlanManifest>,
    },
    /// A new rung for the pending change, durable before its reply. Idempotent on `send_id`:
    /// a repeat with the same key and the same generation and policy is answered from the
    /// durable record without writing again.
    ///
    /// MUST-UNDERSTAND (feature `session-change/v1`), for the same reason as
    /// `SessionRefreshAccepted`. The actor syncs it before returning success.
    SessionRefreshPolicyChanged {
        /// The generation the caller believes is pending.
        generation: u64,
        /// The pending change's new rung.
        policy: String,
        /// The caller's idempotency key.
        send_id: String,
    },
    /// A prefix flush, durable before its reply. The next run applies any pending change,
    /// whatever its policy, and drops the frozen prefix. Idempotent on `send_id`.
    ///
    /// MUST-UNDERSTAND (feature `session-change/v1`), for the same reason as
    /// `SessionRefreshAccepted`. The actor syncs it before returning success.
    SessionPrefixFlushed {
        /// The caller's idempotency key.
        send_id: String,
        /// The generation that was pending when the flush was accepted, if any.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        pending_generation: Option<u64>,
    },

    // ── durable submission queue (survives a crash, unlike a runtime-only queue) ──
    /// A prompt durably queued while a run was active. Carries the submission
    /// IDENTITY plus the caller's full REQUEST (not just the message) so a queued-then-
    /// drained submission runs with the SAME config it would have had if sent when idle
    /// (model, tools, generation, etc.) — the queue is a pure deferral, never a config-
    /// dropping path. Consume/retract is unambiguous across a crash: the pending set is
    /// `Queued − Retracted − consumed-by-a-RunStarted`.
    Queued {
        /// Display-only marks retained while the admitted send waits for delivery.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_marks: Option<broca_protocol::MessageMarks>,
        /// Attested route sender for display only; never grants queue ownership.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sent_by: Option<BindPrincipal>,
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
    /// gated frame requiring [`crate::features::STEER_QUEUE_FEATURE`] (see
    /// [`WalRecord::required_features`]), which such a build refuses as a newer writer.
    Steered {
        /// Display-only marks of the send, not part of its rendered input.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        message_marks: Option<broca_protocol::MessageMarks>,
        /// Attested route sender for display only; never used to authorize a retry.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        sent_by: Option<BindPrincipal>,
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

    /// Written once by every resume of an episode that has no durable terminal, including
    /// after a pause, before any step, tool intent, result or terminal the resume writes.
    /// Only the resume's own `RunConfigRefrozen` and `RunAuthRefrozen` may precede it: the
    /// background cache warmer reads this record as "the run is active again" and must already
    /// have applied the refrozen configuration before sending a warm request. The record says
    /// which episode resumed, from which
    /// durable record, what it rebuilt from the log without calling a provider or a tool, what
    /// it is about to do again, and which tool calls it found with no recorded outcome.
    ///
    /// Replay, the reconstructed
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
    /// model request shaped to refresh the cached prefix. Legacy roots write this only for a
    /// successful warm. Roots using `bill-attempts/v1` normally write one billing terminal per
    /// reserved physical warm send, including failures, through the session's own writer;
    /// power loss or a full WAL can leave only the reservation, which reclaim exposes as possibly
    /// billed. Only a successful warm restarts cache expiry; an unsuccessful terminal is evidence
    /// of an attempt, not a refresh.
    ///
    /// PURELY INFORMATIONAL under the skip contract on this enum: replay, the reconstructed
    /// prompt, projection and the control stream skip it exactly as they skip
    /// [`WalRecord::Unknown`]. It is never a transcript message. Records carrying billing
    /// evidence require `bill-attempts/v1`; legacy records remain ungated.
    /// Legacy billing turns each successful warm into an export fact on the run whose request
    /// it re-sent. Overhaul billing readers use the reservation and envelope, preserving absent
    /// usage as unknown and joining all evidence by attempt_id.
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
        /// Setup's declared view features. Absent on older lineages, whose edited bytes
        /// remain final. Requires `compaction-view-negotiation/v1` when present.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        view_features: Option<Vec<String>>,
        /// A published view that must apply before any model request in this lineage.
        /// Requires `compaction-view-negotiation/v1` when present.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        view_due: Option<SetupViewDue>,
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

    /// A mode-3 view delta, reserved for a later writer. This reader preserves the provider's
    /// JSON edits and checksum without interpreting them or changing the transcript.
    CompactionViewV2(CompactionViewV2),
    /// A mode-3 cut marker, reserved for a later writer. This reader ignores it in all session
    /// and run projections; its coordinates never renumber transcript messages.
    CompactionMarker {
        harness: String,
        session: String,
        view_version: u64,
        first_kept_mid: String,
        first_kept_ordinal: u64,
        checkpoint: u64,
    },
    /// The accepted mode-3 view selected for a step, including a view the compaction provider
    /// returned for that request. Content lives in `CompactionViewV2`; this reader applies neither record.
    CompactionViewAppliedV2 {
        step_id: u64,
        version: u64,
        checksum: String,
    },

    // ── calls held for a human's approval (feature `deferred-call/v1`) ──
    //
    // Every record in this group except `QuestionWithdrawalAcknowledged` is must-understand and is
    // only ever written in a gated frame requiring `crate::features::DEFERRED_CALL_FEATURE` (see
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
    // `crate::features::PROVIDER_HELD_FEATURE`, as is a `RunPaused` with `PauseReason::ToolHeld`.
    // An older build that skipped this record would see a dispatched call without a result,
    // class it indeterminate and seal the run Interrupted, losing a result the provider still
    // owes.
    /// A tool provider accepted the call and answered with a `held` marker (tool-provider
    /// contract §4.1, `held-reply/v1`): the real result arrives later as a `late_results` entry
    /// for `call_key` (§7). The writer must append and sync it before acting on anything else
    /// in the reply. The daemon declares held replies in every build and recovers these
    /// obligations through its late-results runtime.
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
    // requiring `crate::features::DELIVERY_ORDER_FEATURE`: an older build that skipped one would
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
        ride: Option<RideMetadata>,
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

impl Serialize for WalRecord {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        Self::serialize(self, serializer)
    }
}

impl<'de> Deserialize<'de> for WalRecord {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = Box::<serde_json::value::RawValue>::deserialize(deserializer)?;
        #[derive(Deserialize)]
        struct Tag {
            #[serde(rename = "type")]
            kind: String,
        }
        let tag: Tag = serde_json::from_str(raw.get()).map_err(serde::de::Error::custom)?;
        if tag.kind == "compaction_view_v2" {
            serde_json::from_str(raw.get())
                .map(Self::CompactionViewV2)
                .map_err(serde::de::Error::custom)
        } else {
            // Keep the existing tagged decoder for every older record shape. The raw-view
            // branch bypasses its value buffer, which would discard the edits' original bytes.
            Self::deserialize(&mut serde_json::Deserializer::from_str(raw.get()))
                .map_err(serde::de::Error::custom)
        }
    }
}

fn is_recovery_unknown(result: &ToolResult) -> bool {
    result.is_error
        && matches!(&result.output, broca_protocol::ToolOutput::Error {
        message, module, code: _, detail, nothing_sent, retry,
    } if message.starts_with("outcome_unknown") && module.is_none() && detail.is_none()
        && nothing_sent.is_none() && retry.is_none())
}

fn serialize_recovery_unknown<S: serde::Serializer>(
    result: &Arc<ToolResult>,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    if !is_recovery_unknown(result) {
        return Err(<S::Error as serde::ser::Error>::custom(
            "recovery completion result must be a plain outcome_unknown error",
        ));
    }
    result.serialize(serializer)
}

fn deserialize_recovery_unknown<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Arc<ToolResult>, D::Error> {
    let result = Arc::<ToolResult>::deserialize(deserializer)?;
    if !is_recovery_unknown(&result) {
        return Err(<D::Error as serde::de::Error>::custom(
            "recovery completion result must be a plain outcome_unknown error",
        ));
    }
    Ok(result)
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
            cache_evidence,
            websocket_close_code,
            input_transformations,
            input_transformations_unmeasured,
            credential_refresh,
            served_account_ref,
            // In-memory account evidence for the routing reports; never part of the record.
            account_observation: _,
        } = attempt;
        WalRecord::ModelAttemptFinished {
            account_limit_disposition: None,
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
            cache_evidence,
            websocket_close_code,
            input_transformations,
            input_transformations_unmeasured,
            credential_refresh: credential_refresh.map(|refresh| *refresh),
            served_account_ref,
        }
    }

    /// Whether conversation and admission readers skip this record: a type from a newer build
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
                | WalRecord::CompactionViewV2 { .. }
                | WalRecord::CompactionMarker { .. }
                | WalRecord::CompactionViewAppliedV2 { .. }
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
        if matches!(self, Self::CompactionViewV2(view)
            if view.built_through.is_some() || view.new_signed.is_some() || view.applied_by.is_some())
        {
            return &[
                crate::features::COMPACTION_V2_FEATURE,
                crate::features::COMPACTION_VIEW_META_FEATURE,
            ];
        }
        if matches!(
            self,
            Self::RecoveryCut { .. } | Self::RecoveryCallClosed { .. }
        ) {
            return &[crate::recovery::FEATURE];
        }
        if matches!(self, Self::VisibilityLate { .. }) {
            return &[crate::head_proof::VISIBILITY_LATE_FEATURE];
        }
        if matches!(self, Self::WriterOpened { .. } | Self::WriterClosed { .. }) {
            return &[crate::features::WRITER_ID_FEATURE];
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
            with_feature(base, crate::features::MEDIA_CONTENT_FEATURE)
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
            with_feature(base, crate::features::INTERRUPT_REASON_FEATURE)
        } else {
            base
        };
        let has_directives = match self {
            WalRecord::RunStarted { config, .. } => config.turn_directives_v1.is_some(),
            WalRecord::Queued { request, .. } => request.directives.is_some(),
            _ => false,
        };
        let base = if has_directives {
            with_feature(base, crate::features::TURN_DIRECTIVES_FEATURE)
        } else {
            base
        };
        // A send's client_message_id lives in its identity digest (see
        // `SubmissionRequest::client_message_id`). An older build would read the digest without
        // it and answer a re-send under the same `send_id` with another id as a plain retry,
        // where this build refuses it `send_id_reuse`, so the record is must-understand.
        // A manifest that froze any provider declaration is must-understand: an older reader
        // would skip the fields and read replies by a catalog that may have changed since.
        let manifest_for_declarations = match self {
            WalRecord::RunStarted {
                manifest: Some(manifest),
                ..
            }
            | WalRecord::SessionRefreshAccepted { manifest, .. } => Some(manifest.as_ref()),
            _ => None,
        };
        let base = if manifest_for_declarations.is_some_and(|manifest| {
            manifest.fetches.iter().any(|fetch| {
                matches!(
                    &fetch.outcome,
                    FetchOutcome::Answered {
                        declarations_frozen: Some(declarations),
                        ..
                    } if !declarations.is_empty()
                )
            })
        }) {
            with_feature(base, crate::features::PROVIDER_DECLARATIONS_FEATURE)
        } else {
            base
        };
        let base = if manifest_for_declarations.is_some_and(|manifest| {
            manifest.fetches.iter().any(|fetch| {
                matches!(
                    &fetch.outcome,
                    FetchOutcome::Answered {
                        declarations_frozen: Some(declarations),
                        ..
                    } if declarations.schema_pins.is_some()
                )
            })
        }) {
            with_feature(base, crate::features::TOOL_SCHEMA_PINS_FEATURE)
        } else {
            base
        };
        let base = if matches!(self, Self::Queued { request, .. } if request.service_tier_level.is_some())
        {
            with_feature(base, crate::features::SERVICE_TIER_QUEUE_FEATURE)
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
            with_feature(base, crate::features::CLIENT_MESSAGE_ID_FEATURE)
        } else {
            base
        };
        let has_marks = match self {
            Self::RunStarted { message_marks, .. }
            | Self::Queued { message_marks, .. }
            | Self::Steered { message_marks, .. } => message_marks.is_some(),
            Self::SendDelivered {
                ride: Some(ride), ..
            } => ride.speaks_for.is_some(),
            _ => false,
        };
        let base = if has_marks || identity_digest.is_some_and(digest_carries_message_marks) {
            with_feature(base, crate::features::MESSAGE_MARKS_FEATURE)
        } else {
            base
        };
        // A speaker's display name is gated apart from the marks themselves: a reader that knows
        // marks but not names would decode the mark and silently drop the name.
        let named_speaker = match self {
            Self::RunStarted { message_marks, .. }
            | Self::Queued { message_marks, .. }
            | Self::Steered { message_marks, .. } => message_marks
                .as_ref()
                .and_then(|marks| marks.speaks_for.as_ref())
                .is_some_and(|speaker| speaker.name.is_some()),
            Self::SendDelivered {
                ride: Some(ride), ..
            } => ride
                .speaks_for
                .as_ref()
                .is_some_and(|speaker| speaker.name.is_some()),
            _ => false,
        };
        let base = if named_speaker || identity_digest.is_some_and(digest_names_a_speaker) {
            with_feature(base, crate::features::SPEAKS_FOR_NAME_FEATURE)
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
            with_feature(base, crate::features::CHATGPT_CODEX_WIRE_FEATURE)
        } else {
            base
        };
        let base = if self.carries_provider_text_state() {
            with_feature(base, crate::features::PROVIDER_TEXT_STATE_FEATURE)
        } else {
            base
        };
        let base = if self.carries_provider_fallback() {
            with_feature(base, crate::features::PROVIDER_FALLBACK_BLOCK_FEATURE)
        } else {
            base
        };
        let base = if self.carries_refusal_fallback() {
            with_feature(base, crate::features::REFUSAL_FALLBACK_FEATURE)
        } else {
            base
        };
        let frozen = match self {
            Self::RunStarted { config, .. } => Some(&config.frozen_render_config),
            Self::RunConfigRefrozen {
                frozen_render_config,
                ..
            } => Some(frozen_render_config),
            _ => None,
        };
        let aliases = |config: &Value| {
            config
                .get("tool_name_aliases")
                .and_then(Value::as_object)
                .is_some_and(|map| !map.is_empty())
        };
        let base = if frozen.is_some_and(|config| {
            aliases(config)
                || config
                    .pointer("/refusal_fallback/frozen")
                    .is_some_and(aliases)
        }) {
            with_feature(base, crate::features::TOOL_NAME_ALIASES_FEATURE)
        } else {
            base
        };
        let base = if self.carries_openrouter_reasoning() {
            with_feature(base, crate::features::OPENROUTER_REASONING_FEATURE)
        } else {
            base
        };
        let base = if self.carries_structured_output() {
            with_feature(base, crate::features::STRUCTURED_OUTPUT_FEATURE)
        } else {
            base
        };
        let base = if self.carries_structured_output_paragraph_break() {
            with_feature(base, crate::features::STRUCTURED_OUTPUT_PARAGRAPH_FEATURE)
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
            with_feature(base, crate::features::COMPACTION_VIEW_FEATURE)
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
            with_feature(base, crate::features::COMPACTION_RIDE_WAIT_FEATURE)
        } else {
            base
        };
        let base = if matches!(
            self,
            WalRecord::CompactionMessage {
                view_features: Some(_),
                ..
            } | WalRecord::CompactionMessage {
                view_due: Some(_),
                ..
            }
        ) {
            with_feature(base, crate::features::COMPACTION_VIEW_NEGOTIATION_FEATURE)
        } else {
            base
        };
        let base = if self.carries_runner_tags() {
            with_feature(base, crate::features::RUNNER_TAGS_FEATURE)
        } else {
            base
        };
        let computed_tags = match self {
            WalRecord::RunStarted { config, .. } => config.computed_tags.is_some(),
            WalRecord::CompactionMessage { tag_checkpoint, .. } => tag_checkpoint.is_some(),
            _ => false,
        };
        let base = if computed_tags {
            with_feature(base, crate::features::RUNNER_TAGS_V2_FEATURE)
        } else {
            base
        };
        let base = if self.carries_tool_reply() {
            with_feature(base, crate::features::TOOL_REPLY_FEATURE)
        } else {
            base
        };
        let base = if self.carries_auth_selection_method() {
            with_feature(base, crate::features::AUTH_SELECTION_METHOD_FEATURE)
        } else {
            base
        };
        let base = if self.carries_reference_slots() {
            with_feature(base, crate::features::PLAN_REFERENCE_SLOTS_FEATURE)
        } else {
            base
        };
        let base = if self.carries_google_rpc_error_details() {
            with_feature(base, crate::features::GOOGLE_RPC_ERROR_DETAILS_FEATURE)
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
            with_feature(base, crate::features::MESSAGE_TIMES_FEATURE)
        } else {
            base
        };
        let base = if self.carries_billing_attempt() {
            with_feature(base, crate::features::BILL_ATTEMPTS_FEATURE)
        } else {
            base
        };
        let base = if self.carries_billing_vault_facts() {
            with_feature(base, crate::features::BILL_VAULT_FACTS_FEATURE)
        } else {
            base
        };
        let base = if matches!(
            self,
            Self::RunPaused {
                reason: PauseReason::AccountLimit,
                ..
            } | Self::RunPaused {
                account_limit: Some(_),
                ..
            }
        ) || matches!(self, Self::RunStarted { config, .. } if config.account_limit_pause)
            || matches!(
                self,
                Self::ModelAttemptFinished {
                    account_limit_disposition: Some(_),
                    ..
                }
            ) {
            with_feature(base, crate::features::ACCOUNT_LIMIT_PAUSE_FEATURE)
        } else {
            base
        };
        let base = if self.carries_turn_system() {
            with_feature(base, crate::features::TURN_SYSTEM_FEATURE)
        } else {
            base
        };
        let base = if matches!(
            self,
            Self::RunStarted {
                compaction_plan_version,
                config,
                ..
            } if compaction_plan_version.is_some() || config.global_system.is_some()
        ) {
            with_feature(base, crate::features::COMPACTION_V2_FEATURE)
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
            with_feature(base, crate::features::TOOL_TIMES_FEATURE)
        } else {
            base
        }
    }

    fn carries_billing_vault_facts(&self) -> bool {
        let selection = match self {
            Self::RunStarted { config, .. } => config.auth_selection.as_ref(),
            Self::RunAuthRefrozen { auth_selection, .. } => Some(auth_selection),
            Self::AttemptReserved { reservation, .. }
            | Self::AttemptFinished { reservation, .. }
            | Self::AttemptReleased { reservation, .. }
            | Self::AttemptCorrected { reservation, .. } => {
                reservation.credential.auth_selection.as_ref()
            }
            Self::ModelAttemptFinished { reservation, .. }
            | Self::CacheWarmed { reservation, .. } => reservation
                .as_ref()
                .and_then(|reservation| reservation.credential.auth_selection.as_ref()),
            _ => None,
        };
        selection.is_some_and(|selection| selection.billing_vault_facts.is_some())
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

    /// A `RunStarted` or `SessionRefreshAccepted` whose plan manifest froze a provider's
    /// catalog reference slots.
    fn carries_reference_slots(&self) -> bool {
        let manifest = match self {
            WalRecord::RunStarted {
                manifest: Some(manifest),
                ..
            }
            | WalRecord::SessionRefreshAccepted { manifest, .. } => manifest.as_ref(),
            _ => return false,
        };
        manifest.fetches.iter().any(|fetch| {
            matches!(
                fetch.outcome,
                FetchOutcome::Answered {
                    reference_slots: Some(_),
                    ..
                }
            )
        })
    }

    /// Whether a tool this record freezes or queues declares its own reply deadline, which an
    /// older reader would drop while decoding the tool.
    fn carries_tool_reply(&self) -> bool {
        let declares = |tools: &[Tool]| tools.iter().any(|tool| tool.reply.is_some());
        match self {
            WalRecord::RunStarted { config, .. } => declares(&config.tools),
            WalRecord::Queued { request, .. } => request.tools.as_deref().is_some_and(declares),
            WalRecord::SessionRefreshAccepted { manifest, .. } => declares(&manifest.tools),
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
            | Self::SessionRefreshAccepted { .. }
            | Self::SessionRefreshPolicyChanged { .. }
            | Self::SessionPrefixFlushed { .. }
            | Self::CompactionRequest { .. }
            | Self::CompactionAnswer { .. }
            | Self::CompactionWait { .. }
            | Self::CompactionRideWait { .. }
            | Self::CompactionRideWaitEnded { .. }
            | Self::CompactionNotice { .. }
            | Self::CompactionViewV2 { .. }
            | Self::CompactionMarker { .. }
            | Self::CompactionViewAppliedV2 { .. }
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
            | Self::RecoveryCut { .. }
            | Self::RecoveryCallClosed { .. }
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

    fn carries_turn_system(&self) -> bool {
        let messages = |input: &[Message]| input.iter().any(|message| message.turn_system);
        let hooks_carry = |hooks: &Option<Box<Hooks>>| {
            hooks.as_ref().is_some_and(|hooks| {
                hooks
                    .original
                    .iter()
                    .chain(hooks.final_value.iter())
                    .any(|value| {
                        value.as_array().is_some_and(|messages| {
                            messages.iter().any(|message| {
                                message
                                    .get("turn_system")
                                    .and_then(serde_json::Value::as_bool)
                                    == Some(true)
                            })
                        })
                    })
            })
        };
        match self {
            Self::RunStarted { input, hooks, .. } | Self::Steered { input, hooks, .. } => {
                messages(input) || hooks_carry(hooks)
            }
            Self::Queued { request, hooks, .. } => messages(&request.input) || hooks_carry(hooks),
            Self::SendDelivered { input, mode, .. } => {
                *mode == broca_protocol::DeliveryMode::TurnSystem
                    || input.as_deref().is_some_and(|input| messages(input))
            }
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
        use crate::features::{
            DEFERRED_CALL_FEATURE, DISPATCH_MODULE_FEATURE, FLOW_SCOPE_FEATURE,
            PLAN_MANIFEST_FEATURE, RESTART_PAUSE_FEATURE, RETENTION_FEATURE, SCOPE_FEATURE,
            STEER_QUEUE_FEATURE, STEP_TRANSFORMS_FEATURE,
        };
        match self {
            WalRecord::CompactionRideWait { .. } | WalRecord::CompactionRideWaitEnded { .. } => {
                &[crate::features::COMPACTION_RIDE_WAIT_FEATURE]
            }
            WalRecord::CompactionViewV2 { .. }
            | WalRecord::CompactionMarker { .. }
            | WalRecord::CompactionViewAppliedV2 { .. } => {
                &[crate::features::COMPACTION_V2_FEATURE]
            }
            WalRecord::ContextCeilingWait { .. }
            | WalRecord::ContextCeilingDecided { .. }
            | WalRecord::RunFinished {
                reason: RunFinishReason::ContextCeiling,
                ..
            } => &[crate::features::CONTEXT_CEILING_FEATURE],
            WalRecord::StructuredOutputDowngraded { .. } => {
                &[crate::features::STRUCTURED_OUTPUT_DOWNGRADE_FEATURE]
            }
            WalRecord::OutputReservationClamped { .. } => {
                &[crate::features::OUTPUT_RESERVATION_FEATURE]
            }
            WalRecord::RunCancelRequested { .. } => &[crate::features::RUN_CANCEL_AT_FEATURE],
            WalRecord::CompactionRequest { .. }
            | WalRecord::CompactionMessage { .. }
            | WalRecord::CompactionAnswer { .. } => &[crate::features::COMPACTION_FEATURE],
            WalRecord::CompactionWait { .. } => &[crate::features::COMPACTION_WAIT_FEATURE],
            WalRecord::CompactionViewApplied { .. } | WalRecord::CompactionNotice { .. } => {
                &[crate::features::COMPACTION_VIEW_FEATURE]
            }
            WalRecord::RunPaused {
                reason: PauseReason::Restart,
                ..
            } => &[RESTART_PAUSE_FEATURE],
            WalRecord::RunPaused {
                reason: PauseReason::ToolHeld,
                ..
            }
            | WalRecord::ToolCallProviderHeld { .. } => &[crate::features::PROVIDER_HELD_FEATURE],
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
            WalRecord::SendDelivered { ride: Some(_), .. } => &[
                crate::features::DELIVERY_ORDER_FEATURE,
                crate::features::RIDES_V2_FEATURE,
            ],
            WalRecord::DeliveryOrdered { .. }
            | WalRecord::SendEnded { .. }
            | WalRecord::SendDelivered { .. } => &[crate::features::DELIVERY_ORDER_FEATURE],
            WalRecord::SessionRefreshAccepted { .. } => &[
                crate::features::PLAN_MANIFEST_FEATURE,
                crate::features::SESSION_CHANGE_FEATURE,
            ],
            WalRecord::SessionRefreshPolicyChanged { .. }
            | WalRecord::SessionPrefixFlushed { .. } => &[crate::features::SESSION_CHANGE_FEATURE],
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
/// The failed request boundary and selection retained by an account-limit pause. The WAL
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

/// Every feature name `base_required_features` returns for a record that reaches one of the
/// tables below, sorted: `RunStarted` (all three tables), `ToolCallDeferred` (agent-run) and
/// `Queued`, `ToolResult`, `RunFinished`, `InputTokenCapFrozen` and
/// `InputTokenBudgetExhausted` (input cap). Each later table also lists the features the
/// earlier tables add. `required_features` adds every other feature after the tables, so none
/// of them needs listing here.
const UNION_TABLE_BASE_FEATURES: [&str; 6] = [
    crate::features::DEFERRED_CALL_FEATURE,
    crate::features::FLOW_SCOPE_FEATURE,
    crate::features::PLAN_MANIFEST_FEATURE,
    crate::features::RETENTION_FEATURE,
    crate::features::SCOPE_FEATURE,
    crate::features::STEP_TRANSFORMS_FEATURE,
];
/// What a classed `RunStarted` can carry when it reaches the session-class table: the shared
/// list plus the agent-run feature that table runs after.
const CLASS_BASE_FEATURES: [&str; 7] = insert_sorted(
    UNION_TABLE_BASE_FEATURES,
    crate::features::AGENT_RUN_SCOPE_FEATURE,
);
/// What a capped record can carry when it reaches the input-cap table: the session-class
/// table's list plus the session-class feature.
const INPUT_CAP_BASE_FEATURES: [&str; 8] =
    insert_sorted(CLASS_BASE_FEATURES, crate::features::SESSION_CLASS_FEATURE);

static AGENT_RUN_FEATURE_SETS: FeatureUnionTable<6, 7, 64> = FeatureUnionTable::new(
    UNION_TABLE_BASE_FEATURES,
    crate::features::AGENT_RUN_SCOPE_FEATURE,
);
static CLASS_FEATURE_SETS: FeatureUnionTable<7, 8, 128> =
    FeatureUnionTable::new(CLASS_BASE_FEATURES, crate::features::SESSION_CLASS_FEATURE);
static INPUT_CAP_FEATURE_SETS: FeatureUnionTable<8, 9, 256> = FeatureUnionTable::new(
    INPUT_CAP_BASE_FEATURES,
    crate::features::INPUT_TOKEN_CAP_FEATURE,
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
    /// Caller-owned decision fixed for the run; AuthSelection holds routing's own decision.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub route_decision_id: Option<String>,
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
    /// Frozen global instructions for a mode-3 run without a fetch plan. Planned
    /// runs use their own manifest.system_text; legacy runs keep their input layout.
    /// The compaction-v2 feature protects rollback execution while older readers
    /// ignore this optional member and continue to read only transcript input.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub global_system: Option<Arc<Vec<Message>>>,
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
mod feature_union_tests;

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
mod ride_record_bytes {
    #[test]
    fn legacy_send_delivered_bytes_are_frozen() {
        let record = super::WalRecord::SendDelivered {
            ride: None,
            written_at_ms: Some(123),
            submission_id: broca_protocol::SubmissionId("submission".into()),
            run_id: broca_protocol::RunId("run".into()),
            mode: broca_protocol::DeliveryMode::ToolResult,
            input: None,
            tool_call_id: Some(broca_protocol::ToolCallId("call".into())),
            appended_text: Some("\n\n note\n☃ ".into()),
            tags: None,
        };
        assert_eq!(
            serde_json::to_string(&record).unwrap(),
            r#"{"type":"send_delivered","written_at_ms":123,"submission_id":"submission","run_id":"run","mode":"tool_result","tool_call_id":"call","appended_text":"\n\n note\n☃ "}"#
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

// Resume records can retain retry metadata without carrying a new marked entry.
fn digest_carries_message_marks(digest: &str) -> bool {
    #[derive(serde::Deserialize)]
    struct Probe {
        #[serde(default)]
        message_marks: Option<serde::de::IgnoredAny>,
    }
    serde_json::from_str::<Probe>(digest).is_ok_and(|probe| probe.message_marks.is_some())
}

// A retry's identity keeps the whole speaker, so a named speaker in it needs the name reader
// even when the record itself places no marked entry.
fn digest_names_a_speaker(digest: &str) -> bool {
    #[derive(serde::Deserialize)]
    struct Probe {
        #[serde(default)]
        message_marks: Option<broca_protocol::MessageMarks>,
    }
    serde_json::from_str::<Probe>(digest).is_ok_and(|probe| {
        probe
            .message_marks
            .and_then(|marks| marks.speaks_for)
            .is_some_and(|speaker| speaker.name.is_some())
    })
}

#[cfg(test)]
mod message_mark_identity_feature_tests {
    use super::*;

    #[test]
    fn identity_marks_require_the_reader_feature_without_a_new_marked_entry() {
        for bytes in [
            include_str!("../../broca-wal/tests/golden/pre_step_transforms/run_started.json"),
            include_str!("../../broca-wal/tests/golden/pre_step_transforms/queued.json"),
            include_str!("../../broca-wal/tests/golden/pre_step_transforms/steered.json"),
        ] {
            let mut value: serde_json::Value = serde_json::from_str(bytes).unwrap();
            value["request_identity_digest"] =
                serde_json::json!(r#"{"version":1,"message_marks":{"notice":true}}"#);
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert!(record
                .required_features()
                .contains(&crate::features::MESSAGE_MARKS_FEATURE));
        }
    }
}
