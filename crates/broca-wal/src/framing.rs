//! On-disk record framing.
//!
//! ```text
//! frame   := [len: u32 LE][ver: u8][seq: u64 LE][fence: u64 LE][digest: 32][payload: len]
//! record  := frame(ver=1, seq>=1, fence=lease tenure, payload=WalRecord JSON)
//! lineage := frame(ver=2, seq=0, fence=0, payload=16 opaque identity bytes)
//! gated   := frame(ver=3, seq>=1, fence=lease tenure,
//!                  payload={"requires": [feature name, ...], "record": WalRecord JSON})
//! digest  := SHA-256 over (ver ‖ seq ‖ fence ‖ payload)
//! ```
//!
//! A gated frame carries a record that a reader must understand before it may use it: a new
//! record kind, a new value in a closed set, or a new field whose meaning cannot be ignored.
//! `requires` names the features the record uses. A reader checks every name against
//! [`KNOWN_FEATURES`] BEFORE it interprets the record, and refuses the whole WAL with
//! [`DecodeStep::NewerWriter`] if any is missing. Skipping the record would silently lose it,
//! and calling it corruption would invite a repair that destroys a newer build's history.
//!
//! The digest covers `ver`, so a gated frame relabelled as an ordinary one fails its digest
//! rather than being read as a record whose requirements were never checked. Builds that
//! predate gated frames refuse `ver` 3 by name ("unsupported WAL framing version 3"), which
//! is what makes rolling back to them safe: they stop instead of skipping or misreading a record
//! a newer build wrote.
//!
//! Overhaul roots additionally use version 4, with requirements in a CRC32C-
//! protected header and a separate payload SHA-256. Its codec lives in
//! `broca-wal-reader`; legacy encode entry points keep their original bytes.
//!
//! SHA-256 (not crc32) because this is a source-of-truth log; records are
//! low-volume (off the hot path) so a strong digest is cheap. `len` is capped +
//! validated before allocation, so a corrupt length can't force a huge alloc.

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::record::WalRecord;

/// Fixed header bytes: len(4) + ver(1) + seq(8) + fence(8) + digest(32).
pub const HEADER_LEN: usize = 4 + 1 + 8 + 8 + 32;
const DIGEST_LEN: usize = 32;

/// Max payload accepted before allocation. 64 MiB — an assembled unit is far
/// smaller; this guards a corrupt `len` from forcing an unbounded alloc.
pub const MAX_PAYLOAD_LEN: u32 = 64 * 1024 * 1024;

/// A record as read off disk: its metadata + decoded body.
#[derive(Debug, Clone, PartialEq)]
pub struct FramedRecord {
    pub seq: u64,
    pub fence: u64,
    pub record: WalRecord,
}

/// Opaque durable identity stored inside a WAL independently of its record sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct WalLineageId([u8; 16]);

impl WalLineageId {
    /// Only a UUID v4 header supplies the non-repeating identity exposed as `session.head.incarnation`.
    pub fn incarnation(self) -> Option<String> {
        (self.0[6] >> 4 == 4 && self.0[8] >> 6 == 2).then(|| self.to_string())
    }
    pub(crate) fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    pub(crate) fn as_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl std::fmt::Display for WalLineageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let b = self.0;
        write!(
            f,
            "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
            b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7], b[8], b[9], b[10], b[11],
            b[12], b[13], b[14], b[15]
        )
    }
}

#[derive(Debug)]
pub enum FrameError {
    Serialize(serde_json::Error),
    Deserialize(serde_json::Error),
    /// Version 4 header or encoding failure from the shared codec.
    V4(broca_wal_reader::Error),
    /// `len` exceeded `MAX_PAYLOAD_LEN`.
    PayloadTooLarge {
        len: u32,
    },
    /// A fully-read record whose digest didn't verify — CORRUPTION (fail loud).
    DigestMismatch {
        seq: u64,
    },
    /// An incomplete frame has later intact data, so its damage is not confined
    /// to a discardable final append.
    NonTailIncompleteFrame,
    /// Unsupported framing version byte.
    UnsupportedVersion {
        ver: u8,
    },
    /// A lineage metadata frame used an invalid reserved envelope.
    InvalidLineageFrame {
        len: u32,
        seq: u64,
        fence: u64,
    },
    /// A writer asked to gate a record on a feature name outside the capability grammar
    /// (`[a-z0-9-]+/v<digits>`). Refused at write time so no frame carries a name that no
    /// reader could ever list in its known-name table.
    InvalidFeatureName {
        name: String,
    },
}

impl std::fmt::Display for FrameError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FrameError::Serialize(e) => write!(f, "serialize record: {e}"),
            FrameError::Deserialize(e) => write!(f, "deserialize record: {e}"),
            FrameError::V4(e) => write!(f, "{e}"),
            FrameError::PayloadTooLarge { len } => {
                write!(
                    f,
                    "record payload {len} exceeds the {MAX_PAYLOAD_LEN}-byte cap"
                )
            }
            FrameError::DigestMismatch { seq } => {
                write!(f, "record seq={seq} failed its digest — WAL corruption")
            }
            FrameError::NonTailIncompleteFrame => {
                write!(f, "incomplete frame before intact data")
            }
            FrameError::UnsupportedVersion { ver } => {
                write!(f, "unsupported WAL framing version {ver}")
            }
            FrameError::InvalidLineageFrame { len, seq, fence } => write!(
                f,
                "invalid WAL lineage frame (len={len}, seq={seq}, fence={fence})"
            ),
            FrameError::InvalidFeatureName { name } => write!(
                f,
                "invalid WAL feature name {name:?} (expected [a-z0-9-]+/v<number>)"
            ),
        }
    }
}

impl std::error::Error for FrameError {}

/// Framing version byte (the on-disk envelope; distinct from the record schema
/// version inside the payload).
pub const FRAMING_VERSION: u8 = 1;
const LINEAGE_FRAMING_VERSION: u8 = 2;
const LINEAGE_PAYLOAD_LEN: u32 = 16;
/// Framing version of a gated record: its payload is `{"requires": [...], "record": ...}`,
/// and a reader must know every feature name in `requires` before it interprets `record`.
pub const GATED_FRAMING_VERSION: u8 = 3;
pub const V4_FRAMING_VERSION: u8 = broca_wal_reader::frame::VERSION;
pub const INTERRUPT_REASON_FEATURE: &str = "interrupt-reason/v1";
/// Every version 4 frame names this: its header carries the `appended_ms` written immediately
/// before the write and the `writer_id` of the writer that appended it. A reader that knows the
/// version 4 layout but not this name refuses the log, so no build reads those fields as
/// anything other than what they are.
pub const FRAME_STAMPS_FEATURE: &str = "frame-stamps/v1";
pub const TURN_DIRECTIVES_FEATURE: &str = "turn-directives/v1";
/// A `RunStarted`, `Queued` or `Steered` record whose `request_identity_digest` carries the
/// send's `client_message_id` (llm-runner 0.5.1 §9.11). Part of the send's identity: a build
/// without it would answer a re-send under the same `send_id` with another id as a retry.
pub const CLIENT_MESSAGE_ID_FEATURE: &str = "client-message-id/v1";
/// A rewrite ride's provider-set wait (compaction-provider 0.5.1 §4, §8): a Setup
/// `CompactionMessage` record carrying the provider's `ride_wait_ms`, frozen with the session (a
/// build without it would give the session's rides its own short deadline), and the
/// `CompactionRideWait` / `CompactionRideWaitEnded` records of a ride whose wait was reported.
pub const COMPACTION_RIDE_WAIT_FEATURE: &str = "compaction-ride-wait/v1";
/// Media references and ordered tool parts extend closed content enums.
pub const MEDIA_CONTENT_FEATURE: &str = "media-content/v1";

/// Tool-provider declarations frozen into a session's plan manifest (a provider's per-tool
/// result format and its `late_results` session capability). A build without the feature would
/// skip the fields and then read a reply by the global catalog instead of by what the provider
/// declared when the session was admitted, so the declared shapes are must-understand. Nothing
/// requires it unless a provider declared something.
pub const PROVIDER_DECLARATIONS_FEATURE: &str = "provider-declarations/v1";

/// Catalog schema identities frozen in a plan. Readers that drop the map could
/// dispatch a resumed run without its admitted tool definition's schema pin.
pub const TOOL_SCHEMA_PINS_FEATURE: &str = "tool-schema-pins/v1";

/// The feature names this build understands in a gated frame's `requires` list.
///
/// THIS IS THE ONLY TABLE. Every reader path (replay, the anchored range read behind
/// `session.read`, projection, the control lane, fold eligibility, `wal-verify`, archive reads)
/// decodes through [`decode_next`], which consults exactly this list, so no path can accept a
/// record another path refuses.
///
/// A name is added only by the release that interprets it, and must be in a deployed reader
/// before any build writes it: a WAL written with a name missing from this list is refused
/// as [`DecodeStep::NewerWriter`]. `ck-broca --wal-known-features` prints this list so
/// placement tooling can refuse to install a build that cannot read what is already on disk.
pub const KNOWN_FEATURES: &[&str] = &[
    BILL_ATTEMPTS_FEATURE,
    crate::head_proof::VISIBILITY_LATE_FEATURE,
    "writer-id/v1",
    INTERRUPT_REASON_FEATURE,
    TURN_DIRECTIVES_FEATURE,
    AGENT_RUN_SCOPE_FEATURE,
    ARCHIVE_INDEX_V2_FEATURE,
    AUTH_SELECTION_METHOD_FEATURE,
    CHATGPT_CODEX_WIRE_FEATURE,
    CLIENT_MESSAGE_ID_FEATURE,
    COMPACTION_RIDE_WAIT_FEATURE,
    COMPACTION_VIEW_FEATURE,
    COMPACTION_WAIT_FEATURE,
    COMPACTION_FEATURE,
    CONTEXT_CEILING_FEATURE,
    DEFERRED_CALL_FEATURE,
    DELIVERY_ORDER_FEATURE,
    DISPATCH_MODULE_FEATURE,
    FLOW_SCOPE_FEATURE,
    FRAME_STAMPS_FEATURE,
    GOOGLE_RPC_ERROR_DETAILS_FEATURE,
    INPUT_TOKEN_CAP_FEATURE,
    MEDIA_CONTENT_FEATURE,
    OPENROUTER_REASONING_FEATURE,
    OUTPUT_RESERVATION_FEATURE,
    PLAN_MANIFEST_FEATURE,
    PLAN_REFERENCE_SLOTS_FEATURE,
    PROVIDER_DECLARATIONS_FEATURE,
    PROVIDER_FALLBACK_BLOCK_FEATURE,
    TOOL_SCHEMA_PINS_FEATURE,
    PROVIDER_HELD_FEATURE,
    PROVIDER_TEXT_STATE_FEATURE,
    REFUSAL_FALLBACK_FEATURE,
    RESTART_PAUSE_FEATURE,
    RETENTION_FEATURE,
    RUNNER_TAGS_FEATURE,
    RUNNER_TAGS_V2_FEATURE,
    RUN_CANCEL_AT_FEATURE,
    SCOPE_FEATURE,
    SESSION_CHANGE_FEATURE,
    SESSION_CLASS_FEATURE,
    STEER_QUEUE_FEATURE,
    STEP_TRANSFORMS_FEATURE,
    STRUCTURED_OUTPUT_FEATURE,
    STRUCTURED_OUTPUT_DOWNGRADE_FEATURE,
    STRUCTURED_OUTPUT_PARAGRAPH_FEATURE,
    TOOL_REPLY_FEATURE,
    TOOL_TIMES_FEATURE,
    MESSAGE_TIMES_FEATURE,
];

/// Physical-send reservation and settlement evidence for billing readers.
pub const BILL_ATTEMPTS_FEATURE: &str = "bill-attempts/v1";

/// A plan manifest that froze a provider's catalog reference slots (`FetchOutcome::Answered`'s
/// `reference_slots`): the arguments output references are read from (llm-runner §9.13). A
/// reader without it would decode the manifest without the slots and write later episodes'
/// `RunStarted` records without them, so it must refuse the WAL instead.
pub const PLAN_REFERENCE_SLOTS_FEATURE: &str = "plan-reference-slots/v1";

/// Google RPC quota details on a stored provider error must not be silently discarded on read.
pub const GOOGLE_RPC_ERROR_DETAILS_FEATURE: &str = "google-rpc-error-details/v1";

/// Text-part state and the frozen codec policy are required for faithful provider replay.
pub const PROVIDER_TEXT_STATE_FEATURE: &str = "provider-text-state/v1";

/// Ignoring frozen Codex policy or identity would change a resumed request.
pub const CHATGPT_CODEX_WIRE_FEATURE: &str = "chatgpt-codex-wire/v1";

/// OpenRouter-tagged reasoning and its frozen echo policy cannot be read faithfully by older
/// builds. This is distinct from text-part phase/signature state in provider-text-state/v1.
pub const OPENROUTER_REASONING_FEATURE: &str = "openrouter-reasoning/v1";

/// A step's output-token cap lowered below the run's frozen cap so that the request stays
/// inside the run's context limit (`WalRecord::OutputReservationClamped`). The cap is part of
/// the request the step sent, and a resume or a rebuild of that request (cache warm, cache
/// dump) renders it again. A reader without the feature would skip the record and render the
/// frozen cap instead: a different request from the recorded one, which the provider may refuse
/// for crossing the context window. So it must refuse the WAL.
pub const OUTPUT_RESERVATION_FEATURE: &str = "output-reservation/v1";

/// Compaction's issued fence, applied view and other answers. Missing it must refuse the
/// WAL rather than lose an applied view or allow a superseded request to apply on replay.
pub const COMPACTION_FEATURE: &str = "compaction/v1";

/// Durable compaction wait exits, including terminal intents that recovery must seal.
pub const COMPACTION_WAIT_FEATURE: &str = "compaction-wait/v1";

/// Asynchronous compaction (compaction-provider/v1 0.3): a provider view applied at a step
/// boundary together with how it was applied (as published, or with the companion strip of
/// signed reasoning), the advance notices recorded before they are published, and the ceiling
/// policy frozen from Setup. A build that skipped any of them would render a resumed request
/// without the applied view, reuse a notice sequence number, or forget the ceiling, so all
/// three are must-understand.
pub const COMPACTION_VIEW_FEATURE: &str = "compaction-view/v1";

/// Bounded ceiling waits and send decisions, including a non-error terminal state. Ignoring
/// them could repeat a wait after a crash or send a request the runner durably refused.
pub const CONTEXT_CEILING_FEATURE: &str = "context-ceiling/v1";

/// Not a WAL frame feature: no frame ever requires it, so its entry here is inert for the
/// decoder. It is listed because this table is also what `--wal-known-features` reports to
/// `scripts/place-release.sh`, which refuses to install a build whose table does not cover
/// the machine's feature floor (see `floor.rs`). The floor answers "can this build read the
/// data on this machine", archives included. A build that seals archive containers with index
/// format v2 adds this name to the floor first, so placement then refuses any older build:
/// one that cannot recognise a v2 index treats it as damaged, and so reads every such
/// container by full header scans and without the index's cross-check of member headers
/// (see `archive.rs`).
pub const ARCHIVE_INDEX_V2_FEATURE: &str = "archive-index/v2";

/// The feature a `RunStarted` carrying the session's frozen plan manifest requires
/// (`WalRecord::RunStarted::manifest`). A reader without it must refuse such a WAL: skipping the
/// field would render the session without the tools and system text it was admitted with, or
/// fetch them again and get different ones.
pub const PLAN_MANIFEST_FEATURE: &str = "plan-manifest/v1";

/// A durable graceful-restart pause preserves an unfinished episode for boot recovery.
pub const RESTART_PAUSE_FEATURE: &str = "restart-pause/v1";

/// A tool call its provider holds past its reply (tool-provider contract §4.1 and §7): the
/// `ToolCallProviderHeld` record, and a `RunPaused` whose reason is `tool_held`. Nothing else
/// requires it; the held call's later result is an ordinary `ToolResult`. A reader without it
/// must refuse such a WAL: skipping the held record would class the call indeterminate and
/// seal the run Interrupted while the provider still owes its result, and `PauseReason` is a
/// closed enum that has no other value to read `tool_held` as.
///
/// In [`KNOWN_FEATURES`] reader-first: this build's replay classes a held call as open rather
/// than indeterminate, and its runtime resumes the run when the late result arrives. No
/// production path writes either value yet, because release builds leave the `held-reply/v1`
/// route capability undeclared, so no provider can hold a call.
pub const PROVIDER_HELD_FEATURE: &str = "provider-held/v1";

/// Retention policy and queued/steered activity times affect expiry and deletion. A reader
/// must not ignore them and keep content indefinitely. Reader builds enforce retention across
/// admission, projection, reads and deletion; production sends stay off until capability rollout.
pub const RETENTION_FEATURE: &str = "retention/v1";

/// Runner tags: the per-part tag numbers and per-message origins a session's records carry,
/// and a run start frozen to render them (`RunConfig::render_tags`). A reader that skipped them
/// would serve the session's reads without tags, let a later write reuse a number, or render a
/// run's requests without the tags it was admitted to show. Every record of a session admitted
/// by a tag-writing build carries them; sessions admitted earlier keep their exact bytes.
pub const RUNNER_TAGS_FEATURE: &str = "runner-tags/v1";

/// Computed runner tags (llm-runner 0.4.0 `tags::RULES_FEATURE`): a run start frozen under the
/// rules that number parts from the transcript instead of storing a number per part
/// (`RunConfig::computed_tags`, carrying the run's starting checkpoint), and a compaction Setup
/// record carrying the provider's Setup checkpoint (`CompactionMessage::tag_checkpoint`). A
/// reader that skipped either would stamp the run's parts with stored numbers, render its
/// requests by the old rules, or number the session from the wrong anchor.
pub const RUNNER_TAGS_V2_FEATURE: &str = "runner-tags/v2";

/// Pending cancellation changes recovery's next action, so a reader may not skip it.
pub const RUN_CANCEL_AT_FEATURE: &str = "run-cancel-at/v1";

/// Labels fixed on the first send govern later admission and backup placement.
pub const SESSION_CLASS_FEATURE: &str = "session-class/v1";

/// Ignoring a session input cap would authorize paid model steps the caller forbade.
pub const INPUT_TOKEN_CAP_FEATURE: &str = "input-token-cap/v1";

/// A `RunStarted` or `RunAuthRefrozen` whose credential selection names its auth method
/// (`AuthSelection::auth_method`). Its `credential_id` is then the vault's opaque credential
/// id; a reader without this feature would parse that id for a method and resume on the wrong
/// credential or wire family, so it must refuse the log instead.
pub const AUTH_SELECTION_METHOD_FEATURE: &str = "auth-selection-method/v1";

/// A run frozen with structured output renders its schema into every request and ends when the
/// model calls `submit_result`. A reader that skipped the frozen settings would resume the run
/// without the schema or the submit tool, and would dispatch the calls a submit skipped.
pub const STRUCTURED_OUTPUT_FEATURE: &str = "structured-output/v1";

/// A schema compilation refusal changes every subsequent request. Older readers must not
/// skip the downgrade and resume sending the strict grammar the provider already refused.
pub const STRUCTURED_OUTPUT_DOWNGRADE_FEATURE: &str = "structured-output-downgrade/v1";

/// The feature a `RunStarted` or `RunConfigRefrozen` requires when its frozen render config's
/// `structured_output.instructions_paragraph_break` is true. That flag puts a blank line before
/// schema instructions appended to the system message. A reader without it would read the flag
/// as absent and resume the run without the blank line: different request bytes from the ones
/// the run was admitted with, a prompt-cache miss, and a different structured-output prompt.
pub const STRUCTURED_OUTPUT_PARAGRAPH_FEATURE: &str = "structured-output-paragraph/v1";

/// A tool declared on `session.send` that carries its own reply deadline (`Tool::reply`), frozen
/// in a `RunStarted`'s tools or held in a `Queued` request's. A reader without it must refuse
/// such a WAL: decoding the tool would silently drop the deadline, and the resumed or drained
/// run would cut that tool's long calls at the default wait and end the run interrupted.
pub const TOOL_REPLY_FEATURE: &str = "tool-reply/v1";

/// Preserve the tool timestamps subscriptions replay after a restart; older readers refuse
/// these frames rather than silently discarding the observations.
pub const TOOL_TIMES_FEATURE: &str = "tool-times/v1";

/// Preserve read-entry times across rollback; dropping them would change an already served entry.
pub const MESSAGE_TIMES_FEATURE: &str = "message-times/v1";

/// Anthropic's server-side fallback, as a step records it: the `provider_fallback` content part
/// (the handoff block, in any record that carries messages) and a `ModelStepFinished`'s
/// `served_model` or `usage_iterations`. An older build's closed content decoder cannot read the
/// part, and skipping the fields would bill the step to the requested model instead of the one
/// that served it, so all three are must-understand.
pub const PROVIDER_FALLBACK_BLOCK_FEATURE: &str = "provider-fallback-block/v1";

/// Client-side refusal fallback: the `ModelStepRefused` record and a `ModelStepFinished`'s
/// `route`, `refusal_fallback` or `refusal_after_tool_use`. A reader that skipped them would
/// lose the session's fallback schedule, attribute a fallback-answered step to the wrong model,
/// and miss the refused call's billed usage, so all are must-understand.
pub const REFUSAL_FALLBACK_FEATURE: &str = "refusal-fallback/v1";

/// The feature a `RunStarted` carrying the session's frozen scope requires
/// (`WalRecord::RunStarted::scope`). A reader without it must refuse such a WAL: skipping the
/// field would let any caller drive a session only its scope's owner may drive.
pub const SCOPE_FEATURE: &str = "scope/v1";

/// A mid-session plan change the owner accepted (`WalRecord::SessionRefreshAccepted`), its
/// policy change (`WalRecord::SessionRefreshPolicyChanged`) or a prefix flush
/// (`WalRecord::SessionPrefixFlushed`). A reader without it must refuse such a WAL: skipping
/// the records would lose the pending generation, apply the wrong tools or system text on the
/// next run, or answer an identical repeat as stale. Reader-first: this build decodes and folds
/// them; no production path writes them yet.
pub const SESSION_CHANGE_FEATURE: &str = "session-change/v1";

/// Frozen automation identity affects admission. A scope-only reader would ignore the sender
/// constraint and let the owner agent drive a carrier's session, so it must refuse this WAL.
pub const FLOW_SCOPE_FEATURE: &str = "flow-scopes/v1";

/// A scope's `run_id` and `agent_id`, fixed on `RunStarted`. Older readers must refuse these
/// records: ignoring the identifiers would admit an agent-run session as an ordinary session
/// driven by its owner, instead of a model-only session driven by the reserved Basal carrier.
pub const AGENT_RUN_SCOPE_FEATURE: &str = "agent-run-scopes/v1";

/// The feature a `Steered` record requires (`WalRecord::Steered`): a message a caller added to
/// a running turn. A reader without it must refuse such a WAL: skipping the record would drop a
/// user message from the transcript and render a different prompt than the one the run sent.
pub const STEER_QUEUE_FEATURE: &str = "steer-queue/v1";

/// The feature a `ToolDispatchIntent` naming the module its call was dispatched to, or the
/// schema it was dispatched against, requires (`WalRecord::ToolDispatchIntent::module_id` and
/// `::schema_pin`). A reader without it must refuse such a WAL: skipping the fields would
/// report a dispatched call as never dispatched, and lose which schema the provider was held to.
pub const DISPATCH_MODULE_FEATURE: &str = "dispatch-module/v1";

/// Recorded step-transform output and frozen declarations must survive rollback. A reader
/// without this feature must stop instead of continuing a session without its hook outputs.
pub const STEP_TRANSFORMS_FEATURE: &str = "step-transforms/v1";

/// The feature every record of a tool call held for a human's approval requires: the
/// `tool_call_deferred`, `approval_recorded` and `awaiting_approval` records, the three late
/// records, `ToolDispatchIntent.approval_digest` and `.call_key`, and
/// `ToolResult.withdraw_elicitation` and `.provider_held` (see
/// [`WalRecord::required_features`]). A reader without it must refuse such a WAL: skipping
/// them would dispatch a call no human approved, dispatch an approved call twice, or forget a
/// question or a provider-held call it owes a withdrawal.
///
/// Reader builds interpret these records in replay, store projection and session reads. The
/// production-writer source fence keeps them reader-only until a writer release is enabled.
pub const DEFERRED_CALL_FEATURE: &str = "deferred-call/v1";

/// The feature every record of a send with an ordered `delivery` list, a `dedupe_key` or an
/// `expires_at_ms` requires: `delivery_ordered`, `send_ended` and `send_delivered` (see
/// [`WalRecord::required_features`]). A reader without it must refuse such a WAL: skipping them
/// would start a run from a queued send that may never start one, deliver a send that was
/// replaced or expired, or render a prompt without a send that was delivered into it.
pub const DELIVERY_ORDER_FEATURE: &str = "delivery-order/v1";

/// A feature name that test builds treat as known, in addition to [`KNOWN_FEATURES`].
///
/// Tests of the paths that copy or rewrite WAL bytes (fold, unfold, archive extraction,
/// torn-tail repair, maintenance) need a gated frame this build can READ, and the shipped table
/// is empty. Those paths live in other crates, where this crate's `cfg(test)` does not apply,
/// so the name is also enabled by the `test-known-feature` cargo feature, which only
/// dev-dependencies turn on. It is never in [`KNOWN_FEATURES`], so `--wal-known-features` and
/// the placement floor never see it.
#[cfg(any(test, feature = "test-known-feature"))]
pub const TEST_KNOWN_FEATURE: &str = "test-only-known/v1";

#[cfg(any(test, feature = "test-known-feature"))]
const TEST_KNOWN_FEATURES: &[&str] = &[TEST_KNOWN_FEATURE];
#[cfg(not(any(test, feature = "test-known-feature")))]
const TEST_KNOWN_FEATURES: &[&str] = &[];

/// Feature names a test has asked the decoder to treat as UNKNOWN although [`KNOWN_FEATURES`]
/// lists them, so the test can decode as a build released before those features existed would
/// (see [`forget_feature_for_test`]).
#[cfg(any(test, feature = "test-known-feature"))]
static FORGOTTEN_FOR_TEST: std::sync::Mutex<Vec<&'static str>> = std::sync::Mutex::new(Vec::new());

/// Test builds only: make every decode in this process treat `name` as unknown until the
/// returned guard drops, as a build released before `name` existed would.
///
/// The setting is process-wide, so a test using it must be the only test in its binary (or
/// the only one decoding a WAL while the guard lives).
#[cfg(any(test, feature = "test-known-feature"))]
pub fn forget_feature_for_test(name: &'static str) -> ForgottenFeature {
    FORGOTTEN_FOR_TEST
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .push(name);
    ForgottenFeature(name)
}

/// Restores a name [`forget_feature_for_test`] hid when dropped.
#[cfg(any(test, feature = "test-known-feature"))]
pub struct ForgottenFeature(&'static str);

#[cfg(any(test, feature = "test-known-feature"))]
impl Drop for ForgottenFeature {
    fn drop(&mut self) {
        let mut forgotten = FORGOTTEN_FOR_TEST
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if let Some(at) = forgotten.iter().position(|name| *name == self.0) {
            forgotten.remove(at);
        }
    }
}

#[cfg(any(test, feature = "test-known-feature"))]
fn forgotten_for_test(name: &str) -> bool {
    FORGOTTEN_FOR_TEST
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .contains(&name)
}

#[cfg(not(any(test, feature = "test-known-feature")))]
fn forgotten_for_test(_name: &str) -> bool {
    false
}

// An optimised build is a build that could be placed. A test-only name must never make one
// accept a frame that a real build of the same source would refuse as a newer writer.
#[cfg(all(feature = "test-known-feature", not(debug_assertions)))]
compile_error!(
    "broca-wal's `test-known-feature` is for test builds only; it must not reach a release build"
);

/// The envelope a gated frame's payload is written as. Field order is part of the byte
/// layout: `requires` first, then the record.
#[derive(Serialize)]
struct GatedEnvelopeOut<'a> {
    requires: &'a [&'a str],
    record: &'a WalRecord,
}

/// Parse the envelope once, keeping the record's original JSON uninterpreted.
///
/// The record is deliberately not parsed here. A record that needs a feature this build lacks
/// may use values this build's types reject, and that failure would surface as corruption
/// instead of as the newer-writer refusal it really is.
#[derive(Deserialize)]
struct GatedRequires<'a> {
    requires: Vec<String>,
    #[serde(borrow)]
    record: &'a serde_json::value::RawValue,
}

/// Whether `name` is in the capability grammar: one or more of `[a-z0-9-]`, then `/v`, then
/// one or more ASCII digits.
fn is_feature_name(name: &str) -> bool {
    broca_wal_reader::features::is_feature_name(name)
}

fn compute_digest(ver: u8, seq: u64, fence: u64, payload: &[u8]) -> [u8; DIGEST_LEN] {
    let mut h = Sha256::new();
    h.update([ver]);
    h.update(seq.to_le_bytes());
    h.update(fence.to_le_bytes());
    h.update(payload);
    h.finalize().into()
}

/// Verify the fixed frame envelope without interpreting its version or payload.
/// Intact bytes that this reader rejects may belong to a newer writer, not a crash.
pub(crate) fn digest_verifies(buf: &[u8]) -> bool {
    if buf.get(4) == Some(&V4_FRAMING_VERSION) {
        return broca_wal_reader::frame::digest_verifies(buf);
    }
    if buf.len() < HEADER_LEN {
        return false;
    }
    let len = u32::from_le_bytes(buf[..4].try_into().unwrap()) as usize;
    let Some(total) = HEADER_LEN.checked_add(len) else {
        return false;
    };
    if buf.len() < total {
        return false;
    }
    let seq = u64::from_le_bytes(buf[5..13].try_into().unwrap());
    let fence = u64::from_le_bytes(buf[13..21].try_into().unwrap());
    compute_digest(buf[4], seq, fence, &buf[HEADER_LEN..total]).as_slice() == &buf[21..53]
}

/// Encode a record into its complete framed bytes.
///
/// A [`WalRecord::Unknown`] is refused with `FrameError::Serialize`: it holds none of the
/// original record's content, so writing it would replace a newer build's record with an
/// empty placeholder.
pub fn encode(seq: u64, fence: u64, record: &WalRecord) -> Result<Vec<u8>, FrameError> {
    encode_with_requires(seq, fence, &[], record)
}

/// Check caller-supplied bytes against the record whose features the writer will publish.
/// The encoder's exact frame proves the sequence, fence, digest, single-frame boundary and
/// required gates without assuming JSON decoding preserves every in-memory value.
pub(crate) fn validate_preencoded(
    bytes: &[u8],
    seq: u64,
    fence: u64,
    record: &WalRecord,
) -> Result<(), FrameError> {
    let expected = encode(seq, fence, record)?;
    if bytes != expected.as_slice() {
        return Err(FrameError::Serialize(
            <serde_json::Error as serde::ser::Error>::custom(
                "preencoded WAL frame differs from its sequence, fence, record or requirements",
            ),
        ));
    }
    Ok(())
}

/// Encode a record that may need features an older reader lacks.
///
/// The record's own [`WalRecord::required_features`] are always added to `requires`, so a
/// must-understand value can never land in a frame an older build reads without checking.
///
/// With nothing required this is an ordinary version-1 frame, byte for byte what every
/// deployed build reads. Otherwise it writes a version-3 frame whose payload is
/// `{"requires": [...], "record": ...}`, which a reader lacking one of the names refuses as
/// [`DecodeStep::NewerWriter`]. Names are sorted and deduplicated so the same record always
/// produces the same bytes, and each must be in the capability grammar.
pub fn encode_with_requires(
    seq: u64,
    fence: u64,
    requires: &[&str],
    record: &WalRecord,
) -> Result<Vec<u8>, FrameError> {
    let own = record.required_features();
    let requires: Vec<&str> = requires.iter().chain(own.iter()).copied().collect();
    if requires.is_empty() {
        let payload = serde_json::to_vec(record).map_err(FrameError::Serialize)?;
        return frame(FRAMING_VERSION, seq, fence, &payload);
    }
    if let Some(bad) = requires.iter().find(|name| !is_feature_name(name)) {
        return Err(FrameError::InvalidFeatureName {
            name: (*bad).to_string(),
        });
    }
    let mut names = requires;
    names.sort_unstable();
    names.dedup();
    let payload = serde_json::to_vec(&GatedEnvelopeOut {
        requires: &names,
        record,
    })
    .map_err(FrameError::Serialize)?;
    frame(GATED_FRAMING_VERSION, seq, fence, &payload)
}

/// Where a version 4 header's `requires_len` field sits: after len, ver, seq, fence,
/// appended_ms and writer_id.
const V4_REQUIRES_LEN_FIELD: std::ops::Range<usize> = 45..47;

/// Size the version 4 header would occupy, from the header alone. `None` until `buf` holds the
/// two `requires_len` bytes, so a torn write that stops short of them is never sized.
pub fn v4_header_len(buf: &[u8]) -> Option<usize> {
    let field = buf.get(V4_REQUIRES_LEN_FIELD)?;
    let requires_len = u16::from_le_bytes([field[0], field[1]]) as usize;
    broca_wal_reader::frame::FIXED_HEADER_LEN.checked_add(requires_len)
}

/// The shared version 4 codec. broca's own writer builds version 4 headers with it rather than
/// with a second copy of the layout, so a frame broca writes and a frame the published reader
/// decodes can only agree.
pub use broca_wal_reader::frame as v4;

/// Encode an overhaul-root record with caller-supplied writer metadata. This is
/// deliberately separate from legacy encoding: adding the codec must not turn
/// version 4 writes on for existing state roots.
pub fn encode_v4(
    metadata: broca_wal_reader::frame::Metadata,
    requires: &[&str],
    record: &WalRecord,
) -> Result<Vec<u8>, FrameError> {
    let own = record.required_features();
    let requires: Vec<&str> = requires.iter().chain(own.iter()).copied().collect();
    let payload = serde_json::to_vec(record).map_err(FrameError::Serialize)?;
    broca_wal_reader::frame::encode(metadata, &requires, &payload).map_err(|error| match error {
        broca_wal_reader::Error::InvalidFeatureName { name } => {
            FrameError::InvalidFeatureName { name }
        }
        error => FrameError::V4(error),
    })
}

/// A record's version 4 frame, settled except for the two fields the writer stamps.
///
/// The only way to build one is [`PreparedV4Frame::prepare`], which serializes the record with
/// the shared codec, digests it and fixes its must-understand names (the record's own plus
/// [`FRAME_STAMPS_FEATURE`]). An overhaul log writes nothing else: it never accepts caller-built
/// bytes, so every version 4 frame on disk is the codec's encoding of the record it was handed
/// with. Serializing and digesting is the costly half of framing and may run on any thread; the
/// log assembles the short header on its writer thread with the `appended_ms` sampled there and
/// its own `writer_id`.
#[derive(Debug)]
pub struct PreparedV4Frame {
    payload: Vec<u8>,
    payload_sha256: [u8; 32],
    requires: Vec<&'static str>,
    header_len: usize,
}

impl PreparedV4Frame {
    /// Serialize and digest `record`, and check that its header will encode. The header length
    /// depends on the feature names, never on the stamped values, so [`Self::frame_len`] is the
    /// exact size of the frame the log will write.
    pub fn prepare(record: &WalRecord) -> Result<Self, FrameError> {
        let payload = serde_json::to_vec(record).map_err(FrameError::Serialize)?;
        let payload_sha256: [u8; 32] = Sha256::digest(&payload).into();
        let mut requires = record.required_features().to_vec();
        requires.push(FRAME_STAMPS_FEATURE);
        let header_len = v4_header(
            broca_wal_reader::frame::Metadata {
                seq: 0,
                fence: 0,
                appended_ms: 0,
                writer_id: [0; 16],
            },
            &requires,
            payload.len(),
            payload_sha256,
        )?
        .len();
        Ok(Self {
            payload,
            payload_sha256,
            requires,
            header_len,
        })
    }

    /// The bytes the finished frame occupies on disk, for append budgets.
    pub fn frame_len(&self) -> usize {
        self.header_len + self.payload.len()
    }

    /// Every must-understand name the frame's header lists. The machine's feature floor must
    /// list them all before the frame is written.
    pub fn requires(&self) -> &[&'static str] {
        &self.requires
    }

    /// The complete frame, stamped with `metadata`. Only the log that writes it calls this.
    pub(crate) fn stamp(
        &self,
        metadata: broca_wal_reader::frame::Metadata,
    ) -> Result<Vec<u8>, FrameError> {
        let mut frame = v4_header(
            metadata,
            &self.requires,
            self.payload.len(),
            self.payload_sha256,
        )?;
        frame.extend_from_slice(&self.payload);
        Ok(frame)
    }
}

/// The shared codec's version 4 header, with its feature-name refusal mapped to the same error a
/// legacy gated frame reports.
fn v4_header(
    metadata: broca_wal_reader::frame::Metadata,
    requires: &[&str],
    payload_len: usize,
    payload_sha256: [u8; 32],
) -> Result<Vec<u8>, FrameError> {
    broca_wal_reader::frame::encode_header(metadata, requires, payload_len, payload_sha256).map_err(
        |error| match error {
            broca_wal_reader::Error::InvalidFeatureName { name } => {
                FrameError::InvalidFeatureName { name }
            }
            error => FrameError::V4(error),
        },
    )
}

/// The `appended_ms` a version 4 frame written now carries: the wall clock in milliseconds since
/// the Unix epoch, never lower than any value this process has stamped before, so the field does
/// not go backwards within one process when the wall clock does. A clock before the epoch reads
/// as 0 and is then raised to the last stamped value.
pub fn appended_ms_now() -> u64 {
    static LAST: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let wall = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        });
    let previous = LAST.fetch_max(wall, std::sync::atomic::Ordering::Relaxed);
    wall.max(previous)
}

/// Frame an already-serialized record payload, with the same header and digest as
/// [`encode`].
///
/// The engine never calls this directly; every record it writes goes through [`encode`].
/// It is public so a test can write a frame this build has no type for, such as a record
/// type added by a newer build, through the real framing rather than a copy of it.
pub fn encode_payload(seq: u64, fence: u64, payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    frame(FRAMING_VERSION, seq, fence, payload)
}

fn frame(ver: u8, seq: u64, fence: u64, payload: &[u8]) -> Result<Vec<u8>, FrameError> {
    let len =
        u32::try_from(payload.len()).map_err(|_| FrameError::PayloadTooLarge { len: u32::MAX })?;
    if len > MAX_PAYLOAD_LEN {
        return Err(FrameError::PayloadTooLarge { len });
    }
    let digest = compute_digest(ver, seq, fence, payload);

    let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
    out.extend_from_slice(&len.to_le_bytes());
    out.push(ver);
    out.extend_from_slice(&seq.to_le_bytes());
    out.extend_from_slice(&fence.to_le_bytes());
    out.extend_from_slice(&digest);
    out.extend_from_slice(payload);
    Ok(out)
}

/// Total bytes of a lineage metadata frame: the standard header plus the 16-byte identity.
///
/// Fixed by construction — the payload is a `WalLineageId`, which is exactly
/// `LINEAGE_PAYLOAD_LEN` bytes — which is what lets `encode_lineage` return an array rather
/// than allocating.
/// Fixed byte length of a lineage frame, for bounded identity checks without log replay.
pub const LINEAGE_FRAME_LEN: usize = HEADER_LEN + LINEAGE_PAYLOAD_LEN as usize;

/// Encode lineage metadata in the framing stream without consuming a record sequence number.
///
/// Returns a FIXED ARRAY rather than a `Vec` because this runs on EVERY append, not only on the
/// legacy-mint path that needs it: `file_wal.rs` calls it unconditionally and only afterwards
/// decides whether the bytes are used. A heap allocation per WAL append is pure waste for a
/// frame whose length is known at compile time.
pub(crate) fn encode_lineage(lineage_id: WalLineageId) -> [u8; LINEAGE_FRAME_LEN] {
    let payload = lineage_id.as_bytes();
    let digest = compute_digest(LINEAGE_FRAMING_VERSION, 0, 0, &payload);

    let mut out = [0u8; LINEAGE_FRAME_LEN];
    out[0..4].copy_from_slice(&LINEAGE_PAYLOAD_LEN.to_le_bytes());
    out[4] = LINEAGE_FRAMING_VERSION;
    out[5..13].copy_from_slice(&0u64.to_le_bytes());
    out[13..21].copy_from_slice(&0u64.to_le_bytes());
    out[21..53].copy_from_slice(&digest);
    out[53..69].copy_from_slice(&payload);
    out
}

/// Outcome of decoding one frame from a byte cursor.
pub enum DecodeStep<T = FramedRecord> {
    /// A complete, digest-verified record + how many bytes it consumed. Boxed: the
    /// decoded record dwarfs the other variants and this is a transient return
    /// value, so indirection keeps the enum small without a cold-path penalty.
    Record { framed: Box<T>, consumed: usize },
    /// A complete, digest-verified lineage metadata frame.
    Lineage {
        lineage_id: WalLineageId,
        consumed: usize,
    },
    /// EOF mid-frame (an incomplete trailing append) — the TORN TAIL. The caller
    /// discards from here (it was never durably committed). `available` bytes
    /// remained, fewer than needed.
    TornTail,
    /// A fully-present-but-invalid frame — CORRUPTION; the caller FAILS LOUD.
    Corrupt(FrameError),
    /// A complete, digest-verified gated frame that requires features missing from
    /// [`KNOWN_FEATURES`]: a newer build wrote it. The caller refuses the WHOLE WAL by this
    /// name. It is not corruption, so nothing may repair, truncate or fold past it, and it is
    /// not skippable, because the record may change the meaning of everything after it.
    NewerWriter {
        ver: u8,
        /// The unknown names, sorted and deduplicated.
        missing: Vec<String>,
    },
}

/// Decode the next record or lineage metadata frame starting at `buf[0]`. Distinguishes a torn
/// tail (incomplete final append → discard) from corruption (a complete frame that fails its
/// digest/decode → fail loud).
pub fn decode_next(buf: &[u8]) -> DecodeStep {
    decode_next_with(buf, KNOWN_FEATURES)
}

/// [`decode_next`] against an explicit known-name table. Production always passes
/// [`KNOWN_FEATURES`]; tests pass a table with an entry the shipped one lacks, to prove a
/// known name decodes without putting a name in the production table.
fn decode_next_with(buf: &[u8], known: &[&str]) -> DecodeStep {
    decode_projected_with(buf, known, |seq, fence, payload| {
        let record = serde_json::from_slice::<WalRecord>(payload)?;
        Ok(FramedRecord { seq, fence, record })
    })
}

/// Project a verified frame without constructing a full record. Feature requirements are
/// still checked before the projection, so an admission index cannot skip a newer writer.
/// The projection receives only the record JSON, with any gated envelope already removed.
pub(crate) fn decode_projected<T>(
    buf: &[u8],
    project: impl FnOnce(u64, u64, &[u8]) -> Result<T, serde_json::Error>,
) -> DecodeStep<T> {
    decode_projected_with(buf, KNOWN_FEATURES, project)
}

fn decode_projected_with<T>(
    buf: &[u8],
    known: &[&str],
    project: impl FnOnce(u64, u64, &[u8]) -> Result<T, serde_json::Error>,
) -> DecodeStep<T> {
    if buf.get(4) == Some(&V4_FRAMING_VERSION) {
        return decode_v4_projected(buf, known, project);
    }
    if buf.len() < HEADER_LEN {
        return DecodeStep::TornTail; // not even a full header → torn
    }
    let len = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
    let ver = buf[4];
    let seq = u64::from_le_bytes(buf[5..13].try_into().unwrap());
    let fence = u64::from_le_bytes(buf[13..21].try_into().unwrap());
    let digest = &buf[21..53];

    if ver != FRAMING_VERSION && ver != LINEAGE_FRAMING_VERSION && ver != GATED_FRAMING_VERSION {
        return DecodeStep::Corrupt(FrameError::UnsupportedVersion { ver });
    }
    if len > MAX_PAYLOAD_LEN {
        return DecodeStep::Corrupt(FrameError::PayloadTooLarge { len });
    }
    let total = HEADER_LEN + len as usize;
    if buf.len() < total {
        return DecodeStep::TornTail; // header present, payload incomplete → torn
    }
    let payload = &buf[HEADER_LEN..total];
    let expected = compute_digest(ver, seq, fence, payload);
    if expected.as_slice() != digest {
        // A FULLY-present frame whose digest fails = corruption, not a torn write.
        return DecodeStep::Corrupt(FrameError::DigestMismatch { seq });
    }

    if ver == LINEAGE_FRAMING_VERSION {
        if len != LINEAGE_PAYLOAD_LEN || seq != 0 || fence != 0 {
            return DecodeStep::Corrupt(FrameError::InvalidLineageFrame { len, seq, fence });
        }
        let mut bytes = [0u8; LINEAGE_PAYLOAD_LEN as usize];
        bytes.copy_from_slice(payload);
        return DecodeStep::Lineage {
            lineage_id: WalLineageId::from_bytes(bytes),
            consumed: total,
        };
    }

    let payload = if ver == GATED_FRAMING_VERSION {
        let envelope = match serde_json::from_slice::<GatedRequires>(payload) {
            Ok(envelope) => envelope,
            Err(e) => return DecodeStep::Corrupt(FrameError::Deserialize(e)),
        };
        let mut missing: Vec<String> = envelope
            .requires
            .into_iter()
            .filter(|name| {
                (!known.contains(&name.as_str()) || forgotten_for_test(name))
                    && !TEST_KNOWN_FEATURES.contains(&name.as_str())
            })
            .collect();
        if !missing.is_empty() {
            missing.sort_unstable();
            missing.dedup();
            return DecodeStep::NewerWriter { ver, missing };
        }
        envelope.record.get().as_bytes()
    } else {
        payload
    };
    match project(seq, fence, payload) {
        Ok(record) => DecodeStep::Record {
            framed: Box::new(record),
            consumed: total,
        },
        Err(e) => DecodeStep::Corrupt(FrameError::Deserialize(e)),
    }
}

fn decode_v4_projected<T>(
    buf: &[u8],
    known: &[&str],
    project: impl FnOnce(u64, u64, &[u8]) -> Result<T, serde_json::Error>,
) -> DecodeStep<T> {
    use broca_wal_reader::features::Policy;
    use broca_wal_reader::frame::{decode_next_with_policy, DecodeStep as V4Step};

    let known: Vec<&str> = known
        .iter()
        .copied()
        .filter(|name| !forgotten_for_test(name))
        .chain(TEST_KNOWN_FEATURES.iter().copied())
        .collect();
    match decode_next_with_policy(buf, &known, Policy::All) {
        Ok(V4Step::Incomplete) => DecodeStep::TornTail,
        Ok(V4Step::PayloadCorrupt { header, .. }) => {
            DecodeStep::Corrupt(FrameError::DigestMismatch {
                seq: header.metadata.seq,
            })
        }
        Ok(V4Step::Frame(frame)) => {
            let metadata = frame.header.metadata;
            match project(metadata.seq, metadata.fence, frame.payload) {
                Ok(record) => DecodeStep::Record {
                    framed: Box::new(record),
                    consumed: frame.consumed,
                },
                Err(error) => DecodeStep::Corrupt(FrameError::Deserialize(error)),
            }
        }
        Err(broca_wal_reader::Error::NewerWriter { features }) => DecodeStep::NewerWriter {
            ver: V4_FRAMING_VERSION,
            missing: features,
        },
        Err(error) => DecodeStep::Corrupt(FrameError::V4(error)),
        // Policy::All never skips: every unknown name is a whole-log refusal.
        Ok(V4Step::Skipped { header, .. }) => DecodeStep::NewerWriter {
            ver: V4_FRAMING_VERSION,
            missing: header.requires,
        },
    }
}

/// Serializes the tests in this crate that hide a feature with [`forget_feature_for_test`]
/// against tests that decode frames requiring that feature. The hiding is process-wide, so
/// without this a test asserting that a known feature decodes can run while another test hides
/// it, and fail at random.
#[cfg(test)]
fn lock_feature_table_for_test() -> std::sync::MutexGuard<'static, ()> {
    static FEATURE_TABLE: std::sync::Mutex<()> = std::sync::Mutex::new(());
    FEATURE_TABLE
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod deferred_call_tests;

#[cfg(test)]
mod openrouter_reasoning_tests;

#[cfg(test)]
mod media_content_tests;

#[cfg(test)]
mod retention_tests;

#[cfg(test)]
mod provider_held_tests;

#[cfg(test)]
mod fallback_tests;

#[cfg(test)]
mod reader_release_tests;

#[cfg(test)]
mod tests {
    #[test]
    fn message_times_require_an_understanding_reader() {
        use super::*;
        let old = include_str!("../tests/fixtures/before_message_times.jsonl");
        let known: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|feature| *feature != MESSAGE_TIMES_FEATURE)
            .collect();
        for line in old.lines().take(3) {
            let mut value: serde_json::Value = serde_json::from_str(line).unwrap();
            value["written_at_ms"] = serde_json::json!(42);
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert!(record.required_features().contains(&MESSAGE_TIMES_FEATURE));
            let bytes = encode(1, 1, &record).unwrap();
            assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
            match decode_next_with(&bytes, &known) {
                DecodeStep::NewerWriter { missing, .. } => {
                    assert_eq!(missing, vec![MESSAGE_TIMES_FEATURE])
                }
                _ => panic!("a reader without message times must refuse the complete frame"),
            }
        }
    }

    #[test]
    fn message_times_compose_with_existing_admission_features() {
        use super::*;
        let mut value: serde_json::Value = serde_json::from_str(
            include_str!("../tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        value["written_at_ms"] = serde_json::json!(42);
        value["session_class"] = serde_json::json!("head");
        value["config"]["budget"] = serde_json::json!({"max_input_tokens":5});
        let record: WalRecord = serde_json::from_value(value).unwrap();
        for feature in [
            MESSAGE_TIMES_FEATURE,
            SESSION_CLASS_FEATURE,
            INPUT_TOKEN_CAP_FEATURE,
        ] {
            assert!(record.required_features().contains(&feature));
        }
        assert!(matches!(
            decode_next(&encode(1, 1, &record).unwrap()),
            DecodeStep::Record { .. }
        ));
    }

    #[test]
    fn incarnation_requires_a_uuid_v4_header() {
        use super::*;
        assert!(WalLineageId::from_bytes([0; 16]).incarnation().is_none());
        let mut bytes = [1; 16];
        bytes[6] = 0x41;
        bytes[8] = 0x81;
        let id = WalLineageId::from_bytes(bytes);
        assert_eq!(id.incarnation(), Some(id.to_string()));
    }
    use super::*;
    use broca_protocol::StepId;

    fn rec(n: u64) -> WalRecord {
        WalRecord::TurnFinished { step_id: StepId(n) }
    }

    fn v4_metadata() -> broca_wal_reader::frame::Metadata {
        broca_wal_reader::frame::Metadata {
            seq: 7,
            fence: 3,
            appended_ms: 123,
            writer_id: [5; 16],
        }
    }

    #[test]
    fn v4_round_trip_and_projection_delegate_to_shared_codec() {
        let bytes = encode_v4(v4_metadata(), &[], &rec(1)).unwrap();
        assert_eq!(bytes[4], V4_FRAMING_VERSION);
        assert!(digest_verifies(&bytes));
        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!((framed.seq, framed.fence), (7, 3));
                assert_eq!(framed.record, rec(1));
            }
            _ => panic!("expected v4 record"),
        }
        assert!(matches!(
            decode_projected(&bytes, |seq, fence, payload| {
                assert_eq!((seq, fence), (7, 3));
                serde_json::from_slice::<WalRecord>(payload)
            }),
            DecodeStep::Record { .. }
        ));
        // Existing roots must not start writing v4 merely because it is readable.
        assert_eq!(encode(7, 3, &rec(1)).unwrap()[4], FRAMING_VERSION);
    }

    #[test]
    fn v4_unknown_features_refuse_before_record_projection() {
        for feature in ["future-content/v1", "bill-future/v1"] {
            let bytes =
                broca_wal_reader::frame::encode(v4_metadata(), &[feature], b"not JSON").unwrap();
            match decode_projected::<()>(&bytes, |_, _, _| {
                panic!("must not interpret unknown record")
            }) {
                DecodeStep::NewerWriter { ver, missing } => {
                    assert_eq!(ver, V4_FRAMING_VERSION);
                    assert_eq!(missing, vec![feature]);
                }
                _ => panic!("unknown feature must refuse the whole log"),
            }
        }
    }

    #[test]
    fn v4_own_features_are_required_and_unknown_names_do_not_panic_writer() {
        let mut value: serde_json::Value = serde_json::from_str(
            include_str!("../tests/fixtures/before_message_times.jsonl")
                .lines()
                .next()
                .unwrap(),
        )
        .unwrap();
        value["written_at_ms"] = serde_json::json!(42);
        value["session_class"] = serde_json::json!("head");
        value["config"]["budget"] = serde_json::json!({"max_input_tokens":5});
        let record: WalRecord = serde_json::from_value(value).unwrap();
        let bytes = encode_v4(v4_metadata(), &["future-content/v1"], &record).unwrap();
        let header = broca_wal_reader::frame::decode_header(&bytes)
            .unwrap()
            .unwrap();
        for feature in record.required_features() {
            assert!(header.requires.iter().any(|name| name == feature));
        }
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { .. }
        ));
        assert!(matches!(
            encode_v4(v4_metadata(), &["Bad/v1"], &record),
            Err(FrameError::InvalidFeatureName { .. })
        ));
    }

    #[test]
    fn v4_corruption_distinguishes_header_from_payload() {
        let bytes = encode_v4(v4_metadata(), &[], &rec(1)).unwrap();
        let mut header_bad = bytes.clone();
        header_bad[21] ^= 1;
        assert!(matches!(
            decode_next(&header_bad),
            DecodeStep::Corrupt(FrameError::V4(
                broca_wal_reader::Error::HeaderCorrupt { .. }
            ))
        ));
        let mut payload_bad = bytes;
        *payload_bad.last_mut().unwrap() ^= 1;
        assert!(matches!(
            decode_next(&payload_bad),
            DecodeStep::Corrupt(FrameError::DigestMismatch { seq: 7 })
        ));
    }

    #[test]
    fn v4_broca_unlinks_log_held_open_by_reader() {
        use std::io::Read;
        let dir = broca_test_support::scratch_root()
            .join(format!("v4-reader-unlink-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("session.wal");
        let bytes = encode_v4(v4_metadata(), &[], &rec(1)).unwrap();
        std::fs::write(&path, &bytes).unwrap();
        let mut held = broca_wal_reader::open_log(&path).unwrap();
        std::fs::remove_file(&path)
            .expect("a read-only billing handle must not block broca's unlink");
        assert!(!path.exists());
        let mut read = Vec::new();
        held.read_to_end(&mut read).unwrap();
        assert_eq!(read, bytes);
        drop(held);
        std::fs::remove_dir(&dir).unwrap();
    }

    #[test]
    fn round_trip() {
        let bytes = encode(7, 3, &rec(1)).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!(framed.seq, 7);
                assert_eq!(framed.fence, 3);
                assert_eq!(framed.record, rec(1));
            }
            _ => panic!("expected a record"),
        }
    }

    /// A `SessionRefreshAccepted` round-trips byte for byte and requires `session-change/v1`
    /// (with `plan-manifest/v1` for its manifest). A reader without the feature refuses the
    /// frame as a newer writer rather than running the next episode with the wrong tools.
    /// Reader-first: this build decodes and folds these records; no production path writes
    /// them yet.
    #[test]
    fn session_refresh_accepted_round_trips_and_is_must_understand() {
        use super::*;
        let _guard = lock_feature_table_for_test();
        let manifest = crate::record::PlanManifest {
            rich_ui: None,
            plan: crate::record::OpaqueJson::from_json_text(r#"{"id":"plan-a"}"#).unwrap(),
            plan_identity: "plan-a".into(),
            composition_digest: "c".into(),
            fetches: Vec::new(),
            tools: std::sync::Arc::new(Vec::new()),
            system_text: String::new(),
            system_digest: "s".into(),
            join_version: 1,
            role_versions: Default::default(),
            declarations: Vec::new(),
        };
        let record = WalRecord::SessionRefreshAccepted {
            generation: 1,
            policy: "immediately".into(),
            run_in_progress: true,
            applies_from_next_run: true,
            manifest: Box::new(manifest),
        };
        assert!(record.required_features().contains(&SESSION_CHANGE_FEATURE));
        assert!(record.required_features().contains(&PLAN_MANIFEST_FEATURE));
        let bytes = encode(7, 3, &record).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
            _ => panic!("a session-change reader decodes its own record"),
        }
        assert_eq!(
            serde_json::to_string(&record).unwrap(),
            r#"{"type":"session_refresh_accepted","generation":1,"policy":"immediately","run_in_progress":true,"applies_from_next_run":true,"manifest":{"plan":"{\"id\":\"plan-a\"}","plan_identity":"plan-a","composition_digest":"c","fetches":[],"tools":[],"system_text":"","system_digest":"s","join_version":1,"role_versions":{}}}"#,
            "a session-change record keeps its exact bytes"
        );
        let missing: Vec<&str> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|feature| *feature != SESSION_CHANGE_FEATURE)
            .collect();
        match decode_next_with(&bytes, &missing) {
            DecodeStep::NewerWriter { missing, .. } => {
                assert_eq!(missing, vec![SESSION_CHANGE_FEATURE])
            }
            _ => panic!("a reader without session-change/v1 must refuse the frame"),
        }
    }

    /// Policy changes and prefix flushes round-trip and require only `session-change/v1`.
    #[test]
    fn session_policy_and_flush_round_trip_and_require_the_feature() {
        use super::*;
        let _guard = lock_feature_table_for_test();
        for record in [
            WalRecord::SessionRefreshPolicyChanged {
                generation: 1,
                policy: "on_prefix_rebuild".into(),
                send_id: "policy-1".into(),
            },
            WalRecord::SessionPrefixFlushed {
                send_id: "flush-1".into(),
                pending_generation: Some(1),
            },
        ] {
            assert_eq!(record.required_features(), &[SESSION_CHANGE_FEATURE]);
            let bytes = encode(7, 3, &record).unwrap();
            match decode_next(&bytes) {
                DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
                _ => panic!("a session-change reader decodes its own record"),
            }
        }
    }

    /// A digest-valid frame carrying a record type from a newer build decodes as a record, not
    /// as corruption, and the framing refuses to write the lossy placeholder back out.
    #[test]
    fn a_frame_with_an_unknown_record_type_decodes_and_is_never_re_encoded() {
        let bytes = encode_payload(
            4,
            2,
            br#"{"type":"attempt_digest_from_a_newer_build","attempt":1}"#,
        )
        .unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!((framed.seq, framed.fence), (4, 2));
                assert_eq!(framed.record, WalRecord::Unknown);
            }
            _ => panic!("expected a record"),
        }
        assert!(matches!(
            encode(5, 2, &WalRecord::Unknown),
            Err(FrameError::Serialize(_))
        ));
    }

    #[test]
    fn lineage_metadata_round_trips_outside_the_record_sequence() {
        let lineage_id = WalLineageId::from_bytes([0x5a; 16]);
        let bytes = encode_lineage(lineage_id);
        match decode_next(&bytes) {
            DecodeStep::Lineage {
                lineage_id: decoded,
                consumed,
            } => {
                assert_eq!(decoded, lineage_id);
                assert_eq!(consumed, bytes.len());
            }
            _ => panic!("expected lineage metadata"),
        }
    }

    /// Pins the lineage frame's EXACT WIRE BYTES for a known identity.
    ///
    /// The round-trip test above cannot catch a layout change, because it encodes and decodes
    /// with the same code: reorder two fields and both halves agree with each other while every
    /// WAL already on disk becomes unreadable. This asserts the bytes themselves, so the layout
    /// is pinned independently of the encoder that produces it.
    ///
    /// If this fails, the framing changed. That is a DURABILITY BREAK for every existing WAL,
    /// not a golden to refresh -- fix the encoder rather than the expectation.
    #[test]
    fn lineage_frame_wire_bytes_are_pinned() {
        let bytes = encode_lineage(WalLineageId::from_bytes([0x5a; 16]));

        assert_eq!(bytes.len(), LINEAGE_FRAME_LEN);
        assert_eq!(LINEAGE_FRAME_LEN, 69, "header 53 + identity 16");

        // Field-by-field, at fixed offsets, so a failure names WHICH field moved.
        assert_eq!(&bytes[0..4], &16u32.to_le_bytes(), "payload length");
        assert_eq!(bytes[4], 2, "lineage framing version");
        assert_eq!(
            &bytes[5..13],
            &0u64.to_le_bytes(),
            "seq: consumes no record number"
        );
        assert_eq!(&bytes[13..21], &0u64.to_le_bytes(), "fence");
        assert_eq!(&bytes[53..69], &[0x5a; 16], "identity payload");

        // THE DIGEST IS A FROZEN LITERAL, COMPUTED OUTSIDE THIS CRATE.
        //
        // It was first asserted as `compute_digest(2, 0, 0, &[0x5a; 16])`, reasoning that this
        // test is about LAYOUT and the digest only needs to be non-blank. That was wrong, and it
        // reintroduced the very defect this test exists to close: the encoder, the decoder AND
        // that assertion all call `compute_digest`, so a change to its domain moved all three
        // together. Swapping seq/fence inside the hash left the ENTIRE broca-wal suite green
        // while every frame already on disk would decode as DigestMismatch.
        //
        // sha256 over: version byte 0x02, seq 0u64 LE, fence 0u64 LE, payload [0x5a; 16].
        // An expectation derived from the code under test cannot detect a change to that code.
        // If this literal ever needs updating, the framing changed and every existing WAL is
        // unreadable — fix the encoder, not this array.
        const EXPECTED_DIGEST: [u8; 32] = [
            0xb8, 0xc1, 0xc2, 0x6f, 0x10, 0xd3, 0xd6, 0x36, 0xff, 0x9d, 0x9d, 0x83, 0x61, 0x97,
            0x15, 0x8a, 0x04, 0x82, 0xd1, 0x39, 0x75, 0xcc, 0x1b, 0x7c, 0x52, 0xa3, 0xf1, 0x43,
            0xac, 0x76, 0x45, 0x80,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
    }

    /// Pins the EXACT WIRE BYTES of an ordinary record frame (framing version 1).
    ///
    /// Every WAL on disk is made of these frames, so a change to their layout or to the digest
    /// domain makes every existing WAL unreadable. The digest is a literal computed outside this
    /// crate (`shasum -a 256` over the version byte, seq and fence as u64 LE, and the payload),
    /// for the same reason as in the lineage test above: an expectation derived from
    /// `compute_digest` would move together with it.
    #[test]
    fn record_frame_wire_bytes_are_pinned() {
        let bytes = encode(7, 3, &rec(1)).unwrap();
        let payload: &[u8] = br#"{"type":"turn_finished","step_id":1}"#;

        assert_eq!(bytes.len(), HEADER_LEN + payload.len());
        assert_eq!(&bytes[0..4], &36u32.to_le_bytes(), "payload length");
        assert_eq!(bytes[4], 1, "record framing version");
        assert_eq!(&bytes[5..13], &7u64.to_le_bytes(), "seq");
        assert_eq!(&bytes[13..21], &3u64.to_le_bytes(), "fence");
        const EXPECTED_DIGEST: [u8; 32] = [
            0xa6, 0xd9, 0xc6, 0xf0, 0x9c, 0xfc, 0x8e, 0x7e, 0x26, 0xaa, 0x04, 0x2d, 0x10, 0xd5,
            0x01, 0x24, 0xd3, 0xde, 0xf4, 0x44, 0x62, 0xe2, 0x91, 0x81, 0x5a, 0x00, 0x37, 0x57,
            0x27, 0x3d, 0x91, 0x1c,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(&bytes[53..], payload, "payload");
    }

    /// Frame `payload` under an arbitrary version byte with a digest that verifies, so a test
    /// can present a reader with a frame this build never writes.
    fn frame_with_version(ver: u8, seq: u64, fence: u64, payload: &[u8]) -> Vec<u8> {
        let digest = compute_digest(ver, seq, fence, payload);
        let mut out = Vec::with_capacity(HEADER_LEN + payload.len());
        out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        out.push(ver);
        out.extend_from_slice(&seq.to_le_bytes());
        out.extend_from_slice(&fence.to_le_bytes());
        out.extend_from_slice(&digest);
        out.extend_from_slice(payload);
        out
    }

    /// A framing version this build does not know is refused by name, never skipped or read.
    ///
    /// Builds that predate gated frames refuse a version-3 frame with exactly this wording,
    /// "unsupported WAL framing version 3", and that refusal is what makes rolling back to
    /// them safe once version-3 frames exist. This build reads versions 3 and 4, so the property is
    /// asserted for the next unknown version: if a future build writes a new framing version,
    /// rolling back to this build must meet the same named refusal.
    #[test]
    fn an_unknown_framing_version_is_refused_as_unsupported() {
        let bytes = frame_with_version(
            5,
            1,
            1,
            br#"{"requires":["test-feature/v1"],"record":{"type":"turn_finished","step_id":1}}"#,
        );
        match decode_next(&bytes) {
            DecodeStep::Corrupt(error @ FrameError::UnsupportedVersion { ver: 5 }) => {
                assert_eq!(error.to_string(), "unsupported WAL framing version 5");
            }
            _ => panic!("expected an unsupported-version refusal"),
        }
    }

    /// The production `KNOWN_FEATURES` table holds exactly the features this build interprets.
    /// A name enters it only with the release that reads its records, so a build never claims
    /// to understand a frame it would misread; the test-only name is never in it.
    #[test]
    fn the_shipped_known_feature_table_holds_exactly_the_interpreted_features() {
        assert_eq!(
            KNOWN_FEATURES,
            &[
                BILL_ATTEMPTS_FEATURE,
                crate::head_proof::VISIBILITY_LATE_FEATURE,
                "writer-id/v1",
                INTERRUPT_REASON_FEATURE,
                TURN_DIRECTIVES_FEATURE,
                AGENT_RUN_SCOPE_FEATURE,
                ARCHIVE_INDEX_V2_FEATURE,
                AUTH_SELECTION_METHOD_FEATURE,
                CHATGPT_CODEX_WIRE_FEATURE,
                CLIENT_MESSAGE_ID_FEATURE,
                COMPACTION_RIDE_WAIT_FEATURE,
                COMPACTION_VIEW_FEATURE,
                COMPACTION_WAIT_FEATURE,
                COMPACTION_FEATURE,
                CONTEXT_CEILING_FEATURE,
                DEFERRED_CALL_FEATURE,
                DELIVERY_ORDER_FEATURE,
                DISPATCH_MODULE_FEATURE,
                FLOW_SCOPE_FEATURE,
                FRAME_STAMPS_FEATURE,
                GOOGLE_RPC_ERROR_DETAILS_FEATURE,
                INPUT_TOKEN_CAP_FEATURE,
                MEDIA_CONTENT_FEATURE,
                OPENROUTER_REASONING_FEATURE,
                OUTPUT_RESERVATION_FEATURE,
                PLAN_MANIFEST_FEATURE,
                PLAN_REFERENCE_SLOTS_FEATURE,
                PROVIDER_DECLARATIONS_FEATURE,
                PROVIDER_FALLBACK_BLOCK_FEATURE,
                TOOL_SCHEMA_PINS_FEATURE,
                PROVIDER_HELD_FEATURE,
                PROVIDER_TEXT_STATE_FEATURE,
                REFUSAL_FALLBACK_FEATURE,
                RESTART_PAUSE_FEATURE,
                RETENTION_FEATURE,
                RUNNER_TAGS_FEATURE,
                RUNNER_TAGS_V2_FEATURE,
                RUN_CANCEL_AT_FEATURE,
                SCOPE_FEATURE,
                SESSION_CHANGE_FEATURE,
                SESSION_CLASS_FEATURE,
                STEER_QUEUE_FEATURE,
                STEP_TRANSFORMS_FEATURE,
                STRUCTURED_OUTPUT_FEATURE,
                STRUCTURED_OUTPUT_DOWNGRADE_FEATURE,
                STRUCTURED_OUTPUT_PARAGRAPH_FEATURE,
                TOOL_REPLY_FEATURE,
                TOOL_TIMES_FEATURE,
                MESSAGE_TIMES_FEATURE,
            ]
        );
        assert_eq!(TOOL_REPLY_FEATURE, "tool-reply/v1");
        assert_eq!(BILL_ATTEMPTS_FEATURE, "bill-attempts/v1");
        assert_eq!(MESSAGE_TIMES_FEATURE, "message-times/v1");
        assert_eq!(OUTPUT_RESERVATION_FEATURE, "output-reservation/v1");
        assert_eq!(RUNNER_TAGS_FEATURE, "runner-tags/v1");
        assert_eq!(RUNNER_TAGS_V2_FEATURE, "runner-tags/v2");
        assert_eq!(ARCHIVE_INDEX_V2_FEATURE, "archive-index/v2");
        assert_eq!(COMPACTION_FEATURE, "compaction/v1");
        assert_eq!(COMPACTION_WAIT_FEATURE, "compaction-wait/v1");
        assert_eq!(DEFERRED_CALL_FEATURE, "deferred-call/v1");
        assert_eq!(DISPATCH_MODULE_FEATURE, "dispatch-module/v1");
        assert_eq!(PLAN_MANIFEST_FEATURE, "plan-manifest/v1");
        assert_eq!(PLAN_REFERENCE_SLOTS_FEATURE, "plan-reference-slots/v1");
        assert_eq!(PROVIDER_DECLARATIONS_FEATURE, "provider-declarations/v1");
        assert_eq!(TOOL_SCHEMA_PINS_FEATURE, "tool-schema-pins/v1");
        assert_eq!(PROVIDER_TEXT_STATE_FEATURE, "provider-text-state/v1");
        assert_eq!(
            PROVIDER_FALLBACK_BLOCK_FEATURE,
            "provider-fallback-block/v1"
        );
        assert_eq!(REFUSAL_FALLBACK_FEATURE, "refusal-fallback/v1");
        assert_eq!(CHATGPT_CODEX_WIRE_FEATURE, "chatgpt-codex-wire/v1");
        assert_eq!(OPENROUTER_REASONING_FEATURE, "openrouter-reasoning/v1");
        assert_eq!(RESTART_PAUSE_FEATURE, "restart-pause/v1");
        assert_eq!(PROVIDER_HELD_FEATURE, "provider-held/v1");
        assert_eq!(SCOPE_FEATURE, "scope/v1");
        assert_eq!(SESSION_CHANGE_FEATURE, "session-change/v1");
        assert_eq!(FLOW_SCOPE_FEATURE, "flow-scopes/v1");
        assert_eq!(
            GOOGLE_RPC_ERROR_DETAILS_FEATURE,
            "google-rpc-error-details/v1"
        );
        assert_eq!(INPUT_TOKEN_CAP_FEATURE, "input-token-cap/v1");
        assert_eq!(AUTH_SELECTION_METHOD_FEATURE, "auth-selection-method/v1");
        assert_eq!(STEER_QUEUE_FEATURE, "steer-queue/v1");
        assert_eq!(STEP_TRANSFORMS_FEATURE, "step-transforms/v1");
        assert_eq!(STRUCTURED_OUTPUT_FEATURE, "structured-output/v1");
        assert_eq!(
            STRUCTURED_OUTPUT_PARAGRAPH_FEATURE,
            "structured-output-paragraph/v1"
        );
    }

    /// A run start or refreeze whose frozen structured output carries the instructions
    /// paragraph break requires its own feature, so an older reader refuses the WAL by that
    /// name instead of resuming the run without the break. A frozen structured output without
    /// the flag, or with it false, requires nothing new.
    #[test]
    fn auth_selection_method_is_must_understand_on_both_selection_records() {
        use crate::record::WalRecord;
        // The method is the selection's last key, so every older key keeps its place.
        const PINNED: &[&str] = &[
            r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"anthropic","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1,"auth_selection":{"credential_id":"work-claude","account_ref":"acct-1","account_decision":{"path":"routed","admission_id":"r","decision_id":"acd-1"},"auth_method":"oauth"}},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
            r#"{"type":"run_auth_refrozen","ts_ms":5,"auth_selection":{"credential_id":"work-claude","account_ref":"acct-1","auth_method":"oauth"}}"#,
        ];
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != AUTH_SELECTION_METHOD_FEATURE)
            .collect();
        for pinned in PINNED {
            let record: WalRecord = serde_json::from_str(pinned).unwrap();
            assert_eq!(serde_json::to_string(&record).unwrap(), *pinned);
            assert_eq!(record.required_features(), &[AUTH_SELECTION_METHOD_FEATURE]);
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            match decode_next(&bytes) {
                DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
                _ => panic!("this build reads a selection with a method"),
            }
            // A build without the feature refuses the log by the feature's name rather than
            // resume it by parsing the opaque credential id.
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [AUTH_SELECTION_METHOD_FEATURE])
            );

            // The same record without the method needs no feature at all.
            let mut without = record.clone();
            match &mut without {
                WalRecord::RunStarted { config, .. } => {
                    config.auth_selection.as_mut().unwrap().auth_method = None
                }
                WalRecord::RunAuthRefrozen { auth_selection, .. } => {
                    auth_selection.auth_method = None
                }
                _ => unreachable!(),
            }
            assert!(without.required_features().is_empty());
            assert!(matches!(
                decode_next_with(&encode(1, 1, &without).unwrap(), &older),
                DecodeStep::Record { .. }
            ));
        }
    }

    #[test]
    fn structured_output_paragraph_break_is_must_understand() {
        // Serialized from the protocol type, so renaming its field cannot leave this gate
        // looking for a key the writer no longer emits.
        use broca_protocol::structured_output::{FrozenStructuredOutput, Mode, SchemaDelivery};
        let so = serde_json::to_value(FrozenStructuredOutput {
            mode: Mode::FinalText,
            name: "result".into(),
            schema_jcs: "{}".into(),
            enforcement: "best_effort".into(),
            reason: Some("route_unsupported".into()),
            profile: None,
            delivery: SchemaDelivery::Unconstrained,
            instructions_paragraph_break: true,
        })
        .unwrap();
        let mut run = serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}});
        run["config"]["frozen_render_config"] = serde_json::json!({"structured_output": so});
        let mut refrozen = serde_json::json!({"type":"run_config_refrozen","renderer_version":1,"frozen_render_config":{"structured_output": so}});
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != STRUCTURED_OUTPUT_PARAGRAPH_FEATURE)
            .collect();
        for value in [run.clone(), refrozen.clone()] {
            let record: WalRecord = serde_json::from_value(value).unwrap();
            let required = record.required_features();
            assert!(required.contains(&STRUCTURED_OUTPUT_PARAGRAPH_FEATURE));
            assert!(required.contains(&STRUCTURED_OUTPUT_FEATURE));
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [STRUCTURED_OUTPUT_PARAGRAPH_FEATURE])
            );
        }

        // Without the flag (absent or false) the frame requires only structured-output/v1,
        // which every reader of structured-output runs already has.
        for flag in [None, Some(false)] {
            for (value, pointer) in [
                (&mut run, "/config/frozen_render_config/structured_output"),
                (&mut refrozen, "/frozen_render_config/structured_output"),
            ] {
                let so = value.pointer_mut(pointer).unwrap();
                match flag {
                    None => {
                        so.as_object_mut()
                            .unwrap()
                            .remove("instructions_paragraph_break");
                    }
                    Some(flag) => so["instructions_paragraph_break"] = serde_json::json!(flag),
                }
                let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
                assert_eq!(record.required_features(), &[STRUCTURED_OUTPUT_FEATURE]);
            }
        }
    }

    #[test]
    fn structured_output_downgrade_requires_its_own_feature() {
        let record = WalRecord::StructuredOutputDowngraded {
            run_id: broca_protocol::RunId("r".into()),
        };
        assert_eq!(
            record.required_features(),
            &[STRUCTURED_OUTPUT_DOWNGRADE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
        let old: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != STRUCTURED_OUTPUT_DOWNGRADE_FEATURE)
            .collect();
        assert!(matches!(
            decode_next_with(&bytes, &old),
            DecodeStep::NewerWriter { .. }
        ));
    }

    /// A lowered output cap is part of the request its step sent, so it is written in a gated
    /// frame requiring `output-reservation/v1`: a build without the feature refuses the WAL
    /// rather than skip the record and resume the step with the frozen cap.
    #[test]
    fn a_lowered_output_cap_requires_its_own_feature() {
        let record = WalRecord::OutputReservationClamped {
            step_id: broca_protocol::StepId(175),
            max_output_tokens: 124_990,
            estimated_input_tokens: 919_490,
            context_limit: 1_048_576,
        };
        assert_eq!(record.required_features(), &[OUTPUT_RESERVATION_FEATURE]);
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        assert!(
            matches!(decode_next(&bytes), DecodeStep::Record { framed, .. } if framed.record == record)
        );
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != OUTPUT_RESERVATION_FEATURE)
            .collect();
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [OUTPUT_RESERVATION_FEATURE])
        );
    }

    #[test]
    fn provider_text_state_is_must_understand_before_record_decode() {
        // One record is a steered frame decoded with the shipped table; a sibling test hides
        // steer-queue/v1 process-wide.
        let _table = lock_feature_table_for_test();
        let block = serde_json::json!({"type":"text_with_state","text":"","provider_state":{"format":"gemini","data":"opaque+/="}});
        let message = serde_json::json!({"role":"assistant","content":[block.clone()]});
        let run = serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}});
        let mut with_input = run.clone();
        with_input["input"] = serde_json::json!([message.clone()]);
        let mut with_policy = run.clone();
        with_policy["config"]["frozen_render_config"] =
            serde_json::json!({"provider_text_state":true});
        let mut refrozen = serde_json::json!({"type":"run_config_refrozen","renderer_version":1,"frozen_render_config":{"provider_text_state":true}});
        let records = [
            with_input,
            with_policy,
            refrozen.clone(),
            serde_json::json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[block]},"usage":{},"retries_used":0}),
            serde_json::json!({"type":"steered","input":[message.clone()]}),
            serde_json::json!({"type":"queued","submission_id":"q","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[message],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}),
        ];
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != PROVIDER_TEXT_STATE_FEATURE)
            .collect();
        for value in records {
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert!(record
                .required_features()
                .contains(&PROVIDER_TEXT_STATE_FEATURE));
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [PROVIDER_TEXT_STATE_FEATURE])
            );
        }
        let legacy: WalRecord = serde_json::from_value(run).unwrap();
        assert!(legacy.required_features().is_empty());
        refrozen["frozen_render_config"]["provider_text_state"] = serde_json::json!(false);
        let legacy: WalRecord = serde_json::from_value(refrozen).unwrap();
        assert!(legacy.required_features().is_empty());
    }

    #[test]
    fn stored_google_rpc_error_details_require_their_wal_feature() {
        let details = serde_json::json!({
            "errorInfo": {"reason": "QUOTA_EXHAUSTED", "metadata": {}},
            "quotaFailure": {"violations": [{"quotaMetric": "model-tier"}]},
            "retryInfo": {"retryDelay": "21m0s"}
        });
        let values = [
            serde_json::json!({
                "type": "run_finished",
                "reason": "error",
                "usage": {},
                "error": {
                    "class": "transient",
                    "message": "429: quota exhausted",
                    "provider_details": details
                }
            }),
            serde_json::json!({
                "type": "run_paused",
                "run_id": "run-1",
                "reason": "auth_required",
                "error": {
                    "class": "transient",
                    "message": "provider error",
                    "provider_details": details
                }
            }),
            serde_json::json!({
                "type": "retracted",
                "submission_id": "submission-1",
                "failure": {
                    "run_id": "run-1",
                    "error": {
                        "class": "transient",
                        "message": "provider error",
                        "provider_details": details
                    }
                }
            }),
        ];
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != GOOGLE_RPC_ERROR_DETAILS_FEATURE)
            .collect();
        for value in values {
            let record: WalRecord = serde_json::from_value(value).unwrap();
            assert_eq!(
                record.required_features(),
                &[GOOGLE_RPC_ERROR_DETAILS_FEATURE]
            );
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
            assert!(matches!(
                decode_next_with(&bytes, &older),
                DecodeStep::NewerWriter { missing, .. }
                    if missing == [GOOGLE_RPC_ERROR_DETAILS_FEATURE]
            ));
        }

        let legacy: WalRecord = serde_json::from_value(serde_json::json!({
            "type": "run_finished",
            "reason": "error",
            "usage": {},
            "error": {"class": "transient", "message": "429: quota exhausted"}
        }))
        .unwrap();
        assert!(legacy.required_features().is_empty());
        assert_eq!(encode(1, 1, &legacy).unwrap()[4], FRAMING_VERSION);
    }

    #[test]
    fn chatgpt_wire_run_started_is_must_understand() {
        chatgpt_wire_gate(
            serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":8},"input":[],"origin":{"kind":"fresh"}}),
            "/config/frozen_render_config",
        );
    }

    #[test]
    fn chatgpt_wire_refrozen_is_must_understand() {
        chatgpt_wire_gate(
            serde_json::json!({"type":"run_config_refrozen","renderer_version":8,"frozen_render_config":{}}),
            "/frozen_render_config",
        );
    }

    #[test]
    fn chatgpt_continuation_attempt_is_must_understand() {
        let mut value: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/model_attempt_before_progress.json"
        ))
        .unwrap();
        let legacy: WalRecord = serde_json::from_value(value.clone()).unwrap();
        let before = encode(1, 1, &legacy).unwrap();
        value["websocket_request"] = serde_json::json!({"len":123,"sha256":"frame-digest","codex":{"continuation":true,"suffix_item_count":1,"connection_generation":2,"previous_response_id_present":true,"physical_send_count":1}});
        let record: WalRecord = serde_json::from_value(value).unwrap();
        let bytes = encode(1, 1, &record).unwrap();
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != CHATGPT_CODEX_WIRE_FEATURE)
            .collect();
        assert!(matches!(
            decode_next_with(&before, &older),
            DecodeStep::Record { .. }
        ));
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [CHATGPT_CODEX_WIRE_FEATURE])
        );
        assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
    }

    fn chatgpt_wire_gate(mut value: serde_json::Value, pointer: &str) {
        use crate::file_wal::Wal;
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != CHATGPT_CODEX_WIRE_FEATURE)
            .collect();
        let parent = if pointer.starts_with("/config/") {
            &mut value["config"]
        } else {
            &mut value
        };
        parent["frozen_render_config"] = serde_json::json!({});
        let legacy: WalRecord = serde_json::from_value(value.clone()).unwrap();
        let legacy_bytes = encode(1, 1, &legacy).unwrap();
        assert_eq!(legacy_bytes[4], FRAMING_VERSION);
        assert!(matches!(
            decode_next_with(&legacy_bytes, &older),
            DecodeStep::Record { .. }
        ));
        // The outer feature fence applies even to an opaque future profile.
        value.pointer_mut(pointer).unwrap()["chatgpt_wire"] =
            serde_json::json!({"profile":"codex-future"});
        let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        let root = broca_test_support::scratch_root().join(format!(
            "codex-floor-{}-{}",
            std::process::id(),
            pointer.replace('/', "-")
        ));
        std::fs::create_dir_all(&root).unwrap();
        let floor = root.join("floor");
        std::fs::write(&floor, b"not a directory").unwrap();
        let wal_path = root.join("writer.wal");
        let mut wal = crate::file_wal::FileWal::open(&wal_path)
            .unwrap()
            .with_feature_floor(floor.join("feature-floor"));
        wal.append(1, &legacy, true).unwrap();
        let before_append = std::fs::metadata(&wal_path).unwrap().len();
        assert!(wal.append(1, &record, true).is_err());
        assert_eq!(
            std::fs::metadata(&wal_path).unwrap().len(),
            before_append,
            "floor failure must prevent the gated append"
        );
        drop(wal);
        std::fs::remove_file(&floor).unwrap();
        let mut wal = crate::file_wal::FileWal::open(&wal_path)
            .unwrap()
            .with_feature_floor(&floor);
        wal.append(1, &record, true).unwrap();
        assert!(std::fs::read_to_string(&floor)
            .unwrap()
            .contains(CHATGPT_CODEX_WIRE_FEATURE));
        drop(wal);
        std::fs::remove_dir_all(root).unwrap();
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [CHATGPT_CODEX_WIRE_FEATURE])
        );
        assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
        value
            .pointer_mut(pointer)
            .unwrap()
            .as_object_mut()
            .unwrap()
            .remove("chatgpt_wire");
        let restored: WalRecord = serde_json::from_value(value).unwrap();
        assert_eq!(encode(1, 1, &restored).unwrap(), legacy_bytes);
    }

    /// Every record kind that carries runner tags, and a run start frozen to render them, is a
    /// gated frame requiring `runner-tags/v1`: a build that predates the feature refuses it by
    /// that name instead of reading the session without its tags. The same records without
    /// tags keep their ungated bytes.
    #[test]
    fn runner_tags_are_must_understand_and_older_readers_refuse_them_by_name() {
        // One record is a steered frame decoded with the shipped table; a sibling test hides
        // steer-queue/v1 process-wide.
        let _table = lock_feature_table_for_test();
        let run = serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hi"}]}],"origin":{"kind":"fresh"}});
        let result = serde_json::json!({"type":"tool_result","batch_id":"b","result":{"tool_call_id":"c","output":{"kind":"text","text":"r"}},"outcome":"completed"});
        let untagged = [
            run.clone(),
            serde_json::json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[{"type":"text","text":"a"}]},"usage":{},"retries_used":0}),
            result.clone(),
            serde_json::json!({"type":"tool_no_dispatch","batch_id":"b","tool_call_id":"c","reason":{"kind":"denied","reason":"no"}}),
            serde_json::json!({"type":"budget_fired","run_id":"r","kind":"nudge","threshold":1,"ordinal":3}),
        ];
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != RUNNER_TAGS_FEATURE)
            .collect();
        let mut tagged = Vec::new();
        for value in untagged {
            let plain: WalRecord = serde_json::from_value(value.clone()).unwrap();
            assert!(!plain.required_features().contains(&RUNNER_TAGS_FEATURE));
            let mut with_tags = value;
            with_tags["tags"] = if with_tags["type"] == "run_started" {
                serde_json::json!([[7]])
            } else {
                serde_json::json!([7])
            };
            tagged.push(with_tags);
        }
        let mut steered = serde_json::json!({"type":"steered","input":[{"role":"user","content":[{"type":"text","text":"s"}]}]});
        steered["tags"] = serde_json::json!([[8]]);
        tagged.push(steered);
        let mut render = run;
        render["config"]["render_tags"] = serde_json::json!(true);
        tagged.push(render);
        for value in tagged {
            let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
            assert!(
                record.required_features().contains(&RUNNER_TAGS_FEATURE),
                "{value}"
            );
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
                panic!("a tag-reading build decodes {value}");
            };
            assert_eq!(framed.record, record);
            assert_eq!(
                serde_json::to_value(&framed.record).unwrap()["tags"],
                value["tags"],
                "tags survive the round trip"
            );
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [RUNNER_TAGS_FEATURE]),
                "an older reader refuses {value} by name"
            );
        }
    }

    /// A run start or queued send whose tools declare a reply deadline is a gated frame
    /// requiring `tool-reply/v1`: a build that predates the feature refuses it by that name
    /// instead of decoding the tool without its deadline and cutting its long calls. The same
    /// records whose tools declare none keep their ungated bytes.
    #[test]
    fn tool_reply_is_must_understand_and_older_readers_refuse_it_by_name() {
        let tool = serde_json::json!({"name":"research","description":"","input_schema":{"type":"object"},"module":"prefrontal-core","execution_mode":"parallel"});
        let run = serde_json::json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[tool.clone(), tool.clone()],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}});
        let queued = serde_json::json!({"type":"queued","submission_id":"sub","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[],"tools":[tool.clone()],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}});
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != TOOL_REPLY_FEATURE)
            .collect();
        for (plain, path) in [(run, "/config/tools/1"), (queued, "/request/tools/0")] {
            let record: WalRecord = serde_json::from_value(plain.clone()).unwrap();
            assert!(record.required_features().is_empty(), "{plain}");
            assert_eq!(encode(1, 1, &record).unwrap()[4], FRAMING_VERSION);
            let mut declared = plain;
            declared.pointer_mut(path).unwrap()["reply"] = serde_json::json!({"max_ms": 1_800_000});
            let record: WalRecord = serde_json::from_value(declared.clone()).unwrap();
            assert_eq!(record.required_features(), &[TOOL_REPLY_FEATURE]);
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
                panic!("a reply-reading build decodes {declared}");
            };
            assert_eq!(framed.record, record);
            assert_eq!(serde_json::to_value(&framed.record).unwrap(), declared);
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [TOOL_REPLY_FEATURE]),
                "an older reader refuses {declared} by name"
            );
        }
    }

    #[test]
    fn tool_times_are_must_understand_and_legacy_records_remain_untimed() {
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != TOOL_TIMES_FEATURE)
            .collect();
        for (mut value, field) in [
            (
                serde_json::json!({"type":"tool_dispatch_intent","batch_id":"b","tool_call_id":"c","args":{}}),
                "dispatched_at_ms",
            ),
            (
                serde_json::json!({"type":"tool_no_dispatch","batch_id":"b","tool_call_id":"c","reason":{"kind":"denied","reason":"no"}}),
                "dispatched_at_ms",
            ),
            (
                serde_json::json!({"type":"tool_result","batch_id":"b","result":{"tool_call_id":"c","output":{"kind":"text","text":"ok"},"is_error":false},"outcome":"completed"}),
                "finished_at_ms",
            ),
        ] {
            let legacy: WalRecord = serde_json::from_value(value.clone()).unwrap();
            let legacy_bytes = encode(1, 1, &legacy).unwrap();
            assert!(legacy.required_features().is_empty());
            assert!(serde_json::to_value(&legacy).unwrap().get(field).is_none());
            assert!(matches!(
                decode_next_with(&legacy_bytes, &older),
                DecodeStep::Record { .. }
            ));
            value[field] = serde_json::json!(1_800_000_000_123_u64);
            let timed: WalRecord = serde_json::from_value(value).unwrap();
            assert_eq!(timed.required_features(), &[TOOL_TIMES_FEATURE]);
            let bytes = encode(1, 1, &timed).unwrap();
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [TOOL_TIMES_FEATURE])
            );
            let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
                panic!("the current reader must retain tool observation times");
            };
            assert_eq!(framed.record, timed);
        }
    }

    #[test]
    fn tool_times_publish_the_feature_floor_before_the_first_timed_record() {
        use crate::{FileWal, Wal, WalError};
        let root = broca_test_support::scratch_root().join(format!(
            "tool-times-floor-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let path = root.join("session.wal");
        let floor = root.join("wal-feature-floor");
        let legacy: WalRecord = serde_json::from_value(serde_json::json!({
            "type":"tool_no_dispatch","batch_id":"b","tool_call_id":"c",
            "reason":{"kind":"denied","reason":"no"}
        }))
        .unwrap();
        let mut value = serde_json::to_value(&legacy).unwrap();
        value["dispatched_at_ms"] = serde_json::json!(1_800_000_000_123_u64);
        let timed: WalRecord = serde_json::from_value(value).unwrap();
        let mut wal = FileWal::open(&path).unwrap();
        wal.append(1, &legacy, true).unwrap();
        let before = std::fs::read(&path).unwrap();
        assert!(!floor.exists());
        assert!(matches!(
            wal.append(1, &timed, true),
            Err(WalError::FeatureFloor { path: None, .. })
        ));
        assert_eq!(std::fs::read(&path).unwrap(), before);
        drop(wal);
        let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
        wal.append(1, &timed, true).unwrap();
        assert_eq!(
            crate::floor::read_floor(&floor).unwrap(),
            std::collections::BTreeSet::from([TOOL_TIMES_FEATURE.to_owned()])
        );
        assert_eq!(wal.replay().unwrap().last().unwrap().record, timed);
    }

    #[test]
    fn deferred_tool_times_are_must_understand_and_legacy_times_are_absent() {
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != TOOL_TIMES_FEATURE)
            .collect();
        for (mut value, field) in [
            (
                serde_json::json!({"type":"tool_call_deferred","batch_id":"b","call_id":"c","executor":"broca","module":"tools","tool":"edit","args_jcs":"{}","args_hash":"hash","scope":{"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1},"ask":{"prompt":"Allow?","options":[],"expires_at_ms":1000,"on_expiry":"deny","material_damage":false,"late_execution":"execute"}}),
                "deferred_at_ms",
            ),
            (
                serde_json::json!({"type":"approval_recorded","call_key":"key","call_id":"c","decision":"declined","closing_result":{"tool_call_id":"c","output":{"kind":"text","text":"pre_tool_declined"},"is_error":true}}),
                "closed_at_ms",
            ),
        ] {
            let legacy: WalRecord = serde_json::from_value(value.clone()).unwrap();
            assert!(!legacy.required_features().contains(&TOOL_TIMES_FEATURE));
            assert!(serde_json::to_value(&legacy).unwrap().get(field).is_none());
            assert!(matches!(
                decode_next_with(&encode(1, 1, &legacy).unwrap(), &older),
                DecodeStep::Record { .. }
            ));
            value[field] = serde_json::json!(1_800_000_000_123_u64);
            let timed: WalRecord = serde_json::from_value(value).unwrap();
            assert!(timed.required_features().contains(&TOOL_TIMES_FEATURE));
            let bytes = encode(1, 1, &timed).unwrap();
            assert!(
                matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [TOOL_TIMES_FEATURE])
            );
            let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
                panic!("timed approval records decode");
            };
            assert_eq!(framed.record, timed);
        }
    }

    /// Neither the request fence, an applied view, another answer nor a wait exit can be
    /// skipped by a build that predates the corresponding must-understand feature.
    /// A build that does not know `delivery-order/v1` refuses a delivery-order record as a newer
    /// writer instead of skipping it: skipping it could start a run from a send that may never
    /// start one, or deliver a send that was replaced.
    #[test]
    fn delivery_order_records_refuse_older_readers() {
        let record: WalRecord = serde_json::from_str(
            r#"{"type":"delivery_ordered","submission_id":"sub-r2","order":{"modes":["steer"]}}"#,
        )
        .unwrap();
        let bytes = encode(7, 3, &record).unwrap();
        let older: Vec<&str> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|known| *known != DELIVERY_ORDER_FEATURE)
            .collect();
        assert!(matches!(decode_next_with(&bytes, &older),
            DecodeStep::NewerWriter { missing, .. } if missing == [DELIVERY_ORDER_FEATURE]));
        assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
    }

    #[test]
    fn compaction_records_require_their_features_and_refuse_older_readers() {
        for json in include_str!("../tests/fixtures/compaction_records.jsonl").lines() {
            let record: WalRecord = serde_json::from_str(json).unwrap();
            let feature = if json.contains(r#""type":"compaction_wait""#) {
                "compaction-wait/v1"
            } else {
                "compaction/v1"
            };
            assert_eq!(record.required_features(), &[feature]);
            assert!(!record.is_informational());
            let bytes = encode(7, 3, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
                panic!("reader release must decode {json}");
            };
            assert_eq!(consumed, bytes.len());
            assert_eq!(framed.record, record);
            let older: Vec<&str> = KNOWN_FEATURES
                .iter()
                .copied()
                .filter(|known| *known != feature)
                .collect();
            assert!(
                matches!(decode_next_with(&bytes, &older),
                DecodeStep::NewerWriter { missing, .. } if missing == [feature]),
                "{json}"
            );

            // Refusal happens before interpreting a newer writer's closed-set values.
            let payload = format!(
                r#"{{"requires":["{feature}"],"record":{{"type":"compaction_message","kind":"future"}}}}"#
            );
            let future = frame(GATED_FRAMING_VERSION, 7, 3, payload.as_bytes()).unwrap();
            assert!(matches!(decode_next_with(&future, &older),
                DecodeStep::NewerWriter { missing, .. } if missing == [feature]));
        }
    }

    #[test]
    fn agent_run_scope_round_trips_and_old_readers_refuse_its_identity() {
        let payload: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/pre_flow_scoped_run_started.json"
        ))
        .unwrap();
        let mut record: WalRecord = serde_json::from_value(payload["record"].clone()).unwrap();
        if let WalRecord::RunStarted {
            scope: Some(scope), ..
        } = &mut record
        {
            scope.run_id = Some("parent-run".into());
            scope.agent_id = Some("agent-1".into());
            scope.sender = Some(broca_protocol::BindPrincipal::Reserved {
                module_id: "basal".into(),
            });
        } else {
            panic!("fixture must have a scope");
        }
        assert_eq!(
            record.required_features(),
            &[AGENT_RUN_SCOPE_FEATURE, SCOPE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
            _ => panic!("current reader must preserve agent-run identity"),
        }
        match decode_next_with(&bytes, &[SCOPE_FEATURE, FLOW_SCOPE_FEATURE]) {
            DecodeStep::NewerWriter { missing, .. } => {
                assert_eq!(missing, [AGENT_RUN_SCOPE_FEATURE])
            }
            _ => panic!("old reader must refuse agent-run identity"),
        }
    }

    #[test]
    fn flow_scope_round_trips_and_scope_only_readers_refuse_it() {
        let payload: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/pre_flow_scoped_run_started.json"
        ))
        .unwrap();
        let mut record: WalRecord = serde_json::from_value(payload["record"].clone()).unwrap();
        if let WalRecord::RunStarted {
            scope: Some(scope), ..
        } = &mut record
        {
            scope.flow_id = Some("flow-1".into());
            scope.sender = Some(broca_protocol::BindPrincipal::Reserved {
                module_id: "basal".into(),
            });
        } else {
            panic!("fixture must have a scope");
        }
        assert_eq!(
            record.required_features(),
            &[FLOW_SCOPE_FEATURE, SCOPE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
            _ => panic!("current reader must preserve automation identity"),
        }
        match decode_next_with(&bytes, &[SCOPE_FEATURE]) {
            DecodeStep::NewerWriter { missing, .. } => assert_eq!(missing, [FLOW_SCOPE_FEATURE]),
            _ => panic!("scope-only reader must refuse automation identity"),
        }
    }

    #[test]
    fn reader_decodes_restart_pause_v3_and_older_reader_refuses_it() {
        let record: WalRecord = serde_json::from_str(
            r#"{"type":"run_paused","run_id":"r","reason":"restart","usage":{},"retries_used":0}"#,
        )
        .unwrap();
        let bytes = encode_with_requires(7, 1, &[RESTART_PAUSE_FEATURE], &record).unwrap();
        assert_eq!(bytes[4], 3);
        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("reader must decode restart-pause/v1");
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
        assert!(
            matches!(decode_next_with(&bytes, &[]), DecodeStep::NewerWriter { missing, .. } if missing == ["restart-pause/v1"])
        );
    }

    /// The known-name table of a release from before `provider-held/v1`: this build's table
    /// without it.
    fn known_without_provider_held() -> Vec<&'static str> {
        KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != PROVIDER_HELD_FEATURE)
            .collect()
    }

    /// Both values `provider-held/v1` covers gate themselves through `required_features`, with
    /// no caller-supplied names. This build knows the feature and reads them back exactly; a
    /// build from before it refuses the frame as a newer writer's instead of skipping the held
    /// record or failing on the unknown pause reason.
    #[test]
    fn provider_held_records_are_gated_and_an_older_reader_refuses_them() {
        assert!(KNOWN_FEATURES.contains(&PROVIDER_HELD_FEATURE));
        let held: WalRecord = serde_json::from_str(
            r#"{"type":"tool_call_provider_held","batch_id":"b","tool_call_id":"c","call_key":"lin_9:42","module":"core","reason":"parent_answer","held_at_ms":1761220000000,"sequential":false}"#,
        )
        .unwrap();
        let paused: WalRecord =
            serde_json::from_str(r#"{"type":"run_paused","run_id":"r","reason":"tool_held"}"#)
                .unwrap();
        let older = known_without_provider_held();
        for record in [held, paused] {
            assert_eq!(record.required_features(), &[PROVIDER_HELD_FEATURE]);
            let bytes = encode(7, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
                panic!("this build must decode {record:?}");
            };
            assert_eq!(consumed, bytes.len());
            assert_eq!(framed.record, record);
            match decode_next_with(&bytes, &older) {
                DecodeStep::NewerWriter { missing, .. } => {
                    assert_eq!(missing, ["provider-held/v1"], "{record:?}")
                }
                _ => panic!("a reader without provider-held/v1 must refuse {record:?}"),
            }
        }
    }

    /// The pause reasons that existed before `provider-held/v1` keep their frames: an
    /// auth pause stays an ordinary frame and a restart pause needs only `restart-pause/v1`,
    /// so this build still reads both.
    #[test]
    fn older_pause_reasons_do_not_require_provider_held() {
        let auth: WalRecord =
            serde_json::from_str(r#"{"type":"run_paused","run_id":"r","reason":"auth_required"}"#)
                .unwrap();
        assert!(auth.required_features().is_empty());
        assert_eq!(encode(7, 1, &auth).unwrap()[4], FRAMING_VERSION);
        let restart: WalRecord =
            serde_json::from_str(r#"{"type":"run_paused","run_id":"r","reason":"restart"}"#)
                .unwrap();
        assert_eq!(restart.required_features(), &[RESTART_PAUSE_FEATURE]);
        for record in [auth, restart] {
            let bytes = encode(7, 1, &record).unwrap();
            assert!(
                matches!(decode_next(&bytes), DecodeStep::Record { .. }),
                "{record:?}"
            );
        }
    }

    #[test]
    fn reader_decodes_frozen_restart_policy_and_historical_absence() {
        let old: WalRecord = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        assert!(matches!(
            old,
            WalRecord::RunStarted {
                on_restart: None,
                ..
            }
        ));
        let mut value: serde_json::Value = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        value["on_restart"] = serde_json::json!("resume");
        let new: WalRecord = serde_json::from_value(value).unwrap();
        assert!(matches!(
            new,
            WalRecord::RunStarted {
                on_restart: Some(broca_protocol::request::OnRestart::Resume),
                ..
            }
        ));
    }

    /// A `Steered` record, as the bytes of its record.
    const STEERED: &str = r#"{"type":"steered","send_id":"steer-1","request_identity_digest":"d","delivery":"steer","input":[{"role":"user","content":[{"type":"text","text":"about 90 s left, write up now"}]}]}"#;

    /// A `Steered` record is always a gated frame requiring exactly `steer-queue/v1`, even
    /// through plain [`encode`], and this build reads it back intact.
    #[test]
    fn a_steered_record_is_a_gated_frame_requiring_steer_queue_v1_and_round_trips() {
        // Decodes with the shipped table, which a sibling test edits to hide steer-queue/v1.
        let _table = lock_feature_table_for_test();
        let record: WalRecord = serde_json::from_str(STEERED).unwrap();
        assert_eq!(record.required_features(), &[STEER_QUEUE_FEATURE]);
        let bytes = encode(5, 1, &record).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        let payload = format!(r#"{{"requires":["steer-queue/v1"],"record":{STEERED}}}"#);
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);
        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!(framed.record, record);
            }
            _ => panic!("this build must read a steered record"),
        }
    }

    /// A build whose table predates `steer-queue/v1` (the shipped table without it) refuses a
    /// `Steered` frame as a newer writer naming the feature. It never skips the record: that
    /// would drop a user message from the transcript.
    #[test]
    fn a_build_without_steer_queue_v1_refuses_a_steered_record_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(STEERED).unwrap();
        let bytes = encode(6, 1, &record).unwrap();
        match decode_next_with(&bytes, &[SCOPE_FEATURE]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, GATED_FRAMING_VERSION);
                assert_eq!(missing, vec![STEER_QUEUE_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
        let _table = lock_feature_table_for_test();
        let _forgotten = forget_feature_for_test(STEER_QUEUE_FEATURE);
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { .. }
        ));
    }

    /// A `ToolDispatchIntent` naming the module its call was dispatched to and the schema it was
    /// dispatched against, as the bytes of its record.
    const DISPATCH_MODULE_INTENT: &str = r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"edit","args":{"f":"a"},"module_id":"aft","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2}}"#;

    /// An intent carrying `module_id` and `schema_pin` is always a gated frame requiring exactly
    /// `dispatch-module/v1`, even through plain [`encode`], and this build reads both back. The
    /// digest is a literal computed outside this crate over the version byte 3, seq 1 and fence
    /// 1 as u64 LE, and the payload.
    #[test]
    fn a_dispatch_module_intent_is_a_gated_frame_requiring_dispatch_module_v1_and_round_trips() {
        // Decodes with the shipped table, which a sibling test edits to hide dispatch-module/v1.
        let _table = lock_feature_table_for_test();
        let record: WalRecord = serde_json::from_str(DISPATCH_MODULE_INTENT).unwrap();
        assert_eq!(record.required_features(), &[DISPATCH_MODULE_FEATURE]);
        let bytes = encode(1, 1, &record).unwrap();
        let payload =
            format!(r#"{{"requires":["dispatch-module/v1"],"record":{DISPATCH_MODULE_INTENT}}}"#);
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        const EXPECTED_DIGEST: [u8; 32] = [
            0x89, 0x39, 0x61, 0x28, 0xa9, 0xfd, 0x4f, 0xd4, 0xcb, 0x3c, 0x8a, 0x75, 0x87, 0x20,
            0x44, 0xd3, 0xc3, 0xb4, 0xeb, 0xe5, 0xe7, 0xb1, 0xd2, 0x2e, 0x01, 0x09, 0x19, 0x2b,
            0x30, 0x27, 0xac, 0xe3,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);

        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("this build must read an intent naming its dispatch module");
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
        assert_eq!(encode(1, 1, &framed.record).unwrap(), bytes);
        let WalRecord::ToolDispatchIntent {
            module_id,
            schema_pin,
            ..
        } = &framed.record
        else {
            panic!("expected ToolDispatchIntent");
        };
        assert_eq!(module_id.as_deref(), Some("aft"));
        assert_eq!(
            schema_pin
                .as_ref()
                .map(|p| (p.schema_digest.as_str(), p.semantics)),
            Some(("sha256:5f2c", 2))
        );
    }

    /// Either field alone gates the frame: a module without a pin (a session whose frozen tools
    /// carry no schema digests) and a pin without a module both require `dispatch-module/v1`.
    #[test]
    fn either_dispatch_module_field_alone_gates_the_intent() {
        // Decodes with the shipped table, which a sibling test edits to hide dispatch-module/v1.
        let _table = lock_feature_table_for_test();
        let module_only = DISPATCH_MODULE_INTENT.replacen(
            r#","schema_pin":{"schema_digest":"sha256:5f2c","semantics":2}"#,
            "",
            1,
        );
        let pin_only = DISPATCH_MODULE_INTENT.replacen(r#","module_id":"aft""#, "", 1);
        for json in [module_only, pin_only] {
            assert_ne!(json, DISPATCH_MODULE_INTENT);
            let record: WalRecord = serde_json::from_str(&json).unwrap();
            assert_eq!(record.required_features(), &[DISPATCH_MODULE_FEATURE]);
            let bytes = encode(1, 1, &record).unwrap();
            assert_eq!(bytes[4], GATED_FRAMING_VERSION);
            match decode_next(&bytes) {
                DecodeStep::Record { framed, .. } => assert_eq!(framed.record, record),
                _ => panic!("this build must read {json}"),
            }
        }
    }

    /// A build whose table predates `dispatch-module/v1` (the shipped table without it) refuses
    /// an intent naming its module as a newer writer naming the feature. It never reads the
    /// intent with the module ignored, which would report the call as never dispatched.
    #[test]
    fn a_build_without_dispatch_module_v1_refuses_the_intent_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(DISPATCH_MODULE_INTENT).unwrap();
        let bytes = encode(6, 1, &record).unwrap();
        let older: Vec<&str> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != DISPATCH_MODULE_FEATURE)
            .collect();
        match decode_next_with(&bytes, &older) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, GATED_FRAMING_VERSION);
                assert_eq!(missing, vec![DISPATCH_MODULE_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
        let _table = lock_feature_table_for_test();
        let _forgotten = forget_feature_for_test(DISPATCH_MODULE_FEATURE);
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { .. }
        ));
    }

    /// A `Queued` record's `delivery` is ungated: with it the record stays an ordinary frame,
    /// and a record without it keeps the bytes it had before the field existed.
    #[test]
    fn a_queued_delivery_is_an_ungated_field() {
        let plain = r#"{"type":"queued","submission_id":"sub-1","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[]}}"#;
        let record: WalRecord = serde_json::from_str(plain).unwrap();
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], FRAMING_VERSION);
        let WalRecord::Queued { delivery, .. } = &record else {
            panic!("expected Queued");
        };
        assert_eq!(*delivery, None);
        let interrupt = plain.replacen(r#""sub-1","#, r#""sub-1","delivery":"interrupt","#, 1);
        let record: WalRecord = serde_json::from_str(&interrupt).unwrap();
        assert!(record.required_features().is_empty());
        assert_eq!(encode(1, 1, &record).unwrap()[4], FRAMING_VERSION);
        let WalRecord::Queued { delivery, .. } = &record else {
            panic!("expected Queued");
        };
        assert_eq!(*delivery, Some(broca_protocol::Delivery::Interrupt));
    }

    /// A `RunStarted` with a frozen scope, as the bytes of its record.
    const SCOPED_RUN_STARTED: &str = r#"{"type":"run_started","run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"principal":{"kind":"reserved","module_id":"prefrontal-core"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"},"scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3}}"#;

    /// A scoped `RunStarted` is framed as a gated frame requiring exactly `scope/v1`, even
    /// through plain [`encode`], and this build reads it back with the scope intact. The
    /// digest is a literal computed outside this crate over the version byte 3, seq 1 and
    /// fence 1 as u64 LE, and the payload.
    #[test]
    fn a_scoped_run_started_is_a_gated_frame_requiring_scope_v1_and_round_trips() {
        let record: WalRecord = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        assert_eq!(record.required_features(), &[SCOPE_FEATURE]);
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes, encode_with_requires(1, 1, &[], &record).unwrap());
        assert_eq!(
            bytes,
            encode_with_requires(1, 1, &[SCOPE_FEATURE], &record).unwrap()
        );

        let payload = format!(r#"{{"requires":["scope/v1"],"record":{SCOPED_RUN_STARTED}}}"#);
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        const EXPECTED_DIGEST: [u8; 32] = [
            0xd9, 0x0d, 0x4a, 0xe6, 0xfd, 0x7f, 0x40, 0xa1, 0x68, 0xf7, 0x1b, 0x77, 0xe8, 0xce,
            0xa0, 0xf4, 0x3a, 0xdd, 0x9a, 0x5b, 0x24, 0x66, 0x83, 0x53, 0xe0, 0x3b, 0x77, 0x6b,
            0xa8, 0x4e, 0x67, 0xac,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);

        match decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!(framed.record, record);
                let WalRecord::RunStarted { scope, .. } = &framed.record else {
                    panic!("expected RunStarted");
                };
                assert_eq!(
                    scope.as_ref().map(|s| (
                        s.owner.as_owner_string(),
                        s.scope_ref.as_str(),
                        s.scope_epoch
                    )),
                    Some(("reserved:prefrontal-core".to_owned(), "head-1", 3))
                );
            }
            _ => panic!("this build must read a scoped RunStarted"),
        }
    }

    /// A build whose table lacks `scope/v1` (simulated with an empty table) refuses a scoped
    /// `RunStarted` as a newer writer naming the feature, never as corruption, and never by
    /// reading the record with its scope ignored.
    #[test]
    fn a_build_without_scope_v1_refuses_a_scoped_run_started_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(SCOPED_RUN_STARTED).unwrap();
        let bytes = encode(4, 2, &record).unwrap();
        match decode_next_with(&bytes, &[]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, 3);
                assert_eq!(missing, vec![SCOPE_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
    }

    /// A `RunStarted` carrying a frozen plan manifest, as the bytes of its record: a pretty-printed
    /// plan whose keys are not in sorted order, and three answered provider fetches, one of whose
    /// system text is present but empty.
    fn manifest_run_started_json(run: &str) -> String {
        include_str!("../tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", run)
    }

    /// A `RunStarted` with a plan manifest is framed as a gated frame requiring exactly
    /// `plan-manifest/v1`, even through plain [`encode`], and its bytes survive a decode and
    /// re-encode exactly: the opaque plan keeps its whitespace and key order, and a fetch whose
    /// text is present but empty stays present.
    #[test]
    fn a_manifest_run_started_is_a_gated_frame_requiring_plan_manifest_v1_and_round_trips() {
        use crate::record::FetchOutcome;
        let json = manifest_run_started_json("run-7");
        let record: WalRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(record.required_features(), &[PLAN_MANIFEST_FEATURE]);
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        let payload = format!(r#"{{"requires":["plan-manifest/v1"],"record":{json}}}"#);
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);

        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("this build must read a RunStarted with a plan manifest");
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
        assert_eq!(encode(1, 1, &framed.record).unwrap(), bytes);
        let WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } = &framed.record
        else {
            panic!("the manifest must come through the decode");
        };
        assert!(manifest
            .plan
            .as_json_text()
            .starts_with("{\n  \"tool_items\": [\n    {\n      \"provider\": \"aft\","));
        assert_eq!(
            manifest.plan_identity,
            "b66c352ab9cbe2c12d1a1c2bceb553c6e7223a846692f945de6bd293be4e20dc"
        );
        let providers: Vec<&str> = manifest
            .fetches
            .iter()
            .map(|f| f.provider.as_str())
            .collect();
        assert_eq!(providers, ["aft", "magic-context", "prefrontal-core"]);
        let FetchOutcome::Answered { tools, text, .. } = &manifest.fetches[0].outcome else {
            panic!("aft answered");
        };
        assert_eq!(tools.len(), 4);
        assert_eq!(text.as_deref(), Some("Read a file before you edit it."));
        let FetchOutcome::Answered { text, .. } = &manifest.fetches[1].outcome else {
            panic!("magic-context answered");
        };
        assert_eq!(text.as_deref(), Some(""));
        assert_eq!(manifest.tools.len(), 8);
        assert_eq!(manifest.tools[0].name, "bash");
        assert_eq!(manifest.join_version, 1);
        assert_eq!(manifest.role_versions["head"], "4");
    }

    /// A record carrying both a frozen scope and a plan manifest requires both features, sorted,
    /// so a build lacking either refuses it.
    #[test]
    fn a_scoped_manifest_run_started_requires_both_features() {
        let json = manifest_run_started_json("run-8").replacen(
            r#","manifest":"#,
            r#","scope":{"owner":{"kind":"direct"},"ref":"head-1","scope_epoch":3},"manifest":"#,
            1,
        );
        let record: WalRecord = serde_json::from_str(&json).unwrap();
        assert_eq!(
            record.required_features(),
            &[PLAN_MANIFEST_FEATURE, SCOPE_FEATURE]
        );
        let bytes = encode(1, 1, &record).unwrap();
        let payload = format!(r#"{{"requires":["plan-manifest/v1","scope/v1"],"record":{json}}}"#);
        assert_eq!(String::from_utf8(bytes[53..].to_vec()).unwrap(), payload);
        match decode_next_with(&bytes, &[SCOPE_FEATURE, STEER_QUEUE_FEATURE]) {
            DecodeStep::NewerWriter { missing, .. } => {
                assert_eq!(missing, vec![PLAN_MANIFEST_FEATURE]);
            }
            _ => panic!("expected NewerWriter"),
        }
    }

    /// A build whose table predates `plan-manifest/v1` (the shipped table without it) refuses a
    /// `RunStarted` carrying a manifest as a newer writer naming the feature: never as
    /// corruption, and never by reading the record with its manifest ignored, which would
    /// render the session without its frozen tools.
    #[test]
    fn a_build_without_plan_manifest_v1_refuses_a_manifest_run_started_as_newer_writer() {
        let record: WalRecord = serde_json::from_str(&manifest_run_started_json("run-9")).unwrap();
        let bytes = encode(4, 2, &record).unwrap();
        match decode_next_with(&bytes, &[SCOPE_FEATURE, STEER_QUEUE_FEATURE]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, GATED_FRAMING_VERSION);
                assert_eq!(missing, vec![PLAN_MANIFEST_FEATURE]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
    }

    /// Test builds read a gated frame naming [`TEST_KNOWN_FEATURE`] through the production
    /// decoder, which is what lets other crates' tests exercise a readable gated frame, while
    /// the shipped table that placement reads still lacks it.
    #[test]
    fn test_builds_know_the_test_only_feature_and_the_shipped_table_does_not() {
        assert!(!KNOWN_FEATURES.contains(&TEST_KNOWN_FEATURE));
        let bytes = encode_with_requires(2, 1, &[TEST_KNOWN_FEATURE], &rec(2)).unwrap();
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(framed.record, rec(2)),
            _ => panic!("a test build must read the test-only feature"),
        }
    }

    const TEST_FEATURE: &str = "test-feature/v1";

    /// A gated frame's exact bytes, with a digest computed outside this crate over the version
    /// byte 3, seq 1 and fence 1 as u64 LE, and the payload below.
    #[test]
    fn gated_frame_wire_bytes_are_pinned() {
        let bytes = encode_with_requires(1, 1, &[TEST_FEATURE], &rec(1)).unwrap();
        let payload: &[u8] =
            br#"{"requires":["test-feature/v1"],"record":{"type":"turn_finished","step_id":1}}"#;
        assert_eq!(&bytes[0..4], &(payload.len() as u32).to_le_bytes());
        assert_eq!(bytes[4], 3, "gated framing version");
        assert_eq!(&bytes[5..13], &1u64.to_le_bytes(), "seq");
        assert_eq!(&bytes[13..21], &1u64.to_le_bytes(), "fence");
        const EXPECTED_DIGEST: [u8; 32] = [
            0xe4, 0x7e, 0x87, 0xcd, 0x99, 0x12, 0x1c, 0x97, 0xa2, 0x49, 0xb8, 0x23, 0x13, 0x1b,
            0x80, 0x8d, 0xdd, 0x18, 0x9d, 0x68, 0xca, 0x27, 0x07, 0x9b, 0x64, 0x1c, 0xcb, 0x4d,
            0x7b, 0xc8, 0x5b, 0x47,
        ];
        assert_eq!(&bytes[21..53], &EXPECTED_DIGEST[..], "digest");
        assert_eq!(&bytes[53..], payload, "payload");
    }

    /// With nothing required the encoder writes the ordinary version-1 frame, byte for byte, so
    /// no record that needs no gate becomes unreadable to an older build.
    #[test]
    fn an_empty_requires_list_encodes_an_ordinary_version_1_frame() {
        assert_eq!(
            encode_with_requires(7, 3, &[], &rec(1)).unwrap(),
            encode(7, 3, &rec(1)).unwrap()
        );
        assert_eq!(encode_with_requires(7, 3, &[], &rec(1)).unwrap()[4], 1);
    }

    #[test]
    fn requires_are_sorted_deduplicated_and_held_to_the_capability_grammar() {
        let a = encode_with_requires(1, 1, &["b-two/v1", "a-one/v2", "b-two/v1"], &rec(1));
        let b = encode_with_requires(1, 1, &["a-one/v2", "b-two/v1"], &rec(1));
        assert_eq!(a.unwrap(), b.unwrap());
        for bad in ["", "Upper/v1", "name", "name/v", "name/vx", "/v1", "a_b/v1"] {
            assert!(
                matches!(
                    encode_with_requires(1, 1, &[bad], &rec(1)),
                    Err(FrameError::InvalidFeatureName { .. })
                ),
                "{bad:?} must be refused"
            );
        }
    }

    /// A gated frame whose every required name is in the reader's table decodes to its record.
    /// The table here holds a test-only entry; the shipped table is empty.
    #[test]
    fn a_gated_frame_with_known_features_decodes_to_its_record() {
        let bytes = encode_with_requires(9, 2, &[TEST_FEATURE], &rec(4)).unwrap();
        match decode_next_with(&bytes, &[TEST_FEATURE]) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!((framed.seq, framed.fence), (9, 2));
                assert_eq!(framed.record, rec(4));
            }
            _ => panic!("expected the gated record to decode"),
        }
    }

    /// A gated frame naming a feature the reader lacks is refused as a newer writer, naming
    /// exactly the missing features, never as corruption and never skipped.
    #[test]
    fn an_unknown_feature_is_refused_as_newer_writer_not_corruption() {
        let bytes =
            encode_with_requires(1, 1, &["zz-later/v1", TEST_FEATURE, "aa-newer/v2"], &rec(1))
                .unwrap();
        match decode_next_with(&bytes, &[TEST_FEATURE]) {
            DecodeStep::NewerWriter { ver, missing } => {
                assert_eq!(ver, 3);
                assert_eq!(missing, vec!["aa-newer/v2", "zz-later/v1"]);
            }
            DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
            _ => panic!("expected NewerWriter"),
        }
        // `KNOWN_FEATURES` holds none of the three names this frame requires, so `decode_next`
        // reports all three as missing.
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { ver: 3, missing } if missing.len() == 3
        ));
    }

    /// The requirements are checked before the record is interpreted. A newer build's record
    /// may use a value this build's types reject; it must still read as a newer writer.
    #[test]
    fn requirements_are_checked_before_the_record_is_parsed() {
        let bytes = frame_with_version(
            3,
            1,
            1,
            br#"{"requires":["aa-newer/v1"],"record":{"type":"turn_finished","step_id":"not-a-number"}}"#,
        );
        assert!(matches!(
            decode_next(&bytes),
            DecodeStep::NewerWriter { ver: 3, .. }
        ));
    }

    /// A digest-valid gated frame whose envelope is malformed is corruption: it cannot have
    /// come from any writer that follows the format.
    #[test]
    fn a_malformed_gated_envelope_is_corruption() {
        for payload in [
            &br#"{"record":{"type":"turn_finished","step_id":1}}"#[..],
            br#"{"requires":["aa-newer/v1"]}"#,
            br#"{"type":"turn_finished","step_id":1}"#,
            b"not json",
        ] {
            assert!(matches!(
                decode_next(&frame_with_version(3, 1, 1, payload)),
                DecodeStep::Corrupt(FrameError::Deserialize(_))
            ));
        }
    }

    /// The digest covers the version byte, so relabelling a gated frame as an ordinary one
    /// fails its digest. Without that, a flipped version byte would turn a record whose
    /// requirements must be checked into one read with no check at all.
    #[test]
    fn a_gated_frame_relabelled_as_version_1_fails_its_digest() {
        let mut bytes = encode_with_requires(5, 1, &[TEST_FEATURE], &rec(1)).unwrap();
        bytes[4] = FRAMING_VERSION;
        assert!(matches!(
            decode_next_with(&bytes, &[TEST_FEATURE]),
            DecodeStep::Corrupt(FrameError::DigestMismatch { seq: 5 })
        ));
    }

    #[test]
    fn truncated_header_or_payload_is_torn_tail() {
        let bytes = encode(1, 1, &rec(1)).unwrap();
        assert!(matches!(decode_next(&bytes[..3]), DecodeStep::TornTail)); // partial header
        assert!(matches!(
            decode_next(&bytes[..HEADER_LEN + 2]),
            DecodeStep::TornTail
        )); // partial payload
    }

    /// Simulate the reader table before the transform reader release, using every hooked
    /// golden and a declarations-only record. Feature refusal must precede payload decoding.
    #[test]
    fn a_build_without_step_transforms_refuses_hooked_wal_as_newer_writer() {
        // The current-reader check decodes steered and module-naming intent frames with the
        // shipped table; sibling tests hide steer-queue/v1 and dispatch-module/v1 process-wide.
        let _table = lock_feature_table_for_test();
        let older: Vec<&str> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != STEP_TRANSFORMS_FEATURE)
            .collect();
        for json in [
            include_str!("../tests/golden/step_transforms/run_started.json"),
            include_str!("../tests/golden/step_transforms/queued.json"),
            include_str!("../tests/golden/step_transforms/steered.json"),
            include_str!("../tests/golden/step_transforms/model_step_finished.json"),
            include_str!("../tests/golden/step_transforms/tool_dispatch_intent.json"),
            include_str!("../tests/golden/step_transforms/tool_no_dispatch.json"),
            include_str!("../tests/golden/step_transforms/tool_result.json"),
            include_str!("../tests/golden/step_transforms/tool_result_deny.json"),
        ] {
            let value: serde_json::Value = serde_json::from_str(json).unwrap();
            let mut record: WalRecord = serde_json::from_value(value["record"].clone()).unwrap();
            for declarations_only in [false, true] {
                if declarations_only {
                    if let WalRecord::RunStarted { hooks, .. } = &mut record {
                        *hooks = None;
                    } else {
                        continue;
                    }
                }
                let bytes = encode(7, 11, &record).unwrap();
                assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
                match decode_next_with(&bytes, &older) {
                    DecodeStep::NewerWriter { ver, missing } => {
                        assert_eq!(ver, GATED_FRAMING_VERSION);
                        assert_eq!(missing, vec![STEP_TRANSFORMS_FEATURE]);
                    }
                    DecodeStep::Corrupt(error) => panic!("refused as corruption: {error}"),
                    _ => panic!("expected newer-writer refusal"),
                }
            }
        }
    }

    #[test]
    fn flipped_payload_byte_is_corruption_not_torn() {
        let mut bytes = encode(1, 1, &rec(1)).unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff; // corrupt the (complete) payload
        match decode_next(&bytes) {
            DecodeStep::Corrupt(FrameError::DigestMismatch { seq }) => assert_eq!(seq, 1),
            other => panic!(
                "expected DigestMismatch, got {:?}",
                matches!(other, DecodeStep::TornTail)
            ),
        }
    }
}

#[cfg(test)]
mod session_class_tests {
    use super::*;
    #[test]
    fn session_class_is_optional_and_independently_must_understand() {
        let bytes = include_str!("../tests/golden/pre_step_transforms/run_started.json").trim();
        let plain: WalRecord = serde_json::from_str(bytes).unwrap();
        assert!(plain.required_features().is_empty());
        assert_eq!(serde_json::to_string(&plain).unwrap(), bytes);
        let mut value: serde_json::Value = serde_json::from_str(bytes).unwrap();
        value["session_class"] = serde_json::json!("head");
        let classed: WalRecord = serde_json::from_value(value).unwrap();
        assert_eq!(classed.required_features(), &[SESSION_CLASS_FEATURE]);
        assert_eq!(
            serde_json::from_slice::<WalRecord>(&serde_json::to_vec(&classed).unwrap()).unwrap(),
            classed
        );
        let known: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != SESSION_CLASS_FEATURE)
            .collect();
        assert!(
            matches!(decode_next_with(&encode(1, 1, &classed).unwrap(), &known), DecodeStep::NewerWriter { missing, .. } if missing == [SESSION_CLASS_FEATURE])
        );
        assert!(matches!(
            decode_next_with(&encode(1, 1, &plain).unwrap(), &known),
            DecodeStep::Record { .. }
        ));
    }
}

#[cfg(test)]
mod v4_header_len_tests {
    use super::*;

    /// A real version 4 header whose requirements array is `requires` long once encoded.
    fn header(requires: &[&str]) -> Vec<u8> {
        broca_wal_reader::frame::encode_header(
            broca_wal_reader::frame::Metadata {
                seq: 1,
                fence: 1,
                appended_ms: 1,
                writer_id: [7; 16],
            },
            requires,
            0,
            [0; 32],
        )
        .unwrap()
    }

    #[test]
    fn the_header_is_sized_only_once_both_requires_len_bytes_are_present() {
        let full = header(&["frame-stamps/v1"]);
        let expected = full.len();
        // 44 bytes end inside the writer id, 45 before `requires_len`, 46 inside it: none of
        // them can size the header, and none may read past the slice.
        for len in 44..=46 {
            assert_eq!(v4_header_len(&full[..len]), None, "{len} bytes");
        }
        assert_eq!(v4_header_len(&full[..47]), Some(expected), "47 bytes");
        assert_eq!(v4_header_len(&full), Some(expected));
        assert_eq!(v4_header_len(&[]), None);
    }

    #[test]
    fn a_requirements_array_cut_mid_way_is_sized_but_not_decoded() {
        // A torn write that stops inside the requirements array leaves `requires_len` whole, so
        // the header can be sized and recovery knows how much more it would need, but the
        // header itself is incomplete rather than corrupt.
        for requires in [
            &["frame-stamps/v1"][..],
            &["frame-stamps/v1", "input-token-cap/v1"],
        ] {
            let full = header(requires);
            let array_len = full.len() - broca_wal_reader::frame::FIXED_HEADER_LEN;
            let cut = 47 + array_len / 2;
            assert_eq!(v4_header_len(&full[..cut]), Some(full.len()));
            assert_eq!(
                broca_wal_reader::frame::decode_header(&full[..cut]).unwrap(),
                None,
                "{requires:?}: a header cut inside its requirements array needs more bytes"
            );
        }
    }

    #[test]
    fn a_prepared_frame_measures_the_frame_it_stamps() {
        for record in [
            WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(3),
            },
            WalRecord::InputTokenCapFrozen { limit: 9 },
        ] {
            let prepared = PreparedV4Frame::prepare(&record).unwrap();
            assert!(prepared.requires().contains(&FRAME_STAMPS_FEATURE));
            let frame = prepared
                .stamp(broca_wal_reader::frame::Metadata {
                    seq: 4,
                    fence: 2,
                    appended_ms: u64::MAX,
                    writer_id: [1; 16],
                })
                .unwrap();
            assert_eq!(frame.len(), prepared.frame_len(), "{record:?}");
        }
    }
}
