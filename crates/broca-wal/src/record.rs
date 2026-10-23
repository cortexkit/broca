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
    FetchOutcome, FrozenDeclaration, FrozenHoldArgument, FrozenReply, OpaqueJson, PlanManifest,
    ProviderFetch,
};
mod compaction;
pub use compaction::{
    compaction_replacement_sha256, CompactionCursor, CompactionDisposition, CompactionMessageKind,
    CompactionRange, CompactionStabilityRank, CompactionStrip, CompactionViewChoice,
    CompactionWait, CompactionWaitExit, RejectedCompaction, MAX_REPLACEMENT_BYTES,
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
///
/// The EXHAUSTIVE matches on this enum name `Unknown` explicitly, so adding a variant still
/// fails to compile at each of them until someone decides what it means there. Many other
/// sites pick out a few variants and end in `_ =>`; those skip `Unknown` by construction and
/// will also say nothing when a variant is added. Do not add new wildcard arms to silence
/// the compiler at an exhaustive site: name the variant.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum WalRecord {
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
    },
    /// Evidence for ONE provider HTTP attempt of a model step: what was sent and received, as
    /// byte lengths and SHA-256 digests, and how the attempt ended. One per attempt, failed
    /// and cut attempts included, each written before the step's `ModelStepFinished` (or the
    /// run's terminal when the step failed). Appended without an fsync; it becomes durable at
    /// the next existing barrier.
    ///
    /// PURELY INFORMATIONAL under the skip contract on this enum: replay, the reconstructed
    /// prompt, projection, the control stream, billing and export all skip it exactly as they
    /// skip [`WalRecord::Unknown`], so a WAL with these records removed reads as the same
    /// session. `retries_used` on `ModelStepFinished` stays the authoritative retry count.
    ModelAttemptFinished {
        /// The same step id the step's `ModelStepFinished` carries.
        step_id: StepId,
        /// 0-based attempt ordinal within the step, gapless.
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
        /// the last content. Provider and model come from the run's `run_started`; no identity is
        /// duplicated on the attempt.
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
    /// [`WalRecord::Unknown`]. It is never a transcript message, and no reader feature gates it.
    /// Billing reads it: each warm becomes its own export fact, marked `warm`, on the run whose
    /// request it re-sent. An older build skips it and so exports no fact for it.
    CacheWarmed {
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
        /// The usage the provider reported for the warm; a count it did not report is absent.
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
                | WalRecord::ModelAttemptFinished { .. }
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
        let base = self.base_required_features();
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
        if self.carries_google_rpc_error_details() {
            with_feature(base, crate::framing::GOOGLE_RPC_ERROR_DETAILS_FEATURE)
        } else {
            base
        }
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

// `WalRecord::required_features` returns sorted static slices without allocation. Records
// without agent-run identifiers keep their existing requirements; records carrying those
// identifiers add the agent-run requirement to every combination of other frozen fields,
// including malformed combinations a reader may encounter.
const AGENT_RUN_BASE_FEATURES: [&str; 6] = [
    crate::framing::DEFERRED_CALL_FEATURE,
    crate::framing::FLOW_SCOPE_FEATURE,
    crate::framing::PLAN_MANIFEST_FEATURE,
    crate::framing::RETENTION_FEATURE,
    crate::framing::SCOPE_FEATURE,
    crate::framing::STEP_TRANSFORMS_FEATURE,
];

struct AgentRunFeatures {
    names: [&'static str; 7],
    len: usize,
}

const AGENT_RUN_FEATURE_SETS: [AgentRunFeatures; 64] = {
    let mut sets = [const {
        AgentRunFeatures {
            names: [""; 7],
            len: 0,
        }
    }; 64];
    let mut mask = 0;
    while mask < sets.len() {
        sets[mask].names[0] = crate::framing::AGENT_RUN_SCOPE_FEATURE;
        sets[mask].len = 1;
        let mut bit = 0;
        while bit < AGENT_RUN_BASE_FEATURES.len() {
            if mask & (1 << bit) != 0 {
                let at = sets[mask].len;
                sets[mask].names[at] = AGENT_RUN_BASE_FEATURES[bit];
                sets[mask].len += 1;
            }
            bit += 1;
        }
        mask += 1;
    }
    sets
};

fn agent_run_features(base: &[&str]) -> &'static [&'static str] {
    let mask = base.iter().fold(0, |mask, name| {
        let bit = AGENT_RUN_BASE_FEATURES
            .iter()
            .position(|known| known == name)
            .expect("agent-run scope's existing features must be listed");
        mask | (1 << bit)
    });
    let set = &AGENT_RUN_FEATURE_SETS[mask];
    &set.names[..set.len]
}

// Precompute the possible feature-name combinations so append discovers them without allocating.
const CLASS_BASE_FEATURES: [&str; 7] = [
    crate::framing::AGENT_RUN_SCOPE_FEATURE,
    crate::framing::DEFERRED_CALL_FEATURE,
    crate::framing::FLOW_SCOPE_FEATURE,
    crate::framing::PLAN_MANIFEST_FEATURE,
    crate::framing::RETENTION_FEATURE,
    crate::framing::SCOPE_FEATURE,
    crate::framing::STEP_TRANSFORMS_FEATURE,
];
struct ClassFeatures {
    names: [&'static str; 8],
    len: usize,
}
static CLASS_FEATURE_SETS: [ClassFeatures; 128] = {
    let mut sets = [const {
        ClassFeatures {
            names: [""; 8],
            len: 0,
        }
    }; 128];
    let mut mask = 0;
    while mask < sets.len() {
        let mut bit = 0;
        while bit < CLASS_BASE_FEATURES.len() {
            if bit == 6 {
                sets[mask].names[sets[mask].len] = crate::framing::SESSION_CLASS_FEATURE;
                sets[mask].len += 1;
            }
            if mask & (1 << bit) != 0 {
                sets[mask].names[sets[mask].len] = CLASS_BASE_FEATURES[bit];
                sets[mask].len += 1;
            }
            bit += 1;
        }
        mask += 1;
    }
    sets
};
fn session_class_features(base: &[&str]) -> &'static [&'static str] {
    let mask = base.iter().fold(0, |mask, name| {
        let bit = CLASS_BASE_FEATURES
            .iter()
            .position(|known| known == name)
            .expect("classed RunStarted features must be listed");
        mask | (1 << bit)
    });
    let set = &CLASS_FEATURE_SETS[mask];
    &set.names[..set.len]
}

// Precompute sorted feature unions: capped records can also carry scope, hooks or retention.
const INPUT_CAP_BASE_FEATURES: [&str; 8] = [
    crate::framing::AGENT_RUN_SCOPE_FEATURE,
    crate::framing::DEFERRED_CALL_FEATURE,
    crate::framing::FLOW_SCOPE_FEATURE,
    crate::framing::PLAN_MANIFEST_FEATURE,
    crate::framing::RETENTION_FEATURE,
    crate::framing::SCOPE_FEATURE,
    crate::framing::SESSION_CLASS_FEATURE,
    crate::framing::STEP_TRANSFORMS_FEATURE,
];
struct InputCapFeatures {
    names: [&'static str; 9],
    len: usize,
}
static INPUT_CAP_FEATURE_SETS: [InputCapFeatures; 256] = {
    let mut sets = [const {
        InputCapFeatures {
            names: [""; 9],
            len: 0,
        }
    }; 256];
    let mut mask = 0;
    while mask < sets.len() {
        let mut bit = 0;
        while bit < INPUT_CAP_BASE_FEATURES.len() {
            if bit == 3 {
                sets[mask].names[sets[mask].len] = crate::framing::INPUT_TOKEN_CAP_FEATURE;
                sets[mask].len += 1;
            }
            if mask & (1 << bit) != 0 {
                sets[mask].names[sets[mask].len] = INPUT_CAP_BASE_FEATURES[bit];
                sets[mask].len += 1;
            }
            bit += 1;
        }
        mask += 1;
    }
    sets
};
fn input_cap_features(base: &[&str]) -> &'static [&'static str] {
    let mask = base.iter().fold(0, |mask, name| {
        let bit = INPUT_CAP_BASE_FEATURES
            .iter()
            .position(|known| known == name)
            .expect("input cap's existing features must be listed");
        mask | (1 << bit)
    });
    let set = &INPUT_CAP_FEATURE_SETS[mask];
    &set.names[..set.len]
}

/// `base` with `feature` added in sorted position. The feature sets a record can require are a
/// small fixed family, so each distinct union is built once and kept for the process lifetime;
/// the cache is keyed by the address of `base`, which is always a static slice.
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
    /// The credential id (`<method>:<provider>[:<account>]`) to re-`get` on resume. Method,
    /// provider, and account all parse back out of this one string, so no separate fields.
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
    /// method, provider and account all parse back out of it. Use `account_ref` only when you
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
    /// to routing. Absent when broca reports nothing for the run (the `legacy_static` path).
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
/// was used.
pub const ACCOUNT_DECISION_UNREACHABLE: &str = "unreachable";
/// Routing does not select accounts for this provider; the static pick was used.
pub const ACCOUNT_DECISION_UNSUPPORTED: &str = "unsupported";
/// The caller named the account; routing was not asked.
pub const ACCOUNT_DECISION_PINNED: &str = "pinned";
/// The provider had at most one candidate account, so there was nothing to ask.
pub const ACCOUNT_DECISION_SINGLE_CANDIDATE: &str = "single_candidate";
/// The operator's `account_selection.legacy_static` setting in `broca.jsonc`, an emergency
/// switch, bypassed routing entirely.
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
    Denied { reason: String },
    ValidationFailed { detail: String },
    Unavailable { detail: String },
    PolicyBlocked { detail: String },
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
            credential_id: "oauth:anthropic:work".into(),
            account_ref: Some("acct-1".into()),
            resolved_credential_id: Some("oauth:anthropic:shared".into()),
            vault_request: None,
            account_decision: None,
        });
        let record = WalRecord::RunStarted {
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
