//! Must-understand names used by durable record semantics and storage placement.

pub const INTERRUPT_REASON_FEATURE: &str = "interrupt-reason/v1";

pub const TURN_DIRECTIVES_FEATURE: &str = "turn-directives/v1";

/// A `RunStarted`, `Queued` or `Steered` record whose `request_identity_digest` carries the
/// send's `client_message_id` (llm-runner 0.5.1 §9.11). Part of the send's identity: a build
/// without it would answer a re-send under the same `send_id` with another id as a retry.
pub const CLIENT_MESSAGE_ID_FEATURE: &str = "client-message-id/v1";

/// An abstract tier on a durable queued request must survive draining and restart. A reader
/// that ignores the member could silently run the queued work at a different provider tier.
pub const SERVICE_TIER_QUEUE_FEATURE: &str = "service-tier-queue/v1";

/// A rewrite ride's provider-set wait (compaction-provider 0.5.1 §4, §8): a Setup
/// `CompactionMessage` record carrying the provider's `ride_wait_ms`, frozen with the session (a
/// build without it would give the session's rides its own short deadline), and the
/// `CompactionRideWait` / `CompactionRideWaitEnded` records of a ride whose wait was reported.
pub const COMPACTION_RIDE_WAIT_FEATURE: &str = "compaction-ride-wait/v1";

/// Setup's negotiated part rendering and pending first view affect resumed model bytes.
pub const COMPACTION_VIEW_NEGOTIATION_FEATURE: &str = "compaction-view-negotiation/v1";

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

/// Display marks stored beside send bodies and notices appended to tool results.
pub const MESSAGE_MARKS_FEATURE: &str = "message-marks/v1";

/// An optional display name on a stored `speaks_for` mark (llm-runner's `SpeaksFor.name`). It is
/// gated apart from `message-marks/v1` because a reader of marks alone decodes a named speaker
/// and silently drops the name, serving the agent without the name it was sent with.
pub const SPEAKS_FOR_NAME_FEATURE: &str = "speaks-for-name/v1";

/// A timed account pause must not be mistaken for a terminal error or an authentication pause.
pub const ACCOUNT_LIMIT_PAUSE_FEATURE: &str = "account-limit-pause/v1";

/// Compaction v2's accepted views, applied-view selections, cut markers and frozen plan version.
pub const COMPACTION_V2_FEATURE: &str = "compaction-v2/v1";

/// Checksummed view policy and attested applying-module provenance. Older readers
/// cannot reconstruct the policy's checksum or safely omit its rendering effect.
pub const COMPACTION_VIEW_META_FEATURE: &str = "compaction-view-meta/v1";

/// Physical-send reservation and settlement evidence for billing readers.
pub const BILL_ATTEMPTS_FEATURE: &str = "bill-attempts/v1";

/// Admission-time vault facts must survive replay by any writer resuming the run.
/// Reader support ships before production admission starts capturing these snapshots.
pub const BILL_VAULT_FACTS_FEATURE: &str = "bill-vault-facts/v1";

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

/// The folder of a state root that holds the session store's ordinary sessions, the ones
/// backups leave out. The durable and ephemeral classes have their own folders
/// (`session_storage::StorageClass`). Before the session store had classes it kept every session
/// here.
pub const SESSION_STORAGE_DIR: &str = "session-store";

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
/// Listed in `broca_wal::framing::KNOWN_FEATURES`: replay classes a held call as open rather
/// than indeterminate, and the runtime resumes the run when its late result arrives. The
/// daemon installs that runtime and declares `held-reply/v1` on dispatch routes in every build.
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

/// Frozen operator aliases change request identity and must not be dropped by an older reader.
pub const TOOL_NAME_ALIASES_FEATURE: &str = "tool-name-aliases/v1";

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
/// [`crate::record::WalRecord::required_features`]). A reader without it must refuse such a WAL: skipping
/// them would dispatch a call no human approved, dispatch an approved call twice, or forget a
/// question or a provider-held call it owes a withdrawal.
///
/// Reader builds interpret these records in replay, store projection and session reads. The
/// source-code check prevents production writers from emitting them until a writer release is enabled.
pub const DEFERRED_CALL_FEATURE: &str = "deferred-call/v1";

/// The feature every record of a send with an ordered `delivery` list, a `dedupe_key` or an
/// `expires_at_ms` requires: `delivery_ordered`, `send_ended` and `send_delivered` (see
/// [`crate::record::WalRecord::required_features`]). A reader without it must refuse such a WAL: skipping them
/// would start a run from a queued send that may never start one, deliver a send that was
/// replaced or expired, or render a prompt without a send that was delivered into it.
pub const DELIVERY_ORDER_FEATURE: &str = "delivery-order/v1";

/// An individual ride freezes its sender, send identity, allowed forms and chosen renderer.
/// Older readers must refuse it rather than render the ride without its frozen sender,
/// allowed forms and renderer.
pub const RIDES_V2_FEATURE: &str = "rides-v2/v1";

/// Turn-scoped system entries must not be hoisted into legacy global instructions.
pub const TURN_SYSTEM_FEATURE: &str = "turn-system/v1";

/// Writer lifecycle records fence recovery against an earlier writer's unfinished suffix.
pub const WRITER_ID_FEATURE: &str = "writer-id/v1";
