//! The controls a run is started with: signals to stop or cancel it, the planes it reaches
//! modules through, and what admission resolved for it. None of it is rendered into a request.

use std::sync::Arc;

use tokio_util::sync::CancellationToken;

use broca_protocol::ProviderError;

#[cfg(test)]
use super::held_prompt;
#[cfg(all(test, feature = "test-clock"))]
use super::ApprovalWaitProbe;
use super::ProviderHeldCloseOut;
use crate::clock::epoch_millis;

/// Cooperative run controls. Optional controls default to "off"; ordinary control-payload
/// emission defaults to "on" so existing callers retain the complete output stream.
#[derive(Clone)]
pub struct RunControl {
    /// Historical replay fixtures need the retired v1 writer to generate durable
    /// record prefixes at each simulated crash point. Production cannot enable it.
    #[cfg(test)]
    pub(crate) legacy_compaction_fixture: bool,
    /// A compaction ride tells the compaction provider that the prompt cache is already lost,
    /// so held compaction work can land in a rebuild that costs nothing extra. Tests of what a
    /// ride delivers need one on a fresh step, where production has no proven cause to ride
    /// for, so they stand in for the cache clock with this flag: it makes the step's cache count
    /// as proven expired.
    #[cfg(test)]
    pub(crate) compaction_cache_proven_expired: bool,
    /// Session-owned v2 state shared with compaction-runner pushed-view calls.
    pub compaction_session: Option<Arc<crate::compaction_v2::session::Session>>,
    /// Receives durable entry changes without putting v2 notices in the WAL.
    pub compaction_changes: Option<Arc<dyn crate::compaction_v2::session::Changes>>,
    /// Wire admission supplies PLAN_VERSION only for a new log.
    /// Existing logs always take their mode from the first RunStarted.
    pub new_log_compaction_plan_version: Option<u32>,
    /// Attested input route, kept out of request rendering and admission identity.
    pub display_sent_by: Option<broca_protocol::BindPrincipal>,
    pub display_message_marks: Option<broca_protocol::MessageMarks>,
    /// The send's provider-account affinity on the store executor, kept out of model input.
    pub conversation_key: Option<String>,
    /// The session moved here from another computer and this is its first run since. Its
    /// account choice names the credential the session last ran on there (`warm_credential`),
    /// so routing can keep it on the account whose prompt cache already holds its prefix.
    pub moved_here: bool,
    #[cfg(test)]
    pub(crate) held_prompt_probe: Option<Arc<held_prompt::Probe>>,
    #[cfg(test)]
    pub(crate) view_release_probe: Option<Arc<held_prompt::ReleaseProbe>>,
    #[cfg(test)]
    pub(crate) retain_full_prompt: bool,
    /// Installed by the late-result runtime to rejoin durable waiting slots without re-dispatch.
    /// Absent by default: a reader or a caller retry cannot drive a provider-held run.
    pub provider_held_lifecycle: Option<Arc<dyn crate::tool_plane::ProviderHeldLifecycle>>,
    /// Batch-owned close-out state, retained if cancellation arrives after dispatch returns
    /// but before the run records its pause.
    #[doc(hidden)]
    pub provider_held_close_out: Arc<std::sync::Mutex<ProviderHeldCloseOut>>,
    /// Cheap actor-owned status; never a durability or cancellation input.
    pub step_progress: Option<crate::provider::LiveStepProgress>,
    /// A count of the steers carrying the person's own messages into this run, kept by the
    /// session's actor. Each time it rises, the run asks the provider of each tool call it is
    /// waiting on to answer now with what the call has so far (`tool.yield`), so the model can
    /// read the person's message; the call's final result arrives later as a notice. Only calls
    /// whose provider declared it can do this are asked. `None` asks nothing.
    pub person_steers: Option<crate::tool_plane::PersonSteers>,
    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) approval_wait_probe: Option<Arc<ApprovalWaitProbe>>,
    /// Clock shared with the coordinator's expiry decisions, used for `RunStarted`,
    /// `RunFinished` and `RunPaused` timestamps. Unset uses the system clock.
    pub clock: Option<crate::Clock>,
    /// Operator consent frozen only when a fresh episode is admitted; resumes use its WAL policy.
    pub account_limit_pause: bool,
    /// Boot-loaded wait cap for new pauses. Committed pauses replay their recorded wake
    /// time without consulting configuration again.
    pub max_same_account_wait: crate::SameAccountWait,
    /// Immediate cancellation: abort the model stream and stop dispatch, closing the batch
    /// under the existing at-most-once rules before finishing `Cancelled`. `None` disables
    /// this signal. Boundary cancellation uses the shared `steer` inbox instead, so it can
    /// forbid the next StepStarted without aborting this step's model or tool work.
    pub cancel: Option<CancellationToken>,
    /// Per-model-call timeout (the provider watchdog). Normal expiry closes Interrupted
    /// and releases the lease after writer shutdown. On a `broca-state/2` root (a staging
    /// format startup now refuses), a reservation sync failure takes precedence and fails the
    /// step with a WAL error instead; cancellation while awaiting permission releases the
    /// known-unsent reservation.
    /// This does not bound public run completion: a kernel sync that never returns keeps
    /// exclusive writer ownership and requires supervisor-level process recovery.
    /// `None` leaves the model-stream await unbounded.
    pub provider_call_timeout: Option<std::time::Duration>,
    /// Whether to emit ordinary payload-bearing control units (`AssistantMessage`, `ToolCall`,
    /// and `ToolResult`) on `out`. Direct loop consumers use these units, so this defaults to
    /// `true`. Module subscribers reconstruct the same units from the durable WAL projection and
    /// may disable this to avoid cloning payloads into an output channel that discards them.
    /// Lifecycle, pause, and error controls are never suppressed by this setting.
    pub emit_control_payloads: bool,
    /// Emit queue-ordered step positions for an actor's display drain. This is opt-in so
    /// direct loop consumers keep their existing bounded lifecycle-output flow.
    pub emit_display_context: bool,
    /// Pause the run for re-authentication IMMEDIATELY after admission, before any provider
    /// call. Set when the caller already knows — authoritatively, before the loop — that the
    /// credential needs re-auth (e.g. the credential vault returned `needs_reauth`: the
    /// refresh token is dead, so a provider call is doomed and would only waste a request).
    /// The run still durably ADMITS (writes `RunStarted`, so it's a real resumable run with a
    /// run id), then writes `RunPaused{AuthRequired}` carrying this error and returns —
    /// reusing the same durable-pause/resume-after-auth path a mid-run auth failure takes, so
    /// the run continues under the SAME run id once the user re-auths. `None` ⇒ never (the
    /// normal path; a credential problem that only the provider discovers still pauses
    /// mid-run via the Auth-class step failure). Off-by-default ⇒ byte-identical when unset.
    pub pause_for_auth_at_start: Option<ProviderError>,
    /// The auth-selection resolved for this execution. A fresh run freezes it into `RunStarted`;
    /// an auth-required resume compares it with the durable selection and records a separate
    /// off-render auth refreeze when it changed. Persisting the credential id keeps ordinary
    /// resumes on the same method, while persisting the vault account labels billing with the
    /// account that handled each segment. `None` means no vault token (for example, the mock
    /// path) and cannot produce an auth refreeze because there is no resolved selection to write.
    pub auth_selection: Option<broca_wal::AuthSelection>,
    /// The model's context-window limit, resolved at admission from a caller override or the
    /// catalog. A fresh run freezes it onto `RunConfig.context_limit`; a resume ignores this
    /// and keeps the value already frozen in the WAL. `None` means neither source had one.
    pub admitted_context_limit: Option<u64>,
    /// The catalog provider whose rows admission borrowed for a model whose provider the
    /// catalog doesn't carry (`RunConfig::served_provider`). A fresh run freezes it; a resume
    /// keeps the value already frozen.
    pub served_provider: Option<String>,
    /// Admission identity bit for the optional tool surface. It is frozen in `RunConfig` but
    /// never supplied to a provider renderer.
    pub tools_were_omitted: bool,
    /// The module's shutdown stop signal for this run, handed to the provider transport with
    /// each step. Deliberately separate from `cancel`: tripping it never makes the loop write
    /// `Cancelled` (see [`crate::ShutdownSeal`]). `None` means nothing will ever seal this run
    /// from outside, and steps are driven exactly as without it.
    pub shutdown_seal: Option<crate::ShutdownSeal>,
    /// The module's request to stop, tripped when the process begins to shut down. From then on
    /// the run starts no new work: no next model step, and no tool call that has not been sent.
    /// Work already in flight is left to finish: a model response streaming in is read to its
    /// end and a tool call already sent runs to its result. The run then ends `Interrupted` at
    /// the next point where it would have started something, never `Cancelled`, which means a
    /// caller asked; a run whose step in flight turns out to be its last ends as it would have
    /// without the request. Deliberately separate from `cancel` and from `shutdown_seal`: the seal cuts
    /// work in flight, this only refuses to start more. `None` means nothing asks this run to
    /// stop.
    pub stop_requested: Option<CancellationToken>,
    /// Where the session's lineage actor hands this run the messages a caller steers into it
    /// (`session.send` with `delivery: steer`). The loop appends them to the prompt at its next
    /// step boundary, and closes the inbox before it writes a terminal. `None` means nothing can
    /// steer this run, and the loop behaves exactly as without it.
    pub steer: Option<std::sync::Arc<crate::SteerInbox>>,
    /// The actor's admission snapshot head before it exposes this run's live inbox.
    /// Replay owns steers at or below this boundary; the inbox owns later steers.
    /// Keep the original boundary when initialization reloads newer WAL state.
    pub live_steer_after_seq: Option<u64>,
    /// The frozen plan manifest of a session admitted with a fetch plan. A fresh episode
    /// records it on its `RunStarted`: the first episode freezes it there, and every later one
    /// repeats it so the episodes left after a WAL fold still carry it. A `RunStarted` carrying
    /// it is written as a gated frame that builds without `plan-manifest/v1` refuse to read.
    /// `None` for a session admitted without a plan, whose `RunStarted` any build reads.
    pub manifest: Option<Arc<broca_wal::PlanManifest>>,
    /// Replace the provider-view system text only after a recorded refresh applies. A
    /// difference between legacy transcript text and manifest metadata is not a refresh.
    pub manifest_system_override: bool,
    /// The pending plan change a fresh run applies by freezing `manifest`, with its baseline
    /// facts. The session store's run loop (`broca-session-loop`) records it in the batch of
    /// the run's start; this crate's WAL loop ignores it, because it works the application out
    /// again from the WAL.
    pub applies_refresh: Option<broca_protocol::session_change::AppliedChange>,
    /// Told the WAL position of a fresh run's `RunStarted` once it is durable, and of each
    /// provider attempt record as it is appended, so the caller can report them to the routing
    /// module that chooses provider accounts. `None` reports nothing; either way the loop writes
    /// the same records.
    pub account_reporter: Option<Arc<dyn crate::AccountReporter>>,
    /// Optional installed write-time hook adapter. A frozen transform plan cannot advance
    /// without it, including on resume.
    pub hook_plane: Option<Arc<dyn crate::HookPlane>>,
    /// Input transforms computed by the lineage actor. A queue drain carries its recorded
    /// final input here; core only renders and records it, never calls pre_user.
    pub input_hooks: Option<Box<broca_wal::Hooks>>,
    /// An input hook or routing decision refused execution. Admission is durable before this
    /// terminal error is written; no model step or tool dispatch may follow it.
    pub pre_model_error: Option<ProviderError>,
    /// Optional driver that refreshes the provider's prompt cache during tool waits. The
    /// loop uses it only when this episode's keep_warm policy is auto.
    pub keep_warm: Option<Arc<dyn crate::keep_warm_wait::KeepWarmDriver>>,
    /// Files and reads human approval questions under the run's frozen session scope.
    /// The daemon's executor installs the transport; without it, a held call is refused.
    pub elicitation_plane: Option<Arc<dyn crate::tool_plane::ElicitationPlane>>,
    /// The connection to the session's compaction provider, over its daemon route, as the hook
    /// plane is to its step-transform provider.
    pub compaction_plane: Option<Arc<dyn crate::compaction::CompactionPlane>>,
    /// The budget for the compaction Setup call. Default uses COMPACTION_CALL_BUDGET_MS.
    pub compaction_config: crate::compaction::CompactionConfig,
    /// Set at boot. Whether compaction notices carry the provider's cost of editing earlier
    /// history (`HistoryEditCost`), and whether a provider that must resend the whole history
    /// after an edit is asked to reset its request chain before the next render.
    pub compaction_signals: bool,
    /// Views fetched from the session's compaction provider, offered at each step boundary.
    /// Absent, the session renders Setup's message and the views it already applied.
    pub compaction_views: Option<Arc<dyn crate::compaction::CompactionViews>>,
    #[cfg(feature = "test-clock")]
    pub test_compaction: Option<crate::compaction::test_control::Config>,
}

impl RunControl {
    pub(crate) fn now_ms(&self) -> Option<u64> {
        self.clock
            .as_ref()
            .map_or_else(epoch_millis, |clock| Some(clock.now_ms()))
    }

    /// Stop accepting steered messages: the run writes no further step. Called before every
    /// terminal or pause record, so no `Steered` record can follow the episode's end.
    pub(super) fn close_steers(&self) {
        if let Some(inbox) = &self.steer {
            inbox.close();
        }
    }
}

impl Default for RunControl {
    fn default() -> Self {
        Self {
            #[cfg(test)]
            legacy_compaction_fixture: false,
            #[cfg(test)]
            compaction_cache_proven_expired: false,
            compaction_session: None,
            compaction_changes: None,
            new_log_compaction_plan_version: None,
            display_sent_by: None,
            display_message_marks: None,
            conversation_key: None,
            moved_here: false,
            #[cfg(test)]
            held_prompt_probe: None,
            #[cfg(test)]
            view_release_probe: None,
            #[cfg(test)]
            retain_full_prompt: false,
            provider_held_lifecycle: None,
            provider_held_close_out: Default::default(),
            step_progress: None,
            person_steers: None,
            #[cfg(all(test, feature = "test-clock"))]
            approval_wait_probe: None,
            clock: None,
            account_limit_pause: false,
            max_same_account_wait: crate::SameAccountWait::default(),
            cancel: None,
            compaction_plane: None,
            compaction_views: None,
            compaction_config: Default::default(),
            compaction_signals: true,
            #[cfg(feature = "test-clock")]
            test_compaction: None,
            provider_call_timeout: None,
            emit_control_payloads: true,
            emit_display_context: false,
            pause_for_auth_at_start: None,
            auth_selection: None,
            admitted_context_limit: None,
            served_provider: None,
            tools_were_omitted: false,
            shutdown_seal: None,
            stop_requested: None,
            steer: None,
            live_steer_after_seq: None,
            manifest: None,
            manifest_system_override: false,
            applies_refresh: None,
            account_reporter: None,
            keep_warm: None,
            elicitation_plane: None,
            hook_plane: None,
            input_hooks: None,
            pre_model_error: None,
        }
    }
}
