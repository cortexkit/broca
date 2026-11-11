//! The driver: admit a run, then alternate model steps and tool batches until a decision ends
//! the run.
//!
//! Each phase reads fold state from the writer, calls one seam, and appends what happened:
//! - **Admit:** the run's setup parts, `SetupStub` when the setup changed, `RunStart` and the
//!   run's input, as one durable batch before any model call.
//! - **Step:** `StepStart`, the model call, then the assistant message and `StepEnd`.
//! - **Decide:** finish with no tool calls; stop before dispatch when a stop condition holds;
//!   otherwise dispatch.
//! - **Batch:** `BatchStart` and every call's intent as one durable batch, then dispatch, then
//!   every reply and outcome and `BatchEnd` as one durable batch.
//! - **Finish:** `RunEnd`, durable before the run reports its end.

use std::sync::Arc;

use broca_core::capability::ApprovalPolicy;
use broca_core::capability::ToolManifest;
use broca_core::delivery_order::{appended_text, choose_mode, Boundary};
use broca_core::output_events::OutputEvent;
use broca_core::provider::{Provider, StepRuntime};
use broca_core::stop_conditions::{limiting_stop_satisfied, stop_satisfied};
use broca_core::tags::TagNumbering;
use broca_core::tool_plane::{
    BatchCtx, DispatchKey, DispatchRegistration, DispatchResult, ProviderHeldLifecycle,
    ProviderHeldRecorder, ProviderHeldReply, ToolOpenGuard, ToolPlane,
};
use broca_protocol::approval::ApprovalResponse;
use broca_protocol::caller::{PauseReason, RunFinishReason};
use broca_protocol::content::{AssistantMessage, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::error::ProviderError;
use broca_protocol::provider::{ModelAttempt, NormalizedEvent, ProviderControl};
use broca_protocol::request::{
    CallOptions, ExecutionMode, ModelHandle, OnRestart, ResponseFormat, StepContext, StopCondition,
    Tool, ToolChoice,
};
use broca_protocol::request::{EpisodeRenderInput, RenderHistory};
use broca_protocol::tags::{clean_computed_into, render_tags_into, ComputedRules, TagRendering};
use broca_protocol::{
    BatchId, BindIdentity, FinishReason, GenerationConfig, RunId, StepId, ToolCallId, Usage,
};
use broca_protocol::{DeliveryMode, DeliveryOrder};
use broca_session_store::codec::{Digest, Message as _};
use broca_session_store::fold::operations::{CallState, Operations, Resolution};
use broca_session_store::record::{
    Assistant, Attempt, BatchEnd, BatchStart, CallOutcome, CeilingDecided, Delivery, Input, Notice,
    PartKind, Pause, Placement, Record, Redo, ResultAppend, Resumed, RunEnd, RunStart, SendEnded,
    SetupStub, StepEnd, StepStart, ToolHeld, ToolIntent, ToolOutcome, ToolReply,
};
use broca_session_store::session::SessionError;
use broca_wal::ComputedTagRules;
use broca_wal::NoDispatchReason;
use futures::channel::oneshot;
use futures::StreamExt as _;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::entries::{Placed, Tags};
use crate::parts::{canonical, Parts};
use crate::writer::Writer;

/// The step limit a run gets when it names no stop condition, as today's loop.
pub const DEFAULT_MAX_STEPS: u64 = 100;

/// The cache-marker placement every new run freezes, as today's loop.
pub const MARKER_ORDER_VERSION: u32 = 1;

/// What admission decided for a run. The loop freezes all of it into the run's start.
#[derive(Debug, Clone)]
pub struct Admission {
    pub run_id: String,
    pub model: ModelHandle,
    /// The messages this run adds to the conversation.
    pub input: Vec<Message>,
    /// The tools the run may call, in the order the model sees them.
    pub tools: Vec<Tool>,
    pub tool_choice: ToolChoice,
    pub generation: GenerationConfig,
    pub response_format: ResponseFormat,
    pub provider_options: Value,
    /// The provider's frozen render config, opaque to the loop.
    pub render_config: Value,
    pub renderer_version: u32,
    /// Termination predicates, OR'd. Empty means a [`DEFAULT_MAX_STEPS`] step limit.
    pub stop_when: Vec<StopCondition>,
    /// What the run does when the runner stops while it runs: end interrupted (the default),
    /// or pause and continue as the same run after the runner starts again.
    pub on_restart: Option<OnRestart>,
    /// When the run was admitted and when its input was written.
    pub ts_ms: Option<u64>,
    pub written_at_ms: Option<u64>,
    /// The session's frozen plan manifest, for a session admitted with a fetch plan: the tools
    /// its providers answered with, its system text and the hooks it declares.
    pub manifest: Option<std::sync::Arc<broca_wal::PlanManifest>>,
    /// For a session on the current compaction contract, the system text every request renders
    /// above the conversation, which is never part of its transcript: the plan's system text for
    /// a planned run, otherwise the system messages the caller sent, or the previous run's when
    /// it sent none. [`run`] takes it out of `input`; a caller leaves it empty.
    pub global_system: Vec<Message>,
    /// The window the run's model may use, in tokens; compaction states its conditions from it,
    /// and a run on a plan with a compaction provider needs one.
    pub context_limit: Option<u64>,
    /// For a session's first run: the compaction plan version the session starts under, so it
    /// records and renders under the current compaction contract; `None` keeps the established
    /// layout. Today's actor asks for it when the plan names a compaction provider. A later run
    /// keeps its session's layout whatever this says.
    pub compaction_plan_version: Option<u32>,
    /// Whether the run froze part numbers into the model's view. A caller leaves it false: a new
    /// run takes it from its plan. A run read back from its start carries what the start froze.
    pub render_tags: bool,
    /// The tool-call budget the run freezes: reminders after so many tool calls, and the
    /// finalize reminder after which the run's last step has tools disabled. A budget with an
    /// input-token cap is refused for now.
    pub budget: Option<broca_protocol::request::BudgetConfig>,
    /// Whether the run keeps the prompt cache warm while it waits on tool calls. Absent is
    /// off, as today's run start reads it.
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    /// The provider account the run was admitted to, frozen so a resume uses the same one.
    pub auth_selection: Option<broca_wal::AuthSelection>,
    /// Whether the run may pause on its own account when the provider says the account's limit
    /// is reached and resets soon, rather than end, as today's admission freezes it from the
    /// boot setting.
    pub account_limit_pause: bool,
    /// The routing decision the run was admitted under.
    pub route_decision_id: Option<String>,
    /// The service tier the send asked for. The provider's frozen render config holds the tier
    /// requests are sent at; this is what the attempt records and compaction conditions name.
    pub service_tier: Option<broca_protocol::request::ServiceTier>,
    /// The reasoning variant the send asked for, which the compaction provider is told.
    pub variant: Option<String>,
    /// The workload class the send named.
    pub work_class: Option<String>,
    /// The session's context management mode, as admission resolved it.
    pub context_management: Option<broca_protocol::request::ContextManagement>,
    /// The turn directives the send named.
    pub directives: Option<Vec<broca_protocol::request::DirectiveName>>,
    /// Whether the send left its tools out, so the plan's or the session's apply.
    pub tools_were_omitted: bool,
    /// The cache marker order the run's requests are rendered with, frozen into its start. A
    /// session's first run takes the current order ([`MARKER_ORDER_VERSION`]). Every later run
    /// takes the order the session's previous run froze, even when that run froze none (a run
    /// written before the order existed), as today's loop does; a run read back from its start
    /// takes what the start froze.
    pub marker_order_version: Option<u32>,
    /// What the send said about itself. None of it changes a request; all of it is recorded
    /// on the run's start, where status, retries and reads find it.
    pub send: SendFacts,
    /// Set when something decided before the run that it must not reach the model: a
    /// `pre_user` input transform refused the run's input, or routing refused the caller's
    /// decision. The run's start is still written, so the send has a run to report, and the run
    /// then ends with this error before any step, as today's loop ends it. Unlike every other
    /// field it isn't frozen into the start, and a run read back from its start has none.
    pub pre_model_error: Option<ProviderError>,
    /// The pending plan change this run applies by freezing its manifest, recorded after the
    /// run's input in the batch of its start. Like `pre_model_error`, a run read back from its
    /// start has none: the record that followed the start keeps it.
    pub applies_refresh: Option<broca_protocol::session_change::AppliedChange>,
}

/// What the send that started a run recorded about itself, as today's run start records it.
#[derive(Debug, Clone, Default)]
pub struct SendFacts {
    /// The queued send the run consumes, when it started from the queue.
    pub submission_id: Option<String>,
    /// The caller's idempotency key, and the digest of the request it was sent with: a retry
    /// under the key is compared with the digest.
    pub send_id: Option<String>,
    pub request_identity_digest: Option<String>,
    /// The caller's label for the send.
    pub title: Option<String>,
    pub session_class: Option<String>,
    /// How long the session is kept after its last activity, in seconds.
    pub retention_secs: Option<u64>,
    /// Who sent it, as the daemon attested; and, for a send another module made on the owner's
    /// behalf, that module.
    pub principal: Option<broca_protocol::BindPrincipal>,
    pub sent_by: Option<broca_protocol::BindPrincipal>,
    /// The scope the session's runs are frozen to.
    pub scope: Option<broca_protocol::SessionScope>,
    /// What the input hooks did to the run's input, as evidence: the input itself is already
    /// the hooks' final value.
    pub hooks: Option<broca_wal::Hooks>,
    /// The send's display marks, which the run's start records for its input.
    pub message_marks: Option<broca_protocol::MessageMarks>,
}

/// The seams a run calls. None of them is owned by the run.
pub struct Seams<'a, P, T> {
    pub provider: &'a P,
    pub tools: &'a T,
    /// The bind the session's tool calls are routed under.
    pub bind: BindIdentity,
    /// Where control and display events go, if anyone listens.
    pub out: Option<&'a mpsc::Sender<OutputEvent>>,
    /// Cancels the run now: a model step in flight stops and no further call is sent.
    pub cancel: CancellationToken,
    /// Asks the run to stop starting work because the runner is shutting down. Unlike
    /// `cancel`, it cuts nothing in flight: the run stops at its next step boundary, where it
    /// pauses or ends interrupted as its restart policy says.
    pub stop: CancellationToken,
    /// The late-result runtime's waiting slots for calls a provider holds. Without it a held
    /// call is never answered, as in today's loop, and its run pauses.
    pub held: Option<&'a dyn ProviderHeldLifecycle>,
    /// Decides each valid call before it is sent: allow it, narrow its input, or deny it.
    pub approval: &'a dyn ApprovalPolicy,
    /// Calls the hooks a frozen plan declares. A run whose plan declares hooks refuses to start
    /// or resume without it, as today's loop does.
    pub hook_plane: Option<Arc<dyn broca_core::hooks::HookPlane>>,
    /// Milliseconds since the epoch, for the times records carry.
    pub now_ms: &'a (dyn Fn() -> u64 + Sync),
    /// The session's address, given to the provider for its diagnostics only.
    pub session_address: Option<String>,
    /// The compaction provider's calls, for a run whose plan names one. Without it every call
    /// fails as unavailable, as today's loop's do without a plane.
    pub compaction_plane: Option<&'a dyn broca_core::CompactionPlane>,
    /// The session's compaction state, kept for the session's life by whoever owns the session
    /// (today's actor keeps it per session). Without it a run starts from the session's log.
    pub compaction_session: Option<Arc<broca_core::compaction_v2::session::Session>>,
    /// Whether the run tells the session's compaction provider about the session's changes
    /// outside its own steps, at the points today's actor has today's loop tell it
    /// (`RunControl::compaction_changes`; see [`crate::compaction::Changes`]). Without it the
    /// provider hears of them at the next step's notice, as from today's loop run without that
    /// control.
    pub compaction_changes: bool,
    /// Sends the warms that keep the prompt cache alive while a run whose keep-warm is `auto`
    /// waits on tool calls, scheduled by today's keep-warm scheduler. Without it no run warms.
    pub keep_warm: Option<Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<Admission>>>,
    /// Receives what the run saw about its provider account, for account routing: the run's
    /// start once it is durable, and each provider attempt as its record is appended.
    pub account_reports: Option<Arc<dyn AccountReports>>,
    /// The boot setting that bounds how long a run may wait on its own account after the
    /// provider says the account's limit is reached.
    pub max_same_account_wait: broca_core::SameAccountWait,
    /// The account routing gives the session now. A run paused on its account resumes only
    /// on the account it froze; `None` when the caller didn't route one.
    pub auth_selection: Option<broca_wal::AuthSelection>,
    /// Where the run publishes its in-flight model step while the provider call runs, as
    /// today's loop does (`RunControl::step_progress`): the session's owner serves it as an
    /// active run's `step`. It is cleared when the step's call ends. Never a durability input.
    pub step_progress: Option<broca_core::provider::LiveStepProgress>,
    /// What each model call waits for before the provider is asked to render and send it, such
    /// as the media its requests are rendered with while they are still being prepared. Each
    /// step waits for it before anything renders the step's request, its estimates included. A
    /// cancel during the wait ends the run cancelled, as a cancel during the call does.
    pub model_call_ready: Option<Arc<dyn ModelCallReady>>,
}

/// Something a run's model calls wait for (see [`Seams::model_call_ready`]).
#[async_trait::async_trait]
pub trait ModelCallReady: Send + Sync {
    /// Returns once a model call may be rendered, or why none can be: the step then fails with
    /// that error, never sending a request.
    async fn ready(&self) -> Result<(), ProviderError>;
}

/// The run loop's account reports, as today's `broca_core::AccountReporter` receives them. Each
/// names the record it reports by `event_id`, the session's id and the record's seq. The hooks
/// must not block: a reporter queues the event and returns. An attempt's record is appended
/// without a barrier unless the attempt failed, so a reporter that must only report durable
/// records waits for the session's durable bound to reach `seq`, as today's reporter waits for
/// the log's durable head.
pub trait AccountReports: Send + Sync {
    /// A fresh run's start, at `seq`, is durable. Not called on a resume, which writes no
    /// start.
    fn run_started(&self, event_id: &str, seq: u64);
    /// A provider attempt's record was appended at `seq`.
    fn attempt_recorded(&self, event_id: &str, seq: u64, attempt: &ModelAttempt);
}

/// The id an account report names a record by: the session's id and the record's seq.
fn event_id<W: Writer>(writer: &W, seq: u64) -> String {
    format!("{}:{seq}", writer.session_id())
}

/// How a run ended.
#[derive(Debug, Clone, PartialEq)]
pub struct Finished {
    pub reason: RunFinishReason,
    pub usage: Option<Usage>,
    pub retries_used: u32,
    /// Why the run paused, when it paused rather than ended. A paused run reports
    /// `Interrupted` as its reason, as today's loop does, and resumes as the same run.
    pub paused: Option<PauseReason>,
}

#[derive(Debug)]
pub enum LoopError {
    Session(SessionError),
    Parts(String),
    /// A case this build of the loop doesn't handle yet.
    Unsupported(&'static str),
    /// The tool plane refused something the run can't go on without, such as registering a
    /// batch's call keys for late results.
    Tools(String),
    /// A rule in `broca_core::fresh_start`, which the run loop on today's storage calls too,
    /// refused the run before anything was written: for example system text other than the
    /// session's, or a plan whose hooks can't run.
    Refused(broca_core::RunError),
}

impl From<SessionError> for LoopError {
    fn from(error: SessionError) -> Self {
        LoopError::Session(error)
    }
}

/// Runs `admission` to its end on the session behind `writer`.
pub async fn run<W, P, T>(
    writer: &mut W,
    parts: &mut dyn Parts,
    admission: Admission,
    seams: &Seams<'_, P, T>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    refuse_first_protocol_compaction(writer)?;
    refuse_unbuilt(&admission)?;
    // A session on the current compaction contract stays on it, and a new run on it freezes the
    // view library's current plan version, as today's loop does. A session that ran outside it
    // moves onto it at the first run whose plan names a compaction provider: that run enrolls
    // it (see `crate::enroll`).
    let mut admission = admission;
    let contract =
        crate::admit::session_contract(writer.operations(), admission.compaction_plan_version);
    let enrolling =
        contract && !writer.operations().on_contract() && writer.operations().run.is_some();
    if enrolling && !crate::enroll::window_whole(writer.conversation()) {
        return Err(crate::enroll::refused(
            crate::enroll::Refusal::WindowNotWhole,
        ));
    }
    admission.compaction_plan_version = contract.then_some(cortexkit_compaction_view::PLAN_VERSION);
    let mut input = std::mem::take(&mut admission.input);
    if contract {
        admission.global_system = global_system(writer, parts, &mut input, &admission)?;
    } else if let Some(previous) = writer.operations().run.as_ref() {
        // A later run outside the contract: its system messages must repeat the session's.
        broca_core::fresh_start::append_system_messages(
            &mut input,
            &writer.conversation().messages,
            previous_is_import(previous)?,
        )
        .map_err(LoopError::Refused)?;
    }
    admission.input = input;
    // The run numbers its messages' parts by its own computed rules, after the highest number
    // the session has shown, as its start freezes them. On the compaction contract its
    // requests show the projection's numbers instead, but today's reads still number the run's
    // messages by these rules, so they are recorded alike.
    let checkpoint = shown_high(&*writer, &*parts).await?;
    // The enrolling run decides what the session serves about the messages placed before it,
    // numbered after the highest number any earlier request showed, and stores the decisions
    // before its start's batch names them.
    let enrolled = if enrolling {
        let decided = crate::enroll::decide(&*writer, checkpoint).await?;
        let overlay = writer.put_enrollment(&decided.entries).await?;
        Some(decided.record(overlay))
    } else {
        None
    };
    let runner = broca_core::tags::rules_for(
        Some(broca_wal::ComputedTagRules { checkpoint }),
        frozen_render_tags(&admission),
        &admission.render_config,
    )
    .map(|rules| crate::entries::RunnerTags {
        run_id: admission.run_id.clone(),
        rules,
        checkpoint,
    });
    let mut placed = Placed {
        calls: Default::default(),
        runner,
        rich_ui: admission
            .manifest
            .as_ref()
            .and_then(|manifest| manifest.rich_ui.as_ref())
            .and_then(crate::entries::rich_ui),
        facts: Tags::for_session(
            contract,
            admission.manifest.as_deref(),
            &admission.render_config,
        ),
        inner: writer,
    };
    let start_seq = admit(&mut placed, parts, &admission, checkpoint, enrolled).await?;
    if let Some(reports) = &seams.account_reports {
        reports.run_started(&event_id(&placed, start_seq), start_seq);
    }
    let compaction = compaction_ctx(&placed, parts, &admission, seams, true).await?;
    let finished = match &admission.pre_model_error {
        Some(error) => {
            end_with_error(
                &mut placed,
                seams,
                &admission.run_id,
                RunFinishReason::Error,
                error,
            )
            .await?
        }
        None => drive(&mut placed, parts, &admission, seams, compaction.as_ref()).await?,
    };
    // Every end a run writes is followed by a notice of the session's changes, as today's
    // loop follows each of its ends; a pause isn't an end.
    if finished.paused.is_none() {
        let floor = signed_floor(&placed, parts, &mut ToolSetCache::default()).await?;
        if let Some(changes) = crate::compaction::Changes::of(&admission, seams, floor)
            .map(|changes| changes.sharing(compaction.as_ref()))
        {
            changes.tell(&placed, seams).await;
        }
    }
    Ok(finished)
}

/// Takes the system messages out of a run's input and decides the system text its requests
/// render, for a session on the current compaction contract, by
/// `broca_core::fresh_start::new_episode_globals`, given the previous run's text.
fn global_system<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    input: &mut Vec<Message>,
    admission: &Admission,
) -> Result<Vec<Message>, LoopError> {
    let previous = match writer
        .operations()
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .and_then(|start| {
            start
                .setup
                .iter()
                .find(|part| part.kind == PartKind::GlobalSystem)
        }) {
        Some(part) => decode::<Vec<Message>>(Some(parts.get(part).map_err(LoopError::Parts)?))?,
        None => None,
    };
    broca_core::fresh_start::new_episode_globals(
        input,
        admission.manifest.as_deref(),
        previous.as_deref(),
    )
    .map_err(LoopError::Refused)
}

/// Whether the run before a new one is the run start an import wrote, rather than a run's.
fn previous_is_import(run: &broca_session_store::fold::operations::Run) -> Result<bool, LoopError> {
    let Some(start) = run.start.as_ref() else {
        return Ok(false);
    };
    let provider_options = start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("config"))
        .and_then(|config| config.get("provider_options"));
    Ok(broca_core::fresh_start::is_import_start(
        &start.model,
        provider_options.unwrap_or(&Value::Null),
    ))
}

/// Continues the session's latest run from its last durable record, after a crash or a
/// restart. What happens next is what the operational fold says the run must do; a call that
/// may have run is never sent again.
pub async fn resume<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    seams: &Seams<'_, P, T>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    let unfinished = writer
        .operations()
        .run
        .as_ref()
        .is_some_and(|run| run.finished.is_none());
    let finished = resume_run(writer, parts, seams).await?;
    // A resume that wrote the run's end is followed by a notice of the session's changes, as
    // today's loop follows each of its ends.
    if unfinished && finished.paused.is_none() {
        if let Some(Ok(admission)) = writer
            .operations()
            .run
            .as_ref()
            .map(|run| frozen_admission(run, parts))
        {
            let floor = signed_floor(writer, parts, &mut ToolSetCache::default()).await?;
            if let Some(changes) = crate::compaction::Changes::of(&admission, seams, floor) {
                changes.tell(writer, seams).await;
            }
        }
    }
    Ok(finished)
}

/// [`resume`], up to the run's end or pause.
async fn resume_run<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    seams: &Seams<'_, P, T>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    refuse_first_protocol_compaction(writer)?;
    let run = writer
        .operations()
        .run
        .as_ref()
        .ok_or(LoopError::Unsupported("a session with no run to resume"))?;
    let admission = frozen_admission(run, parts)?;
    refuse_unbuilt(&admission)?;
    if let Some(reason) = run.finished {
        return Ok(finished(writer, reason));
    }
    let resolution = run.resolution();
    let cancelling_held = seams.cancel.is_cancelled()
        && run
            .open_batch
            .as_ref()
            .is_some_and(|batch| batch.calls.iter().any(|call| call.held.is_some()));
    // A cancel tripped before a paused run resumes ends it `Cancelled` before anything else a
    // resume does, as today's loop lets a cancel while paused win: nothing is refrozen, no
    // resume is recorded, an account limit isn't waited on, and no model is called. A session's
    // actor ends a paused run this way for an interrupt queued behind it.
    if matches!(resolution, Resolution::Paused(_))
        && seams.cancel.is_cancelled()
        && !cancelling_held
    {
        return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await;
    }
    // A run paused on its account's limit resumes on that same account, and only once the
    // account has reset, as today's resume decides it. Until then nothing is written and the
    // run stays paused; on another account it ends interrupted, saying why.
    if resolution == Resolution::Paused(PauseReason::AccountLimit) && !cancelling_held {
        let frozen = admission.auth_selection.as_ref();
        if seams.auth_selection.as_ref().is_some_and(|current| {
            !broca_core::account_limit_pause::same_selection(current, frozen)
        }) {
            let error = broca_core::account_limit_pause::selection_changed_error();
            return end_with_error(
                writer,
                seams,
                &admission.run_id,
                RunFinishReason::Interrupted,
                &error,
            )
            .await;
        }
        let ready = account_limit_of(writer)?
            .is_some_and(|pause| (seams.now_ms)() >= pause.resume_not_before_ms);
        if !ready {
            return Ok(Finished {
                paused: Some(PauseReason::AccountLimit),
                ..finished(writer, RunFinishReason::Interrupted)
            });
        }
    }
    // A resume that will call the provider again first confirms the provider still renders the
    // run's requests byte for byte, as today's does: a changed renderer or render config would
    // bust the prompt cache, or make the conversation invalid for the provider.
    let drift = match resolution {
        _ if cancelling_held => None,
        Resolution::Resume | Resolution::ProviderHeld(_) | Resolution::Paused(_) => {
            broca_core::provider_errors::render_identity_error(
                seams.provider,
                admission.renderer_version,
                &admission.render_config,
            )
        }
        Resolution::Idle(_) | Resolution::Interrupted(_) => None,
    };
    let mut refrozen = Vec::new();
    if let Resolution::Paused(reason) = resolution {
        if let Some(error) = &drift {
            // Before any provider call no request depends on the old config, so the run takes
            // the provider's current one; after a call, or when either config can't be read, the
            // run can't go on.
            if run.provider_call_started
                || error.provider_code.as_deref() == Some("frozen_render_config_unverifiable")
            {
                return end_with_error(
                    writer,
                    seams,
                    &admission.run_id,
                    RunFinishReason::Interrupted,
                    error,
                )
                .await;
            }
            refrozen.push(Record::ConfigRefrozen(
                broca_session_store::record::ConfigRefrozen {
                    run_id: admission.run_id.clone(),
                    ts_ms: Some((seams.now_ms)()),
                    renderer_version: seams.provider.renderer_version(),
                    render_config: seams.provider.render_config_blob(),
                },
            ));
        }
        // A run resumed on another account than it froze, as routing gives it now, names that
        // one from here on; one paused on its account's limit has already ended above if the
        // account changed.
        if let Some(selection) = seams
            .auth_selection
            .as_ref()
            .filter(|_| reason != PauseReason::AccountLimit)
            .filter(|selection| Some(*selection) != admission.auth_selection.as_ref())
        {
            refrozen.push(Record::AuthRefrozen(
                broca_session_store::record::AuthRefrozen {
                    run_id: admission.run_id.clone(),
                    ts_ms: Some((seams.now_ms)()),
                    auth_selection: serde_json::to_value(selection)
                        .map_err(|e| LoopError::Parts(e.to_string()))?,
                },
            ));
        }
    }
    let drift = drift.filter(|_| !matches!(resolution, Resolution::Paused(_)));
    let runner = runner_of_start(writer, parts, &admission.run_id).await?;
    let mut placed = Placed {
        calls: Default::default(),
        runner,
        rich_ui: admission
            .manifest
            .as_ref()
            .and_then(|manifest| manifest.rich_ui.as_ref())
            .and_then(crate::entries::rich_ui),
        facts: Tags::for_session(
            admission.compaction_plan_version.is_some(),
            admission.manifest.as_deref(),
            &admission.render_config,
        ),
        inner: writer,
    };
    let writer = &mut placed;
    // A durable step that called `submit_result` may survive a crash before the barrier that
    // ends its run. Its calls are never sent on recovery: the run ends here exactly as the live
    // path would have ended it, and the resume redoes nothing.
    let submit = match (&resolution, structured_of(&admission)) {
        (Resolution::Resume, Some(frozen)) => pending_submit(writer, &frozen).map(|s| (frozen, s)),
        _ => None,
    };
    let mut resumed = resumed_record(writer, &resolution);
    // A resume that ends the run instead of redoing anything records nothing to redo.
    if submit.is_some() || drift.is_some() || cancelling_held {
        resumed.redone.clear();
    }
    // A held call's provider may already have answered: the batch closes with what it holds,
    // sending nothing, and then the resume and the run's end are written.
    if let (Some(error), Resolution::ProviderHeld(_)) = (&drift, &resolution) {
        let mut error = error.clone();
        error.provider_code = Some("held_resume_refused".into());
        return close_interrupted(writer, seams, &admission.run_id, Some((&error, resumed))).await;
    }
    // The refreezes come first, durable with the resume, so nothing the resume does renders or
    // bills under the old config or account.
    let barrier = !refrozen.is_empty();
    refrozen.push(Record::Resumed(resumed));
    let mut batch = Some(refrozen);
    writer
        .append_with(
            &mut |_, _, first_seq| {
                let mut batch = batch.take().expect("one batch");
                name_resumed_from(&mut batch, first_seq);
                batch
            },
            barrier,
        )
        .await?;
    if let Some(error) = drift {
        return end_with_error(
            writer,
            seams,
            &admission.run_id,
            RunFinishReason::Interrupted,
            &error,
        )
        .await;
    }
    // The run continues under what this resume refroze.
    let admission = match writer.operations().run.as_ref() {
        Some(run) if barrier => frozen_admission(run, parts)?,
        _ => admission,
    };
    if cancelling_held {
        // Closing a held obligation needs neither a renderer nor model admission. Use the
        // frozen transcript rules to place its synthetic result, then withdraw after the end.
        let ctx = BatchCtx {
            batch_id: BatchId(
                run_state(writer)
                    .open_batch
                    .as_ref()
                    .expect("a held batch is open")
                    .batch_id
                    .clone(),
            ),
            bind: seams.bind.clone(),
            run_id: RunId(admission.run_id.clone()),
            scope: admission.send.scope.clone(),
            media_capture: writer.media(),
        };
        let now = (seams.now_ms)();
        let (mut records, withdrawn) =
            open_batch_close_out(writer, &admission.run_id, now, RunFinishReason::Cancelled);
        records.push(run_end(
            writer,
            &admission.run_id,
            RunFinishReason::Cancelled,
            now,
        ));
        append_end(writer, &admission.run_id, records).await?;
        if let Some(lifecycle) = seams.held {
            for reply in &withdrawn {
                lifecycle.terminal_synced(&ctx, &reply.tool_call_id).await;
            }
        }
        withdraw(seams, &withdrawn).await;
        return Ok(finished(writer, RunFinishReason::Cancelled));
    }
    if let Some((frozen, pending)) = submit {
        return submitted(writer, seams, &admission.run_id, &frozen, &pending).await;
    }
    let compaction = compaction_ctx(writer, parts, &admission, seams, false).await?;
    match resolution {
        Resolution::Resume => drive(writer, parts, &admission, seams, compaction.as_ref()).await,
        Resolution::Idle(reason) => finish(writer, seams, &admission.run_id, reason).await,
        Resolution::Interrupted(_) => {
            close_interrupted(writer, seams, &admission.run_id, None).await
        }
        // A held call's late result arrived: the run goes on from its open batch, waiting on
        // each held call again rather than sending it.
        Resolution::ProviderHeld(_) => {
            drive(writer, parts, &admission, seams, compaction.as_ref()).await
        }
        Resolution::Paused(PauseReason::AuthRequired | PauseReason::Restart) => {
            drive(writer, parts, &admission, seams, compaction.as_ref()).await
        }
        Resolution::Paused(PauseReason::AccountLimit) => {
            drive(writer, parts, &admission, seams, compaction.as_ref()).await
        }
        Resolution::Paused(_) => Err(LoopError::Unsupported("a paused run")),
    }
}

/// The result a resume records for a call that was sent with no result recorded.
const OUTCOME_UNKNOWN: &str = "outcome_unknown: this tool call was sent, but no result was \
recorded before the run stopped, so it may or may not have run. It will not be sent again.";

/// The result a resume records for a planned call the crash stopped before it was sent.
const NOT_RUN_INTERRUPTED: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// Ends a run a crash left with calls that may have run: every call of the open batch gets a
/// result (unknown outcome if it was sent, not run if it wasn't, the provider-held close result
/// if a provider holds it), and the batch and the run's end are durable together. No call is
/// sent. Each provider still holding a call is asked to withdraw it once the end is durable.
///
/// A resume that refuses to go on passes the error the run ends with, and its own record, which
/// today's writes after the batch closes and before the run's end.
async fn close_interrupted<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    refused: Option<(&ProviderError, Resumed)>,
) -> Result<Finished, LoopError> {
    let now = (seams.now_ms)();
    let (mut records, withdrawn) =
        open_batch_close_out(writer, run_id, now, RunFinishReason::Interrupted);
    let mut end = run_end(writer, run_id, RunFinishReason::Interrupted, now);
    if let Some((error, resumed)) = refused {
        if let Record::RunEnd(end) = &mut end {
            end.error =
                Some(serde_json::to_value(error).map_err(|e| LoopError::Parts(e.to_string()))?);
        }
        records.push(Record::Resumed(resumed));
    }
    records.push(end);
    append_end(writer, run_id, records).await?;
    withdraw(seams, &withdrawn).await;
    Ok(finished(writer, RunFinishReason::Interrupted))
}

/// The records that close the run's open batch after an interruption or cancellation, sending nothing: an
/// `outcome_unknown` result for each call that was sent with no result recorded, a not-run
/// result for each planned call that never left, a provider-held call's close result, and the
/// batch's end. Also the held calls whose providers must be asked to withdraw once the run's end
/// is durable. Nothing without an open batch.
fn open_batch_close_out<W: Writer>(
    writer: &W,
    run_id: &str,
    now: u64,
    reason: RunFinishReason,
) -> (Vec<Record>, Vec<ProviderHeldReply>) {
    let mut records = Vec::new();
    let mut withdrawn = Vec::new();
    if let Some(batch) = run_state(writer).open_batch.as_ref() {
        // The calls still waiting for a result, in the order the model asked for them. With
        // every call answered the turn is no longer open, and only the batch needs closing.
        let open = writer.conversation().open_tool_turn();
        for block in open.iter().flat_map(|open| open.content.iter()) {
            let broca_protocol::content::ContentBlock::ToolCall(call) = block else {
                continue;
            };
            let id = &call.tool_call_id;
            let (result, outcome, reason) = match batch.state_of(id) {
                Some(CallState::Dispatched) => (
                    error_result(id, OUTCOME_UNKNOWN),
                    CallOutcome::Unknown,
                    None,
                ),
                Some(CallState::NotSent | CallState::HeldSuccessor) => (
                    error_result(id, NOT_RUN_INTERRUPTED),
                    CallOutcome::Interrupted,
                    None,
                ),
                Some(CallState::ProviderHeld) => {
                    let (result, _, code) =
                        broca_core::tool_results::provider_held_close_result(id, reason);
                    let position = (0..batch.calls.len())
                        .find(|position| batch.id(*position) == id)
                        .expect("a held call is planned");
                    if let Some(held) = batch.held_reply(position) {
                        withdrawn.push(ProviderHeldReply {
                            tool_call_id: held.tool_call_id.clone(),
                            call_key: held.call_key.clone(),
                            module: held.module.clone(),
                            reason: held.reason.clone(),
                            detail_digest: held.detail_digest.clone(),
                        });
                    }
                    (result, CallOutcome::Interrupted, Some(code.to_string()))
                }
                Some(CallState::Terminated) | None => continue,
            };
            terminal_with_reason(
                &mut records,
                run_id,
                &batch.batch_id,
                result,
                outcome,
                reason,
                Some(now),
            );
        }
        records.push(Record::BatchEnd(BatchEnd {
            run_id: run_id.into(),
            batch_id: batch.batch_id.clone(),
        }));
    }
    (records, withdrawn)
}

/// Ends the session's unfinished latest run `Interrupted`, sending nothing, with the records
/// today's seal writes for the same run (`broca_core::resume::interrupted_seal`): on a graceful
/// stop (`SealCause::Shutdown`), or when a session opens on a run a crashed process left running
/// (`SealCause::CrashOrphan`), which first records the resume and names the crash on the run's
/// end.
///
/// An open batch closes as a crash resume closes it ([`open_batch_close_out`]), whatever its
/// calls' state. With no open batch, a tool turn whose calls were never dispatched is kept, each
/// call answered as never run, so nothing already written is dropped. Then the run ends, all in
/// one batch.
///
/// Only a run that was running is sealed here: a paused run waits for its own resume, and a run
/// whose last step already ended is finished by [`resume`].
pub async fn seal<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    seams: &Seams<'_, P, T>,
    cause: broca_core::seal::SealCause,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    use broca_core::seal::SealCause;
    let run = writer
        .operations()
        .run
        .as_ref()
        .ok_or(LoopError::Unsupported("a session with no run to seal"))?;
    if let Some(reason) = run.finished {
        return Ok(finished(writer, reason));
    }
    let run_id = run.run_id.clone();
    let resolution = run.resolution();
    if !matches!(resolution, Resolution::Resume | Resolution::Interrupted(_)) {
        return Err(LoopError::Unsupported("sealing a run that wasn't running"));
    }
    let admission = frozen_admission(run, parts)?;
    let runner = runner_of_start(writer, parts, &run_id).await?;
    // The closing results message is placed with the facts its run's frozen rules give it, as a
    // resume places it.
    let mut placed = Placed {
        calls: Default::default(),
        runner,
        rich_ui: admission
            .manifest
            .as_ref()
            .and_then(|manifest| manifest.rich_ui.as_ref())
            .and_then(crate::entries::rich_ui),
        facts: Tags::for_session(
            admission.compaction_plan_version.is_some(),
            admission.manifest.as_deref(),
            &admission.render_config,
        ),
        inner: writer,
    };
    let writer = &mut placed;
    let now = (seams.now_ms)();
    let mut records = Vec::new();
    if cause == SealCause::CrashOrphan {
        let mut resumed = resumed_record(writer, &resolution);
        resumed.redone.clear();
        records.push(Record::Resumed(resumed));
    }
    // As today's seal chooses: an open batch closes, whatever its calls' state; only with no
    // open batch is a never-dispatched tool turn kept.
    let (close_out, withdrawn) =
        open_batch_close_out(writer, &run_id, now, RunFinishReason::Interrupted);
    if close_out.is_empty() {
        records.extend(never_dispatched_close_out(writer, &run_id, now));
    } else {
        records.extend(close_out);
    }
    let mut end = run_end(writer, &run_id, RunFinishReason::Interrupted, now);
    if let Record::RunEnd(end) = &mut end {
        end.interrupt_reason = Some(
            match cause {
                SealCause::CrashOrphan => "runner_crash",
                SealCause::Shutdown => "runner_shutdown",
            }
            .into(),
        );
        if cause == SealCause::CrashOrphan {
            end.error = Some(
                serde_json::to_value(broca_core::seal::crash_orphan_error())
                    .map_err(|e| LoopError::Parts(e.to_string()))?,
            );
        }
    }
    records.push(end);
    append_end(writer, &run_id, records).await?;
    withdraw(seams, &withdrawn).await;
    Ok(finished(writer, RunFinishReason::Interrupted))
}

/// The records that keep a tool turn whose calls were never dispatched, as today's
/// `broca_core::close_out_pending_turn` writes them: the step's batch started if it wasn't, a
/// never-run result for each call without one, and the batch's end. Nothing when the run's last
/// step asked for no tools.
fn never_dispatched_close_out<W: Writer>(writer: &W, run_id: &str, now: u64) -> Vec<Record> {
    let Some(open) = writer.conversation().open_tool_turn() else {
        return Vec::new();
    };
    let planned: Vec<ToolCallId> = open
        .content
        .iter()
        .filter_map(|block| match block {
            broca_protocol::content::ContentBlock::ToolCall(call) => {
                Some(call.tool_call_id.clone())
            }
            _ => None,
        })
        .collect();
    if planned.is_empty() {
        return Vec::new();
    }
    let run = run_state(writer);
    let mut records = Vec::new();
    let batch_id = match run.open_batch.as_ref() {
        Some(batch) => batch.batch_id.clone(),
        None => {
            let batch_id = format!("{run_id}-b{}", run.completed_steps);
            records.push(Record::BatchStart(BatchStart {
                run_id: run_id.into(),
                batch_id: batch_id.clone(),
                step_id: run.completed_steps,
                planned: planned.clone(),
                mid: None,
            }));
            batch_id
        }
    };
    let answered = |id: &ToolCallId| {
        run.open_batch
            .as_ref()
            .is_some_and(|batch| batch.state_of(id) == Some(CallState::Terminated))
    };
    for id in planned.iter().filter(|id| !answered(id)) {
        terminal(
            &mut records,
            run_id,
            &batch_id,
            broca_core::tool_results::not_dispatched_result(id),
            CallOutcome::Interrupted,
            now,
        );
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    records
}

/// An error result for call `id` saying `message`.
fn error_result(id: &ToolCallId, message: &str) -> ToolResult {
    ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::error(message.to_string()),
        is_error: true,
    }
}

/// The calls of the run's open tool turn that were never sent: all of them when the step's
/// batch never started, else those with no intent and no outcome. `None` without an open turn.
///
/// A call with an intent and no outcome never reaches this: the fold resolves such a run as
/// interrupted, and the resume closes it without sending anything. Leaving such calls out here
/// keeps this filter right on its own.
fn unsent_calls<W: Writer>(writer: &W) -> Option<Vec<ToolCall>> {
    let open = writer.conversation().open_tool_turn()?;
    let batch = run_state(writer).open_batch.as_ref();
    Some(
        open.content
            .iter()
            .filter_map(|block| match block {
                broca_protocol::content::ContentBlock::ToolCall(call) => Some(call.clone()),
                _ => None,
            })
            .filter(|call| {
                batch.is_none_or(|batch| {
                    matches!(
                        batch.state_of(&call.tool_call_id),
                        Some(
                            CallState::NotSent | CallState::ProviderHeld | CallState::HeldSuccessor
                        )
                    )
                })
            })
            .collect(),
    )
}

/// Every call the open tool turn's assistant message asked for.
fn planned_calls<W: Writer>(writer: &W) -> Vec<ToolCall> {
    writer
        .conversation()
        .open_tool_turn()
        .map(|open| {
            open.content
                .iter()
                .filter_map(|block| match block {
                    broca_protocol::content::ContentBlock::ToolCall(call) => Some(call.clone()),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The open batch's calls that already have intents and are still to be answered: a call a
/// provider holds, with its held reply, and a call queued behind one, never sent.
fn known_calls<W: Writer>(writer: &W) -> Vec<Known> {
    let Some(batch) = run_state(writer).open_batch.as_ref() else {
        return Vec::new();
    };
    (0..batch.calls.len())
        .filter(|position| {
            matches!(
                batch.state(*position),
                CallState::ProviderHeld | CallState::HeldSuccessor
            )
        })
        .map(|position| Known {
            id: batch.id(position).clone(),
            key: batch.key(position).map(str::to_owned),
            args: batch.args(position).cloned(),
            held: batch.held_reply(position).map(|held| ProviderHeldReply {
                tool_call_id: held.tool_call_id.clone(),
                call_key: held.call_key.clone(),
                module: held.module.clone(),
                reason: held.reason.clone(),
                detail_digest: held.detail_digest.clone(),
            }),
        })
        .collect()
}

/// What a resume rebuilt, what it does again, and which calls it will never send again.
/// What a resume rebuilt, what it does again, and which calls it will never send again.
///
/// While a tool batch is open, its step's tool turn isn't complete: the counted messages stop
/// before the step's assistant message, and only the batch's unsent calls are done again; the
/// next model step follows the batch and isn't counted as done again.
fn resumed_record<W: Writer>(writer: &W, resolution: &Resolution) -> Resumed {
    let run = run_state(writer);
    let conversation = writer.conversation();
    let tool_results = run.open_batch.as_ref().map_or(0, |batch| {
        (0..batch.calls.len())
            .filter(|position| batch.state(*position) == CallState::Terminated)
            .count()
    });
    let mut messages = conversation.messages.len();
    if run.open_batch.is_some() && conversation.open_tool_turn().is_none() {
        // Every call answered: the turn's assistant message and its results are placed.
        messages = messages.saturating_sub(2);
    }
    let mut redone = Vec::new();
    if *resolution == Resolution::Resume {
        let unsent = unsent_calls(writer).unwrap_or_default();
        redone.extend(unsent.into_iter().map(|call| Redo {
            step_id: None,
            tool_call_id: Some(call.tool_call_id),
        }));
        if run.open_batch.is_none() && conversation.open_tool_turn().is_none() {
            // The next model step has no durable answer: it is asked for again.
            redone.push(Redo {
                step_id: Some(run.completed_steps + 1),
                tool_call_id: None,
            });
        }
    }
    Resumed {
        run_id: run.run_id.clone(),
        // Provisional: the append that writes the record names the seq it resumed from
        // (`name_resumed_from`).
        resumed_from_seq: writer.next_seq().saturating_sub(1),
        messages: messages as u64,
        model_steps: run.completed_steps,
        tool_results: tool_results as u64,
        redone,
        indeterminate: run.indeterminate(),
    }
}

fn decode<T: serde::de::DeserializeOwned>(value: Option<Value>) -> Result<Option<T>, LoopError> {
    value
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| LoopError::Parts(e.to_string()))
}

/// Stores a plan manifest's shared members as setup parts, in a fixed order: the plan, each
/// provider's answer, the manifest's tools, its system text and its hook declarations. Returns
/// what remains of the manifest, or `None` when nothing does.
pub fn manifest_parts(
    manifest: &broca_wal::PlanManifest,
    parts: &mut dyn Parts,
    setup: &mut Vec<broca_session_store::record::PartRef>,
) -> Result<Option<Value>, LoopError> {
    let Value::Object(mut members) =
        serde_json::to_value(manifest).map_err(|e| LoopError::Parts(e.to_string()))?
    else {
        return Err(LoopError::Parts("a manifest isn't an object".into()));
    };
    let mut put = |kind: PartKind, value: &Value| -> Result<(), LoopError> {
        setup.push(parts.put(kind, value).map_err(LoopError::Parts)?);
        Ok(())
    };
    if let Some(plan) = members.remove("plan") {
        put(PartKind::Plan, &plan)?;
    }
    if let Some(Value::Array(fetches)) = members.remove("fetches") {
        for fetch in &fetches {
            put(PartKind::ProviderAnswer, fetch)?;
        }
    }
    for (key, kind) in [
        ("tools", PartKind::Tools),
        ("system_text", PartKind::SystemText),
        ("declarations", PartKind::Declarations),
    ] {
        if let Some(value) = members.remove(key) {
            put(kind, &value)?;
        }
    }
    Ok((!members.is_empty()).then_some(Value::Object(members)))
}

/// The plan manifest a run's start froze, rebuilt from the setup parts from its plan on and the
/// remainder its choices keep. `None` for a run admitted without a plan.
pub fn frozen_manifest(
    start: &broca_session_store::record::RunStart,
    parts: &dyn Parts,
) -> Result<Option<std::sync::Arc<broca_wal::PlanManifest>>, LoopError> {
    broca_session_store::fold::operations::ManifestRef::of_start(start)
        .map(|manifest| stored_manifest(&manifest, parts))
        .transpose()
}

/// The plan manifest `manifest` references, rebuilt from its setup parts and the rest of its
/// members.
pub fn stored_manifest(
    manifest: &broca_session_store::fold::operations::ManifestRef,
    parts: &dyn Parts,
) -> Result<std::sync::Arc<broca_wal::PlanManifest>, LoopError> {
    let mut members = match &manifest.rest {
        Some(Value::Object(rest)) => rest.clone(),
        _ => serde_json::Map::new(),
    };
    let mut fetches = Vec::new();
    for part in &manifest.parts {
        let value = parts.get(part).map_err(LoopError::Parts)?;
        match part.kind {
            PartKind::Plan => {
                members.insert("plan".into(), value);
            }
            PartKind::ProviderAnswer => fetches.push(value),
            PartKind::Tools => {
                members.insert("tools".into(), value);
            }
            PartKind::SystemText => {
                members.insert("system_text".into(), value);
            }
            PartKind::Declarations => {
                members.insert("declarations".into(), value);
            }
            _ => {}
        }
    }
    members.insert("fetches".into(), Value::Array(fetches));
    serde_json::from_value(Value::Object(members))
        .map(std::sync::Arc::new)
        .map_err(|e| LoopError::Parts(format!("the frozen manifest: {e}")))
}

/// A run's admission as its start record froze it, read back from the fold and the setup
/// store. Its input is already in the conversation, so it is left empty.
fn frozen_admission(
    run: &broca_session_store::fold::operations::Run,
    parts: &dyn Parts,
) -> Result<Admission, LoopError> {
    let start = run.start.as_ref().ok_or(LoopError::Unsupported(
        "a run whose start isn't in the fold",
    ))?;
    // An unfinished run on the current compaction contract renders exactly as it started, so it
    // resumes only under the plan version it froze.
    if let (Some(version), None) = (start.compaction_plan_version, run.finished) {
        broca_core::compaction_v2::conditions::check_plan_version(version)
            .map_err(LoopError::Parts)?;
    }
    let mut admission = admission_of_start(start, parts)?;
    // A resume that refroze the run's render config (before any provider call) or its account
    // (onto the one routing gave it) renders and bills under what it refroze.
    if let Some(refrozen) = &run.refrozen_render {
        admission.render_config = refrozen["render_config"].clone();
        admission.renderer_version = decode::<u32>(Some(refrozen["renderer_version"].clone()))?
            .unwrap_or(admission.renderer_version);
    }
    if let Some(selection) = &run.refrozen_auth {
        admission.auth_selection = decode(Some(selection.clone()))?;
    }
    // A run whose strict schema was downgraded renders downgraded from then on, as today's
    // resume rebuilds its provider from the downgraded config.
    if run.structured_downgraded {
        broca_protocol::structured_output::FrozenStructuredOutput::downgrade_render_config(
            &mut admission.render_config,
        );
    }
    Ok(admission)
}

/// The cache marker order `start` froze, which its record keeps beside its config; `None` for a
/// run that froze none.
pub fn frozen_marker_order(start: &broca_session_store::record::RunStart) -> Option<u32> {
    start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("record"))
        .and_then(|record| record.get("marker_order_version"))
        .and_then(Value::as_u64)
        .and_then(|version| u32::try_from(version).ok())
}

/// A run's config members, as `RunStarted.config` spells them, and its plan manifest's setup
/// parts, in order.
pub type ConfigMembers = (serde_json::Map<String, Value>, Vec<(PartKind, Value)>);

/// A run start's frozen config as today's `RunStarted` record spells it, the members of its
/// `config` object, and the setup parts of the run's plan manifest, in order. `part` reads a
/// setup part's value.
///
/// The converter's inverse rebuilds today's record from this, and the serve executor rebuilds
/// the config a new run's admission reads (`run_config_of_start`), so both read a run's start
/// the same way. The config's parts come first (its tools, then the response format every run
/// config has, then its global system text and render config), then the manifest's. So a
/// tools part before the response format is the config's, and one after it is the manifest's.
pub fn run_config_members<E>(
    start: &broca_session_store::record::RunStart,
    mut part: impl FnMut(&broca_session_store::record::PartRef) -> Result<Value, E>,
    not_object: impl Fn() -> E,
) -> Result<ConfigMembers, E> {
    let choices = match start.run_choices.as_ref() {
        None => serde_json::Map::new(),
        Some(Value::Object(choices)) => choices.clone(),
        Some(_) => return Err(not_object()),
    };
    let mut config = match choices.get("config") {
        None => serde_json::Map::new(),
        Some(Value::Object(config)) => config.clone(),
        Some(_) => return Err(not_object()),
    };
    fn json(value: impl serde::Serialize) -> Value {
        serde_json::to_value(value).expect("record values serialize")
    }
    fn set(
        config: &mut serde_json::Map<String, Value>,
        key: &str,
        value: Option<impl serde::Serialize>,
    ) {
        if let Some(value) = value {
            config.insert(key.into(), json(value));
        }
    }
    config.insert("model".into(), json(&start.model));
    config.insert("renderer_version".into(), json(start.renderer_version));
    set(&mut config, "context_limit", start.context_limit);
    set(&mut config, "service_tier", start.service_tier.as_ref());
    set(
        &mut config,
        "route_decision_id",
        start.route_decision_id.as_ref(),
    );
    set(&mut config, "auth_selection", start.auth_selection.as_ref());
    let mut manifest = Vec::new();
    let mut past_config = false;
    for setup in &start.setup {
        let value = part(setup)?;
        match setup.kind {
            PartKind::Tools if !past_config => {
                config.insert("tools".into(), value);
            }
            PartKind::ResponseFormat => {
                config.insert("response_format".into(), value);
                past_config = true;
            }
            PartKind::GlobalSystem => {
                config.insert("global_system".into(), value);
            }
            PartKind::RenderConfig => {
                config.insert("frozen_render_config".into(), value);
            }
            kind => manifest.push((kind, value)),
        }
    }
    Ok((config, manifest))
}

/// The run's frozen config, rebuilt from its start record and setup parts as
/// [`run_config_members`] reads them.
pub fn run_config_of_start(
    start: &broca_session_store::record::RunStart,
    parts: &dyn Parts,
) -> Result<broca_wal::RunConfig, LoopError> {
    let (config, _) = run_config_members(
        start,
        |part| parts.get(part).map_err(LoopError::Parts),
        || LoopError::Parts("a run start's carried config is not an object".into()),
    )?;
    serde_json::from_value(Value::Object(config)).map_err(|e| LoopError::Parts(e.to_string()))
}

/// The admission a run's start froze: its model, settings, tools and system text, read back from
/// the setup parts the start names.
pub fn admission_of_start(
    start: &broca_session_store::record::RunStart,
    parts: &dyn Parts,
) -> Result<Admission, LoopError> {
    let part = |kind: PartKind| -> Result<Option<Value>, LoopError> {
        start
            .setup
            .iter()
            .find(|part| part.kind == kind)
            .map(|part| parts.get(part).map_err(LoopError::Parts))
            .transpose()
    };
    let config = start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("config"))
        .cloned()
        .unwrap_or(Value::Null);
    let member = |key: &str| config.get(key).cloned();
    Ok(Admission {
        run_id: start.run_id.clone(),
        model: start.model.clone(),
        input: Vec::new(),
        tools: decode(part(PartKind::Tools)?)?.unwrap_or_default(),
        tool_choice: decode(member("tool_choice"))?.unwrap_or_default(),
        generation: decode(member("generation"))?.unwrap_or_default(),
        response_format: decode(part(PartKind::ResponseFormat)?)?.unwrap_or_default(),
        provider_options: member("provider_options").unwrap_or(Value::Null),
        render_config: decode::<Value>(part(PartKind::RenderConfig)?)?.unwrap_or(Value::Null),
        renderer_version: start.renderer_version,
        stop_when: decode(member("stop_when"))?.unwrap_or_default(),
        on_restart: decode(start.on_restart.clone())?,
        keep_warm: decode(start.keep_warm.clone())?,
        auth_selection: decode(start.auth_selection.clone())?,
        account_limit_pause: decode::<bool>(member("account_limit_pause"))?.unwrap_or(false),
        ts_ms: start.ts_ms,
        written_at_ms: start.written_at_ms,
        global_system: match (
            start.compaction_plan_version,
            frozen_manifest(start, parts)?,
        ) {
            (None, _) => Vec::new(),
            (Some(_), Some(manifest)) => {
                broca_core::compaction_v2::systems::from_manifest(&manifest)
            }
            (Some(_), None) => decode(part(PartKind::GlobalSystem)?)?.unwrap_or_default(),
        },
        compaction_plan_version: start.compaction_plan_version,
        context_limit: start.context_limit,
        manifest: frozen_manifest(start, parts)?,
        render_tags: member("render_tags")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        marker_order_version: frozen_marker_order(start),
        budget: decode::<broca_protocol::request::BudgetConfig>(member("budget"))?
            .filter(|budget| *budget != broca_protocol::request::BudgetConfig::default()),
        route_decision_id: start.route_decision_id.clone(),
        service_tier: decode(start.service_tier.clone().map(Value::String))?,
        variant: decode(member("variant"))?,
        work_class: decode(member("work_class"))?,
        context_management: decode(member("context_management"))?,
        directives: decode(member("turn_directives_v1"))?,
        tools_were_omitted: decode::<bool>(member("tools_were_omitted"))?.unwrap_or(false),
        send: SendFacts {
            submission_id: start.submission_id.clone(),
            send_id: start.send_id.clone(),
            request_identity_digest: start.request_identity_digest.clone(),
            title: start.title.clone(),
            session_class: start.session_class.clone(),
            retention_secs: start.retention_secs,
            principal: decode(start.principal.clone())?,
            sent_by: decode(
                start
                    .run_choices
                    .as_ref()
                    .and_then(|choices| choices.get("record"))
                    .and_then(|record| record.get("sent_by"))
                    .cloned(),
            )?,
            scope: decode(start.scope.clone())?,
            hooks: decode(start.hooks.clone())?,
            message_marks: decode(start.message_marks.clone())?,
        },
        pre_model_error: None,
        applies_refresh: None,
    })
}

/// A run start's application of a pending plan change, as the record that follows the run's
/// input in the start's batch keeps it.
pub fn refresh_applied(
    applied: &broca_protocol::session_change::AppliedChange,
) -> broca_session_store::record::RefreshApplied {
    broca_session_store::record::RefreshApplied {
        generation: applied.generation,
        applied_by: applied.applied_by.clone(),
        manifest_digest: applied.manifest_digest.clone(),
        composition_digest: applied.composition_digest.clone(),
        absent_items: applied.absent_items.clone(),
    }
}

/// A session compacted under the first compaction protocol keeps rendering under that protocol's
/// last view, which only today's run loop applies, so the new loop refuses it rather than send
/// different bytes.
fn refuse_first_protocol_compaction<W: Writer>(writer: &W) -> Result<(), LoopError> {
    if writer.conversation().first_protocol_compaction
        || writer.operations().first_protocol_compaction
    {
        return Err(LoopError::Unsupported(
            "a session compacted under the first compaction protocol",
        ));
    }
    Ok(())
}

/// Refuses, by name, a run that needs a part of today's run loop the new loop hasn't built yet,
/// rather than running it without that part: an input-token budget (no live session has one),
/// and the client-side refusal fallback. Each is read from what the run froze.
fn refuse_unbuilt(admission: &Admission) -> Result<(), LoopError> {
    if admission
        .budget
        .as_ref()
        .is_some_and(|budget| budget.max_input_tokens.is_some())
    {
        return Err(LoopError::Unsupported("a run with an input-token budget"));
    }
    Ok(())
}

/// The part-number rules a window run froze, and where its messages start in the window.
pub(crate) struct RunTagRules {
    start: usize,
    rules: Option<ComputedRules>,
    computed: Option<ComputedTagRules>,
}

/// What a run's start record froze about its part numbers: whether it renders them, and the
/// number it continues after. A start record never changes once written.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct FrozenTags {
    render_tags: bool,
    computed: Option<ComputedTagRules>,
}

/// The frozen part-number choices of the window's runs, by run id, as a run has read them so
/// far. Reading a run's start is a lookup in the session's id index, so a run reads each window
/// run's start once rather than at every step: after its first step, only a run new to the window
/// costs a lookup. When a seal moves the window's start, the runs that left the window are
/// dropped; a run never comes back into the window.
#[derive(Default)]
pub(crate) struct TagRulesCache {
    by_run: std::collections::HashMap<String, FrozenTags>,
    /// The window start the map was last trimmed at.
    window_start: u64,
}

/// The part-number choices `start` froze into its run config.
fn frozen_tags(start: &RunStart) -> Result<FrozenTags, LoopError> {
    let config = start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("config"))
        .cloned()
        .unwrap_or(Value::Null);
    Ok(FrozenTags {
        render_tags: config["render_tags"].as_bool().unwrap_or(false),
        computed: decode(config.get("computed_tags").cloned())?,
    })
}

/// Each window run's frozen part-number rules, read from its start record: whether it renders
/// numbers, the number it continues after, and its render config, combined by today's
/// `rules_for`, the same function today's loop numbers a run's messages with. A run's start is
/// read once and kept in `cache` (see [`TagRulesCache`]); the render config and the run's place
/// in the prompt are read from `episodes` at every call, as before.
async fn run_tag_rules<W: Writer>(
    writer: &W,
    episodes: &[EpisodeRenderInput],
    cache: &mut TagRulesCache,
) -> Result<Vec<RunTagRules>, LoopError> {
    let conversation = writer.conversation();
    let window = &episodes[conversation.sealed_episodes.len()..];
    let first = (conversation.window_start() - 1) as usize;
    if cache.window_start != conversation.window_start() {
        cache.window_start = conversation.window_start();
        let in_window: std::collections::HashSet<&str> = conversation
            .episodes
            .iter()
            .map(|episode| episode.run_id.as_str())
            .collect();
        cache
            .by_run
            .retain(|run_id, _| in_window.contains(run_id.as_str()));
    }
    let mut out = Vec::with_capacity(window.len());
    for (episode, render) in conversation.episodes.iter().zip(window) {
        let frozen = match cache.by_run.get(&episode.run_id) {
            Some(frozen) => *frozen,
            None => {
                let start = writer.run_start(&episode.run_id).await?.ok_or_else(|| {
                    LoopError::Parts(format!(
                        "the window names run {} but the session holds no start for it",
                        episode.run_id
                    ))
                })?;
                let frozen = frozen_tags(&start)?;
                cache.by_run.insert(episode.run_id.clone(), frozen);
                frozen
            }
        };
        out.push(RunTagRules {
            start: render.prompt_start.saturating_sub(first),
            rules: broca_core::tags::rules_for(
                frozen.computed,
                frozen.render_tags,
                &render.frozen_config,
            ),
            computed: frozen.computed,
        });
    }
    Ok(out)
}

/// Whether the run froze part numbers into the model's view: a run read back from its start
/// carries the frozen choice, and a new run takes it from its plan, as its start record freezes
/// it.
/// The open run `run_id`'s own part numbering, as its start froze it.
async fn runner_of_start<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    run_id: &str,
) -> Result<Option<crate::entries::RunnerTags>, LoopError> {
    let Some(start) = writer.run_start(run_id).await? else {
        return Ok(None);
    };
    let config = run_config_of_start(&start, parts)?;
    Ok(config
        .computed_tags
        .zip(broca_core::tags::computed_rules(&config))
        .map(|(tags, rules)| crate::entries::RunnerTags {
            run_id: run_id.to_owned(),
            rules,
            checkpoint: tags.checkpoint,
        }))
}

fn frozen_render_tags(admission: &Admission) -> bool {
    admission.render_tags
        || admission
            .manifest
            .as_deref()
            .is_some_and(broca_core::tags::plan_renders_tags)
}

/// The request the latest run's step `step_id` sends, built from the session's state exactly as
/// the run loop builds it before sending, without any provider or tool plane: for checking
/// requests rebuilt from stored sessions against the bytes that were sent. A run whose plan names
/// a compaction provider renders from the view the session recorded as applied for that step.
pub async fn offline_request<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
    step_id: u64,
) -> Result<CallOptions, LoopError> {
    offline_request_for(writer, parts, bind, step_id, false).await
}

/// [`offline_request`] for a run whose provider renders media: each message holding media is
/// left untagged, as the run loop leaves it for a media renderer that tags its own copy of the
/// request after preparing the media.
pub async fn offline_media_request<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
    step_id: u64,
) -> Result<CallOptions, LoopError> {
    offline_request_for(writer, parts, bind, step_id, true).await
}

async fn offline_request_for<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    bind: &broca_protocol::BindIdentity,
    step_id: u64,
    defer_media_tags: bool,
) -> Result<CallOptions, LoopError> {
    refuse_first_protocol_compaction(writer)?;
    let run = writer
        .operations()
        .run
        .as_ref()
        .ok_or(LoopError::Unsupported("a session with no run"))?;
    let admission = frozen_admission(run, parts)?;
    let compacted = match (
        admission.compaction_plan_version,
        admission.manifest.as_deref(),
    ) {
        (Some(_), Some(manifest)) => broca_core::compaction::item(manifest)
            .map_err(LoopError::Parts)?
            .is_some(),
        _ => false,
    };
    let episodes = Arc::new(render_history(writer, parts, &mut ToolSetCache::default()).await?);
    let run_rules = run_tag_rules(writer, &episodes, &mut TagRulesCache::default()).await?;
    let tools = Arc::new(admission.tools.clone());
    let enrollment = writer.enrollment().await?;
    let mut opts = call_options(
        writer,
        &admission,
        &tools,
        &episodes,
        &run_rules,
        None,
        step_id,
        compacted,
        defer_media_tags,
        enrollment.as_ref(),
        None,
    )?;
    finalize_tool_choice(writer, &mut opts);
    if !compacted {
        return Ok(opts);
    }
    crate::compaction::offline_prompt(writer, &admission, bind, step_id, opts).await
}

/// Disables tool calls on the budget's finalize step, and says whether the step is one. The step
/// keeps the declared tools byte for byte and disables calls through the tool choice, as today's
/// loop does: dropping the tools would change the request's cached prefix on the run's last
/// step. A step's live request and its offline rebuild both take it from here.
fn finalize_tool_choice<W: Writer>(writer: &W, opts: &mut CallOptions) -> bool {
    let finalizing = crate::budget::finalizing(writer.operations());
    if finalizing {
        opts.tool_choice = Arc::new(ToolChoice::None);
    }
    finalizing
}

/// Model steps and tool batches from the run's current state until a decision ends the run.
async fn drive<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    compaction: Option<&crate::compaction::Ctx>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    let warming = Warming::of(admission, seams);
    // The pause a timed same-account wait left, which the step it resumes must match.
    let mut account_limit_replay = account_limit_of(writer)?;
    // Each window run's place in the prompt and render config, and the signed floor its runs'
    // tools set. A run adds no episode once its input is placed, so these hold until a seal
    // inside the run moves runs out of the window, which replaces them (see below). Each request
    // shares the list.
    let mut tool_sets = ToolSetCache::default();
    let mut episodes = Arc::new(render_history(writer, parts, &mut tool_sets).await?);
    // The changes a batch tells the compaction provider read the transcript the run's steps keep,
    // and report the signed floor the run's requests carry.
    let changes = crate::compaction::Changes::of(admission, seams, episodes.signed_floor)
        .map(|changes| changes.sharing(compaction));
    // Each window run's frozen part numbering, read from its start once for the whole run.
    let mut tag_rules = TagRulesCache::default();
    // On the current compaction contract a request numbers its parts from the entries, so the
    // runs' frozen numbering goes unused there: it's read at the run's first step only, which
    // refuses a window run whose start can't be read, as every step's read would. No run joins
    // the window during the run.
    let contract = admission.compaction_plan_version.is_some();
    let mut tag_rules_read = false;
    // A compacted run's stored prompt, kept between its steps (see `compaction::prompt`), and
    // what its renders rendered of each entry, which the next reuses where unchanged.
    let mut kept_prompt = crate::compaction::prompt::KeptPrompt::default();
    let rendered = broca_core::compaction_v2::render::RenderCache::default();
    let tools = Arc::new(admission.tools.clone());
    // The run's frozen plan says which preset each tool provider's calls carry, which providers
    // were fetched and what they declared, as today's loop reads it from the same plan. A call
    // without its preset is refused by a provider whose route is bound under a scope.
    let mut manifest = ToolManifest::from_tools(admission.tools.clone());
    if let Some(plan) = admission.manifest.as_deref() {
        manifest = manifest
            .with_plan_manifest(plan)
            .map_err(|error| LoopError::Parts(format!("the run's frozen plan: {error}")))?;
    }
    // As today's loop asks once per run: a provider whose media renderer tags its own copy of
    // each request takes the run's tag choice here, and the loop leaves media-holding messages
    // untagged, since preparing their media needs the parts where the model put them.
    let defer_media_tags = seams
        .provider
        .defer_media_tags(frozen_render_tags(admission));
    let mut structured = structured_of(admission);
    let hook_plan = hook_plan(admission, seams)?;
    let hooks = hook_plan
        .as_ref()
        .map(|plan| broca_core::hooks::HookRuntime {
            plan,
            plane: seams.hook_plane.as_ref().expect("checked with the plan"),
            bind: &seams.bind,
            scope: admission.send.scope.as_ref(),
            cancel: Some(&seams.cancel),
            stop: Some(&seams.stop),
            seal: None,
        });
    let stop_when = if admission.stop_when.is_empty() {
        vec![StopCondition::StepCount {
            n: DEFAULT_MAX_STEPS,
        }]
    } else {
        admission.stop_when.clone()
    };
    // A crash after every call of a batch had its result but before the batch closed: it
    // closes now.
    if let Some(batch) = run_state(writer).open_batch.as_ref() {
        let ended =
            (0..batch.calls.len()).all(|position| batch.state(position) == CallState::Terminated);
        if ended {
            let record = Record::BatchEnd(BatchEnd {
                run_id: admission.run_id.clone(),
                batch_id: batch.batch_id.clone(),
            });
            let mut record = Some(record);
            // The batch's close carries the budget firings its calls made due, as the barrier
            // the crash cut would have.
            writer
                .append_with(
                    &mut |conversation, operations, first_seq| {
                        let mut batch: Vec<Record> = record.take().into_iter().collect();
                        let placing = closing_places(conversation);
                        let firings = crate::budget::due(
                            conversation,
                            operations,
                            first_seq,
                            batch.len(),
                            placing,
                        );
                        batch.extend(firings);
                        batch
                    },
                    true,
                )
                .await?;
        }
    }
    // A crash after a step asked for tools and before every call was sent: the calls with no
    // durable intent never left, so they are dispatched now, in the step's batch.
    if let Some(unsent) = unsent_calls(writer).filter(|unsent| !unsent.is_empty()) {
        let step_id = run_state(writer).completed_steps;
        let started = run_state(writer).open_batch.is_some();
        let known = known_calls(writer);
        let planned = planned_calls(writer);
        match batch(
            writer,
            seams,
            &admission.run_id,
            admission.send.scope.as_ref(),
            step_id,
            &planned,
            &unsent,
            &known,
            &manifest,
            hooks.as_ref(),
            started,
            warming.as_ref(),
            changes.as_ref(),
            admission.compaction_plan_version.is_some(),
        )
        .await?
        {
            BatchOutcome::Closed { unknown: false } => {}
            BatchOutcome::Closed { unknown: true } => {
                return finish(
                    writer,
                    seams,
                    &admission.run_id,
                    RunFinishReason::Interrupted,
                )
                .await;
            }
            BatchOutcome::Held(finished) => return Ok(finished),
            BatchOutcome::Cancelled(closed) => {
                let finished =
                    finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await?;
                withdraw(seams, &closed).await;
                return Ok(finished);
            }
        }
    }
    loop {
        // A crash can keep a tool turn durable and lose the budget firings its barrier carried,
        // since today's log writes them record by record. Any firing the durable call count
        // makes due is written here, before anything else at this boundary, as today's loop
        // derives it again before its next request.
        writer
            .append_with(
                &mut |conversation, operations, first_seq| {
                    crate::budget::due(conversation, operations, first_seq, 0, 0)
                },
                true,
            )
            .await?;
        let completed_steps = run_state(writer).completed_steps;
        // Step ids count from 1 within a run, as today's loop numbers them.
        let step_id = completed_steps + 1;
        // The boundary: messages steered or delivered since the last one enter the conversation
        // here, in the same batch as either the step's start or, when a caller cancelled the run
        // now or at this boundary, the run's end. The run's state is read as the batch is built,
        // so a cancel recorded while the last step ran ends the run here.
        //
        // A stop request (the runner is shutting down) is checked after the cancel: the run
        // pauses here if its restart policy says to resume, with the steered messages left for
        // the resume; otherwise it ends interrupted.
        let run_id = admission.run_id.clone();
        let cancelled_now = seams.cancel.is_cancelled();
        let stopping = seams.stop.is_cancelled();
        let now = (seams.now_ms)();
        let mut cancelled_end = Some(run_end(writer, &run_id, RunFinishReason::Cancelled, now));
        let mut stopped_end = Some(run_end(writer, &run_id, RunFinishReason::Interrupted, now));
        if let Some(Record::RunEnd(end)) = stopped_end.as_mut() {
            end.interrupt_reason = Some("runner_shutdown".into());
        }
        let mut restart_pause = Some(Record::Pause(Pause {
            run_id: run_id.clone(),
            reason: PauseReason::Restart,
            ts_ms: Some(now),
            error: None,
            account_limit: None,
        }));
        let resumes = admission.on_restart == Some(OnRestart::Resume);
        // A view the compaction provider handed over applies here, before the step starts, as
        // in today's loop; a boundary that ends the run applies none.
        let ending = cancelled_now
            || stopping
            || writer
                .operations()
                .run
                .as_ref()
                .is_some_and(|run| run.cancel.is_some());
        if let (Some(ctx), false) = (compaction, ending) {
            if let Err(message) = crate::compaction::apply_boundary(
                writer,
                admission,
                seams,
                ctx,
                step_id,
                episodes.signed_floor,
            )
            .await?
            {
                return compaction_unavailable(writer, seams, admission, &message).await;
            }
        }
        // Queued sends with a delivery order are read before the batch is built, since their
        // records may be sealed. One queued after this read waits for the next boundary.
        let ordered = Box::pin(ordered_sends(writer)).await?;
        let boundary = writer
            .append_with(
                &mut |conversation, operations, first_seq| {
                    let run = operations.run.as_ref().expect("a run is active");
                    if !(cancelled_now || run.cancel.is_some()) && stopping && resumes {
                        return restart_pause.take().into_iter().collect();
                    }
                    if cancelled_now || run.cancel.is_some() {
                        let mut batch = placed_copies(operations, &run_id);
                        batch.extend(cancelled_end.take());
                        return batch;
                    }
                    if stopping {
                        let mut batch = placed_copies(operations, &run_id);
                        batch.extend(stopped_end.take());
                        return batch;
                    }
                    // A step starts: ordered sends whose first possible mode here is `steer` or
                    // `turn_system` are delivered, and expired ones end, in queue order, by
                    // today's rules (`broca_core::delivery_order`). Their records come first,
                    // then every waiting message's copy (steers before deliveries), then the
                    // step's start.
                    let mut batch = Vec::new();
                    let mut delivered = Vec::new();
                    let queued: Vec<_> = operations
                        .queue
                        .iter()
                        .filter(|pending| pending.ordered)
                        .filter_map(|pending| {
                            ordered
                                .get(&pending.submission_id)
                                .map(|send| (pending, send))
                        })
                        .collect();
                    // A turn-scoped system message is possible only where the route takes one:
                    // after the message it would follow (the last one placed here so far, else
                    // the last steered, else the conversation's last) and, on a route that needs
                    // it last, with no user message to follow it in this request. As in today's
                    // loop, any later send that may steer counts as one.
                    let mut preceding: Option<Message> = run
                        .pending_steers
                        .iter()
                        .rev()
                        .find_map(|pending| pending.messages.last())
                        .or(conversation.messages.last())
                        .cloned();
                    let last_user = queued.iter().rposition(|(_, send)| {
                        !send.order.expired_at(now)
                            && send.order.modes.contains(&DeliveryMode::Steer)
                    });
                    for (index, (pending, send)) in queued.iter().enumerate() {
                        if send.order.expired_at(now) {
                            batch.push(Record::SendEnded(SendEnded {
                                submission_id: pending.submission_id.clone(),
                                reason: Some(json!({"kind": "expired"})),
                            }));
                            continue;
                        }
                        let has_text = appended_text(&send.input).is_some();
                        let followed_by_user = last_user.is_some_and(|last| last > index);
                        let boundary = Boundary {
                            turn_system: seams
                                .provider
                                .turn_system_possible(preceding.as_ref(), followed_by_user),
                            ..Boundary::STEP
                        };
                        let (mode, placement, input) =
                            match choose_mode(&send.order, boundary, has_text) {
                                Some(DeliveryMode::Steer) => {
                                    ("steer", Placement::Delivered, send.input.clone())
                                }
                                Some(DeliveryMode::TurnSystem) => (
                                    "turn_system",
                                    Placement::DeliveredTurnSystem,
                                    turn_system_input(send, &pending.submission_id, &run_id),
                                ),
                                _ => continue,
                            };
                        let seq = first_seq + batch.len() as u64;
                        delivered.extend(input.iter().map(|message| {
                            Record::Input(Input {
                                // A delivered copy's media are stored where the send's queued
                                // record stored them.
                                media: send.media_of(message),
                                run_id: run_id.clone(),
                                placement,
                                message: message.clone(),
                                written_at_ms: send.written_at_ms,
                                submission_id: Some(pending.submission_id.clone()),
                                notice: None,
                                tags_v1: None,
                                receipt_seq: Some(seq),
                                pruned: Vec::new(),
                            })
                        }));
                        if let Some(last) = input.last() {
                            preceding = Some(last.clone());
                        }
                        batch.push(Record::Delivery(Delivery {
                            submission_id: pending.submission_id.clone(),
                            run_id: run_id.clone(),
                            mode: mode.into(),
                            input,
                            tool_call_id: None,
                            appended_text: None,
                            written_at_ms: send.written_at_ms,
                            tags_v1: None,
                            input_listed: Some(true),
                        }));
                    }
                    batch.extend(placed_copies(operations, &run_id));
                    batch.extend(delivered);
                    batch.push(Record::StepStart(StepStart {
                        run_id: run_id.clone(),
                        step_id,
                        // Minted by the entry-facts writer once the batch's seqs are known.
                        mid: None,
                    }));
                    batch
                },
                false,
            )
            .await?;
        match decision(&boundary) {
            Some(Record::RunEnd(end)) => return Ok(finished(writer, end.reason)),
            Some(Record::Pause(_)) => {
                let mut paused = finished(writer, RunFinishReason::Interrupted);
                paused.paused = Some(PauseReason::Restart);
                return Ok(paused);
            }
            _ => {}
        }
        let mut request_timing = broca_session_store::metrics::Phase::RequestPrepare
            .start()
            .with_session(|| Some(broca_core::session_addr(&seams.bind)));
        let run_rules = if contract && tag_rules_read {
            Vec::new()
        } else {
            tag_rules_read = true;
            run_tag_rules(writer, &episodes, &mut tag_rules).await?
        };
        let enrollment = writer.enrollment().await?;
        let mut opts = call_options(
            writer,
            admission,
            &tools,
            &episodes,
            &run_rules,
            seams.session_address.clone(),
            step_id,
            compaction.is_some(),
            defer_media_tags,
            enrollment.as_ref(),
            compaction.map(|_| &mut kept_prompt),
        )?;
        // The step names the account routing gives the run now, as today's does.
        if let Some(context) = opts.step_context.as_mut() {
            let selection = run_selection(seams, admission);
            context.credential_id = selection.map(|selection| selection.credential_id.clone());
            context.account_ref = selection.and_then(|selection| selection.account_ref.clone());
        }
        let finalizing = finalize_tool_choice(writer, &mut opts);
        // The session's original system message, which a plan's system text takes the place of
        // in the provider view below.
        let durable_system: Vec<Message> = opts.prompt.first().cloned().into_iter().collect();
        // What the step's request waits for before it can be rendered (`Seams::model_call_ready`,
        // such as the session's media) is waited for here, before anything below renders it: the
        // compaction estimate, the enrolling run's own estimate, the output cap's estimate and the
        // check of a request an account-limit pause refused. When the wait fails, none of those
        // renders: the model call's own wait then fails the step with that error before anything
        // is sent, so the run still ends with its end record.
        request_timing.pause();
        let media_timing = broca_session_store::metrics::Phase::MediaWait
            .start()
            .with_session(|| Some(broca_core::session_addr(&seams.bind)));
        let call_ready = match &seams.model_call_ready {
            Some(ready) => tokio::select! {
                ready = ready.ready() => ready.is_ok(),
                _ = seams.cancel.cancelled() => {
                    return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled)
                        .await;
                }
            },
            None => true,
        };
        drop(media_timing);
        request_timing.resume();
        if let Some(ctx) = compaction.filter(|_| call_ready) {
            let marked = writer.conversation().marker.clone();
            match crate::compaction::prepare(
                writer,
                admission,
                seams,
                ctx,
                step_id,
                opts,
                &mut kept_prompt,
                &rendered,
            )
            .await?
            {
                crate::compaction::Send::Ready(ready) => opts = *ready,
                crate::compaction::Send::Unavailable(message) => {
                    return compaction_unavailable(writer, seams, admission, &message).await;
                }
                crate::compaction::Send::Ceiling(detail) => {
                    // Written with the run's end, which is durable, as today's loop writes it.
                    writer
                        .append(
                            &[Record::CeilingDecided(CeilingDecided {
                                step_id,
                                detail: Some(detail),
                            })],
                            false,
                        )
                        .await?;
                    return finish(
                        writer,
                        seams,
                        &admission.run_id,
                        RunFinishReason::ContextCeiling,
                    )
                    .await;
                }
            }
            // The step applied a view with a new cut: the history before its first kept entry
            // is sealed now, before the request goes out; the request is already built, so this
            // changes none of its bytes.
            if let Some(marker) = writer
                .conversation()
                .marker
                .clone()
                .filter(|marker| Some(marker) != marked.as_ref())
            {
                crate::compaction::seal_at_cut(writer, seams, ctx, marker.first_kept_ordinal)
                    .await?;
                // The seal moved the runs wholly before the cut out of the window and kept only
                // those whose settings still shape a request (see `Conversation::seal_window`).
                // The run's list is read again from the sealed conversation, as a run starting
                // now would read it, so later steps carry the bounded list rather than every run
                // the history held when this run started. Every renderer gives the same bytes
                // for both lists (`openai_responses_chatgpt::reduce_episodes`).
                episodes = Arc::new(render_history(writer, parts, &mut tool_sets).await?);
            }
        }
        // Once a refresh applied, the model sees the system text of the plan the run froze in
        // place of the session's original system message, as a view of the request only, as
        // today's loop shows it. A session on the current compaction contract renders its
        // system text from its entries instead.
        if admission.compaction_plan_version.is_none()
            && writer
                .operations()
                .plan
                .as_ref()
                .is_some_and(|plan| plan.applied_generation > 0)
        {
            if let Some(manifest) = &admission.manifest {
                if let Some(view) =
                    broca_core::plan_system::view(&opts, &durable_system, &manifest.system_text)
                {
                    opts = view;
                }
            }
        }
        // Client-side refusal fallback: the step goes to the run's refusal-fallback model while
        // the session's schedule says so, or when it is a step already refused and re-sent. On
        // the run that moved the session onto the current compaction contract, the newest
        // step's usage measures the request from before the move, which rebuilt it, so the
        // prepared request's own estimate decides whether it fits the fallback's window.
        let own_estimate = match (
            call_ready && crate::enroll::is_enrolling_run(writer.operations(), &admission.run_id),
            seams.provider.refusal_fallback_model(),
        ) {
            (true, Some(_)) => {
                let history = writer
                    .operations()
                    .estimate
                    .as_ref()
                    .map(crate::compaction::history)
                    .unwrap_or_default();
                let bytes = seams
                    .provider
                    .try_render_request(&opts)
                    .map_err(|error| LoopError::Parts(error.message))?;
                Some(broca_core::compaction::request_estimate(bytes.len(), &history).0)
            }
            _ => None,
        };
        let mut route =
            crate::refusal::Route::decide(writer, seams.provider, step_id, own_estimate)?;
        let prepared = opts;
        let mut opts = match reserve_for(
            writer, seams, admission, step_id, &prepared, &route, call_ready,
        )
        .await?
        {
            Some(opts) => opts,
            None => return Ok(finished(writer, RunFinishReason::Error)),
        };
        // The step a timed account pause resumes is sent again only if it renders the request
        // the provider refused, byte for byte, as today's resume checks it: anything else ends
        // the run interrupted rather than send a request the pause wasn't for.
        if let Some(pause) = account_limit_replay.take().filter(|_| call_ready) {
            use sha2::Digest as _;
            let bytes = seams
                .provider
                .try_render_request(&opts)
                .map_err(|error| LoopError::Parts(error.message))?;
            if pause.failed_step != StepId(step_id)
                || pause.request_body_len != bytes.len() as u64
                || pause.request_body_sha256 != format!("{:x}", sha2::Sha256::digest(&bytes))
            {
                let error = broca_core::account_limit_pause::request_changed_error();
                return end_with_error(
                    writer,
                    seams,
                    &admission.run_id,
                    RunFinishReason::Interrupted,
                    &error,
                )
                .await;
            }
        }
        // A strict structured-output schema the provider refuses as too large a grammar is
        // downgraded once: the downgrade is durable, the provider renders without the strict
        // flag, and the same step is sent again, its retries counting the refused attempt's, as
        // today's loop does.
        let mut downgraded_now = false;
        let mut prior_retries = 0;
        // The retries of an attempt the run's own model refused, which the re-sent step's count.
        let mut refusal_retries = 0u32;
        drop(request_timing);
        let outcome = loop {
            let outcome = model_step(
                writer,
                seams,
                &admission.run_id,
                step_id,
                opts.clone(),
                requested_cache_ttl(admission),
                admission.account_limit_pause,
            )
            .await?;
            if let Err(StepFailure::Error(error, retries_used, _)) = &outcome {
                // The provider decides: it answers yes only for its own strict schema's refusal.
                if !downgraded_now && seams.provider.structured_output_grammar_rejection(error) {
                    writer
                        .append(
                            &[Record::StructuredDowngraded(
                                broca_session_store::record::StructuredDowngraded {
                                    run_id: admission.run_id.clone(),
                                },
                            )],
                            true,
                        )
                        .await?;
                    seams.provider.downgrade_structured_output();
                    if let Some(frozen) = structured.as_mut() {
                        frozen.downgrade_grammar();
                    }
                    downgraded_now = true;
                    prior_retries = *retries_used;
                    continue;
                }
            }
            // The run's own model refused the step: the refusal's billed usage and the schedule
            // it arms are durable before the same step is re-sent to the fallback model. The
            // refused answer is not kept. A resume after that barrier sends the step there.
            if let Ok(step) = &outcome {
                if let Some(fallback) = route
                    .resend_to(
                        &step.message,
                        step.finish_reason == FinishReason::ContentFilter,
                    )
                    .cloned()
                {
                    let now = Some((seams.now_ms)()).filter(|at| *at != 0);
                    let schedule = broca_core::refusal_fallback::on_source_refusal(
                        crate::refusal::schedule(writer)?.as_ref(),
                        &admission.model,
                        &fallback,
                        now,
                    );
                    writer
                        .append(
                            &[Record::StepRefused(
                                broca_session_store::record::StepRefused {
                                    run_id: admission.run_id.clone(),
                                    step_id,
                                    usage: step.served.total(step.usage),
                                    served_model: step.served.served_model.clone(),
                                    usage_iterations: step
                                        .served
                                        .usage_iterations
                                        .as_ref()
                                        .map(serde_json::to_value)
                                        .transpose()
                                        .map_err(|e| LoopError::Parts(e.to_string()))?,
                                    discarded_blocks: step.message.content.len() as u32,
                                    schedule: serde_json::to_value(&schedule)
                                        .map_err(|e| LoopError::Parts(e.to_string()))?,
                                    ts_ms: now,
                                    by_model: step.served.shares(
                                        admission,
                                        None,
                                        step.served.total(step.usage),
                                    ),
                                },
                            )],
                            true,
                        )
                        .await?;
                    refusal_retries = step.retries_used;
                    route.reroute(seams.provider);
                    opts = match reserve_for(
                        writer, seams, admission, step_id, &prepared, &route, call_ready,
                    )
                    .await?
                    {
                        Some(opts) => opts,
                        None => return Ok(finished(writer, RunFinishReason::Error)),
                    };
                    continue;
                }
            }
            break match outcome {
                Ok(mut step) => {
                    step.retries_used = step
                        .retries_used
                        .saturating_add(prior_retries)
                        .saturating_add(refusal_retries);
                    Ok(step)
                }
                Err(StepFailure::Error(error, retries, limited)) => Err(StepFailure::Error(
                    error,
                    retries
                        .saturating_add(prior_retries)
                        .saturating_add(refusal_retries),
                    limited,
                )),
                Err(failure) => Err(failure),
            };
        };
        let step = match outcome {
            Ok(step) => step,
            Err(StepFailure::Cancelled) => {
                return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await;
            }
            Err(StepFailure::Error(error, retries, limited)) => {
                // The error names the run's account or credential where it is scoped to one, as
                // today's does before deciding anything on it.
                let selection = run_selection(seams, admission);
                let error = Box::new(broca_core::provider_errors::stamp_credential(
                    *error,
                    selection.and_then(|selection| selection.account_ref.as_deref()),
                    selection.map(|selection| selection.credential_id.as_str()),
                ));
                // A failed sign-in pauses the run: after the credential is refreshed, it resumes
                // as the same run, asking for this step again.
                if error.class == broca_protocol::ErrorClass::Auth {
                    return pause(
                        writer,
                        seams,
                        &admission.run_id,
                        PauseReason::AuthRequired,
                        Some(*error),
                        None,
                    )
                    .await;
                }
                // The provider refused the account's limit, which resets soon enough that the
                // session's cache on that account outlives the wait: the run pauses on the same
                // account until then, as today's loop decides it.
                if let Some(pause_on) = limited.and_then(|limited| {
                    account_limit_pause(seams, admission, step_id, &limited, &error)
                }) {
                    return pause(
                        writer,
                        seams,
                        &admission.run_id,
                        PauseReason::AccountLimit,
                        Some(*error),
                        Some(pause_on),
                    )
                    .await;
                }
                let now = (seams.now_ms)();
                let mut record = run_end(writer, &admission.run_id, RunFinishReason::Error, now);
                if let Record::RunEnd(end) = &mut record {
                    end.error = Some(
                        serde_json::to_value(&*error)
                            .map_err(|e| LoopError::Parts(e.to_string()))?,
                    );
                    // The failed step wrote no end of its own, so its retries count here.
                    end.retries_used = end.retries_used.saturating_add(retries);
                }
                append_end(writer, &admission.run_id, vec![record]).await?;
                return Ok(finished(writer, RunFinishReason::Error));
            }
        };
        // The post-assistant hooks edit the answer's text before anything decides on it, so
        // the stop conditions, the record and the next request all see the edited answer.
        let mut step = step;
        let step_hooks = match hooks.as_ref() {
            Some(runtime) => post_assistant(runtime, step_id, &mut step.message).await,
            None => None,
        };
        // A cut call on a budget's finalize step or in a structured run takes paths of today's
        // loop the new one hasn't built; refused by name rather than run without them.
        if !step.cuts.is_empty() && (finalizing || structured.is_some()) {
            return Err(LoopError::Unsupported(
                "a cut tool call on a finalize step or in a structured-output run",
            ));
        }
        // The finalize step's answer is the run's last: calls the model made anyway are dropped
        // before anything records or decides on the answer, and it ends as a stop.
        if finalizing {
            step.message.content.retain(|block| {
                !matches!(block, broca_protocol::content::ContentBlock::ToolCall(_))
            });
            if step.finish_reason == FinishReason::ToolCalls {
                step.finish_reason = FinishReason::Stop;
            }
        }
        // What the step records about the refusal fallback. A refusal after a completed tool
        // call commits as a tool step and arms the schedule, as today's loop commits it.
        let refusal = route.committed(
            writer,
            &admission.model,
            &step.message,
            step.finish_reason == FinishReason::ContentFilter,
            step.served.served_model.as_deref(),
            Some((seams.now_ms)()).filter(|at| *at != 0),
        )?;
        if refusal.after_tool_use {
            step.finish_reason = FinishReason::ToolCalls;
        }
        // Which model each part of the step's usage counts under. The route's model is read
        // here because `Served::members` below moves the route into the step's fallback facts.
        let route_model = refusal.route.as_ref().map(|route| route.model.clone());
        let calls: Vec<ToolCall> = step.message.tool_calls().cloned().collect();
        let stop = stop_satisfied(&stop_when, completed_steps + 1, &step.message).cloned();
        let limiting = limiting_stop_satisfied(&stop_when, completed_steps + 1, &step.message);
        let now = (seams.now_ms)();
        // On a step the fallback model answered, the message is the fallback's.
        let assistant = Message::assistant_from(
            step.message.content.clone(),
            route.model(&admission.model).clone(),
        );
        // The step's whole usage, over every model that ran when the provider handed the
        // request on itself, as today's loop records it on both the message and the step.
        let whole = step.served.total(step.usage);
        // A call whose arguments the provider cut is answered with today's error in the step's
        // own batch, before anything is sent; the batch then runs the other calls.
        let batch_id = format!("{}-b{step_id}", admission.run_id);
        let mut cut = Vec::new();
        if !step.cuts.is_empty() {
            if stop.is_some() || seams.stop.is_cancelled() || seams.cancel.is_cancelled() {
                return Err(LoopError::Unsupported(
                    "a cut tool call on a step that meets a stop condition, a stop or a cancel",
                ));
            }
            cut.push(Record::BatchStart(BatchStart {
                run_id: admission.run_id.clone(),
                batch_id: batch_id.clone(),
                step_id,
                planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
                mid: None,
            }));
            for garbage in &step.cuts {
                terminal_with_reason(
                    &mut cut,
                    &admission.run_id,
                    &batch_id,
                    error_result(&garbage.tool_call_id, &garbage.error_text()),
                    CallOutcome::Completed,
                    Some("tool_args_trailing_garbage".into()),
                    Some(now),
                );
            }
        }
        let uncut: Vec<ToolCall> = calls
            .iter()
            .filter(|call| {
                !step
                    .cuts
                    .iter()
                    .any(|garbage| garbage.tool_call_id == call.tool_call_id)
            })
            .cloned()
            .collect();
        writer
            .append(
                &[
                    Record::Assistant(Assistant {
                        run_id: admission.run_id.clone(),
                        step_id,
                        provider_message_id: step.message.message_id.clone(),
                        message: assistant,
                        usage: Some(whole),
                        written_at_ms: Some(now),
                        last_content_at_ms: None,
                        tags_v1: None,
                        budget_finalize: finalizing.then_some(true),
                        pruned: Vec::new(),
                    }),
                    Record::StepEnd(StepEnd {
                        run_id: admission.run_id.clone(),
                        step_id,
                        usage: Some(whole),
                        finish_reason: Some(step.finish_reason),
                        retries_used: step.retries_used,
                        service_tier_used: step.served.service_tier_used.clone(),
                        speed_used: step.served.speed_used.clone(),
                        call_count: calls.len() as u32,
                        fallback: step.served.members(refusal)?,
                        hooks: step_hooks,
                        budget_finalize: finalizing.then_some(true),
                        // As the converter fills it from today's record: what the iterations
                        // add up to, when that differs from the recorded usage.
                        total_usage: Some(step.served.total(whole)).filter(|total| *total != whole),
                        stop_met: stop.is_some().then_some(true),
                        limiting_stop_met: limiting.then_some(true),
                        by_model: step.served.shares(admission, route_model.as_ref(), whole),
                    }),
                ]
                .into_iter()
                .chain(cut)
                .collect::<Vec<_>>(),
                calls.is_empty(),
            )
            .await?;
        if let Some(ctx) = compaction {
            crate::compaction::notice(writer, admission, seams, ctx, episodes.signed_floor).await?;
        }
        if calls.is_empty() {
            // A message steered in during the step makes the run take one more step, unless a
            // stop condition that limits the run was met. Otherwise the run ends here.
            let end = run_end(
                writer,
                &admission.run_id,
                RunFinishReason::Completed,
                (seams.now_ms)(),
            );
            let run_id = admission.run_id.clone();
            let mut end = Some(end);
            // A completed run admitted with structured output records what it produced with its
            // end: a `final_text` run its answer's text, a `submit_tool` run that never called
            // `submit_result` nothing.
            let mut produced = structured.as_ref().map(|frozen| {
                let text = (frozen.mode == broca_protocol::structured_output::Mode::FinalText)
                    .then(|| broca_core::structured_output::joined_text(&step.message));
                structured_result(
                    &run_id,
                    broca_protocol::structured_output::result(
                        frozen,
                        true,
                        None,
                        text.as_deref(),
                        Vec::new(),
                    ),
                )
            });
            let appended = writer
                .append_with(
                    &mut |_, operations, _| {
                        let run = operations.run.as_ref().expect("a run is active");
                        // The finalize step is the run's last, whatever was steered in.
                        let extends = !finalizing
                            && run.natural_end.is_some_and(|end| end.extendable)
                            && run.pending_messages() > 0;
                        if extends {
                            return Vec::new();
                        }
                        let mut batch = placed_copies(operations, &run_id);
                        batch.extend(produced.take());
                        batch.extend(end.take());
                        batch
                    },
                    true,
                )
                .await?;
            if appended.is_empty() {
                continue;
            }
            return Ok(finished(writer, RunFinishReason::Completed));
        }
        // A `submit_tool` run ends at the step that calls `submit_result`, before anything is
        // dispatched or a stop condition is weighed, as today's loop ends it.
        if let Some(frozen) = structured.as_ref() {
            if let Some(submit) =
                broca_core::structured_output::counted_submit(frozen, &step.message, &[])
            {
                let pending = PendingSubmit {
                    step_id,
                    calls: calls.clone(),
                    submit,
                    started: false,
                    answered: Vec::new(),
                };
                return submitted(writer, seams, &admission.run_id, frozen, &pending).await;
            }
        }
        if let Some(stop) = stop {
            return hard_stop(writer, seams, &admission.run_id, step_id, &calls, &stop).await;
        }
        // A stop, or a caller's cancel that arrived after the model's answer, refuses the batch
        // before any call is gated, so no pre-tool hook runs and the plane is never told.
        if seams.stop.is_cancelled() || seams.cancel.is_cancelled() {
            let cancelled = seams.cancel.is_cancelled();
            return stopped_batch(writer, seams, &admission.run_id, step_id, &calls, cancelled)
                .await;
        }
        match batch(
            writer,
            seams,
            &admission.run_id,
            admission.send.scope.as_ref(),
            step_id,
            &calls,
            &uncut,
            &[],
            &manifest,
            hooks.as_ref(),
            !step.cuts.is_empty(),
            warming.as_ref(),
            changes.as_ref(),
            admission.compaction_plan_version.is_some(),
        )
        .await?
        {
            BatchOutcome::Closed { unknown: false } => {}
            BatchOutcome::Closed { unknown: true } => {
                return finish(
                    writer,
                    seams,
                    &admission.run_id,
                    RunFinishReason::Interrupted,
                )
                .await;
            }
            BatchOutcome::Held(finished) => return Ok(finished),
            BatchOutcome::Cancelled(closed) => {
                let finished =
                    finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await?;
                withdraw(seams, &closed).await;
                return Ok(finished);
            }
        }
    }
}

/// Stores the run's setup parts and appends its start and input as one durable batch.
/// Writes a fresh run's start; returns the seq of its `RunStart` record.
/// `value` as JSON, or `None` when there is none.
fn to_value<T: serde::Serialize>(value: Option<&T>) -> Result<Option<Value>, LoopError> {
    value
        .map(serde_json::to_value)
        .transpose()
        .map_err(|e| LoopError::Parts(e.to_string()))
}

/// Writes the run's start. `checkpoint` is the highest part number the session has shown
/// ([`shown_high`]), which the run's computed numbering continues after. `enrolled` is the
/// record of the session's move onto the current compaction contract when this run makes it,
/// which its start's batch carries.
async fn admit<W: Writer>(
    writer: &mut W,
    parts: &mut dyn Parts,
    admission: &Admission,
    checkpoint: u64,
    enrolled: Option<broca_session_store::record::CompactionEnrolled>,
) -> Result<u64, LoopError> {
    // The session's refusal-fallback schedule carries into the run only under the same model
    // and within a day of its last change, as today's admission decides it.
    let refusal_schedule = broca_core::refusal_fallback::on_episode_admitted(
        crate::refusal::schedule(writer)?,
        &admission.model,
        admission.ts_ms,
    )
    .map(serde_json::to_value)
    .transpose()
    .map_err(|e| LoopError::Parts(e.to_string()))?;
    let mut setup = Vec::new();
    // A planned run's system text is part of its manifest; any other run's is its own part.
    let global_system = (admission.manifest.is_none() && !admission.global_system.is_empty())
        .then(|| serde_json::to_value(&admission.global_system))
        .transpose()
        .map_err(|e| LoopError::Parts(e.to_string()))?
        .unwrap_or(Value::Null);
    for (kind, value) in [
        (PartKind::Tools, serde_json::to_value(&admission.tools)),
        (
            PartKind::ResponseFormat,
            serde_json::to_value(&admission.response_format),
        ),
        (PartKind::GlobalSystem, Ok(global_system)),
        (PartKind::RenderConfig, Ok(admission.render_config.clone())),
    ] {
        let value = value.map_err(|e| LoopError::Parts(e.to_string()))?;
        if value.is_null() {
            continue;
        }
        setup.push(parts.put(kind, &value).map_err(LoopError::Parts)?);
    }
    // A manifest's shared members are setup parts after the run config's, in the order the
    // resume reads them back; what remains of it stays with the run's choices.
    let manifest_rest = match &admission.manifest {
        Some(manifest) => manifest_parts(manifest, parts, &mut setup)?,
        None => None,
    };
    let tool_names = admission
        .tools
        .iter()
        .map(|tool| tool.name.clone())
        .collect();
    let mut config = serde_json::Map::new();
    config.insert("tool_choice".into(), json!(admission.tool_choice));
    config.insert("generation".into(), json!(admission.generation));
    if !admission.provider_options.is_null() {
        config.insert(
            "provider_options".into(),
            admission.provider_options.clone(),
        );
    }
    // A plan that asks for part numbers in the model's view freezes that on the run, as today.
    if frozen_render_tags(admission) {
        config.insert("render_tags".into(), json!(true));
    }
    // Today's computed tag rules, starting after the highest number the session has shown.
    config.insert("computed_tags".into(), json!({ "checkpoint": checkpoint }));
    config.insert("context_epoch".into(), json!(0));
    if !admission.stop_when.is_empty() {
        config.insert("stop_when".into(), json!(admission.stop_when));
    }
    if let Some(budget) = &admission.budget {
        config.insert("budget".into(), json!(budget));
    }
    if admission.account_limit_pause {
        config.insert("account_limit_pause".into(), json!(true));
    }
    // The members today's run config serializes only when set.
    for (key, value) in [
        (
            "turn_directives_v1",
            to_value(admission.directives.as_ref())?,
        ),
        ("work_class", to_value(admission.work_class.as_ref())?),
        ("variant", to_value(admission.variant.as_ref())?),
        (
            "context_management",
            to_value(admission.context_management.as_ref())?,
        ),
        (
            "tools_were_omitted",
            admission.tools_were_omitted.then_some(json!(true)),
        ),
    ] {
        if let Some(value) = value {
            config.insert(key.into(), value);
        }
    }
    let mut choices = serde_json::Map::new();
    choices.insert("config".into(), Value::Object(config));
    if let Some(rest) = manifest_rest {
        choices.insert("manifest".into(), rest);
    }
    let mut record = serde_json::Map::new();
    if let Some(version) = admission.marker_order_version {
        record.insert("marker_order_version".into(), json!(version));
    }
    if let Some(sent_by) = to_value(admission.send.sent_by.as_ref())? {
        record.insert("sent_by".into(), sent_by);
    }
    choices.insert("record".into(), Value::Object(record));
    let mut records = Vec::new();
    if writer.operations().setup_changed(&setup, &admission.model) {
        records.push(Record::SetupStub(SetupStub {
            parts: setup.clone(),
            model: admission.model.clone(),
            tool_names,
            render_key: Some(
                crate::parts::render_key(&admission.render_config).map_err(LoopError::Parts)?,
            ),
        }));
    }
    let message_marks = to_value(admission.send.message_marks.as_ref())?;
    records.push(Record::RunStart(Box::new(RunStart {
        imported_ids: None,
        named_speaker: broca_session_store::record::marks_name_speaker(message_marks.as_ref()),
        message_marks,
        run_id: admission.run_id.clone(),
        model: admission.model.clone(),
        setup,
        renderer_version: admission.renderer_version,
        submission_id: admission.send.submission_id.clone(),
        send_id: admission.send.send_id.clone(),
        request_identity_digest: admission.send.request_identity_digest.clone(),
        ts_ms: admission.ts_ms,
        written_at_ms: admission.written_at_ms,
        session_class: admission.send.session_class.clone(),
        retention_secs: admission.send.retention_secs,
        title: admission.send.title.clone(),
        context_limit: admission.context_limit,
        service_tier: admission.service_tier.map(|tier| tier.as_str().to_owned()),
        route_decision_id: admission.route_decision_id.clone(),
        compaction_plan_version: admission.compaction_plan_version,
        auth_selection: admission
            .auth_selection
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| LoopError::Parts(e.to_string()))?,
        run_choices: Some(Value::Object(choices)),
        origin: Some(json!({ "kind": "fresh" })),
        principal: to_value(admission.send.principal.as_ref())?,
        scope: to_value(admission.send.scope.as_ref())?,
        hooks: to_value(admission.send.hooks.as_ref())?,
        keep_warm: admission
            .keep_warm
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| LoopError::Parts(e.to_string()))?,
        on_restart: admission
            .on_restart
            .map(serde_json::to_value)
            .transpose()
            .map_err(|e| LoopError::Parts(e.to_string()))?,
        seeded: None,
        refusal_schedule,
    })));
    for message in &admission.input {
        records.push(Record::Input(Input {
            media: Vec::new(),
            run_id: admission.run_id.clone(),
            placement: Placement::RunInput,
            message: message.clone(),
            written_at_ms: admission.written_at_ms,
            submission_id: None,
            notice: None,
            tags_v1: None,
            receipt_seq: None,
            pruned: Vec::new(),
        }));
    }
    // The pending plan change this run applies is recorded after its input, so the start is
    // still followed directly by its input.
    if let Some(applied) = &admission.applies_refresh {
        records.push(Record::RefreshApplied(refresh_applied(applied)));
    }
    // So is the session's enrollment, in the same batch as the start that moves it onto the
    // contract: once the batch commits, the session is enrolled for good.
    if let Some(enrolled) = enrolled {
        records.push(Record::CompactionEnrolled(enrolled));
    }
    // The start's seq is the one the batch took, which a writer shared with the session actor
    // knows only once it is appended.
    let position = records
        .iter()
        .position(|record| matches!(record, Record::RunStart(_)))
        .expect("the batch holds the run's start") as u64;
    let first = writer.append(&records, true).await?;
    Ok(first + position)
}

/// The highest part number the session's requests have shown, which a new run's numbering
/// continues after: today's `high_after` over the prompt, each message numbered under the rules
/// of the run that placed it. Only a run that renders numbers counts anything, so a session
/// that has never rendered stays at 0.
///
/// Counted over the window. A window that doesn't start at the session's first message has had
/// numbers sealed out with the history before it, which isn't modelled yet.
async fn shown_high<W: Writer>(writer: &W, parts: &dyn Parts) -> Result<u64, LoopError> {
    let conversation = writer.conversation();
    let start = conversation.window_start();
    // History before the window was sealed at a compaction cut, whose marker records the highest
    // number shown through it; numbers never change at a cut.
    let sealed_high = match (start > 1, &conversation.marker) {
        (false, _) => 0,
        (true, Some(marker)) => marker.checkpoint,
        (true, None) => {
            return Err(LoopError::Unsupported(
                "a new run after history was sealed without a compaction cut",
            ))
        }
    };
    // Each window run's frozen choices: whether it rendered numbers (its start's run config), and
    // whether its replies were cleaned (that and its render config).
    // A request lists the runs sealed before the window first; only the window's own place
    // messages here.
    let rendered = render_episodes(writer, parts)?;
    let window_runs = &rendered[conversation.sealed_episodes.len()..];
    let mut runs = Vec::new();
    for (episode, render_input) in conversation.episodes.iter().zip(window_runs) {
        let start_record = writer.run_start(&episode.run_id).await?.ok_or_else(|| {
            LoopError::Parts(format!(
                "the window names run {} but the session holds no start for it",
                episode.run_id
            ))
        })?;
        let choices = start_record.run_choices.unwrap_or(Value::Null);
        let render = choices["config"]["render_tags"].as_bool().unwrap_or(false);
        let clean =
            broca_core::compaction_v2::metadata::clean_replies(render, &render_input.frozen_config);
        runs.push(((episode.start - 1) as usize, render, clean));
    }
    let mut high = sealed_high;
    for (index, message) in conversation.messages.iter().enumerate() {
        if let Some(base) = conversation
            .entries
            .get(index)
            .and_then(|entry| entry.facts.as_ref())
            .and_then(|facts| facts.runner_tag_base)
        {
            high = high.max(base);
        }
        let index = index + (start - 1) as usize;
        // The run that placed this message is the last one starting at or before it.
        let (render, clean) = runs
            .iter()
            .rev()
            .find(|(first, _, _)| *first <= index)
            .map_or((false, false), |(_, render, clean)| (*render, *clean));
        let mut numbered = message.clone();
        broca_protocol::tags::number_message(
            &mut numbered,
            ComputedRules { render, clean },
            &mut high,
        )
        .map_err(|_| LoopError::Unsupported("part numbers past the numbering limit"))?;
    }
    Ok(high.max(writer.retired_tag_floor().saturating_sub(1)))
}

/// Every window run's place in the prompt (its first message's index, and its first user
/// message's) and the render config it ran under, as requests carry them.
fn render_episodes<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
) -> Result<Vec<EpisodeRenderInput>, LoopError> {
    let conversation = writer.conversation();
    // The runs past the window whose settings still shape a request come first, in run order,
    // then the window's own.
    conversation
        .sealed_episodes
        .iter()
        .chain(&conversation.episodes)
        .map(|episode| {
            // Positions count from the session's first message, as today's: once history is
            // sealed at a cut, the window's first message keeps its place.
            let index = |ordinal: u64| (ordinal - 1) as usize;
            Ok(EpisodeRenderInput {
                prompt_start: index(episode.start),
                first_user: episode.first_user.map(index),
                frozen_config: match &episode.render_config {
                    Some(part) => parts.get(part).map_err(LoopError::Parts)?,
                    None => Value::Null,
                },
            })
        })
        .collect()
}

/// The request history every step of a run carries: [`render_episodes`], and the signed floor
/// folded from the same runs' starts (`broca_core::signed_floor`), with the rule today's loop
/// folds its `RunStarted` records by.
///
/// Past the window the conversation keeps only the runs whose render key changes (what renderers
/// read from a run's render config to render another run's messages: the wire family, model and
/// reasoning effort; see `episode_render_key`), so a run that changed only the tool set can be
/// missing there and the fold may find a different change point than one over every run. That
/// never changes a request: such a point is below the window, where no message the request
/// renders sits, so either floor drops the same reasoning.
async fn render_history<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    floors: &mut ToolSetCache,
) -> Result<RenderHistory, LoopError> {
    let mut history = RenderHistory::from(render_episodes(writer, parts)?);
    history.signed_floor = signed_floor(writer, parts, floors).await?;
    Ok(history)
}

/// The session's signed floor (`broca_core::signed_floor::SignedFloor::floor`), folded from its
/// runs' starts by the rule today's loop folds its `RunStarted` records by. [`render_history`]
/// says how far past the window that fold reaches.
pub async fn signed_floor<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    floors: &mut ToolSetCache,
) -> Result<Option<u64>, LoopError> {
    let conversation = writer.conversation();
    let mut floor = broca_core::signed_floor::SignedFloor::default();
    for episode in conversation
        .sealed_episodes
        .iter()
        .chain(&conversation.episodes)
    {
        let digest = match floors.by_run.get(&episode.run_id) {
            Some(digest) => digest.clone(),
            None => {
                let start = writer.run_start(&episode.run_id).await?.ok_or_else(|| {
                    LoopError::Parts(format!(
                        "the conversation names run {} but the session holds no start for it",
                        episode.run_id
                    ))
                })?;
                let digest = tool_set_of_start(&start, parts)?;
                floors.by_run.insert(episode.run_id.clone(), digest.clone());
                digest
            }
        };
        if let Some(digest) = digest {
            floor.observe_run(episode.start - 1, digest);
        }
    }
    Ok(floor.floor())
}

/// Each run's tool-set digest as its start froze it, by run id, read once per run. `None` for a
/// seeded run, which never sends a request.
#[derive(Default)]
pub struct ToolSetCache {
    by_run: std::collections::HashMap<String, Option<String>>,
}

/// The [`broca_core::signed_floor::tool_set_digest`] of the tools and render config `start`
/// froze, read as [`admission_of_start`] reads them; `None` for a seeded run, as today's replay
/// leaves an import's seeded `RunStarted` out of the fold.
fn tool_set_of_start(start: &RunStart, parts: &dyn Parts) -> Result<Option<String>, LoopError> {
    if start.seeded == Some(true) {
        return Ok(None);
    }
    let part = |kind: PartKind| -> Result<Option<Value>, LoopError> {
        start
            .setup
            .iter()
            .find(|part| part.kind == kind)
            .map(|part| parts.get(part).map_err(LoopError::Parts))
            .transpose()
    };
    let tools: Vec<Tool> = decode(part(PartKind::Tools)?)?.unwrap_or_default();
    let render_config = part(PartKind::RenderConfig)?.unwrap_or(Value::Null);
    Ok(Some(broca_core::signed_floor::tool_set_digest(
        &tools,
        &render_config,
    )))
}

/// The request for step `step_id`: the conversation as the folds hold it, under the run's
/// frozen settings, with the render inputs today's loop gives a request: each message's
/// position, its part numbers under the run's tag rules, and the run's place in the prompt.
/// `enrollment` is what the session's move onto the current compaction contract decided about
/// the messages placed before it, which a request on the contract renders them by. A compacted
/// run on the contract passes the stored prompt it keeps, `kept`, which takes in the step's new
/// messages instead of the request's prompt holding a copy of the window: the request's prompt
/// is the one the compaction provider's view renders from it (`compaction::prepare`).
#[allow(clippy::too_many_arguments)]
fn call_options<W: Writer>(
    writer: &W,
    admission: &Admission,
    tools: &Arc<Vec<Tool>>,
    episodes: &Arc<RenderHistory>,
    run_rules: &[RunTagRules],
    session_address: Option<String>,
    step_id: u64,
    compacted: bool,
    defer_media_tags: bool,
    enrollment: Option<&broca_session_store::session::EnrollmentOverlay>,
    kept: Option<&mut crate::compaction::prompt::KeptPrompt>,
) -> Result<CallOptions, LoopError> {
    let contract = admission.compaction_plan_version.is_some();
    let overflow = |_| LoopError::Unsupported("part numbers past the numbering limit");
    let mut kept = kept.filter(|_| contract && compacted);
    let mut prompt = match kept.as_mut() {
        Some(kept) => {
            kept.update(writer, episodes, enrollment)?;
            Vec::new()
        }
        // A session on the current compaction contract renders its numbers from the entries.
        None if contract => {
            crate::compaction::prompt::taken(writer.conversation().messages.len());
            crate::compaction::prompt::contract_prompt(writer.conversation(), episodes, enrollment)?
        }
        None => {
            let mut prompt = writer.conversation().messages.clone();
            // Off the compaction contract a ride renders into its result's copy here, before
            // anything else edits the results, where today's replay joins it as it reads the
            // ride's record. On the contract the entries render it.
            crate::entries::render_rides(&mut prompt, writer.conversation());
            repetition_guidance(
                &mut prompt,
                episodes,
                writer.conversation().window_start() - 1,
                writer.conversation().window_repetition.as_ref(),
            )?;
            // Each message keeps its place in the session, which the window starts after once
            // history is sealed: the window's first message has the 0-based position
            // `window_start - 1`.
            let first = writer.conversation().window_start() - 1;
            for (index, message) in prompt.iter_mut().enumerate() {
                message.ordinal = Some(first + index as u64);
            }
            // A session on the established layout numbers each run's messages by that run's
            // frozen rules, in the order they were placed, as today's loop numbers them, and
            // renders them in place.
            for (at, run) in run_rules.iter().enumerate() {
                let end = run_rules
                    .get(at + 1)
                    .map_or(prompt.len(), |next| next.start)
                    .min(prompt.len());
                let start = run.start.min(end);
                TagNumbering::for_run(&prompt[..start], run.rules, run.computed)
                    .number_new(&mut prompt[..end])
                    .map_err(overflow)?;
            }
            prompt
        }
    };
    // A session that enrolled sends none of the global system messages it carried before (its
    // stored prompt holds an empty placeholder for each, see
    // `compaction::prompt::contract_prompt`), and each run's place moves past them. Otherwise
    // the request shares the run's episode list rather than copying it, as a compacted run
    // shares the one its stored prompt keeps.
    let episodes = match (kept, contract, enrollment) {
        (Some(kept), _, _) => kept.request_episodes(episodes, enrollment),
        (None, true, Some(enrollment)) => {
            let mut translated = episodes.episodes.clone();
            crate::enroll::translate_episodes(&mut translated, enrollment);
            // The floor is an ordinal, which leaving suppressed messages out doesn't move.
            Arc::new(RenderHistory {
                signed_floor: episodes.signed_floor,
                ..RenderHistory::from(translated)
            })
        }
        _ => Arc::clone(episodes),
    };
    if !contract && defer_media_tags {
        // The provider's media renderer tags a message holding media itself, on its own copy of
        // the request, after preparing that message's media (see `defer_media_tags` above).
        let render = frozen_render_tags(admission);
        for message in prompt.iter_mut() {
            if !broca_session_store::session::message_media(message).is_empty() {
                continue;
            }
            if render {
                render_tags_into(
                    std::slice::from_mut(message),
                    TagRendering::for_render_config(&admission.render_config),
                );
            } else {
                clean_computed_into(std::slice::from_mut(message));
            }
        }
    } else if !contract && frozen_render_tags(admission) {
        // A run frozen with part numbers in the model's view sends user and tool-result text with
        // its numbers in front, as today's loop renders it.
        render_tags_into(
            &mut prompt,
            TagRendering::for_render_config(&admission.render_config),
        );
    } else if !contract {
        clean_computed_into(&mut prompt);
    }
    let mut opts = CallOptions::new(prompt);
    opts.marker_order_version = admission.marker_order_version;
    opts.render_episodes = episodes;
    opts.tools = Arc::clone(tools);
    opts.tool_choice = Arc::new(admission.tool_choice.clone());
    opts.response_format = Arc::new(admission.response_format.clone());
    opts.generation = Arc::new(admission.generation.clone());
    opts.provider_options = Arc::new(admission.provider_options.clone());
    opts.target_model = Some(admission.model.clone());
    opts.step_context = Some(StepContext {
        run_id: admission.run_id.clone(),
        step_index: step_id,
        session_address,
        credential_id: admission
            .auth_selection
            .as_ref()
            .map(|selection| selection.credential_id.clone()),
        account_ref: admission
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.account_ref.clone()),
        max_output_tokens: None,
    });
    // A run with a compaction provider renders its prompt from the provider's view
    // (`compaction::prepare`), which starts from these options.
    if !contract || compacted {
        return Ok(opts);
    }
    // On the current compaction contract a request renders from the entries, as today's loop
    // renders one: text delivered with a tool result is added to the result's copy, and the
    // system text goes above the conversation. This render copies the stored prompt rather than
    // rendering through a view's plan, so the placeholders the prompt holds for the global system
    // messages an enrollment suppressed (`compaction::prompt::contract_prompt`) come out of it.
    let conversation = writer.conversation();
    let entries = broca_session_store::session::EnrollmentOverlay::serve(
        enrollment,
        broca_session_store::session::window_entries(conversation, 1, usize::MAX),
    )
    .iter()
    .map(crate::entries::project)
    .collect::<Result<Vec<_>, _>>()
    .map_err(LoopError::Parts)?;
    let rides = crate::entries::ride_envelopes(conversation);
    let mut prompt = broca_core::compaction_v2::render::deliveries_only_with_rides(
        &opts.prompt,
        &entries,
        &rides,
    )
    .map_err(LoopError::Parts)?;
    if let Some(enrollment) = enrollment {
        crate::enroll::without_suppressed(&mut prompt, enrollment);
    }
    let opts = broca_core::compaction_v2::render::options(&opts, prompt);
    Ok(
        broca_core::compaction_v2::systems::view(&opts, &admission.global_system)
            .map_err(LoopError::Parts)?
            .unwrap_or(opts),
    )
}

/// Adds today's repetition guidance to the copies of tool results the request sends, never to
/// what's stored: each run's model steps (from its first answer up to the next run's input) are
/// folded with fresh counters, as today's loop counts within one run.
/// Adds each run's repetition warnings to its window messages. `prompt` holds the window, whose
/// first message has session position `first`, while each run's place is a session position. A
/// run that started before the window counts on from `spanning`, the counters the latest cut kept
/// for its sealed steps; without them none of its steps were sealed.
pub(crate) fn repetition_guidance(
    prompt: &mut [Message],
    episodes: &[EpisodeRenderInput],
    first: u64,
    spanning: Option<&serde_json::Value>,
) -> Result<(), LoopError> {
    let len = prompt.len();
    let index = |position: usize| position.saturating_sub(first as usize).min(len);
    for (at, episode) in episodes.iter().enumerate() {
        let start = index(episode.prompt_start);
        let end = index(
            episodes
                .get(at + 1)
                .map_or(usize::MAX, |next| next.prompt_start),
        );
        if start >= end {
            continue;
        }
        if episode.prompt_start < first as usize {
            if let Some(seed) = spanning {
                let seed = serde_json::from_value(seed.clone()).map_err(|error| {
                    LoopError::Parts(format!("stored repetition counters: {error}"))
                })?;
                if !broca_core::repetition::restore_repetition_guidance_seeded(
                    &mut prompt[start..end],
                    &seed,
                ) {
                    return Err(LoopError::Parts(
                        "stored repetition counters were taken under other warning rules".into(),
                    ));
                }
                continue;
            }
        }
        let first_step = (start..end)
            .find(|position| prompt[*position].role == broca_protocol::content::Role::Assistant);
        if let Some(first_step) = first_step {
            broca_core::repetition::restore_repetition_guidance(&mut prompt[first_step..end]);
        }
    }
    Ok(())
}

struct Step {
    message: AssistantMessage,
    usage: Usage,
    finish_reason: FinishReason,
    retries_used: u32,
    served: Served,
    /// Calls whose arguments the provider cut: complete JSON followed by runaway whitespace.
    /// They are answered with an error and never sent.
    cuts: Vec<broca_protocol::provider::ToolArgsTrailingGarbage>,
}

/// What the provider said about how it served a step, from its `StepFinished`, recorded exactly
/// as reported: absent stays absent, and nothing is filled from the request.
#[derive(Default)]
struct Served {
    /// The provider's own terminal token. A token longer than 64 bytes is left out rather than
    /// cut, as today's loop does.
    raw_finish_reason: Option<String>,
    service_tier_used: Option<String>,
    speed_used: Option<String>,
    /// The model that answered when the provider handed the request to another model itself.
    served_model: Option<String>,
    /// One usage entry per model that ran, when the provider reported them.
    usage_iterations: Option<Vec<broca_protocol::usage::UsageIteration>>,
}

impl Served {
    /// The step end's `fallback` members, as the converter carries today's record's remaining
    /// members: these facts and what the step records about the refusal fallback, in today's
    /// own `StepFallback` shape. `None` when there are none.
    fn members(&self, refusal: crate::refusal::Committed) -> Result<Option<Value>, LoopError> {
        let fallback = broca_wal::StepFallback {
            served_model: self.served_model.clone(),
            usage_iterations: self.usage_iterations.clone(),
            route: refusal.route,
            refusal_fallback: refusal.schedule,
            refusal_after_tool_use: refusal.after_tool_use,
        };
        let Value::Object(mut members) =
            serde_json::to_value(&fallback).map_err(|e| LoopError::Parts(e.to_string()))?
        else {
            return Err(LoopError::Parts(
                "step fallback facts are not an object".into(),
            ));
        };
        if let Some(reason) = &self.raw_finish_reason {
            members.insert("raw_finish_reason".into(), json!(reason));
        }
        Ok((!members.is_empty()).then_some(Value::Object(members)))
    }

    /// The step's usage split by the model each part counts under, by today's own rule
    /// (`StepServing::usage_by_model`), as `StepEnd::by_model` and `StepRefused::by_model`
    /// record it: `route_model` is the refusal fallback's model when the step was routed there.
    fn shares(
        &self,
        admission: &Admission,
        route_model: Option<&broca_protocol::request::ModelHandle>,
        usage: Usage,
    ) -> Vec<broca_session_store::record::ModelStepUsage> {
        let serving = broca_core::refusal_fallback::StepServing {
            route_model,
            served_model: self.served_model.as_deref(),
            usage,
            iterations: self.usage_iterations.as_deref(),
        };
        let variant = admission.variant.as_deref().filter(|v| !v.is_empty());
        serving
            .usage_by_model(&admission.model, variant)
            .into_iter()
            .map(
                |(provider, model, variant, usage)| broca_session_store::record::ModelStepUsage {
                    provider: provider.to_owned(),
                    model: model.to_owned(),
                    variant: variant.map(str::to_owned),
                    usage,
                },
            )
            .collect()
    }

    /// The step's whole usage: the sum over the models that ran when the provider reported
    /// iterations, else `usage`.
    fn total(&self, usage: Usage) -> Usage {
        broca_protocol::usage::step_usage(usage, self.usage_iterations.as_deref())
    }
}

/// The error a step fails with when the provider's stream ends without an answer and its final
/// `Finished`, as today's loop records it.
fn incomplete_stream() -> ProviderError {
    ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: broca_protocol::ErrorClass::ProviderUnavailable,
        message:
            "provider stream ended without an assembled assistant message and Finished control"
                .into(),
        status: None,
        retry_after_secs: None,
        provider_code: None,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

enum StepFailure {
    Cancelled,
    /// The step failed, after this many retries. `limited` is its last attempt the provider
    /// refused because the account's limit was reached, with its record's seq: the evidence a
    /// timed pause on the same account may rest on.
    Error(
        Box<ProviderError>,
        u32,
        Option<Box<FailedAccountLimitAttempt>>,
    ),
}

use broca_core::account_limit_pause::FailedAccountLimitAttempt;

/// Runs one model call and assembles its answer. Display events go straight to the listener.
/// Asks the provider for one step. Each HTTP attempt's evidence is appended as the attempt
/// ends, without a barrier, as today's loop records it: billing and cache evidence read it.
/// The outer error is the store's; the inner one is the step's.
async fn model_step<W: Writer, P: Provider, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    opts: CallOptions,
    requested_ttl_ms: Option<u64>,
    account_limit_pause: bool,
) -> Result<Result<Step, StepFailure>, LoopError> {
    let mut limited: Option<Box<FailedAccountLimitAttempt>> = None;
    // The step is in flight from here until the call's stream ends: published for the session's
    // owner, and cleared however the call ends, as today's loop publishes it.
    struct InFlight<'a>(Option<&'a broca_core::provider::LiveStepProgress>);
    impl Drop for InFlight<'_> {
        fn drop(&mut self) {
            if let Some(progress) = self.0 {
                *progress.lock().unwrap() = None;
            }
        }
    }
    let _in_flight = InFlight(seams.step_progress.as_ref());
    // A run's keep-warm may ask its calls to note each request their transport sends on its
    // seal, so a warm re-sends those bytes, as today's loop notes them on its shutdown seal.
    let send_seal = seams
        .keep_warm
        .as_ref()
        .and_then(|driver| driver.step_seal());
    if let Some(progress) = &seams.step_progress {
        let now = (seams.now_ms)();
        *progress.lock().unwrap() = Some(broca_protocol::provider::InFlightStep {
            step_id: StepId(step_id),
            started_at_ms: now,
            attempt: 0,
            attempt_started_at_ms: now,
            last_content_at_ms: None,
            max_content_gap_ms: None,
            max_content_gap_ended_at_ms: None,
        });
    }
    if let Some(ready) = &seams.model_call_ready {
        tokio::select! {
            ready = ready.ready() => {
                if let Err(error) = ready {
                    return Ok(Err(StepFailure::Error(Box::new(error), 0, None)));
                }
            }
            _ = seams.cancel.cancelled() => return Ok(Err(StepFailure::Cancelled)),
        }
    }
    let runtime = StepRuntime {
        cancel: seams.cancel.clone(),
        deadline: None,
        seal: send_seal.clone(),
        progress: seams.step_progress.clone(),
        first_attempt: 0,
        attempt_hook: None,
    };
    // A cancel drops the call while it hasn't returned its stream yet, as today's loop drops its
    // step future, so a call slow to answer never holds the run's end back.
    let mut events = tokio::select! {
        biased;
        _ = seams.cancel.cancelled() => return Ok(Err(StepFailure::Cancelled)),
        events = seams
            .provider
            .step_with_runtime(StepId(step_id), Arc::new(opts), runtime) => events,
    };
    let mut message = None;
    // The provider's `StepFinished` usage wins; its final `Finished` fills it only when the
    // step reported none, as today's loop takes them.
    let mut step_usage = Usage::default();
    let mut served = Served::default();
    let mut cuts = Vec::new();
    let outcome = loop {
        // Biased: the cancel is checked before each event, as broca-core's run loop checks it
        // before its step.
        let event = tokio::select! {
            biased;
            _ = seams.cancel.cancelled() => break Err(StepFailure::Cancelled),
            event = events.recv() => event,
        };
        match event {
            Some(NormalizedEvent::Display(display)) => {
                if let Some(out) = seams.out {
                    let _ = out.send(OutputEvent::Display(display)).await;
                }
            }
            Some(NormalizedEvent::Control(ProviderControl::AssistantMessage { message: m })) => {
                message = Some(m);
            }
            Some(NormalizedEvent::Control(ProviderControl::ToolArgsTrailingGarbage { cut })) => {
                cuts.push(cut);
            }
            Some(NormalizedEvent::Control(ProviderControl::StepFinished {
                usage,
                raw_finish_reason,
                service_tier_used,
                speed_used,
                served_model,
                usage_iterations,
                ..
            })) => {
                step_usage = usage;
                served = Served {
                    raw_finish_reason: raw_finish_reason.filter(|reason| reason.len() <= 64),
                    service_tier_used,
                    speed_used,
                    served_model,
                    usage_iterations,
                };
            }
            Some(NormalizedEvent::Control(ProviderControl::Finished {
                usage,
                finish_reason,
                retries_used,
            })) => {
                let usage = if step_usage == Usage::default() {
                    usage
                } else {
                    step_usage
                };
                let Some(message) = message.take() else {
                    break Err(StepFailure::Error(
                        Box::new(incomplete_stream()),
                        retries_used,
                        limited,
                    ));
                };
                // The request's measured input labels its send, which a warm's price reads, as
                // today's loop labels it once its step ends.
                if let Some(seal) = &send_seal {
                    seal.note_usage(StepId(step_id), usage);
                }
                break Ok(Step {
                    message,
                    usage,
                    finish_reason,
                    retries_used,
                    served,
                    cuts,
                });
            }
            Some(NormalizedEvent::Control(ProviderControl::Error {
                error,
                retries_used,
            })) => break Err(StepFailure::Error(Box::new(error), retries_used, limited)),
            Some(NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt })) => {
                limited = record_attempt(
                    writer,
                    seams,
                    run_id,
                    step_id,
                    attempt,
                    requested_ttl_ms,
                    account_limit_pause,
                )
                .await?;
            }
            None => {
                break Err(StepFailure::Error(
                    Box::new(incomplete_stream()),
                    0,
                    limited,
                ))
            }
            Some(NormalizedEvent::Control(_)) => {}
        }
    };
    // A step that failed while the run's cancel was tripped is a cancellation, as broca-core's
    // run loop attributes it (`attribute_step_failure_to_cancel`): the provider may see the
    // cancel and close its stream before this loop looks at the token again.
    let outcome = match outcome {
        Err(StepFailure::Error(..)) if seams.cancel.is_cancelled() => Err(StepFailure::Cancelled),
        outcome => outcome,
    };
    // A cancel stops the step cooperatively: a provider that reports attempt evidence records
    // its attempt in flight and then closes the stream. Its record is read until then, for at
    // most `CANCELLED_STEP_ATTEMPT_DRAIN`, so the attempt is recorded and reported before the
    // run ends, as broca-core's run loop does. Only attempt records are taken: nothing else of
    // a step that no longer counts is.
    if matches!(outcome, Err(StepFailure::Cancelled)) && seams.provider.reports_attempt_evidence() {
        let drained =
            tokio::time::timeout(broca_core::provider::CANCELLED_STEP_ATTEMPT_DRAIN, async {
                while let Some(event) = events.recv().await {
                    if let NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) =
                        event
                    {
                        record_attempt(
                            writer,
                            seams,
                            run_id,
                            step_id,
                            attempt,
                            requested_ttl_ms,
                            account_limit_pause,
                        )
                        .await?;
                    }
                }
                Ok::<(), LoopError>(())
            })
            .await;
        if let Ok(result) = drained {
            result?;
        }
    }
    Ok(outcome)
}

/// Records one attempt's evidence, around the hooks broca-core's run loop calls: keep-warm
/// learns its send time and rate-limit headers, its input transformations are reported, and the
/// provider adds its cache evidence before the record is written and learns which record holds
/// it after. A failed attempt is durable at once (it may have been billed and no step's end will
/// follow it); a completed one rides the step's end. Returns the attempt, with its record's seq,
/// when the provider refused it for the account's limit.
async fn record_attempt<W: Writer, P: Provider, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    mut attempt: broca_protocol::ModelAttempt,
    requested_ttl_ms: Option<u64>,
    account_limit_pause: bool,
) -> Result<Option<Box<FailedAccountLimitAttempt>>, LoopError> {
    if let Some(warm) = &seams.keep_warm {
        warm.note_attempt(&attempt);
    }
    broca_core::input_transformations::report_input_transformations(
        &seams.bind,
        &broca_protocol::RunId(run_id.into()),
        StepId(step_id),
        &attempt,
    );
    seams.provider.annotate_cache_evidence(&mut attempt);
    let durable = attempt.outcome != broca_protocol::AttemptOutcome::Complete;
    // The seq the record took, which a writer shared with the session actor knows only once it
    // is appended.
    let seq = writer
        .append(
            &[attempt_record(
                run_id,
                step_id,
                attempt.clone(),
                requested_ttl_ms,
                account_limit_pause,
            )?],
            durable,
        )
        .await?;
    seams
        .provider
        .cache_attempt_recorded(Some(event_id(writer, seq)));
    if let Some(reports) = &seams.account_reports {
        reports.attempt_recorded(&event_id(writer, seq), seq, &attempt);
    }
    Ok(attempt
        .account_observation
        .limit
        .is_some()
        .then(|| Box::new(FailedAccountLimitAttempt { seq, attempt })))
}

/// A call of a batch that already has an intent, from before the run paused or crashed: it is
/// sent with the key its intent recorded, and a call a provider holds is waited on again, never
/// sent again.
pub(crate) struct Known {
    pub id: ToolCallId,
    pub key: Option<String>,
    pub held: Option<ProviderHeldReply>,
    /// The input its intent recorded, when an approval policy narrowed it; sent instead of the
    /// model's.
    pub args: Option<serde_json::Value>,
}

/// How a tool batch ended.
enum BatchOutcome {
    /// Every call has its outcome; `unknown` when one may have run without its result coming
    /// back, which stops the run, because feeding that call's failure to the model would invite
    /// it to run the call again.
    Closed { unknown: bool },
    /// Only calls a provider holds were still waiting, so the run paused for their late
    /// results.
    Held(Finished),
    /// The caller cancelled while a provider held calls: they were closed and the run must end
    /// cancelled, then ask each provider to withdraw its call.
    Cancelled(Vec<ProviderHeldReply>),
}

/// Dispatches one assistant message's tool calls and records every result. `planned` is every
/// call the message asked for, which the plane is told of; `calls` are the ones still to be
/// answered, all of them unless the batch is resumed. `known` names those that already have
/// intents; every other call gets its intent here. Once the batch's results are durable, and
/// once a held call's result is, `changes` tells the compaction provider, when the run has it.
#[allow(clippy::too_many_arguments)]
async fn batch<W, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    // The scope the run is frozen to: its tool calls are routed under it.
    scope: Option<&broca_protocol::SessionScope>,
    step_id: u64,
    planned: &[ToolCall],
    calls: &[ToolCall],
    known: &[Known],
    manifest: &ToolManifest,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    started: bool,
    warming: Option<&Warming<'_>>,
    changes: Option<&crate::compaction::Changes<'_>>,
    // Whether the session is on the compaction contract, which picks a ride's carrier.
    contract: bool,
) -> Result<BatchOutcome, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    let batch_id = format!("{run_id}-b{step_id}");
    let ctx = BatchCtx {
        batch_id: BatchId(batch_id.clone()),
        bind: seams.bind.clone(),
        run_id: RunId(run_id.into()),
        scope: scope.cloned(),
        // A result keeps only media captured here for that exact call (`checked`).
        media_capture: writer.media(),
    };
    // The plane learns of the batch first, so it can say where each call goes; the intents then
    // record that, with each call's key, before anything is sent. A plane that refuses the batch
    // leaves every call unsent, with a terminal saying why.
    let guard = ToolOpenGuard::new(Some(&seams.cancel), Some(&seams.stop));
    let refusal = seams
        .tools
        .batch_started_guarded(&ctx, planned, manifest, &guard)
        .await
        .err();
    let sequential = calls.iter().any(|call| {
        manifest
            .get(&call.tool_name)
            .is_some_and(|tool| tool.execution_mode == ExecutionMode::Sequential)
    });
    let known_of = |call: &ToolCall| known.iter().find(|known| known.id == call.tool_call_id);
    // The policy may narrow a call's input, so the batch works from its own copy of the calls. A
    // call a resume already knows goes out with the input its intent recorded.
    let mut calls = calls.to_vec();
    for call in &mut calls {
        if let Some(args) = known_of(call).and_then(|known| known.args.clone()) {
            call.input = args;
        }
    }
    let plans = plan_calls(
        &mut calls,
        known,
        refusal.as_ref(),
        manifest,
        seams.approval,
        hooks,
        step_id,
    )
    .await?;
    let calls = &calls[..];
    let session = writer.session_id();
    let now = (seams.now_ms)();
    let mut prepared = Vec::with_capacity(calls.len());
    for (call, plan) in calls.iter().zip(&plans) {
        let intent = match plan {
            CallPlan::Dispatch(_) if known_of(call).is_none() => {
                // Arguments are stored again only when they differ from the input the message
                // last gave this call id, as today's converted records hold them.
                let last = planned
                    .iter()
                    .rev()
                    .find(|other| other.tool_call_id == call.tool_call_id)
                    .map(|other| &other.input);
                Some((
                    canonical(&call.input).map_err(LoopError::Parts)?.1,
                    (last != Some(&call.input)).then(|| call.input.clone()),
                    seams.tools.dispatch_target(&ctx, &call.tool_call_id),
                    seams
                        .tools
                        .dispatch_schema_pin(&ctx, &call.tool_call_id, &call.tool_name),
                ))
            }
            _ => None,
        };
        prepared.push(intent);
    }
    let dispatching = plans
        .iter()
        .any(|plan| matches!(plan, CallPlan::Dispatch(_)));
    // A batch that sends nothing closes in this barrier, and delivers queued sends with its
    // results there; one that sends reads them again when its answers are in.
    let ordered = if dispatching {
        Default::default()
    } else {
        Box::pin(ordered_sends(writer)).await?
    };
    let system_after = system_after_results(seams.provider);
    let written = writer
        .append_with(
            &mut |conversation, operations, first_seq| {
                let mut records = Vec::with_capacity(calls.len() + 1);
                if !started {
                    records.push(Record::BatchStart(BatchStart {
                        run_id: run_id.into(),
                        batch_id: batch_id.clone(),
                        step_id,
                        planned: planned
                            .iter()
                            .map(|call| call.tool_call_id.clone())
                            .collect(),
                        mid: None,
                    }));
                }
                for ((call, plan), intent) in calls.iter().zip(&plans).zip(&prepared) {
                    match (plan, intent) {
                        (
                            CallPlan::Dispatch(hooks),
                            Some((digest, args, module_id, schema_pin)),
                        ) => {
                            // The key names the intent record itself, so it never repeats in
                            // this session and a late result or withdrawal finds exactly this
                            // call.
                            let seq = first_seq + records.len() as u64;
                            records.push(Record::ToolIntent(ToolIntent {
                                run_id: run_id.into(),
                                batch_id: batch_id.clone(),
                                tool_call_id: call.tool_call_id.clone(),
                                tool_name: Some(call.tool_name.clone()),
                                module_id: module_id.clone(),
                                args_digest: *digest,
                                args: args.clone(),
                                call_key: Some(format!("{session}:{seq}")),
                                approval_digest: None,
                                dispatched_at_ms: Some(now),
                                schema_pin: schema_pin.as_ref().map(|pin| {
                                    serde_json::to_value(pin).expect("a pin serializes")
                                }),
                                hooks: hooks.clone(),
                                key_derived: None,
                            }));
                        }
                        (CallPlan::NoDispatch(reason, hooks), _) => {
                            no_dispatch(
                                &mut records,
                                run_id,
                                &batch_id,
                                &call.tool_call_id,
                                reason,
                                hooks.clone(),
                                now,
                            );
                        }
                        (
                            CallPlan::HookTerminal {
                                result,
                                reason,
                                hooks,
                            },
                            _,
                        ) => hook_terminal(
                            &mut records,
                            run_id,
                            &batch_id,
                            result.clone(),
                            reason.clone(),
                            hooks.clone(),
                            now,
                        ),
                        // A batch that sends nothing records its other terminals in the same
                        // barrier; one that sends records them with the answers.
                        (CallPlan::Synthetic { result, reason }, _) if !dispatching => {
                            terminal_with_reason(
                                &mut records,
                                run_id,
                                &batch_id,
                                result.clone(),
                                CallOutcome::Completed,
                                reason.clone(),
                                Some(now),
                            );
                        }
                        _ => {}
                    }
                }
                if !dispatching {
                    records.push(Record::BatchEnd(BatchEnd {
                        run_id: run_id.into(),
                        batch_id: batch_id.clone(),
                    }));
                    // Queued sends ride on the batch's results in the barrier that closes it,
                    // after its end, as today's loop records them.
                    let rides = tool_batch_deliveries(
                        conversation,
                        operations,
                        &records,
                        &ordered,
                        run_id,
                        contract,
                        system_after,
                        now,
                    );
                    records.extend(rides);
                    // The budget firings the step's calls made due go in the barrier that
                    // closes its batch, as today's loop writes them.
                    let placing = closing_places(conversation);
                    let firings = crate::budget::due(
                        conversation,
                        operations,
                        first_seq,
                        records.len(),
                        placing,
                    );
                    records.extend(firings);
                }
                records
            },
            true,
        )
        .await?;
    if !dispatching {
        let _ = seams.tools.batch_finished(&ctx).await;
        if let Some(changes) = changes {
            changes.tell(writer, seams).await;
        }
        return Ok(BatchOutcome::Closed { unknown: false });
    }
    // The calls that are sent, with their keys: a known call's from its intent, a new call's
    // from the intent just written.
    let sending: Vec<ToolCall> = calls
        .iter()
        .zip(&plans)
        .filter(|(_, plan)| matches!(plan, CallPlan::Dispatch(_)))
        .map(|(call, _)| call.clone())
        .collect();
    let keys: Vec<(ToolCallId, String)> = sending
        .iter()
        .filter_map(|call| {
            let key = match known_of(call) {
                Some(known) => known.key.clone(),
                None => written.iter().find_map(|record| match record {
                    Record::ToolIntent(intent) if intent.tool_call_id == call.tool_call_id => {
                        intent.call_key.clone()
                    }
                    _ => None,
                }),
            };
            key.map(|key| (call.tool_call_id.clone(), key))
        })
        .collect();
    let registrations: Vec<DispatchRegistration> = keys
        .iter()
        .map(|(tool_call_id, call_key)| DispatchRegistration {
            tool_call_id: tool_call_id.clone(),
            call_key: call_key.clone(),
            approved: false,
        })
        .collect();
    if !registrations.is_empty() {
        seams
            .tools
            .register_dispatch_group(&ctx, &registrations)
            .await
            .map_err(|error| LoopError::Tools(format!("late-results registration: {error}")))?;
    }

    let held: Vec<Option<&ProviderHeldReply>> = sending
        .iter()
        .map(|call| known_of(call).and_then(|known| known.held.as_ref()))
        .collect();
    let plan = DispatchPlan {
        ctx: &ctx,
        calls: &sending,
        keys: &keys,
        held: &held,
        guard: &guard,
        batch_id: &batch_id,
        run_id,
        sequential,
        hooks,
        step_id,
        changes,
    };
    // While the calls run, the run's keep-warm refreshes the prompt cache, as today's loop
    // does around the same wait. A wait resumed after a restart counts the warms it already
    // sent toward its spending cap.
    let spend = warming.map_or(0.0, |warming| warming.spent(writer.operations(), step_id));
    let dispatched = broca_core::keep_warm_wait::wait_tools_with_spend(
        dispatch_all(writer, seams, &plan),
        warming.map(|warming| warming.wait(&seams.cancel)),
        spend,
    )
    .await?;
    let (answers, paused, closed) = match dispatched {
        Dispatched::All { answers, closed } => (answers, false, closed),
        Dispatched::Held(answers) => (answers, true, Vec::new()),
    };
    // The answers, back in the message's order, with each synthesized terminal in its place.
    let mut answers = answers.into_iter();
    let mut records = Vec::new();
    let mut unknown = false;
    for plan in &plans {
        match plan {
            CallPlan::Dispatch(_) => {
                let Some(answer) = answers.next().flatten() else {
                    continue;
                };
                unknown |= answer.outcome == CallOutcome::Unknown;
                if !answer.durable {
                    let now = (seams.now_ms)();
                    terminal_with_reason(
                        &mut records,
                        run_id,
                        &batch_id,
                        answer.result,
                        answer.outcome,
                        answer.reason,
                        Some(now),
                    );
                    if let Some(Record::ToolOutcome(outcome)) = records.last_mut() {
                        outcome.hooks = answer.hooks;
                    }
                }
            }
            CallPlan::Synthetic { result, reason } => terminal_with_reason(
                &mut records,
                run_id,
                &batch_id,
                result.clone(),
                CallOutcome::Completed,
                reason.clone(),
                Some((seams.now_ms)()),
            ),
            CallPlan::NoDispatch(..) | CallPlan::HookTerminal { .. } => {}
        }
    }
    if paused {
        // The answers that came in are durable before the run pauses; the batch stays open.
        if !records.is_empty() {
            writer.append(&records, true).await?;
        }
        let finished = pause(writer, seams, run_id, PauseReason::ToolHeld, None, None).await?;
        return Ok(BatchOutcome::Held(finished));
    }
    // A batch that closed held calls isn't finished on the plane: the provider still holds them
    // until it's asked to withdraw them.
    if closed.is_empty() {
        let _ = seams.tools.batch_finished(&ctx).await;
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    // The budget firings the step's calls made due go in the barrier that closes its batch, as
    // today's loop writes them; a batch that closed held calls on a cancel makes none. Queued
    // sends ride on its results in the same barrier, after its end, read as it closes.
    let ordered = Box::pin(ordered_sends(writer)).await?;
    let closed_at = (seams.now_ms)();
    let mut records = Some(records);
    let cancelling = !closed.is_empty();
    writer
        .append_with(
            &mut |conversation, operations, first_seq| {
                let mut batch = records.take().expect("one batch");
                let rides = tool_batch_deliveries(
                    conversation,
                    operations,
                    &batch,
                    &ordered,
                    run_id,
                    contract,
                    system_after,
                    closed_at,
                );
                batch.extend(rides);
                if !cancelling {
                    let placing = closing_places(conversation);
                    let firings = crate::budget::due(
                        conversation,
                        operations,
                        first_seq,
                        batch.len(),
                        placing,
                    );
                    batch.extend(firings);
                }
                batch
            },
            true,
        )
        .await?;
    if let Some(changes) = changes {
        changes.tell(writer, seams).await;
    }
    if !closed.is_empty() {
        return Ok(BatchOutcome::Cancelled(closed));
    }
    Ok(BatchOutcome::Closed { unknown })
}

/// What a batch does with one call, decided before anything is written.
enum CallPlan {
    /// Sent to the tool plane; its intent records what the pre-tool hooks did, if any.
    Dispatch(Option<Value>),
    /// Never sent, durable at once: the plane can't be reached, the call is invalid, or the
    /// approval policy denied it. The outcome records what the pre-tool hooks did, if any.
    NoDispatch(NoDispatchReason, Option<Value>),
    /// Never sent because a pre-tool hook refused it, durable at once with the refusal's result
    /// and reason code, and what the hooks did.
    HookTerminal {
        result: ToolResult,
        reason: String,
        hooks: Option<Value>,
    },
    /// Never sent, answered by the loop with `result`: a repeated call id, or a refusal the
    /// plane named.
    Synthetic {
        result: ToolResult,
        reason: Option<String>,
    },
}

/// Decides each call's plan in the message's order, as today's gate does: a call a resume
/// already knows is sent as recorded; a repeated call id gets an error; a refused batch refuses
/// every call; a call the run's manifest doesn't allow is never sent. A valid call then goes
/// through the pre-tool mutate hooks, a second manifest check, the approval policy and the
/// validate hooks; its input in `calls` becomes what those left, so its intent records it.
#[allow(clippy::too_many_arguments)]
async fn plan_calls(
    calls: &mut [ToolCall],
    known: &[Known],
    refusal: Option<&broca_core::tool_plane::ToolPlaneError>,
    manifest: &ToolManifest,
    approval: &dyn ApprovalPolicy,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
) -> Result<Vec<CallPlan>, LoopError> {
    let mut seen = std::collections::HashSet::new();
    let mut plans = Vec::with_capacity(calls.len());
    for call in calls.iter_mut() {
        let first = seen.insert(call.tool_call_id.clone());
        let plan = match gate_call(call, first, known, refusal, manifest) {
            Some(plan) => plan,
            None => gate_hooked(call, manifest, approval, hooks, step_id).await?,
        };
        plans.push(plan);
    }
    Ok(plans)
}

/// A valid call through the pre-tool hooks and the approval policy.
async fn gate_hooked(
    call: &mut ToolCall,
    manifest: &ToolManifest,
    approval: &dyn ApprovalPolicy,
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
) -> Result<CallPlan, LoopError> {
    use broca_core::hooks::{Subject, Trace};
    use cortexkit_role_step_transform_provider::subscription::Phase;
    let original = call.input.clone();
    let mut trace = Trace::default();
    let metadata = |trace: &Trace, input: &Value, intentless: bool| {
        trace
            .metadata(original.clone(), input.clone(), intentless)
            .map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes"))
    };
    if let Some(runtime) = hooks {
        let output = runtime
            .chain(
                broca_core::tool_results::pre_tool_subject(StepId(step_id), call, Phase::Mutate),
                trace,
                |_| true,
            )
            .await;
        trace = output.trace;
        if output.cancellation.is_some() {
            return Err(LoopError::Unsupported(
                "a pre-tool hook cut short by a stop",
            ));
        }
        if let Subject::PreTool { input, .. } = output.subject {
            call.input = input;
        }
        if let Some((provider, text, unavailable)) = output.refusal {
            return Ok(hook_refused(
                call,
                &trace,
                &provider,
                &text,
                unavailable,
                &metadata,
            ));
        }
    }
    if let Err(invalid) = manifest.validate_call(call) {
        return Ok(CallPlan::NoDispatch(
            NoDispatchReason::ValidationFailed {
                detail: invalid.to_string(),
            },
            metadata(&trace, &call.input, true),
        ));
    }
    match approval.decide(call).await {
        ApprovalResponse::Allow { .. } => {}
        ApprovalResponse::AllowNarrowed { input, .. } => call.input = input,
        ApprovalResponse::Deny { reason, .. } => {
            return Ok(CallPlan::NoDispatch(
                NoDispatchReason::Denied { reason },
                metadata(&trace, &call.input, true),
            ));
        }
    }
    if let Some(runtime) = hooks {
        let output = runtime
            .chain(
                broca_core::tool_results::pre_tool_subject(StepId(step_id), call, Phase::Validate),
                trace,
                |_| true,
            )
            .await;
        trace = output.trace;
        if output.cancellation.is_some() {
            return Err(LoopError::Unsupported(
                "a pre-tool hook cut short by a stop",
            ));
        }
        if let Some((provider, text, unavailable)) = output.refusal {
            return Ok(hook_refused(
                call,
                &trace,
                &provider,
                &text,
                unavailable,
                &metadata,
            ));
        }
    }
    Ok(CallPlan::Dispatch(metadata(&trace, &call.input, false)))
}

/// The plan for a call a pre-tool hook refused: today's refusal result and reason code.
fn hook_refused(
    call: &ToolCall,
    trace: &broca_core::hooks::Trace,
    provider: &str,
    text: &str,
    unavailable: bool,
    metadata: &dyn Fn(&broca_core::hooks::Trace, &Value, bool) -> Option<Value>,
) -> CallPlan {
    let (result, reason) =
        broca_core::tool_results::pre_tool_refusal(call, provider, text, unavailable);
    CallPlan::HookTerminal {
        result,
        reason: reason.into(),
        hooks: metadata(trace, &call.input, true),
    }
}

/// The run's hook plan, from its frozen manifest. A plan that declares hooks without a plane to
/// call them refuses the run, as today's loop does; one whose hooks ask a person to approve a
/// call isn't supported yet.
fn hook_plan<P, T>(
    admission: &Admission,
    seams: &Seams<'_, P, T>,
) -> Result<Option<broca_core::hooks::HookPlan>, LoopError> {
    let Some(plan) = broca_core::fresh_start::admitted_hook_plan(
        admission.manifest.as_ref(),
        seams.hook_plane.is_some(),
    )
    .map_err(LoopError::Refused)?
    else {
        return Ok(None);
    };
    if plan.approves() {
        return Err(LoopError::Unsupported(
            "hooks that ask a person to approve a call",
        ));
    }
    Ok(Some(plan))
}

/// The checks that come before the approval policy. `None` means the call reaches the policy. A
/// call a resume already knows is sent as it was, without asking the policy again.
fn gate_call(
    call: &ToolCall,
    first: bool,
    known: &[Known],
    refusal: Option<&broca_core::tool_plane::ToolPlaneError>,
    manifest: &ToolManifest,
) -> Option<CallPlan> {
    use broca_core::tool_plane::ToolPlaneError;
    if known.iter().any(|known| known.id == call.tool_call_id) {
        return Some(CallPlan::Dispatch(None));
    }
    if !first {
        return Some(CallPlan::Synthetic {
            result: error_result(&call.tool_call_id, "duplicate tool_call_id from provider"),
            reason: None,
        });
    }
    match refusal {
        None => {}
        Some(ToolPlaneError::Permanent { code, detail }) => {
            return Some(CallPlan::Synthetic {
                result: ToolResult {
                    tool_call_id: call.tool_call_id.clone(),
                    output: broca_core::tool_plane::permanent_refusal_output(code, detail.as_ref()),
                    is_error: true,
                },
                reason: Some(code.clone()),
            });
        }
        Some(ToolPlaneError::Unavailable(_)) => {
            // The stable message, which sessions already on disk hold as their reason.
            return Some(CallPlan::NoDispatch(
                NoDispatchReason::Unavailable {
                    detail: broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE.into(),
                    module: None,
                    code: None,
                    refusal_detail: None,
                },
                None,
            ));
        }
        Some(ToolPlaneError::ModuleUnavailable {
            module,
            code,
            detail,
        }) => {
            return Some(CallPlan::NoDispatch(
                NoDispatchReason::Unavailable {
                    detail: broca_protocol::TOOL_PLANE_UNAVAILABLE_MESSAGE.into(),
                    module: Some(module.clone()),
                    code: Some(code.clone()),
                    refusal_detail: (!detail.is_empty()).then(|| detail.clone()),
                },
                None,
            ));
        }
        Some(error @ ToolPlaneError::ScopeRefused { .. }) => {
            return Some(CallPlan::NoDispatch(
                NoDispatchReason::Unavailable {
                    detail: error.to_string(),
                    module: None,
                    code: None,
                    refusal_detail: None,
                },
                None,
            ));
        }
        Some(ToolPlaneError::ScopeDrained { reason }) => {
            return Some(CallPlan::Synthetic {
                result: broca_core::tool_results::route_drained_result(&call.tool_call_id, reason),
                reason: Some(reason.clone()),
            });
        }
    }
    if let Err(invalid) = manifest.validate_call(call) {
        return Some(CallPlan::NoDispatch(
            NoDispatchReason::ValidationFailed {
                detail: invalid.to_string(),
            },
            None,
        ));
    }
    None
}

/// A call that was never sent: today's no-dispatch result in the transcript and its reason in
/// the ledger.
fn no_dispatch(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    id: &ToolCallId,
    reason: &NoDispatchReason,
    hooks: Option<Value>,
    now: u64,
) {
    let result = broca_core::tool_results::no_dispatch_result(id, reason);
    let digest = Digest(Sha256::digest(result.encode()).into());
    let is_error = result.is_error;
    records.push(Record::ToolReply(ToolReply {
        run_id: run_id.into(),
        batch_id: Some(batch_id.into()),
        result,
        finished_at_ms: Some(now),
        tags_v1: None,
        media: Vec::new(),
    }));
    records.push(Record::ToolOutcome(ToolOutcome {
        run_id: run_id.into(),
        batch_id: batch_id.into(),
        tool_call_id: id.clone(),
        outcome: CallOutcome::NotDispatched,
        result_digest: digest,
        is_error,
        reason: None,
        finished_at_ms: Some(now),
        detail: serde_json::to_value(reason).ok(),
        hooks,
        // Minted by the entry-facts writer, which knows the seq this record takes.
        call_key: None,
    }));
}

/// A call a pre-tool hook refused: its result in the transcript and its outcome, completed with
/// the refusal's reason code, in the ledger.
fn hook_terminal(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    result: ToolResult,
    reason: String,
    hooks: Option<Value>,
    now: u64,
) {
    terminal_with_reason(
        records,
        run_id,
        batch_id,
        result,
        CallOutcome::Completed,
        Some(reason),
        Some(now),
    );
    if let Some(Record::ToolOutcome(outcome)) = records.last_mut() {
        outcome.hooks = hooks;
    }
}

/// A call's reply in the transcript and its outcome in the ledger.
fn terminal(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    result: ToolResult,
    outcome: CallOutcome,
    now: u64,
) {
    terminal_with_reason(records, run_id, batch_id, result, outcome, None, Some(now));
}

/// `terminal`, with the reason code the outcome records, such as why a held call was closed.
/// A terminal today's loop writes without a time, such as a synthesized one, has none here.
fn terminal_with_reason(
    records: &mut Vec<Record>,
    run_id: &str,
    batch_id: &str,
    result: ToolResult,
    outcome: CallOutcome,
    reason: Option<String>,
    now: Option<u64>,
) {
    let digest = Digest(Sha256::digest(result.encode()).into());
    let tool_call_id = result.tool_call_id.clone();
    let is_error = result.is_error;
    records.push(Record::ToolReply(ToolReply {
        run_id: run_id.into(),
        batch_id: Some(batch_id.into()),
        result,
        finished_at_ms: now,
        tags_v1: None,
        media: Vec::new(),
    }));
    records.push(Record::ToolOutcome(ToolOutcome {
        run_id: run_id.into(),
        batch_id: batch_id.into(),
        tool_call_id,
        outcome,
        result_digest: digest,
        is_error,
        reason,
        finished_at_ms: now,
        detail: None,
        hooks: None,
        // Minted by the entry-facts writer for a call without a dispatch intent.
        call_key: None,
    }));
}

/// A stop condition held on a step that asked for tools. The calls are never dispatched: each
/// gets a result saying so, and the batch and the run's end are durable together, so a crash
/// either loses both (the resumed run takes the step again) or keeps both (the run is over).
/// The structured output a run froze, read from its frozen render config.
fn structured_of(
    admission: &Admission,
) -> Option<broca_protocol::structured_output::FrozenStructuredOutput> {
    broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(Some(
        &admission.render_config,
    ))
}

/// A run's keep-warm: today's scheduler warms the prompt cache while the run waits on tool
/// calls, through the driver the caller gave, for a run whose keep-warm is `auto`.
pub(crate) struct Warming<'a> {
    driver: &'a Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<Admission>>,
    admission: &'a Admission,
    run_id: broca_protocol::RunId,
    /// Whether the run already logged that its cache can't be warmed, so later waits don't
    /// repeat it.
    unsupported_logged: std::sync::atomic::AtomicBool,
    /// A warm that missed the cache stops warming for the rest of the run.
    warm_missed: std::sync::atomic::AtomicBool,
}

impl<'a> Warming<'a> {
    /// The run's keep-warm, if it asked for one and the caller gave a driver.
    pub(crate) fn of<P, T>(admission: &'a Admission, seams: &'a Seams<'_, P, T>) -> Option<Self> {
        if admission.keep_warm != Some(broca_protocol::request::KeepWarm::Auto) {
            return None;
        }
        Some(Self {
            driver: seams.keep_warm.as_ref()?,
            admission,
            run_id: broca_protocol::RunId(admission.run_id.clone()),
            unsupported_logged: Default::default(),
            warm_missed: Default::default(),
        })
    }

    fn wait<'w>(
        &'w self,
        cancel: &'w CancellationToken,
    ) -> broca_core::keep_warm_wait::WarmWait<'w, Admission> {
        broca_core::keep_warm_wait::WarmWait {
            driver: self.driver,
            config: self.admission,
            run_id: &self.run_id,
            cancel: Some(cancel),
            unsupported_logged: &self.unsupported_logged,
            warm_missed: &self.warm_missed,
        }
    }

    /// What the run's keep-warm already spent warming during the wait after step `step_id`.
    fn spent(
        &self,
        operations: &broca_session_store::fold::operations::Operations,
        step_id: u64,
    ) -> f64 {
        warm_spend(self.driver.as_ref(), self.admission, operations, step_id)
    }
}

/// What a run's keep-warm already spent warming during the wait after step `step_id`, priced as
/// today's loop prices it after a restart (`broca_core::keep_warm_wait::batch_spend`): a warm
/// that reached the cache at its recorded price, a failed one that was sent and reported usage
/// at its measured price, any other at nothing. A warm the driver can't price counts as
/// unaffordable, so the wait sends no more.
pub fn warm_spend<C: ?Sized + Sync>(
    driver: &dyn broca_core::keep_warm_wait::KeepWarmDriver<C>,
    config: &C,
    operations: &broca_session_store::fold::operations::Operations,
    step_id: u64,
) -> f64 {
    let Some(run) = operations.run.as_ref() else {
        return 0.0;
    };
    run.warms
        .iter()
        .filter(|warm| warm.step_id == step_id)
        .map(|warm| {
            let billing = |key: &str| warm.billing.as_ref().and_then(|billing| billing.get(key));
            let envelope: Option<broca_wal::record::AttemptEnvelope> = billing("envelope")
                .and_then(|envelope| serde_json::from_value(envelope.clone()).ok());
            if warm.reached_cache {
                return driver
                    .recorded_cost(config, &warm.usage)
                    .unwrap_or(f64::INFINITY);
            }
            match envelope {
                Some(envelope) if broca_core::cache_warm::warm_budget_needs_pricing(&envelope) => {
                    let reservation: Option<broca_wal::record::AttemptReservation> =
                        billing("reservation").and_then(|reservation| {
                            serde_json::from_value(reservation.clone()).ok()
                        });
                    match reservation {
                        Some(reservation) => driver
                            .failed_cost(config, &reservation, &envelope, warm.ttl_ms)
                            .unwrap_or(f64::INFINITY),
                        None => f64::INFINITY,
                    }
                }
                _ => 0.0,
            }
        })
        .sum()
}

/// The account the run uses: the one routing gives it now, else the one it froze at admission,
/// as today's loop chooses it.
fn run_selection<'a, P, T>(
    seams: &'a Seams<'_, P, T>,
    admission: &'a Admission,
) -> Option<&'a broca_wal::AuthSelection> {
    seams
        .auth_selection
        .as_ref()
        .or(admission.auth_selection.as_ref())
}

/// The step's request with its output cap reserved for the model it goes to: the run's own, or
/// the refusal-fallback model, with that model's window. `None` when even the provider's floor
/// doesn't fit: the run's end with the overflow error is then durable. `renders` is false when
/// the step's request can't be rendered, so the cap isn't estimated (see `output::reserve`).
async fn reserve_for<W: Writer, P: Provider, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    step_id: u64,
    prepared: &CallOptions,
    route: &crate::refusal::Route,
    renders: bool,
) -> Result<Option<CallOptions>, LoopError> {
    let mut opts = prepared.clone();
    let mut context_limit = admission.context_limit;
    if route.route == broca_core::refusal_fallback::StepRoute::Fallback {
        // A fallback step tells the renderer which model it renders for.
        opts.target_model = route.fallback.clone();
        context_limit = seams.provider.refusal_fallback_context_limit();
    }
    let model = route.model(&admission.model).clone();
    match crate::output::reserve(
        writer,
        seams.provider,
        step_id,
        opts,
        &model,
        context_limit,
        renders,
    )
    .await?
    {
        crate::output::Reserved::Send(opts) => Ok(Some(*opts)),
        crate::output::Reserved::Overflow(error) => {
            end_with_error(
                writer,
                seams,
                &admission.run_id,
                RunFinishReason::Error,
                &error,
            )
            .await?;
            Ok(None)
        }
    }
}

/// The timed same-account pause the run's state holds, if it is paused on one.
fn account_limit_of<W: Writer>(
    writer: &W,
) -> Result<Option<broca_wal::AccountLimitPause>, LoopError> {
    run_state(writer)
        .account_limit_pause
        .clone()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| LoopError::Parts(format!("account-limit pause: {e}")))
}

/// Ends the run as `reason` with `error` on its end, as today ends a run that can't go on.
async fn end_with_error<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    reason: RunFinishReason,
    error: &ProviderError,
) -> Result<Finished, LoopError> {
    end_run_with_error(writer, run_id, reason, error, (seams.now_ms)()).await
}

/// Ends the session's unfinished latest run `run_id` as `reason`, with `error` on its end and
/// `now_ms` as its time: the end the loop writes for a run that can't go on, durable before it
/// returns, with the messages steered or delivered since the run's last step boundary. For the
/// session's owner, when a run's loop stopped without writing an end.
pub async fn end_run_with_error<W: Writer>(
    writer: &mut W,
    run_id: &str,
    reason: RunFinishReason,
    error: &ProviderError,
    now_ms: u64,
) -> Result<Finished, LoopError> {
    end_run_with_error_and_interrupt_reason(writer, run_id, reason, error, now_ms, None).await
}

/// [`end_run_with_error`] with the recorded interruption reason the actor's seal selected, so
/// callers read the same cause whether the loop or its actor wrote the interrupted end.
pub async fn end_run_with_error_and_interrupt_reason<W: Writer>(
    writer: &mut W,
    run_id: &str,
    reason: RunFinishReason,
    error: &ProviderError,
    now_ms: u64,
    interrupt_reason: Option<String>,
) -> Result<Finished, LoopError> {
    let mut record = run_end(writer, run_id, reason, now_ms);
    if let Record::RunEnd(end) = &mut record {
        end.error = Some(serde_json::to_value(error).map_err(|e| LoopError::Parts(e.to_string()))?);
        end.interrupt_reason = interrupt_reason;
    }
    append_end(writer, run_id, vec![record]).await?;
    Ok(finished(writer, reason))
}

/// The timed pause on the run's own account that `limited`, the step's attempt the provider
/// refused for the account's limit, allows, as today's loop decides it: only for a transient
/// error, only when the run consented at admission, and only when the account resets before
/// the session's cache on it expires and within the boot cap.
fn account_limit_pause<P: Provider, T>(
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    step_id: u64,
    limited: &FailedAccountLimitAttempt,
    error: &ProviderError,
) -> Option<Box<broca_wal::AccountLimitPause>> {
    let selection = run_selection(seams, admission);
    let last_accepted = limited
        .attempt
        .served_account_ref
        .as_deref()
        .or_else(|| selection.and_then(|selection| selection.account_ref.as_deref()))
        .and_then(|account| seams.provider.last_accepted_at_ms(account));
    let budget = broca_core::account_limit_pause::PauseBudget {
        setting: if admission.account_limit_pause {
            seams.max_same_account_wait
        } else {
            broca_core::SameAccountWait::Minutes(0)
        },
        cache_expiry_ms: broca_core::session_account_cache_expiry_ms(
            last_accepted,
            &admission.render_config,
        ),
    };
    limited.pause(StepId(step_id), (seams.now_ms)(), error, selection, &budget)
}

/// The cache lifetime the run's requests ask the provider for, when they ask for one it
/// honours, as today's cache declaration reads it from the run's frozen render config. Only
/// this lifetime can prove the cache expired.
pub(crate) fn requested_cache_ttl(admission: &Admission) -> Option<u64> {
    let frozen: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(admission.render_config.clone()).ok()?;
    broca_provider::cache_warm::requested_ttl_ms(&admission.model.provider_module_id, &frozen)
}

/// How many messages a batch that closes the open tool turn places before anything after it:
/// the assistant message and its results.
fn closing_places(conversation: &broca_session_store::fold::conversation::Conversation) -> u64 {
    if conversation.open_tool_turn().is_some() {
        2
    } else {
        0
    }
}

/// What a resume finds of a submit the crash stopped before the run's end: the open turn's
/// step, its calls, the counted `submit_result` call, and whether the turn's batch already
/// started with the calls already answered. Today's loop writes a submit's result, batch and
/// end in one barrier, so a started batch here is a torn write of that barrier; its answered
/// calls keep their results, and a call that has one isn't counted as the submit.
struct PendingSubmit {
    step_id: u64,
    calls: Vec<ToolCall>,
    submit: ToolCall,
    started: bool,
    answered: Vec<ToolCallId>,
}

fn pending_submit<W: Writer>(
    writer: &W,
    frozen: &broca_protocol::structured_output::FrozenStructuredOutput,
) -> Option<PendingSubmit> {
    let run = run_state(writer);
    let open = writer.conversation().open_tool_turn()?;
    let message = broca_protocol::content::AssistantMessage {
        message_id: broca_protocol::MessageId(String::new()),
        content: open.content.clone(),
    };
    let calls: Vec<ToolCall> = message.tool_calls().cloned().collect();
    let answered: Vec<ToolCallId> = run.open_batch.as_ref().map_or_else(Vec::new, |batch| {
        calls
            .iter()
            .filter(|call| batch.state_of(&call.tool_call_id) == Some(CallState::Terminated))
            .map(|call| call.tool_call_id.clone())
            .collect()
    });
    let existing: Vec<ToolResult> = answered.iter().map(|id| error_result(id, "")).collect();
    let submit = broca_core::structured_output::counted_submit(frozen, &message, &existing)?;
    Some(PendingSubmit {
        step_id: run.completed_steps,
        calls,
        submit,
        started: run.open_batch.is_some(),
        answered,
    })
}

/// A structured-output result record for run `run_id`.
fn structured_result(
    run_id: &str,
    result: broca_protocol::structured_output::StructuredOutputResult,
) -> Record {
    Record::StructuredResult(broca_session_store::record::StructuredResult {
        run_id: run_id.into(),
        result: serde_json::to_value(result).expect("a structured result encodes"),
    })
}

/// A `submit_tool` run's step called `submit_result`. None of the step's calls is dispatched:
/// the counted submit gets the runner's own result, each other call a never-run
/// `skipped_after_submit` result, and the run's structured result is written with them and the
/// run's end, in one barrier, as today's loop writes them.
async fn submitted<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    frozen: &broca_protocol::structured_output::FrozenStructuredOutput,
    pending: &PendingSubmit,
) -> Result<Finished, LoopError> {
    let PendingSubmit {
        step_id,
        calls,
        submit,
        started,
        answered,
    } = pending;
    let step_id = *step_id;
    use broca_protocol::structured_output::{
        SkippedSibling, SKIPPED_AFTER_SUBMIT, SKIPPED_AFTER_SUBMIT_MESSAGE, SUBMIT_RESULT_RECORDED,
    };
    let skipped = calls
        .iter()
        .filter(|call| call.tool_call_id != submit.tool_call_id)
        .map(|call| SkippedSibling::new(call.tool_call_id.0.clone(), call.tool_name.clone()))
        .collect();
    let batch_id = format!("{run_id}-b{step_id}");
    let now = (seams.now_ms)();
    let mut records = vec![structured_result(
        run_id,
        broca_protocol::structured_output::result(frozen, true, Some(&submit.input), None, skipped),
    )];
    if !started {
        records.push(Record::BatchStart(BatchStart {
            run_id: run_id.into(),
            batch_id: batch_id.clone(),
            step_id,
            planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
            mid: None,
        }));
    }
    for call in calls {
        if answered.contains(&call.tool_call_id) {
            continue;
        }
        if call.tool_call_id == submit.tool_call_id {
            let result = ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text {
                    text: SUBMIT_RESULT_RECORDED.into(),
                },
                is_error: false,
            };
            terminal(
                &mut records,
                run_id,
                &batch_id,
                result,
                CallOutcome::Completed,
                now,
            );
        } else {
            terminal_with_reason(
                &mut records,
                run_id,
                &batch_id,
                error_result(&call.tool_call_id, SKIPPED_AFTER_SUBMIT_MESSAGE),
                CallOutcome::MaxSteps,
                Some(SKIPPED_AFTER_SUBMIT.into()),
                Some(now),
            );
        }
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    records.push(run_end(writer, run_id, RunFinishReason::Completed, now));
    append_end(writer, run_id, records).await?;
    Ok(finished(writer, RunFinishReason::Completed))
}

async fn hard_stop<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    stop: &StopCondition,
) -> Result<Finished, LoopError> {
    let (outcome, message, reason) = match stop {
        StopCondition::StepCount { .. } => (
            CallOutcome::MaxSteps,
            "tool not run: max steps reached before dispatch",
            RunFinishReason::MaxSteps,
        ),
        _ => (
            CallOutcome::Interrupted,
            "tool not run: interrupted before dispatch",
            RunFinishReason::Completed,
        ),
    };
    let batch_id = format!("{run_id}-b{step_id}");
    let mut records = vec![Record::BatchStart(BatchStart {
        run_id: run_id.into(),
        batch_id: batch_id.clone(),
        step_id,
        planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
        mid: None,
    })];
    let now = (seams.now_ms)();
    for call in calls {
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::error(message.to_string()),
            is_error: true,
        };
        terminal(&mut records, run_id, &batch_id, result, outcome, now);
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    // The budget firings the step's calls made due go in the same barrier, after the repaired
    // batch, as today's hard stop writes them.
    let end = run_end(writer, run_id, reason, now);
    let run_id = run_id.to_string();
    let mut records = Some(records);
    let mut end = Some(end);
    writer
        .append_with(
            &mut |conversation, operations, first_seq| {
                let mut batch = records.take().expect("one batch");
                let placing = closing_places(conversation);
                let firings =
                    crate::budget::due(conversation, operations, first_seq, batch.len(), placing);
                batch.extend(firings);
                batch.extend(placed_copies(operations, &run_id));
                batch.extend(end.take());
                batch
            },
            true,
        )
        .await?;
    Ok(finished(writer, reason))
}

/// The runner asked the run to stop, or the caller cancelled it, before its batch was
/// dispatched. No call is sent: each gets a result saying so, the tool plane is never told the
/// batch started, and the run ends cancelled (`cancelled`) or interrupted whatever its restart
/// policy, as today's loop does. The batch and the run's end are two barriers, as there.
async fn stopped_batch<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    calls: &[ToolCall],
    cancelled: bool,
) -> Result<Finished, LoopError> {
    let batch_id = format!("{run_id}-b{step_id}");
    let now = (seams.now_ms)();
    let mut records = vec![Record::BatchStart(BatchStart {
        run_id: run_id.into(),
        batch_id: batch_id.clone(),
        step_id,
        planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
        mid: None,
    })];
    let mut seen = std::collections::HashSet::new();
    for call in calls {
        // A repeated call id still gets today's duplicate error, not "not run".
        if !seen.insert(call.tool_call_id.clone()) {
            terminal_with_reason(
                &mut records,
                run_id,
                &batch_id,
                error_result(&call.tool_call_id, "duplicate tool_call_id from provider"),
                CallOutcome::Completed,
                None,
                Some(now),
            );
            continue;
        }
        let result = ToolResult {
            tool_call_id: call.tool_call_id.clone(),
            output: ToolOutput::error(NOT_DISPATCHED.to_string()),
            is_error: true,
        };
        terminal(
            &mut records,
            run_id,
            &batch_id,
            result,
            CallOutcome::Interrupted,
            now,
        );
    }
    records.push(Record::BatchEnd(BatchEnd {
        run_id: run_id.into(),
        batch_id,
    }));
    writer.append(&records, true).await?;
    // A caller's cancel ends the run cancelled; a stop ends it interrupted by the shutdown.
    let reason = if cancelled {
        RunFinishReason::Cancelled
    } else {
        RunFinishReason::Interrupted
    };
    let mut end = run_end(writer, run_id, reason, now);
    if let Record::RunEnd(end) = &mut end {
        if !cancelled {
            end.interrupt_reason = Some("runner_shutdown".into());
        }
    }
    append_end(writer, run_id, vec![end]).await?;
    Ok(finished(writer, reason))
}

/// A queued send with a delivery order, as a boundary delivers it.
struct OrderedSend {
    order: DeliveryOrder,
    /// The user messages a `steer` or `turn_system` delivery places: the input after the send's
    /// `pre_user` hooks, without system messages, as today's queue holds it.
    input: Vec<Message>,
    written_at_ms: Option<u64>,
    /// The send's id as a ride names it: the caller's `send_id`, else its submission id.
    send_id: String,
    /// Who a ride names as its sender: the module that sent it, else `broca`.
    ride_sender: String,
    /// What a ride's text is shown as having come from: its sender and the plan template it was
    /// rendered from.
    notice: Notice,
    /// Where the queued record stored the bytes of each medium the send's messages carry, by
    /// digest: a delivered copy of a message points at the same objects.
    media: std::collections::HashMap<String, broca_session_store::record::MediaObject>,
}

impl OrderedSend {
    /// Where each medium of `message`, a message this send delivers, is stored. Matched by
    /// digest, since the send's `pre_user` transforms may have rewritten the messages its
    /// queued record kept.
    fn media_of(&self, message: &Message) -> Vec<broca_session_store::record::InputMedia> {
        broca_session_store::session::message_media(message)
            .into_iter()
            .filter_map(|(block, part, reference)| {
                self.media.get(&reference.sha256).map(|object| {
                    broca_session_store::record::InputMedia {
                        block,
                        part,
                        log: object.log,
                        generation: object.generation,
                        seq: object.seq,
                    }
                })
            })
            .collect()
    }
}

/// The module a principal names, else `broca`, as today's ride envelope names a sender.
fn module_or_broca(principal: Option<&broca_protocol::BindPrincipal>) -> String {
    match principal {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => module_id.clone(),
        _ => "broca".into(),
    }
}

/// The queue's sends that carry a delivery order, read from their records, with what today's
/// loop reads about each: the order, the input, who sent it and the time it was accepted.
async fn ordered_sends<W: Writer>(
    writer: &mut W,
) -> Result<std::collections::BTreeMap<String, OrderedSend>, LoopError> {
    let ids: Vec<String> = writer
        .operations()
        .queue
        .iter()
        .filter(|pending| pending.ordered)
        .map(|pending| pending.submission_id.clone())
        .collect();
    let mut sends = std::collections::BTreeMap::new();
    for id in ids {
        let records = writer.send_records(&id).await?;
        let order = records.iter().find_map(|record| match record {
            Record::DeliveryOrdered(ordered) => Some(ordered.order.clone()),
            _ => None,
        });
        let queued = records.iter().find_map(|record| match record {
            Record::QueuedSend(queued) => Some(queued),
            _ => None,
        });
        let (Some(order), Some(queued)) = (order, queued) else {
            return Err(LoopError::Parts(format!(
                "queued send {id} is missing its order or its request"
            )));
        };
        let order: DeliveryOrder =
            serde_json::from_value(order).map_err(|e| LoopError::Parts(e.to_string()))?;
        let principal = |value: Option<&Value>| -> Result<_, LoopError> {
            value
                .map(|value| serde_json::from_value::<broca_protocol::BindPrincipal>(value.clone()))
                .transpose()
                .map_err(|e| LoopError::Parts(format!("queued send {id}'s sender: {e}")))
        };
        let ordered_by = principal(records.iter().find_map(|record| match record {
            Record::DeliveryOrdered(ordered) => ordered.sent_by.as_ref(),
            _ => None,
        }))?;
        let receipt = |name: &str| {
            queued
                .receipt
                .as_ref()
                .and_then(|receipt| receipt.get(name))
        };
        let queued_by = principal(receipt("sent_by"))?;
        let queued_as = principal(receipt("principal"))?;
        // A ride names the module its queued record says sent it, as today's ride ledger reads
        // it; the notice names the one its order says, as today's projection reads it.
        let ride_sender = module_or_broca(
            queued_by
                .as_ref()
                .or(ordered_by.as_ref())
                .or(queued_as.as_ref()),
        );
        let notice_sender = broca_core::compaction_v2::entries::delivery_sender(
            ordered_by
                .as_ref()
                .or(queued_by.as_ref())
                .or(queued_as.as_ref()),
            DeliveryMode::ToolResult,
        )
        .unwrap_or_else(|| "broca".into());
        let notice = Notice {
            sender: notice_sender,
            template: order.template.as_ref().map(|template| template.id.clone()),
        };
        let hooked = queued
            .receipt
            .as_ref()
            .and_then(|receipt| receipt.get("hooks"))
            .and_then(|hooks| hooks.get("final_value"))
            .cloned()
            .and_then(|value| serde_json::from_value::<Vec<Message>>(value).ok());
        let input = hooked
            .unwrap_or_else(|| queued.input.clone())
            .into_iter()
            .filter(|message| message.role != Role::System)
            .collect();
        let send_id = queued.send_id.clone().unwrap_or_else(|| id.clone());
        let media = broca_session_store::session::stored_media(&Record::QueuedSend(queued.clone()))
            .into_iter()
            .collect();
        sends.insert(
            id,
            OrderedSend {
                order,
                input,
                written_at_ms: queued.written_at_ms,
                send_id,
                ride_sender,
                notice,
                media,
            },
        );
    }
    Ok(sends)
}

/// The records that deliver queued sends at the close of a tool batch, as today's loop decides
/// them under its inbox lock when the batch commits: an expired send ends, and one whose first
/// possible mode here is `tool_result` rides on the batch's carrier result (`ResultAppend` with
/// its envelope, then `Delivery`), in queue order. Every other send waits for the next boundary.
/// `batch` is the closing batch so far, whose replies place the turn's results unless an earlier
/// batch placed them; `system_after_results` is whether the run's route would take a
/// turn-scoped system message right after them, which a send listing `turn_system` first waits
/// for. On the compaction contract the carrier is the batch's last result, whatever it holds;
/// off it, the last text or error result.
#[allow(clippy::too_many_arguments)]
fn tool_batch_deliveries(
    conversation: &broca_session_store::fold::conversation::Conversation,
    operations: &Operations,
    batch: &[Record],
    ordered: &std::collections::BTreeMap<String, OrderedSend>,
    run_id: &str,
    contract: bool,
    system_after_results: bool,
    now: u64,
) -> Vec<Record> {
    use broca_session_store::record::{RideEnvelope, RideForm};
    let pending: Vec<&OrderedSend> = operations
        .queue
        .iter()
        .filter(|pending| pending.ordered)
        .filter_map(|pending| ordered.get(&pending.submission_id))
        .collect();
    if pending.is_empty() {
        return Vec::new();
    }
    // The batch's results in planned order: the results message its replies place, or the one an
    // earlier batch placed when every reply was already durable.
    // A record the fold refuses here is refused again when the batch is appended, which
    // reports it; until then it places nothing.
    let mut placing = conversation.without_window();
    for record in batch
        .iter()
        .filter(|record| record.stream() == broca_session_store::record::Stream::Transcript)
    {
        let _ = placing.apply(record);
    }
    let results: Vec<&ToolResult> = placing
        .messages
        .last()
        .or(conversation.messages.last())
        .filter(|message| message.role == Role::Tool)
        .map(|message| {
            message
                .content
                .iter()
                .filter_map(|block| match block {
                    broca_protocol::content::ContentBlock::ToolResult(result) => Some(result),
                    _ => None,
                })
                .collect()
        })
        .unwrap_or_default();
    let carrier = if contract {
        broca_core::delivery_order::carrier_mode3(results.iter().copied())
    } else {
        broca_core::delivery_order::carrier(results.iter().copied())
    }
    .cloned();
    let boundary = Boundary {
        turn_system: system_after_results,
        ..Boundary::tool_batch(carrier.is_some())
    };
    let mut records = Vec::new();
    for pending in operations.queue.iter().filter(|pending| pending.ordered) {
        let Some(send) = ordered.get(&pending.submission_id) else {
            continue;
        };
        if send.order.expired_at(now) {
            records.push(Record::SendEnded(SendEnded {
                submission_id: pending.submission_id.clone(),
                reason: Some(json!({"kind": "expired"})),
            }));
            continue;
        }
        let text = appended_text(&send.input);
        let (Some(DeliveryMode::ToolResult), Some(carrier), Some(text)) = (
            choose_mode(&send.order, boundary, text.is_some()),
            &carrier,
            text,
        ) else {
            continue;
        };
        records.push(Record::ResultAppend(ResultAppend {
            run_id: run_id.into(),
            tool_call_id: carrier.clone(),
            text: text.clone(),
            // Who the text came from is an entry fact of the compaction contract, as today's
            // projection keeps it; off the contract the envelope alone names the sender.
            notice: contract.then(|| send.notice.clone()),
            written_at_ms: send.written_at_ms,
            send_id: Some(send.send_id.clone()),
            ride: Some(RideEnvelope {
                // The carrier's key is known once the batch's records are placed: the entry
                // facts writer fills it in from the call's intent, or from its outcome.
                call_key: String::new(),
                sender: send.ride_sender.clone(),
                forms: send
                    .order
                    .modes
                    .iter()
                    .filter_map(|mode| match mode {
                        DeliveryMode::ToolResult => Some("tool_result".to_owned()),
                        DeliveryMode::TurnSystem => Some("turn_system".to_owned()),
                        _ => None,
                    })
                    .collect(),
                form: RideForm::Append,
                // The agent the queued send's marks say its module spoke for, as today's
                // envelope takes it from the queued record.
                speaks_for: pending
                    .message_marks
                    .as_ref()
                    .and_then(|marks| marks.get("speaks_for"))
                    .cloned(),
                named_speaker: broca_session_store::record::marks_name_speaker(
                    pending.message_marks.as_ref(),
                ),
            }),
        }));
        records.push(Record::Delivery(Delivery {
            submission_id: pending.submission_id.clone(),
            run_id: run_id.into(),
            mode: "tool_result".into(),
            input: Vec::new(),
            tool_call_id: Some(carrier.clone()),
            appended_text: Some(text),
            written_at_ms: send.written_at_ms,
            tags_v1: None,
            input_listed: None,
        }));
    }
    records
}

/// Whether the run's route would take a turn-scoped system message right after a tool batch's
/// results, as today's loop asks its provider before a batch commits.
fn system_after_results<P: Provider>(provider: &P) -> bool {
    provider.turn_system_possible(Some(&Message::new(Role::Tool, Vec::new())), false)
}

/// The messages a `turn_system` delivery of `send` places: its text as turn-scoped system text,
/// frozen as today's loop freezes it (`broca_core::delivery_order::system_delivered`).
fn turn_system_input(send: &OrderedSend, submission_id: &str, run_id: &str) -> Vec<Message> {
    let core = broca_core::delivery_order::OrderedSend {
        admitted_at_ms: send.written_at_ms,
        submission_id: broca_protocol::SubmissionId(submission_id.into()),
        order: send.order.clone(),
        principal: None,
        input: send.input.clone(),
    };
    match broca_core::delivery_order::system_delivered(&core, &RunId(run_id.into())) {
        broca_wal::WalRecord::SendDelivered {
            input: Some(input), ..
        } => input.as_ref().clone(),
        _ => unreachable!("a turn-scoped system delivery records the messages it places"),
    }
}

/// The result a call gets when the run stopped before the call was dispatched.
const NOT_DISPATCHED: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// A held reply on its way to the batch's writer, with where to say it's durable.
type HeldRequest = (ProviderHeldReply, oneshot::Sender<Result<(), String>>);

/// The held-reply recorder the loop hands the plane. The plane's dispatch future runs while
/// the batch driver owns the writer, so the recorder doesn't write: it asks the driver to
/// append the held record durably and waits for the answer.
struct HeldChannel {
    requests: futures::channel::mpsc::UnboundedSender<HeldRequest>,
}

#[async_trait::async_trait]
impl ProviderHeldRecorder for HeldChannel {
    async fn sync_held(&self, reply: &ProviderHeldReply) -> Result<(), String> {
        let (answer, answered) = oneshot::channel();
        self.requests
            .unbounded_send((reply.clone(), answer))
            .map_err(|_| "the batch no longer takes held replies".to_string())?;
        answered
            .await
            .map_err(|_| "the batch stopped before the held reply was durable".to_string())?
    }
}

/// What a batch's dispatch works from.
struct DispatchPlan<'p> {
    ctx: &'p BatchCtx,
    calls: &'p [ToolCall],
    keys: &'p [(ToolCallId, String)],
    /// Per call, in order: the provider's held reply when the call was held before the run
    /// paused; that call waits for its late result again instead of being sent.
    held: &'p [Option<&'p ProviderHeldReply>],
    guard: &'p ToolOpenGuard,
    batch_id: &'p str,
    run_id: &'p str,
    sequential: bool,
    hooks: Option<&'p broca_core::hooks::HookRuntime<'p>>,
    step_id: u64,
    /// Tells the compaction provider once a held call's result is durable.
    changes: Option<&'p crate::compaction::Changes<'p>>,
}

/// What a call resolved to, before the batch takes it: the result and outcome, the code the
/// outcome records, and what its post-tool hooks did.
struct Resolved {
    result: ToolResult,
    outcome: CallOutcome,
    reason: Option<String>,
    hooks: Option<Value>,
}

impl From<(ToolResult, CallOutcome)> for Resolved {
    fn from((result, outcome): (ToolResult, CallOutcome)) -> Self {
        Self {
            result,
            outcome,
            reason: None,
            hooks: None,
        }
    }
}

/// One call's answer.
struct Answer {
    result: ToolResult,
    outcome: CallOutcome,
    /// The code its outcome records: a held call a cancel closed, or a result a post-tool hook
    /// withheld.
    reason: Option<String>,
    /// What its post-tool hooks did, recorded on its outcome.
    hooks: Option<Value>,
    /// Already appended: a held call's answer is durable as soon as it arrives.
    durable: bool,
}

/// How a batch's dispatch ended. Answers are in call order; `None` for a call still waiting.
enum Dispatched {
    /// Every call answered. `closed` are the held calls a cancel closed, whose providers are
    /// asked to withdraw them once the run's end is durable.
    All {
        answers: Vec<Option<Answer>>,
        closed: Vec<ProviderHeldReply>,
    },
    /// Only calls a provider holds were still waiting.
    Held(Vec<Option<Answer>>),
}

/// Dispatches every call, in parallel unless one of them must run alone, and collects their
/// answers, matching today's held-batch scheduling:
/// - each call carries its key and the batch's held-reply recorder; a held reply is appended,
///   durably, while the other calls keep running;
/// - a held call's answer is appended, durably, as soon as it arrives, before any call queued
///   behind it is sent, and the plane is then told it's durable;
/// - after a call whose outcome is unknown, no later call is sent: each gets the interrupted
///   result;
/// - when every call still running is held, the dispatch stops and the run pauses.
async fn dispatch_all<W: Writer, P, T: ToolPlane>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    plan: &DispatchPlan<'_>,
) -> Result<Dispatched, LoopError> {
    use futures::stream::FuturesUnordered;
    let calls = plan.calls;
    let (requests, mut held_requests) = futures::channel::mpsc::unbounded::<HeldRequest>();
    let recorder = HeldChannel { requests };
    let recorder = &recorder;
    let key_of = |call: &ToolCall| {
        plan.keys
            .iter()
            .find(|(id, _)| *id == call.tool_call_id)
            .map(|(_, key)| key.as_str())
            .expect("every dispatched call has an intent with a key")
    };
    let mut open_held: Vec<ToolCallId> = calls
        .iter()
        .zip(plan.held)
        .filter(|(_, held)| held.is_some())
        .map(|(call, _)| call.tool_call_id.clone())
        .collect();
    let mut answers: Vec<Option<Answer>> = calls.iter().map(|_| None).collect();
    let mut jobs = FuturesUnordered::new();
    let mut next = 0;
    let mut tripped = false;
    let mut closed: Vec<ProviderHeldReply> = Vec::new();
    loop {
        // A caller's cancel closes every call a provider holds: it gets the close result, durable
        // at once, and its provider is asked to withdraw it after the run's end. Calls still
        // running finish as usual.
        if seams.cancel.is_cancelled() && !open_held.is_empty() {
            for id in std::mem::take(&mut open_held) {
                let index = calls
                    .iter()
                    .position(|call| call.tool_call_id == id)
                    .expect("a held call is one of the batch's");
                let (result, outcome, code) = held_close_result(&id);
                let mut records = Vec::new();
                terminal_with_reason(
                    &mut records,
                    plan.run_id,
                    plan.batch_id,
                    result.clone(),
                    outcome,
                    Some(code.into()),
                    Some((seams.now_ms)()),
                );
                writer.append(&records, true).await?;
                if let Some(lifecycle) = seams.held {
                    lifecycle.terminal_synced(plan.ctx, &id).await;
                }
                closed.push(held_reply_of(writer, plan, &id));
                answers[index] = Some(Answer {
                    result,
                    outcome,
                    reason: Some(code.into()),
                    hooks: None,
                    durable: true,
                });
            }
        }
        while next < calls.len() && (!plan.sequential || jobs.is_empty()) {
            let index = next;
            next += 1;
            let call = &calls[index];
            let held = plan.held[index];
            let interrupted = tripped && held.is_none();
            let key = key_of(call);
            jobs.push(async move {
                let answer: Resolved = if interrupted {
                    (
                        interrupted_result(&call.tool_call_id),
                        CallOutcome::Interrupted,
                    )
                        .into()
                } else if let Some(reply) = held {
                    match seams.held {
                        Some(lifecycle) => resolve(
                            call,
                            checked(plan.ctx, lifecycle.wait(plan.ctx, call, reply).await),
                        )
                        .into(),
                        None => std::future::pending().await,
                    }
                } else {
                    dispatch_one(seams.tools, plan, call, key, recorder).await
                };
                (index, answer)
            });
        }
        if answers.iter().all(Option::is_some) {
            return Ok(Dispatched::All { answers, closed });
        }
        if !tripped && !open_held.is_empty() && jobs.len() == open_held.len() {
            // Only held calls wait. Anything already answered is taken first, as today's
            // loop does, then the run pauses.
            if let Some(Some((index, answer))) = futures::FutureExt::now_or_never(jobs.next()) {
                settle(
                    writer,
                    seams,
                    plan,
                    &mut open_held,
                    &mut answers,
                    &mut tripped,
                    index,
                    answer,
                )
                .await?;
                continue;
            }
            // A cancel that came in meanwhile closes the held calls instead of pausing on them.
            if seams.cancel.is_cancelled() {
                continue;
            }
            return Ok(Dispatched::Held(answers));
        }
        // While a provider holds a call, a cancel also wakes the dispatch, to close it.
        let hold_open = !open_held.is_empty();
        let cancel = async {
            if !hold_open {
                std::future::pending::<()>().await;
            }
            seams.cancel.cancelled().await;
        };
        let cancel = std::pin::pin!(cancel);
        let woke = futures::future::select(
            futures::future::select(jobs.next(), held_requests.next()),
            cancel,
        )
        .await;
        let futures::future::Either::Left((woke, _)) = woke else {
            continue;
        };
        match woke {
            futures::future::Either::Left((Some((index, answer)), _)) => {
                if answers[index].is_some() {
                    // A held call a cancel already closed: its late answer is ignored.
                    continue;
                }
                settle(
                    writer,
                    seams,
                    plan,
                    &mut open_held,
                    &mut answers,
                    &mut tripped,
                    index,
                    answer,
                )
                .await?;
            }
            futures::future::Either::Left((None, _)) => {
                unreachable!("jobs run until every call has an answer")
            }
            futures::future::Either::Right((Some((reply, answered)), _)) => {
                let record = Record::ToolHeld(ToolHeld {
                    batch_id: plan.batch_id.into(),
                    tool_call_id: reply.tool_call_id.clone(),
                    call_key: reply.call_key.clone(),
                    module: reply.module.clone(),
                    reason: reply.reason.clone(),
                    detail_digest: reply.detail_digest.clone(),
                    held_at_ms: (seams.now_ms)(),
                    sequential: plan.sequential,
                });
                let durable = writer
                    .append(&[record], true)
                    .await
                    .map(|_| ())
                    .map_err(|error| format!("{error:?}"));
                if durable.is_ok() {
                    open_held.push(reply.tool_call_id.clone());
                }
                let _ = answered.send(durable);
            }
            futures::future::Either::Right((None, _)) => {
                unreachable!("the recorder lives as long as the dispatches")
            }
        }
    }
}

/// Takes call `index`'s answer. A held call's answer is appended durably at once, and the plane
/// told so, before any call behind it is sent.
#[allow(clippy::too_many_arguments)]
async fn settle<W: Writer, P, T: ToolPlane>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    plan: &DispatchPlan<'_>,
    open_held: &mut Vec<ToolCallId>,
    answers: &mut [Option<Answer>],
    tripped: &mut bool,
    index: usize,
    resolved: Resolved,
) -> Result<(), LoopError> {
    let Resolved {
        result,
        outcome,
        reason,
        hooks,
    } = resolved;
    *tripped |= outcome == CallOutcome::Unknown;
    let id = plan.calls[index].tool_call_id.clone();
    let durable = open_held.contains(&id);
    if durable {
        let mut records = Vec::new();
        terminal(
            &mut records,
            plan.run_id,
            plan.batch_id,
            result.clone(),
            outcome,
            (seams.now_ms)(),
        );
        writer.append(&records, true).await?;
        open_held.retain(|held| *held != id);
        if let Some(changes) = plan.changes {
            changes.tell(writer, seams).await;
        }
        if let Some(lifecycle) = seams.held {
            lifecycle.terminal_synced(plan.ctx, &id).await;
        }
    }
    answers[index] = Some(Answer {
        result,
        outcome,
        reason,
        hooks,
        durable,
    });
    Ok(())
}

/// Asks each held call's provider to withdraw it, once the run's end is durable. Best effort:
/// a failure can't reopen the call, and a late answer for it is already ignored.
async fn withdraw<P, T>(seams: &Seams<'_, P, T>, closed: &[ProviderHeldReply]) {
    if let Some(lifecycle) = seams.held {
        for reply in closed {
            let _ = lifecycle.withdraw_held(reply).await;
        }
    }
}

/// The result, outcome and code a held call gets when a cancel closes it, as today's loop
/// closes one.
fn held_close_result(id: &ToolCallId) -> (ToolResult, CallOutcome, &'static str) {
    let (result, _, code) =
        broca_core::tool_results::provider_held_close_result(id, RunFinishReason::Cancelled);
    (result, CallOutcome::Interrupted, code)
}

/// The provider's held reply for the open held call `id`, as the run recorded it.
fn held_reply_of<W: Writer>(
    writer: &W,
    plan: &DispatchPlan<'_>,
    id: &ToolCallId,
) -> ProviderHeldReply {
    if let Some(reply) = plan
        .held
        .iter()
        .flatten()
        .find(|reply| reply.tool_call_id == *id)
    {
        return (*reply).clone();
    }
    let batch = run_state(writer)
        .open_batch
        .as_ref()
        .expect("a held call's batch is open");
    let position = (0..batch.calls.len())
        .find(|position| batch.id(*position) == id)
        .expect("a held call is planned");
    let held = batch.held_reply(position).expect("the call is held");
    ProviderHeldReply {
        tool_call_id: held.tool_call_id.clone(),
        call_key: held.call_key.clone(),
        module: held.module.clone(),
        reason: held.reason.clone(),
        detail_digest: held.detail_digest.clone(),
    }
}

/// The result a call gets when an earlier call of its batch may have run without an answer,
/// so it was never sent.
fn interrupted_result(id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: id.clone(),
        output: ToolOutput::error("tool not run: batch interrupted"),
        is_error: true,
    }
}

/// Sends one call with its key and the batch's held-reply recorder, and resolves the answer. A
/// result the provider returned goes through the post-tool hooks the plan subscribes for its
/// tool, before the batch takes it.
async fn dispatch_one<T: ToolPlane>(
    tools: &T,
    plan: &DispatchPlan<'_>,
    call: &ToolCall,
    call_key: &str,
    recorder: &HeldChannel,
) -> Resolved {
    let key = DispatchKey {
        call_key,
        held_recorder: Some(recorder),
    };
    match checked(
        plan.ctx,
        tools
            .dispatch_guarded(plan.ctx, call, Some(key), plan.guard)
            .await,
    ) {
        DispatchResult::Completed(result) => {
            post_tool(plan.hooks, plan.step_id, call, call_key, result).await
        }
        other => resolve(call, other).into(),
    }
}

/// A completed result keeps only the media the batch's capture holds for that exact call; every
/// other media part becomes a provenance marker, before hooks or the record see the result. A
/// reference to stored bytes, or one with the right digest and other metadata, isn't evidence
/// that this call produced them. Today's loop does the same in `resolve`.
fn checked(ctx: &BatchCtx, mut answer: DispatchResult) -> DispatchResult {
    if let DispatchResult::Completed(result) = &mut answer {
        let captured = ctx
            .media_capture
            .as_ref()
            .map(|capture| capture.captured_for(&ctx.batch_id, &result.tool_call_id))
            .unwrap_or_default();
        broca_core::tool_media::enforce_provenance(result, &captured);
    }
    answer
}

/// A model answer's text through the post-assistant hooks: each accepted edit is applied to the
/// answer's text parts in place. Returns what the hooks did, for the step's end record, or
/// `None` when they did nothing. The trace records the answer as the provider gave it.
async fn post_assistant(
    runtime: &broca_core::hooks::HookRuntime<'_>,
    step_id: u64,
    message: &mut broca_protocol::content::AssistantMessage,
) -> Option<Value> {
    use broca_protocol::content::ContentBlock;
    let original = serde_json::to_value(&*message).expect("an answer serializes");
    let subject = broca_core::hooks::Subject::PostAssistant {
        step_id: step_id.to_string(),
        blocks: message
            .content
            .iter()
            .filter_map(|part| match part {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    Some(text.clone())
                }
                _ => None,
            })
            .collect(),
    };
    let output = runtime
        .chain(subject, broca_core::hooks::Trace::default(), |_| true)
        .await;
    broca_core::hooks::apply_message_ops(&mut message.content, &output.operations)
        .expect("checked ops");
    output
        .trace
        .metadata(
            original,
            serde_json::to_value(&*message).expect("an answer serializes"),
            false,
        )
        .map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes"))
}

/// A provider's result through the post-tool hooks subscribed for its tool, as today's loop
/// runs them: a JSON result stays hookless, and a hook may edit the text or, when it was
/// required and unavailable, withhold the result.
async fn post_tool(
    hooks: Option<&broca_core::hooks::HookRuntime<'_>>,
    step_id: u64,
    call: &ToolCall,
    call_key: &str,
    result: ToolResult,
) -> Resolved {
    let subscribed = !matches!(result.output, ToolOutput::Json { .. })
        && hooks.is_some_and(|runtime| runtime.plan.post_tool(&call.tool_name));
    let Some(runtime) = hooks.filter(|_| subscribed) else {
        return (result, CallOutcome::Completed).into();
    };
    let output = runtime
        .chain(
            broca_core::hooks::Subject::PostTool {
                step_id: step_id.to_string(),
                tool: call.tool_name.clone(),
                tool_call_id: result.tool_call_id.0.clone(),
                call_key: Some(call_key.to_owned()),
                blocks: vec![broca_core::tool_results::post_tool_text(&result)],
                is_error: result.is_error,
            },
            broca_core::hooks::Trace::default(),
            |_| true,
        )
        .await;
    let (result, reason, hooks) =
        broca_core::tool_results::transformed_tool_result(&result, &output);
    Resolved {
        result,
        outcome: CallOutcome::Completed,
        reason,
        hooks: hooks.map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes")),
    }
}

/// A call's result and outcome from what the tool plane said:
/// - a result is the tool's own answer;
/// - never sent is safe: the model gets an error result and may try again;
/// - sent with no result may have had its effect: the model gets an error result, the outcome
///   is unknown, and the run stops.
fn resolve(call: &ToolCall, result: DispatchResult) -> (ToolResult, CallOutcome) {
    let error = |message: String| ToolResult {
        tool_call_id: call.tool_call_id.clone(),
        output: ToolOutput::error(message),
        is_error: true,
    };
    match result {
        DispatchResult::Completed(result) => (result, CallOutcome::Completed),
        DispatchResult::NotSent { reason } => (
            error(format!("tool not dispatched: {reason}")),
            CallOutcome::Completed,
        ),
        DispatchResult::SentButOutcomeUnknown { reason } => (
            error(format!(
                "tool outcome unknown (interrupted for review): {reason}"
            )),
            CallOutcome::Unknown,
        ),
        // A drained scope route ends the run differently; not handled by this build.
        DispatchResult::ScopeDrained { reason, .. } => (
            error(format!(
                "tool outcome unknown (interrupted for review): {reason}"
            )),
            CallOutcome::Unknown,
        ),
    }
}

/// The run's state as the operational fold holds it. Admission appended its start, so it
/// exists for the whole run.
/// Ends the run as today's loop does when its compaction provider can't serve it: an error
/// with code `compaction_unavailable`, before any model call of the step.
async fn compaction_unavailable<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    message: &str,
) -> Result<Finished, LoopError> {
    let mut record = run_end(
        writer,
        &admission.run_id,
        RunFinishReason::Error,
        (seams.now_ms)(),
    );
    if let Record::RunEnd(end) = &mut record {
        end.error = Some(
            serde_json::to_value(crate::compaction::unavailable_error(message))
                .map_err(|e| LoopError::Parts(e.to_string()))?,
        );
    }
    append_end(writer, &admission.run_id, vec![record]).await?;
    Ok(finished(writer, RunFinishReason::Error))
}

/// The compaction state and plane a run uses, when its session is on the current compaction
/// contract and its plan names a compaction provider. The state is rebuilt from the open log
/// the first time a session uses it, then kept by whoever owns the session, as today's actor
/// keeps it; a fresh run starts its step history again.
async fn compaction_ctx<W: Writer, P, T>(
    writer: &W,
    parts: &dyn Parts,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    fresh: bool,
) -> Result<Option<crate::compaction::Ctx>, LoopError> {
    if admission.compaction_plan_version.is_none() {
        return Ok(None);
    }
    let Some(manifest) = admission.manifest.as_deref() else {
        return Ok(None);
    };
    let Some((provider, preset, _, _)) =
        broca_core::compaction::item(manifest).map_err(LoopError::Parts)?
    else {
        return Ok(None);
    };
    let preset =
        preset.ok_or_else(|| LoopError::Parts("a compaction preset must be a string".into()))?;
    let session = seams.compaction_session.clone().unwrap_or_default();
    if !session.is_initialized() {
        let records = writer.open_log_records().await?;
        let state = crate::compaction::restore(&records, &seams.bind, writer.operations())
            .map_err(LoopError::Parts)?;
        session.restore(state).await;
    }
    session.claim_serving_start();
    if fresh {
        session.state.lock().await.begin_run();
    }
    let has_view = session.state.lock().await.applied().is_some();
    session.runtime.begin_send(has_view).await;
    Ok(Some(crate::compaction::Ctx {
        earlier: crate::compaction::earlier_context(writer, admission, parts).await?,
        session,
        provider,
        preset,
        window: Default::default(),
    }))
}

/// A batch's last record apart from the entry facts after it: what a boundary batch decided,
/// whether the run's end, a pause or the step's start.
fn decision(records: &[Record]) -> Option<&Record> {
    records
        .iter()
        .rev()
        .find(|record| !matches!(record, Record::EntryFacts(_)))
}

fn run_state<W: Writer>(writer: &W) -> &broca_session_store::fold::operations::Run {
    writer
        .operations()
        .run
        .as_ref()
        .expect("the run's start is appended before any step")
}

/// The run's end record. Its usage is the run's total, present once a step completed.
fn run_end<W: Writer>(writer: &W, run_id: &str, reason: RunFinishReason, now: u64) -> Record {
    let run = run_state(writer);
    // Today's end record always carries the run's usage, empty when it took no step.
    Record::RunEnd(RunEnd {
        run_id: run_id.into(),
        reason,
        usage: Some(run.usage),
        ts_ms: Some(now),
        retries_used: run.retries_used,
        interrupt_reason: None,
        error: None,
    })
}

/// How the run ended, as its end record states it.
fn finished<W: Writer>(writer: &W, reason: RunFinishReason) -> Finished {
    let run = run_state(writer);
    Finished {
        reason,
        usage: (run.completed_steps > 0).then_some(run.usage),
        retries_used: run.retries_used,
        paused: None,
    }
}

/// Pauses the run at a boundary, durably, before the run reports it. A caller's cancel wins
/// until the pause is durable: the run then ends cancelled instead, so nobody is left with a
/// paused run they already abandoned. Messages steered in stay unplaced, for the resume.
async fn pause<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    reason: PauseReason,
    error: Option<ProviderError>,
    account_limit: Option<Box<broca_wal::AccountLimitPause>>,
) -> Result<Finished, LoopError> {
    let error = error
        .map(|error| serde_json::to_value(error).map_err(|e| LoopError::Parts(e.to_string())))
        .transpose()?;
    let cancelled_now = seams.cancel.is_cancelled();
    let mut cancelled_end = Some(run_end(
        writer,
        run_id,
        RunFinishReason::Cancelled,
        (seams.now_ms)(),
    ));
    let mut pause = Some(Record::Pause(Pause {
        run_id: run_id.into(),
        reason,
        // An account-limit pause is dated by when it decided to pause, as today's is.
        ts_ms: Some(
            account_limit
                .as_ref()
                .map_or_else(|| (seams.now_ms)(), |pause| pause.paused_at_ms),
        ),
        error,
        account_limit: account_limit
            .map(|pause| serde_json::to_value(pause).map_err(|e| LoopError::Parts(e.to_string())))
            .transpose()?,
    }));
    let owned_run_id = run_id.to_string();
    let batch = writer
        .append_with(
            &mut |_, operations, _| {
                let run = operations.run.as_ref().expect("a run is active");
                if cancelled_now || run.cancel.is_some() {
                    let mut batch = placed_copies(operations, &owned_run_id);
                    batch.extend(cancelled_end.take());
                    batch
                } else {
                    pause.take().into_iter().collect()
                }
            },
            true,
        )
        .await?;
    if matches!(decision(&batch), Some(Record::RunEnd(_))) {
        return Ok(finished(writer, RunFinishReason::Cancelled));
    }
    let mut paused = finished(writer, RunFinishReason::Interrupted);
    paused.paused = Some(reason);
    Ok(paused)
}

/// Appends the run's end, durable before the run reports it.
async fn finish<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    reason: RunFinishReason,
) -> Result<Finished, LoopError> {
    let record = run_end(writer, run_id, reason, (seams.now_ms)());
    append_end(writer, run_id, vec![record]).await?;
    Ok(finished(writer, reason))
}

/// Appends `records`, which end with the run's end, as one durable batch. Messages steered or
/// delivered since the last boundary enter the conversation just before the end: the run ends
/// with them in its conversation, as today's loop leaves them.
async fn append_end<W: Writer>(
    writer: &mut W,
    run_id: &str,
    records: Vec<Record>,
) -> Result<(), LoopError> {
    let run_id = run_id.to_string();
    let mut records = Some(records);
    writer
        .append_with(
            &mut |_, operations, first_seq| {
                let mut batch = records.take().expect("one batch");
                let end = batch.pop().expect("the batch ends with the run's end");
                batch.extend(placed_copies(operations, &run_id));
                batch.push(end);
                name_resumed_from(&mut batch, first_seq);
                batch
            },
            true,
        )
        .await?;
    Ok(())
}

/// Names, in each `Resumed` record of a batch, the record just before the batch: the seq before
/// the batch's first. Only the append knows that seq, since a writer the session actor shares
/// (`ActorWriter`) can lag the records the actor appended since the run last saw the folds.
fn name_resumed_from(batch: &mut [Record], first_seq: u64) {
    for record in batch {
        if let Record::Resumed(resumed) = record {
            resumed.resumed_from_seq = first_seq.saturating_sub(1);
        }
    }
}

/// Copies of the messages steered or delivered since the last boundary, as transcript
/// entries. Each names the seq of the record that accepted it.
fn placed_copies(operations: &Operations, run_id: &str) -> Vec<Record> {
    let Some(run) = operations.run.as_ref() else {
        return Vec::new();
    };
    run.pending_steers
        .iter()
        .flat_map(|pending| {
            pending
                .messages
                .iter()
                .enumerate()
                .map(move |(index, message)| {
                    Record::Input(Input {
                        // The copy's media are stored where the accepting record stored them.
                        media: pending.media_at(index),
                        run_id: run_id.to_string(),
                        placement: pending.placement,
                        message: message.clone(),
                        written_at_ms: pending.written_at_ms,
                        submission_id: pending.submission_id.clone(),
                        notice: None,
                        tags_v1: pending.tags_at(index),
                        receipt_seq: pending.seq,
                        pruned: Vec::new(),
                    })
                })
        })
        .collect()
}

/// One HTTP attempt's record, from the provider's evidence. Billing and the folds read the typed
/// fields; every other field the provider measured is kept as diagnostics telemetry under the
/// name it has on the provider's evidence. Billing details are absent: no attempt in this loop
/// is billed through a reservation yet.
/// `requested_ttl_ms` is the cache lifetime the run's requests ask for, recorded on an attempt
/// that reached the cache.
/// `account_limit_pause` is the run's frozen consent to a timed same-account pause: with it, an
/// attempt the provider refused for the account's limit records that limit's disposition, as
/// today's `account_limit_pause::attempt_record` does.
fn attempt_record(
    run_id: &str,
    step_id: u64,
    attempt: ModelAttempt,
    requested_ttl_ms: Option<u64>,
    account_limit_pause: bool,
) -> Result<Record, LoopError> {
    let disposition = account_limit_pause
        .then(|| {
            attempt
                .account_observation
                .limit
                .as_ref()
                .map(|limit| limit.disposition)
        })
        .flatten();
    let invalid = |what: &str| LoopError::Parts(format!("attempt evidence: {what}"));
    let mut value = serde_json::to_value(&attempt).map_err(|e| invalid(&e.to_string()))?;
    let fields = value
        .as_object_mut()
        .ok_or_else(|| invalid("not an object"))?;
    let mut take = |key: &str| fields.remove(key);
    let digest = |value: Option<Value>, key: &str| -> Result<Digest, LoopError> {
        let text = value.as_ref().and_then(Value::as_str).unwrap_or_default();
        hex_digest(text).ok_or_else(|| invalid(&format!("{key} is not a SHA-256 hex digest")))
    };
    let outcome = take("outcome")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| invalid("outcome"))?;
    take("attempt");
    take("request_body_len");
    take("response_body_len");
    take("http_status");
    take("ts_ms");
    take("sent_at_ms");
    take("first_content_at_ms");
    take("last_content_at_ms");
    take("transport");
    take("served_account_ref");
    let reached =
        broca_core::cache_warm::attempt_reached_the_cache(attempt.http_status, &attempt.outcome);
    let request_body_sha256 = digest(take("request_body_sha256"), "request_body_sha256")?;
    let response_body_sha256 = digest(take("response_body_sha256"), "response_body_sha256")?;
    let telemetry = (!fields.is_empty()).then(|| Value::Object(std::mem::take(fields)));
    Ok(Record::Attempt(Attempt {
        run_id: run_id.into(),
        step_id,
        attempt: attempt.attempt,
        attempt_id: Some(
            broca_core::attempt_id::mint_attempt_id()
                .map_err(|e| invalid(&format!("attempt identity: {e}")))?
                .as_str()
                .to_owned(),
        ),
        outcome,
        http_status: attempt.http_status.map(u32::from),
        request_body_len: attempt.request_body_len,
        request_body_sha256,
        response_body_len: attempt.response_body_len,
        response_body_sha256,
        ts_ms: attempt.ts_ms,
        sent_at_ms: attempt.sent_at_ms,
        first_content_at_ms: attempt.content_progress.first_content_at_ms,
        last_content_at_ms: attempt.content_progress.last_content_at_ms,
        transport: attempt.transport.clone(),
        served_account_ref: attempt.served_account_ref.clone(),
        billing: disposition
            .map(|disposition| {
                serde_json::to_value(disposition)
                    .map(|disposition| json!({ "account_limit_disposition": disposition }))
            })
            .transpose()
            .map_err(|e| invalid(&e.to_string()))?,
        telemetry,
        reached_cache: reached.then_some(true),
        requested_cache_ttl_ms: requested_ttl_ms.filter(|_| reached),
    }))
}

/// The digest a 64-character hex string spells, or `None`.
fn hex_digest(text: &str) -> Option<Digest> {
    if text.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (index, pair) in text.as_bytes().chunks(2).enumerate() {
        out[index] = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(Digest(out))
}
