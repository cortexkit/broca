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
use broca_records::record::{ComputedTagRules, FrozenProviderDeclarations, NoDispatchReason};
use broca_session_store::codec::{Digest, Message as _};
use broca_session_store::fold::operations::{CallState, Operations, Resolution};
use broca_session_store::record::{
    Assistant, BatchEnd, BatchStart, CallOutcome, CeilingDecided, Delivery, Input, Notice,
    PartKind, PartRef, Pause, Placement, Record, Redo, ResultAppend, Resumed, RunEnd, RunStart,
    SendEnded, SetupStub, StepEnd, StepStart, ToolHeld, ToolIntent, ToolOutcome, ToolReply,
};
use broca_session_store::session::SessionError;
use futures::channel::oneshot;
use futures::StreamExt as _;
use serde_json::{json, Value};
use sha2::{Digest as _, Sha256};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::entries::{Placed, Tags};
use crate::parts::{canonical, Parts};
use crate::writer::Writer;

mod attempt;
use attempt::{attempt_record, AttemptContext, RequestFacts};
mod display;
use display::restream;
mod warming;
pub(crate) use warming::Warming;
pub use warming::{warm_count, warm_spend};
mod close_out;
use close_out::{held_close_result, never_dispatched_close_out, open_batch_close_out};
mod call_plan;
use call_plan::{hook_plan, plan_calls, CallPlan};
mod dispatch;
use dispatch::{dispatch_all, DispatchPlan, Dispatched};
mod pause;
use pause::{pause, sign_in_at_start, sign_in_refused, take_deposited};
mod plan_notes;
use plan_notes::{removed_tools, with_plan_notes, with_plan_system};
mod resolved;
use resolved::{resolve, Resolved};
mod stops;
use stops::{hard_stop, stop_before_batch, stopped_batch, stopped_batch_in};
mod send_facts;
pub use send_facts::SendFacts;
mod setup;
use setup::{store_setup, Setup};
mod start_config;
pub use start_config::{
    frozen_marker_order, run_config_members, run_config_of_run, run_config_of_start,
    run_frozen_facts, ConfigMembers, FrozenFacts, SetupParts,
};

/// The cache-marker placement a session's first run freezes (see
/// `broca_provider::cache_markers::plan_markers` and `admit::marker_order_for_next_run`).
pub const MARKER_ORDER_VERSION: u32 = 2;

/// What admission decided for a run. The loop freezes all of it into the run's start.
#[derive(Debug, Clone)]
pub struct Admission {
    pub run_id: String,
    pub model: ModelHandle,
    /// The messages this run adds to the conversation.
    pub input: Vec<Message>,
    /// The tools the run may call, in the order the model sees them. Shared with every other
    /// holder of the same setup part (see `crate::parts::shared`).
    pub tools: Arc<Vec<Tool>>,
    pub tool_choice: ToolChoice,
    pub generation: GenerationConfig,
    pub response_format: Arc<ResponseFormat>,
    pub provider_options: Value,
    /// The provider's frozen render config, opaque to the loop. Shared like `tools`: a run that
    /// changes its own copies it first.
    pub render_config: Arc<Value>,
    pub renderer_version: u32,
    /// Termination predicates, OR'd. Empty means no limit: the run goes on until the model
    /// stops, and broca never caps a run's steps on its own.
    pub stop_when: Vec<StopCondition>,
    /// What the run does when the runner stops while it runs: end interrupted (the default),
    /// or pause and continue as the same run after the runner starts again.
    pub on_restart: Option<OnRestart>,
    /// When the run was admitted and when its input was written.
    pub ts_ms: Option<u64>,
    pub written_at_ms: Option<u64>,
    /// The session's frozen plan manifest, for a session admitted with a fetch plan: the tools
    /// its providers answered with, its system text and the hooks it declares.
    pub manifest: Option<std::sync::Arc<broca_records::record::PlanManifest>>,
    /// Transport-only differences from the plan, frozen on this run rather than in shared
    /// setup parts. A resume reads these without consulting the provider again.
    pub transport: std::collections::BTreeMap<String, FrozenProviderDeclarations>,
    /// For a session on the current compaction contract, the system text every request renders
    /// above the conversation, which is never part of its transcript: the plan's system text for
    /// a planned run, otherwise the system messages the caller sent, or the previous run's when
    /// it sent none. [`run`] takes it out of `input`; a caller leaves it empty. Shared like
    /// `tools`.
    pub global_system: Arc<Vec<Message>>,
    /// The window the run's model may use, in tokens; compaction states its conditions from it,
    /// and a run on a plan with a compaction provider needs one.
    pub context_limit: Option<u64>,
    /// For a session's first run, this is the compaction plan version to start under, so it
    /// records and renders under the current compaction contract. `None` keeps the established
    /// layout. A later run keeps its session's layout regardless of this value.
    pub compaction_plan_version: Option<u32>,
    /// Whether the run froze part numbers into the model's view. A caller leaves it false: a new
    /// run takes it from its plan. A run read back from its start carries what the start froze.
    pub render_tags: bool,
    /// The tool-call budget the run freezes: reminders after so many tool calls, and the
    /// finalize reminder after which the run's last step has tools disabled. A budget with an
    /// input-token cap is refused for now.
    pub budget: Option<broca_protocol::request::BudgetConfig>,
    /// Whether the run keeps the prompt cache warm while it waits on tool calls. Absent is
    /// off.
    pub keep_warm: Option<broca_protocol::request::KeepWarm>,
    /// The provider account the run was admitted to, frozen so a resume uses the same one.
    pub auth_selection: Option<broca_records::record::AuthSelection>,
    /// Whether the run may pause on its own account when the provider says the account's limit
    /// is reached and resets soon, rather than end. Admission freezes this from the boot
    /// setting.
    pub account_limit_pause: bool,
    /// The routing decision the run was admitted under.
    pub route_decision_id: Option<String>,
    /// The provider and model the send asked routing to choose an account for, which a joining
    /// send must not change. Stored in the run's config members.
    pub account_route: Option<cortexkit_role_llm_runner::send::AccountRoute>,
    /// The catalog provider whose rows the run borrowed, for a model whose provider the catalog
    /// doesn't carry (`RunConfig::served_provider`). Stored in the run's config members, so a
    /// resume and the run's spend read it back and never resolve it again.
    pub served_provider: Option<String>,
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
    /// takes `admit::marker_order_for_next_run` of the order the session's previous run froze;
    /// a run read back from its start takes what the start froze.
    pub marker_order_version: Option<u32>,
    /// What the send said about itself. None of it changes a request; all of it is recorded
    /// on the run's start, where status, retries and reads find it.
    pub send: SendFacts,
    /// Set when something decided before the run that it must not reach the model: a
    /// `pre_user` input transform refused the run's input, or routing refused the caller's
    /// decision. The run's start is still written, so the send has a run to report, and the run
    /// then ends with this error before any step. Unlike every other
    /// field it isn't frozen into the start, and a run read back from its start has none.
    pub pre_model_error: Option<ProviderError>,
    /// The pending plan change this run applies by freezing its manifest, recorded after the
    /// run's input in the batch of its start. Like `pre_model_error`, a run read back from its
    /// start has none: the record that followed the start keeps it.
    pub applies_refresh: Option<broca_protocol::session_change::AppliedChange>,
    /// The notes the run delivers about a change pending under `notes_until_rebuild`; like
    /// `applies_refresh`, recorded in its start's batch and absent on a run read back.
    pub plan_notes: Option<broca_protocol::session_change::NotesDelivery>,
    /// Whether the run starts at a prefix rebuild of its session or applies a pending plan
    /// change, where it takes a versioned tool's current same-major definition (see
    /// `crate::plan::take_compatible_tools`). Not frozen: a run read back from its start reads
    /// false, and keeps the tools its start recorded.
    pub rebuild_point: bool,
    /// Whether the run is a head's turn (`RunRequest::head_turn`), which a continue request
    /// at a usage wall or sign-in loss tells routing. Not frozen: a run read back from its
    /// start reads false.
    pub head_turn: bool,
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
    /// call is never answered, and its run pauses.
    pub held: Option<&'a dyn ProviderHeldLifecycle>,
    /// Decides each valid call before it is sent: allow it, narrow its input, or deny it.
    pub approval: &'a dyn ApprovalPolicy,
    /// Calls the hooks a frozen plan declares. A run whose plan declares hooks refuses to start
    /// or resume without it.
    pub hook_plane: Option<Arc<dyn broca_core::hooks::HookPlane>>,
    /// Milliseconds since the epoch, for the times records carry.
    pub now_ms: &'a (dyn Fn() -> u64 + Sync),
    /// The session's address, given to the provider for its diagnostics only.
    pub session_address: Option<String>,
    /// The compaction provider's calls, for a run whose plan names one. Without it every call
    /// fails as unavailable.
    pub compaction_plane: Option<&'a dyn broca_core::CompactionPlane>,
    /// The session's compaction state, kept for the session's life by whoever owns the session.
    /// Without it a run starts from the session's log.
    pub compaction_session: Option<Arc<broca_core::compaction_v2::session::Session>>,
    /// Whether the run tells the session's compaction provider about the session's changes
    /// outside its own steps, at the points controlled by
    /// `RunControl::compaction_changes` (see [`crate::compaction::Changes`]). Without it the
    /// provider hears of them at the next step's notice.
    pub compaction_changes: bool,
    /// Sends the warms that keep the prompt cache alive while a run whose keep-warm is `auto`
    /// waits on tool calls. Without it no run warms.
    pub keep_warm: Option<Arc<dyn broca_core::keep_warm_wait::KeepWarmDriver<Admission>>>,
    /// Receives what the run saw about its provider account, for account routing: the run's
    /// start once it is durable, and each provider attempt as its record is appended.
    pub account_reports: Option<Arc<dyn AccountReports>>,
    /// The boot setting that bounds how long a run may wait on its own account after the
    /// provider says the account's limit is reached.
    pub max_same_account_wait: broca_core::SameAccountWait,
    /// The account routing gives the session now. A run paused on its account resumes only
    /// on the account it froze; `None` when the caller didn't route one.
    pub auth_selection: Option<broca_records::record::AuthSelection>,
    /// Where the run publishes its in-flight model step while the provider call runs: the
    /// session's owner serves it as an active run's `step`. It is cleared when the step's call
    /// ends. Never a durability input.
    pub step_progress: Option<broca_core::provider::LiveStepProgress>,
    /// What each model call waits for before the provider is asked to render and send it, such
    /// as the media its requests are rendered with while they are still being prepared. Each
    /// step waits for it before anything renders the step's request, its estimates included. A
    /// cancel during the wait ends the run cancelled, as a cancel during the call does.
    pub model_call_ready: Option<Arc<dyn ModelCallReady>>,
    /// The credential source requires sign-in before another model call. A held resume first
    /// syncs the results already supplied by its tool provider, then records the auth pause.
    pub pause_for_auth_at_start: Option<ProviderError>,
    /// Asks for another routed account at a usage wall or definite sign-in loss (see
    /// [`crate::account_move`]). Without it the run ends or pauses as before.
    pub account_continue: Option<&'a dyn crate::account_move::AccountContinue<P>>,
    /// The person's steers the session's owner counts while the run runs. A call waiting on a
    /// provider that can hand calls back (llm-runner `call_yield`) is asked to on each one.
    /// Without it no call is handed back for a steer.
    pub person_steers: Option<broca_core::tool_plane::PersonSteers>,
}

impl<'a, P, T> Seams<'a, P, T> {
    /// These seams, with the run moved to another account: `provider` sends on it and
    /// `auth_selection` names it.
    fn moved_to<'b>(
        &'b self,
        provider: &'b P,
        auth_selection: broca_records::record::AuthSelection,
    ) -> Seams<'b, P, T> {
        Seams {
            person_steers: self.person_steers.clone(),
            provider,
            tools: self.tools,
            bind: self.bind.clone(),
            out: self.out,
            cancel: self.cancel.clone(),
            stop: self.stop.clone(),
            held: self.held,
            approval: self.approval,
            hook_plane: self.hook_plane.clone(),
            now_ms: self.now_ms,
            session_address: self.session_address.clone(),
            compaction_plane: self.compaction_plane,
            compaction_session: self.compaction_session.clone(),
            compaction_changes: self.compaction_changes,
            keep_warm: self.keep_warm.clone(),
            account_reports: self.account_reports.clone(),
            max_same_account_wait: self.max_same_account_wait,
            auth_selection: Some(auth_selection),
            step_progress: self.step_progress.clone(),
            model_call_ready: self.model_call_ready.clone(),
            // A sign-in the credential needed at the run's start was dealt with before any
            // step, so a move, which happens inside a step, has none to make.
            pause_for_auth_at_start: None,
            account_continue: self.account_continue,
        }
    }
}

/// Something a run's model calls wait for (see [`Seams::model_call_ready`]).
#[async_trait::async_trait]
pub trait ModelCallReady: Send + Sync {
    /// Returns once a model call may be rendered, or why none can be: the step then fails with
    /// that error, never sending a request.
    async fn ready(&self) -> Result<(), ProviderError>;
}

/// The run loop's account reports. Each names the record it reports by `event_id`, the
/// session's id and the record's seq. The hooks must not block: a reporter queues the event
/// and returns. An attempt's record is appended without a barrier unless the attempt failed,
/// so a reporter that must only report durable records waits for the session's durable bound
/// to reach `seq`.
pub trait AccountReports: Send + Sync {
    /// A fresh run's start, at `seq`, is durable. Not called on a resume, which writes no
    /// start.
    fn run_started(&self, event_id: &str, seq: u64);
    /// A provider attempt's record was appended at `seq`.
    fn attempt_recorded(&self, event_id: &str, seq: u64, attempt: &ModelAttempt);
    /// The same, with the selection the attempt was sent under: the run's account now, which
    /// after a move at a quota wall is the account it moved to. A reporter that names the
    /// attempt's credential and account to routing takes them from here, so every attempt names
    /// the account that served it however the reports before it fared. The default forwards to
    /// [`Self::attempt_recorded`].
    fn attempt_recorded_on(
        &self,
        event_id: &str,
        seq: u64,
        attempt: &ModelAttempt,
        _selection: Option<&broca_records::record::AuthSelection>,
    ) {
        self.attempt_recorded(event_id, seq, attempt);
    }
    /// The run moved to the account `selection` names at a quota wall, and the record that
    /// says so, at `seq`, is durable. Reported once per move, before any attempt on that
    /// account. The default reports nothing.
    fn account_moved(
        &self,
        _event_id: &str,
        _seq: u64,
        _selection: &broca_records::record::AuthSelection,
    ) {
    }
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
    /// `Interrupted` as its reason and resumes as the same run.
    pub paused: Option<PauseReason>,
    /// In-memory routing outcome for scheduling this actor's next sign-in re-check.
    pub sign_in: Option<crate::account_move::SignInOutcome>,
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
    /// A rule in `broca_core::fresh_start` refused the run before anything was written: for
    /// example system text other than the session's, or a plan whose hooks can't run.
    Refused(broca_core::RunError),
}

mod entry;
pub use entry::run;

/// The body of [`run`], which boxes it.
async fn run_loop<W, P, T>(
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
    let mut transport = crate::transport::Guard::bind(seams.provider, &seams.bind);
    crate::transport::seed(
        writer,
        seams.provider,
        seams
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.account_ref.as_deref()),
        &admission.render_config,
        (seams.now_ms)(),
    )
    .await?;
    refuse_first_protocol_compaction(writer)?;
    refuse_unbuilt(&admission)?;
    // A session on the current compaction contract stays on it, and a new run on it freezes the
    // view library's current plan version. A session that ran outside it
    // moves onto it at the first run whose plan names a compaction provider: that run enrolls
    // it (see `crate::enroll`).
    let mut admission = admission;
    if let Some(manifest) = admission.manifest.clone() {
        admission.transport = seams
            .tools
            .run_transport(&seams.bind, admission.send.scope.as_ref(), &manifest)
            .await;
        // A prefix rebuild writes the prompt afresh, so the run takes a versioned tool's
        // same-major definition there; its start records the tools it took.
        if admission.rebuild_point {
            let changes = seams
                .tools
                .compatible_tools(&seams.bind, admission.send.scope.as_ref(), &manifest)
                .await;
            if let Some((taken, tools)) =
                crate::plan_tools::take_compatible_tools(&manifest, &admission.tools, &changes)
            {
                admission.manifest = Some(Arc::new(taken));
                admission.tools = tools;
            }
        }
    }
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
        admission.global_system = Arc::new(global_system(writer, parts, &mut input, &admission)?);
    } else if let Some(previous) = writer.operations().run.as_ref() {
        // A later run outside the contract: its system messages must repeat the session's.
        // Only the window's system messages are compared, so only they are copied out of it.
        let systems: Vec<Message> = writer
            .conversation()
            .held_messages_of(Role::System)
            .map_err(crate::compaction::not_held)?
            .iter()
            .map(|message| Message::clone(message))
            .collect();
        broca_core::fresh_start::append_system_messages(
            &mut input,
            &systems,
            previous_is_import(previous)?,
        )
        .map_err(LoopError::Refused)?;
    }
    admission.input = input;
    // The run numbers its messages' parts by its own computed rules, after the highest number
    // the session has shown, as its start freezes them. On the compaction contract, requests
    // show the projection's numbers instead, but the run's numbering rules are still recorded.
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
    // The schedule is read first, so a session whose schedule can't be read stores no part.
    let refusal_schedule = refusal_schedule(&*writer, &admission)?;
    // From here the run holds the decoded values of the parts its start names, as its resume
    // and the session's next run read them, so all of them render the same requests.
    let setup = store_setup(parts, &mut admission)?;
    let runner = broca_core::tags::rules_for(
        Some(broca_records::record::ComputedTagRules { checkpoint }),
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
    let start_seq = admit(
        &mut placed,
        &admission,
        setup,
        refusal_schedule,
        checkpoint,
        enrolled,
    )
    .await?;
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
        None => match seams.pause_for_auth_at_start.clone() {
            Some(error) => {
                sign_in_at_start(
                    &mut placed,
                    parts,
                    seams,
                    &admission,
                    compaction.as_ref(),
                    error,
                )
                .await?
            }
            None => drive(&mut placed, parts, &admission, seams, compaction.as_ref()).await?,
        },
    };
    // Every end a run writes is followed by a notice of the session's changes; a pause isn't
    // an end.
    if finished.paused.is_none() {
        let floor = signed_floor(&placed, parts, &mut ToolSetCache::default()).await?;
        if let Some(changes) = crate::compaction::Changes::of(&admission, seams, floor)
            .map(|changes| changes.sharing(compaction.as_ref()))
        {
            changes.tell(&placed, seams).await;
        }
    }
    transport.finished(&finished);
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
        Some(part) => {
            Some(crate::parts::shared::<Vec<Message>, _>(parts, part).map_err(LoopError::Parts)?)
        }
        None => None,
    };
    broca_core::fresh_start::new_episode_globals(
        input,
        admission.manifest.as_deref(),
        previous.as_ref().map(|previous| previous.as_slice()),
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
    let mut transport = crate::transport::Guard::bind(seams.provider, &seams.bind);
    if let Some(run) = writer.operations().run.as_ref() {
        let admission = frozen_admission(run, parts)?;
        crate::transport::seed(
            writer,
            seams.provider,
            seams
                .auth_selection
                .as_ref()
                .and_then(|selection| selection.account_ref.as_deref()),
            &admission.render_config,
            (seams.now_ms)(),
        )
        .await?;
    }
    let unfinished = writer
        .operations()
        .run
        .as_ref()
        .is_some_and(|run| run.finished.is_none());
    let finished = resume_run(writer, parts, seams).await?;
    // A resume that wrote the run's end is followed by a notice of the session's changes.
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
    transport.finished(&finished);
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
    // resume does: nothing is refrozen, no resume is recorded, an account limit isn't waited
    // on, and no model is called. An interrupt queued behind a paused run ends it this way.
    if matches!(resolution, Resolution::Paused(_))
        && seams.cancel.is_cancelled()
        && !cancelling_held
    {
        return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await;
    }
    if resolution == Resolution::Paused(PauseReason::AuthRequired)
        && seams
            .account_continue
            .is_some_and(|continuer| continuer.probe_only())
        && seams.pause_for_auth_at_start.is_none()
    {
        return Ok(Finished {
            paused: Some(PauseReason::AuthRequired),
            ..finished(writer, RunFinishReason::Interrupted)
        });
    }
    let runner = runner_of_start(writer, parts, &admission.run_id).await?;
    let mut sign_in = None;
    let mut sign_in_error = seams.pause_for_auth_at_start.clone();
    let account_ready = resolution == Resolution::Paused(PauseReason::AccountLimit)
        && account_limit_of(writer)?
            .is_some_and(|pause| (seams.now_ms)() >= pause.resume_not_before_ms);
    let may_ask = account_ready
        || (resolution == Resolution::Paused(PauseReason::AuthRequired)
            && seams
                .account_continue
                .is_some_and(|continuer| continuer.recheck_sign_in()));
    let moved = if may_ask {
        if let Some(error) = &mut sign_in_error {
            let step_id = run_state(writer).completed_steps.saturating_add(1);
            let mut refused = sign_in_refused(writer, seams, &admission, error, step_id).await?;
            match crate::account_move::signed_out(
                writer,
                parts,
                seams,
                &admission,
                step_id,
                error,
                &mut refused,
            )
            .await?
            {
                Some(crate::account_move::AtWall::Moved(moved)) => Some(moved),
                Some(crate::account_move::AtWall::Held(answer)) => {
                    sign_in = Some(crate::account_move::SignInOutcome::hold(
                        answer.until_ms,
                        (seams.now_ms)(),
                    ));
                    error.account_continue = Some(Box::new(answer));
                    None
                }
                Some(crate::account_move::AtWall::Stayed(answer)) => {
                    sign_in = Some(if answer.is_some() {
                        crate::account_move::SignInOutcome::Refuse
                    } else {
                        crate::account_move::SignInOutcome::Unreachable
                    });
                    error.account_continue = answer.map(Box::new);
                    None
                }
                Some(crate::account_move::AtWall::Stopped) if seams.cancel.is_cancelled() => {
                    return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled)
                        .await
                }
                _ => None,
            }
        } else {
            None
        }
    } else {
        None
    };
    let moved_seams = moved
        .as_ref()
        .map(|moved| seams.moved_to(&moved.provider, moved.selection.clone()));
    let seams = moved_seams.as_ref().unwrap_or(seams);
    // A quota pause keeps the account so it can reuse its cached prompt after the reset.
    // That cache is unusable after sign-in loss, so a due pause may take an offered account
    // through the ordinary auth-resume move batch instead.
    let resolution = if account_ready && moved.is_some() {
        Resolution::Paused(PauseReason::AuthRequired)
    } else {
        resolution
    };
    // A run paused on its account's limit resumes on that same account, and only once the
    // account has reset. Until then nothing is written and the
    // run stays paused; on another account it ends interrupted, saying why.
    if resolution == Resolution::Paused(PauseReason::AccountLimit) && !cancelling_held {
        let frozen = admission.auth_selection.as_ref();
        if seams.pause_for_auth_at_start.is_none()
            && seams.auth_selection.as_ref().is_some_and(|current| {
                !broca_core::account_limit_pause::same_selection(current, frozen)
            })
        {
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
    // An unresolved sign-in pause stays parked without another resume record. A timed
    // account pause that is ready changes to sign-in before checking credential-bound config.
    if let Some(error) = sign_in_error.filter(|_| moved.is_none()) {
        if resolution == Resolution::Paused(PauseReason::AuthRequired) {
            return Ok(Finished {
                paused: Some(PauseReason::AuthRequired),
                sign_in,
                ..finished(writer, RunFinishReason::Interrupted)
            });
        }
        if resolution == Resolution::Paused(PauseReason::AccountLimit) {
            let mut finished = pause(
                writer,
                seams,
                &admission.run_id,
                PauseReason::AuthRequired,
                Some(error),
                None,
                None,
            )
            .await?;
            finished.sign_in = sign_in;
            return Ok(finished);
        }
    }
    // A resume that will call the provider again first confirms the provider still renders the
    // run's requests byte for byte. A changed renderer or render config would bust the prompt
    // cache or make the conversation invalid for the provider.
    let drift = match resolution {
        _ if cancelling_held || seams.pause_for_auth_at_start.is_some() => None,
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
        // A paused run resumed on another account than it froze (routing gave it one), on a
        // route whose render config names the account: the Claude Code persona, which the
        // request body carries. The provider renders the persona refrozen for the new account,
        // and the run takes it even after a provider call, as a move inside a run does: the
        // next request is the first on that account, which holds none of the old one's cache.
        // Any other difference still ends the run.
        let persona_follows_account = seams
            .auth_selection
            .as_ref()
            .and_then(|selection| selection.account_ref.as_deref())
            .is_some_and(|account| {
                reason != PauseReason::AccountLimit
                    && admission
                        .auth_selection
                        .as_ref()
                        .and_then(|frozen| frozen.account_ref.as_deref())
                        != Some(account)
            })
            && seams.provider.renderer_version() == admission.renderer_version
            && differs_only_in_persona(
                &seams.provider.render_config_blob(),
                &admission.render_config,
            );
        if let Some(error) = &drift {
            // Before any provider call no request depends on the old config, so the run takes
            // the provider's current one; after a call, or when either config can't be read, the
            // run can't go on.
            if (run.provider_call_started && !persona_follows_account)
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
            .filter(|_| seams.pause_for_auth_at_start.is_none())
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
    if submit.is_some()
        || drift.is_some()
        || cancelling_held
        || stop_before_batch(writer, &admission.stop_when).is_some()
    {
        resumed.redone.clear();
    }
    // A held call's provider may already have answered: the batch closes with what it holds,
    // sending nothing, and then the resume and the run's end are written.
    if let (Some(error), Resolution::ProviderHeld(_)) = (&drift, &resolution) {
        let mut error = error.clone();
        error.provider_code = Some("held_resume_refused".into());
        return close_interrupted(writer, seams, &admission, Some((&error, resumed))).await;
    }
    // The refreezes come first, durable with the resume, so nothing the resume does renders or
    // bills under the old config or account.
    let barrier = !refrozen.is_empty();
    let auth_position = refrozen
        .iter()
        .position(|record| matches!(record, Record::AuthRefrozen(_)));
    refrozen.push(Record::Resumed(resumed));
    let mut batch = Some(refrozen);
    let mut auth_seq = None;
    writer
        .append_with(
            &mut |_, _, first_seq| {
                let mut batch = batch.take().expect("one batch");
                auth_seq = auth_position.map(|position| first_seq + position as u64);
                name_resumed_from(&mut batch, first_seq);
                batch
            },
            barrier,
        )
        .await?;
    if let Some(moved) = &moved {
        if let (Some(reports), Some(seq)) = (&seams.account_reports, auth_seq) {
            reports.account_moved(&event_id(writer, seq), seq, &moved.selection);
        }
        seams
            .account_continue
            .expect("prepared move")
            .adopted(&moved.selection);
    }
    let mut moved_transport = moved
        .as_ref()
        .map(|moved| crate::transport::Guard::bind(&moved.provider, &seams.bind));
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
        return close_held_cancelled(writer, seams, &admission).await;
    }
    if let Some((frozen, pending)) = submit {
        return submitted(writer, seams, &admission.run_id, &frozen, &pending).await;
    }
    if let Some(error) = seams.pause_for_auth_at_start.clone() {
        return sign_in_at_start(writer, parts, seams, &admission, None, error).await;
    }
    let compaction = compaction_ctx(writer, parts, &admission, seams, false).await?;
    let finished = match resolution {
        Resolution::Resume => drive(writer, parts, &admission, seams, compaction.as_ref()).await,
        Resolution::Idle(reason) => finish_idle(writer, seams, &admission, reason).await,
        Resolution::Interrupted(_) => close_interrupted(writer, seams, &admission, None).await,
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
    }?;
    if let Some(transport) = &mut moved_transport {
        transport.finished(&finished);
    }
    Ok(finished)
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
/// A resume that refuses to go on passes the error the run ends with and its own record, which
/// is written after the batch closes and before the run's end.
async fn close_interrupted<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    refused: Option<(&ProviderError, Resumed)>,
) -> Result<Finished, LoopError> {
    let run_id = admission.run_id.as_str();
    let ctx = held_batch_ctx(writer, seams, admission);
    let now = (seams.now_ms)();
    let (mut records, withdrawn) =
        open_batch_close_out(writer, run_id, now, RunFinishReason::Interrupted);
    let mut end = run_end(writer, run_id, RunFinishReason::Interrupted, now);
    if let Some((error, resumed)) = refused {
        if let Record::RunEnd(end) = &mut end {
            end.error = Some(
                serde_json::to_value(error)
                    .map_err(|e| LoopError::Parts(e.to_string()))?
                    .into(),
            );
        }
        records.push(Record::Resumed(resumed));
    }
    records.push(end);
    append_end(writer, run_id, records).await?;
    synced_closed(seams, ctx.as_ref(), &withdrawn).await;
    withdraw(seams, &withdrawn).await;
    Ok(finished(writer, RunFinishReason::Interrupted))
}

/// Ends a run cancelled while a provider holds calls of its open batch, sending nothing. A
/// result a provider already deposited is that call's result: it is written and acknowledged
/// first ([`take_deposited`]). Each call still held then gets its close result, durable with
/// the batch's and the run's end; its slot is released and its provider asked to withdraw it.
/// Closing a held obligation needs neither a renderer nor model admission.
async fn close_held_cancelled<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
) -> Result<Finished, LoopError> {
    take_deposited(writer, seams, admission).await?;
    let ctx = held_batch_ctx(writer, seams, admission);
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
    synced_closed(seams, ctx.as_ref(), &withdrawn).await;
    withdraw(seams, &withdrawn).await;
    Ok(finished(writer, RunFinishReason::Cancelled))
}

/// The context of the run's open batch, as its held calls' lifecycle knows it.
fn held_batch_ctx<W: Writer, P, T>(
    writer: &W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
) -> Option<BatchCtx> {
    let batch = run_state(writer).open_batch.as_ref()?;
    Some(BatchCtx {
        batch_id: BatchId(batch.batch_id.clone()),
        bind: seams.bind.clone(),
        run_id: RunId(admission.run_id.clone()),
        scope: admission.send.scope.clone(),
        media_capture: writer.media(),
    })
}

/// Tells the held calls' lifecycle that each of `closed` has its close result durably, so a
/// late result already deposited for one is released rather than kept waiting for a run that
/// will never take it.
async fn synced_closed<P, T>(
    seams: &Seams<'_, P, T>,
    ctx: Option<&BatchCtx>,
    closed: &[ProviderHeldReply],
) {
    if let (Some(lifecycle), Some(ctx)) = (seams.held, ctx) {
        for reply in closed {
            lifecycle.terminal_synced(ctx, &reply.tool_call_id).await;
        }
    }
}

/// Ends the session's unfinished latest run `Interrupted`, sending nothing, with the records
/// produced by `broca_core::resume::interrupted_seal` for the same run: on a graceful stop
/// (`SealCause::Shutdown`), or when a session opens on a run a crashed process left running
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
    // An open batch closes regardless of its calls' state; only when there is no open batch is
    // a never-dispatched tool turn kept.
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
                    .map_err(|e| LoopError::Parts(e.to_string()))?
                    .into(),
            );
        }
    }
    records.push(end);
    append_end(writer, &run_id, records).await?;
    withdraw(seams, &withdrawn).await;
    Ok(finished(writer, RunFinishReason::Interrupted))
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
            held_sequential: batch.calls[position].held == Some(true),
            args: batch.args(position).map(broca_protocol::json::Json::parse),
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
    manifest: &broca_records::record::PlanManifest,
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
) -> Result<Option<std::sync::Arc<broca_records::record::PlanManifest>>, LoopError> {
    broca_session_store::fold::operations::ManifestRef::of_start(start)
        .map(|manifest| stored_manifest(&manifest, parts))
        .transpose()
}

/// The plan manifest `manifest` references, rebuilt from its setup parts and the rest of its
/// members. Shared with every other holder of the same manifest (see `crate::parts::shared`),
/// and its tool list with every holder of its tools part.
pub fn stored_manifest(
    manifest: &broca_session_store::fold::operations::ManifestRef,
    parts: &dyn Parts,
) -> Result<std::sync::Arc<broca_records::record::PlanManifest>, LoopError> {
    let rebuild = || -> Result<broca_records::record::PlanManifest, String> {
        let mut members = match &manifest.rest {
            Some(Value::Object(rest)) => rest.clone(),
            _ => serde_json::Map::new(),
        };
        let mut fetches = Vec::new();
        let mut tools = None;
        for part in &manifest.parts {
            if part.kind == PartKind::Tools {
                // The tool list is the one every holder of the part shares. The member is
                // filled in once the rest of the manifest is read.
                tools = Some(crate::parts::shared_or::<Vec<Tool>, _>(parts, part, |e| {
                    format!("the frozen manifest: {e}")
                })?);
                members.insert("tools".into(), Value::Array(Vec::new()));
                continue;
            }
            let value = parts.get(part)?;
            match part.kind {
                PartKind::Plan => {
                    members.insert("plan".into(), value);
                }
                PartKind::ProviderAnswer => fetches.push(value),
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
        let mut rebuilt: broca_records::record::PlanManifest =
            serde_json::from_value(Value::Object(members))
                .map_err(|e| format!("the frozen manifest: {e}"))?;
        if let Some(tools) = tools {
            rebuilt.tools = tools;
        }
        Ok(rebuilt)
    };
    match parts.decoded() {
        Some(decoded) => decoded.get_or_decode(manifest_key(manifest), rebuild),
        None => rebuild().map(std::sync::Arc::new),
    }
    .map_err(LoopError::Parts)
}

/// The key the manifest `manifest` references is shared under: its parts, in order, and the
/// rest of its members exactly as the record spells them.
fn manifest_key(manifest: &broca_session_store::fold::operations::ManifestRef) -> Digest {
    let rest = manifest
        .rest
        .as_ref()
        .map(|rest| serde_json::to_vec(rest).expect("a JSON value always serializes"))
        .unwrap_or_default();
    crate::parts::derived_key(
        "plan manifest",
        manifest.parts.iter().map(|part| (part.kind, &part.digest)),
        &rest,
    )
}

/// The system text a run on the current compaction contract renders from its plan manifest
/// (`broca_core::compaction_v2::systems::from_manifest`), shared by every run whose manifest
/// holds the same system text part. `reference` is the manifest's reference, `manifest` the
/// manifest it references.
fn manifest_system(
    reference: &broca_session_store::fold::operations::ManifestRef,
    manifest: &broca_records::record::PlanManifest,
    parts: &dyn Parts,
) -> Result<Arc<Vec<Message>>, LoopError> {
    let derive = || Ok(broca_core::compaction_v2::systems::from_manifest(manifest));
    let text = reference
        .parts
        .iter()
        .find(|part| part.kind == PartKind::SystemText);
    match (parts.decoded(), text) {
        (Some(decoded), Some(text)) => decoded
            .get_or_decode(
                crate::parts::derived_key(
                    "plan manifest system messages",
                    [(text.kind, &text.digest)],
                    &[],
                ),
                derive,
            )
            .map_err(LoopError::Parts),
        _ => derive().map(Arc::new).map_err(LoopError::Parts),
    }
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
        // Read from the run's own record, not a setup part, so it is the run's alone.
        admission.render_config = Arc::new(refrozen["render_config"].clone());
        admission.renderer_version = decode::<u32>(Some(refrozen["renderer_version"].clone()))?
            .unwrap_or(admission.renderer_version);
    }
    if let Some(selection) = &run.refrozen_auth {
        admission.auth_selection = decode(Some(selection.clone()))?;
    }
    // A run whose strict schema was downgraded renders downgraded from then on; its provider is
    // rebuilt from the downgraded config.
    if run.structured_downgraded {
        downgrade_render_config(&mut admission.render_config);
    }
    Ok(admission)
}

/// Downgrades a run's strict schema in its render config
/// (`FrozenStructuredOutput::downgrade_render_config`). The config may be shared, so `make_mut`
/// gives the run a value of its own first: a copy while another holder keeps the shared one,
/// otherwise the value itself, which the decoded-part cache then no longer hands out. Every
/// other holder keeps the config as decoded.
pub(crate) fn downgrade_render_config(render_config: &mut Arc<Value>) {
    broca_protocol::structured_output::FrozenStructuredOutput::downgrade_render_config(
        Arc::make_mut(render_config),
    );
}

/// The admission a run's start froze: its model, settings, tools and system text, read back from
/// the setup parts the start names.
pub fn admission_of_start(
    start: &broca_session_store::record::RunStart,
    parts: &dyn Parts,
) -> Result<Admission, LoopError> {
    // Each part is read as the value every other holder of it shares.
    fn shared<T>(parts: &dyn Parts, part: Option<&PartRef>) -> Result<Option<Arc<T>>, LoopError>
    where
        T: serde::de::DeserializeOwned + Send + Sync + 'static,
    {
        part.map(|part| crate::parts::shared(parts, part).map_err(LoopError::Parts))
            .transpose()
    }
    let part = |kind: PartKind| start.setup.iter().find(|part| part.kind == kind);
    let config = start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("config"))
        .cloned()
        .unwrap_or(Value::Null);
    let member = |key: &str| config.get(key).cloned();
    // Read once, where the system text first needs it, and then held.
    let manifest;
    Ok(Admission {
        run_id: start.run_id.clone(),
        model: start.model.clone(),
        input: Vec::new(),
        tools: shared(parts, part(PartKind::Tools))?.unwrap_or_default(),
        tool_choice: decode(member("tool_choice"))?.unwrap_or_default(),
        generation: decode(member("generation"))?.unwrap_or_default(),
        response_format: shared(parts, part(PartKind::ResponseFormat))?.unwrap_or_default(),
        provider_options: member("provider_options").unwrap_or(Value::Null),
        render_config: match shared(parts, part(PartKind::RenderConfig))? {
            Some(render_config) => start
                .render_config(render_config)
                .map_err(|error| LoopError::Parts(error.to_string()))?,
            None => Arc::new(Value::Null),
        },
        renderer_version: start.renderer_version,
        stop_when: decode(member("stop_when"))?.unwrap_or_default(),
        on_restart: decode(start.on_restart.clone())?,
        keep_warm: decode(start.keep_warm.clone())?,
        auth_selection: decode(start.auth_selection.clone())?,
        account_limit_pause: decode::<bool>(member("account_limit_pause"))?.unwrap_or(false),
        ts_ms: start.ts_ms,
        written_at_ms: start.written_at_ms,
        global_system: {
            let reference = broca_session_store::fold::operations::ManifestRef::of_start(start);
            manifest = reference
                .as_ref()
                .map(|reference| stored_manifest(reference, parts))
                .transpose()?;
            match (start.compaction_plan_version, &reference, &manifest) {
                (None, _, _) => Arc::default(),
                (Some(_), Some(reference), Some(manifest)) => {
                    manifest_system(reference, manifest, parts)?
                }
                (Some(_), _, _) => shared(parts, part(PartKind::GlobalSystem))?.unwrap_or_default(),
            }
        },
        compaction_plan_version: start.compaction_plan_version,
        context_limit: start.context_limit,
        manifest,
        transport: decode(
            start
                .run_choices
                .as_ref()
                .and_then(|choices| choices.get("transport"))
                .cloned(),
        )?
        .unwrap_or_default(),
        render_tags: member("render_tags")
            .and_then(|value| value.as_bool())
            .unwrap_or(false),
        marker_order_version: frozen_marker_order(start),
        budget: decode::<broca_protocol::request::BudgetConfig>(member("budget"))?
            .filter(|budget| *budget != broca_protocol::request::BudgetConfig::default()),
        route_decision_id: start.route_decision_id.clone(),
        account_route: decode(member("account_route"))?,
        served_provider: None,
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
            // Its own record beside the start, which a start read back never writes again.
            retention_policy: None,
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
        plan_notes: None,
        rebuild_point: false,
        head_turn: false,
    })
}

/// A session compacted under the first compaction protocol keeps rendering under that protocol's
/// last view; the run loop refuses it rather than send different bytes.
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

/// Refuses, by name, a run that needs a part of the run loop not built yet, rather than running
/// it without that part: an input-token budget (no live session has one), or the client-side
/// refusal fallback. Each is read from what the run froze.
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
/// numbers, the number it continues after, and its render config, combined by `rules_for`,
/// which numbers the run's messages. A run's start is read once and kept in `cache` (see
/// [`TagRulesCache`]); the render config and the run's place in the prompt are read from
/// `episodes` at every call.
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

/// Whether `admission`'s requests show each ride as its own text block after the tool turn's
/// results, which then hold only the tools' own output: its frozen render config says
/// `ride_placement: after_results` on an Anthropic Messages route. Every other run renders each
/// ride inside its carrying result, as before.
///
/// Only runs admitted while the 12 November trial switch was on carry `after_results`; new runs
/// never get it. Keep this choice for resumed or rebuilt requests until those stored runs no
/// longer need rendering.
pub(crate) fn rides_after_results(admission: &Admission) -> bool {
    let config = &*admission.render_config;
    config.get("family_id").and_then(Value::as_str)
        == Some(broca_protocol::tags::ANTHROPIC_MESSAGES_FAMILY)
        && config
            .get(broca_provider::wire::RidePlacement::MEMBER)
            .cloned()
            .and_then(|member| {
                serde_json::from_value::<broca_provider::wire::RidePlacement>(member).ok()
            })
            == Some(broca_provider::wire::RidePlacement::AfterResults)
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
    let tools = Arc::clone(&admission.tools);
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
        None,
    )?;
    finalize_tool_choice(writer, &mut opts);
    if !compacted {
        let durable_system: Vec<Message> = opts.prompt.first().cloned().into_iter().collect();
        let opts = with_plan_system(writer, &admission, &durable_system, opts);
        return Ok(with_plan_notes(writer, opts));
    }
    let opts = crate::compaction::offline_prompt(writer, &admission, bind, step_id, opts).await?;
    Ok(with_plan_notes(writer, opts))
}

/// Disables tool calls on the budget's finalize step, and says whether the step is one. The step
/// keeps the declared tools byte for byte and disables calls through the tool choice; dropping
/// the tools would change the request's cached prefix on the run's last step. A step's live
/// request and its offline rebuild both take it from here.
fn finalize_tool_choice<W: Writer>(writer: &W, opts: &mut CallOptions) -> bool {
    let finalizing = crate::budget::finalizing(writer.operations());
    if finalizing {
        opts.tool_choice = Arc::new(ToolChoice::None);
    }
    finalizing
}

/// Whether two render configs differ, and only in the Claude Code persona they freeze.
fn differs_only_in_persona(current: &Value, frozen: &Value) -> bool {
    const PERSONA: &str = "claude_code_persona";
    let (Some(current), Some(frozen)) = (current.as_object(), frozen.as_object()) else {
        return false;
    };
    if current.get(PERSONA) == frozen.get(PERSONA) {
        return false;
    }
    // The refusal-fallback model's config carries the same persona as the source, and is
    // refrozen with it: a fallback still naming another persona than its source differs.
    let fallback_persona = |config: &serde_json::Map<String, Value>| {
        config
            .get("refusal_fallback")
            .and_then(|fallback| fallback.get("frozen"))
            .and_then(|fallback| fallback.get(PERSONA))
            .filter(|persona| !persona.is_null())
            .cloned()
    };
    if fallback_persona(current).is_some_and(|persona| Some(&persona) != current.get(PERSONA)) {
        return false;
    }
    let without = |config: &serde_json::Map<String, Value>| {
        let mut config = config.clone();
        config.remove(PERSONA);
        if let Some(fallback) = config
            .get_mut("refusal_fallback")
            .and_then(|fallback| fallback.get_mut("frozen"))
            .and_then(Value::as_object_mut)
        {
            fallback.remove(PERSONA);
        }
        config
    };
    without(current) == without(frozen)
}

/// Model steps and tool batches from the run's current state until a decision ends the run.
///
/// A step whose account spent its usage window may move the run to another account (see
/// [`crate::account_move`]): the step is then entered again on the new account's provider, under
/// the selection and render config the move made durable, until the run ends or moves again.
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
    let mut moving = None;
    let finished = drive_from(
        writer,
        parts,
        admission,
        seams,
        compaction,
        None,
        &mut moving,
    )
    .await?;
    let Some(mut next) = moving.take() else {
        return Ok(finished);
    };
    // The account the run left is not sent to again by this run.
    seams.provider.close_run_transport();
    // A head's turn isn't frozen in the run's start (see `Admission::head_turn`), so the
    // admission read back below says false: the run keeps the one it began under, and routing
    // hears of a head's turn at every usage wall that asks it for another account, not just
    // the first.
    let head_turn = admission.head_turn;
    loop {
        let run = run_state(writer);
        let structured_downgraded = run.structured_downgraded;
        // The run goes on under what the move refroze, as a resume reads it back.
        let mut admission = frozen_admission(run, parts)?;
        admission.head_turn = head_turn;
        let crate::account_move::Moving { moved, reentry } = next;
        // A strict schema the run already downgraded stays downgraded on the new provider, which
        // was built from the run's frozen config without it.
        if structured_downgraded {
            moved.provider.downgrade_structured_output();
        }
        // The new account's provider owns its transport for the rest of the run, as the run's
        // own provider does: bound to the session, seeded with what the log says the new
        // account last accepted (which a timed pause on it weighs), and completed or closed
        // with the run's end, or closed at the next move.
        let mut transport = crate::transport::Guard::bind(&moved.provider, &seams.bind);
        crate::transport::seed(
            &*writer,
            &moved.provider,
            moved.selection.account_ref.as_deref(),
            &admission.render_config,
            (seams.now_ms)(),
        )
        .await?;
        let moved_seams = seams.moved_to(&moved.provider, moved.selection.clone());
        let finished = drive_from(
            writer,
            parts,
            &admission,
            &moved_seams,
            compaction,
            Some(reentry),
            &mut moving,
        )
        .await?;
        let Some(again) = moving.take() else {
            transport.finished(&finished);
            return Ok(finished);
        };
        drop(transport);
        next = again;
    }
}

/// [`drive`], from the run's current state or, with `reentry`, from inside the step a move
/// interrupted. A move at a wall puts what it made durable in `moving` and returns a placeholder
/// the caller discards.
async fn drive_from<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    compaction: Option<&crate::compaction::Ctx>,
    reentry: Option<crate::account_move::Reentry>,
    moving: &mut Option<crate::account_move::Moving<P>>,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    // A return before the assistant is stored leaves its streamed display without a replacement.
    let mut live_display = false;
    let result = drive_from_inner(
        writer,
        parts,
        admission,
        seams,
        compaction,
        reentry,
        moving,
        &mut live_display,
    )
    .await;
    reset_live_display(seams.out, &mut live_display).await;
    result
}

async fn reset_live_display(out: Option<&mpsc::Sender<OutputEvent>>, live: &mut bool) {
    if std::mem::take(live) {
        if let Some(out) = out {
            let _ = out
                .send(OutputEvent::Display(
                    broca_protocol::DisplayEvent::DisplayReset,
                ))
                .await;
        }
    }
}

#[allow(clippy::too_many_arguments)]
async fn drive_from_inner<W, P, T>(
    writer: &mut W,
    parts: &dyn Parts,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    compaction: Option<&crate::compaction::Ctx>,
    mut reentry: Option<crate::account_move::Reentry>,
    moving: &mut Option<crate::account_move::Moving<P>>,
    live_display: &mut bool,
) -> Result<Finished, LoopError>
where
    W: Writer,
    P: Provider,
    T: ToolPlane,
{
    let warming = Warming::of(admission, seams);
    // The pause a timed same-account wait left, which the step it resumes must match. A step
    // entered again after a move resumes no pause.
    let mut account_limit_replay = match reentry {
        Some(_) => None,
        None => account_limit_of(writer)?,
    };
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
    let tools = Arc::clone(&admission.tools);
    // The run's frozen plan says which preset each tool provider's calls carry, which providers
    // were fetched, and what they declared. A call without its preset is refused by a provider
    // whose route is bound under a scope.
    let mut manifest = ToolManifest::from_shared_tools(Arc::clone(&admission.tools));
    if let Some(plan) = admission.manifest.as_deref() {
        manifest = manifest
            .with_plan_manifest(plan)
            .map_err(|error| LoopError::Parts(format!("the run's frozen plan: {error}")))?;
    }
    manifest = manifest.with_run_transport(&admission.transport);
    // Once per run, the run asks whether the provider's media renderer tags its own copy of each
    // request. If it does, the provider takes the run's tag choice here, and the run leaves
    // media-holding messages untagged, since preparing their media needs the parts where the
    // model put them.
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
    let stop_when = admission.stop_when.clone();
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
        // A stop asked for now and acknowledged before the crash owes the calls nothing, as a
        // live stop between the answer and the batch does: they close as not run. The durable
        // request is read here too, so a resume never sends after that acknowledgement even if
        // its cancel token was made fresh.
        let cancelled_now = seams.cancel.is_cancelled()
            || run_state(writer).cancel == Some(broca_session_store::record::CancelPoint::Now);
        if let Some(stop) = stop_before_batch(writer, &stop_when) {
            return hard_stop(writer, seams, &admission.run_id, step_id, &planned, &stop).await;
        }
        if cancelled_now || seams.stop.is_cancelled() {
            // A call a provider holds was sent, so it is never closed as not run. A cancel
            // takes any result already deposited and closes the rest as held calls close; a
            // stop leaves the batch parked on them, for the next process to take up.
            if known.iter().any(|known| known.held.is_some()) {
                if cancelled_now {
                    return close_held_cancelled(writer, seams, admission).await;
                }
                let ctx = held_batch_ctx(writer, seams, admission);
                return pause(
                    writer,
                    seams,
                    &admission.run_id,
                    PauseReason::ToolHeld,
                    None,
                    None,
                    ctx.as_ref(),
                )
                .await;
            }
            let calls = if started { &unsent } else { &planned };
            return stopped_batch_in(
                writer,
                seams,
                &admission.run_id,
                step_id,
                calls,
                cancelled_now,
                !started,
            )
            .await;
        }
        match batch(
            writer,
            seams,
            &admission.run_id,
            admission.send.scope.as_ref(),
            step_id,
            // The step's request isn't rendered again, so its calls cite no number.
            &[],
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
            BatchOutcome::Closed { interrupted: false } => {}
            BatchOutcome::Closed { interrupted: true } => {
                return finish_in_doubt(writer, seams, &admission.run_id).await;
            }
            BatchOutcome::Held(finished) | BatchOutcome::Stopped(finished) => return Ok(finished),
            BatchOutcome::Cancelled(closed) => {
                let finished =
                    finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await?;
                withdraw(seams, &closed).await;
                return Ok(finished);
            }
            BatchOutcome::InDoubt(closed) => {
                let finished = finish_in_doubt(writer, seams, &admission.run_id).await?;
                withdraw(seams, &closed).await;
                return Ok(finished);
            }
        }
    }
    loop {
        // A step entered again after a move at a wall started already: it writes no boundary,
        // and what is steered or delivered meanwhile waits for the next one.
        let entering = reentry.take();
        // A crash can leave a tool turn durable without the budget firings its barrier carried.
        // Any firing the durable call count makes due is written here, before anything else at
        // this boundary; the run loop derives it again before its next request.
        if entering.is_none() {
            writer
                .append_with(
                    &mut |conversation, operations, first_seq| {
                        crate::budget::due(conversation, operations, first_seq, 0, 0)
                    },
                    true,
                )
                .await?;
        }
        let completed_steps = run_state(writer).completed_steps;
        // Step ids count from 1 within a run.
        let step_id = completed_steps + 1;
        debug_assert!(entering
            .as_ref()
            .is_none_or(|entering| entering.step_id == step_id));
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
        // A view the compaction provider handed over applies here, before the step starts; a
        // boundary that ends the run applies none.
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
        let ordered = match entering {
            Some(_) => Default::default(),
            None => Box::pin(ordered_sends(writer)).await?,
        };
        // A step entered again after a move writes no boundary, unless the run must end or
        // pause here: a cancel or a stop that came while routing was asked or the move was
        // written is acted on before anything is sent on the new account.
        let boundary = if entering.is_some() && !ending {
            Vec::new()
        } else {
            writer
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
                        // `turn_system` are delivered, and expired ones end, in queue order,
                        // according to `broca_core::delivery_order`. Their records come first, then
                        // every waiting message's copy (steers before deliveries), then the step's
                        // start.
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
                        // the last steered, else the conversation's last) and, on a route that
                        // needs it last, with no user message to follow it in this request. Any
                        // later send that may steer counts as a user message following it.
                        let mut preceding: Option<Message> = run
                            .pending_steers
                            .iter()
                            .rev()
                            .find_map(|pending| pending.messages.last())
                            .or(conversation.newest().map(|message| &**message))
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
                                        turn_system_input(send, &pending.submission_id),
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
                                    message: Arc::new(message.clone()),
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
                .await?
        };
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
        // Deriving the stored prompt again reads every window message's content, so content
        // the window released is read back first, for this step only.
        let whole = if compaction.is_some()
            && !writer.conversation().released_ordinals().is_empty()
            && kept_prompt.rederives(
                writer,
                &episodes,
                enrollment.as_ref(),
                crate::repetition::after_input(&admission.render_config),
            ) {
            Some(crate::writer::whole_conversation(writer).await?)
        } else {
            None
        };
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
            whole.as_ref(),
        )?;
        drop(whole);
        // The step names the account routing gives the run now.
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
        // The part numbers the step's request shows, recorded on each of its attempts so a
        // citation of a number in its answer can be resolved later (`tag_read`). Only a request
        // rendered for the compaction contract shows numbers.
        let mut shown_tags = Vec::new();
        if let Some(ctx) = compaction.filter(|_| call_ready) {
            match crate::compaction::prepare(
                writer,
                admission,
                seams,
                ctx,
                step_id,
                opts,
                &mut kept_prompt,
                &rendered,
                entering.as_ref().map(|entering| &*entering.accepted_under),
                entering.as_ref().and_then(|entering| entering.overflow),
            )
            .await?
            {
                crate::compaction::Send::Ready(ready) => {
                    opts = *ready;
                    shown_tags =
                        broca_session_store::record::TagRange::of_sorted(&rendered.shown_tags());
                }
                crate::compaction::Send::Unavailable(message) => {
                    return compaction_unavailable(writer, seams, admission, &message).await;
                }
                crate::compaction::Send::Ceiling(detail) => {
                    // Written with the run's end, which is durable.
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
            // The session's cut lies above the window's start: the history before its first kept
            // entry is sealed now, before the request goes out. The request is already built, so
            // this changes none of its bytes. The cut may be this step's, from a view the provider
            // pushed between steps (applied at the step's start) or one it returned while the
            // request was prepared, or one applied before the history was ever sealed.
            if let Some(ordinal) = crate::compaction::cut_to_seal(writer, ctx) {
                let start = writer.conversation().window_start();
                crate::compaction::seal_at_cut(
                    writer,
                    seams,
                    ctx,
                    ordinal,
                    crate::repetition::after_input(&admission.render_config),
                )
                .await?;
                let sealed_from = writer.conversation().window_start();
                ctx.session.sealed_at(ordinal, sealed_from);
                if sealed_from != start {
                    // The seal moved the runs wholly before the cut out of the window and kept
                    // only those whose settings still shape a request (see
                    // `Conversation::seal_window`). The run's list is read again from the sealed
                    // conversation, as a run starting now would read it, so later steps carry the
                    // bounded list rather than every run the history held when this run started.
                    // Every renderer gives the same bytes for both lists
                    // (`openai_responses_chatgpt::reduce_episodes`).
                    episodes = Arc::new(render_history(writer, parts, &mut tool_sets).await?);
                }
            }
        }
        let opts = with_plan_system(writer, admission, &durable_system, opts);
        let opts = with_plan_notes(writer, opts);
        // Client-side refusal fallback: the step goes to the run's refusal-fallback model while
        // the session's schedule says so, or when it is a step already refused and re-sent.
        let mut route = crate::refusal::Route::decide(writer, seams.provider, step_id)?;
        // The step holds one copy of its request's options, which every attempt's call shares.
        // Reserving the output cap changes only the target model and the step context, so a
        // re-reserve for the fallback model starts from these again rather than from a second
        // copy of the whole request kept for it.
        let prepared = (opts.target_model.clone(), opts.step_context.clone());
        let mut opts = Arc::new(reserve_for(writer, step_id, opts, &route));
        // A step resumed after a timed account pause is sent again only if it renders the request
        // the provider refused byte for byte; otherwise, the run ends interrupted rather than
        // sending a request the pause wasn't for.
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
        // flag, and the same step is sent again, with the refused attempt counting toward its
        // retries.
        let mut downgraded_now = false;
        // A step entered again after a move counts the retries its refused attempts used, and
        // records its attempts after theirs.
        let entered_retries = entering.as_ref().map_or(0, |entering| entering.retries);
        let first_attempt = entering
            .as_ref()
            .map_or(0, |entering| entering.first_attempt);
        let mut prior_retries = entered_retries;
        // The retries of an attempt the run's own model refused, which the re-sent step's count.
        let mut refusal_retries = 0u32;
        drop(request_timing);
        let outcome = loop {
            let outcome = model_step(
                writer,
                seams,
                &admission.run_id,
                step_id,
                Arc::clone(&opts),
                requested_cache_ttl(admission),
                admission.account_limit_pause,
                first_attempt,
                run_selection(seams, admission),
                live_display,
            )
            .await?;
            // A failed call may be retried on a downgraded grammar or another account.
            if outcome.is_err() {
                reset_live_display(seams.out, live_display).await;
            }
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
                    prior_retries = entered_retries.saturating_add(*retries_used);
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
                    // The refused attempt's deltas already reached display subscribers.
                    reset_live_display(seams.out, live_display).await;
                    refusal_retries = step.retries_used;
                    route.reroute(seams.provider);
                    let mut again = Arc::unwrap_or_clone(opts);
                    (again.target_model, again.step_context) = prepared.clone();
                    opts = Arc::new(reserve_for(writer, step_id, again, &route));
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
                // The error names the run's account or credential where it is scoped to one.
                let selection = run_selection(seams, admission);
                let error = Box::new(broca_core::provider_errors::stamp_credential(
                    *error,
                    selection.and_then(|selection| selection.account_ref.as_deref()),
                    selection.map(|selection| selection.credential_id.as_str()),
                ));
                // The provider refused the request as too long. broca never sizes a request
                // before sending it, so this refusal is what asks the compaction provider for a
                // smaller view: the step is entered again, rides `over_limit` with the
                // provider's own count, and is sent once more on the view it gets. A second
                // refusal of the same step ends the run at the context ceiling, with the
                // provider's numbers. Without a compaction provider the refusal ends the run as
                // any error does.
                if error.class == broca_protocol::ErrorClass::ContextOverflow
                    && compaction.is_some()
                {
                    use broca_core::compaction::view::{
                        reported_limit_tokens, reported_prompt_tokens,
                    };
                    let request_tokens = reported_prompt_tokens(&error.message);
                    if entering
                        .as_ref()
                        .is_none_or(|entering| entering.overflow.is_none())
                    {
                        let first_attempt = writer
                            .open_log_records()
                            .await?
                            .iter()
                            .rev()
                            .find_map(|(_, record)| match record {
                                Record::Attempt(attempt)
                                    if attempt.run_id == admission.run_id
                                        && attempt.step_id == step_id =>
                                {
                                    Some(attempt.attempt.saturating_add(1))
                                }
                                _ => None,
                            })
                            .unwrap_or(0);
                        let (refused, accepted_under) = match entering {
                            Some(entering) => (entering.refused, entering.accepted_under),
                            None => (Vec::new(), Box::new(admission.clone())),
                        };
                        reentry = Some(crate::account_move::Reentry {
                            step_id,
                            refused,
                            retries,
                            first_attempt,
                            accepted_under,
                            overflow: Some(broca_core::compaction_v2::request::Overflow {
                                request_tokens,
                            }),
                        });
                        continue;
                    }
                    let window = reported_limit_tokens(&error.message).or(admission.context_limit);
                    let detail = window
                        .zip(request_tokens)
                        .map(|(window, size)| {
                            serde_json::to_value(broca_protocol::ContextCeilingDetail::new(
                                window,
                                size,
                                "over_window",
                            ))
                        })
                        .transpose()
                        .map_err(|e| LoopError::Parts(e.to_string()))?;
                    writer
                        .append(
                            &[Record::CeilingDecided(CeilingDecided { step_id, detail })],
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
                // A definite sign-in failure can move the same step to another routed account.
                // All other auth errors retain their existing resumable pause.
                if error.class == broca_protocol::ErrorClass::Auth {
                    if crate::account_move::signed_out_selection(seams, admission, &error, step_id)
                        .is_none()
                    {
                        return pause(
                            writer,
                            seams,
                            &admission.run_id,
                            PauseReason::AuthRequired,
                            Some(*error),
                            None,
                            None,
                        )
                        .await;
                    }
                    let records = writer.open_log_records().await?;
                    let mut refused = entering
                        .as_ref()
                        .map(|entry| entry.refused.clone())
                        .unwrap_or_else(|| {
                            crate::account_move::refused_in_step(
                                &records,
                                &admission.run_id,
                                step_id,
                            )
                        });
                    let mut error = *error;
                    let outcome = crate::account_move::signed_out(
                        writer,
                        parts,
                        seams,
                        admission,
                        step_id,
                        &error,
                        &mut refused,
                    )
                    .await?;
                    let sign_in = match outcome {
                        Some(crate::account_move::AtWall::Moved(moved)) => {
                            crate::account_move::record_move(writer, seams, admission, &moved)
                                .await?;
                            let first_attempt = records
                                .iter()
                                .rev()
                                .find_map(|(_, record)| match record {
                                    Record::Attempt(attempt)
                                        if attempt.run_id == admission.run_id
                                            && attempt.step_id == step_id =>
                                    {
                                        Some(attempt.attempt.saturating_add(1))
                                    }
                                    _ => None,
                                })
                                .unwrap_or(0);
                            *moving = Some(crate::account_move::Moving {
                                moved,
                                reentry: crate::account_move::Reentry {
                                    step_id,
                                    refused,
                                    retries,
                                    first_attempt,
                                    accepted_under: entering
                                        .map(|entry| entry.accepted_under)
                                        .unwrap_or_else(|| Box::new(admission.clone())),
                                    overflow: None,
                                },
                            });
                            return Ok(finished(writer, RunFinishReason::Interrupted));
                        }
                        Some(crate::account_move::AtWall::Held(answer)) => {
                            let outcome = Some(crate::account_move::SignInOutcome::hold(
                                answer.until_ms,
                                (seams.now_ms)(),
                            ));
                            error.account_continue = Some(Box::new(answer));
                            outcome
                        }
                        Some(crate::account_move::AtWall::Stayed(answer)) => {
                            let outcome = Some(if answer.is_some() {
                                crate::account_move::SignInOutcome::Refuse
                            } else {
                                crate::account_move::SignInOutcome::Unreachable
                            });
                            error.account_continue = answer.map(Box::new);
                            outcome
                        }
                        Some(crate::account_move::AtWall::Stopped)
                            if seams.cancel.is_cancelled() =>
                        {
                            return finish(
                                writer,
                                seams,
                                &admission.run_id,
                                RunFinishReason::Cancelled,
                            )
                            .await;
                        }
                        _ => None,
                    };
                    let mut finished = pause(
                        writer,
                        seams,
                        &admission.run_id,
                        PauseReason::AuthRequired,
                        Some(error),
                        None,
                        None,
                    )
                    .await?;
                    finished.sign_in = sign_in;
                    return Ok(finished);
                }
                // The provider refused the account's limit, which resets soon enough that the
                // session's cache on that account outlives the wait: the run pauses on the same
                // account until then.
                if let Some(pause_on) = limited.as_deref().and_then(|limited| {
                    account_limit_pause(seams, admission, step_id, limited, &error)
                }) {
                    return pause(
                        writer,
                        seams,
                        &admission.run_id,
                        PauseReason::AccountLimit,
                        Some(*error),
                        Some(pause_on),
                        None,
                    )
                    .await;
                }
                // No timed pause, so the run would end here: when the refused attempt hit a quota
                // wall, routing may move the run to another account instead, and the step is
                // entered again there. Otherwise routing's answer, if it gave one, goes on the
                // error the run ends with.
                let mut error = error;
                if let Some(limited) = limited.as_deref() {
                    // The accounts that already refused this step: carried across a move in
                    // this process, else read from the run's records, since a run paused or
                    // crashed after a move resumes the step with nothing in memory.
                    let mut refused = match entering.as_ref() {
                        Some(entering) => entering.refused.clone(),
                        None => crate::account_move::refused_in_step(
                            &writer.open_log_records().await?,
                            &admission.run_id,
                            step_id,
                        ),
                    };
                    match crate::account_move::at_wall(
                        writer,
                        parts,
                        seams,
                        admission,
                        step_id,
                        limited,
                        error.retry_after_secs,
                        &mut refused,
                    )
                    .await?
                    {
                        Some(crate::account_move::AtWall::Moved(moved)) => {
                            let accepted_under = match entering {
                                Some(entering) => entering.accepted_under,
                                None => Box::new(admission.clone()),
                            };
                            *moving = Some(crate::account_move::Moving {
                                moved,
                                reentry: crate::account_move::Reentry {
                                    step_id,
                                    refused,
                                    retries,
                                    first_attempt: limited.attempt.attempt.saturating_add(1),
                                    accepted_under,
                                    overflow: None,
                                },
                            });
                            return Ok(finished(writer, RunFinishReason::Interrupted));
                        }
                        Some(
                            crate::account_move::AtWall::Stayed(Some(answer))
                            | crate::account_move::AtWall::Held(answer),
                        ) => {
                            crate::account_move::with_routing_answer(
                                &mut error,
                                answer,
                                (seams.now_ms)(),
                            );
                        }
                        // A cancel ends the run as cancelled. A stop pauses it for the restart when
                        // its policy is to resume, and the resume asks for this step again; without
                        // that policy the run ends with the provider's error, as it would have had
                        // routing not been asked.
                        Some(crate::account_move::AtWall::Stopped) => {
                            if seams.cancel.is_cancelled() {
                                return finish(
                                    writer,
                                    seams,
                                    &admission.run_id,
                                    RunFinishReason::Cancelled,
                                )
                                .await;
                            }
                            if admission.on_restart == Some(OnRestart::Resume) {
                                return pause(
                                    writer,
                                    seams,
                                    &admission.run_id,
                                    PauseReason::Restart,
                                    None,
                                    None,
                                    None,
                                )
                                .await;
                            }
                        }
                        Some(
                            crate::account_move::AtWall::Stayed(None)
                            | crate::account_move::AtWall::Ignored,
                        )
                        | None => {}
                    }
                }
                let now = (seams.now_ms)();
                let mut record = run_end(writer, &admission.run_id, RunFinishReason::Error, now);
                if let Record::RunEnd(end) = &mut record {
                    end.error = Some(
                        serde_json::to_value(&*error)
                            .map_err(|e| LoopError::Parts(e.to_string()))?
                            .into(),
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
        // What streamed live, kept only when the answer can still change before it is stored.
        let streamed = (hooks.is_some() || finalizing).then(|| step.message.content.clone());
        let step_hooks = match hooks.as_ref() {
            Some(runtime) => post_assistant(runtime, step_id, &mut step.message).await,
            None => None,
        };
        // A cut call on a budget's finalize step or in a structured run needs handling the run
        // loop hasn't built, so it is refused by name rather than run without it.
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
        // Words that streamed live and won't be stored are abandoned: the display lane is reset
        // and given the answer as it will be stored.
        if streamed.is_some_and(|streamed| streamed != step.message.content) {
            let mid = display::step_mid(writer);
            restream(seams.out, &step.message, mid.as_deref(), live_display).await;
        }
        // What the step records about the refusal fallback. A refusal after a completed tool
        // call commits as a tool step and arms the schedule.
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
        // request on itself.
        let whole = step.served.total(step.usage);
        // A call whose arguments the provider cut is answered with an error in the step's
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
                shown_tags: shown_tags.clone(),
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
                        message: Arc::new(assistant),
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
                        fallback: (step.served.members(refusal)?).map(Into::into),
                        hooks: (step_hooks).map(Into::into),
                        budget_finalize: finalizing.then_some(true),
                        // The total across iterations, when it differs from the recorded usage.
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
        *live_display = false;
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
        // dispatched or a stop condition is weighed.
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
            &shown_tags,
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
            BatchOutcome::Closed { interrupted: false } => {}
            BatchOutcome::Closed { interrupted: true } => {
                return finish_in_doubt(writer, seams, &admission.run_id).await;
            }
            BatchOutcome::Held(finished) | BatchOutcome::Stopped(finished) => return Ok(finished),
            BatchOutcome::Cancelled(closed) => {
                let finished =
                    finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await?;
                withdraw(seams, &closed).await;
                return Ok(finished);
            }
            BatchOutcome::InDoubt(closed) => {
                let finished = finish_in_doubt(writer, seams, &admission.run_id).await?;
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

/// The session's refusal-fallback schedule the run carries: only under the same model and within
/// a day of its last change.
fn refusal_schedule<W: Writer>(
    writer: &W,
    admission: &Admission,
) -> Result<Option<Value>, LoopError> {
    broca_core::refusal_fallback::on_episode_admitted(
        crate::refusal::schedule(writer)?,
        &admission.model,
        admission.ts_ms,
    )
    .map(serde_json::to_value)
    .transpose()
    .map_err(|e| LoopError::Parts(e.to_string()))
}

/// Writes the run's start, naming the setup parts [`store_setup`] stored. `refusal_schedule` is
/// what [`refusal_schedule`] decided. `checkpoint` is the highest part number the session has
/// shown ([`shown_high`]), which the run's computed numbering continues after. `enrolled` is the
/// record of the session's move onto the current compaction contract when this run makes it,
/// which its start's batch carries.
async fn admit<W: Writer>(
    writer: &mut W,
    admission: &Admission,
    setup: Setup,
    refusal_schedule: Option<Value>,
    checkpoint: u64,
    enrolled: Option<broca_session_store::record::CompactionEnrolled>,
) -> Result<u64, LoopError> {
    let Setup {
        parts: setup,
        manifest_rest,
        render_identity,
    } = setup;
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
    // Computed tag rules start after the highest number the session has shown.
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
    // The members the current run config serializes only when set.
    for (key, value) in [
        (
            "turn_directives_v1",
            to_value(admission.directives.as_ref())?,
        ),
        ("work_class", to_value(admission.work_class.as_ref())?),
        ("account_route", to_value(admission.account_route.as_ref())?),
        (
            "served_provider",
            to_value(admission.served_provider.as_ref())?,
        ),
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
    if !admission.transport.is_empty() {
        choices.insert("transport".into(), json!(admission.transport));
    }
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
        render_identity,
        named_speaker: broca_session_store::record::marks_name_speaker(message_marks.as_ref()),
        kept_notice: broca_session_store::record::marks_kept_notice(message_marks.as_ref()),
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
            .as_ref()
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
            message: Arc::new(message.clone()),
            written_at_ms: admission.written_at_ms,
            submission_id: None,
            notice: None,
            tags_v1: None,
            receipt_seq: None,
            pruned: Vec::new(),
        }));
    }
    records.extend(plan_notes::change_records(admission, writer));
    // So is the session's enrollment, in the same batch as the start that moves it onto the
    // contract: once the batch commits, the session is enrolled for good.
    if let Some(enrolled) = enrolled {
        records.push(Record::CompactionEnrolled(enrolled));
    }
    // And the storage policy the send freezes: the batch that holds the send holds its policy.
    if let Some(policy) = &admission.send.retention_policy {
        records.push(Record::Retention(broca_session_store::record::Retention {
            event: broca_session_store::record::RetentionEvent::Set,
            at_ms: admission.written_at_ms.or(admission.ts_ms).unwrap_or(0),
            policy: Some(policy.clone()),
        }));
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
/// continues after: `high_after` over the prompt, with each message numbered under the rules
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
    // The run that placed a message is the last one starting at or before it. Runs are in the
    // order they started and messages in the order they were placed, so one pass finds each.
    let mut placing = (false, false);
    let mut next_run = runs.iter().peekable();
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
        while let Some((_, render, clean)) = next_run.next_if(|(first, _, _)| *first <= index) {
            placing = (*render, *clean);
        }
        let (render, clean) = placing;
        high = crate::window_shape::runner_high(message, ComputedRules { render, clean }, high)
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
            // Positions count from the session's first message: once history is sealed at a cut,
            // the window's first message keeps its place.
            let index = |ordinal: u64| (ordinal - 1) as usize;
            Ok(EpisodeRenderInput {
                prompt_start: index(episode.start),
                first_user: episode.first_user.map(index),
                frozen_config: match &episode.render_config {
                    Some(part) => crate::parts::value(parts, part).map_err(LoopError::Parts)?,
                    None => Value::Null,
                },
            })
        })
        .collect()
}

/// The request history every step of a run carries: [`render_episodes`], and the signed floor
/// folded from the same runs' starts (`broca_core::signed_floor`), using their `RunStarted`
/// records.
///
/// Past the window, the conversation keeps only the runs whose render key changes (what renderers
/// read from a run's render config to render another run's messages: the wire family, model,
/// reasoning effort and thinking mode; see `episode_render_key`), so a run that changed only the
/// tool set can be missing there and the fold may find a different change point than one over
/// every run. That never changes a request: such a point is below the window, where no message
/// the request renders sits, so either floor drops the same reasoning.
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
/// runs' starts using `RunStarted` records. [`render_history`] says how far past the window that
/// fold reaches.
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
                let digest = start_config::tool_set_of_start(&start, parts)?;
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

/// The request for step `step_id`: the conversation as the folds hold it, under the run's
/// frozen settings, with the render inputs the run loop gives a request: each message's
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
    whole: Option<&broca_session_store::fold::conversation::Conversation>,
) -> Result<CallOptions, LoopError> {
    let contract = admission.compaction_plan_version.is_some();
    let overflow = |_| LoopError::Unsupported("part numbers past the numbering limit");
    let mut kept = kept.filter(|_| contract && compacted);
    let mut prompt = match kept.as_mut() {
        Some(kept) => {
            kept.update(
                writer,
                episodes,
                enrollment,
                crate::repetition::after_input(&admission.render_config),
                whole,
            )?;
            Vec::new()
        }
        // A session on the current compaction contract renders its numbers from the entries.
        None if contract => {
            crate::compaction::prompt::taken(writer.conversation().messages.len());
            crate::compaction::prompt::contract_prompt(
                writer.conversation(),
                episodes,
                enrollment,
                crate::repetition::after_input(&admission.render_config),
                &writer.session_id(),
            )?
        }
        None => {
            // The request's one copy of the window, which the render inputs below are written into.
            let mut prompt: Vec<Message> = writer
                .conversation()
                .held_messages()
                .map_err(crate::compaction::not_held)?
                .iter()
                .map(|message| Message::clone(message))
                .collect();
            // Off the compaction contract, a ride renders into its result's copy here, before
            // anything else edits the results. On the contract, the entries render it.
            crate::entries::render_rides(
                &mut prompt,
                writer.conversation(),
                rides_after_results(admission),
            );
            crate::repetition::repetition_guidance(
                &mut prompt,
                episodes,
                writer.conversation().window_start() - 1,
                writer.conversation().window_repetition.as_ref(),
                crate::repetition::after_input(&admission.render_config)
                    .then_some((writer.conversation(), writer.session_id())),
            )?;
            // Each message keeps its place in the session, which the window starts after once
            // history is sealed: the window's first message has the 0-based position
            // `window_start - 1`.
            let first = writer.conversation().window_start() - 1;
            for (index, message) in prompt.iter_mut().enumerate() {
                message.ordinal = Some(first + index as u64);
            }
            // A session on the established layout numbers each run's messages by that run's
            // frozen rules, in the order they were placed, and renders them in place.
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
        // its numbers in front.
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
    opts.response_format = Arc::clone(&admission.response_format);
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
    // On the current compaction contract, a request renders from the entries: text delivered with
    // a tool result is added to the result's copy, and the system text goes above the conversation.
    // This render copies the stored prompt rather than rendering through a view's plan, so the
    // placeholders the prompt holds for the global system messages an enrollment suppressed
    // (`compaction::prompt::contract_prompt`) come out of it.
    let conversation = writer.conversation();
    let entries = broca_session_store::session::EnrollmentOverlay::serve(
        enrollment,
        broca_session_store::session::window_entries(conversation, 1, usize::MAX)
            .map_err(crate::compaction::not_held)?,
    )
    .iter()
    .map(crate::entries::project)
    .collect::<Result<Vec<_>, _>>()
    .map_err(LoopError::Parts)?;
    let mut rides = crate::entries::ride_envelopes(conversation);
    rides.set_after_results(rides_after_results(admission));
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
    /// cut.
    raw_finish_reason: Option<String>,
    service_tier_used: Option<String>,
    speed_used: Option<String>,
    /// The model that answered when the provider handed the request to another model itself.
    served_model: Option<String>,
    /// One usage entry per model that ran, when the provider reported them.
    usage_iterations: Option<Vec<broca_protocol::usage::UsageIteration>>,
}

impl Served {
    /// The step end's `fallback` members: the facts the step records about the refusal fallback,
    /// in the `StepFallback` shape. `None` when there are none.
    fn members(&self, refusal: crate::refusal::Committed) -> Result<Option<Value>, LoopError> {
        let fallback = broca_records::record::StepFallback {
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

    /// The step's usage split by the model each part counts under, using
    /// `StepServing::usage_by_model`, as `StepEnd::by_model` and `StepRefused::by_model` record it:
    /// `route_model` is the refusal fallback's model when the step was routed there.
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
/// `Finished`.
fn incomplete_stream() -> ProviderError {
    ProviderError {
        provider_details: None,
        cause: None,
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
        account_continue: None,
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
/// Asks the provider for one step. Each HTTP attempt's evidence is appended as the attempt ends,
/// without a barrier; billing and cache evidence read it. The outer error is the session store's;
/// the inner one is the step's. `first_attempt` is the ordinal the call's first attempt is
/// recorded with: zero, or after the attempts a step entered again after an account move already
/// recorded.
#[allow(clippy::too_many_arguments)]
async fn model_step<W: Writer, P: Provider, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    opts: Arc<CallOptions>,
    requested_ttl_ms: Option<u64>,
    account_limit_pause: bool,
    first_attempt: u32,
    selection: Option<&broca_records::record::AuthSelection>,
    live_display: &mut bool,
) -> Result<Result<Step, StepFailure>, LoopError> {
    let mut limited: Option<Box<FailedAccountLimitAttempt>> = None;
    // The step is in flight from here until the call's stream ends: published for the session's
    // owner, and cleared however the call ends.
    struct InFlight<'a>(Option<&'a broca_core::provider::LiveStepProgress>);
    impl Drop for InFlight<'_> {
        fn drop(&mut self) {
            if let Some(progress) = self.0 {
                *progress.lock().unwrap() = None;
            }
        }
    }
    let _in_flight = InFlight(seams.step_progress.as_ref());
    let floor = crate::compaction::floor_through(writer, &[], opts.render_episodes.signed_floor);
    let request = RequestFacts::of(&opts, floor.await?);
    let context = AttemptContext {
        requested_ttl_ms,
        account_limit_pause,
        request,
    };
    // A run's keep-warm may ask its calls to note each request their transport sends on its
    // seal, so a warm re-sends those bytes.
    let send_seal = seams
        .keep_warm
        .as_ref()
        .and_then(|driver| driver.step_seal());
    // The call's options are shared with the provider. The published progress names them
    // weakly, so the session's owner can size the request in flight without keeping it.
    if let Some(progress) = &seams.step_progress {
        let now = (seams.now_ms)();
        *progress.lock().unwrap() = Some(broca_protocol::provider::InFlightStep {
            retry_awaits: None,
            report_awaits: true,
            step_id: StepId(step_id),
            started_at_ms: now,
            attempt: 0,
            attempt_started_at_ms: now,
            last_content_at_ms: None,
            max_content_gap_ms: None,
            max_content_gap_ended_at_ms: None,
            request: broca_protocol::provider::InFlightRequest::of(&opts),
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
        first_attempt,
        attempt_hook: None,
    };
    // A cancel drops the call while it hasn't returned its stream, so a call slow to answer
    // never holds the run's end back.
    let mut events = tokio::select! {
        biased;
        _ = seams.cancel.cancelled() => return Ok(Err(StepFailure::Cancelled)),
        events = seams
            .provider
            .step_with_runtime(StepId(step_id), opts, runtime) => events,
    };
    let mut message = None;
    // The provider's `StepFinished` usage wins; its final `Finished` fills it only when the
    // step reported none.
    let mut step_usage = Usage::default();
    let mut served = Served::default();
    let mut cuts = Vec::new();
    let mut join = display::ToolInputJoin::default();
    let step_mid = display::step_mid(writer);
    let outcome = loop {
        // Biased: the cancel is checked before each event, as broca-core's run loop checks it
        // before its step.
        let deadline = join.deadline();
        let event = tokio::select! {
            biased;
            _ = seams.cancel.cancelled() => break Err(StepFailure::Cancelled),
            event = events.recv() => event,
            () = display::until(deadline) => {
                join.flush(seams.out, step_mid.as_deref()).await;
                continue;
            }
        };
        // A held tool-input delta goes out before anything else the step does.
        if !matches!(event, Some(NormalizedEvent::Display(_))) {
            join.flush(seams.out, step_mid.as_deref()).await;
        }
        match event {
            Some(NormalizedEvent::Display(display)) => {
                display::note_live(&display, live_display);
                if let Some(out) = seams.out {
                    for display in join.take(display, tokio::time::Instant::now()) {
                        let display = display::stamp(display, step_mid.as_deref());
                        let _ = out.send(OutputEvent::Display(display)).await;
                    }
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
                // The request's measured input labels its send, which a warm's price reads.
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
                limited =
                    record_attempt(writer, seams, run_id, step_id, attempt, &context, selection)
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
                            writer, seams, run_id, step_id, attempt, &context, selection,
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
#[allow(clippy::too_many_arguments)]
async fn record_attempt<W: Writer, P: Provider, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    step_id: u64,
    mut attempt: broca_protocol::ModelAttempt,
    context: &AttemptContext,
    selection: Option<&broca_records::record::AuthSelection>,
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
            &[attempt_record(run_id, step_id, attempt.clone(), context)?],
            durable,
        )
        .await?;
    seams
        .provider
        .cache_attempt_recorded(Some(event_id(writer, seq)));
    if let Some(reports) = &seams.account_reports {
        reports.attempt_recorded_on(&event_id(writer, seq), seq, &attempt, selection);
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
    /// The provider held it while its batch ran one call at a time (`ToolHeld::sequential`), so
    /// the calls after it wait for its result.
    pub held_sequential: bool,
    /// The input its intent recorded, when an approval policy narrowed it; sent instead of the
    /// model's.
    pub args: Option<serde_json::Value>,
}

/// How a tool batch ended.
enum BatchOutcome {
    /// Every call has its outcome; `interrupted` when the run must stop for review after the
    /// batch: a call may have run without its result coming back (feeding that call's failure
    /// to the model would invite it to run the call again), or the batch's scope was gone when
    /// it started, so none of its calls could run and later ones won't either.
    Closed { interrupted: bool },
    /// Only calls a provider holds were still waiting, so the run paused for their late
    /// results.
    Held(Finished),
    /// The caller cancelled while a provider held calls: they were closed and the run must end
    /// cancelled, then ask each provider to withdraw its call.
    Cancelled(Vec<ProviderHeldReply>),
    /// A call's outcome is unknown while a provider held others: they were closed and the run
    /// must end interrupted for review, then ask each provider to withdraw its call.
    InDoubt(Vec<ProviderHeldReply>),
    /// A stop or the caller's cancel cut a pre-tool hook short: the batch closed unsent and
    /// the run ended.
    Stopped(Finished),
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
    // The part numbers the step's request showed, which the batch's start records.
    shown_tags: &[broca_session_store::record::TagRange],
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
    let guard = ToolOpenGuard::new(Some(&seams.cancel), Some(&seams.stop))
        .with_person_steers(seams.person_steers.clone());
    let refusal = seams
        .tools
        .batch_started_guarded(&ctx, planned, manifest, &guard)
        .await
        .err();
    // The scope was gone when the batch started: each call gets its `route_drained` result
    // (`plan_call`) and the run stops interrupted after the batch.
    let scope_drained = matches!(
        refusal,
        Some(broca_core::tool_plane::ToolPlaneError::ScopeDrained { .. })
    );
    // A resumed batch runs in the order its first dispatch did. Only its unanswered calls are
    // given here, and the call that made it sequential may already be answered; a held call
    // recorded whether later calls wait for it, and they still do.
    let sequential = known.iter().any(|known| known.held_sequential)
        || calls.iter().any(|call| {
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
            call.input = args.into();
        }
    }
    let Some(plans) = plan_calls(
        &mut calls,
        known,
        refusal.as_ref(),
        manifest,
        seams.approval,
        hooks,
        step_id,
        &removed_tools(writer.operations()),
    )
    .await?
    else {
        // A pre-tool hook was cut short, so nothing of the batch was written or sent. The
        // plane, told of the batch above, is told it ended; the batch then closes as a stop
        // before gating closes it. A resumed batch already has records of its own, which that
        // close-out would repeat.
        let _ = seams.tools.batch_finished(&ctx).await;
        if started || !known.is_empty() {
            return Err(LoopError::Unsupported(
                "a pre-tool hook cut short by a stop in a resumed batch",
            ));
        }
        let cancelled = seams.cancel.is_cancelled();
        return stopped_batch(writer, seams, run_id, step_id, planned, cancelled)
            .await
            .map(BatchOutcome::Stopped);
    };
    let calls = &calls[..];
    let session = writer.session_id();
    let now = (seams.now_ms)();
    let mut prepared = Vec::with_capacity(calls.len());
    for (call, plan) in calls.iter().zip(&plans) {
        let intent = match plan {
            CallPlan::Dispatch(_) if known_of(call).is_none() => {
                // Arguments are stored again only when they differ from the input the message
                // last gave this call id.
                let last = planned
                    .iter()
                    .rev()
                    .find(|other| other.tool_call_id == call.tool_call_id)
                    .map(|other| &other.input);
                Some((
                    canonical(&(call.input).parse())
                        .map_err(LoopError::Parts)?
                        .1,
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
                        shown_tags: shown_tags.to_vec(),
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
                                schema_pin: (schema_pin.as_ref().map(|pin| {
                                    serde_json::to_value(pin).expect("a pin serializes")
                                }))
                                .map(Into::into),
                                hooks: (hooks.clone()).map(Into::into),
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
                    // after its end.
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
                    // closes its batch.
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
        return Ok(BatchOutcome::Closed {
            interrupted: scope_drained,
        });
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
    // While the calls run, the run's keep-warm refreshes the prompt cache. A wait resumed after
    // a restart counts the warms it already sent toward its caps.
    let spend = warming.map_or(0.0, |warming| warming.spent(writer.operations(), step_id));
    let sent = warming.map_or(0, |_| warm_count(writer.operations(), step_id));
    let dispatched = broca_core::keep_warm_wait::wait_tools_with_spend(
        dispatch_all(writer, seams, &plan),
        warming.map(|warming| warming.wait(&seams.cancel, sent)),
        spend,
    )
    .await?;
    let (answers, paused, closed) = match dispatched {
        Dispatched::All { answers, closed } => (answers, false, closed),
        Dispatched::Held(answers) => (answers, true, Vec::new()),
    };
    // Calls a provider still holds. A synthesized terminal for a repeat of one of their ids
    // waits until the held call's own result is stored: an outcome names only its id, and
    // reading the session back gives it to the first copy still open, which would be the held
    // call.
    let still_held: std::collections::HashSet<&ToolCallId> = sending
        .iter()
        .zip(&answers)
        .filter(|(_, answer)| answer.is_none())
        .map(|(call, _)| &call.tool_call_id)
        .collect();
    // The answers, back in the message's order, with each synthesized terminal in its place.
    let mut answers = answers.into_iter();
    let mut records = Vec::new();
    let mut unknown = false;
    for plan in &plans {
        match plan {
            // Its answer was appended when it arrived.
            CallPlan::Dispatch(_) => {
                let Some(answer) = answers.next().flatten() else {
                    continue;
                };
                unknown |= answer.outcome == CallOutcome::Unknown;
            }
            CallPlan::Synthetic { result, .. }
                if paused && still_held.contains(&result.tool_call_id) => {}
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
        let finished = pause(
            writer,
            seams,
            run_id,
            PauseReason::ToolHeld,
            None,
            None,
            Some(&ctx),
        )
        .await?;
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
    // Budget firings caused by the step's calls go in the barrier that closes its batch; a batch
    // that closes held calls on a cancel makes none. Queued sends ride on its results in the same
    // barrier, after its end, read as it closes.
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
        // Held calls close on the caller's cancel, or because another call's outcome is unknown.
        return Ok(if unknown && !seams.cancel.is_cancelled() {
            BatchOutcome::InDoubt(closed)
        } else {
            BatchOutcome::Cancelled(closed)
        });
    }
    Ok(BatchOutcome::Closed {
        interrupted: unknown || scope_drained,
    })
}

/// A call that was never sent: its no-dispatch result in the transcript and its reason in
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
        yielded_by: None,
        run_id: run_id.into(),
        batch_id: batch_id.into(),
        tool_call_id: id.clone(),
        outcome: CallOutcome::NotDispatched,
        result_digest: digest,
        is_error,
        reason: None,
        finished_at_ms: Some(now),
        detail: (serde_json::to_value(reason).ok()).map(Into::into),
        hooks: hooks.map(Into::into),
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
        outcome.hooks = (hooks).map(Into::into);
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
/// A terminal written without a time, such as a synthesized one, has none here.
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
        yielded_by: None,
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

/// The account the run uses: the one routing gives it now, else the one it froze at admission.
pub(crate) fn run_selection<'a, P, T>(
    seams: &'a Seams<'_, P, T>,
    admission: &'a Admission,
) -> Option<&'a broca_records::record::AuthSelection> {
    seams
        .auth_selection
        .as_ref()
        .or(admission.auth_selection.as_ref())
}

/// The step's request for the model it goes to, the run's own or the refusal-fallback model,
/// with its output cap (see `output::reserve`).
fn reserve_for<W: Writer>(
    writer: &W,
    step_id: u64,
    mut opts: CallOptions,
    route: &crate::refusal::Route,
) -> CallOptions {
    if route.route == broca_core::refusal_fallback::StepRoute::Fallback {
        // A fallback step tells the renderer which model it renders for.
        opts.target_model = route.fallback.clone();
    }
    crate::output::reserve(writer.operations(), step_id, opts)
}

/// The timed same-account pause the run's state holds, if it is paused on one.
fn account_limit_of<W: Writer>(
    writer: &W,
) -> Result<Option<broca_records::record::AccountLimitPause>, LoopError> {
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
        end.error = Some(
            serde_json::to_value(error)
                .map_err(|e| LoopError::Parts(e.to_string()))?
                .into(),
        );
        end.interrupt_reason = interrupt_reason;
    }
    append_end(writer, run_id, vec![record]).await?;
    Ok(finished(writer, reason))
}

/// Records a refused restart continuation and its interrupted terminal without dispatching
/// anything. Set `record_resume` when the attempt failed before recording a resume entry;
/// owners that already recorded it keep the existing entry instead of appending a duplicate.
pub async fn end_restart_resume_refused<W: Writer>(
    writer: &mut W,
    run_id: &str,
    error: &ProviderError,
    now_ms: u64,
    record_resume: bool,
) -> Result<Finished, LoopError> {
    if record_resume {
        let resolution = run_state(writer).resolution();
        let mut batch = Some(vec![Record::Resumed(resumed_record(writer, &resolution))]);
        writer
            .append_with(
                &mut |_, _, first_seq| {
                    let mut batch = batch.take().expect("one refused resume entry");
                    name_resumed_from(&mut batch, first_seq);
                    batch
                },
                false,
            )
            .await?;
    }
    let mut record = run_end(writer, run_id, RunFinishReason::Interrupted, now_ms);
    if let Record::RunEnd(end) = &mut record {
        end.interrupt_reason = Some("runner_crash".into());
        end.error = Some(
            serde_json::to_value(error)
                .map_err(|e| LoopError::Parts(e.to_string()))?
                .into(),
        );
    }
    append_end(writer, run_id, vec![record]).await?;
    Ok(finished(writer, RunFinishReason::Interrupted))
}

/// The timed pause on the run's own account that `limited`, the step's attempt the provider refused
/// for the account's limit, allows: only for a transient error, only when the run consented at
/// admission, and only when the account resets before the session's cache on it expires and within
/// the boot cap.
fn account_limit_pause<P: Provider, T>(
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    step_id: u64,
    limited: &FailedAccountLimitAttempt,
    error: &ProviderError,
) -> Option<Box<broca_records::record::AccountLimitPause>> {
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

/// The cache lifetime the run's requests ask the provider for, when they ask for one it honours, is
/// read from the run's frozen render config. Only this lifetime can prove the cache expired.
pub(crate) fn requested_cache_ttl(admission: &Admission) -> Option<u64> {
    let frozen: broca_provider::wire::FrozenRenderConfig =
        serde_json::from_value(Value::clone(&admission.render_config)).ok()?;
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
/// started with the calls already answered. The answered calls keep their results, and a call
/// that has one isn't counted as the submit.
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
/// run's end, in one barrier.
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
        broca_protocol::structured_output::result(
            frozen,
            true,
            Some(&(submit.input).parse()),
            None,
            skipped,
        ),
    )];
    if !started {
        records.push(Record::BatchStart(BatchStart {
            run_id: run_id.into(),
            batch_id: batch_id.clone(),
            step_id,
            planned: calls.iter().map(|call| call.tool_call_id.clone()).collect(),
            mid: None,
            shown_tags: Vec::new(),
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

/// A queued send with a delivery order, as a boundary delivers it.
struct OrderedSend {
    order: DeliveryOrder,
    /// The user messages a `steer` or `turn_system` delivery places: the input after the send's
    /// `pre_user` hooks, without system messages.
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

/// The module a principal names, or `broca` if there is none.
fn module_or_broca(principal: Option<&broca_protocol::BindPrincipal>) -> String {
    match principal {
        Some(broca_protocol::BindPrincipal::Reserved { module_id }) => module_id.clone(),
        _ => "broca".into(),
    }
}

/// The queue's sends that carry a delivery order, read from their records: the order, the input,
/// who sent each send, and when it was accepted.
async fn ordered_sends<W: Writer>(
    writer: &mut W,
) -> Result<std::collections::BTreeMap<String, OrderedSend>, LoopError> {
    // The session's actor queues sends while the run waits on its model or its tools, so the
    // run's copy of the queue may lag. Without catching up, a send queued while a tool ran
    // would miss that tool's results and wait for a later boundary.
    writer.caught_up().await?;
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
        let receipt_value = queued
            .receipt
            .as_ref()
            .map(broca_protocol::json::Json::parse);
        let receipt = |name: &str| receipt_value.as_ref().and_then(|receipt| receipt.get(name));
        let queued_by = principal(receipt("sent_by"))?;
        let queued_as = principal(receipt("principal"))?;
        // A ride names the module its queued record says sent it; the notice names the one its
        // order says.
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
            .map(broca_protocol::json::Json::parse)
            .and_then(|receipt| receipt.get("hooks")?.get("final_value").cloned())
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

/// The records that deliver queued sends at the close of a tool batch: an expired send ends, and
/// one whose first possible mode here is `tool_result` rides on the batch's carrier result
/// (`ResultAppend` with its envelope, then `Delivery`), in queue order. Every other send waits for
/// the next boundary. `batch` is the closing batch so far, whose replies place the turn's results
/// unless an earlier batch placed them; `system_after_results` is whether the run's route would
/// take a turn-scoped system message right after them, which a send listing `turn_system` first
/// waits for. On the compaction contract the carrier is the batch's last result, whatever it
/// holds; off it, the last text or error result.
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
        .newest()
        .or(conversation.newest())
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
            // Who the text came from is an entry fact of the compaction contract; outside the
            // contract, the envelope alone names the sender.
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
                // The agent its module spoke for, according to the queued send's marks.
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
/// results; queried from the provider before a batch commits.
fn system_after_results<P: Provider>(provider: &P) -> bool {
    provider.turn_system_possible(Some(&Message::new(Role::Tool, Vec::new())), false)
}

/// The messages a `turn_system` delivery of `send` places: its text as turn-scoped system text
/// (`broca_core::delivery_order::system_messages`).
fn turn_system_input(send: &OrderedSend, submission_id: &str) -> Vec<Message> {
    broca_core::delivery_order::system_messages(&broca_core::delivery_order::OrderedSend {
        admitted_at_ms: send.written_at_ms,
        submission_id: broca_protocol::SubmissionId(submission_id.into()),
        order: send.order.clone(),
        principal: None,
        input: send.input.clone(),
    })
}

/// The result a call gets when the run stopped before the call was dispatched.
const NOT_DISPATCHED: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// Asks each held call's provider to withdraw it, once the run's end is durable. Best effort:
/// a failure can't reopen the call, and a late answer for it is already ignored.
async fn withdraw<P, T>(seams: &Seams<'_, P, T>, closed: &[ProviderHeldReply]) {
    if let Some(lifecycle) = seams.held {
        for reply in closed {
            let _ = lifecycle.withdraw_held(reply).await;
        }
    }
}

/// A completed result keeps only the media the batch's capture holds for that exact call; every
/// other media part becomes a provenance marker before hooks or the record see the result. A
/// reference to stored bytes, or one with the right digest and other metadata, isn't evidence
/// that this call produced them.
fn checked(ctx: &BatchCtx, mut answer: DispatchResult) -> DispatchResult {
    if let DispatchResult::Completed(result) | DispatchResult::Yielded { result, .. } = &mut answer
    {
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
                    Some(text.to_string())
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

/// A provider's result through the post-tool hooks subscribed for its tool: a JSON result stays
/// hookless, and a hook may edit the text or, when it was required and unavailable, withhold the
/// result.
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
        detail: None,
        hooks: hooks.map(|hooks| serde_json::to_value(hooks).expect("hook metadata serializes")),
        yielded_by: None,
    }
}

/// The run's state as the operational fold holds it. Admission appended its start, so it
/// exists for the whole run.
/// Ends the run when its compaction provider can't serve it: an error with code
/// `compaction_unavailable`, before any model call of the step.
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
                .map_err(|e| LoopError::Parts(e.to_string()))?
                .into(),
        );
    }
    append_end(writer, &admission.run_id, vec![record]).await?;
    Ok(finished(writer, RunFinishReason::Error))
}

/// The compaction state and plane a run uses, when its session is on the current compaction
/// contract and its plan names a compaction provider. The state is rebuilt from the open log
/// the first time a session uses it, then kept by whoever owns the session; a fresh run starts
/// its step history again.
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
        let state = crate::compaction::restore_live(writer, &seams.bind).await?;
        session.restore(state).await;
    }
    session.claim_serving_start();
    if fresh {
        session.state.lock().await.begin_run();
    }
    let has_view = session.state.lock().await.applied().is_some();
    session.runtime.begin_send(has_view).await;
    // The run keeps one transcript for its life; its owner gets a weak handle on it, once.
    let window = crate::compaction::WindowTranscript::default();
    writer.share_window(window.downgrade());
    Ok(Some(crate::compaction::Ctx {
        earlier: crate::compaction::earlier_context(writer, admission, parts).await?,
        session,
        provider,
        preset,
        window,
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

pub(crate) fn run_state<W: Writer>(writer: &W) -> &broca_session_store::fold::operations::Run {
    writer
        .operations()
        .run
        .as_ref()
        .expect("the run's start is appended before any step")
}

/// The run's end record. Its usage is the run's total, present once a step completed.
fn run_end<W: Writer>(writer: &W, run_id: &str, reason: RunFinishReason, now: u64) -> Record {
    let run = run_state(writer);
    // The end record always carries the run's usage, empty when it took no step.
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
        sign_in: None,
    }
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

/// Ends a run a crash left idle: its last step answered without calls, and the run's end was
/// never written. A completed run admitted with structured output records what it produced with
/// that end, as the step would have: the step writes both in one barrier, so neither is durable.
async fn finish_idle<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    reason: RunFinishReason,
) -> Result<Finished, LoopError> {
    let mut records = Vec::new();
    if reason == RunFinishReason::Completed {
        if let Some(frozen) = structured_of(admission) {
            let text = (frozen.mode == broca_protocol::structured_output::Mode::FinalText)
                .then(|| writer.conversation().newest().cloned())
                .flatten()
                .filter(|message| message.role == Role::Assistant)
                .map(|message| broca_core::structured_output::joined_text_of(&message.content));
            records.push(structured_result(
                &admission.run_id,
                broca_protocol::structured_output::result(
                    &frozen,
                    true,
                    None,
                    text.as_deref(),
                    Vec::new(),
                ),
            ));
        }
    }
    records.push(run_end(writer, &admission.run_id, reason, (seams.now_ms)()));
    append_end(writer, &admission.run_id, records).await?;
    Ok(finished(writer, reason))
}

/// Ends a run whose batch left a call that may have run without its result: the call is never
/// sent again, so the run stops. The caller's cancel ends it cancelled, since interrupted names a
/// stop nobody asked for; the runner's shutdown ends it interrupted, saying so, as a stop at a
/// boundary does; anything else ends it interrupted.
async fn finish_in_doubt<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
) -> Result<Finished, LoopError> {
    let reason = if seams.cancel.is_cancelled() {
        RunFinishReason::Cancelled
    } else {
        RunFinishReason::Interrupted
    };
    let mut record = run_end(writer, run_id, reason, (seams.now_ms)());
    if let Record::RunEnd(end) = &mut record {
        if reason == RunFinishReason::Interrupted && seams.stop.is_cancelled() {
            end.interrupt_reason = Some("runner_shutdown".into());
        }
    }
    append_end(writer, run_id, vec![record]).await?;
    Ok(finished(writer, reason))
}

/// Appends `records`, which end with the run's end, as one durable batch. Messages steered or
/// delivered since the last boundary enter the conversation just before the end: the run ends
/// with them in its conversation.
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
                        message: Arc::new(message.clone()),
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
