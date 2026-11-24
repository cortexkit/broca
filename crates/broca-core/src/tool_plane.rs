//! The tool-dispatch seam. The loop dispatches tool calls ONLY through this
//! trait, so the deterministic in-process stub and the real subc consumer
//! connection (runner → subc → AFT) are two impls of one contract, never a
//! rewrite.
//!
//! The trait deliberately models the REAL subc dispatch contract so the stub
//! can't diverge:
//!  - **batch lifecycle**: `batch_started{call_ids}` → per-call `dispatch` →
//!    `batch_finished` brackets one assistant turn's tool calls as an atomic unit.
//!  - **tool_call_id fence**: a repeated `tool_call_id` returns the PRIOR result
//!    and never re-executes — the at-most-once guarantee that lets resume safely
//!    re-dispatch a call whose result wasn't durably recorded.
//!  - **assembled result shape**: `ToolResult { tool_call_id, output, is_error }`.
//!
//! Orchestration the LOOP owns (not the plane): parallel-vs-sequential
//! (`execution_mode`), source ordering of results, the approval gate, and the
//! log durability barrier. The plane executes one call and fences it.

use async_trait::async_trait;
use broca_protocol::{BindIdentity, RunId, SessionScope};
use broca_protocol::{ToolCall, ToolCallId, ToolResult};
pub use broca_records::record::DispatchSchemaPin;

use crate::capability::ToolManifest;

use broca_protocol::BatchId;

/// Per-batch dispatch context. `bind` scopes routes/capabilities to the session.
#[derive(Debug, Clone)]
pub struct BatchCtx {
    pub batch_id: BatchId,
    pub bind: BindIdentity,
    /// The run this batch belongs to. When the daemon reports the run's scope as not live, the
    /// tool plane refuses the rest of this run's calls and remembers that under this id; a
    /// later run of the same session is checked afresh.
    pub run_id: RunId,
    /// The scope the run was admitted under, as its episode's `RunStarted` froze it; `None` for
    /// an unscoped run. A plane opens every provider route for this batch under exactly this
    /// scope, and never opens an unscoped route for a scoped run.
    pub scope: Option<SessionScope>,
    /// Session-owned publication and registration, shared by ordinary and late calls. A plane
    /// captures a result's media only when this is set; without it media arrives as placeholder
    /// text.
    pub media_capture: Option<std::sync::Arc<dyn crate::tool_media::MediaCapture>>,
}

/// A durable intent's identity, prepared as part of one dispatch barrier.
#[derive(Debug, Clone)]
pub struct DispatchRegistration {
    pub tool_call_id: ToolCallId,
    pub call_key: String,
    pub approved: bool,
}

/// Ownership needed to find a call's log without consulting the run index.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct LateResultRegistration {
    pub lineage: String,
    pub call_key: String,
    pub module: String,
    pub bind: BindIdentity,
    pub scope: Option<SessionScope>,
}

/// The intake implementation must commit and sync the entire group before returning success.
/// Registering calls does not start a feed reader: ordinary calls may never be held.
#[async_trait]
pub trait LateResultsIntake: Send + Sync {
    async fn register_group(&self, calls: &[LateResultRegistration]) -> Result<(), String>;

    /// Forget registrations whose calls are settled for good: called once the batch commit
    /// barrier that wrote their terminals is synced. Each pair is `(module, call_key)`. A late
    /// entry for a forgotten key is then a no-op that is acked, which is safe because nothing of
    /// that call can still be delivered.
    async fn prune(&self, _calls: &[(String, String)]) -> Result<(), String> {
        Ok(())
    }

    /// The batch committed these calls' inline terminals; none was ever provider-held.
    /// Implementations may also retire the withdrawal watches created before dispatch.
    async fn prune_inline(&self, calls: &[(String, String)]) -> Result<(), String> {
        self.prune(calls).await
    }
}

/// The first reply's durable metadata. Interim content is deliberately not representable here.
#[derive(Debug, Clone)]
pub struct ProviderHeldReply {
    pub tool_call_id: ToolCallId,
    pub call_key: String,
    pub module: String,
    pub reason: String,
    pub detail_digest: Option<String>,
}

/// The batch's single log writer acknowledges this only after the held record is synced.
#[async_trait]
pub trait ProviderHeldRecorder: Send + Sync {
    async fn sync_held(&self, reply: &ProviderHeldReply) -> Result<(), String>;
}

/// Installed together with intake before a tool route may declare held-reply support.
/// Called only after the batch has synced the held record. The implementation owns the
/// live/paused waiting slot, and returns only the eventual terminal, never interim content.
#[async_trait]
pub trait ProviderHeldLifecycle: Send + Sync {
    /// A deposited result still owned by the waiting slot. Recovery uses this only after
    /// continuation admission is refused, so recording the provider's answer needs no model
    /// credentials or renderer. The slot retains it until `terminal_synced`.
    fn deposited_result(&self, _ctx: &BatchCtx, _reply: &ProviderHeldReply) -> Option<ToolResult> {
        None
    }

    async fn wait(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        reply: &ProviderHeldReply,
    ) -> DispatchResult;

    /// The batch has synced the terminal `wait` returned for this held call. Whoever handed the
    /// late result over may now treat it as delivered (and so let the provider's entry be
    /// acked); before this point a crash could still lose it.
    async fn terminal_synced(&self, _ctx: &BatchCtx, _tool_call_id: &ToolCallId) {}

    /// The batch has synced the terminal of a call its provider, `carrier`, handed back for the
    /// person's steer ([`DispatchResult::Yielded`]). The provider still owes the call's own
    /// terminal, which arrives on its late-results feed and becomes a notice, so that feed must
    /// be read even though no batch waits on the call.
    async fn yielded_synced(&self, _ctx: &BatchCtx, _carrier: &str) {}

    /// Best-effort provider withdrawal after the run's seal is durable. A transport failure
    /// cannot reopen the call; late entries for it are already no-ops.
    async fn withdraw_held(&self, _call: &ProviderHeldReply) -> Result<(), String> {
        Ok(())
    }
}

#[derive(Clone, Copy)]
pub struct DispatchKey<'a> {
    pub call_key: &'a str,
    pub held_recorder: Option<&'a dyn ProviderHeldRecorder>,
}

/// A failure of the tool PLANE itself (transport/route), distinct from a tool
/// EXECUTION failure (which is a `ToolResult { is_error }`, conversation data).
/// The loop stays graceful on these — it synthesizes `is_error` results so the
/// model can adapt; it never hangs or crashes the run.
#[derive(Debug, Clone)]
pub enum ToolPlaneError {
    /// A terminal provider refusal that another attempt on the same route cannot repair.
    Permanent {
        code: String,
        detail: Option<serde_json::Value>,
    },
    /// The plane could not reach the executor after its bounded transport retry
    /// (subc unreachable / route gone / timeout).
    Unavailable(String),
    /// A tool route's open kept being refused with a module-availability code
    /// (`target_unavailable`, `module_reloading`, `module_warming`, `module_timeout`)
    /// for the whole tool-dispatch patience, so the module is restarting or down.
    /// Proven undelivered: nothing was sent. The loop reports it with the stable
    /// `tool plane unavailable` message plus this attribution, so the model can retry
    /// rather than give up.
    ModuleUnavailable {
        module: String,
        code: String,
        detail: String,
    },
    /// A scoped run's route could not be opened for a reason to do with its scope, named by
    /// `code`: `scope_unsupported` (the daemon does not admit scoped routes),
    /// `scope_unverifiable` (the scope's state could not be confirmed), or a refusal the daemon
    /// never lifts for this scope, such as `scope_not_live` or `scope_ended`. Nothing was sent.
    ScopeRefused { code: String, detail: String },
    /// A route of this session closed while no call was in flight on it, and the daemon says
    /// (asked with `scope.describe`) that the scope or its parent is gone (`reason`: `scope_ended`, `scope_not_live` or
    /// `scope_parent_ended`). The loop gives every call of the batch a `route_drained` result
    /// naming `reason` and stops the run Interrupted. Nothing was sent.
    ScopeDrained { reason: String },
}

impl std::fmt::Display for ToolPlaneError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Permanent { code, detail } => {
                write!(f, "permanent tool-provider refusal: {code}")?;
                if let Some(detail) = detail { write!(f, " — {detail}")?; }
                Ok(())
            }
            // This text reaches the MODEL as a synthesized tool result, and it is the only
            // account of the failure a caller gets. "tool plane unavailable" alone cost a
            // peer a two-module trace with error-byte capture to learn that their bind had
            // been REJECTED and by whom -- the message named a missing capability and hid
            // both the refusal and its source. Keep whatever the plane reports verbatim.
            ToolPlaneError::Unavailable(m) => {
                write!(
                    f,
                    "tool plane unavailable (route bind or transport failed): {m}"
                )
            }
            ToolPlaneError::ScopeRefused { code, detail } => write!(f, "{code}: {detail}"),
            ToolPlaneError::ScopeDrained { reason } => write!(
                f,
                "route_drained: the session's scope route was closed ({reason}); nothing was sent"
            ),
            ToolPlaneError::ModuleUnavailable {
                module,
                code,
                detail,
            } => write!(
                f,
                "tool plane unavailable (module '{module}' is restarting or unavailable; nothing was sent): {code} {detail}"
            ),
        }
    }
}

impl std::error::Error for ToolPlaneError {}

/// Keep terminal refusal attribution in conversation data instead of calling it an outage.
pub fn permanent_refusal_output(
    code: &str,
    detail: Option<&serde_json::Value>,
) -> broca_protocol::ToolOutput {
    broca_protocol::ToolOutput::Json {
        value: serde_json::json!({"class":"permanent", "code":code, "detail":detail}).into(),
    }
}

/// The outcome of dispatching ONE call. The durability barrier needs to know
/// whether a side effect could have occurred, so a plane failure is NOT a single
/// opaque error — it is classified by whether the request LEFT the process:
///  - `Completed`  — a definite assembled result (the tool ran; `is_error` may be
///    set by the tool itself).
///  - `NotSent`    — the request never left the process (connect failed / route
///    gone / refused before send). SAFE: no side effect occurred → the loop makes a
///    normal `is_error` result the model can retry.
///  - `SentButOutcomeUnknown` — the request WAS sent but no result came back
///    (timeout / connection reset / executor crash AFTER send). The side effect
///    MAY have applied → AMBIGUOUS → the run stops for review rather than feeding
///    the unknown back to the model. NEVER a normal "unavailable" error (which the
///    model would retry = a DUPLICATE side effect) and NEVER an auto-retry.
#[derive(Debug, Clone)]
pub enum DispatchResult {
    Completed(ToolResult),
    /// The provider received the call and refused it with a refusal whose code and detail the
    /// call's outcome records, so the control lane can name it: today only
    /// `tool_major_changed`, the provider now serving a different major of the tool. `result`
    /// is the error result the model sees; `detail` is [`refusal_detail`]. Like any refusal of
    /// a received call, it is never re-sent.
    Refused {
        result: ToolResult,
        code: String,
        detail: serde_json::Value,
    },
    NotSent {
        reason: String,
    },
    SentButOutcomeUnknown {
        reason: String,
    },
    /// The route the call was to use, opened under the run's scope, closed while the call was in
    /// flight. `reason` names what the daemon said about the scope when asked
    /// (`scope.describe`): the scope or its parent is gone, the scope is now bound to a different
    /// agent so a fresh route would carry another identity (`scope_agent_changed`), or the
    /// scope's state could not be confirmed (`scope_unverifiable`). Whether the request had
    /// left decides the outcome, not the reason:
    ///  - `sent: false` — the request never left: the call gets `route_drained` naming the
    ///    reason and is never re-dispatched, not even on a fresh route;
    ///  - `sent: true` — the request left and no result came back: the outcome is unknown and
    ///    the run stops Interrupted, never Cancelled.
    ScopeDrained {
        reason: String,
        sent: bool,
    },
    /// The call was handed back for the person's steer (llm-runner `call_yield`): `result` is
    /// the interim content the provider answered the yield with, and it is the call's result
    /// from now on. The work goes on at `carrier`, the call's provider, whose later terminal for
    /// the call reaches the session as a notice.
    Yielded {
        result: ToolResult,
        carrier: String,
    },
}

/// What the outcome of a call its provider refused records as its detail: the provider that
/// refused it and the refusal's own detail, `{provider, refusal}`. A `tool_major_changed`
/// refusal's detail is `{tool, expected, current}`, the pin the call carried and the provider's
/// current one.
pub fn refusal_detail(provider: &str, detail: Option<&serde_json::Value>) -> serde_json::Value {
    let mut recorded = serde_json::Map::new();
    recorded.insert("provider".into(), provider.into());
    if let Some(detail) = detail {
        recorded.insert("refusal".into(), detail.clone());
    }
    serde_json::Value::Object(recorded)
}

/// Run signals carried to an approved adapter's final physical-send boundary.
/// Clones observe the same tokens while route and flow-control waits are pending.
#[derive(Clone, Default)]
pub struct DispatchSignals {
    cancel: Option<tokio_util::sync::CancellationToken>,
    stop: Option<tokio_util::sync::CancellationToken>,
    /// The run's absolute step deadline, carried so the route-open wait in front of the send
    /// ends with it (see [`ToolOpenGuard`]). It never makes an enqueued call count as unsent.
    deadline: Option<tokio::time::Instant>,
    /// Counts the person's own steers the run took (see [`PersonSteers`]).
    person: Option<PersonSteers>,
}
impl DispatchSignals {
    pub fn new(
        cancel: Option<&tokio_util::sync::CancellationToken>,
        stop: Option<&tokio_util::sync::CancellationToken>,
    ) -> Self {
        Self {
            cancel: cancel.cloned(),
            stop: stop.cloned(),
            deadline: None,
            person: None,
        }
    }

    /// The person's steers the run has taken so far, or `None` when the run counts none.
    /// A call reads this as it is sent, then waits in [`Self::person_steered`] for a later one.
    pub fn person_steers_seen(&self) -> Option<u64> {
        self.person.as_ref().map(|steers| *steers.0.borrow())
    }

    /// Resolves once the run takes a person's steer after the `seen`-th, and never when the run
    /// counts none (or its count can no longer change).
    pub async fn person_steered(&self, seen: u64) {
        let Some(PersonSteers(steers)) = &self.person else {
            return std::future::pending().await;
        };
        let mut steers = steers.clone();
        if steers.wait_for(|count| *count > seen).await.is_err() {
            std::future::pending::<()>().await;
        }
    }
    pub fn interrupted(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
            || self
                .stop
                .as_ref()
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
    }
    pub async fn cancelled(&self) {
        async fn wait(token: Option<&tokio_util::sync::CancellationToken>) {
            match token {
                Some(token) => token.cancelled().await,
                None => std::future::pending().await,
            }
        }
        tokio::select! {
            _ = wait(self.cancel.as_ref()) => {}
            _ = wait(self.stop.as_ref()) => {}
        }
    }

    /// Cancel an already-enqueued call only for an explicit caller decision.
    /// Graceful stop prevents new dispatches but lets in-flight work finish within grace;
    /// the run actor aborts the dispatch task if that grace expires.
    pub async fn caller_cancelled(&self) {
        match &self.cancel {
            Some(token) => token.cancelled().await,
            None => std::future::pending().await,
        }
    }

    /// A graceful runner stop, distinct from the person's decision to cancel the call.
    pub async fn stopping(&self) {
        match &self.stop {
            Some(token) => token.cancelled().await,
            None => std::future::pending().await,
        }
    }
}

impl From<&DispatchSignals> for ToolOpenGuard {
    /// The approved path already carries run signals to its send boundary; the same tokens
    /// and the same absolute deadline bound its route-open wait.
    fn from(signals: &DispatchSignals) -> Self {
        Self {
            cancel: signals.cancel.clone(),
            stop: signals.stop.clone(),
            person: signals.person.clone(),
            deadline: signals.deadline,
        }
    }
}

/// How many of the person's own steers a run has taken (llm-runner `call_yield`): a steer
/// whose sender the session's plan marks `person`, carrying no notice mark and no words spoken
/// for an agent. The session's owner counts them as each one becomes durable; a call waiting on
/// a provider that can hand calls back asks for that once a steer arrives after it was sent.
#[derive(Clone, Debug)]
pub struct PersonSteers(pub tokio::sync::watch::Receiver<u64>);

/// What ends a tool-dispatch route-open wait early: the run's cancel and stop/shutdown
/// tokens, and the run's absolute step deadline when it has one. A batch builds one guard
/// and hands the same guard to its batch start and to every call, so every route-open wait
/// of the batch (first open and every re-open) ends at the same instant. Carried separately
/// from [`DispatchSignals`] (which guards the physical send) so planes whose opens never
/// wait keep ignoring it; [`ToolOpenGuard::signals`] gives the matching send guard. Clones
/// observe the same tokens while the wait is pending.
#[derive(Clone, Default)]
pub struct ToolOpenGuard {
    cancel: Option<tokio_util::sync::CancellationToken>,
    stop: Option<tokio_util::sync::CancellationToken>,
    person: Option<PersonSteers>,
    /// The run's step deadline, as an absolute instant: no route-open wait of the batch, and
    /// no open attempt in flight, outlasts it. `None` when the run has none, which leaves the
    /// plane's own patience as the only bound.
    pub deadline: Option<tokio::time::Instant>,
}
impl ToolOpenGuard {
    /// The same guard ending at the absolute `deadline` (the run's step deadline).
    pub fn with_deadline(mut self, deadline: Option<tokio::time::Instant>) -> Self {
        self.deadline = deadline;
        self
    }

    pub fn new(
        cancel: Option<&tokio_util::sync::CancellationToken>,
        stop: Option<&tokio_util::sync::CancellationToken>,
    ) -> Self {
        Self {
            cancel: cancel.cloned(),
            stop: stop.cloned(),
            person: None,
            deadline: None,
        }
    }
    /// The same guard, with the count of the person's steers its calls hand back on.
    pub fn with_person_steers(mut self, person: Option<PersonSteers>) -> Self {
        self.person = person;
        self
    }
    /// The send-boundary signals matching this guard: the same tokens and deadline, for
    /// the physical enqueue that follows the open.
    pub fn signals(&self) -> DispatchSignals {
        DispatchSignals {
            cancel: self.cancel.clone(),
            stop: self.stop.clone(),
            deadline: self.deadline,
            person: self.person.clone(),
        }
    }
    pub fn cancel_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.cancel.clone()
    }
    pub fn stop_token(&self) -> Option<tokio_util::sync::CancellationToken> {
        self.stop.clone()
    }
    pub fn interrupted(&self) -> bool {
        self.cancel
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
            || self
                .stop
                .as_ref()
                .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
    }
    pub async fn cancelled(&self) {
        async fn wait(token: Option<&tokio_util::sync::CancellationToken>) {
            match token {
                Some(token) => token.cancelled().await,
                None => std::future::pending().await,
            }
        }
        tokio::select! {
            _ = wait(self.cancel.as_ref()) => {}
            _ = wait(self.stop.as_ref()) => {}
        }
    }
}

pub use broca_records::{record::FrozenProviderDeclarations, record::PlanManifest};

/// One tool's current definition, which a run at a prefix rebuild takes in place of the one
/// its session froze ([`ToolPlane::compatible_tools`]).
#[derive(Debug, Clone, PartialEq)]
pub struct CompatibleTool {
    /// The plan provider serving the tool.
    pub provider: String,
    /// The tool as the provider's catalog now answers it, under the same name.
    pub tool: broca_protocol::request::Tool,
    /// Its pin under that definition: the same major, the current digest and semantics.
    pub pin: DispatchSchemaPin,
}

/// The tool-dispatch contract. One batch = one assistant turn's tool calls.
#[async_trait]
pub trait ToolPlane: Send + Sync {
    /// Read only transport declarations for a fresh run. Legacy storage and resumed runs do
    /// not call this hook; adapters without discovery retain the plan's frozen declarations.
    async fn run_transport(
        &self,
        _bind: &BindIdentity,
        _scope: Option<&broca_protocol::SessionScope>,
        _manifest: &broca_records::record::PlanManifest,
    ) -> std::collections::BTreeMap<String, broca_records::record::FrozenProviderDeclarations> {
        Default::default()
    }

    /// The current definitions of `manifest`'s versioned tools that a fresh run at a prefix
    /// rebuild takes (llm-runner `tool_versions`): each tool whose provider now serves the same
    /// major as its frozen pin, under a different description, input schema or schema digest.
    /// Read from what [`Self::run_transport`] just read for the same run, so it asks no
    /// provider again. A tool whose provider could not be read, or that changed major, is not
    /// listed and keeps its frozen definition. Adapters without discovery list none.
    async fn compatible_tools(
        &self,
        _manifest: &broca_records::record::PlanManifest,
    ) -> Vec<CompatibleTool> {
        Vec::new()
    }

    /// Prepare routing state for this batch before dispatch starts. `calls` is
    /// the full set in this batch, and `manifest` is the immutable tool manifest captured when
    /// the run was admitted. On `Err` the loop synthesizes `is_error` results for every
    /// call so the batch fails gracefully.
    async fn batch_started(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
    ) -> Result<(), ToolPlaneError>;

    /// Dispatch ONE tool call and return its outcome. FENCES by `tool_call_id`: a
    /// repeated id returns the prior result and does NOT re-execute (at-most-once).
    /// Tool EXECUTION failures come back as `Completed(ToolResult { is_error })`
    /// (conversation data); a PLANE failure is classified `NotSent` (safe — never
    /// left) vs `SentButOutcomeUnknown`
    /// (ambiguous — side effect may have applied) so the loop can treat each case
    /// correctly (retry the safe one, stop the run on the ambiguous one).
    async fn dispatch(&self, ctx: &BatchCtx, call: &ToolCall) -> DispatchResult;

    /// Register every eligible key in one synced intake transaction before dispatching
    /// any member of the group. Adapters without late results keep their existing path.
    async fn register_dispatch_group(
        &self,
        _ctx: &BatchCtx,
        _calls: &[DispatchRegistration],
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }

    async fn dispatch_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        _key: DispatchKey<'_>,
    ) -> DispatchResult {
        self.dispatch(ctx, call).await
    }

    async fn dispatch_approved_keyed(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        args_jcs: &str,
        key: DispatchKey<'_>,
        signals: &DispatchSignals,
    ) -> DispatchResult {
        self.dispatch_approved(ctx, call, args_jcs, key.call_key, signals)
            .await
    }

    /// Dispatch approved arguments without parsing and reserializing them. Adapters must
    /// preserve `args_jcs` verbatim and forward the deferred call's `call_key`. Core verifies
    /// that the answer binds to these arguments; consent issues no tool token. The approval
    /// digest remains durable history, not provider request data.
    /// Check `signals` after all pre-send waits, immediately before the physical
    /// request. An interrupted unsent request returns `NotSent`, never Unknown.
    /// Until the daemon installs an approval-capable adapter, approval fails closed.
    async fn dispatch_approved(
        &self,
        _ctx: &BatchCtx,
        _call: &ToolCall,
        _args_jcs: &str,
        _call_key: &str,
        _signals: &DispatchSignals,
    ) -> DispatchResult {
        DispatchResult::NotSent {
            reason: "approved_dispatch_unavailable".into(),
        }
    }

    /// Open a fresh route under the session scope saved in ToolCallDeferred before
    /// executing an approved call. A cached route cannot prove the scope is still live.
    async fn recheck_approval_scope(
        &self,
        _ctx: &BatchCtx,
        _call: &ToolCall,
        _module: Option<&str>,
    ) -> Result<(), ToolPlaneError> {
        Err(ToolPlaneError::Unavailable(
            "approval_scope_recheck_unavailable".into(),
        ))
    }

    /// The tool provider module the call `tool_call_id` will be sent to, as resolved by
    /// [`batch_started`](Self::batch_started), or `None` when the plane routes calls to no
    /// module (an in-process plane) or the call resolves to no module and will fail without
    /// being sent. The loop records it on the call's dispatch intent, which `session.read`
    /// reports as the call's `dispatched_to`. Asked after `batch_started` and before the intent
    /// is written, so it must answer from state `batch_started` already settled.
    fn dispatch_target(&self, _ctx: &BatchCtx, _tool_call_id: &ToolCallId) -> Option<String> {
        None
    }

    /// The provider definition frozen for this call. In-process and legacy
    /// non-plan tools have no catalog semantics to report, so their pin is unknown.
    fn dispatch_schema_pin(
        &self,
        _ctx: &BatchCtx,
        _tool_call_id: &ToolCallId,
        _tool: &str,
    ) -> Option<broca_records::record::DispatchSchemaPin> {
        None
    }

    /// Close a batch — modelling `tool_batch_finished{batch_id}`.
    async fn batch_finished(&self, ctx: &BatchCtx) -> Result<(), ToolPlaneError>;

    /// batch_started with an early-exit guard for the route-open wait: run cancel or
    /// stop/shutdown ends the wait at once instead of holding the tool-dispatch
    /// patience. Planes whose opens never wait keep the default, which ignores the guard.
    async fn batch_started_guarded(
        &self,
        ctx: &BatchCtx,
        calls: &[ToolCall],
        manifest: &ToolManifest,
        guard: &ToolOpenGuard,
    ) -> Result<(), ToolPlaneError> {
        let _ = guard;
        self.batch_started(ctx, calls, manifest).await
    }

    /// dispatch/dispatch_keyed with the same early-exit guard for the route-open wait.
    /// `key` selects the keyed form, as dispatch_keyed does. Planes whose dispatch
    /// never waits keep the default.
    async fn dispatch_guarded(
        &self,
        ctx: &BatchCtx,
        call: &ToolCall,
        key: Option<DispatchKey<'_>>,
        guard: &ToolOpenGuard,
    ) -> DispatchResult {
        let _ = guard;
        match key {
            Some(key) => self.dispatch_keyed(ctx, call, key).await,
            None => self.dispatch(ctx, call).await,
        }
    }
}

/// An error shown to the model may be synthesized before any provider request, so
/// execution feedback must distinguish actual execution from the displayed result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApprovalExecution {
    Executed,
    ApprovedNotExecuted,
    OutcomeUnknown,
}
