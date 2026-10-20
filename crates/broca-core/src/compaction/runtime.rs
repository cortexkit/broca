//! The run loop's calls into the session's compaction provider, and the WAL records that make
//! them durable.
//!
//! Setup is a call the run waits on before its first model call. After it, the provider
//! announces each view it publishes with a `view_ready` event ([`CompactionViews`]). The run
//! fetches the view with `view.get` only at a step boundary and applies it there
//! ([`Runtime::views_at_boundary`], [`Runtime::apply_view`]). When the request's prefix is
//! being rewritten anyway, the run also asks the provider for a view itself, with a short
//! `compaction.ride`. Advance notices are recorded here and published from the durable WAL by
//! the bus publisher ([`Runtime::record_notice`]).

use super::*;
use crate::compaction_provider::{
    errors::RefuseCode,
    notice::{AdvanceNotice, Event, ViewReady},
    ride::{RideAnswer, RideCause, RideRequest, MAX_REWRITE_RIDE_DEADLINE_MS},
    setup::{SetupAnswer, SetupRequest},
    view::{CompactionView, ViewGetAnswer, ViewGetRequest},
};
use async_trait::async_trait;
use broca_protocol::{
    error::{ErrorClass, ProviderError, ProviderRefusal},
    BindIdentity, RunFinishReason,
};

#[derive(Debug, Clone)]
pub struct CompactionUnavailable;

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    pub call_budget_ms: u64,
}
impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            call_budget_ms: COMPACTION_CALL_BUDGET_MS,
        }
    }
}

async fn clock_deadline(clock: Option<&crate::Clock>, deadline_ms: u64) {
    #[cfg(feature = "test-clock")]
    if let Some(clock) = clock {
        let mut changes = clock.subscribe();
        loop {
            let remaining = deadline_ms.saturating_sub(clock.now_ms());
            if remaining == 0 {
                return;
            }
            tokio::select! {
                _ = tokio::time::sleep(std::time::Duration::from_millis(remaining)) => {},
                _ = changes.changed() => {},
            }
        }
    }
    let now = clock.map_or_else(super::now, crate::Clock::now_ms);
    tokio::time::sleep(std::time::Duration::from_millis(
        deadline_ms.saturating_sub(now),
    ))
    .await;
}

/// A sibling of HookPlane on the same module-level route machinery. Route errors and answers
/// that do not decode are failed calls.
#[async_trait]
pub trait CompactionPlane: Send + Sync {
    async fn setup(
        &self,
        provider: &str,
        bind: &BindIdentity,
        request: SetupRequest,
    ) -> Result<SetupAnswer, CompactionUnavailable>;

    /// `view.get`: fetch the view a `view_ready` event announced (compaction-provider/v1 §6).
    /// The default fails the call, which leaves the run on its current view, as a provider
    /// that cannot be reached does.
    async fn view_get(
        &self,
        _provider: &str,
        _bind: &BindIdentity,
        _request: ViewGetRequest,
    ) -> Result<ViewGetAnswer, CompactionUnavailable> {
        Err(CompactionUnavailable)
    }

    /// `compaction.ride`: ask the provider for a view now (compaction-provider/v1 §8). The
    /// caller bounds the call by `request.deadline_ms`. The default fails the call, which the
    /// run treats as the answer `nothing`.
    async fn ride(
        &self,
        _provider: &str,
        _bind: &BindIdentity,
        _request: RideRequest,
    ) -> Result<RideAnswer, CompactionUnavailable> {
        Err(CompactionUnavailable)
    }
}

/// The `view_ready` events the session's compaction provider published, waiting for the run's
/// next step boundary. An event only says that a view exists. At a boundary where no ride
/// applied a view, the run loop asks for the newest one and fetches that view with `view.get`
/// there, never mid-step, then records, before the step, what it did with it.
pub trait CompactionViews: Send + Sync {
    /// The newest `view_ready` for the session `session_ref` whose version is above `after`.
    /// `lineage_id` is the lineage the run's transcript counts in. A `view_ready` event names
    /// no lineage, so a source fed from the bus ignores it; the fetched view's own lineage is
    /// checked when it is offered, and a view of another lineage is never applied.
    fn newest_ready(
        &self,
        session_ref: &str,
        lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady>;
}

/// The newest `view_ready` event received for each session, kept in memory until a run's step
/// boundary asks for it. Losing it (a restart) loses nothing durable: the provider keeps
/// serving its newest view with `view.get`, and announces every later one.
#[derive(Default)]
pub struct ViewReadyInbox {
    newest: std::sync::Mutex<HashMap<String, ViewReady>>,
}

impl ViewReadyInbox {
    /// Keep `ready` when it is the newest version seen for its session. Returns whether it
    /// was kept; an older or repeated event (the bus may redeliver) changes nothing.
    pub fn announce(&self, ready: ViewReady) -> bool {
        let mut newest = self.newest.lock().unwrap_or_else(|e| e.into_inner());
        match newest.get(&ready.session_ref) {
            Some(held) if held.view_version >= ready.view_version => false,
            _ => {
                newest.insert(ready.session_ref.clone(), ready);
                true
            }
        }
    }

    /// Drop what is held for a session that no longer runs here.
    pub fn forget(&self, session_ref: &str) {
        self.newest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(session_ref);
    }
}

impl CompactionViews for ViewReadyInbox {
    fn newest_ready(
        &self,
        session_ref: &str,
        _lineage_id: &str,
        after: Option<u64>,
    ) -> Option<ViewReady> {
        self.newest
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(session_ref)
            .filter(|ready| after.is_none_or(|current| ready.view_version > current))
            .cloned()
    }
}

/// The outcome of fetching an announced view with `view.get`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Fetch {
    View(CompactionView),
    /// The provider refused with this code: it does not hold that version or that session,
    /// or gave a code broca does not know, which counts the same. The run keeps its current
    /// view and waits for the next `view_ready` (compaction-provider/v1 §6).
    Refused(String),
    /// No answer within the call budget, an `ERROR`, or an answer that does not decode. The
    /// run keeps its current view and may fetch again at a later boundary.
    Unavailable,
}

/// What the run loop remembers about views from one step boundary to the next.
#[derive(Default)]
pub(crate) struct BoundaryViews {
    /// The newest version this run will not offer again: refused, rejected, stale, or one the
    /// provider said it does not hold.
    declined: Option<u64>,
    /// The newest version a notice already reported, so no event is reported twice.
    reported: Option<u64>,
    /// The last view fetched and not applied, with the event that announced it. A deferred
    /// view, or one that is not (yet) a prefix of the transcript, is offered again at a later
    /// boundary from here, without fetching it again.
    held: Option<(ViewReady, CompactionView)>,
}

pub(crate) struct Runtime<'a> {
    pub provider: &'a str,
    pub bind: &'a BindIdentity,
    pub plane: Option<&'a Arc<dyn CompactionPlane>>,
    pub writer: &'a crate::WalHandle,
    pub control: &'a crate::RunControl,
}
/// How a run ends when a recorded compaction wait seals it.
pub(crate) struct Terminal(pub RunFinishReason, pub Option<Box<ProviderError>>);

fn unavailable() -> Box<ProviderError> {
    Box::new(error(
        None,
        "compaction Setup unavailable",
        Some("compaction_unavailable".into()),
        false,
    ))
}
pub(crate) fn error(
    refusal: Option<Box<ProviderRefusal>>,
    reason: &str,
    provider_code: Option<String>,
    retryable: bool,
) -> ProviderError {
    ProviderError {
        provider_subcode: None,
        refusal,
        class: if retryable {
            ErrorClass::Transient
        } else {
            ErrorClass::Permanent
        },
        message: reason.into(),
        status: None,
        retry_after_secs: None,
        provider_code,
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}
fn refusal(
    provider: &str,
    code: RefuseCode,
    reason: &str,
    provider_code: Option<String>,
) -> ProviderError {
    let retryable = code.retryable();
    error(
        Some(Box::new(ProviderRefusal {
            provider: provider.into(),
            reason: Some(reason.into()),
            provider_detail_code: provider_code,
        })),
        reason,
        Some(code.as_str().into()),
        retryable,
    )
}

/// Seal a wait the removed synchronous client left open when the process stopped. Recovery
/// is never permission to call the provider again: a terminal intent already recorded ends the
/// run with that exit, and a wait with none ends `interrupted`, recorded before the terminal.
pub(crate) async fn seal_recorded_wait(
    writer: &crate::WalHandle,
    state: &mut CompactionState,
) -> Result<Option<Terminal>, crate::RunError> {
    let Some(wait) = state.open_wait.clone() else {
        return Ok(None);
    };
    let exit = wait
        .terminal_exit
        .unwrap_or(CompactionWaitExit::Interrupted);
    if wait.terminal_exit.is_none() {
        let record = WalRecord::CompactionWait {
            exited: exit,
            reason: wait.wait.reason.clone(),
        };
        writer.record(record.clone());
        writer.sync().await?;
        state.fold(&record);
    }
    Ok(Some(terminal_exit(exit)))
}

/// The run error for a request broca will not send because it is over the model's context
/// window (compaction-provider/v1 §8: the runner never sends over the window). Not retryable:
/// sending it again changes nothing until a view makes the request smaller.
pub(crate) fn context_ceiling_error(
    detail: &cortexkit_role_llm_runner::errors::ContextCeilingDetail,
) -> ProviderError {
    error(
        None,
        &format!(
            "context_ceiling: the request is estimated at {} tokens, over the model's context \
             window of {} tokens ({})",
            detail.size, detail.window, detail.cause
        ),
        Some("context_ceiling".into()),
        false,
    )
}

impl Runtime<'_> {
    fn now(&self) -> u64 {
        self.control
            .clock
            .as_ref()
            .map_or_else(super::now, crate::Clock::now_ms)
    }
    async fn commit(
        &self,
        state: &mut CompactionState,
        record: WalRecord,
    ) -> Result<(), crate::RunError> {
        self.writer.record(record.clone());
        self.writer.sync().await?;
        state.fold(&record);
        Ok(())
    }

    /// Call Setup once for the session and record its answer before the first model call
    /// (compaction-provider/v1 §4). A failure, a timeout, an answer for another request, an
    /// initial message broca cannot render or a malformed ceiling is `compaction_unavailable`;
    /// a refusal carries the provider's role code. Either way nothing is recorded, so the next
    /// send calls Setup again.
    pub async fn setup(
        &self,
        mut request: SetupRequest,
        raw: &[Message],
    ) -> Result<CompactionState, Box<ProviderError>> {
        let Some(plane) = self.plane else {
            return Err(unavailable());
        };
        request.now = self.now();
        let request_id = request.request_id.clone();
        let answer = tokio::select! {
            answer = plane.setup(self.provider, self.bind, request) => answer.ok(),
            _ = clock_deadline(self.control.clock.as_ref(), self.now().saturating_add(self.control.compaction_config.call_budget_ms)) => None,
        }.ok_or_else(unavailable)?;
        if answer.request_id() != request_id {
            return Err(unavailable());
        }
        let (initial, ceiling) = match answer {
            SetupAnswer::Refuse {
                code,
                reason,
                provider_code,
                ..
            } => {
                return Err(Box::new(refusal(
                    self.provider,
                    code,
                    &reason,
                    provider_code,
                )))
            }
            SetupAnswer::Ready {
                initial, ceiling, ..
            } => (initial, ceiling),
            // The answer decodes strictly on `answer`, so no other kind reaches here.
            #[allow(unreachable_patterns)]
            _ => return Err(unavailable()),
        };
        if ceiling
            .as_ref()
            .is_some_and(|policy| policy.check().is_err())
        {
            return Err(unavailable());
        }
        let messages = validate_initial(&initial, raw).ok_or_else(unavailable)?;
        let record = WalRecord::CompactionMessage {
            provider: self.provider.into(),
            kind: CompactionMessageKind::Setup,
            compaction_id: initial.compaction_id,
            version: initial.version,
            range: CompactionRange {
                from: initial.range.from,
                to: initial.range.to,
            },
            sha256: broca_wal::compaction_replacement_sha256(&messages)
                .map_err(|_| unavailable())?,
            replacement: Arc::new(messages),
            request_id: None,
            cursor: None,
            call_when: None,
            stability: None,
            ceiling: ceiling.map(|policy| {
                OpaqueJson::from_json_text(serde_json::to_string(&policy).expect("role JSON"))
                    .expect("role JSON")
            }),
        };
        let mut state = CompactionState::default();
        self.commit(&mut state, record)
            .await
            .map_err(|_| unavailable())?;
        state
            .applied
            .as_mut()
            .expect("Setup applied")
            .thinking_prefix_len = raw.len() as u64;
        Ok(state)
    }

    /// Fetch the view `ready` announced, with `view.get`, within the call budget.
    pub(crate) async fn view_get(&self, ready: &ViewReady) -> Fetch {
        let Some(plane) = self.plane else {
            return Fetch::Unavailable;
        };
        let answer = tokio::select! {
            answer = plane.view_get(self.provider, self.bind, ViewGetRequest::for_ready(ready)) => answer.ok(),
            _ = clock_deadline(self.control.clock.as_ref(), self.now().saturating_add(self.control.compaction_config.call_budget_ms)) => None,
        };
        match answer {
            Some(ViewGetAnswer::View { view }) => Fetch::View(view),
            Some(ViewGetAnswer::Refuse { code, .. }) => Fetch::Refused(code),
            // The answer decodes strictly on `answer`, so no other kind reaches here.
            #[allow(unreachable_patterns)]
            Some(_) | None => Fetch::Unavailable,
        }
    }

    /// Ride the provider for a view because the request's prefix is being rewritten anyway
    /// (compaction-provider/v1 §8). The ride waits at most broca's own short deadline, which
    /// it sends as `deadline_ms`. A missed deadline, a failed call or `nothing` gives `None`,
    /// and the request goes out under the current view.
    pub(crate) async fn ride(&self, session_ref: &str, cause: RideCause) -> Option<CompactionView> {
        let plane = self.plane?;
        let deadline_ms = self
            .control
            .compaction_config
            .call_budget_ms
            .clamp(1, MAX_REWRITE_RIDE_DEADLINE_MS);
        let request = RideRequest::new(session_ref, cause, deadline_ms);
        let answer = tokio::select! {
            answer = plane.ride(self.provider, self.bind, request) => answer.ok(),
            _ = clock_deadline(self.control.clock.as_ref(), self.now().saturating_add(deadline_ms)) => None,
        };
        match answer? {
            RideAnswer::View { view } => Some(view),
            _ => None,
        }
    }

    /// The views of one step boundary (compaction-provider/v1 §7 and §8), before the request
    /// renders. First a ride, when the request's prefix is being rewritten anyway
    /// ([`view::rewrite_ride_cause`]); a view it brings is checked against its own digest.
    /// Then, unless the ride's view applied, the newest `view_ready` in `inbox` above the view
    /// rendered now: that view is fetched with `view.get` here, at the boundary, and checked
    /// against the event's digest. At most one view applies per step. Returns the events the
    /// boundary's notice reports (a deferred or refused view).
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn views_at_boundary(
        &self,
        views: &mut BoundaryViews,
        state: &mut CompactionState,
        inbox: Option<&dyn CompactionViews>,
        session_ref: &str,
        lineage_id: &str,
        model: &str,
        prompt: &[Message],
        stored: &[Message],
        prefix_bound: bool,
    ) -> Result<Vec<Event>, crate::RunError> {
        let mut events = Vec::new();
        if let Some(cause) = view::rewrite_ride_cause(state, model) {
            if let Some(view) = Box::pin(self.ride(session_ref, cause)).await {
                let ready = ViewReady::for_view(&view);
                let applied = Box::pin(self.offer(
                    views,
                    state,
                    &view,
                    &ready,
                    lineage_id,
                    prompt,
                    stored,
                    prefix_bound,
                    &mut events,
                ))
                .await?;
                if applied {
                    return Ok(events);
                }
            }
        }
        let Some(inbox) = inbox else {
            return Ok(events);
        };
        let current = state.view.as_ref().map(|applied| applied.view.view_version);
        let Some(ready) = inbox
            .newest_ready(session_ref, lineage_id, current)
            .filter(|ready| views.declined != Some(ready.view_version))
        else {
            return Ok(events);
        };
        let fetched = match &views.held {
            Some((held, view)) if *held == ready => view.clone(),
            _ => match Box::pin(self.view_get(&ready)).await {
                Fetch::View(view) => {
                    views.held = Some((ready.clone(), view.clone()));
                    view
                }
                Fetch::Refused(code) => {
                    tracing::info!(
                        view_version = ready.view_version,
                        %code,
                        "compaction provider refused view.get; keeping the current view"
                    );
                    views.declined = Some(ready.view_version);
                    return Ok(events);
                }
                Fetch::Unavailable => return Ok(events),
            },
        };
        Box::pin(self.offer(
            views,
            state,
            &fetched,
            &ready,
            lineage_id,
            prompt,
            stored,
            prefix_bound,
            &mut events,
        ))
        .await?;
        Ok(events)
    }

    /// Offer one fetched view and remember the outcome in `views`; whether it applied.
    #[allow(clippy::too_many_arguments)]
    async fn offer(
        &self,
        views: &mut BoundaryViews,
        state: &mut CompactionState,
        fetched: &CompactionView,
        ready: &ViewReady,
        lineage_id: &str,
        prompt: &[Message],
        stored: &[Message],
        prefix_bound: bool,
        events: &mut Vec<Event>,
    ) -> Result<bool, crate::RunError> {
        let version = fetched.view_version;
        let (decision, event) = Box::pin(self.offer_view(
            state,
            fetched,
            ready,
            lineage_id,
            prompt,
            stored,
            prefix_bound,
        ))
        .await?;
        match &decision {
            // A refused, rejected or stale view is never offered to this run again. A deferred
            // view is offered at every later boundary, and so is one that is not (yet) a prefix
            // of this transcript: it may cover messages this run has still to write again after
            // a restart.
            view::Decision::Refuse(reason) => {
                tracing::info!(view_version = version, reason, "compaction view refused");
                views.declined = Some(version);
            }
            view::Decision::Rejected(problem) => {
                tracing::info!(view_version = version, problem = %problem.name(), "compaction view rejected");
                views.declined = Some(version);
            }
            view::Decision::Stale => views.declined = Some(version),
            // Not (yet) a prefix of this transcript: held and offered again at every later
            // boundary, so it is not logged here, where it would repeat at each one.
            view::Decision::Discard | view::Decision::Defer(_) | view::Decision::Apply(_) => {}
        }
        if views.reported != Some(version) {
            if let Some(event) = event {
                events.push(event);
                views.reported = Some(version);
            }
        }
        let applied = matches!(decision, view::Decision::Apply(_));
        if applied || views.declined == Some(version) {
            views.held = None;
        }
        Ok(applied)
    }

    /// Offer a fetched view at a step boundary: decide what to do with it
    /// ([`view::decide`]), record it when it applies, and return the event the next notice
    /// reports for it, if any. The caller holds a deferred view and offers it again at a later
    /// boundary.
    ///
    #[allow(clippy::too_many_arguments)]
    pub async fn offer_view(
        &self,
        state: &mut CompactionState,
        fetched: &CompactionView,
        ready: &crate::compaction_provider::notice::ViewReady,
        lineage_id: &str,
        prompt: &[Message],
        stored: &[Message],
        prefix_bound: bool,
    ) -> Result<
        (
            view::Decision,
            Option<crate::compaction_provider::notice::Event>,
        ),
        crate::RunError,
    > {
        let decision = view::decide(
            fetched,
            ready,
            state,
            lineage_id,
            prompt,
            stored,
            prefix_bound,
        );
        if let view::Decision::Apply(choice) = &decision {
            self.apply_view(state, fetched, choice.clone()).await?;
        }
        let event = decision.event(fetched.view_version);
        Ok((decision, event))
    }

    /// Record `view` as applied with `choice`, at the step boundary before the step that first
    /// renders it, and fold it into `state`. Durable before it returns, so a crash after this
    /// point resumes rendering the view without asking the provider again.
    async fn apply_view(
        &self,
        state: &mut CompactionState,
        view: &CompactionView,
        choice: broca_wal::CompactionViewChoice,
    ) -> Result<(), crate::RunError> {
        let json = serde_json::to_string(view).map_err(|e| crate::RunError::Wal(e.to_string()))?;
        self.commit(
            state,
            WalRecord::CompactionViewApplied {
                provider: self.provider.into(),
                view_version: view.view_version,
                view: OpaqueJson::from_json_text(json)
                    .map_err(|e| crate::RunError::Wal(e.to_string()))?,
                choice,
            },
        )
        .await
    }

    /// Record an advance notice and fold it into `state`. The bus publisher projects notices
    /// from the durable WAL, so this is the only place a notice comes into being, and it is
    /// durable before anything can publish it. A notice the role would refuse is not recorded.
    pub async fn record_notice(
        &self,
        state: &mut CompactionState,
        notice: AdvanceNotice,
    ) -> Result<(), crate::RunError> {
        if let Err(problem) = notice.check() {
            tracing::warn!(problem = %problem.name(), "not recording a malformed advance notice");
            return Ok(());
        }
        let record = WalRecord::CompactionNotice {
            seq: notice.seq,
            headers: notice.to_headers(),
        };
        self.writer.record(record.clone());
        // The publisher projects only durable WAL ranges, so this barrier is what keeps a
        // notice from reaching the bus before a crash could still lose it.
        self.writer.sync().await?;
        state.fold(&record);
        Ok(())
    }
}

fn terminal_exit(exit: CompactionWaitExit) -> Terminal {
    match exit {
        CompactionWaitExit::Cancel => Terminal(RunFinishReason::Cancelled, None),
        // A continuing exit closes the wait in the fold, so only a terminal one is left open;
        // treat anything else as the interruption it would have been.
        CompactionWaitExit::Stopped
        | CompactionWaitExit::Interrupted
        | CompactionWaitExit::Ready
        | CompactionWaitExit::Bound
        | CompactionWaitExit::Cap => Terminal(RunFinishReason::Interrupted, None),
        CompactionWaitExit::Exceeded => Terminal(
            RunFinishReason::Error,
            Some(Box::new(error(
                None,
                "compaction wait exceeded; request cannot be shown to fit",
                Some("compaction_wait_exceeded".into()),
                true,
            ))),
        ),
    }
}
