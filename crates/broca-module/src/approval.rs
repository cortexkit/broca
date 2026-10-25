//! Reports approval-call states and retries withdrawals for unanswered questions after calls close.
//! The session owns this work because a run task can finish before the question is withdrawn.

use broca_core::tool_plane::{ApprovalWithdrawal, BatchCtx, ElicitationPlane};
use broca_core::{DeferredCallProjection, RunIndex, WalHandle, WalStore};
use broca_protocol::BindIdentity;
#[cfg(test)]
use broca_protocol::RunId;
use broca_wal::WalRecord;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

pub(crate) fn open_batch_view(
    batch: Option<&broca_core::OpenToolBatch>,
) -> broca_wire::OpenBatchView {
    match batch {
        None => broca_wire::OpenBatchView::None,
        Some(batch) => broca_wire::OpenBatchView::Open {
            batch_id: batch.batch_id.0.clone(),
            step_id: batch.step_id.0,
            calls: batch
                .calls
                .iter()
                .map(|(id, state)| broca_wire::OpenBatchCall {
                    call_id: id.0.clone(),
                    state: *state,
                })
                .collect(),
        },
    }
}

pub(crate) fn call_views(projection: &DeferredCallProjection) -> Vec<broca_wire::DeferredCallView> {
    projection
        .calls
        .iter()
        .filter_map(|(key, call)| {
            let WalRecord::ToolCallDeferred {
                batch_id, call_id, ..
            } = &*call.prepared
            else {
                return None;
            };
            Some(broca_wire::DeferredCallView {
                call_key: key.clone(),
                batch_id: batch_id.0.clone(),
                call_id: call_id.0.clone(),
                state: call.state,
            })
        })
        .collect()
}

pub(crate) fn late_views(
    calls: &[broca_core::LateApprovalView],
) -> Vec<broca_wire::LateApprovalView> {
    calls
        .iter()
        .filter_map(|call| {
            let WalRecord::LateApprovalRecorded {
                elicitation_id,
                decision,
                ..
            } = &*call.approval
            else {
                return None;
            };
            Some(broca_wire::LateApprovalView {
                call_key: call.call_key.clone(),
                elicitation_id: elicitation_id.clone(),
                decision: match decision {
                    broca_wal::LateDecision::Approved => "approved",
                    broca_wal::LateDecision::Declined => "declined",
                    broca_wal::LateDecision::None => "none",
                }
                .into(),
                outcome: call.outcome.map(|outcome| {
                    match outcome {
                        broca_wal::LateOutcome::Completed => "completed",
                        broca_wal::LateOutcome::NotSent => "not_sent",
                        broca_wal::LateOutcome::Unknown => "unknown",
                    }
                    .into()
                }),
                result: call.result.as_deref().cloned(),
            })
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn awaiting_approval(
    projection: &DeferredCallProjection,
    run_id: &RunId,
) -> Option<broca_wire::AwaitingApproval> {
    let mut pending = Vec::new();
    let mut batch = None;
    for (key, call) in &projection.calls {
        if call.run_id.as_ref() != Some(run_id) || call.state != broca_core::OpenCallState::Deferred
        {
            continue;
        }
        let WalRecord::ToolCallDeferred {
            batch_id,
            call_id,
            ask,
            ..
        } = &*call.prepared
        else {
            continue;
        };
        batch = Some(batch_id.0.clone());
        pending.push(broca_wire::PendingApproval {
            call_key: key.clone(),
            call_id: call_id.0.clone(),
            elicitation_id: call.elicitation_id.clone(),
            expires_at_ms: ask.expires_at_ms,
        });
    }
    batch.map(|batch_id| broca_wire::AwaitingApproval { batch_id, pending })
}

/// A failed or incomplete WAL read cannot establish that every closed question was withdrawn.
/// Keep the run index's withdrawal flag unchanged until a complete read proves no work remains.
pub(crate) fn complete_frames(
    range: broca_wal::WalRangeRead,
    head: u64,
) -> Result<Vec<broca_wal::FramedRecord>, String> {
    match range.into_records() {
        broca_wal::WalRangeRecords::Complete(frames) => Ok(frames),
        broca_wal::WalRangeRecords::Absent if head == 0 => Ok(Vec::new()),
        _ => Err("approval debt requires a complete durable WAL read".into()),
    }
}

pub(crate) fn projection_from_frames(
    lineage: Option<broca_wal::WalLineageId>,
    frames: &[broca_wal::FramedRecord],
) -> Result<DeferredCallProjection, String> {
    if lineage.is_none()
        && frames
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
    {
        return Err("Prepared call has no WAL lineage identity".into());
    }
    let mut projection =
        DeferredCallProjection::new(lineage.map(|id| id.to_string()).unwrap_or_default());
    projection.fold_frames(frames);
    Ok(projection)
}

pub(crate) fn read_projection<W: WalStore>(
    wal: &W,
    key: &BindIdentity,
    writer: &WalHandle,
) -> Result<DeferredCallProjection, String> {
    let head = writer.durable_head().load(Ordering::Acquire);
    let range = wal
        .read_records_range(key, 0, Some(head), None)
        .map_err(|e| e.to_string())?;
    let frames = complete_frames(range, head)?;
    projection_from_frames(writer.lineage_id(), &frames)
}

/// Set `withdrawals_owed` on the run index before recording a terminal run state. If the
/// process dies between these writes, boot can still find the active or flagged run. Recording
/// the terminal first could hide an unanswered question from both startup scans.
pub(crate) fn owed_for_run(projection: &DeferredCallProjection, run_id: &str) -> bool {
    projection.obligations().withdrawals.iter().any(|owed| {
        projection
            .calls
            .get(&owed.withdrawal.call_key)
            .is_some_and(|call| call.run_id.as_ref().is_some_and(|id| id.0 == run_id))
    })
}

pub(crate) fn record_terminal_debt(
    index: &dyn RunIndex,
    projection: &DeferredCallProjection,
    run_id: &str,
) -> Result<bool, String> {
    let owed = owed_for_run(projection, run_id);
    if owed {
        index.index_run_withdrawals_owed(run_id, true)?;
    }
    Ok(owed)
}

pub(crate) struct WithdrawalTask {
    task: Option<tokio::task::JoinHandle<()>>,
    cancel: tokio_util::sync::CancellationToken,
    wake: Arc<tokio::sync::Notify>,
    pending: Arc<AtomicBool>,
    generation: Arc<AtomicU64>,
    settled_generation: Arc<AtomicU64>,
    /// Set once the worker stops after store or WAL failures outlast the retry window.
    stopped: Arc<AtomicBool>,
}

fn stopped_withdrawal_sessions() -> &'static Mutex<std::collections::HashSet<BindIdentity>> {
    static STOPPED: std::sync::OnceLock<Mutex<std::collections::HashSet<BindIdentity>>> =
        std::sync::OnceLock::new();
    STOPPED.get_or_init(Mutex::default)
}

impl WithdrawalTask {
    #[cfg(test)]
    pub(crate) fn spawn<W: WalStore + 'static>(
        wal: Arc<W>,
        key: BindIdentity,
        writer: Arc<WalHandle>,
        index: Option<Arc<dyn RunIndex>>,
        lock: Arc<Mutex<()>>,
        plane: Arc<dyn ElicitationPlane>,
        clock: broca_core::Clock,
    ) -> Self {
        Self::spawn_with_pending(wal, key, writer, index, lock, plane, clock, true)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn spawn_with_pending<W: WalStore + 'static>(
        wal: Arc<W>,
        key: BindIdentity,
        writer: Arc<WalHandle>,
        index: Option<Arc<dyn RunIndex>>,
        lock: Arc<Mutex<()>>,
        plane: Arc<dyn ElicitationPlane>,
        clock: broca_core::Clock,
        initial_pending: bool,
    ) -> Self {
        let cancel = tokio_util::sync::CancellationToken::new();
        let wake = Arc::new(tokio::sync::Notify::new());
        let pending = Arc::new(AtomicBool::new(initial_pending));
        let task_cancel = cancel.clone();
        let task_wake = wake.clone();
        let task_pending = pending.clone();
        let generation = Arc::new(AtomicU64::new(u64::from(initial_pending)));
        let task_generation = generation.clone();
        let settled_generation = Arc::new(AtomicU64::new(0));
        let task_settled = settled_generation.clone();
        // Actors can close and reopen within one process. A fresh actor must not reopen
        // the retry window for a session whose durable storage already failed for minutes.
        let already_stopped = stopped_withdrawal_sessions().lock().unwrap().contains(&key);
        let stopped = Arc::new(AtomicBool::new(already_stopped));
        let task_stopped = stopped.clone();
        let task = tokio::spawn(async move {
            if already_stopped {
                task_cancel.cancelled().await;
                return;
            }
            let mut backoff = std::time::Duration::from_millis(100);
            let mut refusals = RefusalWindows::<String>::default();
            // Store and WAL failures share one window: they are failures of this session's
            // durable state, not of any one question.
            let mut store_failures = RefusalWindows::<()>::default();
            if !initial_pending {
                tokio::select! { _ = task_cancel.cancelled() => return, _ = task_wake.notified() => {} }
            }
            loop {
                let pass_generation = task_generation.load(Ordering::Acquire);
                let result = service_pass_cancellable(
                    wal.clone(),
                    key.clone(),
                    writer.clone(),
                    index.clone(),
                    lock.clone(),
                    plane.as_ref(),
                    &clock,
                    &task_cancel,
                    &mut refusals,
                )
                .await;
                if task_cancel.is_cancelled() {
                    break;
                }
                let (retry, failure) = match result {
                    Ok(outcome) => (
                        !outcome.empty || outcome.cleanup_error.is_some(),
                        outcome.cleanup_error,
                    ),
                    Err(error) => (true, Some(error)),
                };
                match failure {
                    Some(error) => {
                        match store_failures.refused(&(), &error, tokio::time::Instant::now()) {
                            Refused::First => {
                                tracing::warn!(target: "approval", session = %key.session, retry_window_s = WITHDRAWAL_RETRY_WINDOW.as_secs(), "question withdrawal debt remains unknown; retrying quietly: {error}");
                            }
                            Refused::Retrying => {}
                            Refused::Exhausted(window) => {
                                // Nothing was settled, so the WAL and the run index still record
                                // the debt and the next process retries it. This worker stops so
                                // the session can close instead of failing every few seconds.
                                tracing::error!(target: "approval", session = %key.session, failures = window.refusals, "question withdrawal debt could not be read or cleaned up for {}s; no more attempts until restart: {error}", WITHDRAWAL_RETRY_WINDOW.as_secs());
                                stopped_withdrawal_sessions()
                                    .lock()
                                    .unwrap()
                                    .insert(key.clone());
                                task_stopped.store(true, Ordering::Release);
                                task_pending.store(false, Ordering::Release);
                                task_cancel.cancelled().await;
                                break;
                            }
                        }
                    }
                    None => {
                        if let Some(window) = store_failures.settled(&()) {
                            tracing::info!(target: "approval", session = %key.session, failures = window.refusals, "question withdrawal debt is readable again");
                        }
                    }
                }
                let changed = task_generation.load(Ordering::Acquire) != pass_generation;
                let retry = retry || changed;
                if !retry {
                    task_settled.store(pass_generation, Ordering::Release);
                }
                task_pending.store(retry, Ordering::Release);
                if retry {
                    tokio::select! { _ = task_cancel.cancelled() => break, _ = tokio::time::sleep(backoff) => {} }
                    backoff = backoff
                        .saturating_mul(2)
                        .min(std::time::Duration::from_secs(5));
                } else {
                    backoff = std::time::Duration::from_millis(100);
                    tokio::select! { _ = task_cancel.cancelled() => break, _ = task_wake.notified() => {} }
                    task_pending.store(true, Ordering::Release);
                }
            }
        });
        Self {
            task: Some(task),
            cancel,
            wake,
            pending,
            generation,
            settled_generation,
            stopped,
        }
    }

    pub(crate) fn wake(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.pending.store(true, Ordering::Release);
        self.wake.notify_one();
    }
    /// Whether this session must stay open for withdrawal work. A worker that gave up after
    /// persistent store failures owes nothing more in this process.
    pub(crate) fn pending(&self, _head: u64) -> bool {
        if self.stopped.load(Ordering::Acquire) {
            return false;
        }
        self.pending.load(Ordering::Acquire)
            || self.settled_generation.load(Ordering::Acquire)
                != self.generation.load(Ordering::Acquire)
    }
    #[cfg(test)]
    pub(crate) fn cancellation(&self) -> tokio_util::sync::CancellationToken {
        self.cancel.clone()
    }

    pub(crate) async fn stop(mut self) {
        self.cancel.cancel();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}

impl Drop for WithdrawalTask {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// How long a withdrawal keeps retrying after its first refusal before broca gives up on it.
///
/// The retryable refusals are a consent module that is reloading, warming or briefly
/// unreachable, and a scope that has not synced yet. Those clear within seconds; five
/// minutes also rides out a full daemon restart. The limit must not depend on the card's
/// own expiry, because a card filed without an expiry never expires, and while a session
/// still owes a withdrawal it cannot close. Giving up is safe: the card stays visible until
/// someone answers it or it expires, which costs only a stale prompt.
pub(crate) const WITHDRAWAL_RETRY_WINDOW: std::time::Duration =
    std::time::Duration::from_secs(5 * 60);

/// The durable reason recorded when broca stops retrying a withdrawal.
pub(crate) const WITHDRAWAL_RETRY_EXHAUSTED: &str = "withdrawal_retry_exhausted";

/// Cingulate's withdraw refusals that mean the card is already settled, pruned or owned by
/// someone else. Nothing is left for broca to withdraw, so these settle as withdrawn.
fn card_already_gone(code: &str) -> bool {
    matches!(code, "elicitation_not_found" | "elicitation_not_owner")
}

/// What one withdrawal attempt means for the debt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum WithdrawalStep {
    /// Stop owing the withdrawal, recording this answer and reason.
    Settle(broca_wal::WithdrawalAnswer, Option<String>),
    /// The refusal may clear on its own; try again. Carries the code for logs.
    Retry(String),
}

/// Classify a withdraw reply, or `None` when the attempt timed out.
///
/// Only a reply the plane marks retryable is retried. The consent plane derives that flag
/// from subc-protocol's shared route-open predicate (`is_retryable_route_open`) plus the
/// consent module's own transient codes, and replies `NotSent` only for refusals it judged
/// final. Every other refusal is final here too: no list of final codes is kept in broca.
pub(crate) fn withdrawal_step(reply: Option<ApprovalWithdrawal>) -> WithdrawalStep {
    use broca_wal::WithdrawalAnswer::{Acknowledged, NotSent};
    let code = match reply {
        None => return WithdrawalStep::Retry("withdrawal_timeout".into()),
        Some(ApprovalWithdrawal::Acknowledged) => {
            return WithdrawalStep::Settle(Acknowledged, None)
        }
        Some(ApprovalWithdrawal::Retry(error)) if error.retryable => {
            return WithdrawalStep::Retry(error.code)
        }
        Some(ApprovalWithdrawal::Retry(error)) => error.code,
        Some(ApprovalWithdrawal::NotSent { reason }) => reason,
    };
    if card_already_gone(&code) {
        WithdrawalStep::Settle(Acknowledged, Some(code))
    } else {
        WithdrawalStep::Settle(NotSent, Some(code))
    }
}

/// Retry windows for withdrawals that were refused, keyed by whatever names one debt.
///
/// A window opens at the first refusal and closes when the debt settles. Once a window is
/// older than [`WITHDRAWAL_RETRY_WINDOW`] the worker gives up on that debt. Times come from
/// Tokio's clock, so a test with a paused clock controls them, and the limit does not move
/// when the wall clock jumps.
pub(crate) struct RefusalWindows<K> {
    open: std::collections::HashMap<K, RefusalWindow>,
}

#[derive(Debug, Clone)]
pub(crate) struct RefusalWindow {
    pub(crate) since: tokio::time::Instant,
    pub(crate) refusals: u64,
    pub(crate) last_code: String,
}

/// Where a debt stands after one more refusal.
#[derive(Debug)]
pub(crate) enum Refused {
    /// The first refusal: log it once.
    First,
    /// Still inside the window: retry quietly.
    Retrying,
    /// The window has run out: give up and log it once.
    Exhausted(RefusalWindow),
}

impl<K> Default for RefusalWindows<K> {
    fn default() -> Self {
        Self {
            open: std::collections::HashMap::new(),
        }
    }
}

impl<K: std::hash::Hash + Eq + Clone> RefusalWindows<K> {
    /// Record a final refusal without discarding an earlier retry episode before it is logged.
    pub(crate) fn final_refusal(&mut self, key: &K, code: &str) -> bool {
        let window = self
            .open
            .entry(key.clone())
            .or_insert_with(|| RefusalWindow {
                since: tokio::time::Instant::now(),
                refusals: 0,
                last_code: code.to_owned(),
            });
        window.refusals += 1;
        code.clone_into(&mut window.last_code);
        window.refusals == 1
    }

    /// Return an exhausted window without forgetting it until settlement is durable.
    /// Storage failure must not start another retry window or repeat the first-refusal log.
    pub(crate) fn exhausted(
        &mut self,
        key: &K,
        now: tokio::time::Instant,
    ) -> Option<RefusalWindow> {
        let expired = self
            .open
            .get(key)
            .is_some_and(|window| now.duration_since(window.since) >= WITHDRAWAL_RETRY_WINDOW);
        if expired {
            self.open.get(key).cloned()
        } else {
            None
        }
    }

    pub(crate) fn refused(&mut self, key: &K, code: &str, now: tokio::time::Instant) -> Refused {
        let Some(window) = self.open.get_mut(key) else {
            self.open.insert(
                key.clone(),
                RefusalWindow {
                    since: now,
                    refusals: 1,
                    last_code: code.to_owned(),
                },
            );
            return Refused::First;
        };
        window.refusals += 1;
        code.clone_into(&mut window.last_code);
        if now.duration_since(window.since) >= WITHDRAWAL_RETRY_WINDOW {
            let window = window.clone();
            Refused::Exhausted(window)
        } else {
            Refused::Retrying
        }
    }

    /// Close the window when the debt settles; returns it if the debt had been refused.
    pub(crate) fn settled(&mut self, key: &K) -> Option<RefusalWindow> {
        self.open.remove(key)
    }

    /// Forget retry windows for debts no longer owed, for example after another writer
    /// settled them. The windows are process-local; restarting does not settle durable debt.
    pub(crate) fn retain(&mut self, mut owed: impl FnMut(&K) -> bool) {
        self.open.retain(|key, _| owed(key));
    }
}

/// What one service pass found.
pub(crate) struct PassOutcome {
    /// No withdrawal is owed any more.
    pub(crate) empty: bool,
    /// The first run-index cleanup step that failed. The pass must run again, and the
    /// worker treats it like any other store failure.
    pub(crate) cleanup_error: Option<String>,
}

#[cfg(test)]
pub(crate) async fn service_pass<W: WalStore + 'static>(
    wal: Arc<W>,
    key: BindIdentity,
    writer: Arc<WalHandle>,
    index: Option<Arc<dyn RunIndex>>,
    lock: Arc<Mutex<()>>,
    plane: &dyn ElicitationPlane,
    clock: &broca_core::Clock,
) -> Result<bool, String> {
    service_pass_with(
        wal,
        key,
        writer,
        index,
        lock,
        plane,
        clock,
        &mut RefusalWindows::default(),
    )
    .await
}

/// One pass that keeps refusal windows across calls, as the worker does.
#[cfg(test)]
#[allow(clippy::too_many_arguments)]
pub(crate) async fn service_pass_with<W: WalStore + 'static>(
    wal: Arc<W>,
    key: BindIdentity,
    writer: Arc<WalHandle>,
    index: Option<Arc<dyn RunIndex>>,
    lock: Arc<Mutex<()>>,
    plane: &dyn ElicitationPlane,
    clock: &broca_core::Clock,
    refusals: &mut RefusalWindows<String>,
) -> Result<bool, String> {
    let outcome = service_pass_cancellable(
        wal,
        key,
        writer,
        index,
        lock,
        plane,
        clock,
        &tokio_util::sync::CancellationToken::new(),
        refusals,
    )
    .await?;
    Ok(outcome.empty && outcome.cleanup_error.is_none())
}

/// How far one owed withdrawal got in a pass.
enum Attempt {
    Step(WithdrawalStep),
    /// The debt's retry window had already run out, so no attempt was made.
    GaveUp,
}

#[allow(clippy::too_many_arguments)]
async fn service_pass_cancellable<W: WalStore + 'static>(
    wal: Arc<W>,
    key: BindIdentity,
    writer: Arc<WalHandle>,
    index: Option<Arc<dyn RunIndex>>,
    lock: Arc<Mutex<()>>,
    plane: &dyn ElicitationPlane,
    clock: &broca_core::Clock,
    cancel: &tokio_util::sync::CancellationToken,
    refusals: &mut RefusalWindows<String>,
) -> Result<PassOutcome, String> {
    const CANCELLED: PassOutcome = PassOutcome {
        empty: false,
        cleanup_error: None,
    };
    let projection = {
        let wal = wal.clone();
        let key = key.clone();
        let writer = writer.clone();
        broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::WalRead, move || {
            read_projection(wal.as_ref(), &key, &writer)
        })
        .await
        .map_err(|e| e.to_string())??
    };
    if cancel.is_cancelled() {
        return Ok(CANCELLED);
    }
    let withdrawals = projection.obligations().withdrawals;
    refusals.retain(|call_key| {
        withdrawals
            .iter()
            .any(|owed| &owed.withdrawal.call_key == call_key)
    });
    for owed in withdrawals {
        if cancel.is_cancelled() {
            return Ok(CANCELLED);
        }
        let WalRecord::ToolCallDeferred {
            batch_id,
            scope,
            ask,
            ..
        } = &owed.prepared.record
        else {
            continue;
        };
        let call_key = owed.withdrawal.call_key.clone();
        let mut id = owed.withdrawal.elicitation_id;
        if clock.now_ms() >= ask.expires_at_ms {
            writer.record(WalRecord::QuestionWithdrawalAcknowledged {
                call_key: call_key.clone(),
                elicitation_id: id,
                answer: broca_wal::WithdrawalAnswer::NotSent,
                reason: Some("expired".into()),
            });
            writer.sync().await.map_err(|e| e.to_string())?;
            if let Some(window) = refusals.settled(&call_key) {
                tracing::info!(target: "approval", session = %key.session, %call_key, refusals = window.refusals, last_code = %window.last_code, "question withdrawal settled after refusals: the question expired");
            }
            continue;
        }
        let attempt = 'attempt: {
            if refusals
                .exhausted(&call_key, tokio::time::Instant::now())
                .is_some()
            {
                break 'attempt Attempt::GaveUp;
            }
            // A debt without its run cannot be serviced. It is held like a refused one, so
            // it is logged once and given up on at the same limit.
            let Some(run_id) = projection.calls[&call_key].run_id.clone() else {
                break 'attempt Attempt::Step(WithdrawalStep::Retry("no_originating_run".into()));
            };
            let ctx = BatchCtx {
                // Approval operations do not capture tool media.
                media_capture: None,
                batch_id: batch_id.clone(),
                bind: key.clone(),
                run_id,
                scope: Some(scope.clone()),
            };
            let elicitation_id = match &id {
                Some(known) => known.clone(),
                None => {
                    let filing = tokio::time::timeout(
                        std::time::Duration::from_millis(
                            ask.expires_at_ms.saturating_sub(clock.now_ms()),
                        )
                        .min(std::time::Duration::from_secs(5)),
                        plane.request(&ctx, &call_key, &owed.prepared.record, clock.now_ms()),
                    )
                    .await;
                    match filing {
                        Ok(Ok(filing)) => {
                            id = Some(filing.elicitation_id.clone());
                            filing.elicitation_id
                        }
                        Ok(Err(error)) if !error.retryable => {
                            break 'attempt Attempt::Step(WithdrawalStep::Settle(
                                broca_wal::WithdrawalAnswer::NotSent,
                                Some(error.code),
                            ))
                        }
                        Ok(Err(error)) => {
                            break 'attempt Attempt::Step(WithdrawalStep::Retry(error.code))
                        }
                        Err(_) => {
                            break 'attempt Attempt::Step(WithdrawalStep::Retry(
                                "filing_timeout".into(),
                            ))
                        }
                    }
                }
            };
            // A slow re-filing can cross the deadline. Never send a withdrawal past it.
            if clock.now_ms() >= ask.expires_at_ms {
                break 'attempt Attempt::Step(WithdrawalStep::Settle(
                    broca_wal::WithdrawalAnswer::NotSent,
                    Some("expired".into()),
                ));
            }
            let reply = tokio::time::timeout(
                std::time::Duration::from_millis(ask.expires_at_ms.saturating_sub(clock.now_ms()))
                    .min(std::time::Duration::from_secs(5)),
                plane.withdraw(&ctx, &elicitation_id),
            )
            .await;
            Attempt::Step(withdrawal_step(reply.ok()))
        };
        let (answer, reason) = match attempt {
            Attempt::Step(WithdrawalStep::Settle(answer, reason)) => {
                if let Some(code) = reason.as_deref() {
                    if refusals.final_refusal(&call_key, code) {
                        tracing::warn!(target: "approval", session = %key.session, %call_key, %code, "question withdrawal refused; the refusal is final");
                    }
                }
                (answer, reason)
            }
            Attempt::Step(WithdrawalStep::Retry(code)) => {
                match refusals.refused(&call_key, &code, tokio::time::Instant::now()) {
                    Refused::First => {
                        tracing::warn!(target: "approval", session = %key.session, %call_key, %code, retry_window_s = WITHDRAWAL_RETRY_WINDOW.as_secs(), "question withdrawal refused; retrying quietly until it settles or the retry window ends");
                        continue;
                    }
                    Refused::Retrying => continue,
                    Refused::Exhausted(_) => give_up(),
                }
            }
            Attempt::GaveUp => give_up(),
        };
        writer.record(WalRecord::QuestionWithdrawalAcknowledged {
            call_key: call_key.clone(),
            elicitation_id: id,
            answer,
            reason: reason.clone(),
        });
        writer.sync().await.map_err(|e| e.to_string())?;
        if let Some(window) = refusals.settled(&call_key) {
            if reason.as_deref() == Some(WITHDRAWAL_RETRY_EXHAUSTED) {
                tracing::warn!(target: "approval", session = %key.session, %call_key, refusals = window.refusals, last_code = %window.last_code, retry_window_s = WITHDRAWAL_RETRY_WINDOW.as_secs(), "question withdrawal given up; the question stays visible until it is answered or expires");
            } else {
                tracing::info!(target: "approval", session = %key.session, %call_key, refusals = window.refusals, last_code = %window.last_code, reason = reason.as_deref().unwrap_or("withdrawn"), "question withdrawal settled after refusals");
            }
        }
    }
    // After syncing withdrawal acknowledgements, read the entire WAL again before clearing
    // `withdrawals_owed`. The shared lock prevents a terminal index writer from setting the
    // flag between that read and the clear. A tail after the projection watermark can omit
    // older unanswered questions; a failed read cannot prove that there are none.
    let cancel = cancel.clone();
    broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::StoreWrite, move || {
        let _guard = lock.lock().map_err(|e| e.to_string())?;
        let projection = read_projection(wal.as_ref(), &key, &writer)?;
        let debt = projection.obligations();
        if cancel.is_cancelled() {
            return Ok(CANCELLED);
        }
        // Only the first failure is kept: the worker logs store failures once, not per run.
        let mut cleanup_error = None;
        if let Some(index) = index {
            let runs: std::collections::BTreeSet<_> = projection
                .calls
                .values()
                .filter_map(|call| call.run_id.as_ref().map(|id| id.0.clone()))
                .collect();
            for run_id in runs {
                let owed = debt.withdrawals.iter().any(|owed| {
                    projection.calls[&owed.withdrawal.call_key]
                        .run_id
                        .as_ref()
                        .is_some_and(|id| id.0 == run_id)
                });
                let row = match index.lookup_run(&run_id) {
                    Ok(row) => row,
                    Err(error) => {
                        cleanup_error.get_or_insert(format!(
                            "withdrawal index cleanup for run {run_id}: {error}"
                        ));
                        continue;
                    }
                };
                if !owed && row.is_some_and(|row| row.withdrawals_owed && row.state.is_terminal()) {
                    if cancel.is_cancelled() {
                        return Ok(CANCELLED);
                    }
                    if let Err(error) = index.index_run_withdrawals_owed(&run_id, false) {
                        cleanup_error.get_or_insert(format!(
                            "withdrawal index cleanup for run {run_id}: {error}"
                        ));
                    }
                }
            }
        }
        Ok(PassOutcome {
            empty: debt.withdrawals.is_empty(),
            cleanup_error,
        })
    })
    .await
    .map_err(|e| e.to_string())?
}

/// Stop retrying with a durable reason. Logging follows the WAL sync, not the attempt:
/// a failed sync must not announce settlement before it is durable.
fn give_up() -> (broca_wal::WithdrawalAnswer, Option<String>) {
    (
        broca_wal::WithdrawalAnswer::NotSent,
        Some(WITHDRAWAL_RETRY_EXHAUSTED.into()),
    )
}

#[derive(serde::Serialize)]
struct LateViewRef<'a> {
    call_key: &'a str,
    elicitation_id: &'a str,
    decision: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    outcome: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    result: Option<&'a broca_protocol::ToolResult>,
}
struct ByteCount(u64);
impl std::io::Write for ByteCount {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self.0.saturating_add(bytes.len() as u64);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn late_page(
    calls: &[broca_core::LateApprovalView],
    after: Option<&str>,
    cap: u64,
) -> Result<(Vec<broca_wire::LateApprovalView>, Option<String>), broca_wire::InvalidLateCursor> {
    let start = match after {
        None => 0,
        Some(cursor) => {
            calls
                .iter()
                .position(|call| call.call_key == cursor)
                .ok_or_else(|| broca_wire::InvalidLateCursor::new(cursor))?
                + 1
        }
    };
    let mut total = 0u64;
    let mut end = start;
    for call in &calls[start..] {
        let WalRecord::LateApprovalRecorded {
            elicitation_id,
            decision,
            ..
        } = &*call.approval
        else {
            continue;
        };
        let view = LateViewRef {
            call_key: &call.call_key,
            elicitation_id,
            decision: match decision {
                broca_wal::LateDecision::Approved => "approved",
                broca_wal::LateDecision::Declined => "declined",
                broca_wal::LateDecision::None => "none",
            },
            outcome: call.outcome.map(|outcome| match outcome {
                broca_wal::LateOutcome::Completed => "completed",
                broca_wal::LateOutcome::NotSent => "not_sent",
                broca_wal::LateOutcome::Unknown => "unknown",
            }),
            result: call.result.as_deref(),
        };
        let mut size = ByteCount(0);
        if serde_json::to_writer(&mut size, &view).is_err() {
            size.0 = u64::MAX;
        }
        if end > start && total.saturating_add(size.0) > cap {
            break;
        }
        total = total.saturating_add(size.0);
        end += 1;
        if total > cap {
            break;
        }
    }
    let next = (end < calls.len() && end > start).then(|| calls[end - 1].call_key.clone());
    Ok((late_views(&calls[start..end]), next))
}

#[derive(Default)]
pub(crate) struct ActorJobs {
    count: std::sync::atomic::AtomicUsize,
    done: tokio::sync::Notify,
}
pub(crate) struct ActorJob {
    jobs: Arc<ActorJobs>,
    writer: Option<Arc<WalHandle>>,
}
impl ActorJobs {
    pub(crate) fn track(self: &Arc<Self>, writer: Arc<WalHandle>) -> ActorJob {
        self.count.fetch_add(1, Ordering::AcqRel);
        ActorJob {
            jobs: self.clone(),
            writer: Some(writer),
        }
    }
    pub(crate) async fn join(&self) {
        loop {
            let done = self.done.notified();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            done.await;
        }
    }
}
impl ActorJob {
    pub(crate) fn writer(&self) -> &WalHandle {
        self.writer.as_deref().unwrap()
    }
}
impl Drop for ActorJob {
    fn drop(&mut self) {
        drop(self.writer.take());
        self.jobs.count.fetch_sub(1, Ordering::AcqRel);
        self.jobs.done.notify_waiters();
    }
}
