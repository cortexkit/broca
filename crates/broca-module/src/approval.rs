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
        let task = tokio::spawn(async move {
            let mut backoff = std::time::Duration::from_millis(100);
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
                )
                .await;
                if task_cancel.is_cancelled() {
                    break;
                }
                let retry = match result {
                    Ok(empty) => !empty,
                    Err(error) => {
                        tracing::warn!(target: "approval", "question withdrawal debt remains unknown: {error}");
                        true
                    }
                };
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
        }
    }

    pub(crate) fn wake(&self) {
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.pending.store(true, Ordering::Release);
        self.wake.notify_one();
    }
    pub(crate) fn pending(&self, _head: u64) -> bool {
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

pub(crate) fn terminal_refusal(code: &str) -> bool {
    matches!(
        code,
        "answered"
            | "expired"
            | "elicitation_not_owner"
            | "elicitation_not_found"
            | "session_ended"
            | "scope_ended"
            | "scope_parent_ended"
            | "scope_not_live"
            | "scope_epoch_mismatch"
            | "scope_not_carrier"
            | "scope_unsupported"
            | "scope_unverifiable"
    )
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
    service_pass_cancellable(
        wal,
        key,
        writer,
        index,
        lock,
        plane,
        clock,
        &tokio_util::sync::CancellationToken::new(),
    )
    .await
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
) -> Result<bool, String> {
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
        return Ok(false);
    }
    for owed in projection.obligations().withdrawals {
        if cancel.is_cancelled() {
            return Ok(false);
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
        let mut id = owed.withdrawal.elicitation_id;
        if clock.now_ms() >= ask.expires_at_ms {
            writer.record(WalRecord::QuestionWithdrawalAcknowledged {
                call_key: owed.withdrawal.call_key,
                elicitation_id: id,
                answer: broca_wal::WithdrawalAnswer::NotSent,
                reason: Some("expired".into()),
            });
            writer.sync().await.map_err(|e| e.to_string())?;
            continue;
        }
        let Some(run_id) = projection.calls[&owed.withdrawal.call_key].run_id.clone() else {
            tracing::warn!(target: "approval", call_key = %owed.withdrawal.call_key, "withdrawal has no originating run; retaining it until expiry");
            continue;
        };
        let ctx = BatchCtx {
            batch_id: batch_id.clone(),
            bind: key.clone(),
            run_id,
            scope: Some(scope.clone()),
        };
        let mut terminal = None;
        if clock.now_ms() >= ask.expires_at_ms {
            terminal = Some((broca_wal::WithdrawalAnswer::NotSent, Some("expired".into())));
        } else if id.is_none() {
            let filing = tokio::time::timeout(
                std::time::Duration::from_millis(ask.expires_at_ms.saturating_sub(clock.now_ms()))
                    .min(std::time::Duration::from_secs(5)),
                plane.request(
                    &ctx,
                    &owed.withdrawal.call_key,
                    &owed.prepared.record,
                    clock.now_ms(),
                ),
            )
            .await;
            match filing {
                Ok(Ok(filing)) => id = Some(filing.elicitation_id),
                Ok(Err(error)) if !error.retryable && terminal_refusal(&error.code) => {
                    terminal = Some((broca_wal::WithdrawalAnswer::NotSent, Some(error.code)))
                }
                _ => continue,
            }
        }
        if terminal.is_none() {
            // A slow re-filing can cross the deadline. Never send a withdrawal past it.
            if clock.now_ms() >= ask.expires_at_ms {
                terminal = Some((broca_wal::WithdrawalAnswer::NotSent, Some("expired".into())));
            } else if let Some(id) = &id {
                let withdrawal = tokio::time::timeout(
                    std::time::Duration::from_millis(
                        ask.expires_at_ms.saturating_sub(clock.now_ms()),
                    )
                    .min(std::time::Duration::from_secs(5)),
                    plane.withdraw(&ctx, id),
                )
                .await;
                terminal = match withdrawal {
                    Ok(ApprovalWithdrawal::Acknowledged) => {
                        Some((broca_wal::WithdrawalAnswer::Acknowledged, None))
                    }
                    Ok(ApprovalWithdrawal::NotSent { reason }) if terminal_refusal(&reason) => {
                        Some((broca_wal::WithdrawalAnswer::NotSent, Some(reason)))
                    }
                    Ok(ApprovalWithdrawal::Retry(error))
                        if !error.retryable && terminal_refusal(&error.code) =>
                    {
                        Some((broca_wal::WithdrawalAnswer::NotSent, Some(error.code)))
                    }
                    _ => None,
                };
            }
        }
        if let Some((answer, reason)) = terminal {
            writer.record(WalRecord::QuestionWithdrawalAcknowledged {
                call_key: owed.withdrawal.call_key,
                elicitation_id: id,
                answer,
                reason,
            });
            writer.sync().await.map_err(|e| e.to_string())?;
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
            return Ok(false);
        }
        let mut cleanup_failed = false;
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
                    Err(error) => { cleanup_failed = true; tracing::warn!(target: "approval", %run_id, "withdrawal index cleanup will retry: {error}"); continue; }
                };
                if !owed
                    && row.is_some_and(|row| row.withdrawals_owed && row.state.is_terminal())
                {
                    if cancel.is_cancelled() {
                        return Ok(false);
                    }
                    if let Err(error) = index.index_run_withdrawals_owed(&run_id, false) {
                        cleanup_failed = true;
                        tracing::warn!(target: "approval", %run_id, "withdrawal index cleanup will retry: {error}");
                    }
                }
            }
        }
        Ok(debt.withdrawals.is_empty() && !cleanup_failed)
    })
    .await
    .map_err(|e| e.to_string())?
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
