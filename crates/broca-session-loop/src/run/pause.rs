//! How a run pauses at a boundary: for a sign-in its account needs, at the run's start or
//! later, and for any other reason. Each writes the pause durably before the run reports it.

use super::*;

pub(super) async fn sign_in_refused<W: Writer, P: Provider, T>(
    writer: &W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    error: &ProviderError,
    step_id: u64,
) -> Result<Vec<String>, LoopError> {
    if crate::account_move::signed_out_selection(seams, admission, error, step_id).is_none() {
        return Ok(Vec::new());
    }
    Ok(crate::account_move::refused_in_step(
        &writer.open_log_records().await?,
        &admission.run_id,
        step_id,
    ))
}

pub(super) async fn sign_in_at_start<W: Writer, P: Provider, T: ToolPlane>(
    writer: &mut W,
    parts: &dyn Parts,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    compaction: Option<&crate::compaction::Ctx>,
    mut error: ProviderError,
) -> Result<Finished, LoopError> {
    let step_id = run_state(writer).completed_steps.saturating_add(1);
    let mut refused = sign_in_refused(writer, seams, admission, &error, step_id).await?;
    let sign_in = match crate::account_move::signed_out(
        writer,
        parts,
        seams,
        admission,
        step_id,
        &error,
        &mut refused,
    )
    .await?
    {
        Some(crate::account_move::AtWall::Moved(moved)) => {
            crate::account_move::record_move(writer, seams, admission, &moved).await?;
            seams.provider.close_run_transport();
            let admission = frozen_admission(run_state(writer), parts)?;
            let moved_seams = seams.moved_to(&moved.provider, moved.selection.clone());
            let mut transport = crate::transport::Guard::bind(&moved.provider, &seams.bind);
            let moved_compaction = if compaction.is_none() {
                compaction_ctx(writer, parts, &admission, &moved_seams, false).await?
            } else {
                None
            };
            let finished = drive(
                writer,
                parts,
                &admission,
                &moved_seams,
                compaction.or(moved_compaction.as_ref()),
            )
            .await?;
            transport.finished(&finished);
            return Ok(finished);
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
        Some(crate::account_move::AtWall::Stopped) if seams.cancel.is_cancelled() => {
            return finish(writer, seams, &admission.run_id, RunFinishReason::Cancelled).await
        }
        _ => None,
    };
    let mut finished = pause_for_sign_in(writer, seams, admission, error).await?;
    finished.sign_in = sign_in;
    Ok(finished)
}

/// Retains only answers already deposited by held-call providers before parking for sign-in.
/// No unsent tool or held successor is dispatched while the model account is unavailable.
pub(super) async fn pause_for_sign_in<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    error: ProviderError,
) -> Result<Finished, LoopError> {
    if let (Some(lifecycle), Some(batch)) = (seams.held, run_state(writer).open_batch.as_ref()) {
        let ctx = BatchCtx {
            batch_id: BatchId(batch.batch_id.clone()),
            bind: seams.bind.clone(),
            run_id: RunId(admission.run_id.clone()),
            scope: admission.send.scope.clone(),
            media_capture: writer.media(),
        };
        // A deposited result is taken as a held wait takes it: its media checked against what
        // the batch captured, then through the post-tool hooks subscribed for its tool.
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
        let step_id = batch.step_id;
        let planned = planned_calls(writer);
        let mut records = Vec::new();
        let mut delivered = Vec::new();
        for held in known_calls(writer).into_iter().filter_map(|call| call.held) {
            let Some(result) = lifecycle.deposited_result(&ctx, &held) else {
                continue;
            };
            let Some(call) = planned
                .iter()
                .find(|call| call.tool_call_id == held.tool_call_id)
            else {
                continue;
            };
            let resolved = match checked(&ctx, DispatchResult::Completed(result)) {
                DispatchResult::Completed(result) => {
                    post_tool(hooks.as_ref(), step_id, call, &held.call_key, result).await
                }
                other => resolve(call, other).into(),
            };
            terminal_with_reason(
                &mut records,
                &admission.run_id,
                &ctx.batch_id.0,
                resolved.result,
                resolved.outcome,
                resolved.reason,
                Some((seams.now_ms)()),
            );
            if let Some(Record::ToolOutcome(record)) = records.last_mut() {
                record.hooks = resolved.hooks.map(Into::into);
            }
            delivered.push(held.tool_call_id);
        }
        // Acknowledge only after the terminal barrier, independently of the auth pause.
        writer.append(&records, true).await?;
        for id in delivered {
            lifecycle.terminal_synced(&ctx, &id).await;
        }
        if run_state(writer).open_batch.as_ref().is_some_and(|batch| {
            (0..batch.calls.len()).all(|position| batch.state(position) == CallState::Terminated)
        }) {
            writer
                .append(
                    &[Record::BatchEnd(BatchEnd {
                        run_id: admission.run_id.clone(),
                        batch_id: ctx.batch_id.0,
                    })],
                    true,
                )
                .await?;
        }
    }
    pause(
        writer,
        seams,
        &admission.run_id,
        PauseReason::AuthRequired,
        Some(error),
        None,
        None,
    )
    .await
}

/// Pauses the run at a boundary, durably, before the run reports it. A caller's cancel wins
/// until the pause is durable: the run then ends cancelled instead, so nobody is left with a
/// paused run they already abandoned. Messages steered in stay unplaced, for the resume.
///
/// A run that pauses on calls a provider holds (`held`, the batch they belong to) has its batch
/// open. When the cancel wins, that batch closes in the same durable write as the run's end, as
/// cancelling a parked run closes it: each held call gets its close result and each call still
/// in flight the unknown-outcome result. A run that ended with its batch open would leave its
/// tool turn without results, and the session's next run could not number its messages.
pub(super) async fn pause<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    run_id: &str,
    reason: PauseReason,
    error: Option<ProviderError>,
    account_limit: Option<Box<broca_wal::AccountLimitPause>>,
    held: Option<&BatchCtx>,
) -> Result<Finished, LoopError> {
    let error = error
        .map(|error| serde_json::to_value(error).map_err(|e| LoopError::Parts(e.to_string())))
        .transpose()?;
    let cancelled_now = seams.cancel.is_cancelled();
    let now = (seams.now_ms)();
    // Only the run writes its batch's records, so the close-out read here still holds when the
    // write below decides.
    let (close_out, withdrawn) =
        open_batch_close_out(writer, run_id, now, RunFinishReason::Cancelled);
    let mut close_out = Some(close_out);
    let mut cancelled_end = Some(run_end(writer, run_id, RunFinishReason::Cancelled, now));
    let mut pause = Some(Record::Pause(Pause {
        run_id: run_id.into(),
        reason,
        // An account-limit pause is dated by when it decided to pause, as today's is.
        ts_ms: Some(
            account_limit
                .as_ref()
                .map_or_else(|| (seams.now_ms)(), |pause| pause.paused_at_ms),
        ),
        error: error.map(Into::into),
        account_limit: (account_limit
            .map(|pause| serde_json::to_value(pause).map_err(|e| LoopError::Parts(e.to_string())))
            .transpose()?)
        .map(Into::into),
    }));
    let owned_run_id = run_id.to_string();
    let batch = writer
        .append_with(
            &mut |_, operations, _| {
                let run = operations.run.as_ref().expect("a run is active");
                if cancelled_now || run.cancel.is_some() {
                    let mut batch = close_out.take().unwrap_or_default();
                    batch.extend(placed_copies(operations, &owned_run_id));
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
        if let (Some(lifecycle), Some(ctx)) = (seams.held, held) {
            for reply in &withdrawn {
                lifecycle.terminal_synced(ctx, &reply.tool_call_id).await;
            }
        }
        withdraw(seams, &withdrawn).await;
        return Ok(finished(writer, RunFinishReason::Cancelled));
    }
    let mut paused = finished(writer, RunFinishReason::Interrupted);
    paused.paused = Some(reason);
    Ok(paused)
}
