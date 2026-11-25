//! How a tool batch sends its calls and takes their answers: every call dispatched, in parallel
//! unless one must run alone, each answer appended as it arrives, and the batch paused when only
//! calls a provider holds are still waiting.

use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

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
pub(super) struct DispatchPlan<'p> {
    pub(super) ctx: &'p BatchCtx,
    pub(super) calls: &'p [ToolCall],
    pub(super) keys: &'p [(ToolCallId, String)],
    /// Per call, in order: the provider's held reply when the call was held before the run
    /// paused; that call waits for its late result again instead of being sent.
    pub(super) held: &'p [Option<&'p ProviderHeldReply>],
    /// If registering the group failed, new calls cannot be sent safely.
    pub(super) registration_error: Option<&'p str>,
    /// Proof the run's scope had ended when the batch opened.
    pub(super) scope_end: Option<String>,
    pub(super) guard: &'p ToolOpenGuard,
    pub(super) batch_id: &'p str,
    pub(super) run_id: &'p str,
    pub(super) sequential: bool,
    pub(super) hooks: Option<&'p broca_core::hooks::HookRuntime<'p>>,
    pub(super) step_id: u64,
    /// Tells the compaction provider once a held call's result is durable.
    pub(super) changes: Option<&'p crate::compaction::Changes<'p>>,
}

/// How a batch's dispatch ended. A paused batch reports which calls still need an answer.
pub(super) enum Dispatched {
    /// Every call answered. Providers are asked to withdraw any closed holds once the run ends.
    All {
        closed: Vec<ProviderHeldReply>,
        scope_end: Option<String>,
    },
    /// Only calls a provider holds were still waiting.
    Held(Vec<bool>),
}

/// Dispatches every call, in parallel unless one of them must run alone, and collects their
/// answers:
/// - each call carries its key and the batch's held-reply recorder; a held reply is appended,
///   durably, while the other calls keep running;
/// - every call's answer is appended as soon as it arrives, so a reader sees it while the
///   batch's other calls still run; a held or handed-back call's answer durably, before any call
///   queued behind it is sent, and the plane is then told it's durable;
/// - an unknown outcome closes just that call; other calls still run or wait for their answers;
/// - when every call still running is held and still waiting for its provider's answer, the
///   dispatch stops and the run pauses. A held call whose answer is in is only finishing local
///   work (its media check and post-tool hooks), and is awaited as any call is.
pub(super) async fn dispatch_all<W: Writer, P, T: ToolPlane>(
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
    let mut settled = vec![false; calls.len()];
    // Per call, whether its provider has answered: its dispatch or its held wait returned, so
    // what is left of its job is local work. Pausing then would drop an answer already taken
    // from the provider, with nothing left to resume the run for it.
    let returned: Vec<AtomicBool> = calls.iter().map(|_| AtomicBool::new(false)).collect();
    let returned = &returned;
    let still_waiting = |id: &ToolCallId| {
        calls
            .iter()
            .position(|call| call.tool_call_id == *id)
            .is_some_and(|index| !returned[index].load(Ordering::Relaxed))
    };
    let mut jobs = FuturesUnordered::new();
    // Per call, what stops its job: a held call a cancel closes must not keep its batch waiting
    // on a provider that may never answer, as the calls queued behind it would.
    let mut aborts: Vec<Option<futures::future::AbortHandle>> =
        calls.iter().map(|_| None).collect();
    let mut next = 0;
    let mut closed: Vec<ProviderHeldReply> = Vec::new();
    let mut scope_end = plan.scope_end.clone();
    loop {
        // A cancel or a dead scope closes a hold that still has no answer. Calls already
        // answering finish as usual; providers are asked to withdraw the closed holds after
        // the run's end is durable.
        let cancelled = seams.cancel.is_cancelled();
        if (cancelled || scope_end.is_some()) && !open_held.is_empty() {
            let (waiting, answered_held): (Vec<_>, Vec<_>) = std::mem::take(&mut open_held)
                .into_iter()
                .partition(|id| still_waiting(id));
            open_held = answered_held;
            for id in waiting {
                let index = calls
                    .iter()
                    .position(|call| call.tool_call_id == id)
                    .expect("a held call is one of the batch's");
                let (result, outcome, code) = match scope_end.as_ref().filter(|_| !cancelled) {
                    Some(reason) => (
                        broca_core::tool_results::live_outcome_unknown_result(
                            &id,
                            &format!(
                                "the run's scope ended while the provider held the call ({reason})"
                            ),
                        ),
                        CallOutcome::Unknown,
                        cortexkit_role_llm_runner::errors::tool_result_reasons::OUTCOME_UNKNOWN,
                    ),
                    None => held_close_result(&id, RunFinishReason::Cancelled),
                };
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
                settled[index] = true;
                if let Some(abort) = aborts[index].take() {
                    abort.abort();
                }
            }
        }
        while next < calls.len() && (!plan.sequential || jobs.is_empty()) {
            let index = next;
            next += 1;
            if settled[index] {
                continue;
            }
            let call = &calls[index];
            let held = plan.held[index];
            let scope_end = scope_end.clone();
            let key = key_of(call);
            let (job, abort) = futures::future::abortable(async move {
                if let Some(reply) = held {
                    match seams.held {
                        Some(lifecycle) => {
                            let waited = lifecycle.wait(plan.ctx, call, reply).await;
                            returned[index].store(true, Ordering::Relaxed);
                            answered(plan, call, key, checked(plan.ctx, waited)).await
                        }
                        None => std::future::pending().await,
                    }
                } else if let Some(reason) = scope_end {
                    Resolved {
                        reason: Some(reason.clone()),
                        ..resolve(
                            call,
                            DispatchResult::ScopeDrained {
                                reason,
                                sent: false,
                            },
                        )
                        .into()
                    }
                } else if let Some(reason) = plan.registration_error {
                    resolve(
                        call,
                        DispatchResult::NotSent {
                            reason: reason.into(),
                        },
                    )
                    .into()
                } else {
                    dispatch_one(seams.tools, plan, call, key, recorder, &returned[index]).await
                }
            });
            aborts[index] = Some(abort);
            jobs.push(async move { (index, job.await.ok()) });
        }
        if settled.iter().all(|&done| done) {
            return Ok(Dispatched::All { closed, scope_end });
        }
        if !open_held.is_empty() && jobs.len() == open_held.len() {
            // Only held calls wait. Anything already answered is taken first, then the run pauses.
            if let Some(Some((index, answer))) = futures::FutureExt::now_or_never(jobs.next()) {
                let Some(answer) = answer else {
                    continue;
                };
                let ended = settle(writer, seams, plan, &mut open_held, index, answer).await?;
                scope_end = scope_end.or(ended);
                settled[index] = true;
                continue;
            }
            // The run pauses only while every call left still waits for its provider: a held
            // call whose answer is in is waited for below, as any call is.
            if open_held.iter().all(&still_waiting) {
                // A cancel that came in meanwhile closes the held calls instead of pausing on
                // them.
                if seams.cancel.is_cancelled() {
                    continue;
                }
                return Ok(Dispatched::Held(settled));
            }
        }
        // While a provider holds a call still waiting for its answer, a cancel also wakes the
        // dispatch, to close it.
        let hold_open = open_held.iter().any(&still_waiting);
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
                // A held call a cancel already closed: its late answer, or its job's end once
                // stopped, is ignored.
                let Some(answer) = answer.filter(|_| !settled[index]) else {
                    continue;
                };
                let ended = settle(writer, seams, plan, &mut open_held, index, answer).await?;
                scope_end = scope_end.or(ended);
                settled[index] = true;
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
async fn settle<W: Writer, P, T: ToolPlane>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    plan: &DispatchPlan<'_>,
    open_held: &mut Vec<ToolCallId>,
    index: usize,
    resolved: Resolved,
) -> Result<Option<String>, LoopError> {
    let Resolved {
        result,
        outcome,
        reason,
        scope_end,
        detail,
        hooks,
        yielded_by,
    } = resolved;
    let id = plan.calls[index].tool_call_id.clone();
    // A handed-back call is durable at once too, naming its provider: from then on the
    // provider's terminal for it is a notice, even across a crash before the batch ends.
    let yielded = yielded_by.is_some();
    let durable = open_held.contains(&id) || yielded;
    if durable {
        let mut records = Vec::new();
        terminal_with_reason(
            &mut records,
            plan.run_id,
            plan.batch_id,
            result,
            outcome,
            reason,
            Some((seams.now_ms)()),
        );
        if let Some(Record::ToolOutcome(record)) = records.last_mut() {
            record.hooks = hooks.map(Into::into);
            record.detail = detail.map(Into::into);
            record.yielded_by = yielded_by.clone();
        }
        writer.append(&records, true).await?;
        open_held.retain(|held| *held != id);
        if let Some(changes) = plan.changes {
            changes.tell(writer, seams).await;
        }
        if let Some(lifecycle) = seams.held {
            match &yielded_by {
                // The provider still owes the handed-back call its own terminal, which arrives
                // on the provider's late-results feed; a handed-back call was never in a
                // waiting slot, so there is nothing to release.
                Some(carrier) => lifecycle.yielded_synced(plan.ctx, carrier).await,
                None => lifecycle.terminal_synced(plan.ctx, &id).await,
            }
        }
    } else {
        // Written and synced now: a reader sees it while the batch's other calls run (a `say` by a
        // long `bash`), and a crash can't lose a result a reader saw, which would make it unknown.
        // The next request still lists the batch's results in the order its calls were planned.
        let mut records = Vec::new();
        terminal_with_reason(
            &mut records,
            plan.run_id,
            plan.batch_id,
            result,
            outcome,
            reason,
            Some((seams.now_ms)()),
        );
        if let Some(Record::ToolOutcome(record)) = records.last_mut() {
            record.hooks = hooks.map(Into::into);
            record.detail = detail.map(Into::into);
        }
        writer.append(&records, true).await?;
    }
    Ok(scope_end)
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

/// Sends one call with its key and the batch's held-reply recorder, and resolves the answer. A
/// result the provider returned goes through the post-tool hooks the plan subscribes for its
/// tool, before the batch takes it. `returned` is set once the provider has answered.
async fn dispatch_one<T: ToolPlane>(
    tools: &T,
    plan: &DispatchPlan<'_>,
    call: &ToolCall,
    call_key: &str,
    recorder: &HeldChannel,
    returned: &AtomicBool,
) -> Resolved {
    let key = DispatchKey {
        call_key,
        held_recorder: Some(recorder),
    };
    let sent = tools
        .dispatch_guarded(plan.ctx, call, Some(key), plan.guard)
        .await;
    returned.store(true, Ordering::Relaxed);
    answered(plan, call, call_key, checked(plan.ctx, sent)).await
}

/// What a call's provider answered, as the batch takes it. A result goes through the post-tool
/// hooks the plan subscribes for its tool, whether it came back on the dispatch or later, after
/// a hold: a hook that edits or withholds a result must see every result the model will.
async fn answered(
    plan: &DispatchPlan<'_>,
    call: &ToolCall,
    call_key: &str,
    answer: DispatchResult,
) -> Resolved {
    use cortexkit_role_llm_runner::errors::tool_result_reasons::OUTCOME_UNKNOWN;
    match answer {
        // A matched reply can still be unusable (for example, a held result that expired).
        DispatchResult::Completed(result) if matches!(&result.output, ToolOutput::Error { code: Some(code), .. } if code == OUTCOME_UNKNOWN) => {
            Resolved {
                reason: Some(OUTCOME_UNKNOWN.into()),
                ..(result, CallOutcome::Unknown).into()
            }
        }
        DispatchResult::Completed(result) => {
            post_tool(plan.hooks, plan.step_id, call, call_key, result).await
        }
        // The interim content a hand-back answered with is the call's result, as any result is.
        DispatchResult::Yielded { result, carrier } => Resolved {
            yielded_by: Some(carrier),
            ..post_tool(plan.hooks, plan.step_id, call, call_key, result).await
        },
        // The refusal's code is the outcome's reason unless a post-tool hook gave one.
        DispatchResult::Refused {
            result,
            code,
            detail,
        } => {
            let resolved = post_tool(plan.hooks, plan.step_id, call, call_key, result).await;
            Resolved {
                reason: resolved.reason.clone().or(Some(code)),
                detail: Some(detail),
                ..resolved
            }
        }
        DispatchResult::ScopeDrained { ref reason, sent } => {
            let scope_end = (plan.ctx.scope.is_some()
                && broca_core::tool_plane::scope_has_ended(reason))
            .then(|| reason.clone());
            let reason = Some(if sent {
                OUTCOME_UNKNOWN.into()
            } else {
                reason.clone()
            });
            Resolved {
                reason,
                scope_end,
                ..resolve(call, answer).into()
            }
        }
        DispatchResult::SentButOutcomeUnknown { .. } => Resolved {
            reason: Some(OUTCOME_UNKNOWN.into()),
            ..resolve(call, answer).into()
        },
        other => resolve(call, other).into(),
    }
}
