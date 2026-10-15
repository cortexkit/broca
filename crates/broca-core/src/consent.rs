//! Check consent answers against frozen tool arguments and recover missing card ids
//! without reopening calls already closed by crash recovery.
use crate::tool_plane::{
    ApprovalCursor, ApprovalExecution, ApprovalRouteError, ApprovalWithdrawal, BatchCtx,
    ElicitationPlane,
};
use broca_wal::{AwaitingApprovalEntry, WalRecord, WithdrawalAnswer};
use serde_json::{json, Value};
use std::{future::Future, sync::Mutex, time::Duration};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConsentDecision {
    Approved,
    Declined,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConsentAnswer {
    pub id: String,
    pub decision: ConsentDecision,
}

pub fn refusal(code: &str, retryable: bool) -> ApprovalRouteError {
    ApprovalRouteError {
        code: code.into(),
        retryable,
    }
}

/// The returned binding is opaque to Cingulate; broca alone checks all four frozen fields.
pub fn binding(prepared: &WalRecord) -> Value {
    let WalRecord::ToolCallDeferred {
        args_hash,
        module,
        schema_pin,
        tool,
        ..
    } = prepared
    else {
        panic!("consent binding requires ToolCallDeferred")
    };
    json!({"args_hash":args_hash,"module":module,"schema_pin":schema_pin,"tool":tool})
}

/// Only an explicit answered approve with the exact binding authorizes a call.
/// Expired records ignore legacy choice metadata, which is not a person's live answer.
pub fn answer(
    record: &Value,
    key: &str,
    prepared: &WalRecord,
) -> Result<Option<ConsentAnswer>, ApprovalRouteError> {
    if record.get("requester_ref").and_then(Value::as_str) != Some(key) {
        return Ok(None);
    }
    let state = record.get("state").and_then(Value::as_str);
    if state == Some("pending") {
        return Ok(None);
    }
    let id = record
        .get("elicitation_id")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or_else(|| refusal("consent_record_unreadable", true))?;
    let decision = match state {
        Some("answered") => {
            let expected = binding(prepared);
            let returned = record
                .get("binding")
                .ok_or_else(|| refusal("consent_binding_mismatch", false))?;
            let returned = serde_jcs::to_vec(returned)
                .map_err(|_| refusal("consent_binding_mismatch", false))?;
            if returned != serde_jcs::to_vec(&expected).expect("frozen binding is canonicalizable")
            {
                return Err(refusal("consent_binding_mismatch", false));
            }
            match record.get("answered_choice_id").and_then(Value::as_str) {
                Some("approve") => ConsentDecision::Approved,
                Some("deny") => ConsentDecision::Declined,
                Some(_) => return Err(refusal("consent_choice_unsupported", false)),
                None => return Err(refusal("consent_record_unreadable", true)),
            }
        }
        Some("expired" | "withdrawn" | "closed") => ConsentDecision::None,
        _ => return Err(refusal("consent_record_unreadable", true)),
    };
    Ok(Some(ConsentAnswer {
        id: id.into(),
        decision,
    }))
}

/// Scan the bound owner's settled-card feed from zero, following every continuation
/// before selecting an answer. Runs sharing that owner also share this feed: acknowledging
/// a cursor would discard another run's answers, so no run sends an ack.
pub async fn lookup(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
) -> Result<Option<ConsentAnswer>, ApprovalRouteError> {
    lookup_with_progress(
        plane,
        ctx,
        key,
        prepared,
        &Mutex::new(LookupProgress::default()),
    )
    .await
}

/// Cursor and binding-checked settlement decisions for one call's owner-feed scan.
/// Save progress only after an entire page is readable, so a timed-out RPC can
/// resume without losing earlier decisions. This is not WAL authorization state:
/// a new process must scan from zero before authorizing a pending recorded id.
#[derive(Debug, Default)]
pub struct LookupProgress {
    key: Option<String>,
    binding: Option<Value>,
    cursor: Option<ApprovalCursor>,
    found: Option<ConsentAnswer>,
    complete: bool,
    expected: Option<ConsentAnswer>,
}
impl LookupProgress {
    pub fn restart(&mut self) {
        self.cursor = None;
        self.found = None;
        self.complete = false;
        self.expected = None;
    }
    pub fn expected(&self) -> Option<ConsentAnswer> {
        self.expected.clone()
    }
}

pub async fn lookup_with_progress(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    progress: &Mutex<LookupProgress>,
) -> Result<Option<ConsentAnswer>, ApprovalRouteError> {
    let (mut cursor, mut found) = {
        let mut state = progress.lock().expect("consent scan progress");
        let expected = binding(prepared);
        if state.key.as_deref() != Some(key) || state.binding.as_ref() != Some(&expected) {
            state.restart();
            state.key = Some(key.into());
            state.binding = Some(expected);
        }
        if state.complete {
            return Ok(state.found.clone());
        }
        (state.cursor.clone(), state.found.clone())
    };
    let mut reset = false;
    loop {
        let page = match plane.answers(ctx, cursor.as_ref()).await {
            Err(error) if error.code == "cursor_incarnation_changed" && !reset => {
                cursor = None;
                let mut state = progress.lock().expect("consent scan progress");
                let expected = state.expected.clone();
                state.restart();
                state.expected = expected.clone();
                state.found = expected.clone();
                found = expected;
                reset = true;
                continue;
            }
            result => result?,
        };
        for record in &page.records {
            if let Some(candidate) = answer(record, key, prepared)? {
                if found
                    .as_ref()
                    .is_some_and(|old| old.decision != candidate.decision)
                {
                    return Err(refusal("consent_conflicting_decisions", false));
                }
                if found.is_none() {
                    found = Some(candidate);
                }
            }
        }
        if page.records.is_empty() {
            let mut state = progress.lock().expect("consent scan progress");
            state.complete = true;
            state.found = found.clone();
            state.cursor = Some(page.cursor);
            return Ok(found);
        }
        if cursor.as_ref().is_some_and(|old| old == &page.cursor) {
            return Err(refusal("consent_cursor_stalled", true));
        }
        cursor = Some(page.cursor);
        let mut state = progress.lock().expect("consent scan progress");
        state.cursor = cursor.clone();
        state.found = found.clone();
    }
}

/// An approving get/await readback is a candidate, not authorization. Start a
/// fresh settlement snapshot including that candidate, retaining this snapshot's
/// progress if its budget expires before every continuation has been checked.
pub async fn verify_readback(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    candidate: ConsentAnswer,
    progress: &Mutex<LookupProgress>,
) -> Result<ConsentAnswer, ApprovalRouteError> {
    {
        let mut state = progress.lock().expect("consent scan progress");
        let expected_binding = binding(prepared);
        if state.expected.as_ref() != Some(&candidate)
            || state.key.as_deref() != Some(key)
            || state.binding.as_ref() != Some(&expected_binding)
        {
            state.restart();
            state.key = Some(key.into());
            state.binding = Some(expected_binding);
            state.expected = Some(candidate.clone());
            state.found = Some(candidate.clone());
        }
    }
    lookup_with_progress(plane, ctx, key, prepared, progress).await?;
    Ok(candidate)
}

#[derive(Debug, Clone)]
pub struct RecoveredCard {
    pub id: String,
    pub answer: Option<ConsentAnswer>,
}

fn id_record(ctx: &BatchCtx, key: &str, prepared: &WalRecord, id: &str) -> WalRecord {
    let WalRecord::ToolCallDeferred { call_id, ask, .. } = prepared else {
        unreachable!()
    };
    WalRecord::AwaitingApproval {
        batch_id: ctx.batch_id.clone(),
        pending: vec![AwaitingApprovalEntry {
            call_key: key.into(),
            call_id: call_id.clone(),
            elicitation_id: Some(id.into()),
            expires_at_ms: ask.expires_at_ms,
        }],
    }
}

/// Recover an id-less card without authorizing execution. `durable` must write AND sync
/// each supplied record before returning. A newly filed id is durable before the second
/// lookup or duplicate withdrawal; an adopted older id is durable before returning.
pub async fn recover<F, Fut>(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    now_ms: u64,
    durable: F,
) -> Result<RecoveredCard, ApprovalRouteError>
where
    F: FnMut(WalRecord) -> Fut,
    Fut: Future<Output = Result<(), ApprovalRouteError>>,
{
    recover_with_progress(
        plane,
        ctx,
        key,
        prepared,
        now_ms,
        &Mutex::new(LookupProgress::default()),
        durable,
    )
    .await
}

pub async fn recover_with_progress<F, Fut>(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    now_ms: u64,
    progress: &Mutex<LookupProgress>,
    mut durable: F,
) -> Result<RecoveredCard, ApprovalRouteError>
where
    F: FnMut(WalRecord) -> Fut,
    Fut: Future<Output = Result<(), ApprovalRouteError>>,
{
    if let Some(answer) = lookup_with_progress(plane, ctx, key, prepared, progress).await? {
        durable(id_record(ctx, key, prepared, &answer.id)).await?;
        return Ok(RecoveredCard {
            id: answer.id.clone(),
            answer: Some(answer),
        });
    }
    progress.lock().expect("consent scan progress").restart();
    let filing = plane.request(ctx, key, prepared, now_ms).await?;
    if filing.elicitation_id.is_empty() {
        return Err(refusal("consent_record_unreadable", false));
    }
    durable(id_record(ctx, key, prepared, &filing.elicitation_id)).await?;
    reconcile_with_progress(
        plane,
        ctx,
        key,
        prepared,
        &filing.elicitation_id,
        progress,
        durable,
    )
    .await
}

/// A recorded id identifies a card but does not prove that the post-filing lookup
/// completed. Pending resumes and interrupted recovery retries reconcile it again.
pub async fn reconcile<F, Fut>(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    id: &str,
    durable: F,
) -> Result<RecoveredCard, ApprovalRouteError>
where
    F: FnMut(WalRecord) -> Fut,
    Fut: Future<Output = Result<(), ApprovalRouteError>>,
{
    reconcile_with_progress(
        plane,
        ctx,
        key,
        prepared,
        id,
        &Mutex::new(LookupProgress::default()),
        durable,
    )
    .await
}

pub async fn reconcile_with_progress<F, Fut>(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    id: &str,
    progress: &Mutex<LookupProgress>,
    mut durable: F,
) -> Result<RecoveredCard, ApprovalRouteError>
where
    F: FnMut(WalRecord) -> Fut,
    Fut: Future<Output = Result<(), ApprovalRouteError>>,
{
    if let Some(answer) = lookup_with_progress(plane, ctx, key, prepared, progress).await? {
        if answer.id != id {
            withdraw_duplicate(plane, ctx, key, prepared, id, answer.decision).await?;
        }
        durable(id_record(ctx, key, prepared, &answer.id)).await?;
        return Ok(RecoveredCard {
            id: answer.id.clone(),
            answer: Some(answer),
        });
    }
    Ok(RecoveredCard {
        id: id.into(),
        answer: None,
    })
}

async fn withdraw_duplicate(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    id: &str,
    older: ConsentDecision,
) -> Result<(), ApprovalRouteError> {
    let reason = match plane.withdraw(ctx, id).await {
        ApprovalWithdrawal::Acknowledged => return Ok(()),
        ApprovalWithdrawal::NotSent { reason } => reason,
        ApprovalWithdrawal::Retry(error) if !error.retryable && terminal_refusal(&error.code) => {
            error.code
        }
        ApprovalWithdrawal::Retry(error) => return Err(error),
    };
    // A refused withdrawal is not proof that the duplicate stayed unanswered.
    // Read its terminal record before adopting the older card's decision.
    let question = plane.get(ctx, id).await?;
    let duplicate = answer(&question.record, key, prepared)?
        .filter(|answer| answer.id == id && question.terminal)
        .ok_or_else(|| refusal("consent_withdrawal_unproven", true))?;
    if reason == "answered" && duplicate.decision == ConsentDecision::None {
        return Err(refusal("consent_withdrawal_unproven", true));
    }
    if duplicate.decision != ConsentDecision::None && duplicate.decision != older {
        return Err(refusal("consent_conflicting_decisions", false));
    }
    Ok(())
}

/// Reporting is best-effort in this process only. Every retry uses the identical value;
/// no durable debt is created, and cancellation drops any outstanding report route.
pub async fn report(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    id: &str,
    value: ApprovalExecution,
    cancel: Option<&tokio_util::sync::CancellationToken>,
) {
    for attempt in 0..3 {
        let result = tokio::select! {
            biased;
            _ = async { match cancel { Some(token) => token.cancelled().await, None => std::future::pending().await } } => return,
            result = tokio::time::timeout(Duration::from_secs(1), plane.report_execution(ctx, id, value)) => result,
        };
        if matches!(result, Ok(Ok(()))) || matches!(result, Ok(Err(ref error)) if !error.retryable)
        {
            return;
        }
        if attempt < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}

pub fn terminal_refusal(code: &str) -> bool {
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

/// Close a crash-interrupted call's outstanding card without executing its tool.
/// Integration handoff: `broca-module::approval::service_pass_cancellable` must call
/// `consent::close_out(plane, &ctx, &call_key, &prepared, recorded_id, now_ms, durable)`
/// instead of its id-less request/withdraw/acknowledgement sequence. Its `durable`
/// callback writes each returned WAL record and awaits writer.sync(); only then may
/// the service clear its obligation to withdraw that card. This replaces unconditional
/// refiling with settlement lookup and durable id recovery. Wrap the whole call in its cancellation
/// select. This helper never dispatches, even when a recovered answer approves.
pub async fn close_out<F, Fut>(
    plane: &dyn ElicitationPlane,
    ctx: &BatchCtx,
    key: &str,
    prepared: &WalRecord,
    recorded_id: Option<&str>,
    now_ms: u64,
    mut durable: F,
) -> Result<(), ApprovalRouteError>
where
    F: FnMut(WalRecord) -> Fut,
    Fut: Future<Output = Result<(), ApprovalRouteError>>,
{
    let recovered = match recorded_id {
        Some(id) => RecoveredCard {
            id: id.into(),
            answer: None,
        },
        None => match recover(plane, ctx, key, prepared, now_ms, &mut durable).await {
            Ok(card) => card,
            Err(error) if !error.retryable && terminal_refusal(&error.code) => {
                durable(WalRecord::QuestionWithdrawalAcknowledged {
                    call_key: key.into(),
                    elicitation_id: None,
                    answer: WithdrawalAnswer::NotSent,
                    reason: Some(error.code),
                })
                .await?;
                return Ok(());
            }
            Err(error) => return Err(error),
        },
    };
    let (outcome, reason, mut settled) = if recovered.answer.is_some() {
        (
            WithdrawalAnswer::NotSent,
            Some("answered".into()),
            recovered.answer,
        )
    } else {
        match plane.withdraw(ctx, &recovered.id).await {
            ApprovalWithdrawal::Acknowledged => (WithdrawalAnswer::Acknowledged, None, None),
            ApprovalWithdrawal::NotSent { reason } if terminal_refusal(&reason) => {
                (WithdrawalAnswer::NotSent, Some(reason), None)
            }
            ApprovalWithdrawal::Retry(error)
                if !error.retryable && terminal_refusal(&error.code) =>
            {
                (WithdrawalAnswer::NotSent, Some(error.code), None)
            }
            ApprovalWithdrawal::Retry(error) => return Err(error),
            ApprovalWithdrawal::NotSent { reason } => return Err(refusal(&reason, true)),
        }
    };
    if reason.as_deref() == Some("answered") && settled.is_none() {
        let question = plane.get(ctx, &recovered.id).await?;
        settled = answer(&question.record, key, prepared)?;
        if settled.is_none() {
            return Err(refusal("consent_record_unreadable", true));
        }
    }
    durable(WalRecord::QuestionWithdrawalAcknowledged {
        call_key: key.into(),
        elicitation_id: Some(recovered.id.clone()),
        answer: outcome,
        reason,
    })
    .await?;
    if settled.is_some_and(|answer| answer.decision == ConsentDecision::Approved) {
        report(
            plane,
            ctx,
            &recovered.id,
            ApprovalExecution::ApprovedNotExecuted,
            None,
        )
        .await;
    }
    Ok(())
}

#[cfg(test)]
mod tests;
