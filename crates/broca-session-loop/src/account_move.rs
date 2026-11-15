//! Moving a run to another provider account inside a step, when the provider refuses the step's
//! request because the run's account has spent its usage window (a quota wall), or the
//! credential needs sign-in and has no unexpired token.
//!
//! The loop can't reach routing or the credential vault; the session's executor can. So the
//! executor answers one question through [`AccountContinue`]: given this wall, which account may
//! the run move to, with a provider already built for it. The loop decides whether to ask,
//! checks the answer, makes the move durable, and only then sends anything on the new account:
//!
//! 1. The refused attempt is already durable: a failed attempt's record is appended with a
//!    barrier as it ends.
//! 2. The executor is asked. Nothing it prepares is used yet.
//! 3. `AuthRefrozen` naming the new account is appended with a barrier, after `ConfigRefrozen`
//!    when the render config names the account too (the Claude Code persona does).
//! 4. Only then does the loop switch to the new provider and prepare the step's request again,
//!    as the next attempt of the same step.
//!
//! If the records of step 3 can't be written, the prepared provider is dropped with the error,
//! so nothing is left that would send on the new account.
//!
//! A run asks only where it would otherwise end: a wall that today's rule answers with a timed
//! pause on the same account keeps that pause, and a hold, a refusal, an answer that doesn't
//! arrive or an offer the loop can't accept all end the run as before, with routing's answer on
//! the error where there was one.
//!
//! Sign-in failures reuse the offer checks and durable move. Without an offer they retain
//! the auth pause; a typed in-memory outcome tells the actor when that pause may ask again.

use broca_core::account_limit_pause::FailedAccountLimitAttempt;
use broca_protocol::error::AccountContinueAnswer;
use broca_session_store::record::{AuthRefrozen, ConfigRefrozen, Record};
use serde_json::Value;

use crate::run::{Admission, LoopError, Seams};
use crate::writer::Writer;

/// The quota windows routing's continue request takes. A credit wall (the account has no money
/// left) and any other window are never continued.
const CONTINUED_WINDOWS: [&str; 2] = ["five_hour", "seven_day"];

/// One wall, as the refused attempt's own record and the run's current selection state it.
pub struct Wall<'a> {
    pub run_id: &'a str,
    pub step_id: u64,
    /// The selection the refused attempt was sent under.
    pub selection: &'a broca_wal::AuthSelection,
    /// The decision routing's continue request names as the run's predecessor (see
    /// [`predecessor`]).
    pub predecessor: &'a str,
    /// The credential and account that hit the wall.
    pub credential_id: &'a str,
    pub account: &'a str,
    /// `five_hour`, `seven_day`, or `signed_out` without usage timing evidence.
    pub limit_window: &'a str,
    pub resets_at_ms: Option<u64>,
    /// The provider's own retry-after on the refused attempt's error, in whole seconds rounded
    /// up, when it gave one.
    pub retry_after_s: Option<u64>,
    pub observed_at_ms: u64,
    /// The seq of the refused attempt's record: it names this wall, so an id derived from it is
    /// the same however often the question is sent.
    pub attempt_seq: u64,
    /// Every account that already refused this step, the one above included. An offer of any of
    /// them is not taken.
    pub refused_accounts: &'a [String],
    /// The render config the run renders under now, for a provider that must render the same
    /// request on the new account.
    pub renderer_version: u32,
    pub render_config: &'a Value,
    /// The run's frozen config as a resume would read it now (see
    /// [`crate::run::run_config_of_run`]): what the executor admits the new account's provider
    /// from.
    pub run_config: &'a broca_wal::RunConfig,
    /// The run is a head's turn (see `Admission::head_turn`).
    pub head_turn: bool,
    /// Set when routing is asked a second time at this wall because the account it offered
    /// first was no longer reserved when the run could take it. The question is the same, under
    /// a new selection id, and shares the first ask's budget.
    pub reask: bool,
}

/// What the executor answers.
pub enum Answer<P> {
    /// Routing offered an account, and the executor resolved its credential and built the
    /// provider that sends on it.
    Move(Box<Move<P>>),
    /// An exchange completed without a move. `Some` carries routing's refusal; `None`
    /// means a request was sent but no usable answer arrived within its budget.
    Stay(Option<AccountContinueAnswer>),
    /// Routing has no compatible account yet; an absent deadline waits for a credential re-check.
    Hold(AccountContinueAnswer),
    /// No authoritative continuation answer: no exchange, or an offer refused locally.
    Ignored,
}

/// Scheduling information for the actor owning a sign-in pause. Never persisted or inferred
/// from the public error detail, which does not distinguish an untimed hold from a refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignInOutcome {
    Hold {
        until_ms: Option<u64>,
        answered_at_ms: u64,
    },
    Refuse,
    Unreachable,
}

impl SignInOutcome {
    pub fn hold(until_ms: Option<u64>, now_ms: u64) -> Self {
        Self::Hold {
            until_ms,
            answered_at_ms: now_ms,
        }
    }
}

/// An account the run may move to, ready to use once the move is durable.
pub struct Move<P> {
    /// The selection the run renders and bills under from the move on, with the offer's
    /// decision id.
    pub selection: broca_wal::AuthSelection,
    /// The account routing offered, which the selection names.
    pub account: String,
    /// When routing's reservation of the account ends.
    pub valid_until_ms: u64,
    /// The render config the run renders with on the new account, when it differs from the
    /// current one: a Claude Code persona names its account in the request body.
    pub render: Option<(u32, Value)>,
    /// The provider that sends on the new account.
    pub provider: P,
}

/// The executor's side of a move at a wall (see the module docs).
#[async_trait::async_trait]
pub trait AccountContinue<P>: Send + Sync {
    /// Asks routing for another account at `wall`, within the same budget an admission has.
    async fn ask(&self, wall: &Wall<'_>) -> Answer<P>;
    /// The move to `selection` is durable and the run sends on it from now on. Called once per
    /// move, after the records and before any request on the new account.
    fn adopted(&self, selection: &broca_wal::AuthSelection);
    /// Whether this credential re-check may ask again for an already parked sign-in pause.
    fn recheck_sign_in(&self) -> bool {
        true
    }
    fn probe_only(&self) -> bool {
        false
    }
}

/// What the loop did at a wall it asked about.
pub(crate) enum AtWall<P> {
    /// The move is durable: the step is prepared again on `Move::provider`.
    Moved(Box<Move<P>>),
    /// No move; routing's answer, if it gave one, for the run's error.
    Stayed(Option<AccountContinueAnswer>),
    Held(AccountContinueAnswer),
    Ignored,
    /// The run was cancelled, or the runner asked to stop, while routing was being asked:
    /// nothing was recorded and the question was dropped. The run ends or pauses as it would
    /// at a step's boundary.
    Stopped,
}

/// How the loop enters a step again, after a move or after the model provider refused its
/// request as too long. No step boundary is written: the step started already, and messages a
/// caller steered into the run meanwhile wait for the next step's boundary. The step's request
/// is prepared again, on the new account or after an `over_limit` ride.
pub(crate) struct Reentry {
    pub step_id: u64,
    /// The accounts that refused this step so far.
    pub refused: Vec<String>,
    /// The retries the step's refused attempts used, which the step's count includes.
    pub retries: u32,
    /// The ordinal the next attempt is recorded with, after the refused one.
    pub first_attempt: u32,
    /// The run's admission before this step's first move. The run's last request the provider
    /// accepted was sent under it, so the compaction provider compares the new account with
    /// its account.
    pub accepted_under: Box<Admission>,
    /// Set when the provider refused the step's request as too long: the request is prepared
    /// again after an `over_limit` ride, and a second such refusal ends the run at the context
    /// ceiling.
    pub overflow: Option<broca_core::compaction_v2::request::Overflow>,
}

/// A move the loop made, for the driver to continue on.
pub(crate) struct Moving<P> {
    pub moved: Box<Move<P>>,
    pub reentry: Reentry,
}

/// Asks at a wall when the run may: the refused attempt is a quota wall on a window routing
/// continues, the run's selection came from routing (a fixed key or a single candidate has no
/// decision to continue), and the session's executor can ask. `None` when it may not, so the
/// run ends exactly as before. `refused` gains the walled account; an offer of any account in
/// it is refused here.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn at_wall<W: Writer, P, T>(
    writer: &mut W,
    parts: &dyn crate::parts::Parts,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    step_id: u64,
    limited: &FailedAccountLimitAttempt,
    retry_after_secs: Option<f64>,
    refused: &mut Vec<String>,
) -> Result<Option<AtWall<P>>, LoopError> {
    let Some(_) = seams.account_continue else {
        return Ok(None);
    };
    let Some(limit) = limited.attempt.account_observation.limit.as_ref() else {
        return Ok(None);
    };
    let Some(window) = limit
        .limit_window
        .as_deref()
        .filter(|window| CONTINUED_WINDOWS.contains(window))
    else {
        return Ok(None);
    };
    let Some(selection) = crate::run::run_selection(seams, admission) else {
        return Ok(None);
    };
    let routed = selection.account_decision.as_ref().is_some_and(|decision| {
        decision.path == broca_wal::ACCOUNT_DECISION_ROUTED
            && decision.decision_id.is_some()
            && decision.admission_id.is_some()
    });
    if !routed {
        return Ok(None);
    }
    let Some(predecessor) = predecessor(
        start_selection(writer).as_ref(),
        admission.route_decision_id.as_deref(),
        selection,
    ) else {
        return Ok(None);
    };
    // The account the refused attempt was served by, as its own record names it.
    let Some(account) = limited
        .attempt
        .served_account_ref
        .as_deref()
        .or(selection.account_ref.as_deref())
        .map(str::to_owned)
    else {
        return Ok(None);
    };
    if !refused.contains(&account) {
        refused.push(account.clone());
    }
    let run_config = crate::run::run_config_of_run(crate::run::run_state(writer), parts)?;
    let wall = Wall {
        run_id: &admission.run_id,
        step_id,
        selection,
        predecessor: &predecessor,
        credential_id: &selection.credential_id,
        account: &account,
        limit_window: window,
        resets_at_ms: limit.resets_at_ms,
        retry_after_s: retry_after_seconds(retry_after_secs),
        observed_at_ms: limit.observed_at_ms,
        attempt_seq: limited.seq,
        refused_accounts: refused,
        renderer_version: admission.renderer_version,
        render_config: &admission.render_config,
        run_config: &run_config,
        head_turn: admission.head_turn,
        reask: false,
    };
    // An offer is taken only while routing still reserves its account. One that lapsed before
    // the move could be written is dead: routing is asked once more, under a new selection id,
    // within what is left of the wall's budget, and a second unusable answer ends the run.
    let answer = prepare(seams, wall).await;
    if let AtWall::Moved(moved) = &answer {
        record_move(writer, seams, admission, moved).await?;
    }
    Ok(Some(answer))
}

async fn prepare<P, T>(seams: &Seams<'_, P, T>, mut wall: Wall<'_>) -> AtWall<P> {
    let continuer = seams
        .account_continue
        .expect("the caller checked continuation");
    let moved = loop {
        // A cancel or a stop does not wait for routing: an answer that arrives after either
        // would move a run that must end or pause.
        let answer = tokio::select! {
            answer = continuer.ask(&wall) => answer,
            () = seams.cancel.cancelled() => return AtWall::Stopped,
            () = seams.stop.cancelled() => return AtWall::Stopped,
        };
        let moved = match answer {
            Answer::Stay(answer) => return AtWall::Stayed(answer),
            Answer::Hold(answer) => return AtWall::Held(answer),
            Answer::Ignored => return AtWall::Ignored,
            Answer::Move(moved) => moved,
        };
        // The executor checks these too, before it reads the offered credential, and logs what
        // it refuses; they are checked again here because the loop is what must never send on
        // such an account: one that refused this step already (the walled one included), or a
        // selection that names another account than the offer.
        if wall.refused_accounts.contains(&moved.account)
            || moved.selection.account_ref.as_deref() != Some(moved.account.as_str())
        {
            return AtWall::Ignored;
        }
        let now = (seams.now_ms)();
        if now < moved.valid_until_ms {
            break moved;
        }
        if wall.reask {
            return AtWall::Ignored;
        }
        wall.reask = true;
    };
    AtWall::Moved(moved)
}

/// Prepare the same account move for a definite sign-in failure. A paused caller appends
/// the refreezes together with its resume, rather than lifting the pause in a separate batch.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn signed_out<W: Writer, P: broca_core::Provider, T>(
    writer: &W,
    parts: &dyn crate::parts::Parts,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    step_id: u64,
    error: &broca_protocol::ProviderError,
    refused: &mut Vec<String>,
) -> Result<Option<AtWall<P>>, LoopError> {
    let Some(selection) = signed_out_selection(seams, admission, error, step_id) else {
        return Ok(None);
    };
    let account = selection
        .account_ref
        .as_deref()
        .expect("eligible selection names an account");
    let Some(predecessor) = predecessor(
        start_selection(writer).as_ref(),
        admission.route_decision_id.as_deref(),
        selection,
    ) else {
        return Ok(None);
    };
    if !refused.iter().any(|id| id == account) {
        refused.push(account.to_owned());
    }
    let run_config = crate::run::run_config_of_run(crate::run::run_state(writer), parts)?;
    Ok(Some(
        prepare(
            seams,
            Wall {
                run_id: &admission.run_id,
                step_id,
                selection,
                predecessor: &predecessor,
                credential_id: &selection.credential_id,
                account,
                limit_window: "signed_out",
                resets_at_ms: None,
                retry_after_s: None,
                observed_at_ms: (seams.now_ms)(),
                attempt_seq: writer.next_seq(),
                refused_accounts: refused,
                renderer_version: admission.renderer_version,
                render_config: &admission.render_config,
                run_config: &run_config,
                head_turn: admission.head_turn,
                reask: false,
            },
        )
        .await,
    ))
}

pub(crate) fn signed_out_selection<'a, P: broca_core::Provider, T>(
    seams: &'a Seams<'_, P, T>,
    admission: &'a Admission,
    error: &broca_protocol::ProviderError,
    step_id: u64,
) -> Option<&'a broca_wal::AuthSelection> {
    if seams.account_continue.is_none() || error.class != broca_protocol::ErrorClass::Auth {
        return None;
    }
    // Keep the run's recorded routing decision and admission id. An unsuccessful vault
    // read may give the executor only a credential id, which cannot identify the run to routing.
    let selection = admission
        .auth_selection
        .as_ref()
        .or(seams.auth_selection.as_ref())
        .filter(|selection| {
            selection.account_decision.as_ref().is_some_and(|decision| {
                decision.path == broca_wal::ACCOUNT_DECISION_ROUTED
                    && decision.decision_id.is_some()
                    && decision.admission_id.is_some()
            })
        })
        .filter(|selection| selection.account_ref.is_some())?;
    ((error.provider_code.as_deref() == Some("needs_reauth") && error.status.is_none())
        || (matches!(error.status, Some(401 | 403))
            && seams
                .provider
                .definite_auth_failure(broca_protocol::StepId(step_id))))
    .then_some(selection)
}

pub(crate) async fn record_move<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    moved: &Move<P>,
) -> Result<(), LoopError> {
    let now = (seams.now_ms)();
    let ts_ms = Some(now);
    let mut records = Vec::with_capacity(2);
    if let Some((renderer_version, render_config)) = &moved.render {
        records.push(Record::ConfigRefrozen(ConfigRefrozen {
            run_id: admission.run_id.clone(),
            ts_ms,
            renderer_version: *renderer_version,
            render_config: render_config.clone(),
        }));
    }
    records.push(Record::AuthRefrozen(AuthRefrozen {
        run_id: admission.run_id.clone(),
        ts_ms,
        auth_selection: serde_json::to_value(&moved.selection)
            .map_err(|e| LoopError::Parts(e.to_string()))?,
    }));
    // Durable before anything is sent on the new account. On an error the move, and the
    // provider built for it, are dropped here.
    let first_seq = writer.append(&records, true).await?;
    // Routing hears of the move, after the refused attempt's report and before any attempt on
    // the new account: the account refreeze names it.
    if let Some(reports) = &seams.account_reports {
        let seq = first_seq + records.len() as u64 - 1;
        reports.account_moved(
            &format!("{}:{seq}", writer.session_id()),
            seq,
            &moved.selection,
        );
    }
    seams
        .account_continue
        .expect("prepared move")
        .adopted(&moved.selection);
    Ok(())
}

/// The decision routing knows a run by when broca asks it to continue the run elsewhere
/// (routing contract 0.7.34, the continue request's predecessor). While the run still sends
/// under the account decision its start froze, that is the caller's routing selection the run was
/// admitted under (`route_decision_id`), when the send carried one: routing chose the account
/// for that selection. A run admitted without one names its account decision. After a move, or
/// a resume routing gave another account, `selection` holds the decision of that offer, which
/// is the predecessor from then on. `None` when the selection has no decision id.
pub fn predecessor(
    start_selection: Option<&Value>,
    route_decision_id: Option<&str>,
    selection: &broca_wal::AuthSelection,
) -> Option<String> {
    let current = selection
        .account_decision
        .as_ref()?
        .decision_id
        .as_deref()?;
    let admitted = start_selection
        .and_then(|start| start.get("account_decision"))
        .and_then(|decision| decision.get("decision_id"))
        .and_then(Value::as_str);
    Some(match route_decision_id {
        Some(select) if admitted == Some(current) => select.to_owned(),
        _ => current.to_owned(),
    })
}

/// The account selection the run's start froze, before any move or resume refroze it.
fn start_selection<W: Writer>(writer: &W) -> Option<Value> {
    crate::run::run_state(writer)
        .start
        .as_ref()
        .and_then(|start| start.auth_selection.clone())
}

/// A provider's retry-after in whole seconds, rounded up, as routing's continue request takes
/// it. A value that isn't a finite, non-negative number of seconds is left out.
fn retry_after_seconds(secs: Option<f64>) -> Option<u64> {
    let secs = secs?.ceil();
    (secs.is_finite() && secs >= 0.0 && secs < u64::MAX as f64).then_some(secs as u64)
}

/// Whether run `run_id` moved to another account at a quota wall, read from its records.
///
/// Two writers record `AuthRefrozen` for a run: a move at a wall, and the resume of a paused
/// run that routing gave another account than the one it froze. A resume writes its refreezes
/// in one append together with its `Resumed` record, whose `resumed_from_seq` is the seq just
/// before that append, so a refreeze inside a resume's append lies after some `Resumed`'s
/// `resumed_from_seq` and before that `Resumed`. A move writes no `Resumed`. So an
/// `AuthRefrozen` of the run that no `Resumed` encloses this way is a move at a wall.
///
/// `records` must hold the run's records, as the open log of a session holds an unfinished
/// run's. A move that is not among them reads as no move: the run then keeps the account it
/// recorded rather than asking routing again.
pub fn moved_at_a_wall(run_id: &str, records: &[(u64, Record)]) -> bool {
    let resumes: Vec<(u64, u64)> = records
        .iter()
        .filter_map(|(seq, record)| match record {
            Record::Resumed(resumed) if resumed.run_id == run_id => {
                Some((resumed.resumed_from_seq, *seq))
            }
            _ => None,
        })
        .collect();
    records.iter().any(|(seq, record)| {
        matches!(record, Record::AuthRefrozen(refrozen) if refrozen.run_id == run_id)
            && !resumes
                .iter()
                .any(|(from, resumed)| from < seq && seq < resumed)
    })
}

/// Accounts this unfinished step moved away from, including a move that lifts a sign-in
/// pause. A resume does not erase the account that already proved unusable for the step.
pub fn refused_in_step(records: &[(u64, Record)], run_id: &str, step_id: u64) -> Vec<String> {
    let mut refused = Vec::new();
    let mut current_account: Option<String> = None;
    let mut current_step = 1;
    for (_, record) in records {
        match record {
            Record::RunStart(start) if start.run_id == run_id => {
                current_account = start
                    .auth_selection
                    .as_ref()
                    .and_then(|selection| selection.get("account_ref"))
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                current_step = 1;
            }
            Record::StepStart(start) if start.run_id == run_id => current_step = start.step_id,
            Record::StepEnd(end) if end.run_id == run_id => {
                current_step = end.step_id.saturating_add(1)
            }
            Record::Attempt(attempt) if attempt.run_id == run_id => {
                current_step = attempt.step_id;
                if let Some(account) = &attempt.served_account_ref {
                    current_account = Some(account.clone());
                }
            }
            Record::AuthRefrozen(refrozen) if refrozen.run_id == run_id => {
                let next = refrozen
                    .auth_selection
                    .get("account_ref")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                if current_step == step_id && current_account != next {
                    if let Some(account) = &current_account {
                        if !refused.contains(account) {
                            refused.push(account.clone());
                        }
                    }
                }
                current_account = next;
            }
            _ => {}
        }
    }
    refused
}

/// Whether a resume of `run` can durably record a change of account before it calls the
/// provider: the run is paused for a sign-in or for a restart, whose resume writes the account
/// and the config that names it together with its `Resumed` record. Every other resume (a run
/// waiting on a held tool call, a crash's resume, a timed pause on the account's limit, which
/// may only continue on its own account) records no such change, so it must continue on the
/// account it recorded.
pub fn resume_can_record_account_change(run: &broca_session_store::fold::operations::Run) -> bool {
    matches!(
        run.resolution(),
        broca_session_store::fold::operations::Resolution::Paused(
            broca_protocol::PauseReason::AuthRequired | broca_protocol::PauseReason::Restart
        )
    )
}

/// The account `run` sends on now: its newest `AuthRefrozen` (a move to another account at a
/// quota wall, or a resume on the account routing gave it), else the one its start froze. A
/// reader that checks something against the run's account, such as a steer's auth pin, reads it
/// here rather than from the start.
pub fn current_auth_selection(
    run: &broca_session_store::fold::operations::Run,
    parts: &dyn crate::parts::Parts,
) -> Result<Option<broca_wal::AuthSelection>, LoopError> {
    if let Some(refrozen) = &run.refrozen_auth {
        return serde_json::from_value(refrozen.clone())
            .map(Some)
            .map_err(|e| LoopError::Parts(format!("the run's account refreeze: {e}")));
    }
    let start = run.start.as_ref().ok_or(LoopError::Unsupported(
        "a run whose start isn't in the fold",
    ))?;
    Ok(crate::run::admission_of_start(start, parts)?.auth_selection)
}

/// Stores routing's answer in `account_continue` on the error that ends a run whose account
/// spent its usage window and which routing didn't move. A hold that names when an account
/// should have room (`until_ms`) also tells the caller when to try again: `retry_after_secs`
/// becomes the whole seconds from `now_ms`, when the run ends with the error, to that time,
/// rounded up and never below zero, unless the provider's own retry-after is already sooner.
/// `resets_at_ms` stays the spent account's own reset. A hold without a time, and a refusal,
/// leave `retry_after_secs` as it was.
pub fn with_routing_answer(
    error: &mut broca_protocol::ProviderError,
    answer: AccountContinueAnswer,
    now_ms: u64,
) {
    if let Some(until_ms) = answer.until_ms {
        let retry = until_ms.saturating_sub(now_ms).div_ceil(1000) as f64;
        if error.retry_after_secs.is_none_or(|own| retry < own) {
            error.retry_after_secs = Some(retry);
        }
    }
    error.account_continue = Some(Box::new(answer));
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    const NOW: u64 = 1_000_000;

    fn refrozen(run_id: &str) -> Record {
        Record::AuthRefrozen(AuthRefrozen {
            run_id: run_id.into(),
            ts_ms: None,
            auth_selection: json!({"credential_id": "c"}),
        })
    }

    fn resumed(run_id: &str, resumed_from_seq: u64) -> Record {
        Record::Resumed(broca_session_store::record::Resumed {
            run_id: run_id.into(),
            resumed_from_seq,
            messages: 0,
            model_steps: 0,
            tool_results: 0,
            redone: Vec::new(),
            indeterminate: Vec::new(),
        })
    }

    fn config_refrozen(run_id: &str) -> Record {
        Record::ConfigRefrozen(ConfigRefrozen {
            run_id: run_id.into(),
            ts_ms: None,
            renderer_version: 1,
            render_config: json!({}),
        })
    }

    /// The provider's retry-after goes to routing in whole seconds, rounded up; an unusable
    /// value is left out.
    #[test]
    fn a_retry_after_is_sent_in_whole_seconds_rounded_up() {
        assert_eq!(retry_after_seconds(None), None);
        assert_eq!(retry_after_seconds(Some(604_800.0)), Some(604_800));
        assert_eq!(retry_after_seconds(Some(1.2)), Some(2));
        assert_eq!(retry_after_seconds(Some(0.0)), Some(0));
        assert_eq!(retry_after_seconds(Some(-1.0)), None);
        assert_eq!(retry_after_seconds(Some(f64::NAN)), None);
        assert_eq!(retry_after_seconds(Some(f64::INFINITY)), None);
    }

    /// A refreeze a resume wrote with its `Resumed` is not a move; one written on its own, as
    /// a move at a wall writes it, is, also when a later resume of the moved run follows it.
    #[test]
    fn only_a_refreeze_outside_a_resume_is_a_move_at_a_wall() {
        // A sign-in pause's resume on another account: the refreezes ride the resume's append.
        let resumed_on_another = vec![
            (10, config_refrozen("run")),
            (11, refrozen("run")),
            (12, resumed("run", 9)),
        ];
        assert!(!moved_at_a_wall("run", &resumed_on_another));
        // A move at a wall, then a restart pause's resume with no refreeze, its batch starting
        // right after the move.
        let moved_then_resumed = vec![
            (20, config_refrozen("run")),
            (21, refrozen("run")),
            (22, resumed("run", 21)),
        ];
        assert!(moved_at_a_wall("run", &moved_then_resumed));
        // A move followed by a resume that refroze again: the run did move at a wall.
        let moved_then_refrozen = vec![
            (30, refrozen("run")),
            (40, refrozen("run")),
            (41, resumed("run", 39)),
        ];
        assert!(moved_at_a_wall("run", &moved_then_refrozen));
        // Another run's records say nothing about this one.
        assert!(!moved_at_a_wall("other", &moved_then_resumed));
        assert!(!moved_at_a_wall("run", &[]));
    }

    fn limit_error(retry_after_secs: Option<f64>) -> broca_protocol::ProviderError {
        let mut error = json!({
            "class": "transient",
            "message": "the account's usage window is spent",
            "status": 429,
            "provider_code": "account_limit_exhausted",
            "resets_at_ms": NOW + 3 * 86_400_000,
            "limit_window": "seven_day",
        });
        if let Some(secs) = retry_after_secs {
            error["retry_after_secs"] = json!(secs);
        }
        serde_json::from_value(error).unwrap()
    }

    fn hold(until_ms: Option<u64>) -> AccountContinueAnswer {
        AccountContinueAnswer {
            routing_code: "no_compatible_account".into(),
            until_ms,
        }
    }

    /// A hold sooner than anything the provider said sets the retry time to it, rounded up to
    /// whole seconds; the account's own reset is kept.
    #[test]
    fn a_hold_with_a_time_sets_the_retry_time() {
        let mut error = limit_error(None);
        with_routing_answer(&mut error, hold(Some(NOW + 90_001)), NOW);
        assert_eq!(error.retry_after_secs, Some(91.0));
        assert_eq!(error.resets_at_ms, Some(NOW + 3 * 86_400_000));
        assert_eq!(
            error.account_continue,
            Some(Box::new(hold(Some(NOW + 90_001))))
        );

        let mut sooner = limit_error(Some(600.0));
        with_routing_answer(&mut sooner, hold(Some(NOW + 60_000)), NOW);
        assert_eq!(sooner.retry_after_secs, Some(60.0));

        // A time already past retries at once.
        let mut past = limit_error(None);
        with_routing_answer(&mut past, hold(Some(NOW - 5_000)), NOW);
        assert_eq!(past.retry_after_secs, Some(0.0));
    }

    /// The provider's own retry-after wins when it is sooner than the hold's time.
    #[test]
    fn a_later_hold_keeps_the_providers_sooner_retry_time() {
        let mut error = limit_error(Some(30.0));
        with_routing_answer(&mut error, hold(Some(NOW + 3_600_000)), NOW);
        assert_eq!(error.retry_after_secs, Some(30.0));
        assert!(error.account_continue.is_some());
    }

    /// A hold without a time and a refusal carry routing's answer and leave the retry time as
    /// it was.
    #[test]
    fn a_hold_without_a_time_or_a_refusal_leaves_the_retry_time() {
        for own in [None, Some(30.0)] {
            let mut held = limit_error(own);
            with_routing_answer(&mut held, hold(None), NOW);
            assert_eq!(held.retry_after_secs, own);
            assert_eq!(held.account_continue, Some(Box::new(hold(None))));

            let refusal = AccountContinueAnswer {
                routing_code: "account_pinned".into(),
                until_ms: None,
            };
            let mut refused = limit_error(own);
            with_routing_answer(&mut refused, refusal.clone(), NOW);
            assert_eq!(refused.retry_after_secs, own);
            assert_eq!(refused.account_continue, Some(Box::new(refusal)));
        }
    }

    /// The selection a run's start froze, routed under account decision `decision`.
    fn routed(decision: Option<&str>) -> broca_wal::AuthSelection {
        serde_json::from_value(json!({
            "credential_id": "c",
            "account_decision": {"path": "routed", "admission_id": "a", "decision_id": decision},
        }))
        .unwrap()
    }

    #[test]
    fn a_continue_names_the_runs_selection_until_it_sends_under_another_decision() {
        let start = serde_json::to_value(routed(Some("acd-1"))).unwrap();
        // Still on its admitted account: the caller's selection, else its account decision.
        assert_eq!(
            predecessor(Some(&start), Some("dec-1"), &routed(Some("acd-1"))).as_deref(),
            Some("dec-1")
        );
        assert_eq!(
            predecessor(Some(&start), None, &routed(Some("acd-1"))).as_deref(),
            Some("acd-1")
        );
        // Moved, or resumed on another account: the offer's decision.
        assert_eq!(
            predecessor(Some(&start), Some("dec-1"), &routed(Some("acd-2"))).as_deref(),
            Some("acd-2")
        );
        // A selection without a decision gives routing nothing to continue.
        assert_eq!(
            predecessor(Some(&start), Some("dec-1"), &routed(None)),
            None
        );
    }
}
