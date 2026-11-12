//! Moving a run to another provider account inside a step, when the provider refuses the step's
//! request because the run's account has spent its usage window (a quota wall).
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
    /// The selection the refused attempt was sent under. Its decision is the one routing knows
    /// the run by now.
    pub selection: &'a broca_wal::AuthSelection,
    /// The credential and account that hit the wall.
    pub credential_id: &'a str,
    pub account: &'a str,
    /// `five_hour` or `seven_day`.
    pub limit_window: &'a str,
    pub resets_at_ms: Option<u64>,
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
    /// No move. `Some` carries routing's hold or refusal, which goes on the run's error; `None`
    /// when the executor didn't ask (routing doesn't declare the operation, or that couldn't be
    /// told) or had no usable answer (routing unreachable within the budget, or an offer the
    /// executor refused).
    Stay(Option<AccountContinueAnswer>),
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
}

/// What the loop did at a wall it asked about.
pub(crate) enum AtWall<P> {
    /// The move is durable: the step is prepared again on `Move::provider`.
    Moved(Box<Move<P>>),
    /// No move; routing's answer, if it gave one, for the run's error.
    Stayed(Option<AccountContinueAnswer>),
}

/// How the loop enters a step again after a move: no boundary is written (the step started
/// already, and messages steered since wait for the next boundary), and the step's request is
/// prepared again on the new account.
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
pub(crate) async fn at_wall<W: Writer, P, T>(
    writer: &mut W,
    parts: &dyn crate::parts::Parts,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    step_id: u64,
    limited: &FailedAccountLimitAttempt,
    refused: &mut Vec<String>,
) -> Result<Option<AtWall<P>>, LoopError> {
    let Some(continuer) = seams.account_continue else {
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
        credential_id: &selection.credential_id,
        account: &account,
        limit_window: window,
        resets_at_ms: limit.resets_at_ms,
        observed_at_ms: limit.observed_at_ms,
        attempt_seq: limited.seq,
        refused_accounts: refused,
        renderer_version: admission.renderer_version,
        render_config: &admission.render_config,
        run_config: &run_config,
        reask: false,
    };
    // An offer is taken only while routing still reserves its account. One that lapsed before
    // the move could be written is dead: routing is asked once more, under a new selection id,
    // within what is left of the wall's budget, and a second unusable answer ends the run.
    let mut wall = wall;
    let (moved, now) = loop {
        let moved = match continuer.ask(&wall).await {
            Answer::Stay(answer) => return Ok(Some(AtWall::Stayed(answer))),
            Answer::Move(moved) => moved,
        };
        // The executor checks these too, before it reads the offered credential, and logs what
        // it refuses; they are checked again here because the loop is what must never send on
        // such an account: one that refused this step already (the walled one included), or a
        // selection that names another account than the offer.
        if refused.contains(&moved.account)
            || moved.selection.account_ref.as_deref() != Some(moved.account.as_str())
        {
            return Ok(Some(AtWall::Stayed(None)));
        }
        let now = (seams.now_ms)();
        if now < moved.valid_until_ms {
            break (moved, now);
        }
        if wall.reask {
            return Ok(Some(AtWall::Stayed(None)));
        }
        wall.reask = true;
    };
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
    continuer.adopted(&moved.selection);
    Ok(Some(AtWall::Moved(moved)))
}
