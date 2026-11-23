//! The state of one fork of a head session (llm-runner `fork_answer`), folded from the `Fork`
//! records of the fork's own session.
//!
//! A fork lives in a short-lived session of its own in the session store's ephemeral root. Its
//! first record says who prepared it and which head request it is based on; later records say
//! which asks began and how each attempt ended, when the fork ended or was revoked, and which
//! routing admissions and spend facts are settled. This fold is the one reading of those records:
//! serving a fork, recovering it after a restart and deciding when retention may delete its
//! session all read the state it builds.

use std::collections::{BTreeMap, BTreeSet};

use broca_session_store::record::{Fork, ForkEvent, Record};

/// One fork's state, as its session's records leave it.
#[derive(Debug, Clone, PartialEq)]
pub struct ForkState {
    pub fork_id: String,
    /// The principal that prepared the fork: the only one it answers.
    pub principal: String,
    /// The head request the fork is based on, as the fork's server recorded it.
    pub base: serde_json::Value,
    pub prepared_at_ms: u64,
    /// How long the fork lives after its last counted activity.
    pub idle_ms: u64,
    /// How long the fork's session is kept after the fork can last be used.
    pub retain_ms: u64,
    /// Every ask an attempt was started for, by ask id.
    pub asks: BTreeMap<String, Ask>,
    /// The attempt that began and hasn't ended. A fork has at most one.
    pub in_flight: Option<InFlight>,
    /// The fork's private conversation after the base: each answered question with its answer,
    /// in order. A failed model call's question never joins it.
    pub branch: Vec<Turn>,
    pub ended_at_ms: Option<u64>,
    pub revoked_at_ms: Option<u64>,
    /// Routing admissions an attempt took and nothing has released yet.
    pub unreleased: BTreeSet<String>,
    /// Finished attempts, by ask id and attempt number, whose spend isn't in the usage export yet.
    pub unbilled: BTreeSet<(String, u64)>,
    /// When the fork's idle clock last restarted: its prepare, or the end of an attempt, which
    /// begins only once the fork's base request was rebuilt to its recorded digest.
    expiry_basis_ms: u64,
    /// The newest time any of the fork's records carries.
    last_at_ms: u64,
}

/// An ask that has had at least one attempt.
#[derive(Debug, Clone, PartialEq)]
pub struct Ask {
    /// The question of the ask's latest attempt. An ask whose answer is stored keeps the question
    /// that answer answered.
    pub question: String,
    /// The effort hint of the ask's latest attempt, kept like its question: part of the ask's
    /// identity, so a stored answer replays only for the same hint or the same absence of one.
    pub effort: Option<String>,
    /// How many attempts have begun for the ask.
    pub attempts: u64,
    /// The stored answer, which every later ask with the same id and question replays.
    pub answer: Option<StoredAnswer>,
}

/// What an ask answered: replayed unchanged.
#[derive(Debug, Clone, PartialEq)]
pub struct StoredAnswer {
    pub answer: String,
    pub finish: String,
    /// llm-runner `RunUsage` JSON.
    pub usage: serde_json::Value,
    /// The lowered effort the answer was given at, when the ask's hint lowered it.
    pub effort_applied: Option<String>,
    pub at_ms: u64,
}

/// The attempt in flight.
#[derive(Debug, Clone, PartialEq)]
pub struct InFlight {
    pub ask_id: String,
    pub attempt: u64,
    pub question: String,
    pub admission_id: Option<String>,
    /// Usage recorded for the attempt so far.
    pub usage: Option<serde_json::Value>,
    pub started_at_ms: u64,
}

/// A question and the answer that joined the fork's conversation.
#[derive(Debug, Clone, PartialEq)]
pub struct Turn {
    pub question: String,
    pub answer: String,
}

/// The finish of an answer whose question and text stay out of the fork's conversation.
pub const FINISH_MODEL_ERROR: &str = "model_error";

impl ForkState {
    /// The fork whose session holds `records`, in seq order; `None` when they hold no `Fork`
    /// record. Records of other kinds are passed over. An error names records that contradict
    /// each other, which the fork's server never writes.
    pub fn fold<'a>(
        records: impl IntoIterator<Item = &'a Record>,
    ) -> Result<Option<ForkState>, String> {
        let mut state: Option<ForkState> = None;
        for record in records {
            let Record::Fork(fork) = record else {
                continue;
            };
            match &mut state {
                None => state = Some(Self::prepared(fork)?),
                Some(state) => state.apply(fork)?,
            }
        }
        Ok(state)
    }

    fn prepared(fork: &Fork) -> Result<ForkState, String> {
        if fork.event != ForkEvent::Prepared {
            return Err(format!(
                "fork {} starts with {:?}, not its prepare",
                fork.fork_id, fork.event
            ));
        }
        let missing = |field: &str| format!("fork {}'s prepare has no {field}", fork.fork_id);
        Ok(ForkState {
            fork_id: fork.fork_id.clone(),
            principal: fork.principal.clone().ok_or_else(|| missing("principal"))?,
            base: fork.base.clone().ok_or_else(|| missing("base"))?,
            prepared_at_ms: fork.at_ms,
            idle_ms: fork.idle_ms.ok_or_else(|| missing("idle_ms"))?,
            retain_ms: fork.retain_ms.ok_or_else(|| missing("retain_ms"))?,
            asks: BTreeMap::new(),
            in_flight: None,
            branch: Vec::new(),
            ended_at_ms: None,
            revoked_at_ms: None,
            unreleased: BTreeSet::new(),
            unbilled: BTreeSet::new(),
            expiry_basis_ms: fork.at_ms,
            last_at_ms: fork.at_ms,
        })
    }

    fn apply(&mut self, fork: &Fork) -> Result<(), String> {
        let id = self.fork_id.clone();
        if fork.fork_id != id {
            return Err(format!(
                "fork {id}'s session holds a record of fork {}",
                fork.fork_id
            ));
        }
        let event = fork.event;
        let missing = |field: &str| format!("fork {id}'s {event:?} has no {field}");
        self.last_at_ms = self.last_at_ms.max(fork.at_ms);
        match event {
            ForkEvent::Prepared => return Err(format!("fork {id} is prepared twice")),
            ForkEvent::AskStarted => {
                if let Some(open) = &self.in_flight {
                    return Err(format!(
                        "fork {id} began an attempt while ask {} attempt {} was in flight",
                        open.ask_id, open.attempt
                    ));
                }
                let ask_id = fork.ask_id.clone().ok_or_else(|| missing("ask_id"))?;
                let attempt = fork.attempt.ok_or_else(|| missing("attempt"))?;
                let question = fork.question.clone().ok_or_else(|| missing("question"))?;
                let ask = self.asks.entry(ask_id.clone()).or_insert_with(|| Ask {
                    question: question.clone(),
                    effort: None,
                    attempts: 0,
                    answer: None,
                });
                if ask.answer.is_some() {
                    return Err(format!(
                        "fork {id} began an attempt for ask {ask_id}, whose answer is stored"
                    ));
                }
                if attempt != ask.attempts + 1 {
                    return Err(format!(
                        "fork {id}'s ask {ask_id} began attempt {attempt} after {}",
                        ask.attempts
                    ));
                }
                ask.attempts = attempt;
                ask.question.clone_from(&question);
                ask.effort.clone_from(&fork.effort);
                if let Some(admission) = &fork.admission_id {
                    self.unreleased.insert(admission.clone());
                }
                self.in_flight = Some(InFlight {
                    ask_id,
                    attempt,
                    question,
                    admission_id: fork.admission_id.clone(),
                    usage: None,
                    started_at_ms: fork.at_ms,
                });
            }
            ForkEvent::AskUsage => {
                let open = self.open_attempt(fork)?;
                open.usage = Some(fork.usage.clone().ok_or_else(|| missing("usage"))?);
            }
            ForkEvent::AskFinished => {
                self.open_attempt(fork)?;
                let answer = fork.answer.clone().ok_or_else(|| missing("answer"))?;
                let finish = fork.finish.clone().ok_or_else(|| missing("finish"))?;
                let usage = fork.usage.clone().ok_or_else(|| missing("usage"))?;
                let open = self.in_flight.take().expect("checked to be in flight");
                if finish != FINISH_MODEL_ERROR {
                    self.branch.push(Turn {
                        question: open.question.clone(),
                        answer: answer.clone(),
                    });
                }
                self.unbilled.insert((open.ask_id.clone(), open.attempt));
                self.asks
                    .get_mut(&open.ask_id)
                    .expect("an attempt in flight has its ask")
                    .answer = Some(StoredAnswer {
                    answer,
                    finish,
                    usage,
                    effort_applied: fork.effort_applied.clone(),
                    at_ms: fork.at_ms,
                });
                self.expiry_basis_ms = self.expiry_basis_ms.max(fork.at_ms);
            }
            ForkEvent::AskRefused => {
                self.open_attempt(fork)?;
                self.in_flight = None;
                self.expiry_basis_ms = self.expiry_basis_ms.max(fork.at_ms);
            }
            ForkEvent::Ended => {
                self.ended_at_ms.get_or_insert(fork.at_ms);
            }
            ForkEvent::Revoked => {
                self.revoked_at_ms.get_or_insert(fork.at_ms);
            }
            ForkEvent::Released => {
                let admission = fork
                    .admission_id
                    .as_ref()
                    .ok_or_else(|| missing("admission_id"))?;
                self.unreleased.remove(admission);
            }
            ForkEvent::Billed => {
                let ask_id = fork.ask_id.clone().ok_or_else(|| missing("ask_id"))?;
                let attempt = fork.attempt.ok_or_else(|| missing("attempt"))?;
                self.unbilled.remove(&(ask_id, attempt));
            }
        }
        Ok(())
    }

    /// The attempt in flight, which `fork` must name.
    fn open_attempt(&mut self, fork: &Fork) -> Result<&mut InFlight, String> {
        let id = &self.fork_id;
        match &mut self.in_flight {
            Some(open)
                if fork.ask_id.as_deref() == Some(open.ask_id.as_str())
                    && fork.attempt == Some(open.attempt) =>
            {
                Ok(open)
            }
            _ => Err(format!(
                "fork {id}'s {:?} names ask {:?} attempt {:?}, which isn't in flight",
                fork.event, fork.ask_id, fork.attempt
            )),
        }
    }

    /// When the fork expires: its idle time after its prepare or the end of its last attempt.
    /// `None` while an attempt is in flight, which holds expiry off.
    pub fn expires_at_ms(&self) -> Option<u64> {
        if self.in_flight.is_some() {
            return None;
        }
        Some(self.expiry_basis_ms.saturating_add(self.idle_ms))
    }

    /// Whether the fork has expired at `now_ms`.
    pub fn expired(&self, now_ms: u64) -> bool {
        self.expires_at_ms().is_some_and(|at| now_ms >= at)
    }

    /// The newest time any of the fork's records carries.
    pub fn last_at_ms(&self) -> u64 {
        self.last_at_ms
    }

    /// When retention may delete the fork's session: its retention time past the latest moment
    /// the fork could still expire. The fork ends, is revoked or expires no later than its idle
    /// time after its newest record, so its stored answers stay replayable for at least the
    /// retention time after that.
    pub fn retained_until_ms(&self) -> u64 {
        self.last_at_ms
            .saturating_add(self.idle_ms)
            .saturating_add(self.retain_ms)
    }
}

#[cfg(test)]
#[path = "fork_state_tests.rs"]
mod tests;
