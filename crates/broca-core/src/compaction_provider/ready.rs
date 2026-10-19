// Retained from cortexkit-role-compaction-provider 0.2.4 (MIT); legacy sync client only.
//! `compaction.ready`: a provider tells the runner that the work behind its
//! `wait` answer is ready for the runner to request again.
//!
//! The runner serves this op; the provider sends it. On receiving it for
//! the newest request it issued, the runner calls `compaction.step` again
//! with a fresh status instead of waiting out the bound. It is a hint: the
//! runner's bound is the backstop, so a provider that never sends it only
//! makes the step wait longer.
//!
//! The request type and the runner's check belong to `llm-runner/v1`, and
//! are re-exported here so a provider builds exactly what the runner
//! decodes. Before considering a ready hint, the runner compares the route's
//! caller stamp (the daemon-verified module that opened the route) with the
//! session's configured provider at `plan.compaction_item.provider`. It refuses
//! a different module, or a session with no compaction provider, with
//! `not_session_compaction_provider`: an unrelated module must not wake the
//! session's compaction wait. A matching provider wakes the wait only when the
//! hint names the newest issued request; older hints are ignored. CONTRACT.md
//! §10 defines these checks.

pub use cortexkit_role_llm_runner::compaction::plan_compaction_provider;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct CompactionReady {
    pub session: String,
    pub request_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ReadyOutcome {
    CallAgain,
    Ignored,
}

impl CompactionReady {
    pub fn new(session: impl Into<String>, request_id: impl Into<String>) -> Self {
        Self {
            session: session.into(),
            request_id: request_id.into(),
        }
    }

    pub fn check(
        &self,
        caller: &str,
        session_provider: Option<&str>,
        newest_request_id: Option<&str>,
    ) -> Result<ReadyOutcome, &'static str> {
        if session_provider != Some(caller) {
            return Err("not_session_compaction_provider");
        }
        Ok(if newest_request_id == Some(self.request_id.as_str()) {
            ReadyOutcome::CallAgain
        } else {
            ReadyOutcome::Ignored
        })
    }
}

/// The runner's reply to `compaction.ready`: the empty object `{}`, the
/// same whether the hint was acted on or ignored, so it reveals no outcome.
/// Unknown fields are ignored.
#[derive(Clone, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[non_exhaustive]
pub struct ReadyReply {}

impl ReadyReply {
    pub fn new() -> Self {
        Self::default()
    }
}
