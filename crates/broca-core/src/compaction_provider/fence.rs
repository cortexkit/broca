// Retained from cortexkit-role-compaction-provider 0.2.4 (MIT); legacy sync client only.
//! The request fence and the version rule: whether the runner acts on an
//! answer.
//!
//! Before each call the runner durably records the id of the request it is
//! issuing, its newest. An answer is acted on only if it names that newest
//! request and arrives before that request's call deadline. A
//! CompactionMessage must also carry a version higher than the last applied
//! one, be well formed, and end no later than the newest message the
//! request's status reported. Anything else is recorded and never applied,
//! whatever its version, and the runner keeps serving the last applied
//! CompactionMessage. The provider's answer to the next request carries the
//! content instead.
//!
//! The fence covers every answer, not only CompactionMessages: a late
//! `refuse` must not end a run that has moved on, and a late `noop` must not
//! move the cursor.

use crate::compaction_provider::answer::{MessageProblem, StepAnswer};

/// What the runner knows when an answer arrives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FenceState {
    /// The id of the newest request the runner issued for this session.
    pub newest_request_id: String,
    /// The version of the last applied CompactionMessage; `None` before
    /// Setup's initial CompactionMessage is recorded.
    pub last_applied_version: Option<u64>,
    /// The newest ordinal the newest request's status reported; `None` when
    /// the lineage had no message yet.
    pub newest_ordinal: Option<u64>,
    /// The newest request's call deadline, in milliseconds since the Unix
    /// epoch on the runner's clock: the instant the runner times the call
    /// out and goes on with the last applied CompactionMessage. `None` when
    /// the runner set no deadline for the call.
    pub newest_deadline_ms: Option<u64>,
}

/// Why an answer is recorded but not acted on. The names are the values a
/// later status carries in `last_not_applied.reason`; a runner's own
/// structural checks add `structural` beside them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NotApplied {
    /// The answer names a request other than the newest issued.
    SupersededRequest,
    /// The answer names the newest request but arrived at or after that
    /// request's call deadline, when the runner had already timed the call
    /// out.
    Late,
    /// The CompactionMessage's version is not higher than the last applied.
    StaleVersion,
    /// The CompactionMessage is malformed on its own.
    Malformed(MessageProblem),
    /// The range ends after the newest message the request reported.
    RangeBeyondNewest,
}

impl NotApplied {
    /// The reason's name as the vectors and `last_not_applied.reason` spell
    /// it.
    pub fn name(self) -> &'static str {
        match self {
            Self::SupersededRequest => "superseded_request",
            Self::Late => "late",
            Self::StaleVersion => "stale_version",
            Self::Malformed(problem) => problem.name(),
            Self::RangeBeyondNewest => "range_beyond_newest",
        }
    }
}

/// The runner's decision for one answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Disposition {
    /// Act on it: apply the CompactionMessage, record the `noop`, enter the
    /// wait, or end the run for the `refuse`.
    Act,
    /// Record it, act on nothing, keep the last applied CompactionMessage.
    NotApplied(NotApplied),
}

impl Disposition {
    /// The decision's name as the vectors spell it: `act`, or the reason.
    pub fn name(self) -> &'static str {
        match self {
            Self::Act => "act",
            Self::NotApplied(reason) => reason.name(),
        }
    }
}

/// Decide whether the runner acts on `answer`, which arrived at
/// `arrived_at_ms` on the runner's clock. Checks run in a fixed order: the
/// request fence, then the call deadline, then the message on its own, then
/// the version, then the range against the newest message.
pub fn dispose(state: &FenceState, answer: &StepAnswer, arrived_at_ms: u64) -> Disposition {
    if answer.request_id() != state.newest_request_id {
        return Disposition::NotApplied(NotApplied::SupersededRequest);
    }
    if state
        .newest_deadline_ms
        .is_some_and(|deadline| arrived_at_ms >= deadline)
    {
        return Disposition::NotApplied(NotApplied::Late);
    }
    let StepAnswer::CompactionMessage { compaction, .. } = answer else {
        return Disposition::Act;
    };
    if let Err(problem) = compaction.check() {
        return Disposition::NotApplied(NotApplied::Malformed(problem));
    }
    if let Some(last) = state.last_applied_version {
        if compaction.version <= last {
            return Disposition::NotApplied(NotApplied::StaleVersion);
        }
    }
    // The first ordinal after the newest message; 0 when there is none, so
    // only an empty range at 0 fits an empty lineage.
    let end = state.newest_ordinal.map_or(0, |newest| newest + 1);
    if compaction.range.to > end {
        return Disposition::NotApplied(NotApplied::RangeBeyondNewest);
    }
    Disposition::Act
}
