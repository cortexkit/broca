//! The cold step: what happens to a session nobody has used for days.
//!
//! A session that comes back within hours should load with one read, so an idle session is left
//! alone. Only after days with no activity does the cold step run. It never seals the window,
//! because a session with an unfinished run must resume byte for byte.
//!
//! 1. **Confirm inactivity** from the open log itself: the newest time any record carries must be
//!    at least `idle_ms` old. A catalog only nominates sessions; the cold step decides under the
//!    session's own writer. A log with no timed record can't be confirmed, so nothing happens.
//! 2. **Seal every ledger record**, as a ledger seal does.
//! 3. **Prune, only if no run is unfinished.** If no run is active, paused or holding a tool turn
//!    open, the new open log gets the transcript records with finished runs' opaque reasoning
//!    replaced by stubs (see `prune`). The session's prompt cache expired days ago and nothing is
//!    in flight, so the next request renders those entries without it. A session with an
//!    unfinished run keeps every byte.
//! 4. **Commit** with one compare-and-swap of the head, which records when the session went
//!    cold. Opening a writer clears that again: a cold session wakes exactly like an idle one.

use crate::backend::SessionBackend;
use crate::fold::operations::Resolution;
use crate::record::ChangeKind;

use super::{Sealed, Session, SessionError};

/// What the cold step did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cold {
    /// The ledger was sealed. `pruned` says whether finished runs' opaque reasoning was pruned
    /// from the transcript kept open.
    Applied { sealed: Sealed, pruned: bool },
    /// The session was active more recently than `idle_ms` ago. Nothing was written.
    Active { last_active_ms: u64 },
    /// No record in the open log carries a time, so inactivity can't be confirmed. Nothing was
    /// written.
    Unknown,
}

impl<'b, B: SessionBackend> Session<'b, B> {
    /// Runs the cold step if the session has had no activity for `idle_ms`.
    pub async fn cold(&mut self, now_ms: u64, idle_ms: u64) -> Result<Cold, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        if self.operations.open_change.is_some() {
            return Err(SessionError::ChangeOpen);
        }
        let open = self.read_open_log().await?;
        let Some(last_active_ms) = open
            .0
            .iter()
            .filter_map(|(_, record)| record.activity_ms())
            .max()
        else {
            return Ok(Cold::Unknown);
        };
        if now_ms.saturating_sub(last_active_ms) < idle_ms {
            return Ok(Cold::Active { last_active_ms });
        }
        let run_idle = self
            .operations
            .run
            .as_ref()
            .is_none_or(|run| matches!(run.resolution(), Resolution::Idle(_)));
        let pruned = run_idle && self.conversation.open_tool_turn().is_none();
        let sealed = self
            .seal_read(ChangeKind::Cold, None, pruned, open, now_ms)
            .await?;
        Ok(Cold::Applied { sealed, pruned })
    }
}

#[cfg(test)]
mod tests;
