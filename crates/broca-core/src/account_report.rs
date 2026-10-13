//! The run loop's hooks for reporting what a run saw about its provider account.
//!
//! The serve binary reports each run's admission, every provider attempt and the run's end to
//! the routing module, which chooses accounts. Those reports must name WAL records by their
//! lineage and sequence number, and only the loop knows a record's sequence number at the
//! moment it is written, so the loop calls these hooks and the reporter does the rest. The
//! hooks are synchronous and must not block: a reporter queues the event and returns. Nothing
//! here writes to the WAL, so a run with no reporter writes exactly the same bytes.

use broca_protocol::ModelAttempt;
use broca_wal::WalLineageId;

/// Receives a run's account events from the loop. See the module documentation.
pub trait AccountReporter: Send + Sync {
    /// A fresh run's `RunStarted` (at `wal_seq`) has been synced. Not called on a resume,
    /// which writes no `RunStarted`.
    fn run_started(&self, lineage: Option<WalLineageId>, wal_seq: u64);

    /// A provider attempt's record was appended at `wal_seq`. It is NOT yet durable: attempt
    /// records ride the next barrier, so a reporter that must only report durable records
    /// waits for the writer's durable head to reach `wal_seq`.
    fn attempt_recorded(&self, lineage: Option<WalLineageId>, wal_seq: u64, attempt: &ModelAttempt);
}
