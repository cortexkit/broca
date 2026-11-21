//! Spend facts for runs kept in the session store, written to the usage export.
//!
//! A run's end makes its billing owed: the operational fold lists it in `billing_owed`. The
//! store actor bills every owed run after a run ends and when it opens the session, before it
//! takes a command: it rebuilds the records read by billing
//! ([`broca_session_loop::billing`]) with the run's model attempts, hands them to the export
//! index's `emit_run_with_attempts`, which writes the run's segment facts and a fact per
//! attempt, and only once that insert has committed appends a `Billed` record, which settles
//! the run. The insert keeps a fact already stored under the same id unchanged, so billing
//! a run twice (after a crash between the insert and its `Billed`) writes nothing new.

use std::sync::Arc;

use broca_core::{ChargeBasisResolver, ExportIndex};
use broca_diagnostics::{spawn_blocking, JobKind};
use broca_protocol::{BindIdentity, RunId};
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::session::Session;

/// Where a session's spend facts are written: the deployment's usage export, and the
/// charge-basis resolver used to bill runs.
#[derive(Clone)]
pub struct StoreBilling {
    pub index: Arc<dyn ExportIndex>,
    pub resolver: Arc<dyn ChargeBasisResolver + Send + Sync>,
}

impl StoreBilling {
    /// Inserts the spend facts `records` hold for `run_id`: records the caller built, for a
    /// model call no run of a session's own records made (a fork attempt's, see
    /// `crate::fork_spend`). Returns once the insert has committed.
    pub async fn insert_records(
        &self,
        records: Vec<broca_wal::FramedRecord>,
        run_id: &str,
    ) -> Result<(), String> {
        let index = Arc::clone(&self.index);
        let resolver = Arc::clone(&self.resolver);
        let run_id = RunId(run_id.to_owned());
        spawn_blocking(JobKind::StoreWrite, move || {
            index.emit_run(&records, &run_id, resolver.as_ref())
        })
        .await
        .map_err(|error| format!("the usage export task failed: {error}"))?
        .map_err(|error| format!("the usage export refused the facts: {error}"))
    }

    /// Inserts the spend facts of `run_id`, read from `session`. Returns once the insert has
    /// committed; the caller then records the run billed.
    pub async fn insert<B: SessionBackend>(
        &self,
        session: &Session<B>,
        bind: &BindIdentity,
        parts: &dyn Parts,
        run_id: &str,
    ) -> Result<(), String> {
        let (records, attempts) =
            broca_session_loop::billing::run_billing_inputs(session, run_id, bind, parts).await?;
        let index = Arc::clone(&self.index);
        let resolver = Arc::clone(&self.resolver);
        let run_id = RunId(run_id.to_owned());
        spawn_blocking(JobKind::StoreWrite, move || {
            index.emit_run_with_attempts(&records, &run_id, &attempts, resolver.as_ref())
        })
        .await
        .map_err(|error| format!("the usage export task failed: {error}"))?
        .map_err(|error| format!("the usage export refused the facts: {error}"))
    }
}
