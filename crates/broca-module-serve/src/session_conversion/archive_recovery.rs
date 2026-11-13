use super::*;

impl Engine {
    /// Preserve the archive payload and seal only a private copy. The ready copy is outside the
    /// disposable converted tree: a failed verifier or process restart must reuse its exact tail.
    pub(super) fn recover_archive(
        &self,
        source: &Source,
        path: &Path,
        records: &mut Vec<FramedRecord>,
        lineage: Option<WalLineageId>,
        digest: &str,
    ) -> Result<Option<String>, Failure> {
        let raw: Vec<_> = records.iter().map(|frame| frame.record.clone()).collect();
        let prior = broca_core::replay(&raw).map_err(io)?;
        let Some(run) = prior.run_id.as_ref() else {
            return Ok(None);
        };
        if broca_core::reconcile_run_state(&raw, &run.0).is_some_and(|state| {
            state.state.is_terminal() || state.state == broca_core::RunIndexState::Paused
        }) {
            return Ok(None);
        }
        let address = source_address(source).ok_or_else(|| io("archive has no address"))?;
        let directory = self
            .root
            .join("conversion-recovery")
            .join(&address)
            .join(digest);
        let ready = directory.join("recovered.wal");
        if !ready.try_exists().map_err(io)? {
            self.ledger.bind_source(&source.id, digest).map_err(io)?;
            self.step(source, "recovering", None, None)?;
            broca_owner_only::create_dir_all(&directory).map_err(io)?;
            broca_wal::migration::durable_dir(&self.root, &directory).map_err(io)?;
            let temporary = directory.join("recovering.wal");
            let payload = archived_source(path, &address)?;
            use std::io::Write;
            let mut file = broca_owner_only::OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(&temporary)
                .map_err(io)?;
            file.write_all(&payload.payload).map_err(io)?;
            file.sync_all().map_err(io)?;
            drop(file);
            recover_live(&self.root, &temporary, records)?;
            std::fs::rename(&temporary, &ready).map_err(io)?;
            broca_wal::migration::sync_dir(&directory).map_err(io)?;
        }
        broca_wal::migration::sync_dir(&directory).map_err(io)?;
        let (recovered, recovered_lineage, _) = read_source(&ready, None)?;
        // Opening an old header assigns a lineage to the private copy. Conversion still uses
        // the source's original namespace; only an existing source lineage must remain equal.
        if (lineage.is_some() && recovered_lineage != lineage)
            || !recovered.starts_with(records)
            || !matches!(
                recovered.last().map(|frame| &frame.record),
                Some(WalRecord::RunFinished {
                    reason: broca_protocol::caller::RunFinishReason::Interrupted,
                    ..
                })
            )
        {
            return Err(Failure::failed(
                "recovery_changed",
                "the persisted archive recovery does not extend its bound prefix",
            ));
        }
        let run = run.0.clone();
        *records = recovered;
        Ok(Some(run))
    }
}

/// Use the actor's billing path before acknowledging the new terminal. A failed insert leaves
/// the run owed and the conversion unpublished, including for a run with no billable segments.
pub(super) async fn bill_recovered(
    staging: &Path,
    bind: &broca_protocol::BindIdentity,
    deployment: &DeploymentKey,
    runtime: Option<&broca_module::store_open::StoreRuntime>,
    converted: &mut broca_session_convert::Converted,
    run: &str,
) -> Result<(), Failure> {
    use broca_session_store::{
        backend::{LocalBackend, SessionAddr},
        session::Session,
    };
    let billing = runtime
        .and_then(|runtime| runtime.billing())
        .ok_or_else(|| {
            Failure::failed(
                "billing_failed",
                "archive recovery requires the configured usage export",
            )
        })?;
    let mut session = Session::load(
        LocalBackend::new(staging),
        SessionAddr(broca_core::session_addr(bind)),
        &broca_session_convert::stage::triple(bind),
        deployment,
    )
    .await
    .map_err(|error| io(format!("open recovered staging: {error:?}")))?;
    let parts = broca_session_loop::parts::MemoryParts {
        parts: converted.parts.clone(),
    };
    billing
        .insert(&session, bind, &parts, run)
        .await
        .map_err(|error| Failure::failed("billing_failed", error))?;
    let billed = broca_session_store::record::Record::Billed(broca_session_store::record::Billed {
        run_id: run.into(),
    });
    session
        .append(std::slice::from_ref(&billed), true)
        .await
        .map_err(|error| io(format!("acknowledge recovered billing: {error:?}")))?;
    session
        .close()
        .await
        .map_err(|error| io(format!("close recovered staging: {error:?}")))?;
    converted.records.push(billed);
    Ok(())
}
