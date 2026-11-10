//! Boot discovery for the new storage uses heads and operational folds, never the run index.

use super::*;

#[derive(Debug, Default)]
pub struct StoreRecoverySweep {
    pub inspected: usize,
    pub recovered: usize,
    pub lease_held: usize,
    pub failed: Vec<String>,
}

impl crate::registry::LineageRegistry {
    /// Opens unfinished store sessions under the ordinary lease before serving begins. The
    /// actor's open-time recovery seals crash orphans, parks held calls, and completes owed
    /// batch barriers before it answers the status command. Finished sessions stay closed.
    pub async fn recover_store_crash_orphans(&self) -> Result<StoreRecoverySweep, String> {
        let Some(runtime) = self.store_runtime() else {
            return Ok(StoreRecoverySweep::default());
        };
        let mut sweep = StoreRecoverySweep::default();
        for class in StorageClass::ALL {
            let backend = runtime.backend(class);
            let addresses = backend
                .stored_sessions()
                .map_err(|error| format!("list {class} store sessions: {error:?}"))?;
            for addr in addresses {
                sweep.inspected += 1;
                let result = async {
                    let (head, _) = backend
                        .read_stored_head(&addr)
                        .map_err(|error| format!("read head: {error:?}"))?
                        .ok_or_else(|| "head disappeared".to_owned())?;
                    let triple = broca_session_store::head::stored_triple(&head)
                        .map_err(|error| format!("decode head identity: {error:?}"))?;
                    let key = BindIdentity::new(
                        PathBuf::from(&triple.project_root),
                        &triple.harness,
                        &triple.session,
                    );
                    if broca_core::session_addr(&key) != addr.0 {
                        return Err("head identity does not match its address".into());
                    }
                    match Session::view(
                        runtime.backend(class),
                        addr.clone(),
                        &triple,
                        runtime.deployment(),
                    )
                    .await
                    {
                        Ok(view)
                            if !view
                                .operations
                                .run
                                .as_ref()
                                .is_some_and(|run| run.finished.is_none()) =>
                        {
                            return Ok(())
                        }
                        Ok(_) | Err(SessionError::NeedsRecovery) => {}
                        Err(error) => return Err(format!("read operational state: {error:?}")),
                    }
                    match self.route_status(&key).await {
                        Ok(_) => sweep.recovered += 1,
                        Err(OpenError::LeaseHeld(_)) => sweep.lease_held += 1,
                        Err(error) => return Err(format!("open recovery: {error}")),
                    }
                    Ok(())
                }
                .await;
                if let Err(error) = result {
                    sweep.failed.push(format!("{}: {error}", addr.0));
                }
            }
        }
        Ok(sweep)
    }
}
