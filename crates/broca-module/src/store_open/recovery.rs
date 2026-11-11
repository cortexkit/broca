//! Boot discovery for the new storage uses heads and operational folds, never the run index.

use super::*;

/// The rebuildable candidate sets live inside the class roots already covered by backup.
pub fn prepare_directories(state_root: &Path) -> std::io::Result<()> {
    for class in StorageClass::ALL {
        super::recovery_index::prepare(&class.store_root(state_root))?;
    }
    Ok(())
}

/// Register an administrative install before publishing its head. The caller must prevent
/// an actor from writing the session until installation finishes, so it cannot retire this
/// registration before the installed state is durable. A finished session is a harmless extra member.
pub fn register_candidate(root: &Path, addr: &SessionAddr) -> std::io::Result<()> {
    super::recovery_index::prepare(root)?;
    super::recovery_index::insert(root, addr)
}

/// Only missing coverage or durable-head warming may enumerate all stored heads. Keep the
/// inventory lazy so a complete worker-root index never pays for its idle session count.
pub(crate) fn candidate_addresses(
    indexed: Option<Vec<SessionAddr>>,
    class: StorageClass,
    inventory: impl FnOnce() -> Result<Vec<SessionAddr>, String>,
) -> Result<Vec<SessionAddr>, String> {
    let mut addresses = match indexed {
        Some(mut candidates) if class == StorageClass::Durable => {
            candidates.extend(inventory()?);
            candidates
        }
        Some(candidates) => candidates,
        None => inventory()?,
    };
    addresses.sort_by(|a, b| a.0.cmp(&b.0));
    addresses.dedup();
    Ok(addresses)
}

#[derive(Debug, Default)]
pub struct StoreRecoverySweep {
    pub inspected: usize,
    pub recovered: usize,
    pub lease_held: usize,
    pub failed: Vec<String>,
}

impl crate::registry::LineageRegistry {
    /// Opens candidate store sessions under the ordinary lease in the background. The
    /// actor's open-time recovery seals crash orphans, parks held calls, and finishes pending
    /// durable tool-batch writes before it answers the status command. Finished automatic heads also open
    /// once to rebuild their idle cache timers; their actors close on the ordinary idle deadline.
    /// The flag that closes a one-shot session immediately after its run is not durable. Core
    /// never gives those sessions the long-lived `head` class; if a caller did, a restart would
    /// re-arm that head too, still bounded by the durable auto warm cap.
    pub async fn recover_store_crash_orphans(&self) -> Result<StoreRecoverySweep, String> {
        let Some(runtime) = self.store_runtime() else {
            return Ok(StoreRecoverySweep::default());
        };
        let mut sweep = StoreRecoverySweep::default();
        for class in StorageClass::ALL {
            let backend = runtime.backend(class);
            let root = class.store_root(&runtime.state_root);
            let indexed = super::recovery_index::candidates(&root).ok().flatten();
            let rebuilding = indexed.is_none();
            let registered: std::collections::HashSet<_> = indexed
                .as_ref()
                .into_iter()
                .flatten()
                .map(|addr| addr.0.clone())
                .collect();
            // Automatic idle warming belongs to durable heads. Workers' idle sessions in
            // the other roots must not turn each restart into an all-session read.
            let addresses = candidate_addresses(indexed, class, || {
                backend
                    .stored_sessions()
                    .map_err(|error| format!("list {class} store sessions: {error:?}"))
            })?;
            let failures_before = sweep.failed.len();
            for addr in addresses {
                if self.stopping_signal().is_cancelled() {
                    return Ok(sweep);
                }
                tokio::task::yield_now().await;
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
                            if !registered.contains(&addr.0)
                                && view.operations.queue.is_empty()
                                && (runtime.billing.is_none()
                                    || view.operations.billing_owed.is_empty())
                                && !view.operations.run.as_ref().is_some_and(|run| {
                                    run.finished.is_none()
                                        || run.start.as_ref().is_some_and(|start| {
                                            start.session_class.as_deref() == Some("head")
                                                && start.keep_warm.as_ref()
                                                    == Some(&serde_json::json!("auto"))
                                        })
                                }) =>
                        {
                            return Ok(())
                        }
                        Ok(_) | Err(SessionError::NeedsRecovery) => {}
                        Err(error) => return Err(format!("read operational state: {error:?}")),
                    }
                    super::recovery_index::insert(&root, &addr)
                        .map_err(|error| format!("register recovery: {error}"))?;
                    let recovered = async {
                        let actor = self.actor_for_mode(&key, None, None, true).await?;
                        let (reply, response) = tokio::sync::oneshot::channel();
                        actor
                            .send(crate::actor::LineageCmd::Status { reply })
                            .await
                            .map_err(|_| OpenError::Open("recovery actor closed".into()))?;
                        response
                            .await
                            .map_err(|_| OpenError::Open("recovery barrier lost".into()))?;
                        Ok::<_, OpenError>(())
                    }
                    .await;
                    match recovered {
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
            // Encrypted heads cannot be classified until the key arrives. Leave the marker
            // absent on any failure so a later pass cannot mistake a partial scan for coverage.
            if rebuilding && sweep.failed.len() == failures_before {
                super::recovery_index::complete(&root)
                    .map_err(|error| format!("complete {class} recovery inventory: {error}"))?;
            }
        }
        Ok(sweep)
    }
}
