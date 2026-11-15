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

/// Whether a session's stored state leaves its open-time recovery anything to do: queued sends,
/// usage billing still owed (when `billing` runs), an unfinished run that opening acts on, or a
/// durable head that warms itself and so opens once at boot to rebuild its warming timer.
///
/// The one unfinished run opening leaves as it is: a run paused for sign-in on an account
/// routing didn't choose. Only a person signing in or a caller's own command moves it, so
/// opening it at boot changes nothing. Such pauses outlive the caller that started them (one-shot
/// workers whose account was signed out weeks ago), and keeping them counted re-opened hundreds
/// of sessions at every restart. The actor's open-time recovery makes the same decision (see
/// `StoreActor::recover`). Without the session's setup `parts` the run's account can't be read,
/// so every unfinished run counts.
pub(crate) fn needs_recovery(
    operations: &broca_session_store::fold::operations::Operations,
    billing: bool,
    parts: Option<&dyn broca_session_loop::parts::Parts>,
) -> bool {
    owes_recovery(operations, billing, parts)
        || operations.run.as_ref().is_some_and(|run| {
            run.start.as_ref().is_some_and(|start| {
                start.session_class.as_deref() == Some("head")
                    && start.keep_warm.as_ref() == Some(&serde_json::json!("auto"))
            })
        })
}

/// [`needs_recovery`] without a durable head's warming timer: what keeps a session registered
/// for recovery. The actor retires its registration when this is false; durable heads need no
/// registration to be found at boot, since every durable head is read there.
pub(crate) fn owes_recovery(
    operations: &broca_session_store::fold::operations::Operations,
    billing: bool,
    parts: Option<&dyn broca_session_loop::parts::Parts>,
) -> bool {
    !operations.queue.is_empty()
        || (billing && !operations.billing_owed.is_empty())
        || operations.run.as_ref().is_some_and(|run| {
            run.finished.is_none()
                && !parts.is_some_and(|parts| waits_only_on_a_sign_in(run, parts))
        })
}

/// Whether `run` is paused for sign-in with nothing opening could do for it: no cancel to
/// finish, and an account routing didn't choose, so there is no move to ask routing for
/// (see [`sign_in_move_routed`]).
fn waits_only_on_a_sign_in(
    run: &broca_session_store::fold::operations::Run,
    parts: &dyn broca_session_loop::parts::Parts,
) -> bool {
    use broca_session_store::fold::operations::Resolution;
    run.cancel.is_none()
        && matches!(
            run.resolution(),
            Resolution::Paused(broca_protocol::PauseReason::AuthRequired)
        )
        && !sign_in_move_routed(run, parts)
}

/// Whether `run`'s current account is one routing chose for its admission, the only kind a run
/// paused for sign-in can ask routing to move off when the session opens. A run whose account
/// can't be read is taken as not routed: opening could not ask for it either.
pub(crate) fn sign_in_move_routed(
    run: &broca_session_store::fold::operations::Run,
    parts: &dyn broca_session_loop::parts::Parts,
) -> bool {
    broca_session_loop::account_move::current_auth_selection(run, parts)
        .ok()
        .flatten()
        .and_then(|selection| selection.account_decision)
        .is_some_and(|decision| {
            decision.path == broca_wal::ACCOUNT_DECISION_ROUTED
                && decision.admission_id.is_some()
                && decision.decision_id.is_some()
        })
}

/// Retires the recovery registration of an installed session (see [`register_candidate`]) once
/// its published state leaves recovery nothing to do, so a boot doesn't open it to find that
/// out. Conversion installs every old session this way; registered and left, each converted
/// session was opened at the next boot (29,840 at one restart). The installer must still keep
/// actors off the session, so no run can have registered it since the install did. A session
/// whose state can't be read keeps its registration. Returns whether it was retired.
pub async fn retire_settled_install(
    root: &Path,
    addr: &SessionAddr,
    deployment: &DeploymentKey,
) -> Result<bool, String> {
    let backend = LocalBackend::new(root);
    let Some((head, _)) = backend
        .read_stored_head(addr)
        .map_err(|error| format!("read head: {error:?}"))?
    else {
        return Ok(false);
    };
    let triple = broca_session_store::head::stored_triple(&head)
        .map_err(|error| format!("decode head identity: {error:?}"))?;
    let view = match Session::view(backend, addr.clone(), &triple, deployment).await {
        Ok(view) => view,
        Err(_) => return Ok(false),
    };
    // Billing is taken to run: a session that still owes it stays registered either way.
    if needs_recovery(&view.operations, true, None) {
        return Ok(false);
    }
    super::recovery_index::remove(root, addr)
        .map_err(|error| format!("retire recovery: {error}"))?;
    Ok(true)
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
    /// Registered sessions found owing nothing, whose registration was retired without opening.
    pub retired: usize,
    pub lease_held: usize,
    pub deferred: usize,
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
            let deferred_before = sweep.deferred;
            for addr in addresses {
                if self.stopping_signal().is_cancelled() {
                    return Ok(sweep);
                }
                tokio::task::yield_now().await;
                let result = async {
                    let (head, _) = backend
                        .read_stored_head(&addr)
                        .map_err(|error| format!("read head: {error:?}"))?
                        .ok_or_else(|| "head disappeared".to_owned())?;
                    let cipher = broca_session_store::head::stored_cipher(&head)
                        .map_err(|error| format!("decode head mode: {error:?}"))?;
                    // An encrypted session's unfinished-run and queue state cannot be read
                    // before the key arrives. Keep its durable candidate for the key-loaded pass.
                    if cipher != Cipher::None && !runtime.key_loaded() {
                        sweep.deferred += 1;
                        return Ok(());
                    }
                    sweep.inspected += 1;
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
                    // The view reads every record that reached the store, the ones a crash left
                    // past the head included, so a session it finds with nothing owed has
                    // nothing for an open to do. A registration only says a session might owe
                    // something; when it owes nothing the registration is retired here, without
                    // opening the session.
                    match Session::view(
                        runtime.backend(class),
                        addr.clone(),
                        &triple,
                        runtime.deployment(),
                    )
                    .await
                    {
                        Ok(view)
                            if !needs_recovery(
                                &view.operations,
                                runtime.billing.is_some(),
                                Some(runtime.parts_for(class, cipher)
                                    as &dyn broca_session_loop::parts::Parts),
                            ) =>
                        {
                            if registered.contains(&addr.0) {
                                super::recovery_index::remove(&root, &addr)
                                    .map_err(|error| format!("retire recovery: {error}"))?;
                                sweep.retired += 1;
                            }
                            return Ok(());
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
                        let (reply, response) = tokio::sync::oneshot::channel();
                        actor
                            .send(crate::actor::LineageCmd::ResumeRestart { reply })
                            .await
                            .map_err(|_| OpenError::Open("restart recovery actor closed".into()))?;
                        response
                            .await
                            .map_err(|_| OpenError::Open("restart recovery reply lost".into()))?
                            .map_err(OpenError::Open)?;
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
            // absent on deferral or failure so a later pass cannot mistake a partial scan for coverage.
            if rebuilding
                && sweep.failed.len() == failures_before
                && sweep.deferred == deferred_before
            {
                super::recovery_index::complete(&root)
                    .map_err(|error| format!("complete {class} recovery inventory: {error}"))?;
            }
        }
        Ok(sweep)
    }
}
