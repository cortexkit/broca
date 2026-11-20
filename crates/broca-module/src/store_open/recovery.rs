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
/// session whose latest run asked for warming and so needs its warming timer back at boot. The
/// boot sweep arms that timer without opening the session, only while its cache is live (see
/// [`LineageRegistry::idle_warm_due_at`]), and retires its registration once the cache is gone.
/// Installing a converted old session ([`retire_settled_install`]) happens with no module to ask
/// for the route's cache policy, so it keeps such a session registered and leaves the decision
/// to the next boot's sweep.
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
    owes_recovery(operations, billing, parts) || warms_itself(operations)
}

/// Whether the session's latest run froze a keep-warm mode, so its cache is warmed while idle.
fn warms_itself(operations: &broca_session_store::fold::operations::Operations) -> bool {
    operations.run.as_ref().is_some_and(|run| {
        run.start
            .as_ref()
            .and_then(|start| start.keep_warm.clone())
            .and_then(|keep_warm| {
                serde_json::from_value::<broca_protocol::request::KeepWarm>(keep_warm).ok()
            })
            .is_some_and(|keep_warm| keep_warm.warms())
    })
}

/// [`needs_recovery`] without the warming timer: what an open does for a session whatever its
/// cache.
fn owes_recovery(
    operations: &broca_session_store::fold::operations::Operations,
    billing: bool,
    parts: Option<&dyn broca_session_loop::parts::Parts>,
) -> bool {
    recovery_owed(operations, billing, parts).is_some()
}

/// What the session owes that keeps it a recovery candidate, named for the boot sweep's
/// counts, or `None` when it owes nothing ([`owes_recovery`] is `false`). The first thing owed
/// is named: queued sends opening would start or expire ([`opening_drains`]), then unwritten
/// billing, then the unfinished run by how it stands.
pub(crate) fn recovery_owed(
    operations: &broca_session_store::fold::operations::Operations,
    billing: bool,
    parts: Option<&dyn broca_session_loop::parts::Parts>,
) -> Option<&'static str> {
    use broca_protocol::PauseReason;
    use broca_session_store::fold::operations::Resolution;
    if operations
        .queue
        .iter()
        .any(|pending| opening_drains(pending, None))
    {
        return Some("queued_sends");
    }
    if billing && !operations.billing_owed.is_empty() {
        return Some("billing_owed");
    }
    let run = operations.run.as_ref().filter(|run| {
        run.finished.is_none() && !parts.is_some_and(|parts| waits_only_on_a_sign_in(run, parts))
    })?;
    Some(match run.resolution() {
        Resolution::Idle(_) => "run_unsealed",
        Resolution::Interrupted(_) => "run_interrupted",
        Resolution::ProviderHeld(_) => "run_held",
        Resolution::Paused(PauseReason::AuthRequired) => "run_paused_sign_in",
        Resolution::Paused(PauseReason::Restart) => "run_paused_restart",
        Resolution::Paused(PauseReason::AccountLimit) => "run_paused_account_limit",
        Resolution::Paused(_) => "run_paused_other",
        Resolution::Resume => "run_resume",
    })
}

/// Whether opening the session would do something for the queued send `pending`, at `now` when
/// given: start its run, or end it as expired. An ordered send that can't start a run waits for
/// a running run, so opening an idle session does nothing for it until it expires. Without
/// `now`, any expiry counts, since it comes due later.
pub(crate) fn opening_drains(
    pending: &broca_session_store::fold::operations::Pending,
    now: Option<u64>,
) -> bool {
    !pending.waits_for_run.unwrap_or(false)
        || pending
            .expires_at_ms
            .is_some_and(|at| now.is_none_or(|now| at <= now))
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
    /// The sessions opened, counted by what they owed ([`recovery_owed`]). A count that stays
    /// the same across restarts names something opening doesn't settle.
    pub opened_for: std::collections::BTreeMap<&'static str, usize>,
    /// Finished sessions that warm themselves and still have a cache to keep, whose warming
    /// timer was armed without opening them: the session opens when its next warm is due.
    pub warm_armed: usize,
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        })
}

impl crate::registry::LineageRegistry {
    /// When a finished head that warms itself is next due a warm, if it still has a cache to
    /// keep at `now_ms`: its last cached request is within the cache lifetime its route
    /// declares. The time is the one its actor arms (see `StoreActor::idle_warm_due`), so the
    /// boot sweep arms the head's warming timer without opening it, and the timer opens it when
    /// the warm is due; the actor checks the rest of the head's warming rules then. Past the
    /// cache lifetime, or when the run's config or its route's cache policy can't be read, the
    /// actor would arm no timer, so there is nothing to arm. Opening every such head at boot
    /// to arm its timer loaded about 90 heads at each restart, read most of the gigabytes a
    /// restart reads, and kept them in memory until each one idled out.
    fn idle_warm_due_at(
        &self,
        operations: &broca_session_store::fold::operations::Operations,
        parts: &dyn broca_session_loop::parts::Parts,
        executor: &dyn crate::executor::RunExecutor,
        now_ms: u64,
    ) -> Option<u64> {
        let run = operations
            .run
            .as_ref()
            .filter(|run| run.finished.is_some())?;
        let (start, send) = (run.start.as_ref()?, operations.last_cache_send.as_ref()?);
        let config = crate::store_actor::warm_run_config(run, start, parts).ok()?;
        let ttl_ms = self
            .cache_declaration(&config)
            .filter(|declaration| declaration.warm.is_ok())
            .and_then(|declaration| declaration.ttl_ms)
            .filter(|ttl| *ttl > 0)?;
        broca_core::cache_warm::check_warm_expiry(
            Some(broca_core::CacheClockReading {
                last_sent_at_ms: send.sent_at_ms,
                ttl_ms: Some(ttl_ms),
                prefix_tokens: None,
            }),
            now_ms,
        )
        .ok()?;
        Some(
            broca_core::keep_warm_wait::WarmTiming::for_ttl(
                ttl_ms,
                executor.warm_after_send_ms(&config),
            )
            .next_due_ms(send.sent_at_ms),
        )
    }

    /// Opens candidate store sessions under the ordinary lease in the background. The
    /// actor's open-time recovery seals crash orphans, parks held calls, and finishes pending
    /// durable tool-batch writes before it answers the status command. Finished sessions that
    /// warm themselves get their idle cache timers back without opening; each timer opens its
    /// session when the warm is due. The flag that closes a one-shot session immediately after
    /// its run is not durable, so a one-shot session whose cache policy asked for warming is
    /// re-armed by a restart too, still bounded by its warming's cap.
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
            // Every durable head is read; other sessions only when registered, so workers' idle
            // sessions don't turn each restart into an all-session read. A session that warms
            // itself stays registered for that (see `StoreActor::retire_recovery`).
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
                    let mut warm_due = None;
                    let owed = match Session::view(
                        runtime.backend(class),
                        addr.clone(),
                        &triple,
                        runtime.deployment(),
                    )
                    .await
                    {
                        Ok(view) => {
                            let parts = runtime.parts_for(class, cipher)
                                as &dyn broca_session_loop::parts::Parts;
                            let owed = recovery_owed(
                                &view.operations,
                                runtime.billing.is_some(),
                                Some(parts),
                            );
                            if owed.is_none() && warms_itself(&view.operations) {
                                warm_due = self.idle_warm_due_at(
                                    &view.operations,
                                    parts,
                                    runtime.executor.as_ref(),
                                    now_ms(),
                                );
                            }
                            owed
                        }
                        // The session lives on another machine now, which recovers it if anything
                        // is owed; this machine can't open it.
                        Err(SessionError::SessionMoved { .. }) => None,
                        Err(SessionError::NeedsRecovery) => Some("needs_recovery"),
                        Err(error) => return Err(format!("read operational state: {error:?}")),
                    };
                    if let Some(due_ms) = warm_due {
                        // It stays registered, so the next boot arms its timer again.
                        super::recovery_index::insert(&root, &addr)
                            .map_err(|error| format!("register recovery: {error}"))?;
                        self.handle().arm_idle_warm(&key, due_ms, now_ms());
                        sweep.warm_armed += 1;
                        return Ok(());
                    }
                    let Some(opened_for) = owed else {
                        if registered.contains(&addr.0) {
                            super::recovery_index::remove(&root, &addr)
                                .map_err(|error| format!("retire recovery: {error}"))?;
                            sweep.retired += 1;
                        }
                        return Ok(());
                    };
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
                        Ok(_) => {
                            sweep.recovered += 1;
                            *sweep.opened_for.entry(opened_for).or_default() += 1;
                        }
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
