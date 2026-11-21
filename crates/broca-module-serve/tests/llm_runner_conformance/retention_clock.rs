//! The daemon clock the retention and fork cases move, and the check that an expired session is
//! gone from the session store.

use super::*;

/// How long a clock move waits for the retention pass to delete a session that came due.
const DELETION_WAIT: Duration = Duration::from_secs(60);

fn wall_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// How many lines across the daemon's log files under `logs` contain `text`.
fn log_count(logs: &Path, text: &str) -> usize {
    std::fs::read_dir(logs)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
        .map(|log| log.lines().filter(|line| line.contains(text)).count())
        .sum()
}

impl BrocaSubject {
    pub(super) async fn advance_retention(
        &self,
        session: &str,
        milliseconds: u64,
    ) -> Result<(), HarnessError> {
        self.advance_session_clock(session, milliseconds, true)
            .await
    }

    pub(super) async fn advance_fork_time(
        &self,
        session: &str,
        milliseconds: u64,
    ) -> Result<(), HarnessError> {
        self.advance_session_clock(session, milliseconds, false)
            .await
    }

    /// Move the daemon's clock `milliseconds` past where it reads now, and wait for a retention
    /// pass that ran after the move. When `retained` and the session's retention comes due by
    /// then, also wait until the pass has deleted it.
    async fn advance_session_clock(
        &self,
        session: &str,
        milliseconds: u64,
        retained: bool,
    ) -> Result<(), HarnessError> {
        let state = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| HarnessError::new("retention session has no daemon state root"))?;
        let logs = state
            .parent()
            .unwrap()
            .join("data-home/cortexkit/broca/logs");
        let before = log_count(&logs, "retention sweep complete");
        let control: Value = std::fs::read(state.join("test-clock"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or(json!({}));
        let now = control["now_ms"].as_u64().unwrap_or_else(|| {
            (wall_ms() as i64 + control["offset_ms"].as_i64().unwrap_or(0)) as u64
        });
        let target = now
            .checked_add(milliseconds)
            .ok_or_else(|| HarnessError::new("retention clock overflow"))?;
        let key = self.bind_identity(session);
        let due = retained
            && broca_module::retention::store::store_policy(
                &stored_log::stored_log_records(&state, &key).await,
            )
            .and_then(|(_, _, deadline)| deadline)
            .is_some_and(|deadline| deadline <= target);
        let mut pulse = control["offset_ms"].as_i64().unwrap_or(0);
        let deadline = Instant::now() + DELETION_WAIT;
        let mut next_pulse = Instant::now();
        loop {
            if Instant::now() >= next_pulse {
                pulse = if pulse == 0 { 1 } else { 0 };
                let temp = state.join("test-clock.tmp");
                std::fs::write(
                    &temp,
                    json!({"now_ms":target,"offset_ms":pulse}).to_string(),
                )
                .and_then(|()| std::fs::rename(&temp, state.join("test-clock")))
                .map_err(|error| HarnessError::new(error.to_string()))?;
                // Toggling the clock control's offset_ms wakes another deletion pass after
                // an expiring actor releases its lease. now_ms fixes the daemon's actual time
                // at the requested target, so these notifications do not advance it further.
                next_pulse = Instant::now() + Duration::from_secs(1);
            }
            if log_count(&logs, "retention sweep complete") > before
                && (!due || deletion_finished(&state, &key)?)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(HarnessError::new(
                    "retention clock did not complete due daemon deletion",
                ));
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}

impl BrocaSubject {
    /// Kills ck-broca at a retention deletion's recovery boundary: the session's expiry marker
    /// durable and the session still whole. Deletion writes the marker before it removes the
    /// session's head, so a head folder the daemon can't write holds the deletion there, with
    /// the content intact, without cutting a log or fabricating a marker.
    pub(super) async fn kill_retention(
        &self,
        mut handle: Live,
        point: &KillPoint,
        mut trigger: Trigger<'_>,
    ) -> Result<KillReport, HarnessError> {
        use std::os::unix::fs::PermissionsExt;
        let session = self
            .shared
            .last_session
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| HarnessError::new("retention kill has no written session"))?;
        let key = self.bind_identity(&session);
        let addr = broca_session_store::backend::SessionAddr(broca_core::session_addr(&key));
        let root = broca_wal::StorageClass::Ephemeral.store_root(&handle.state_root);
        let heads = root.join("heads").join(&addr.0);
        let head = heads.join("head");
        let triple = broca_session_store::head::SessionTriple {
            project_root: key.project_root.display().to_string(),
            harness: key.harness.clone(),
            session: key.session.clone(),
        };
        let set_mode = |mode| {
            std::fs::set_permissions(&heads, std::fs::Permissions::from_mode(mode))
                .map_err(|error| HarnessError::new(format!("{}: {error}", heads.display())))
        };
        set_mode(0o500)?;
        let reached = tokio::time::timeout(DELETION_WAIT, async {
            loop {
                let markers = broca_session_store::expired::read(
                    &root,
                    &addr,
                    &triple,
                    &broca_session_store::keys::DeploymentKey::unloaded(1),
                )
                .map_err(|error| HarnessError::new(format!("{error:?}")))?;
                if !markers.is_empty() {
                    return if head.is_file() {
                        Ok(())
                    } else {
                        Err(HarnessError::new(
                            "retention cut did not preserve the session",
                        ))
                    };
                }
                sleep(Duration::from_millis(10)).await;
            }
        });
        let result = tokio::select! {
            () = &mut trigger => Err(HarnessError::new("retention trigger finished before the expiry marker")),
            result = reached => result.unwrap_or_else(|_| Err(HarnessError::new("the retention expiry marker was not reached"))),
        };
        let killed = handle.kill_all();
        drop(handle);
        set_mode(0o700)?;
        result?;
        if !killed {
            return Err(HarnessError::new("no ck-broca process was found to kill"));
        }
        Ok(KillReport {
            point: point.clone(),
            mechanism: KillMechanism::ProcessSignal,
        })
    }
}

/// Whether `key`'s session is gone from the session store: no class holds its head.
pub(super) fn deletion_finished(
    state: &Path,
    key: &broca_protocol::BindIdentity,
) -> Result<bool, HarnessError> {
    broca_wal::session_storage_class(state, &broca_core::session_addr(key))
        .map(|class| class.is_none())
        .map_err(|error| HarnessError::new(error.to_string()))
}
