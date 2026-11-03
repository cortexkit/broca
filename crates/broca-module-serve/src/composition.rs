//! The daemon's format-selected stores and feed of synced WAL publications.

use super::state_format::StateFormat;
use broca_core::{FileWalStore, SessionInventory, SessionRefIndex, SessionRefResolver};
use broca_protocol::BindIdentity;
use broca_store::db::{LogLocation, OverhaulDb};
use broca_wal::file_wal::{DurablePublication, PublicationSlot};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

pub(super) struct Stores {
    pub legacy: Option<Arc<broca_store::SqliteStore>>,
    pub overhaul: Option<Overhaul>,
}

pub(super) struct Overhaul {
    pub db: Arc<OverhaulDb>,
    pub publications: Arc<Publications>,
    stop: Arc<AtomicBool>,
    worker: Option<std::thread::JoinHandle<()>>,
}

pub(super) struct Publications {
    db: Arc<OverhaulDb>,
    root: PathBuf,
    slots: Mutex<HashMap<BindIdentity, Arc<PublicationSlot>>>,
    gates: Mutex<SessionGates>,
    blocked: Mutex<HashMap<String, String>>,
}

type SessionGates = HashMap<BindIdentity, std::sync::Weak<Mutex<()>>>;

/// Select the writer-lease ownership policy for the state format.
pub(super) fn opener_for_state_root(
    leases: Arc<broca_core::FileLeaseStore>,
    wal: Arc<FileWalStore>,
    executor: Arc<dyn broca_module::executor::RunExecutor>,
    format: StateFormat,
    root: &Path,
) -> Result<
    broca_module::DefaultOpener<broca_core::FileLeaseStore, FileWalStore>,
    broca_module_serve::ServeIoError,
> {
    use broca_module::DefaultOpener;
    match format {
        StateFormat::Legacy => Ok(DefaultOpener::new(leases, wal, executor)),
        StateFormat::Overhaul => {
            // Record the actual lease root before attaching its owned domain. The
            // writer, not the actor, must retain authority if an extra Arc survives.
            DefaultOpener::with_file_leases(root.join("leases"), wal, executor)
                .with_lease_domain(broca_core::lease::WalLeaseDomain::new(root.join("leases")))
                .map_err(|error| broca_module_serve::ServeIoError(error.to_string()))
        }
    }
}

pub(super) enum RestartDiscovery {
    Legacy(Arc<broca_store::SqliteStore>),
    Overhaul(Arc<dyn broca_core::RecoveryDiscovery>),
}
pub(super) enum RestartDiscoveryError {
    Legacy(broca_store::StoreError),
    Overhaul(String),
}
impl std::fmt::Debug for RestartDiscoveryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Legacy(error) => std::fmt::Debug::fmt(error, f),
            Self::Overhaul(error) => std::fmt::Debug::fmt(error, f),
        }
    }
}
impl RestartDiscovery {
    pub fn candidates(&self) -> Result<Vec<(String, BindIdentity)>, RestartDiscoveryError> {
        match self {
            Self::Legacy(index) => index
                .restart_resume_candidates()
                .map_err(RestartDiscoveryError::Legacy),
            Self::Overhaul(index) => index
                .restart_resume_candidates()
                .map_err(RestartDiscoveryError::Overhaul),
        }
    }
}

impl Stores {
    /// Install the format-selected writer and finish recovery before exposing stores
    /// to restart handling, inventory or service readers. The caller holds the root lock.
    pub async fn boot(
        format: StateFormat,
        root: &Path,
        mut wal: FileWalStore,
    ) -> Result<(Self, Arc<FileWalStore>), String> {
        let mut stores = match format {
            StateFormat::Legacy => Self::deferred_legacy(),
            StateFormat::Overhaul => Self::open_overhaul(root)?,
        };
        if let Some(overhaul) = &stores.overhaul {
            let publications = overhaul.publications.clone();
            wal = wal.with_session_guard(Arc::new(move |session| {
                publications.ensure_available(session)
            }));
            wal = wal.with_publication_observer(overhaul.publications.observer());
            wal.set_session_ref_index(overhaul.db.clone());
        }
        let wal = Arc::new(wal);
        broca_module::actor::writer_reclaim::reap_temporary_markers_at_boot(wal.clone()).await?;
        if let Some(overhaul) = &mut stores.overhaul {
            let recovery_wal = wal.clone();
            let publications = overhaul.publications.clone();
            broca_diagnostics::spawn_blocking(
                broca_diagnostics::JobKind::Recovery,
                broca_wal::access_counter::inherit(move || publications.reconcile(&recovery_wal)),
            )
            .await
            .map_err(|error| format!("join boot index reconciliation: {error}"))??;
            overhaul.start()?;
        }
        Ok((stores, wal))
    }

    pub fn open_boot_legacy(
        root: broca_wal::state_format::LegacyStateRoot<'_>,
        wal_config: broca_wal::WalConfig,
    ) -> Result<Self, String> {
        Ok(Self {
            legacy: Some(Arc::new(
                broca_store::SqliteStore::open_boot_legacy(root)
                    .map_err(|e| format!("open run index: {e}"))?
                    .with_wal_config(wal_config),
            )),
            overhaul: None,
        })
    }
    /// Legacy store construction stays at its original startup point, after the
    /// held-call runtime has opened. A failed earlier boot must not create its DB.
    pub fn deferred_legacy() -> Self {
        Self {
            legacy: None,
            overhaul: None,
        }
    }

    pub fn open_overhaul(root: &Path) -> Result<Self, String> {
        let db = Arc::new(OverhaulDb::open(root).map_err(|e| e.to_string())?);
        let publications = Arc::new(Publications {
            db: db.clone(),
            root: root.to_path_buf(),
            slots: Mutex::default(),
            gates: Mutex::default(),
            blocked: Mutex::default(),
        });
        Ok(Self {
            legacy: None,
            overhaul: Some(Overhaul {
                db,
                publications,
                stop: Arc::default(),
                worker: None,
            }),
        })
    }
}

impl Publications {
    pub fn catch_up_all(&self) -> Result<Vec<BindIdentity>, String> {
        let sessions: Vec<_> = self
            .slots
            .lock()
            .map_err(|e| e.to_string())?
            .iter()
            .filter(|(key, _)| {
                !self
                    .blocked
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .contains_key(&broca_core::session_addr(key))
            })
            .map(|(key, _)| key.clone())
            .collect();
        let mut failed = Vec::new();
        for session in sessions {
            if self.witness(&session, None).is_err() {
                failed.push(session);
            }
        }
        Ok(failed)
    }
    pub fn gate_for(&self, session: &BindIdentity) -> Arc<Mutex<()>> {
        let mut gates = self.gates.lock().unwrap_or_else(|e| e.into_inner());
        gates.retain(|_, gate| gate.strong_count() > 0);
        if let Some(gate) = gates.get(session).and_then(std::sync::Weak::upgrade) {
            return gate;
        }
        let gate = Arc::new(Mutex::new(()));
        gates.insert(session.clone(), Arc::downgrade(&gate));
        gate
    }

    pub fn gate_factory(self: &Arc<Self>) -> Arc<broca_store::db::SessionGateFactory> {
        let this = self.clone();
        Arc::new(move |session| this.gate_for(session))
    }

    pub fn ensure_available(&self, session: &BindIdentity) -> Result<(), String> {
        if let Some(error) = self
            .blocked
            .lock()
            .map_err(|e| e.to_string())?
            .get(&broca_core::session_addr(session))
        {
            return Err(error.clone());
        }
        if self
            .slots
            .lock()
            .map_err(|e| e.to_string())?
            .get(session)
            .is_some_and(|slot| slot.is_invalid())
        {
            return Err(format!(
                "session {} has an invalid publication identity",
                broca_core::session_addr(session)
            ));
        }
        Ok(())
    }
    pub fn observer(self: &Arc<Self>) -> Arc<broca_core::wal_writer::PublicationObserverFactory> {
        let this = self.clone();
        Arc::new(move |session, path, lineage| {
            // Registration happens during writer open, not on its sync path.
            let mut paths = vec![path.to_path_buf()];
            if let Some(name) = path.file_name() {
                for dir in ["wal", "wal-durable", "wal-ephemeral"] {
                    paths.push(this.root.join(dir).join(name));
                }
            }
            let slot = Arc::new(PublicationSlot::new(paths, lineage));
            // Open is under the existing writer exclusion. Retain an acknowledged
            // prefix until this descriptor publishes its first successful sync.
            let previous = this.slots.lock().unwrap().get(session).cloned();
            if let Some(previous) = previous.and_then(|slot| slot.snapshot()) {
                if std::fs::metadata(path)
                    .is_ok_and(|metadata| previous.byte_end() <= metadata.len())
                {
                    slot.seed_previous(&previous);
                }
            }
            this.slots
                .lock()
                .unwrap()
                .insert(session.clone(), slot.clone());
            Arc::new(move |publication| {
                slot.record_borrowed(publication);
                if slot.is_invalid() {
                    Err("publication identity mismatch")
                } else {
                    Ok(())
                }
            })
        })
    }

    fn remember(&self, session: BindIdentity, publication: &DurablePublication) {
        let slot = Arc::new(PublicationSlot::new(
            vec![publication.path().to_path_buf()],
            publication.lineage(),
        ));
        slot.record(publication);
        self.slots.lock().unwrap().insert(session, slot);
    }

    fn publish(
        &self,
        session: &BindIdentity,
        publication: &DurablePublication,
    ) -> Result<(), String> {
        self.publish_bound(session, publication, false)
    }

    fn publish_bound(
        &self,
        session: &BindIdentity,
        publication: &DurablePublication,
        historical: bool,
    ) -> Result<(), String> {
        let current = self.db.read_snapshot(|tx| {
            use rusqlite::OptionalExtension;
            Ok(tx.query_row("SELECT indexed_seq,indexed_end,lineage_id FROM session WHERE project_root=?1 AND harness=?2 AND session_key=?3",rusqlite::params![session.project_root.to_string_lossy(),session.harness,session.session],|r| Ok((r.get::<_,Option<u64>>(0)?.unwrap_or(0),r.get::<_,u64>(1)?,r.get::<_,Option<String>>(2)?))).optional()?)
        }).map_err(|e| e.to_string())?;
        if current
            == Some((
                publication.sequence(),
                publication.byte_end(),
                Some(publication.lineage().to_string()),
            ))
        {
            return Ok(());
        }
        if historical
            && current.as_ref().is_some_and(|(seq, end, lineage)| {
                *seq >= publication.sequence()
                    && *end >= publication.byte_end()
                    && lineage.as_deref() == Some(publication.lineage().to_string().as_str())
            })
        {
            return Ok(());
        }
        self.db
            .publish(
                session,
                &broca_core::agent_session_ref(session),
                LogLocation::Live,
                publication,
            )
            .map(|_| ())
            .map_err(|e| e.to_string())
    }

    /// A read takes the latest in-memory synced prefix, then catches the index up
    /// to it. A stalled background publisher cannot turn acknowledged writes stale.
    pub fn witness(
        &self,
        session: &BindIdentity,
        head: Option<u64>,
    ) -> Result<DurablePublication, String> {
        let gate = self.gate_for(session);
        let _guard = gate.lock().map_err(|e| e.to_string())?;
        self.witness_locked(session, head)
    }

    pub fn witness_locked(
        &self,
        session: &BindIdentity,
        head: Option<u64>,
    ) -> Result<DurablePublication, String> {
        self.ensure_available(session)?;
        let publication = self
            .slots
            .lock()
            .unwrap()
            .get(session)
            .and_then(|slot| slot.snapshot())
            .ok_or_else(|| format!("no descriptor-proven publication for session {}:{} at {}; boot reconciliation required",
                session.harness, session.session, session.project_root.display()))?;
        if let Some(head) = head.filter(|head| *head != publication.sequence()) {
            let offset = self.db.read_snapshot(|tx| {
                use rusqlite::OptionalExtension;
                let sid: Option<String> = tx.query_row("SELECT sid FROM session WHERE project_root=?1 AND harness=?2 AND session_key=?3", rusqlite::params![session.project_root.to_string_lossy(),session.harness,session.session], |r| r.get(0)).optional()?;
                Ok(tx.query_row("SELECT MAX(off) FROM (SELECT frame_off AS off FROM projection_boundary WHERE sid=?1 AND seq<=?2 UNION ALL SELECT run_started_off FROM episode WHERE sid=?1 AND run_started_seq<=?2)", rusqlite::params![sid,head], |r| r.get::<_, Option<u64>>(0))?.unwrap_or(0))
            }).map_err(|e| e.to_string())?;
            return publication
                .prefix_at(head, offset)
                .map_err(|e| e.to_string());
        }
        self.publish(session, &publication)?;
        Ok(publication)
    }
}

impl Overhaul {
    pub fn run_rows(
        &self,
        wal: Arc<FileWalStore>,
    ) -> Arc<broca_module::registry::OverhaulRunSource> {
        let all = self.publications.clone();
        let one = self.publications.clone();
        Arc::new(
            broca_module::registry::OverhaulRunSource::new(self.db.clone(), wal).with_consistency(
                Arc::new(move || {
                    let failed = all.catch_up_all()?;
                    if failed.is_empty() {
                        Ok(())
                    } else {
                        Err(format!(
                            "run-row publication unavailable for {} session(s)",
                            failed.len()
                        ))
                    }
                }),
                Arc::new(move |key| one.witness_locked(key, None)),
                self.publications.gate_factory(),
            ),
        )
    }

    pub fn recovery_discovery(&self) -> Arc<dyn broca_core::RecoveryDiscovery> {
        Arc::new(Recovery {
            db: self.db.clone(),
            publications: self.publications.clone(),
        })
    }

    pub fn event_store(&self) -> Arc<dyn broca_store::bus_events::RunEventStore> {
        let publications = self.publications.clone();
        let available = self.publications.clone();
        let failed_sessions = Arc::new(Mutex::new(Vec::new()));
        let failed_read = failed_sessions.clone();
        Arc::new(
            broca_store::db::OverhaulRunEventStore::new(self.db.clone())
                .with_before_read(Arc::new(move || {
                    let failed = publications.catch_up_all()?;
                    // Keep publication failures local to their sessions; committed
                    // outbox bodies remain independent of the live WAL projection.
                    *failed_sessions.lock().map_err(|e| e.to_string())? = failed;
                    Ok(())
                }))
                .with_session_filter(Arc::new(move |key| {
                    failed_read.lock().is_ok_and(|failed| !failed.contains(key))
                        && available.ensure_available(key).is_ok()
                })),
        )
    }
    #[cfg(test)]
    pub fn compaction_receipts(
        &self,
    ) -> Arc<dyn broca_store::compaction_receipts::CompactionReceiptStore> {
        let publications = self.publications.clone();
        self.db.compaction_receipt_store_serialized(
            move |session| publications.witness_locked(session, None),
            self.publications.gate_factory(),
        )
    }
    pub fn inventory(&self) -> Arc<dyn SessionInventory> {
        Arc::new(Inventory {
            db: self.db.clone(),
            publications: self.publications.clone(),
        })
    }

    pub fn admission_reader(&self) -> Arc<dyn broca_core::context::AdmissionReader> {
        let publications = self.publications.clone();
        Arc::new(Admission {
            inner: self
                .db
                .admission_reader(move |session| publications.witness_locked(session, None)),
            publications: self.publications.clone(),
        })
    }

    #[cfg(test)]
    fn reconcile(&self, wal: &FileWalStore) -> Result<(), String> {
        self.publications.reconcile(wal)
    }
}

impl Publications {
    /// Reconcile every live WAL before constructing any service or recovery reader.
    pub fn reconcile(&self, wal: &FileWalStore) -> Result<(), String> {
        self.blocked.lock().map_err(|e| e.to_string())?.clear();
        let reader = broca_core::ReadOnlyWalReader::new(&self.root);
        let mut addresses = reader.addresses().unwrap_or_else(|error| {
            broca_wal::access_counter::seed_fallback("boot archive inventory unavailable");
            tracing::warn!(target:"publication", "archive inventory unavailable at boot: {error}");
            Default::default()
        });
        for dir in ["wal", "wal-durable", "wal-ephemeral"] {
            if let Ok(entries) = std::fs::read_dir(self.root.join(dir)) {
                for entry in entries.flatten() {
                    if let Some(address) = entry
                        .file_name()
                        .to_str()
                        .and_then(|name| name.strip_suffix(".wal"))
                    {
                        addresses.insert(address.to_string());
                    }
                }
            }
        }
        for address in addresses {
            let result: Result<(), (&'static str, String)> = (|| {
                let path = ["wal", "wal-durable", "wal-ephemeral"]
                    .into_iter()
                    .map(|dir| self.root.join(dir).join(format!("{address}.wal")))
                    .find(|path| path.is_file())
                    .ok_or_else(|| {
                        (
                            "boot archived WAL unavailable",
                            "archive recovery is not enabled".to_string(),
                        )
                    })?;
                let _checked_header = broca_wal::file_wal::WalRecovery::open(
                    &path,
                    broca_wal::DEFAULT_MAX_READ_BYTES,
                )
                .map_err(|error| {
                    let reason = if matches!(error, broca_wal::WalError::TooLarge { .. }) {
                        "boot WAL over byte ceiling"
                    } else {
                        "boot lineage header unavailable"
                    };
                    (reason, error.to_string())
                })?;
                let mut session = self.db.read_snapshot(|tx| {
                use rusqlite::OptionalExtension;
                Ok(tx.query_row("SELECT project_root,harness,session_key FROM session_identity WHERE address=?1", [&address], |r| {
                    Ok(BindIdentity::new(r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?))
                }).optional()?)
            }).map_err(|e| ("boot session index unavailable",e.to_string()))?;
                if session.is_none() {
                    broca_wal::access_counter::seed_fallback("boot identity inventory missing");
                    let (records, _) = reader
                        .records(&address)
                        .map_err(|e| ("boot identity unavailable", e.to_string()))?;
                    session = records.iter().find_map(|frame| match &frame.record {
                        broca_wal::WalRecord::RunStarted { session, .. } => Some(session.clone()),
                        _ => None,
                    });
                }
                let session = session.ok_or_else(|| {
                    (
                        "boot identity unavailable",
                        format!("WAL {address} has no recoverable session identity"),
                    )
                })?;
                if broca_core::session_addr(&session) != address {
                    return Err((
                        "boot identity unavailable",
                        "WAL address and identity differ".into(),
                    ));
                }
                wal.check_move(&session, false)
                    .map_err(|e| ("boot session placement unavailable", e.to_string()))?;
                let result = self
                    .db
                    .catch_up_session(&broca_store::db::ReconcileSession {
                        bind: &session,
                        session_ref: &broca_core::agent_session_ref(&session),
                        location: LogLocation::Live,
                        path: &path,
                    })
                    .map_err(|e| ("boot WAL session unavailable", e.to_string()))?;
                self.remember(session, &result.publication);
                Ok(())
            })();
            if let Err((reason, error)) = result {
                broca_wal::access_counter::seed_fallback(reason);
                let message = format!("WAL {address} unavailable at boot ({reason}): {error}");
                self.blocked
                    .lock()
                    .map_err(|e| e.to_string())?
                    .insert(address, message.clone());
                tracing::warn!(target:"publication", "{message}; other sessions remain available");
            }
        }
        Ok(())
    }
}

impl Overhaul {
    pub fn start(&mut self) -> Result<(), String> {
        let publications = self.publications.clone();
        let stop = self.stop.clone();
        self.worker = Some(std::thread::Builder::new().name("broca-db-publisher".into()).spawn(broca_wal::access_counter::inherit(move || {
            let mut published = HashMap::new();
            loop {
                let sessions = {
                    let slots = publications.slots.lock().unwrap();
                    published.retain(|session, _| slots.contains_key(session));
                    slots.keys().cloned().collect::<Vec<_>>()
                };
                for session in sessions {
                    if publications.ensure_available(&session).is_err() { continue; }
                    let gate = publications.gate_for(&session);
                    let _guard = gate.lock().unwrap();
                    // Re-read under the operation gate; a service may already
                    // have published a newer proof while this worker was waiting.
                    let Some(current) = publications.slots.lock().unwrap().get(&session).cloned() else { continue; };
                    if let Some(witness) = current.snapshot() {
                        if published.get(&session) == Some(&witness) { continue; }
                        match publications.publish(&session, &witness) {
                            Ok(()) => { published.insert(session, witness); }
                            Err(error) => tracing::warn!(target: "publication", "database publication failed; proof retained for catch-up: {error}"),
                        }
                    }
                }
                if stop.load(Ordering::Acquire) { break; }
                std::thread::park_timeout(std::time::Duration::from_millis(10));
            }
        })).map_err(|e| e.to_string())?);
        Ok(())
    }
}

struct Inventory {
    db: Arc<OverhaulDb>,
    publications: Arc<Publications>,
}

struct Recovery {
    db: Arc<OverhaulDb>,
    publications: Arc<Publications>,
}
impl broca_core::RecoveryDiscovery for Recovery {
    fn active_runs(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        let failed = self.publications.catch_up_all()?;
        Ok(self
            .db
            .active_runs()
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|(_, key)| {
                !failed.contains(key) && self.publications.ensure_available(key).is_ok()
            })
            .collect())
    }
    fn restart_resume_candidates(&self) -> Result<Vec<(String, BindIdentity)>, String> {
        let failed = self.publications.catch_up_all()?;
        Ok(self
            .db
            .restart_resume_candidates()
            .map_err(|e| e.to_string())?
            .into_iter()
            .filter(|(_, key)| {
                !failed.contains(key) && self.publications.ensure_available(key).is_ok()
            })
            .collect())
    }
    fn withdrawal_candidates(&self) -> Result<Vec<broca_core::WithdrawalRun>, String> {
        let failed = self.publications.catch_up_all()?;
        Ok(
            broca_core::RecoveryDiscovery::withdrawal_candidates(self.db.as_ref())?
                .into_iter()
                .filter(|row| {
                    !failed.contains(&row.session)
                        && self.publications.ensure_available(&row.session).is_ok()
                })
                .collect(),
        )
    }
}

impl SessionRefIndex for Inventory {
    fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
        self.db.record_session(address, session)
    }
}
impl SessionRefResolver for Inventory {
    fn resolve_refs(&self, refs: &[&str]) -> Result<broca_core::SessionRefs, String> {
        self.db.resolve_refs(refs)
    }
    fn named_refs(&self, name: &str, limit: u32) -> Result<broca_core::SessionRefs, String> {
        self.db.named_refs(name, limit)
    }
}
impl SessionInventory for Inventory {
    fn prepare_inventory(&self, root: &Path) -> Result<(), String> {
        self.db.prepare_inventory(root)
    }
    fn rebuild_inventory(&self, root: &Path) -> Result<(), String> {
        self.db.rebuild_inventory(root)
    }
    fn retained_sessions(&self) -> Result<Vec<BindIdentity>, String> {
        self.db.retained_sessions().map_err(|e| e.to_string())
    }
    fn forget_projection(&self, session: &BindIdentity) -> Result<(), String> {
        // Move/delete callers have already stopped this session's writer. Remove
        // its proof under the publication gate so the worker cannot recreate rows.
        let gate = self.publications.gate_for(session);
        let _guard = gate.lock().map_err(|e| e.to_string())?;
        self.db.forget_projection(session)?;
        self.publications
            .slots
            .lock()
            .map_err(|e| e.to_string())?
            .remove(session);
        Ok(())
    }
}

struct Admission {
    inner: Arc<dyn broca_core::context::AdmissionReader>,
    publications: Arc<Publications>,
}

struct Resume {
    db: Arc<OverhaulDb>,
    publications: Arc<Publications>,
    session: BindIdentity,
    reader: Mutex<Option<Arc<dyn broca_core::resume::OffsetResumeReader>>>,
}

impl Publications {
    pub fn resume_reader(
        self: &Arc<Self>,
        session: &BindIdentity,
    ) -> Result<Arc<dyn broca_core::resume::OffsetResumeReader>, String> {
        self.ensure_available(session)?;
        Ok(Arc::new(Resume {
            db: self.db.clone(),
            publications: self.clone(),
            session: session.clone(),
            reader: Mutex::new(None),
        }))
    }
}

impl broca_core::resume::OffsetResumeReader for Resume {
    fn snapshot(
        &self,
        head: u64,
    ) -> Result<broca_core::resume::OffsetResumeSnapshot, broca_core::replay::SeedFallback> {
        let gate = self.publications.gate_for(&self.session);
        let _guard = gate.lock().map_err(|_| {
            broca_core::replay::SeedFallback::new("resume session gate unavailable")
        })?;
        let proof = self
            .publications
            .witness_locked(&self.session, Some(head))
            .map_err(|_| broca_core::replay::SeedFallback::new("resume publication unavailable"))?;
        self.publications
            .publish_bound(&self.session, &proof, true)
            .map_err(|_| {
                broca_core::replay::SeedFallback::new("resume publication catch-up unavailable")
            })?;
        let reader = self.db.resume_reader(self.session.clone(), proof);
        let result = reader.snapshot(head);
        *self
            .reader
            .lock()
            .map_err(|_| broca_core::replay::SeedFallback::new("resume reader unavailable"))? =
            Some(reader);
        result
    }
    fn read_at(&self, off: u64, len: u64) -> std::io::Result<Vec<u8>> {
        let gate = self.publications.gate_for(&self.session);
        let _guard = gate
            .lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?;
        self.reader
            .lock()
            .map_err(|e| std::io::Error::other(e.to_string()))?
            .as_ref()
            .ok_or_else(|| std::io::Error::other("resume snapshot must precede range reads"))?
            .read_at(off, len)
    }
    fn reconstructed(&self, fallback: Option<broca_core::replay::SeedFallback>) {
        if let Ok(reader) = self.reader.lock() {
            if let Some(reader) = reader.as_ref() {
                reader.reconstructed(fallback);
            }
        }
    }
}

impl broca_core::context::AdmissionReader for Admission {
    fn prior_snapshot(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<broca_core::run::PriorWalSnapshot, String> {
        let gate = self.publications.gate_for(session);
        let _guard = gate.lock().map_err(|e| e.to_string())?;
        self.inner.prior_snapshot(session, head)
    }
    fn sender_snapshot(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<broca_core::run::PriorWalSnapshot, String> {
        let gate = self.publications.gate_for(session);
        let _guard = gate.lock().map_err(|e| e.to_string())?;
        self.inner.sender_snapshot(session, head)
    }
    fn paused_episode(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<Option<broca_core::admission::LastEpisodeV1>, String> {
        let gate = self.publications.gate_for(session);
        let _guard = gate.lock().map_err(|e| e.to_string())?;
        self.inner.paused_episode(session, head)
    }
}

impl Drop for Overhaul {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.thread().unpark();
            let _ = worker.join();
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn an_invalid_slot_does_not_disable_other_sessions_recovery_or_events() {
        assert_discovery_failure_is_isolated(true);
    }

    #[test]
    fn a_failed_catch_up_does_not_disable_other_sessions_recovery_or_events() {
        assert_discovery_failure_is_isolated(false);
    }

    fn assert_discovery_failure_is_isolated(invalid_slot: bool) {
        let root = Root::new(if invalid_slot {
            "isolated-invalid-slot"
        } else {
            "isolated-catch-up-failure"
        });
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let events = overhaul.event_store();
        events.enable_bus_events("test").unwrap();
        events.begin_bus_event_period(100).unwrap();
        let bad = BindIdentity::new("/composition", "runner", "bad-slot");
        let good = BindIdentity::new("/composition", "runner", "healthy-slot");
        let mut proofs = Vec::new();
        for (key, id) in [(&bad, "bad-slot"), (&good, "healthy-slot")] {
            let mut writer = wal_store.open(key).unwrap();
            let mut start = started(key, id);
            if let WalRecord::RunStarted { ts_ms, .. } = &mut start {
                *ts_ms = Some(101);
            }
            writer.append(1, &start, true).unwrap();
            let proof = overhaul.publications.witness(key, None).unwrap();
            overhaul.db.index_run_withdrawals_owed(id, true).unwrap();
            events
                .project_new_bus_events(
                    key,
                    &proof.lineage().to_string(),
                    &writer.replay().unwrap(),
                    102,
                )
                .unwrap();
            proofs.push(proof);
        }
        if invalid_slot {
            overhaul
                .publications
                .slots
                .lock()
                .unwrap()
                .get(&bad)
                .unwrap()
                .record(&proofs[1]);
        } else {
            let mut writer = wal_store.open(&bad).unwrap();
            writer.append(1, &finished(), true).unwrap();
            drop(writer);
            std::fs::remove_file(proofs[0].path()).unwrap();
            assert!(overhaul.publications.ensure_available(&bad).is_ok());
            assert!(overhaul.publications.witness(&bad, None).is_err());
        }
        let recovery = overhaul.recovery_discovery();
        let active = recovery.active_runs();
        let restart = recovery.restart_resume_candidates();
        let debt = recovery.withdrawal_candidates();
        let census = events.bus_event_open_sessions();
        let checkpoint = events.bus_event_checkpoint(&good);
        let position = events.bus_event_position(&good, &proofs[1].lineage().to_string());
        let pending = events.pending_bus_event();
        for rows in [active, restart] {
            let rows = rows.expect("healthy recovery discovery must continue");
            assert!(rows.iter().any(|(_, key)| key == &good));
            assert!(rows.iter().all(|(_, key)| key != &bad));
        }
        let debt = debt.expect("healthy withdrawal discovery must continue");
        assert!(debt.iter().any(|row| row.session == good));
        assert!(debt.iter().all(|row| row.session != bad));
        assert_eq!(census.unwrap(), vec![good.clone()]);
        assert!(checkpoint.unwrap().is_some());
        assert_eq!(position.unwrap(), 1);
        // Already committed outbox bodies do not depend on a live publication.
        // Drain the earlier event as well, rather than losing it or starving the healthy one.
        let pending = pending.unwrap().unwrap();
        assert_eq!(
            pending.headers.get("run_id").map(String::as_str),
            Some("bad-slot")
        );
        events
            .acknowledge_bus_event(
                &pending,
                cortexkit_bus_trait::PublishReceipt::new(1, false),
                103,
            )
            .unwrap();
        assert_eq!(
            events
                .pending_bus_event()
                .unwrap()
                .unwrap()
                .headers
                .get("run_id")
                .map(String::as_str),
            Some("healthy-slot")
        );
        assert!(events.bus_event_checkpoint(&bad).is_err());
        assert!(events
            .bus_event_position(&bad, &proofs[0].lineage().to_string())
            .is_err());
    }

    #[test]
    fn an_acknowledged_unindexed_run_with_failed_publication_is_not_absent() {
        let root = Root::new("failed-advisory-refresh");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bad = BindIdentity::new("/composition", "runner", "unindexed-bad");
        let good = BindIdentity::new("/composition", "runner", "valid-proof");
        let mut bad_writer = wal_store.open(&bad).unwrap();
        bad_writer
            .append(1, &started(&bad, "acknowledged-bad"), true)
            .unwrap();
        let good_writer = wal_store.open(&good).unwrap();
        good_writer.sync().unwrap();
        overhaul
            .publications
            .slots
            .lock()
            .unwrap()
            .get(&bad)
            .unwrap()
            .record(&good_writer.durable_publication().unwrap());
        let opener = broca_module::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(root.0.join("leases"))),
            wal_store.clone(),
            Arc::new(RefusesExecution),
        );
        let registry = broca_module::LineageRegistry::new_with_run_rows(
            Arc::new(opener),
            Some(wal_store.clone()),
            None,
            Some(overhaul.run_rows(wal_store)),
        );
        assert!(
            registry
                .overhaul_lookup_run("acknowledged-bad")
                .unwrap()
                .is_err(),
            "failed publication is unknown/unavailable, never a successful absence"
        );
        assert!(registry
            .overhaul_restart_resume_candidates()
            .unwrap()
            .is_err());
        assert!(registry.overhaul_active_runs(&[]).unwrap().is_err());
    }

    #[test]
    fn legacy_restart_warning_preserves_typed_debug_bytes() {
        let root = Root::with_format("restart-warning", false);
        let stores = Stores::open_boot_legacy(
            broca_wal::state_format::decide_state_root(&root.0)
                .unwrap()
                .legacy()
                .unwrap(),
            broca_wal::WalConfig::LEGACY,
        )
        .unwrap();
        let store = stores.legacy.unwrap();
        rusqlite::Connection::open(root.0.join("run-index.db"))
            .unwrap()
            .execute_batch("DROP TABLE run_index")
            .unwrap();
        let direct = store.restart_resume_candidates();
        assert!(direct.is_err());
        let expected = format!(
            "restart resume: cannot list candidate runs: {:?}",
            Ok::<_, std::io::Error>(direct)
        );
        let source = RestartDiscovery::Legacy(store);
        let actual = format!(
            "restart resume: cannot list candidate runs: {:?}",
            Ok::<_, std::io::Error>(source.candidates())
        );
        assert_eq!(actual.as_bytes(), expected.as_bytes());
    }

    #[cfg(feature = "test-clock")]
    fn assert_resume_or_receipt_io_holds_session_gate(resume: bool) {
        use broca_wal::fs_ops::{self, FsOps};
        use std::sync::atomic::AtomicUsize;
        struct CheckIo {
            gate: Arc<Mutex<()>>,
            reads: Arc<AtomicUsize>,
        }
        impl FsOps for CheckIo {
            fn open(
                &self,
                p: &Path,
                o: &fs_ops::OpenOptions,
            ) -> std::io::Result<Box<dyn fs_ops::FileOps>> {
                if p.extension().is_some_and(|ext| ext == "wal") {
                    self.reads.fetch_add(1, Ordering::Relaxed);
                    assert!(
                        self.gate.try_lock().is_err(),
                        "WAL source read escaped its session publication gate"
                    );
                }
                fs_ops::RealFs.open(p, o)
            }
            fn metadata(&self, p: &Path) -> std::io::Result<std::fs::Metadata> {
                fs_ops::RealFs.metadata(p)
            }
            fn create_dir_all(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.create_dir_all(p)
            }
            fn rename(&self, a: &Path, b: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.rename(a, b)
            }
            fn remove_file(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.remove_file(p)
            }
            fn sync_dir(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.sync_dir(p)
            }
            fn create_dir(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.create_dir(p)
            }
            fn hard_link(&self, a: &Path, b: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.hard_link(a, b)
            }
            fn canonicalize(&self, p: &Path) -> std::io::Result<PathBuf> {
                fs_ops::RealFs.canonicalize(p)
            }
            fn read_dir(&self, p: &Path) -> std::io::Result<fs_ops::ReadDir> {
                fs_ops::RealFs.read_dir(p)
            }
            fn validate_if_exists(&self, p: &Path) -> std::io::Result<()> {
                fs_ops::RealFs.validate_if_exists(p)
            }
        }
        let root = Root::new(if resume {
            "resume-gate"
        } else {
            "receipt-gate"
        });
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "read-gate");
        let mut writer = wal_store.open(&bind).unwrap();
        writer.append(1, &started(&bind, "gate"), false).unwrap();
        writer.append(1, &finished(), true).unwrap();
        let proof = overhaul.publications.witness(&bind, None).unwrap();
        let gate = overhaul.publications.gate_for(&bind);
        let reads = Arc::new(AtomicUsize::new(0));
        let reader = overhaul.publications.resume_reader(&bind).unwrap();
        let receipts = overhaul.compaction_receipts();
        fs_ops::with_fs_ops(
            Arc::new(CheckIo {
                gate,
                reads: reads.clone(),
            }),
            || {
                if resume {
                    let _ = reader.snapshot(proof.sequence());
                    assert_eq!(reader.read_at(0, 69).unwrap().len(), 69);
                } else {
                    receipts
                        .current_compaction_facts(wal_store.as_ref(), &bind)
                        .unwrap();
                }
            },
        );
        assert!(
            reads.load(Ordering::Relaxed) > 0,
            "the test must reach a WAL source read"
        );
    }

    #[cfg(feature = "test-clock")]
    #[test]
    fn resume_snapshot_and_ranges_remain_inside_the_session_gate() {
        assert_resume_or_receipt_io_holds_session_gate(true);
    }
    #[cfg(feature = "test-clock")]
    #[test]
    fn compaction_receipt_source_remains_inside_the_session_gate() {
        assert_resume_or_receipt_io_holds_session_gate(false);
    }

    #[test]
    fn another_sessions_admission_does_not_wait_for_a_rebuild() {
        let root = Root::new("independent-session");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let a = BindIdentity::new("/composition", "runner", "slow");
        let b = BindIdentity::new("/composition", "runner", "healthy");
        let mut writer = wal_store.open(&b).unwrap();
        writer.append(1, &started(&b, "healthy"), false).unwrap();
        writer.append(1, &finished(), true).unwrap();
        let proof = overhaul.publications.witness(&b, None).unwrap();
        let gate = overhaul.publications.gate_for(&a);
        let guard = gate.lock().unwrap();
        assert!(
            overhaul.publications.gate_for(&b).try_lock().is_ok(),
            "unrelated sessions must have independent publication gates"
        );
        let (started_tx, started_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let db = overhaul.db.clone();
        let rebuilding = std::thread::spawn(move || {
            db.authoritative_transaction(|_| {
                started_tx.send(()).unwrap();
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap()
        });
        started_rx.recv().unwrap();
        let reader = overhaul.admission_reader();
        let (read_tx, read_rx) = std::sync::mpsc::channel();
        let reading = std::thread::spawn(move || {
            read_tx
                .send(reader.prior_snapshot(&b, proof.sequence()))
                .unwrap()
        });
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            broca_test_support::wait_until_sync(
                "healthy session admission while another session rebuilds",
                || read_rx.try_recv().map_err(|e| e.to_string()),
            )
        }));
        release_tx.send(()).unwrap();
        rebuilding.join().unwrap();
        drop(guard);
        reading.join().unwrap();
        let snapshot = result
            .expect("healthy admission must complete while the other session rebuilds")
            .expect("healthy admission must return a snapshot, not a fast read failure");
        assert_eq!(snapshot.head_seq, 2);
        assert_eq!(snapshot.base_watermark, 1);
        let base = snapshot.checkpoint_at_base.as_ref().unwrap();
        assert_eq!(base.at_seq, 1);
        assert_eq!(base.last_episode.as_ref().unwrap().run_id.0, "healthy");
        assert_eq!(snapshot.checkpoint_at_head.at_seq, 2);
        assert_eq!(
            snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .unwrap()
                .run_id
                .0,
            "healthy"
        );
        assert_eq!(snapshot.tail.len(), 1);
        assert_eq!(snapshot.tail[0].seq, 2);
        assert!(matches!(
            &snapshot.tail[0].record,
            WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Completed,
                ..
            }
        ));
    }

    #[test]
    fn bad_boot_sessions_do_not_refuse_healthy_sessions() {
        let root = Root::new("partial-boot");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let healthy = BindIdentity::new("/composition", "runner", "healthy");
        {
            let mut writer = wal_store.open(&healthy).unwrap();
            writer
                .append(1, &started(&healthy, "healthy"), false)
                .unwrap();
            writer.append(1, &finished(), true).unwrap();
        }
        overhaul.publications.witness(&healthy, None).unwrap();
        let broken = BindIdentity::new("/composition", "runner", "torn");
        let path = wal_store.wal_path(&broken);
        std::fs::write(&path, b"torn lineage").unwrap();
        let identityless = BindIdentity::new("/composition", "runner", "identityless");
        let file = broca_wal::FileWal::open(wal_store.wal_path(&identityless)).unwrap();
        file.sync().unwrap();
        drop(file);
        let huge = BindIdentity::new("/composition", "runner", "oversized");
        std::fs::File::create(wal_store.wal_path(&huge))
            .unwrap()
            .set_len(broca_wal::DEFAULT_MAX_READ_BYTES + 1)
            .unwrap();
        let archived = BindIdentity::new("/composition", "runner", "archived");
        {
            let writer = wal_store.open(&archived).unwrap();
            writer.sync().unwrap();
        }
        let archived_path = wal_store.resolve_live_path(&archived).unwrap().unwrap();
        let archive_dir = root.0.join("wal-archive");
        std::fs::create_dir_all(&archive_dir).unwrap();
        let archive_path = archive_dir.join("fold-1.ark");
        let mut archive = broca_wal::archive::ArchiveWriter::create(&archive_path)
            .unwrap()
            .without_feature_floor();
        archive
            .append(
                &broca_core::session_addr(&archived),
                &std::fs::read(&archived_path).unwrap(),
            )
            .unwrap();
        archive.seal().unwrap();
        std::fs::remove_file(archived_path).unwrap();
        let archive_before = std::fs::read(&archive_path).unwrap();
        let before = std::fs::read(&path).unwrap();
        let counter = broca_wal::access_counter::WalAccessCounter::default();
        assert!(counter
            .operation("partial boot")
            .run(|| overhaul.reconcile(&wal_store))
            .is_ok());
        assert!(overhaul.publications.witness(&healthy, None).is_ok());
        for session in [&broken, &identityless, &huge, &archived] {
            assert!(overhaul.publications.witness(session, None).is_err());
        }
        assert_eq!(std::fs::read(path).unwrap(), before);
        assert_eq!(std::fs::read(archive_path).unwrap(), archive_before);
        assert!(wal_store.open(&broken).is_err());
        for reason in [
            "boot lineage header unavailable",
            "boot identity unavailable",
            "boot WAL over byte ceiling",
            "boot archived WAL unavailable",
        ] {
            assert!(counter.snapshot()[0].seed_fallbacks.contains(&reason));
        }
    }

    #[test]
    fn acknowledged_run_discovery_catches_up_before_the_publisher_drains() {
        let root = Root::new("fresh-discovery");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "discovery");
        let mut writer = wal_store.open(&bind).unwrap();
        writer.append(1, &started(&bind, "fresh"), true).unwrap();
        let discovery = overhaul.recovery_discovery();
        assert!(
            discovery
                .active_runs()
                .unwrap()
                .iter()
                .any(|(run, _)| run == "fresh"),
            "acknowledged RunStarted must be discoverable before a publisher tick"
        );
        assert!(discovery
            .restart_resume_candidates()
            .unwrap()
            .iter()
            .any(|(run, _)| run == "fresh"));
    }

    #[test]
    fn reopening_a_writer_keeps_the_previously_acknowledged_discovery_head() {
        let root = Root::new("reopen-discovery-head");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "reopened");
        {
            let mut writer = wal_store.open(&bind).unwrap();
            writer
                .append(1, &started(&bind, "ack-before-reopen"), true)
                .unwrap();
        }
        assert!(overhaul
            .db
            .lookup_run("ack-before-reopen")
            .unwrap()
            .is_none());
        let _reopened = wal_store.open(&bind).unwrap();
        assert!(
            overhaul
                .recovery_discovery()
                .active_runs()
                .unwrap()
                .iter()
                .any(|(id, _)| id == "ack-before-reopen"),
            "opening a descriptor must not erase the previous durable head before its first sync"
        );
    }

    #[test]
    fn run_lookup_and_rows_use_the_acknowledged_prefix_before_a_publisher_tick() {
        let root = Root::new("fresh-run-row");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "run-row");
        let mut writer = wal_store.open(&bind).unwrap();
        let opener = broca_module::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(root.0.join("leases"))),
            wal_store.clone(),
            Arc::new(RefusesExecution),
        );
        let source = overhaul.run_rows(wal_store.clone());
        let registry = broca_module::LineageRegistry::new_with_run_rows(
            Arc::new(opener),
            Some(wal_store),
            None,
            Some(source),
        );
        writer
            .append(1, &started(&bind, "row-fresh"), true)
            .unwrap();
        let row = registry
            .overhaul_lookup_run("row-fresh")
            .unwrap()
            .unwrap()
            .expect("acknowledged run must be indexed before lookup");
        assert_eq!(row.state, broca_core::RunIndexState::Active);
        writer.append(1, &finished(), false).unwrap();
        assert!(
            registry
                .overhaul_active_runs(&[])
                .unwrap()
                .unwrap()
                .iter()
                .any(|(id, _)| id.0 == "row-fresh"),
            "an unsynced terminal must not enter the run-row view"
        );
        writer.sync().unwrap();
        assert_eq!(
            registry
                .overhaul_lookup_run("row-fresh")
                .unwrap()
                .unwrap()
                .unwrap()
                .state,
            broca_core::RunIndexState::Completed
        );
        assert!(registry
            .overhaul_active_runs(&[])
            .unwrap()
            .unwrap()
            .is_empty());
    }

    #[test]
    fn run_event_discovery_catches_acknowledged_runs_before_a_publisher_tick() {
        let root = Root::new("fresh-run-event");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "run-event");
        let events = overhaul.event_store();
        events.enable_bus_events("test").unwrap();
        events.begin_bus_event_period(100).unwrap();
        let mut writer = wal_store.open(&bind).unwrap();
        let mut start = started(&bind, "bus-fresh");
        if let WalRecord::RunStarted { ts_ms, .. } = &mut start {
            *ts_ms = Some(101);
        }
        writer.append(1, &start, true).unwrap();
        assert!(overhaul.db.lookup_run("bus-fresh").unwrap().is_none());
        assert!(
            events.bus_event_open_sessions().unwrap().contains(&bind),
            "run events must discover the acknowledged WAL before the worker drains"
        );
    }

    #[tokio::test]
    async fn native_retained_event_get_does_not_require_a_legacy_materializer() {
        let root = Root::new("native-event-get");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "event-get");
        let events = overhaul.event_store();
        events.enable_bus_events("test").unwrap();
        events.begin_bus_event_period(100).unwrap();
        let mut writer = wal_store.open(&bind).unwrap();
        let mut start = started(&bind, "retained-event");
        if let WalRecord::RunStarted { ts_ms, .. } = &mut start {
            *ts_ms = Some(101);
        }
        writer.append(1, &start, true).unwrap();
        let proof = writer.durable_publication().unwrap();
        events
            .project_new_bus_events(
                &bind,
                &proof.lineage().to_string(),
                &writer.replay().unwrap(),
                102,
            )
            .unwrap();
        let retained = events
            .pending_bus_event()
            .unwrap()
            .expect("the event was retained before bus publication");
        let opener = broca_module::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(root.0.join("leases"))),
            wal_store.clone(),
            Arc::new(RefusesExecution),
        );
        let registry =
            broca_module::LineageRegistry::new_with_reader(Arc::new(opener), wal_store, None);
        let reader = events.clone();
        registry.set_run_event_reader(Arc::new(move |key| {
            reader.get_bus_event(key).map_err(|e| e.to_string())
        }));
        let value = registry
            .route_run_event(&bind, &retained.event_key)
            .await
            .unwrap();
        assert!(
            matches!(value,broca_wire::RunEventGetResponse::Found{jcs,..} if jcs.as_bytes()==retained.body)
        );
        assert!(matches!(
            registry
                .route_run_event(
                    &BindIdentity::new("/composition", "runner", "other"),
                    &retained.event_key
                )
                .await
                .unwrap(),
            broca_wire::RunEventGetResponse::Absent
        ));
        no_legacy_files(&root);
    }

    #[test]
    fn mismatched_publication_fails_only_its_session_without_poisoning_registration() {
        let root = Root::new("publication-mismatch");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal_store = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "mismatch");
        let writer = wal_store.open(&bind).unwrap();
        writer.sync().unwrap();
        let previous = writer.durable_publication().unwrap();
        let factory = overhaul.publications.observer();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            factory(
                &bind,
                &root.0.join("wal/different-address.wal"),
                previous.lineage(),
            )
        }));
        assert!(
            result.is_ok(),
            "registration must never panic while holding the session map"
        );
        assert!(result.unwrap()(previous.borrowed()).is_err());
        assert!(
            overhaul.publications.witness(&bind, None).is_err(),
            "the mismatched session must be unavailable"
        );
        assert!(overhaul.publications.slots.lock().is_ok());
        let other = BindIdentity::new("/composition", "runner", "healthy");
        let healthy = wal_store.open(&other).unwrap();
        healthy.sync().unwrap();
        assert!(overhaul.publications.witness(&other, None).is_ok());
        drop(healthy);
        drop(writer);
    }

    use super::*;
    use broca_core::WalStore;
    use broca_wal::{RunConfig, Wal, WalRecord};

    struct Root(PathBuf);
    impl Root {
        fn new(name: &str) -> Self {
            Self::with_format(name, true)
        }
        fn with_format(name: &str, overhaul: bool) -> Self {
            static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "composition-{name}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&root).unwrap();
            super::super::state_format::prepare(&root, overhaul).unwrap();
            Self(root)
        }
    }
    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    fn started(session: &BindIdentity, run: &str) -> WalRecord {
        WalRecord::RunStarted {
            compaction_plan_version: None,
            sent_by: None,
            written_at_ms: None,
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: None,
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            run_id: broca_protocol::RunId(run.into()),
            session: session.clone(),
            ts_ms: None,
            config: Box::new(RunConfig {
                global_system: None,
                account_limit_pause: false,
                turn_directives_v1: None,
                render_tags: false,
                computed_tags: None,
                model: broca_protocol::request::ModelHandle::new("openai", "test-model"),
                tools: Vec::new().into(),
                tool_choice: Default::default(),
                generation: Default::default(),
                response_format: Default::default(),
                provider_options: serde_json::Value::Null,
                cache_tiers: serde_json::Value::Null,
                context_epoch: 0,
                renderer_version: 1,
                frozen_render_config: serde_json::Value::Null,
                auth_selection: None,
                tools_were_omitted: false,
                budget: None,
                context_limit: None,
                work_class: None,
                variant: None,
                service_tier: None,
                stop_when: Vec::new(),
                context_management: None,
            }),
            input: vec![broca_protocol::Message::new(
                broca_protocol::Role::User,
                vec![broca_protocol::ContentBlock::Text {
                    text: "hello".into(),
                }],
            )]
            .into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        }
    }

    fn finished() -> WalRecord {
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: broca_protocol::RunFinishReason::Completed,
            usage: broca_protocol::Usage::default(),
            retries_used: 0,
            error: None,
        }
    }

    fn wal(root: &Root, overhaul: &Overhaul) -> Arc<FileWalStore> {
        let publications = overhaul.publications.clone();
        let wal = Arc::new(
            FileWalStore::with_config(root.0.join("wal"), broca_wal::WalConfig::OVERHAUL)
                .with_session_guard(Arc::new(move |session| {
                    publications.ensure_available(session)
                }))
                .with_publication_observer(overhaul.publications.observer()),
        );
        wal.set_session_ref_index(overhaul.db.clone());
        wal
    }

    fn no_legacy_files(root: &Root) {
        for file in ["run-index.db", "run-index.db-wal", "run-index.db-shm"] {
            assert!(!root.0.join(file).exists(), "{file}");
        }
    }

    struct RefusesExecution;
    #[async_trait::async_trait]
    impl broca_module::executor::RunExecutor for RefusesExecution {
        async fn execute(
            &self,
            job: broca_module::executor::RunJob,
            _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
        ) -> Result<broca_core::RunSummary, broca_core::RunError> {
            let error = broca_core::RunError::Wal("fixture stops after admission reads".into());
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Err(error.clone()));
            }
            Err(error)
        }
    }

    fn request(bind: &BindIdentity) -> broca_core::RunRequest {
        broca_core::RunRequest {
            input_written_at_ms: None,
            directives: None,
            session_class: None,
            retention: None,
            on_restart: None,
            keep_warm: None,
            project_id: None,
            principal: None,
            run_id: broca_protocol::RunId("probe".into()),
            session: bind.clone(),
            model: broca_protocol::request::ModelHandle::new("openai", "test-model"),
            input: Vec::new(),
            tools: Vec::new(),
            policy_allowed: Default::default(),
            tool_choice: Default::default(),
            generation: Default::default(),
            cache: None,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
            submission_id: None,
            send_id: None,
            request_identity_digest: None,
            title: None,
            scope: None,
            inherit_frozen_cache: false,
            structured_output: None,
        }
    }

    struct MeasuredAdmission {
        inner: Arc<dyn broca_core::context::AdmissionReader>,
        access: broca_wal::access_counter::WalAccessCounter,
    }
    impl broca_core::context::AdmissionReader for MeasuredAdmission {
        fn prior_snapshot(
            &self,
            session: &BindIdentity,
            head: u64,
        ) -> Result<broca_core::run::PriorWalSnapshot, String> {
            self.access
                .operation("admission prior")
                .run(|| self.inner.prior_snapshot(session, head))
        }
        fn sender_snapshot(
            &self,
            session: &BindIdentity,
            head: u64,
        ) -> Result<broca_core::run::PriorWalSnapshot, String> {
            self.access
                .operation("admission sender")
                .run(|| self.inner.sender_snapshot(session, head))
        }
        fn paused_episode(
            &self,
            session: &BindIdentity,
            head: u64,
        ) -> Result<Option<broca_core::admission::LastEpisodeV1>, String> {
            self.access
                .operation("admission paused")
                .run(|| self.inner.paused_episode(session, head))
        }
    }

    #[tokio::test]
    async fn real_owned_opener_installs_bounded_admission_model_attach_and_raw_readers() {
        let root = Root::new("real-opener");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "real-opener");
        {
            let mut writer = wal.open(&bind).unwrap();
            writer
                .append(1, &started(&bind, "completed"), false)
                .unwrap();
            writer.append(1, &finished(), true).unwrap();
        }
        overhaul.publications.witness(&bind, None).unwrap();
        let access = broca_wal::access_counter::WalAccessCounter::default();
        let source = overhaul.publications.clone();
        let episodes = Arc::new(
            broca_module::registry::EpisodeReadSource::new(
                overhaul.db.clone(),
                Arc::new(move |session, head| source.witness_locked(session, head)),
            )
            .with_session_gates(overhaul.publications.gate_factory())
            .with_access_counter(access.clone()),
        );
        let opener = super::super::opener_for_state_root(
            Arc::new(broca_core::FileLeaseStore::new(root.0.join("leases"))),
            wal.clone(),
            Arc::new(RefusesExecution),
            StateFormat::Overhaul,
            &root.0,
        )
        .unwrap()
        .with_readers(Arc::new(broca_module::opener::ActorReadSources {
            admission: Arc::new(MeasuredAdmission {
                inner: overhaul.admission_reader(),
                access: access.clone(),
            }),
            episodes: episodes.clone(),
        }));
        let source = overhaul.publications.clone();
        let registry = broca_module::LineageRegistry::new_with_run_rows(
            Arc::new(opener),
            Some(wal.clone()),
            None,
            Some(Arc::new(broca_module::registry::OverhaulRunSource::new(
                overhaul.db.clone(),
                wal,
            ))),
        )
        .with_episode_source(episodes)
        .with_raw_page_source(Arc::new(
            broca_module::registry::RawPageSource::new(
                overhaul.db.clone(),
                Arc::new(move |session, head| source.witness_locked(session, head)),
            )
            .with_session_gates(overhaul.publications.gate_factory()),
        ));
        let outcome = broca_test_support::wait_for(
            "owned opener admission",
            registry.route_send(
                &bind,
                request(&bind),
                broca_module::executor::RunKind::FreshOrResume,
            ),
            || format!("{:?}", access.snapshot()),
        )
        .await;
        assert!(outcome.is_ok());
        let page = access
            .operation("raw page")
            .run_future(registry.route_read_page(&bind, None, None, false))
            .await
            .unwrap();
        assert_eq!(page.page.messages.len(), 1);
        let model = registry
            .route_read_model_page(&bind, &Default::default())
            .await
            .unwrap();
        assert_eq!(model.messages.len(), 1);
        let (sink, mut delivered) = tokio::sync::mpsc::channel(32);
        registry
            .route_subscribe(&bind, broca_module::fanout::FromCursor::Start, sink)
            .await
            .unwrap();
        let first =
            broca_test_support::wait_for("database attach replay", delivered.recv(), || {
                format!("{:?}", access.snapshot())
            })
            .await
            .unwrap();
        assert!(!matches!(first, broca_module::fanout::Delivery::Error(_)));
        let measured = access.snapshot();
        for (kind, expected_reads, expected_decodes) in [
            ("admission prior", 11, 17),
            ("raw page", 8, 11),
            ("model page", 8, 17),
            ("model attach", 2, 7),
        ] {
            let reads: Vec<_> = measured
                .iter()
                .filter(|operation| operation.name == kind)
                .collect();
            assert!(!reads.is_empty(), "{kind}: {measured:?}");
            assert!(
                reads.iter().any(|operation| !operation.reads.is_empty()),
                "{kind}: {measured:?}"
            );
            for operation in reads {
                assert_eq!(
                    operation
                        .reads
                        .iter()
                        .filter(|r| r.offset == 0 && r.length > 69)
                        .count(),
                    0,
                    "{kind}: {operation:?}"
                );
                assert_eq!(
                    operation.reads.len(),
                    expected_reads,
                    "{kind}: {operation:?}"
                );
                assert_eq!(
                    operation.frames_decoded, expected_decodes,
                    "{kind}: {operation:?}"
                );
                assert!(operation.seed_fallbacks.is_empty(), "{kind}: {operation:?}");
            }
        }
        delivered.close();
        assert!(registry.stop_and_seal().await);
        no_legacy_files(&root);
    }

    #[cfg(feature = "test-clock")]
    #[tokio::test]
    async fn legacy_real_opener_constructs_no_overhaul_reader_and_keeps_its_read_path() {
        let root = Root::with_format("legacy-real-opener", false);
        let stores = Stores::open_boot_legacy(
            broca_wal::state_format::decide_state_root(&root.0)
                .unwrap()
                .legacy()
                .unwrap(),
            broca_wal::WalConfig::LEGACY,
        )
        .unwrap();
        assert!(stores.overhaul.is_none());
        let index = stores.legacy.as_ref().unwrap();
        let wal = Arc::new(FileWalStore::with_config(
            root.0.join("wal"),
            broca_wal::WalConfig::LEGACY,
        ));
        wal.set_session_ref_index(index.clone());
        let opener = super::super::opener_for_state_root(
            Arc::new(broca_core::FileLeaseStore::new(root.0.join("leases"))),
            wal.clone(),
            Arc::new(RefusesExecution),
            StateFormat::Legacy,
            &root.0,
        )
        .unwrap()
        .with_run_index(index.clone())
        .with_materialized_store(index.clone());
        let registry = broca_module::LineageRegistry::new_with_reader(
            Arc::new(opener),
            wal.clone(),
            Some(index.clone()),
        );
        let bind = BindIdentity::new("/composition", "runner", "legacy");
        registry.actor_for(&bind).await.unwrap();
        // Pause actor housekeeping so this count covers only the requested page.
        let guard = registry
            .lock_move_sessions_with_mode(
                std::slice::from_ref(&bind),
                false,
                broca_module::registry::CutLockMode::Mutate,
            )
            .await
            .unwrap();
        let before = wal.range_reads_for_test();
        let page = registry
            .route_read_page(&bind, None, None, false)
            .await
            .unwrap();
        assert!(page.page.messages.is_empty());
        assert_eq!(wal.range_reads_for_test() - before, 1);
        assert!(root.0.join("run-index.db").exists());
        assert!(!root.0.join("broca.db").exists());
        drop(guard);
        assert!(registry.stop_and_seal().await);
    }

    #[tokio::test]
    async fn unfinished_checkpoint_still_uses_whole_log_writer_recovery_on_owned_reopen() {
        let root = Root::new("unfinished-recovery");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "unfinished");
        let length;
        {
            let mut writer = wal.open(&bind).unwrap();
            writer
                .append(1, &started(&bind, "completed"), false)
                .unwrap();
            writer.append(1, &finished(), true).unwrap();
            writer
                .append(1, &started(&bind, "unfinished"), true)
                .unwrap();
            let proof = overhaul.publications.witness(&bind, None).unwrap();
            length = proof.byte_end();
        }
        let publications = overhaul.publications.clone();
        let episodes = Arc::new(
            broca_module::registry::EpisodeReadSource::new(
                overhaul.db.clone(),
                Arc::new(move |session, head| publications.witness_locked(session, head)),
            )
            .with_session_gates(overhaul.publications.gate_factory()),
        );
        let opener = super::super::opener_for_state_root(
            Arc::new(broca_core::FileLeaseStore::new(root.0.join("leases"))),
            wal.clone(),
            Arc::new(RefusesExecution),
            StateFormat::Overhaul,
            &root.0,
        )
        .unwrap()
        .with_readers(Arc::new(broca_module::opener::ActorReadSources {
            admission: overhaul.admission_reader(),
            episodes,
        }));
        let registry = broca_module::LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
        let access = broca_wal::access_counter::WalAccessCounter::default();
        access
            .operation("unfinished writer recovery")
            .run_future(registry.actor_for(&bind))
            .await
            .unwrap();
        let measured = &access.snapshot()[0];
        assert_eq!(measured.writer_opens.len(), 1, "{measured:?}");
        let recovery = &measured.writer_opens[0];
        assert_eq!(recovery.opens, 1);
        assert_eq!(
            recovery.bytes,
            length * 3,
            "existing preflight, recovery and predecessor scans: {measured:?}"
        );
        assert_eq!(
            recovery.frames_decoded, 21,
            "three records and four lineage frames per scan: {measured:?}"
        );
        assert!(registry.stop_and_seal().await);
        no_legacy_files(&root);
    }

    #[test]
    fn burst_from_raw_maintenance_writer_keeps_latest_proof_until_publisher_drains() {
        let root = Root::new("burst");
        let mut stores = Stores::open_overhaul(&root.0).unwrap();
        assert!(stores.legacy.is_none());
        let overhaul = stores.overhaul.as_mut().unwrap();
        let wal = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "burst");
        let mut writer = wal.open(&bind).unwrap();
        for n in 0..128 {
            writer
                .append(1, &started(&bind, &format!("run-{n}")), false)
                .unwrap();
            writer.append(1, &finished(), true).unwrap();
        }
        assert_eq!(overhaul.publications.slots.lock().unwrap().len(), 1);
        assert!(overhaul.db.lookup_run("run-127").unwrap().is_none());
        // The worker was deliberately not running during the entire burst.
        overhaul.start().unwrap();
        broca_test_support::wait_until_sync("publisher drains newest raw-writer proof", || {
            overhaul
                .db
                .lookup_run("run-127")
                .map_err(|e| e.to_string())?
                .map(|_| ())
                .ok_or_else(|| "newest publication not indexed yet".into())
        });
        let proof = overhaul.publications.witness(&bind, None).unwrap();
        assert_eq!(proof.sequence(), writer.last_seq());
        no_legacy_files(&root);
    }

    #[test]
    fn historical_attach_proof_stops_at_handoff_when_publisher_is_ahead() {
        let root = Root::new("attach-handoff");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "handoff");
        let mut writer = wal.open(&bind).unwrap();
        writer.append(1, &started(&bind, "first"), false).unwrap();
        writer.append(1, &finished(), true).unwrap();
        let old = overhaul.publications.witness(&bind, None).unwrap();
        writer.append(1, &started(&bind, "later"), false).unwrap();
        writer.append(1, &finished(), true).unwrap();
        overhaul.publications.witness(&bind, None).unwrap();
        let access = broca_wal::access_counter::WalAccessCounter::default();
        let handoff = access.operation("historical attach proof").run(|| {
            overhaul
                .publications
                .witness(&bind, Some(old.sequence()))
                .unwrap()
        });
        assert_eq!(handoff, old);
        let measured = &access.snapshot()[0];
        assert!(!measured.reads.is_empty());
        assert!(
            measured
                .reads
                .iter()
                .all(|read| read.offset + read.length <= old.byte_end()),
            "{measured:?}"
        );
        assert_eq!(
            measured
                .reads
                .iter()
                .filter(|read| read.offset == 0 && read.length > 69)
                .count(),
            0
        );
        assert!(measured.writer_opens.is_empty());
    }

    #[test]
    fn forgotten_session_retires_its_proof_without_recreating_projection_rows() {
        let root = Root::new("retire-proof");
        let mut stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_mut().unwrap();
        let wal = wal(&root, overhaul);
        let old = BindIdentity::new("/composition", "runner", "forgotten");
        {
            let mut writer = wal.open(&old).unwrap();
            writer
                .append(1, &started(&old, "forgotten"), false)
                .unwrap();
            writer.append(1, &finished(), true).unwrap();
        }
        overhaul.publications.witness(&old, None).unwrap();
        overhaul.inventory().forget_projection(&old).unwrap();
        assert!(overhaul.publications.slots.lock().unwrap().is_empty());
        let live = BindIdentity::new("/composition", "runner", "live");
        let mut writer = wal.open(&live).unwrap();
        writer.append(1, &started(&live, "live"), false).unwrap();
        writer.append(1, &finished(), true).unwrap();
        overhaul.start().unwrap();
        broca_test_support::wait_until_sync(
            "publisher processes the remaining live session",
            || {
                overhaul
                    .db
                    .lookup_run("live")
                    .map_err(|e| e.to_string())?
                    .map(|_| ())
                    .ok_or_else(|| "live publication not indexed yet".into())
            },
        );
        assert!(overhaul.db.lookup_run("forgotten").unwrap().is_none());
        assert_eq!(overhaul.publications.slots.lock().unwrap().len(), 1);
        no_legacy_files(&root);
    }

    #[test]
    fn idle_read_catches_up_acknowledged_tail_without_waiting_for_background_worker() {
        let root = Root::new("idle-read");
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal = wal(&root, overhaul);
        let bind = BindIdentity::new("/composition", "runner", "idle");
        let mut writer = wal.open(&bind).unwrap();
        writer.append(1, &started(&bind, "first"), false).unwrap();
        writer.append(1, &finished(), true).unwrap();
        overhaul.publications.witness(&bind, None).unwrap();
        writer.append(1, &started(&bind, "second"), false).unwrap();
        writer.append(1, &finished(), true).unwrap();
        let counter = broca_wal::access_counter::WalAccessCounter::default();
        let page = counter.operation("idle raw page").run(|| {
            let proof = overhaul.publications.witness(&bind, None).unwrap();
            overhaul
                .db
                .read_raw_page(
                    &bind,
                    &broca_core::agent_session_ref(&bind),
                    LogLocation::Live,
                    &proof,
                    Default::default(),
                )
                .unwrap()
        });
        assert_eq!(page.at_seq, writer.last_seq());
        assert_eq!(page.page.messages.len(), 2);
        let access = &counter.snapshot()[0];
        assert!(!access.reads.is_empty());
        assert_eq!(
            access
                .reads
                .iter()
                .filter(|r| r.offset == 0 && r.length > 69)
                .count(),
            0,
            "{access:?}"
        );
        assert!(access.writer_opens.is_empty(), "{access:?}");
        no_legacy_files(&root);
    }

    #[test]
    fn boot_witness_serves_closed_session_without_opening_a_writer_or_scanning_origin() {
        let root = Root::new("closed-boot");
        let bind = BindIdentity::new("/composition", "runner", "closed");
        {
            let stores = Stores::open_overhaul(&root.0).unwrap();
            let overhaul = stores.overhaul.as_ref().unwrap();
            let wal = wal(&root, overhaul);
            let mut writer = wal.open(&bind).unwrap();
            writer.append(1, &started(&bind, "closed"), false).unwrap();
            writer.append(1, &finished(), true).unwrap();
            overhaul.publications.witness(&bind, None).unwrap();
        }
        let stores = Stores::open_overhaul(&root.0).unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        let wal = wal(&root, overhaul);
        let counter = broca_wal::access_counter::WalAccessCounter::default();
        counter.operation("boot then closed page").run(|| {
            overhaul.reconcile(&wal).unwrap();
            let proof = overhaul.publications.witness(&bind, None).unwrap();
            assert_eq!(
                overhaul
                    .db
                    .read_raw_page(
                        &bind,
                        &broca_core::agent_session_ref(&bind),
                        LogLocation::Live,
                        &proof,
                        Default::default()
                    )
                    .unwrap()
                    .page
                    .messages
                    .len(),
                1
            );
        });
        let access = &counter.snapshot()[0];
        assert!(!access.reads.is_empty());
        assert!(access.writer_opens.is_empty(), "{access:?}");
        assert_eq!(
            access
                .reads
                .iter()
                .filter(|r| r.offset == 0 && r.length > 69)
                .count(),
            0,
            "{access:?}"
        );
        no_legacy_files(&root);
    }
}
