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
    pub gate: Arc<Mutex<()>>,
}

impl Stores {
    /// Legacy store construction stays at its original startup point, after the
    /// held-call runtime has opened. A failed earlier boot must not create its DB.
    pub fn deferred_legacy() -> Self {
        Self {
            legacy: None,
            overhaul: None,
        }
    }

    pub fn open(format: StateFormat, root: &Path) -> Result<Self, String> {
        match format {
            StateFormat::Legacy => Ok(Self {
                legacy: Some(Arc::new(
                    broca_store::SqliteStore::open(root.join("run-index.db"))
                        .map_err(|e| format!("open run index: {e}"))?,
                )),
                overhaul: None,
            }),
            StateFormat::Overhaul => {
                let db = Arc::new(OverhaulDb::open(root).map_err(|e| e.to_string())?);
                let publications = Arc::new(Publications {
                    db: db.clone(),
                    root: root.to_path_buf(),
                    slots: Mutex::default(),
                    gate: Arc::default(),
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
    }
}

impl Publications {
    pub fn observer(self: &Arc<Self>) -> Arc<broca_core::wal_writer::PublicationObserverFactory> {
        let this = self.clone();
        Arc::new(move |session, path, lineage| {
            // Registration happens during writer open, not on its sync path.
            let name = path.file_name().expect("a WAL has a file name");
            let paths = vec![
                path.to_path_buf(),
                this.root.join("wal").join(name),
                this.root.join("wal-durable").join(name),
                this.root.join("wal-ephemeral").join(name),
            ];
            let slot = Arc::new(PublicationSlot::new(paths, lineage));
            let mut slots = this.slots.lock().unwrap();
            if let Some(previous) = slots.get(session).and_then(|slot| slot.snapshot()) {
                if previous.lineage() == lineage {
                    slot.record(&previous);
                }
            }
            slots.insert(session.clone(), slot.clone());
            Arc::new(move |publication| slot.record(publication))
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
        let _guard = self.gate.lock().map_err(|e| e.to_string())?;
        self.witness_locked(session, head)
    }

    pub fn witness_locked(
        &self,
        session: &BindIdentity,
        head: Option<u64>,
    ) -> Result<DurablePublication, String> {
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
            gate: self.publications.gate.clone(),
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
        let reader = broca_core::ReadOnlyWalReader::new(&self.root);
        for address in reader.addresses().map_err(|e| e.to_string())? {
            let mut session = self.db.read_snapshot(|tx| {
                use rusqlite::OptionalExtension;
                Ok(tx.query_row("SELECT project_root,harness,session_key FROM session_identity WHERE address=?1", [&address], |r| {
                    Ok(BindIdentity::new(r.get::<_,String>(0)?, r.get::<_,String>(1)?, r.get::<_,String>(2)?))
                }).optional()?)
            }).map_err(|e| e.to_string())?;
            if session.is_none() {
                broca_wal::access_counter::seed_fallback("boot identity inventory missing");
                let (records, _) = reader.records(&address).map_err(|e| e.to_string())?;
                session = records.iter().find_map(|frame| match &frame.record {
                    broca_wal::WalRecord::RunStarted { session, .. } => Some(session.clone()),
                    _ => None,
                });
            }
            let session = session
                .ok_or_else(|| format!("WAL {address} has no recoverable session identity"))?;
            let path = wal
                .resolve_live_path(&session)
                .map_err(|e| e.to_string())?
                .ok_or_else(|| {
                    format!("format-2 WAL {address} is archived; archive recovery is not enabled")
                })?;
            let result = self
                .db
                .catch_up_session(&broca_store::db::ReconcileSession {
                    bind: &session,
                    session_ref: &broca_core::agent_session_ref(&session),
                    location: LogLocation::Live,
                    path: &path,
                })
                .map_err(|e| e.to_string())?;
            self.remember(session, &result.publication);
        }
        Ok(())
    }
}

impl Overhaul {
    pub fn start(&mut self) -> Result<(), String> {
        let publications = self.publications.clone();
        let stop = self.stop.clone();
        self.worker = Some(std::thread::Builder::new().name("broca-db-publisher".into()).spawn(move || {
            let mut published = HashMap::new();
            loop {
                let sessions = {
                    let slots = publications.slots.lock().unwrap();
                    published.retain(|session, _| slots.contains_key(session));
                    slots.keys().cloned().collect::<Vec<_>>()
                };
                for session in sessions {
                    let _guard = publications.gate.lock().unwrap();
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
        }).map_err(|e| e.to_string())?);
        Ok(())
    }
}

struct Inventory {
    db: Arc<OverhaulDb>,
    publications: Arc<Publications>,
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
        let _guard = self.publications.gate.lock().map_err(|e| e.to_string())?;
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
    gate: Arc<Mutex<()>>,
}

impl broca_core::context::AdmissionReader for Admission {
    fn prior_snapshot(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<broca_core::run::PriorWalSnapshot, String> {
        let _guard = self.gate.lock().map_err(|e| e.to_string())?;
        self.inner.prior_snapshot(session, head)
    }
    fn sender_snapshot(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<broca_core::run::PriorWalSnapshot, String> {
        let _guard = self.gate.lock().map_err(|e| e.to_string())?;
        self.inner.sender_snapshot(session, head)
    }
    fn paused_episode(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<Option<broca_core::admission::LastEpisodeV1>, String> {
        let _guard = self.gate.lock().map_err(|e| e.to_string())?;
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
    use super::*;
    use broca_core::WalStore;
    use broca_wal::{RunConfig, WalRecord};

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
        let wal = Arc::new(
            FileWalStore::with_config(root.0.join("wal"), broca_wal::WalConfig::OVERHAUL)
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
        let stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
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
            .with_gate(overhaul.publications.gate.clone())
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
            .with_gate(overhaul.publications.gate.clone()),
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
            ("raw page", 12, 15),
            ("model page", 12, 21),
            ("model attach", 6, 11),
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
        let stores = Stores::open(StateFormat::Legacy, &root.0).unwrap();
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
        let stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
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
            .with_gate(overhaul.publications.gate.clone()),
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
        let mut stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
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
        let stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
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
        let mut stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
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
        let stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
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
            let stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
            let overhaul = stores.overhaul.as_ref().unwrap();
            let wal = wal(&root, overhaul);
            let mut writer = wal.open(&bind).unwrap();
            writer.append(1, &started(&bind, "closed"), false).unwrap();
            writer.append(1, &finished(), true).unwrap();
            overhaul.publications.witness(&bind, None).unwrap();
        }
        let stores = Stores::open(StateFormat::Overhaul, &root.0).unwrap();
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
