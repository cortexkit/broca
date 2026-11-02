//! Test host for the installed daemon composition, without the module-wire transport.

use crate::composition::{opener_for_state_root, Publications, Stores};
use crate::state_format::StateFormat;
use broca_core::{FileLeaseStore, FileWalStore};
use broca_module::executor::{RunExecutor, RunJob, RunKind};
use broca_protocol::BindIdentity;
use broca_wal::access_counter::WalAccessCounter;
use std::path::Path;
use std::sync::Arc;

pub(crate) type ResumedPrompts = Arc<std::sync::Mutex<Vec<Vec<broca_protocol::Message>>>>;

pub(crate) struct OverhaulDaemon {
    pub registry: broca_module::LineageRegistry,
    pub wal: Arc<FileWalStore>,
    pub stores: Stores,
    pub resumed_prompts: ResumedPrompts,
    pub resume_windows: Arc<std::sync::Mutex<Vec<bool>>>,
    pub execution_done: Arc<tokio::sync::Notify>,
}

impl OverhaulDaemon {
    pub async fn shutdown(self) {
        assert!(
            self.registry.stop_and_seal().await,
            "installed writer did not seal"
        );
    }
    pub async fn boot(root: &Path, access: WalAccessCounter) -> Self {
        let (stores, wal) = access
            .operation("boot and publication feed")
            .run_future(Stores::boot(
                StateFormat::Overhaul,
                root,
                FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL),
            ))
            .await
            .unwrap();
        let overhaul = stores.overhaul.as_ref().unwrap();
        access.operation("restart discovery").run(|| {
            crate::composition::RestartDiscovery::Overhaul(overhaul.recovery_discovery())
                .candidates()
                .unwrap();
        });
        let publications = overhaul.publications.clone();
        let resumed_prompts = ResumedPrompts::default();
        let resume_windows = Arc::new(std::sync::Mutex::new(Vec::new()));
        let execution_done = Arc::new(tokio::sync::Notify::new());
        let executor = Arc::new(Executor {
            wal: wal.clone(),
            publications,
            access: access.clone(),
            resumed_prompts: resumed_prompts.clone(),
            resume_windows: resume_windows.clone(),
            execution_done: execution_done.clone(),
        });
        let publications = overhaul.publications.clone();
        let episodes = Arc::new(
            broca_module::registry::EpisodeReadSource::new(
                overhaul.db.clone(),
                Arc::new(move |key, head| publications.witness_locked(key, head)),
            )
            .with_session_gates(overhaul.publications.gate_factory())
            .with_access_counter(access.clone()),
        );
        let opener = opener_for_state_root(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            wal.clone(),
            executor,
            StateFormat::Overhaul,
            root,
        )
        .unwrap()
        .with_readers(Arc::new(broca_module::opener::ActorReadSources {
            admission: Arc::new(Admission {
                inner: overhaul.admission_reader(),
                access,
            }),
            episodes: episodes.clone(),
        }));
        let publications = overhaul.publications.clone();
        let registry = broca_module::LineageRegistry::new_with_run_rows(
            Arc::new(opener),
            Some(wal.clone()),
            None,
            Some(overhaul.run_rows(wal.clone())),
        )
        .with_episode_source(episodes)
        .with_raw_page_source(Arc::new(
            broca_module::registry::RawPageSource::new(
                overhaul.db.clone(),
                Arc::new(move |key, head| publications.witness_locked(key, head)),
            )
            .with_session_gates(overhaul.publications.gate_factory()),
        ));
        registry.set_session_address_resolver(overhaul.db.clone());
        Self {
            registry,
            wal,
            stores,
            resumed_prompts,
            resume_windows,
            execution_done,
        }
    }
}

struct Executor {
    wal: Arc<FileWalStore>,
    publications: Arc<Publications>,
    access: WalAccessCounter,
    resumed_prompts: ResumedPrompts,
    resume_windows: Arc<std::sync::Mutex<Vec<bool>>>,
    execution_done: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl RunExecutor for Executor {
    async fn execute(
        &self,
        mut job: RunJob,
        out: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        let operation = self
            .access
            .operation(format!("resume seed {}", job.req.run_id.0));
        if job.req.run_id.0 == "zero-step" {
            let stop = tokio_util::sync::CancellationToken::new();
            stop.cancel();
            job.control.stop_requested = Some(stop);
        }
        let result = operation
            .run_future(async {
                let mode = match job.kind {
                    RunKind::FreshOrResume => broca_core::StartMode::FreshOrResume,
                    RunKind::AppendEpisode => broca_core::StartMode::AppendEpisode,
                    RunKind::ResumeAfterAuth => broca_core::StartMode::ResumeAfterAuth,
                };
                let answer = (broca_crash_harness::process_kill_probe().scripts)().remove(0);
                let provider = broca_core::StubProvider::new(vec![answer]);
                let reader = ObservedResume {
                    inner: self
                        .publications
                        .resume_reader(&job.req.session)
                        .map_err(broca_core::RunError::Wal)?,
                    windows: self.resume_windows.clone(),
                };
                let result = broca_core::run::run_loop_on_writer_from_offsets(
                    job.req,
                    &job.writer,
                    mode,
                    &provider,
                    &broca_core::StubToolPlane::new(),
                    &broca_core::AllowAll,
                    self.wal.as_ref(),
                    job.control,
                    &out,
                    job.on_admitted,
                    &reader,
                )
                .await;
                self.resumed_prompts
                    .lock()
                    .unwrap()
                    .extend(provider.calls().into_iter().map(|call| call.prompt));
                result
            })
            .await;
        self.execution_done.notify_one();
        result
    }
}

struct ObservedResume {
    inner: Arc<dyn broca_core::resume::OffsetResumeReader>,
    windows: Arc<std::sync::Mutex<Vec<bool>>>,
}
impl broca_core::resume::OffsetResumeReader for ObservedResume {
    fn snapshot(
        &self,
        head: u64,
    ) -> Result<broca_core::resume::OffsetResumeSnapshot, broca_core::replay::SeedFallback> {
        let snapshot = self.inner.snapshot(head)?;
        self.windows.lock().unwrap().push(snapshot.window.is_some());
        Ok(snapshot)
    }
    fn read_at(&self, offset: u64, length: u64) -> std::io::Result<Vec<u8>> {
        self.inner.read_at(offset, length)
    }
    fn reconstructed(&self, fallback: Option<broca_core::replay::SeedFallback>) {
        self.inner.reconstructed(fallback);
    }
}

struct Admission {
    inner: Arc<dyn broca_core::context::AdmissionReader>,
    access: WalAccessCounter,
}
impl broca_core::context::AdmissionReader for Admission {
    fn prior_snapshot(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<broca_core::PriorWalSnapshot, String> {
        self.access
            .operation("admission prior")
            .run(|| self.inner.prior_snapshot(session, head))
    }
    fn sender_snapshot(
        &self,
        session: &BindIdentity,
        head: u64,
    ) -> Result<broca_core::PriorWalSnapshot, String> {
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

/// Keep fixture-writer startup reads in the opening operation even when the
/// production writer dispatches its blocking open onto another thread.
pub(crate) struct CountedWriterStore {
    pub inner: Arc<FileWalStore>,
    pub operation: broca_wal::access_counter::WalAccessOperation,
}
impl broca_core::WalStore for CountedWriterStore {
    fn open(
        &self,
        session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.operation.run(|| self.inner.open(session))
    }
    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.operation.run(|| {
            broca_wal::access_counter::writer_open(&self.inner.wal_path(session), false, || {
                self.inner.read_records(session)
            })
        })
    }
    fn open_owned_writer(
        &self,
        session: &BindIdentity,
        retention: Option<u64>,
        class: Option<&str>,
        lease: Box<dyn broca_core::lease::LeaseHandle>,
    ) -> Result<broca_core::wal_writer::WriterAndLease, broca_wal::WalError> {
        self.operation.run(|| {
            self.inner
                .open_owned_writer(session, retention, class, lease)
        })
    }
    fn wal_config(&self) -> broca_wal::WalConfig {
        broca_wal::WalConfig::OVERHAUL
    }
    fn writer_marker_root(&self) -> Option<std::path::PathBuf> {
        self.inner.writer_marker_root()
    }
}
