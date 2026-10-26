//! The default lineage opener: acquire the session lease, spawn the session-scoped WAL
//! writer that stamps its records with the lease epoch, and start the actor.
//!
//! The opener is the seam between the writer-agnostic registry and the concrete
//! lease/WAL/executor. The race harness supplies a fake opener (in-process lease + fake
//! executor) so the admission concurrency is deterministically testable; this is the real
//! one.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::lease::{OwnedWalTenure, WalLeaseDomain};
use broca_core::{lease_key_for, LeaseError, LeaseHandle, LeaseStore, WalHandle, WalStore};

use crate::actor::{LineageActor, IDLE_TTL};
use crate::executor::RunExecutor;
use crate::registry::{ActorHandle, LineageKey, LineageOpener, OpenError, RegistryHandle};

/// Opens a lineage with a real lease + session WAL writer + the configured executor.
pub struct DefaultOpener<L: LeaseStore, W: WalStore> {
    projection_fences: std::sync::Mutex<
        std::collections::HashMap<LineageKey, std::sync::Weak<tokio::sync::Mutex<()>>>,
    >,
    lease_store: Arc<L>,
    wal_store: Arc<W>,
    executor: Arc<dyn RunExecutor>,
    idle_ttl: Duration,
    /// The durable run index handed to each actor (coordinator-owned). `None` ⇒ run-keyed
    /// ops fall back to the WAL (no fast index). The real serve binary configures it.
    run_index: Option<Arc<dyn broca_core::RunIndex>>,
    materialized_store: Option<Arc<dyn broca_core::SessionStore>>,
    export_index: Option<Arc<dyn broca_core::ExportIndex>>,
    charge_basis_resolver: Option<Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>>,
    /// When set, actor opens acquire an owned writer tenure from this domain instead of
    /// a bare lease from `lease_store`, and the session writer task keeps that tenure
    /// until its WAL closes. Unset keeps the guarded open, where the actor holds the lease.
    lease_domain: Option<WalLeaseDomain>,
}

/// The session authority an open holds between acquisition and the writer spawn.
enum OpenAuthority {
    /// A bare lease the actor will hold beside its writer (no lease domain configured).
    Lease(Box<dyn LeaseHandle>),
    /// An owned tenure the writer open consumes and the writer task keeps.
    Tenure(OwnedWalTenure),
}

impl<L: LeaseStore, W: WalStore> DefaultOpener<L, W> {
    pub fn new(lease_store: Arc<L>, wal_store: Arc<W>, executor: Arc<dyn RunExecutor>) -> Self {
        Self {
            projection_fences: Default::default(),
            lease_store,
            wal_store,
            executor,
            idle_ttl: IDLE_TTL,
            run_index: None,
            materialized_store: None,
            export_index: None,
            charge_basis_resolver: None,
            lease_domain: None,
        }
    }

    /// Open actors under owned writer tenures minted by `domain`. The domain must name
    /// the same lease directory as the lease store, so owned and guarded acquisitions
    /// of one session contend on one lock.
    pub fn with_lease_domain(mut self, domain: WalLeaseDomain) -> Self {
        self.lease_domain = Some(domain);
        self
    }

    /// Override the idle-close TTL (tests use a short one to exercise the close races).
    pub fn with_idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = ttl;
        self
    }

    /// Configure the durable run index (the serve binary wires a SqliteStore here).
    pub fn with_run_index(mut self, run_index: Arc<dyn broca_core::RunIndex>) -> Self {
        self.run_index = Some(run_index);
        self
    }

    /// Configure materialized reads and WAL projection independently of the run-index capability.
    pub fn with_materialized_store(mut self, store: Arc<dyn broca_core::SessionStore>) -> Self {
        self.materialized_store = Some(store);
        self
    }

    /// Configure the daemon-wide usage export projection. Emission is best-effort after the
    /// terminal run-index write; the WAL remains authoritative if the export write fails.
    pub fn with_export_index(mut self, index: Arc<dyn broca_core::ExportIndex>) -> Self {
        self.export_index = Some(index);
        self
    }

    /// Configure the provider/auth resolver used to derive immutable charge-basis facts.
    pub fn with_charge_basis_resolver(
        mut self,
        resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>,
    ) -> Self {
        self.charge_basis_resolver = Some(resolver);
        self
    }
}

#[async_trait]
impl<L: LeaseStore + 'static, W: WalStore + 'static> LineageOpener for DefaultOpener<L, W> {
    fn run_index_reader(&self) -> Option<Arc<dyn broca_core::RunIndex>> {
        self.run_index.clone()
    }

    fn retire_session_transport(&self, session: &LineageKey) {
        self.executor.retire_session_transport(session);
    }

    fn retire_idle_session_transport(&self, session: &LineageKey) {
        self.executor.retire_idle_session_transport(session);
    }

    fn retire_all_transports(&self) {
        self.executor.retire_all_transports();
    }
    async fn media_profile(
        &self,
        request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, broca_core::RunError>
    {
        self.executor.media_profile(request).await
    }
    fn check_move(&self, key: &LineageKey, write: bool) -> Result<(), OpenError> {
        self.wal_store
            .check_move(key, write)
            .map_err(|e| OpenError::from_wal_read(e.to_string(), &e))
    }
    async fn backfill_attribution(
        &self,
        key: &LineageKey,
        range: broca_wal::WalRangeRead,
    ) -> Result<(), String> {
        let Some(store) = self.materialized_store.clone() else {
            return Ok(());
        };
        // A send waits for this small transaction, rather than observing its lease as busy.
        // A live actor holds the real lease, so this read-side repair never competes with it.
        let Ok(fence) = self.projection_fence(key).try_lock_owned() else {
            return Ok(());
        };
        let lease = match self.lease_store.acquire(&lease_key_for(key)) {
            Ok(lease) => lease,
            Err(LeaseError::Held { .. }) => return Ok(()),
            Err(error) => return Err(error.to_string()),
        };
        let session = key.clone();
        broca_diagnostics::spawn_blocking(broca_diagnostics::JobKind::Projection, move || {
            // Keep both fences inside the blocking job, even if its caller is cancelled.
            let (_fence, _lease) = (fence, lease);
            let snapshot = store.read_admission_snapshot(&session)?;
            if let Some(snapshot) = snapshot {
                let range_head = match range.records() {
                    broca_wal::WalRangeRecords::Complete(records) => records.last().map_or(0, |frame| frame.seq),
                    _ => return Err("attribution backfill requires a complete durable WAL".into()),
                };
                if snapshot.watermark_seq > range_head || matches!((snapshot.lineage_id.as_deref(), range.lineage_id.map(|id| id.to_string()).as_deref()), (Some(stored), Some(observed)) if stored != observed) {
                    return Ok(());
                }
            }
            store.project_range(&session, &range).map(|_| ())
        }).await.map_err(|error| error.to_string())?
    }
    fn elicitation_plane(&self) -> Option<Arc<dyn broca_core::tool_plane::ElicitationPlane>> {
        self.executor.elicitation_plane()
    }
    fn hook_plane_available(&self) -> bool {
        self.executor.hook_plane().is_some()
    }

    fn compaction_supported(&self) -> bool {
        self.executor.compaction_plane().is_some()
    }
    fn backup_classes(&self) -> Vec<String> {
        self.wal_store.backup_classes()
    }

    async fn open(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.open_with_retention(key, gen, registry, None).await
    }

    async fn open_for_status(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.open_mode(key, gen, registry, None, None, true).await
    }

    async fn open_with_retention(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
    ) -> Result<ActorHandle, OpenError> {
        self.open_mode(key, gen, registry, retention, None, false)
            .await
    }

    async fn open_with_class(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
        session_class: Option<String>,
    ) -> Result<ActorHandle, OpenError> {
        self.open_mode(key, gen, registry, retention, session_class, false)
            .await
    }
}

impl<L: LeaseStore + 'static, W: WalStore + 'static> DefaultOpener<L, W> {
    fn projection_fence(&self, key: &LineageKey) -> Arc<tokio::sync::Mutex<()>> {
        let mut fences = self.projection_fences.lock().unwrap();
        fences.retain(|_, fence| fence.strong_count() > 0);
        if let Some(fence) = fences.get(key).and_then(std::sync::Weak::upgrade) {
            return fence;
        }
        let fence = Arc::new(tokio::sync::Mutex::new(()));
        fences.insert(key.clone(), Arc::downgrade(&fence));
        fence
    }
    async fn open_mode(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
        session_class: Option<String>,
        status: bool,
    ) -> Result<ActorHandle, OpenError> {
        self.wal_store
            .check_move(key, true)
            .map_err(|e| OpenError::from_wal_read(e.to_string(), &e))?;
        debug_assert!(
            key.validate_keyable().is_ok(),
            "DefaultOpener identity must be validated before durable key derivation"
        );
        let projection_fence = self.projection_fence(key).lock_owned().await;
        // 1. Acquire the SESSION lease (held for the actor's whole lifetime). With a lease
        //    domain it is an owned tenure, which the writer task will keep.
        let authority = loop {
            let acquired = match &self.lease_domain {
                Some(domain) => domain.acquire(key).map(OpenAuthority::Tenure),
                None => self
                    .lease_store
                    .acquire(&lease_key_for(key))
                    .map(OpenAuthority::Lease),
            };
            match acquired {
                Ok(authority) => break authority,
                Err(LeaseError::Held { .. }) => {
                    if status {
                        return Err(OpenError::LeaseHeld(key.session.clone()));
                    }
                    let deleting = if let Some(service) = registry.retention_service() {
                        let session = key.clone();
                        tokio::task::spawn_blocking(move || service.expired(&session, None))
                            .await
                            .map_err(|error| OpenError::Open(error.to_string()))?
                            .map_err(OpenError::Open)?
                            .is_some()
                    } else {
                        false
                    };
                    if !deleting {
                        return Err(OpenError::LeaseHeld(key.session.clone()));
                    }
                    // An expired lineage must finish deletion before a send can choose a new
                    // WAL directory. Another live writer still receives the ordinary refusal.
                    let stop = registry.stopping_signal();
                    tokio::select! {
                        _ = stop.cancelled() => return Err(OpenError::Open("module is stopping".into())),
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
                Err(error) => return Err(OpenError::Open(error.to_string())),
            }
        };
        drop(projection_fence);

        if matches!(authority, OpenAuthority::Tenure(_)) {
            // The one move-fence validation of this tenure, before any mutation the open
            // performs: expiry deletion, archive restore, torn-tail recovery or lineage
            // creation. The check before acquisition proves nothing about the tenure, since
            // a cut can freeze the session between the two; every new tenure checks again,
            // and no clean result carries over from an earlier one.
            if let Err(error) = self.wal_store.check_move(key, true) {
                drop(authority);
                return Err(OpenError::from_wal_read(error.to_string(), &error));
            }
        }

        let service = registry.retention_service();
        let store = self.materialized_store.clone();
        let wal = self.wal_store.clone();
        let session = key.clone();
        // Moving the lease into recovery prevents cancellation from releasing it while
        // deletion or admission validation still touches this address.
        let authority = tokio::task::spawn_blocking(move || {
            if status && service.is_some() {
                let meta = store
                    .as_ref()
                    .map(|store| store.read_admission_snapshot(&session))
                    .transpose()
                    .map_err(OpenError::Open)?
                    .flatten();
                // A live file's first RunStarted decides whether it is retained; an older
                // store checkpoint may describe a replaced file. When no WAL is present,
                // checkpoint.lineage_retention identifies the stored first-run policy and
                // a deletion tombstone can establish expiry without reading transcript data.
                let checkpoint_policy = meta
                    .as_ref()
                    .and_then(|meta| meta.checkpoint.as_ref())
                    .and_then(|checkpoint| checkpoint.lineage_retention);
                let retained = if wal.wal_present(&session) == Some(false) {
                    matches!(
                        checkpoint_policy,
                        Some(broca_core::LineageRetention::Retained { .. })
                    )
                } else {
                    wal.is_retention_lineage(&session)
                        .map_err(|e| OpenError::from_wal_read(e.to_string(), &e))?
                };
                if retained || wal.wal_present(&session) == Some(false) {
                    if let Some(service) = &service {
                        if let Some(expired_at_ms) =
                            service.expired(&session, None).map_err(OpenError::Open)?
                        {
                            return Err(OpenError::Expired { expired_at_ms });
                        }
                    } else if retained {
                        return Err(OpenError::Open(
                            "retention status service is not configured".into(),
                        ));
                    }
                }
            } else if let Some(service) = service {
                service
                    .delete_expired(&session, service.clock.now_ms())
                    .map_err(OpenError::Open)?;
            } else if retention.is_some() {
                return Err(OpenError::Open(
                    "retention service is not configured".into(),
                ));
            }
            if retention.is_some() {
                let meta = store
                    .as_ref()
                    .map(|store| store.read_admission_snapshot(&session))
                    .transpose()
                    .map_err(OpenError::Open)?
                    .flatten();
                let exists = wal
                    .lineage_exists(&session, meta.as_ref())
                    .map_err(|e| OpenError::Open(e.to_string()))?;
                let records = match wal.read_records(&session) {
                    Ok(records) => records,
                    // A missing live WAL permits a first run only when lineage_exists also
                    // rules out a log in an archive and session history kept only in the store.
                    Err(broca_wal::WalError::Io(error))
                        if error.kind() == std::io::ErrorKind::NotFound && !exists =>
                    {
                        Vec::new()
                    }
                    Err(error) => return Err(OpenError::Open(error.to_string())),
                };
                let current = crate::retention::policy(&records).map(|(seconds, _, _)| seconds);
                crate::retention::validate_retention(retention, current, exists)
                    .map_err(OpenError::RetentionInvalid)?;
            }
            Ok(authority)
        })
        .await
        .map_err(|e| OpenError::Open(e.to_string()))??;

        // 2. Build the control-stream fan-out FIRST — it produces the record observer the
        //    session writer publishes to (so the writer's appended records feed the
        //    projector → control ring).
        let (fanout, observer) = crate::fanout::Fanout::new(self.wal_store.clone(), key.clone());

        // 3. Open + recover + prove the SESSION WAL away from the async runtime, then spawn its
        //    writer with the fan-out observer. The lease moves through that blocking operation so
        //    cancellation cannot release it while recovery still touches the WAL. A failed proof
        //    returns no handle, and the registry cannot publish an Open slot. An owned tenure is
        //    consumed by the open and kept by the writer task; a bare lease comes back for the
        //    actor to hold.
        let map_open_error = |error: broca_core::WalOpenError| match error {
            broca_core::WalOpenError::SessionMoving { cut_id } => {
                OpenError::SessionMoving { cut_id }
            }
            broca_core::WalOpenError::SessionMoved { holder } => OpenError::SessionMoved { holder },
            broca_core::WalOpenError::NewerWriter { missing, error } => OpenError::NewerWriter {
                missing,
                reason: error.to_string(),
            },
            // WalWriteError erases I/O and recovery/proof causes; corruption cannot be
            // retried away, so do not classify this opaque write/open failure as transient.
            broca_core::WalOpenError::Failed(error) => OpenError::Open(error.to_string()),
        };
        let (writer, lease) = match authority {
            OpenAuthority::Lease(lease) => {
                let (writer, lease) = WalHandle::open_and_spawn_observed_with_class(
                    self.wal_store.clone(),
                    key.clone(),
                    lease,
                    Some(observer),
                    retention,
                    session_class,
                )
                .await
                .map_err(map_open_error)?;
                (writer, Some(lease))
            }
            OpenAuthority::Tenure(tenure) => {
                let writer = WalHandle::open_and_spawn_owned_with_class(
                    self.wal_store.clone(),
                    key.clone(),
                    tenure,
                    Some(observer),
                    retention,
                    session_class,
                )
                .await
                .map_err(map_open_error)?;
                (writer, None)
            }
        };
        if retention.is_some() || writer.is_retention_lineage() == Some(true) {
            if let Some(service) = registry.retention_service() {
                service.remember_session(key).map_err(OpenError::Open)?;
            }
        }
        let writer = Arc::new(writer);

        // 4. Start the actor, which owns the writer + fan-out (and a bare lease, if any)
        //    until close.
        Ok(match lease {
            Some(lease) => LineageActor::spawn_with_export(
                key.clone(),
                gen,
                lease,
                writer,
                self.executor.clone(),
                self.wal_store.clone(),
                registry,
                self.idle_ttl,
                fanout,
                self.run_index.clone(),
                self.materialized_store.clone(),
                self.export_index.clone(),
                self.charge_basis_resolver.clone(),
            ),
            None => LineageActor::spawn_owned_with_export(
                key.clone(),
                gen,
                writer,
                self.executor.clone(),
                self.wal_store.clone(),
                registry,
                self.idle_ttl,
                fanout,
                self.run_index.clone(),
                self.materialized_store.clone(),
                self.export_index.clone(),
                self.charge_basis_resolver.clone(),
            ),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::thread::ThreadId;

    use async_trait::async_trait;
    use broca_core::{FileLeaseStore, OutputEvent, RunError, RunSummary};
    use broca_protocol::BindIdentity;
    use broca_wal::{Wal, WalError, WalRecord};
    use tokio::sync::mpsc;

    use super::*;
    use crate::executor::{RunExecutor, RunJob};
    use crate::registry::LineageRegistry;

    struct FailingRecoveryWal;

    impl Wal for FailingRecoveryWal {
        fn append(
            &mut self,
            _fence: u64,
            _record: &WalRecord,
            _sync: bool,
        ) -> Result<u64, WalError> {
            unreachable!("writer construction must fail before append")
        }

        fn sync(&self) -> Result<(), WalError> {
            Err(WalError::Io(std::io::Error::other(
                "injected recovery fsync failure",
            )))
        }

        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn tail_after_seq(
            &self,
            _after_seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn last_seq(&self) -> u64 {
            3
        }
    }

    struct FailingRecoveryStore;

    impl WalStore for FailingRecoveryStore {
        fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            Ok(Box::new(FailingRecoveryWal))
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }
    }

    struct CountingRecoveryWal {
        open_thread: ThreadId,
        sync_calls: Arc<AtomicUsize>,
    }

    impl Wal for CountingRecoveryWal {
        fn append(
            &mut self,
            _fence: u64,
            _record: &WalRecord,
            _sync: bool,
        ) -> Result<u64, WalError> {
            unreachable!("the idle-open test never appends")
        }

        fn sync(&self) -> Result<(), WalError> {
            assert_eq!(
                std::thread::current().id(),
                self.open_thread,
                "WAL open and recovery proof must run in one blocking operation"
            );
            self.sync_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn tail_after_seq(
            &self,
            _after_seq: u64,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }

        fn last_seq(&self) -> u64 {
            0
        }
    }

    struct CountingRecoveryStore {
        open_threads: Arc<Mutex<Vec<ThreadId>>>,
        sync_calls: Arc<AtomicUsize>,
    }

    impl WalStore for CountingRecoveryStore {
        fn open(&self, _session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            let open_thread = std::thread::current().id();
            self.open_threads.lock().unwrap().push(open_thread);
            Ok(Box::new(CountingRecoveryWal {
                open_thread,
                sync_calls: Arc::clone(&self.sync_calls),
            }))
        }

        fn read_records(
            &self,
            _session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            Ok(Vec::new())
        }
    }

    struct NeverExecutor;

    #[async_trait]
    impl RunExecutor for NeverExecutor {
        async fn execute(
            &self,
            _job: RunJob,
            _out: mpsc::Sender<OutputEvent>,
        ) -> Result<RunSummary, RunError> {
            unreachable!("writer construction must fail before a run can start")
        }
    }

    async fn wait_for_actor_close(registry: &LineageRegistry, key: &BindIdentity) {
        for _ in 0..40 {
            tokio::time::sleep(Duration::from_millis(25)).await;
            if registry.slot_state(key).is_none() {
                return;
            }
        }
        panic!("actor did not close after its idle TTL");
    }

    #[tokio::test]
    async fn recovery_fsync_failure_fails_open_without_publishing_an_open_slot() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-opener-recovery-failure-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create test project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let opener = Arc::new(DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            Arc::new(FailingRecoveryStore),
            Arc::new(NeverExecutor),
        ));
        let registry = LineageRegistry::new(opener);

        let error = match registry.actor_for(&key).await {
            Ok(_) => panic!("recovery fsync failure must refuse actor open"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("injected recovery fsync failure"),
            "the failed recovery barrier must propagate through DefaultOpener: {error}"
        );
        assert_eq!(registry.slot_counts().open, 0);
        assert_eq!(registry.slot_counts().opening, 0);
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn async_open_proves_once_off_runtime_and_reproves_each_tenure() {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-opener-recovery-async-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create test project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let caller_thread = std::thread::current().id();
        let open_threads = Arc::new(Mutex::new(Vec::new()));
        let sync_calls = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(CountingRecoveryStore {
            open_threads: Arc::clone(&open_threads),
            sync_calls: Arc::clone(&sync_calls),
        });
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(root.join("leases"))),
                store,
                Arc::new(NeverExecutor),
            )
            .with_idle_ttl(Duration::from_millis(20)),
        );
        let registry = LineageRegistry::new(opener);

        drop(registry.actor_for(&key).await.expect("first actor open"));
        assert_eq!(sync_calls.load(Ordering::SeqCst), 1);
        {
            let threads = open_threads.lock().unwrap();
            assert_eq!(threads.len(), 1);
            assert_ne!(
                threads[0], caller_thread,
                "WAL open must not run on the current-thread runtime"
            );
        }
        wait_for_actor_close(&registry, &key).await;

        drop(registry.actor_for(&key).await.expect("reopened actor"));
        assert_eq!(
            sync_calls.load(Ordering::SeqCst),
            2,
            "each actor tenure performs exactly one recovery proof"
        );
        {
            let threads = open_threads.lock().unwrap();
            assert_eq!(threads.len(), 2);
            assert!(
                threads.iter().all(|thread| *thread != caller_thread),
                "every reopen must keep WAL recovery off the current-thread runtime"
            );
        }
        wait_for_actor_close(&registry, &key).await;
        let _ = std::fs::remove_dir_all(root);
    }

    /// A file-backed store that lets a cut freeze the session right after the first
    /// move-fence check passes, and records whether a writer open was ever attempted.
    struct FreezeAfterFirstCheck {
        inner: broca_core::FileWalStore,
        state_root: std::path::PathBuf,
        checks: AtomicUsize,
        writer_opens: AtomicUsize,
    }

    const CUT: &str = "0123456789abcdef0123456789abcdef";

    fn freeze(state_root: &std::path::Path, wal: &std::path::Path) {
        broca_wal::migration::freeze(
            state_root,
            wal,
            &broca_wal::migration::Freeze {
                cut_id: CUT.into(),
                end_offset: 0,
                lineage_id: "lineage".into(),
            },
        )
        .expect("publish freeze")
        .expect("session was not frozen yet");
    }

    impl WalStore for FreezeAfterFirstCheck {
        fn check_move(&self, session: &BindIdentity, write: bool) -> Result<(), WalError> {
            let result = self.inner.check_move(session, write);
            if self.checks.fetch_add(1, Ordering::SeqCst) == 0 {
                freeze(&self.state_root, &self.inner.wal_path(session));
            }
            result
        }
        fn open(&self, session: &BindIdentity) -> Result<Box<dyn Wal + Send>, WalError> {
            self.writer_opens.fetch_add(1, Ordering::SeqCst);
            self.inner.open(session)
        }
        fn read_records(
            &self,
            session: &BindIdentity,
        ) -> Result<Vec<broca_wal::FramedRecord>, WalError> {
            self.inner.read_records(session)
        }
        fn open_writer(
            &self,
            session: &BindIdentity,
            retention: Option<u64>,
            session_class: Option<&str>,
            lease: Box<dyn LeaseHandle>,
        ) -> Result<broca_core::wal_writer::WriterAndLease, WalError> {
            self.writer_opens.fetch_add(1, Ordering::SeqCst);
            self.inner
                .open_writer(session, retention, session_class, lease)
        }
        fn open_owned_writer(
            &self,
            session: &BindIdentity,
            retention: Option<u64>,
            session_class: Option<&str>,
            lease: Box<dyn LeaseHandle>,
        ) -> Result<broca_core::wal_writer::WriterAndLease, WalError> {
            self.writer_opens.fetch_add(1, Ordering::SeqCst);
            self.inner
                .open_owned_writer(session, retention, session_class, lease)
        }
    }

    /// Write a session log with one record followed by a torn frame, which a writer
    /// open would truncate during recovery.
    fn torn_session_log(store: &broca_core::FileWalStore, key: &BindIdentity) -> Vec<u8> {
        let mut wal = store.open(key).expect("seed WAL");
        wal.append(
            1,
            &WalRecord::StepStarted {
                step_id: broca_protocol::StepId(1),
            },
            true,
        )
        .expect("seed record");
        drop(wal);
        let path = store.wal_path(key);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.extend_from_slice(&[0x5a; 7]);
        std::fs::write(&path, &bytes).unwrap();
        bytes
    }

    #[tokio::test]
    async fn owned_open_checks_move_fence_before_recovery() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-opener-owned-fence-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create test project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let store = Arc::new(FreezeAfterFirstCheck {
            inner: broca_core::FileWalStore::new(root.join("wal")),
            state_root: root.clone(),
            checks: AtomicUsize::new(0),
            writer_opens: AtomicUsize::new(0),
        });
        let torn = torn_session_log(&store.inner, &key);
        let domain = WalLeaseDomain::new(root.join("leases"));
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(root.join("leases"))),
                store.clone(),
                Arc::new(NeverExecutor),
            )
            .with_lease_domain(domain.clone()),
        );
        let registry = LineageRegistry::new(opener.clone());

        // The check before acquisition passes; a cut freezes the session before the
        // tenure is acquired. The tenure's own check must refuse before any mutation.
        let error = match opener
            .open_with_class(&key, 1, registry.handle(), None, None)
            .await
        {
            Ok(_) => panic!("a session frozen before its tenure must not open"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, OpenError::SessionMoving { cut_id } if cut_id == CUT),
            "unexpected error: {error}"
        );
        assert_eq!(
            store.checks.load(Ordering::SeqCst),
            2,
            "one check before acquisition, one under the tenure"
        );
        assert_eq!(
            store.writer_opens.load(Ordering::SeqCst),
            0,
            "the refusal precedes the writer open and its recovery"
        );
        assert_eq!(
            std::fs::read(store.inner.wal_path(&key)).unwrap(),
            torn,
            "the torn tail is untouched: no recovery truncation ran"
        );
        drop(
            domain
                .acquire(&key)
                .expect("the refused open released its tenure"),
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn owned_tenure_reopen_rechecks_move_fence() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-opener-owned-recheck-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("project")).expect("create test project");
        let key = BindIdentity::new(root.join("project"), "runner", "session");
        let wal = Arc::new(broca_core::FileWalStore::new(root.join("wal")));
        let domain = WalLeaseDomain::new(root.join("leases"));
        let leases = FileLeaseStore::new(root.join("leases"));
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(root.join("leases"))),
                wal.clone(),
                Arc::new(NeverExecutor),
            )
            .with_lease_domain(domain.clone())
            .with_idle_ttl(Duration::from_millis(20)),
        );
        let registry = LineageRegistry::new(opener.clone());

        // The first tenure opens clean, and its writer task keeps the lease.
        drop(registry.actor_for(&key).await.expect("first owned open"));
        assert!(
            matches!(
                leases.acquire(&lease_key_for(&key)),
                Err(LeaseError::Held { .. })
            ),
            "the open actor's writer holds the session lease"
        );
        wait_for_actor_close(&registry, &key).await;
        drop(
            leases
                .acquire(&lease_key_for(&key))
                .expect("closing the writer released the tenure"),
        );

        // A cut freezes the session between tenures. The next tenure must find the
        // marker itself rather than trust the earlier tenure's clean check.
        let path = wal.wal_path(&key);
        let before = std::fs::read(&path).unwrap();
        freeze(&root, &path);
        let error = match opener
            .open_with_class(&key, 2, registry.handle(), None, None)
            .await
        {
            Ok(_) => panic!("a reopened tenure must recheck the move fence"),
            Err(error) => error,
        };
        assert!(
            matches!(&error, OpenError::SessionMoving { cut_id } if cut_id == CUT),
            "unexpected error: {error}"
        );
        assert_eq!(std::fs::read(&path).unwrap(), before);

        // Once the freeze is withdrawn, a fresh tenure opens again.
        broca_wal::migration::abort_freeze(&path, CUT).unwrap();
        drop(registry.actor_for(&key).await.expect("reopen after abort"));
        wait_for_actor_close(&registry, &key).await;
        let _ = std::fs::remove_dir_all(root);
    }
}
