//! Lease-gated reclamation of writers that stopped without a durable close.
//!
//! A marker is only a discovery hint. Its identity must match its address, and the
//! session's exclusive lease must be acquired before opening a successor. Neither
//! elapsed time nor a process id is evidence that a writer can no longer append.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::time::Duration;

use broca_core::lease::{LeaseError, WalLeaseDomain};
use broca_core::wal_writer::{decode_writer_marker, session_addr};
use broca_core::{FileWalStore, WalHandle, WalStore};
use broca_diagnostics::{spawn_blocking, JobKind};
use broca_protocol::BindIdentity;
use tokio::sync::Notify;

const PASS_INTERVAL: Duration = Duration::from_secs(5 * 60);
const MARKER_BYTES_LIMIT: u64 = 64 * 1024;

fn wakes() -> &'static Mutex<HashMap<PathBuf, Weak<Notify>>> {
    static WAKES: OnceLock<Mutex<HashMap<PathBuf, Weak<Notify>>>> = OnceLock::new();
    WAKES.get_or_init(Mutex::default)
}

/// Wake maintenance only after teardown has closed the writer and released its lease.
/// A pending notification survives a pass already in progress, so a death during that
/// pass is not postponed until the next timer tick.
pub(super) fn wake_after_teardown(store: &impl WalStore) {
    let Some(root) = store.writer_marker_root() else {
        return;
    };
    if let Some(wake) = wakes().lock().unwrap().get(&root).and_then(Weak::upgrade) {
        wake.notify_one();
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    pub reclaimed: usize,
    pub live: usize,
    pub invalid: usize,
    pub failed: usize,
    pub vanished: usize,
}

/// Dropping the supervisor stops future passes. An in-flight WAL operation retains
/// its owned tenure until its writable descriptor has closed, even on cancellation.
pub struct ReclaimTask {
    join: tokio::task::JoinHandle<()>,
    #[cfg(test)]
    reports: tokio::sync::mpsc::UnboundedReceiver<PassReport>,
}

impl Drop for ReclaimTask {
    fn drop(&mut self) {
        self.join.abort();
    }
}

#[derive(Clone)]
struct Reclaimer {
    store: Arc<FileWalStore>,
    root: PathBuf,
    #[cfg(all(test, feature = "test-clock"))]
    fs: Option<Arc<dyn broca_wal::fs_ops::FsOps>>,
}

/// Start an immediate background pass followed by five-minute passes and poison
/// wake-ups. Legacy stores have no writer markers and start no maintenance task.
pub fn spawn(store: Arc<FileWalStore>) -> Option<ReclaimTask> {
    spawn_after_boot(store, async { Ok(()) })
}

/// Defer every reclaim pass, including teardown wake-ups, until crash-orphan
/// marking and sealing finish. A failed boot task leaves markers for the next start.
pub fn spawn_after_boot(
    store: Arc<FileWalStore>,
    boot: impl std::future::Future<Output = Result<(), String>> + Send + 'static,
) -> Option<ReclaimTask> {
    let root = store.writer_marker_root()?;
    Some(
        Reclaimer {
            store,
            root,
            #[cfg(all(test, feature = "test-clock"))]
            fs: None,
        }
        .spawn_after(PASS_INTERVAL, boot),
    )
}

/// Remove unpublished temporary marker names at boot, while the caller holds the
/// state-root lock and before starting its writers. WriterOpened is appended only
/// after rename to the final name, so no opened writer depends on a temporary name.
pub async fn reap_temporary_markers_at_boot(store: Arc<FileWalStore>) -> Result<usize, String> {
    let Some(root) = store.writer_marker_root() else {
        return Ok(0);
    };
    spawn_blocking(JobKind::Recovery, move || {
        let directory = root.join("open-writers");
        let entries = match std::fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => return Err(error.to_string()),
        };
        let mut reaped = 0;
        for entry in entries {
            let entry = entry.map_err(|error| error.to_string())?;
            let path = entry.path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(final_name) = name.strip_suffix(".tmp") else {
                continue;
            };
            if !valid_marker_name(final_name)
                || !entry
                    .file_type()
                    .map_err(|error| error.to_string())?
                    .is_file()
            {
                continue;
            }
            match broca_wal::fs_ops::remove_file(&path) {
                Ok(()) => reaped += 1,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(format!(
                        "cannot reap temporary writer marker {}: {error}",
                        path.display()
                    ))
                }
            }
        }
        if reaped > 0 {
            broca_wal::fs_ops::sync_dir(&directory).map_err(|error| error.to_string())?;
        }
        Ok(reaped)
    })
    .await
    .map_err(|error| error.to_string())?
}

impl Reclaimer {
    #[cfg(test)]
    fn spawn(self, period: Duration) -> ReclaimTask {
        self.spawn_after(period, async { Ok(()) })
    }

    fn spawn_after(
        self,
        period: Duration,
        boot: impl std::future::Future<Output = Result<(), String>> + Send + 'static,
    ) -> ReclaimTask {
        let wake = {
            let mut entries = wakes().lock().unwrap();
            entries.retain(|_, wake| wake.strong_count() > 0);
            if let Some(wake) = entries.get(&self.root).and_then(Weak::upgrade) {
                wake
            } else {
                let wake = Arc::new(Notify::new());
                entries.insert(self.root.clone(), Arc::downgrade(&wake));
                wake
            }
        };
        #[cfg(test)]
        let (reports_tx, reports) = tokio::sync::mpsc::unbounded_channel();
        let join = tokio::spawn(async move {
            if let Err(error) = boot.await {
                tracing::warn!(target: "writer_reclaim", %error, "boot recovery did not complete; writer reclamation deferred until next start");
                return;
            }
            let mut timer = tokio::time::interval(period);
            timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = timer.tick() => {},
                    _ = wake.notified() => {},
                }
                match self.pass().await {
                    Ok(report) => {
                        tracing::info!(target: "writer_reclaim", ?report, "writer reclaim pass complete");
                        #[cfg(test)]
                        let _ = reports_tx.send(report);
                    }
                    Err(error) => {
                        tracing::warn!(target: "writer_reclaim", %error, "writer reclaim scan failed; markers retained")
                    }
                }
            }
        });
        ReclaimTask {
            join,
            #[cfg(test)]
            reports,
        }
    }

    async fn pass(&self) -> Result<PassReport, String> {
        let directory = self.root.join("open-writers");
        let markers = spawn_blocking(JobKind::Recovery, move || {
            match std::fs::read_dir(&directory) {
                Ok(entries) => entries
                    .map(|entry| entry.map(|entry| entry.path()))
                    .collect::<std::io::Result<Vec<_>>>(),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
                Err(error) => Err(error),
            }
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        let mut report = PassReport::default();
        for path in markers {
            if path.extension().is_some_and(|extension| extension == "tmp") {
                continue;
            }
            let read_path = path.clone();
            let candidate = spawn_blocking(JobKind::Recovery, move || read_marker(&read_path))
                .await
                .map_err(|error| error.to_string())?;
            let identity = match candidate {
                Ok(Some(identity)) => identity,
                Ok(None) => {
                    report.vanished += 1;
                    continue;
                }
                Err(error) => {
                    report.invalid += 1;
                    tracing::warn!(target: "writer_reclaim", path = %path.display(), %error, "invalid writer marker retained");
                    continue;
                }
            };
            match self.reclaim(path.clone(), identity).await {
                Ok(Reclaimed::Closed) => report.reclaimed += 1,
                Ok(Reclaimed::Live) => report.live += 1,
                Ok(Reclaimed::Vanished) => report.vanished += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(target: "writer_reclaim", path = %path.display(), %error, "writer reclaim failed; marker retained for retry");
                }
            }
        }
        Ok(report)
    }

    async fn reclaim(&self, marker: PathBuf, identity: BindIdentity) -> Result<Reclaimed, String> {
        let root = self.root.clone();
        let session = identity.clone();
        let acquired = spawn_blocking(JobKind::Recovery, move || {
            WalLeaseDomain::new(root.join("leases")).acquire(&session)
        })
        .await
        .map_err(|error| error.to_string())?;
        let tenure = match acquired {
            Ok(tenure) => tenure,
            Err(LeaseError::Held { .. }) => return Ok(Reclaimed::Live),
            Err(error) => return Err(error.to_string()),
        };
        self.reclaim_acquired(marker, identity, tenure).await
    }

    async fn reclaim_acquired(
        &self,
        marker: PathBuf,
        identity: BindIdentity,
        tenure: broca_core::lease::OwnedWalTenure,
    ) -> Result<Reclaimed, String> {
        // Reopening a missing log must not create an empty conversation under an old key.
        // The marker can precede admission, but the WAL itself is created before the marker.
        let check_path = marker.clone();
        let check_identity = identity.clone();
        let store = self.store.clone();
        let checked = spawn_blocking(JobKind::Recovery, move || -> Result<Option<()>, String> {
            // A clean close may have removed the marker since discovery. Re-read it
            // under the lease before doing recovery or creating a successor.
            let Some(found) = read_marker(&check_path)? else {
                return Ok(None);
            };
            if found != check_identity {
                return Err("writer marker identity changed after discovery".into());
            }
            store
                .check_move(&check_identity, true)
                .map_err(|error| error.to_string())?;
            if store.wal_present(&check_identity) == Some(false) {
                return Err("writer marker names a missing WAL".into());
            }
            Ok(Some(()))
        })
        .await
        .map_err(|error| error.to_string())??;
        if checked.is_none() {
            return Ok(Reclaimed::Vanished);
        }
        #[cfg(all(test, feature = "test-clock"))]
        let writer = if let Some(fs) = &self.fs {
            WalHandle::open_and_spawn_owned_for_reclaim_with_fs_for_test(
                self.store.clone(),
                identity,
                tenure,
                fs.clone(),
            )
            .await
        } else {
            WalHandle::open_and_spawn_owned_for_reclaim(self.store.clone(), identity, tenure).await
        };
        #[cfg(not(all(test, feature = "test-clock")))]
        let writer =
            WalHandle::open_and_spawn_owned_for_reclaim(self.store.clone(), identity, tenure).await;
        let writer = writer.map_err(|error| error.to_string())?;
        let (closed, tenure) = writer.shutdown_into_tenure().await;
        closed.map_err(|error| error.to_string())?;
        // Keep the lease through unlink, including if the awaiting supervisor is cancelled.
        // A resurrected marker merely causes another harmless open/close on the next pass.
        #[cfg(all(test, feature = "test-clock"))]
        let fs = self.fs.clone();
        spawn_blocking(JobKind::Recovery, move || {
            let _tenure = tenure;
            let remove = || match broca_wal::fs_ops::remove_file(&marker) {
                Ok(()) => Ok(()),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
                Err(error) => Err(error),
            };
            #[cfg(all(test, feature = "test-clock"))]
            if let Some(fs) = fs {
                return broca_wal::fs_ops::with_fs_ops(fs, remove);
            }
            remove()
        })
        .await
        .map_err(|error| error.to_string())?
        .map_err(|error| error.to_string())?;
        Ok(Reclaimed::Closed)
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Reclaimed {
    Closed,
    Live,
    Vanished,
}

fn read_marker(path: &Path) -> Result<Option<BindIdentity>, String> {
    use std::io::Read;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("non-UTF-8 writer marker name")?;
    if !valid_marker_name(name) {
        return Err("invalid writer marker address or writer id".into());
    }
    let (address, _) = name.split_once('.').expect("validated marker name");
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    if !metadata.file_type().is_file() {
        return Err("writer marker is not a regular file".into());
    }
    let mut bytes = Vec::new();
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.to_string()),
    };
    file.take(MARKER_BYTES_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MARKER_BYTES_LIMIT {
        return Err("writer marker exceeds the size limit".into());
    }
    let identity = decode_writer_marker(&bytes)?;
    if session_addr(&identity) != address {
        return Err("writer marker identity does not match its address".into());
    }
    Ok(Some(identity))
}

fn valid_marker_name(name: &str) -> bool {
    let Some((address, writer)) = name.split_once('.') else {
        return false;
    };
    let hex = |text: &str| {
        text.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    address.len() == 16 && writer.len() == 32 && hex(address) && hex(writer)
}

#[cfg(all(test, feature = "test-clock", unix))]
#[path = "../../broca-crash-harness/src/power_loss.rs"]
mod power_loss;

#[cfg(test)]
mod tests {
    use super::*;
    use broca_wal::WalRecord;
    use std::io::{BufRead, Write};
    use std::process::{Child, Command, Stdio};
    use std::sync::atomic::{AtomicU64, Ordering};

    struct Root {
        path: PathBuf,
        store: Arc<FileWalStore>,
        identity: BindIdentity,
    }

    impl Root {
        fn new() -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let path = broca_test_support::scratch_root().join(format!(
                "writer-reclaim-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            std::fs::create_dir_all(&path).unwrap();
            Self::at(path)
        }

        fn at(path: PathBuf) -> Self {
            Self {
                store: Arc::new(FileWalStore::with_config(
                    path.join("wal"),
                    broca_wal::WalConfig::OVERHAUL,
                )),
                identity: BindIdentity::new("/writer-reclaim", "runner", "session"),
                path,
            }
        }

        fn reclaimer(&self) -> Reclaimer {
            Reclaimer {
                root: self.path.clone(),
                store: self.store.clone(),
                #[cfg(feature = "test-clock")]
                fs: None,
            }
        }

        fn acquire(&self) -> broca_core::lease::OwnedWalTenure {
            WalLeaseDomain::new(self.path.join("leases"))
                .acquire(&self.identity)
                .unwrap()
        }

        async fn writer(&self) -> WalHandle {
            WalHandle::open_and_spawn_owned(
                self.store.clone(),
                self.identity.clone(),
                self.acquire(),
                None,
            )
            .await
            .unwrap()
        }

        fn records(&self) -> Vec<broca_wal::FramedRecord> {
            broca_wal::replay_file(self.store.wal_path(&self.identity), u64::MAX).unwrap()
        }

        fn markers(&self) -> Vec<PathBuf> {
            std::fs::read_dir(self.path.join("open-writers"))
                .unwrap()
                .map(|entry| entry.unwrap().path())
                .collect()
        }

        fn child(&self, mode: &str) -> Command {
            let mut command = Command::new(std::env::current_exe().unwrap());
            command
                .args([
                    "--exact",
                    "actor::writer_reclaim::tests::reclaim_process_child",
                    "--nocapture",
                ])
                .env("BROCA_RECLAIM_CHILD_ROOT", &self.path)
                .env("BROCA_RECLAIM_CHILD_MODE", mode);
            command
        }

        fn live_child(&self) -> LiveChild {
            self.live_child_mode("writer")
        }

        fn live_child_mode(&self, mode: &str) -> LiveChild {
            let mut child = LiveChild(self.child(mode).stdout(Stdio::piped()).spawn().unwrap());
            let output = child.0.stdout.take().unwrap();
            let (tx, rx) = std::sync::mpsc::channel();
            std::thread::spawn(move || {
                for line in std::io::BufReader::new(output).lines() {
                    if line.unwrap() == "WRITER_READY" {
                        let _ = tx.send(());
                        return;
                    }
                }
            });
            rx.recv_timeout(Duration::from_secs(30))
                .expect("child opened a real owned writer");
            child
        }

        fn dead_writer(&self) -> ([u8; 16], PathBuf) {
            let mut child = self.live_child();
            child.0.kill().unwrap();
            assert!(!child.0.wait().unwrap().success());
            let records = self.records();
            let WalRecord::WriterOpened { writer_id, .. } = records.last().unwrap().record else {
                panic!("child is killed before any admission or close")
            };
            (writer_id, self.markers().pop().unwrap())
        }

        fn assert_superseded(&self, old: [u8; 16]) {
            let records = self.records();
            let opened = records.iter().find(|frame| matches!(
                &frame.record, WalRecord::WriterOpened { previous_writer_id: Some(previous), cause, .. }
                    if *previous == old && cause == "reclaim"
            )).expect("reclaim durably opened a successor without a send");
            let WalRecord::WriterOpened {
                recovered_through_seq,
                ..
            } = opened.record
            else {
                unreachable!()
            };
            assert_eq!(recovered_through_seq, opened.seq - 1);
            // A simulated crash may interrupt the first reclaim successor too. A later pass
            // can open another successor, so only the final writer must have WriterClosed.
            let final_writer = records
                .iter()
                .rev()
                .find_map(|frame| match frame.record {
                    WalRecord::WriterOpened { writer_id, .. } => Some(writer_id),
                    _ => None,
                })
                .unwrap();
            assert!(
                matches!(records.last().unwrap().record, WalRecord::WriterClosed { writer_id } if writer_id == final_writer)
            );
            assert_eq!(records.iter().filter(|frame| matches!(frame.record, WalRecord::WriterOpened { writer_id, .. } if writer_id == old)).count(), 1);
            assert!(!records
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::RunStarted { .. })));
        }
    }

    impl Drop for Root {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.path);
        }
    }

    struct LiveChild(Child);
    impl Drop for LiveChild {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    #[test]
    fn reclaim_process_child() {
        let Some(path) = std::env::var_os("BROCA_RECLAIM_CHILD_ROOT") else {
            return;
        };
        let root = Root::at(path.into());
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let mode = std::env::var("BROCA_RECLAIM_CHILD_MODE").unwrap();
            if mode == "writer" || mode == "orphan-writer" {
                let writer = root.writer().await;
                if mode == "orphan-writer" {
                    writer
                        .record(orphan_started(&root.identity))
                        .into_result()
                        .unwrap();
                    writer.sync().await.unwrap();
                }
                println!("WRITER_READY");
                std::io::stdout().flush().unwrap();
                std::future::pending::<()>().await;
            } else {
                assert_eq!(
                    root.reclaimer().pass().await.unwrap(),
                    PassReport {
                        live: 1,
                        ..Default::default()
                    }
                );
            }
        });
        // The child shares its parent's state root; only the parent owns cleanup.
        std::mem::forget(root);
    }

    #[tokio::test]
    async fn reclaim_after_process_kill() {
        let root = Root::new();
        let (old, marker) = root.dead_writer();
        assert_eq!(
            root.records().len(),
            1,
            "pre-admission log holds only lifecycle records"
        );
        assert_eq!(read_marker(&marker).unwrap(), Some(root.identity.clone()));
        assert_eq!(
            root.reclaimer().pass().await.unwrap(),
            PassReport {
                reclaimed: 1,
                ..Default::default()
            }
        );
        root.assert_superseded(old);
        assert!(root.markers().is_empty());
        let count = root.records().len();
        assert_eq!(
            root.reclaimer().pass().await.unwrap(),
            PassReport::default()
        );
        assert_eq!(
            root.records().len(),
            count,
            "repeated reclaim is a no-op after cleanup"
        );
    }

    #[tokio::test]
    async fn surviving_marker_beside_its_own_durable_close_gets_a_successor() {
        let root = Root::new();
        let writer = root.writer().await;
        let marker = root.markers().pop().unwrap();
        let bytes = std::fs::read(&marker).unwrap();
        let WalRecord::WriterOpened { writer_id: old, .. } = root.records()[0].record else {
            panic!("opened writer")
        };
        writer.shutdown().await.unwrap();
        assert!(
            matches!(root.records().last().unwrap().record, WalRecord::WriterClosed { writer_id } if writer_id == old)
        );
        // Model a close whose bytes reached disk but whose durable-head update for
        // readers failed: the marker remains, and readers need a newer WriterOpened.
        std::fs::write(&marker, bytes).unwrap();
        assert_eq!(
            root.reclaimer().pass().await.unwrap(),
            PassReport {
                reclaimed: 1,
                ..Default::default()
            }
        );
        root.assert_superseded(old);
        assert_eq!(root.records().len(), 4);
        assert!(root.markers().is_empty());
    }

    fn orphan_started(identity: &BindIdentity) -> WalRecord {
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
            ts_ms: None,
            run_id: broca_protocol::RunId("orphan".into()),
            session: identity.clone(),
            config: Box::new(super::super::tests::min_config()),
            input: super::super::tests::text_req(identity, "orphan")
                .input
                .into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        }
    }

    #[tokio::test]
    async fn reclaim_waits_for_boot_orphan_recovery_and_seals_once() {
        use broca_core::RunIndex;
        struct NeverExecutes;
        #[async_trait::async_trait]
        impl crate::executor::RunExecutor for NeverExecutes {
            async fn execute(
                &self,
                _: crate::executor::RunJob,
                _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
            ) -> Result<broca_core::RunSummary, broca_core::RunError> {
                panic!("crash-orphan recovery must seal without executing a provider");
            }
        }
        let root = Root::new();
        let mut child = root.live_child_mode("orphan-writer");
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let index = Arc::new(broca_store::SqliteStore::open(root.path.join("runs.db")).unwrap());
        index
            .index_run_active(&broca_core::RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id: "orphan".into(),
                session: root.identity.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: broca_core::RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .unwrap();
        let opener = crate::DefaultOpener::with_file_leases(
            root.path.join("leases"),
            root.store.clone(),
            Arc::new(NeverExecutes),
        )
        .with_lease_domain(WalLeaseDomain::new(root.path.join("leases")))
        .unwrap()
        .with_idle_ttl(Duration::from_secs(3600))
        .with_run_index(index);
        let registry =
            crate::LineageRegistry::new_with_reader(Arc::new(opener), root.store.clone(), None);
        let before = std::fs::read(root.store.wal_path(&root.identity)).unwrap();
        let (release, wait) = tokio::sync::oneshot::channel();
        let boot_registry = registry.clone();
        let store = root.store.clone();
        let identity = root.identity.clone();
        let mut task = spawn_after_boot(root.store.clone(), async move {
            wait.await.map_err(|error| error.to_string())?;
            let started = std::fs::metadata(store.wal_path(&identity))
                .unwrap()
                .modified()
                .unwrap()
                + Duration::from_secs(1);
            let marked = boot_registry.mark_crash_orphans(
                store.as_ref(),
                vec![(broca_protocol::RunId("orphan".into()), identity)],
                started,
            );
            assert_eq!(
                marked.marked, 1,
                "the stale writer also has a crash-orphan mark"
            );
            let sealed = boot_registry.seal_marked_crash_orphans(marked).await;
            assert_eq!(sealed.sealed, 1);
            Ok(())
        })
        .unwrap();
        wake_after_teardown(root.store.as_ref());
        assert!(
            tokio::time::timeout(Duration::from_millis(50), task.reports.recv())
                .await
                .is_err(),
            "neither startup nor a wake may bypass boot recovery"
        );
        assert_eq!(
            std::fs::read(root.store.wal_path(&root.identity)).unwrap(),
            before
        );
        release.send(()).unwrap();
        let report = tokio::time::timeout(Duration::from_secs(10), task.reports.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            report.live, 2,
            "the sealer's owned lease excludes reclaim of both its own and its predecessor's markers"
        );
        drop(task);
        assert!(registry.stop_and_seal().await);
        assert!(
            root.markers().is_empty(),
            "the published boot successor closes and retires its predecessor marker"
        );
        assert_eq!(
            root.reclaimer().pass().await.unwrap(),
            PassReport::default()
        );
        let records = root.records();
        assert_eq!(
            records
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::WriterOpened { .. }))
                .count(),
            2,
            "the boot sealer opens exactly one successor"
        );
        assert_eq!(
            records
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::RunFinished { .. }))
                .count(),
            1,
            "the orphan is sealed exactly once"
        );
    }

    #[tokio::test]
    async fn reclaim_skips_live_lease() {
        let root = Root::new();
        let writer = root.writer().await;
        let bytes = std::fs::read(root.store.wal_path(&root.identity)).unwrap();
        assert_eq!(
            root.reclaimer().pass().await.unwrap(),
            PassReport {
                live: 1,
                ..Default::default()
            }
        );
        assert_eq!(
            std::fs::read(root.store.wal_path(&root.identity)).unwrap(),
            bytes
        );
        assert_eq!(root.markers().len(), 1);
        writer.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn reclaim_rechecks_a_marker_removed_by_clean_close() {
        let root = Root::new();
        let writer = root.writer().await;
        let marker = root.markers().pop().unwrap();
        let identity = read_marker(&marker).unwrap().unwrap();
        writer.shutdown().await.unwrap();
        let before = root.records().len();
        assert_eq!(
            root.reclaimer()
                .reclaim(marker.clone(), identity)
                .await
                .unwrap(),
            Reclaimed::Vanished
        );
        assert_eq!(
            read_marker(&marker).unwrap(),
            None,
            "a vanished scan entry is benign"
        );
        assert_eq!(
            root.records().len(),
            before,
            "clean close must not cause a redundant successor"
        );
    }

    #[tokio::test]
    async fn reclaim_refuses_frozen_and_moved_sessions() {
        use broca_wal::migration::{self, Freeze, Moved};
        for moved in [false, true] {
            let root = Root::new();
            let (_, marker) = root.dead_writer();
            let wal = root.store.wal_path(&root.identity);
            if moved {
                migration::atomic_json(
                    &root.path,
                    &wal.with_extension("moved"),
                    &Moved {
                        cut_id: "move-reclaim".into(),
                        holder: "recipient".into(),
                        lineage_id: "lineage".into(),
                    },
                )
                .unwrap();
            } else {
                migration::freeze(
                    &root.path,
                    &wal,
                    &Freeze {
                        cut_id: "move-reclaim".into(),
                        end_offset: std::fs::metadata(&wal).unwrap().len(),
                        lineage_id: "lineage".into(),
                    },
                )
                .unwrap()
                .unwrap();
            }
            let before = std::fs::read(&wal).unwrap();
            assert_eq!(
                root.reclaimer().pass().await.unwrap(),
                PassReport {
                    failed: 1,
                    ..Default::default()
                }
            );
            assert!(
                marker.exists(),
                "move refusal leaves the writer marker for retry"
            );
            assert_eq!(std::fs::read(&wal).unwrap(), before);
        }
    }

    #[tokio::test]
    async fn move_recipient_guard_excludes_reclaim_until_install_finishes() {
        use crate::cut_state::CutArtifacts;
        use crate::move_files::{
            FileCutArtifacts, MoveDriver, MoveObservation, MovePlans, PlannedSession,
        };
        use broca_wire::agent_data::{errors::ErrorBody, refs::Refs, Op};
        struct NoActor;
        #[async_trait::async_trait]
        impl crate::registry::LineageOpener for NoActor {
            async fn open(
                &self,
                _: &crate::registry::LineageKey,
                _: u64,
                _: crate::registry::RegistryHandle,
            ) -> Result<crate::registry::ActorHandle, crate::registry::OpenError> {
                panic!("a move recipient guard must not open a session actor");
            }
        }
        struct Plan(PlannedSession);
        #[async_trait::async_trait]
        impl MoveDriver for Plan {
            async fn plan(&self, _: &str, _: &Refs, _: Op) -> Result<MovePlans, ErrorBody> {
                Ok(vec![self.0.clone()].into())
            }
            async fn cover(&self, _: &str) -> Result<u64, ErrorBody> {
                unreachable!()
            }
            async fn export(&self, _: &str, _: &[u8]) -> Result<(), ErrorBody> {
                unreachable!()
            }
            fn reply(
                &self,
                _: Op,
                _: &[PlannedSession],
                _: &[u64],
                _: &[MoveObservation],
            ) -> Result<serde_json::Value, ErrorBody> {
                unreachable!()
            }
            fn drop_projection(&self, _: &BindIdentity) -> Result<(), String> {
                unreachable!()
            }
        }
        let root = Root::new();
        let (old, marker) = root.dead_writer();
        let bytes = std::fs::read(root.store.wal_path(&root.identity)).unwrap();
        let reference = broca_wire::agent_data::session_ref(
            &root.identity.harness,
            &root.identity.project_root.to_string_lossy(),
            &root.identity.session,
        );
        let plan = PlannedSession {
            reference: reference.clone(),
            session: root.identity.clone(),
            end_offset: bytes.len() as u64,
            digest: broca_wal::migration::digest(&bytes),
            sealed_digest: Some(broca_wal::migration::digest(&bytes)),
            bytes: bytes.clone(),
            format: "broca-wal/v4;features=writer-id/v1".into(),
            lineage_id: "lineage".into(),
            local_archive: None,
            divergence: None,
            divergence_reason: None,
        };
        let registry =
            crate::LineageRegistry::new_with_reader(Arc::new(NoActor), root.store.clone(), None);
        let export = Arc::new(broca_store::SqliteStore::open(root.path.join("move.db")).unwrap());
        let artifacts = FileCutArtifacts::new(
            root.path.clone(),
            registry,
            root.store.clone(),
            export,
            Arc::new(Plan(plan)),
        );
        let refs: Refs = serde_json::from_value(
            serde_json::json!([{"ref_kind":"broca-session", "ref":reference}]),
        )
        .unwrap();
        // This is the actual import guard retained across staging, install and publication.
        let install_guard = artifacts
            .lock_refs("0123456789abcdef0123456789abcdef", &refs, Op::Import)
            .await
            .unwrap();
        assert_eq!(
            root.reclaimer().pass().await.unwrap(),
            PassReport {
                live: 1,
                ..Default::default()
            }
        );
        assert!(marker.exists());
        assert_eq!(
            std::fs::read(root.store.wal_path(&root.identity)).unwrap(),
            bytes
        );
        drop(install_guard);
        assert_eq!(root.reclaimer().pass().await.unwrap().reclaimed, 1);
        root.assert_superseded(old);
    }

    #[tokio::test]
    async fn boot_reaps_temporary_marker_names_without_hiding_them_from_convergence() {
        let root = Root::new();
        let writer = root.writer().await;
        let marker = root.markers().pop().unwrap();
        let temporary = marker.with_extension(format!(
            "{}.tmp",
            marker.extension().unwrap().to_str().unwrap()
        ));
        std::fs::write(&temporary, b"{torn marker").unwrap();
        assert_eq!(
            root.markers().len(),
            2,
            "convergence includes temporary files"
        );
        assert_eq!(
            reap_temporary_markers_at_boot(root.store.clone())
                .await
                .unwrap(),
            1
        );
        assert_eq!(root.markers(), [marker]);
        writer.shutdown().await.unwrap();
        assert!(root.markers().is_empty());
        assert_eq!(
            reap_temporary_markers_at_boot(root.store.clone())
                .await
                .unwrap(),
            0
        );
    }

    #[tokio::test]
    async fn reclaim_supersedes_dead_writer_after_poison() {
        struct HoldWriter(tokio::sync::mpsc::UnboundedSender<Weak<WalHandle>>);
        #[async_trait::async_trait]
        impl crate::executor::RunExecutor for HoldWriter {
            async fn execute(
                &self,
                job: crate::executor::RunJob,
                _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
            ) -> Result<broca_core::RunSummary, broca_core::RunError> {
                self.0.send(Arc::downgrade(&job.writer)).unwrap();
                if let Some(admitted) = job.on_admitted {
                    let _ = admitted.send(Ok(job.req.run_id));
                }
                std::future::pending().await
            }
        }

        let root = Root::new();
        let (tx, mut writers) = tokio::sync::mpsc::unbounded_channel();
        let opener = crate::DefaultOpener::with_file_leases(
            root.path.join("leases"),
            root.store.clone(),
            Arc::new(HoldWriter(tx)),
        )
        .with_lease_domain(WalLeaseDomain::new(root.path.join("leases")))
        .unwrap()
        .with_idle_ttl(Duration::from_secs(3600));
        let registry =
            crate::LineageRegistry::new_with_reader(Arc::new(opener), root.store.clone(), None);
        let actor = registry.actor_for(&root.identity).await.unwrap();
        let mut task = spawn(root.store.clone()).unwrap();
        let report = tokio::time::timeout(Duration::from_secs(10), task.reports.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.live, 1, "startup sees the actor's real owned lease");
        let request = super::super::tests::text_req(&root.identity, "poison-run");
        assert!(matches!(
            registry
                .route_send(
                    &root.identity,
                    request,
                    crate::executor::RunKind::FreshOrResume
                )
                .await
                .unwrap(),
            super::super::SendOutcome::Active { .. }
        ));
        let writer = writers.recv().await.unwrap().upgrade().unwrap();
        let WalRecord::WriterOpened { writer_id: old, .. } = root.records()[0].record else {
            panic!("initial writer")
        };
        // FileWal's foreign-write guard turns an unexpected length change into poison.
        // The single torn byte is discarded by the successor's normal recovery path.
        std::fs::OpenOptions::new()
            .append(true)
            .open(root.store.wal_path(&root.identity))
            .unwrap()
            .write_all(&[0xff])
            .unwrap();
        writer
            .record(WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1),
            })
            .into_result()
            .unwrap();
        assert!(writer.sync().await.is_err());
        drop(writer);
        let (reply, status) = tokio::sync::oneshot::channel();
        assert!(actor
            .send(super::super::LineageCmd::Status { reply })
            .await
            .is_ok());
        assert!(matches!(
            status.await.unwrap(),
            super::super::LineageStatus::Closing
        ));
        // No send reopens this log. Only teardown's wake can run before the five-minute tick.
        let report = tokio::time::timeout(Duration::from_secs(10), task.reports.recv())
            .await
            .expect("poison teardown must wake reclaim immediately")
            .unwrap();
        assert_eq!(
            report,
            PassReport {
                reclaimed: 1,
                ..Default::default()
            }
        );
        root.assert_superseded(old);
        assert!(root.markers().is_empty());
        assert_eq!(registry.slot_counts().open, 0);
        drop(task);
    }

    #[tokio::test]
    async fn two_daemons_never_reclaim_live_writer() {
        let root = Root::new();
        let _writer_process = root.live_child();
        let bytes = std::fs::read(root.store.wal_path(&root.identity)).unwrap();
        let mut first = root.child("reclaim").spawn().unwrap();
        let mut second = root.child("reclaim").spawn().unwrap();
        assert!(first.wait().unwrap().success());
        assert!(second.wait().unwrap().success());
        assert_eq!(
            std::fs::read(root.store.wal_path(&root.identity)).unwrap(),
            bytes
        );
        assert_eq!(root.markers().len(), 1);
    }

    #[tokio::test]
    async fn reclaim_refuses_malformed_and_mismatched_marker_identities() {
        let root = Root::new();
        let (old, marker) = root.dead_writer();
        let valid = std::fs::read(&marker).unwrap();
        let before = std::fs::read(root.store.wal_path(&root.identity)).unwrap();
        let mut wrong: serde_json::Value = serde_json::from_slice(&valid).unwrap();
        wrong["identity"]["session"] = "another-session".into();
        for bytes in [
            b"{".to_vec(),
            serde_json::to_vec(&wrong).unwrap(),
            valid
                .iter()
                .copied()
                .chain(std::iter::repeat_n(b' ', MARKER_BYTES_LIMIT as usize))
                .collect(),
        ] {
            std::fs::write(&marker, bytes).unwrap();
            assert_eq!(
                root.reclaimer().pass().await.unwrap(),
                PassReport {
                    invalid: 1,
                    ..Default::default()
                }
            );
            assert!(marker.exists());
            assert_eq!(
                std::fs::read(root.store.wal_path(&root.identity)).unwrap(),
                before
            );
            assert_eq!(
                std::fs::read_dir(root.path.join("leases")).unwrap().count(),
                1,
                "no guessed identity lease was created"
            );
        }
        std::fs::write(&marker, valid).unwrap();
        assert_eq!(root.reclaimer().pass().await.unwrap().reclaimed, 1);
        root.assert_superseded(old);
    }

    #[tokio::test]
    async fn reclaim_runs_at_startup_and_on_periodic_pass_without_a_send() {
        let root = Root::new();
        let (old, _) = root.dead_writer();
        let mut task = root.reclaimer().spawn(Duration::from_millis(100));
        let report = tokio::time::timeout(Duration::from_secs(10), task.reports.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.reclaimed, 1);
        root.assert_superseded(old);
        let (next, _) = root.dead_writer();
        loop {
            let report = tokio::time::timeout(Duration::from_secs(10), task.reports.recv())
                .await
                .unwrap()
                .unwrap();
            if report.reclaimed != 0 {
                break;
            }
        }
        // The child reopened the same log, so its predecessor is already a closed reclaim writer.
        let records = root.records();
        assert!(records.iter().any(|frame| matches!(frame.record, WalRecord::WriterOpened { previous_writer_id: Some(previous), .. } if previous == next)));
        drop(task);
    }

    #[test]
    fn legacy_store_starts_no_reclaimer() {
        let root = Root::new();
        let store = Arc::new(FileWalStore::new(root.path.join("wal")));
        assert!(spawn(store).is_none());
        assert!(!root.path.join("open-writers").exists());
    }

    #[cfg(all(feature = "test-clock", unix))]
    #[tokio::test]
    async fn reclaim_crash_and_power_loss_cuts_are_idempotent() {
        use super::power_loss::{Family, RecordingFs};

        let root = Root::new();
        let (old, marker) = root.dead_writer();
        // Lease epochs are outside the WAL filesystem model. Acquire before recording,
        // then exercise the same owned reclaim body while the real OS lock stays held.
        let tenure = root.acquire();
        let fs = Arc::new(RecordingFs::new(&root.path).unwrap());
        let mut reclaimer = root.reclaimer();
        reclaimer.fs = Some(fs.clone());
        reclaimer
            .reclaim_acquired(marker.clone(), root.identity.clone(), tenure)
            .await
            .unwrap();
        let states = fs.enumerate(100_000).unwrap();
        assert!(states
            .iter()
            .any(|state| state.family == Family::SyncedOnly));
        assert!(states
            .iter()
            .any(|state| state.family == Family::PartialPersistence));
        assert!(states.iter().any(|state| !state
            .state
            .files
            .contains_key(&PathBuf::from("open-writers").join(marker.file_name().unwrap()))));
        assert_eq!(
            states.len(),
            432,
            "all reclaim crash/power-loss images must be exercised"
        );
        for state in states {
            let recovery = Root::new();
            std::fs::remove_dir_all(&recovery.path).unwrap();
            state.state.install(&recovery.path).unwrap();
            reap_temporary_markers_at_boot(recovery.store.clone())
                .await
                .unwrap();
            let old_marker = recovery
                .path
                .join("open-writers")
                .join(marker.file_name().unwrap());
            if !old_marker.exists() {
                recovery.assert_superseded(old);
            }
            let report = recovery.reclaimer().pass().await.unwrap();
            assert_eq!(report.failed, 0, "cut {} {:?}", state.cut, state.family);
            assert_eq!(report.invalid, 0, "published marker payload must be atomic");
            recovery.assert_superseded(old);
            assert!(recovery.markers().is_empty());
            let count = recovery.records().len();
            assert_eq!(
                recovery.reclaimer().pass().await.unwrap(),
                PassReport::default()
            );
            assert_eq!(
                recovery.records().len(),
                count,
                "retry after cut {} is idempotent",
                state.cut
            );
        }
    }
}
