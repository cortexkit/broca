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
    let root = store.writer_marker_root()?;
    Some(
        Reclaimer {
            store,
            root,
            #[cfg(all(test, feature = "test-clock"))]
            fs: None,
        }
        .spawn(PASS_INTERVAL),
    )
}

impl Reclaimer {
    fn spawn(self, period: Duration) -> ReclaimTask {
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
                Ok(identity) => identity,
                Err(error) => {
                    report.invalid += 1;
                    tracing::warn!(target: "writer_reclaim", path = %path.display(), %error, "invalid writer marker retained");
                    continue;
                }
            };
            match self.reclaim(path.clone(), identity).await {
                Ok(true) => report.reclaimed += 1,
                Ok(false) => report.live += 1,
                Err(error) => {
                    report.failed += 1;
                    tracing::warn!(target: "writer_reclaim", path = %path.display(), %error, "writer reclaim failed; marker retained for retry");
                }
            }
        }
        Ok(report)
    }

    async fn reclaim(&self, marker: PathBuf, identity: BindIdentity) -> Result<bool, String> {
        let root = self.root.clone();
        let session = identity.clone();
        let acquired = spawn_blocking(JobKind::Recovery, move || {
            WalLeaseDomain::new(root.join("leases")).acquire(&session)
        })
        .await
        .map_err(|error| error.to_string())?;
        let tenure = match acquired {
            Ok(tenure) => tenure,
            Err(LeaseError::Held { .. }) => return Ok(false),
            Err(error) => return Err(error.to_string()),
        };
        self.reclaim_acquired(marker, identity, tenure).await?;
        Ok(true)
    }

    async fn reclaim_acquired(
        &self,
        marker: PathBuf,
        identity: BindIdentity,
        tenure: broca_core::lease::OwnedWalTenure,
    ) -> Result<(), String> {
        // Reopening a missing log must not create an empty conversation under an old key.
        // The marker can precede admission, but the WAL itself is created before the marker.
        if self.store.wal_present(&identity) == Some(false) {
            return Err("writer marker names a missing WAL".into());
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
        Ok(())
    }
}

fn read_marker(path: &Path) -> Result<BindIdentity, String> {
    use std::io::Read;
    let name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or("non-UTF-8 writer marker name")?;
    let (address, writer) = name.split_once('.').ok_or("invalid writer marker name")?;
    let hex = |text: &str| {
        text.bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    };
    if address.len() != 16 || writer.len() != 32 || !hex(address) || !hex(writer) {
        return Err("invalid writer marker address or writer id".into());
    }
    if !std::fs::symlink_metadata(path)
        .map_err(|error| error.to_string())?
        .file_type()
        .is_file()
    {
        return Err("writer marker is not a regular file".into());
    }
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .map_err(|error| error.to_string())?
        .take(MARKER_BYTES_LIMIT + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > MARKER_BYTES_LIMIT {
        return Err("writer marker exceeds the size limit".into());
    }
    let identity = decode_writer_marker(&bytes)?;
    if session_addr(&identity) != address {
        return Err("writer marker identity does not match its address".into());
    }
    Ok(identity)
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
                .filter(|path| path.extension().is_none_or(|extension| extension != "tmp"))
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
            let mut child = LiveChild(self.child("writer").stdout(Stdio::piped()).spawn().unwrap());
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
            if std::env::var("BROCA_RECLAIM_CHILD_MODE").unwrap() == "writer" {
                let _writer = root.writer().await;
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
        assert_eq!(read_marker(&marker).unwrap(), root.identity);
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
        println!(
            "reclaim operation crash/power-loss images: {}",
            states.len()
        );
        for state in states {
            let recovery = Root::new();
            std::fs::remove_dir_all(&recovery.path).unwrap();
            state.state.install(&recovery.path).unwrap();
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
