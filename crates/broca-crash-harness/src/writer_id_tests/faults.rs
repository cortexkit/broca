use super::*;
use crate::proof_tests::batch_recording::BatchFs;
use broca_core::{FileWalStore, WalHandle, WalStore};
use broca_wal::fs_ops::{FileOps, FsOps, OpenOptions, ReadDir};
use std::fs::Metadata;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::{atomic::AtomicBool, Mutex};

struct FaultFs {
    inner: Arc<BatchFs>,
    log: PathBuf,
    fail_sync: Arc<AtomicBool>,
    fail_sync_after: Option<Arc<Mutex<usize>>>,
    fail_manifest_dir_sync: bool,
    manifest_renamed: AtomicBool,
    fail_unlink: AtomicBool,
    unlink_attempted: AtomicBool,
}

/// Reuse the log-sync injector for a repair whose resize must remain observable
/// after the failed barrier. The caller clears the flag before retrying.
pub(crate) fn recovery_sync_fault(
    root: &Path,
    log: PathBuf,
) -> (Arc<dyn FsOps>, Arc<AtomicBool>, Arc<RecordingFs>) {
    let inner = Arc::new(BatchFs::new());
    let recording = inner.begin(root);
    let fail_sync = Arc::new(AtomicBool::new(true));
    let fs = Arc::new(FaultFs {
        inner,
        log,
        fail_sync: fail_sync.clone(),
        fail_sync_after: None,
        fail_manifest_dir_sync: false,
        manifest_renamed: AtomicBool::new(false),
        fail_unlink: AtomicBool::new(false),
        unlink_attempted: AtomicBool::new(false),
    });
    (fs, fail_sync, recording)
}

/// The first log barrier protects truncation; fail the next one after marker
/// bytes are visible, so retry must not mistake visibility for durability.
pub(crate) fn recovery_marker_sync_fault(
    root: &Path,
    log: PathBuf,
) -> (Arc<dyn FsOps>, Arc<RecordingFs>) {
    let inner = Arc::new(BatchFs::new());
    let recording = inner.begin(root);
    let fs = Arc::new(FaultFs {
        inner,
        log,
        fail_sync: Arc::new(AtomicBool::new(false)),
        fail_sync_after: Some(Arc::new(Mutex::new(2))),
        fail_manifest_dir_sync: false,
        manifest_renamed: AtomicBool::new(false),
        fail_unlink: AtomicBool::new(false),
        unlink_attempted: AtomicBool::new(false),
    });
    (fs, recording)
}

pub(crate) fn recovery_manifest_dir_sync_fault(
    root: &Path,
    log: PathBuf,
) -> (Arc<dyn FsOps>, Arc<RecordingFs>) {
    let inner = Arc::new(BatchFs::new());
    let recording = inner.begin(root);
    let fs = Arc::new(FaultFs {
        inner,
        log,
        fail_sync: Arc::new(AtomicBool::new(false)),
        fail_sync_after: None,
        fail_manifest_dir_sync: true,
        manifest_renamed: AtomicBool::new(false),
        fail_unlink: AtomicBool::new(false),
        unlink_attempted: AtomicBool::new(false),
    });
    (fs, recording)
}

impl FsOps for FaultFs {
    fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
        let inner = self.inner.open(path, options)?;
        if path == self.log {
            Ok(Box::new(FaultFile {
                inner,
                fail_sync: self.fail_sync.clone(),
                fail_sync_after: self.fail_sync_after.clone(),
            }))
        } else {
            Ok(inner)
        }
    }
    fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        self.inner.metadata(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)?;
        if to.file_name().is_some_and(|name| {
            let name = name.to_string_lossy();
            name.contains(".cut-") && name.ends_with(".json")
        }) {
            self.manifest_renamed.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        if path
            .parent()
            .is_some_and(|parent| parent.ends_with("open-writers"))
        {
            self.unlink_attempted.store(true, Ordering::SeqCst);
            if self.fail_unlink.load(Ordering::SeqCst) {
                return Err(io::Error::other("injected writer marker unlink failure"));
            }
        }
        self.inner.remove_file(path)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        if self.fail_manifest_dir_sync && self.manifest_renamed.swap(false, Ordering::SeqCst) {
            return Err(io::Error::other("injected manifest directory sync failure"));
        }
        self.inner.sync_dir(path)
    }
    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir(path)
    }
    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.hard_link(from, to)
    }
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.inner.canonicalize(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
        self.inner.read_dir(path)
    }
    fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
        self.inner.validate_if_exists(path)
    }
}

struct FaultFile {
    inner: Box<dyn FileOps>,
    fail_sync: Arc<AtomicBool>,
    fail_sync_after: Option<Arc<Mutex<usize>>>,
}
impl Read for FaultFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.inner.read(bytes)
    }
}
impl Write for FaultFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl Seek for FaultFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.inner.seek(position)
    }
}
impl FileOps for FaultFile {
    fn metadata(&self) -> io::Result<Metadata> {
        self.inner.metadata()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_all(&self) -> io::Result<()> {
        if self.fail_sync.load(Ordering::SeqCst) {
            return Err(io::Error::other("injected writer close sync failure"));
        }
        if self.fail_sync_after.as_ref().is_some_and(|remaining| {
            let mut remaining = remaining.lock().unwrap();
            let fail = *remaining == 1;
            *remaining = remaining.saturating_sub(1);
            fail
        }) {
            return Err(io::Error::other("injected recovery marker sync failure"));
        }
        self.inner.sync_all()
    }
    fn try_clone(&self) -> io::Result<Box<dyn FileOps>> {
        Ok(Box::new(Self {
            inner: self.inner.try_clone()?,
            fail_sync: self.fail_sync.clone(),
            fail_sync_after: self.fail_sync_after.clone(),
        }))
    }
    fn lock(&self) -> io::Result<()> {
        self.inner.lock()
    }
    fn unlock(&self) -> io::Result<()> {
        self.inner.unlock()
    }
    fn try_lock(&self) -> Result<(), std::fs::TryLockError> {
        self.inner.try_lock()
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.inner.read_exact_at(bytes, offset)
    }
}

fn spawn(root: &Scratch) -> (WalHandle, Arc<FaultFs>, Arc<RecordingFs>) {
    let store = FileWalStore::with_config(root.0.join("wal"), broca_wal::WalConfig::OVERHAUL);
    let session = broca_protocol::BindIdentity::new("/writer-id-fault", "runner", "session");
    let fs = Arc::new(FaultFs {
        inner: Arc::new(BatchFs::new()),
        log: store.wal_path(&session),
        fail_sync: Arc::new(AtomicBool::new(false)),
        fail_sync_after: None,
        fail_manifest_dir_sync: false,
        manifest_renamed: AtomicBool::new(false),
        fail_unlink: AtomicBool::new(false),
        unlink_attempted: AtomicBool::new(false),
    });
    let handle = with_fs_ops(fs.clone(), || {
        let wal = store.open(&session).unwrap();
        WalHandle::spawn_in_store_with_fs_for_test(wal, &store, &session, 1, None, fs.clone())
            .unwrap()
    });
    // Readiness proves the open prefix and its namespace durable. Journal only
    // the close under test, rebinding the production writer's existing handles.
    let recording = fs.inner.begin(&root.0);
    (handle, fs, recording)
}

fn synced_image(fs: &RecordingFs) -> State {
    let states = fs.enumerate(100_000).unwrap();
    let last = states.iter().map(|state| state.cut).max().unwrap();
    states
        .into_iter()
        .find(|state| state.cut == last && state.family == Family::SyncedOnly)
        .unwrap()
        .state
}

fn recover_image(
    root: &Scratch,
    fs: &FaultFs,
    recording: &RecordingFs,
) -> (Vec<broca_wal::FramedRecord>, bool) {
    let recovered = Scratch::new();
    recovered.clear();
    synced_image(recording).install(&recovered.0).unwrap();
    let relative_log = fs.log.strip_prefix(&root.0).unwrap();
    let wal = FileWal::open(recovered.0.join(relative_log)).unwrap();
    let records = wal.replay().unwrap();
    let marker_kept = std::fs::read_dir(recovered.0.join("open-writers"))
        .unwrap()
        .count()
        == 1;
    (records, marker_kept)
}

#[tokio::test]
async fn real_handle_failed_close_sync_publishes_no_close_and_keeps_marker_after_power_loss() {
    let root = Scratch::new();
    let (handle, fs, recording) = spawn(&root);
    let durable_head = handle.durable_head();
    assert_eq!(durable_head.load(Ordering::Acquire), 1);
    fs.fail_sync.store(true, Ordering::SeqCst);
    let error = handle.shutdown().await.expect_err("close sync must fail");
    assert!(error
        .to_string()
        .contains("injected writer close sync failure"));
    assert_eq!(
        durable_head.load(Ordering::Acquire),
        1,
        "failed close cannot publish a durable cursor"
    );
    assert!(!fs.unlink_attempted.load(Ordering::SeqCst));
    assert_eq!(
        std::fs::read_dir(root.0.join("open-writers"))
            .unwrap()
            .count(),
        1
    );
    // A failed sync may leave readable bytes. Only the synced-only crash image can assert absence
    // of a durable close, and it is recovered by the same FileWal opener as a real restart.
    let (records, marker_kept) = recover_image(&root, &fs, &recording);
    assert_eq!(records.len(), 1);
    assert!(matches!(records[0].record, WalRecord::WriterOpened { .. }));
    assert!(marker_kept);
}

#[tokio::test]
async fn real_handle_unlink_failure_preserves_durable_close_and_marker_for_reclaim() {
    let root = Scratch::new();
    let (handle, fs, recording) = spawn(&root);
    let durable_head = handle.durable_head();
    fs.fail_unlink.store(true, Ordering::SeqCst);
    handle
        .shutdown()
        .await
        .expect("a durable close is authoritative despite cleanup failure");
    assert!(fs.unlink_attempted.load(Ordering::SeqCst));
    assert_eq!(durable_head.load(Ordering::Acquire), 2);
    let (records, marker_kept) = recover_image(&root, &fs, &recording);
    assert_eq!(records.len(), 2);
    assert!(matches!(records[1].record, WalRecord::WriterClosed { .. }));
    assert!(marker_kept);
}
crate::require_process_kill!();
