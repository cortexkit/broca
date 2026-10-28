use super::*;
use broca_core::{FileWalStore, WalHandle, WalStore};
use broca_wal::fs_ops::{FileOps, FsOps, OpenOptions, ReadDir};
use std::fs::Metadata;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::sync::atomic::AtomicBool;

struct FaultFs {
    inner: Arc<RecordingFs>,
    log: PathBuf,
    fail_sync: Arc<AtomicBool>,
    fail_unlink: AtomicBool,
    unlink_attempted: AtomicBool,
}

impl FsOps for FaultFs {
    fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
        let inner = self.inner.open(path, options)?;
        if path == self.log {
            Ok(Box::new(FaultFile {
                inner,
                fail_sync: self.fail_sync.clone(),
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
        self.inner.rename(from, to)
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
        self.inner.sync_all()
    }
    fn try_clone(&self) -> io::Result<Box<dyn FileOps>> {
        Ok(Box::new(Self {
            inner: self.inner.try_clone()?,
            fail_sync: self.fail_sync.clone(),
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

fn spawn(root: &Scratch) -> (WalHandle, Arc<FaultFs>) {
    let store = FileWalStore::with_config(root.0.join("wal"), broca_wal::WalConfig::OVERHAUL);
    let session = broca_protocol::BindIdentity::new("/writer-id-fault", "runner", "session");
    let fs = Arc::new(FaultFs {
        inner: Arc::new(RecordingFs::new(&root.0).unwrap()),
        log: store.wal_path(&session),
        fail_sync: Arc::new(AtomicBool::new(false)),
        fail_unlink: AtomicBool::new(false),
        unlink_attempted: AtomicBool::new(false),
    });
    let handle = with_fs_ops(fs.clone(), || {
        let wal = store.open(&session).unwrap();
        WalHandle::spawn_in_store_with_fs_for_test(wal, &store, &session, 1, None, fs.clone())
            .unwrap()
    });
    (handle, fs)
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

fn recover_image(root: &Scratch, fs: &FaultFs) -> (Vec<broca_wal::FramedRecord>, bool) {
    let recovered = Scratch::new();
    recovered.clear();
    synced_image(&fs.inner).install(&recovered.0).unwrap();
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
    let (handle, fs) = spawn(&root);
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
    let (records, marker_kept) = recover_image(&root, &fs);
    assert_eq!(records.len(), 1);
    assert!(matches!(records[0].record, WalRecord::WriterOpened { .. }));
    assert!(marker_kept);
}

#[tokio::test]
async fn real_handle_unlink_failure_preserves_durable_close_and_marker_for_reclaim() {
    let root = Scratch::new();
    let (handle, fs) = spawn(&root);
    let durable_head = handle.durable_head();
    fs.fail_unlink.store(true, Ordering::SeqCst);
    handle
        .shutdown()
        .await
        .expect("a durable close is authoritative despite cleanup failure");
    assert!(fs.unlink_attempted.load(Ordering::SeqCst));
    assert_eq!(durable_head.load(Ordering::Acquire), 2);
    let (records, marker_kept) = recover_image(&root, &fs);
    assert_eq!(records.len(), 2);
    assert!(matches!(records[1].record, WalRecord::WriterClosed { .. }));
    assert!(marker_kept);
}
