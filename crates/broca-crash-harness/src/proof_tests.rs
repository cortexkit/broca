//! Power-loss cuts through durable-head publication and real FileWal recovery.

use crate::power_loss::RecordingFs;
#[cfg(feature = "writer-id-faults")]
use crate::power_loss::{Family, State};
use broca_wal::{fs_ops::with_fs_ops, head_proof::sidecar_path, FileWal, Wal};
#[cfg(feature = "writer-id-faults")]
use broca_wal::{
    head_proof::{HeadProof, PublishedLog},
    WalRecord,
};
use std::{
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
struct Probe {
    now: AtomicU64,
    stall_proof_writes: AtomicUsize,
    stall_log_syncs: AtomicUsize,
    head_lookups: AtomicUsize,
    head_renames: AtomicUsize,
    log_syncs: AtomicUsize,
    events: Mutex<Vec<(&'static str, PathBuf)>>,
    lookup_on_link: Mutex<Option<(Arc<broca_core::FileWalStore>, broca_protocol::BindIdentity)>>,
    link_lookup_checks: AtomicUsize,
    fail_directory_once: Mutex<Option<PathBuf>>,
}

struct ProbeFs {
    inner: Arc<dyn broca_wal::fs_ops::FsOps>,
    probe: Arc<Probe>,
}

#[cfg(feature = "writer-id-faults")]
pub(crate) mod batch_recording {
    use super::*;
    use broca_wal::fs_ops::{FileOps, FsOps, OpenOptions};

    type Backend = Arc<Mutex<(u64, Arc<dyn FsOps>)>>;
    type LiveHandles = Arc<Mutex<Vec<LiveHandle>>>;

    /// Start the crash history at a real writer's ready barrier. Existing file
    /// handles rebind at the same path/cursor with the same access options, but
    /// without create/create_new/truncate. Subsequent writer I/O goes through
    /// the unchanged RecordingFs without replaying file creation.
    pub(crate) struct BatchFs {
        backend: Backend,
        live: LiveHandles,
    }

    impl BatchFs {
        pub(crate) fn new() -> Self {
            Self {
                backend: Arc::new(Mutex::new((0, Arc::new(broca_wal::fs_ops::RealFs)))),
                live: Default::default(),
            }
        }
        pub(crate) fn begin(&self, root: &Path) -> Arc<RecordingFs> {
            let recording = Arc::new(RecordingFs::new(root).unwrap());
            let mut backend = self.backend.lock().unwrap();
            backend.0 += 1;
            backend.1 = recording.clone();
            recording
        }
        /// Relocation may remove an old pathname before the next handle call.
        /// Rebind live descriptors while the ready writer is still idle and
        /// their original names exist, without any file or directory sync.
        pub(super) fn begin_before_relocation(&self, root: &Path) -> Arc<RecordingFs> {
            let recording = self.begin(root);
            let generation = self.current().0;
            self.live.lock().unwrap().retain(|entry| {
                let Some(handle) = entry.handle.upgrade() else {
                    return false;
                };
                rebind(
                    &mut handle.lock().unwrap(),
                    generation,
                    recording.as_ref(),
                    &entry.path,
                    &entry.options,
                )
                .unwrap();
                true
            });
            recording
        }
        fn current(&self) -> (u64, Arc<dyn FsOps>) {
            self.backend.lock().unwrap().clone()
        }
    }

    struct Handle {
        generation: u64,
        file: Box<dyn FileOps>,
    }
    struct LiveHandle {
        path: PathBuf,
        options: OpenOptions,
        handle: std::sync::Weak<Mutex<Handle>>,
    }
    struct BatchFile {
        path: PathBuf,
        options: OpenOptions,
        backend: Backend,
        handle: Arc<Mutex<Handle>>,
        live: LiveHandles,
    }

    fn rebind(
        handle: &mut Handle,
        generation: u64,
        fs: &dyn FsOps,
        path: &Path,
        original: &OpenOptions,
    ) -> io::Result<()> {
        if handle.generation != generation {
            let cursor = handle.file.stream_position()?;
            let mut options = original.clone();
            options.create = false;
            options.create_new = false;
            options.truncate = false;
            let mut file = fs.open(path, &options)?;
            file.seek(SeekFrom::Start(cursor))?;
            *handle = Handle { generation, file };
        }
        Ok(())
    }

    impl BatchFile {
        fn rebound(&self) -> io::Result<std::sync::MutexGuard<'_, Handle>> {
            let (generation, fs) = self.backend.lock().unwrap().clone();
            let mut handle = self.handle.lock().unwrap();
            rebind(
                &mut handle,
                generation,
                fs.as_ref(),
                &self.path,
                &self.options,
            )?;
            Ok(handle)
        }
    }

    impl FsOps for BatchFs {
        fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
            let (generation, fs) = self.current();
            let handle = Arc::new(Mutex::new(Handle {
                generation,
                file: fs.open(path, options)?,
            }));
            self.live.lock().unwrap().push(LiveHandle {
                path: path.to_path_buf(),
                options: options.clone(),
                handle: Arc::downgrade(&handle),
            });
            Ok(Box::new(BatchFile {
                path: path.to_path_buf(),
                options: options.clone(),
                backend: self.backend.clone(),
                handle,
                live: self.live.clone(),
            }))
        }
        fn metadata(&self, path: &Path) -> io::Result<std::fs::Metadata> {
            self.current().1.metadata(path)
        }
        fn create_dir_all(&self, path: &Path) -> io::Result<()> {
            self.current().1.create_dir_all(path)
        }
        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.current().1.rename(from, to)
        }
        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.current().1.remove_file(path)
        }
        fn sync_dir(&self, path: &Path) -> io::Result<()> {
            self.current().1.sync_dir(path)
        }
        fn create_dir(&self, path: &Path) -> io::Result<()> {
            self.current().1.create_dir(path)
        }
        fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
            self.current().1.hard_link(from, to)
        }
        fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
            self.current().1.canonicalize(path)
        }
        fn read_dir(&self, path: &Path) -> io::Result<broca_wal::fs_ops::ReadDir> {
            self.current().1.read_dir(path)
        }
        fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
            self.current().1.validate_if_exists(path)
        }
    }
    impl Read for BatchFile {
        fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
            self.rebound()?.file.read(bytes)
        }
    }
    impl Write for BatchFile {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.rebound()?.file.write(bytes)
        }
        fn flush(&mut self) -> io::Result<()> {
            self.rebound()?.file.flush()
        }
    }
    impl Seek for BatchFile {
        fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
            self.rebound()?.file.seek(from)
        }
    }
    impl FileOps for BatchFile {
        fn metadata(&self) -> io::Result<std::fs::Metadata> {
            self.rebound()?.file.metadata()
        }
        fn set_len(&self, len: u64) -> io::Result<()> {
            self.rebound()?.file.set_len(len)
        }
        fn sync_all(&self) -> io::Result<()> {
            self.rebound()?.file.sync_all()
        }
        fn try_clone(&self) -> io::Result<Box<dyn FileOps>> {
            let handle = {
                let handle = self.rebound()?;
                Arc::new(Mutex::new(Handle {
                    generation: handle.generation,
                    file: handle.file.try_clone()?,
                }))
            };
            self.live.lock().unwrap().push(LiveHandle {
                path: self.path.clone(),
                options: self.options.clone(),
                handle: Arc::downgrade(&handle),
            });
            Ok(Box::new(Self {
                path: self.path.clone(),
                options: self.options.clone(),
                backend: self.backend.clone(),
                handle,
                live: self.live.clone(),
            }))
        }
        fn lock(&self) -> io::Result<()> {
            self.rebound()?.file.lock()
        }
        fn unlock(&self) -> io::Result<()> {
            self.rebound()?.file.unlock()
        }
        fn try_lock(&self) -> Result<(), std::fs::TryLockError> {
            self.rebound()
                .map_err(std::fs::TryLockError::Error)?
                .file
                .try_lock()
        }
        fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
            self.rebound()?.file.read_exact_at(bytes, offset)
        }
    }
}

fn is_head(path: &Path) -> bool {
    path.extension().is_some_and(|ext| ext == "head")
}

impl broca_wal::fs_ops::FsOps for ProbeFs {
    fn open(
        &self,
        path: &Path,
        options: &broca_wal::fs_ops::OpenOptions,
    ) -> io::Result<Box<dyn broca_wal::fs_ops::FileOps>> {
        let file = self.inner.open(path, options)?;
        Ok(Box::new(ProbeFile {
            inner: file,
            path: path.to_path_buf(),
            probe: self.probe.clone(),
        }))
    }
    fn metadata(&self, path: &Path) -> io::Result<std::fs::Metadata> {
        if is_head(path) {
            self.probe.head_lookups.fetch_add(1, Ordering::SeqCst);
        }
        self.inner.metadata(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        if is_head(from) || is_head(to) {
            self.probe.head_renames.fetch_add(1, Ordering::SeqCst);
        }
        self.probe
            .events
            .lock()
            .unwrap()
            .push(("rename", from.to_path_buf()));
        self.inner.rename(from, to)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        let fail = {
            let mut target = self.probe.fail_directory_once.lock().unwrap();
            if target.as_deref() == Some(path) {
                target.take();
                true
            } else {
                false
            }
        };
        self.probe.events.lock().unwrap().push((
            if fail { "failed_sync_dir" } else { "sync_dir" },
            path.to_path_buf(),
        ));
        if fail {
            return Err(io::Error::other("injected source-directory sync failure"));
        }
        self.inner.sync_dir(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.probe
            .events
            .lock()
            .unwrap()
            .push(("unlink", path.to_path_buf()));
        self.inner.remove_file(path)
    }
    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir(path)
    }
    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.probe
            .events
            .lock()
            .unwrap()
            .push(("hard_link", from.to_path_buf()));
        self.inner.hard_link(from, to)?;
        if from.extension().is_some_and(|ext| ext == "wal") {
            if let Some((store, session)) = self.probe.lookup_on_link.lock().unwrap().as_ref() {
                assert!(matches!(
                    store.resolve_live_path(session),
                    Err(broca_wal::WalError::RelocationIncomplete { .. })
                ));
                assert!(
                    from.exists() && to.exists(),
                    "lease-free lookup must not remove either in-flight placement name"
                );
                self.probe.link_lookup_checks.fetch_add(1, Ordering::SeqCst);
            }
        }
        Ok(())
    }
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.inner.canonicalize(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<broca_wal::fs_ops::ReadDir> {
        self.inner.read_dir(path)
    }
    fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
        self.inner.validate_if_exists(path)
    }
}

struct ProbeFile {
    inner: Box<dyn broca_wal::fs_ops::FileOps>,
    path: PathBuf,
    probe: Arc<Probe>,
}
impl Read for ProbeFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.inner.read(bytes)
    }
}
impl Write for ProbeFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if is_head(&self.path) && self.probe.stall_proof_writes.swap(0, Ordering::SeqCst) > 0 {
            self.probe.now.fetch_add(70_000, Ordering::SeqCst);
        }
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}
impl Seek for ProbeFile {
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.inner.seek(from)
    }
}
impl broca_wal::fs_ops::FileOps for ProbeFile {
    fn metadata(&self) -> io::Result<std::fs::Metadata> {
        self.inner.metadata()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.inner.set_len(len)
    }
    fn sync_all(&self) -> io::Result<()> {
        self.inner.sync_all()?;
        if self.path.extension().is_some_and(|ext| ext == "wal") {
            self.probe.log_syncs.fetch_add(1, Ordering::SeqCst);
            let remaining = self.probe.stall_log_syncs.load(Ordering::SeqCst);
            if remaining > 0 {
                self.probe
                    .stall_log_syncs
                    .store(remaining - 1, Ordering::SeqCst);
                self.probe.now.fetch_add(70_000, Ordering::SeqCst);
            }
        }
        Ok(())
    }
    fn try_clone(&self) -> io::Result<Box<dyn broca_wal::fs_ops::FileOps>> {
        Ok(Box::new(Self {
            inner: self.inner.try_clone()?,
            path: self.path.clone(),
            probe: self.probe.clone(),
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

struct Scratch(PathBuf);
impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = broca_test_support::scratch_root().join(format!(
            "head-proof-power-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn legacy_relocation_never_looks_up_or_moves_a_head() {
    let root = Scratch::new();
    let source = root.0.join("wal/legacy.wal");
    let destination = root.0.join("wal-ephemeral/legacy.wal");
    let mut wal = FileWal::open(&source).unwrap();
    let before = std::fs::read(&source).unwrap();
    let source_head = sidecar_path(&source);
    std::fs::write(&source_head, b"legacy orphan must stay untouched").unwrap();
    let probe = Arc::new(Probe::default());
    let fs = Arc::new(ProbeFs {
        inner: Arc::new(RecordingFs::new(&root.0).unwrap()),
        probe: probe.clone(),
    });
    with_fs_ops(fs, || wal.relocate_unstarted(&destination)).unwrap();
    assert_eq!(
        probe.head_lookups.load(Ordering::SeqCst),
        0,
        "legacy placement must not probe a proof path"
    );
    assert_eq!(probe.head_renames.load(Ordering::SeqCst), 0);
    assert_eq!(wal.log_path(), Some(destination.as_path()));
    assert_eq!(std::fs::read(&destination).unwrap(), before);
    assert!(!source.exists());
    assert_eq!(
        std::fs::read(source_head).unwrap(),
        b"legacy orphan must stay untouched"
    );
    assert!(!sidecar_path(&destination).exists());
}

#[test]
fn legacy_relocation_cuts_contain_no_conversation_data() {
    let root = Scratch::new();
    let source = root.0.join("wal/legacy.wal");
    let destination = root.0.join("wal-ephemeral/legacy.wal");
    let wal = FileWal::open(&source).unwrap();
    assert_eq!(wal.last_seq(), 0);
    assert!(wal.replay().unwrap().is_empty());
    drop(wal);
    let bytes = std::fs::read(&source).unwrap();
    assert!(
        matches!(broca_wal::framing::decode_next(&bytes), broca_wal::framing::DecodeStep::Lineage { consumed, .. } if consumed == bytes.len())
    );
    let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
    with_fs_ops(recording.clone(), || {
        let mut wal = FileWal::open(&source).unwrap();
        wal.relocate_unstarted(&destination).unwrap();
    });
    let source = source.strip_prefix(&root.0).unwrap();
    let destination = destination.strip_prefix(&root.0).unwrap();
    let states = recording.enumerate(100_000).unwrap();
    let mut unnamed = 0;
    for cut in &states {
        unnamed += usize::from(
            !cut.state.files.contains_key(source) && !cut.state.files.contains_key(destination),
        );
        for path in [source, destination] {
            if let Some(bytes) = cut.state.files.get(path) {
                assert!(
                    matches!(broca_wal::framing::decode_next(bytes), broca_wal::framing::DecodeStep::Lineage { consumed, .. } if consumed == bytes.len())
                );
            }
        }
    }
    eprintln!("legacy relocation: {unnamed} of {} cuts lose both names; source contains only lineage, no conversation records", states.len());
}

#[cfg(feature = "writer-id-faults")]
#[test]
fn lease_free_lookup_between_link_and_unlink_preserves_both_names() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let root = Scratch::new();
    let probe = Arc::new(Probe::default());
    probe.now.store(1000, Ordering::SeqCst);
    let fs = Arc::new(ProbeFs {
        inner: Arc::new(RecordingFs::new(&root.0).unwrap()),
        probe: probe.clone(),
    });
    let (store, session, writer) = spawn_recorded(&root.0, fs.clone());
    runtime.block_on(writer.shutdown()).unwrap();
    let normal = store.wal_path(&session);
    let ephemeral = store.ephemeral_wal_path(&session);
    let _tenure = broca_core::lease::WalLeaseDomain::new(root.0.join("leases"))
        .acquire(&session)
        .unwrap();
    *probe.lookup_on_link.lock().unwrap() = Some((Arc::new(store), session));
    with_fs_ops(fs, || {
        let mut wal = FileWal::open(&normal)
            .unwrap()
            .with_wal_config(broca_wal::WalConfig::OVERHAUL)
            .unwrap();
        wal.relocate_unstarted(&ephemeral).unwrap();
        // Exercise the reverse placement too: discovery must not pick a direction.
        wal.relocate_unstarted(&normal).unwrap();
    });
    assert_eq!(probe.link_lookup_checks.load(Ordering::SeqCst), 2);
    assert!(normal.exists() && !ephemeral.exists());
}

#[cfg(feature = "writer-id-faults")]
#[test]
fn identical_bytes_in_distinct_files_are_not_a_repairable_placement() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let root = Scratch::new();
    let probe = Arc::new(Probe::default());
    probe.now.store(1000, Ordering::SeqCst);
    let fs = Arc::new(ProbeFs {
        inner: Arc::new(RecordingFs::new(&root.0).unwrap()),
        probe,
    });
    let (store, session, writer) = spawn_recorded(&root.0, fs);
    runtime.block_on(writer.shutdown()).unwrap();
    let normal = store.wal_path(&session);
    let ephemeral = store.ephemeral_wal_path(&session);
    std::fs::create_dir_all(ephemeral.parent().unwrap()).unwrap();
    std::fs::copy(&normal, &ephemeral).unwrap();
    let bytes = std::fs::read(&normal).unwrap();
    assert!(
        matches!(store.resolve_live_path(&session), Err(broca_wal::WalError::Io(error)) if error.kind() == io::ErrorKind::InvalidData)
    );
    let tenure = broca_core::lease::WalLeaseDomain::new(root.0.join("leases"))
        .acquire(&session)
        .unwrap();
    let error = runtime
        .block_on(broca_core::WalHandle::open_and_spawn_owned(
            Arc::new(store),
            session,
            tenure,
            None,
        ))
        .err()
        .expect("different files must refuse an owned repair too");
    assert!(matches!(error, broca_core::WalOpenError::Failed(_)));
    assert_eq!(std::fs::read(&normal).unwrap(), bytes);
    assert_eq!(std::fs::read(&ephemeral).unwrap(), bytes);
}

#[cfg(feature = "writer-id-faults")]
fn spawn_recorded(
    root: &Path,
    fs: Arc<ProbeFs>,
) -> (
    broca_core::FileWalStore,
    broca_protocol::BindIdentity,
    broca_core::WalHandle,
) {
    try_spawn_recorded(root, fs).unwrap()
}

#[cfg(feature = "writer-id-faults")]
fn try_spawn_recorded(
    root: &Path,
    fs: Arc<ProbeFs>,
) -> Result<
    (
        broca_core::FileWalStore,
        broca_protocol::BindIdentity,
        broca_core::WalHandle,
    ),
    broca_core::WalWriteError,
> {
    use broca_core::WalStore;
    let store =
        broca_core::FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL);
    let session = broca_protocol::BindIdentity::new("/proof", "runner", "recorded");
    let probe = fs.probe.clone();
    let handle = with_fs_ops(fs.clone(), || {
        let wal = store
            .open(&session)
            .map_err(|error| broca_core::WalWriteError::Failed(error.to_string()))?;
        broca_core::WalHandle::spawn_in_store_with_fs_and_clock_for_test(
            wal,
            &store,
            &session,
            1,
            None,
            fs,
            move || probe.now.load(Ordering::SeqCst),
        )
    });
    Ok((store, session, handle?))
}

#[cfg(feature = "writer-id-faults")]
fn spawn_owned_recorded(
    store: &Arc<broca_core::FileWalStore>,
    session: &broca_protocol::BindIdentity,
    leases: &broca_core::FileLeaseStore,
    fs: Arc<ProbeFs>,
    retention: Option<u64>,
) -> broca_core::WalHandle {
    use broca_core::{LeaseStore, WalStore};
    let lease = leases.acquire(&broca_core::lease_key_for(session)).unwrap();
    let fence = lease.epoch();
    let clock = fs.probe.clone();
    with_fs_ops(fs.clone(), || {
        let (wal, lease) = store
            .open_owned_writer(session, retention, None, lease)
            .unwrap();
        broca_core::WalHandle::spawn_in_store_with_fs_and_clock_for_test(
            wal,
            store.as_ref(),
            session,
            fence,
            Some(lease),
            fs,
            move || clock.now.load(Ordering::SeqCst),
        )
        .unwrap()
    })
}

#[cfg(feature = "writer-id-faults")]
fn first_run(session: &broca_protocol::BindIdentity) -> WalRecord {
    let mut record: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    ))
    .unwrap();
    if let WalRecord::RunStarted {
        session: key,
        run_id,
        retention,
        ts_ms,
        ..
    } = &mut record
    {
        *key = session.clone();
        *run_id = broca_protocol::RunId("first".into());
        *retention = Some(60);
        *ts_ms = Some(1000);
    }
    record
}

#[cfg(feature = "writer-id-faults")]
fn contains_started(mut bytes: &[u8]) -> bool {
    while !bytes.is_empty() {
        match broca_wal::framing::decode_next(bytes) {
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                if matches!(framed.record, WalRecord::RunStarted { .. }) {
                    return true;
                }
                bytes = &bytes[consumed..];
            }
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => bytes = &bytes[consumed..],
            _ => break,
        }
    }
    false
}

#[cfg(feature = "writer-id-faults")]
#[test]
fn failed_source_barrier_is_repeated_before_a_reopened_log_starts() {
    use broca_core::WalStore;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let root = Scratch::new();
    let lease_root = Scratch::new();
    let leases = broca_core::FileLeaseStore::new(lease_root.0.join("leases"));
    let session = broca_protocol::BindIdentity::new("/proof", "runner", "source-barrier");
    std::fs::create_dir_all(root.0.join("wal-durable")).unwrap();
    let store = Arc::new(broca_core::FileWalStore::with_config(
        root.0.join("wal"),
        broca_wal::WalConfig::OVERHAUL,
    ));
    let probe = Arc::new(Probe::default());
    probe.now.store(1000, Ordering::SeqCst);
    let batch = Arc::new(batch_recording::BatchFs::new());
    let fs = Arc::new(ProbeFs {
        inner: batch.clone(),
        probe: probe.clone(),
    });
    let writer = spawn_owned_recorded(&store, &session, &leases, fs.clone(), None);
    let recording = batch.begin_before_relocation(&root.0);
    let source = store.wal_path(&session);
    let destination = store.ephemeral_wal_path(&session);
    *probe.fail_directory_once.lock().unwrap() = Some(source.parent().unwrap().to_path_buf());
    writer.record(first_run(&session)).into_result().unwrap();
    let failure = runtime.block_on(writer.sync()).unwrap_err();
    assert!(
        matches!(&failure, broca_core::WalWriteError::RelocationIncomplete { reason } if reason.contains("source-directory")),
        "{failure:?}"
    );
    assert!(
        matches!(broca_core::RunError::from(failure), broca_core::RunError::Transient(reason) if reason.contains("wal_relocation_incomplete"))
    );
    assert!(
        !source.exists() && destination.exists(),
        "unlink succeeded before its directory barrier failed"
    );
    assert!(!store
        .read_records(&session)
        .unwrap()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::RunStarted { .. })));
    assert!(matches!(
        writer.record(first_run(&session)),
        broca_core::wal_writer::RecordOutcome::Refused(
            broca_core::WalWriteError::RelocationIncomplete { .. }
        )
    ));
    assert!(runtime.block_on(writer.shutdown()).is_err());
    probe.events.lock().unwrap().clear();
    let writer = spawn_owned_recorded(&store, &session, &leases, fs, Some(60));
    writer.record(first_run(&session)).into_result().unwrap();
    runtime.block_on(writer.sync()).unwrap();
    let events = probe.events.lock().unwrap().clone();
    assert!(
        events
            .iter()
            .any(|(op, path)| *op == "sync_dir" && path == source.parent().unwrap()),
        "the singleton/same-path retry must still complete the old source barrier"
    );
    assert!(events
        .iter()
        .any(|(op, path)| *op == "sync_dir" && path == destination.parent().unwrap()));
    assert!(events
        .iter()
        .any(|(op, path)| *op == "sync_dir" && path == &root.0.join("wal-durable")));
    drop(events);
    let states = recording.enumerate(2_000_000).unwrap();
    runtime.block_on(writer.shutdown()).unwrap();
    let source = source.strip_prefix(&root.0).unwrap();
    let destination = destination.strip_prefix(&root.0).unwrap();
    let mut started = 0;
    let mut aliases = 0;
    let mut recovered_aliases = 0;
    let mut seen_aliases = std::collections::BTreeSet::new();
    for state in &states {
        let old = state.state.files.get(source);
        let new = state.state.files.get(destination);
        let is_started = old.or(new).is_some_and(|bytes| contains_started(bytes));
        started += usize::from(is_started);
        aliases += usize::from(old.is_some() && new.is_some());
        assert!(
            !is_started || old.is_none() || new.is_none(),
            "a started alias pair survived cut {} after the retry",
            state.cut
        );
        if old.is_some() && new.is_some() && seen_aliases.insert(state.state.clone()) {
            let recovery = Scratch::new();
            std::fs::remove_dir_all(&recovery.0).unwrap();
            state.state.install(&recovery.0).unwrap();
            let recovered_store = Arc::new(broca_core::FileWalStore::with_config(
                recovery.0.join("wal"),
                broca_wal::WalConfig::OVERHAUL,
            ));
            let tenure = broca_core::lease::WalLeaseDomain::new(lease_root.0.join("leases"))
                .acquire(&session)
                .unwrap();
            match runtime.block_on(broca_core::WalHandle::open_and_spawn_owned_with_retention(
                recovered_store.clone(),
                session.clone(),
                tenure,
                None,
                Some(60),
            )) {
                Ok(writer) => {
                    assert_eq!(
                        recovered_store.resolve_live_path(&session).unwrap(),
                        Some(recovery.0.join(destination))
                    );
                    runtime.block_on(writer.shutdown()).unwrap();
                    recovered_aliases += 1;
                }
                Err(error) => assert!(
                    error.to_string().contains("corrupt"),
                    "unexpected alias recovery refusal: {error}"
                ),
            }
        }
    }
    assert!(started > 0 && aliases > 0 && recovered_aliases > 0);
    eprintln!("source-barrier error/reopen: {} power-loss states, {started} started states, {aliases} unstarted alias states, {recovered_aliases} recovered alias images", states.len());
}

#[cfg(feature = "writer-id-faults")]
#[tokio::test]
async fn stalled_proof_write_is_followed_by_a_newer_published_marker() {
    use broca_core::WalStore;
    let root = Scratch::new();
    let probe = Arc::new(Probe::default());
    probe.now.store(1000, Ordering::SeqCst);
    let fs = Arc::new(ProbeFs {
        inner: Arc::new(RecordingFs::new(&root.0).unwrap()),
        probe: probe.clone(),
    });
    let (store, session, handle) = spawn_recorded(&root.0, fs);
    handle
        .record(WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: broca_protocol::caller::RunFinishReason::Completed,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        })
        .into_result()
        .unwrap();
    probe.stall_proof_writes.store(1, Ordering::SeqCst);
    handle.sync().await.unwrap();
    let path = store.wal_path(&session);
    let head = HeadProof::decode(&std::fs::read(sidecar_path(&path)).unwrap()).unwrap();
    assert_eq!(head.appended_ms, 71_000, "a proof write that exposes an old head late must publish a newer frame before the barrier succeeds");
    assert!(matches!(
        store.read_records(&session).unwrap().last().unwrap().record,
        WalRecord::VisibilityLate { through_seq: 2, .. }
    ));
    handle.shutdown().await.unwrap();
}

#[cfg(feature = "writer-id-faults")]
#[tokio::test]
async fn stalled_close_publication_keeps_a_marker_for_its_successor() {
    use broca_core::WalStore;
    let root = Scratch::new();
    let probe = Arc::new(Probe::default());
    probe.now.store(1000, Ordering::SeqCst);
    let fs = Arc::new(ProbeFs {
        inner: Arc::new(RecordingFs::new(&root.0).unwrap()),
        probe: probe.clone(),
    });
    let (store, session, handle) = spawn_recorded(&root.0, fs.clone());
    probe.stall_proof_writes.store(1, Ordering::SeqCst);
    let error = handle.shutdown().await.unwrap_err();
    assert!(error.to_string().contains("late_close_publication"));
    assert_eq!(
        std::fs::read_dir(root.0.join("open-writers"))
            .unwrap()
            .count(),
        1
    );
    let records = store.read_records(&session).unwrap();
    assert!(matches!(
        records.last().unwrap().record,
        WalRecord::WriterClosed { .. }
    ));
    let (_, _, successor) = spawn_recorded(&root.0, fs);
    let path = store.wal_path(&session);
    let bytes = std::fs::read(&path).unwrap();
    let sidecar = std::fs::read(sidecar_path(&path)).unwrap();
    let published = PublishedLog::open(&bytes, &sidecar).unwrap().unwrap();
    assert_eq!(published.head().durable_seq, 3);
    assert_eq!(published.head().appended_ms, 71_000);
    assert!(matches!(
        store.read_records(&session).unwrap().last().unwrap().record,
        WalRecord::WriterOpened {
            recovered_through_seq: 2,
            ..
        }
    ));
    successor.shutdown().await.unwrap();
}

#[cfg(feature = "writer-id-faults")]
#[tokio::test]
async fn lifecycle_publication_reuses_completed_barriers() {
    use broca_core::WalStore;
    let root = Scratch::new();
    let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
    let probe = Arc::new(Probe::default());
    probe.now.store(1000, Ordering::SeqCst);
    let fs = Arc::new(ProbeFs {
        inner: recording,
        probe: probe.clone(),
    });
    let store =
        broca_core::FileWalStore::with_config(root.0.join("wal"), broca_wal::WalConfig::OVERHAUL);
    let session = broca_protocol::BindIdentity::new("/proof", "runner", "barriers");
    let handle = with_fs_ops(fs.clone(), || {
        let wal = store.open(&session).unwrap();
        let before = probe.log_syncs.load(Ordering::SeqCst);
        let clock = probe.clone();
        let handle = broca_core::WalHandle::spawn_in_store_with_fs_and_clock_for_test(
            wal,
            &store,
            &session,
            1,
            None,
            fs,
            move || clock.now.load(Ordering::SeqCst),
        )
        .unwrap();
        assert_eq!(probe.log_syncs.load(Ordering::SeqCst) - before, 3, "recovery proof, recovered-prefix barrier and WriterOpened barrier, with no publication resync");
        handle
    });
    let before_close = probe.log_syncs.load(Ordering::SeqCst);
    handle.shutdown().await.unwrap();
    assert_eq!(
        probe.log_syncs.load(Ordering::SeqCst) - before_close,
        1,
        "WriterClosed's completed barrier is reused by publication"
    );
}

#[cfg(feature = "writer-id-faults")]
fn check_publication_image(state: &State, log_path: &Path, late_before: u64) -> Option<HeadProof> {
    let bytes = state.files.get(log_path).map_or(&[][..], Vec::as_slice);
    let sidecar = state
        .files
        .get(&sidecar_path(log_path))
        .map_or(&[][..], Vec::as_slice);
    HeadProof::decode(sidecar)?;
    let published = PublishedLog::open(bytes, sidecar)
        .expect("CRC-valid proof must not be ahead of its synced prefix")
        .expect("intact proof is available");
    let mut finish = false;
    let mut last_seq = 0;
    for step in published.frames(broca_wal::KNOWN_FEATURES) {
        match step.unwrap() {
            broca_wal::framing::v4::DecodeStep::Frame(frame) => {
                last_seq = frame.header.metadata.seq;
                finish |= matches!(
                    serde_json::from_slice::<WalRecord>(frame.payload).unwrap(),
                    WalRecord::RunFinished { .. }
                );
            }
            _ => panic!("a proof exposed incomplete or unsynced payload bytes"),
        }
    }
    assert_eq!(last_seq, published.head().durable_seq);
    if finish && published.head().appended_ms < late_before {
        assert!(
            state
                .files
                .keys()
                .any(|path| path.starts_with("open-writers")),
            "an interrupted late exposure must retain its reclaim obligation"
        );
    }
    Some(published.head().clone())
}

#[cfg(feature = "writer-id-faults")]
#[test]
/// A stalled proof write can expose an old head transiently. A completed
/// barrier must repair it; interrupted states keep the successor obligation.
fn late_finish_never_exposed_without_newer_frame() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut total = 0;
    let mut with_proof = false;
    let mut without_proof = false;
    let mut finish_proof_survived = false;
    let mut finish_proof_lost = false;
    let mut retried_sync = false;
    let mut recovered = 0;
    let mut refused = 0;
    let mut first_cuts = 0;
    let mut recovery_images = 0;
    for stall_proof in [false, true] {
        let root = Scratch::new();
        let batch_fs = Arc::new(batch_recording::BatchFs::new());
        let probe = Arc::new(Probe::default());
        probe.now.store(1000, Ordering::SeqCst);
        let fs = Arc::new(ProbeFs {
            inner: batch_fs.clone(),
            probe: probe.clone(),
        });
        let (store, session, writer) = spawn_recorded(&root.0, fs);
        // Start from the real ready barrier, not a hand-built recovered prefix.
        // The crash schedule begins with the ordinary unsynced finish append.
        let recording = batch_fs.begin(&root.0);
        writer
            .record(WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: broca_protocol::caller::RunFinishReason::Completed,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            })
            .into_result()
            .unwrap();
        if stall_proof {
            probe.stall_proof_writes.store(1, Ordering::SeqCst);
        } else {
            probe.stall_log_syncs.store(2, Ordering::SeqCst);
        }
        let published_seq = runtime.block_on(writer.sync()).unwrap();
        let head =
            HeadProof::decode(&std::fs::read(sidecar_path(&store.wal_path(&session))).unwrap())
                .unwrap();
        assert_eq!(head.durable_seq, published_seq);
        assert_eq!(head.appended_ms, if stall_proof { 71_000 } else { 141_000 }, "the real dirty-batch barrier must finish the newer-marker publication before succeeding");
        assert_eq!(published_seq, if stall_proof { 3 } else { 4 });
        retried_sync |= published_seq >= 4;
        let log = store
            .wal_path(&session)
            .strip_prefix(&root.0)
            .unwrap()
            .to_path_buf();
        let first_states = recording.enumerate(2_000_000).unwrap();
        runtime.block_on(writer.shutdown()).unwrap();
        assert!(first_states
            .iter()
            .any(|state| state.family == Family::PartialPersistence));
        let mut seen_images = std::collections::BTreeSet::new();
        for first in first_states {
            first_cuts += 1;
            let first_head = check_publication_image(&first.state, &log, 71_000);
            with_proof |= first_head.is_some();
            without_proof |= first_head.is_none();
            let covers_finish = first_head
                .as_ref()
                .is_some_and(|head| head.durable_seq >= 2);
            finish_proof_survived |= covers_finish;
            finish_proof_lost |= !covers_finish;
            // Recovery has the same fixed clock for identical filesystem images.
            // Check every cut, but enumerate recovery once per exact State; no
            // distinct surviving image is omitted or sampled.
            if !seen_images.insert(first.state.clone()) {
                continue;
            }
            recovery_images += 1;
            let recovery = Scratch::new();
            std::fs::remove_dir_all(&recovery.0).unwrap();
            first.state.install(&recovery.0).unwrap();
            let recovery_recording = Arc::new(RecordingFs::new(&recovery.0).unwrap());
            let recovery_probe = Arc::new(Probe::default());
            recovery_probe.now.store(1_000_000, Ordering::SeqCst);
            let fs = Arc::new(ProbeFs {
                inner: recovery_recording.clone(),
                probe: recovery_probe,
            });
            let successor = match try_spawn_recorded(&recovery.0, fs) {
                Ok((store, session, successor)) => {
                    let path = store.wal_path(&session);
                    let head =
                        HeadProof::decode(&std::fs::read(sidecar_path(&path)).unwrap()).unwrap();
                    assert_eq!(
                        head.appended_ms, 1_000_000,
                        "real recovery publishes its fresh WriterOpened"
                    );
                    recovered += 1;
                    Some(successor)
                }
                Err(error) => {
                    assert!(
                        error.to_string().contains("corrupt"),
                        "unexpected recovery refusal: {error}"
                    );
                    refused += 1;
                    None
                }
            };
            let second_states = recovery_recording.enumerate(2_000_000).unwrap();
            for second in second_states {
                total += 1;
                assert!(
                    total <= 32_000_000,
                    "two-crash exploration budget exceeded at {total} states, {first_cuts} first cuts, {recovery_images} distinct images, proof_stall={stall_proof}; refusing rather than sampling"
                );
                check_publication_image(&second.state, &log, 1_000_000);
            }
            // Enumerate filesystem crash states while the real writer is idle;
            // shut it down afterward so orderly cleanup is outside that history.
            if let Some(successor) = successor {
                runtime.block_on(successor.shutdown()).unwrap();
            }
        }
    }
    assert!(
        with_proof
            && without_proof
            && finish_proof_survived
            && finish_proof_lost
            && retried_sync
            && recovered > 0
    );
    eprintln!("real-writer two-crash publication: {first_cuts} first cuts, {recovery_images} distinct recovery images, {total} second-cut states; {recovered} recoveries; {refused} fail-closed corrupt tails");
}

#[cfg(feature = "writer-id-faults")]
#[test]
fn interrupted_relocation_validates_pairing_and_discovery() {
    use broca_core::WalStore;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let mut pairing_window_seen = false;
    let mut moved_proof_seen = false;
    let mut orphan_rejected = false;
    let mut states_checked = 0;
    for source_proof in [false, true] {
        for orphan_destination in [false, true] {
            let root = Scratch::new();
            let probe = Arc::new(Probe::default());
            probe.now.store(1000, Ordering::SeqCst);
            let setup = Arc::new(ProbeFs {
                inner: Arc::new(RecordingFs::new(&root.0).unwrap()),
                probe,
            });
            let (store, session, writer) = spawn_recorded(&root.0, setup);
            runtime.block_on(writer.shutdown()).unwrap();
            let source = store.wal_path(&session);
            let destination = root
                .0
                .join("wal-ephemeral")
                .join(source.file_name().unwrap());
            std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
            if !source_proof {
                std::fs::remove_file(sidecar_path(&source)).unwrap();
            }
            if orphan_destination {
                let orphan = Scratch::new();
                let p = Arc::new(Probe::default());
                p.now.store(1000, Ordering::SeqCst);
                let fs = Arc::new(ProbeFs {
                    inner: Arc::new(RecordingFs::new(&orphan.0).unwrap()),
                    probe: p,
                });
                let (other, other_session, writer) = spawn_recorded(&orphan.0, fs);
                runtime.block_on(writer.shutdown()).unwrap();
                std::fs::copy(
                    sidecar_path(&other.wal_path(&other_session)),
                    sidecar_path(&destination),
                )
                .unwrap();
            }
            let recording = Arc::new(RecordingFs::new(&root.0).unwrap());
            let probe = Arc::new(Probe::default());
            let fs = Arc::new(ProbeFs {
                inner: recording.clone(),
                probe: probe.clone(),
            });
            with_fs_ops(fs, || {
                let mut wal = FileWal::open(&source)
                    .unwrap()
                    .with_wal_config(broca_wal::WalConfig::OVERHAUL)
                    .unwrap();
                wal.relocate_unstarted(&destination).unwrap();
                assert_eq!(wal.log_path(), Some(destination.as_path()));
            });
            let events = probe.events.lock().unwrap();
            let log_link = events
                .iter()
                .position(|(op, path)| *op == "hard_link" && path == &source)
                .unwrap();
            let log_unlink = events
                .iter()
                .position(|(op, path)| *op == "unlink" && path == &source)
                .unwrap();
            let protected_destination = events
                .iter()
                .enumerate()
                .position(|(index, (op, path))| {
                    index > log_link && *op == "sync_dir" && path == destination.parent().unwrap()
                })
                .unwrap();
            assert!(log_link < protected_destination && protected_destination < log_unlink);
            let destination_sync = events
                .iter()
                .rposition(|(op, path)| *op == "sync_dir" && path == destination.parent().unwrap())
                .unwrap();
            let source_sync = events
                .iter()
                .rposition(|(op, path)| *op == "sync_dir" && path == source.parent().unwrap())
                .unwrap();
            assert!(log_unlink < destination_sync && destination_sync < source_sync);
            if source_proof {
                let proof_rename = events
                    .iter()
                    .position(|(op, path)| *op == "rename" && path == &sidecar_path(&source))
                    .unwrap();
                assert!(log_unlink < proof_rename && proof_rename < destination_sync);
            }
            drop(events);
            let source = source.strip_prefix(&root.0).unwrap().to_path_buf();
            let destination = destination.strip_prefix(&root.0).unwrap().to_path_buf();
            for cut in recording.enumerate(100_000).unwrap() {
                states_checked += 1;
                let old_log = cut.state.files.contains_key(&source);
                let new_log = cut.state.files.contains_key(&destination);
                pairing_window_seen |=
                    new_log && cut.state.files.contains_key(&sidecar_path(&source));
                moved_proof_seen |= new_log
                    && !cut.state.files.contains_key(&sidecar_path(&source))
                    && cut.state.files.contains_key(&sidecar_path(&destination));
                for path in [&source, &destination] {
                    if let Some(bytes) = cut.state.files.get(path) {
                        let sidecar = cut
                            .state
                            .files
                            .get(&sidecar_path(path))
                            .map_or(&[][..], Vec::as_slice);
                        match PublishedLog::open(bytes, sidecar) {
                            Ok(Some(log)) => assert_eq!(log.head().durable_seq, 2),
                            Ok(None) => assert!(sidecar.is_empty()),
                            Err(_) => {
                                assert!(orphan_destination && path == &destination);
                                orphan_rejected = true;
                            }
                        }
                    }
                }
                let recovery = Scratch::new();
                std::fs::remove_dir_all(&recovery.0).unwrap();
                cut.state.install(&recovery.0).unwrap();
                let store = broca_core::FileWalStore::with_config(
                    recovery.0.join("wal"),
                    broca_wal::WalConfig::OVERHAUL,
                );
                let discovered = store.open(&session);
                if old_log && new_log {
                    assert!(matches!(discovered, Err(broca_wal::WalError::RelocationIncomplete { .. })), "lease-free discovery reports unavailable rather than selecting or deleting a name");
                    assert!(
                        recovery.0.join(&source).exists() && recovery.0.join(&destination).exists()
                    );
                    for (retention, class) in
                        [(Some(60), None), (None, None), (None, Some("backup"))]
                    {
                        let repair_root = Scratch::new();
                        std::fs::remove_dir_all(&repair_root.0).unwrap();
                        cut.state.install(&repair_root.0).unwrap();
                        let store = Arc::new(
                            broca_core::FileWalStore::with_config(
                                repair_root.0.join("wal"),
                                broca_wal::WalConfig::OVERHAUL,
                            )
                            .with_backup_classes(vec!["backup".into()]),
                        );
                        let expected = if retention.is_some() {
                            store.ephemeral_wal_path(&session)
                        } else if class.is_some() {
                            store.durable_wal_path(&session)
                        } else {
                            store.wal_path(&session)
                        };
                        let lease_root = Scratch::new();
                        let tenure =
                            broca_core::lease::WalLeaseDomain::new(lease_root.0.join("leases"))
                                .acquire(&session)
                                .unwrap();
                        let writer = runtime
                            .block_on(broca_core::WalHandle::open_and_spawn_owned_with_class(
                                store.clone(),
                                session.clone(),
                                tenure,
                                None,
                                retention,
                                class.map(str::to_owned),
                            ))
                            .unwrap();
                        assert_eq!(
                            store.resolve_live_path(&session).unwrap(),
                            Some(expected.clone())
                        );
                        for path in [
                            store.wal_path(&session),
                            store.ephemeral_wal_path(&session),
                            store.durable_wal_path(&session),
                        ] {
                            assert_eq!(path.exists(), path == expected);
                        }
                        runtime.block_on(writer.shutdown()).unwrap();
                    }
                } else {
                    assert!(
                        old_log || new_log,
                        "relocation lost both durable log names at cut {}",
                        cut.cut
                    );
                    assert_eq!(discovered.unwrap().last_seq(), 2);
                }
            }
        }
    }
    assert!(pairing_window_seen && moved_proof_seen && orphan_rejected);
    eprintln!("interrupted relocation: {states_checked} power-loss states");
}
