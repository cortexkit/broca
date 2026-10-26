//! Synchronous filesystem seam for durability tests. The default backend uses the same
//! owner-only creation and platform-specific directory barriers as the WAL writer.
//!
//! With the non-shipping `fs-override` feature, an override is scoped to one thread,
//! not the process. Keep the entire synchronous program (including recovery and
//! directory barriers) inside `with_fs_ops`; handles retain their backend, but path
//! operations use the current thread's backend. Without that feature, path operations
//! call `RealFs` directly and there is no override API or thread-local backend.

use std::{
    fs::Metadata,
    io::{self, Read, Seek, SeekFrom, Write},
    path::Path,
};

#[cfg(feature = "fs-override")]
use std::{cell::RefCell, sync::Arc};

/// File-handle operations whose ordering matters to a power-loss model.
pub trait FileOps: Read + Write + Seek + Send + Sync {
    fn metadata(&self) -> io::Result<Metadata>;
    fn set_len(&self, len: u64) -> io::Result<()>;
    fn sync_all(&self) -> io::Result<()>;
}

impl FileOps for std::fs::File {
    fn metadata(&self) -> io::Result<Metadata> {
        self.metadata()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.set_len(len)
    }
    fn sync_all(&self) -> io::Result<()> {
        self.sync_all()
    }
}

/// Path operations used by the WAL. `sync_dir` must not imply a file-content sync.
pub trait FsOps: Send + Sync {
    fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>>;
    fn metadata(&self, path: &Path) -> io::Result<Metadata>;
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn sync_dir(&self, path: &Path) -> io::Result<()>;
}

/// Creation flags are exposed so recording backends can distinguish allocation,
/// truncation and append from a read-only open.
#[derive(Clone, Debug, Default)]
pub struct OpenOptions {
    pub read: bool,
    pub write: bool,
    pub append: bool,
    pub create: bool,
    pub create_new: bool,
    pub truncate: bool,
}

impl OpenOptions {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn read(&mut self, value: bool) -> &mut Self {
        self.read = value;
        self
    }
    pub fn write(&mut self, value: bool) -> &mut Self {
        self.write = value;
        self
    }
    pub fn append(&mut self, value: bool) -> &mut Self {
        self.append = value;
        self
    }
    pub fn create(&mut self, value: bool) -> &mut Self {
        self.create = value;
        self
    }
    pub fn create_new(&mut self, value: bool) -> &mut Self {
        self.create_new = value;
        self
    }
    pub fn truncate(&mut self, value: bool) -> &mut Self {
        self.truncate = value;
        self
    }
    pub fn open(&self, path: impl AsRef<Path>) -> io::Result<File> {
        current().open(path.as_ref(), self).map(File)
    }
}

/// Real syscalls; no persistence behavior is simulated in production.
#[derive(Debug)]
pub struct RealFs;

impl FsOps for RealFs {
    fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
        // Preserve ordinary read opens; the owner-only helper is for writable handles.
        if options.read
            && !options.write
            && !options.append
            && !options.create
            && !options.create_new
            && !options.truncate
        {
            return Ok(Box::new(std::fs::File::open(path)?));
        }
        let mut real = broca_owner_only::OpenOptions::new();
        real.read(options.read)
            .write(options.write)
            .append(options.append)
            .create(options.create)
            .create_new(options.create_new)
            .truncate(options.truncate);
        Ok(Box::new(real.open(path)?))
    }
    fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        std::fs::metadata(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        broca_owner_only::create_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        #[cfg(unix)]
        {
            std::fs::File::open(path)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = path;
            Ok(())
        }
    }
}

#[cfg(feature = "fs-override")]
thread_local! {
    static CURRENT: RefCell<Arc<dyn FsOps>> = RefCell::new(Arc::new(RealFs));
}

#[cfg(feature = "fs-override")]
fn current() -> Arc<dyn FsOps> {
    // Keep the marker on the WAL I/O path so release staging and placement reject
    // artifacts built with fs-override, even if they never call with_fs_ops.
    std::hint::black_box("BROCA_TEST_FS_OVERRIDE_V1");
    CURRENT.with(|fs| fs.borrow().clone())
}

#[cfg(not(feature = "fs-override"))]
fn current() -> RealFs {
    RealFs
}

/// Override the backend for synchronous calls on this thread, restoring it on unwind.
#[cfg(feature = "fs-override")]
pub fn with_fs_ops<T>(fs: Arc<dyn FsOps>, action: impl FnOnce() -> T) -> T {
    struct Restore(Option<Arc<dyn FsOps>>);
    impl Drop for Restore {
        fn drop(&mut self) {
            CURRENT.with(|fs| *fs.borrow_mut() = self.0.take().unwrap());
        }
    }
    let _restore = Restore(Some(CURRENT.with(|current| current.replace(fs))));
    action()
}

pub fn metadata(path: impl AsRef<Path>) -> io::Result<Metadata> {
    current().metadata(path.as_ref())
}

pub fn try_exists(path: &Path) -> io::Result<bool> {
    match metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

pub fn create_dir_all(path: &Path) -> io::Result<()> {
    current().create_dir_all(path)
}
pub fn rename(from: &Path, to: &Path) -> io::Result<()> {
    current().rename(from, to)
}
pub fn remove_file(path: &Path) -> io::Result<()> {
    current().remove_file(path)
}
pub fn sync_dir(path: &Path) -> io::Result<()> {
    current().sync_dir(path)
}

/// A handle keeps the recording backend alive even after its opening scope returns.
pub struct File(Box<dyn FileOps>);

impl File {
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new().read(true).open(path)
    }
    pub fn create(path: impl AsRef<Path>) -> io::Result<Self> {
        OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }
    pub fn metadata(&self) -> io::Result<Metadata> {
        self.0.metadata()
    }
    pub fn set_len(&self, len: u64) -> io::Result<()> {
        self.0.set_len(len)
    }
    pub fn sync_all(&self) -> io::Result<()> {
        self.0.sync_all()
    }
}
impl Read for File {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.0.read(bytes)
    }
}
impl Write for File {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}
impl Seek for File {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.0.seek(position)
    }
}
