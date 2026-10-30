// Attempt tracing is independent of the backend's persistence model.
// The caller supplies the WAL API alias so crate-local unit tests can reuse it.

use std::fs::{Metadata, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use wal_api::fs_ops::{FileOps, FsOps, OpenOptions, ReadDir};

pub struct IoTraceFs {
    root: PathBuf,
    backend: Arc<dyn FsOps>,
    attempts: Arc<Mutex<Vec<String>>>,
}

impl IoTraceFs {
    pub fn new(root: &Path, backend: Arc<dyn FsOps>) -> Self {
        Self {
            root: root.to_path_buf(),
            backend,
            attempts: Arc::new(Mutex::new(Vec::new())),
        }
    }
    pub fn attempts(&self) -> Vec<String> {
        self.attempts.lock().unwrap().clone()
    }
    fn relative(&self, path: &Path) -> String {
        let relative = path.strip_prefix(&self.root).unwrap_or(path);
        if relative.as_os_str().is_empty() {
            ".".into()
        } else {
            relative.to_string_lossy().into_owned()
        }
    }
    fn note(&self, operation: &str, path: &Path) {
        self.attempts
            .lock()
            .unwrap()
            .push(format!("{operation} {}", self.relative(path)));
    }
    fn pair(&self, operation: &str, from: &Path, to: &Path) {
        self.attempts.lock().unwrap().push(format!(
            "{operation} {} -> {}",
            self.relative(from),
            self.relative(to)
        ));
    }
}

impl FsOps for IoTraceFs {
    fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
        self.note(
            &format!(
                "open r={} w={} a={} c={} n={} t={} ordinary={}",
                options.read,
                options.write,
                options.append,
                options.create,
                options.create_new,
                options.truncate,
                options.ordinary
            ),
            path,
        );
        let inner = self.backend.open(path, options)?;
        Ok(Box::new(TraceFile {
            inner,
            path: self.relative(path),
            attempts: self.attempts.clone(),
        }))
    }
    fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        self.note("metadata", path);
        self.backend.metadata(path)
    }
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.note("mkdir_all", path);
        self.backend.create_dir_all(path)
    }
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.pair("rename", from, to);
        self.backend.rename(from, to)
    }
    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.note("remove", path);
        self.backend.remove_file(path)
    }
    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        self.note("sync_dir", path);
        self.backend.sync_dir(path)
    }
    fn create_dir(&self, path: &Path) -> io::Result<()> {
        self.note("mkdir", path);
        self.backend.create_dir(path)
    }
    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.pair("link", from, to);
        self.backend.hard_link(from, to)
    }
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        self.note("canonicalize", path);
        self.backend.canonicalize(path)
    }
    fn read_dir(&self, path: &Path) -> io::Result<ReadDir> {
        self.note("read_dir", path);
        self.backend.read_dir(path)
    }
    fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
        self.note("validate", path);
        self.backend.validate_if_exists(path)
    }
}

struct TraceFile {
    inner: Box<dyn FileOps>,
    path: String,
    attempts: Arc<Mutex<Vec<String>>>,
}
impl TraceFile {
    fn note(&self, operation: &str) {
        self.attempts
            .lock()
            .unwrap()
            .push(format!("{operation} {}", self.path));
    }
}
impl Read for TraceFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.note(&format!("read {}", bytes.len()));
        self.inner.read(bytes)
    }
}
impl Write for TraceFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.note(&format!("write {}", bytes.len()));
        self.inner.write(bytes)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.note("flush");
        self.inner.flush()
    }
}
impl Seek for TraceFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        self.note(&format!("seek {position:?}"));
        self.inner.seek(position)
    }
}
impl FileOps for TraceFile {
    fn metadata(&self) -> io::Result<Metadata> {
        self.note("file_metadata");
        self.inner.metadata()
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        self.note(&format!("resize {len}"));
        self.inner.set_len(len)
    }
    fn sync_all(&self) -> io::Result<()> {
        self.note("sync_file");
        self.inner.sync_all()
    }
    fn try_clone(&self) -> io::Result<Box<dyn FileOps>> {
        self.note("clone");
        Ok(Box::new(Self {
            inner: self.inner.try_clone()?,
            path: self.path.clone(),
            attempts: self.attempts.clone(),
        }))
    }
    fn lock(&self) -> io::Result<()> {
        self.note("lock");
        self.inner.lock()
    }
    fn unlock(&self) -> io::Result<()> {
        self.note("unlock");
        self.inner.unlock()
    }
    fn try_lock(&self) -> Result<(), TryLockError> {
        self.note("try_lock");
        self.inner.try_lock()
    }
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.note(&format!("read_at {offset} {}", bytes.len()));
        self.inner.read_exact_at(bytes, offset)
    }
}
