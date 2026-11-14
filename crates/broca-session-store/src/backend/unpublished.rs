//! Filesystem execution for conversion's unserved trees. Unlike `LocalBackend`, individual
//! mutations are not durable: staging must be finished before verification, and scratch must
//! never be published. The ordinary backend's durability contract is unchanged.

use super::*;

/// Progress boundaries of unpublished construction and semantic verification.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Step {
    LogCreated,
    LogAppended,
    ObjectWritten,
    VersionWritten,
    HeadWritten,
    FileSynced,
    DirectorySynced,
    ScratchLoaded,
    ScratchAppended,
    ScratchCompared,
}

impl Step {
    pub fn name(self) -> &'static str {
        match self {
            Self::LogCreated => "staged_log_created",
            Self::LogAppended => "staged_log_appended",
            Self::ObjectWritten => "staged_object_written",
            Self::VersionWritten => "staged_version_written",
            Self::HeadWritten => "staged_head_written",
            Self::FileSynced => "staged_file_synced",
            Self::DirectorySynced => "staged_directory_synced",
            Self::ScratchLoaded => "scratch_loaded",
            Self::ScratchAppended => "scratch_appended",
            Self::ScratchCompared => "scratch_compared",
        }
    }
}

pub type Progress<'a> = Option<&'a (dyn Fn(Step) -> io::Result<()> + Send + Sync)>;

pub fn progress(callback: Progress<'_>, step: Step) -> io::Result<()> {
    callback.map_or(Ok(()), |callback| callback(step))
}

/// An exclusively owned, disposable filesystem tree, never a live session backend.
/// Staging coalesces the creation heads in memory. Scratch keeps temporary-file/rename head
/// writes and reads the actual copied files, but pays no durability barriers.
pub struct UnpublishedBackend<'a> {
    local: LocalBackend,
    heads: Mutex<HashMap<SessionAddr, (Vec<u8>, Version)>>,
    staging: bool,
    progress: Progress<'a>,
}

impl<'a> UnpublishedBackend<'a> {
    /// Builds a fresh conversion tree. The caller must discard the entire tree on any failure
    /// and must not expose the session or its identities before `finish` succeeds.
    pub fn staging(root: impl Into<PathBuf>, progress: Progress<'a>) -> Self {
        Self {
            local: LocalBackend::new(root),
            heads: Mutex::default(),
            staging: true,
            progress,
        }
    }

    /// Executes verification mutations on a copy which is deleted, never published.
    pub fn scratch(root: impl Into<PathBuf>, progress: Progress<'a>) -> Self {
        Self {
            local: LocalBackend::new(root),
            heads: Mutex::default(),
            staging: false,
            progress,
        }
    }

    fn step(&self, step: Step) -> Result<(), BackendError> {
        Ok(progress(self.progress, step)?)
    }

    /// Writes only the final head and matching snapshot, then syncs every final file and
    /// directory once, children before parents. Intermediate creation versions were never
    /// published, so no backup, export or restore reader can have pinned them.
    pub fn finish(self) -> Result<(), BackendError> {
        if !self.staging {
            return Err(BackendError::Corrupt(
                "a verification scratch tree cannot be published",
            ));
        }
        for (addr, (head, version)) in self.heads.lock().unwrap().iter() {
            let dir = self.local.head_dir(addr);
            let versions = dir.join(VERSIONS_DIR);
            broca_owner_only::create_dir_all(&versions)?;
            write_unsynced(
                &versions,
                ".version.tmp",
                &version.to_string(),
                &[&version.to_le_bytes(), head],
            )?;
            self.step(Step::VersionWritten)?;
            write_unsynced(&dir, "head.tmp", "head", &[&version.to_le_bytes(), head])?;
            self.step(Step::HeadWritten)?;
        }
        self.sync_tree(&self.local.root)?;
        if let Some(parent) = self.local.root.parent() {
            sync_dir(parent)?;
            self.step(Step::DirectorySynced)?;
        }
        Ok(())
    }

    fn sync_tree(&self, dir: &Path) -> Result<(), BackendError> {
        let mut entries = std::fs::read_dir(dir)?.collect::<io::Result<Vec<_>>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let kind = entry.file_type()?;
            if kind.is_dir() {
                self.sync_tree(&entry.path())?;
            } else if kind.is_file() {
                crate::durable::sync(
                    &OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(entry.path())?,
                )?;
                self.step(Step::FileSynced)?;
            } else {
                return Err(BackendError::Corrupt(
                    "an unpublished tree contains a non-file entry",
                ));
            }
        }
        sync_dir(dir)?;
        self.step(Step::DirectorySynced)
    }
}

fn write_unsynced(dir: &Path, temp: &str, name: &str, parts: &[&[u8]]) -> io::Result<()> {
    let temp = dir.join(temp);
    let mut file = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&temp)?;
    for part in parts {
        file.write_all(part)?;
    }
    drop(file);
    std::fs::rename(temp, dir.join(name))
}

impl SessionBackend for UnpublishedBackend<'_> {
    async fn read_head(&self, s: &SessionAddr) -> Result<Option<(Vec<u8>, Version)>, BackendError> {
        if self.staging {
            if let Some(head) = self.heads.lock().unwrap().get(s) {
                return Ok(Some(head.clone()));
            }
        }
        self.local.read_head(s).await
    }

    async fn write_head(
        &self,
        s: &SessionAddr,
        head: &[u8],
        expected: Option<Version>,
        next: Version,
    ) -> Result<Version, BackendError> {
        let current = self.read_head(s).await?.map(|(_, version)| version);
        if current != expected || expected.is_some_and(|at| next <= at) {
            return Err(BackendError::Fenced);
        }
        if !self.staging {
            let dir = self.local.head_dir(s);
            let versions = dir.join(VERSIONS_DIR);
            broca_owner_only::create_dir_all(&versions)?;
            write_unsynced(
                &versions,
                ".version.tmp",
                &next.to_string(),
                &[&next.to_le_bytes(), head],
            )?;
            self.step(Step::VersionWritten)?;
            write_unsynced(&dir, "head.tmp", "head", &[&next.to_le_bytes(), head])?;
            self.step(Step::HeadWritten)?;
        }
        self.heads
            .lock()
            .unwrap()
            .insert(s.clone(), (head.to_vec(), next));
        self.local.fences.lock().unwrap().insert(s.clone(), next);
        Ok(next)
    }

    async fn create_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.local.fence(s, at)?;
        broca_owner_only::create_dir_all(self.local.log_dir(s))?;
        let file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(self.local.log_path(s, log))?;
        self.local
            .open_logs
            .lock()
            .unwrap()
            .insert((s.clone(), *log), file);
        self.step(Step::LogCreated)
    }

    async fn read_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        range: Range<u64>,
    ) -> Result<Vec<u8>, BackendError> {
        self.local.read_log(s, log, range).await
    }
    async fn log_end(&self, s: &SessionAddr, log: &Id128) -> Result<u64, BackendError> {
        self.local.log_end(s, log).await
    }

    async fn append_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        bytes: &[u8],
        expected_end: u64,
        at: Version,
        _barrier: bool,
    ) -> Result<u64, BackendError> {
        let end = self
            .local
            .append_log(s, log, bytes, expected_end, at, false)
            .await?;
        self.step(Step::LogAppended)?;
        Ok(end)
    }

    async fn truncate_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        to: u64,
        expected_end: u64,
        at: Version,
    ) -> Result<(), BackendError> {
        self.local.fence(s, at)?;
        self.local.with_log(s, log, |file| {
            if file.metadata()?.len() != expected_end || to > expected_end {
                return Err(BackendError::Fenced);
            }
            file.set_len(to)?;
            Ok(())
        })
    }

    async fn delete_log(
        &self,
        s: &SessionAddr,
        log: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.local.fence(s, at)?;
        self.local
            .open_logs
            .lock()
            .unwrap()
            .remove(&(s.clone(), *log));
        remove_file(&self.local.log_path(s, log))
    }

    async fn put_object(
        &self,
        s: &SessionAddr,
        name: &str,
        bytes: &[u8],
        at: Version,
    ) -> Result<(), BackendError> {
        self.local.fence(s, at)?;
        let path = self.local.object_path(s, name)?;
        broca_owner_only::create_dir_all(path.parent().unwrap())?;
        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        file.write_all(bytes)?;
        self.step(Step::ObjectWritten)
    }
    async fn sync_objects(
        &self,
        s: &SessionAddr,
        _change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.local.fence(s, at)
    }
    async fn get_object(
        &self,
        s: &SessionAddr,
        name: &str,
    ) -> Result<Option<Vec<u8>>, BackendError> {
        self.local.get_object(s, name).await
    }
    async fn delete_object(
        &self,
        s: &SessionAddr,
        name: &str,
        at: Version,
    ) -> Result<(), BackendError> {
        self.local.fence(s, at)?;
        remove_file(&self.local.object_path(s, name)?)
    }
    async fn delete_change(
        &self,
        s: &SessionAddr,
        change: &Id128,
        at: Version,
    ) -> Result<(), BackendError> {
        self.local.fence(s, at)?;
        remove_dir(&self.local.object_dir(s).join(hex(&change.0)))
    }
    async fn delete_session(&self, s: &SessionAddr, at: Version) -> Result<(), BackendError> {
        self.local.fence(s, at)?;
        for dir in [
            self.local.head_dir(s),
            self.local.log_dir(s),
            self.local.object_dir(s),
        ] {
            remove_dir(&dir)?;
        }
        self.heads.lock().unwrap().remove(s);
        self.local.fences.lock().unwrap().remove(s);
        self.local
            .open_logs
            .lock()
            .unwrap()
            .retain(|(addr, _), _| addr != s);
        Ok(())
    }
    // Setup belongs to the shared published store, never to a disposable backend. Keeping
    // these methods durable prevents accidentally treating shared parts as scratch data.
    async fn put_setup(&self, name: &str, bytes: &[u8]) -> Result<(), BackendError> {
        self.local.put_setup(name, bytes).await
    }
    async fn get_setup(&self, name: &str) -> Result<Option<Vec<u8>>, BackendError> {
        self.local.get_setup(name).await
    }
    async fn list_setup(&self) -> Result<Vec<String>, BackendError> {
        self.local.list_setup().await
    }
}

fn remove_file(path: &Path) -> Result<(), BackendError> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}
fn remove_dir(path: &Path) -> Result<(), BackendError> {
    match std::fs::remove_dir_all(path) {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_head_updates_keep_the_disk_compare_and_swap() {
        let root = broca_test_support::scratch_root()
            .join(format!("scratch-cas-{}", hex(&crate::keys::random_id().0)));
        let address = SessionAddr("0123456789abcdef".into());
        let live = LocalBackend::new(&root);
        let scratch = UnpublishedBackend::scratch(&root, None);
        futures::executor::block_on(async {
            live.write_head(&address, b"initial", None, 1)
                .await
                .unwrap();
            scratch.read_head(&address).await.unwrap();
            scratch
                .write_head(&address, b"scratch", Some(1), 2)
                .await
                .unwrap();
            live.write_head(&address, b"replaced", Some(2), 3)
                .await
                .unwrap();
            assert!(matches!(
                scratch.write_head(&address, b"stale", Some(2), 4).await,
                Err(BackendError::Fenced)
            ));
            assert_eq!(
                live.read_head(&address).await.unwrap(),
                Some((b"replaced".to_vec(), 3))
            );
        });
        drop(scratch);
        std::fs::remove_dir_all(root).unwrap();
    }
}
