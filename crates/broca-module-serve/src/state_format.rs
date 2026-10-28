//! The state-format marker: which code runs on a state root.
//!
//! Until the cutover, broca carries two implementations of its durable state side by side:
//! today's (the "legacy" format) and the data overhaul's (`broca-state/2`). A file named
//! `state-format` at the top of the state root decides which one runs, by these rules:
//!
//! - A root whose `state-format` reads `broca-state/2` runs the overhaul code. Any other value
//!   refuses to serve and names the runbook.
//! - A root with no `state-format`:
//!   - holding any entry other than `state-format.tmp` and the state-root lock file
//!     (`broca.lock`) is a LEGACY root. Before the cutover build it runs today's code; from the
//!     cutover build on it refuses and names the runbook.
//!   - missing, empty, or holding only `state-format.tmp` and `broca.lock` is a NEW root. The lock
//!     file holds no state and is created before the marker rules run. Before the cutover build
//!     it is started as a legacy root unless `broca.jsonc` sets `"state_format": 2` (tests and
//!     staging only); from the cutover build on it becomes an overhaul root.
//!
//! CREATING AN OVERHAUL ROOT WRITES THE MARKER FIRST: `state-format.tmp`, fsync, rename to
//! `state-format`, fsync the root directory, and only then anything else (logs, leases, WAL,
//! databases). Only the empty state-root lock file comes earlier. A power loss anywhere in that
//! sequence leaves either a root that is still new (empty, or holding only the temporary file and
//! the lock file) or a complete overhaul root. It can never leave a
//! root that holds other files but no marker, which would be read as a legacy root on the next
//! start, and it can never leave an empty or partial `state-format`, which would be refused.
//!
//! AN OVERHAUL ROOT NEVER RUNS TODAY'S DESTRUCTIVE OR EXPORTING BACKGROUND WORK: the retention
//! sweeper, the archive scheduler, the request-dump pruner and the usage (billing) export. Each
//! is listed in [`BackgroundTask`], and startup asks [`StateFormat::start`] before starting it.
//! Those background deletions and exports stay off on an overhaul root until the overhaul's
//! own retention and billing code replaces the tasks. Every other component keeps today's
//! behaviour on an overhaul root until its replacement lands.

use std::ffi::OsString;
use std::fmt;
use std::io;
use std::path::{Path, PathBuf};

use broca_module_serve::state_root_lock::LOCK_FILE;

/// The marker file at the top of a state root.
pub(crate) const MARKER_FILE: &str = "state-format";

/// The temporary file the marker is written to before it is renamed into place.
pub(crate) const MARKER_TMP_FILE: &str = "state-format.tmp";

/// The only marker value this build runs.
pub(crate) const OVERHAUL_FORMAT: &str = "broca-state/2";

/// The bytes written to a new marker.
const MARKER_BYTES: &[u8] = b"broca-state/2\n";

/// The operator's procedure for moving a machine onto a supported state root. Every refusal
/// names it.
pub(crate) const RUNBOOK: &str = "docs/clean-start-runbook.md";

/// The variable that turns on rendered-request dumps, and with them the dump pruner.
pub(crate) const REQUEST_DUMP_DIR_ENV: &str = "BROCA_DUMP_REQUEST_DIR";

/// Whether this build predates the cutover. The cutover build changes [`BUILD_PHASE`] and
/// nothing else here: new roots then become overhaul roots and legacy roots refuse.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CutPhase {
    BeforeCut,
    // Constructed only by tests until the cutover build selects it.
    #[cfg_attr(not(test), allow(dead_code))]
    AfterCut,
}

/// The phase this build runs in.
pub(crate) const BUILD_PHASE: CutPhase = CutPhase::BeforeCut;

/// Which implementation runs on the state root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StateFormat {
    /// Today's format and code.
    Legacy,
    /// `broca-state/2`.
    Overhaul,
}

/// What a state root holds, read before anything is written to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RootContents {
    /// Missing, empty, or holding only `state-format.tmp` and the state-root lock file.
    New,
    /// Holding `state-format`, with these bytes.
    Marked(Vec<u8>),
    /// No `state-format`, but other entries.
    Legacy,
}

/// What startup does with a root.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Decision {
    /// Run this format on the root as it is.
    Run(StateFormat),
    /// Write the overhaul marker, then run the overhaul format.
    CreateOverhaul,
}

/// Why broca will not serve from a state root.
#[derive(Debug)]
pub(crate) enum StateFormatError {
    /// The marker names a format this build does not run.
    UnknownMarker { root: PathBuf, found: String },
    /// A legacy root on a build that no longer runs the legacy format.
    LegacyAfterCut { root: PathBuf },
    /// The root could not be read or the marker could not be written.
    Io {
        action: &'static str,
        path: PathBuf,
        error: io::Error,
    },
}

impl fmt::Display for StateFormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownMarker { root, found } => write!(
                f,
                "refusing to serve: the state root {} is marked {MARKER_FILE} = {found:?}, a \
                 state format this build does not run (it runs only today's format and \
                 {OVERHAUL_FORMAT}). Follow {RUNBOOK} to start this machine on a supported state \
                 root.",
                root.display()
            ),
            Self::LegacyAfterCut { root } => write!(
                f,
                "refusing to serve: the state root {} holds today's state format, which this \
                 build no longer runs. Follow {RUNBOOK} to move it aside and start on an empty \
                 root.",
                root.display()
            ),
            Self::Io {
                action,
                path,
                error,
            } => write!(
                f,
                "cannot {action} {}: {error}. Refusing to start rather than guess which state \
                 format runs there; see {RUNBOOK}.",
                path.display()
            ),
        }
    }
}

impl std::error::Error for StateFormatError {}

fn io_error(action: &'static str, path: &Path) -> impl FnOnce(io::Error) -> StateFormatError {
    let path = path.to_path_buf();
    move |error| StateFormatError::Io {
        action,
        path,
        error,
    }
}

/// The filesystem operations the marker rules use, so a test can stop the sequence at every
/// operation and replay what a power loss could leave behind.
pub(crate) trait MarkerFs {
    /// The entry names in `dir`, or `None` when `dir` does not exist.
    fn list(&self, dir: &Path) -> io::Result<Option<Vec<OsString>>>;
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;
    fn create_dir_all(&self, dir: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    /// Create `path`, which must not exist, holding `bytes`. Does not sync.
    fn create_new(&self, path: &Path, bytes: &[u8]) -> io::Result<()>;
    fn sync_file(&self, path: &Path) -> io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;
    fn sync_dir(&self, dir: &Path) -> io::Result<()>;
}

/// The real filesystem. Directories and files are created owner-only, like the rest of the
/// state root.
pub(crate) struct RealFs;

impl MarkerFs for RealFs {
    fn list(&self, dir: &Path) -> io::Result<Option<Vec<OsString>>> {
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error),
        };
        entries
            .map(|entry| entry.map(|entry| entry.file_name()))
            .collect::<io::Result<Vec<_>>>()
            .map(Some)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
        broca_owner_only::create_dir_all(dir)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        std::fs::remove_file(path)
    }

    fn create_new(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
        let mut file = broca_owner_only::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        io::Write::write_all(&mut file, bytes)
    }

    fn sync_file(&self, path: &Path) -> io::Result<()> {
        // Opened for writing because Windows flushes a file only through a writable handle.
        broca_owner_only::OpenOptions::new()
            .write(true)
            .open(path)?
            .sync_all()
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        std::fs::rename(from, to)
    }

    fn sync_dir(&self, dir: &Path) -> io::Result<()> {
        // Directory handles can be synced on Unix only. Elsewhere the rename is the last step
        // the platform offers, the same choice broca's WAL directory sync makes.
        #[cfg(unix)]
        {
            std::fs::File::open(dir)?.sync_all()
        }
        #[cfg(not(unix))]
        {
            let _ = dir;
            Ok(())
        }
    }
}

/// Read what `root` holds without changing it.
pub(crate) fn inspect_root(
    fs: &impl MarkerFs,
    root: &Path,
) -> Result<RootContents, StateFormatError> {
    let Some(names) = fs
        .list(root)
        .map_err(io_error("list the state root", root))?
    else {
        return Ok(RootContents::New);
    };
    if names.iter().any(|name| name == MARKER_FILE) {
        let marker = root.join(MARKER_FILE);
        let bytes = fs
            .read(&marker)
            .map_err(io_error("read the state-format marker", &marker))?;
        return Ok(RootContents::Marked(bytes));
    }
    // The state-root lock file holds no state, and `ck-broca` creates it before this check runs
    // (see `state_root_lock`), so a root holding only it is still new.
    if names
        .iter()
        .all(|name| name == MARKER_TMP_FILE || name == LOCK_FILE)
    {
        Ok(RootContents::New)
    } else {
        Ok(RootContents::Legacy)
    }
}

/// True when marker bytes name the overhaul format. One trailing newline is allowed, so an
/// operator may write the file with an editor.
fn names_overhaul(bytes: &[u8]) -> bool {
    bytes.strip_suffix(b"\n").unwrap_or(bytes) == OVERHAUL_FORMAT.as_bytes()
}

/// A marker's bytes as an operator can read them in a refusal, without dumping a large file.
fn describe_marker(bytes: &[u8]) -> String {
    const SHOWN: usize = 64;
    let text = String::from_utf8_lossy(&bytes[..bytes.len().min(SHOWN)]).into_owned();
    if bytes.len() > SHOWN {
        format!("{text}... ({} bytes)", bytes.len())
    } else {
        text
    }
}

/// Apply the marker rules to what a root holds.
pub(crate) fn decide(
    root: &Path,
    contents: &RootContents,
    overhaul_opt_in: bool,
    phase: CutPhase,
) -> Result<Decision, StateFormatError> {
    match (contents, phase) {
        (RootContents::Marked(bytes), _) if names_overhaul(bytes) => {
            Ok(Decision::Run(StateFormat::Overhaul))
        }
        (RootContents::Marked(bytes), _) => Err(StateFormatError::UnknownMarker {
            root: root.to_path_buf(),
            found: describe_marker(bytes),
        }),
        (RootContents::Legacy, CutPhase::BeforeCut) => Ok(Decision::Run(StateFormat::Legacy)),
        (RootContents::Legacy, CutPhase::AfterCut) => Err(StateFormatError::LegacyAfterCut {
            root: root.to_path_buf(),
        }),
        (RootContents::New, CutPhase::BeforeCut) if !overhaul_opt_in => {
            Ok(Decision::Run(StateFormat::Legacy))
        }
        (RootContents::New, _) => Ok(Decision::CreateOverhaul),
    }
}

/// Make `root` a complete overhaul root, writing the marker before anything else.
pub(crate) fn create_overhaul_root(
    fs: &impl MarkerFs,
    root: &Path,
) -> Result<(), StateFormatError> {
    let existed = fs
        .list(root)
        .map_err(io_error("list the state root", root))?
        .is_some();
    if !existed {
        fs.create_dir_all(root)
            .map_err(io_error("create the state root", root))?;
        // Sync the parent so the root directory itself survives a power loss. Without it the
        // marker would be durable inside a directory that might not be.
        let parent = match root.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent,
            _ => Path::new("."),
        };
        fs.sync_dir(parent)
            .map_err(io_error("sync the state root's parent directory", parent))?;
    }
    let tmp = root.join(MARKER_TMP_FILE);
    let marker = root.join(MARKER_FILE);
    // An earlier attempt cut by a crash may have left a partial temporary file.
    match fs.remove_file(&tmp) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(io_error("remove the stale marker", &tmp)(error)),
    }
    fs.create_new(&tmp, MARKER_BYTES)
        .map_err(io_error("write the state-format marker", &tmp))?;
    fs.sync_file(&tmp)
        .map_err(io_error("sync the state-format marker", &tmp))?;
    fs.rename(&tmp, &marker)
        .map_err(io_error("install the state-format marker", &marker))?;
    fs.sync_dir(root)
        .map_err(io_error("sync the state root", root))?;
    Ok(())
}

/// Decide which format runs on `root`, creating the overhaul marker when the rules say so.
/// Call before anything else touches the root.
pub(crate) fn prepare_with(
    fs: &impl MarkerFs,
    root: &Path,
    overhaul_opt_in: bool,
    phase: CutPhase,
) -> Result<StateFormat, StateFormatError> {
    let contents = inspect_root(fs, root)?;
    if contents == RootContents::Legacy && overhaul_opt_in && phase == CutPhase::BeforeCut {
        tracing::warn!(
            target: "lifecycle",
            root = %root.display(),
            "broca.jsonc sets state_format 2, but the state root already holds today's format; \
             it keeps running today's format. Only an empty root becomes {OVERHAUL_FORMAT}."
        );
    }
    let format = match decide(root, &contents, overhaul_opt_in, phase)? {
        Decision::Run(format) => format,
        Decision::CreateOverhaul => {
            create_overhaul_root(fs, root)?;
            StateFormat::Overhaul
        }
    };
    tracing::info!(target: "lifecycle", root = %root.display(), ?format, "state format decided");
    Ok(format)
}

/// [`prepare_with`] on the real filesystem, in this build's phase.
pub(crate) fn prepare(root: &Path, overhaul_opt_in: bool) -> Result<StateFormat, StateFormatError> {
    prepare_with(&RealFs, root, overhaul_opt_in, BUILD_PHASE)
}

/// The log frame format every WAL store of this process is built with, decided once here from
/// the root [`prepare`] just decided. The WAL's own classifier reads the marker again and also
/// refuses an overhaul root whose `wal/` is a symbolic link or resolves outside the root; boot
/// stops on any refusal, and on a classification that disagrees with `format`.
pub(crate) fn wal_config(root: &Path, format: StateFormat) -> Result<broca_wal::WalConfig, String> {
    let config = broca_wal::classify_state_root(root)
        .map_err(|error| format!("refusing to serve: {error}. See {RUNBOOK}."))?;
    if config.is_overhaul() != (format == StateFormat::Overhaul) {
        return Err(format!(
            "refusing to serve: the state root {} was decided as {format:?} but its log format \
             reads as {:?}; it changed during startup. See {RUNBOOK}.",
            root.display(),
            config.format
        ));
    }
    Ok(config)
}

/// Today's background work that deletes or exports state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BackgroundTask {
    /// The session retention sweeper, and System One's batch sweep beside it.
    RetentionSweeper,
    /// The idle-WAL archive timer.
    ArchiveScheduler,
    /// The rendered-request dump pruner, which runs whenever dumps are written.
    RequestDumpPruner,
    /// The usage export: the startup catch-up, the per-run export facts and System One's
    /// export bridge.
    UsageExport,
}

impl BackgroundTask {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const ALL: [Self; 4] = [
        Self::RetentionSweeper,
        Self::ArchiveScheduler,
        Self::RequestDumpPruner,
        Self::UsageExport,
    ];
}

impl StateFormat {
    /// Whether this format runs `task`. Each task has its own arm so one can later be
    /// handed to its overhaul replacement without touching the others.
    pub(crate) fn runs(self, task: BackgroundTask) -> bool {
        match (self, task) {
            (Self::Legacy, _) => true,
            (
                Self::Overhaul,
                BackgroundTask::RetentionSweeper
                | BackgroundTask::ArchiveScheduler
                | BackgroundTask::RequestDumpPruner
                | BackgroundTask::UsageExport,
            ) => false,
        }
    }

    /// Run `start` only when this format runs `task`; otherwise say it was skipped.
    pub(crate) fn start<T>(self, task: BackgroundTask, start: impl FnOnce() -> T) -> Option<T> {
        if self.runs(task) {
            Some(start())
        } else {
            tracing::info!(
                target: "lifecycle",
                ?task,
                "not started: an {OVERHAUL_FORMAT} root runs none of today's deleting or \
                 exporting background work"
            );
            None
        }
    }

    /// The dump pruner lives in the provider transport and runs whenever a dump is written, so
    /// on a format that must not run it, startup refuses while dumps are turned on rather than
    /// starting it behind this check's back.
    pub(crate) fn check_request_dumps(
        self,
        env: impl Fn(&str) -> Option<OsString>,
    ) -> Result<(), String> {
        if self.runs(BackgroundTask::RequestDumpPruner) {
            return Ok(());
        }
        match env(REQUEST_DUMP_DIR_ENV) {
            Some(dir) if !dir.is_empty() => Err(format!(
                "refusing to start: {REQUEST_DUMP_DIR_ENV} is set, but this is an \
                 {OVERHAUL_FORMAT} state root, which runs neither request dumps nor their \
                 pruner yet. Unset {REQUEST_DUMP_DIR_ENV}."
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;
    use std::collections::{BTreeMap, BTreeSet};
    use std::sync::atomic::{AtomicU64, Ordering};

    // ---- Real-filesystem fixtures ----

    /// A scratch directory under broca's owner-only test root, removed on drop. The state root
    /// under test is `<scratch>/root`, which does not exist until a test creates it.
    struct Scratch(PathBuf);

    impl Scratch {
        fn new(tag: &str) -> Self {
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let dir = broca_test_support::scratch_root().join(format!(
                "broca-state-format-{tag}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).expect("create scratch dir");
            Self(dir)
        }

        fn root(&self) -> PathBuf {
            self.0.join("root")
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// Every entry under `root`, recursively, with file bytes; `None` for a directory.
    fn tree(root: &Path) -> BTreeMap<PathBuf, Option<Vec<u8>>> {
        let mut out = BTreeMap::new();
        let mut pending = vec![root.to_path_buf()];
        while let Some(dir) = pending.pop() {
            let Ok(entries) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in entries {
                let path = entry.expect("read entry").path();
                let relative = path.strip_prefix(root).unwrap().to_path_buf();
                if path.is_dir() {
                    out.insert(relative, None);
                    pending.push(path);
                } else {
                    out.insert(relative, Some(std::fs::read(&path).expect("read file")));
                }
            }
        }
        out
    }

    // ---- The power-loss shim ----
    //
    // A model of one state root and its parent directory that records every filesystem
    // operation, then enumerates what a power loss after any prefix of those operations can
    // leave behind: for each object (the parent directory, the root directory, each file's
    // contents) everything synced survives, plus any in-order prefix of the operations issued
    // after its last sync, chosen independently per object. File contents may survive torn at
    // any byte. This models which filesystem operations can survive a power loss.

    /// A surviving state: whether the root directory exists, and its files.
    #[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
    struct Snapshot {
        root_exists: bool,
        files: BTreeMap<String, Vec<u8>>,
    }

    impl Snapshot {
        fn missing() -> Self {
            Self {
                root_exists: false,
                files: BTreeMap::new(),
            }
        }

        fn with(files: &[(&str, &[u8])]) -> Self {
            Self {
                root_exists: true,
                files: files
                    .iter()
                    .map(|(name, bytes)| ((*name).to_owned(), bytes.to_vec()))
                    .collect(),
            }
        }
    }

    #[derive(Clone, Debug, PartialEq, Eq)]
    enum Op {
        CreateRoot,
        SyncParent,
        Link { name: String, inode: usize },
        Unlink { name: String },
        Rename { from: String, to: String },
        Append { inode: usize, bytes: Vec<u8> },
        SyncFile { inode: usize },
        SyncRoot,
    }

    #[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
    enum Object {
        Parent,
        Root,
        Inode(usize),
    }

    impl Op {
        /// The object this operation changes, or for a sync, the object it makes durable.
        fn object(&self) -> Object {
            match self {
                Op::CreateRoot | Op::SyncParent => Object::Parent,
                Op::Link { .. } | Op::Unlink { .. } | Op::Rename { .. } | Op::SyncRoot => {
                    Object::Root
                }
                Op::Append { inode, .. } | Op::SyncFile { inode } => Object::Inode(*inode),
            }
        }

        fn is_sync(&self) -> bool {
            matches!(self, Op::SyncParent | Op::SyncRoot | Op::SyncFile { .. })
        }
    }

    struct Volatile {
        root_exists: bool,
        entries: BTreeMap<String, usize>,
        data: Vec<Vec<u8>>,
    }

    struct Shim {
        parent: PathBuf,
        root: PathBuf,
        /// The durable state the run started from; its files are inodes `0..base.files.len()`.
        base: Snapshot,
        state: RefCell<Volatile>,
        ops: RefCell<Vec<Op>>,
    }

    impl Shim {
        fn new(base: Snapshot) -> Self {
            let parent = PathBuf::from("/model");
            let root = parent.join("root");
            let entries = base
                .files
                .keys()
                .enumerate()
                .map(|(inode, name)| (name.clone(), inode))
                .collect();
            let data = base.files.values().cloned().collect();
            Self {
                parent,
                root,
                state: RefCell::new(Volatile {
                    root_exists: base.root_exists,
                    entries,
                    data,
                }),
                base,
                ops: RefCell::new(Vec::new()),
            }
        }

        fn name_in_root(&self, path: &Path) -> io::Result<String> {
            if path.parent() != Some(self.root.as_path()) {
                panic!("the model holds only files directly in the root, not {path:?}");
            }
            if !self.state.borrow().root_exists {
                return Err(io::ErrorKind::NotFound.into());
            }
            Ok(path.file_name().unwrap().to_str().unwrap().to_owned())
        }

        fn inode(&self, path: &Path) -> io::Result<usize> {
            let name = self.name_in_root(path)?;
            self.state
                .borrow()
                .entries
                .get(&name)
                .copied()
                .ok_or_else(|| io::ErrorKind::NotFound.into())
        }

        fn record(&self, op: Op) {
            self.ops.borrow_mut().push(op);
        }

        fn ops(&self) -> Vec<Op> {
            self.ops.borrow().clone()
        }

        /// Every state a power loss after the first `cut` recorded operations can leave.
        fn surviving(&self, cut: usize) -> BTreeSet<Snapshot> {
            let ops = &self.ops.borrow()[..cut];
            let base_entries: BTreeMap<String, usize> = self
                .base
                .files
                .keys()
                .enumerate()
                .map(|(inode, name)| (name.clone(), inode))
                .collect();
            let mut base_data: Vec<Vec<u8>> = self.base.files.values().cloned().collect();
            let mut objects: BTreeSet<Object> = [Object::Parent, Object::Root].into();
            for op in ops {
                objects.insert(op.object());
                if let Op::Link { inode, .. } = op {
                    while base_data.len() <= *inode {
                        base_data.push(Vec::new());
                    }
                }
            }
            // For each object, the range of how much of its unsynced work survives: a count of
            // operations for directories, a byte length for file contents.
            let mut ranges: Vec<(Object, usize, usize)> = Vec::new();
            for object in &objects {
                let mine: Vec<(usize, &Op)> = ops
                    .iter()
                    .enumerate()
                    .filter(|(_, op)| op.object() == *object)
                    .collect();
                let last_sync = mine
                    .iter()
                    .rev()
                    .find(|(_, op)| op.is_sync())
                    .map(|(index, _)| *index);
                let synced = |index: usize| last_sync.is_some_and(|sync| index < sync);
                let changes = mine.iter().filter(|(_, op)| !op.is_sync());
                match object {
                    Object::Parent | Object::Root => {
                        let durable = changes.clone().filter(|(i, _)| synced(*i)).count();
                        ranges.push((*object, durable, changes.count()));
                    }
                    Object::Inode(inode) => {
                        let base_len = base_data[*inode].len();
                        let mut durable = base_len;
                        let mut issued = base_len;
                        for (index, op) in changes {
                            let Op::Append { bytes, .. } = op else {
                                unreachable!("only appends change file contents")
                            };
                            issued += bytes.len();
                            if synced(*index) {
                                durable = issued;
                            }
                        }
                        ranges.push((*object, durable, issued));
                    }
                }
            }
            // Full contents of every file as issued; a surviving state keeps a prefix of each.
            let mut contents = base_data.clone();
            for op in ops {
                if let Op::Append { inode, bytes } = op {
                    contents[*inode].extend_from_slice(bytes);
                }
            }
            let mut out = BTreeSet::new();
            let mut choice = vec![0usize; ranges.len()];
            enumerate(&ranges, 0, &mut choice, &mut |choice| {
                let kept = |object: Object| {
                    let index = ranges.iter().position(|(o, _, _)| *o == object).unwrap();
                    choice[index]
                };
                let root_exists = self.base.root_exists || kept(Object::Parent) > 0;
                if !root_exists {
                    out.insert(Snapshot::missing());
                    return;
                }
                let mut entries = base_entries.clone();
                let root_ops = ops
                    .iter()
                    .filter(|op| op.object() == Object::Root && !op.is_sync())
                    .take(kept(Object::Root));
                for op in root_ops {
                    match op {
                        Op::Link { name, inode } => {
                            entries.insert(name.clone(), *inode);
                        }
                        Op::Unlink { name } => {
                            entries.remove(name);
                        }
                        Op::Rename { from, to } => {
                            let inode = entries.remove(from).expect("rename source");
                            entries.insert(to.clone(), inode);
                        }
                        _ => unreachable!(),
                    }
                }
                let files = entries
                    .into_iter()
                    .map(|(name, inode)| {
                        let length = if objects.contains(&Object::Inode(inode)) {
                            kept(Object::Inode(inode))
                        } else {
                            contents[inode].len()
                        };
                        (name, contents[inode][..length].to_vec())
                    })
                    .collect();
                out.insert(Snapshot {
                    root_exists: true,
                    files,
                });
            });
            out
        }
    }

    fn enumerate(
        ranges: &[(Object, usize, usize)],
        at: usize,
        choice: &mut Vec<usize>,
        visit: &mut impl FnMut(&[usize]),
    ) {
        if at == ranges.len() {
            visit(choice);
            return;
        }
        for kept in ranges[at].1..=ranges[at].2 {
            choice[at] = kept;
            enumerate(ranges, at + 1, choice, visit);
        }
    }

    impl MarkerFs for Shim {
        fn list(&self, dir: &Path) -> io::Result<Option<Vec<OsString>>> {
            assert_eq!(dir, self.root, "the model lists only the root");
            let state = self.state.borrow();
            Ok(state
                .root_exists
                .then(|| state.entries.keys().map(OsString::from).collect()))
        }

        fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
            let inode = self.inode(path)?;
            Ok(self.state.borrow().data[inode].clone())
        }

        fn create_dir_all(&self, dir: &Path) -> io::Result<()> {
            assert_eq!(dir, self.root, "the model creates only the root");
            let mut state = self.state.borrow_mut();
            if !state.root_exists {
                state.root_exists = true;
                drop(state);
                self.record(Op::CreateRoot);
            }
            Ok(())
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            let name = self.name_in_root(path)?;
            if self.state.borrow_mut().entries.remove(&name).is_none() {
                return Err(io::ErrorKind::NotFound.into());
            }
            self.record(Op::Unlink { name });
            Ok(())
        }

        fn create_new(&self, path: &Path, bytes: &[u8]) -> io::Result<()> {
            let name = self.name_in_root(path)?;
            let mut state = self.state.borrow_mut();
            if state.entries.contains_key(&name) {
                return Err(io::ErrorKind::AlreadyExists.into());
            }
            let inode = state.data.len();
            state.data.push(bytes.to_vec());
            state.entries.insert(name.clone(), inode);
            drop(state);
            self.record(Op::Link { name, inode });
            self.record(Op::Append {
                inode,
                bytes: bytes.to_vec(),
            });
            Ok(())
        }

        fn sync_file(&self, path: &Path) -> io::Result<()> {
            let inode = self.inode(path)?;
            self.record(Op::SyncFile { inode });
            Ok(())
        }

        fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
            let (from, to) = (self.name_in_root(from)?, self.name_in_root(to)?);
            let mut state = self.state.borrow_mut();
            let inode = state.entries.remove(&from).ok_or(io::ErrorKind::NotFound)?;
            state.entries.insert(to.clone(), inode);
            drop(state);
            self.record(Op::Rename { from, to });
            Ok(())
        }

        fn sync_dir(&self, dir: &Path) -> io::Result<()> {
            if dir == self.root {
                self.record(Op::SyncRoot);
            } else if dir == self.parent {
                self.record(Op::SyncParent);
            } else {
                panic!("the model syncs only the root and its parent, not {dir:?}");
            }
            Ok(())
        }
    }

    /// Startup on an opted-in root as `ck-broca` runs it: take the state-root lock (create a
    /// missing root, sync its parent, create the empty lock file, as `state_root_lock::acquire`
    /// does), decide the format, then (standing in for the lease store, `broca.db` and the logs)
    /// write and sync one more file.
    fn start_opted_in(shim: &Shim) {
        if shim.list(&shim.root).unwrap().is_none() {
            shim.create_dir_all(&shim.root).unwrap();
            shim.sync_dir(&shim.parent).unwrap();
        }
        let lock = shim.root.join(LOCK_FILE);
        if shim.inode(&lock).is_err() {
            shim.create_new(&lock, b"").unwrap();
        }
        let format = prepare_with(shim, &shim.root, true, CutPhase::BeforeCut)
            .expect("an opted-in start never refuses a new or overhaul root");
        assert_eq!(format, StateFormat::Overhaul);
        let index = shim.root.join("broca.db");
        if shim.inode(&index).is_err() {
            shim.create_new(&index, b"index").unwrap();
            shim.sync_file(&index).unwrap();
        }
    }

    /// The power-loss invariant: a surviving state is new or a complete overhaul root.
    fn assert_new_or_complete(state: &Snapshot, context: &str) {
        let shim = Shim::new(state.clone());
        let contents = inspect_root(&shim, &shim.root).expect("the model never fails a read");
        match &contents {
            RootContents::New => {}
            RootContents::Marked(bytes) if names_overhaul(bytes) => {}
            other => panic!("{context}: surviving state {state:?} reads as {other:?}"),
        }
        decide(&shim.root, &contents, true, CutPhase::BeforeCut)
            .unwrap_or_else(|error| panic!("{context}: {state:?} refused: {error}"));
    }

    /// Run startup from `start`, cut it after every operation, and check every surviving state;
    /// then restart from each surviving state and cut that recovery too. Returns every state seen.
    fn cut_everywhere(start: Snapshot) -> BTreeSet<Snapshot> {
        let mut seen = BTreeSet::new();
        let first = Shim::new(start.clone());
        start_opted_in(&first);
        for cut in 0..=first.ops().len() {
            for state in first.surviving(cut) {
                assert_new_or_complete(&state, &format!("from {start:?}, cut after {cut}"));
                // A second power loss during the restart's own recovery.
                let again = Shim::new(state.clone());
                start_opted_in(&again);
                for second in 0..=again.ops().len() {
                    for after in again.surviving(second) {
                        assert_new_or_complete(
                            &after,
                            &format!("from {state:?}, second cut after {second}"),
                        );
                        seen.insert(after);
                    }
                }
                seen.insert(state);
            }
        }
        seen
    }

    // ---- Acceptance ----

    #[test]
    fn marker_written_before_any_file() {
        // The recorded sequence: the marker is durable before any other file is created.
        let shim = Shim::new(Snapshot::missing());
        start_opted_in(&shim);
        let ops = shim.ops();
        let installed = ops
            .iter()
            .position(|op| {
                *op == Op::Rename {
                    from: MARKER_TMP_FILE.into(),
                    to: MARKER_FILE.into(),
                }
            })
            .expect("the marker is renamed into place");
        let root_synced = installed
            + ops[installed..]
                .iter()
                .position(|op| *op == Op::SyncRoot)
                .expect("the root is synced after the rename");
        let tmp_inode = ops
            .iter()
            .find_map(|op| match op {
                Op::Link { name, inode } if name == MARKER_TMP_FILE => Some(*inode),
                _ => None,
            })
            .expect("the marker is written to its temporary file");
        let tmp_synced = ops
            .iter()
            .position(|op| *op == Op::SyncFile { inode: tmp_inode })
            .expect("the temporary marker is synced");
        assert!(tmp_synced < installed, "synced before the rename: {ops:?}");
        for (index, op) in ops.iter().enumerate() {
            if let Op::Link { name, .. } = op {
                assert!(
                    name == MARKER_TMP_FILE || name == LOCK_FILE || index > root_synced,
                    "{name} created before the marker was durable: {ops:?}"
                );
            }
        }
        assert!(
            ops.iter()
                .any(|op| matches!(op, Op::Link { name, .. } if name == "broca.db")),
            "the stand-in startup file was written, so the order above was checked: {ops:?}"
        );

        // The real filesystem: a missing root and a root holding a stale partial temporary
        // marker both end up holding the marker alone, with its exact bytes.
        for stale in [None, Some(&b"broca-st"[..])] {
            let scratch = Scratch::new("marker-first");
            let root = scratch.root();
            if let Some(bytes) = stale {
                std::fs::create_dir_all(&root).unwrap();
                std::fs::write(root.join(MARKER_TMP_FILE), bytes).unwrap();
            }
            let format = prepare_with(&RealFs, &root, true, CutPhase::BeforeCut).unwrap();
            assert_eq!(format, StateFormat::Overhaul);
            assert_eq!(
                tree(&root),
                BTreeMap::from([(PathBuf::from(MARKER_FILE), Some(MARKER_BYTES.to_vec()))]),
                "stale temporary marker: {stale:?}"
            );
            // The next start reads it back as an overhaul root and writes nothing.
            assert_eq!(
                prepare_with(&RealFs, &root, false, CutPhase::BeforeCut).unwrap(),
                StateFormat::Overhaul
            );
            assert_eq!(tree(&root).len(), 1);
        }
    }

    #[test]
    fn legacy_root_runs_legacy_before_cut() {
        let scratch = Scratch::new("legacy");
        let root = scratch.root();
        std::fs::create_dir_all(root.join("wal")).unwrap();
        std::fs::write(root.join("run-index.db"), b"index").unwrap();
        // A stale temporary marker does not make a root holding state new.
        std::fs::write(root.join(MARKER_TMP_FILE), b"broca-state/2\n").unwrap();
        let before = tree(&root);
        for opt_in in [false, true] {
            assert_eq!(
                prepare_with(&RealFs, &root, opt_in, CutPhase::BeforeCut).unwrap(),
                StateFormat::Legacy,
                "opt-in {opt_in}"
            );
            assert_eq!(
                tree(&root),
                before,
                "a legacy root is not touched (opt-in {opt_in})"
            );
        }
        // From the cutover build on, the same root refuses and names the runbook.
        let error = prepare_with(&RealFs, &root, false, CutPhase::AfterCut).unwrap_err();
        assert!(
            matches!(error, StateFormatError::LegacyAfterCut { .. }),
            "{error:?}"
        );
        assert!(error.to_string().contains(RUNBOOK), "{error}");
        assert_eq!(tree(&root), before);
    }

    #[test]
    fn empty_root_is_legacy_without_opt_in() {
        type Setup = fn(&Path);
        let setups: [(&str, Setup); 5] = [
            ("missing", |_| {}),
            ("empty", |root| std::fs::create_dir_all(root).unwrap()),
            ("only a temporary marker", |root| {
                std::fs::create_dir_all(root).unwrap();
                std::fs::write(root.join(MARKER_TMP_FILE), b"broca-").unwrap();
            }),
            // `ck-broca` creates the lock file before the marker rules run.
            ("only the state-root lock", |root| {
                std::fs::create_dir_all(root).unwrap();
                std::fs::write(root.join(LOCK_FILE), b"").unwrap();
            }),
            ("the lock and a temporary marker", |root| {
                std::fs::create_dir_all(root).unwrap();
                std::fs::write(root.join(LOCK_FILE), b"").unwrap();
                std::fs::write(root.join(MARKER_TMP_FILE), b"broca-").unwrap();
            }),
        ];
        for (name, setup) in setups {
            let scratch = Scratch::new("empty");
            let root = scratch.root();
            setup(&root);
            let before = tree(&root);
            assert_eq!(
                inspect_root(&RealFs, &root).unwrap(),
                RootContents::New,
                "{name}"
            );
            assert_eq!(
                prepare_with(&RealFs, &root, false, CutPhase::BeforeCut).unwrap(),
                StateFormat::Legacy,
                "{name}: without the opt-in a new root starts as today's format"
            );
            assert_eq!(tree(&root), before, "{name}: no marker without the opt-in");
            assert_eq!(
                prepare_with(&RealFs, &root, true, CutPhase::BeforeCut).unwrap(),
                StateFormat::Overhaul,
                "{name}: with the opt-in it becomes an overhaul root"
            );
            assert_eq!(
                std::fs::read(root.join(MARKER_FILE)).unwrap(),
                MARKER_BYTES,
                "{name}"
            );
        }
        // From the cutover build on, a new root becomes an overhaul root without the opt-in.
        assert_eq!(
            decide(
                Path::new("/r"),
                &RootContents::New,
                false,
                CutPhase::AfterCut
            )
            .unwrap(),
            Decision::CreateOverhaul
        );
    }

    #[test]
    fn unknown_marker_refuses_with_runbook() {
        let unknown: [&[u8]; 6] = [
            b"broca-state/3\n",
            b"broca-state/1",
            b"",
            b"broca-state/",
            b"broca-state/2\n\n",
            b" broca-state/2",
        ];
        for marker in unknown {
            for (opt_in, phase) in [
                (false, CutPhase::BeforeCut),
                (true, CutPhase::BeforeCut),
                (false, CutPhase::AfterCut),
            ] {
                let scratch = Scratch::new("unknown");
                let root = scratch.root();
                std::fs::create_dir_all(root.join("wal")).unwrap();
                std::fs::write(root.join(MARKER_FILE), marker).unwrap();
                let before = tree(&root);
                let error = prepare_with(&RealFs, &root, opt_in, phase).unwrap_err();
                let StateFormatError::UnknownMarker { found, .. } = &error else {
                    panic!("{marker:?}: {error:?}");
                };
                assert_eq!(found.as_bytes(), marker, "the refusal shows what was found");
                let message = error.to_string();
                assert!(message.contains(RUNBOOK), "{message}");
                assert!(message.contains(&root.display().to_string()), "{message}");
                assert_eq!(tree(&root), before, "a refused root is not touched");
            }
        }
        // The overhaul marker with or without its trailing newline runs, whatever else the root
        // holds.
        for marker in [&b"broca-state/2"[..], b"broca-state/2\n"] {
            let scratch = Scratch::new("known");
            let root = scratch.root();
            std::fs::create_dir_all(root.join("wal")).unwrap();
            std::fs::write(root.join(MARKER_FILE), marker).unwrap();
            assert_eq!(
                prepare_with(&RealFs, &root, false, CutPhase::BeforeCut).unwrap(),
                StateFormat::Overhaul
            );
        }
        // A long marker is shown truncated.
        let long = describe_marker(&[b'x'; 200]);
        assert!(long.ends_with("... (200 bytes)"), "{long}");
    }

    #[test]
    fn overhaul_root_starts_no_sweeper_archiver_pruner_or_export() {
        let named = [
            BackgroundTask::RetentionSweeper,
            BackgroundTask::ArchiveScheduler,
            BackgroundTask::RequestDumpPruner,
            BackgroundTask::UsageExport,
        ];
        assert_eq!(
            BackgroundTask::ALL,
            named,
            "each of the four is asserted below"
        );
        for task in BackgroundTask::ALL {
            let mut started = false;
            assert!(StateFormat::Overhaul
                .start(task, || started = true)
                .is_none());
            assert!(!started, "{task:?} started on an overhaul root");
            assert!(!StateFormat::Overhaul.runs(task), "{task:?}");
            // The control: today's format still starts every one of them.
            let mut started = false;
            assert!(StateFormat::Legacy.start(task, || started = true).is_some());
            assert!(started, "{task:?} not started on a legacy root");
        }
        // The dump pruner runs inside the provider transport whenever a dump is written, so an
        // overhaul root refuses to start with dumps turned on.
        let dumps_on = |key: &str| (key == REQUEST_DUMP_DIR_ENV).then(|| OsString::from("/d"));
        let dumps_empty = |key: &str| (key == REQUEST_DUMP_DIR_ENV).then(OsString::new);
        let error = StateFormat::Overhaul
            .check_request_dumps(dumps_on)
            .unwrap_err();
        assert!(error.contains(REQUEST_DUMP_DIR_ENV), "{error}");
        assert!(StateFormat::Overhaul
            .check_request_dumps(dumps_empty)
            .is_ok());
        assert!(StateFormat::Overhaul.check_request_dumps(|_| None).is_ok());
        assert!(StateFormat::Legacy.check_request_dumps(dumps_on).is_ok());
    }

    #[test]
    fn power_loss_while_creating_an_overhaul_root_leaves_new_or_complete() {
        let mut seen = BTreeSet::new();
        for start in [
            Snapshot::missing(),
            Snapshot::with(&[]),
            Snapshot::with(&[(MARKER_TMP_FILE, b"broca")]),
            // An overhaul root created before the lock file existed.
            Snapshot::with(&[(MARKER_FILE, MARKER_BYTES)]),
        ] {
            seen.extend(cut_everywhere(start));
        }
        // The enumeration reached every kind of state the invariant allows, so it was exercised.
        // The lock file is created before the marker, so every state after that point contains
        // the lock file.
        let lock: (&str, &[u8]) = (LOCK_FILE, b"");
        let started = Snapshot::with(&[(MARKER_FILE, MARKER_BYTES), ("broca.db", b"index"), lock]);
        for expected in [
            Snapshot::missing(),
            Snapshot::with(&[]),
            Snapshot::with(&[lock]),
            Snapshot::with(&[(MARKER_TMP_FILE, b""), lock]),
            Snapshot::with(&[(MARKER_TMP_FILE, b"broca-st"), lock]),
            Snapshot::with(&[(MARKER_FILE, MARKER_BYTES), lock]),
            started,
        ] {
            assert!(seen.contains(&expected), "never produced {expected:?}");
        }
    }

    #[test]
    fn the_wal_config_follows_the_decided_format() {
        let scratch = Scratch::new("wal-config");
        let root = scratch.root();
        // A new root without the opt-in runs today's format, and its logs are legacy.
        let format = prepare_with(&RealFs, &root, false, CutPhase::BeforeCut).unwrap();
        assert_eq!(
            wal_config(&root, format).unwrap(),
            broca_wal::WalConfig::LEGACY
        );
        // With the opt-in it becomes an overhaul root, and its logs are version 4.
        let format = prepare_with(&RealFs, &root, true, CutPhase::BeforeCut).unwrap();
        assert_eq!(format, StateFormat::Overhaul);
        assert_eq!(
            wal_config(&root, format).unwrap(),
            broca_wal::WalConfig::OVERHAUL
        );
        // A decision the marker no longer matches refuses rather than pick one.
        let error = wal_config(&root, StateFormat::Legacy).unwrap_err();
        assert!(error.contains(RUNBOOK), "{error}");
    }

    #[test]
    fn boot_refuses_an_unreadable_marker() {
        // A directory where the marker belongs cannot be read as a file, whoever runs the test.
        let scratch = Scratch::new("wal-config-unreadable");
        let root = scratch.root();
        std::fs::create_dir_all(root.join(MARKER_FILE)).unwrap();
        for format in [StateFormat::Legacy, StateFormat::Overhaul] {
            let error = wal_config(&root, format).unwrap_err();
            assert!(error.contains(MARKER_FILE), "{error}");
            assert!(error.contains(RUNBOOK), "{error}");
        }
        // The marker rules refuse the same root before the WAL classifier is asked.
        assert!(prepare_with(&RealFs, &root, false, CutPhase::BeforeCut).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn boot_refuses_an_overhaul_root_whose_wal_is_a_symlink() {
        let scratch = Scratch::new("wal-config-symlink");
        let root = scratch.root();
        let format = prepare_with(&RealFs, &root, true, CutPhase::BeforeCut).unwrap();
        assert_eq!(format, StateFormat::Overhaul);
        let elsewhere = scratch.0.join("elsewhere");
        std::fs::create_dir_all(&elsewhere).unwrap();
        std::os::unix::fs::symlink(&elsewhere, root.join("wal")).unwrap();
        let error = wal_config(&root, format).unwrap_err();
        assert!(error.contains("symbolic link"), "{error}");
    }

    /// The shim can fail the test above: renaming the marker before syncing it leaves a state
    /// with an empty marker, which the next start refuses.
    #[test]
    fn power_loss_check_catches_a_marker_renamed_before_its_sync() {
        let shim = Shim::new(Snapshot::missing());
        let (tmp, marker) = (shim.root.join(MARKER_TMP_FILE), shim.root.join(MARKER_FILE));
        shim.create_dir_all(&shim.root).unwrap();
        shim.sync_dir(&shim.parent).unwrap();
        shim.create_new(&tmp, MARKER_BYTES).unwrap();
        shim.rename(&tmp, &marker).unwrap();
        shim.sync_dir(&shim.root).unwrap();
        let refused: BTreeSet<Snapshot> = (0..=shim.ops().len())
            .flat_map(|cut| shim.surviving(cut))
            .filter(|state| {
                let model = Shim::new(state.clone());
                let contents = inspect_root(&model, &model.root).unwrap();
                decide(&model.root, &contents, true, CutPhase::BeforeCut).is_err()
            })
            .collect();
        assert!(
            refused.contains(&Snapshot::with(&[(MARKER_FILE, b"")])),
            "the unsynced rename must be able to leave an empty marker: {refused:?}"
        );
    }
}
