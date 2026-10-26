//! Operation-level power-loss model, backed by real files so recovery uses the real WAL.
//!
//! File contents and directory entries persist independently. A barrier protects every
//! earlier operation in its stream; later writes and unrelated directory operations may
//! persist in any subset, including a later block while an earlier block is lost.
//! Unsynced writes can tear: persisted length and persisted data are independent, and
//! missing extension blocks read as zeros. Blocks are 4096 bytes. To bound exhaustive
//! enumeration, tear lengths are write/frame/header/block boundaries and one byte on
//! either side of each boundary, not every interior byte. Every block subset and every
//! combination of these tear lengths is visited at each cut, or the budget refuses the
//! proof without sampling. A conservative recursion budget also refuses oversized
//! histories before traversal can exhaust the call stack. Synced writes are applied whole.
//!
//! Same-directory rename is assumed crash-atomic: removal and replacement persist
//! together. Proofs require journaled metadata with that contract (APFS, ext4 or NTFS),
//! not just POSIX rename visibility. Across directories the two entries persist
//! independently; if both survive, they retain the same file inode.
//!
//! Non-Unix recordings must start from an empty root: stable Windows std cannot expose
//! link identity/count, so existing files cannot safely be distinguished from hard links.
//! Programs must perform relevant I/O through `broca_wal::fs_ops` on one thread; direct
//! syscalls outside that seam are not evidence of durability.

use std::{
    collections::{BTreeMap, BTreeSet},
    fs::{self, Metadata},
    io::{self, Read, Seek, SeekFrom, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
};

use broca_wal::fs_ops::{with_fs_ops, FileOps, FsOps, OpenOptions, RealFs};

type Inode = usize;
pub const BLOCK_SIZE: usize = 4096;
const MAX_ENUMERATION_DEPTH: usize = 128;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Node {
    Directory,
    File(Inode),
}

#[derive(Clone, Debug)]
enum Change {
    Write {
        inode: Inode,
        offset: u64,
        bytes: Vec<u8>,
    },
    Resize {
        inode: Inode,
        len: u64,
    },
    Entry {
        path: PathBuf,
        node: Option<Node>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Stream {
    File(Inode),
    Directory(PathBuf),
}

impl Change {
    fn stream(&self) -> Stream {
        match self {
            Self::Write { inode, .. } | Self::Resize { inode, .. } => Stream::File(*inode),
            Self::Entry { path, .. } => Stream::Directory(path.parent().unwrap().to_path_buf()),
        }
    }
}

#[derive(Clone, Debug)]
enum Operation {
    Observe,
    Change(Vec<Change>),
    Sync(Stream),
}

#[derive(Clone, Default)]
struct Image {
    entries: BTreeMap<PathBuf, Node>,
    contents: BTreeMap<Inode, Vec<u8>>,
}

impl Image {
    fn apply(&mut self, change: &Change) {
        match change {
            Change::Entry {
                path,
                node: Some(node),
            } => {
                self.entries.insert(path.clone(), node.clone());
            }
            Change::Entry { path, node: None } => {
                self.entries.remove(path);
            }
            Change::Resize { inode, len } => {
                self.contents
                    .entry(*inode)
                    .or_default()
                    .resize(*len as usize, 0);
            }
            Change::Write {
                inode,
                offset,
                bytes,
            } => {
                let contents = self.contents.entry(*inode).or_default();
                let end = *offset as usize + bytes.len();
                if contents.len() < end {
                    contents.resize(end, 0);
                }
                contents[*offset as usize..end].copy_from_slice(bytes);
            }
        }
    }

    fn visible(&self) -> State {
        let mut state = State::default();
        state.directories.insert(PathBuf::new());
        let mut first_name: BTreeMap<Inode, PathBuf> = BTreeMap::new();
        // A persisted child cannot be reached if its parent's entry was lost.
        let mut entries: Vec<_> = self.entries.iter().collect();
        entries.sort_by_key(|(path, _)| path.components().count());
        for (path, node) in entries {
            if path.as_os_str().is_empty() || !state.directories.contains(path.parent().unwrap()) {
                continue;
            }
            match node {
                Node::Directory => {
                    state.directories.insert(path.clone());
                }
                Node::File(inode) => {
                    if let Some(first) = first_name.get(inode) {
                        state.hard_links.insert(path.clone(), first.clone());
                    } else {
                        first_name.insert(*inode, path.clone());
                    }
                    state.files.insert(
                        path.clone(),
                        self.contents.get(inode).cloned().unwrap_or_default(),
                    );
                }
            }
        }
        state
    }
}

/// A surviving filesystem, with paths relative to the recording root.
#[derive(Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
pub struct State {
    pub files: BTreeMap<PathBuf, Vec<u8>>,
    pub directories: BTreeSet<PathBuf>,
    /// Additional names for an inode, mapped to its first visible name. A cut
    /// between two directory barriers can leave both sides of a rename visible.
    pub hard_links: BTreeMap<PathBuf, PathBuf>,
}

impl State {
    /// Install into an absent or empty root. Existing data is never overwritten.
    pub fn install(&self, root: &Path) -> io::Result<()> {
        fs::create_dir_all(root)?;
        if fs::read_dir(root)?.next().is_some() {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "power-loss state root is not empty",
            ));
        }
        for path in self.directories.iter().chain(self.files.keys()) {
            if path.is_absolute()
                || path
                    .components()
                    .any(|c| !matches!(c, Component::Normal(_)))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "power-loss state contains a non-relative path",
                ));
            }
        }
        for dir in &self.directories {
            fs::create_dir_all(root.join(dir))?;
        }
        for (path, first) in &self.hard_links {
            if self.hard_links.contains_key(first)
                || !self.files.contains_key(path)
                || self.files.get(path) != self.files.get(first)
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "power-loss state contains an invalid hard link",
                ));
            }
        }
        for (path, bytes) in &self.files {
            if !self.hard_links.contains_key(path) {
                fs::write(root.join(path), bytes)?;
            }
        }
        for (path, first) in &self.hard_links {
            fs::hard_link(root.join(first), root.join(path))?;
        }
        Ok(())
    }
}

/// The first choice at each cut is exactly the barrier-protected state.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Family {
    SyncedOnly,
    PartialPersistence,
}

#[derive(Clone, Debug)]
pub struct SurvivingState {
    /// Number of completed filesystem operations before power is lost (zero is included).
    pub cut: usize,
    pub family: Family,
    pub state: State,
}

#[derive(Clone, Debug)]
pub struct RecoveryState {
    pub first: SurvivingState,
    pub second: SurvivingState,
}

struct Journal {
    initial: Image,
    live: Image,
    next_inode: Inode,
    operations: Vec<Operation>,
}

impl Journal {
    fn push(&mut self, operation: Operation) {
        if let Operation::Change(changes) = &operation {
            for change in changes {
                self.live.apply(change);
            }
        }
        self.operations.push(operation);
    }
}

/// Records successful operations while executing them against an isolated real root.
/// Initial files are considered durable. Symlinks and directory renames are rejected;
/// directories are tracked by path, not by directory inode. Unix file inode identities
/// preserve hard links, including both surviving names after a cross-directory rename.
/// On other platforms, initial files and cross-directory renames are refused.
/// Files must not change behind the recorder: those writes have no persistence history.
#[derive(Clone)]
pub struct RecordingFs {
    root: PathBuf,
    root_alias: PathBuf,
    journal: Arc<Mutex<Journal>>,
}

impl RecordingFs {
    pub fn new(root: &Path) -> io::Result<Self> {
        let root_alias = absolute_path(root)?;
        fs::create_dir_all(root)?;
        if !cfg!(unix) && fs::read_dir(root)?.next().is_some() {
            return Err(unsupported("non-empty initial roots on non-Unix: stable std cannot establish hard-link identity"));
        }
        let root = root.canonicalize()?;
        let mut initial = Image::default();
        let mut next_inode = 0;
        snapshot(
            &root,
            Path::new(""),
            &mut initial,
            &mut next_inode,
            &mut BTreeMap::new(),
        )?;
        Ok(Self {
            root,
            root_alias,
            journal: Arc::new(Mutex::new(Journal {
                live: initial.clone(),
                initial,
                next_inode,
                operations: Vec::new(),
            })),
        })
    }

    fn relative(&self, path: &Path) -> io::Result<PathBuf> {
        let absolute = absolute_path(path)?;
        // Temporary roots on macOS can be spelled /var or /private/var. Accept
        // the caller's original root spelling without following links inside it.
        let relative = absolute
            .strip_prefix(&self.root)
            .or_else(|_| absolute.strip_prefix(&self.root_alias))
            .map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!(
                        "filesystem operation escapes recording root: {}",
                        path.display()
                    ),
                )
            })?;
        // Do not follow a symlink out of the sandbox, even for read-only calls.
        let mut candidate = self.root.clone();
        for part in relative.components() {
            candidate.push(part);
            match fs::symlink_metadata(&candidate) {
                Ok(meta) if meta.file_type().is_symlink() => return Err(unsupported("symlinks")),
                Ok(_) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => break,
                Err(error) => return Err(error),
            }
        }
        Ok(relative.to_path_buf())
    }

    /// Enumerate every operation cut, unsynced operation subset, bounded tear and block subset.
    /// The limit is a refusal, never sampling: exceeding it cannot give a passing proof.
    pub fn enumerate(&self, limit: usize) -> io::Result<Vec<SurvivingState>> {
        let mut remaining = limit;
        self.enumerate_with_budget(&mut remaining)
    }

    fn enumerate_with_budget(&self, remaining: &mut usize) -> io::Result<Vec<SurvivingState>> {
        let journal = self.journal.lock().unwrap();
        check_recursion_budget(&journal.operations)?;
        let mut histories: BTreeMap<Stream, Vec<Vec<Change>>> = BTreeMap::new();
        let mut durable: BTreeMap<Stream, usize> = BTreeMap::new();
        let mut states = Vec::new();
        for cut in 0..=journal.operations.len() {
            if cut > 0 {
                match &journal.operations[cut - 1] {
                    Operation::Observe => {}
                    Operation::Change(changes) => {
                        // A rename within one directory is a single namespace operation.
                        // Across directories, each directory has its own durability barrier.
                        let mut grouped: BTreeMap<Stream, Vec<Change>> = BTreeMap::new();
                        for change in changes {
                            grouped
                                .entry(change.stream())
                                .or_default()
                                .push(change.clone());
                        }
                        for (stream, changes) in grouped {
                            histories.entry(stream).or_default().push(changes);
                        }
                    }
                    Operation::Sync(stream) => {
                        durable.insert(stream.clone(), histories.get(stream).map_or(0, Vec::len));
                    }
                }
            }
            let streams: Vec<_> = histories
                .iter()
                .map(|(stream, changes)| {
                    (
                        changes.as_slice(),
                        durable.get(stream).copied().unwrap_or(0),
                    )
                })
                .collect();
            let mut seen = BTreeSet::new();
            enumerate_streams(
                &streams,
                0,
                journal.initial.clone(),
                false,
                &mut |image, partial| {
                    if *remaining == 0 {
                        return Err(state_limit());
                    }
                    *remaining -= 1;
                    let state = image.visible();
                    if seen.insert(state.clone()) {
                        states.push(SurvivingState {
                            cut,
                            family: if partial {
                                Family::PartialPersistence
                            } else {
                                Family::SyncedOnly
                            },
                            state,
                        });
                    }
                    Ok(())
                },
            )?;
        }
        Ok(states)
    }

    /// Re-run recovery from every first-cut state on a fresh root, then enumerate a
    /// second loss at every recovery operation, including loss of recovery's writes.
    /// The same exploration budget covers both passes, including duplicate candidates.
    /// The caller checks invariants on the second-cut states, or installs each with
    /// `State::install` and runs recovery again to check the resulting filesystem.
    pub fn enumerate_recovery(
        &self,
        scratch: &Path,
        limit: usize,
        recover: impl Fn(&Path) -> io::Result<()>,
    ) -> io::Result<Vec<RecoveryState>> {
        let mut states = Vec::new();
        let mut remaining = limit;
        for (index, first) in self
            .enumerate_with_budget(&mut remaining)?
            .into_iter()
            .enumerate()
        {
            let root = scratch.join(format!("recovery-{index}"));
            first.state.install(&root)?;
            let recording = Arc::new(Self::new(&root)?);
            with_fs_ops(recording.clone(), || recover(&root))?;
            for second in recording.enumerate_with_budget(&mut remaining)? {
                states.push(RecoveryState {
                    first: first.clone(),
                    second,
                });
            }
        }
        Ok(states)
    }
}

fn check_recursion_budget(operations: &[Operation]) -> io::Result<()> {
    let mut depths: BTreeMap<Stream, usize> = BTreeMap::new();
    for operation in operations {
        match operation {
            Operation::Change(changes) => {
                let mut grouped: BTreeMap<Stream, usize> = BTreeMap::new();
                for change in changes {
                    let depth = grouped.entry(change.stream()).or_insert(3);
                    if let Change::Write { offset, bytes, .. } = change {
                        *depth = depth.saturating_add(
                            (*offset as usize % BLOCK_SIZE)
                                .saturating_add(bytes.len())
                                .div_ceil(BLOCK_SIZE),
                        );
                    }
                }
                for (stream, depth) in grouped {
                    let current = depths.entry(stream).or_default();
                    *current = current.saturating_add(depth);
                }
            }
            Operation::Sync(stream) => {
                if let Some(depth) = depths.get_mut(stream) {
                    *depth = 0;
                }
            }
            Operation::Observe => {}
        }
        let depth = depths
            .values()
            .fold(depths.len(), |sum, depth| sum.saturating_add(*depth));
        if depth > MAX_ENUMERATION_DEPTH {
            return Err(io::Error::other(
                "power-loss enumeration exceeded its recursion budget",
            ));
        }
    }
    Ok(())
}

fn enumerate_streams(
    streams: &[(&[Vec<Change>], usize)],
    index: usize,
    image: Image,
    partial: bool,
    visit: &mut dyn FnMut(&Image, bool) -> io::Result<()>,
) -> io::Result<()> {
    let Some((changes, durable)) = streams.get(index) else {
        return visit(&image, partial);
    };
    let mut prefix = image;
    for operation in &changes[..*durable] {
        for change in operation {
            prefix.apply(change);
        }
    }
    enumerate_unsynced(
        &changes[*durable..],
        &prefix,
        partial,
        &mut |image, partial| enumerate_streams(streams, index + 1, image.clone(), partial, visit),
    )
}

fn enumerate_unsynced(
    operations: &[Vec<Change>],
    image: &Image,
    partial: bool,
    visit: &mut dyn FnMut(&Image, bool) -> io::Result<()>,
) -> io::Result<()> {
    let Some((operation, rest)) = operations.split_first() else {
        return visit(image, partial);
    };
    enumerate_unsynced(rest, image, partial, visit)?;
    if let [Change::Write {
        inode,
        offset,
        bytes,
    }] = operation.as_slice()
    {
        enumerate_write(image, *inode, *offset, bytes, &mut |image| {
            enumerate_unsynced(rest, image, true, visit)
        })
    } else {
        let mut changed = image.clone();
        for change in operation {
            changed.apply(change);
        }
        enumerate_unsynced(rest, &changed, true, visit)
    }
}

/// Length metadata may reach storage ahead of the data blocks whose contents it describes.
fn enumerate_write(
    image: &Image,
    inode: Inode,
    offset: u64,
    bytes: &[u8],
    visit: &mut dyn FnMut(&Image) -> io::Result<()>,
) -> io::Result<()> {
    let points = tear_points(offset, bytes);
    for &length in &points {
        if length == 0 {
            continue;
        }
        let mut extended = image.clone();
        let contents = extended.contents.entry(inode).or_default();
        let end = offset as usize + length;
        if contents.len() < end {
            contents.resize(end, 0);
        }
        for &data_len in points.iter().filter(|&&point| point <= length) {
            enumerate_blocks(&extended, inode, offset, &bytes[..data_len], &mut |image| {
                visit(image)
            })?;
        }
    }
    Ok(())
}

fn enumerate_blocks(
    image: &Image,
    inode: Inode,
    offset: u64,
    bytes: &[u8],
    visit: &mut dyn FnMut(&Image) -> io::Result<()>,
) -> io::Result<()> {
    if bytes.is_empty() {
        return visit(image);
    }
    let block_len = (BLOCK_SIZE - offset as usize % BLOCK_SIZE).min(bytes.len());
    let (block, rest) = bytes.split_at(block_len);
    enumerate_blocks(image, inode, offset + block_len as u64, rest, visit)?;
    let mut persisted = image.clone();
    persisted.apply(&Change::Write {
        inode,
        offset,
        bytes: block.to_vec(),
    });
    enumerate_blocks(&persisted, inode, offset + block_len as u64, rest, visit)
}

fn tear_points(offset: u64, bytes: &[u8]) -> Vec<usize> {
    let mut boundaries = BTreeSet::from([0, bytes.len()]);
    let mut block = BLOCK_SIZE - offset as usize % BLOCK_SIZE;
    while block < bytes.len() {
        boundaries.insert(block);
        block += BLOCK_SIZE;
    }
    let mut frame_start = 0;
    while frame_start < bytes.len() {
        use broca_wal::framing::DecodeStep;
        let consumed = match broca_wal::framing::decode_next(&bytes[frame_start..]) {
            DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } => consumed,
            _ => break,
        };
        boundaries.insert(frame_start + broca_wal::framing::HEADER_LEN);
        frame_start += consumed;
        boundaries.insert(frame_start);
    }
    let mut points = BTreeSet::new();
    for boundary in boundaries {
        points.insert(boundary);
        if boundary > 0 {
            points.insert(boundary - 1);
        }
        if boundary < bytes.len() {
            points.insert(boundary + 1);
        }
    }
    points.into_iter().collect()
}

fn unsupported(subject: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::Unsupported,
        format!("power-loss recording does not model {subject}"),
    )
}
fn state_limit() -> io::Error {
    io::Error::other("power-loss enumeration exceeded its state limit")
}

fn absolute_path(path: &Path) -> io::Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut normalized = PathBuf::new();
    for component in absolute.components() {
        match component {
            Component::CurDir => {}
            Component::ParentDir => return Err(unsupported("parent traversal")),
            other => normalized.push(other.as_os_str()),
        }
    }
    Ok(normalized)
}

// Off Unix, any initial file is refused before inode numbering, so the two inode
// parameters only travel through the directory recursion there.
#[cfg_attr(not(unix), allow(clippy::only_used_in_recursion))]
fn snapshot(
    root: &Path,
    relative: &Path,
    image: &mut Image,
    next_inode: &mut Inode,
    identities: &mut BTreeMap<(u64, u64), Inode>,
) -> io::Result<()> {
    image
        .entries
        .insert(relative.to_path_buf(), Node::Directory);
    for entry in fs::read_dir(root.join(relative))? {
        let entry = entry?;
        let path = relative.join(entry.file_name());
        let meta = fs::symlink_metadata(entry.path())?;
        if meta.is_dir() {
            snapshot(root, &path, image, next_inode, identities)?;
        } else if meta.is_file() {
            #[cfg(not(unix))]
            return Err(unsupported(
                "initial files on non-Unix: stable std cannot establish hard-link identity",
            ));
            #[cfg(unix)]
            {
                use std::os::unix::fs::MetadataExt;
                let identity = (meta.dev(), meta.ino());
                let inode = identities.get(&identity).copied().unwrap_or_else(|| {
                    let inode = *next_inode;
                    *next_inode += 1;
                    identities.insert(identity, inode);
                    inode
                });
                image.entries.insert(path, Node::File(inode));
                image.contents.insert(inode, fs::read(entry.path())?);
            }
        } else {
            return Err(unsupported("symlinks or special files"));
        }
    }
    Ok(())
}

impl FsOps for RecordingFs {
    fn create_dir(&self, path: &Path) -> io::Result<()> {
        let path = self.relative(path)?;
        let mut journal = self.journal.lock().unwrap();
        RealFs.create_dir(&self.root.join(&path))?;
        journal.push(Operation::Change(vec![Change::Entry {
            path,
            node: Some(Node::Directory),
        }]));
        Ok(())
    }
    fn hard_link(&self, from: &Path, to: &Path) -> io::Result<()> {
        if !cfg!(unix) {
            return Err(unsupported("hard links on non-Unix"));
        }
        let from = self.relative(from)?;
        let to = self.relative(to)?;
        let mut journal = self.journal.lock().unwrap();
        let node = journal
            .live
            .entries
            .get(&from)
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        if node == Node::Directory {
            return Err(unsupported("directory hard links"));
        }
        RealFs.hard_link(&self.root.join(from), &self.root.join(&to))?;
        journal.push(Operation::Change(vec![Change::Entry {
            path: to,
            node: Some(node),
        }]));
        Ok(())
    }
    fn canonicalize(&self, path: &Path) -> io::Result<PathBuf> {
        let path = self.relative(path)?;
        let result = RealFs.canonicalize(&self.root.join(path))?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(result)
    }
    fn read_dir(&self, path: &Path) -> io::Result<broca_wal::fs_ops::ReadDir> {
        let path = self.relative(path)?;
        let result = RealFs.read_dir(&self.root.join(path))?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(result)
    }
    fn validate_if_exists(&self, path: &Path) -> io::Result<()> {
        let path = self.relative(path)?;
        RealFs.validate_if_exists(&self.root.join(path))?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(())
    }
    fn open(&self, path: &Path, options: &OpenOptions) -> io::Result<Box<dyn FileOps>> {
        let path = self.relative(path)?;
        let mut journal = self.journal.lock().unwrap();
        let file = RealFs.open(&self.root.join(&path), options)?;
        let mut changes = Vec::new();
        let inode = match journal.live.entries.get(&path) {
            Some(Node::File(inode)) => *inode,
            Some(Node::Directory) => return Err(unsupported("directory handles; use sync_dir")),
            None => {
                let inode = journal.next_inode;
                journal.next_inode += 1;
                changes.push(Change::Entry {
                    path: path.clone(),
                    node: Some(Node::File(inode)),
                });
                inode
            }
        };
        if options.truncate && !options.create_new {
            changes.push(Change::Resize { inode, len: 0 });
        }
        journal.push(if changes.is_empty() {
            Operation::Observe
        } else {
            Operation::Change(changes)
        });
        Ok(Box::new(RecordingFile {
            file,
            inode,
            append: options.append,
            journal: self.journal.clone(),
        }))
    }

    fn metadata(&self, path: &Path) -> io::Result<Metadata> {
        let path = self.relative(path)?;
        let meta = RealFs.metadata(&self.root.join(path))?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(meta)
    }

    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        let path = self.relative(path)?;
        let mut journal = self.journal.lock().unwrap();
        // create_dir_all is a convenience API, but each mkdir is a separate cut.
        let mut parent = PathBuf::new();
        for component in path.components() {
            parent.push(component);
            if matches!(journal.live.entries.get(&parent), Some(Node::File(_))) {
                return RealFs.create_dir_all(&self.root.join(&parent));
            }
            if !journal.live.entries.contains_key(&parent) {
                RealFs.create_dir_all(&self.root.join(&parent))?;
                journal.push(Operation::Change(vec![Change::Entry {
                    path: parent.clone(),
                    node: Some(Node::Directory),
                }]));
            }
        }
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let from = self.relative(from)?;
        let to = self.relative(to)?;
        if !cfg!(unix) && from.parent() != to.parent() {
            return Err(unsupported("cross-directory renames on non-Unix platforms"));
        }
        let mut journal = self.journal.lock().unwrap();
        let node = journal
            .live
            .entries
            .get(&from)
            .cloned()
            .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?;
        if node == Node::Directory {
            return Err(unsupported("directory renames"));
        }
        RealFs.rename(&self.root.join(&from), &self.root.join(&to))?;
        if from == to || journal.live.entries.get(&to) == Some(&node) {
            journal.push(Operation::Observe);
        } else {
            journal.push(Operation::Change(vec![
                Change::Entry {
                    path: from,
                    node: None,
                },
                Change::Entry {
                    path: to,
                    node: Some(node),
                },
            ]));
        }
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        let path = self.relative(path)?;
        let mut journal = self.journal.lock().unwrap();
        RealFs.remove_file(&self.root.join(&path))?;
        journal.push(Operation::Change(vec![Change::Entry { path, node: None }]));
        Ok(())
    }

    fn sync_dir(&self, path: &Path) -> io::Result<()> {
        let path = self.relative(path)?;
        RealFs.sync_dir(&self.root.join(&path))?;
        self.journal.lock().unwrap().push(if cfg!(unix) {
            Operation::Sync(Stream::Directory(path))
        } else {
            Operation::Observe
        });
        Ok(())
    }
}

struct RecordingFile {
    file: Box<dyn FileOps>,
    inode: Inode,
    append: bool,
    journal: Arc<Mutex<Journal>>,
}

impl Read for RecordingFile {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        let read = self.file.read(bytes)?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(read)
    }
}
impl Write for RecordingFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let mut journal = self.journal.lock().unwrap();
        let offset = if self.append {
            self.file.metadata()?.len()
        } else {
            self.file.stream_position()?
        };
        let written = self.file.write(bytes)?;
        journal.push(Operation::Change(vec![Change::Write {
            inode: self.inode,
            offset,
            bytes: bytes[..written].to_vec(),
        }]));
        Ok(written)
    }
    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(())
    }
}
impl Seek for RecordingFile {
    fn seek(&mut self, position: SeekFrom) -> io::Result<u64> {
        let position = self.file.seek(position)?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(position)
    }
}
impl FileOps for RecordingFile {
    fn try_clone(&self) -> io::Result<Box<dyn FileOps>> {
        Ok(Box::new(Self {
            file: self.file.try_clone()?,
            inode: self.inode,
            append: self.append,
            journal: self.journal.clone(),
        }))
    }
    fn lock(&self) -> io::Result<()> {
        self.file.lock()?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(())
    }
    fn unlock(&self) -> io::Result<()> {
        self.file.unlock()?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(())
    }
    fn try_lock(&self) -> Result<(), std::fs::TryLockError> {
        self.file.try_lock()?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(())
    }
    #[cfg(unix)]
    fn read_exact_at(&self, bytes: &mut [u8], offset: u64) -> io::Result<()> {
        self.file.read_exact_at(bytes, offset)?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(())
    }
    fn metadata(&self) -> io::Result<Metadata> {
        let meta = self.file.metadata()?;
        self.journal.lock().unwrap().push(Operation::Observe);
        Ok(meta)
    }
    fn set_len(&self, len: u64) -> io::Result<()> {
        let mut journal = self.journal.lock().unwrap();
        self.file.set_len(len)?;
        journal.push(Operation::Change(vec![Change::Resize {
            inode: self.inode,
            len,
        }]));
        Ok(())
    }
    fn sync_all(&self) -> io::Result<()> {
        let mut journal = self.journal.lock().unwrap();
        self.file.sync_all()?;
        journal.push(Operation::Sync(Stream::File(self.inode)));
        Ok(())
    }
}

#[cfg(all(test, unix))]
mod tests {
    use super::*;
    use broca_wal::{fs_ops, FileWal, Wal, WalRecord};
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Scratch(PathBuf);
    impl Scratch {
        fn new() -> Self {
            static NEXT: AtomicUsize = AtomicUsize::new(0);
            let root = broca_test_support::scratch_root().join(format!(
                "power-loss-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&root).unwrap();
            Self(root)
        }
        fn recording(&self) -> Arc<RecordingFs> {
            Arc::new(RecordingFs::new(&self.0).unwrap())
        }
    }
    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }
    fn bytes<'a>(state: &'a State, path: &str) -> Option<&'a [u8]> {
        state.files.get(Path::new(path)).map(Vec::as_slice)
    }
    fn final_states(recording: &RecordingFs) -> Vec<SurvivingState> {
        let states = recording.enumerate(100_000).unwrap();
        let final_cut = states.last().unwrap().cut;
        states
            .into_iter()
            .filter(|state| state.cut == final_cut)
            .collect()
    }

    #[test]
    fn power_loss_shim_catches_rename_before_fsync() {
        let root = Scratch::new();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            let mut file = fs_ops::File::create(root.0.join("proof.tmp")).unwrap();
            file.write_all(b"committed").unwrap();
            file.flush().unwrap();
            fs_ops::rename(root.0.join("proof.tmp"), root.0.join("proof")).unwrap();
            fs_ops::sync_dir(&root.0).unwrap();
            // Deliberately missing file.sync_all(): rename and directory sync cannot
            // protect the bytes of the file whose name they publish.
        });
        let states = final_states(&recording);
        assert!(states
            .iter()
            .any(|s| bytes(&s.state, "proof") == Some(b"".as_slice())));
        assert!(states
            .iter()
            .any(|s| bytes(&s.state, "proof") == Some(b"committed".as_slice())));
        assert!(states
            .iter()
            .all(|s| bytes(&s.state, "proof.tmp").is_none()));
    }

    #[test]
    fn power_loss_shim_keeps_synced_state() {
        let root = Scratch::new();
        let recording = root.recording();
        let mut first_sync = 0;
        let mut second_sync = 0;
        let mut names_synced = 0;
        with_fs_ops(recording.clone(), || {
            fs_ops::create_dir_all(root.0.join("nested")).unwrap();
            let mut file = fs_ops::File::create(root.0.join("nested/data")).unwrap();
            file.write_all(b"durable-").unwrap();
            file.sync_all().unwrap();
            first_sync = recording.journal.lock().unwrap().operations.len();
            file.write_all(b"prefix").unwrap();
            file.sync_all().unwrap();
            second_sync = recording.journal.lock().unwrap().operations.len();
            fs_ops::sync_dir(&root.0.join("nested")).unwrap();
            fs_ops::sync_dir(&root.0).unwrap();
            names_synced = recording.journal.lock().unwrap().operations.len();
            file.write_all(b"-maybe").unwrap();
            let mut proof = fs_ops::File::create(root.0.join("proof")).unwrap();
            proof.write_all(b"unsynced proof").unwrap();
        });
        for surviving in recording.enumerate(100_000).unwrap() {
            if surviving.cut >= names_synced {
                assert!(bytes(&surviving.state, "nested/data").is_some());
            }
            if let Some(bytes) = bytes(&surviving.state, "nested/data") {
                if surviving.cut >= second_sync {
                    assert!(bytes.starts_with(b"durable-prefix"));
                } else if surviving.cut >= first_sync {
                    assert!(bytes.starts_with(b"durable-"));
                }
            }
        }
        let states = final_states(&recording);
        assert!(states.len() >= 4);
        assert!(states.iter().all(|s| bytes(&s.state, "nested/data")
            .unwrap()
            .starts_with(b"durable-prefix")));
        let synced = states
            .iter()
            .find(|s| s.family == Family::SyncedOnly)
            .unwrap();
        assert_eq!(
            bytes(&synced.state, "nested/data"),
            Some(b"durable-prefix".as_slice())
        );
        assert_eq!(bytes(&synced.state, "proof"), None);
        assert!(states
            .iter()
            .any(|s| bytes(&s.state, "proof") == Some(b"unsynced proof".as_slice())));
    }

    #[test]
    fn power_loss_enumerates_partial_persistence() {
        let root = Scratch::new();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            let mut early = fs_ops::File::create(root.0.join("early")).unwrap();
            let mut late = fs_ops::File::create(root.0.join("late")).unwrap();
            fs_ops::sync_dir(&root.0).unwrap();
            early.write_all(b"lost").unwrap();
            late.write_all(b"later").unwrap();
            late.write_all(b"-last").unwrap();
        });
        let states = final_states(&recording);
        assert!(states
            .iter()
            .any(|s| bytes(&s.state, "early") == Some(b"".as_slice())
                && bytes(&s.state, "late") == Some(b"later-last".as_slice())));
        assert!(states
            .iter()
            .any(|s| bytes(&s.state, "late") == Some(b"later".as_slice())));
        assert!(
            states.iter().all(|s| {
                let data = bytes(&s.state, "late").unwrap();
                data.len() <= b"later-last".len()
                    && data
                        .iter()
                        .zip(b"later-last")
                        .all(|(&actual, &expected)| actual == 0 || actual == expected)
            }),
            "tears and missing blocks may erase bytes, never invent payload data"
        );
        assert!(
            recording.enumerate(1).is_err(),
            "a state budget must fail closed, not sample"
        );
    }

    #[test]
    fn power_loss_cuts_recovery_and_preserves_its_synced_prefix() {
        let root = Scratch::new();
        let recovery_root = Scratch::new();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            let mut file = fs_ops::File::create(root.0.join("data")).unwrap();
            file.write_all(b"initial").unwrap();
            file.sync_all().unwrap();
            fs_ops::sync_dir(&root.0).unwrap();
        });
        let states = recording
            .enumerate_recovery(&recovery_root.0, 100_000, |root| {
                let mut file = OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .open(root.join("repair"))?;
                file.write_all(b"fixed")?;
                file.sync_all()?;
                fs_ops::sync_dir(root)?;
                file.write_all(b"-pending")?;
                Ok(())
            })
            .unwrap();
        assert!(states.iter().any(|s| s.second.cut == 0));
        assert!(states
            .iter()
            .any(|s| s.second.cut > 0 && bytes(&s.second.state, "repair") == Some(b"".as_slice())));
        assert!(states
            .iter()
            .any(|s| bytes(&s.second.state, "repair") == Some(b"fixed-pending".as_slice())));
        let final_second_cut = states.iter().map(|s| s.second.cut).max().unwrap();
        assert!(states
            .iter()
            .filter(|s| s.second.cut == final_second_cut)
            .all(|s| bytes(&s.second.state, "repair")
                .unwrap()
                .starts_with(b"fixed")));
        let first_final_cut = states.iter().map(|s| s.first.cut).max().unwrap();
        assert!(states
            .iter()
            .filter(|s| s.first.cut == first_final_cut)
            .all(|s| bytes(&s.second.state, "data") == Some(b"initial".as_slice())));
    }

    #[test]
    fn power_loss_real_wal_uses_the_recording_backend() {
        let root = Scratch::new();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            let mut wal = FileWal::open(root.0.join("nested/log")).unwrap();
            wal.append(
                1,
                &WalRecord::StepStarted {
                    step_id: broca_protocol::StepId(1),
                },
                true,
            )
            .unwrap();
        });
        for surviving in final_states(&recording) {
            let restored = Scratch::new();
            surviving.state.install(&restored.0).unwrap();
            let wal = FileWal::open(restored.0.join("nested/log")).unwrap();
            assert_eq!(wal.replay().unwrap().len(), 1);
        }
    }

    #[test]
    fn power_loss_tracks_rename_and_unlink_separately_from_inode_sync() {
        let root = Scratch::new();
        fs::write(root.0.join("old"), b"seed").unwrap();
        let recording = root.recording();
        let mut after_rename = 0;
        with_fs_ops(recording.clone(), || {
            let mut file = OpenOptions::new()
                .write(true)
                .open(root.0.join("old"))
                .unwrap();
            fs_ops::rename(root.0.join("old"), root.0.join("new")).unwrap();
            file.write_all(b"live").unwrap();
            file.sync_all().unwrap();
            after_rename = recording.journal.lock().unwrap().operations.len();
            fs_ops::remove_file(root.0.join("new")).unwrap();
            fs_ops::sync_dir(&root.0).unwrap();
        });
        let states = recording.enumerate(100_000).unwrap();
        let renamed: Vec<_> = states.iter().filter(|s| s.cut == after_rename).collect();
        assert!(renamed
            .iter()
            .any(|s| bytes(&s.state, "old") == Some(b"live".as_slice())));
        assert!(renamed
            .iter()
            .any(|s| bytes(&s.state, "new") == Some(b"live".as_slice())));
        assert!(renamed.iter().all(|s| s.state.files.len() == 1));
        assert!(final_states(&recording)
            .iter()
            .all(|s| s.state.files.is_empty()));
    }

    #[test]
    fn power_loss_accepts_root_alias_but_rejects_internal_symlinks() {
        let scratch = Scratch::new();
        let root = scratch.0.join("real");
        fs::create_dir(&root).unwrap();
        let alias = scratch.0.join("alias");
        std::os::unix::fs::symlink(&root, &alias).unwrap();
        let recording = Arc::new(RecordingFs::new(&alias).unwrap());
        with_fs_ops(recording.clone(), || {
            let mut file = fs_ops::File::create(alias.join("data")).unwrap();
            file.write_all(b"protected").unwrap();
            file.sync_all().unwrap();
            fs_ops::sync_dir(&alias).unwrap();
            std::os::unix::fs::symlink(&scratch.0, root.join("escape")).unwrap();
            assert!(fs_ops::File::create(alias.join("escape/outside")).is_err());
        });
        assert!(!scratch.0.join("outside").exists());
        assert!(final_states(&recording)
            .iter()
            .all(|s| bytes(&s.state, "data") == Some(b"protected".as_slice())));
        fs_ops::File::create(scratch.0.join("outside-scope")).unwrap();
    }

    #[test]
    fn power_loss_recovery_preserves_two_surviving_names_for_one_inode() {
        let scratch = Scratch::new();
        fs::create_dir(scratch.0.join("source")).unwrap();
        fs::create_dir(scratch.0.join("target")).unwrap();
        fs::write(scratch.0.join("source/data"), b"seed").unwrap();
        let recording = scratch.recording();
        with_fs_ops(recording.clone(), || {
            fs_ops::rename(scratch.0.join("source/data"), scratch.0.join("target/data")).unwrap();
        });
        let both = final_states(&recording)
            .into_iter()
            .find(|s| s.state.files.len() == 2)
            .unwrap();
        assert_eq!(both.state.hard_links.len(), 1);
        let recovered = Scratch::new();
        both.state.install(&recovered.0).unwrap();
        let recovery = recovered.recording();
        with_fs_ops(recovery.clone(), || {
            let mut file = OpenOptions::new()
                .write(true)
                .open(recovered.0.join("source/data"))
                .unwrap();
            file.write_all(b"live").unwrap();
            file.sync_all().unwrap();
            let mut other = fs_ops::File::open(recovered.0.join("target/data")).unwrap();
            let mut contents = Vec::new();
            other.read_to_end(&mut contents).unwrap();
            assert_eq!(contents, b"live");
        });
        assert!(final_states(&recovery).iter().all(|s| {
            bytes(&s.state, "source/data") == Some(b"live".as_slice())
                && bytes(&s.state, "target/data") == Some(b"live".as_slice())
                && s.state.hard_links.len() == 1
        }));
    }

    #[test]
    fn power_loss_tears_extensions_and_keeps_later_blocks_without_earlier_data() {
        let root = Scratch::new();
        fs::write(root.0.join("data"), b"base").unwrap();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            OpenOptions::new()
                .append(true)
                .open(root.0.join("data"))
                .unwrap()
                .write_all(&vec![b'x'; 8192])
                .unwrap();
        });
        let states = final_states(&recording);
        assert!(
            states
                .iter()
                .any(|s| bytes(&s.state, "data") == Some(b"basex".as_slice())),
            "a successful write can leave a one-byte torn extension"
        );
        assert!(
            states.iter().any(|s| {
                let data = bytes(&s.state, "data").unwrap();
                data.len() == 8196
                    && data[..4] == *b"base"
                    && data[4..4096].iter().all(|&b| b == 0)
                    && data[4096..8196].iter().all(|&b| b == b'x')
            }),
            "persisted length and later blocks must not imply persistence of earlier blocks"
        );
        assert!(
            states.iter().any(|s| {
                let data = bytes(&s.state, "data").unwrap();
                data.len() == 8196 && data[..4] == *b"base" && data[4..].iter().all(|&b| b == 0)
            }),
            "length metadata may persist without any extension data"
        );
        for mask in 0..8 {
            assert!(
                states.iter().any(|s| {
                    let data = bytes(&s.state, "data").unwrap();
                    data.len() == 8196
                        && [(4, 4096), (4096, 8192), (8192, 8196)]
                            .iter()
                            .enumerate()
                            .all(|(bit, &(start, end))| {
                                data[start..end]
                                    .iter()
                                    .all(|&b| b == if mask & (1 << bit) == 0 { 0 } else { b'x' })
                            })
                }),
                "every 4096-byte block persistence mask must be enumerated: {mask}"
            );
        }
    }

    #[test]
    fn power_loss_persists_later_writes_and_unrelated_directory_entries_first() {
        let root = Scratch::new();
        fs::write(root.0.join("data"), vec![b'a'; 8192]).unwrap();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            let mut file = OpenOptions::new()
                .write(true)
                .open(root.0.join("data"))
                .unwrap();
            file.write_all(&vec![b'x'; 4096]).unwrap();
            file.seek(SeekFrom::Start(4096)).unwrap();
            file.write_all(&vec![b'y'; 4096]).unwrap();
            fs_ops::File::create(root.0.join("early")).unwrap();
            fs_ops::File::create(root.0.join("late")).unwrap();
        });
        let states = final_states(&recording);
        assert!(
            states.iter().any(|s| {
                let data = bytes(&s.state, "data").unwrap();
                data[..4096].iter().all(|&b| b == b'a') && data[4096..].iter().all(|&b| b == b'y')
            }),
            "different blocks of the same inode can persist out of order"
        );
        assert!(
            states
                .iter()
                .any(|s| bytes(&s.state, "early").is_none() && bytes(&s.state, "late").is_some()),
            "unrelated entries in one directory need not persist in creation order"
        );
    }

    #[test]
    fn power_loss_truncates_existing_files_and_orders_shrink_write_extend_at_sync() {
        let root = Scratch::new();
        fs::write(root.0.join("truncate"), b"old bytes").unwrap();
        fs::write(root.0.join("repair"), b"abcdefgh").unwrap();
        let recording = root.recording();
        let mut before_sync = 0;
        let mut after_sync = 0;
        with_fs_ops(recording.clone(), || {
            fs_ops::File::create(root.0.join("truncate")).unwrap();
            let mut file = OpenOptions::new()
                .write(true)
                .open(root.0.join("repair"))
                .unwrap();
            file.set_len(2).unwrap();
            file.write_all(b"XY").unwrap();
            file.set_len(6).unwrap();
            before_sync = recording.journal.lock().unwrap().operations.len();
            file.sync_all().unwrap();
            after_sync = recording.journal.lock().unwrap().operations.len();
        });
        let states = recording.enumerate(100_000).unwrap();
        assert!(states
            .iter()
            .any(|s| bytes(&s.state, "truncate") == Some(b"old bytes".as_slice())));
        assert!(states
            .iter()
            .any(|s| bytes(&s.state, "truncate") == Some(b"".as_slice())));
        let before: Vec<_> = states.iter().filter(|s| s.cut == before_sync).collect();
        assert!(before
            .iter()
            .any(|s| bytes(&s.state, "repair") == Some(b"abcdefgh".as_slice())));
        assert!(before
            .iter()
            .any(|s| bytes(&s.state, "repair") == Some(b"XY\0\0\0\0".as_slice())));
        assert!(before
            .iter()
            .any(|s| bytes(&s.state, "repair") == Some(b"ab".as_slice())));
        assert!(states
            .iter()
            .filter(|s| s.cut >= after_sync)
            .all(|s| bytes(&s.state, "repair") == Some(b"XY\0\0\0\0".as_slice())));
    }

    fn large_pending_step() -> WalRecord {
        WalRecord::ModelStepFinished {
            written_at_ms: None,
            tags: None,
            hooks: None,
            step_id: broca_protocol::StepId(1),
            assistant_message: broca_protocol::content::AssistantMessage {
                message_id: broca_protocol::MessageId("large".into()),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "x".repeat(9000),
                }],
            }
            .into(),
            usage: broca_protocol::Usage::default(),
            finish_reason: None,
            retries_used: 0,
            service_tier_used: None,
            speed_used: None,
        }
    }

    #[test]
    fn power_loss_wal_torn_blocks_recover_an_authentic_prefix_after_two_cuts() {
        let root = Scratch::new();
        let durable = WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        };
        let pending = large_pending_step();
        FileWal::open(root.0.join("wal/log"))
            .unwrap()
            .append(1, &durable, true)
            .unwrap();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            FileWal::open(root.0.join("wal/log"))
                .unwrap()
                .append(1, &pending, false)
                .unwrap();
        });
        let expected = [
            serde_json::to_value(&durable).unwrap(),
            serde_json::to_value(&pending).unwrap(),
        ];
        let assert_prefix = |wal: &FileWal| {
            let recovered: Vec<_> = wal
                .replay()
                .unwrap()
                .iter()
                .map(|frame| serde_json::to_value(&frame.record).unwrap())
                .collect();
            assert!(!recovered.is_empty() && recovered.len() <= expected.len());
            assert_eq!(recovered, expected[..recovered.len()]);
        };
        let assert_repaired_bytes = |path: &Path| {
            let data = fs::read(path).unwrap();
            let mut offset = 0;
            while offset < data.len() {
                use broca_wal::framing::DecodeStep;
                offset += match broca_wal::framing::decode_next(&data[offset..]) {
                    DecodeStep::Record { consumed, .. } | DecodeStep::Lineage { consumed, .. } => {
                        consumed
                    }
                    _ => panic!("recovery left a damaged WAL frame at byte offset {offset}"),
                };
            }
        };
        let mut remaining = 1_000_000;
        let states: BTreeSet<_> = recording
            .enumerate_with_budget(&mut remaining)
            .unwrap()
            .into_iter()
            .map(|s| s.state)
            .collect();
        assert!(states.iter().any(|s| s.files[Path::new("wal/log")]
            .windows(4096)
            .any(|block| block.iter().all(|&b| b == 0))));
        let mut repaired = 0;
        for state in states {
            let first_root = Scratch::new();
            state.install(&first_root.0).unwrap();
            let recovery = first_root.recording();
            with_fs_ops(recovery.clone(), || {
                assert_prefix(&FileWal::open(first_root.0.join("wal/log")).unwrap());
                assert_repaired_bytes(&first_root.0.join("wal/log"));
            });
            let journal = recovery.journal.lock().unwrap();
            if let Some(resize) = journal.operations.iter().position(|op| matches!(op, Operation::Change(changes) if matches!(changes.as_slice(), [Change::Resize { .. }]))) {
                let sync = journal.operations[resize + 1..].iter().position(|op| matches!(op, Operation::Sync(Stream::File(_)))).unwrap() + resize + 1;
                assert!(sync > resize);
                repaired += 1;
            }
            drop(journal);
            let second_states: BTreeSet<_> = recovery
                .enumerate_with_budget(&mut remaining)
                .unwrap()
                .into_iter()
                .map(|s| s.state)
                .collect();
            for second in second_states {
                let second_root = Scratch::new();
                second.install(&second_root.0).unwrap();
                assert_prefix(&FileWal::open(second_root.0.join("wal/log")).unwrap());
                assert_repaired_bytes(&second_root.0.join("wal/log"));
            }
        }
        assert!(
            repaired > 0,
            "the modeled torn append must exercise real WAL truncation and its repair sync"
        );
    }

    #[test]
    fn power_loss_records_floor_and_relocation_operations() {
        let root = Scratch::new();
        let recording = root.recording();
        with_fs_ops(recording.clone(), || {
            broca_wal::raise_floor(&root.0.join("floor"), &["archive-index/v2"]).unwrap();
            FileWal::open(root.0.join("source/log"))
                .unwrap()
                .relocate_unstarted(&root.0.join("target/log"))
                .unwrap();
        });
        let states = final_states(&recording);
        assert!(states.iter().all(|s| {
            bytes(&s.state, "floor") == Some(b"archive-index/v2\n".as_slice())
                && bytes(&s.state, "target/log").is_some_and(|data| !data.is_empty())
                && bytes(&s.state, "source/log").is_none()
        }));
    }

    #[test]
    #[ignore = "Known availability bug: two pending appends can leave non-tail damage; recovery has no durable synced-length boundary"]
    fn power_loss_two_pending_appends_recover_without_refusing_durable_prefix() {
        let root = Scratch::new();
        let path = root.0.join("wal/log");
        let durable = WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        };
        FileWal::open(&path)
            .unwrap()
            .append(1, &durable, true)
            .unwrap();
        let durable_bytes = fs::read(&path).unwrap();
        let recording = root.recording();
        let mut later_offset = 0;
        with_fs_ops(recording.clone(), || {
            let mut wal = FileWal::open(&path).unwrap();
            wal.append(1, &large_pending_step(), false).unwrap();
            later_offset = fs::metadata(&path).unwrap().len() as usize;
            wal.append(
                1,
                &WalRecord::StepStarted {
                    step_id: broca_protocol::StepId(2),
                },
                false,
            )
            .unwrap();
        });
        let witness = recording.enumerate(2_000_000).unwrap().into_iter().find(|state| {
            let data = &state.state.files[Path::new("wal/log")];
            data.len() > later_offset
                && data[..durable_bytes.len()] == durable_bytes
                && data[durable_bytes.len()..4096].iter().all(|&b| b == 0)
                && matches!(broca_wal::framing::decode_next(&data[later_offset..]), broca_wal::framing::DecodeStep::Record { framed, .. } if framed.seq == 3)
        }).expect("the model must include a lost early block with an intact later pending frame");
        let restored = Scratch::new();
        witness.state.install(&restored.0).unwrap();
        let reopened = FileWal::open(restored.0.join("wal/log"));
        if let Err(error) = &reopened {
            assert!(
                matches!(error, broca_wal::WalError::Corrupt { .. }),
                "unexpected refusal instead of typed non-tail corruption: {error}"
            );
        }
        let recovered = reopened.expect("known availability bug: synced prefix survives, but non-tail unsynced damage refuses recovery");
        let records = recovered.replay().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            serde_json::to_value(&records[0].record).unwrap(),
            serde_json::to_value(&durable).unwrap()
        );
    }
}

#[cfg(all(test, unix))]
#[test]
fn power_loss_refuses_deep_histories_before_recursive_traversal() {
    let root = broca_test_support::scratch_root().join("power-loss-depth-budget");
    let recording = RecordingFs::new(&root).unwrap();
    for index in 0..MAX_ENUMERATION_DEPTH {
        recording
            .journal
            .lock()
            .unwrap()
            .push(Operation::Change(vec![Change::Entry {
                path: PathBuf::from(format!("entry-{index}")),
                node: Some(Node::File(index)),
            }]));
    }
    let error = recording.enumerate(usize::MAX).unwrap_err();
    assert!(error.to_string().contains("recursion budget"));
    fs::remove_dir_all(root).unwrap();
}

#[cfg(all(test, not(unix)))]
#[test]
fn power_loss_non_unix_refuses_nonempty_initial_roots() {
    let root = broca_test_support::scratch_root().join("power-loss-initial-links");
    fs::create_dir_all(&root).unwrap();
    assert!(RecordingFs::new(&root).is_ok());
    fs::write(root.join("existing"), b"unknown link count").unwrap();
    assert!(RecordingFs::new(&root).is_err());
    fs::remove_dir_all(root).unwrap();
}
