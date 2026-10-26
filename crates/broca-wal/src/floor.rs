//! The WAL feature floor: the set of feature names that frames on this machine have already
//! been written with.
//!
//! A build that does not know one of those names refuses every WAL holding such a frame, so
//! placing it would roll the machine back into data it cannot read. `scripts/place-release.sh`
//! reads this file and refuses a staged build whose known-name table
//! (`ck-broca --wal-known-features`) does not cover it; `scripts/wal-floor.sh` describes the
//! same format from the reading side.
//!
//! The writer raises the floor BEFORE the first frame that needs a name the floor lacks: a
//! crash between the two then leaves a floor that names a feature no frame uses yet, which only
//! makes placement stricter than it has to be. The other order could leave a gated frame on
//! disk that placement does not know about, and a rollback onto a build that cannot read it.
//!
//! Format: one feature name per line, sorted, ending with a newline. Blank lines and
//! surrounding whitespace are ignored when reading. The file only ever grows, and is replaced
//! by writing a temporary file in the same directory and renaming it over the old one, so a
//! reader never sees a half-written floor.

use crate::fs_ops;
use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, RwLock};

/// The floor's file name, in broca's state root (next to the `wal/` directory).
pub const FEATURE_FLOOR_FILENAME: &str = "wal-feature-floor";

/// Serializes raises within this process, so two sessions writing their first gated frames at
/// once cannot each read the old floor and have one rename drop the other's name.
static RAISE_LOCK: Mutex<()> = Mutex::new(());

// A floor only grows: deleting it or removing names while writers are alive is unsupported.
// Placement/rollback only reads it, so a successful durability proof remains valid for this
// process. Key by absolute path to keep distinct state roots (and relative working directories)
// separate without opening or canonicalizing a file on the append hot path.
static DURABLE_FLOORS: RwLock<BTreeMap<PathBuf, BTreeSet<String>>> = RwLock::new(BTreeMap::new());

fn memo_covers(path: &Path, names: &[&str]) -> bool {
    DURABLE_FLOORS
        .read()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .get(path)
        .is_some_and(|floor| names.iter().all(|name| floor.contains(*name)))
}

fn remember_durable_floor(path: PathBuf, floor: BTreeSet<String>) {
    DURABLE_FLOORS
        .write()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .insert(path, floor);
}

/// The names in the floor at `path`. A missing file is an empty floor: nothing gated has been
/// written here yet. A file that exists but cannot be read is an error, never an empty floor.
pub fn read_floor(path: &Path) -> std::io::Result<BTreeSet<String>> {
    match fs_ops::read_to_string(path) {
        Ok(text) => Ok(text
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(str::to_owned)
            .collect()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(BTreeSet::new()),
        Err(error) => Err(error),
    }
}

/// Make sure the floor at `path` lists every name in `names`, adding the missing ones.
///
/// Names already proved durable by this process return without filesystem I/O. A cold call
/// syncs even an already-covered floor; new names are written to a temporary file, synced,
/// renamed over the floor and directory-synced before the caller writes the gated frame.
pub fn raise_floor(path: &Path, names: &[&str]) -> std::io::Result<()> {
    let memo_path = std::path::absolute(path)?;
    if memo_covers(&memo_path, names) {
        return Ok(());
    }
    let _guard = RAISE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    // Another caller may have completed the durable raise while we waited for the slow path.
    if memo_covers(&memo_path, names) {
        return Ok(());
    }
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    if !dir.as_os_str().is_empty() {
        #[cfg(test)]
        io_tests::observe(io_tests::Io::CreateDir)?;
        fs_ops::create_dir_all(dir)?;
    }
    // Lock a stable sibling, never the floor inode that rename replaces. Different
    // session leases do not serialize updates to this shared machine-wide file.
    let mut options = fs_ops::OpenOptions::new();
    options.read(true).write(true).create(true).truncate(false);
    #[cfg(test)]
    io_tests::observe(io_tests::Io::Open)?;
    let lock = options.open(lock_path(path))?;
    #[cfg(test)]
    process_tests::before_lock(&lock);
    #[cfg(test)]
    io_tests::observe(io_tests::Io::Lock)?;
    lock.lock()?;
    #[cfg(test)]
    io_tests::observe(io_tests::Io::Read)?;
    let mut floor = read_floor(path)?;
    #[cfg(test)]
    process_tests::after_read();
    let before = floor.len();
    floor.extend(names.iter().map(|name| (*name).to_owned()));
    if floor.len() == before {
        // A prior process may have renamed this readable floor without syncing
        // its parent directory. Visibility now does not prove the rename durable.
        // Sync through a writable handle: Windows `FlushFileBuffers` refuses a read-only one
        // with "Access is denied", while unix accepts either.
        if fs_ops::try_exists(path)? {
            #[cfg(test)]
            io_tests::observe(io_tests::Io::Open)?;
            let file = fs_ops::OpenOptions::ordinary().write(true).open(path)?;
            #[cfg(test)]
            io_tests::observe(io_tests::Io::FileSync)?;
            file.sync_all()?;
        }
        #[cfg(test)]
        io_tests::observe(io_tests::Io::DirSync)?;
        crate::directory_sync::sync_dir(dir)?;
        remember_durable_floor(memo_path, floor);
        return Ok(());
    }
    let temp = temp_path(path);
    let contents: String = floor.iter().map(|name| format!("{name}\n")).collect();
    let written = (|| {
        let mut options = fs_ops::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(test)]
        io_tests::observe(io_tests::Io::Open)?;
        let mut file = options.open(&temp)?;
        file.write_all(contents.as_bytes())?;
        #[cfg(test)]
        io_tests::observe(io_tests::Io::FileSync)?;
        file.sync_all()?;
        #[cfg(test)]
        io_tests::observe(io_tests::Io::Rename)?;
        fs_ops::rename(&temp, path)
    })();
    if let Err(error) = written {
        let _ = fs_ops::remove_file(&temp);
        return Err(error);
    }
    #[cfg(test)]
    io_tests::observe(io_tests::Io::DirSync)?;
    crate::directory_sync::sync_dir(dir)?;
    remember_durable_floor(memo_path, floor);
    Ok(())
}

fn lock_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .unwrap_or_else(|| std::ffi::OsStr::new(FEATURE_FLOOR_FILENAME))
        .to_os_string();
    name.push(".lock");
    path.with_file_name(name)
}

fn temp_path(path: &Path) -> PathBuf {
    let name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_else(|| FEATURE_FLOOR_FILENAME.to_owned());
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_nanos())
        .unwrap_or(0);
    path.with_file_name(format!(".{name}.{}.{nanos}.tmp", std::process::id()))
}

#[cfg(test)]
mod tests {
    use super::*;

    pub(super) fn scratch(tag: &str) -> PathBuf {
        let dir = broca_test_support::scratch_root().join(format!(
            "broca-floor-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[cfg(unix)]
    #[test]
    fn relative_floor_syncs_current_directory() {
        let path = PathBuf::from(format!(
            ".broca-floor-relative-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_file(&path);
        crate::directory_sync::with_failure(Path::new("."), || {
            assert!(raise_floor(&path, &["scope/v1"]).is_err());
            assert!(raise_floor(&path, &["scope/v1"]).is_err());
        });
        raise_floor(&path, &["scope/v1"]).unwrap();
        std::fs::remove_file(&path).unwrap();
        std::fs::remove_file(lock_path(&path)).unwrap();
    }

    /// A missing floor is empty; a raise writes the sorted names in the format the placement
    /// scripts read, leaves no temporary file behind, and only ever adds names.
    #[test]
    fn a_raise_writes_the_sorted_union_and_never_removes_a_name() {
        let dir = scratch("union").join("state");
        let floor = dir.join(FEATURE_FLOOR_FILENAME);
        assert!(read_floor(&floor).unwrap().is_empty());

        raise_floor(&floor, &["scope/v1"]).unwrap();
        assert_eq!(std::fs::read_to_string(&floor).unwrap(), "scope/v1\n");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                std::fs::metadata(&floor).unwrap().permissions().mode() & 0o777,
                0o600
            );
            assert_eq!(
                std::fs::metadata(&dir).unwrap().permissions().mode() & 0o777,
                0o700
            );
        }

        std::fs::write(&floor, "  zz-later/v2 \n\nscope/v1\n").unwrap();
        raise_floor(&floor, &["plan-manifest/v1"]).unwrap();
        assert_eq!(
            std::fs::read_to_string(&floor).unwrap(),
            "plan-manifest/v1\nscope/v1\nzz-later/v2\n"
        );
        let mut entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        entries.sort();
        assert_eq!(
            entries,
            vec![
                std::ffi::OsString::from(FEATURE_FLOOR_FILENAME),
                std::ffi::OsString::from(format!("{FEATURE_FLOOR_FILENAME}.lock"))
            ]
        );
    }

    /// A floor that already covers the names is not rewritten at all.
    #[test]
    fn a_covered_floor_is_left_untouched() {
        let dir = scratch("covered");
        let floor = dir.join(FEATURE_FLOOR_FILENAME);
        std::fs::write(&floor, "scope/v1").unwrap();
        raise_floor(&floor, &["scope/v1"]).unwrap();
        assert_eq!(std::fs::read_to_string(&floor).unwrap(), "scope/v1");
    }

    /// A floor that exists but cannot be read is an error, never treated as empty: that would
    /// rewrite it without the names it held.
    #[test]
    fn an_unreadable_floor_is_an_error() {
        let dir = scratch("unreadable");
        let floor = dir.join(FEATURE_FLOOR_FILENAME);
        std::fs::create_dir(&floor).unwrap();
        assert!(read_floor(&floor).is_err());
        assert!(raise_floor(&floor, &["scope/v1"]).is_err());
    }
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, observe, BroadParent,
    };
    #[test]
    fn windows_floor_temporary_and_replacement_are_private() {
        let fixture = BroadParent::new("floor");
        let path = fixture
            .path
            .join("floor/nested")
            .join(FEATURE_FLOOR_FILENAME);
        let seen = std::rc::Rc::new(std::cell::Cell::new(0));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.set(output.get() + 1);
            },
            || {
                raise_floor(&path, &["first"]).unwrap();
                raise_floor(&path, &["second"]).unwrap();
            },
        );
        assert_eq!(
            seen.get(),
            4,
            "stable lock and temporary publication opens must all be inspected"
        );
        assert_private(&path);
        assert_private(path.parent().unwrap());
        assert_private(&fixture.path.join("floor"));
    }
}

#[cfg(test)]
mod process_tests;

#[cfg(test)]
mod io_tests;
