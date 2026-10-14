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

use std::collections::BTreeSet;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

/// The floor's file name, in broca's state root (next to the `wal/` directory).
pub const FEATURE_FLOOR_FILENAME: &str = "wal-feature-floor";

/// Serializes raises within this process, so two sessions writing their first gated frames at
/// once cannot each read the old floor and have one rename drop the other's name.
static RAISE_LOCK: Mutex<()> = Mutex::new(());

/// The names in the floor at `path`. A missing file is an empty floor: nothing gated has been
/// written here yet. A file that exists but cannot be read is an error, never an empty floor.
pub fn read_floor(path: &Path) -> std::io::Result<BTreeSet<String>> {
    match std::fs::read_to_string(path) {
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
/// Returns without writing when the floor already covers them. Otherwise it writes the union
/// to a temporary file beside the floor, syncs it, renames it over the floor and syncs the
/// directory, so the raise is durable before the caller writes the frame that needs it.
pub fn raise_floor(path: &Path, names: &[&str]) -> std::io::Result<()> {
    let _guard = RAISE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let mut floor = read_floor(path)?;
    let before = floor.len();
    floor.extend(names.iter().map(|name| (*name).to_owned()));
    let dir = path.parent().unwrap_or_else(|| Path::new("."));
    if floor.len() == before {
        // A prior process may have renamed this readable floor without syncing
        // its parent directory. Visibility now does not prove the rename durable.
        if path.try_exists()? {
            std::fs::File::open(path)?.sync_all()?;
        }
        return crate::directory_sync::sync_dir(dir);
    }
    if !dir.as_os_str().is_empty() {
        broca_owner_only::create_dir_all(dir)?;
    }
    let temp = temp_path(path);
    let contents: String = floor.iter().map(|name| format!("{name}\n")).collect();
    let written = (|| {
        let mut options = broca_owner_only::OpenOptions::new();
        options.write(true).create_new(true);
        let mut file = options.open(&temp)?;
        file.write_all(contents.as_bytes())?;
        file.sync_all()?;
        std::fs::rename(&temp, path)
    })();
    if let Err(error) = written {
        let _ = std::fs::remove_file(&temp);
        return Err(error);
    }
    crate::directory_sync::sync_dir(dir)
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

    fn scratch(tag: &str) -> PathBuf {
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
        std::fs::remove_file(path).unwrap();
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

        std::fs::write(&floor, "  zz-later/v2 \n\nplan-manifest/v1\n").unwrap();
        raise_floor(&floor, &["scope/v1"]).unwrap();
        assert_eq!(
            std::fs::read_to_string(&floor).unwrap(),
            "plan-manifest/v1\nscope/v1\nzz-later/v2\n"
        );
        let entries: Vec<_> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|entry| entry.unwrap().file_name())
            .collect();
        assert_eq!(
            entries,
            vec![std::ffi::OsString::from(FEATURE_FLOOR_FILENAME)]
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
            2,
            "both temporary publications must be inspected before rename"
        );
        assert_private(&path);
        assert_private(path.parent().unwrap());
        assert_private(&fixture.path.join("floor"));
    }
}
