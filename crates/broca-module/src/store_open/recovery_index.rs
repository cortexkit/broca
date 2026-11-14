//! A rebuildable, content-free set of sessions that may need open-time recovery.
//!
//! Membership is synced before a run can write its start. Only the actor holding the session
//! removes it, after its terminal and billing are durable. Extra members are safe; missing
//! members are not. A missing or damaged completeness marker requires a background inventory
//! pass, so roots written by older versions need no blocking migration.
//!
//! Builds predating this index do not maintain it. After running one and upgrading again,
//! delete `recovery-candidates/complete` under `session-store`, `session-store-durable`, and
//! `session-store-ephemeral` to force a background rebuild.

use std::{io, path::Path};

use broca_session_store::backend::SessionAddr;

const DIRECTORY: &str = "recovery-candidates";
const COMPLETE: &str = "complete";
const VERSION: &[u8] = b"1\n";

#[cfg(all(test, feature = "test-clock"))]
type AfterCheck = Box<dyn Fn(&Path)>;

#[cfg(all(test, feature = "test-clock"))]
std::thread_local! {
    static AFTER_CHECK: std::cell::RefCell<Option<AfterCheck>> = const { std::cell::RefCell::new(None) };
    static SYNC_COUNTS: std::cell::Cell<(usize, usize)> = const { std::cell::Cell::new((0, 0)) };
}

fn sync_file(file: &std::fs::File) -> io::Result<()> {
    #[cfg(all(test, feature = "test-clock"))]
    SYNC_COUNTS.with(|counts| counts.set((counts.get().0 + 1, counts.get().1)));
    broca_session_store::durable::sync(file)
}

fn sync_dir(path: &Path) -> io::Result<()> {
    #[cfg(all(test, feature = "test-clock"))]
    SYNC_COUNTS.with(|counts| counts.set((counts.get().0, counts.get().1 + 1)));
    broca_session_store::durable::sync_dir(path)
}

#[cfg(all(test, feature = "test-clock"))]
pub(crate) fn sync_counts_for_test() -> (usize, usize) {
    SYNC_COUNTS.with(std::cell::Cell::get)
}

#[cfg(all(test, feature = "test-clock"))]
pub(crate) fn prepare_complete_for_test(root: &Path) -> io::Result<()> {
    prepare(root)?;
    complete(root)
}

#[cfg(all(test, feature = "test-clock"))]
pub(crate) fn members_for_test(root: &Path) -> io::Result<Option<Vec<SessionAddr>>> {
    candidates(root)
}

pub(super) fn prepare(root: &Path) -> io::Result<()> {
    broca_owner_only::create_dir_all(root.join(DIRECTORY))?;
    sync_dir(root)
}

pub(crate) fn insert(root: &Path, addr: &SessionAddr) -> io::Result<()> {
    let dir = root.join(DIRECTORY);
    // Sync existing markers too: a previous attempt may have created the file but failed
    // its directory barrier, and must not let a retry start an unindexed run.
    let file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.join(&addr.0))?;
    sync_file(&file)?;
    sync_dir(&dir)
}

pub(crate) fn remove(root: &Path, addr: &SessionAddr) -> io::Result<()> {
    let dir = root.join(DIRECTORY);
    match std::fs::remove_file(dir.join(&addr.0)) {
        // A lost unlink leaves a stale candidate, never a missing unfinished run.
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn address(name: &str) -> bool {
    name.len() == 16 && name.bytes().all(|b| b.is_ascii_hexdigit())
}

/// `None` means the inventory cannot vouch for completeness, never an empty inventory.
pub(super) fn candidates(root: &Path) -> io::Result<Option<Vec<SessionAddr>>> {
    let dir = root.join(DIRECTORY);
    if std::fs::read(dir.join(COMPLETE))? != VERSION {
        return Ok(None);
    }
    let mut members = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_name() == COMPLETE {
            continue;
        }
        let name = entry.file_name();
        let Some(name) = name.to_str().filter(|name| address(name)) else {
            return Ok(None);
        };
        if !std::fs::read(entry.path())?.is_empty() {
            return Ok(None);
        }
        members.push(SessionAddr(name.into()));
    }
    Ok(Some(members))
}

/// Publish completeness only after the rebuild inspected every stored session's head and
/// operational state successfully. Concurrent actors may add valid members during the
/// inventory; those must never be replaced by its snapshot.
pub(super) fn complete(root: &Path) -> io::Result<()> {
    let dir = root.join(DIRECTORY);
    for entry in std::fs::read_dir(&dir)? {
        let entry = entry?;
        if entry.file_name() == COMPLETE {
            continue;
        }
        let name = entry.file_name();
        let valid = name.to_str().filter(|name| address(name));
        let damaged =
            valid.is_some() && !std::fs::read(entry.path()).is_ok_and(|bytes| bytes.is_empty());
        #[cfg(all(test, feature = "test-clock"))]
        AFTER_CHECK.with(|hook| {
            if let Some(hook) = hook.borrow().as_ref() {
                hook(&entry.path());
            }
        });
        if let Some(name) = valid {
            if damaged {
                // An actor may have synced this member since the check. Repairing it can
                // leave a stale extra; deleting it could lose a run's durable registration.
                insert(root, &SessionAddr(name.into()))?;
            }
        } else {
            std::fs::remove_file(entry.path())?;
        }
    }
    use std::io::Write;
    let mut file = broca_owner_only::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(dir.join(COMPLETE))?;
    file.write_all(VERSION)?;
    sync_file(&file)?;
    sync_dir(&dir)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn missing_and_damaged_coverage_require_inventory_rebuild() {
        let root = broca_test_support::scratch_root()
            .join(format!("recovery-index-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        prepare(&root).unwrap();
        assert!(candidates(&root).is_err());
        let addr = SessionAddr("0123456789abcdef".into());
        insert(&root, &addr).unwrap();
        complete(&root).unwrap();
        assert_eq!(candidates(&root).unwrap().unwrap(), vec![addr.clone()]);
        std::fs::write(root.join(DIRECTORY).join(COMPLETE), b"damaged").unwrap();
        assert!(candidates(&root).unwrap().is_none());
        complete(&root).unwrap();
        std::fs::write(root.join(DIRECTORY).join(&addr.0), b"damaged").unwrap();
        assert!(candidates(&root).unwrap().is_none());
        complete(&root).unwrap();
        assert_eq!(candidates(&root).unwrap().unwrap(), vec![addr.clone()]);
        insert(&root, &addr).unwrap();
        assert_eq!(candidates(&root).unwrap().unwrap(), vec![addr.clone()]);
        remove(&root, &addr).unwrap();
        assert!(candidates(&root).unwrap().unwrap().is_empty());
        std::fs::remove_dir_all(root).unwrap();
    }

    #[cfg(feature = "test-clock")]
    #[test]
    fn rebuilding_damaged_coverage_keeps_a_concurrently_synced_member() {
        let root = broca_test_support::scratch_root().join(format!(
            "recovery-index-interleaving-{}",
            std::process::id(),
        ));
        let _ = std::fs::remove_dir_all(&root);
        prepare(&root).unwrap();
        let addr = SessionAddr("0123456789abcdef".into());
        insert(&root, &addr).unwrap();
        complete(&root).unwrap();
        let marker = root.join(DIRECTORY).join(&addr.0);
        std::fs::write(&marker, b"damaged").unwrap();
        assert!(candidates(&root).unwrap().is_none());
        let (checked, check) = std::sync::mpsc::sync_channel(1);
        let (resume, resumed) = std::sync::mpsc::sync_channel(1);
        let rebuilding = root.clone();
        let rebuild = std::thread::spawn(move || {
            AFTER_CHECK.with(|hook| {
                *hook.borrow_mut() = Some(Box::new(move |path| {
                    if path == marker {
                        checked.send(()).unwrap();
                        resumed
                            .recv_timeout(broca_test_support::EVENT_HANG_GUARD)
                            .unwrap();
                    }
                }));
            });
            complete(&rebuilding)
        });
        check
            .recv_timeout(broca_test_support::EVENT_HANG_GUARD)
            .unwrap();
        // The actor's registration is durable before the rebuild acts on its earlier check.
        insert(&root, &addr).unwrap();
        resume.send(()).unwrap();
        rebuild.join().unwrap().unwrap();
        assert_eq!(
            candidates(&root).unwrap().unwrap(),
            vec![addr],
            "publishing complete must not lose the actor's synced membership"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
