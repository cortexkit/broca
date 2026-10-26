use broca_owner_only::OpenOptions;
use broca_protocol::BindIdentity;
#[cfg(unix)]
use std::fs::File;
use std::io::{self, Write};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::lease::LeaseHandle;

#[cfg(test)]
type Observer = Box<dyn FnMut(&str)>;

#[cfg(test)]
thread_local! {
    pub(super) static OBSERVE: std::cell::RefCell<Option<Observer>> =
        const { std::cell::RefCell::new(None) };
}

fn observed(_operation: &str) {
    #[cfg(test)]
    OBSERVE.with(|hook| {
        if let Some(hook) = hook.borrow_mut().as_mut() {
            hook(_operation);
        }
    });
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> io::Result<()> {
    File::open(path)?.sync_all()
}

#[cfg(not(unix))]
fn sync_directory(_path: &Path) -> io::Result<()> {
    // As with WAL creation, non-Unix durability covers file contents, not the directory
    // entry. A lost rename falls back to the intact archive; subsequent appends can be
    // lost. This inherited platform exposure is bounded by the archived transcript.
    Ok(())
}

/// What permits publishing a restored archive member as a session's live log.
///
/// Restoring writes a new live file, so it needs the same single-writer authority as
/// an append. The owned writer open passes the live lease of its own tenure, and the
/// restore refuses before any I/O unless that lease is the session's own. Lease-free
/// store opens (guarded standalone fixtures and today's lease-returning writer open)
/// keep their existing behaviour: their caller holds the session lease outside this
/// signature, so they declare that explicitly instead of passing a token.
pub(super) enum RestoreAuthority<'a> {
    /// The open's owned session tenure; the same lease later moves into the writer.
    OwnedTenure {
        lease: &'a dyn LeaseHandle,
        session: &'a BindIdentity,
    },
    /// A guarded open whose caller holds the lease outside this call.
    GuardedStandalone,
}

impl RestoreAuthority<'_> {
    /// Refuse a tenure that belongs to another session. The key is the namespaced
    /// session lease key, so a lease for any other identity, backend or module fails.
    fn verify(&self) -> io::Result<()> {
        match self {
            Self::OwnedTenure { lease, session } => {
                if lease.key() == &crate::lease::lease_key_for(session) {
                    Ok(())
                } else {
                    Err(io::Error::new(
                        io::ErrorKind::PermissionDenied,
                        format!(
                            "archive restore refused: the tenure is not the session lease for '{}'",
                            session.session
                        ),
                    ))
                }
            }
            Self::GuardedStandalone => Ok(()),
        }
    }
}

/// Called only by append admission, with the session lease held and no live WAL open.
/// The authority is checked before the first filesystem operation.
pub(super) fn restore(
    path: &Path,
    payload: &[u8],
    authority: RestoreAuthority<'_>,
) -> io::Result<()> {
    authority.verify()?;
    observed("resolved");
    let parent = path.parent().expect("canonical WAL has a parent");
    // A restored state tree may contain archives without a wal directory. Sync each
    // newly created directory's parent too, so the final rename is actually reachable.
    let mut missing = Vec::new();
    let mut directory = parent;
    while !directory.as_os_str().is_empty() && !directory.try_exists()? {
        missing.push(directory.to_path_buf());
        directory = directory.parent().unwrap_or_else(|| Path::new(""));
    }
    broca_owner_only::create_dir_all(parent)?;
    observed("directory created");
    for directory in missing.iter().rev() {
        let ancestor = directory
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        sync_directory(ancestor)?;
        observed("ancestor synced");
    }
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let (temporary, mut file) = loop {
        let temporary = parent.join(format!(
            ".extract-{}-{}.tmp",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        match options.open(&temporary) {
            Ok(file) => break (temporary, file),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error),
        }
    };
    let result = (|| {
        observed("temp created");
        for bytes in payload.chunks(4096) {
            file.write_all(bytes)?;
            observed("temp written");
        }
        file.sync_all()?;
        observed("temp synced");
        drop(file);
        // Before rename, live is absent and readers serve the intact archive. After
        // rename, live-wins sees a COMPLETE file, never a partially written transcript.
        // The temp shares the canonical directory so this cannot cross filesystems.
        std::fs::rename(&temporary, path)?;
        observed("renamed");
        // Existing FileWal opens set dir_needs_sync=false: no later append barrier
        // will make this rename durable. Unix sync failures must abort admission.
        sync_directory(parent)?;
        observed("parent synced");
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

#[cfg(test)]
mod authority_tests {
    use super::*;
    use crate::lease::{lease_key_for, FileLeaseStore, LeaseStore};

    fn scratch(tag: &str) -> std::path::PathBuf {
        let root = broca_test_support::scratch_root().join(format!(
            "broca-extraction-authority-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&root);
        root
    }

    #[test]
    fn restore_refuses_another_sessions_tenure_before_any_io() {
        let root = scratch("foreign");
        let leases = FileLeaseStore::new(root.join("leases"));
        let session = BindIdentity::new("/p", "runner", "mine");
        let other = BindIdentity::new("/p", "runner", "theirs");
        let foreign = leases.acquire(&lease_key_for(&other)).unwrap();
        let path = root.join("wal/nested/session.wal");
        let operations = std::rc::Rc::new(std::cell::Cell::new(0));
        let seen = operations.clone();
        OBSERVE.with(|hook| *hook.borrow_mut() = Some(Box::new(move |_| seen.set(seen.get() + 1))));
        let error = restore(
            &path,
            b"archive payload",
            RestoreAuthority::OwnedTenure {
                lease: foreign.as_ref(),
                session: &session,
            },
        )
        .expect_err("a tenure for another session must not restore this log");
        OBSERVE.with(|hook| *hook.borrow_mut() = None);
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(operations.get(), 0, "refusal precedes every restore step");
        assert!(
            !root.join("wal").exists(),
            "no directory, temporary or live file may be created"
        );
        drop(foreign);
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn restore_publishes_under_the_sessions_own_tenure() {
        let root = scratch("own");
        let leases = FileLeaseStore::new(root.join("leases"));
        let session = BindIdentity::new("/p", "runner", "mine");
        let lease = leases.acquire(&lease_key_for(&session)).unwrap();
        let path = root.join("wal/session.wal");
        restore(
            &path,
            b"archive payload",
            RestoreAuthority::OwnedTenure {
                lease: lease.as_ref(),
                session: &session,
            },
        )
        .unwrap();
        assert_eq!(std::fs::read(&path).unwrap(), b"archive payload");
        drop(lease);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(all(test, windows))]
mod windows_acl_tests {
    use super::*;
    use broca_owner_only::test_support::{
        assert_protected_private as assert_private, observe, BroadParent,
    };
    #[test]
    fn windows_extraction_temporary_and_replacement_are_private() {
        let fixture = BroadParent::new("extraction");
        let path = fixture.path.join("nested/session.wal");
        let seen = std::rc::Rc::new(std::cell::Cell::new(0));
        let output = seen.clone();
        observe(
            move |path| {
                assert_private(path);
                output.set(output.get() + 1);
            },
            || {
                restore(
                    &path,
                    b"archive payload",
                    RestoreAuthority::GuardedStandalone,
                )
                .unwrap()
            },
        );
        assert_eq!(
            seen.get(),
            1,
            "temporary extraction must be inspected before rename"
        );
        assert_private(&path);
        assert_private(&fixture.path.join("nested"));
        assert_eq!(std::fs::read(&path).unwrap(), b"archive payload");
    }
}
