//! Root-aware construction for operators that still require the legacy store.
//! Daemon boot selection is deliberately separate from this factory.

use std::{path::Path, sync::Arc};

use broca_core::{FileLeaseStore, FileWalStore};
use broca_store::{require_legacy_operator_root, LegacyOnlyOperator, SqliteStore, StoreError};

pub struct LegacyBundle {
    pub wal: Arc<FileWalStore>,
    pub leases: Arc<FileLeaseStore>,
    pub store: Arc<SqliteStore>,
}

#[derive(Debug)]
pub enum BundleError {
    LegacyOnly(LegacyOnlyOperator),
    Store(StoreError),
}

impl std::fmt::Display for BundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::LegacyOnly(error) => error.fmt(f),
            Self::Store(error) => error.fmt(f),
        }
    }
}

impl std::error::Error for BundleError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::LegacyOnly(error) => Some(error),
            Self::Store(error) => Some(error),
        }
    }
}

/// Keep the legacy WAL, lease and SQLite construction together so secondary executables cannot
/// accidentally compose a legacy store on an overhaul root. No fallback store is fabricated.
pub fn open_legacy_bundle(root: &Path, operator: &str) -> Result<LegacyBundle, BundleError> {
    require_legacy_operator_root(root, operator).map_err(BundleError::LegacyOnly)?;
    let wal = Arc::new(FileWalStore::new(root.join("wal")));
    let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
    let store = Arc::new(SqliteStore::open(root.join("run-index.db")).map_err(BundleError::Store)?);
    wal.set_session_ref_index(store.clone());
    Ok(LegacyBundle { wal, leases, store })
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) fn root(label: &str) -> std::path::PathBuf {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "operator-{label}-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir(&root).unwrap();
        root
    }

    pub(crate) fn refused_roots(label: &str, invoke: impl Fn(&Path)) {
        for marker in [
            Some(b"broca-state/2\n".as_slice()),
            Some(b"malformed"),
            None,
        ] {
            for existing in [false, true] {
                let root = root(label);
                match marker {
                    Some(bytes) => std::fs::write(root.join("state-format"), bytes).unwrap(),
                    None => std::fs::create_dir(root.join("state-format")).unwrap(),
                }
                let mut preserved = vec![("operator-data", b"keep operator data".as_slice())];
                if existing {
                    preserved.extend([
                        ("run-index.db", b"not a SQLite database".as_slice()),
                        ("run-index.db-wal", b"preserve WAL"),
                        ("run-index.db-shm", b"preserve SHM"),
                    ]);
                }
                for (name, bytes) in &preserved {
                    std::fs::write(root.join(name), bytes).unwrap();
                }
                invoke(&root);
                assert_eq!(
                    std::fs::read_dir(&root).unwrap().count(),
                    preserved.len() + 1
                );
                for (name, bytes) in preserved {
                    assert_eq!(std::fs::read(root.join(name)).unwrap(), bytes);
                }
                match marker {
                    Some(bytes) => {
                        assert_eq!(std::fs::read(root.join("state-format")).unwrap(), bytes)
                    }
                    None => assert!(root.join("state-format").is_dir()),
                }
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }

    #[test]
    fn legacy_bundle_refuses_before_opening_any_legacy_dependency() {
        refused_roots("bundle", |root| {
            let error = match open_legacy_bundle(root, "test-operator") {
                Err(BundleError::LegacyOnly(error)) => error,
                _ => panic!("expected a typed legacy-only refusal"),
            };
            assert_eq!(error.operator, "test-operator");
            assert_eq!(error.root, root);
        });
    }

    #[test]
    fn legacy_bundle_keeps_direct_store_construction_and_existing_data() {
        let root = root("legacy-bundle");
        let direct = root.join("direct.db");
        drop(SqliteStore::open(&direct).unwrap());
        let expected = std::fs::read(&direct).unwrap();
        let bundle = open_legacy_bundle(&root, "test-operator").unwrap();
        drop(bundle);
        assert_eq!(std::fs::read(root.join("run-index.db")).unwrap(), expected);
        let conn = rusqlite::Connection::open(root.join("run-index.db")).unwrap();
        conn.execute_batch(
            "CREATE TABLE operator_data(value TEXT); INSERT INTO operator_data VALUES ('keep');",
        )
        .unwrap();
        drop(conn);
        drop(open_legacy_bundle(&root, "test-operator").unwrap());
        let conn = rusqlite::Connection::open(root.join("run-index.db")).unwrap();
        assert_eq!(
            conn.query_row("SELECT value FROM operator_data", [], |row| row
                .get::<_, String>(0))
                .unwrap(),
            "keep"
        );
        assert!(!root.join("state-format").exists());
        drop(conn);
        std::fs::remove_dir_all(root).unwrap();
    }
}
