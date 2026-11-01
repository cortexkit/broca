/// A legacy-only operator cannot use a root whose marker does not prove it is legacy.
#[derive(Debug)]
pub struct LegacyOnlyOperator {
    pub operator: String,
    pub root: std::path::PathBuf,
    pub reason: Option<broca_wal::state_format::StateRootError>,
}

impl std::fmt::Display for LegacyOnlyOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{} is legacy-only: refusing state root {}",
            self.operator,
            self.root.display()
        )?;
        match &self.reason {
            Some(reason) => write!(f, ": {reason}"),
            None => write!(f, " marked broca-state/2"),
        }
    }
}

impl std::error::Error for LegacyOnlyOperator {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.reason.as_ref().map(|reason| reason as _)
    }
}

/// Classify without preparing the root or opening its legacy database. Unknown and unreadable
/// markers are not evidence of a legacy root, including when legacy files already exist.
pub fn require_legacy_operator_root(
    root: &std::path::Path,
    operator: impl Into<String>,
) -> Result<(), LegacyOnlyOperator> {
    let reason = match broca_wal::state_format::classify_state_root(root) {
        Ok(config) if !config.is_overhaul() => return Ok(()),
        Ok(_) => None,
        Err(reason) => Some(reason),
    };
    Err(LegacyOnlyOperator {
        operator: operator.into(),
        root: root.to_path_buf(),
        reason,
    })
}

/// Read only: even preview must not create or migrate a store. An absent database
/// supplies no identities, while a database read failure must not masquerade as absence.
pub fn fold_identities(
    root: &std::path::Path,
) -> Result<
    std::collections::BTreeMap<String, broca_protocol::BindIdentity>,
    Box<dyn std::error::Error>,
> {
    require_legacy_operator_root(root, "fold-identities")?;
    let database = root.join("run-index.db");
    let mut identities = std::collections::BTreeMap::new();
    if !database.try_exists()? {
        return Ok(identities);
    }
    let conn = rusqlite::Connection::open_with_flags(
        database,
        rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?;
    let mut statement = conn.prepare("SELECT session FROM meta")?;
    let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
    for row in rows {
        let raw = row?;
        let fields: Vec<_> = raw.split('\u{1f}').collect();
        let [project, harness, session] = fields.as_slice() else {
            continue;
        };
        let identity = broca_protocol::BindIdentity::new(*project, *harness, *session);
        if identity.validate_keyable().is_err() {
            continue;
        }
        identities.insert(broca_core::session_addr(&identity), identity);
    }
    Ok(identities)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fold_identity_reader_refuses_before_legacy_database_access() {
        for (index, marker) in [Some(b"broca-state/2".as_slice()), Some(b"malformed"), None]
            .into_iter()
            .enumerate()
        {
            for existing in [false, true] {
                let root = broca_test_support::scratch_root().join(format!(
                    "fold-identities-{}-{index}-{existing}",
                    std::process::id()
                ));
                std::fs::create_dir(&root).unwrap();
                match marker {
                    Some(bytes) => std::fs::write(root.join("state-format"), bytes).unwrap(),
                    None => std::fs::create_dir(root.join("state-format")).unwrap(),
                }
                for name in [
                    "run-index.db",
                    "run-index.db-wal",
                    "run-index.db-shm",
                    "operator-data",
                ] {
                    if existing || name == "operator-data" {
                        std::fs::write(root.join(name), name).unwrap();
                    }
                }
                let error = fold_identities(&root).unwrap_err();
                let refusal = error.downcast_ref::<LegacyOnlyOperator>().unwrap();
                assert_eq!(refusal.operator, "fold-identities");
                assert_eq!(refusal.root, root);
                for name in [
                    "run-index.db",
                    "run-index.db-wal",
                    "run-index.db-shm",
                    "operator-data",
                ] {
                    if existing || name == "operator-data" {
                        assert_eq!(std::fs::read(root.join(name)).unwrap(), name.as_bytes());
                    } else {
                        assert!(!root.join(name).exists());
                    }
                }
                assert_eq!(
                    std::fs::read_dir(&root).unwrap().count(),
                    if existing { 5 } else { 2 }
                );
                std::fs::remove_dir_all(root).unwrap();
            }
        }
    }

    #[test]
    fn missing_marker_preserves_legacy_fold_identity_lookup() {
        let root = broca_test_support::scratch_root()
            .join(format!("legacy-fold-identities-{}", std::process::id()));
        std::fs::create_dir(&root).unwrap();
        assert!(fold_identities(&root).unwrap().is_empty());
        assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
        let conn = rusqlite::Connection::open(root.join("run-index.db")).unwrap();
        conn.execute_batch("CREATE TABLE meta(session TEXT);")
            .unwrap();
        let key = broca_protocol::BindIdentity::new("/project", "harness", "session");
        conn.execute(
            "INSERT INTO meta VALUES (?1)",
            ["/project\u{1f}harness\u{1f}session"],
        )
        .unwrap();
        drop(conn);
        let before = std::fs::read(root.join("run-index.db")).unwrap();
        let identities = fold_identities(&root).unwrap();
        assert_eq!(identities.get(&broca_core::session_addr(&key)), Some(&key));
        assert_eq!(std::fs::read(root.join("run-index.db")).unwrap(), before);
        assert!(!root.join("state-format").exists());
        assert!(!root.join("run-index.db-wal").exists());
        assert!(!root.join("run-index.db-shm").exists());
        std::fs::remove_dir_all(root).unwrap();
    }
}
