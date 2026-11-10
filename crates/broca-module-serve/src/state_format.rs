//! Refuse unsupported state roots before startup opens any durable stores.

use std::path::Path;

/// A marker is never permission to reinterpret an unsupported root as today's format.
pub(crate) fn prepare(
    root: &Path,
) -> Result<broca_wal::state_format::StateRootDecision<'_>, String> {
    let marker = root.join("state-format");
    match std::fs::read(&marker) {
        Ok(bytes) => {
            return Err(format!(
                "refusing to serve: the state root {} is marked state-format = {:?}, a state \
                 format this build does not run. Start with a fresh state root.",
                root.display(),
                String::from_utf8_lossy(&bytes[..bytes.len().min(64)])
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(format!(
                "refusing to serve: cannot read the state-format marker {}: {error}",
                marker.display()
            ));
        }
    }
    let decision = broca_wal::state_format::decide_state_root(root)
        .map_err(|error| format!("refusing to serve: {error}. Start with a fresh state root."))?;
    if decision.config().is_overhaul() {
        return Err(format!(
            "refusing to serve: the state root {} is marked broca-state/2, a removed staging \
             format. Start with a fresh state root.",
            root.display()
        ));
    }
    Ok(decision)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn broca_state_2_is_refused_by_name_without_changing_the_root() {
        let root = broca_test_support::scratch_root()
            .join(format!("removed-state-format-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        for marker in [b"broca-state/2".as_slice(), b"broca-state/2\n"] {
            std::fs::write(root.join("state-format"), marker).unwrap();
            std::fs::write(root.join("broca.db"), b"old staging data").unwrap();
            // Even an invalid log directory must not hide the unsupported format's name.
            std::fs::write(root.join("wal"), b"not a log directory").unwrap();
            let error = match prepare(&root) {
                Ok(_) => panic!("broca-state/2 must not run as today's format"),
                Err(error) => error,
            };
            assert!(error.contains("broca-state/2"), "{error}");
            assert!(error.contains("fresh state root"), "{error}");
            assert!(!error.contains("runbook"), "{error}");
            assert_eq!(std::fs::read(root.join("state-format")).unwrap(), marker);
            assert_eq!(
                std::fs::read(root.join("broca.db")).unwrap(),
                b"old staging data"
            );
            assert_eq!(
                std::fs::read(root.join("wal")).unwrap(),
                b"not a log directory"
            );
            assert_eq!(std::fs::read_dir(&root).unwrap().count(), 3);
        }
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn unmarked_roots_keep_todays_format_and_unknown_markers_refuse() {
        let root = broca_test_support::scratch_root()
            .join(format!("unmarked-state-format-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        assert!(prepare(&root).unwrap().legacy().is_some());
        assert!(!root.exists());
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("run-index.db"), b"legacy index").unwrap();
        assert!(prepare(&root).unwrap().legacy().is_some());
        std::fs::write(root.join("state-format"), b"unknown-format").unwrap();
        let error = prepare(&root).err().unwrap();
        assert!(error.contains("unknown-format"), "{error}");
        assert!(error.contains("fresh state root"), "{error}");
        assert_eq!(
            std::fs::read(root.join("run-index.db")).unwrap(),
            b"legacy index"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}
