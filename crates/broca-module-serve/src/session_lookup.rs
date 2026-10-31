//! Task-id lookup is isolated here so its backing store can change without changing callers.

use std::path::Path;

use broca_protocol::BindIdentity;

/// Resolve the name as supplied before trying its `alfonso:` spelling. This reads
/// the compact session-ref inventory, not the run history, and never migrates it.
pub fn lookup(root: &Path, id: &str) -> Result<BindIdentity, String> {
    let started = std::time::Instant::now();
    let result = lookup_inner(root, id);
    tracing::debug!(
        elapsed_ms = started.elapsed().as_millis(),
        "session name lookup finished"
    );
    result
}

fn lookup_inner(root: &Path, id: &str) -> Result<BindIdentity, String> {
    let index =
        root.join(broca_store::db::operational_database_name(root).unwrap_or("state-format"));
    let store = broca_store::db::open_session_resolver(root).map_err(|_| {
        format!(
            "cannot read session-ref inventory {} for {id:?}; pass --project-root and --harness",
            index.display()
        )
    })?;
    lookup_with_resolver(store.as_ref(), id)
}

pub fn lookup_with_resolver(
    store: &dyn broca_core::SessionRefResolver,
    id: &str,
) -> Result<BindIdentity, String> {
    let mut names = vec![id.to_owned()];
    if !id.starts_with("alfonso:") {
        names.push(format!("alfonso:{id}"));
    }
    let mut complete = true;
    for name in &names {
        let found = store.named_refs(name, 101).map_err(|_| {
            format!(
                "cannot read session-ref inventory for {name:?}; pass --project-root and --harness"
            )
        })?;
        complete &= found.complete;
        let truncated = found.sessions.len() >= 101;
        let mut candidates: Vec<_> = found.sessions.into_values().map(|(_, bind)| bind).collect();
        candidates.sort_by(|a, b| {
            (&a.project_root, &a.harness, &a.session).cmp(&(
                &b.project_root,
                &b.harness,
                &b.session,
            ))
        });
        candidates.dedup();
        if candidates.len() == 1 && !truncated {
            if !complete {
                return Err(format!("session {name:?} was found in an incomplete session-ref inventory; cannot establish uniqueness; pass --project-root and --harness"));
            }
            return Ok(candidates.remove(0));
        }
        if !candidates.is_empty() {
            let choices = candidates
                .iter()
                .take(100)
                .map(|bind| {
                    format!(
                        "  --project-root {:?} --harness {:?} --session {:?}",
                        bind.project_root, bind.harness, bind.session,
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            return Err(format!(
                "ambiguous session {name:?}; pass --project-root and --harness:\n{choices}{}",
                if truncated {
                    "\n  (more candidates omitted)"
                } else {
                    ""
                }
            ));
        }
    }
    let searched = names
        .iter()
        .map(|name| format!("{name:?}"))
        .collect::<Vec<_>>()
        .join(", ");
    Err(if complete {
        format!("unknown session; searched {searched}; pass --project-root and --harness")
    } else {
        format!("session not found in an incomplete session-ref inventory; searched {searched}; pass --project-root and --harness")
    })
}

#[cfg(test)]
mod operational_port_tests {
    use super::*;
    use broca_core::{SessionInventory, SessionRefIndex};

    #[test]
    fn root_aware_lookup_keeps_absent_incomplete_and_error_classifications() {
        let root = broca_test_support::scratch_root()
            .join(format!("broca-lookup-ports-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(root.join("state-format"), b"broca-state/2\n").unwrap();
        assert!(lookup(&root, "name")
            .unwrap_err()
            .contains("cannot read session-ref inventory"));
        assert!(!root.join("broca.db").exists());
        let db = broca_store::db::OverhaulDb::open(&root).unwrap();
        assert!(lookup(&root, "name")
            .unwrap_err()
            .contains("incomplete session-ref inventory"));
        let session = BindIdentity::new("/project", "runner", "alfonso:name");
        db.record_session(&broca_core::session_addr(&session), &session)
            .unwrap();
        assert!(lookup(&root, "name")
            .unwrap_err()
            .contains("cannot establish uniqueness"));
        db.rebuild_inventory(&root).unwrap();
        assert_eq!(lookup(&root, "name").unwrap(), session);
        assert!(lookup(&root, "missing")
            .unwrap_err()
            .starts_with("unknown session;"));
        for name in ["run-index.db", "run-index.db-wal", "run-index.db-shm"] {
            std::fs::write(root.join(name), b"operator sentinel").unwrap();
        }
        assert_eq!(lookup(&root, "name").unwrap(), session);
        for name in ["run-index.db", "run-index.db-wal", "run-index.db-shm"] {
            assert_eq!(
                std::fs::read(root.join(name)).unwrap(),
                b"operator sentinel"
            );
        }
        let other = BindIdentity::new("/other", "runner", "alfonso:name");
        db.record_session(&broca_core::session_addr(&other), &other)
            .unwrap();
        assert!(lookup(&root, "name")
            .unwrap_err()
            .starts_with("ambiguous session"));
        std::fs::write(root.join("state-format"), b"unknown-format").unwrap();
        assert!(lookup(&root, "name")
            .unwrap_err()
            .contains("cannot read session-ref inventory"));
        drop(db);
        std::fs::remove_dir_all(root).unwrap();
    }
}
