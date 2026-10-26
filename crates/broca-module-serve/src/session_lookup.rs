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
    let index = root.join("run-index.db");
    let store = broca_store::SqliteStore::open_read_only(&index).map_err(|_| {
        format!(
            "cannot read session-ref inventory {} for {id:?}; pass --project-root and --harness",
            index.display()
        )
    })?;
    let mut names = vec![id.to_owned()];
    if !id.starts_with("alfonso:") {
        names.push(format!("alfonso:{id}"));
    }
    let mut complete = true;
    for name in &names {
        let found = store.session_refs_named(name, 101).map_err(|_| {
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
