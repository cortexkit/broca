//! Reads a session the way a test outside the daemon can: from disk, without a writer.
//!
//! A real-daemon test runs broca with `session_storage.encryption` off, so the sessions it
//! creates are stored plain and read without the deployment key. Include this file with
//! `#[path = "common/stored_log.rs"] mod stored_log;` (or the path from the test's own folder).

#![allow(dead_code)]

use std::path::Path;

use broca_protocol::BindIdentity;
use broca_session_store::{
    backend::{LocalBackend, SessionAddr},
    head::SessionTriple,
    keys::DeploymentKey,
    record::Record,
    session::Session,
};

/// The settings a real-daemon test merges into broca's config so the sessions it creates are
/// readable here.
pub fn plain_storage() -> serde_json::Value {
    serde_json::json!({"session_storage": {"encryption": "off"}})
}

/// Every record of `identity`'s open log in the state root `state_root`, in log order. Empty
/// while the session doesn't exist. Records an earlier seal moved into sealed history are not
/// in the open log; the short sessions tests make are never sealed.
pub async fn stored_records(state_root: &Path, identity: &BindIdentity) -> Vec<Record> {
    stored_log_records(state_root, identity)
        .await
        .into_iter()
        .map(|(_, record)| record)
        .collect()
}

/// [`stored_records`] with each record's log sequence number.
pub async fn stored_log_records(state_root: &Path, identity: &BindIdentity) -> Vec<(u64, Record)> {
    let address = broca_core::session_addr(identity);
    let Some(class) = broca_wal::session_storage_class(state_root, &address).unwrap() else {
        return Vec::new();
    };
    let view = match Session::view(
        LocalBackend::new(class.store_root(state_root)),
        SessionAddr(address),
        &SessionTriple {
            project_root: identity.project_root.display().to_string(),
            harness: identity.harness.clone(),
            session: identity.session.clone(),
        },
        &DeploymentKey::unloaded(1),
    )
    .await
    {
        Ok(view) => view,
        // A refused creating send removes the session it made. Its storage class is found until
        // the removal finishes, so a read in between finds no head: the session doesn't exist.
        Err(broca_session_store::session::SessionError::Missing) => return Vec::new(),
        Err(error) => panic!("a plain session reads without a writer: {error:?}"),
    };
    view.open_log_records().await.expect("the open log reads")
}

/// The frozen config of each run `identity`'s session started, in the order the runs started,
/// read back from each run's start record and the plain setup parts its storage root holds.
pub async fn stored_run_configs(
    state_root: &Path,
    identity: &BindIdentity,
) -> Vec<broca_wal::RunConfig> {
    let address = broca_core::session_addr(identity);
    let Some(class) = broca_wal::session_storage_class(state_root, &address).unwrap() else {
        return Vec::new();
    };
    let backend = LocalBackend::new(class.store_root(state_root));
    let loaded = broca_session_store::setup::SetupStore::new(
        &backend,
        &broca_session_store::setup::SetupKey::plain(),
    )
    .load_all()
    .await
    .expect("the root's plain setup parts load");
    let parts = broca_session_loop::parts::DeploymentParts::loaded(loaded);
    stored_records(state_root, identity)
        .await
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => Some(
                broca_session_loop::run::run_config_of_start(start, &parts)
                    .expect("a run's start reads back its config"),
            ),
            _ => None,
        })
        .collect()
}

/// Every spend fact the module in `state_root` wrote to its usage export, as JSON, in export
/// order. Empty before the module created its export.
pub fn stored_spend_facts(state_root: &Path) -> Vec<serde_json::Value> {
    let path = state_root.join("run-index.db");
    if !path.exists() {
        return Vec::new();
    }
    let db =
        rusqlite::Connection::open_with_flags(&path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
            .expect("the usage export opens read-only");
    let mut statement = db
        .prepare("SELECT segment_json FROM export_facts ORDER BY export_seq")
        .expect("the usage export has its facts table");
    statement
        .query_map([], |row| row.get::<_, String>(0))
        .expect("the usage export reads")
        .map(|row| serde_json::from_str(&row.expect("a fact row reads")).expect("a fact is JSON"))
        .collect()
}

/// Every file under the session store's roots in `state_root`, with its size: a snapshot two
/// readings of which are equal exactly when nothing was written to any session in between.
/// The recovery index's completeness marker is left out: the startup inventory writes it on its
/// own schedule, which on a slow machine falls after a test's first reading.
pub fn stored_files(state_root: &Path) -> std::collections::BTreeMap<std::path::PathBuf, u64> {
    fn walk(dir: &Path, out: &mut std::collections::BTreeMap<std::path::PathBuf, u64>) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let meta = entry.metadata().unwrap();
            if meta.is_dir() {
                walk(&path, out);
            } else if !path.ends_with("recovery-candidates/complete") {
                out.insert(path, meta.len());
            }
        }
    }
    let mut out = std::collections::BTreeMap::new();
    for class in broca_wal::StorageClass::ALL {
        walk(&class.store_root(state_root), &mut out);
    }
    out
}

/// What each run `identity`'s session started was admitted with (its model, input, tools,
/// frozen plan manifest, scope and the rest), in the order the runs started.
pub async fn stored_admissions(
    state_root: &Path,
    identity: &BindIdentity,
) -> Vec<broca_session_loop::run::Admission> {
    let address = broca_core::session_addr(identity);
    let Some(class) = broca_wal::session_storage_class(state_root, &address).unwrap() else {
        return Vec::new();
    };
    let backend = LocalBackend::new(class.store_root(state_root));
    let loaded = broca_session_store::setup::SetupStore::new(
        &backend,
        &broca_session_store::setup::SetupKey::plain(),
    )
    .load_all()
    .await
    .expect("the root's plain setup parts load");
    let parts = broca_session_loop::parts::DeploymentParts::loaded(loaded);
    stored_records(state_root, identity)
        .await
        .iter()
        .filter_map(|record| match record {
            Record::RunStart(start) => Some(
                broca_session_loop::run::admission_of_start(start, &parts)
                    .expect("a run's start reads back its admission"),
            ),
            _ => None,
        })
        .collect()
}
