use super::scenarios::{edit_plane, edit_request, one_edit_scripts};
use broca_crash_harness::{KillTrigger, Render, RunnerHandle, Scenario};
use broca_protocol::{BindIdentity, RunFinishReason};

fn request() -> broca_core::RunRequest {
    let mut request = edit_request();
    request.session = BindIdentity::new("/stamped/worktree", "runner", "address-crash");
    request
}

const FIRST_SEND: Scenario = Scenario {
    name: "session_address_first_send",
    request,
    control: broca_core::RunControl::default,
    scripts: one_edit_scripts,
    render: Render::Canonical,
    plane: edit_plane,
    expected: RunFinishReason::Completed,
};

#[tokio::test]
async fn interrupted_first_send_resumes_under_its_reserved_workdir_address() {
    let handle = RunnerHandle::new(FIRST_SEND).await;
    let bind = BindIdentity::new("/original/transport", "runner", "address-crash");
    let fixture = broca_test_support::scratch_root()
        .join(format!("address-crash-index-{}", std::process::id()));
    std::fs::create_dir_all(&fixture).unwrap();
    let database = fixture.join("run-index.db");
    {
        let index = broca_store::SqliteStore::open(&database).unwrap();
        broca_store::session_refs::migrate(
            &database,
            &broca_core::ReadOnlyWalReader::new(&fixture),
        )
        .unwrap();
        assert_eq!(
            index
                .resolve_session_address(&bind, Some(std::path::Path::new("/stamped/worktree")))
                .unwrap(),
            request().session
        );
    }
    let reference = broca_core::agent_session_ref(&request().session);
    let inventory = {
        let index = broca_store::SqliteStore::open_read_only(&database).unwrap();
        let inventory = index
            .session_refs(std::iter::once(reference.as_str()))
            .unwrap();
        assert!(
            inventory.complete,
            "the closed source inventory must be complete"
        );
        assert_eq!(inventory.sessions[&reference].1, request().session);
        inventory
    };
    let snapshot_files: Vec<_> = ["run-index.db", "run-index.db-wal", "run-index.db-shm"]
        .into_iter()
        .filter(|name| fixture.join(name).try_exists().unwrap())
        .collect();
    assert!(
        snapshot_files.contains(&"run-index.db-wal"),
        "the fixture must exercise committed, uncheckpointed SQLite pages"
    );
    assert!(
        std::fs::metadata(fixture.join("run-index.db-wal"))
            .unwrap()
            .len()
            > 0
    );
    for point in handle.record_boundaries(true) {
        let kill = handle.kill_at(point, KillTrigger::Truncate).await;
        // The bind reservation was committed before the interrupted first send.
        // Store close deliberately retains uncheckpointed SQLite pages, so the main
        // database alone is not a snapshot. Copy its closed file family, including
        // the WAL and any shared-memory index, alongside each durable Broca WAL cut.
        let recovered_database = kill.state_root.join("run-index.db");
        for name in &snapshot_files {
            let destination = kill.state_root.join(name);
            assert!(
                !destination.try_exists().unwrap(),
                "{}: do not overwrite an existing SQLite snapshot member {name}",
                kill.label()
            );
            std::fs::copy(fixture.join(name), destination).unwrap();
        }
        {
            let index = broca_store::SqliteStore::open_read_only(&recovered_database).unwrap();
            let copied = index
                .session_refs(std::iter::once(reference.as_str()))
                .unwrap();
            assert!(
                copied.complete,
                "{}: copying the snapshot must preserve completeness without repair",
                kill.label()
            );
            assert_eq!(copied.sessions, inventory.sessions, "{}", kill.label());
        }
        let index = broca_store::SqliteStore::open(&recovered_database).unwrap();
        let rebound = BindIdentity::new("/different/transport", "runner", "address-crash");
        let recovered = index
            .resolve_session_address(&rebound, Some(std::path::Path::new("/stamped/worktree")))
            .unwrap();
        assert_eq!(recovered, request().session, "{}", kill.label());
        let resumed = handle.resume(&kill).await;
        handle.check(&kill, &resumed);
        let started: Vec<_> = resumed
            .log
            .records()
            .into_iter()
            .filter_map(|record| match record {
                broca_wal::WalRecord::RunStarted { session, .. } => Some(session),
                _ => None,
            })
            .collect();
        assert_eq!(started, [recovered], "{}", kill.label());
    }
    std::fs::remove_dir_all(fixture).unwrap();
}
