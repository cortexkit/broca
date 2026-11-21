use super::*;
use std::sync::Arc;

/// Stores `session` as the session store keeps a session, with only its head, in storage
/// class `class` of the state root `root`.
pub(super) fn stored_in(
    root: &std::path::Path,
    session: &BindIdentity,
    class: broca_wal::StorageClass,
) {
    use broca_session_store::{
        backend::{LocalBackend, SessionAddr},
        head::SessionTriple,
        keys::{Cipher, DeploymentKey},
        session::Session,
    };
    let backend = LocalBackend::new(class.store_root(root));
    let key = DeploymentKey::new(7, &[4; 32]).creating(Cipher::None);
    let triple = SessionTriple {
        project_root: session.project_root.display().to_string(),
        harness: session.harness.clone(),
        session: session.session.clone(),
    };
    tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap()
        .block_on(async {
            Session::create(
                backend,
                SessionAddr(session_addr(session)),
                triple,
                &key,
                false,
            )
            .await
            .unwrap()
            .close()
            .await
            .unwrap();
        });
}

/// As [`stored_in`], in the durable class.
pub(super) fn stored(root: &std::path::Path, session: &BindIdentity) {
    stored_in(root, session, broca_wal::StorageClass::Durable);
}

/// The head file of `session`, stored by [`stored`].
pub(super) fn head_path(root: &std::path::Path, session: &BindIdentity) -> std::path::PathBuf {
    broca_wal::StorageClass::Durable
        .store_root(root)
        .join("heads")
        .join(session_addr(session))
        .join("head")
}

/// A ref the index lacks is absent only once a census (the pass that records every stored
/// session in the index) has finished: before that it is unknown, so a move asking is told to
/// retry.
#[test]
fn absence_is_unknown_until_a_census_finishes() {
    let store = SqliteStore::open_in_memory().unwrap();
    let missing = "00000000000000000000000000000000";
    let found = store.session_refs([missing].into_iter()).unwrap();
    assert!(found.sessions.is_empty());
    assert!(!found.complete);
    store
        .conn
        .lock()
        .unwrap()
        .execute("INSERT INTO agent_data_census_done VALUES(1)", [])
        .unwrap();
    assert!(store.session_refs([missing].into_iter()).unwrap().complete);
}

#[test]
fn store_lookup_finds_a_session_in_any_storage_class() {
    for class in broca_wal::StorageClass::ALL {
        let root = broca_test_support::scratch_root()
            .join(format!("session-ref-{}-{class}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        drop(SqliteStore::open(root.join("run-index.db")).unwrap());
        let session = BindIdentity::new("/realistic/project/root", "broca", class.name());
        stored_in(&root, &session, class);
        migrate(&root.join("run-index.db"), &StoredSessions::new(&root)).unwrap();
        let reopened = SqliteStore::open(root.join("run-index.db")).unwrap();
        let reference = agent_session_ref(&session);
        let found = reopened
            .session_refs([reference.as_str()].into_iter())
            .unwrap();
        assert!(found.complete);
        let (address, identity) = &found.sessions[&reference];
        assert_eq!(address, &session_addr(&session));
        assert_eq!(identity, &session);
    }
}

#[test]
fn historical_triples_never_alias_a_physical_address() {
    let store = SqliteStore::open_in_memory().unwrap();
    let old = BindIdentity::new("/project", "broca", "old");
    let current = BindIdentity::new("/project", "broca", "current");
    store.record_session(&session_addr(&current), &old).unwrap();
    assert!(store
        .session_refs([agent_session_ref(&old).as_str()].into_iter())
        .unwrap()
        .sessions
        .is_empty());
}

/// A session admitted while the index can't take its row keeps the index marked incomplete, so
/// its ref is unknown rather than absent, and a census then finds it in the session store.
#[test]
fn locked_projection_keeps_admission_and_repair_recovers_session() {
    let root = broca_test_support::scratch_root()
        .join(format!("session-ref-locked-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let index = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
    let session = BindIdentity::new("/project", "broca", "during-outage");
    migrate(&root.join("run-index.db"), &StoredSessions::new(&root)).unwrap();
    let reference = agent_session_ref(&session);
    assert!(
        index
            .session_refs([reference.as_str()].into_iter())
            .unwrap()
            .complete
    );
    let blocker = Connection::open(root.join("run-index.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let admission = broca_core::record_candidate(
        Some(index.as_ref()),
        &root,
        &session_addr(&session),
        &session,
    )
    .unwrap();
    assert!(admission.is_some());
    stored(&root, &session);
    drop(admission);
    let marker = root.join(broca_core::SESSION_PROJECTION_INCOMPLETE);
    assert_eq!(std::fs::metadata(&marker).unwrap().len(), 0);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(&marker).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    let missing = index
        .session_refs([reference.as_str()].into_iter())
        .unwrap();
    assert!(!missing.complete);
    assert!(missing.sessions.is_empty());
    blocker.execute_batch("ROLLBACK").unwrap();
    let counts = migrate(&root.join("run-index.db"), &StoredSessions::new(&root)).unwrap();
    assert!(counts.complete);
    assert_eq!(counts.not_represented, 1);
    assert!(!marker.exists());
    let found = index
        .session_refs([reference.as_str()].into_iter())
        .unwrap();
    assert!(found.complete);
    assert_eq!(found.sessions[&reference].1, session);
}

#[test]
fn overlapping_census_late_unreadable_cannot_undo_committed_coverage() {
    let root = broca_test_support::scratch_root().join("census-overlap-late-unreadable");
    std::fs::create_dir_all(&root).unwrap();
    let db = root.join("run-index.db");
    let store = SqliteStore::open(&db).unwrap();
    let session = BindIdentity::new("/census/project", "broca", "overlap");
    stored(&root, &session);
    let address = session_addr(&session);
    let path = head_path(&root, &session);
    let original = std::fs::read(&path).unwrap();
    std::fs::write(&path, b"damaged head").unwrap();

    let root_b = root.clone();
    let db_b = db.clone();
    let b_completed = std::rc::Rc::new(std::cell::Cell::new(false));
    let b_observed = b_completed.clone();
    census::AFTER_READ_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            // Pass A has read a damaged head but has not committed its error. Pass B reads
            // the repaired head and commits Covered before A resumes its write.
            std::fs::write(path, original).unwrap();
            let mut conn_b = rusqlite::Connection::open(db_b).unwrap();
            crate::busy::configure_writer_connection(&conn_b).unwrap();
            let covered =
                census::run(&mut conn_b, &StoredSessions::new(&root_b), None, false).unwrap();
            assert!(covered.complete);
            b_observed.set(true);
        }))
    });
    let mut conn_a = rusqlite::Connection::open(&db).unwrap();
    crate::busy::configure_writer_connection(&conn_a).unwrap();
    let late = census::run(&mut conn_a, &StoredSessions::new(&root), None, false).unwrap();
    assert!(b_completed.get());
    assert!(late.complete);
    assert_eq!(late.unreadable, 0);
    let unreadable: u64 = conn_a
        .query_row(
            "SELECT count(*) FROM agent_data_census_unreadable",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        unreadable, 0,
        "a stale failed read must not quarantine an address another pass covered"
    );

    // Obsolete observations left for already covered addresses do not count as
    // pending damage or make a complete projection permanently unverifiable.
    conn_a
        .execute(
            "INSERT INTO agent_data_census_unreadable VALUES(?1,'obsolete error')",
            [&address],
        )
        .unwrap();
    conn_a
        .execute("DELETE FROM agent_data_census_done", [])
        .unwrap();
    let resumed = census::run(&mut conn_a, &StoredSessions::new(&root), None, false).unwrap();
    assert!(resumed.complete);
    assert_eq!(resumed.unreadable, 0);
    let reference = agent_session_ref(&session);
    let refs = store
        .session_refs([reference.as_str()].into_iter())
        .unwrap();
    assert!(refs.complete);
    assert_eq!(refs.sessions[&reference].1, session);
}

#[test]
fn census_unobserved_inventory_address_remains_uncovered_until_verified() {
    let root = broca_test_support::scratch_root().join("census-unobserved-address");
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let session = BindIdentity::new("/census/project", "broca", "unobserved");
    let address = session_addr(&session);
    let reference = agent_session_ref(&session);
    {
        let conn = store.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO agent_data_census(address) VALUES(?1)",
            [&address],
        )
        .unwrap();
        conn.execute("INSERT INTO agent_data_census_done VALUES(1)", [])
            .unwrap();
    }
    assert!(
        !store
            .session_refs([reference.as_str()].into_iter())
            .unwrap()
            .complete,
        "a stale done marker cannot attest a still-uncovered address"
    );
    let missing = census::run(
        &mut store.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(!missing.complete);
    assert_eq!(missing.unreadable, 1);
    {
        let conn = store.conn.lock().unwrap();
        let covered: bool = conn
            .query_row(
                "SELECT covered FROM agent_data_census WHERE address=?1",
                [&address],
                |row| row.get(0),
            )
            .unwrap();
        assert!(
            !covered,
            "not observed in an inventory is not proof of verified coverage"
        );
        let reason: String = conn
            .query_row(
                "SELECT reason FROM agent_data_census_unreadable WHERE address=?1",
                [&address],
                |row| row.get(0),
            )
            .unwrap();
        assert!(reason.starts_with("inventory_unobserved:"));
    }
    stored(&root, &session);
    let verified = census::run(
        &mut store.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(verified.complete);
    assert_eq!(verified.unreadable, 0);
    let refs = store
        .session_refs([reference.as_str()].into_iter())
        .unwrap();
    assert!(refs.complete);
    assert_eq!(refs.sessions[&reference].1, session);
}

#[test]
fn census_quarantines_damage_and_commits_healthy_coverage_across_restarts() {
    let root = broca_test_support::scratch_root().join("census-quarantine-restart");
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let mut sessions = Vec::new();
    for i in 0..3 {
        let session = BindIdentity::new("/census/project", "broca", format!("damage-{i}"));
        stored(&root, &session);
        sessions.push(session);
    }
    sessions.sort_by_key(broca_core::session_addr);
    let path = head_path(&root, &sessions[1]);
    let original = std::fs::read(&path).unwrap();
    let damaged = b"damaged head".to_vec();
    std::fs::write(&path, &damaged).unwrap();
    let first = census::run(
        &mut store.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        Some(2),
        false,
    )
    .unwrap();
    assert!(!first.complete);
    assert_eq!(first.unreadable, 1);
    {
        let conn = store.conn.lock().unwrap();
        let covered: u64 = conn
            .query_row(
                "SELECT count(*) FROM agent_data_census WHERE covered=1",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            covered, 1,
            "damage must not discard an earlier healthy action in the same batch"
        );
        let error: String = conn
            .query_row(
                "SELECT reason FROM agent_data_census_unreadable WHERE address=?1",
                [broca_core::session_addr(&sessions[1])],
                |row| row.get(0),
            )
            .unwrap();
        assert!(!error.is_empty());
        // Simulate a committed candidate without its Covered action. Restart must
        // replay this address even though its first candidate row already exists.
        conn.execute(
            "UPDATE agent_data_census SET covered=0 WHERE address=?1",
            [broca_core::session_addr(&sessions[0])],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO agent_data_census_inflight VALUES(?1)",
            [broca_core::session_addr(&sessions[0])],
        )
        .unwrap();
    }
    drop(store);
    let reopened = SqliteStore::open(root.join("run-index.db")).unwrap();
    let second = census::run(
        &mut reopened.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(!second.complete);
    assert_eq!(second.unreadable, 1);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        damaged,
        "census readers must not repair a session head"
    );
    for session in [&sessions[0], &sessions[2]] {
        let reference = agent_session_ref(session);
        let refs = reopened
            .session_refs([reference.as_str()].into_iter())
            .unwrap();
        assert!(!refs.complete, "quarantined history is not absence");
        assert_eq!(&refs.sessions[&reference].1, session);
    }
    let conn = reopened.conn.lock().unwrap();
    let covered: u64 = conn
        .query_row(
            "SELECT count(*) FROM agent_data_census WHERE covered=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(covered, 2);
    let inflight: u64 = conn
        .query_row(
            "SELECT count(*) FROM agent_data_census_inflight",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(inflight, 0);
    drop(conn);
    std::fs::write(&path, &original).unwrap();
    let repaired = census::run(
        &mut reopened.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(repaired.complete);
    assert_eq!(repaired.unreadable, 0);
    for session in sessions {
        let reference = agent_session_ref(&session);
        let refs = reopened
            .session_refs([reference.as_str()].into_iter())
            .unwrap();
        assert!(refs.complete);
        assert_eq!(refs.sessions[&reference].1, session);
    }
}

#[test]
fn census_resumes_coverage_without_rereading_covered_sessions() {
    let root =
        broca_test_support::scratch_root().join(format!("census-resume-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let mut sessions = Vec::new();
    for i in 0..4 {
        let session = BindIdentity::new("/census/project", "broca", format!("resume-{i}"));
        stored(&root, &session);
        sessions.push(session);
    }
    let first = census::run(
        &mut store.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        Some(2),
        false,
    )
    .unwrap();
    assert!(!first.complete);
    assert_eq!(first.not_represented, 2);
    assert!(
        !store
            .session_refs(["00000000000000000000000000000000"].into_iter())
            .unwrap()
            .complete
    );
    let covered: Vec<String> = {
        let conn = store.conn.lock().unwrap();
        let mut statement = conn
            .prepare("SELECT address FROM agent_data_census WHERE covered=1")
            .unwrap();
        statement
            .query_map([], |row| row.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap()
    };
    // If restart rereads a covered session this damage makes it fail. A move still opens the
    // head with its key and refuses a damaged one; coverage is not authority over the session.
    for address in &covered {
        std::fs::write(
            broca_wal::StorageClass::Durable
                .store_root(&root)
                .join("heads")
                .join(address)
                .join("head"),
            b"corrupt",
        )
        .unwrap();
    }
    drop(store);
    let reopened = SqliteStore::open(root.join("run-index.db")).unwrap();
    let second = census::run(
        &mut reopened.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(second.complete);
    assert_eq!(second.resumed, 2);
    assert_eq!(second.not_represented, 2);
    for session in sessions {
        let reference = agent_session_ref(&session);
        let found = reopened
            .session_refs([reference.as_str()].into_iter())
            .unwrap();
        assert!(found.complete);
        assert_eq!(found.sessions[&reference].1, session);
    }
}

/// A head whose session belongs at another address is damaged (copied, or written to the wrong
/// folder): the census quarantines the address rather than map a ref to the wrong folder.
#[test]
fn census_quarantines_a_head_that_names_another_address() {
    let root =
        broca_test_support::scratch_root().join(format!("census-misplaced-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let session = BindIdentity::new("/census/project", "broca", "misplaced");
    stored(&root, &session);
    let heads = broca_wal::StorageClass::Durable
        .store_root(&root)
        .join("heads");
    let elsewhere = "0123456789abcdef";
    std::fs::create_dir_all(heads.join(elsewhere)).unwrap();
    std::fs::copy(
        head_path(&root, &session),
        heads.join(elsewhere).join("head"),
    )
    .unwrap();
    let counts = census::run(
        &mut store.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(!counts.complete);
    assert_eq!(counts.unreadable, 1);
    let reason: String = store
        .conn
        .lock()
        .unwrap()
        .query_row(
            "SELECT reason FROM agent_data_census_unreadable WHERE address=?1",
            [elsewhere],
            |row| row.get(0),
        )
        .unwrap();
    assert!(reason.contains("another address"), "{reason}");
}

/// The census records only the sessions the index doesn't name yet, and reads each head once.
#[test]
fn census_records_only_unrepresented_sessions() {
    let root = broca_test_support::scratch_root()
        .join(format!("census-unrepresented-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let represented = BindIdentity::new("/census/project", "broca", "represented");
    let unindexed = BindIdentity::new("/census/project", "broca", "unindexed");
    for session in [&represented, &unindexed] {
        stored(&root, session);
    }
    store
        .record_session(&session_addr(&represented), &represented)
        .unwrap();
    let counts = census::run(
        &mut store.conn.lock().unwrap(),
        &StoredSessions::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(counts.complete);
    assert_eq!(counts.addresses, 2);
    assert_eq!(counts.not_represented, 1);
    let store_root = broca_wal::StorageClass::Durable.store_root(&root);
    let backend = broca_session_store::backend::LocalBackend::new(&store_root);
    let heads: u64 = [&represented, &unindexed]
        .into_iter()
        .map(|session| {
            let addr = broca_session_store::backend::SessionAddr(session_addr(session));
            backend.read_stored_head(&addr).unwrap().unwrap().0.len() as u64
        })
        .sum();
    assert_eq!(counts.bytes_read, heads);
    let reference = agent_session_ref(&unindexed);
    assert!(store
        .session_refs([reference.as_str()].into_iter())
        .unwrap()
        .sessions
        .contains_key(&reference));
}

#[cfg(unix)]
#[test]
fn census_over_read_only_state_root_never_changes_sessions_or_manifest() {
    use std::os::unix::fs::PermissionsExt;
    fn snapshot(
        path: &std::path::Path,
    ) -> BTreeMap<std::path::PathBuf, (Vec<u8>, std::time::SystemTime)> {
        let mut result = BTreeMap::new();
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                result.extend(snapshot(&path));
            } else {
                result.insert(
                    path.clone(),
                    (
                        std::fs::read(&path).unwrap(),
                        std::fs::metadata(&path).unwrap().modified().unwrap(),
                    ),
                );
            }
        }
        result
    }
    fn protect(
        path: &std::path::Path,
        saved: &mut Vec<(std::path::PathBuf, std::fs::Permissions)>,
    ) {
        for entry in std::fs::read_dir(path).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                protect(&path, saved);
            } else {
                saved.push((
                    path.clone(),
                    std::fs::metadata(&path).unwrap().permissions(),
                ));
                std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o400)).unwrap();
            }
        }
        saved.push((
            path.to_owned(),
            std::fs::metadata(path).unwrap().permissions(),
        ));
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o500)).unwrap();
    }
    struct Restore(Vec<(std::path::PathBuf, std::fs::Permissions)>);
    impl Drop for Restore {
        fn drop(&mut self) {
            for (path, permissions) in self.0.drain(..).rev() {
                std::fs::set_permissions(path, permissions).unwrap();
            }
        }
    }
    let parent =
        broca_test_support::scratch_root().join(format!("readonly-census-{}", std::process::id()));
    let root = parent.join("state");
    std::fs::create_dir_all(&root).unwrap();
    let session = BindIdentity::new("/readonly/project", "broca", "durable");
    stored(&root, &session);
    let retained = BindIdentity::new("/readonly/project", "broca", "retained");
    stored_in(&root, &retained, broca_wal::StorageClass::Ephemeral);
    std::fs::write(
        root.join("move-inventory.json"),
        b"cleanup must not run in a census",
    )
    .unwrap();
    let store = SqliteStore::open(parent.join("run-index.copy.db")).unwrap();
    let mut restore = Restore(Vec::new());
    protect(&root, &mut restore.0);
    let before = snapshot(&root);
    let counts = migrate(
        &parent.join("run-index.copy.db"),
        &StoredSessions::new(&root),
    )
    .unwrap();
    assert!(counts.complete);
    assert_eq!(counts.not_represented, 2);
    assert_eq!(snapshot(&root), before);
    assert!(
        store
            .session_refs([agent_session_ref(&session).as_str()].into_iter())
            .unwrap()
            .complete
    );
}

/// An admission whose ref the index can't take, and whose incomplete marker can't be written
/// either, is refused: nothing would tell a move the ref is merely unknown.
#[test]
fn failed_gap_marker_refuses_the_admission() {
    struct Failed;
    impl SessionRefIndex for Failed {
        fn record_session(&self, _: &str, _: &BindIdentity) -> Result<(), String> {
            Err("projection unavailable".into())
        }
    }
    let root = broca_test_support::scratch_root()
        .join(format!("session-ref-no-marker-{}", std::process::id()));
    std::fs::create_dir_all(root.join(broca_core::SESSION_PROJECTION_INCOMPLETE)).unwrap();
    let session = BindIdentity::new("/project", "broca", "no-marker");
    let Err(error) =
        broca_core::record_candidate(Some(&Failed), &root, &session_addr(&session), &session)
    else {
        panic!("the admission was accepted without a marker");
    };
    assert!(
        error.contains("cannot persist incomplete marker"),
        "{error}"
    );
}

#[test]
fn repair_cannot_clear_gap_across_an_overlapping_admission_or_installation() {
    struct Failed;
    impl SessionRefIndex for Failed {
        fn record_session(&self, _: &str, _: &BindIdentity) -> Result<(), String> {
            Err("projection unavailable".into())
        }
    }
    let root = broca_test_support::scratch_root()
        .join(format!("session-ref-overlap-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let session = BindIdentity::new("/project", "broca", "overlap");
    let address = session_addr(&session);
    let repair = broca_core::ProjectionRepair::begin(&root).unwrap().unwrap();
    // An admission while the repair runs: its incomplete-index marker outlives the repair that
    // began first.
    drop(broca_core::record_candidate(Some(&Failed), &root, &address, &session).unwrap());
    assert!(!repair.finish().unwrap());
    assert!(broca_core::session_projection_incomplete(&root).unwrap());
    let installation =
        broca_core::record_candidate(Some(&Failed), &root, &address, &session).unwrap();
    assert!(installation.is_some());
    assert!(broca_core::ProjectionRepair::begin(&root)
        .unwrap()
        .is_none());
    drop(installation);
    assert!(broca_core::ProjectionRepair::begin(&root)
        .unwrap()
        .unwrap()
        .finish()
        .unwrap());
    assert!(!broca_core::session_projection_incomplete(&root).unwrap());
}

#[test]
fn projection_repair_observes_other_process_writers() {
    const CHILD_ROOT: &str = "BROCA_SESSION_REF_GAP_TEST_ROOT";
    if let Some(root) = std::env::var_os(CHILD_ROOT) {
        struct Failed;
        impl SessionRefIndex for Failed {
            fn record_session(&self, _: &str, _: &BindIdentity) -> Result<(), String> {
                Err("projection unavailable".into())
            }
        }
        let root = std::path::PathBuf::from(root);
        let session = BindIdentity::new("/project", "broca", "other-process");
        drop(
            broca_core::record_candidate(Some(&Failed), &root, &session_addr(&session), &session)
                .unwrap(),
        );
        return;
    }
    let root = broca_test_support::scratch_root()
        .join(format!("session-ref-cross-process-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let repair = broca_core::ProjectionRepair::begin(&root).unwrap().unwrap();
    struct Reap(std::process::Child);
    impl Drop for Reap {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let mut child = Reap(
        std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "session_refs::tests::projection_repair_observes_other_process_writers",
                "--exact",
            ])
            .env(CHILD_ROOT, &root)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap(),
    );
    let status = broca_test_support::wait_until_sync("projection-gap child writer", || {
        child
            .0
            .try_wait()
            .map_err(|error| error.to_string())?
            .ok_or_else(|| "child writer still running".to_owned())
    });
    assert!(status.success());
    assert!(!repair.finish().unwrap());
    assert!(broca_core::session_projection_incomplete(&root).unwrap());
}

#[test]
fn session_ref_v2_startup_rekey_is_idempotent_and_keeps_v1() {
    let store = SqliteStore::open_in_memory().unwrap();
    let session = BindIdentity::new("/synthetic/project", "runner", "ref-migration");
    let v1 = broca_core::session_refs::legacy_session_ref(
        &session.harness,
        &session.project_root.to_string_lossy(),
        &session.session,
    );
    let v2 = agent_session_ref(&session);
    let address = session_addr(&session);
    {
        let conn = store.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO agent_data_session_ref VALUES(?1,?2,?3,?4,?5)",
            params![v1, address, "/synthetic/project", "runner", "ref-migration"],
        )
        .unwrap();
        init_schema(&conn).unwrap();
        let changes = conn.total_changes();
        init_schema(&conn).unwrap();
        assert_eq!(
            conn.total_changes(),
            changes,
            "repeated startup must not rewrite aliases"
        );
    }
    let found = store
        .session_refs([v1.as_str(), v2.as_str()].into_iter())
        .unwrap();
    assert_eq!(found.sessions[&v1], (address.clone(), session.clone()));
    assert_eq!(found.sessions[&v2], (address, session));
}

/// A start skips the alias walk when the alias rule and the session count are as the last walk
/// left them, and walks again for a session an older build added without its alias.
#[test]
fn session_ref_v2_rekey_runs_only_after_a_change() {
    let store = SqliteStore::open_in_memory().unwrap();
    let conn = store.conn.lock().unwrap();
    let add = |name: &str| {
        let session = BindIdentity::new("/synthetic/project", "runner", name);
        let v1 = broca_core::session_refs::legacy_session_ref(
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        );
        conn.execute(
            "INSERT INTO agent_data_session_ref VALUES(?1,?2,?3,?4,?5)",
            params![
                v1,
                session_addr(&session),
                "/synthetic/project",
                "runner",
                name
            ],
        )
        .unwrap();
        agent_session_ref(&session)
    };
    let aliased = |alias: &str| -> bool {
        conn.query_row(
            "SELECT count(*) FROM agent_data_session_ref_v2 WHERE session_ref=?1",
            [alias],
            |row| row.get::<_, i64>(0),
        )
        .unwrap()
            > 0
    };
    let first = add("first");
    init_schema(&conn).unwrap();
    assert!(aliased(&first));
    // An older build adds a session and no alias; the count tells the next start to walk.
    let second = add("second");
    assert!(!aliased(&second));
    init_schema(&conn).unwrap();
    assert!(aliased(&second));
    let recorded: i64 = conn
        .query_row(
            "SELECT sessions FROM agent_data_session_ref_v2_rule WHERE id=1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(recorded, 2);
}

#[test]
fn session_ref_v2_ambiguous_roots_refuse_but_v1_stays_exact() {
    let store = SqliteStore::open_in_memory().unwrap();
    let first = BindIdentity::new("/synthetic/one", "runner", "shared");
    let second = BindIdentity::new("/synthetic/two", "runner", "shared");
    for session in [&first, &second] {
        store
            .record_session(&session_addr(session), session)
            .unwrap();
        let v1 = broca_core::session_refs::legacy_session_ref(
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        );
        assert_eq!(
            store
                .session_refs([v1.as_str()].into_iter())
                .unwrap()
                .sessions[&v1]
                .1,
            *session
        );
    }
    assert_ne!(session_addr(&first), session_addr(&second));
    let v2 = agent_session_ref(&first);
    assert!(matches!(
        store.session_refs([v2.as_str()].into_iter()),
        Err(StoreError::InconsistentSnapshot("session_ref_ambiguous"))
    ));
}
