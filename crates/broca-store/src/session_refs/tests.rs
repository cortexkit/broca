use super::*;
use broca_core::{ReadOnlyWalReader, WalStore};
use broca_protocol::ModelHandle;
use std::sync::Arc;

fn started(session: &BindIdentity, retained: bool) -> broca_wal::WalRecord {
    let mut record = crate::conformance::run_started(
        1,
        "run",
        session,
        "fixture",
        ModelHandle::new("stub", "model"),
    )
    .record;
    if let broca_wal::WalRecord::RunStarted { retention, .. } = &mut record {
        *retention = retained.then_some(3600);
    }
    record
}

#[test]
fn backfill_maps_every_distinct_run_index_triple_once() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch("CREATE TABLE run_index(run_id TEXT PRIMARY KEY,session TEXT NOT NULL)")
        .unwrap();
    let sessions: Vec<_> = (0_usize..31)
        .map(|i| {
            BindIdentity::new(
                format!("/project/{i}"),
                if i.is_multiple_of(2) {
                    "broca"
                } else {
                    "runner"
                },
                format!("session-{i}"),
            )
        })
        .collect();
    for i in 0..95 {
        conn.execute(
            "INSERT INTO run_index VALUES(?1,?2)",
            params![
                format!("run-{i}"),
                serde_json::to_string(&sessions[i % sessions.len()]).unwrap()
            ],
        )
        .unwrap();
    }
    let counts = backfill(&conn).unwrap();
    assert_eq!(
        (counts.rows, counts.distinct_sessions, counts.inserted),
        (95, 31, 31)
    );
    for session in &sessions {
        let (address,root,harness,name): (String,String,String,String) = conn.query_row(
            "SELECT address,project_root,harness,session FROM agent_data_session_ref WHERE session_ref=?1", [agent_session_ref(session)],
            |row| Ok((row.get(0)?,row.get(1)?,row.get(2)?,row.get(3)?))).unwrap();
        assert_eq!(address, session_addr(session));
        assert_eq!(BindIdentity::new(root, harness, name), *session);
    }
    let counts = backfill(&conn).unwrap();
    assert_eq!(
        (counts.rows, counts.distinct_sessions, counts.inserted),
        (0, 0, 0)
    );
}

#[test]
fn unfinished_backfill_and_legacy_admissions_keep_absence_unknown() {
    let store = SqliteStore::open_in_memory().unwrap();
    backfill(&store.conn.lock().unwrap()).unwrap();
    store
        .conn
        .lock()
        .unwrap()
        .execute("INSERT INTO agent_data_census_done VALUES(1)", [])
        .unwrap();
    let missing = "00000000000000000000000000000000";
    assert!(store.session_refs([missing].into_iter()).unwrap().complete);
    store
        .conn
        .lock()
        .unwrap()
        .execute("DELETE FROM agent_data_projection", [])
        .unwrap();
    let found = store.session_refs([missing].into_iter()).unwrap();
    assert!(found.sessions.is_empty());
    assert!(!found.complete);
    backfill(&store.conn.lock().unwrap()).unwrap();
    let session = BindIdentity::new("/legacy/project", "broca", "session");
    store
        .conn
        .lock()
        .unwrap()
        .execute(
            "INSERT INTO run_index(run_id,session,state) VALUES('old',?1,'active')",
            [serde_json::to_string(&session).unwrap()],
        )
        .unwrap();
    let reference = agent_session_ref(&session);
    let found = store
        .session_refs([reference.as_str()].into_iter())
        .unwrap();
    assert!(found.sessions.is_empty());
    assert!(!found.complete);
    backfill(&store.conn.lock().unwrap()).unwrap();
    store
        .conn
        .lock()
        .unwrap()
        .execute("INSERT INTO agent_data_census_done VALUES(1)", [])
        .unwrap();
    let found = store
        .session_refs([reference.as_str()].into_iter())
        .unwrap();
    assert!(found.complete);
    assert_eq!(found.sessions[&reference].1, session);
}

#[test]
fn store_lookup_survives_live_retained_and_archived_locations() {
    for population in ["live", "retained", "archived"] {
        let root = broca_test_support::scratch_root()
            .join(format!("session-ref-{}-{population}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let index = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
        let wal = broca_core::FileWalStore::new(root.join("wal"));
        wal.set_session_ref_index(index.clone());
        let session = BindIdentity::new("/realistic/project/root", "broca", population);
        let retained = population == "retained";
        wal.open_with_retention(&session, retained.then_some(3600))
            .unwrap()
            .append(1, &started(&session, retained), true)
            .unwrap();
        let bytes = wal.bytes_for_move(&session).unwrap().unwrap();
        if population == "archived" {
            let directory = root.join("wal-archive");
            std::fs::create_dir_all(&directory).unwrap();
            let mut archive =
                broca_wal::archive::ArchiveWriter::create(directory.join("fold-1.ark"))
                    .unwrap()
                    .without_feature_floor();
            archive.append(&session_addr(&session), &bytes).unwrap();
            archive.seal().unwrap();
            std::fs::remove_file(wal.wal_path(&session)).unwrap();
        }
        drop(index);
        migrate(&root.join("run-index.db"), &ReadOnlyWalReader::new(&root)).unwrap();
        let reopened = SqliteStore::open(root.join("run-index.db")).unwrap();
        let reference = agent_session_ref(&session);
        let found = reopened
            .session_refs([reference.as_str()].into_iter())
            .unwrap();
        assert!(found.complete);
        let (address, identity) = &found.sessions[&reference];
        assert_eq!(address, &session_addr(&session));
        assert_eq!(identity, &session);
        assert_eq!(wal.bytes_for_move(identity).unwrap().unwrap(), bytes);
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

#[test]
fn legacy_cleanup_removes_only_inventory_artifacts() {
    let root = broca_test_support::scratch_root()
        .join(format!("inventory-cleanup-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for name in [
        "move-inventory.json",
        "move-inventory.tmp-12-34",
        "move-inventory.tmp-56-78",
        "run-index.db",
        "move-inventory.other",
    ] {
        std::fs::write(root.join(name), b"123456789").unwrap();
    }
    assert_eq!(cleanup_legacy_inventory(&root).unwrap(), (3, 27));
    assert!(root.join("run-index.db").exists());
    assert!(root.join("move-inventory.other").exists());
    assert_eq!(cleanup_legacy_inventory(&root).unwrap(), (0, 0));
}

#[test]
fn locked_projection_keeps_admission_and_repair_recovers_session() {
    let root = broca_test_support::scratch_root()
        .join(format!("session-ref-locked-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let index = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
    wal.set_session_ref_index(index.clone());
    let session = BindIdentity::new("/project", "broca", "during-outage");
    let mut writer = wal.open(&session).unwrap();
    migrate(&root.join("run-index.db"), &ReadOnlyWalReader::new(&root)).unwrap();
    let reference = agent_session_ref(&session);
    assert!(
        index
            .session_refs([reference.as_str()].into_iter())
            .unwrap()
            .complete
    );
    let blocker = Connection::open(root.join("run-index.db")).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    assert_eq!(
        writer.append(1, &started(&session, false), true).unwrap(),
        1
    );
    assert_eq!(writer.replay().unwrap().len(), 1);
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
    let counts = migrate(&root.join("run-index.db"), &ReadOnlyWalReader::new(&root)).unwrap();
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
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/census/project", "broca", "overlap");
    wal.open(&session)
        .unwrap()
        .append(1, &started(&session, false), true)
        .unwrap();
    let address = session_addr(&session);
    let path = wal.wal_path(&session);
    let original = std::fs::read(&path).unwrap();
    let mut damaged = original.clone();
    damaged[21] ^= 1;
    std::fs::write(&path, damaged).unwrap();
    backfill(&store.conn.lock().unwrap()).unwrap();

    let root_b = root.clone();
    let db_b = db.clone();
    let b_completed = std::rc::Rc::new(std::cell::Cell::new(false));
    let b_observed = b_completed.clone();
    census::AFTER_READ_HOOK.with(|hook| {
        *hook.borrow_mut() = Some(Box::new(move || {
            // Pass A has read damaged bytes but has not committed its error. Pass B
            // verifies repaired bytes and commits Covered before A resumes its write.
            std::fs::write(path, original).unwrap();
            let mut conn_b = rusqlite::Connection::open(db_b).unwrap();
            crate::busy::configure_writer_connection(&conn_b).unwrap();
            let covered =
                census::run(&mut conn_b, &ReadOnlyWalReader::new(&root_b), None, false).unwrap();
            assert!(covered.complete);
            b_observed.set(true);
        }))
    });
    let mut conn_a = rusqlite::Connection::open(&db).unwrap();
    crate::busy::configure_writer_connection(&conn_a).unwrap();
    let late = census::run(&mut conn_a, &ReadOnlyWalReader::new(&root), None, false).unwrap();
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
    let resumed = census::run(&mut conn_a, &ReadOnlyWalReader::new(&root), None, false).unwrap();
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
    std::fs::create_dir_all(root.join("wal")).unwrap();
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let session = BindIdentity::new("/census/project", "broca", "unobserved");
    let address = session_addr(&session);
    let reference = agent_session_ref(&session);
    backfill(&store.conn.lock().unwrap()).unwrap();
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
        &ReadOnlyWalReader::new(&root),
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
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    wal.open(&session)
        .unwrap()
        .append(1, &started(&session, false), true)
        .unwrap();
    let verified = census::run(
        &mut store.conn.lock().unwrap(),
        &ReadOnlyWalReader::new(&root),
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
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let mut sessions = Vec::new();
    for i in 0..3 {
        let session = BindIdentity::new("/census/project", "broca", format!("damage-{i}"));
        wal.open(&session)
            .unwrap()
            .append(1, &started(&session, false), true)
            .unwrap();
        sessions.push(session);
    }
    sessions.sort_by_key(broca_core::session_addr);
    let path = wal.wal_path(&sessions[1]);
    let original = std::fs::read(&path).unwrap();
    let mut damaged = original.clone();
    damaged[21] ^= 1;
    std::fs::write(&path, &damaged).unwrap();
    backfill(&store.conn.lock().unwrap()).unwrap();
    let first = census::run(
        &mut store.conn.lock().unwrap(),
        &ReadOnlyWalReader::new(&root),
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
        &ReadOnlyWalReader::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(!second.complete);
    assert_eq!(second.unreadable, 1);
    assert_eq!(
        std::fs::read(&path).unwrap(),
        damaged,
        "census readers must not repair WAL bytes"
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
        &ReadOnlyWalReader::new(&root),
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
fn census_resumes_coverage_without_replaying_covered_wals() {
    let root =
        broca_test_support::scratch_root().join(format!("census-resume-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let mut sessions = Vec::new();
    for i in 0..4 {
        let session = BindIdentity::new("/legacy-import/project", "broca", format!("import-{i}"));
        wal.open(&session)
            .unwrap()
            .append(1, &started(&session, false), true)
            .unwrap();
        sessions.push(session);
    }
    backfill(&store.conn.lock().unwrap()).unwrap();
    let first = census::run(
        &mut store.conn.lock().unwrap(),
        &ReadOnlyWalReader::new(&root),
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
    // If restart rereads a covered payload this corruption makes it fail. The
    // actual move still rejects corrupt bytes; coverage is not byte authority.
    for address in &covered {
        std::fs::write(root.join("wal").join(format!("{address}.wal")), b"corrupt").unwrap();
    }
    drop(store);
    let reopened = SqliteStore::open(root.join("run-index.db")).unwrap();
    let second = census::run(
        &mut reopened.conn.lock().unwrap(),
        &ReadOnlyWalReader::new(&root),
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

#[test]
fn census_repairs_legacy_imports_and_reads_existing_candidates_only_as_metadata() {
    let root =
        broca_test_support::scratch_root().join(format!("census-legacy-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let live = BindIdentity::new("/legacy/project", "broca", "represented");
    let archived = BindIdentity::new("/legacy/project", "broca", "unindexed-archive");
    for session in [&live, &archived] {
        wal.open(session)
            .unwrap()
            .append(1, &started(session, false), true)
            .unwrap();
    }
    store.record_session(&session_addr(&live), &live).unwrap();
    let bytes = wal.bytes_for_move(&archived).unwrap().unwrap();
    std::fs::create_dir_all(root.join("wal-archive")).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
        .unwrap()
        .without_feature_floor();
    writer.append(&session_addr(&archived), &bytes).unwrap();
    writer.seal().unwrap();
    std::fs::remove_file(wal.wal_path(&archived)).unwrap();
    backfill(&store.conn.lock().unwrap()).unwrap();
    let counts = census::run(
        &mut store.conn.lock().unwrap(),
        &ReadOnlyWalReader::new(&root),
        None,
        false,
    )
    .unwrap();
    assert!(counts.complete);
    assert_eq!(counts.addresses, 2);
    assert_eq!(counts.not_represented, 1);
    assert_eq!(
        counts.bytes_read,
        bytes.len() as u64 + 2 * (broca_wal::framing::HEADER_LEN as u64 + 16)
    );
    assert_eq!(wal.bytes_for_move(&archived).unwrap().unwrap(), bytes);
    let reference = agent_session_ref(&archived);
    assert!(store
        .session_refs([reference.as_str()].into_iter())
        .unwrap()
        .sessions
        .contains_key(&reference));
}

#[tokio::test]
async fn provider_free_import_registers_ref_before_acknowledgment() {
    use broca_core::{ImportOrigin, ImportRequest, WalHandle};
    let root = broca_test_support::scratch_root()
        .join(format!("session-ref-import-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
    migrate(&root.join("run-index.db"), &ReadOnlyWalReader::new(&root)).unwrap();
    wal.set_session_ref_index(store.clone());
    let session = BindIdentity::new("/project/import", "broca", "imported");
    let writer = WalHandle::spawn(wal.open(&session).unwrap(), 1).unwrap();
    let broca_wal::WalRecord::RunStarted { input, .. } = started(&session, false) else {
        unreachable!()
    };
    broca_core::run_import(
        ImportRequest {
            project_id: None,
            principal: None,
            run_id: broca_protocol::RunId("import-run".into()),
            session: session.clone(),
            messages: input.to_vec(),
            origin: ImportOrigin {
                source_ref: "fixture:source".into(),
            },
        },
        &writer,
        &wal,
    )
    .await
    .unwrap();
    let reference = agent_session_ref(&session);
    let found = store
        .session_refs([reference.as_str()].into_iter())
        .unwrap();
    assert!(found.complete);
    assert_eq!(found.sessions[&reference].1, session);
    writer.shutdown().await.unwrap();
}

#[cfg(unix)]
#[test]
fn census_over_read_only_state_root_never_changes_logs_or_manifest() {
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
    let root = parent.join("logs");
    std::fs::create_dir_all(&root).unwrap();
    let writer = broca_core::FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/readonly/project", "broca", "live");
    writer
        .open(&session)
        .unwrap()
        .append(1, &started(&session, false), true)
        .unwrap();
    let archived = BindIdentity::new("/readonly/project", "broca", "archived");
    std::fs::create_dir_all(root.join("wal-archive")).unwrap();
    let mut archive =
        broca_wal::archive::ArchiveWriter::create(root.join("wal-archive/fold-1.ark"))
            .unwrap()
            .without_feature_floor();
    archive
        .append(
            &session_addr(&archived),
            &broca_wal::framing::encode(1, 1, &started(&archived, false)).unwrap(),
        )
        .unwrap();
    archive.seal().unwrap();
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
        &ReadOnlyWalReader::new(&root),
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

#[test]
fn failed_gap_marker_refuses_untracked_wal_bytes() {
    struct Failed;
    impl SessionRefIndex for Failed {
        fn record_session(&self, _: &str, _: &BindIdentity) -> Result<(), String> {
            Err("projection unavailable".into())
        }
    }
    let root = broca_test_support::scratch_root()
        .join(format!("session-ref-no-marker-{}", std::process::id()));
    std::fs::create_dir_all(root.join(broca_core::SESSION_PROJECTION_INCOMPLETE)).unwrap();
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    wal.set_session_ref_index(Arc::new(Failed));
    let session = BindIdentity::new("/project", "broca", "no-marker");
    let mut writer = wal.open(&session).unwrap();
    assert!(writer
        .append(1, &started(&session, false), true)
        .unwrap_err()
        .to_string()
        .contains("cannot persist incomplete marker"));
    assert!(writer.replay().unwrap().is_empty());
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
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    wal.set_session_ref_index(Arc::new(Failed));
    let session = BindIdentity::new("/project", "broca", "overlap");
    let repair = broca_core::ProjectionRepair::begin(&root).unwrap().unwrap();
    wal.open(&session)
        .unwrap()
        .append(1, &started(&session, false), true)
        .unwrap();
    assert!(!repair.finish().unwrap());
    assert!(broca_core::session_projection_incomplete(&root).unwrap());
    let installation = wal
        .record_session_before_install(&session_addr(&session), &session)
        .unwrap();
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
        let wal = broca_core::FileWalStore::new(root.join("wal"));
        wal.set_session_ref_index(Arc::new(Failed));
        let session = BindIdentity::new("/project", "broca", "other-process");
        wal.open(&session)
            .unwrap()
            .append(1, &started(&session, false), true)
            .unwrap();
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
