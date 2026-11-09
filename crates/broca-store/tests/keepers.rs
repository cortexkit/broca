use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use broca_core::{ExportCursor, ExportIndex, SessionRefIndex};
use broca_protocol::{BindIdentity, BindPrincipal, RunId};
use broca_store::{
    session_address::{InventoryUnavailable, ScopeReservation, SessionAddressError},
    ExportStore, SessionInventory, SqliteStore,
};
use rusqlite::{Connection, OpenFlags};

struct Fixture(PathBuf);

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "keepers-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let fixture = Self(root.join("run-index.db"));
        std::fs::write(&fixture.0, include_bytes!("fixtures/keepers-v044.db")).unwrap();
        fixture
    }

    fn conn(&self) -> Connection {
        Connection::open_with_flags(&self.0, OpenFlags::SQLITE_OPEN_READ_WRITE).unwrap()
    }

    fn snapshot(&self) -> Vec<String> {
        let conn = self.conn();
        let mut result = vec![format!(
            "user_version={}",
            conn.query_row("PRAGMA user_version", [], |row| row.get::<_, i64>(0))
                .unwrap()
        )];
        let mut schema = conn
            .prepare("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
            .unwrap();
        for row in schema
            .query_map([], |row| {
                Ok(format!(
                    "{:?}",
                    (
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, Option<String>>(3)?
                    )
                ))
            })
            .unwrap()
        {
            result.push(row.unwrap());
        }
        let tables = conn
            .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
            .unwrap()
            .query_map([], |row| row.get::<_, String>(0))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        for table in tables {
            let mut statement = conn.prepare(&format!("SELECT * FROM \"{table}\"")).unwrap();
            let count = statement.column_count();
            let mut rows = statement
                .query_map([], |row| {
                    (0..count)
                        .map(|column| row.get::<_, rusqlite::types::Value>(column))
                        .collect::<Result<Vec<_>, _>>()
                })
                .unwrap()
                .map(|row| format!("{table}:{:?}", row.unwrap()))
                .collect::<Vec<_>>();
            rows.sort();
            result.extend(rows);
        }
        result
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        std::fs::remove_dir_all(self.0.parent().unwrap()).unwrap();
    }
}

fn known() -> BindIdentity {
    BindIdentity::new("/fixture/original", "fixture", "known")
}

fn reservation() -> ScopeReservation {
    ScopeReservation {
        owner: BindPrincipal::Reserved {
            module_id: "core".into(),
        },
        scope_ref: "fixture-scope".into(),
    }
}

#[test]
fn standalone_export_reads_legacy_fixture_without_changing_schema_or_rows() {
    let fixture = Fixture::new();
    let before = fixture.snapshot();
    let export = ExportStore::open(&fixture.0).unwrap();
    let page = export.read_export_page(None, 1).unwrap();
    assert_eq!(page.facts.len(), 1);
    assert_eq!(page.facts[0].export_seq, 1);
    assert_eq!(
        page.facts[0].segment.as_session().unwrap().run_id.0,
        "fixture-run"
    );
    assert_eq!(export.high_water(), 2);
    assert_eq!(page.high_water, 2);
    assert_eq!(export.source_instance_id(), page.source_instance_id);
    assert_eq!(export.source_epoch(), page.source_epoch);
    let next = export.read_export_page(page.next.clone(), 1).unwrap();
    assert_eq!(next.facts.len(), 1);
    assert_eq!(next.facts[0].export_seq, 2);
    assert!(export
        .read_export_page(next.next, 1)
        .unwrap()
        .facts
        .is_empty());
    let stale = ExportCursor {
        source_instance_id: "another-source".into(),
        source_epoch: page.source_epoch,
        export_seq: 0,
    };
    assert!(matches!(
        export.read_export_page(Some(stale), 1),
        Err(broca_core::ExportError::EpochChanged { high_water: 2, .. })
    ));
    assert!(export
        .is_imported_run(&known(), &RunId("fixture-imported".into()))
        .unwrap());
    assert!(!export
        .is_imported_run(&known(), &RunId("local-run".into()))
        .unwrap());
    drop(export);
    assert_eq!(fixture.snapshot(), before);
    let legacy = SqliteStore::open_read_only(&fixture.0).unwrap();
    assert_eq!(legacy.read_export_page(None, 1).unwrap(), page);
}

#[test]
fn standalone_export_appends_imported_floor_and_system_one_facts_in_legacy_tables() {
    let fixture = Fixture::new();
    let export = ExportStore::open(&fixture.0).unwrap();
    export
        .record_imported_runs(&known(), "new-cut", &[RunId("another-imported".into())])
        .unwrap();
    export
        .record_imported_runs(&known(), "new-cut", &[RunId("another-imported".into())])
        .unwrap();
    let legacy = SqliteStore::open_read_only(&fixture.0).unwrap();
    assert!(legacy
        .is_imported_run(&known(), &RunId("another-imported".into()))
        .unwrap());
    assert!(legacy
        .is_imported_run(&known(), &RunId("fixture-imported".into()))
        .unwrap());
    assert_eq!(
        fixture
            .conn()
            .query_row("SELECT COUNT(*) FROM export_imported_runs", [], |row| row
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        2
    );
    let fact = broca_core::system_one_fact::SystemOneFact {
        fact_id: "fixture-system-one".into(),
        batch_id: "fixture-batch".into(),
        item_index: 0,
        provider: "fixture-provider".into(),
        model: "fixture-model".into(),
        auth_selection: None,
        account_ref: None,
        run_id: None,
        flow_id: None,
        charge_basis: broca_core::ChargeBasis::Unknown,
        charge_basis_mapping_version: 1,
        charge_basis_overrides: String::new(),
        usage: broca_core::system_one_fact::SystemOneUsage {
            input_tokens: Some(11),
            output_tokens: Some(3),
        },
        occurred_at_ms: 1234,
    };
    assert!(export.emit_system_one_fact(&fact).unwrap());
    assert!(!export.emit_system_one_fact(&fact).unwrap());
    let mut collision = fact.clone();
    collision.occurred_at_ms += 1;
    assert!(export.emit_system_one_fact(&collision).is_err());
    assert_eq!(export.high_water(), 3);
    assert_eq!(
        legacy.read_export_page(None, 100).unwrap(),
        export.read_export_page(None, 100).unwrap()
    );
    drop(export);
    let reopened = ExportStore::open(&fixture.0).unwrap();
    assert_eq!(reopened.high_water(), 3);
    assert_eq!(reopened.read_export_page(None, 100).unwrap().facts.len(), 3);
}

#[test]
fn standalone_inventory_reads_legacy_roots_aliases_scope_and_ambiguity_without_writes() {
    let fixture = Fixture::new();
    let before = fixture.snapshot();
    let inventory = SessionInventory::open(&fixture.0).unwrap();
    assert_eq!(
        inventory
            .resolve_session_address(&known(), Some(Path::new("/different/stamp")), None)
            .unwrap(),
        known()
    );
    assert_eq!(
        inventory.scope_reservation(&known()).unwrap(),
        Some(reservation())
    );
    let v1 = broca_core::wal_writer::legacy_session_ref("fixture", "/fixture/original", "known");
    let v2 = broca_core::agent_session_ref(&known());
    let refs = inventory
        .session_refs([v1.as_str(), v2.as_str()].into_iter())
        .unwrap();
    assert!(refs.complete);
    assert_eq!(refs.sessions.len(), 2);
    for key in [v1, v2] {
        assert_eq!(refs.sessions[&key].1, known());
    }
    let named = inventory.session_refs_named("known", 10).unwrap();
    assert!(named.complete);
    assert_eq!(named.sessions.len(), 1);
    let ambiguous = BindIdentity::new("/anywhere", "fixture", "ambiguous");
    assert_eq!(
        inventory.resolve_session_address(&ambiguous, None, None),
        Err(SessionAddressError::Ambiguous)
    );
    assert!(inventory
        .session_refs([broca_core::agent_session_ref(&ambiguous).as_str()].into_iter())
        .is_err());
    drop(inventory);
    assert_eq!(fixture.snapshot(), before);
}

#[test]
fn standalone_inventory_reserves_once_and_records_candidates_in_legacy_tables() {
    let fixture = Fixture::new();
    let inventory = SessionInventory::open(&fixture.0).unwrap();
    let new = BindIdentity::new("/ignored", "fixture", "new");
    let resolved = inventory
        .resolve_session_address(&new, Some(Path::new("/new/stamp")), Some(&reservation()))
        .unwrap();
    assert_eq!(resolved, BindIdentity::new("/new/stamp", "fixture", "new"));
    assert_eq!(
        inventory.resolve_session_address(&new, None, None).unwrap(),
        resolved
    );
    let unscoped = BindIdentity::new("/ignored", "fixture", "unscoped");
    assert_eq!(
        inventory
            .resolve_session_address(&unscoped, None, None)
            .unwrap()
            .project_root,
        Path::new("broca-unscoped-v1")
    );
    let candidate = BindIdentity::new("/candidate", "fixture", "candidate");
    inventory
        .record_session(&broca_core::session_addr(&candidate), &candidate)
        .unwrap();
    drop(inventory);
    let legacy = SqliteStore::open_read_only(&fixture.0).unwrap();
    assert_eq!(
        legacy.scope_reservation(&resolved).unwrap(),
        Some(reservation())
    );
    assert_eq!(
        legacy
            .resolve_session_address(&candidate, None, None)
            .unwrap(),
        candidate
    );
    assert_eq!(
        legacy.resolve_session_address(&new, None, None).unwrap(),
        resolved
    );
}

#[test]
fn standalone_inventory_does_not_turn_incomplete_discovery_or_gaps_into_absence() {
    let fixture = Fixture::new();
    fixture
        .conn()
        .execute("DELETE FROM agent_data_census_done", [])
        .unwrap();
    let inventory = SessionInventory::open(&fixture.0).unwrap();
    let missing = BindIdentity::new("/ignored", "fixture", "missing");
    assert_eq!(
        inventory.resolve_session_address(&missing, None, None),
        Err(SessionAddressError::Unavailable(
            InventoryUnavailable::IncompleteInventory
        ))
    );
    assert_eq!(
        inventory
            .resolve_session_address(&known(), None, None)
            .unwrap(),
        known()
    );
    let refs = inventory
        .session_refs([broca_core::agent_session_ref(&missing).as_str()].into_iter())
        .unwrap();
    assert!(!refs.complete);
    assert!(refs.sessions.is_empty());
    fixture
        .conn()
        .execute("INSERT INTO agent_data_census_done VALUES(1)", [])
        .unwrap();
    std::fs::write(
        fixture
            .0
            .with_file_name("session-ref-projection.incomplete"),
        [],
    )
    .unwrap();
    assert_eq!(
        inventory.resolve_session_address(&missing, None, None),
        Err(SessionAddressError::Unavailable(
            InventoryUnavailable::GapMarker
        ))
    );
    assert!(
        !inventory
            .session_refs_named("missing", 10)
            .unwrap()
            .complete
    );
}

#[test]
fn standalone_opens_refuse_missing_databases_and_each_required_table_by_name() {
    let fixture = Fixture::new();
    let missing = fixture.0.with_file_name("missing.db");
    assert!(ExportStore::open(&missing)
        .err()
        .unwrap()
        .to_string()
        .contains("missing.db"));
    assert!(SessionInventory::open(&missing)
        .err()
        .unwrap()
        .to_string()
        .contains("missing.db"));
    assert!(!missing.exists());
    for (owner, tables) in [
        (
            "ExportStore",
            vec!["export_meta", "export_facts", "export_imported_runs"],
        ),
        (
            "SessionInventory",
            vec![
                "agent_data_session_ref",
                "agent_data_session_ref_v2",
                "session_scope_reservation",
                "agent_data_projection",
                "agent_data_census",
                "agent_data_census_inflight",
                "agent_data_census_unreadable",
                "agent_data_census_done",
                "agent_data_pending_session",
            ],
        ),
    ] {
        for table in tables {
            let fixture = Fixture::new();
            fixture
                .conn()
                .execute_batch(&format!("DROP TABLE {table}"))
                .unwrap();
            let before = fixture.snapshot();
            let error = if owner == "ExportStore" {
                ExportStore::open(&fixture.0).err().unwrap().to_string()
            } else {
                SessionInventory::open(&fixture.0)
                    .err()
                    .unwrap()
                    .to_string()
            };
            assert!(error.contains(owner) && error.contains(table), "{error}");
            assert_eq!(fixture.snapshot(), before);
        }
    }
}
