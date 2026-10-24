use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::atomic::{AtomicU64, Ordering};

use broca_core::{FileWalStore, Persister, WalStore};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::ModelHandle,
    BindIdentity, RunId,
};
use broca_store::SqliteStore;
use broca_wal::{FramedRecord, RunConfig, RunOrigin, SeedOriginKind, WalRecord};
use rusqlite::Connection;

static NEXT_ROOT: AtomicU64 = AtomicU64::new(0);

fn state_root(label: &str) -> PathBuf {
    let root = broca_test_support::scratch_root().join(format!(
        "broca-store-maintenance-{label}-{}-{}",
        std::process::id(),
        NEXT_ROOT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(root.join("wal")).expect("create temporary state root");
    root
}

fn session(root: &Path, label: &str) -> BindIdentity {
    BindIdentity::new(root.join("project"), "harness", label)
}

fn import_records(session: &BindIdentity) -> Vec<FramedRecord> {
    vec![
        FramedRecord {
            seq: 1,
            fence: 1,
            record: WalRecord::RunStarted {
                written_at_ms: None,
                tags: None,
                session_class: None,
                retention: None,
                hooks: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                marker_order_version: None,
                manifest: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                ts_ms: None,
                run_id: RunId("maintenance-run".into()),
                session: session.clone(),
                config: Box::new(RunConfig {
                    turn_directives_v1: None,
                    render_tags: false,
                    computed_tags: None,
                    model: ModelHandle::new("import", "import"),
                    tools: Vec::new().into(),
                    tool_choice: Default::default(),
                    generation: Default::default(),
                    response_format: Default::default(),
                    provider_options: serde_json::Value::Null,
                    cache_tiers: serde_json::Value::Null,
                    context_epoch: 0,
                    renderer_version: 1,
                    frozen_render_config: serde_json::Value::Null,
                    auth_selection: None,
                    tools_were_omitted: false,
                    budget: None,
                    context_limit: None,
                    work_class: None,
                    variant: None,
                    service_tier: None,
                    stop_when: Vec::new(),
                    context_management: None,
                }),
                input: vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "maintenance fixture".into(),
                    }],
                )]
                .into(),
                origin: RunOrigin::SeededFrom {
                    origin_kind: SeedOriginKind::Import,
                    source_ref: "fixture:maintenance".into(),
                    origin_device_pseudonym: None,
                    base_seq: None,
                },
                submission_id: None,
            },
        },
        FramedRecord {
            seq: 2,
            fence: 1,
            record: WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: None,
                reason: broca_protocol::RunFinishReason::Completed,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
        },
    ]
}

fn store_key(session: &BindIdentity) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        session.project_root.display(),
        session.harness,
        session.session
    )
}

fn run(root: &Path, operation: &str, dry_run: bool) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_store-maintenance"));
    command.arg(operation).arg(root);
    if dry_run {
        command.arg("--dry-run");
    }
    command.output().expect("run maintenance binary")
}

fn stdout(output: &Output) -> String {
    String::from_utf8(output.stdout.clone()).expect("maintenance output is UTF-8")
}

#[test]
fn page_usage_cli_reports_operation_counts_and_dry_run_rolls_back() {
    let root = state_root("page-usage");
    let database = root.join("run-index.db");
    let store = SqliteStore::open(&database).expect("open store");
    let session = session(&root, "page-usage");
    Persister::project(&store, &session, &import_records(&session)).expect("project fixture");
    drop(store);

    let conn = Connection::open(&database).expect("open fixture database");
    conn.execute(
        "UPDATE meta SET last_episode_usage_json=NULL WHERE session=?1",
        [store_key(&session)],
    )
    .expect("create legacy page-usage row");
    let before: Option<String> = conn
        .query_row(
            "SELECT last_episode_usage_json FROM meta WHERE session=?1",
            [store_key(&session)],
            |row| row.get(0),
        )
        .expect("read legacy page-usage value");
    drop(conn);

    let preview_store = SqliteStore::open(&database).expect("reopen store for preview");
    let expected = preview_store
        .preview_page_usage_backfill()
        .expect("preview page-usage backfill");
    drop(preview_store);
    let dry = run(&root, "page-usage", true);
    assert_eq!(dry.status.code(), Some(0));
    assert_eq!(
        stdout(&dry),
        format!(
            "scanned {}\nconverted {}\nempty {}\ndeclined {}\ntotal {}\n",
            expected.scanned,
            expected.converted,
            expected.empty,
            expected.declined,
            expected.converted + expected.empty + expected.declined
        )
    );
    let after_dry: Option<String> = Connection::open(&database)
        .expect("reopen after dry-run")
        .query_row(
            "SELECT last_episode_usage_json FROM meta WHERE session=?1",
            [store_key(&session)],
            |row| row.get(0),
        )
        .expect("read value after dry-run");
    assert_eq!(
        after_dry, before,
        "dry-run must leave the SQL value unchanged"
    );

    let applied = run(&root, "page-usage", false);
    assert_eq!(applied.status.code(), Some(0));
    assert_eq!(stdout(&applied), stdout(&dry));
}

#[test]
fn control_head_cli_reports_counts_preserves_dry_run_and_exits_one_on_decline() {
    let root = state_root("control-head");
    let database = root.join("run-index.db");
    let store = SqliteStore::open(&database).expect("open store");
    let repairable = session(&root, "repairable");
    let declined = session(&root, "declined");
    let repairable_records = import_records(&repairable);
    let declined_records = import_records(&declined);
    Persister::project(&store, &repairable, &repairable_records).expect("project repairable row");
    Persister::project(&store, &declined, &declined_records).expect("project declined row");
    drop(store);

    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&repairable).expect("open repairable WAL");
    for record in &repairable_records {
        wal.append(record.fence, &record.record, false)
            .expect("append fixture record");
    }
    wal.sync().expect("sync repairable WAL");
    drop(wal);

    let conn = Connection::open(&database).expect("open fixture database");
    conn.execute(
        "UPDATE meta SET control_head_wal_seq=NULL, control_head_sub_index=NULL
          WHERE session IN (?1, ?2)",
        rusqlite::params![store_key(&repairable), store_key(&declined)],
    )
    .expect("create legacy control-head rows");
    let before: Vec<(String, Option<i64>, Option<i64>)> = {
        let mut statement = conn
            .prepare(
                "SELECT session, control_head_wal_seq, control_head_sub_index
                   FROM meta ORDER BY session",
            )
            .expect("prepare metadata read");
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("query metadata")
            .collect::<Result<_, _>>()
            .expect("collect metadata")
    };
    drop(conn);

    let preview_store = SqliteStore::open(&database).expect("reopen store for preview");
    let expected = preview_store
        .preview_control_head_repair(&wal_store)
        .expect("preview control-head repair");
    drop(preview_store);
    let dry = run(&root, "control-head", true);
    assert_eq!(
        dry.status.code(),
        Some(1),
        "a declined row is a partial repair"
    );
    // The whole stdout, byte for byte, rather than a set of `contains` checks: a report an
    // operator reads during an incident should not gain or lose a line without a test saying
    // so. This caught the `declined_no_wal` line being added.
    //
    // `declined_no_wal` is asserted through the counts rather than hardcoded, so the case
    // this fixture builds -- a declined row whose WAL genuinely is absent -- stays visible
    // here if the fixture changes shape.
    assert_eq!(
        stdout(&dry),
        format!(
            "scanned {}\nrepaired {}\ndeclined {}\ndeclined_no_wal {}\ntotal {}\n",
            expected.scanned,
            expected.repaired,
            expected.declined,
            expected.declined_no_wal,
            expected.repaired + expected.declined
        )
    );
    let after_dry: Vec<(String, Option<i64>, Option<i64>)> = {
        let conn = Connection::open(&database).expect("reopen after dry-run");
        let mut statement = conn
            .prepare(
                "SELECT session, control_head_wal_seq, control_head_sub_index
                   FROM meta ORDER BY session",
            )
            .expect("prepare post-dry-run read");
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)))
            .expect("query post-dry-run metadata")
            .collect::<Result<_, _>>()
            .expect("collect post-dry-run metadata")
    };
    assert_eq!(
        after_dry, before,
        "dry-run must leave both cursor columns unchanged"
    );

    let applied = run(&root, "control-head", false);
    assert_eq!(applied.status.code(), Some(1));
    assert_eq!(stdout(&applied), stdout(&dry));
}

/// Maintenance never repairs, folds or rewrites a WAL a newer build appended to. Its bytes are
/// intact, only unreadable here: control-head repair declines the row (and does not call the
/// WAL missing), the fold skips it under its own reason, the manifest rebuild refuses by name,
/// and the WAL is byte-identical afterwards.
#[test]
fn maintenance_treats_a_newer_writer_wal_as_not_repairable() {
    let root = state_root("newer-writer");
    let database = root.join("run-index.db");
    let identity = session(&root, "newer-writer");
    let mut records = import_records(&identity);
    if let WalRecord::RunStarted { ts_ms, .. } = &mut records[0].record {
        *ts_ms = Some(1);
    }
    let store = SqliteStore::open(&database).expect("open store");
    Persister::project(&store, &identity, &records).expect("project fixture");
    drop(store);
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&identity).expect("open WAL");
    for record in &records {
        wal.append(1, &record.record, true)
            .expect("append fixture record");
    }
    drop(wal);
    let path = wal_store.wal_path(&identity);
    let mut bytes = std::fs::read(&path).unwrap();
    bytes.extend(
        broca_wal::framing::encode_with_requires(
            records.len() as u64 + 1,
            1,
            &["from-the-future/v1"],
            &records[1].record,
        )
        .unwrap(),
    );
    std::fs::write(&path, &bytes).unwrap();
    Connection::open(&database)
        .expect("open fixture database")
        .execute(
            "UPDATE meta SET control_head_wal_seq=NULL, control_head_sub_index=NULL
              WHERE session=?1",
            [store_key(&identity)],
        )
        .expect("create a legacy control-head row");

    let repair = run(&root, "control-head", false);
    assert_eq!(repair.status.code(), Some(1), "a declined row");
    assert_eq!(
        stdout(&repair),
        "scanned 1\nrepaired 0\ndeclined 1\ndeclined_no_wal 0\ntotal 1\n"
    );

    let fold = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        fold.status.success(),
        "{}",
        String::from_utf8_lossy(&fold.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&fold.stdout).unwrap();
    assert_eq!(report["removed"], 0);
    assert_eq!(
        report["skipped"]["newer_writer"].as_array().unwrap().len(),
        1
    );
    assert!(report["skipped"].get("undecodable").is_none(), "{report}");

    let manifest = run(&root, "wal-manifest", false);
    let stderr = String::from_utf8_lossy(&manifest.stderr);
    assert_eq!(manifest.status.code(), Some(2), "{stderr}");
    assert!(stderr.contains("NewerWriter"), "{stderr}");

    assert_eq!(std::fs::read(&path).unwrap(), bytes);
}

/// `reconcile-active` declines a row whose WAL a newer build appended to, and counts it as
/// declined rather than as a run with no terminal.
///
/// The WAL cannot be read here, so this build knows nothing about how that run ended; calling it
/// "unsealed" would claim the WAL agrees that nothing ended it. The newer-writer frame sits
/// AFTER the run's terminal, so a reader that stopped at the frame and used the records before
/// it would find the terminal and reconcile the row; the row must stay `active` instead. A
/// second session with the same records and no gated frame is reconciled, which shows the
/// fixture is one the command would otherwise act on.
#[test]
fn reconcile_active_declines_a_newer_writer_wal_and_counts_it_declined() {
    use broca_core::{RunIndex, RunIndexRow, RunIndexState};

    let root = state_root("reconcile-newer-writer");
    let database = root.join("run-index.db");
    let store = SqliteStore::open(&database).expect("open store");
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut gated_bytes = Vec::new();
    for label in ["stale", "newer-writer"] {
        let identity = session(&root, label);
        let mut records = import_records(&identity);
        let run_id = format!("run-{label}");
        if let WalRecord::RunStarted { run_id: id, .. } = &mut records[0].record {
            *id = RunId(run_id.clone());
        }
        store
            .index_run_active(&RunIndexRow {
                withdrawals_owed: false,
                title: None,
                run_id,
                session: identity.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Active,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
            })
            .expect("index the run as active");
        let mut wal = wal_store.open(&identity).expect("open WAL");
        for record in &records {
            wal.append(record.fence, &record.record, true)
                .expect("append fixture record");
        }
        drop(wal);
        if label == "newer-writer" {
            let path = wal_store.wal_path(&identity);
            let mut bytes = std::fs::read(&path).unwrap();
            bytes.extend(
                broca_wal::framing::encode_with_requires(
                    records.len() as u64 + 1,
                    1,
                    &["from-the-future/v1"],
                    &WalRecord::StepStarted {
                        step_id: broca_protocol::StepId(9),
                    },
                )
                .unwrap(),
            );
            std::fs::write(&path, &bytes).unwrap();
            gated_bytes = bytes;
        }
    }
    drop(store);

    let output = run(&root, "reconcile-active", false);
    assert_eq!(
        output.status.code(),
        Some(1),
        "a declined row is a partial reconcile: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        stdout(&output),
        "scanned 2\nreconciled 1\nunsealed 0\ndeclined 1\ntotal 2\n"
    );
    let states: Vec<(String, String)> = {
        let conn = Connection::open(&database).expect("reopen store");
        let mut statement = conn
            .prepare("SELECT run_id, state FROM run_index ORDER BY run_id")
            .expect("prepare run-index read");
        statement
            .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
            .expect("query run index")
            .collect::<Result<_, _>>()
            .expect("collect run index")
    };
    assert_eq!(
        states,
        [
            ("run-newer-writer".to_string(), "active".to_string()),
            ("run-stale".to_string(), "completed".to_string()),
        ]
    );
    assert_eq!(
        std::fs::read(wal_store.wal_path(&session(&root, "newer-writer"))).unwrap(),
        gated_bytes,
        "reconcile-active never writes the WAL"
    );
    std::fs::remove_dir_all(root).unwrap();
}

/// Every `store-maintenance` path that reads WALs leaves a readable gated frame byte for byte.
///
/// reconcile-active and control-head only read the WAL. wal-fold, the extraction on the next
/// open, and `--unfold` move its bytes; none may re-encode the records it decoded, which would
/// write the gated frame back as a version-1 frame without its `requires` list and still decode
/// to the same records. The run's terminal is the gated frame, so reconcile-active can only
/// reconcile the row by reading through it.
#[test]
fn maintenance_keeps_a_readable_gated_frame_byte_for_byte() {
    use broca_core::{RunIndex, RunIndexRow, RunIndexState};

    let root = state_root("gated-known");
    let database = root.join("run-index.db");
    let identity = session(&root, "gated-known");
    let mut records = import_records(&identity);
    if let WalRecord::RunStarted { ts_ms, .. } = &mut records[0].record {
        *ts_ms = Some(1);
    }
    let store = SqliteStore::open(&database).expect("open store");
    Persister::project(&store, &identity, &records).expect("project fixture");
    store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "maintenance-run".into(),
            session: identity.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index the run as active");
    drop(store);
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&identity).expect("open WAL");
    wal.append(1, &records[0].record, true)
        .expect("append run_started");
    drop(wal);
    let path = wal_store.wal_path(&identity);
    let mut bytes = std::fs::read(&path).unwrap();
    let gated_at = bytes.len();
    let gated = broca_wal::framing::encode_with_requires(
        2,
        1,
        &[broca_wal::framing::TEST_KNOWN_FEATURE],
        &records[1].record,
    )
    .unwrap();
    bytes.extend(&gated);
    std::fs::write(&path, &bytes).unwrap();
    let carries_gate = |b: &[u8]| b.get(gated_at..gated_at + gated.len()) == Some(&gated[..]);
    Connection::open(&database)
        .expect("open fixture database")
        .execute(
            "UPDATE meta SET control_head_wal_seq=NULL, control_head_sub_index=NULL
              WHERE session=?1",
            [store_key(&identity)],
        )
        .expect("create a legacy control-head row");

    let reconcile = run(&root, "reconcile-active", false);
    assert_eq!(
        stdout(&reconcile),
        "scanned 1\nreconciled 1\nunsealed 0\ndeclined 0\ntotal 1\n",
        "{}",
        String::from_utf8_lossy(&reconcile.stderr)
    );
    let repair = run(&root, "control-head", false);
    assert_eq!(
        stdout(&repair),
        "scanned 1\nrepaired 1\ndeclined 0\ndeclined_no_wal 0\ntotal 1\n",
        "{}",
        String::from_utf8_lossy(&repair.stderr)
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "readers wrote the WAL"
    );

    let fold = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        fold.status.success(),
        "{}",
        String::from_utf8_lossy(&fold.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&fold.stdout).unwrap();
    assert_eq!(report["removed"], 1, "{report}");
    let journal = report["journal"].as_str().unwrap().to_owned();
    let mut archive = broca_wal::archive::ArchiveReader::open(
        root.join("wal-archive")
            .join(&journal)
            .with_extension("ark"),
        broca_wal::DEFAULT_MAX_READ_BYTES,
    )
    .unwrap();
    let member = archive
        .read_member(&broca_core::session_addr(&identity))
        .unwrap()
        .unwrap()
        .payload;
    drop(archive);
    assert!(
        carries_gate(&member),
        "wal-fold archived a WAL without its gate"
    );
    assert_eq!(member, bytes);

    // Opening a folded session extracts its archived bytes back to the live path.
    drop(wal_store.open(&identity).expect("extract on open"));
    let extracted = std::fs::read(&path).unwrap();
    assert!(carries_gate(&extracted), "extraction lost the gate");
    assert_eq!(extracted, bytes);

    let unfold = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--resume")
        .arg(&journal)
        .arg("--unfold")
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        unfold.status.success(),
        "{}",
        String::from_utf8_lossy(&unfold.stderr)
    );
    let unfolded = std::fs::read(&path).unwrap();
    assert!(carries_gate(&unfolded), "unfold lost the gate");
    assert_eq!(unfolded, bytes);
    std::fs::remove_dir_all(root).unwrap();
}

/// Every `store-maintenance` path that reads WALs reads a WAL whose `RunStarted` froze a plan
/// manifest (a gated frame requiring `plan-manifest/v1`, which this build knows) as healthy, and
/// leaves it byte for byte: reconcile-active reconciles the run, control-head repairs its row,
/// the WAL-manifest rebuild succeeds, and wal-fold, extraction on the next open, and `--unfold`
/// all give back the same bytes, from which the manifest decodes intact.
#[test]
fn maintenance_reads_a_plan_manifest_wal_and_keeps_it_byte_for_byte() {
    use broca_core::{RunIndex, RunIndexRow, RunIndexState};

    let root = state_root("plan-manifest");
    let database = root.join("run-index.db");
    let identity = session(&root, "plan-manifest");
    let fixture: WalRecord = serde_json::from_str(
        &include_str!("../../../broca-wal/tests/fixtures/plan_manifest_run_started.json")
            .trim_end()
            .replace("{RUN_ID}", "fixture"),
    )
    .unwrap();
    let WalRecord::RunStarted {
        manifest: frozen, ..
    } = fixture
    else {
        unreachable!()
    };
    let frozen = frozen.expect("the fixture carries a manifest");
    let mut records = import_records(&identity);
    if let WalRecord::RunStarted {
        ts_ms, manifest, ..
    } = &mut records[0].record
    {
        *ts_ms = Some(1);
        *manifest = Some(frozen.clone());
    }
    let store = SqliteStore::open(&database).expect("open store");
    Persister::project(&store, &identity, &records).expect("project fixture");
    store
        .index_run_active(&RunIndexRow {
            withdrawals_owed: false,
            title: None,
            run_id: "maintenance-run".into(),
            session: identity.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
        })
        .expect("index the run as active");
    drop(store);
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&identity).expect("open WAL");
    for record in &records {
        wal.append(1, &record.record, true)
            .expect("append fixture record");
    }
    drop(wal);
    let path = wal_store.wal_path(&identity);
    let bytes = std::fs::read(&path).unwrap();
    let gate = br#"{"requires":["plan-manifest/v1"],"record":{"type":"run_started""#;
    assert!(bytes.windows(gate.len()).any(|window| window == gate));
    Connection::open(&database)
        .expect("open fixture database")
        .execute(
            "UPDATE meta SET control_head_wal_seq=NULL, control_head_sub_index=NULL
              WHERE session=?1",
            [store_key(&identity)],
        )
        .expect("create a legacy control-head row");

    let reconcile = run(&root, "reconcile-active", false);
    assert_eq!(
        stdout(&reconcile),
        "scanned 1\nreconciled 1\nunsealed 0\ndeclined 0\ntotal 1\n",
        "{}",
        String::from_utf8_lossy(&reconcile.stderr)
    );
    let repair = run(&root, "control-head", false);
    assert_eq!(
        stdout(&repair),
        "scanned 1\nrepaired 1\ndeclined 0\ndeclined_no_wal 0\ntotal 1\n",
        "{}",
        String::from_utf8_lossy(&repair.stderr)
    );
    let manifest_rebuild = run(&root, "wal-manifest", false);
    assert!(
        manifest_rebuild.status.success(),
        "{}",
        String::from_utf8_lossy(&manifest_rebuild.stderr)
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "readers wrote the WAL"
    );

    let fold = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        fold.status.success(),
        "{}",
        String::from_utf8_lossy(&fold.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&fold.stdout).unwrap();
    assert_eq!(report["removed"], 1, "{report}");
    let journal = report["journal"].as_str().unwrap().to_owned();
    let mut archive = broca_wal::archive::ArchiveReader::open(
        root.join("wal-archive")
            .join(&journal)
            .with_extension("ark"),
        broca_wal::DEFAULT_MAX_READ_BYTES,
    )
    .unwrap();
    let member = archive
        .read_member(&broca_core::session_addr(&identity))
        .unwrap()
        .unwrap()
        .payload;
    drop(archive);
    assert_eq!(member, bytes, "wal-fold changed the WAL");

    drop(wal_store.open(&identity).expect("extract on open"));
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "extraction changed the WAL"
    );

    let unfold = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--resume")
        .arg(&journal)
        .arg("--unfold")
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        unfold.status.success(),
        "{}",
        String::from_utf8_lossy(&unfold.stderr)
    );
    assert_eq!(
        std::fs::read(&path).unwrap(),
        bytes,
        "unfold changed the WAL"
    );
    let read = wal_store.read_records(&identity).unwrap();
    match &read[0].record {
        WalRecord::RunStarted {
            manifest: Some(manifest),
            ..
        } => assert_eq!(manifest, &frozen),
        other => panic!("expected RunStarted with its manifest, got {other:?}"),
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn wal_fold_cli_defaults_to_report_and_archived_control_head_remains_repairable() {
    let root = state_root("fold");
    let identity = session(&root, "cold");
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let mut records = import_records(&identity);
    if let WalRecord::RunStarted { ts_ms, .. } = &mut records[0].record {
        *ts_ms = Some(1);
    }
    Persister::project(&store, &identity, &records).unwrap();
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&identity).unwrap();
    for record in &records {
        wal.append(1, &record.record, true).unwrap();
    }
    drop(wal);
    let path = wal_store.wal_path(&identity);
    let bytes = std::fs::read(&path).unwrap();
    let modified = std::fs::metadata(&path).unwrap().modified().unwrap();
    let output = run(&root, "wal-fold", false);
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().modified().unwrap(),
        modified
    );
    assert!(!root.join("wal-archive").exists());
    let output = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["removed"], 1);
    assert!(!path.exists());
    assert_eq!(wal_store.read_records(&identity).unwrap(), records);
    // SLICE 4 CHANGED THIS CONTRACT DELIBERATELY, and this assertion is how the change is
    // visible from the CLI's side. Until extraction existed, opening a folded session
    // REFUSED loudly, so slices 3 and 4 could land separately without a window where a fold
    // plus one send destroyed a transcript. Now open EXTRACTS: the archived bytes return to
    // the canonical path BEFORE FileWal's create_new could mint a fresh lineage over them.
    let wal = wal_store.open(&identity).unwrap();
    assert_eq!(wal.replay().unwrap(), records);
    // Dropped before the tree is read and removed: the handle is live until it is not, and
    // Windows refuses both a directory removal and some opens while it is held.
    drop(wal);
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    let counts = store.preview_control_head_repair(&wal_store).unwrap();
    assert_eq!(counts.declined_no_wal, 0);
    let output = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--resume")
        .arg(report["journal"].as_str().unwrap())
        .arg("--unfold")
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(std::fs::read(&path).unwrap(), bytes);
    // WINDOWS CANNOT REMOVE A DIRECTORY CONTAINING AN OPEN FILE, and this store still holds
    // run-index.db inside `root`. On unix the unlink would succeed and the drop would look
    // like noise, which is exactly why it needs saying: the two sibling tests in this file
    // drop their store and do not remove their tree at all, so this test was the only one
    // doing both halves and it did them in the wrong order.
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn wal_fold_uses_rekeyed_meta_identity_and_preserves_embedded_records() {
    let root = state_root("fold-rekeyed");
    let embedded = BindIdentity::new(root.join("project"), "runner", "rekeyed");
    let resolved = BindIdentity::new(root.join("project"), "broca", "rekeyed");
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let mut records = import_records(&embedded);
    if let WalRecord::RunStarted { ts_ms, .. } = &mut records[0].record {
        *ts_ms = Some(1);
    }
    Persister::project(&store, &embedded, &records).unwrap();
    drop(store);
    let conn = Connection::open(root.join("run-index.db")).unwrap();
    assert_eq!(
        conn.execute(
            "UPDATE meta SET session=?1 WHERE session=?2",
            [store_key(&resolved), store_key(&embedded)]
        )
        .unwrap(),
        1
    );
    drop(conn);
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&embedded).unwrap();
    for record in &records {
        wal.append(1, &record.record, true).unwrap();
    }
    drop(wal);
    let bytes = std::fs::read(wal_store.wal_path(&embedded)).unwrap();
    std::fs::rename(wal_store.wal_path(&embedded), wal_store.wal_path(&resolved)).unwrap();
    let preview = run(&root, "wal-fold", false);
    assert!(
        preview.status.success(),
        "{}",
        String::from_utf8_lossy(&preview.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&preview.stdout).unwrap();
    assert_eq!(
        report["resolved_from_store"],
        serde_json::json!([broca_core::session_addr(&resolved)])
    );
    assert!(!root.join("wal-archive").exists());
    let output = Command::new(env!("CARGO_BIN_EXE_store-maintenance"))
        .arg("wal-fold")
        .arg(&root)
        .arg("--write")
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(report["removed"], 1);
    assert!(!wal_store.wal_path(&resolved).exists());
    let mut archive = broca_wal::archive::ArchiveReader::open(
        root.join("wal-archive")
            .join(report["journal"].as_str().unwrap())
            .with_extension("ark"),
        broca_wal::DEFAULT_MAX_READ_BYTES,
    )
    .unwrap();
    assert_eq!(
        archive
            .read_member(&broca_core::session_addr(&resolved))
            .unwrap()
            .unwrap()
            .payload,
        bytes
    );
    drop(archive);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn wal_manifest_counts_only_episodes_with_project_ids() {
    let root = state_root("project-observations");
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let session = session(&root, "observations");
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut wal = wal_store.open(&session).unwrap();
    let mut records = Vec::new();
    for (index, id) in [Some("pj-a"), Some("pj-a"), None].into_iter().enumerate() {
        let mut episode = import_records(&session);
        if let WalRecord::RunStarted {
            run_id, project_id, ..
        } = &mut episode[0].record
        {
            *run_id = RunId(format!("episode-{index}"));
            *project_id = id.map(str::to_owned);
        }
        for mut frame in episode {
            frame.seq = records.len() as u64 + 1;
            wal.append(1, &frame.record, true).unwrap();
            records.push(frame);
        }
    }
    Persister::project(&store, &session, &records).unwrap();
    let rebuild = store.rebuild_wal_manifest(&wal_store).unwrap();
    let address = broca_core::session_addr(&session);
    let observed = &rebuild.project_observations[&address];
    assert_eq!(observed.episodes_with_project_id, 2);
    assert_eq!(
        observed
            .project_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["pj-a"]
    );
    let before = std::fs::read(wal_store.manifest_path()).unwrap();
    let report = run(&root, "wal-manifest", false);
    assert_eq!(report.status.code(), Some(0), "{:?}", report);
    assert!(stdout(&report).contains(&format!("project_observations {address} {{\"episodes_with_project_id\":2,\"project_ids\":[\"pj-a\"]}}")));
    assert_eq!(
        std::fs::read(wal_store.manifest_path()).unwrap(),
        before,
        "report must not replace the sidecar"
    );
    drop(wal);
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn wal_manifest_mapping_bytes_ignore_project_observations() {
    let root = state_root("manifest-observation-bytes");
    let store = SqliteStore::open(root.join("run-index.db")).unwrap();
    let session = session(&root, "session\twith-tab");
    let wal_store = FileWalStore::new(root.join("wal"));
    let mut records = import_records(&session);
    let WalRecord::RunStarted { project_id, .. } = &mut records[0].record else {
        panic!("fixture must begin with RunStarted");
    };
    *project_id = Some("pj-a".into());
    let mut wal = wal_store.open(&session).unwrap();
    for frame in &records {
        wal.append(frame.fence, &frame.record, true).unwrap();
    }
    drop(wal);
    Persister::project(&store, &session, &records).unwrap();
    let observed = store.rebuild_wal_manifest(&wal_store).unwrap();
    let address = broca_core::session_addr(&session);
    assert_eq!(
        observed.project_observations[&address].episodes_with_project_id,
        1
    );

    // Preserve the actual lineage header while removing only the observations from the
    // same WAL. Recreating the file through open() would mint a different lineage id.
    let path = wal_store.wal_path(&session);
    let original = std::fs::read(&path).unwrap();
    let broca_wal::framing::DecodeStep::Lineage { consumed, .. } =
        broca_wal::framing::decode_next(&original)
    else {
        panic!("expected the original lineage header");
    };
    let mut without_observations = original[..consumed].to_vec();
    for frame in &mut records {
        if let WalRecord::RunStarted { project_id, .. } = &mut frame.record {
            *project_id = None;
        }
        without_observations
            .extend(broca_wal::framing::encode(frame.seq, frame.fence, &frame.record).unwrap());
    }
    assert_ne!(original, without_observations);
    std::fs::write(&path, without_observations).unwrap();
    let absent = store.rebuild_wal_manifest(&wal_store).unwrap();
    assert_eq!(
        absent.project_observations[&address].episodes_with_project_id,
        0
    );
    assert!(absent.project_observations[&address].project_ids.is_empty());
    assert_eq!(observed.addresses, absent.addresses);
    assert_eq!(observed.contents.as_bytes(), absent.contents.as_bytes());
    drop(store);
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn media_sweep_reclaims_uploads_only_in_write_mode_and_fails_closed() {
    let root = state_root("upload-sweep");
    broca_core::media_store::create_media_directories(&root).unwrap();
    let uploads = broca_core::media_uploads::Uploads::at(&root);
    let owner = session(&root, "uploads");
    let request = serde_json::from_value(serde_json::json!({"mime":"application/pdf","bytes":0,"sha256":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"})).unwrap();
    let abandoned = uploads.begin(&owner, &request, 0).unwrap().upload_id;
    let expired = uploads.begin(&owner, &request, 0).unwrap().upload_id;
    uploads.commit(&owner, &expired, 0).unwrap();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let fresh = uploads.begin(&owner, &request, now).unwrap().upload_id;
    uploads.commit(&owner, &fresh, now).unwrap();
    let invoke = |write: bool| {
        let mut command = Command::new(env!("CARGO_BIN_EXE_store-maintenance"));
        command.arg("media-sweep").arg(&root);
        if write {
            command.arg("--write");
        }
        command.output().unwrap()
    };
    let preview = invoke(false);
    assert!(preview.status.success(), "{:?}", preview);
    assert!(stdout(&preview).contains("uploads_removed 2"));
    for id in [&abandoned, &expired, &fresh] {
        assert!(root.join("media-uploads/objects").join(id).exists());
    }
    let applied = invoke(true);
    assert!(applied.status.success(), "{:?}", applied);
    assert!(stdout(&applied).contains("uploads_removed 2"));
    for id in [&abandoned, &expired] {
        assert!(!root.join("media-uploads/objects").join(id).exists());
    }
    assert!(root.join("media-uploads/objects").join(&fresh).exists());
    let retained = uploads.begin(&owner, &request, 0).unwrap().upload_id;
    std::fs::write(root.join("media-uploads/refs/broken"), b"bad metadata").unwrap();
    let refused = invoke(true);
    assert!(!refused.status.success());
    assert!(String::from_utf8_lossy(&refused.stderr).contains("upload sweep failed"));
    for id in [&retained, &fresh] {
        assert!(root.join("media-uploads/objects").join(id).exists());
    }
}
