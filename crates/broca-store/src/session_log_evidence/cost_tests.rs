use super::*;
use broca_core::{FileWalStore, WalStore};
use broca_protocol::{BindIdentity, ContentBlock, Message, Role};
use std::{cell::RefCell, time::Instant};

thread_local! {
    static SQL: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
}

fn trace_sql(sql: &str) {
    SQL.with(|queries| queries.borrow_mut().push(sql.to_owned()));
}

fn before() -> bool {
    std::env::var("BROCA_EVIDENCE_COST_PHASE").as_deref() == Ok("before")
}

fn output_directory() -> std::path::PathBuf {
    std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .join(".cortexkit/internal-docs/investigations/stale-root-audit-perf")
}

struct CostFixture {
    root: std::path::PathBuf,
    store: SqliteStore,
    session: BindIdentity,
    lineage: String,
    index_build_ns: u128,
}

impl CostFixture {
    fn new(messages: usize, target_runs: usize, other_runs: usize) -> Self {
        let root = broca_test_support::scratch_root().join(format!(
            "evidence-cost-{}-{messages}-{target_runs}-{other_runs}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).unwrap();
        let store = SqliteStore::open(root.join("run-index.db")).unwrap();
        {
            let conn = store.conn.lock().unwrap();
            for index in indexes::plan(&conn)
                .unwrap()
                .into_iter()
                .filter(|index| index.applicable && index.exists)
            {
                conn.execute_batch(&format!("DROP INDEX {}", index.name))
                    .unwrap();
            }
        }
        let session = BindIdentity::new("/old/project", "broca", "target");
        let wal_store = FileWalStore::new(root.join("wal"));
        let wal = wal_store.open(&session).unwrap();
        let lineage = wal.lineage_id().unwrap().to_string();
        drop(wal);
        let key = SqliteStore::key(&session);
        let target_json = serde_json::to_string(&session).unwrap();
        let message = serde_json::to_string(&Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "x".repeat(512),
            }],
        ))
        .unwrap();
        {
            let mut conn = store.conn.lock().unwrap();
            let tx = conn.transaction().unwrap();
            tx.execute("INSERT INTO agent_data_writer VALUES(1)", [])
                .unwrap();
            {
                let mut insert = tx.prepare("INSERT INTO run_index(run_id,session,state,lineage_id) VALUES(?1,?2,'completed',?3)").unwrap();
                for n in 0..other_runs {
                    let other =
                        BindIdentity::new("/other/project", "broca", format!("other-{n:06}"));
                    insert
                        .execute(params![
                            format!("other-{n:06}"),
                            serde_json::to_string(&other).unwrap(),
                            "other-lineage"
                        ])
                        .unwrap();
                }
                for n in 0..target_runs {
                    insert
                        .execute(params![format!("target-{n:06}"), target_json, lineage])
                        .unwrap();
                }
            }
            {
                let mut insert = tx
                    .prepare("INSERT INTO message(session,ord,json) VALUES(?1,?2,?3)")
                    .unwrap();
                for n in 0..messages {
                    insert.execute(params![key, n as i64, message]).unwrap();
                }
            }
            tx.execute("INSERT INTO meta(session,watermark_seq,lineage_id,context_epoch,prefix_len,last_run_id) VALUES(?1,?2,?3,0,?2,'target-000000')", params![key, messages as i64, lineage]).unwrap();
            tx.execute("DELETE FROM agent_data_writer", []).unwrap();
            tx.commit().unwrap();
            conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE)")
                .unwrap();
        }
        let index_build_ns = if before() {
            0
        } else {
            let start = Instant::now();
            store.prepare_session_log_indexes().unwrap();
            start.elapsed().as_nanos()
        };
        Self {
            root,
            store,
            session,
            lineage,
            index_build_ns,
        }
    }

    fn plans(&self) -> Vec<serde_json::Value> {
        self.plans_for(false)
    }

    fn plans_for(&self, removed: bool) -> Vec<serde_json::Value> {
        let mut conn = self.store.conn.lock().unwrap();
        SQL.with(|queries| queries.borrow_mut().clear());
        conn.trace(Some(trace_sql));
        let result = evidence(&conn, &self.session).unwrap();
        conn.trace(None);
        assert!(result.content);
        assert_eq!(result.removed, removed);
        assert_eq!(
            result.lineages,
            std::collections::BTreeSet::from([self.lineage.clone()])
        );
        let queries = SQL.with(|queries| std::mem::take(&mut *queries.borrow_mut()));
        queries
            .into_iter()
            .map(|sql| {
                let executable = sql.trim_start().strip_prefix("-- ").unwrap_or(&sql);
                let mut statement = conn
                    .prepare(&format!("EXPLAIN QUERY PLAN {executable}"))
                    .unwrap_or_else(|error| panic!("cannot explain {sql:?}: {error}"));
                let details: Vec<String> = statement
                    .query_map([], |row| row.get(3))
                    .unwrap()
                    .collect::<rusqlite::Result<_>>()
                    .unwrap();
                let mut replay = conn.prepare(executable).unwrap();
                let returned = {
                    let mut rows = replay.query([]).unwrap();
                    let mut count = 0;
                    while rows.next().unwrap().is_some() {
                        count += 1;
                    }
                    count
                };
                serde_json::json!({"sql": sql, "plan": details, "rows": returned,
                    "vm_steps": replay.get_status(rusqlite::StatementStatus::VmStep)})
            })
            .collect()
    }
}

#[test]
fn evidence_queries_seek_session_identity_instead_of_scanning_runs() {
    let fixture = CostFixture::new(32, 32, 64);
    let plans = fixture.plans();
    let scans: Vec<_> = plans
        .iter()
        .filter(|query| {
            query["plan"]
                .as_array()
                .unwrap()
                .iter()
                .any(|line| line.as_str().unwrap().contains("SCAN run_index"))
        })
        .collect();
    assert!(scans.is_empty(), "unbounded run-index queries: {scans:#?}");
    for query in &plans {
        if query["sql"]
            .as_str()
            .unwrap()
            .starts_with("SELECT lineage_id FROM")
        {
            assert!(
                query["rows"].as_u64().unwrap() <= 1,
                "lineage query must seek one value: {query:#?}"
            );
        }
    }
}

/// The large fixture and wall-time observation run manually, not as a CI latency threshold.
#[test]
#[ignore = "manual evidence query-plan and real-sized fixture measurement"]
fn evidence_cost_real_sized_fixture() {
    let fixture = CostFixture::new(100_000, 100_000, 200_000);
    let plans = fixture.plans();
    let mut elapsed = Vec::new();
    for _ in 0..3 {
        let conn = fixture.store.conn.lock().unwrap();
        let start = Instant::now();
        let result = evidence(&conn, &fixture.session).unwrap();
        elapsed.push(start.elapsed().as_nanos());
        assert!(result.content && !result.removed);
        assert_eq!(
            result.lineages,
            std::collections::BTreeSet::from([fixture.lineage.clone()])
        );
    }
    elapsed.sort_unstable();
    let expired = if before() {
        serde_json::Value::Null
    } else {
        fixture
            .store
            .retention_deletion_pending(&fixture.lineage, &fixture.session, true)
            .unwrap();
        {
            let conn = fixture.store.conn.lock().unwrap();
            conn.execute(
                "UPDATE run_index SET expired_at_ms=1 WHERE session=?1",
                [serde_json::to_string(&fixture.session).unwrap()],
            )
            .unwrap();
        }
        let queries = fixture.plans_for(true);
        let mut times = Vec::new();
        for _ in 0..3 {
            let conn = fixture.store.conn.lock().unwrap();
            let start = Instant::now();
            let result = evidence(&conn, &fixture.session).unwrap();
            times.push(start.elapsed().as_nanos());
            assert!(result.content && result.removed);
        }
        times.sort_unstable();
        serde_json::json!({"evidence_ns": times, "median_ns": times[1], "queries": queries})
    };
    let report = serde_json::json!({
        "phase": if before() { "before" } else { "after" }, "messages": 100_000, "target_runs": 100_000, "other_runs": 200_000,
        "index_build_ns": fixture.index_build_ns,
        "evidence_ns": elapsed, "median_ns": elapsed[1], "queries": plans,
        "database_bytes": std::fs::metadata(fixture.root.join("run-index.db")).unwrap().len(),
        "sqlite_version": rusqlite::version(),
        "expired": expired,
    });
    let directory = output_directory();
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join(if before() {
            "evidence-before.json"
        } else {
            "evidence-after.json"
        }),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!(
        "EVIDENCE_COST phase={} median_ns={} database_bytes={} queries={} index_build_ns={}",
        report["phase"],
        report["median_ns"],
        report["database_bytes"],
        report["queries"].as_array().unwrap().len(),
        fixture.index_build_ns
    );
}

/// Compare two ref populations so the fixed root-discovery work is not multiplied
/// into the per-ref extrapolation. Logs and references are real, not mocked.
#[test]
#[ignore = "manual audit population scaling measurement"]
fn audit_cost_ordinary_refs() {
    let fixture = CostFixture::new(100_000, 100_000, 200_000);
    let wal_store = FileWalStore::new(fixture.root.join("wal"));
    std::fs::remove_file(wal_store.wal_path(&fixture.session)).unwrap();
    {
        let conn = fixture.store.conn.lock().unwrap();
        crate::session_refs::insert(
            &conn,
            &broca_core::session_addr(&fixture.session),
            &fixture.session,
        )
        .unwrap();
    }
    let mut observations = Vec::new();
    let mut previous = 0;
    for population in [1000, 10_000] {
        let mut identities = Vec::new();
        for n in previous..population {
            let session =
                BindIdentity::new("/ordinary/project", "broca", format!("ordinary-{n:06}"));
            let mut wal = wal_store.open(&session).unwrap();
            let start = crate::conformance::run_started(
                0,
                &format!("ordinary-{n:06}"),
                &session,
                "ordinary",
                broca_protocol::ModelHandle::new("stub", "model"),
            );
            wal.append(1, &start.record, false).unwrap();
            drop(wal);
            identities.push(session);
        }
        {
            let mut conn = fixture.store.conn.lock().unwrap();
            let tx = conn.transaction().unwrap();
            for session in identities {
                crate::session_refs::insert(&tx, &broca_core::session_addr(&session), &session)
                    .unwrap();
            }
            tx.commit().unwrap();
        }
        let start = Instant::now();
        let findings = fixture.store.audit_session_refs(|_| Ok(())).unwrap();
        let elapsed = start.elapsed().as_nanos();
        assert_eq!(findings, 1);
        observations.push(serde_json::json!({"ordinary_refs": population, "slow_missing_refs": 1, "audit_ns": elapsed}));
        previous = population;
    }
    let report = serde_json::json!({"runs": 300_000, "messages": 100_000, "observations": observations, "sqlite_version": rusqlite::version()});
    let directory = output_directory();
    std::fs::create_dir_all(&directory).unwrap();
    std::fs::write(
        directory.join("audit-scaling.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("AUDIT_SCALING {report}");
}

#[test]
fn indexed_lineages_preserve_duplicates_case_empty_values_and_json_identity() {
    let fixture = CostFixture::new(0, 8, 0);
    let conn = fixture.store.conn.lock().unwrap();
    for (run, value) in [
        ("target-000001", ""),
        ("target-000002", "B"),
        ("target-000003", "b"),
    ] {
        conn.execute(
            "UPDATE run_index SET lineage_id=?1 WHERE run_id=?2",
            params![value, run],
        )
        .unwrap();
    }
    conn.execute("UPDATE run_index SET session=?1 WHERE run_id='target-000002'", [" { \"session\":\"target\", \"harness\":\"broca\", \"project_root\":\"/old/project\" } "]).unwrap();
    let result = evidence(&conn, &fixture.session).unwrap();
    assert_eq!(
        result.lineages,
        std::collections::BTreeSet::from([
            String::new(),
            "B".into(),
            "b".into(),
            fixture.lineage.clone()
        ])
    );
}

#[test]
fn repeated_lineage_values_use_bounded_seeks_without_statistics() {
    let fixture = CostFixture::new(1000, 1000, 0);
    {
        let conn = fixture.store.conn.lock().unwrap();
        conn.execute_batch("ALTER TABLE message ADD COLUMN lineage_id TEXT")
            .unwrap();
        conn.execute("UPDATE message SET lineage_id=?1", [&fixture.lineage])
            .unwrap();
    }
    fixture.store.prepare_session_log_indexes().unwrap();
    let plans = fixture.plans();
    for query in plans {
        let sql = query["sql"].as_str().unwrap();
        if sql.starts_with("SELECT lineage_id FROM run_index")
            || sql.starts_with("SELECT lineage_id FROM message ")
        {
            assert!(query["rows"].as_u64().unwrap() <= 1, "{query:#?}");
            assert!(
                query["vm_steps"].as_u64().unwrap() < 100,
                "duplicate rows must not be visited: {query:#?}"
            );
        }
    }
}
