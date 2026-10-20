//! Retention scenarios against the real `ck-broca` behind the real `ck-subc` daemon.
//!
//! Admission and marker scenarios start retained lineages over the real serve path. Recovery
//! scenarios also seed older retained WALs directly, so boot enrollment and incomplete deletion
//! remain covered independently of admission. Later sends, index writes, provider calls,
//! request dumps and logs are produced by the daemon itself.
//!
//! Time moves through the `test-clock` control file in the state root, which a binary built
//! with the `test-clock` feature polls. Every scenario computes the instants it expects from
//! the WAL records the daemon wrote, never from the wall clock.

use std::time::{SystemTime, UNIX_EPOCH};

use broca_core::LeaseStore;

use super::*;

/// The retention every scenario's first run freezes, in seconds.
const RETENTION_S: u64 = 3600;
/// How long a scenario waits for the daemon to finish a deletion it has started. The declared
/// bound is an hour; with the clock control the daemon reacts within a poll interval.
const DELETION_WAIT: Duration = Duration::from_secs(60);

/// Whether the real-daemon rigs are present. A missing rig fails the test when
/// `BROCA_REQUIRE_SUBC` is set and skips it, out loud, otherwise.
fn rigs_present(what: &str) -> bool {
    for (name, env_key) in [
        ("ck-subc", "SUBC_CORE_BIN"),
        ("fake-aft-stub", "FAKE_AFT_STUB_BIN"),
    ] {
        if !sibling_bin(name, env_key).exists() {
            assert!(
                !require_subc(),
                "required real-daemon binary is missing: {name}"
            );
            println!("SKIP {what}: missing {name}");
            return false;
        }
    }
    // A test process launched by a daemon inherits that daemon's identity; the rig's own
    // daemon refuses it, so it is cleared (as the conformance test does).
    std::env::remove_var("SUBC_MODULE_ID");
    std::env::remove_var("SUBC_LAUNCH_NONCE");
    true
}

fn scratch(tag: &str) -> PathBuf {
    let scratch =
        broca_test_support::scratch_root().join(format!("retention-{tag}-{}", std::process::id()));
    broca_test_support::remove_rig_dir(&scratch);
    std::fs::create_dir_all(&scratch).unwrap();
    scratch
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

/// A marker no other test or earlier run can have written.
fn unique_marker(tag: &str) -> String {
    format!(
        "RETENTION-MARKER-{tag}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .subsec_nanos()
    )
}

/// Move the daemon's clock to the wall clock plus `offset_ms`. The control file is replaced
/// atomically, so a poll never reads half of it.
fn set_clock(state: &Path, offset_ms: i64) {
    std::fs::create_dir_all(state).unwrap();
    let temp = state.join("test-clock.tmp");
    std::fs::write(&temp, json!({ "offset_ms": offset_ms }).to_string()).unwrap();
    std::fs::rename(&temp, state.join("test-clock")).unwrap();
}

/// Move the daemon's clock so that it reads at least `instant_ms` now.
fn set_clock_at_least(state: &Path, instant_ms: u64) {
    set_clock(state, instant_ms as i64 - wall_ms() as i64 + 1_000);
}

fn wal_store(state: &Path) -> broca_core::FileWalStore {
    broca_core::FileWalStore::new(state.join("wal"))
}

fn lease(state: &Path, key: &broca_protocol::BindIdentity) -> Box<dyn broca_core::LeaseHandle> {
    broca_core::FileLeaseStore::new(state.join("leases"))
        .acquire(&broca_core::lease_key_for(key))
        .unwrap()
}

/// The first run of a retention lineage, written as a broca build that admits `retention` on
/// `session.send` would have written it: a `RunStarted` freezing [`RETENTION_S`] and its `RunFinished`. Returns
/// the lineage id.
#[allow(clippy::too_many_arguments)]
fn seed_first_run(
    state: &Path,
    key: &broca_protocol::BindIdentity,
    run_id: &str,
    prompt: &str,
    title: &str,
    started_ms: u64,
    finished_ms: u64,
) -> String {
    let store = wal_store(state);
    let _lease = lease(state, key);
    let mut wal = store.open_with_retention(key, Some(RETENTION_S)).unwrap();
    let mut start: Value = serde_json::from_str(include_str!(
        "../../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    ))
    .unwrap();
    start["retention"] = json!(RETENTION_S);
    start["ts_ms"] = json!(started_ms);
    start["session"] = serde_json::to_value(key).unwrap();
    start["run_id"] = json!(run_id);
    start["title"] = json!(title);
    // The system prompt is fixed at a session's first episode, so it is the one every later
    // send from this rig repeats (see `send_fields`).
    start["input"] = json!([
        {"role": "system", "content": [{"type": "text", "text": format!("{SESSION_MARKER}{}", key.session)}]},
        {"role": "user", "content": [{"type": "text", "text": prompt}]},
    ]);
    let start: broca_wal::WalRecord = serde_json::from_value(start).unwrap();
    wal.append(1, &start, true).unwrap();
    wal.append(
        1,
        &broca_wal::WalRecord::RunFinished {
            ts_ms: Some(finished_ms),
            reason: broca_protocol::RunFinishReason::Completed,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .unwrap();
    let lineage = wal.lineage_id().unwrap().to_string();
    // A writer that admits retention records the address's session in a content-free sidecar
    // when it first indexes the lineage; boot enrollment and deletion recovery find the
    // session through it.
    std::fs::write(
        store.ephemeral_wal_path(key).with_extension("session"),
        serde_json::to_vec(key).unwrap(),
    )
    .unwrap();
    lineage
}

/// The records of the address's live retention WAL.
fn ephemeral_records(
    state: &Path,
    key: &broca_protocol::BindIdentity,
) -> Vec<broca_wal::WalRecord> {
    broca_wal::replay_file(
        wal_store(state).ephemeral_wal_path(key),
        broca_wal::DEFAULT_MAX_READ_BYTES,
    )
    .unwrap()
    .into_iter()
    .map(|framed| framed.record)
    .collect()
}

/// The lineage's expiry as the daemon must compute it: the newest send or lifecycle record's
/// time plus the retention. `None` while a run is not terminal.
fn expiry_from_records(records: &[broca_wal::WalRecord]) -> Option<u64> {
    use broca_wal::WalRecord as R;
    let mut activity = 0;
    let mut open = false;
    for record in records {
        match record {
            R::RunStarted { ts_ms, .. } => {
                activity = ts_ms.unwrap_or(activity);
                open = true;
            }
            R::Queued { ts_ms, .. } | R::Steered { ts_ms, .. } => {
                activity = ts_ms.unwrap_or(activity);
            }
            R::RunFinished { ts_ms, .. } => {
                activity = ts_ms.unwrap_or(activity);
                open = false;
            }
            R::RunPaused { ts_ms, .. } => {
                activity = ts_ms.unwrap_or(activity);
                open = true;
            }
            _ => {}
        }
    }
    (!open).then_some(activity + RETENTION_S * 1000)
}

fn response(method: &str, reply: Reply) -> Value {
    match reply {
        Reply::Response(value) => value,
        Reply::Error(error) => panic!(
            "{method} refused {} {}: {:?}",
            error.code, error.message, error.detail
        ),
    }
}

/// The `expired_at_ms` of an `expired` refusal; panics on anything else.
fn expired_at(method: &str, reply: Reply) -> u64 {
    match reply {
        Reply::Error(error) if error.code == "expired" => error
            .detail
            .as_ref()
            .and_then(|detail| detail["expired_at_ms"].as_u64())
            .unwrap_or_else(|| panic!("{method}: expired without an instant: {error:?}")),
        Reply::Error(error) => panic!(
            "{method}: expected expired, got {} {}: {:?}",
            error.code, error.message, error.detail
        ),
        Reply::Response(value) => panic!("{method}: expected expired, content was served: {value}"),
    }
}

/// Wait until `run_id` is no longer active.
async fn wait_run_end(route: &BrocaRoute, run_id: &str) -> Value {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        let reply = route
            .request("run.status", json!({ "run_id": run_id }))
            .await
            .unwrap();
        // Right after a restart the boot pass may still be sealing the cut run; status then
        // answers a refusal classed transient ("could not tell"), which means poll again.
        let status = match reply {
            Reply::Error(error)
                if error
                    .detail
                    .as_ref()
                    .is_some_and(|detail| detail["class"] == "transient") =>
            {
                assert!(
                    Instant::now() < deadline,
                    "run {run_id} status stayed refused: {} {}",
                    error.code,
                    error.message
                );
                sleep(Duration::from_millis(50)).await;
                continue;
            }
            reply => response("run.status", reply),
        };
        if status["state"] != "active" {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "run {run_id} never ended: {status}"
        );
        sleep(Duration::from_millis(50)).await;
    }
}

/// Wait until the lineage's tombstone records `completed_at_ms`, and return the tombstone.
///
/// Besides clock changes, the daemon sweeps on a five-minute real-time interval, which picks up
/// a deletion an open actor's lease deferred. Rather than wait for it, the clock is moved
/// forward one second at a time, each move running a pass as the interval would. The total
/// moved stays far inside the declared `delete_within_ms` of an hour.
async fn wait_deletion_completed(state: &Path, stone: &Path) -> Value {
    let deadline = Instant::now() + DELETION_WAIT;
    let mut offset_ms = current_offset(state);
    let started_offset = offset_ms;
    let mut next_nudge = Instant::now() + Duration::from_secs(1);
    loop {
        if let Ok(bytes) = std::fs::read(stone) {
            let value: Value = serde_json::from_slice(&bytes).unwrap();
            if value.get("completed_at_ms").is_some() {
                assert!(
                    offset_ms - started_offset < 3_600_000,
                    "deletion needed more than delete_within_ms of clock movement"
                );
                return value;
            }
        }
        assert!(
            Instant::now() < deadline,
            "deletion never completed: {}",
            stone.display()
        );
        if Instant::now() >= next_nudge {
            offset_ms += 1_000;
            set_clock(state, offset_ms);
            next_nudge = Instant::now() + Duration::from_secs(1);
        }
        sleep(Duration::from_millis(25)).await;
    }
}

/// The offset the clock control file holds now, or zero without one.
fn current_offset(state: &Path) -> i64 {
    std::fs::read(state.join("test-clock"))
        .ok()
        .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
        .and_then(|value| value["offset_ms"].as_i64())
        .unwrap_or(0)
}

/// Every file under `roots` whose bytes contain `marker`.
fn files_holding(marker: &str, roots: &[&Path]) -> Vec<PathBuf> {
    fn walk(dir: &Path, marker: &[u8], found: &mut Vec<PathBuf>, scanned: &mut usize) {
        let Ok(entries) = std::fs::read_dir(dir) else {
            return;
        };
        for entry in entries.flatten() {
            let path = entry.path();
            let Ok(kind) = entry.file_type() else {
                continue;
            };
            if kind.is_dir() {
                walk(&path, marker, found, scanned);
            } else if kind.is_file() {
                *scanned += 1;
                if std::fs::read(&path)
                    .is_ok_and(|bytes| bytes.windows(marker.len()).any(|w| w == marker))
                {
                    found.push(path);
                }
            }
        }
    }
    let mut found = Vec::new();
    let mut scanned = 0;
    for root in roots {
        walk(root, marker.as_bytes(), &mut found, &mut scanned);
    }
    assert!(scanned > 0, "the marker scan read no files under {roots:?}");
    found
}

/// The three files SQLite keeps for `run-index.db`, each checked for `marker` if present.
fn index_files_holding(state: &Path, marker: &str) -> Vec<PathBuf> {
    ["run-index.db", "run-index.db-wal", "run-index.db-shm"]
        .into_iter()
        .map(|name| state.join(name))
        .filter(|path| {
            std::fs::read(path).is_ok_and(|bytes| {
                bytes
                    .windows(marker.len())
                    .any(|window| window == marker.as_bytes())
            })
        })
        .collect()
}

fn index(state: &Path) -> rusqlite::Connection {
    rusqlite::Connection::open(state.join("run-index.db")).unwrap()
}

/// `(state, title, expired_at_ms, lineage_id)` of a `run_index` row.
type RunRow = (String, Option<String>, Option<i64>, Option<String>);

/// The run's `run_index` row, if it has one.
fn run_row(state: &Path, run_id: &str) -> Option<RunRow> {
    use rusqlite::OptionalExtension;
    index(state)
        .query_row(
            "SELECT state, title, expired_at_ms, lineage_id FROM run_index WHERE run_id=?1",
            [run_id],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .optional()
        .unwrap()
}

/// Every stored export fact, by `fact_id`, with its exact bytes.
fn export_facts(state: &Path) -> std::collections::BTreeMap<String, (String, String)> {
    let conn = index(state);
    let mut statement = conn
        .prepare("SELECT fact_id, run_id, segment_json FROM export_facts")
        .unwrap();
    let rows = statement
        .query_map([], |row| Ok((row.get(0)?, (row.get(1)?, row.get(2)?))))
        .unwrap();
    rows.map(Result::unwrap).collect()
}

fn tool_call(id: &str, arguments: Value, result: Value, hold: bool) -> ScriptedPart {
    ScriptedPart::ToolCall(cortexkit_role_llm_runner_conformance::ScriptedToolCall {
        tool_call_id: id.into(),
        tool: SCRIPTED_TOOL.into(),
        arguments,
        result,
        hold,
    })
}

fn turn(parts: Vec<ScriptedPart>) -> cortexkit_role_llm_runner_conformance::ScriptedTurn {
    cortexkit_role_llm_runner_conformance::ScriptedTurn::new(parts).with_usage(
        cortexkit_role_llm_runner_conformance::ScriptedUsage::new(1, 0, 0, 1),
    )
}

/// A send without `retention` into `session`, so it continues whatever lineage is there.
fn follow_up(subject: &BrocaSubject, session: &str, prompt: &str, send_id: &str) -> Value {
    let mut params = subject.send_fields(session, false);
    params.insert("prompt".into(), json!(prompt));
    params.insert("send_id".into(), json!(send_id));
    Value::Object(params)
}

/// The marker check. A retention lineage carries one marker in its prompts, in a tool result
/// and in the titles of three runs. Before expiry the daemon serves it from the WAL while none
/// of `run-index.db`, `-wal` and `-shm` holds it. Across expiry an external reader holds a
/// `run-index.db` read transaction; deletion still completes. Afterwards the marker is in no
/// file under the state root, the logs or the request-dump directory; every kept `run_index`
/// row is content-free and terminal; stored export facts keep their bytes; and the next boot's
/// export catch-up finds nothing missed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_retention_marker_never_reaches_the_index_and_is_gone_after_expiry() {
    if !rigs_present("retention marker") {
        return;
    }
    let scratch = scratch("marker");
    let mut subject = BrocaSubject::new(&scratch).await;
    let dumps = scratch.join("dumps");
    subject.dump_dir = Some(dumps.clone());
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let session = "retained-marker";
    let key = subject.bind_identity(session);
    let marker = unique_marker("scan");
    let arguments = json!({ "lookup": "marker-tool" });
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    turn(vec![ScriptedPart::Text("first run".into())]),
                    turn(vec![
                        ScriptedPart::Text("calling the tool".into()),
                        tool_call(
                            "call_marker",
                            arguments.clone(),
                            json!({ "echo": format!("tool result {marker}") }),
                            false,
                        ),
                    ]),
                    turn(vec![ScriptedPart::Text("after the tool".into())]),
                    turn(vec![ScriptedPart::Text("third run".into())]),
                ],
            },
        )
        .await
        .unwrap();

    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let mut first = Value::Object(subject.send_fields(session, true));
    first["prompt"] = json!(format!("first prompt {marker}"));
    first["title"] = json!(format!("first title {marker}"));
    first["send_id"] = json!("retained-first");
    first["retention"] = json!(RETENTION_S);
    let admitted = response(
        "session.send",
        route.request("session.send", first).await.unwrap(),
    );
    let first_run = admitted["run_id"].as_str().unwrap().to_owned();
    assert_eq!(wait_run_end(&route, &first_run).await["state"], "completed");
    let lineage = lineage_of(&wal_store(&state).ephemeral_wal_path(&key));
    let mut runs = vec![first_run];
    for (n, prompt) in ["second", "third"].into_iter().enumerate() {
        let mut params = follow_up(
            &subject,
            session,
            &format!("{prompt} prompt {marker}"),
            &format!("marker-send-{n}"),
        );
        params["title"] = json!(format!("{prompt} title {marker}"));
        let sent = response(
            "session.send",
            route.request("session.send", params).await.unwrap(),
        );
        let run_id = sent["run_id"].as_str().unwrap().to_owned();
        let status = wait_run_end(&route, &run_id).await;
        assert_eq!(status["state"], "completed", "{status}");
        runs.push(run_id);
    }
    assert_eq!(subject.tool_invocations(&arguments).await, 1);
    // Require the unique content marker in the admitted WAL before testing its erasure.
    // An absent marker would let a deletion scan pass even if admission never wrote content.
    let live_wal = wal_store(&state).ephemeral_wal_path(&key);
    let wal_bytes = std::fs::read(&live_wal).unwrap();
    assert!(
        wal_bytes
            .windows(marker.len())
            .any(|window| window == marker.as_bytes()),
        "the retention WAL does not hold the marker"
    );
    assert!(!wal_store(&state).wal_path(&key).exists());
    let records = ephemeral_records(&state, &key);
    assert!(
        records
            .iter()
            .any(|record| matches!(record, broca_wal::WalRecord::ToolResult { .. }))
            && records
                .iter()
                .filter(|record| matches!(record, broca_wal::WalRecord::RunStarted { .. }))
                .count()
                == 3,
        "{records:?}"
    );

    // Before expiry: served from the WAL, absent from the index files.
    let page = response(
        "session.read",
        route.request("session.read", json!({})).await.unwrap(),
    );
    assert!(page.to_string().contains(&marker), "{page}");
    response(
        "session.head",
        route.request("session.head", json!({})).await.unwrap(),
    );
    for run_id in &runs {
        response(
            "run.status",
            route
                .request("run.status", json!({ "run_id": run_id }))
                .await
                .unwrap(),
        );
        let (_, title, expired, row_lineage) =
            run_row(&state, run_id).expect("every run is indexed, content-free");
        assert_eq!(title, None, "{run_id}");
        assert_eq!(expired, None, "{run_id}");
        assert_eq!(row_lineage.as_deref(), Some(lineage.as_str()), "{run_id}");
    }
    assert_eq!(
        index_files_holding(&state, &marker),
        Vec::<PathBuf>::new(),
        "the marker reached run-index.db before expiry"
    );
    let facts_before = export_facts(&state);
    assert!(
        runs[1..]
            .iter()
            .all(|run| facts_before.values().any(|(fact_run, _)| fact_run == run)),
        "the live runs were not exported: {facts_before:?}"
    );

    // Across expiry an external reader keeps a read transaction open on run-index.db.
    let expiry = expiry_from_records(&records).expect("every run is terminal");
    let mut reader = index(&state);
    let read = reader.transaction().unwrap();
    let _: i64 = read
        .query_row("SELECT count(*) FROM run_index", [], |row| row.get(0))
        .unwrap();
    set_clock_at_least(&state, expiry);
    let stone_path = wal_store(&state).tombstone_path(&key, &lineage);
    let stone = wait_deletion_completed(&state, &stone_path).await;
    drop(read);
    assert_eq!(stone["expired_at_ms"], expiry);
    assert!(!live_wal.exists());

    // After expiry: every read and control call answers the instant, with no content.
    for (method, params) in [
        ("session.read", json!({})),
        ("session.read", json!({ "from_ordinal": 0 })),
        ("session.head", json!({})),
    ] {
        let reply = route.request(method, params).await.unwrap();
        assert_eq!(expired_at(method, reply), expiry);
    }
    for run_id in &runs {
        for method in ["run.status", "run.result"] {
            let reply = route
                .request(method, json!({ "run_id": run_id }))
                .await
                .unwrap();
            assert_eq!(expired_at(method, reply), expiry, "{method} {run_id}");
        }
        let (state_name, title, expired, _) = run_row(&state, run_id).unwrap();
        assert_eq!(state_name, "completed", "{run_id}");
        assert_eq!(title, None, "{run_id}");
        assert_eq!(expired, Some(expiry as i64), "{run_id}");
    }
    let facts_after = export_facts(&state);
    for (fact_id, fact) in &facts_before {
        assert_eq!(
            facts_after.get(fact_id),
            Some(fact),
            "fact {fact_id} changed"
        );
    }
    drop(route);
    drop(live);

    let logs = root.join("data-home/cortexkit/broca/logs");
    let first_boot_catch_ups = catch_up_lines(&logs);
    let mut roots = vec![state.as_path(), logs.as_path()];
    if dumps.exists() {
        roots.push(dumps.as_path());
    }
    assert_eq!(
        files_holding(&marker, &roots),
        Vec::<PathBuf>::new(),
        "the marker outlived expiry"
    );

    // The next boot's export catch-up has nothing left to replay from the deleted WAL.
    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(30);
    let second_boot = loop {
        let fresh: Vec<_> = catch_up_lines(&logs)
            .into_iter()
            .filter(|line| !first_boot_catch_ups.contains(line))
            .collect();
        if !fresh.is_empty() {
            break fresh;
        }
        assert!(
            Instant::now() < deadline,
            "the second boot logged no export catch-up: {first_boot_catch_ups:#?}"
        );
        sleep(Duration::from_millis(50)).await;
    };
    assert!(
        second_boot
            .iter()
            .all(|line| line.contains("nothing missed")),
        "{second_boot:#?}"
    );
    assert_eq!(export_facts(&state), facts_after);
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

/// Every log line, across the log directory's files, in which the boot's usage-export catch-up
/// reports its outcome. Lines carry timestamps, so a later boot's line is a new one.
fn catch_up_lines(logs: &Path) -> std::collections::BTreeSet<String> {
    std::fs::read_dir(logs)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
        .flat_map(|text| {
            text.lines()
                .filter(|line| {
                    line.contains("usage-export startup catch-up")
                        || line.contains("missed usage-export run")
                })
                .map(str::to_owned)
                .collect::<Vec<_>>()
        })
        .collect()
}

/// Write a tombstone file in the shape the daemon's deletion writes:
/// `{lineage_id, expired_at_ms, seq}`, plus `completed_at_ms` once deletion finished.
fn write_tombstone(
    state: &Path,
    key: &broca_protocol::BindIdentity,
    lineage: &str,
    expired_at_ms: u64,
    seq: u64,
    completed_at_ms: Option<u64>,
) -> PathBuf {
    let path = wal_store(state).tombstone_path(key, lineage);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let mut stone = json!({ "lineage_id": lineage, "expired_at_ms": expired_at_ms, "seq": seq });
    if let Some(completed) = completed_at_ms {
        stone["completed_at_ms"] = json!(completed);
    }
    std::fs::write(&path, serde_json::to_vec(&stone).unwrap()).unwrap();
    path
}

/// Check that every read and control call on the session answers `expired` with `expected`.
/// Each call has a timeout, so one that waits on the session lease fails the test by name
/// instead of hanging it.
async fn address_calls_answer(route: &BrocaRoute, run_id: &str, expected: u64) {
    for (method, params) in [
        ("session.read", json!({})),
        ("session.read", json!({ "from_ordinal": 0 })),
        ("session.head", json!({})),
        ("session.warm", json!({})),
        ("run.cancel", json!({ "run_id": run_id })),
        (
            "session.retract",
            json!({ "submission_id": "sub-never-queued" }),
        ),
        ("run.status", json!({ "run_id": run_id })),
        ("run.result", json!({ "run_id": run_id })),
    ] {
        let reply = tokio::time::timeout(Duration::from_secs(20), route.request(method, params))
            .await
            .unwrap_or_else(|_| panic!("{method} did not answer"))
            .unwrap();
        assert_eq!(expired_at(method, reply), expected, "{method}");
    }
}

/// A durable tombstone whose lineage matches the live WAL keeps the address expired even
/// with the clock moved below the expiry instant, and the instant is that tombstone's, not an
/// older lineage's later one. Another process holds the session lease through boot, so boot recovery
/// leaves the deletion to the sweep and the daemon serves with the matching WAL still live.
/// Once the lease is free, a sweep pass finishes the deletion and the answers do not change.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_matching_tombstone_keeps_a_live_wal_expired_below_its_instant() {
    if !rigs_present("retention matching tombstone") {
        return;
    }
    let scratch = scratch("matching-stone");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let key = subject.bind_identity("matching-stone");
    let now = wall_ms();
    let lineage = seed_first_run(
        &state,
        &key,
        "stone-run",
        "matching tombstone prompt",
        "matching tombstone title",
        now - 2_000,
        now - 1_000,
    );
    let expiry = now - 1_000 + RETENTION_S * 1000;
    // An older lineage's completed deletion whose instant is later than the live lineage's,
    // as after a clock rollback. Neither the live lineage's answers nor, once it is deleted,
    // the address's may come from it: its own tombstone wins while its WAL is live, and the
    // higher sequence wins afterwards.
    write_tombstone(
        &state,
        &key,
        "older-lineage",
        expiry + 10_000_000,
        1,
        Some(expiry + 10_000_000),
    );
    let stone_path = write_tombstone(&state, &key, &lineage, expiry, 2, None);
    // The clock reads an hour before the WAL's own expiry instant, so only the tombstone can
    // make this lineage expired.
    set_clock(&state, -3_600_000);
    let held = lease(&state, &key);
    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, &key.session, &subject.owner_stamp())
        .await
        .unwrap();
    let live_wal = wal_store(&state).ephemeral_wal_path(&key);
    assert!(
        live_wal.exists(),
        "boot finished a deletion under a held lease"
    );
    for method in ["session.read", "session.head"] {
        let reply = tokio::time::timeout(Duration::from_secs(20), route.request(method, json!({})))
            .await
            .unwrap_or_else(|_| panic!("{method} waited on the held lease"))
            .unwrap();
        assert_eq!(expired_at(method, reply), expiry, "{method}");
    }
    assert!(live_wal.exists());
    drop(held);
    address_calls_answer(&route, "stone-run", expiry).await;
    // The daemon runs a sweep pass whenever the clock control changes. The clock stays below
    // the tombstone's instant.
    set_clock(&state, -3_599_000);
    let stone = wait_deletion_completed(&state, &stone_path).await;
    assert_eq!(stone["expired_at_ms"], expiry);
    assert!(!live_wal.exists());
    assert_eq!(deleting_files(&state), Vec::<PathBuf>::new());
    address_calls_answer(&route, "stone-run", expiry).await;
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

/// Every moved-aside WAL still waiting in `wal-ephemeral/` for its deletion.
fn deleting_files(state: &Path) -> Vec<PathBuf> {
    std::fs::read_dir(wal_store(state).ephemeral_root())
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "deleting"))
        .collect()
}

/// How many lines across the daemon's log files contain `text`.
fn log_count(logs: &Path, text: &str) -> usize {
    std::fs::read_dir(logs)
        .into_iter()
        .flatten()
        .flatten()
        .filter_map(|entry| std::fs::read_to_string(entry.path()).ok())
        .map(|log| log.lines().filter(|line| line.contains(text)).count())
        .sum()
}

async fn wait_for_log(logs: &Path, text: &str, at_least: usize) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while log_count(logs, text) < at_least {
        assert!(
            Instant::now() < deadline,
            "the daemon never logged {text:?} {at_least} time(s)"
        );
        sleep(Duration::from_millis(25)).await;
    }
}

/// A `session.subscribe` held open on a route of its own until the daemon ends it. The task
/// returns every event and how the stream ended.
async fn hold_subscription(route: &BrocaRoute) -> tokio::task::JoinHandle<(Vec<Value>, String)> {
    let own = route
        .connection
        .route_open_management("broca", &route.session)
        .await
        .unwrap();
    let body = serde_json::to_vec(&json!({ "method": "session.subscribe", "params": {} })).unwrap();
    let mut stream = route.connection.subscribe(&own, body).await.unwrap();
    let connection = route.connection.clone();
    tokio::spawn(async move {
        let mut events = Vec::new();
        let ending = loop {
            match stream.next().await {
                Ok(Some(event)) => {
                    events.push(serde_json::from_slice(&event).unwrap_or(Value::Null))
                }
                Ok(None) => break "ended".to_owned(),
                Err(error) => break error.to_string(),
            }
        };
        drop(stream);
        let _ = connection.route_close(&own).await;
        (events, ending)
    })
}

/// The address's live `retention_lineage` deadline, as the index records it.
fn indexed_deadline(state: &Path, lineage: &str) -> Option<i64> {
    index(state)
        .query_row(
            "SELECT expires_at_ms FROM retention_lineage WHERE lineage_id=?1",
            [lineage],
            |row| row.get(0),
        )
        .unwrap()
}

/// When index writes fail, expiry still follows the WAL's records. A follow-up send is admitted
/// while another process holds `run-index.db`'s write lock, so its `run_index` insert and the
/// lineage's content-free row update both fail, leaving the first run's deadline indexed. With
/// the run still active, the clock passes that deadline: the sweep pass checks the WAL, sees
/// an active run and skips. The run's terminal record is then stamped by the moved clock, the
/// lineage stays readable until one retention after that terminal, and expires exactly then:
/// the deletion lands with no restart and no read, the subscriber held open since before the
/// send is ended, and `run.status` and `run.result` for the run whose insert failed answer
/// `expired` with the terminal's time plus the retention.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_failed_retention_index_writes_still_expire_on_the_wal_deadline() {
    if !rigs_present("retention index failure") {
        return;
    }
    let scratch = scratch("index-failure");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let logs = root.join("data-home/cortexkit/broca/logs");
    let session = "index-failure";
    let key = subject.bind_identity(session);
    let now = wall_ms();
    let lineage = seed_first_run(
        &state,
        &key,
        "index-first",
        "index failure prompt",
        "index failure title",
        now - 2_000,
        now - 1_000,
    );
    let first_expiry = now - 1_000 + RETENTION_S * 1000;
    let arguments = json!({ "held": "index-failure" });
    subject
        .install_script(
            session,
            Script {
                turns: vec![
                    turn(vec![
                        ScriptedPart::Text("holding".into()),
                        tool_call("call_held", arguments.clone(), json!({ "ok": true }), true),
                    ]),
                    turn(vec![ScriptedPart::Text("done".into())]),
                ],
            },
        )
        .await
        .unwrap();
    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    // Boot enrollment indexed the seeded lineage's deadline.
    assert_eq!(
        indexed_deadline(&state, &lineage),
        Some(first_expiry as i64)
    );
    let subscription = hold_subscription(&route).await;
    // Opening the session's actor writes to the index, so the write lock below is taken only
    // once the actor is open and serving.
    response(
        "session.head",
        route.request("session.head", json!({})).await.unwrap(),
    );
    sleep(Duration::from_secs(1)).await;

    let active_failures = log_count(&logs, "run-index active write failed");
    let row_failures = log_count(&logs, "content-free index write failed; repair enrolled");
    let blocker = index(&state);
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    // The send's reply waits behind the failing index writes, longer than the client's
    // request timeout, so it is sent on a route of its own and its run is read from the WAL.
    let sender = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let send = follow_up(&subject, session, "a held run", "index-failure-send");
    let mut sending = Some(tokio::spawn(async move {
        let reply = sender.request("session.send", send).await;
        (sender, reply)
    }));
    // Admission runs behind the failing writes, so the call can take a while to arrive. A
    // refusal ends the wait at once; the client's own request timeout does not.
    let deadline = Instant::now() + Duration::from_secs(120);
    while subject.tool_invocations(&arguments).await == 0 {
        if sending.as_ref().is_some_and(|call| call.is_finished()) {
            let (_, reply) = sending.take().unwrap().await.unwrap();
            if let Ok(Reply::Error(error)) = &reply {
                panic!("the send was refused: {error:?}");
            }
        }
        assert!(
            Instant::now() < deadline,
            "the held tool call never arrived; the send answered: {}",
            sending.is_none()
        );
        sleep(Duration::from_millis(50)).await;
    }
    let run_id = ephemeral_records(&state, &key)
        .iter()
        .rev()
        .find_map(|record| match record {
            broca_wal::WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
            _ => None,
        })
        .filter(|run_id| run_id != "index-first")
        .expect("the send wrote its RunStarted");
    wait_for_log(&logs, "run-index active write failed", active_failures + 1).await;
    wait_for_log(
        &logs,
        "content-free index write failed; repair enrolled",
        row_failures + 1,
    )
    .await;
    // Assert the first finished run's indexed expiry while the write blocker is held.
    // After ROLLBACK, background repair may derive no deadline for the running episode.
    assert_eq!(
        indexed_deadline(&state, &lineage),
        Some(first_expiry as i64)
    );
    blocker.execute_batch("ROLLBACK").unwrap();
    drop(blocker);
    if let Some(call) = sending {
        drop(call.await.unwrap());
    }
    let passes = log_count(&logs, "retention sweep complete");
    set_clock_at_least(&state, first_expiry);
    wait_for_log(&logs, "retention sweep complete", passes + 1).await;
    let stone_path = wal_store(&state).tombstone_path(&key, &lineage);
    assert!(
        !stone_path.exists(),
        "an active run's lineage was tombstoned"
    );
    let page = response(
        "session.read",
        route.request("session.read", json!({})).await.unwrap(),
    );
    assert!(page.to_string().contains("a held run"), "{page}");

    subject.release_tool_call(&arguments).await.unwrap();
    let status = wait_run_end(&route, &run_id).await;
    assert_eq!(status["state"], "completed", "{status}");
    let records = ephemeral_records(&state, &key);
    let expiry = expiry_from_records(&records).unwrap();
    let terminal = expiry - RETENTION_S * 1000;
    assert!(
        terminal >= first_expiry,
        "the terminal {terminal} was not stamped in the advanced clock's domain"
    );

    // Five seconds before that expiry the lineage is still served, even across a sweep pass.
    let passes = log_count(&logs, "retention sweep complete");
    set_clock(&state, expiry as i64 - 5_000 - wall_ms() as i64);
    wait_for_log(&logs, "retention sweep complete", passes + 1).await;
    assert!(!stone_path.exists());
    response(
        "session.head",
        route.request("session.head", json!({})).await.unwrap(),
    );

    set_clock_at_least(&state, expiry);
    let stone = wait_deletion_completed(&state, &stone_path).await;
    assert_eq!(stone["expired_at_ms"], expiry);
    let (events, ending) = tokio::time::timeout(Duration::from_secs(30), subscription)
        .await
        .expect("the subscription outlived expiry")
        .unwrap();
    assert!(
        ending.contains("expired")
            || events
                .last()
                .is_some_and(|event| event.to_string().contains("expired")),
        "the subscription ended without expired: {ending} {events:?}"
    );
    for method in ["run.status", "run.result"] {
        let reply = route
            .request(method, json!({ "run_id": run_id }))
            .await
            .unwrap();
        assert_eq!(expired_at(method, reply), expiry, "{method}");
    }
    let (state_name, title, expired, row_lineage) =
        run_row(&state, &run_id).expect("re-derivation inserted the failed run's row");
    assert_eq!(state_name, "completed");
    assert_eq!(title, None);
    assert_eq!(expired, Some(expiry as i64));
    assert_eq!(row_lineage.as_deref(), Some(lineage.as_str()));
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

/// Arm the rig to hold the runner at `point` and report the session that reaches it.
fn arm(subject: &BrocaSubject, point: &'static str) -> Arc<Notify> {
    let reached = Arc::new(Notify::new());
    *subject.shared.armed.lock().unwrap() = Some(Armed {
        point,
        reached: reached.clone(),
        session: Arc::new(Mutex::new(None)),
    });
    reached
}

/// Real process kills at each record boundary of a send into a retention lineage, with the
/// clock advanced while the daemon is down. Each restart ends the cut run with a terminal
/// stamped in the advanced clock's domain; the lineage then expires exactly one retention after
/// that terminal, never earlier, and its deletion completes with the tombstone carrying that
/// instant rather than one computed from the wall clock.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_retention_sends_killed_at_each_record_boundary_resume_to_complete_deletion() {
    if !rigs_present("retention send kills") {
        return;
    }
    let scratch = scratch("send-kills");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let mut live = subject.start(&root).await.unwrap();
    for point in [
        points::SEND_RECORDED,
        points::DISPATCH_INTENT,
        points::TOOL_RESULT_RECORDED,
        points::TERMINAL,
    ] {
        let session = format!("killed-at-{}", point.to_ascii_lowercase());
        let key = subject.bind_identity(&session);
        let started = (wall_ms() as i64 + current_offset(&state)) as u64;
        let lineage = seed_first_run(
            &state,
            &key,
            &format!("{session}-first"),
            "killed send prompt",
            "killed send title",
            started - 2_000,
            started - 1_000,
        );
        let arguments = json!({ "cut": point });
        subject
            .install_script(
                &session,
                Script {
                    turns: vec![
                        turn(vec![
                            ScriptedPart::Text("calling".into()),
                            tool_call("call_cut", arguments.clone(), json!({ "ok": point }), false),
                        ]),
                        turn(vec![ScriptedPart::Text("done".into())]),
                    ],
                },
            )
            .await
            .unwrap();
        let route = subject
            .session_route(&live, &session, &subject.owner_stamp())
            .await
            .unwrap();
        let send = follow_up(&subject, &session, "a send to be killed", "killed-send");
        let mut sending = None;
        let run_id = if point == points::TERMINAL {
            let sent = response(
                "session.send",
                route.request("session.send", send).await.unwrap(),
            );
            let run_id = sent["run_id"].as_str().unwrap().to_owned();
            wait_run_end(&route, &run_id).await;
            run_id
        } else {
            let reached = arm(&subject, point);
            let call = tokio::spawn(async move { route.request("session.send", send).await });
            tokio::time::timeout(Duration::from_secs(60), reached.notified())
                .await
                .unwrap_or_else(|_| panic!("the runner never reached {point}"));
            let records = ephemeral_records(&state, &key);
            let run_id = records
                .iter()
                .rev()
                .find_map(|record| match record {
                    broca_wal::WalRecord::RunStarted { run_id, .. } => Some(run_id.0.clone()),
                    _ => None,
                })
                .filter(|run_id| !run_id.ends_with("-first"))
                .unwrap_or_else(|| panic!("{point}: the send wrote no RunStarted: {records:?}"));
            sending = Some(call);
            run_id
        };
        // A run that ended before the kill is deleted by the restart's boot recovery, so its
        // expiry is read from the WAL now.
        let ended_before_kill = ephemeral_records(&state, &key);
        assert!(live.kill_all(), "{point}: no ck-broca process was killed");
        subject.shared.released.notify_waiters();
        // The cut send's reply is not needed: the kill ended the call.
        if let Some(call) = sending {
            call.abort();
        }
        drop(live);
        // Two hours pass while the daemon is down.
        let offset_ms = current_offset(&state) + 7_200_000;
        set_clock(&state, offset_ms);
        let restarted_at = (wall_ms() as i64 + offset_ms) as u64;
        live = subject.start(&root).await.unwrap();
        let route = subject
            .session_route(&live, &session, &subject.owner_stamp())
            .await
            .unwrap();
        if point != points::TERMINAL {
            // The restart ends the cut run; a run that ended before the kill is already past
            // its expiry once the clock has moved two hours.
            let status = wait_run_end(&route, &run_id).await;
            assert_ne!(status["state"], "paused", "{point}: {status}");
        }
        let records = if point == points::TERMINAL {
            ended_before_kill
        } else {
            ephemeral_records(&state, &key)
        };
        let expiry = expiry_from_records(&records)
            .unwrap_or_else(|| panic!("{point}: the cut run never ended: {records:?}"));
        if point != points::TERMINAL {
            assert!(
                expiry - RETENTION_S * 1000 >= restarted_at - 1_000,
                "{point}: the restart's terminal was not stamped in the advanced clock's domain"
            );
        }
        let stone_path = wal_store(&state).tombstone_path(&key, &lineage);
        if point == points::TERMINAL {
            // The run ended before the kill; two hours past its terminal it is already expired.
            assert!(expiry < restarted_at, "{point}");
        } else {
            response(
                "session.read",
                route.request("session.read", json!({})).await.unwrap(),
            );
            assert!(!stone_path.exists(), "{point}: expired before its instant");
            set_clock_at_least(&state, expiry);
        }
        let stone = wait_deletion_completed(&state, &stone_path).await;
        assert_eq!(stone["expired_at_ms"], expiry, "{point}");
        assert!(
            !wal_store(&state).ephemeral_wal_path(&key).exists(),
            "{point}"
        );
        let reply = route.request("session.read", json!({})).await.unwrap();
        assert_eq!(expired_at("session.read", reply), expiry, "{point}");
        for method in ["run.status", "run.result"] {
            let reply = route
                .request(method, json!({ "run_id": run_id }))
                .await
                .unwrap();
            assert_eq!(expired_at(method, reply), expiry, "{point} {method}");
        }
    }
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

/// The store key `meta` and `message` rows use: the session triple joined by U+001F.
fn store_key(key: &broca_protocol::BindIdentity) -> String {
    format!(
        "{}\u{1f}{}\u{1f}{}",
        key.project_root.display(),
        key.harness,
        key.session
    )
}

/// How many `meta` and `message` rows the address holds.
fn content_rows(state: &Path, key: &broca_protocol::BindIdentity) -> (i64, i64) {
    let conn = index(state);
    let count = |table: &str| -> i64 {
        conn.query_row(
            &format!("SELECT count(*) FROM {table} WHERE session=?1"),
            [store_key(key)],
            |row| row.get(0),
        )
        .unwrap()
    };
    (count("meta"), count("message"))
}

fn lineage_of(path: &Path) -> String {
    broca_wal::read_records_range(path, broca_wal::DEFAULT_MAX_READ_BYTES, 0, None, None)
        .unwrap()
        .lineage_id
        .expect("an identified lineage")
        .to_string()
}

/// Expired A, then a forever B at the same address, then B folded.
///
/// A is left between move-aside and its row update while another process holds the session
/// lease. A send to the address waits for the lease and creates nothing until A's deletion is
/// complete; then B is admitted under `wal/` with a lineage of its own. A real kill followed by
/// a restart that resumes A's deletion (its tombstone missing `completed_at_ms`, as after a
/// kill past file deletion) leaves B's WAL bytes and its `meta`/`message` rows untouched. Runs
/// of A keep answering A's instant while B's content is served. After B is folded into the
/// archive it is still served, and it is served from its archived member alone once its
/// `meta` row is gone: an archived member supersedes the older lineage's tombstone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_a_send_waits_for_unfinished_deletion_and_the_fresh_lineage_survives_it() {
    if !rigs_present("retention serialized lineages") {
        return;
    }
    let scratch = scratch("serialized");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let session = "serialized";
    let key = subject.bind_identity(session);
    let store = wal_store(&state);
    let now = wall_ms();
    let lineage_a = seed_first_run(
        &state,
        &key,
        "serialized-a",
        "lineage A prompt",
        "lineage A title",
        now - 7_200_000,
        now - 7_199_000,
    );
    let expiry_a = now - 7_199_000 + RETENTION_S * 1000;
    // A's deletion was cut between move-aside and its row update.
    let stone_a = write_tombstone(&state, &key, &lineage_a, expiry_a, 1, None);
    let live_a = store.ephemeral_wal_path(&key);
    std::fs::write(
        live_a.with_extension("session"),
        serde_json::to_vec(&key).unwrap(),
    )
    .unwrap();
    let deleting_a = store.ephemeral_root().join(format!(
        "{}.{lineage_a}.deleting",
        broca_core::session_addr(&key)
    ));
    std::fs::rename(&live_a, &deleting_a).unwrap();
    subject
        .install_script(
            session,
            Script {
                turns: vec![turn(vec![ScriptedPart::Text("lineage B answer".into())])],
            },
        )
        .await
        .unwrap();

    let held = lease(&state, &key);
    let mut live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let send = follow_up(&subject, session, "lineage B prompt", "serialized-b");
    let sending = tokio::spawn(async move {
        let reply = route.request("session.send", send).await;
        (route, reply)
    });
    sleep(Duration::from_secs(2)).await;
    assert!(
        !sending.is_finished(),
        "the send did not wait for A's deletion"
    );
    assert!(
        !store.wal_path(&key).exists(),
        "B was created before A completed"
    );
    assert!(!live_a.exists());
    assert!(
        deleting_a.exists(),
        "A's deletion advanced under a held lease"
    );
    drop(held);
    let (route, reply) = tokio::time::timeout(Duration::from_secs(60), sending)
        .await
        .expect("the send never proceeded after the lease was released")
        .unwrap();
    let sent = response("session.send", reply.unwrap());
    let run_b = sent["run_id"].as_str().unwrap().to_owned();
    assert_eq!(wait_run_end(&route, &run_b).await["state"], "completed");
    let stone = serde_json::from_slice::<Value>(&std::fs::read(&stone_a).unwrap()).unwrap();
    assert!(stone["completed_at_ms"].is_u64(), "{stone}");
    assert!(!deleting_a.exists());
    let portable_b = store.wal_path(&key);
    let lineage_b = lineage_of(&portable_b);
    assert_ne!(lineage_b, lineage_a);
    assert!(!live_a.exists());
    let check_served = |page: Value| {
        let text = page.to_string();
        assert!(text.contains("lineage B prompt"), "{text}");
        assert!(!text.contains("lineage A prompt"), "{text}");
    };
    check_served(response(
        "session.read",
        route.request("session.read", json!({})).await.unwrap(),
    ));
    let reply = route
        .request("run.status", json!({ "run_id": "serialized-a" }))
        .await
        .unwrap();
    assert_eq!(expired_at("run.status", reply), expiry_a);
    let b_bytes = std::fs::read(&portable_b).unwrap();
    let b_rows = content_rows(&state, &key);
    assert!(
        b_rows.0 == 1 && b_rows.1 > 0,
        "B was not projected: {b_rows:?}"
    );
    drop(route);

    // A real kill after A's file deletion and before its completion was recorded: the restart
    // records completion and touches nothing of B.
    assert!(live.kill_all());
    drop(live);
    let mut stone = stone;
    stone.as_object_mut().unwrap().remove("completed_at_ms");
    std::fs::write(&stone_a, serde_json::to_vec(&stone).unwrap()).unwrap();
    live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let stone = serde_json::from_slice::<Value>(&std::fs::read(&stone_a).unwrap()).unwrap();
    assert!(
        stone["completed_at_ms"].is_u64(),
        "boot left A incomplete: {stone}"
    );
    assert_eq!(stone["expired_at_ms"], expiry_a);
    assert_eq!(std::fs::read(&portable_b).unwrap(), b_bytes);
    assert_eq!(content_rows(&state, &key), b_rows);
    check_served(response(
        "session.read",
        route.request("session.read", json!({})).await.unwrap(),
    ));
    for (run, expected) in [("serialized-a", Some(expiry_a)), (run_b.as_str(), None)] {
        let reply = route
            .request("run.status", json!({ "run_id": run }))
            .await
            .unwrap();
        match expected {
            Some(instant) => assert_eq!(expired_at("run.status", reply), instant),
            None => assert_eq!(response("run.status", reply)["state"], "completed"),
        }
    }
    drop(route);

    // B, folded into the archive, is still B.
    assert!(live.kill_all());
    drop(live);
    let identities = broca_store::fold_identities(&state).unwrap();
    let options = broca_core::wal_fold::FoldOptions {
        write: true,
        ..Default::default()
    };
    let report = broca_core::wal_fold::fold_with_identities(
        &state,
        broca_core::wal_fold::FoldSettings {
            options: &options,
            cold_ms: 0,
        },
        &identities,
    )
    .unwrap();
    assert_eq!(report.removed, 1, "{report:?}");
    assert!(!portable_b.exists());
    live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    check_served(response(
        "session.read",
        route.request("session.read", json!({})).await.unwrap(),
    ));
    let reply = route
        .request("run.status", json!({ "run_id": "serialized-a" }))
        .await
        .unwrap();
    assert_eq!(expired_at("run.status", reply), expiry_a);
    drop(route);

    // With B's meta row gone, its archived member alone still claims the address.
    assert!(live.kill_all());
    drop(live);
    index(&state)
        .execute("DELETE FROM meta WHERE session=?1", [store_key(&key)])
        .unwrap();
    live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let reply = route.request("session.head", json!({})).await.unwrap();
    response("session.head", reply);
    check_served(response(
        "session.read",
        route.request("session.read", json!({})).await.unwrap(),
    ));
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

/// A provider's error text never reaches the logs for a retention lineage. The model answers a
/// send into a retention lineage, and one into a forever session, with HTTP 400 whose message
/// and code both carry a marker. Both texts reach broca (each run's terminal record holds its
/// marker), yet no file under the logs holds the retention marker, before or after deletion,
/// and once the lineage is deleted nothing under the state root holds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_retention_provider_errors_never_reach_the_logs() {
    if !rigs_present("retention provider errors") {
        return;
    }
    let scratch = scratch("provider-errors");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let logs = root.join("data-home/cortexkit/broca/logs");
    let retained = "provider-error-retained";
    let forever = "provider-error-forever";
    let key = subject.bind_identity(retained);
    let retained_marker = unique_marker("provider-retained");
    let forever_marker = unique_marker("provider-forever");
    let now = wall_ms();
    let lineage = seed_first_run(
        &state,
        &key,
        "provider-error-first",
        "provider error prompt",
        "provider error title",
        now - 2_000,
        now - 1_000,
    );
    for (session, marker) in [(retained, &retained_marker), (forever, &forever_marker)] {
        subject.shared.errors.lock().unwrap().insert(
            session.to_owned(),
            json!({"error": {
                "message": format!("backend rejected the request: {marker}"),
                "type": "invalid_request_error",
                "code": marker,
            }})
            .to_string(),
        );
    }
    let live = subject.start(&root).await.unwrap();
    for session in [retained, forever] {
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let sent = response(
            "session.send",
            route
                .request(
                    "session.send",
                    follow_up(&subject, session, "a refused send", "provider-error-send"),
                )
                .await
                .unwrap(),
        );
        let run_id = sent["run_id"].as_str().unwrap().to_owned();
        let status = wait_run_end(&route, &run_id).await;
        assert_ne!(status["state"], "completed", "{session}: {status}");
    }
    // Both errors reached broca: each run's terminal record carries the backend's text. So a
    // log line formatting the error would have carried the marker too.
    let holds = |path: &Path, marker: &str| {
        std::fs::read(path).is_ok_and(|bytes| {
            bytes
                .windows(marker.len())
                .any(|window| window == marker.as_bytes())
        })
    };
    assert!(holds(
        &wal_store(&state).wal_path(&subject.bind_identity(forever)),
        &forever_marker
    ));
    assert!(holds(
        &wal_store(&state).ephemeral_wal_path(&key),
        &retained_marker
    ));
    let state_logs = state.join("logs");
    assert_eq!(
        files_holding(&retained_marker, &[logs.as_path(), state_logs.as_path()]),
        Vec::<PathBuf>::new(),
        "a retention lineage's provider error reached the logs"
    );

    let records = ephemeral_records(&state, &key);
    let expiry = expiry_from_records(&records).unwrap();
    set_clock_at_least(&state, expiry);
    let stone_path = wal_store(&state).tombstone_path(&key, &lineage);
    wait_deletion_completed(&state, &stone_path).await;
    drop(live);
    assert_eq!(
        files_holding(&retained_marker, &[state.as_path(), logs.as_path()]),
        Vec::<PathBuf>::new(),
        "the retention lineage's provider error outlived its deletion"
    );
    broca_test_support::remove_rig_dir(scratch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ck_broca_shared_retention_clock_waits_for_actual_deletion() {
    if !rigs_present("shared retention clock") {
        return;
    }
    let scratch = scratch("shared-clock");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let session = "shared-retention-clock";
    let key = subject.bind_identity(session);
    let now = wall_ms();
    seed_first_run(
        &state,
        &key,
        "clock-run",
        "prompt",
        "title",
        now - 2000,
        now - 1000,
    );
    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    subject
        .advance_retention_clock(session, RETENTION_S * 1000 + 1000)
        .await
        .unwrap();
    assert!(subject
        .retention_deletion_finished(&live, session)
        .await
        .unwrap());
    assert!(!wal_store(&state).ephemeral_wal_path(&key).exists());
    assert_eq!(
        expired_at(
            "run.status",
            route.request("run.status", json!({})).await.unwrap()
        ),
        now - 1000 + RETENTION_S * 1000
    );
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ck_broca_retained_size_refusal_uses_real_serve_golden_and_durable_error() {
    if !rigs_present("retained size bound") {
        return;
    }
    let scratch = scratch("size-bound");
    let subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    let session = "size-admission";
    let key = subject.bind_identity(session);
    let now = wall_ms();
    seed_first_run(
        &state,
        &key,
        "size-first",
        "prompt",
        "title",
        now - 2000,
        now - 1000,
    );
    std::fs::write(state.join("test-retained-wal-cap"), "4096").unwrap();
    let live = subject.start(&root).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let reply = route
        .request(
            "session.send",
            follow_up(&subject, session, &"x".repeat(8192), "too-large-input"),
        )
        .await
        .unwrap();
    let Reply::Error(error) = reply else {
        panic!("oversize input was admitted: {reply:?}");
    };
    let golden: Value =
        serde_json::from_str(include_str!("../golden/retained_session_too_large.json")).unwrap();
    assert_eq!(
        json!({"code": error.code, "message": error.message, "detail": error.detail}),
        golden
    );
    assert!(
        std::fs::metadata(wal_store(&state).ephemeral_wal_path(&key))
            .unwrap()
            .len()
            <= 4096
    );

    let session = "size-mid-run";
    let key = subject.bind_identity(session);
    std::fs::write(state.join("test-retained-wal-cap"), "32768").unwrap();
    seed_first_run(
        &state,
        &key,
        "size-mid-first",
        "prompt",
        "title",
        now - 2000,
        now - 1000,
    );
    subject
        .install_script(
            session,
            Script {
                turns: vec![turn(vec![ScriptedPart::Text("y".repeat(131072))])],
            },
        )
        .await
        .unwrap();
    let mid = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let sent = response(
        "session.send",
        mid.request(
            "session.send",
            follow_up(&subject, session, "small input", "mid-limit"),
        )
        .await
        .unwrap(),
    );
    let run_id = sent["run_id"].as_str().unwrap();
    assert_eq!(wait_run_end(&mid, run_id).await["state"], "error");
    let records = ephemeral_records(&state, &key);
    assert!(
        matches!(records.last().unwrap(), broca_wal::WalRecord::RunFinished {
        reason: broca_protocol::RunFinishReason::Error, error: Some(error), ..
    } if error.provider_code.as_deref() == Some("retained_session_too_large") && error.class == broca_protocol::ErrorClass::Permanent)
    );
    assert!(
        std::fs::metadata(wal_store(&state).ephemeral_wal_path(&key))
            .unwrap()
            .len()
            <= 32768
    );
    subject
        .advance_retention_clock(session, RETENTION_S * 1000 + 1000)
        .await
        .unwrap();
    assert!(subject
        .retention_deletion_finished(&live, session)
        .await
        .unwrap());
    drop(mid);
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

impl BrocaSubject {
    pub(super) async fn kill_retention(
        &self,
        mut handle: Live,
        point: &KillPoint,
        mut trigger: Trigger<'_>,
    ) -> Result<KillReport, HarnessError> {
        let session = self
            .shared
            .last_session
            .lock()
            .unwrap()
            .clone()
            .ok_or_else(|| HarnessError::new("retention kill has no written session"))?;
        let key = self.bind_identity(&session);
        let wal = wal_store(&handle.state_root);
        let live_wal = wal.ephemeral_wal_path(&key);
        let lineage = lineage_of(&live_wal);
        let stone = wal.tombstone_path(&key, &lineage);
        let obstacle = wal.ephemeral_root().join(format!(
            "{}.{lineage}.deleting",
            broca_core::session_addr(&key)
        ));
        // Deletion fsyncs its tombstone before renaming the live WAL to this destination.
        // A directory blocks that rename, holding a real recovery boundary with content intact
        // without truncating the WAL or fabricating a tombstone.
        std::fs::create_dir(&obstacle).map_err(|e| HarnessError::new(e.to_string()))?;
        let reached = tokio::time::timeout(DELETION_WAIT, async {
            loop {
                if let Ok(bytes) = std::fs::read(&stone) {
                    let value: Value = serde_json::from_slice(&bytes)
                        .map_err(|e| HarnessError::new(e.to_string()))?;
                    if value["completed_at_ms"].is_null() && live_wal.is_file() {
                        return Ok(());
                    }
                    return Err(HarnessError::new(
                        "retention cut did not preserve live content",
                    ));
                }
                sleep(Duration::from_millis(10)).await;
            }
        });
        let result = tokio::select! {
            () = &mut trigger => Err(HarnessError::new("retention trigger finished before tombstone")),
            result = reached => result.unwrap_or_else(|_| Err(HarnessError::new("retention tombstone was not reached"))),
        };
        let killed = handle.kill_all();
        drop(handle);
        std::fs::remove_dir(&obstacle).map_err(|e| HarnessError::new(e.to_string()))?;
        result?;
        if !killed {
            return Err(HarnessError::new("no ck-broca process was found to kill"));
        }
        Ok(KillReport {
            point: point.clone(),
            mechanism: KillMechanism::ProcessSignal,
        })
    }

    pub(super) async fn advance_retention(
        &self,
        session: &str,
        milliseconds: u64,
    ) -> Result<(), HarnessError> {
        let state = self
            .shared
            .session_roots
            .lock()
            .unwrap()
            .get(session)
            .cloned()
            .ok_or_else(|| HarnessError::new("retention session has no daemon state root"))?;
        let logs = state
            .parent()
            .unwrap()
            .join("data-home/cortexkit/broca/logs");
        let before = log_count(&logs, "retention sweep complete");
        let control: Value = std::fs::read(state.join("test-clock"))
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or(json!({}));
        let now = control["now_ms"].as_u64().unwrap_or_else(|| {
            (wall_ms() as i64 + control["offset_ms"].as_i64().unwrap_or(0)) as u64
        });
        let target = now
            .checked_add(milliseconds)
            .ok_or_else(|| HarnessError::new("retention clock overflow"))?;
        let key = self.bind_identity(session);
        let wal = wal_store(&state);
        let records = broca_wal::replay_file(
            wal.ephemeral_wal_path(&key),
            broca_wal::DEFAULT_MAX_READ_BYTES,
        )
        .map_err(|error| HarnessError::new(error.to_string()))?;
        let due = broca_module::retention::policy(&records)
            .and_then(|(_, _, deadline)| deadline)
            .is_some_and(|deadline| deadline <= target);
        let mut pulse = control["offset_ms"].as_i64().unwrap_or(0);
        let deadline = Instant::now() + DELETION_WAIT;
        let mut next_pulse = Instant::now();
        loop {
            if Instant::now() >= next_pulse {
                pulse = if pulse == 0 { 1 } else { 0 };
                let temp = state.join("test-clock.tmp");
                std::fs::write(
                    &temp,
                    json!({"now_ms":target,"offset_ms":pulse}).to_string(),
                )
                .and_then(|()| std::fs::rename(&temp, state.join("test-clock")))
                .map_err(|error| HarnessError::new(error.to_string()))?;
                // Toggling the clock control's offset_ms wakes another deletion pass after
                // an expiring actor releases its lease. now_ms fixes the daemon's actual time
                // at the requested target, so these notifications do not advance it further.
                next_pulse = Instant::now() + Duration::from_secs(1);
            }
            if log_count(&logs, "retention sweep complete") > before
                && (!due || deletion_finished(&state, &key)?)
            {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(HarnessError::new(
                    "retention clock did not complete due daemon deletion",
                ));
            }
            sleep(Duration::from_millis(25)).await;
        }
    }
}

pub(super) fn deletion_finished(
    state: &Path,
    key: &broca_protocol::BindIdentity,
) -> Result<bool, HarnessError> {
    let wal = wal_store(state);
    if wal.ephemeral_wal_path(key).exists()
        || wal
            .ephemeral_wal_path(key)
            .with_extension("session")
            .exists()
        || wal
            .ephemeral_wal_path(key)
            .with_extension("session.tmp")
            .exists()
    {
        return Ok(false);
    }
    let address = broca_core::session_addr(key);
    if deleting_files(state).iter().any(|path| {
        path.file_name()
            .is_some_and(|name| name.to_string_lossy().starts_with(&address))
    }) {
        return Ok(false);
    }
    let mut latest = None;
    for entry in std::fs::read_dir(wal.tombstone_path(key, ""))
        .into_iter()
        .flatten()
    {
        let entry = entry.map_err(|error| HarnessError::new(error.to_string()))?;
        if entry.path().extension().is_some() {
            continue;
        }
        let stone: Value = serde_json::from_slice(
            &std::fs::read(entry.path()).map_err(|error| HarnessError::new(error.to_string()))?,
        )
        .map_err(|error| HarnessError::new(error.to_string()))?;
        if latest
            .as_ref()
            .is_none_or(|old: &Value| old["seq"].as_u64() < stone["seq"].as_u64())
        {
            latest = Some(stone);
        }
    }
    let Some(stone) = latest.filter(|stone| stone["completed_at_ms"].is_u64()) else {
        return Ok(false);
    };
    let conn = rusqlite::Connection::open(state.join("run-index.db"))
        .map_err(|e| HarnessError::new(e.to_string()))?;
    let invalid: u64 = conn.query_row(
        "SELECT count(*) FROM run_index WHERE lineage_id=?1 AND (expired_at_ms IS NULL OR title IS NOT NULL OR state IN ('active','paused'))",
        [stone["lineage_id"].as_str().unwrap_or("")], |row| row.get(0),
    ).map_err(|e| HarnessError::new(e.to_string()))?;
    let store_key = format!(
        "{}\u{1f}{}\u{1f}{}",
        key.project_root.display(),
        key.harness,
        key.session
    );
    let materialized: bool = conn
        .query_row(
            "SELECT EXISTS(SELECT 1 FROM meta WHERE session=?1)",
            [store_key],
            |row| row.get(0),
        )
        .map_err(|e| HarnessError::new(e.to_string()))?;
    Ok(invalid == 0 && !materialized)
}

/// Retained first sends reach real admission, freeze the policy, and use only ephemeral WALs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ck_broca_writer_admits_retention_and_records_policy() {
    if !rigs_present("retention writer admission") {
        return;
    }
    let scratch = scratch("writer-admission");
    let subject = BrocaSubject::new(&scratch).await;
    let session = "writer-admission";
    subject
        .install_script(
            session,
            Script {
                turns: vec![turn(vec![ScriptedPart::Text("retained answer".into())])],
            },
        )
        .await
        .unwrap();
    let live = subject.start(&scratch.join("daemon")).await.unwrap();
    let route = subject
        .session_route(&live, session, &subject.owner_stamp())
        .await
        .unwrap();
    let mut params = Value::Object(subject.send_fields(session, true));
    params["prompt"] = json!("retained prompt");
    params["send_id"] = json!("retained-first");
    params["retention"] = json!(3600);
    let sent = response(
        "session.send",
        route.request("session.send", params).await.unwrap(),
    );
    let run = sent["run_id"].as_str().unwrap();
    assert_eq!(wait_run_end(&route, run).await["state"], "completed");
    let key = subject.bind_identity(session);
    let store = wal_store(&live.state_root);
    assert!(!store.wal_path(&key).exists());
    assert!(store.ephemeral_wal_path(&key).exists());
    assert!(ephemeral_records(&live.state_root, &key)
        .iter()
        .any(|record| matches!(
            record,
            broca_wal::WalRecord::RunStarted {
                retention: Some(3600),
                ..
            }
        )));
    drop(route);
    drop(live);
    broca_test_support::remove_rig_dir(scratch);
}

/// Run the independently built v0.3.193 reader on WALs admitted by this writer, including
/// the `retained_session_too_large` provider_code string and content-free deletion tombstones.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn released_reader_reads_retained_writer_sessions_before_and_after_deletion() {
    if !rigs_present("retention rollback") {
        return;
    }
    let Some(reader) = std::env::var_os("BROCA_RETENTION_READER_BIN").map(PathBuf::from) else {
        assert!(
            std::env::var_os("BROCA_REQUIRE_RETENTION_READER").is_none(),
            "released retention reader is required"
        );
        println!("SKIP retention rollback: BROCA_RETENTION_READER_BIN is not set");
        return;
    };
    let version = std::process::Command::new(&reader)
        .arg("--version")
        .env_clear()
        .output()
        .unwrap();
    assert!(version.status.success());
    assert_eq!(
        String::from_utf8(version.stdout).unwrap().trim(),
        "ck-broca 0.3.193"
    );
    let scratch = scratch("rollback");
    // Read the writer through its ckdev- copy, the same bytes the subject launches; the guard
    // refuses any bare production-named binary path in test code.
    assert_ne!(
        std::fs::read(&reader).unwrap(),
        std::fs::read(broca_test_support::ckdev_binary(
            env!("CARGO_BIN_EXE_ck-broca"),
            &scratch
        ))
        .unwrap(),
        "the reader must not be a renamed writer"
    );
    let mut subject = BrocaSubject::new(&scratch).await;
    let root = scratch.join("daemon");
    let state = root.join("broca-state");
    std::fs::create_dir_all(&state).unwrap();
    std::fs::write(state.join("test-retained-wal-cap"), "32768").unwrap();
    let marker = unique_marker("rollback");
    let mut runs = Vec::new();
    let live = subject.start(&root).await.unwrap();
    for (session, text, expected_state) in [
        (
            "rollback-retained",
            "retained answer".to_owned(),
            "completed",
        ),
        ("rollback-size-error", "x".repeat(131072), "error"),
    ] {
        subject
            .install_script(
                session,
                Script {
                    turns: vec![turn(vec![ScriptedPart::Text(text)])],
                },
            )
            .await
            .unwrap();
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let mut params = Value::Object(subject.send_fields(session, true));
        params["retention"] = json!(RETENTION_S);
        params["prompt"] = json!(format!("retained prompt {marker}"));
        params["send_id"] = json!("rollback-first");
        let sent = response(
            "session.send",
            route.request("session.send", params).await.unwrap(),
        );
        let run = sent["run_id"].as_str().unwrap().to_owned();
        assert_eq!(wait_run_end(&route, &run).await["state"], expected_state);
        let page = response(
            "session.read",
            route.request("session.read", json!({})).await.unwrap(),
        );
        assert!(page.to_string().contains(&marker));
        let expiry =
            expiry_from_records(&ephemeral_records(&state, &subject.bind_identity(session)))
                .unwrap();
        runs.push((session, run, expected_state, page, expiry));
    }
    drop(live);
    subject.binary = Some(reader);
    let live = subject.start(&root).await.unwrap();
    for (session, run, expected_state, page, _) in &runs {
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        assert_eq!(
            response(
                "session.read",
                route.request("session.read", json!({})).await.unwrap()
            ),
            *page
        );
        let status = response(
            "run.status",
            route
                .request("run.status", json!({"run_id":run}))
                .await
                .unwrap(),
        );
        assert_eq!(status["state"], *expected_state);
        if *expected_state == "error" {
            assert_eq!(
                status["error"]["provider_code"],
                "retained_session_too_large"
            );
        }
    }
    subject
        .advance_retention_clock(runs[0].0, RETENTION_S * 1000 + 1000)
        .await
        .unwrap();
    for (session, run, _, _, expiry) in &runs {
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let key = subject.bind_identity(session);
        let lineage = run_row(&state, run).unwrap().3.unwrap();
        wait_deletion_completed(&state, &wal_store(&state).tombstone_path(&key, &lineage)).await;
        assert!(subject
            .retention_deletion_finished(&live, session)
            .await
            .unwrap());
        address_calls_answer(&route, run, *expiry).await;
    }
    drop(live);
    let live = subject.start(&root).await.unwrap();
    for (session, run, _, _, _) in &runs {
        let route = subject
            .session_route(&live, session, &subject.owner_stamp())
            .await
            .unwrap();
        let key = subject.bind_identity(session);
        let stones = wal_store(&state).tombstone_path(&key, "");
        let stone: Value = std::fs::read_dir(stones)
            .unwrap()
            .map(|entry| {
                serde_json::from_slice::<Value>(&std::fs::read(entry.unwrap().path()).unwrap())
                    .unwrap()
            })
            .next()
            .unwrap();
        assert!(stone["completed_at_ms"].as_u64().is_some());
        address_calls_answer(&route, run, stone["expired_at_ms"].as_u64().unwrap()).await;
    }
    drop(live);
    assert_eq!(
        files_holding(
            &marker,
            &[
                state.as_path(),
                root.join("data-home/cortexkit/broca/logs").as_path()
            ]
        ),
        Vec::<PathBuf>::new()
    );
    broca_test_support::remove_rig_dir(scratch);
}
