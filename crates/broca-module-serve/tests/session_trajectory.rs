use std::path::{Path, PathBuf};
use std::process::Command;

use broca_core::{agent_session_ref, session_addr, FileWalStore, StorePageSnapshot, WalStore};
use broca_module_serve::{session_lookup, trajectory};
use broca_protocol::BindIdentity;
use broca_wal::WalRecord;
use serde_json::{json, Value};

fn decode(value: Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

fn user(text: &str) -> Value {
    json!({"role":"user", "content":[{"type":"text", "text":text}]})
}

fn started(bind: &BindIdentity, run: &str, at: Option<u64>, text: &str) -> Value {
    json!({"type":"run_started", "run_id":run, "session":bind, "ts_ms":at,
        "written_at_ms":at, "send_id":format!("send-{run}"), "config":{
            "model":{"provider_module_id":"fixture", "model_id":"model"},
            "variant":"high", "tools":[], "tool_choice":{"type":"auto"},
            "generation":{}, "response_format":{"type":"text"},
            "context_epoch":0, "renderer_version":1
        }, "input":[user(text)]})
}

fn fixture(root: &Path, bind: &BindIdentity, records: &[WalRecord]) -> FileWalStore {
    std::fs::create_dir_all(root).unwrap();
    drop(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    let store = FileWalStore::new(root.join("wal"));
    let mut writer = store.open(bind).unwrap();
    for record in records {
        writer.append(1, record, true).unwrap();
    }
    drop(writer);
    inventory(root, bind);
    store
}

fn inventory(root: &Path, bind: &BindIdentity) {
    let conn = rusqlite::Connection::open(root.join("run-index.db")).unwrap();
    conn.execute(
        "INSERT OR REPLACE INTO agent_data_session_ref VALUES(?1,?2,?3,?4,?5)",
        rusqlite::params![
            agent_session_ref(bind),
            session_addr(bind),
            bind.project_root.to_str().unwrap(),
            bind.harness,
            bind.session
        ],
    )
    .unwrap();
    conn.execute_batch(
        "INSERT OR REPLACE INTO agent_data_projection VALUES(1,1);
        INSERT OR REPLACE INTO agent_data_census_done VALUES(1);
        DELETE FROM agent_data_pending_session;
        UPDATE agent_data_census SET covered=1;",
    )
    .unwrap();
}

fn root(tag: &str) -> PathBuf {
    broca_test_support::scratch_root().join(format!("session-trajectory-{tag}"))
}

#[test]
fn session_trajectory_lookup_names_ambiguity_and_incomplete_inventory() {
    let root = root("lookup");
    let bind = BindIdentity::new(root.join("worker"), "broca", "alfonso:bg_lookup");
    fixture(
        &root,
        &bind,
        &[decode(started(&bind, "run", Some(1), "hi"))],
    );
    // A bare-name lookup must not depend on any run-index row or its JSON column.
    let conn = rusqlite::Connection::open(root.join("run-index.db")).unwrap();
    conn.execute_batch("DROP TABLE run_index").unwrap();
    assert_eq!(session_lookup::lookup(&root, "bg_lookup").unwrap(), bind);
    for positional in [false, true] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_broca-session"));
        command
            .arg("--subc")
            .arg(root.join("connection-must-not-be-printed.json"))
            .arg("--state-root")
            .arg(&root);
        if positional {
            command.args(["read", "bg_lookup"]);
        } else {
            command.args(["--session", "bg_lookup", "read"]);
        }
        let output = command.output().unwrap();
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.contains("cannot connect subc"), "{output}");
        assert!(
            !output.contains("connection-must-not-be-printed"),
            "{output}"
        );
    }
    for (project, harness, id, expected) in [
        (
            bind.project_root.clone(),
            None,
            "bg_lookup",
            "cannot connect subc",
        ),
        (
            root.join("wrong-project"),
            None,
            "bg_lookup",
            "does not match --project-root",
        ),
        (
            root.join("literal-project"),
            Some("broca"),
            "bg_not_recorded",
            "cannot connect subc",
        ),
    ] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_broca-session"));
        command
            .arg("--subc")
            .arg(root.join("connection-must-not-be-printed.json"))
            .arg("--state-root")
            .arg(&root)
            .arg("--project-root")
            .arg(project);
        if let Some(harness) = harness {
            command.args(["--harness", harness]);
        }
        let output = command.args(["read", id]).output().unwrap();
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.contains(expected), "{output}");
        assert!(!output.contains("connection-must-not-be-printed"));
    }
    let cache_output = root.join("cache-output");
    let output = Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args(["cache-dump", "--session", "bg_unknown", "--state-root"])
        .arg(&root)
        .arg("--out")
        .arg(&cache_output)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("alfonso:bg_unknown"));
    assert!(!cache_output.exists());
    assert_eq!(
        session_lookup::lookup(&root, "alfonso:bg_lookup").unwrap(),
        bind
    );
    let missing = session_lookup::lookup(&root, "bg_unknown").unwrap_err();
    assert!(missing.contains("unknown session"), "{missing}");
    assert!(missing.contains("bg_unknown") && missing.contains("alfonso:bg_unknown"));
    let exact = BindIdentity::new(root.join("exact"), "runner", "bg_lookup");
    inventory(&root, &exact);
    assert_eq!(session_lookup::lookup(&root, "bg_lookup").unwrap(), exact);
    let other = BindIdentity::new(root.join("other"), "runner", "alfonso:bg_lookup");
    inventory(&root, &other);
    let error = session_lookup::lookup(&root, "alfonso:bg_lookup").unwrap_err();
    assert!(
        error.contains("ambiguous")
            && error.contains("--project-root")
            && error.contains("--harness")
    );
    assert!(
        error.contains("worker") && error.contains("other"),
        "{error}"
    );
    conn.execute_batch("DELETE FROM agent_data_census_done")
        .unwrap();
    let error = session_lookup::lookup(&root, "bg_missing").unwrap_err();
    assert!(
        error.contains("not found in an incomplete session-ref inventory"),
        "{error}"
    );
    assert!(!error.contains("unknown session"));
    let error = session_lookup::lookup(&root, "bg_lookup").unwrap_err();
    assert!(error.contains("cannot establish uniqueness"), "{error}");
    drop(conn);
    std::fs::remove_dir_all(root).unwrap();
}

fn records(bind: &BindIdentity) -> Vec<WalRecord> {
    let mut second = started(bind, "run-2", Some(2500), "queued");
    second["submission_id"] = json!("queued-1");
    vec![
        started(bind, "run-1", Some(1000), "hello"),
        json!({"type":"step_started", "step_id":1}),
        json!({"type":"queued", "submission_id":"queued-1", "written_at_ms":1100,
            "request":{"model":{"provider_module_id":"fixture", "model_id":"model"},
                "input":[user("queued")], "policy_allowed":[], "tool_choice":{"type":"auto"},
                "generation":{}, "stop_when":[]}}),
        json!({"type":"model_attempt_finished", "step_id":1, "attempt":0,
            "request_body_len":10, "request_body_sha256":"a", "response_body_len":0,
            "response_body_sha256":"b", "outcome":"http_error", "http_status":503,
            "sent_at_ms":1050, "ts_ms":1200, "transport":"https"}),
        json!({"type":"model_attempt_finished", "step_id":1, "attempt":1,
            "request_body_len":10, "request_body_sha256":"a", "response_body_len":20,
            "response_body_sha256":"c", "outcome":"complete", "http_status":200,
            "sent_at_ms":1250, "first_content_at_ms":1280, "ts_ms":1350, "transport":"https"}),
        json!({"type":"model_step_finished", "step_id":1, "written_at_ms":1350,
            "assistant_message":{"message_id":"assistant-1", "content":[
                {"type":"reasoning", "text":"private reasoning", "signature":"never export"},
                {"type":"text", "text":"checking"},
                {"type":"tool_call", "tool_call_id":"call-1", "tool_name":"bash",
                    "input":{"command":"exit 7", "runon":"linux", "background":false,
                        "timeout":1200, "wait":true}}
            ]}, "usage":{"input_tokens":11,"cached_input_tokens":3,"output_tokens":5},
            "finish_reason":"tool_calls", "retries_used":1}),
        json!({"type":"tool_batch_started", "batch_id":"batch-1", "step_id":1, "planned_calls":["call-1"]}),
        json!({"type":"tool_dispatch_intent", "batch_id":"batch-1", "tool_call_id":"call-1",
            "tool_name":"bash", "module_id":"aft", "dispatched_at_ms":1400,
            "args":{"command":"exit 7", "runon":"linux", "background":false, "timeout":1200, "wait":true}}),
        json!({"type":"steered", "written_at_ms":1450, "input":[user("steer")]}),
        json!({"type":"tool_result", "batch_id":"batch-1", "finished_at_ms":1500,
            "result":{"tool_call_id":"call-1", "output":{"kind":"text", "text":"failed\n[exit code: 7]\n"}, "is_error":true},
            "outcome":"completed"}),
        json!({"type":"tool_batch_finished", "batch_id":"batch-1"}),
        json!({"type":"turn_finished", "step_id":1}),
        json!({"type":"run_paused", "run_id":"run-1", "reason":"restart", "ts_ms":1600}),
        json!({"type":"run_finished", "reason":"interrupted", "usage":{}, "ts_ms":1800}),
        second,
        json!({"type":"step_started", "step_id":2}),
        json!({"type":"model_step_finished", "step_id":2, "written_at_ms":2700,
            "assistant_message":{"message_id":"assistant-2", "content":[{"type":"text", "text":"done"}]},
            "usage":{}, "finish_reason":"stop"}),
        json!({"type":"turn_finished", "step_id":2}),
        json!({"type":"run_finished", "reason":"completed", "usage":{}, "ts_ms":2800}),
    ].into_iter().map(decode).collect()
}

#[test]
fn session_trajectory_events_transcript_and_archive_are_read_only() {
    let root = root("events");
    let bind = BindIdentity::new(root.join("worker"), "broca", "alfonso:bg_trajectory");
    let store = fixture(&root, &bind, &records(&bind));
    let frames = store.read_records(&bind).unwrap();
    let live = store.resolve_live_path(&bind).unwrap().unwrap();
    let original = std::fs::read(&live).unwrap();
    let rows = trajectory::read(&root, &bind, false).unwrap();
    let kinds: Vec<_> = rows
        .iter()
        .map(|row| row["kind"].as_str().unwrap())
        .collect();
    assert_eq!(
        kinds,
        [
            "run",
            "model_request",
            "model_request",
            "model_step",
            "tool_call",
            "run_end",
            "run_end",
            "gap",
            "run",
            "model_step",
            "run_end"
        ]
    );
    assert_eq!(rows[0]["model"], "fixture/model");
    assert_eq!(rows[0]["variant"], "high");
    assert_eq!(rows[0]["send_id"], "send-run-1");
    assert_eq!(rows[0]["started_ms"], 1000);
    assert_eq!(rows[1]["attempt"], 0);
    assert_eq!(rows[1]["http_status"], 503);
    assert_eq!(rows[1]["transport"], "https");
    assert!(rows[1]["first_content_at_ms"].is_null());
    assert_eq!(rows[2]["attempt"], 1);
    assert_eq!(rows[2]["first_content_at_ms"], 1280);
    assert_eq!(rows[2]["ended_at_ms"], 1350);
    assert_eq!(rows[3]["input_tokens"], 11);
    assert_eq!(rows[3]["cached_input_tokens"], 3);
    assert_eq!(rows[3]["output_tokens"], 5);
    let call = &rows[4];
    assert_eq!(call["step_id"], 1);
    assert_eq!(call["tool_call_id"], "call-1");
    assert_eq!(call["module_id"], "aft");
    assert_eq!(call["dispatched"], true);
    assert_eq!(call["dispatched_at_ms"], 1400);
    assert_eq!(call["result_at_ms"], 1500);
    assert_eq!(call["is_error"], true);
    assert_eq!(call["result_bytes"], "failed\n[exit code: 7]\n".len());
    assert_eq!(call["outcome"], "completed");
    assert_eq!(call["command"], "exit 7");
    assert_eq!(call["runon"], "linux");
    assert_eq!(call["background"], false);
    assert_eq!(call["timeout_ms"], 1200);
    assert_eq!(call["wait"], true);
    assert_eq!(call["exit_code"], 7);
    let identity = store
        .read_records_range(&bind, 0, None, None)
        .unwrap()
        .lineage_id
        .unwrap();
    assert_eq!(call["call_key"], format!("{identity}:{}", frames[7].seq));
    assert_eq!(rows[5]["end"], "paused:restart");
    assert_eq!(rows[6]["end"], "finished:interrupted");
    assert_eq!(
        rows[7],
        json!({"kind":"gap", "from_ms":1800, "to_ms":2500,
        "duration_ms":700, "previous_run_id":"run-1", "previous_end":"finished:interrupted"})
    );
    assert!(rows[9]["input_tokens"].is_null());
    assert!(rows[9]["output_tokens"].is_null());
    let transcript = trajectory::read(&root, &bind, true).unwrap();
    let messages: Vec<_> = transcript
        .iter()
        .filter(|row| row["kind"] == "message")
        .collect();
    let page =
        broca_core::read_session_page(&StorePageSnapshot::empty(), &frames, Some(0), Some(500))
            .unwrap();
    assert_eq!(messages.len(), page.messages.len());
    for expected in page.messages {
        let actual = messages
            .iter()
            .find(|row| row["ordinal"] == expected.ordinal)
            .unwrap();
        assert_eq!(actual["mid"], format!("m{}", expected.ordinal));
        assert_eq!(
            actual["role"],
            serde_json::to_value(expected.message.role).unwrap()
        );
        assert_eq!(actual["at_ms"], json!(expected.written_at));
        let expected_content: Vec<_> = expected
            .message
            .content
            .iter()
            .filter(|block| {
                !matches!(
                    block,
                    broca_protocol::ContentBlock::Reasoning { .. }
                        | broca_protocol::ContentBlock::RedactedReasoning { .. }
                )
            })
            .map(|block| serde_json::to_value(block).unwrap())
            .collect();
        assert_eq!(actual["content"], json!(expected_content));
    }
    let sources: Vec<_> = messages
        .iter()
        .map(|row| row["source"].as_str().unwrap())
        .collect();
    assert_eq!(
        sources,
        ["user", "queued", "assistant", "steer", "tool", "assistant"]
    );
    let reasoning: Vec<_> = transcript
        .iter()
        .filter(|row| row["kind"] == "reasoning")
        .collect();
    assert_eq!(reasoning.len(), 2);
    assert_eq!(reasoning[0]["bytes"], "private reasoning".len());
    let encoded = serde_json::to_string(&transcript).unwrap();
    assert!(!encoded.contains("private reasoning") && !encoded.contains("never export"));
    assert!(encoded.contains("failed\\n[exit code: 7]\\n"));
    assert!(encoded.contains("exit 7"));
    assert_eq!(std::fs::read(&live).unwrap(), original);
    let output = Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args(["trajectory", "bg_trajectory", "--state-root"])
        .arg(&root)
        .arg("--out")
        .arg(&live)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("existing files are never overwritten")
    );
    assert_eq!(std::fs::read(&live).unwrap(), original);
    std::fs::create_dir_all(root.join("wal-archive")).unwrap();
    let mut archive =
        broca_wal::archive::ArchiveWriter::create(root.join("wal-archive/fold-1.ark")).unwrap();
    archive = archive.without_feature_floor();
    archive.append(&session_addr(&bind), &original).unwrap();
    archive.seal().unwrap();
    std::fs::remove_file(&live).unwrap();
    assert_eq!(trajectory::read(&root, &bind, true).unwrap(), transcript);
    let out = root.join("trajectory.jsonl");
    let output = Command::new(env!("CARGO_BIN_EXE_broca-session"))
        .args([
            "trajectory",
            "bg_trajectory",
            "--transcript",
            "--state-root",
        ])
        .arg(&root)
        .arg("--out")
        .arg(&out)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    let exported: Vec<Value> = std::fs::read_to_string(out)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(exported, transcript);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(root.join("trajectory.jsonl"))
                .unwrap()
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
    }
    assert!(!live.exists(), "archive reads must not extract a live WAL");
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn session_trajectory_recorded_key_absent_times_and_no_exit_code() {
    let root = root("absent");
    let bind = BindIdentity::new(root.join("worker"), "broca", "alfonso:bg_dispatch_test");
    let mut data = records(&bind);
    if let WalRecord::RunStarted {
        ts_ms,
        written_at_ms,
        ..
    } = &mut data[0]
    {
        *ts_ms = None;
        *written_at_ms = None;
    }
    if let WalRecord::ToolDispatchIntent {
        call_key,
        dispatched_at_ms,
        module_id,
        ..
    } = &mut data[7]
    {
        *call_key = Some("saved-approval-key".into());
        *dispatched_at_ms = None;
        *module_id = None;
    }
    if let WalRecord::ToolResult {
        result,
        finished_at_ms,
        ..
    } = &mut data[9]
    {
        let result = std::sync::Arc::make_mut(result);
        result.output = broca_protocol::content::ToolOutput::Text {
            text: "no exit marker".into(),
        };
        *finished_at_ms = None;
    }
    fixture(&root, &bind, &data);
    assert_eq!(
        session_lookup::lookup(&root, "bg_dispatch_test").unwrap(),
        bind
    );
    let rows = trajectory::read(&root, &bind, false).unwrap();
    assert!(rows[0]["started_ms"].is_null());
    let call = rows.iter().find(|row| row["kind"] == "tool_call").unwrap();
    assert_eq!(call["call_key"], "saved-approval-key");
    assert!(call["dispatched_at_ms"].is_null() && call["result_at_ms"].is_null());
    assert!(call["module_id"].is_null());
    assert!(call.get("exit_code").is_none());
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn session_trajectory_pending_inputs_cache_lookup_and_torn_tail() {
    use std::io::Write;
    let root = root("pending");
    let bind = BindIdentity::new(root.join("worker"), "broca", "alfonso:bg_pending");
    let all = records(&bind);
    let pending = vec![
        all[0].clone(),
        all[1].clone(),
        all[2].clone(),
        all[8].clone(),
    ];
    let store = fixture(&root, &bind, &pending);
    let live = store.resolve_live_path(&bind).unwrap().unwrap();
    std::fs::OpenOptions::new()
        .append(true)
        .open(&live)
        .unwrap()
        .write_all(&[0, 1])
        .unwrap();
    let before = std::fs::read(&live).unwrap();
    let rows = trajectory::read(&root, &bind, true).unwrap();
    let messages: Vec<_> = rows.iter().filter(|row| row["kind"] == "message").collect();
    assert_eq!(messages.len(), 3);
    assert_eq!(messages[0]["ordinal"], 0);
    assert_eq!(messages[1]["source"], "queued");
    assert_eq!(messages[1]["at_ms"], 1100);
    assert!(messages[1]["ordinal"].is_null() && messages[1]["mid"].is_null());
    assert_eq!(messages[2]["source"], "steer");
    assert_eq!(messages[2]["at_ms"], 1450);
    assert!(messages[2]["ordinal"].is_null() && messages[2]["mid"].is_null());
    let summary = broca_module_serve::cache_dump::export(
        &root,
        &["bg_pending".into()],
        None,
        &root.join("cache"),
    )
    .unwrap();
    assert_eq!(summary.sessions_read, 1);
    assert_eq!(summary.calls_examined, 0);
    assert_eq!(
        std::fs::read(&live).unwrap(),
        before,
        "inspection must not repair a torn tail"
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn session_trajectory_no_dispatch_preserves_unknown_result_metadata() {
    let root = root("no-dispatch");
    let bind = BindIdentity::new(root.join("worker"), "broca", "alfonso:bg_denied");
    let mut data = records(&bind);
    data[7] = decode(json!({"type":"tool_no_dispatch", "batch_id":"batch-1",
        "tool_call_id":"call-1", "dispatched_at_ms":1400,
        "reason":{"kind":"denied", "reason":"not permitted"}}));
    data.remove(9);
    let store = fixture(&root, &bind, &data);
    let rows = trajectory::read(&root, &bind, true).unwrap();
    let call = rows.iter().find(|row| row["kind"] == "tool_call").unwrap();
    assert_eq!(call["tool_name"], "bash");
    assert_eq!(call["dispatched"], false);
    assert_eq!(call["outcome"], "denied");
    assert!(call["module_id"].is_null() && call["result_at_ms"].is_null());
    assert!(call.get("exit_code").is_none() && call.get("result_bytes").is_none());
    let frames = store.read_records(&bind).unwrap();
    let page =
        broca_core::read_session_page(&StorePageSnapshot::empty(), &frames, Some(0), Some(500))
            .unwrap();
    let tool = rows
        .iter()
        .find(|row| row["kind"] == "message" && row["role"] == "tool")
        .unwrap();
    let expected = page
        .messages
        .iter()
        .find(|message| message.message.role == broca_protocol::content::Role::Tool)
        .unwrap();
    assert_eq!(tool["ordinal"], expected.ordinal);
    assert!(tool["content"].to_string().contains("not permitted"));
    std::fs::remove_dir_all(root).unwrap();
}

#[test]
fn session_trajectory_open_tool_turn_coordinates_follow_session_read() {
    for count in [6, 8, 10] {
        let root = root(&format!("open-turn-{count}"));
        let bind = BindIdentity::new(root.join("worker"), "broca", "alfonso:bg_open");
        let all = records(&bind);
        let store = fixture(&root, &bind, &all[..count]);
        let frames = store.read_records(&bind).unwrap();
        let page =
            broca_core::read_session_page(&StorePageSnapshot::empty(), &frames, Some(0), Some(500))
                .unwrap();
        let rows = trajectory::read(&root, &bind, true).unwrap();
        for row in rows.iter().filter(|row| {
            row["kind"] == "message" && (row["role"] == "assistant" || row["role"] == "tool")
        }) {
            let expected = page
                .messages
                .iter()
                .find(|message| serde_json::to_value(message.message.role).unwrap() == row["role"]);
            assert_eq!(
                row["ordinal"],
                json!(expected.map(|message| message.ordinal))
            );
            assert_eq!(
                row["mid"],
                json!(expected.map(|message| format!("m{}", message.ordinal)))
            );
            assert_eq!(
                row["at_ms"],
                if row["role"] == "assistant" {
                    1350
                } else {
                    1500
                }
            );
        }
        if count == 6 {
            assert_eq!(page.prompt_len, 1);
        } else {
            assert!(page.prompt_len >= 2);
        }
        std::fs::remove_dir_all(root).unwrap();
    }
}
