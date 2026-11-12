use std::path::PathBuf;
use std::process::Command;

fn fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/opencode.db")
}

fn claude_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/claude-code")
}

fn codex_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../ck-session-import/tests/fixtures/codex/sessions")
}

fn goose_fixture() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../ck-session-import/tests/fixtures/goose.db")
}

fn command() -> Command {
    broca_test_support::ckdev_command(
        env!("CARGO_BIN_EXE_ck-import"),
        broca_test_support::scratch_root(),
    )
}

#[test]
fn opencode1_extract_prints_a_single_source_descriptor_without_a_daemon() {
    let dir = broca_test_support::scratch_root()
        .join(format!("cli-opencode1-extract-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["opencode1-extract", "--session", "ses-text", "--db"])
            .arg(fixture())
            .arg("--out-dir")
            .arg(&dir)
            .output()
    })
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let descriptor: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(descriptor["messages"], 2);
    assert!(descriptor["newest_mid"].is_string());
    let path = std::path::Path::new(descriptor["path"].as_str().unwrap());
    assert_eq!(
        descriptor["sha256"],
        ck_session_import::opencode_extract::sha256_file(path).unwrap()
    );
    assert_eq!(descriptor.as_object().unwrap().len(), 4);
    std::fs::remove_dir_all(dir).unwrap();
}

fn check_fixture(tag: &str) -> (PathBuf, rusqlite::Connection) {
    let dir = broca_test_support::scratch_root()
        .join(format!("cli-opencode1-check-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = rusqlite::Connection::open(dir.join("extract.db")).unwrap();
    db.execute_batch(include_str!(
        "../../../ck-session-import/tests/fixtures/head/opencode1.sql"
    ))
    .unwrap();
    (dir, db)
}

fn check_extract(path: &std::path::Path) -> std::process::Output {
    broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["opencode1-check", "--session", "ses_synthetic", "--extract"])
            .arg(path)
            .output()
    })
    .unwrap()
}

#[test]
fn opencode1_check_reports_only_counts_from_the_import_projection() {
    let (dir, db) = check_fixture("counts");
    let output = check_extract(&dir.join("extract.db"));
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let counts: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        counts,
        serde_json::json!({"ok":true,
        "source_rows_by_role":{"user":3,"assistant":5,"system":1},
        "imported_messages_by_role":{"user":3,"assistant":3,"tool":2,"system":0},
        "tool_calls":2, "tool_results":{"completed":2,"error":0,"interrupted_as_result":0,"open_call_placeholders":0},
        "media_objects":1,"media_occurrences":2,"dropped_system_rows":1,"rows_projected_nothing":3,"finished_summaries":2})
    );
    db.execute_batch("UPDATE part SET data=json_set(data,'$.state.status','running') WHERE id='prt_009';
        UPDATE part SET data=json_set(data,'$.state.status','error','$.state.metadata',json('{\"interrupted\":true,\"output\":\"secret tool content\"}')) WHERE id='prt_007';").unwrap();
    let output = check_extract(&dir.join("extract.db"));
    assert!(output.status.success());
    assert!(output.stderr.is_empty());
    let counts: serde_json::Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        counts["tool_results"],
        serde_json::json!({"completed":0,"error":0,"interrupted_as_result":1,"open_call_placeholders":1})
    );
    for secret in [
        "msg_",
        "ses_synthetic",
        "secret tool content",
        "Synthetic",
        "iVBOR",
    ] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
    }
    drop(db);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn opencode1_check_refusals_are_code_and_ordinal_only_and_admit_real_media() {
    let (dir, db) = check_fixture("refusals");
    db.execute("UPDATE part SET message_id='msg_009',data=json_set(data,'$.state.status','pending','$.callID','secret-call-id') WHERE id='prt_009'", []).unwrap();
    let output = check_extract(&dir.join("extract.db"));
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({"ok":false,"refusal":{"code":"import_open_call","message_ordinal":9}})
    );
    // The CLI must use the byte validator, not a metadata-only accepting test callback.
    db.execute("UPDATE part SET data=json_set(data,'$.url','data:image/png;base64,c2VjcmV0IGNvbnRlbnQ=') WHERE id='prt_004'", []).unwrap();
    let output = check_extract(&dir.join("extract.db"));
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({"ok":false,"refusal":{"code":"import_media_refused","message_ordinal":1}})
    );
    let output = check_extract(&dir.join("absent-sensitive-path.db"));
    assert_eq!(output.status.code(), Some(1));
    assert!(output.stderr.is_empty());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({"ok":false,"refusal":{"code":"import_source_unreadable","message_ordinal":null}})
    );
    drop(db);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn opencode1_check_writes_a_wire_decodable_offline_move_list_and_newest() {
    let (dir, db) = check_fixture("moves");
    let run = |output: &std::path::Path, floor: &str| {
        broca_test_support::retry_executable_file_busy(|| {
            command()
                .args(["opencode1-check", "--session", "ses_synthetic", "--extract"])
                .arg(dir.join("extract.db"))
                .arg("--moves-out")
                .arg(output)
                .args(["--tag-floor", floor])
                .output()
        })
        .unwrap()
    };
    let target = dir.join("moves.json");
    let output = run(&target, "40");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(String::from_utf8_lossy(&output.stderr).lines().count(), 1);
    assert!(String::from_utf8_lossy(&output.stderr).contains("2 added ids are unminted"));
    assert!(String::from_utf8_lossy(&output.stderr).contains("newest mid/digest are exact"));
    broca_owner_only::validate(&target).unwrap();
    let wrapper: serde_json::Value =
        serde_json::from_slice(&std::fs::read(&target).unwrap()).unwrap();
    assert_eq!(wrapper["offline"], true);
    assert_eq!(wrapper["unminted_prefix"], "unminted:");
    assert_eq!(wrapper["moves"]["shape"], "plugin");
    assert_eq!(
        wrapper["moves"]["rows"],
        serde_json::json!([
            {"mid":"msg_001","entry":1,"finished_summary":false},
            {"mid":"msg_002","entry":2,"finished_summary":false},
            {"mid":"unminted:1","entry":3,"finished_summary":false,"added":true},
            {"mid":"msg_003","entry":4,"finished_summary":false},
            {"mid":"msg_004","entry":5,"finished_summary":false},
            {"mid":"unminted:2","entry":6,"finished_summary":false,"added":true},
            {"mid":"msg_005","entry":null,"finished_summary":false},
            {"mid":"msg_006","entry":7,"finished_summary":false},
            {"mid":"msg_007","entry":8,"finished_summary":true},
            {"mid":"msg_008","entry":null,"finished_summary":true},
            {"mid":"msg_009","entry":null,"finished_summary":false}
        ])
    );
    assert_eq!(wrapper["newest"]["ordinal"], 8);
    assert_eq!(wrapper["newest"]["mid"], "msg_007");
    let digest = wrapper["newest"]["digest"].as_str().unwrap();
    assert!(!digest.is_empty() && !digest.starts_with("unminted:"));
    assert!(wrapper["moves"]["rows"]
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row.get("source_entry").is_none()));
    assert!(!String::from_utf8_lossy(&output.stdout).contains("msg_"));
    db.execute_batch("DELETE FROM part WHERE session_id='ses_synthetic' AND message_id>='msg_005'; DELETE FROM message WHERE session_id='ses_synthetic' AND id>='msg_005';").unwrap();
    let output = run(&dir.join("added-newest.json"), "40");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("newest mid/digest are also unminted"));
    let wrapper: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("added-newest.json")).unwrap()).unwrap();
    assert_eq!(
        wrapper["newest"],
        serde_json::json!({"ordinal":6,"mid":"unminted:2","digest":"unminted:2"})
    );
    let bad = dir.join("bad-floor.json");
    let output = run(&bad, "18446744073709551615");
    assert_eq!(output.status.code(), Some(1));
    assert!(!bad.exists());
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).unwrap(),
        serde_json::json!({"ok":false,"refusal":{"code":"import_tag_floor_refused","message_ordinal":null}})
    );
    drop(db);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn list_human_output_snapshot() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["opencode", "--list", "--source-db"])
            .arg(fixture())
            .output()
    })
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert_eq!(
        stderr,
        "ID\tMESSAGES\tSIZE_BYTES\tTITLE\tCWD\n\
         ses-tools\t3\t-\tTool fixture\t/tmp/scrubbed-project\n\
         ses-text\t2\t-\tText fixture\t/tmp/text-project\n"
    );
}

#[test]
fn json_list_is_a_parseable_event_stream() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["opencode", "--list", "--json", "--source-db"])
            .arg(fixture())
            .output()
    })
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.len(), 3);
    assert_eq!(events[0]["event"], "progress");
    assert_eq!(events[0]["action"], "session");
    assert_eq!(events[2]["event"], "done");
}

#[test]
fn claude_code_is_registered_for_listing() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["claude-code", "--list", "--source-db"])
            .arg(claude_fixture())
            .output()
    })
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("11111111-1111-4111-8111-111111111111"));
    assert!(stderr.contains("22222222-2222-4222-8222-222222222222"));
}

#[test]
fn codex_list_uses_registered_reader() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["codex", "--list", "--source-db"])
            .arg(codex_fixture())
            .output()
    })
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(output.stdout.is_empty());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("codex-tools"));
    assert!(stderr.contains("codex-text"));
}

#[test]
fn goose_json_list_uses_registered_reader() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["goose", "--list", "--json", "--source-db"])
            .arg(goose_fixture())
            .output()
    })
    .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events[0]["id"], "goose-tools");
    assert!(events[0]["approximate_size_bytes"].is_null());
    assert_eq!(events.last().unwrap()["event"], "done");
}

#[test]
fn clap_parse_errors_are_json_when_json_is_requested() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command().args(["--json", "--not-a-real-option"]).output()
    })
    .unwrap();
    assert!(!output.status.success());
    assert!(output.stderr.is_empty());
    let event: serde_json::Value =
        serde_json::from_slice(output.stdout.strip_suffix(b"\n").unwrap()).unwrap();
    assert_eq!(event["event"], "error");
    assert!(event["message"]
        .as_str()
        .unwrap()
        .contains("unexpected argument"));
}

#[test]
fn help_remains_human_even_when_json_is_present() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command().args(["--json", "--help"]).output()
    })
    .unwrap();
    assert!(output.status.success());
    assert!(String::from_utf8(output.stdout).unwrap().contains("Usage:"));
}

#[test]
fn unsupported_harness_error_lists_supported_harnesses() {
    let output =
        broca_test_support::retry_executable_file_busy(|| command().arg("unknown").output())
            .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("this build supports: opencode, claude-code, codex, goose, pi"));
}

#[test]
fn client_message_cap_fails_before_connecting_to_subc() {
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["opencode", "ses-text", "--json", "--source-db"])
            .arg(fixture())
            .args(["--message-count-override", "100001"])
            .output()
    })
    .unwrap();
    assert!(!output.status.success());
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(events.last().unwrap()["event"], "error");
    assert!(events.last().unwrap()["message"]
        .as_str()
        .unwrap()
        .contains("session too large to import in one op"));
    assert!(!events.last().unwrap()["message"]
        .as_str()
        .unwrap()
        .contains("--subc"));
}
