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
         ses-tools\t3\t590\tTool fixture\t/tmp/scrubbed-project\n\
         ses-text\t2\t177\tText fixture\t/tmp/text-project\n"
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
fn unreadable_mc_database_warns_and_continues_lineage_setup() {
    let missing = broca_test_support::scratch_root()
        .join(format!("ck-import-missing-mc-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&missing);
    let output = broca_test_support::retry_executable_file_busy(|| {
        command()
            .args(["opencode", "ses-text", "--json", "--source-db"])
            .arg(fixture())
            .arg("--mc-state-db")
            .arg(&missing)
            .output()
    })
    .unwrap();
    assert!(!output.status.success());
    let events: Vec<serde_json::Value> = String::from_utf8(output.stdout)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert!(events
        .iter()
        .any(|event| { event["event"] == "warning" && event["kind"] == "mc_state_unavailable" }));
    assert!(events
        .iter()
        .any(|event| { event["action"] == "mc_state" && event["status"] == "unavailable" }));
    assert!(events.last().unwrap()["message"]
        .as_str()
        .unwrap()
        .contains("--subc"));
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
