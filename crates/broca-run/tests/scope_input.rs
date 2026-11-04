use std::{path::Path, process::Stdio};
use tokio::{io::AsyncWriteExt, process::Command};

fn output_text(output: &std::process::Output) -> String {
    format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    )
}

fn scratch(label: &str) -> std::path::PathBuf {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let root = broca_test_support::scratch_root().join(format!(
        "{label}-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn command(root: &Path, scoped: bool) -> Command {
    let mut command = broca_test_support::tokio_rig_command(
        Path::new(env!("CARGO_BIN_EXE_broca-run")),
        root,
        "scope-input",
    )
    .unwrap();
    command
        .args([
            "--model",
            "mock",
            "--prompt",
            "test",
            "--base-url",
            "http://127.0.0.1:1",
        ])
        .arg("--subc")
        .arg(root.join("does-not-exist.json"))
        .env("BROCA_STATE_ROOT", root.join("state"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if scoped {
        command.args(["--scope", "rig/run/1", "--preset", "worker"]);
    }
    command
}

async fn run(input: &[u8]) -> std::process::Output {
    let root = scratch("scope-input");
    let mut command = command(&root, true);
    let mut child = broca_test_support::retry_executable_file_busy(|| command.spawn()).unwrap();
    {
        let mut stdin = child.stdin.take().unwrap();
        stdin.write_all(input).await.unwrap();
    }
    broca_test_support::wait_for("scope input refusal", child.wait_with_output(), || {
        "the CLI did not exit".into()
    })
    .await
    .unwrap()
}

#[tokio::test]
async fn scope_without_proof_is_refused_before_connecting() {
    let output = run(b"").await;
    assert!(!output.status.success());
    let text = output_text(&output);
    assert!(text.contains("scope_bind_proof_required"));
    assert!(!text.contains("connect subc"));
}

#[tokio::test]
async fn malformed_proof_is_named_without_echo_and_before_connecting() {
    let input = "secret-invalid-proof-input";
    let output = run(input.as_bytes()).await;
    assert!(!output.status.success());
    let text = output_text(&output);
    assert!(text.contains("scope_bind_proof_invalid"));
    assert!(!text.contains(input));
    assert!(!text.contains("connect subc"));
}

#[tokio::test]
async fn valid_private_proof_is_not_echoed_on_connection_failure() {
    use base64::Engine;
    let input = "11".repeat(32);
    let output = run(input.as_bytes()).await;
    let text = output_text(&output);
    assert!(text.contains("connect subc"));
    assert!(!text.contains(&input));
    assert!(!text.contains(&base64::engine::general_purpose::URL_SAFE_NO_PAD.encode([0x11u8; 32])));
}

#[tokio::test]
async fn no_scope_does_not_read_stdin() {
    let root = scratch("unscoped-input");
    let mut command = command(&root, false);
    let mut child = broca_test_support::retry_executable_file_busy(|| command.spawn()).unwrap();
    let held_stdin = child.stdin.take().unwrap();
    let output = broca_test_support::wait_for(
        "unscoped CLI leaves stdin alone",
        child.wait_with_output(),
        || "the CLI waited for stdin without --scope".into(),
    )
    .await
    .unwrap();
    drop(held_stdin);
    assert!(output_text(&output).contains("connect subc"));
}
