#[test]
fn unknown_flags_are_refused_before_collecting_paths() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_session-convert"))
        .args(["move-check", "--out-file", "not-a-log"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("unknown option --out-file"), "{stderr}");
}

/// A legacy log of two runs, each answering in one model step, written at `path`.
fn legacy_log(path: &std::path::Path) {
    use broca_wal::{Wal as _, WalRecord};
    let frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    let mut wal = broca_wal::FileWal::open(path).unwrap();
    for (run, step, at) in [("r1", 1u64, 1_000u64), ("r2", 2, 2_000)] {
        let mut start: WalRecord = serde_json::from_value(serde_json::json!({
            "type": "run_started", "run_id": run, "ts_ms": at,
            "session": {"project_root": "/project", "harness": "runner", "session": "s"},
            "config": {
                "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0,
                "renderer_version": frozen.renderer_version,
                "frozen_render_config": serde_json::to_value(&frozen).unwrap(),
            },
            "input": [{"role": "user", "content": [{"type": "text", "text": "Hello."}]}],
            "origin": {"kind": "fresh"},
        }))
        .unwrap();
        if let WalRecord::RunStarted { config, .. } = &mut start {
            config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
        }
        wal.append(1, &start, true).unwrap();
        for record in [
            serde_json::json!({"type": "step_started", "step_id": step}),
            serde_json::json!({"type": "model_step_finished", "step_id": step,
                "assistant_message": {"message_id": format!("m{step}"),
                    "content": [{"type": "text", "text": "Done."}]},
                "usage": {"input_tokens": 3, "output_tokens": 1},
                "finish_reason": "stop", "retries_used": 0}),
            serde_json::json!({"type": "run_finished", "reason": "completed", "ts_ms": at + 3,
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
        ] {
            let record: WalRecord = serde_json::from_value(record).unwrap();
            wal.append(1, &record, true).unwrap();
        }
    }
}

fn store_load_check(args: &[&str]) -> (bool, serde_json::Value) {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_session-convert"))
        .arg("store-load-check")
        .args(args)
        .output()
        .unwrap();
    let report = serde_json::from_slice(&output.stdout).unwrap_or_else(|_| {
        panic!(
            "a report on stdout; stderr: {}",
            String::from_utf8_lossy(&output.stderr)
        )
    });
    (output.status.success(), report)
}

/// A legacy log is staged as a plain session under the scratch folder and loads alike on both
/// paths; with `--keep` the staged session stays, and the store mode checks it there too.
#[test]
fn store_load_check_stages_legacy_logs_and_reads_stores() {
    let root = broca_test_support::scratch_root()
        .join(format!("store-load-check-cli-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let log = root.join("session.wal");
    legacy_log(&log);
    let scratch = root.join("scratch");
    let (passed, report) = store_load_check(&[
        "--from-legacy",
        "--keep",
        "--dir",
        scratch.to_str().unwrap(),
        log.to_str().unwrap(),
    ]);
    assert!(passed, "{report:#}");
    assert_eq!(report["legacy"]["staged"], 1, "{report:#}");
    assert_eq!(report["agreed"], 1, "{report:#}");
    assert_eq!(report["differed"], 0, "{report:#}");

    let staging = scratch.join("store-load-check/session-store/staging");
    let (passed, report) = store_load_check(&[staging.to_str().unwrap()]);
    assert!(passed, "{report:#}");
    assert_eq!(
        (
            &report["sessions"],
            &report["agreed"],
            &report["key_unavailable"]
        ),
        (&1.into(), &1.into(), &0.into()),
        "{report:#}"
    );
    assert!(report["records"].as_u64().unwrap() > 0, "{report:#}");

    // Without --keep, nothing is left under the scratch folder.
    let bare = root.join("bare");
    let (passed, _) = store_load_check(&[
        "--from-legacy",
        "--dir",
        bare.to_str().unwrap(),
        log.to_str().unwrap(),
    ]);
    assert!(passed);
    assert!(!bare.join("store-load-check").exists());
    let _ = std::fs::remove_dir_all(&root);
}
