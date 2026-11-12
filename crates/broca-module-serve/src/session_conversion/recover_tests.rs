//! The live log's recovery before a conversion: a run a crash cut is sealed, a paused run is not.

use super::*;

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

/// A live log of one run that started a step and then either paused for sign-in (`paused`) or
/// was cut there by a crash, written in `root`.
fn written(root: &Path, paused: bool) -> (PathBuf, Vec<FramedRecord>) {
    let path = root.join("session.wal");
    let mut wal = broca_wal::FileWal::open(path.clone())
        .unwrap()
        .with_feature_floor(root.join(broca_wal::FEATURE_FLOOR_FILENAME));
    let mut records = vec![
        record(serde_json::json!({
            "type": "run_started", "run_id": "r1", "ts_ms": 1_000,
            "session": {"project_root": "/project", "harness": "runner", "session": "s"},
            "config": {
                "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0,
                "renderer_version": 0,
            },
            "input": [{"role": "user", "content": [{"type": "text", "text": "Hello."}]}],
            "origin": {"kind": "fresh"},
        })),
        record(serde_json::json!({"type": "step_started", "step_id": 1})),
    ];
    if paused {
        records.push(record(serde_json::json!({
            "type": "run_paused", "run_id": "r1", "ts_ms": 1_002, "reason": "auth_required",
        })));
    }
    for record in &records {
        wal.append(1, record, true).unwrap();
    }
    drop(wal);
    (
        path.clone(),
        broca_wal::replay_file(&path, u64::MAX).unwrap(),
    )
}

fn scratch(tag: &str) -> PathBuf {
    let root = broca_test_support::scratch_root().join(format!(
        "session-conversion-recover-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

/// A live session whose latest run paused for sign-in stays paused through the recovery a
/// conversion runs first: nothing is appended to its log, as today's open appends nothing.
#[test]
fn a_paused_live_run_is_left_paused() {
    let root = scratch("paused");
    let (path, records) = written(&root, true);
    recover_live(&root, &path, &records).unwrap();
    let after = broca_wal::replay_file(&path, u64::MAX).unwrap();
    assert_eq!(after.len(), records.len(), "nothing is appended");
    let raw: Vec<WalRecord> = after.into_iter().map(|frame| frame.record).collect();
    assert_eq!(
        broca_core::reconcile_run_state(&raw, "r1").unwrap().state,
        broca_core::RunIndexState::Paused
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A live session whose latest run a crash cut mid-step is sealed interrupted, as before.
#[test]
fn a_cut_live_run_is_sealed_interrupted() {
    let root = scratch("cut");
    let (path, records) = written(&root, false);
    recover_live(&root, &path, &records).unwrap();
    let after = broca_wal::replay_file(&path, u64::MAX).unwrap();
    assert!(after.len() > records.len(), "the seal is appended");
    let raw: Vec<WalRecord> = after.into_iter().map(|frame| frame.record).collect();
    assert_eq!(
        broca_core::reconcile_run_state(&raw, "r1").unwrap().state,
        broca_core::RunIndexState::Interrupted
    );
    let _ = std::fs::remove_dir_all(&root);
}
