use broca_core::AdmissionCheckpointV1;
use broca_wal::{FramedRecord, WalRecord};

#[test]
fn admission_checkpoint_before_message_marks_keeps_exact_bytes() {
    let pinned = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/golden/admission_before_message_marks.json"
    ))
    .unwrap();
    let restored: AdmissionCheckpointV1 = serde_json::from_str(&pinned).unwrap();
    assert!(
        restored.tool_set_history.is_unknown(),
        "an absent history must remain unknown, never known-empty"
    );
    let old_bytes = serde_json::to_string(&restored).unwrap();
    assert_eq!(old_bytes, pinned.trim_end());
    assert!(!old_bytes.contains("message_marks"));

    // Rebuilding today knows there were no earlier runs; legacy checkpoint bytes cannot say so.
    let queued: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/queued.json"
    ))
    .unwrap();
    let checkpoint = AdmissionCheckpointV1::from_frames(&[FramedRecord {
        seq: 1,
        fence: 1,
        record: queued,
    }])
    .unwrap();
    assert_eq!(
        checkpoint.tool_set_history,
        broca_core::signed_floor::ToolSetHistory::Known(Default::default())
    );
    let bytes = serde_json::to_string(&checkpoint).unwrap();
    assert!(bytes.contains("\"tool_set_history\":{}"));
    assert!(!bytes.contains("message_marks"));
    let rebuilt: AdmissionCheckpointV1 = serde_json::from_str(&bytes).unwrap();
    assert_eq!(rebuilt, checkpoint);
}

#[test]
fn checkpoint_reader_retains_already_recorded_pending_marks() {
    let mut old: serde_json::Value =
        serde_json::from_str(include_str!("golden/admission_before_message_marks.json")).unwrap();
    old["pending_fifo"][0]["message_marks"] = serde_json::json!({"speaks_for":{"agent_id":"Ada"}});
    let restored: AdmissionCheckpointV1 = serde_json::from_value(old).unwrap();
    let encoded = serde_json::to_value(&restored).unwrap();
    assert_eq!(
        encoded["pending_fifo"][0]["message_marks"],
        serde_json::json!({"speaks_for":{"agent_id":"Ada"}})
    );

    let mut queued: serde_json::Value = serde_json::from_str(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/queued.json"
    ))
    .unwrap();
    queued["message_marks"] = serde_json::json!({"notice":true});
    let checkpoint = AdmissionCheckpointV1::from_frames(&[FramedRecord {
        seq: 1,
        fence: 1,
        record: serde_json::from_value(queued).unwrap(),
    }])
    .unwrap();
    let encoded = serde_json::to_value(&checkpoint).unwrap();
    assert_eq!(
        encoded["pending_fifo"][0]["message_marks"],
        serde_json::json!({"notice":true})
    );
}
