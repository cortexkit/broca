use super::*;
use serde_json::{json, Value};

fn records_with_openrouter_state() -> Vec<WalRecord> {
    let block = json!({"type":"redacted_reasoning","data":"{\"reasoning\":\"plan\",\"reasoning_details\":[{\"type\":\"reasoning.encrypted\",\"data\":\"opaque+/=\"}]}","provider_format":"open_router"});
    let message = json!({"role":"assistant","content":[block.clone()]});
    let run = json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"openrouter","model_id":"google/gemini-3-flash-preview"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":4},"input":[],"origin":{"kind":"fresh"}});
    let mut with_input = run.clone();
    with_input["input"] = json!([message.clone()]);
    let mut with_policy = run;
    with_policy["config"]["frozen_render_config"] =
        json!({"reasoning_echo":{"format":"open_router_details","turns":"every_assistant"}});
    let mut compaction: Value = serde_json::from_str(
        include_str!("../../tests/fixtures/compaction_records.jsonl")
            .lines()
            .find(|line| line.contains("\"type\":\"compaction_message\""))
            .unwrap(),
    )
    .unwrap();
    compaction["replacement"] = json!([message.clone()]);
    [
        with_input,
        with_policy,
        json!({"type":"run_config_refrozen","renderer_version":4,"frozen_render_config":{"reasoning_echo":{"format":"open_router_details","turns":"tool_calls"}}}),
        json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[block]},"usage":{},"retries_used":0}),
        json!({"type":"steered","input":[message.clone()]}),
        json!({"type":"queued","submission_id":"q","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[message],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}),
        compaction,
    ].into_iter().map(|value| serde_json::from_value(value).unwrap()).collect()
}

fn older_features() -> Vec<&'static str> {
    KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != OPENROUTER_REASONING_FEATURE)
        .collect()
}

#[test]
fn openrouter_reasoning_is_must_understand_before_record_decode() {
    let older = older_features();
    for record in records_with_openrouter_state() {
        let bytes = encode(1, 1, &record).unwrap();
        let outcome = decode_next_with(&bytes, &older);
        assert!(
            matches!(&outcome, DecodeStep::NewerWriter { missing, .. } if missing == &[OPENROUTER_REASONING_FEATURE]),
            "older reader must refuse OpenRouter state by name before record decoding"
        );
        assert_eq!(bytes[4], GATED_FRAMING_VERSION);
        assert!(record
            .required_features()
            .contains(&OPENROUTER_REASONING_FEATURE));
        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("current reader refused its own frame")
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
    }
}

#[test]
fn non_openrouter_reasoning_records_remain_readable() {
    let older = older_features();
    let mut records: Vec<WalRecord> =
        include_str!("../../tests/fixtures/before_openrouter_reasoning.jsonl")
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
    for frozen in [
        json!({}),
        json!({"reasoning_content_required":true}),
        json!({"reasoning_echo":{"format":"reasoning_content","turns":"every_assistant"}}),
    ] {
        records.push(serde_json::from_value(json!({"type":"run_config_refrozen","renderer_version":4,"frozen_render_config":frozen})).unwrap());
    }
    for record in records {
        assert!(!record
            .required_features()
            .contains(&OPENROUTER_REASONING_FEATURE));
        let bytes = encode(1, 1, &record).unwrap();
        assert_eq!(bytes[4], FRAMING_VERSION);
        assert!(matches!(
            decode_next_with(&bytes, &older),
            DecodeStep::Record { .. }
        ));
    }
}

#[test]
fn openrouter_append_publishes_feature_floor_before_state() {
    use crate::{FileWal, Wal, WalError};
    let root = broca_test_support::scratch_root().join(format!(
        "openrouter-floor-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let path = root.join("session.wal");
    let floor = root.join("wal-feature-floor");
    let record = records_with_openrouter_state().remove(3);
    let mut wal = FileWal::open(&path).unwrap();
    let plain: WalRecord =
        serde_json::from_value(json!({"type":"step_started","step_id":1})).unwrap();
    wal.append(1, &plain, true).unwrap();
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        wal.append(1, &record, true),
        Err(WalError::FeatureFloor { path: None, .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    drop(wal);
    let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
    wal.append(1, &plain, true).unwrap();
    assert!(
        !floor.exists(),
        "ordinary frames do not raise the feature floor"
    );
    wal.append(1, &record, true).unwrap();
    assert_eq!(
        crate::floor::read_floor(&floor).unwrap(),
        std::collections::BTreeSet::from([OPENROUTER_REASONING_FEATURE.to_owned()])
    );
    assert_eq!(wal.replay().unwrap().len(), 3);
}
