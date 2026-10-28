//! Both content-filter fallback features are must-understand: a reader that lacks one refuses,
//! by that name, every frame carrying something it covers, and each record or field requires
//! exactly its own feature.

use super::*;
use serde_json::{json, Value};

const SCHEDULE: &str = r#"{"source":{"provider_module_id":"anthropic","model_id":"claude-opus-5-5"},"fallback":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"phase":{"kind":"fallback","remaining":10},"re_refusals":0,"updated_at_ms":1790000000000}"#;

fn plain_step() -> Value {
    json!({"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m","content":[{"type":"text","text":"ok"}]},"usage":{},"retries_used":0})
}

/// Every record shape that carries the handoff part or a server-side fallback step field.
fn records_with_server_side_fallback() -> Vec<Value> {
    let block = json!({"type":"provider_fallback","from_model":"claude-opus-5-5","to_model":"claude-opus-4-8"});
    let message =
        json!({"role":"assistant","content":[{"type":"text","text":"Let me"},block.clone()]});
    let mut run = json!({"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[],"origin":{"kind":"fresh"}});
    run["input"] = json!([message.clone()]);
    let mut with_block = plain_step();
    with_block["assistant_message"]["content"] = json!([block]);
    let mut served = plain_step();
    served["served_model"] = json!("claude-opus-4-8");
    let mut iterations = plain_step();
    iterations["usage_iterations"] = json!([{"type":"message","model":"claude-opus-5-5","input_tokens":3,"output_tokens":0},{"type":"fallback_message","model":"claude-opus-4-8","input_tokens":3,"output_tokens":9,"future":{"x":1}}]);
    let mut compaction: Value = serde_json::from_str(
        include_str!("../../tests/fixtures/compaction_records.jsonl")
            .lines()
            .find(|line| line.contains("\"type\":\"compaction_message\""))
            .unwrap(),
    )
    .unwrap();
    compaction["replacement"] = json!([message.clone()]);
    vec![
        run,
        with_block,
        served,
        iterations,
        json!({"type":"steered","input":[message.clone()]}),
        json!({"type":"queued","submission_id":"q","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[message.clone()],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}),
        json!({"type":"send_delivered","submission_id":"sub-r2","run_id":"r1","mode":"steer","input":[message]}),
        compaction,
    ]
}

/// Every record shape that carries client-side refusal-fallback state.
fn records_with_refusal_fallback() -> Vec<Value> {
    let schedule: Value = serde_json::from_str(SCHEDULE).unwrap();
    let mut routed = plain_step();
    routed["route"] = json!({"model":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"reason":"refusal"});
    let mut scheduled = plain_step();
    scheduled["refusal_fallback"] = schedule.clone();
    let mut after_tool_use = plain_step();
    after_tool_use["refusal_after_tool_use"] = json!(true);
    vec![
        json!({"type":"model_step_refused","step_id":1,"usage":{"input_tokens":7,"output_tokens":2},"discarded_blocks":1,"schedule":schedule}),
        routed,
        scheduled,
        after_tool_use,
    ]
}

fn without(feature: &str) -> Vec<&'static str> {
    KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != feature)
        .collect()
}

fn assert_refused_by_name_only(value: Value, feature: &'static str) {
    let older = without(feature);
    let record: WalRecord = serde_json::from_value(value.clone()).unwrap();
    assert!(
        record.required_features().contains(&feature),
        "{value}: {:?}",
        record.required_features()
    );
    let bytes = encode(1, 1, &record).unwrap();
    assert_eq!(bytes[4], GATED_FRAMING_VERSION, "{value}");
    assert!(
        matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [feature]),
        "a reader without {feature} must refuse {value} by that name"
    );
    let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
        panic!("this build must read its own frame: {value}")
    };
    assert_eq!(consumed, bytes.len());
    assert_eq!(framed.record, record);
}

#[test]
fn provider_fallback_block_is_must_understand_and_older_readers_refuse_it_by_name() {
    // A steered and a delivered frame are decoded with the shipped table; a sibling test hides
    // steer-queue/v1 process-wide.
    let _table = lock_feature_table_for_test();
    for value in records_with_server_side_fallback() {
        assert_refused_by_name_only(value, PROVIDER_FALLBACK_BLOCK_FEATURE);
    }
}

#[test]
fn refusal_fallback_is_must_understand_and_older_readers_refuse_it_by_name() {
    for value in records_with_refusal_fallback() {
        assert_refused_by_name_only(value, REFUSAL_FALLBACK_FEATURE);
    }
}

/// Each new record and each new step field adds exactly its own feature, and nothing else, to
/// what the record already required; a step without them requires nothing.
#[test]
fn each_fallback_record_or_field_requires_exactly_its_feature() {
    let plain: WalRecord = serde_json::from_value(plain_step()).unwrap();
    assert!(plain.required_features().is_empty());
    let schedule: Value = serde_json::from_str(SCHEDULE).unwrap();
    let cases: [(&str, Value, &[&str]); 6] = [
        (
            "assistant_message",
            json!({"message_id":"m","content":[{"type":"provider_fallback","from_model":"a","to_model":"b"}]}),
            &[PROVIDER_FALLBACK_BLOCK_FEATURE],
        ),
        (
            "served_model",
            json!("claude-opus-4-8"),
            &[PROVIDER_FALLBACK_BLOCK_FEATURE],
        ),
        (
            "usage_iterations",
            json!([{"type":"message"}]),
            &[PROVIDER_FALLBACK_BLOCK_FEATURE],
        ),
        (
            "route",
            json!({"model":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"reason":"schedule"}),
            &[REFUSAL_FALLBACK_FEATURE],
        ),
        (
            "refusal_fallback",
            schedule.clone(),
            &[REFUSAL_FALLBACK_FEATURE],
        ),
        (
            "refusal_after_tool_use",
            json!(true),
            &[REFUSAL_FALLBACK_FEATURE],
        ),
    ];
    for (field, value, expected) in cases {
        let mut step = plain_step();
        step[field] = value;
        let record: WalRecord = serde_json::from_value(step).unwrap();
        assert_eq!(record.required_features(), expected, "{field}");
    }
    // An explicit false flag is the absent flag: no bytes, no feature.
    let mut step = plain_step();
    step["refusal_after_tool_use"] = json!(false);
    let record: WalRecord = serde_json::from_value(step).unwrap();
    assert!(record.required_features().is_empty());
    assert_eq!(serde_json::to_value(&record).unwrap(), plain_step());

    // The refused-step record needs only its own feature, even with its optional members set.
    let refused: WalRecord = serde_json::from_value(json!({"type":"model_step_refused","step_id":1,"usage":{},"served_model":"claude-opus-4-8","discarded_blocks":0,"schedule":schedule,"ts_ms":5})).unwrap();
    assert_eq!(refused.required_features(), &[REFUSAL_FALLBACK_FEATURE]);
    assert!(!refused.is_informational());
}
