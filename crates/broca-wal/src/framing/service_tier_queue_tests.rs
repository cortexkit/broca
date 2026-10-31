use super::*;
use serde_json::{json, Value};

const GOLDEN: &str = include_str!("../../tests/golden/service_tier_queue/queued.json");

fn record() -> WalRecord {
    let envelope: Value = serde_json::from_str(GOLDEN).unwrap();
    serde_json::from_value(envelope["record"].clone()).unwrap()
}

#[test]
fn queued_abstract_tier_bytes_and_reader_requirements_are_pinned() {
    let record = record();
    assert_eq!(record.required_features(), [SERVICE_TIER_QUEUE_FEATURE]);
    let bytes = encode(7, 3, &record).unwrap();
    assert_eq!(bytes[4], GATED_FRAMING_VERSION);
    assert_eq!(
        std::str::from_utf8(&bytes[HEADER_LEN..]).unwrap(),
        GOLDEN.trim_end()
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "ffef0aa8a8496dd3dd8b56918851a702ebcbe4c71fbaa68ebc912fe75a91e31b"
    );

    let older: Vec<_> = KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|name| *name != SERVICE_TIER_QUEUE_FEATURE)
        .collect();
    let v4 = encode_v4(
        v4::Metadata {
            seq: 7,
            fence: 3,
            appended_ms: 123,
            writer_id: [5; 16],
        },
        &[],
        &record,
    )
    .unwrap();
    for bytes in [bytes, v4] {
        assert!(
            matches!(decode_next(&bytes), DecodeStep::Record { framed, .. } if framed.record == record)
        );
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [SERVICE_TIER_QUEUE_FEATURE])
        );
    }
}

#[test]
fn queued_tier_readers_keep_every_level_and_compose_feature_requirements() {
    for level in ["standard", "fast", "ultrafast", "future-level"] {
        let mut value = serde_json::to_value(record()).unwrap();
        value["request"]["service_tier_level"] = json!(level);
        value["request"]["budget"] = json!({"max_input_tokens": 100});
        let record: WalRecord = serde_json::from_value(value).unwrap();
        assert!(record
            .required_features()
            .contains(&SERVICE_TIER_QUEUE_FEATURE));
        assert!(record
            .required_features()
            .contains(&INPUT_TOKEN_CAP_FEATURE));
        let bytes = encode(1, 1, &record).unwrap();
        let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
            panic!("queued tier must decode")
        };
        let WalRecord::Queued { request, .. } = framed.record else {
            panic!("queued record")
        };
        assert_eq!(request.service_tier_level.as_deref(), Some(level));
    }
}

#[test]
fn absent_abstract_tier_keeps_legacy_queued_bytes_and_feature_floor() {
    let legacy = r#"{"type":"queued","submission_id":"sub-tier","request":{"model":{"provider_module_id":"openai","model_id":"gpt-6.1-sol"},"input":[],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#;
    let record: WalRecord = serde_json::from_str(legacy).unwrap();
    assert!(record.required_features().is_empty());
    assert_eq!(serde_json::to_string(&record).unwrap(), legacy);
    let bytes = encode(7, 3, &record).unwrap();
    assert_eq!(bytes[4], FRAMING_VERSION);
    assert_eq!(std::str::from_utf8(&bytes[HEADER_LEN..]).unwrap(), legacy);
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "3fc8a592828489dbf05eec2490bd59966c1bdd0f7eb90f94305cbba53ef7d92b"
    );
}

#[test]
fn shared_service_tier_send_vectors_retain_explicit_levels_and_absence_in_the_queue() {
    // The shared service-tier-v1 corpus pins send levels. Only those cases apply to the
    // queue reader; discovery, usage and ride production belong to the writer release.
    let samples = include_str!("../../tests/fixtures/service-tier-v1/cases.json");
    let sums = include_str!("../../tests/fixtures/service-tier-v1/SHA256SUMS");
    assert_eq!(
        sums.trim_end(),
        "7e8b37e09ec91fde56738d29514e45de8063bd48a96acfdb88b1bb70ded5abe5  cases.json"
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(samples.as_bytes())),
        "7e8b37e09ec91fde56738d29514e45de8063bd48a96acfdb88b1bb70ded5abe5"
    );
    let cases: Value = serde_json::from_str(samples).unwrap();
    for send in cases["sends"].as_array().unwrap() {
        let mut value = serde_json::to_value(record()).unwrap();
        value["request"]["service_tier_level"] =
            send.get("service_tier").cloned().unwrap_or(Value::Null);
        let record: WalRecord = serde_json::from_value(value).unwrap();
        let present = send.get("service_tier").is_some();
        assert_eq!(
            record
                .required_features()
                .contains(&SERVICE_TIER_QUEUE_FEATURE),
            present
        );
        let bytes = encode(1, 1, &record).unwrap();
        let DecodeStep::Record { framed, .. } = decode_next(&bytes) else {
            panic!("shared queued tier must decode")
        };
        let WalRecord::Queued { request, .. } = framed.record else {
            panic!("queued record")
        };
        assert_eq!(
            request.service_tier_level.as_deref(),
            send.get("service_tier").and_then(Value::as_str)
        );
    }
}
