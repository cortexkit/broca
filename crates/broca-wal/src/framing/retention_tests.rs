//! Readers that cannot interpret retention policy or activity timestamps must refuse
//! the gated WAL rather than parse it as a session whose content can be kept forever.

use super::*;
use serde_json::{json, Value};

const RUN_STARTED: &str = include_str!("../../tests/golden/pre_step_transforms/run_started.json");
const QUEUED: &str = include_str!("../../tests/golden/pre_step_transforms/queued.json");
const STEERED: &str = include_str!("../../tests/golden/pre_step_transforms/steered.json");

fn round_trip(record: &WalRecord, expected: &[&str]) {
    // A sibling test hides steer-queue/v1 process-wide; this decodes steered frames with the
    // shipped table, so it must not run while that feature is hidden.
    let _table = lock_feature_table_for_test();
    assert_eq!(record.required_features(), expected);
    let bytes = encode(7, 11, record).unwrap();
    assert_eq!(
        bytes[4],
        if expected.is_empty() {
            FRAMING_VERSION
        } else {
            GATED_FRAMING_VERSION
        }
    );
    if !expected.is_empty() {
        let payload: Value = serde_json::from_slice(&bytes[HEADER_LEN..]).unwrap();
        assert_eq!(payload["requires"], json!(expected));
    }
    let mut reader = KNOWN_FEATURES.to_vec();
    reader.push(RETENTION_FEATURE);
    let DecodeStep::Record { framed, consumed } = decode_next_with(&bytes, &reader) else {
        panic!("retention-capable reader must decode {record:?}");
    };
    assert_eq!(consumed, bytes.len());
    assert_eq!((framed.seq, framed.fence), (7, 11));
    assert_eq!(&framed.record, record);
    assert!(matches!(decode_next(&bytes), DecodeStep::Record { .. }));
    if expected.contains(&RETENTION_FEATURE) {
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|feature| *feature != RETENTION_FEATURE)
            .collect();
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [RETENTION_FEATURE])
        );
    }
}

fn with_fields(old: &str, retention: Option<u64>, ts_ms: Option<u64>) -> WalRecord {
    let mut value: Value = serde_json::from_str(old).unwrap();
    if let Some(seconds) = retention {
        value["retention"] = json!(seconds);
    }
    if let Some(time) = ts_ms {
        value["ts_ms"] = json!(time);
    }
    serde_json::from_value(value).unwrap()
}

fn assert_json_fields(record: &WalRecord, retention: Option<u64>, ts_ms: Option<u64>) {
    let value = serde_json::to_value(record).unwrap();
    assert_eq!(value.get("retention"), retention.map(|n| json!(n)).as_ref());
    assert_eq!(value.get("ts_ms"), ts_ms.map(|n| json!(n)).as_ref());
    assert_eq!(serde_json::from_value::<WalRecord>(value).unwrap(), *record);
}

#[test]
fn run_started_retention_round_trips_present_and_absent() {
    for retention in [None, Some(0), Some(3600), Some(u64::MAX)] {
        for ts_ms in [None, Some(1790000000000)] {
            let record = with_fields(RUN_STARTED, retention, ts_ms);
            assert!(
                matches!(record, WalRecord::RunStarted { retention: r, ts_ms: t, .. }
                if r == retention && t == ts_ms)
            );
            assert_json_fields(&record, retention, ts_ms);
            // RunStarted's existing metering timestamp alone remains ungated.
            round_trip(
                &record,
                if retention.is_some() {
                    &[RETENTION_FEATURE]
                } else {
                    &[]
                },
            );
        }
    }
}

#[test]
fn queued_retention_and_activity_round_trip_independently() {
    for retention in [None, Some(0), Some(3600), Some(u64::MAX)] {
        for ts_ms in [None, Some(0), Some(1790000000000), Some(u64::MAX)] {
            let record = with_fields(QUEUED, retention, ts_ms);
            assert!(
                matches!(record, WalRecord::Queued { retention: r, ts_ms: t, .. }
                if r == retention && t == ts_ms)
            );
            assert_json_fields(&record, retention, ts_ms);
            round_trip(
                &record,
                if retention.is_some() || ts_ms.is_some() {
                    &[RETENTION_FEATURE]
                } else {
                    &[]
                },
            );
        }
    }
}

#[test]
fn steered_retention_and_activity_round_trip_independently() {
    for retention in [None, Some(0), Some(3600), Some(u64::MAX)] {
        for ts_ms in [None, Some(0), Some(1790000000000), Some(u64::MAX)] {
            let record = with_fields(STEERED, retention, ts_ms);
            assert!(
                matches!(record, WalRecord::Steered { retention: r, ts_ms: t, .. }
                if r == retention && t == ts_ms)
            );
            assert_json_fields(&record, retention, ts_ms);
            round_trip(
                &record,
                if retention.is_some() || ts_ms.is_some() {
                    &[RETENTION_FEATURE, STEER_QUEUE_FEATURE]
                } else {
                    &[STEER_QUEUE_FEATURE]
                },
            );
        }
    }
}

#[test]
fn retention_combines_with_all_existing_send_features() {
    let hooked: Value = serde_json::from_str(include_str!(
        "../../tests/golden/step_transforms/run_started.json"
    ))
    .unwrap();
    let scoped: Value = serde_json::from_str(include_str!(
        "../../tests/fixtures/pre_flow_scoped_run_started.json"
    ))
    .unwrap();
    for retention in [false, true] {
        for scope in 0..3 {
            for manifest in 0..3 {
                for hooks in [false, true] {
                    let mut value: Value = serde_json::from_str(RUN_STARTED).unwrap();
                    let mut expected = Vec::new();
                    if scope == 2 {
                        expected.push(FLOW_SCOPE_FEATURE);
                    }
                    if manifest > 0 {
                        value["manifest"] = hooked["record"]["manifest"].clone();
                        if manifest == 1 {
                            value["manifest"]["declarations"] = json!([]);
                        }
                        expected.push(PLAN_MANIFEST_FEATURE);
                    }
                    if retention {
                        value["retention"] = json!(3600);
                        expected.push(RETENTION_FEATURE);
                    }
                    if scope > 0 {
                        value["scope"] = scoped["record"]["scope"].clone();
                        if scope == 2 {
                            value["scope"]["flow_id"] = json!("flow-1");
                        }
                        expected.push(SCOPE_FEATURE);
                    }
                    if hooks {
                        value["hooks"] = hooked["record"]["hooks"].clone();
                    }
                    if hooks || manifest == 2 {
                        expected.push(STEP_TRANSFORMS_FEATURE);
                    }
                    round_trip(&serde_json::from_value(value).unwrap(), &expected);
                }
            }
        }
    }
    for (old, base) in [(QUEUED, &[][..]), (STEERED, &[STEER_QUEUE_FEATURE][..])] {
        for retention in [None, Some(3600)] {
            for ts_ms in [None, Some(1790000000000)] {
                for hooks in [false, true] {
                    let mut record = with_fields(old, retention, ts_ms);
                    let mut expected = Vec::new();
                    if retention.is_some() || ts_ms.is_some() {
                        expected.push(RETENTION_FEATURE);
                    }
                    expected.extend_from_slice(base);
                    if hooks {
                        let mut value = serde_json::to_value(&record).unwrap();
                        value["hooks"] = hooked["record"]["hooks"].clone();
                        record = serde_json::from_value(value).unwrap();
                        expected.push(STEP_TRANSFORMS_FEATURE);
                    }
                    round_trip(&record, &expected);
                }
            }
        }
    }
}

#[test]
fn incomplete_reader_refuses_retention_before_interpreting_new_fields() {
    assert!(KNOWN_FEATURES.contains(&RETENTION_FEATURE));
    let older: Vec<_> = KNOWN_FEATURES
        .iter()
        .copied()
        .filter(|feature| *feature != RETENTION_FEATURE)
        .collect();
    let future = frame(GATED_FRAMING_VERSION, 7, 11,
        br#"{"requires":["retention/v1"],"record":{"type":"run_started","retention":"future-policy"}}"#).unwrap();
    assert!(
        matches!(decode_next_with(&future, &older), DecodeStep::NewerWriter { missing, .. }
        if missing == [RETENTION_FEATURE])
    );
}
