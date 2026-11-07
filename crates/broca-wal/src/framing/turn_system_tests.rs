use super::*;
use broca_protocol::{ContentBlock, DeliveryMode, Message, Role, RunId, SubmissionId};
use std::sync::Arc;

#[test]
fn turn_system_writer_serializer_requires_reader_support_and_preserves_legacy_bytes() {
    let _table = lock_feature_table_for_test();
    let mut message = Message::new(
        Role::System,
        vec![ContentBlock::Text {
            text: "Keep the summary concise.".into(),
        }],
    );
    let legacy = serde_json::to_value(&message).unwrap();
    assert_eq!(
        legacy,
        serde_json::json!({"role":"system","content":[{"type":"text","text":"Keep the summary concise."}]})
    );
    message.turn_system = true;
    let delivered = WalRecord::SendDelivered {
        ride: None,
        written_at_ms: None,
        tags: None,
        submission_id: SubmissionId("note".into()),
        run_id: RunId("r".into()),
        mode: DeliveryMode::TurnSystem,
        input: Some(Arc::new(vec![message.clone()])),
        tool_call_id: None,
        appended_text: None,
    };
    let steered = WalRecord::Steered {
        sent_by: None,
        written_at_ms: None,
        tags: None,
        hooks: None,
        title: None,
        send_id: None,
        request_identity_digest: None,
        retention: None,
        ts_ms: None,
        delivery: None,
        input: Arc::new(vec![message]),
    };
    for record in [delivered, steered] {
        assert!(record.required_features().contains(&TURN_SYSTEM_FEATURE));
        let bytes = encode(1, 1, &record).unwrap();
        let older: Vec<_> = KNOWN_FEATURES
            .iter()
            .copied()
            .filter(|name| *name != TURN_SYSTEM_FEATURE)
            .collect();
        assert!(
            matches!(decode_next_with(&bytes, &older), DecodeStep::NewerWriter { missing, .. } if missing == [TURN_SYSTEM_FEATURE])
        );
        let DecodeStep::Record { framed, consumed } = decode_next(&bytes) else {
            panic!("reader must decode writer output");
        };
        assert_eq!(consumed, bytes.len());
        assert_eq!(framed.record, record);
    }
}
