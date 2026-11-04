use broca_protocol::FinishReason;
use broca_wal::framing::{self, DecodeStep};
use broca_wal::WalRecord;
use sha2::{Digest, Sha256};

/// Native provider finish evidence must not rewrite records that never carried it.
/// Pin both the JSON member order and the complete version-4 frame independently.
#[test]
fn model_step_finished_bytes_before_raw_finish_reason_are_pinned() {
    let json: &[u8] = include_bytes!("golden/model_step_finished.before_raw_finish_reason.json");
    assert_eq!(
        format!("{:x}", Sha256::digest(json)),
        "627754f30d94e9305803789201dc842be6d4d7b16275188690ddec7a9a27a0cb"
    );
    let record: WalRecord = serde_json::from_slice(json).unwrap();
    assert!(matches!(
        record,
        WalRecord::ModelStepFinished {
            step_id: broca_protocol::StepId(7),
            finish_reason: Some(FinishReason::Stop),
            ..
        }
    ));
    assert_eq!(serde_json::to_vec(&record).unwrap().as_slice(), json);
    assert!(serde_json::from_slice::<serde_json::Value>(json)
        .unwrap()
        .get("raw_finish_reason")
        .is_none());
    let bytes = framing::encode_v4(
        framing::v4::Metadata {
            seq: 7,
            fence: 3,
            appended_ms: 123,
            writer_id: [5; 16],
        },
        &[],
        &record,
    )
    .unwrap();
    let pinned: &[u8] =
        include_bytes!("golden/model_step_finished.before_raw_finish_reason.v4.bin");
    assert_eq!(bytes.as_slice(), pinned);
    assert_eq!(
        format!("{:x}", Sha256::digest(pinned)),
        "fcfa30939b18eb2c10903e6d99ec16510c3c890304f17273c0d1305e2f72acfc"
    );
    match framing::decode_next(pinned) {
        DecodeStep::Record { framed, consumed } => {
            assert_eq!(consumed, pinned.len());
            assert_eq!((framed.seq, framed.fence), (7, 3));
            assert_eq!(framed.record, record);
        }
        _ => panic!("pinned model-step frame did not decode as a record"),
    }
}
