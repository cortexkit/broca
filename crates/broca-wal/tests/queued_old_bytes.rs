use broca_wal::framing::{self, DecodeStep};
use broca_wal::WalRecord;
use sha2::{Digest, Sha256};

#[test]
fn queued_bytes_before_display_sender_are_pinned() {
    let json = include_str!("golden/pre_step_transforms/queued.json")
        .trim()
        .as_bytes();
    let record: WalRecord = serde_json::from_slice(json).unwrap();
    let encoded_json = serde_json::to_vec(&record).unwrap();
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
    assert_eq!(encoded_json, json);
    assert_eq!(
        format!("{:x}", Sha256::digest(json)),
        "19338e45f652861afc61e4be4a406b63d63b7573faf02fff76e7ebe42b339296"
    );
    let pinned = include_str!("golden/queued.before_display_sender.v4.hex").trim();
    assert_eq!(
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        pinned
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "ac1a2f46011c0bce17c739f397fec9feab0a7f0e73162f8907e686d16c1a6568"
    );
    match framing::decode_next(&bytes) {
        DecodeStep::Record { framed, consumed } => {
            assert_eq!(consumed, bytes.len());
            assert_eq!(framed.record, record);
        }
        _ => panic!("pinned input frame did not decode as a record"),
    }
}
