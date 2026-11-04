use broca_wal::framing::{self, DecodeStep};
use broca_wal::WalRecord;
use sha2::{Digest, Sha256};

#[test]
fn run_started_bytes_before_display_sender_are_pinned() {
    let json = include_str!("golden/pre_step_transforms/run_started.json")
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
        "11ef313a829381a50aaef64e109dcd96351bde4408f00e1d41146e1d285452ac"
    );
    let pinned = include_str!("golden/run_started.before_display_sender.v4.hex").trim();
    assert_eq!(
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        pinned
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "e073bf971215be3b1391d9d35b92488800eca21f892e01b61bbd24137d6d6353"
    );
    match framing::decode_next(&bytes) {
        DecodeStep::Record { framed, consumed } => {
            assert_eq!(consumed, bytes.len());
            assert_eq!(framed.record, record);
        }
        _ => panic!("pinned input frame did not decode as a record"),
    }
}
