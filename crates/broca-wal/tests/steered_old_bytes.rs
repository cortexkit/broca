use broca_wal::framing::{self, DecodeStep};
use broca_wal::WalRecord;
use sha2::{Digest, Sha256};

#[test]
fn steered_bytes_before_display_sender_are_pinned() {
    let json = include_str!("golden/pre_step_transforms/steered.json")
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
        "1631cf04a7519575eb951795dedee6f0a8d267f0bdf85dbec4440c0262e725f7"
    );
    let pinned = include_str!("golden/steered.before_display_sender.v4.hex").trim();
    assert_eq!(
        bytes.iter().map(|b| format!("{b:02x}")).collect::<String>(),
        pinned
    );
    assert_eq!(
        format!("{:x}", Sha256::digest(&bytes)),
        "58981dcc969050998351b2e2104bca7186bc42ec720a0176a46776a449bdd29e"
    );
    match framing::decode_next(&bytes) {
        DecodeStep::Record { framed, consumed } => {
            assert_eq!(consumed, bytes.len());
            assert_eq!(framed.record, record);
        }
        _ => panic!("pinned input frame did not decode as a record"),
    }
}
