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

#[test]
fn flow_run_started_bytes_before_agent_id_freeze_are_pinned() {
    let payload = include_str!("fixtures/pre_agent_id_frozen_flow_run_started.json").trim_end();
    let envelope: serde_json::Value = serde_json::from_str(payload).unwrap();
    let record: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
    let bytes = framing::encode(1, 1, &record).unwrap();
    assert_eq!(bytes[4], 3);
    assert_eq!(&bytes[53..], payload.as_bytes());
    assert_eq!(
        format!("{:x}", Sha256::digest(payload.as_bytes())),
        "aff6e09262c555b3c9d46d2cd69511bf56d501641d16dbaa376eb92fe23746ca"
    );
}

#[test]
fn old_decoder_accepts_future_flow_discriminator_without_changing_replay_inputs() {
    let mut envelope: serde_json::Value = serde_json::from_str(include_str!(
        "fixtures/pre_agent_id_frozen_flow_run_started.json"
    ))
    .unwrap();
    let old: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
    envelope["record"]["scope"]["agent_id_frozen"] = serde_json::json!(true);
    let future: WalRecord = serde_json::from_value(envelope["record"].clone()).unwrap();
    // Replay and rendering consume the decoded record, which the old decoder leaves identical.
    assert_eq!(future, old);
}
