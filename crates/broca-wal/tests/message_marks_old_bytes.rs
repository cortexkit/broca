//! Marked sends and rides as builds before `speaks_for.name` write them: an agent mark without a
//! name, and a notice. A name is gated by its own reader feature, so these records must keep
//! exactly these bytes and require only the features they required before names existed.
//! The literals are decoded rather than built, so the pin cannot move together with the encoder.

use broca_wal::framing::{self, DecodeStep};
use broca_wal::WalRecord;
use sha2::{Digest, Sha256};

struct Pinned {
    json: &'static str,
    frame_sha256: &'static str,
    requires: &'static [&'static str],
}

const MARKED: [Pinned; 5] = [
    Pinned {
        json: r#"{"type":"run_started","message_marks":{"speaks_for":{"agent_id":"Ada"}},"run_id":"run-7","session":{"project_root":"/tmp/project","harness":"runner","session":"session"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#,
        frame_sha256: "21f516f8651744a00b7e6b19709b5fc2f8752405653908a12c6ab281d0604406",
        requires: &["message-marks/v1"],
    },
    Pinned {
        json: r#"{"type":"queued","message_marks":{"speaks_for":{"agent_id":"Ada"}},"submission_id":"sub-run-8","request":{"model":{"provider_module_id":"p","model_id":"m"},"input":[{"role":"user","content":[{"type":"text","text":"queued"}]}],"policy_allowed":[],"tool_choice":{"type":"auto"},"generation":{},"stop_when":[]}}"#,
        frame_sha256: "c753d3ab82763d963410bce576c3888ac7cc94659c4b6259c98ce8c2338d36cc",
        requires: &["message-marks/v1"],
    },
    Pinned {
        json: r#"{"type":"steered","message_marks":{"speaks_for":{"agent_id":"Ada"}},"send_id":"steer-1","request_identity_digest":"v1:abc","delivery":"steer","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
        frame_sha256: "6c21b0bf0ffdbf7b39eb423f84d4ab99282890d72db46c3d4ed13e6822619d9b",
        requires: &["message-marks/v1", "steer-queue/v1"],
    },
    Pinned {
        json: r#"{"type":"steered","message_marks":{"notice":true},"send_id":"steer-1","request_identity_digest":"v1:abc","delivery":"steer","input":[{"role":"user","content":[{"type":"text","text":"write up now"}]}]}"#,
        frame_sha256: "7944fe007bf96c925c8c8d313adb4041c26fe9f547dbb413d1d4f5a71a09a0c3",
        requires: &["message-marks/v1", "steer-queue/v1"],
    },
    Pinned {
        json: r#"{"type":"send_delivered","ride":{"call_key":"call-1","send_id":"send-1","sender":"prefrontal-core","speaks_for":{"agent_id":"Ada"},"forms":["append"],"form":"append"},"submission_id":"sub-1","run_id":"run-1","mode":"tool_result","tool_call_id":"call-1","appended_text":"\n\nhello"}"#,
        frame_sha256: "1fad8497580424f508ef102784d0ad3bb245b3f1c73aa3581a6dee8cd2de5713",
        requires: &["delivery-order/v1", "message-marks/v1", "rides-v2/v1"],
    },
];

#[test]
fn marked_send_and_ride_bytes_before_speaker_names_are_pinned() {
    for pinned in MARKED {
        let record: WalRecord = serde_json::from_str(pinned.json).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), pinned.json);
        assert_eq!(
            record.required_features(),
            pinned.requires,
            "{}",
            pinned.json
        );
        let bytes = framing::encode(7, 3, &record).unwrap();
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            pinned.frame_sha256,
            "{}",
            pinned.json
        );
        match framing::decode_next(&bytes) {
            DecodeStep::Record { framed, consumed } => {
                assert_eq!(consumed, bytes.len());
                assert_eq!(framed.record, record);
            }
            _ => panic!("pinned marked frame did not decode: {}", pinned.json),
        }
    }
}
