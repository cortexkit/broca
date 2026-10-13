//! Freeze complete send-record frames before adding retention metadata. Both legacy and
//! step-transformed sends must keep their bytes when the new optional members are absent.

use broca_wal::{
    framing::{decode_next, encode, DecodeStep},
    WalRecord,
};

const OLD: &[(&str, &str)] = &[
    (
        include_str!("../golden/pre_step_transforms/run_started.json"),
        include_str!("../golden/pre_step_transforms/run_started.frame.hex"),
    ),
    (
        include_str!("../golden/pre_step_transforms/queued.json"),
        include_str!("../golden/pre_step_transforms/queued.frame.hex"),
    ),
    (
        include_str!("../golden/pre_step_transforms/steered.json"),
        include_str!("../golden/pre_step_transforms/steered.frame.hex"),
    ),
    (
        include_str!("../golden/step_transforms/run_started.json"),
        include_str!("../golden/step_transforms/run_started.frame.hex"),
    ),
    (
        include_str!("../golden/step_transforms/queued.json"),
        include_str!("../golden/step_transforms/queued.frame.hex"),
    ),
    (
        include_str!("../golden/step_transforms/steered.json"),
        include_str!("../golden/step_transforms/steered.frame.hex"),
    ),
];

#[test]
fn pre_retention_send_frames_keep_their_original_bytes() {
    for (json, hex) in OLD {
        let payload: serde_json::Value = serde_json::from_str(json).unwrap();
        let record: WalRecord =
            serde_json::from_value(payload.get("record").unwrap_or(&payload).clone()).unwrap();
        let hex = hex.trim_end();
        assert_eq!(hex.len() % 2, 0);
        let pinned: Vec<u8> = hex
            .as_bytes()
            .as_chunks::<2>()
            .0
            .iter()
            .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
            .collect();
        let encoded = encode(7, 11, &record).unwrap();
        assert_eq!(encoded, pinned, "pre-retention frame changed: {json}");
        if payload.get("record").is_some() {
            assert_eq!(
                &encoded[broca_wal::framing::HEADER_LEN..],
                json.trim_end().as_bytes()
            );
        } else {
            assert_eq!(serde_json::to_string(&record).unwrap(), json.trim_end());
        }
        let DecodeStep::Record { framed, consumed } = decode_next(&pinned) else {
            panic!("pre-retention frame must decode: {json}");
        };
        assert_eq!(consumed, pinned.len());
        assert_eq!((framed.seq, framed.fence), (7, 11));
        assert_eq!(framed.record, record);
    }
}
