//! Records pinned before step-transform fields exist. Keep these fixtures separate from the
//! hooked-record goldens: an unhooked record must retain its entire on-disk frame, not just its
//! decoded value, after a reader release adds new optional members.

use super::WalRecord;
use crate::framing::{decode_next, encode, DecodeStep};

const OLD: &[(&str, &str)] = &[
    (
        include_str!("../../tests/golden/pre_step_transforms/run_started.json"),
        include_str!("../../tests/golden/pre_step_transforms/run_started.frame.hex"),
    ),
    (
        include_str!("../../tests/golden/pre_step_transforms/queued.json"),
        include_str!("../../tests/golden/pre_step_transforms/queued.frame.hex"),
    ),
    (
        include_str!("../../tests/golden/pre_step_transforms/steered.json"),
        include_str!("../../tests/golden/pre_step_transforms/steered.frame.hex"),
    ),
    (
        include_str!("../../tests/golden/pre_step_transforms/model_step_finished.json"),
        include_str!("../../tests/golden/pre_step_transforms/model_step_finished.frame.hex"),
    ),
    (
        include_str!("../../tests/golden/pre_step_transforms/tool_dispatch_intent.json"),
        include_str!("../../tests/golden/pre_step_transforms/tool_dispatch_intent.frame.hex"),
    ),
    (
        include_str!("../../tests/golden/pre_step_transforms/tool_no_dispatch.json"),
        include_str!("../../tests/golden/pre_step_transforms/tool_no_dispatch.frame.hex"),
    ),
    (
        include_str!("../../tests/golden/pre_step_transforms/tool_result.json"),
        include_str!("../../tests/golden/pre_step_transforms/tool_result.frame.hex"),
    ),
    (
        include_str!("../../tests/golden/pre_step_transforms/plan_manifest.json"),
        include_str!("../../tests/golden/pre_step_transforms/plan_manifest.frame.hex"),
    ),
];

fn unhex(hex: &str) -> Vec<u8> {
    let hex = hex.trim_end();
    assert_eq!(hex.len() % 2, 0);
    hex.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
        .collect()
}

#[test]
fn pre_step_transform_frames_keep_their_original_bytes() {
    for (json, hex) in OLD {
        let record: WalRecord = serde_json::from_str(json).unwrap();
        assert_eq!(serde_json::to_string(&record).unwrap(), json.trim_end());
        let pinned = unhex(hex);
        assert_eq!(encode(7, 11, &record).unwrap(), pinned);
        let DecodeStep::Record { framed, consumed } = decode_next(&pinned) else {
            panic!("pinned old frame must decode: {json}");
        };
        assert_eq!(consumed, pinned.len());
        assert_eq!(framed.seq, 7);
        assert_eq!(framed.fence, 11);
        assert_eq!(framed.record, record);
    }
}
