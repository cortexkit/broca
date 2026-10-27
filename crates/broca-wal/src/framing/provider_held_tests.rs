//! `provider-held/v1` on the write path: a run that is never provider-held writes the same
//! ordinary frames as before the feature existed and never raises the feature floor, while a
//! held run gates exactly its `ToolCallProviderHeld` and `RunPaused {tool_held}` frames and
//! raises the floor before the first of them reaches the file.

use super::*;

const RUN_STARTED: &str = r#"{"type":"run_started","run_id":"r","session":{"project_root":"/tmp/p","harness":"h","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"hello"}]}],"origin":{"kind":"fresh"}}"#;

/// One run with two tool batches (the second dispatched one call at a time, as a sequential
/// batch is), an auth pause and a finish: every record kind a never-held tool run writes,
/// as JSON exactly as builds before `provider-held/v1` wrote it.
const NEVER_HELD_RUN: &[&str] = &[
    RUN_STARTED,
    r#"{"type":"step_started","step_id":1}"#,
    r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"t1","tool_name":"ask","input":{"q":"a"}},{"type":"tool_call","tool_call_id":"t2","tool_name":"edit","input":{"f":"a"}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}"#,
    r#"{"type":"tool_batch_started","batch_id":"r-b1","step_id":1,"planned_calls":["t1","t2"]}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"ask","args":{"q":"a"}}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t2","tool_name":"edit","args":{"f":"a"}}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"yes"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t2","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"tool_batch_finished","batch_id":"r-b1"}"#,
    r#"{"type":"turn_finished","step_id":1}"#,
    r#"{"type":"run_paused","run_id":"r","reason":"auth_required"}"#,
    r#"{"type":"step_started","step_id":2}"#,
    r#"{"type":"model_step_finished","step_id":2,"assistant_message":{"message_id":"m2","content":[{"type":"tool_call","tool_call_id":"t3","tool_name":"edit","input":{"f":"b"}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}"#,
    r#"{"type":"tool_batch_started","batch_id":"r-b2","step_id":2,"planned_calls":["t3"]}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b2","tool_call_id":"t3","tool_name":"edit","args":{"f":"b"}}"#,
    r#"{"type":"tool_result","batch_id":"r-b2","result":{"tool_call_id":"t3","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"tool_batch_finished","batch_id":"r-b2"}"#,
    r#"{"type":"turn_finished","step_id":2}"#,
    r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
];

/// The same first batch with `t1` held by its provider: the run pauses `tool_held`, and the
/// late result later arrives as an ordinary `ToolResult` under the original batch.
const HELD_RUN: &[&str] = &[
    RUN_STARTED,
    r#"{"type":"step_started","step_id":1}"#,
    r#"{"type":"model_step_finished","step_id":1,"assistant_message":{"message_id":"m1","content":[{"type":"tool_call","tool_call_id":"t1","tool_name":"ask","input":{"q":"a"}},{"type":"tool_call","tool_call_id":"t2","tool_name":"edit","input":{"f":"a"}}]},"usage":{},"finish_reason":"tool_calls","retries_used":0}"#,
    r#"{"type":"tool_batch_started","batch_id":"r-b1","step_id":1,"planned_calls":["t1","t2"]}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t1","tool_name":"ask","args":{"q":"a"}}"#,
    r#"{"type":"tool_dispatch_intent","batch_id":"r-b1","tool_call_id":"t2","tool_name":"edit","args":{"f":"a"}}"#,
    r#"{"type":"tool_call_provider_held","batch_id":"r-b1","tool_call_id":"t1","call_key":"lin_9:5","module":"core","reason":"parent_answer","held_at_ms":1761220000000,"sequential":false}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t2","output":{"kind":"text","text":"done"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"run_paused","run_id":"r","ts_ms":1761220000001,"reason":"tool_held"}"#,
    r#"{"type":"tool_result","batch_id":"r-b1","result":{"tool_call_id":"t1","output":{"kind":"text","text":"yes"},"is_error":false},"outcome":"completed"}"#,
    r#"{"type":"tool_batch_finished","batch_id":"r-b1"}"#,
    r#"{"type":"turn_finished","step_id":1}"#,
    r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#,
];

fn records(lines: &[&str]) -> Vec<WalRecord> {
    lines
        .iter()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}

fn scratch(tag: &str) -> std::path::PathBuf {
    broca_test_support::scratch_root().join(format!(
        "provider-held-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

/// Each frame of a never-held run is an ordinary version-1 frame whose payload is the
/// record's JSON exactly as written before `provider-held/v1` existed, and appending the whole
/// run with a feature floor configured never creates the floor.
#[test]
fn a_never_held_run_writes_todays_frames_and_never_raises_the_floor() {
    use crate::{FileWal, Wal};
    for (seq, line) in NEVER_HELD_RUN.iter().enumerate() {
        let record: WalRecord = serde_json::from_str(line).unwrap();
        assert!(record.required_features().is_empty(), "{line}");
        let bytes = encode(seq as u64 + 1, 3, &record).unwrap();
        assert_eq!(bytes[4], FRAMING_VERSION, "{line}");
        assert_eq!(&bytes[HEADER_LEN..], line.as_bytes());
    }
    let root = scratch("never-held");
    let path = root.join("session.wal");
    let floor = root.join(crate::FEATURE_FLOOR_FILENAME);
    let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
    for record in records(NEVER_HELD_RUN) {
        wal.append(1, &record, true).unwrap();
    }
    assert!(
        !floor.exists(),
        "a run that is never provider-held must not raise the feature floor"
    );
    let replayed: Vec<WalRecord> = wal
        .replay()
        .unwrap()
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    assert_eq!(replayed, records(NEVER_HELD_RUN));
}

/// In a held run only the held record and the `tool_held` pause need `provider-held/v1`:
/// every other frame, the held call's late `ToolResult` included, stays an ordinary frame that
/// a build without the feature still reads. This build lists the feature, so it reads the two
/// gated frames too, and a reader whose table lacks the name refuses exactly those two.
#[test]
fn a_held_run_gates_only_the_held_record_and_its_pause() {
    assert!(KNOWN_FEATURES.contains(&PROVIDER_HELD_FEATURE));
    let mut gated = Vec::new();
    for (seq, record) in records(HELD_RUN).iter().enumerate() {
        let bytes = encode(seq as u64 + 1, 3, record).unwrap();
        match decode_next(&bytes) {
            DecodeStep::Record { framed, .. } => assert_eq!(&framed.record, record),
            _ => panic!("frame {seq} must decode in a build that lists the feature"),
        }
        if bytes[4] == GATED_FRAMING_VERSION {
            assert_eq!(record.required_features(), [PROVIDER_HELD_FEATURE]);
            gated.push(seq);
        } else {
            assert_eq!(bytes[4], FRAMING_VERSION);
        }
    }
    let kinds: Vec<&str> = gated.iter().map(|at| HELD_RUN[*at]).collect();
    assert_eq!(kinds, [HELD_RUN[6], HELD_RUN[8]]);
}

/// The floor names `provider-held/v1` before the first gated frame is on disk: with no floor
/// configured, or a floor that cannot be raised, the append is refused and the file is left
/// exactly as it was. Once the floor is raised the frame is written and this build, which
/// lists the feature, reads it back.
#[test]
fn the_floor_is_raised_before_the_first_provider_held_frame() {
    use crate::{FileWal, Wal, WalError};
    let all = records(HELD_RUN);
    let (prefix, rest) = all.split_at(6);
    let held = &rest[0];
    let root = scratch("floor");
    let path = root.join("session.wal");

    let mut wal = FileWal::open(&path).unwrap();
    for record in prefix {
        wal.append(1, record, true).unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    assert!(matches!(
        wal.append(1, held, true),
        Err(WalError::FeatureFloor { path: None, .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    drop(wal);

    // A floor whose directory is a regular file cannot be written.
    let blocker = root.join("not-a-directory");
    std::fs::write(&blocker, b"").unwrap();
    let mut wal = FileWal::open(&path)
        .unwrap()
        .with_feature_floor(blocker.join(crate::FEATURE_FLOOR_FILENAME));
    assert!(matches!(
        wal.append(1, held, true),
        Err(WalError::FeatureFloor { path: Some(_), .. })
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before);
    drop(wal);

    let floor = root.join(crate::FEATURE_FLOOR_FILENAME);
    let mut wal = FileWal::open(&path).unwrap().with_feature_floor(&floor);
    assert!(!floor.exists());
    wal.append(1, held, true).unwrap();
    assert_eq!(
        crate::floor::read_floor(&floor).unwrap(),
        std::collections::BTreeSet::from([PROVIDER_HELD_FEATURE.to_owned()])
    );
    let after = std::fs::read(&path).unwrap();
    assert!(after.len() > before.len() && after.starts_with(&before));
    let replayed: Vec<WalRecord> = wal
        .replay()
        .unwrap()
        .into_iter()
        .map(|framed| framed.record)
        .collect();
    assert_eq!(replayed.last(), Some(held));
}
