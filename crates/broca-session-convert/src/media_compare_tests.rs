//! `media-check`'s comparison of the image engine fed today's records with the engine fed the
//! converted session's media events, over synthetic logs: one whose first-protocol compaction
//! replaced an image, one with images and no compaction, one without images, and a planted
//! difference the comparison must report.

use std::path::{Path, PathBuf};

use broca_protocol::content::ContentBlock;
use broca_provider::media_profile::{select_media_profile, AgingThresholds, MediaQuirk};
use broca_provider::spec::{InputModalities, WireFamilyId};
use broca_session_store::record::Record;
use broca_wal::{FramedRecord, Wal as _, WalRecord};
use serde_json::{json, Value};

use crate::media_compare::{self, Checked};

fn scratch(tag: &str) -> PathBuf {
    let root = broca_test_support::scratch_root().join(format!(
        "session-convert-media-{}-{tag}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn sha256(bytes: &[u8]) -> String {
    crate::verify::sha256_hex(bytes)
}

fn image_ref(bytes: &[u8]) -> Value {
    json!({"kind": "image", "mime": "image/png", "byte_size": bytes.len(), "width": 1,
        "height": 1, "sha256": sha256(bytes)})
}

/// A message's image block holding `bytes`.
fn image(bytes: &[u8]) -> Value {
    let mut block = image_ref(bytes);
    block["type"] = json!("media");
    block
}

fn text(text: &str) -> Value {
    json!({"type": "text", "text": text})
}

fn record(value: Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

/// A run start whose frozen render config renders through today's Anthropic family, with media
/// limits under which every unprotected image ages out at the first boundary that sees one.
fn run_started(run: &str, at: u64, content: Vec<Value>) -> WalRecord {
    let mut frozen = broca_provider::families::anthropic_frozen_config("claude-test");
    let mut limits = select_media_profile(
        WireFamilyId::AnthropicMessages,
        Some(MediaQuirk::Anthropic),
        "claude-test",
        InputModalities::from_value(&json!(["image"])),
        Some(1_000_000),
    );
    limits.aging = AgingThresholds {
        byte_trigger: 1,
        byte_target: 0,
        // Far above any test's tokens, and exact in the canonical JSON setup parts are kept in.
        token_trigger: 1 << 40,
        token_target: 1 << 40,
    };
    frozen.media = Some(limits);
    let mut start = record(json!({
        "type": "run_started", "run_id": run, "ts_ms": at,
        "session": {"project_root": "/project", "harness": "runner", "session": "s"},
        "config": {
            "model": {"provider_module_id": "anthropic", "model_id": "claude-test"},
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0,
            "renderer_version": frozen.renderer_version,
            "frozen_render_config": serde_json::to_value(&frozen).unwrap(),
        },
        "input": [{"role": "user", "content": content}],
        "origin": {"kind": "fresh"},
    }));
    if let WalRecord::RunStarted { config, .. } = &mut start {
        config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
    }
    start
}

fn attempt(step: u64, at: u64) -> WalRecord {
    record(json!({
        "type": "model_attempt_finished", "step_id": step, "attempt": 0,
        "outcome": "complete", "ts_ms": at, "request_body_len": 1,
        "request_body_sha256": sha256(b"request"), "response_body_len": 0,
        "response_body_sha256": sha256(b""),
    }))
}

fn answered(step: u64, content: Value, finish: &str) -> WalRecord {
    record(json!({
        "type": "model_step_finished", "step_id": step,
        "assistant_message": {"message_id": format!("m{step}"), "content": content},
        "usage": {"input_tokens": 3, "output_tokens": 1},
        "finish_reason": finish, "retries_used": 0,
    }))
}

fn finished(at: u64) -> WalRecord {
    record(json!({
        "type": "run_finished", "reason": "completed", "ts_ms": at,
        "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0,
    }))
}

/// One step answering in text.
fn text_step(step: u64, at: u64) -> Vec<WalRecord> {
    vec![
        record(json!({"type": "step_started", "step_id": step})),
        attempt(step, at),
        answered(step, json!([text("Seen.")]), "stop"),
    ]
}

/// `records` numbered from 1, with the lineage of a WAL opened for them: today's message ids carry
/// it. The records aren't written to it: a WAL refuses records holding media unless it is
/// configured to write the media feature, which this test doesn't set up, and the conversion
/// reads only the records and the lineage.
fn wal(root: &Path, records: Vec<WalRecord>) -> (Vec<FramedRecord>, Option<String>) {
    let wal = broca_wal::FileWal::open(root.join("session.wal")).unwrap();
    let lineage = wal.lineage_id().map(|id| id.to_string());
    drop(wal);
    let framed = (1u64..)
        .zip(records)
        .map(|(seq, record)| FramedRecord {
            seq,
            fence: 1,
            record,
        })
        .collect();
    (framed, lineage)
}

/// Three images: a person's in the first run, a tool's screenshot, and a person's in the second
/// run. With `compaction`, a first-protocol compaction between the runs replaces the first run's
/// input with a summary that shows the person's image again.
fn images_log(compaction: bool) -> Vec<WalRecord> {
    let (person, shot, later): (&[u8], &[u8], &[u8]) =
        (b"a person's photo", b"a screenshot", b"a later photo");
    let mut records = vec![
        run_started("r1", 1_000, vec![text("Look."), image(person)]),
        record(json!({"type": "step_started", "step_id": 1})),
        attempt(1, 1_001),
        answered(
            1,
            json!([{"type": "tool_call", "tool_call_id": "c1", "tool_name": "photo",
                "input": {}}]),
            "tool_calls",
        ),
        record(
            json!({"type": "tool_batch_started", "batch_id": "b1", "step_id": 1,
            "planned_calls": ["c1"]}),
        ),
        record(
            json!({"type": "tool_dispatch_intent", "batch_id": "b1", "tool_call_id": "c1",
            "args": {}}),
        ),
        record(
            json!({"type": "tool_result", "batch_id": "b1", "result": {"tool_call_id": "c1",
            "output": {"kind": "parts", "parts": [{"kind": "text", "text": "the screen"},
                {"kind": "media", "media": image_ref(shot)}]}, "is_error": false},
            "outcome": "completed"}),
        ),
        record(json!({"type": "tool_batch_finished", "batch_id": "b1"})),
    ];
    records.extend(text_step(2, 1_002));
    records.push(finished(1_003));
    if compaction {
        records.push(record(json!({
            "type": "compaction_message", "provider": "compactor", "kind": "setup",
            "compaction_id": "media-view", "version": 1, "range": {"from": 0, "to": 1},
            "replacement": [{"role": "user", "content": [text("summary"), image(person)]}],
            "sha256": "fixture",
        })));
    }
    records.push(run_started("r2", 2_000, vec![text("Again."), image(later)]));
    records.extend(text_step(3, 2_001));
    records.push(finished(2_002));
    records
}

/// Two runs without an image.
fn plain_log() -> Vec<WalRecord> {
    let mut records = vec![run_started("r1", 1_000, vec![text("Hello.")])];
    records.extend(text_step(1, 1_001));
    records.push(finished(1_002));
    records.push(run_started("r2", 2_000, vec![text("Again.")]));
    records.extend(text_step(2, 2_001));
    records.push(finished(2_002));
    records
}

/// `media-check` over a log: today's fold against the store's events.
fn checked(records: &[FramedRecord], lineage: Option<&str>) -> Checked {
    let mut converted = crate::convert(records, lineage).unwrap();
    let appends_in_conversation =
        !broca_core::is_mode3_log(records.iter().map(|frame| &frame.record));
    media_compare::check(records, &mut converted, appends_in_conversation).unwrap()
}

/// A first-protocol compaction has no media events: past it the store's engine continues from
/// today's state, so the run after it still compares, and both hold what the compaction decided:
/// the ordinals it covers and the image its replacement shows again.
#[test]
fn a_first_protocol_compaction_session_compares_past_the_compaction() {
    let root = scratch("compaction");
    let records = images_log(true);
    let (records, lineage) = wal(&root, records);
    let checked = checked(&records, lineage.as_deref());
    assert_eq!(checked.differences, Vec::new());
    assert_eq!((checked.runs, checked.images), (2, 3));
    let store = checked
        .store
        .as_ref()
        .expect("the store's engine was compared");
    for state in [&checked.today.state, &store.state] {
        assert_eq!(
            state.covered,
            Some(0),
            "the compaction covers the first run's input"
        );
        assert_eq!(state.replacements.len(), 1);
        assert_eq!(
            state.replacements[0].0[0].sha256,
            sha256(b"a person's photo"),
            "the replacement shows the person's image again"
        );
    }
    assert!(
        !checked.today.aging[0].aged.is_empty(),
        "aging was compared"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A log with images and no compaction: the two folds agree at every run start and at the end,
/// with the aging the limits decide.
#[test]
fn a_session_with_images_agrees_both_ways() {
    let root = scratch("images");
    let records = images_log(false);
    let (records, lineage) = wal(&root, records);
    let checked = checked(&records, lineage.as_deref());
    assert_eq!(checked.differences, Vec::new());
    assert_eq!((checked.runs, checked.images), (2, 3));
    assert!(checked.store.is_some(), "the store's engine was compared");
    assert_eq!(checked.today.state.covered, None);
    assert!(
        !checked.today.aging[0].aged.is_empty(),
        "aging was compared"
    );
    let _ = std::fs::remove_dir_all(&root);
}

/// A log without images: the store keeps no engine, and the scalars its fold keeps agree with
/// today's.
#[test]
fn a_session_without_images_compares_its_step_counter() {
    let root = scratch("plain");
    let (records, lineage) = wal(&root, plain_log());
    let checked = checked(&records, lineage.as_deref());
    assert_eq!(checked.differences, Vec::new());
    assert_eq!((checked.runs, checked.images), (2, 0));
    assert_eq!(checked.store, None);
    assert_eq!(checked.today.state.boundary, 2);
    let _ = std::fs::remove_dir_all(&root);
}

/// A difference planted in the converted records is reported where it shows, by field: the
/// second run's image named by another digest is captured differently at the end, and nothing
/// differs before that run starts.
#[test]
fn a_planted_difference_is_reported_by_field() {
    let root = scratch("planted");
    let records = images_log(false);
    let (records, lineage) = wal(&root, records);
    let mut converted = crate::convert(&records, lineage.as_deref()).unwrap();
    let later = sha256(b"a later photo");
    let mut planted = 0;
    for record in &mut converted.records {
        if let Record::Input(input) = record {
            for block in &mut std::sync::Arc::make_mut(&mut input.message).content {
                if let ContentBlock::Media(reference) = block {
                    if reference.sha256 == later {
                        reference.sha256 = "f".repeat(64);
                        planted += 1;
                    }
                }
            }
        }
    }
    assert_eq!(planted, 1, "the second run's input holds the later image");
    let appends_in_conversation =
        !broca_core::is_mode3_log(records.iter().map(|frame| &frame.record));
    let checked = media_compare::check(&records, &mut converted, appends_in_conversation).unwrap();
    let named: Vec<_> = checked
        .differences
        .iter()
        .map(|difference| (difference.at.as_str(), difference.field.as_str()))
        .collect();
    assert!(named.contains(&("end", "captures")), "{named:?}");
    assert!(
        named.iter().all(|(at, _)| *at == "end"),
        "nothing differs before the second run starts: {named:?}"
    );
    let _ = std::fs::remove_dir_all(&root);
}
