//! The saved index round trip: an engine driven over a scripted history of the old storage's
//! records, saved through [`to_stored`] and the store's own encoding, and restored through
//! [`from_stored`], holds the same facts and the same aging state, with its limits, its open
//! step and where its media are stored.

use broca_core::media_request::SessionMedia;
use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::media_event::MediaLimits;
use broca_provider::media_profile::{select_media_profile, AgingThresholds, MediaQuirk};
use broca_provider::media_request::MediaAging;
use broca_provider::spec::{InputModalities, WireFamilyId};
use broca_records::record::WalRecord;
use broca_session_store::codec::{Id128, Message as _};
use broca_session_store::record::MediaObject;
use broca_session_store::session::StoredMediaIndex;
use serde_json::json;

use super::{from_stored, to_stored};

/// The facts compared read no bytes.
#[derive(Debug)]
struct NoBytes;

impl MediaBytes for NoBytes {
    fn read_verified(&self, _reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        unreachable!("the facts compared read no bytes")
    }
}

fn image(name: &str, size: u64) -> MediaRef {
    serde_json::from_value(json!({
        "kind": "image", "mime": "image/png", "byte_size": size,
        "sha256": name.repeat(64), "width": 8, "height": 8,
    }))
    .unwrap()
}

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

fn person(reference: &MediaRef) -> Message {
    Message::new(
        Role::User,
        vec![
            ContentBlock::Text {
                text: "look".into(),
            },
            ContentBlock::Media(reference.clone()),
        ],
    )
}

fn tool_result(call: &str, reference: &MediaRef) -> WalRecord {
    record(
        json!({"type": "tool_result", "batch_id": "batch", "outcome": "completed",
        "result": {"tool_call_id": call, "is_error": false, "output": {"kind": "parts",
            "parts": [{"kind": "text", "text": "screen"},
                      {"kind": "media", "media": reference}]}}}),
    )
}

fn step(n: u64) -> WalRecord {
    WalRecord::StepStarted {
        step_id: broca_protocol::StepId(n),
    }
}

/// Folds `records` through the old storage's entry point, under the limits each run froze.
fn fold(records: &[WalRecord], limits: &MediaLimits) -> (SessionMedia, MediaAging) {
    let mut media = SessionMedia::new(NoBytes);
    let mut aging = MediaAging::default();
    let mut active = serde_json::from_value(limits.0.clone()).unwrap();
    for record in records {
        aging
            .fold_wal_record(&mut active, &mut media, record)
            .unwrap();
    }
    (media, aging)
}

/// A history whose engine holds every kind of state the saved index carries: a person's image
/// protected for the open step, tool images aged out, a batch whose call was planned twice, an
/// image a first-protocol compaction replaced (so its selection is saved as it stands), and an
/// accepted message not placed yet.
fn history(limits: &MediaLimits) -> Vec<WalRecord> {
    let first = image("a", 4_000);
    let screens = [image("b", 4_000), image("c", 4_000)];
    let steered = image("d", 4_000);
    let held = image("e", 4_000);
    let replacement = Message::new(
        Role::User,
        vec![
            ContentBlock::Text {
                text: "summary".into(),
            },
            ContentBlock::Media(first.clone()),
        ],
    );
    vec![
        record(json!({
            "type": "run_started", "run_id": "run",
            "session": {"project_root": "/fixture", "harness": "media", "session": "session"},
            "config": {"model": broca_protocol::ModelHandle::new("anthropic", "claude"),
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0, "renderer_version": 1,
                "frozen_render_config": {"media": limits.0}},
            "input": [person(&first)], "origin": {"kind": "fresh"}
        })),
        step(1),
        record(json!({"type": "model_step_finished", "step_id": 1,
            "assistant_message": {"message_id": "answer1", "content": [
                {"type": "tool_call", "tool_call_id": "one", "tool_name": "screen", "input": {}},
                {"type": "tool_call", "tool_call_id": "two", "tool_name": "screen", "input": {}}]},
            "usage": broca_protocol::Usage::default()})),
        record(
            json!({"type": "tool_batch_started", "step_id": 1, "batch_id": "batch",
            "planned_calls": ["one", "two", "one"]}),
        ),
        tool_result("one", &screens[0]),
        tool_result("two", &screens[1]),
        record(json!({"type": "tool_batch_finished", "batch_id": "batch"})),
        record(json!({"type": "steered", "input": [person(&steered)]})),
        step(2),
        record(
            json!({"type": "model_attempt_finished", "step_id": 2, "attempt": 1,
            "outcome": "http_error", "ts_ms": 1, "request_body_len": 0,
            "request_body_sha256": "a", "response_body_len": 0, "response_body_sha256": "b"}),
        ),
        record(json!({
            "type": "compaction_message", "provider": "compactor", "kind": "setup",
            "compaction_id": "media-view", "version": 1, "range": {"from": 0, "to": 1},
            "replacement": [replacement], "sha256": "fixture"
        })),
        record(json!({"type": "steered", "input": [person(&held)]})),
    ]
}

fn limits() -> MediaLimits {
    let mut profile = select_media_profile(
        WireFamilyId::AnthropicMessages,
        Some(MediaQuirk::Anthropic),
        "claude",
        InputModalities::from_value(&json!(["image", "pdf"])),
        Some(1_000_000),
    );
    // Every unprotected image ages out at the first boundary that sees one.
    profile.aging = AgingThresholds {
        byte_trigger: 1,
        byte_target: 0,
        token_trigger: u64::MAX,
        token_target: u64::MAX,
    };
    MediaLimits(serde_json::to_value(profile).unwrap())
}

fn objects() -> Vec<(String, MediaObject)> {
    ["a", "b"]
        .iter()
        .enumerate()
        .map(|(n, name)| {
            (
                name.repeat(64),
                MediaObject {
                    part: n as u32,
                    log: Id128([1; 16]),
                    generation: Id128([2; 16]),
                    seq: 3 + n as u64,
                },
            )
        })
        .collect()
}

#[test]
fn a_saved_index_restores_the_engine_it_was_saved_from() {
    let limits = limits();
    let records = history(&limits);
    let (media, aging) = fold(&records, &limits);
    let facts = media.facts();
    let saved_aging = aging.state();
    // The history must hold what the round trip is meant to carry.
    assert!(!facts.protected.is_empty(), "a protected image");
    assert!(
        !facts.pending.is_empty() && !facts.held.is_empty(),
        "an accepted image"
    );
    assert!(!facts.replacements.is_empty(), "a replaced image");
    assert_eq!(facts.covered, Some(0));
    assert!(!saved_aging.aged.is_empty(), "an aged image");
    assert!(!saved_aging.shown.is_empty());
    let state = media.index_state();
    assert!(
        state.selection.is_some(),
        "a selection a compaction decided"
    );
    assert_eq!(
        state.batch.len(),
        3,
        "the batch keeps the position of a call planned twice"
    );

    let objects = objects();
    let index = to_stored(&media, &aging, Some(&limits), true, &objects);
    // Through the store's own encoding, as a seal writes it and an open reads it.
    let index = StoredMediaIndex::decode(&index.encode()).unwrap();
    index.check(records.len() as u64).unwrap();
    assert!(!index.holds_no_media());

    let restored = from_stored(&index).unwrap();
    assert_eq!(restored.media, state);
    assert_eq!(restored.limits.as_ref(), Some(&limits));
    assert!(restored.step_open);
    assert_eq!(restored.objects, objects);
    let engine = SessionMedia::from_index_state(restored.media, NoBytes).unwrap();
    assert_eq!(engine.facts(), facts);
    assert_eq!(engine.index_state(), state);
    let restored_aging = MediaAging::from_state(restored.aging).unwrap();
    assert_eq!(restored_aging.state(), saved_aging);
    assert_eq!(restored_aging.totals(), aging.totals());
}

#[test]
fn an_engine_without_a_compaction_saves_no_selection() {
    let limits = limits();
    let mut records = history(&limits);
    // Up to the attempt: no compaction yet.
    records.truncate(10);
    let (media, aging) = fold(&records, &limits);
    let index = to_stored(&media, &aging, None, false, &[]);
    assert_eq!(index.selection, None);
    assert_eq!(index.covered, None);
    assert_eq!(index.limits, None);
    let restored = from_stored(&index).unwrap();
    assert!(!restored.step_open);
    let engine = SessionMedia::from_index_state(restored.media, NoBytes).unwrap();
    assert_eq!(engine.facts(), media.facts());
    assert_eq!(
        MediaAging::from_state(restored.aging).unwrap().state(),
        aging.state()
    );
}

#[test]
fn a_damaged_index_is_refused_not_restored() {
    let limits = limits();
    let records = history(&limits);
    let (media, aging) = fold(&records, &limits);
    let index = to_stored(&media, &aging, Some(&limits), true, &[]);

    let mut named = index.clone();
    named.protected[0].record += 1_000;
    let restored = from_stored(&named).unwrap();
    assert!(SessionMedia::from_index_state(restored.media, NoBytes).is_err());

    let mut totals = index.clone();
    totals.aging.bytes += 1;
    assert!(MediaAging::from_state(from_stored(&totals).unwrap().aging).is_err());

    let mut slot = index;
    slot.placed[0].skeleton.slots[0].block = slot.placed[0].skeleton.blocks;
    assert!(SessionMedia::from_index_state(from_stored(&slot).unwrap().media, NoBytes).is_err());
}
