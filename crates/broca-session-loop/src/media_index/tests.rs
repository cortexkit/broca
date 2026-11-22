//! The saved index round trip: an engine driven over a scripted history of the session store's
//! media events, saved through [`to_stored`] and the store's own encoding, and restored through
//! [`from_stored`], holds the same facts and the same aging state, with its limits, its open
//! step and where its media are stored.

use broca_core::media_request::SessionMedia;
use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::media_event::{MediaEvent, MediaInput, MediaLimits, MediaMessage, MediaPart};
use broca_protocol::{StepId, ToolCallId};
use broca_provider::media_profile::{select_media_profile, AgingThresholds, MediaQuirk};
use broca_provider::media_request::MediaAging;
use broca_provider::spec::{InputModalities, WireFamilyId};
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

/// A shown screenshot result: a text part, then the image, in the results message at `ordinal`.
fn tool_result(record: u64, call: &str, ordinal: u64, reference: &MediaRef) -> MediaEvent {
    MediaEvent::ToolResult {
        record,
        call: ToolCallId(call.into()),
        ordinal: Some(ordinal),
        shown: true,
        media: vec![MediaPart {
            part: 1,
            reference: reference.clone(),
        }],
    }
}

/// Feeds `events` to a new engine under the limits the first run froze, as an open session does.
fn fold(events: &[MediaEvent], limits: &MediaLimits) -> (SessionMedia, MediaAging) {
    let mut media = SessionMedia::new(NoBytes);
    let mut aging = MediaAging::default();
    let mut active = serde_json::from_value(limits.0.clone()).unwrap();
    for event in events {
        aging
            .fold_media_event(&mut active, &mut media, event)
            .unwrap();
    }
    (media, aging)
}

/// The number of records [`history`] stands for: its events name records up to this one.
const HISTORY_RECORDS: u64 = 11;

/// A history whose engine holds every kind of state the saved index carries: a person's image
/// protected for the open step, tool images aged out, a batch whose call was planned twice, and
/// an accepted message not placed yet. Each event names the record that states it: the run's
/// start (1), step 1 (2) and its answer at ordinal 1 (3), the batch (4) and its two results in
/// the results message at ordinal 2 (5, 6), the batch's end (7), a steer (8), step 2 (9) that
/// places the steer at ordinal 3, a failed attempt (10), and a second steer (11).
fn history(limits: &MediaLimits) -> Vec<MediaEvent> {
    let first = person(&image("a", 4_000));
    let screens = [image("b", 4_000), image("c", 4_000)];
    vec![
        MediaEvent::RunStart {
            record: 1,
            inputs: vec![MediaInput {
                message: 0,
                ordinal: 0,
                skeleton: MediaMessage::of(&first),
            }],
            limits: Some(limits.clone()),
        },
        MediaEvent::StepStart { step: StepId(1) },
        MediaEvent::StepEnd,
        MediaEvent::BatchStart {
            calls: ["one", "two", "one"]
                .map(|call| ToolCallId(call.into()))
                .into(),
        },
        tool_result(5, "one", 2, &screens[0]),
        tool_result(6, "two", 2, &screens[1]),
        MediaEvent::Accepted {
            record: 8,
            messages: vec![MediaMessage::of(&person(&image("d", 4_000)))],
        },
        MediaEvent::Placed { first: 3 },
        MediaEvent::StepStart { step: StepId(2) },
        MediaEvent::AttemptEnd,
        MediaEvent::Accepted {
            record: 11,
            messages: vec![MediaMessage::of(&person(&image("e", 4_000)))],
        },
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
    let events = history(&limits);
    let (media, aging) = fold(&events, &limits);
    let facts = media.facts();
    let saved_aging = aging.state();
    // The history must hold what the round trip is meant to carry.
    assert!(!facts.protected.is_empty(), "a protected image");
    assert!(
        !facts.pending.is_empty() && !facts.held.is_empty(),
        "an accepted image"
    );
    assert!(!saved_aging.aged.is_empty(), "an aged image");
    assert!(!saved_aging.shown.is_empty());
    let state = media.index_state();
    assert_eq!(
        state.batch.len(),
        3,
        "the batch keeps the position of a call planned twice"
    );

    let objects = objects();
    let index = to_stored(&media, &aging, Some(&limits), true, &objects);
    // Through the store's own encoding, as a seal writes it and an open reads it.
    let index = StoredMediaIndex::decode(&index.encode()).unwrap();
    index.check(HISTORY_RECORDS).unwrap();
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
    let mut events = history(&limits);
    // Up to the attempt, before the second steer.
    events.truncate(10);
    let (media, aging) = fold(&events, &limits);
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
    let (media, aging) = fold(&history(&limits), &limits);
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
