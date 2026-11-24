use super::*;
use crate::{
    media_profile::{select_media_profile, AgingThresholds, DimensionLimit},
    spec::{InputModalities, WireFamilyId},
};
use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::media_event::{MediaEvent, MediaInput, MediaMessage, MediaPart};
use broca_protocol::{Role, StepId, ToolCallId};
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

#[cfg(feature = "test-attachments")]
mod render_tests;

/// A session's media objects held in memory, by digest. A read returns an object's bytes only
/// when their size and SHA-256 match the `MediaRef` asked for, as the session store checks its
/// media objects.
#[derive(Debug, Clone, Default)]
struct Objects(Arc<Mutex<BTreeMap<String, Vec<u8>>>>);

impl MediaBytes for Objects {
    fn read_verified(&self, reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        let bytes = self
            .0
            .lock()
            .unwrap()
            .get(&reference.sha256)
            .cloned()
            .ok_or_else(|| MediaReadError::Missing(reference.sha256.clone()))?;
        let matches = bytes.len() as u64 == reference.byte_size && {
            use sha2::Digest as _;
            format!("{:x}", sha2::Sha256::digest(&bytes)) == reference.sha256
        };
        if matches {
            Ok(bytes)
        } else {
            Err(MediaReadError::Corrupt(reference.sha256.clone()))
        }
    }
}

impl std::panic::RefUnwindSafe for Objects {}

struct Fixture {
    store: Objects,
    media: MediaRef,
    bytes: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        let store = Objects::default();
        let png = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
        let bytes: Vec<_> = (0..png.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&png[i..i + 2], 16).unwrap())
            .collect();
        let media = broca_core::media_validation::validate_media(&bytes, "image/png").unwrap();
        store
            .0
            .lock()
            .unwrap()
            .insert(media.sha256.clone(), bytes.clone());
        Self {
            store,
            media,
            bytes,
        }
    }

    /// The engine's state after the history's events so far.
    fn session(&self, history: &mut History) -> SessionMedia {
        let mut state = SessionMedia::new(self.store.clone());
        history.feed(&mut state);
        state
    }

    fn person(&self) -> Message {
        Message::new(Role::User, vec![ContentBlock::Media(self.media.clone())])
    }

    fn tool(&self, call: &str) -> Message {
        Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(broca_protocol::ToolResult {
                tool_call_id: broca_protocol::ToolCallId(call.into()),
                output: ToolOutput::Parts {
                    parts: vec![ToolOutputPart::Media {
                        media: self.media.clone(),
                    }],
                },
                is_error: false,
            })],
        )
    }
}

fn profile() -> MediaProfile {
    select_media_profile(
        WireFamilyId::AnthropicMessages,
        Some(crate::media_profile::MediaQuirk::Anthropic),
        "claude",
        InputModalities::from_value(&json!(["image", "pdf", "audio", "video"])),
        Some(1_000_000),
    )
}

/// A session's history as the session store states it to the media engine: one event per fact,
/// with each record numbered from 1 in the order the store writes it (a capture is named by its
/// record) and each message's place in the conversation worked out as messages are placed.
#[derive(Default)]
struct History {
    events: Vec<MediaEvent>,
    /// Events already fed to an engine by [`History::feed`].
    fed: usize,
    records: u64,
    /// The number of messages placed so far: the next message's ordinal.
    placed: u64,
    /// Messages accepted into the running run and not placed yet.
    held: u64,
    /// The ordinal of the answer whose tool calls wait for their results.
    waiting: Option<u64>,
}

impl History {
    fn record(&mut self) -> u64 {
        self.records += 1;
        self.records
    }

    /// A run starts, its input placed at the end of the conversation.
    fn start(&mut self, input: &[Message]) -> &mut Self {
        let record = self.record();
        let inputs = input
            .iter()
            .enumerate()
            .map(|(message, value)| MediaInput {
                message,
                ordinal: self.placed + message as u64,
                skeleton: MediaMessage::of(value),
            })
            .filter(|input| input.skeleton.holds_media())
            .collect();
        self.placed += input.len() as u64;
        self.held = 0;
        self.waiting = None;
        self.events.push(MediaEvent::RunStart {
            record,
            inputs,
            limits: None,
        });
        self
    }

    /// Model step `n` starts. The messages accepted before it are placed first.
    fn step(&mut self, n: u64) -> &mut Self {
        self.record();
        if self.held > 0 {
            self.events.push(MediaEvent::Placed { first: self.placed });
            self.placed += self.held;
            self.held = 0;
        }
        self.events.push(MediaEvent::StepStart { step: StepId(n) });
        self
    }

    /// A person's messages join the running run; they are placed at the next step.
    fn steer(&mut self, input: &[Message]) -> &mut Self {
        let record = self.record();
        self.held += input.len() as u64;
        self.events.push(MediaEvent::Accepted {
            record,
            messages: input.iter().map(MediaMessage::of).collect(),
        });
        self
    }

    /// The model step ends with its answer placed. An answer that calls `call` starts a batch
    /// for it.
    fn finished(&mut self, call: Option<&str>) -> &mut Self {
        self.record();
        let answer = self.placed;
        self.placed += 1;
        self.events.push(MediaEvent::StepEnd);
        if let Some(call) = call {
            self.record();
            self.waiting = Some(answer);
            self.events.push(MediaEvent::BatchStart {
                calls: vec![ToolCallId(call.into())],
            });
        }
        self
    }

    /// The tool result `message` holds. It sits in the results message after the answer
    /// waiting for it, if one is.
    fn tool(&mut self, message: &Message) -> &mut Self {
        let record = self.record();
        let ContentBlock::ToolResult(result) = &message.content[0] else {
            unreachable!()
        };
        let ToolOutput::Parts { parts } = &result.output else {
            unreachable!()
        };
        let media = parts
            .iter()
            .enumerate()
            .filter_map(|(part, value)| match value {
                ToolOutputPart::Media { media } => Some(MediaPart {
                    part,
                    reference: media.clone(),
                }),
                ToolOutputPart::Text { .. } => None,
            })
            .collect();
        self.events.push(MediaEvent::ToolResult {
            record,
            call: result.tool_call_id.clone(),
            ordinal: self.waiting.map(|answer| answer + 1),
            shown: true,
            media,
        });
        self
    }

    /// The batch ends, and its results message is placed after the answer.
    fn batch_end(&mut self) -> &mut Self {
        self.record();
        if self.waiting.take().is_some() {
            self.placed += 1;
        }
        self
    }

    /// Applies the events not fed yet to `session`.
    fn feed(&mut self, session: &mut SessionMedia) {
        for event in &self.events[self.fed..] {
            session.apply(event).unwrap();
        }
        self.fed = self.events.len();
    }
}

fn planned(
    session: &SessionMedia,
    profile: &MediaProfile,
    prompt: &[Message],
    state: &mut MediaAging,
) -> (PreparedBundle, Vec<AgingEvent>) {
    let events = state.select_view(profile, &session.bindings(prompt));
    (
        prepare(session, Some(profile), prompt, state, &Default::default()).unwrap(),
        events,
    )
}

fn bytes(bundle: &PreparedBundle) -> Vec<u8> {
    // The preparation contract's output, not an approximation of a provider request.
    serde_json::to_vec(&json!({"prompt":bundle.prompt,"payloads":bundle.media.iter().map(|media| &media.base64).collect::<Vec<_>>()})).unwrap()
}

fn reasons(bundle: &PreparedBundle, reason: &str) -> usize {
    serde_json::to_string(&bundle.prompt)
        .unwrap()
        .matches(&format!("reason={reason}"))
        .count()
}

#[test]
fn fresh_protected_bundle_over_trigger_is_full_on_retry_and_every_resume_cut() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = STANDARD.encode(&fixture.bytes).len() as u64;
    profile.aging = AgingThresholds {
        byte_trigger: sb,
        byte_target: sb / 2,
        token_trigger: 100,
        token_target: 50,
    };
    let input = vec![fixture.person(), fixture.person(), fixture.person()];
    let mut prompt = input.clone();
    broca_protocol::content::stamp_ordinals(&mut prompt);
    let mut history = History::default();
    history.start(&input).step(1);
    let mut session = fixture.session(&mut history);
    let mut aging = MediaAging::default();
    let (first, events) = planned(&session, &profile, &prompt, &mut aging);
    assert_eq!(
        first.media.len(),
        3,
        "the entire first-step bundle is protected"
    );
    assert_eq!(first.encoded_media_bytes(), sb * 3);
    assert!(first
        .media
        .iter()
        .all(|media| media.base64 == STANDARD.encode(&fixture.bytes)));
    assert_eq!(
        bytes(
            &prepare(
                &session,
                Some(&profile),
                &prompt,
                &aging,
                &Default::default()
            )
            .unwrap()
        ),
        bytes(&first)
    );
    let mut rebuilt = MediaAging::default();
    for event in events {
        rebuilt.apply(&profile, event);
    }
    assert_eq!(
        bytes(
            &prepare(
                &session,
                Some(&profile),
                &prompt,
                &rebuilt,
                &Default::default()
            )
            .unwrap()
        ),
        bytes(&first),
        "aging rebuilt from its own events"
    );
    history.step(2).feed(&mut session);
    let (later, _) = planned(&session, &profile, &prompt, &mut aging);
    assert_eq!(
        reasons(&later, "aged"),
        3,
        "protection ends only at the following durable boundary"
    );
}

#[test]
fn durable_steer_boundary_protects_occurrences_not_shared_digests() {
    let fixture = Fixture::new();
    let mut history = History::default();
    history
        .start(&[fixture.person()])
        .step(1)
        .steer(&[fixture.person()]);
    let mut session = fixture.session(&mut history);
    let mut prompt = vec![fixture.person(), fixture.person()];
    broca_protocol::content::stamp_ordinals(&mut prompt);
    history.step(2).feed(&mut session);
    let bindings = session.bindings(&prompt);
    assert_eq!(bindings.len(), 2);
    assert!(!bindings[0].protected);
    assert!(bindings[1].protected);
    assert_ne!(bindings[0].id, bindings[1].id);
    history.step(2).feed(&mut session);
    assert!(
        session.bindings(&prompt)[1].protected,
        "retry cannot end input protection"
    );
    history.step(3).feed(&mut session);
    assert!(session
        .bindings(&prompt)
        .iter()
        .all(|binding| !binding.protected));
}

#[test]
fn later_text_pressure_caps_person_media_below_aging_thresholds() {
    let fixture = Fixture::new();
    let session = fixture.session(
        History::default()
            .start(&[fixture.person()])
            .step(1)
            .step(2),
    );
    let mut prompt = vec![
        fixture.person(),
        Message::new(
            Role::Tool,
            vec![ContentBlock::Text {
                text: "a later text-only tool result".into(),
            }],
        ),
    ];
    broca_protocol::content::stamp_ordinals(&mut prompt);
    let profile = profile();
    let mut aging = MediaAging::default();
    let (mut first, _) = planned(&session, &profile, &prompt, &mut aging);
    assert_eq!(first.media.len(), 1);
    let before = bytes(&first).len() as u64;
    let mut limited = profile.clone();
    // Text and wrappers can consume almost the entire request even when media are far below Cb.
    limited.request_cap_bytes = before - 1;
    fit_request(&limited, &mut first, |bundle| bytes(bundle).len() as u64).unwrap();
    assert_eq!(reasons(&first, "request_cap"), 1);
    assert_eq!(reasons(&first, "aged"), 0);
    let mut retry = prepare(
        &session,
        Some(&profile),
        &prompt,
        &aging,
        &Default::default(),
    )
    .unwrap();
    fit_request(&limited, &mut retry, |bundle| bytes(bundle).len() as u64).unwrap();
    assert_eq!(bytes(&retry), bytes(&first));
}

#[test]
fn limits_remove_oldest_tools_before_people_and_exhaust_before_refusing() {
    let fixture = Fixture::new();
    let person = fixture.person();
    let old_tool = fixture.tool("old");
    let new_tool = fixture.tool("new");
    let session = fixture.session(
        History::default()
            .start(std::slice::from_ref(&person))
            .step(1)
            .step(2)
            .tool(&old_tool)
            .tool(&new_tool),
    );
    let mut prompt = vec![person, old_tool, new_tool];
    prompt[0].ordinal = Some(0);
    let profile = profile();
    let (mut bundle, _) = planned(&session, &profile, &prompt, &mut MediaAging::default());
    assert_eq!(bundle.media.len(), 3);
    let sb = bundle.media[0].encoded_bytes();
    let mut limited = profile.clone();
    limited.request_cap_bytes = sb * 2;
    fit_request(&limited, &mut bundle, PreparedBundle::encoded_media_bytes).unwrap();
    assert_eq!(bundle.media.len(), 2);
    assert_eq!(bundle.media.iter().filter(|media| media.tool).count(), 1);
    assert_eq!(
        bundle.media[1].location.message, 2,
        "oldest tool goes first even though the person input is older"
    );
    let err = fit_request(&limited, &mut bundle, |_| limited.request_cap_bytes + 1).unwrap_err();
    assert_eq!(err.code(), "request_too_large");
    assert!(
        bundle.media.is_empty(),
        "all unprotected media must be marked before refusal"
    );
    assert_eq!(reasons(&bundle, "request_cap"), 3);
}

#[test]
fn count_dependent_dimensions_stop_at_threshold_or_when_large_images_are_gone() {
    let fixture = Fixture::new();
    let mut profile = profile();
    profile.dimension_limits = vec![DimensionLimit {
        above_count: 20,
        max_side_px: 0,
        counts_documents: false,
    }];
    let tools: Vec<_> = (0..21)
        .map(|n| fixture.tool(&format!("screen{n}")))
        .collect();
    let mut history = History::default();
    history.start(&[]).step(1);
    for tool in &tools {
        history.tool(tool);
    }
    let session = fixture.session(&mut history);
    let (bundle, _) = planned(&session, &profile, &tools, &mut MediaAging::default());
    assert_eq!(
        bundle.media.len(),
        20,
        "marking stops when the count-dependent limit no longer applies"
    );
    assert_eq!(reasons(&bundle, "request_cap"), 1);

    profile.dimension_limits[0].max_side_px = 1;
    let (bundle, _) = planned(&session, &profile, &tools, &mut MediaAging::default());
    assert_eq!(
        bundle.media.len(),
        21,
        "above the threshold is valid when every shown image fits the side limit"
    );
    profile.image.max_count = Some(7);
    let (bundle, _) = planned(&session, &profile, &tools, &mut MediaAging::default());
    assert_eq!(bundle.media.len(), 7);
    assert_eq!(reasons(&bundle, "request_cap"), 14);
}

#[test]
fn post_view_aging_ignores_omitted_media_and_keeps_hysteresis_on_reappearance() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = STANDARD.encode(&fixture.bytes).len() as u64;
    profile.aging = AgingThresholds {
        byte_trigger: sb * 2,
        byte_target: sb,
        token_trigger: 100,
        token_target: 50,
    };
    let tools: Vec<_> = (0..3)
        .map(|n| fixture.tool(&format!("screen{n}")))
        .collect();
    let mut history = History::default();
    history.start(&[]).step(1);
    for tool in &tools {
        history.tool(tool);
    }
    let session = fixture.session(&mut history);
    let mut aging = MediaAging::default();
    let (one, _) = planned(&session, &profile, &tools[2..], &mut aging);
    assert_eq!(
        one.media.len(),
        1,
        "the compaction view, not the raw transcript, supplies aging totals"
    );
    let (all, _) = planned(&session, &profile, &tools, &mut aging);
    assert_eq!(all.media.len(), 1);
    assert_eq!(reasons(&all, "aged"), 2);
    let (hidden, _) = planned(&session, &profile, &tools[2..], &mut aging);
    assert_eq!(hidden.media.len(), 1);
    let (reappeared, _) = planned(&session, &profile, &tools, &mut aging);
    assert_eq!(bytes(&reappeared), bytes(&all));
}

#[test]
fn stored_media_profiles_and_session_scope_fail_closed_without_reading_foreign_bytes() {
    let fixture = Fixture::new();
    let mut prompt = vec![fixture.person()];
    broca_protocol::content::stamp_ordinals(&mut prompt);
    let session = fixture.session(History::default().start(&prompt).step(1));
    let mut profile = profile();
    profile.modalities.image = Some(false);
    assert_eq!(
        reasons(
            &prepare(
                &session,
                Some(&profile),
                &prompt,
                &MediaAging::default(),
                &Default::default()
            )
            .unwrap(),
            "modality"
        ),
        1
    );
    profile.modalities.image = Some(true);
    profile.image.user_message = MediaCarrier::Marker;
    assert_eq!(
        reasons(
            &prepare(
                &session,
                Some(&profile),
                &prompt,
                &MediaAging::default(),
                &Default::default()
            )
            .unwrap(),
            "carrier"
        ),
        1
    );
    profile.image.user_message = MediaCarrier::Native;
    profile.image.max_item_bytes = Some(crate::media_profile::ByteLimit::Decoded(1));
    assert_eq!(
        reasons(
            &prepare(
                &session,
                Some(&profile),
                &prompt,
                &MediaAging::default(),
                &Default::default()
            )
            .unwrap(),
            "too_large"
        ),
        1
    );
    profile.image.max_item_bytes = None;
    let foreign = fixture.session(History::default().start(&[]).step(1));
    assert_eq!(
        reasons(
            &prepare(
                &foreign,
                Some(&profile),
                &prompt,
                &MediaAging::default(),
                &Default::default()
            )
            .unwrap(),
            "provenance"
        ),
        1
    );
    assert!(
        foreign.read(&fixture.media).is_err(),
        "an existing shared-store object does not grant session authority"
    );
    let objects = &fixture.store.0;
    objects
        .lock()
        .unwrap()
        .insert(fixture.media.sha256.clone(), b"corrupt".to_vec());
    let err = prepare(
        &session,
        Some(&profile),
        &prompt,
        &MediaAging::default(),
        &Default::default(),
    )
    .unwrap_err();
    assert_eq!(err.code(), "media_object_corrupt");
    assert!(err.to_string().contains(&fixture.media.sha256));
    objects.lock().unwrap().remove(&fixture.media.sha256);
    assert_eq!(
        prepare(
            &session,
            Some(&profile),
            &prompt,
            &MediaAging::default(),
            &Default::default()
        )
        .unwrap_err()
        .code(),
        "media_object_missing"
    );
}

#[test]
fn forty_screenshots_stay_in_payload_cap_with_bounded_aging_prefix_changes() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = STANDARD.encode(&fixture.bytes).len() as u64;
    let st = profile.media_tokens(&fixture.media);
    profile.request_cap_bytes = sb * 16;
    profile.aging = AgingThresholds {
        byte_trigger: sb * 8,
        byte_target: sb * 4,
        token_trigger: st * 12,
        token_target: st * 6,
    };
    let mut history = History::default();
    let mut session = fixture.session(history.start(&[]));
    let mut prompt = Vec::new();
    let mut aging = MediaAging::default();
    let mut replay = Vec::new();
    let mut prefix_changes = 0;
    let mut previous_aged = 0;
    let mut shows = 0;
    for n in 1..=40 {
        let screenshot = fixture.tool(&format!("screen{n}"));
        history.step(n).tool(&screenshot).feed(&mut session);
        prompt.push(screenshot);
        let (bundle, delta) = planned(&session, &profile, &prompt, &mut aging);
        shows += delta
            .iter()
            .filter(|event| matches!(event, AgingEvent::Show { .. }))
            .count();
        replay.extend(delta);
        assert!(bundle.encoded_media_bytes() <= profile.request_cap_bytes);
        assert_eq!(reasons(&bundle, "request_cap"), 0);
        let aged = reasons(&bundle, "aged");
        if aged != previous_aged {
            prefix_changes += 1;
            previous_aged = aged;
        }
        let mut resumed = MediaAging::default();
        for event in replay.iter().copied() {
            resumed.apply(&profile, event);
        }
        assert_eq!(
            bytes(
                &prepare(
                    &session,
                    Some(&profile),
                    &prompt,
                    &resumed,
                    &Default::default()
                )
                .unwrap()
            ),
            bytes(&bundle),
            "resume step {n}"
        );
    }
    let bound = (40.0
        * (sb as f64 / (profile.aging.byte_trigger - profile.aging.byte_target) as f64)
            .max(st as f64 / (profile.aging.token_trigger - profile.aging.token_target) as f64))
    .ceil() as usize;
    assert!(
        prefix_changes > 0 && prefix_changes <= bound,
        "{prefix_changes} changes exceed {bound}"
    );
    assert_eq!(
        shows, 40,
        "unchanged media are not re-added to running aging totals"
    );
    assert_eq!(
        replay.len(),
        80,
        "one media delta and one boundary per step, not a transcript per step"
    );
}

#[test]
fn independent_byte_and_token_triggers_reduce_to_both_targets() {
    let mut profile = profile();
    profile.aging = AgingThresholds {
        byte_trigger: 100,
        byte_target: 50,
        token_trigger: 100,
        token_target: 50,
    };
    for costs in [
        vec![(60, 1), (60, 1)],
        vec![(1, 60), (1, 60)],
        vec![(80, 1), (1, 80), (30, 30)],
    ] {
        let events: Vec<_> = costs
            .iter()
            .enumerate()
            .map(|(i, (bytes, tokens))| AgingEvent::Show {
                id: Occurrence {
                    capture: MediaId {
                        record: i as u64,
                        message: 0,
                        block: 0,
                        part: None,
                    },
                    repetition: 0,
                },
                cost: MediaCost {
                    bytes: *bytes,
                    tokens: *tokens,
                },
                protected: false,
            })
            .chain([AgingEvent::Boundary])
            .collect();
        let mut state = MediaAging::default();
        for event in events {
            state.apply(&profile, event);
        }
        assert!(state.totals().bytes <= 50 && state.totals().tokens <= 50);
    }
}

#[test]
fn running_aging_follows_each_screenshot_step() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = STANDARD.encode(&fixture.bytes).len() as u64;
    profile.aging = AgingThresholds {
        byte_trigger: sb * 8,
        byte_target: sb * 4,
        token_trigger: 100,
        token_target: 50,
    };
    let mut history = History::default();
    let mut session = fixture.session(history.start(&[]));
    let mut aging = MediaAging::default();
    aging.fold_changes(&profile, session.take_changes());
    let mut prompt = Vec::new();
    for n in 1..=41 {
        history.step(n).feed(&mut session);
        aging.fold_changes(&profile, session.take_changes());
        aging.apply(&profile, AgingEvent::Boundary);
        let live = prepare(
            &session,
            Some(&profile),
            &prompt,
            &aging,
            &Default::default(),
        )
        .unwrap();
        assert!(live.encoded_media_bytes() <= profile.request_cap_bytes);
        if n == 41 {
            break;
        }
        let call = format!("screen{n}");
        let tool = fixture.tool(&call);
        // The step's end is an aging boundary, before the batch it asks for adds its result.
        aging.apply(&profile, AgingEvent::Boundary);
        history
            .finished(Some(&call))
            .tool(&tool)
            .batch_end()
            .feed(&mut session);
        aging.fold_changes(&profile, session.take_changes());
        let answer: Vec<ContentBlock> = serde_json::from_value(
            json!([{"type":"tool_call","tool_call_id":call,"tool_name":"screen","input":{}}]),
        )
        .unwrap();
        prompt.extend([Message::new(Role::Assistant, answer), tool]);
        broca_protocol::content::stamp_ordinals(&mut prompt);
    }
    assert_eq!(
        reasons(
            &prepare(
                &session,
                Some(&profile),
                &prompt,
                &aging,
                &Default::default()
            )
            .unwrap(),
            "aged"
        ),
        35
    );
}

/// A compaction view's inserted image is no capture: it is shown from the bytes the request
/// carries (`CallOptions::view_media`). Without those bytes it renders as its provenance marker,
/// and for a model that reads no images, as its modality marker.
#[test]
fn a_view_inserted_image_is_shown_from_the_requests_view_media() {
    let fixture = Fixture::new();
    let profile = profile();
    let session = fixture.session(&mut History::default());
    let prompt = vec![fixture.person()];
    let mut view = ViewMedia::default();
    view.insert(fixture.media.sha256.clone(), fixture.bytes.clone().into());

    let shown = prepare(
        &session,
        Some(&profile),
        &prompt,
        &MediaAging::default(),
        &view,
    )
    .unwrap();
    assert_eq!(shown.media.len(), 1, "the view's image is shown");
    assert_eq!(shown.media[0].base64, STANDARD.encode(&fixture.bytes));
    assert!(!shown.media[0].protected);
    assert_eq!(reasons(&shown, "provenance"), 0);

    let unknown = prepare(
        &session,
        Some(&profile),
        &prompt,
        &MediaAging::default(),
        &Default::default(),
    )
    .unwrap();
    assert!(unknown.media.is_empty());
    assert_eq!(reasons(&unknown, "provenance"), 1, "no bytes: its marker");

    let text_model = prepare(&session, None, &prompt, &MediaAging::default(), &view).unwrap();
    assert!(text_model.media.is_empty());
    assert_eq!(
        reasons(&text_model, "modality"),
        1,
        "no images read: its marker"
    );
}

/// `png` with a text chunk added before its end: other bytes, still a valid PNG.
fn other_png(png: &[u8]) -> Vec<u8> {
    fn crc(bytes: &[u8]) -> u32 {
        let mut crc = !0u32;
        for &byte in bytes {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                crc = if crc & 1 != 0 {
                    (crc >> 1) ^ 0xedb8_8320
                } else {
                    crc >> 1
                };
            }
        }
        !crc
    }
    let chunk = b"tEXtkey\0view";
    let end = png.len() - 12;
    let mut out = png[..end].to_vec();
    out.extend_from_slice(&((chunk.len() - 4) as u32).to_be_bytes());
    out.extend_from_slice(chunk);
    out.extend_from_slice(&crc(chunk).to_be_bytes());
    out.extend_from_slice(&png[end..]);
    out
}

/// A view's image counts toward the request's limits like the session's own media, and is never
/// protected: a request over its limits drops it instead of being refused.
#[test]
fn request_fitting_drops_a_view_image_beside_a_protected_capture() {
    let fixture = Fixture::new();
    let profile = profile();
    let input = vec![fixture.person()];
    let mut captured = input.clone();
    broca_protocol::content::stamp_ordinals(&mut captured);
    let mut history = History::default();
    history.start(&input).step(1);
    let session = fixture.session(&mut history);
    let drawn = other_png(&fixture.bytes);
    let reference = broca_core::media_validation::validate_media(&drawn, "image/png").unwrap();
    let mut prompt = vec![Message::new(
        Role::User,
        vec![ContentBlock::Media(reference.clone())],
    )];
    prompt.extend(captured);
    let mut view = ViewMedia::default();
    view.insert(reference.sha256.clone(), drawn.clone().into());
    let mut aging = MediaAging::default();
    aging.select_view(&profile, &session.bindings(&prompt));

    let mut bundle = prepare(&session, Some(&profile), &prompt, &aging, &view).unwrap();
    assert_eq!(bundle.media.len(), 2, "the view's image and the person's");
    // Over the cap while both are shown.
    fit_request(&profile, &mut bundle, |bundle| {
        if bundle.media.len() > 1 {
            u64::MAX
        } else {
            0
        }
    })
    .unwrap();
    assert_eq!(bundle.media.len(), 1);
    assert_eq!(
        bundle.media[0].reference, fixture.media,
        "the person's image stays"
    );
    assert_eq!(
        reasons(&bundle, "request_cap"),
        1,
        "the view's image is marked"
    );
}
