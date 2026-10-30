use super::*;
use crate::{
    media_profile::{select_media_profile, AgingThresholds, DimensionLimit},
    spec::{InputModalities, WireFamilyId},
};
use broca_core::media_store::{MediaStore, MediaStoreClass};
use broca_protocol::{BindIdentity, Role};
use broca_wal::WalRecord;
use serde_json::json;
use std::path::PathBuf;

mod checkpoint_compat_tests;
#[cfg(feature = "test-attachments")]
mod live_tests;
mod render_tests;
#[cfg(feature = "test-attachments")]
mod reservation_aging_tests;
#[cfg(feature = "test-attachments")]
mod seed_repair_tests;
#[cfg(feature = "test-attachments")]
mod transport_failure_tests;

struct Fixture {
    root: PathBuf,
    store: MediaStore,
    media: MediaRef,
    bytes: Vec<u8>,
}

impl Fixture {
    fn new() -> Self {
        // `scratch_root` is already private to this process; a counter keeps parallel tests apart.
        // The wall clock cannot: on macOS it ticks in microseconds, so two fixtures created in the
        // same tick shared one directory and the first to drop deleted the other's store.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "media-request-{}",
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        broca_owner_only::create_dir_all(&root).unwrap();
        let store = MediaStore::open(&root, MediaStoreClass::Durable).unwrap();
        let png = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
        let bytes: Vec<_> = (0..png.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&png[i..i + 2], 16).unwrap())
            .collect();
        let media = broca_core::media_validation::validate_media(&bytes, "image/png").unwrap();
        let identity = BindIdentity {
            project_root: root.clone(),
            harness: "media".into(),
            session: "session".into(),
        };
        store
            .session(&identity)
            .unwrap()
            .register_and_append(&media, &bytes, || Ok(()))
            .unwrap();
        Self {
            root,
            store,
            media,
            bytes,
        }
    }

    fn session(&self, records: &[WalRecord]) -> SessionMedia {
        let mut state = SessionMedia::new(self.store.clone());
        for record in records {
            state.fold(record).unwrap();
        }
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

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
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

fn start(input: &[Message]) -> WalRecord {
    serde_json::from_value(json!({
        "type": "run_started", "run_id": "run",
        "session": {"project_root": "/fixture", "harness": "media", "session": "session"},
        "config": {"model": broca_protocol::ModelHandle::new("anthropic", "claude"), "tools": [],
            "tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},
            "context_epoch":0,"renderer_version":1,"frozen_render_config":{}},
        "input":input,"origin":{"kind":"fresh"}
    }))
    .unwrap()
}

fn step(n: u64) -> WalRecord {
    WalRecord::StepStarted {
        step_id: broca_protocol::StepId(n),
    }
}

fn steer(input: &[Message]) -> WalRecord {
    serde_json::from_value(json!({"type":"steered", "input": input})).unwrap()
}

fn tool_record(message: &Message) -> WalRecord {
    let ContentBlock::ToolResult(result) = &message.content[0] else {
        unreachable!()
    };
    serde_json::from_value(
        json!({"type":"tool_result", "batch_id":"batch", "result": result, "outcome":"completed"}),
    )
    .unwrap()
}

fn planned(
    session: &SessionMedia,
    profile: &MediaProfile,
    prompt: &[Message],
    state: &mut MediaAging,
) -> (PreparedBundle, Vec<AgingEvent>) {
    let events = state.select_view(profile, &session.bindings(prompt));
    (
        prepare(session, Some(profile), prompt, state).unwrap(),
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
    let records = vec![start(&input), step(1)];
    let session = fixture.session(&records);
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
        bytes(&prepare(&session, Some(&profile), &prompt, &aging).unwrap()),
        bytes(&first)
    );
    for cut in 0..=records.len() {
        let mut resumed = fixture.session(&records[..cut]);
        for record in &records[cut..] {
            resumed.fold(record).unwrap();
        }
        let rebuilt = MediaAging::replay(&profile, events.iter().copied());
        assert_eq!(
            bytes(&prepare(&resumed, Some(&profile), &prompt, &rebuilt).unwrap()),
            bytes(&first),
            "durable cut {cut}"
        );
    }
    let mut recovered = SessionMedia::new(fixture.store.clone());
    let recovered_aging = MediaAging::replay_wal(&profile, &mut recovered, &records).unwrap();
    assert_eq!(
        bytes(&prepare(&recovered, Some(&profile), &prompt, &recovered_aging).unwrap()),
        bytes(&first)
    );
    let mut next = session;
    next.fold(&step(2)).unwrap();
    let (later, _) = planned(&next, &profile, &prompt, &mut aging);
    assert_eq!(
        reasons(&later, "aged"),
        3,
        "protection ends only at the following durable boundary"
    );
}

#[test]
fn durable_steer_boundary_protects_occurrences_not_shared_digests() {
    let fixture = Fixture::new();
    let mut session = fixture.session(&[
        start(&[fixture.person()]),
        step(1),
        steer(&[fixture.person()]),
    ]);
    let mut prompt = vec![fixture.person(), fixture.person()];
    broca_protocol::content::stamp_ordinals(&mut prompt);
    session.fold(&step(2)).unwrap();
    let bindings = session.bindings(&prompt);
    assert_eq!(bindings.len(), 2);
    assert!(!bindings[0].protected);
    assert!(bindings[1].protected);
    assert_ne!(bindings[0].id, bindings[1].id);
    session.fold(&step(2)).unwrap();
    assert!(
        session.bindings(&prompt)[1].protected,
        "retry cannot end input protection"
    );
    session.fold(&step(3)).unwrap();
    assert!(session
        .bindings(&prompt)
        .iter()
        .all(|binding| !binding.protected));
}

#[test]
fn later_text_pressure_caps_person_media_below_aging_thresholds() {
    let fixture = Fixture::new();
    let session = fixture.session(&[start(&[fixture.person()]), step(1), step(2)]);
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
    let (mut first, events) = planned(&session, &profile, &prompt, &mut aging);
    assert_eq!(first.media.len(), 1);
    let before = bytes(&first).len() as u64;
    let mut limited = profile.clone();
    // Text and wrappers can consume almost the entire request even when media are far below Cb.
    limited.request_cap_bytes = before - 1;
    fit_request(&limited, &mut first, |bundle| bytes(bundle).len() as u64).unwrap();
    assert_eq!(reasons(&first, "request_cap"), 1);
    assert_eq!(reasons(&first, "aged"), 0);
    for state in [MediaAging::replay(&profile, events), aging] {
        let mut retry = prepare(&session, Some(&profile), &prompt, &state).unwrap();
        fit_request(&limited, &mut retry, |bundle| bytes(bundle).len() as u64).unwrap();
        assert_eq!(bytes(&retry), bytes(&first));
    }
}

#[test]
fn limits_remove_oldest_tools_before_people_and_exhaust_before_refusing() {
    let fixture = Fixture::new();
    let person = fixture.person();
    let old_tool = fixture.tool("old");
    let new_tool = fixture.tool("new");
    let session = fixture.session(&[
        start(std::slice::from_ref(&person)),
        step(1),
        step(2),
        tool_record(&old_tool),
        tool_record(&new_tool),
    ]);
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
    let mut records = vec![start(&[]), step(1)];
    records.extend(tools.iter().map(tool_record));
    let session = fixture.session(&records);
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
    let mut records = vec![start(&[]), step(1)];
    records.extend(tools.iter().map(tool_record));
    let session = fixture.session(&records);
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
    let session = fixture.session(&[start(&prompt), step(1)]);
    let mut profile = profile();
    profile.modalities.image = Some(false);
    assert_eq!(
        reasons(
            &prepare(&session, Some(&profile), &prompt, &MediaAging::default()).unwrap(),
            "modality"
        ),
        1
    );
    profile.modalities.image = Some(true);
    profile.image.user_message = MediaCarrier::Marker;
    assert_eq!(
        reasons(
            &prepare(&session, Some(&profile), &prompt, &MediaAging::default()).unwrap(),
            "carrier"
        ),
        1
    );
    profile.image.user_message = MediaCarrier::Native;
    profile.image.max_item_bytes = Some(crate::media_profile::ByteLimit::Decoded(1));
    assert_eq!(
        reasons(
            &prepare(&session, Some(&profile), &prompt, &MediaAging::default()).unwrap(),
            "too_large"
        ),
        1
    );
    profile.image.max_item_bytes = None;
    let foreign = fixture.session(&[start(&[]), step(1)]);
    assert_eq!(
        reasons(
            &prepare(&foreign, Some(&profile), &prompt, &MediaAging::default()).unwrap(),
            "provenance"
        ),
        1
    );
    assert!(
        foreign.read(&fixture.media).is_err(),
        "an existing shared-store object does not grant session authority"
    );
    let object = fixture
        .root
        .join("media-durable/objects")
        .join(&fixture.media.sha256);
    std::fs::write(&object, b"corrupt").unwrap();
    let err = prepare(&session, Some(&profile), &prompt, &MediaAging::default()).unwrap_err();
    assert_eq!(err.code(), "media_object_corrupt");
    assert!(err.to_string().contains(&fixture.media.sha256));
    std::fs::remove_file(object).unwrap();
    assert_eq!(
        prepare(&session, Some(&profile), &prompt, &MediaAging::default())
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
    let mut session = fixture.session(&[start(&[])]);
    let mut prompt = Vec::new();
    let mut aging = MediaAging::default();
    let mut replay = Vec::new();
    let mut prefix_changes = 0;
    let mut previous_aged = 0;
    let mut shows = 0;
    for n in 1..=40 {
        session.fold(&step(n)).unwrap();
        let screenshot = fixture.tool(&format!("screen{n}"));
        session.fold(&tool_record(&screenshot)).unwrap();
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
        let resumed = MediaAging::replay(&profile, replay.iter().copied());
        assert_eq!(
            bytes(&prepare(&session, Some(&profile), &prompt, &resumed).unwrap()),
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
        let state = MediaAging::replay(&profile, events);
        assert!(state.totals().bytes <= 50 && state.totals().tokens <= 50);
    }
}

fn finished(n: u64, call: Option<&str>) -> WalRecord {
    let content = match call {
        Some(call) => {
            json!([{"type":"tool_call","tool_call_id":call,"tool_name":"screen","input":{}}])
        }
        None => json!([{"type":"text","text":"continue"}]),
    };
    serde_json::from_value(json!({"type":"model_step_finished", "step_id":n,
        "assistant_message":{"message_id":format!("answer{n}"),"content":content},
        "usage":broca_protocol::Usage::default()}))
    .unwrap()
}

#[test]
fn durable_wal_replay_rebuilds_running_aging_after_each_screenshot_step() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = STANDARD.encode(&fixture.bytes).len() as u64;
    profile.aging = AgingThresholds {
        byte_trigger: sb * 8,
        byte_target: sb * 4,
        token_trigger: 100,
        token_target: 50,
    };
    let mut session = fixture.session(&[]);
    let mut records = vec![start(&[])];
    session.fold(&records[0]).unwrap();
    let mut aging = MediaAging::default();
    aging.fold_changes(&profile, session.take_changes());
    let mut prompt = Vec::new();
    for n in 1..=41 {
        let begin = step(n);
        session.fold(&begin).unwrap();
        records.push(begin);
        aging.fold_changes(&profile, session.take_changes());
        aging.apply(&profile, AgingEvent::Boundary);
        let live = prepare(&session, Some(&profile), &prompt, &aging).unwrap();
        let durable: Vec<WalRecord> =
            serde_json::from_slice(&serde_json::to_vec(&records).unwrap()).unwrap();
        let mut recovered = SessionMedia::new(fixture.store.clone());
        let rebuilt = MediaAging::replay_wal(&profile, &mut recovered, &durable).unwrap();
        let resumed = prepare(&recovered, Some(&profile), &prompt, &rebuilt).unwrap();
        assert_eq!(
            bytes(&resumed),
            bytes(&live),
            "WAL cut before provider transmission at step {n}"
        );
        assert!(live.encoded_media_bytes() <= profile.request_cap_bytes);
        if n == 41 {
            break;
        }
        let call = format!("screen{n}");
        let finish = finished(n, Some(&call));
        let assistant = if let WalRecord::ModelStepFinished {
            assistant_message, ..
        } = &finish
        {
            Message::new(Role::Assistant, assistant_message.content.clone())
        } else {
            unreachable!()
        };
        let tool = fixture.tool(&call);
        let tail = vec![finish,
            serde_json::from_value(json!({"type":"tool_batch_started","batch_id":"batch","step_id":n,"planned_calls":[call]})).unwrap(),
            tool_record(&tool),
            serde_json::from_value(json!({"type":"tool_batch_finished","batch_id":"batch"})).unwrap(),
        ];
        for record in tail {
            if matches!(record, WalRecord::ModelStepFinished { .. }) {
                aging.apply(&profile, AgingEvent::Boundary);
            }
            session.fold(&record).unwrap();
            aging.fold_changes(&profile, session.take_changes());
            records.push(record);
        }
        prompt.extend([assistant, tool]);
        broca_protocol::content::stamp_ordinals(&mut prompt);
    }
    assert_eq!(
        reasons(
            &prepare(&session, Some(&profile), &prompt, &aging).unwrap(),
            "aged"
        ),
        35
    );
}

#[test]
fn wal_view_drops_and_keep_text_edits_preserve_media_age_and_protection() {
    use broca_core::compaction_provider::{
        lineage::MessageRef,
        view::{CompactionView, PartEdit, ViewRange},
    };
    let fixture = Fixture::new();
    let mut original = fixture.person();
    original.content.insert(
        0,
        ContentBlock::Text {
            text: "large text before the media".repeat(1_000),
        },
    );
    let input = vec![original.clone(), fixture.person(), fixture.person()];
    let mut records = vec![start(&input), step(1), finished(1, None), step(2)];
    let view = CompactionView::new(
        "session",
        1,
        "lineage",
        MessageRef {
            ordinal: 2,
            mid: "m2".into(),
        },
        vec![ViewRange::Keep {
            from: 0,
            to: 1,
            edits: vec![PartEdit::drop(0, "m0", "m0#0")],
        }],
    );
    records.push(WalRecord::CompactionViewApplied {
        provider: "compact".into(),
        view_version: 1,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: broca_wal::CompactionViewChoice::Applied,
    });
    let mut profile = profile();
    let sb = STANDARD.encode(&fixture.bytes).len() as u64;
    profile.aging = AgingThresholds {
        byte_trigger: sb,
        byte_target: sb / 2,
        token_trigger: 100,
        token_target: 50,
    };
    let mut recovered = SessionMedia::new(fixture.store.clone());
    let rebuilt = MediaAging::replay_wal(&profile, &mut recovered, &records).unwrap();
    let bindings = recovered.selected_bindings();
    assert_eq!(
        bindings.len(),
        1,
        "only the selected view contributes to aging"
    );
    assert!(!bindings[0].protected);
    assert!(!rebuilt.is_aged(Occurrence {
        capture: bindings[0].id,
        repetition: 0
    }));
    original.content.remove(0);
    original.ordinal = Some(0);
    let bundle = prepare(&recovered, Some(&profile), &[original], &rebuilt).unwrap();
    assert_eq!(
        bundle.media.len(),
        1,
        "dropping an earlier text block must not lose the kept media's binding"
    );
}

#[test]
fn replacement_of_new_input_keeps_protection_when_an_older_input_shares_its_digest() {
    use broca_core::compaction_provider::{
        lineage::MessageRef,
        view::{CompactionView, ViewRange},
    };
    let fixture = Fixture::new();
    let person = fixture.person();
    let mut records = vec![
        start(std::slice::from_ref(&person)),
        step(1),
        finished(1, None),
        steer(std::slice::from_ref(&person)),
        step(2),
    ];
    let view = CompactionView::new(
        "session",
        1,
        "lineage",
        MessageRef {
            ordinal: 2,
            mid: "m2".into(),
        },
        vec![
            ViewRange::Keep {
                from: 0,
                to: 2,
                edits: vec![],
            },
            ViewRange::Replace {
                from: 2,
                to: 3,
                bytes: serde_json::to_string(std::slice::from_ref(&person)).unwrap(),
            },
        ],
    );
    records.push(WalRecord::CompactionViewApplied {
        provider: "compact".into(),
        view_version: 1,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: broca_wal::CompactionViewChoice::Applied,
    });
    let mut profile = profile();
    let sb = STANDARD.encode(&fixture.bytes).len() as u64;
    profile.aging = AgingThresholds {
        byte_trigger: sb / 2,
        byte_target: sb / 4,
        token_trigger: 100,
        token_target: 50,
    };
    let mut session = SessionMedia::new(fixture.store.clone());
    let aging = MediaAging::replay_wal(&profile, &mut session, &records).unwrap();
    let mut old = person.clone();
    old.ordinal = Some(0);
    let prompt = vec![old, person];
    let bindings = session.bindings(&prompt);
    assert!(!bindings[0].protected);
    assert!(
        bindings[1].protected,
        "replacement provenance is its carried range, not the oldest session-wide digest"
    );
    let bundle = prepare(&session, Some(&profile), &prompt, &aging).unwrap();
    assert_eq!(bundle.media.len(), 1);
    assert_eq!(bundle.media[0].location.message, 1);
    assert_eq!(reasons(&bundle, "aged"), 1);
}
