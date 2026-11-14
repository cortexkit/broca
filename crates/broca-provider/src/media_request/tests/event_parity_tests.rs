//! The image engine driven two ways over the same history: the old storage's records through
//! `SessionMedia::fold` and `MediaAging::fold_wal_record`, and the events those records stand
//! for through `SessionMedia::apply` and `MediaAging::fold_media_event`. After every record the
//! two hold the same facts and the same aging state, and at every step boundary they prepare
//! and render the same request bytes. The histories are the ones the other media tests build.
use super::*;
use broca_core::media_request::{
    MediaCheckpoint, MediaHistory, WalMediaEvents, WalMediaEventsError,
};
use broca_protocol::media_event::MediaEvent;
use broca_protocol::CallOptions;

struct Engine {
    session: SessionMedia,
    aging: MediaAging,
    active: MediaProfile,
    step_open: bool,
}

impl Engine {
    fn fresh(fixture: &Fixture, profile: &MediaProfile) -> Self {
        Self {
            session: SessionMedia::new(fixture.store.clone()),
            aging: MediaAging::default(),
            active: profile.clone(),
            step_open: false,
        }
    }

    /// Restore a checkpoint as `LiveMedia::from_checkpoint` does.
    fn restored(fixture: &Fixture, checkpoint: &MediaCheckpoint, profile: &MediaProfile) -> Self {
        let (session, history, step_open, _) =
            checkpoint.clone().into_session(fixture.store.clone(), None);
        let mut active = profile.clone();
        let mut aging = MediaAging::default();
        for event in history {
            match event {
                MediaHistory::Profile { media, selected } => {
                    let next: MediaProfile = serde_json::from_value(media).unwrap();
                    if next != active {
                        aging = MediaAging::default();
                        aging.reconcile_view(&next, &selected);
                        active = next;
                    }
                }
                MediaHistory::Changes(changes) => aging.fold_changes(&active, changes),
                MediaHistory::Boundary => aging.apply(&active, AgingEvent::Boundary),
            }
        }
        Self {
            session,
            aging,
            active,
            step_open,
        }
    }

    #[allow(clippy::type_complexity)]
    fn aging_state(
        &self,
    ) -> (
        &BTreeMap<Occurrence, (MediaCost, bool)>,
        &BTreeSet<Occurrence>,
        &BTreeSet<Occurrence>,
        MediaCost,
    ) {
        (
            &self.aging.shown,
            &self.aging.unprotected,
            &self.aging.aged,
            self.aging.totals(),
        )
    }

    /// The request a step renders now: a provisional boundary when a step is open, as
    /// `LiveMedia` applies one, then the prepared bundle and the complete request body.
    fn render(&self, prompt: &[Message]) -> Result<(Vec<u8>, Vec<u8>), String> {
        let mut aging = self.aging.clone();
        if self.step_open {
            aging.apply(&self.active, AgingEvent::Boundary);
        }
        let mut bundle = prepare(&self.session, Some(&self.active), prompt, &aging)
            .map_err(|error| error.code().to_owned())?;
        let prepared = bytes(&bundle);
        let frozen = frozen(&self.active);
        let body = render_prepared(
            &*crate::registry::wire_family(frozen.family_id).unwrap(),
            &frozen,
            &CallOptions::new(prompt.to_vec()),
            Some(&mut bundle),
            &|prompt| prompt.to_vec(),
        )
        .map_err(|error| error.code().to_owned())?;
        Ok((prepared, body))
    }
}

fn frozen(profile: &MediaProfile) -> crate::wire::FrozenRenderConfig {
    let mut frozen =
        crate::families::frozen_config_for_family(WireFamilyId::AnthropicMessages, "claude");
    frozen.media = Some(profile.clone());
    frozen
}

/// A prompt holding every medium the engine has placed: its skeleton messages at their
/// ordinals.
fn skeleton_prompt(session: &SessionMedia) -> Vec<Message> {
    session
        .facts()
        .skeleton
        .into_iter()
        .map(|(ordinal, skeleton)| {
            let mut message = skeleton.to_message().unwrap();
            message.ordinal = Some(ordinal);
            message
        })
        .collect()
}

struct BothWays {
    name: &'static str,
    old: Engine,
    new: Engine,
    events: WalMediaEvents,
    seq: u64,
    records: Vec<WalRecord>,
    /// A prompt the history's own test renders, beside the ones built from the engine.
    prompt: Vec<Message>,
    renders: usize,
}

impl BothWays {
    fn new(name: &'static str, fixture: &Fixture, profile: &MediaProfile) -> Self {
        Self {
            name,
            old: Engine::fresh(fixture, profile),
            new: Engine::fresh(fixture, profile),
            events: WalMediaEvents::new(),
            seq: 0,
            records: Vec::new(),
            prompt: Vec::new(),
            renders: 0,
        }
    }

    /// Both engines continue from one checkpoint of `prefix`, as a converted session
    /// continues from the index saved with it.
    fn restored(
        name: &'static str,
        fixture: &Fixture,
        profile: &MediaProfile,
        prefix: &[WalRecord],
    ) -> Self {
        let mut checkpoint = MediaCheckpoint::empty();
        for (index, record) in prefix.iter().enumerate() {
            checkpoint.fold(index as u64 + 1, record).unwrap();
        }
        let checkpoint = MediaCheckpoint::decode(&checkpoint.encode()).unwrap();
        let old = Engine::restored(fixture, &checkpoint, profile);
        let new = Engine::restored(fixture, &checkpoint, profile);
        let events = WalMediaEvents::continuing(&old.session);
        let both = Self {
            name,
            old,
            new,
            events,
            seq: checkpoint.at_seq,
            records: prefix.to_vec(),
            prompt: Vec::new(),
            renders: 0,
        };
        both.assert_same("the restored checkpoint");
        both
    }

    fn push(&mut self, record: &WalRecord) {
        self.seq += 1;
        let context = format!(
            "{}: record {} ({})",
            self.name,
            self.seq,
            serde_json::to_value(record).unwrap()["type"]
        );
        self.old
            .aging
            .fold_wal_record(&mut self.old.active, &mut self.old.session, record)
            .unwrap();
        match record {
            WalRecord::RunStarted { .. } | WalRecord::ModelStepFinished { .. } => {
                self.old.step_open = false
            }
            WalRecord::StepStarted { .. } => self.old.step_open = true,
            _ => {}
        }
        let events = self
            .events
            .events(self.seq, record)
            .unwrap_or_else(|error| panic!("{context}: {error}"));
        for event in &events {
            self.new
                .aging
                .fold_media_event(&mut self.new.active, &mut self.new.session, event)
                .unwrap();
            match event {
                MediaEvent::RunStart { .. } | MediaEvent::StepEnd => self.new.step_open = false,
                MediaEvent::StepStart { .. } => self.new.step_open = true,
                _ => {}
            }
        }
        self.records.push(record.clone());
        self.assert_same(&context);
        if matches!(
            record,
            WalRecord::StepStarted { .. }
                | WalRecord::ModelAttemptFinished { .. }
                | WalRecord::ModelStepFinished { .. }
        ) {
            self.assert_same_request(&context);
        }
    }

    fn extend(&mut self, records: &[WalRecord]) {
        for record in records {
            self.push(record);
        }
    }

    fn assert_same(&self, context: &str) {
        assert_eq!(
            self.old.session.facts(),
            self.new.session.facts(),
            "engine facts after {context}"
        );
        assert_eq!(
            self.old.aging_state(),
            self.new.aging_state(),
            "aging after {context}"
        );
        assert_eq!(self.old.active, self.new.active, "limits after {context}");
        assert_eq!(
            self.old.step_open, self.new.step_open,
            "open step after {context}"
        );
    }

    fn assert_same_request(&mut self, context: &str) {
        let mut prompts = vec![skeleton_prompt(&self.old.session), self.prompt.clone()];
        if let Ok(replayed) = broca_core::replay::replay(&self.records) {
            prompts.push(replayed.prompt);
        }
        for prompt in prompts {
            if references(&prompt).is_empty() {
                continue;
            }
            let old = self.old.render(&prompt);
            assert_eq!(old, self.new.render(&prompt), "request after {context}");
            if old.is_ok() {
                self.renders += 1;
            }
        }
    }

    /// Every history must have compared at least one rendered request that holds media.
    fn finish(self) -> Engine {
        assert!(
            self.renders > 0,
            "{}: no request holding media was compared",
            self.name
        );
        self.new
    }
}

fn encoded(fixture: &Fixture) -> u64 {
    STANDARD.encode(&fixture.bytes).len() as u64
}

fn aging(profile: &mut MediaProfile, bytes: (u64, u64), tokens: (u64, u64)) {
    profile.aging = AgingThresholds {
        byte_trigger: bytes.0,
        byte_target: bytes.1,
        token_trigger: tokens.0,
        token_target: tokens.1,
    };
}

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

/// A run start whose frozen render config carries `profile` as its media limits.
fn start_with(input: &[Message], run: &str, profile: &MediaProfile) -> WalRecord {
    let mut start = start(input);
    if let WalRecord::RunStarted { config, run_id, .. } = &mut start {
        *run_id = broca_protocol::RunId(run.into());
        config.frozen_render_config = serde_json::to_value(frozen(profile)).unwrap();
    }
    start
}

fn batch(step: u64, calls: &[&str]) -> WalRecord {
    record(
        json!({"type":"tool_batch_started","step_id":step,"batch_id":"batch","planned_calls":calls}),
    )
}

fn batch_finished() -> WalRecord {
    record(json!({"type":"tool_batch_finished","batch_id":"batch"}))
}

fn attempt_failed(step: u64, attempt: u32) -> WalRecord {
    record(
        json!({"type":"model_attempt_finished","step_id":step,"attempt":attempt,
        "outcome":"http_error","ts_ms":77,"request_body_len":0,"request_body_sha256":"a",
        "response_body_len":0,"response_body_sha256":"b"}),
    )
}

fn run_finished(reason: &str) -> WalRecord {
    record(json!({"type":"run_finished","reason":reason,"usage":{}}))
}

fn late(message: &Message) -> WalRecord {
    let ContentBlock::ToolResult(result) = &message.content[0] else {
        unreachable!()
    };
    record(
        json!({"type":"late_result","call_key":"lineage:7","outcome":"completed","result":result}),
    )
}

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

fn tool_with(call: &str, parts: Vec<ToolOutputPart>) -> Message {
    Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(broca_protocol::ToolResult {
            tool_call_id: broca_protocol::ToolCallId(call.into()),
            output: ToolOutput::Parts { parts },
            is_error: false,
        })],
    )
}

#[test]
fn person_inputs_stay_protected_until_the_next_step_both_ways() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = encoded(&fixture);
    aging(&mut profile, (sb, sb / 2), (100, 50));
    let mut both = BothWays::new("fresh protected inputs", &fixture, &profile);
    both.extend(&[
        start(&[fixture.person(), fixture.person(), fixture.person()]),
        step(1),
        step(1),
        step(2),
    ]);
    both.finish();
}

#[test]
fn a_steer_protects_its_own_occurrence_both_ways() {
    let fixture = Fixture::new();
    let mut both = BothWays::new("steer boundary", &fixture, &profile());
    both.extend(&[
        start(&[fixture.person()]),
        step(1),
        steer(&[fixture.person()]),
        step(2),
        step(2),
        step(3),
    ]);
    both.finish();
}

#[test]
fn tool_results_without_a_waiting_assistant_both_ways() {
    let fixture = Fixture::new();
    let mut both = BothWays::new("tools before people", &fixture, &profile());
    both.extend(&[
        start(&[fixture.person()]),
        step(1),
        step(2),
        tool_record(&fixture.tool("old")),
        tool_record(&fixture.tool("new")),
        step(3),
    ]);
    let mut screens = vec![start(&[]), step(1)];
    let tools: Vec<_> = (0..21)
        .map(|n| fixture.tool(&format!("screen{n}")))
        .collect();
    screens.extend(tools.iter().map(tool_record));
    screens.push(step(2));
    // As the original test renders them: results without ordinals, bound by call.
    both.prompt = tools;
    both.extend(&screens);
    both.finish();
}

#[test]
fn forty_screenshots_age_past_the_limits_both_ways() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = encoded(&fixture);
    let st = profile.media_tokens(&fixture.media);
    profile.request_cap_bytes = sb * 16;
    aging(&mut profile, (sb * 8, sb * 4), (st * 12, st * 6));
    let mut both = BothWays::new("forty screenshots", &fixture, &profile);
    both.push(&start(&[]));
    for n in 1..=40 {
        both.push(&step(n));
        // As the original test renders them: results without ordinals, bound by call.
        let screenshot = fixture.tool(&format!("screen{n}"));
        both.prompt.push(screenshot.clone());
        both.push(&tool_record(&screenshot));
        // The original test ages at every step through its renderer; on records, an
        // attempt's end is that boundary.
        both.push(&attempt_failed(n, 0));
    }
    both.push(&step(41));
    let new = both.finish();
    assert!(
        !new.aging.aged.is_empty(),
        "the history must age media for the comparison to cover aging"
    );
}

#[test]
fn screenshot_steps_through_tool_batches_both_ways() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = encoded(&fixture);
    aging(&mut profile, (sb * 8, sb * 4), (100, 50));
    let mut both = BothWays::new("screenshot batches", &fixture, &profile);
    both.push(&start(&[]));
    for n in 1..=40 {
        let call = format!("screen{n}");
        both.extend(&[
            step(n),
            finished(n, Some(&call)),
            batch(n, &[&call]),
            tool_record(&fixture.tool(&call)),
            batch_finished(),
        ]);
    }
    both.push(&step(41));
    let new = both.finish();
    assert!(!new.aging.aged.is_empty());
}

/// The run history `live_tests` and the reservation tests build: a person's image, a tool
/// call whose result holds media, the next step.
fn live_history(fixture: &Fixture, profile: &MediaProfile, tool: Message) -> Vec<WalRecord> {
    vec![
        start_with(&[fixture.person()], "run", profile),
        step(1),
        finished(1, Some("call")),
        batch(1, &["call"]),
        tool_record(&tool),
        batch_finished(),
        record(json!({"type":"turn_finished","step_id":1})),
        step(2),
    ]
}

#[test]
fn new_limits_at_the_next_run_restart_aging_both_ways() {
    let fixture = Fixture::new();
    let mut profile = profile();
    aging(&mut profile, (1, 0), (u64::MAX, u64::MAX));
    let mut both = BothWays::new("limits between runs", &fixture, &profile);
    both.extend(&live_history(&fixture, &profile, fixture.tool("call")));
    let mut next = profile.clone();
    next.aging.byte_trigger = u64::MAX;
    both.extend(&[
        finished(2, None),
        record(json!({"type":"turn_finished","step_id":2})),
        run_finished("completed"),
        start_with(&[fixture.person()], "next", &next),
        step(1),
        finished(1, None),
    ]);
    let new = both.finish();
    assert_eq!(new.active, next);
}

#[test]
fn failed_attempts_are_aging_boundaries_both_ways() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = encoded(&fixture);
    aging(&mut profile, (sb * 2, sb), (u64::MAX, u64::MAX));
    let two = tool_with(
        "call",
        vec![
            ToolOutputPart::Media {
                media: fixture.media.clone(),
            },
            ToolOutputPart::Media {
                media: fixture.media.clone(),
            },
        ],
    );
    let mut both = BothWays::new("failed attempts", &fixture, &profile);
    both.extend(&live_history(&fixture, &profile, two));
    both.extend(&[
        attempt_failed(2, 0),
        attempt_failed(2, 1),
        finished(2, None),
    ]);
    let new = both.finish();
    assert_eq!(new.aging.aged.len(), 2);
}

#[test]
fn limits_refrozen_inside_a_run_both_ways() {
    let fixture = Fixture::new();
    let profile = profile();
    let mut tighter = profile.clone();
    let sb = encoded(&fixture);
    aging(&mut tighter, (sb, 0), (u64::MAX, u64::MAX));
    let mut both = BothWays::new("refrozen limits", &fixture, &profile);
    both.extend(&live_history(&fixture, &profile, fixture.tool("call")));
    both.extend(&[
        record(json!({"type":"run_config_refrozen","renderer_version":1,
            "frozen_render_config":serde_json::to_value(frozen(&tighter)).unwrap()})),
        attempt_failed(2, 0),
        record(json!({"type":"run_config_refrozen","renderer_version":1,
            "frozen_render_config":{}})),
        finished(2, None),
    ]);
    let new = both.finish();
    assert_eq!(new.active, tighter);
}

#[test]
fn delivered_sends_and_multi_message_steers_both_ways() {
    let fixture = Fixture::new();
    let mut both = BothWays::new("delivered sends", &fixture, &profile());
    let send = [text(Role::User, "before the image"), fixture.person()];
    both.extend(&[
        start(&[text(Role::User, "no media at first")]),
        step(1),
        record(json!({"type":"queued","submission_id":"send","request":{
            "model":{"provider_module_id":"fake","model_id":"fake"},"input":send}})),
        record(
            json!({"type":"send_delivered","submission_id":"send","run_id":"run",
            "mode":"steer","input":send}),
        ),
        steer(&[text(Role::User, "a steer without media"), fixture.person()]),
        step(2),
        finished(2, None),
        steer(&[fixture.person()]),
        run_finished("completed"),
        start(&[fixture.person()]),
        step(1),
    ]);
    both.finish();
}

#[test]
fn late_results_are_captured_but_never_shown_both_ways() {
    let fixture = Fixture::new();
    let mut both = BothWays::new("late results", &fixture, &profile());
    both.extend(&[
        start(&[fixture.person()]),
        step(1),
        finished(1, Some("a")),
        batch(1, &["a"]),
        // An assistant waits for its results: a late result of an older call is captured at
        // the waiting batch's results position, and never shown.
        late(&fixture.tool("older")),
        tool_record(&fixture.tool("a")),
        batch_finished(),
        step(2),
        late(&fixture.tool("a")),
        step(3),
    ]);
    both.finish();
}

#[test]
fn batch_positions_duplicate_calls_and_system_inputs_both_ways() {
    let fixture = Fixture::new();
    let mut system = fixture.person();
    system.role = Role::System;
    let text_only = tool_with(
        "first",
        vec![ToolOutputPart::Text {
            text: "only text".into(),
        }],
    );
    let mut both = BothWays::new("batch positions", &fixture, &profile());
    both.extend(&[
        start(&[system, text(Role::User, "go"), fixture.person()]),
        step(1),
        finished(1, Some("first")),
        batch(1, &["first", "second", "second"]),
        tool_record(&text_only),
        tool_record(&fixture.tool("second")),
        tool_record(&fixture.tool("second")),
        batch_finished(),
        step(2),
    ]);
    both.finish();
}

#[test]
fn an_import_seed_and_its_interrupted_cut_both_ways() {
    let fixture = Fixture::new();
    let assistant = Message::new(
        Role::Assistant,
        vec![ContentBlock::ToolCall(broca_protocol::ToolCall {
            tool_call_id: broca_protocol::ToolCallId("imported".into()),
            tool_name: "screen".into(),
            input: json!({"long": "x".repeat(1_000)}).into(),
            thought_signature: None,
        })],
    );
    let imported = [
        fixture.person(),
        assistant,
        fixture.tool("imported"),
        text(Role::User, "after the import"),
    ];
    let mut seed = start(&imported);
    if let WalRecord::RunStarted { origin, .. } = &mut seed {
        *origin = broca_wal::RunOrigin::SeededFrom {
            origin_kind: broca_wal::SeedOriginKind::Import,
            source_ref: "transcript".into(),
            origin_device_pseudonym: None,
            base_seq: None,
        };
    }
    let mut both = BothWays::new("import seed", &fixture, &profile());
    both.extend(&[
        seed.clone(),
        // An import whose run ended before any step gives its ordinals back.
        run_finished("interrupted"),
        seed,
        run_finished("completed"),
        start(&[fixture.person()]),
        step(1),
        finished(1, None),
    ]);
    both.finish();
}

/// A first-protocol compaction record (CompactionViewApplied, or a CompactionMessage) has no
/// media event: a session that applied one continues from a checkpoint saved after it.
/// `prefix` ends with such a record.
fn continue_after(
    name: &'static str,
    fixture: &Fixture,
    profile: &MediaProfile,
    prefix: &[WalRecord],
    tail: &[WalRecord],
) {
    let mut fresh = BothWays::new(name, fixture, profile);
    let (compaction, before) = prefix.split_last().unwrap();
    fresh.extend(before);
    let refused = fresh
        .events
        .clone()
        .events(fresh.seq + 1, compaction)
        .unwrap_err();
    assert!(
        matches!(refused, WalMediaEventsError::NeedsSavedIndex { .. }),
        "{name}: {refused}"
    );
    let mut both = BothWays::restored(name, fixture, profile, prefix);
    both.extend(tail);
    both.finish();
}

fn more_steps(fixture: &Fixture, first: u64) -> Vec<WalRecord> {
    vec![
        finished(first, None),
        steer(&[fixture.person()]),
        step(first + 1),
        finished(first + 1, Some("later")),
        batch(first + 1, &["later"]),
        tool_record(&fixture.tool("later")),
        batch_finished(),
        step(first + 2),
        finished(first + 2, None),
    ]
}

#[test]
fn a_restored_checkpoint_after_a_kept_view_continues_both_ways() {
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
    let prefix = vec![
        start(&[original, fixture.person(), fixture.person()]),
        step(1),
        finished(1, None),
        step(2),
        WalRecord::CompactionViewApplied {
            provider: "compact".into(),
            view_version: 1,
            view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap())
                .unwrap(),
            choice: broca_wal::CompactionViewChoice::Applied,
        },
    ];
    let mut profile = profile();
    let sb = encoded(&fixture);
    aging(&mut profile, (sb, sb / 2), (100, 50));
    continue_after(
        "kept view",
        &fixture,
        &profile,
        &prefix,
        &more_steps(&fixture, 2),
    );
}

#[test]
fn a_restored_checkpoint_after_a_replacement_view_continues_both_ways() {
    use broca_core::compaction_provider::{
        lineage::MessageRef,
        view::{CompactionView, ViewRange},
    };
    let fixture = Fixture::new();
    let person = fixture.person();
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
    let prefix = vec![
        start(std::slice::from_ref(&person)),
        step(1),
        finished(1, None),
        steer(std::slice::from_ref(&person)),
        step(2),
        WalRecord::CompactionViewApplied {
            provider: "compact".into(),
            view_version: 1,
            view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap())
                .unwrap(),
            choice: broca_wal::CompactionViewChoice::Applied,
        },
    ];
    let mut profile = profile();
    let sb = encoded(&fixture);
    aging(&mut profile, (sb / 2, sb / 4), (100, 50));
    continue_after(
        "replacement view",
        &fixture,
        &profile,
        &prefix,
        &more_steps(&fixture, 2),
    );
}

#[test]
fn a_restored_checkpoint_after_a_setup_replacement_continues_both_ways() {
    let fixture = Fixture::new();
    let profile = profile();
    let mut prefix = live_history(&fixture, &profile, fixture.tool("call"));
    let replacement = Message::new(
        Role::User,
        vec![
            ContentBlock::Text {
                text: "summary".into(),
            },
            ContentBlock::Media(fixture.media.clone()),
        ],
    );
    prefix.push(record(json!({
        "type":"compaction_message", "provider":"compactor", "kind":"setup",
        "compaction_id":"media-view", "version":1, "range":{"from":0,"to":1},
        "replacement":[replacement], "sha256":"fixture"
    })));
    continue_after(
        "setup replacement",
        &fixture,
        &profile,
        &prefix,
        &more_steps(&fixture, 2),
    );
}

#[test]
fn a_checkpoint_at_every_cut_continues_both_ways() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = encoded(&fixture);
    aging(&mut profile, (sb * 2, sb), (u64::MAX, u64::MAX));
    let mut records = live_history(&fixture, &profile, fixture.tool("call"));
    records.extend(more_steps(&fixture, 2));
    for cut in 0..records.len() {
        let mut both = BothWays::restored("every cut", &fixture, &profile, &records[..cut]);
        both.extend(&records[cut..]);
        both.finish();
    }
}

/// Drives one engine with events only, for the facts the old storage has no record of.
struct EventsOnly {
    engine: Engine,
    events: WalMediaEvents,
    seq: u64,
    records: Vec<WalRecord>,
}

impl EventsOnly {
    fn new(fixture: &Fixture, profile: &MediaProfile) -> Self {
        Self {
            engine: Engine::fresh(fixture, profile),
            events: WalMediaEvents::new(),
            seq: 0,
            records: Vec::new(),
        }
    }

    fn record(&mut self, record: &WalRecord) {
        self.seq += 1;
        for event in self.events.events(self.seq, record).unwrap() {
            self.event(&event);
        }
        self.records.push(record.clone());
    }

    fn event(&mut self, event: &MediaEvent) {
        let engine = &mut self.engine;
        engine
            .aging
            .fold_media_event(&mut engine.active, &mut engine.session, event)
            .unwrap();
        match event {
            MediaEvent::RunStart { .. } | MediaEvent::StepEnd | MediaEvent::Truncated { .. } => {
                engine.step_open = false
            }
            MediaEvent::StepStart { .. } => engine.step_open = true,
            _ => {}
        }
    }
}

fn occurrence(capture: MediaId) -> Occurrence {
    Occurrence {
        capture,
        repetition: 0,
    }
}

#[test]
fn a_truncation_forgets_the_media_it_cuts_and_keeps_earlier_ages() {
    let fixture = Fixture::new();
    let mut profile = profile();
    let sb = encoded(&fixture);
    aging(&mut profile, (sb, sb), (u64::MAX, u64::MAX));
    let mut run = EventsOnly::new(&fixture, &profile);
    for record in [
        start(&[fixture.person()]),
        step(1),
        finished(1, Some("a")),
        batch(1, &["a"]),
        tool_record(&fixture.tool("a")),
        batch_finished(),
        step(2),
        finished(2, Some("b")),
        batch(2, &["b"]),
        tool_record(&fixture.tool("b")),
        steer(&[fixture.person()]),
    ] {
        run.record(&record);
    }
    let person = MediaId {
        record: 1,
        message: 0,
        block: 0,
        part: None,
    };
    let tool = |record| MediaId {
        record,
        message: 0,
        block: 0,
        part: Some(0),
    };
    let before = run.engine.session.facts();
    assert_eq!(
        before.captures.len(),
        4,
        "person, two tool images, a held steer"
    );
    assert!(run.engine.aging.is_aged(occurrence(person)));
    assert_eq!(run.engine.aging.totals().bytes, sb * 2);
    // The transcript keeps ordinals 0 to 2: the person's image, the first call and its
    // result. The second call's result (ordinal 4) and the held steer go.
    run.event(&MediaEvent::Truncated { len: 3 });
    let after = run.engine.session.facts();
    assert_eq!(
        after
            .captures
            .iter()
            .map(|capture| capture.id)
            .collect::<Vec<_>>(),
        vec![person, tool(5)]
    );
    assert!(after.pending.is_empty() && after.held.is_empty() && after.protected.is_empty());
    assert!(after.held_messages.is_empty() && after.batch_order.is_empty());
    assert_eq!(after.last_step, None);
    assert_eq!(
        after
            .skeleton
            .iter()
            .map(|(ordinal, _)| *ordinal)
            .collect::<Vec<_>>(),
        vec![0, 2]
    );
    assert_eq!(after.boundary, before.boundary + 1, "the cut ends the step");
    assert!(after.selected.iter().all(|binding| !binding.protected));
    let aging_state = &run.engine.aging;
    assert_eq!(
        aging_state.shown.keys().copied().collect::<Vec<_>>(),
        vec![occurrence(person), occurrence(tool(5))]
    );
    assert!(
        aging_state.is_aged(occurrence(person)),
        "an aged image stays aged"
    );
    assert!(!aging_state.is_aged(occurrence(tool(10))));
    assert_eq!(
        aging_state.totals().bytes,
        sb,
        "only the first tool image still counts"
    );
    assert_eq!(
        aging_state.unprotected.iter().copied().collect::<Vec<_>>(),
        vec![occurrence(tool(5))]
    );

    let mut prompt: Vec<_> = broca_core::replay::replay(&run.records)
        .unwrap()
        .prompt
        .into_iter()
        .filter(|message| message.ordinal.is_some_and(|ordinal| ordinal < 3))
        .collect();
    let (bundle, _) = run.engine.render(&prompt).unwrap();
    let bundle = String::from_utf8(bundle).unwrap();
    assert_eq!(bundle.matches("reason=aged").count(), 1, "{bundle}");
    assert_eq!(bundle.matches("reason=provenance").count(), 0, "{bundle}");

    // The session continues: a new input takes the first freed ordinal, shows and is
    // protected for its first step, as any input is.
    let mut next = fixture.person();
    next.ordinal = Some(3);
    run.event(&MediaEvent::RunStart {
        record: run.seq + 1,
        inputs: vec![broca_protocol::media_event::MediaInput {
            message: 0,
            ordinal: 3,
            skeleton: broca_protocol::media_event::MediaMessage::of(&next),
        }],
        limits: None,
    });
    run.event(&MediaEvent::StepStart {
        step: broca_protocol::StepId(1),
    });
    prompt.push(next);
    let bindings = run.engine.session.bindings(&prompt);
    assert_eq!(bindings.len(), 3);
    assert!(bindings[2].protected && !bindings[0].protected && !bindings[1].protected);
    let (bundle, _) = run.engine.render(&prompt).unwrap();
    let bundle = String::from_utf8(bundle).unwrap();
    assert_eq!(bundle.matches("reason=aged").count(), 1, "{bundle}");
}

#[test]
fn a_truncation_keeps_a_shared_reference_readable_through_a_capture_that_remains() {
    let fixture = Fixture::new();
    let mut run = EventsOnly::new(&fixture, &profile());
    for record in [
        start(&[text(Role::User, "go")]),
        step(1),
        finished(1, Some("a")),
        batch(1, &["a"]),
        // The steer captures the image first, so its capture is the one the reference is
        // read through until the cut removes it.
        steer(&[fixture.person()]),
        tool_record(&fixture.tool("a")),
        batch_finished(),
    ] {
        run.record(&record);
    }
    run.event(&MediaEvent::Truncated { len: 3 });
    assert_eq!(run.engine.session.facts().captures.len(), 1);
    let prompt: Vec<_> = broca_core::replay::replay(&run.records)
        .unwrap()
        .prompt
        .into_iter()
        .filter(|message| message.ordinal.is_some_and(|ordinal| ordinal < 3))
        .collect();
    let (bundle, _) = run.engine.render(&prompt).unwrap();
    let bundle = String::from_utf8(bundle).unwrap();
    assert!(
        bundle.contains(&STANDARD.encode(&fixture.bytes)),
        "the remaining tool image is hydrated: {bundle}"
    );
}
