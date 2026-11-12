use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::ids::{MessageId, StepId, ToolCallId};
use broca_protocol::media_event::{MediaEvent, MediaInput, MediaLimits, MediaMessage, MediaPart};
use broca_protocol::request::ModelHandle;

use super::*;
use crate::codec::{Digest, Id128};
use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;
use crate::record::{
    Assistant, Attempt, BatchEnd, BatchStart, Billed, CallOutcome, Compaction, CompactionEnrolled,
    ConfigRefrozen, Delivery, EnrollmentOverlayRef, EntryFacts, Event, Input, PartKind, PartRef,
    Placement, Record, RefreshApplied, RunEnd, RunStart, Steer, StepEnd, StepStart, ToolOutcome,
    ToolReply, TurnFacts,
};

/// A session's records folded in batches, as a session folds them: the media fold first, with the
/// store's two folds as they stand before each record.
pub(crate) struct History {
    pub(crate) conversation: Conversation,
    pub(crate) operations: Operations,
    pub(crate) media: MediaFold,
    pub(crate) next: u64,
}

impl History {
    pub(crate) fn new() -> Self {
        Self {
            conversation: Conversation::new(false),
            operations: Operations::default(),
            media: MediaFold::default(),
            next: 1,
        }
    }

    /// Folds one batch: the seq of its first record, and what it handed over.
    pub(crate) fn batch(&mut self, records: Vec<Record>) -> (u64, MediaFeed) {
        let first = self.next;
        let mut feed = MediaFeed::default();
        for record in &records {
            let seq = self.next;
            self.next += 1;
            self.media
                .apply(seq, record, &self.conversation, &self.operations, &mut feed);
            crate::session::apply(&mut self.conversation, &mut self.operations, seq, record)
                .unwrap_or_else(|error| panic!("{} at {seq}: {error}", record.name()));
        }
        self.media.flush(&mut feed);
        (first, feed)
    }

    /// Folds one batch of a session that has already captured a medium: the events it handed
    /// over, every one complete.
    pub(crate) fn events(&mut self, records: Vec<Record>) -> (u64, Vec<MediaEvent>) {
        let (first, feed) = self.batch(records);
        assert_eq!(feed.start, None);
        assert_eq!(feed.needs_saved_index, None);
        let events = feed
            .items
            .into_iter()
            .map(|item| match item {
                MediaItem::Event(event) => event,
                other => panic!("an item without its limits: {other:?}"),
            })
            .collect();
        (first, events)
    }

    /// The next seq a record takes.
    pub(crate) fn seq(&self) -> u64 {
        self.next
    }
}

pub(crate) fn model() -> ModelHandle {
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

/// An image named by one hex digit, which its digest repeats.
pub(crate) fn image(name: &str) -> MediaRef {
    serde_json::from_value(serde_json::json!({
        "kind": "image",
        "mime": "image/png",
        "byte_size": 1,
        "sha256": name.repeat(64),
        "width": 1,
        "height": 1,
    }))
    .unwrap()
}

pub(crate) fn render_config() -> PartRef {
    PartRef {
        kind: PartKind::RenderConfig,
        digest: Digest([7; 32]),
    }
}

pub(crate) fn start(run: &str, setup: Vec<PartRef>, seeded: bool) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        run_id: run.into(),
        model: model(),
        setup,
        renderer_version: 0,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: None,
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: seeded.then_some(true),
        refusal_schedule: None,
        imported_ids: None,
        message_marks: None,
    }))
}

pub(crate) fn end(run: &str) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: None,
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

pub(crate) fn input(
    run: &str,
    placement: Placement,
    receipt: Option<u64>,
    message: Message,
) -> Record {
    Record::Input(Input {
        run_id: run.into(),
        placement,
        message,
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: receipt,
        pruned: Vec::new(),
        media: Vec::new(),
    })
}

pub(crate) fn text(text: &str) -> ContentBlock {
    ContentBlock::Text { text: text.into() }
}

pub(crate) fn said(words: &str) -> Message {
    Message::new(Role::User, vec![text(words)])
}

pub(crate) fn photo(words: &str, name: &str) -> Message {
    Message::new(
        Role::User,
        vec![text(words), ContentBlock::Media(image(name))],
    )
}

pub(crate) fn facts(ordinal: u64) -> Record {
    Record::EntryFacts(EntryFacts {
        send_id: None,
        named_speaker: None,
        ordinal,
        written_at_ms: Some(ordinal),
        tag_base: 0,
        notice: None,
        usage: None,
        generated_under: None,
        native: false,
        render_tags: false,
        clean_reply: false,
        mid: format!("m{ordinal}"),
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        notices: Vec::new(),
        runner_tag_base: None,
        legacy_view_seq: None,
    })
}

pub(crate) fn step_start(run: &str, step: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id: step,
        mid: None,
    })
}

pub(crate) fn attempt(run: &str, step: u64) -> Record {
    Record::Attempt(Attempt {
        run_id: run.into(),
        step_id: step,
        attempt: 1,
        attempt_id: None,
        outcome: "complete".into(),
        http_status: Some(200),
        request_body_len: 0,
        request_body_sha256: Digest([0; 32]),
        response_body_len: 0,
        response_body_sha256: Digest([0; 32]),
        ts_ms: 1,
        sent_at_ms: None,
        first_content_at_ms: None,
        last_content_at_ms: None,
        transport: None,
        served_account_ref: None,
        billing: None,
        telemetry: None,
        reached_cache: None,
        requested_cache_ttl_ms: None,
    })
}

pub(crate) fn call(id: &str) -> ContentBlock {
    serde_json::from_value(serde_json::json!({
        "type": "tool_call", "tool_call_id": id, "tool_name": "photo", "input": {}
    }))
    .unwrap()
}

pub(crate) fn assistant(run: &str, step: u64, content: Vec<ContentBlock>) -> Record {
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: step,
        provider_message_id: MessageId(format!("p-{run}-{step}")),
        message: Message::assistant_from(content, model()),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

pub(crate) fn step_end(run: &str, step: u64, calls: u32) -> Record {
    Record::StepEnd(StepEnd {
        run_id: run.into(),
        step_id: step,
        usage: None,
        finish_reason: None,
        retries_used: 0,
        service_tier_used: None,
        speed_used: None,
        call_count: calls,
        fallback: None,
        hooks: None,
        budget_finalize: None,
        total_usage: None,
        stop_met: None,
        limiting_stop_met: None,
        by_model: Vec::new(),
    })
}

/// A step whose answer is `content`, as the loop writes it: its start, an attempt, then the
/// assistant message, the step's end and the message's facts.
pub(crate) fn step(
    history: &mut History,
    run: &str,
    step: u64,
    ordinal: u64,
    content: Vec<ContentBlock>,
) {
    let calls = content
        .iter()
        .filter(|block| matches!(block, ContentBlock::ToolCall(_)))
        .count() as u32;
    history.batch(vec![step_start(run, step)]);
    history.batch(vec![attempt(run, step)]);
    history.batch(vec![
        assistant(run, step, content),
        step_end(run, step, calls),
        facts(ordinal),
    ]);
}

pub(crate) fn batch_start(run: &str, step: u64, calls: &[&str]) -> Record {
    Record::BatchStart(BatchStart {
        run_id: run.into(),
        batch_id: format!("b{step}"),
        step_id: step,
        planned: calls.iter().map(|id| ToolCallId((*id).into())).collect(),
        mid: None,
    })
}

pub(crate) fn batch_end(run: &str, step: u64) -> Record {
    Record::BatchEnd(BatchEnd {
        run_id: run.into(),
        batch_id: format!("b{step}"),
    })
}

/// A tool result with a text part, then one media part per image.
pub(crate) fn result(id: &str, images: &[&str]) -> serde_json::Value {
    let mut parts = vec![serde_json::json!({"kind": "text", "text": "photos"})];
    for name in images {
        parts.push(serde_json::json!({"kind": "media", "media": image(name)}));
    }
    serde_json::json!({
        "tool_call_id": id, "output": {"kind": "parts", "parts": parts}, "is_error": false
    })
}

pub(crate) fn reply(run: &str, step: u64, id: &str, images: &[&str]) -> Record {
    Record::ToolReply(ToolReply {
        run_id: run.into(),
        batch_id: Some(format!("b{step}")),
        result: serde_json::from_value(result(id, images)).unwrap(),
        finished_at_ms: None,
        tags_v1: None,
        media: Vec::new(),
    })
}

pub(crate) fn outcome(run: &str, step: u64, id: &str) -> Record {
    Record::ToolOutcome(ToolOutcome {
        run_id: run.into(),
        batch_id: format!("b{step}"),
        tool_call_id: ToolCallId(id.into()),
        outcome: CallOutcome::Completed,
        result_digest: Digest([0; 32]),
        is_error: false,
        reason: None,
        finished_at_ms: None,
        detail: None,
        hooks: None,
        call_key: None,
    })
}

pub(crate) fn turn_facts(ordinal: u64) -> Record {
    Record::TurnFacts(TurnFacts {
        ordinal,
        calls: Vec::new(),
    })
}

pub(crate) fn steer(messages: Vec<Message>) -> Record {
    Record::Steer(Steer {
        input: messages,
        written_at_ms: None,
        tags_v1: None,
        receipt: None,
        media: Vec::new(),
        message_marks: None,
        named_speaker: None,
    })
}

pub(crate) fn delivery(run: &str, messages: Vec<Message>) -> Record {
    Record::Delivery(Delivery {
        submission_id: "q1".into(),
        run_id: run.into(),
        mode: "steer".into(),
        input: messages,
        tool_call_id: None,
        appended_text: None,
        written_at_ms: None,
        tags_v1: None,
        input_listed: Some(true),
    })
}

pub(crate) fn refrozen(run: &str, render_config: serde_json::Value) -> Record {
    Record::ConfigRefrozen(ConfigRefrozen {
        run_id: run.into(),
        ts_ms: None,
        renderer_version: 1,
        render_config,
    })
}

pub(crate) fn media(parts: &[(usize, &str)]) -> Vec<MediaPart> {
    parts
        .iter()
        .map(|(part, name)| MediaPart {
            part: *part,
            reference: image(name),
        })
        .collect()
}

pub(crate) fn limits(max: u64) -> MediaLimits {
    MediaLimits(serde_json::json!({ "max_images": max }))
}

/// A first run without media, at ordinals 1 and 2, with one step.
pub(crate) fn plain_run(history: &mut History) {
    history.batch(vec![
        start("r1", vec![render_config()], false),
        input("r1", Placement::RunInput, None, said("hello")),
        facts(1),
    ]);
    step(history, "r1", 1, 2, vec![text("hi")]);
    history.batch(vec![end("r1")]);
}

/// A session whose second run's input carries the first image, `a`, at ordinal 3 (from 0), and
/// whose step 1 has started. Returns the run start's seq.
pub(crate) fn captured() -> (History, u64) {
    let mut history = History::new();
    plain_run(&mut history);
    let (at, feed) = history.batch(vec![
        start("r2", Vec::new(), false),
        input("r2", Placement::RunInput, None, photo("look", "a")),
        facts(3),
    ]);
    assert!(feed.start.is_some());
    let (_, events) = history.events(vec![step_start("r2", 1)]);
    assert_eq!(events, vec![MediaEvent::StepStart { step: StepId(1) }]);
    (history, at)
}

/// A run's start hands over its inputs that hold media at the ordinals their facts name, after
/// the inputs and their facts. A session's first capture hands over the scalars it kept until
/// then. A run whose render config is a setup part is handed over without its limits, which the
/// caller completes from that part.
#[test]
fn a_run_start_places_its_inputs_and_starts_the_index_at_the_first_capture() {
    let mut history = History::new();
    plain_run(&mut history);
    let looked = photo("look", "a");
    let (at, feed) = history.batch(vec![
        start("r2", vec![render_config()], false),
        input("r2", Placement::RunInput, None, said("first")),
        input("r2", Placement::RunInput, None, looked.clone()),
        facts(3),
        facts(4),
    ]);
    assert_eq!(
        feed.start,
        Some(MediaStart {
            steps: 1,
            last_step: None,
            step_open: false,
            batch: Vec::new(),
            limits: LimitsSource::Known(None),
        })
    );
    let expected = RunStartNeedingLimits {
        record: at,
        inputs: vec![MediaInput {
            message: 1,
            ordinal: 3,
            skeleton: MediaMessage::of(&looked),
        }],
        render_config: render_config(),
    };
    assert_eq!(feed.items, vec![MediaItem::RunStart(expected.clone())]);
    assert_eq!(
        expected.with_limits(Some(limits(2))),
        MediaEvent::RunStart {
            record: at,
            inputs: vec![MediaInput {
                message: 1,
                ordinal: 3,
                skeleton: MediaMessage::of(&looked),
            }],
            limits: Some(limits(2)),
        }
    );
    // A later run without a render config is complete as handed over, and a run without media
    // is handed over too once the session captured something: it resets what a run holds.
    history.batch(vec![end("r2")]);
    let (at, events) = history.events(vec![
        start("r3", Vec::new(), false),
        input("r3", Placement::RunInput, None, said("again")),
        facts(5),
    ]);
    assert_eq!(
        events,
        vec![MediaEvent::RunStart {
            record: at,
            inputs: Vec::new(),
            limits: None,
        }]
    );
}

/// Steered and delivered messages are handed over when accepted, each message with its
/// skeleton. A boundary writes their copies, the step's start and then the copies' facts in one
/// batch: the placement is handed over at the first copy's ordinal, before the step's start.
#[test]
fn steers_and_deliveries_are_accepted_then_placed_before_the_steps_start() {
    let (mut history, _) = captured();
    assert_eq!(
        history.events(vec![attempt("r2", 1)]).1,
        vec![MediaEvent::AttemptEnd]
    );
    let shown = photo("and this", "b");
    let (steered, events) = history.events(vec![steer(vec![shown.clone(), said("also")])]);
    assert_eq!(
        events,
        vec![MediaEvent::Accepted {
            record: steered,
            messages: vec![MediaMessage::of(&shown), MediaMessage::of(&said("also"))],
        }]
    );
    history.batch(vec![
        assistant("r2", 1, vec![text("seen")]),
        step_end("r2", 1, 0),
        facts(4),
    ]);
    let delivered = photo("from the queue", "c");
    let at = history.seq();
    let (_, events) = history.events(vec![
        delivery("r2", vec![delivered.clone()]),
        input("r2", Placement::Steer, Some(steered), shown),
        input("r2", Placement::Steer, Some(steered), said("also")),
        input("r2", Placement::Delivered, Some(at), delivered.clone()),
        step_start("r2", 2),
        facts(5),
        facts(6),
        facts(7),
    ]);
    assert_eq!(
        events,
        vec![
            MediaEvent::Accepted {
                record: at,
                messages: vec![MediaMessage::of(&delivered)],
            },
            MediaEvent::Placed { first: 4 },
            MediaEvent::StepStart { step: StepId(2) },
        ]
    );
}

/// A session's first capture is a steer, while a steer accepted before it, without media, waits
/// for the same boundary. The first capture hands over the waiting steer before its own, so the
/// engine places both from the first one's copy, as the full history would.
#[test]
fn a_first_capture_by_a_steer_hands_over_the_steers_waiting_before_it() {
    let mut history = History::new();
    plain_run(&mut history);
    history.batch(vec![
        start("r2", vec![render_config()], false),
        input("r2", Placement::RunInput, None, said("go")),
        facts(3),
    ]);
    history.batch(vec![step_start("r2", 1)]);
    let (early, feed) = history.batch(vec![steer(vec![said("wait")])]);
    assert!(feed.is_empty());
    let shown = photo("this", "a");
    let (late, feed) = history.batch(vec![steer(vec![shown.clone()])]);
    assert_eq!(
        feed.start,
        Some(MediaStart {
            steps: 2,
            last_step: Some(StepId(1)),
            step_open: true,
            batch: Vec::new(),
            limits: LimitsSource::Part(render_config()),
        })
    );
    assert_eq!(
        feed.items,
        vec![
            MediaItem::Event(MediaEvent::Accepted {
                record: early,
                messages: vec![MediaMessage::of(&said("wait"))],
            }),
            MediaItem::Event(MediaEvent::Accepted {
                record: late,
                messages: vec![MediaMessage::of(&shown)],
            }),
        ]
    );
    assert_eq!(
        history
            .events(vec![
                assistant("r2", 1, vec![text("ok")]),
                step_end("r2", 1, 0),
                facts(4),
            ])
            .1,
        vec![MediaEvent::StepEnd]
    );
    let (_, events) = history.events(vec![
        input("r2", Placement::Steer, Some(early), said("wait")),
        input("r2", Placement::Steer, Some(late), shown),
        step_start("r2", 2),
        facts(5),
        facts(6),
    ]);
    assert_eq!(
        events,
        vec![
            MediaEvent::Placed { first: 4 },
            MediaEvent::StepStart { step: StepId(2) },
        ]
    );
}

/// A batch whose results hold images in two of three calls: the session's first capture is the
/// first result, which hands over the batch's order and the run's limits. Each result is handed
/// over at its outcome, which is the capture's record, at the results message's ordinal: the
/// waiting assistant message's, plus one.
#[test]
fn a_tool_batch_with_images_in_two_results() {
    let mut history = History::new();
    plain_run(&mut history);
    history.batch(vec![
        start("r2", vec![render_config()], false),
        input("r2", Placement::RunInput, None, said("take photos")),
        facts(3),
    ]);
    step(
        &mut history,
        "r2",
        1,
        4,
        vec![call("x"), call("y"), call("z")],
    );
    history.batch(vec![batch_start("r2", 1, &["x", "y", "z"])]);
    let first = history.seq();
    let (_, feed) = history.batch(vec![
        reply("r2", 1, "x", &["a"]),
        outcome("r2", 1, "x"),
        reply("r2", 1, "y", &[]),
        outcome("r2", 1, "y"),
        reply("r2", 1, "z", &["b", "c"]),
        outcome("r2", 1, "z"),
        facts(5),
        turn_facts(4),
        batch_end("r2", 1),
    ]);
    assert_eq!(
        feed.start,
        Some(MediaStart {
            steps: 2,
            last_step: Some(StepId(1)),
            step_open: false,
            batch: ["x", "y", "z"].map(|id| ToolCallId(id.into())).to_vec(),
            limits: LimitsSource::Part(render_config()),
        })
    );
    let result = |record: u64, call: &str, parts: Vec<MediaPart>| {
        MediaItem::Event(MediaEvent::ToolResult {
            record,
            call: ToolCallId(call.into()),
            ordinal: Some(4),
            shown: true,
            media: parts,
        })
    };
    assert_eq!(
        feed.items,
        vec![
            result(first + 1, "x", media(&[(1, "a")])),
            result(first + 3, "y", Vec::new()),
            result(first + 5, "z", media(&[(1, "b"), (2, "c")])),
        ]
    );
    // The next batch is handed over whole: its start, and its results.
    step(&mut history, "r2", 2, 6, vec![call("w")]);
    let (_, events) = history.events(vec![batch_start("r2", 2, &["w"])]);
    assert_eq!(
        events,
        vec![MediaEvent::BatchStart {
            calls: vec![ToolCallId("w".into())],
        }]
    );
}

/// Where a record stored the bytes of the media it names is handed over with the record, by
/// digest, so the engine's owner can read them: a digest alone doesn't name the object.
#[test]
fn the_feed_hands_over_where_each_medium_is_stored() {
    let mut history = History::new();
    plain_run(&mut history);
    history.batch(vec![
        start("r2", vec![render_config()], false),
        input("r2", Placement::RunInput, None, said("take photos")),
        facts(3),
    ]);
    step(&mut history, "r2", 1, 4, vec![call("x")]);
    history.batch(vec![batch_start("r2", 1, &["x"])]);
    let stored = |part: u32, seq: u64| crate::record::MediaObject {
        part,
        log: crate::codec::Id128([1; 16]),
        generation: crate::codec::Id128([2; 16]),
        seq,
    };
    let mut photos = reply("r2", 1, "x", &["a", "b"]);
    let Record::ToolReply(written) = &mut photos else {
        unreachable!()
    };
    written.media = vec![stored(1, 40), stored(2, 40)];
    let (_, feed) = history.batch(vec![
        photos,
        outcome("r2", 1, "x"),
        facts(5),
        turn_facts(4),
        batch_end("r2", 1),
    ]);
    assert_eq!(
        feed.objects,
        vec![
            (image("a").sha256, stored(1, 40)),
            (image("b").sha256, stored(2, 40)),
        ]
    );
}

/// A result that arrives after its batch closed (a converted session's `late_result` event) is
/// captured but never shown, and no assistant message waits for it.
#[test]
fn a_late_result_is_captured_and_never_shown() {
    let (mut history, _) = captured();
    let (at, events) = history.events(vec![Record::Event(Event {
        name: "late_result".into(),
        run_id: Some("r2".into()),
        body: Some(serde_json::json!({
            "call_key": "k", "outcome": "completed", "result": result("held", &["d"]),
        })),
    })]);
    assert_eq!(
        events,
        vec![MediaEvent::ToolResult {
            record: at,
            call: ToolCallId("held".into()),
            ordinal: None,
            shown: false,
            media: media(&[(1, "d")]),
        }]
    );
}

/// A refreeze that changes the media limits hands them over; one whose config has none changes
/// nothing. A session's first capture after a refreeze starts with the refrozen limits.
#[test]
fn a_config_refreeze_changes_the_limits() {
    let (mut history, _) = captured();
    let (_, events) = history.events(vec![refrozen(
        "r2",
        serde_json::json!({ "media": { "max_images": 4 } }),
    )]);
    assert_eq!(
        events,
        vec![MediaEvent::LimitsChanged { limits: limits(4) }]
    );
    let (_, events) = history.events(vec![refrozen("r2", serde_json::json!({ "other": 1 }))]);
    assert_eq!(events, Vec::new());

    let mut history = History::new();
    plain_run(&mut history);
    history.batch(vec![
        start("r2", vec![render_config()], false),
        input("r2", Placement::RunInput, None, said("go")),
        facts(3),
    ]);
    let (_, feed) = history.batch(vec![refrozen(
        "r2",
        serde_json::json!({ "media": { "max_images": 3 } }),
    )]);
    assert!(feed.is_empty());
    let (_, feed) = history.batch(vec![steer(vec![photo("now", "a")])]);
    assert_eq!(
        feed.start.map(|start| start.limits),
        Some(LimitsSource::Known(Some(limits(3))))
    );
}

/// An import's seeded run is a run start whose inputs are the imported messages, from ordinal 1:
/// a user message's media are a person's, a tool result's media a tool's, as their skeletons
/// say.
#[test]
fn an_imports_seeded_run_places_the_imported_messages() {
    let mut history = History::new();
    let asked = photo("look", "a");
    let tool = Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(
            serde_json::from_value(result("t1", &["b"])).unwrap(),
        )],
    );
    let (at, feed) = history.batch(vec![
        start("seed", Vec::new(), true),
        input("seed", Placement::Imported, None, asked.clone()),
        facts(1),
        input(
            "seed",
            Placement::Imported,
            None,
            Message::assistant_from(vec![text("calling"), call("t1")], model()),
        ),
        turn_facts(2),
        facts(2),
        input("seed", Placement::Imported, None, tool.clone()),
        facts(3),
        end("seed"),
    ]);
    assert_eq!(
        feed.start,
        Some(MediaStart {
            steps: 0,
            last_step: None,
            step_open: false,
            batch: Vec::new(),
            limits: LimitsSource::Known(None),
        })
    );
    let tool_skeleton = MediaMessage::of(&tool);
    assert!(tool_skeleton.slots[0].tool_part.is_some());
    assert_eq!(
        feed.items,
        vec![MediaItem::Event(MediaEvent::RunStart {
            record: at,
            inputs: vec![
                MediaInput {
                    message: 0,
                    ordinal: 0,
                    skeleton: MediaMessage::of(&asked),
                },
                MediaInput {
                    message: 2,
                    ordinal: 2,
                    skeleton: tool_skeleton,
                },
            ],
            limits: None,
        })]
    );
}

/// The records only the new storage writes that don't touch media hand nothing over: an
/// enrollment in the current compaction contract and an applied refresh, written in a run
/// start's batch after its inputs, and a run's billing.
#[test]
fn enrollment_refresh_and_billing_hand_nothing_over() {
    let (mut history, _) = captured();
    history.batch(vec![end("r2")]);
    let (at, events) = history.events(vec![
        start("r3", Vec::new(), false),
        input("r3", Placement::RunInput, None, said("again")),
        facts(4),
        Record::RefreshApplied(RefreshApplied {
            generation: 1,
            applied_by: "append".into(),
            manifest_digest: "d".into(),
            composition_digest: None,
            absent_items: Vec::new(),
        }),
        Record::CompactionEnrolled(CompactionEnrolled {
            through_ordinal: 3,
            tag_floor: 0,
            tag_high: 0,
            overlay: EnrollmentOverlayRef {
                log: Id128([1; 16]),
                generation: Id128([2; 16]),
                seq: 1,
            },
        }),
    ]);
    assert_eq!(
        events,
        vec![MediaEvent::RunStart {
            record: at,
            inputs: Vec::new(),
            limits: None,
        }]
    );
    let (_, events) = history.events(vec![Record::Billed(Billed {
        run_id: "r3".into(),
    })]);
    assert_eq!(events, Vec::new());
}

/// A record of the first compaction protocol hands over no event: it names the seq where only a
/// saved index can carry the session on.
#[test]
fn a_first_protocol_compaction_record_needs_a_saved_index() {
    let (mut history, _) = captured();
    let (at, feed) = history.batch(vec![Record::Compaction(Compaction {
        version: 1,
        body: serde_json::json!({}),
    })]);
    assert_eq!(feed.needs_saved_index, Some(at));
    assert!(feed.items.is_empty());
    let (at, feed) = history.batch(vec![Record::Event(Event {
        name: "compaction_view_applied".into(),
        run_id: None,
        body: Some(serde_json::json!({})),
    })]);
    assert_eq!(feed.needs_saved_index, Some(at));
    assert!(feed.items.is_empty());
}

/// A session that never captured a medium keeps four scalars and nothing else, however long it
/// runs, and hands nothing over: no step's start or end, no attempt.
#[test]
fn a_session_without_media_keeps_four_scalars_and_hands_nothing_over() {
    let mut history = History::new();
    let mut ordinal = 0;
    for run in 0..250 {
        let run = format!("r{run}");
        ordinal += 1;
        let (_, feed) = history.batch(vec![
            start(&run, vec![render_config()], false),
            input(&run, Placement::RunInput, None, said("work")),
            facts(ordinal),
        ]);
        assert!(feed.is_empty());
        for step_id in 1..=4 {
            history.batch(vec![step_start(&run, step_id)]);
            // A retry starts the same step again.
            history.batch(vec![step_start(&run, step_id)]);
            history.batch(vec![attempt(&run, step_id)]);
            ordinal += 1;
            let (_, feed) = history.batch(vec![
                assistant(&run, step_id, vec![call("c")]),
                step_end(&run, step_id, 1),
                facts(ordinal),
            ]);
            assert!(feed.is_empty());
            history.batch(vec![batch_start(&run, step_id, &["c"])]);
            ordinal += 1;
            let (_, feed) = history.batch(vec![
                reply(&run, step_id, "c", &[]),
                outcome(&run, step_id, "c"),
                facts(ordinal),
                batch_end(&run, step_id),
            ]);
            assert!(feed.is_empty());
        }
        history.batch(vec![end(&run)]);
    }
    let fold = &history.media;
    assert_eq!(fold.steps, 1_000);
    assert_eq!(fold.last_step, Some(4));
    assert!(!fold.step_open);
    assert_eq!(fold.batch, vec![ToolCallId("c".into())]);
    assert_eq!(fold.captured, None);
    assert_eq!(fold.work, None);
    // The four scalars, the unset capture state and the empty slot for a batch's records.
    assert!(
        std::mem::size_of::<MediaFold>() <= 80,
        "{}",
        std::mem::size_of::<MediaFold>()
    );
}

fn recovery_cut(ordinal: Option<u64>) -> Record {
    Record::RecoveryCut(crate::record::RecoveryCut {
        change: Id128([3; 16]),
        at: 0,
        durable_bound: 0,
        removed_from: 1,
        removed_through: 1,
        loss_aware: true,
        transcript_from: ordinal.map(|_| 1),
        ordinal,
        history_version: ordinal.map(|_| 2),
        unattributed_risk: false,
        saved: "removed".into(),
    })
}

/// A recovery cut that may have lost the transcript from message 5 (counted from 1, as the
/// record counts) keeps messages 1 to 4: ordinals `0..4` as events count them. It drops the
/// accepted messages not placed yet. A recovery that lost no transcript record cuts nothing.
#[test]
fn a_recovery_cut_keeps_the_messages_before_the_first_it_may_have_lost() {
    let (mut history, _) = captured();
    history.events(vec![steer(vec![photo("late", "b")])]);
    assert!(history
        .media
        .captured
        .as_ref()
        .is_some_and(|captured| captured.first_accepted.is_some()));
    let (_, events) = history.events(vec![recovery_cut(None)]);
    assert_eq!(events, Vec::new());
    let (_, events) = history.events(vec![recovery_cut(Some(5))]);
    assert_eq!(events, vec![MediaEvent::Truncated { len: 4 }]);
    assert_eq!(
        history.media.captured,
        Some(Captured {
            first_accepted: None
        })
    );
}
