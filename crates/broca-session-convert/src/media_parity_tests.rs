//! The image engine fed two ways over the same history: through the media events the session
//! store's own fold produces from its records (`broca_session_store::fold::media`), and through
//! the events today's storage's records stand for (`WalMediaEvents`, which the engine's own tests
//! hold equal to its old fold of those records). After each batch the two engines hold the same
//! facts. The store's engine is created at the session's first capture from the scalars the fold
//! hands over then, as the session's owner creates it.

use broca_core::media_request::{SessionMedia, WalMediaEvents};
use broca_core::media_store::{MediaBytes, MediaReadError};
use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, MediaRef, Message, Role};
use broca_protocol::ids::{MessageId, StepId, ToolCallId};
use broca_protocol::media_event::MediaEvent;
use broca_protocol::request::ModelHandle;
use broca_session_store::codec::Digest;
use broca_session_store::fold::conversation::Conversation;
use broca_session_store::fold::media::{MediaFeed, MediaFold, MediaItem, MediaStart};
use broca_session_store::fold::operations::Operations;
use broca_session_store::record::{
    Assistant, Attempt, BatchEnd, BatchStart, CallOutcome, Delivery, EntryFacts, Event, Input,
    Placement, Record, RunEnd, RunStart, Steer, StepEnd, StepStart, ToolOutcome, ToolReply,
};
use broca_wal::WalRecord;
use serde_json::json;

/// The engine reads no bytes here.
#[derive(Debug)]
struct NoBytes;

impl MediaBytes for NoBytes {
    fn read_verified(&self, _reference: &MediaRef) -> Result<Vec<u8>, MediaReadError> {
        unreachable!("the facts compared read no bytes")
    }
}

fn engine() -> SessionMedia {
    SessionMedia::new(NoBytes)
}

/// The engine the session's owner creates at the first capture: one whose step counter, latest
/// step and batch order are the scalars the fold kept until then.
fn engine_from(start: &MediaStart) -> SessionMedia {
    let mut engine = engine();
    let last = start.last_step.map(|step| step.0);
    // Distinct consecutive step ids each move the counter once; none of them is the last.
    let others = [u64::MAX - 1, u64::MAX - 2];
    let moves = start.steps - u64::from(last.is_some());
    for n in 0..moves {
        let step = StepId(others[n as usize % 2]);
        engine.apply(&MediaEvent::StepStart { step }).unwrap();
    }
    match last {
        Some(step) => engine
            .apply(&MediaEvent::StepStart { step: StepId(step) })
            .unwrap(),
        // A run start forgets the latest step and adds nothing.
        None => engine
            .apply(&MediaEvent::RunStart {
                record: 0,
                inputs: Vec::new(),
                limits: None,
            })
            .unwrap(),
    }
    engine
        .apply(&MediaEvent::BatchStart {
            calls: start.batch.clone(),
        })
        .unwrap();
    engine
}

/// One history, folded both ways.
struct BothWays {
    conversation: Conversation,
    operations: Operations,
    fold: MediaFold,
    next: u64,
    /// The engine fed the store's events, once the session captured something.
    store: Option<SessionMedia>,
    events: WalMediaEvents,
    /// The engine fed the events of today's records.
    today: SessionMedia,
}

impl BothWays {
    fn new() -> Self {
        Self {
            conversation: Conversation::new(false),
            operations: Operations::default(),
            fold: MediaFold::default(),
            next: 1,
            store: None,
            events: WalMediaEvents::new(),
            today: engine(),
        }
    }

    /// Folds one batch of store records, and today's records it stands for, each named by the
    /// position in the batch of the store record that holds what it captures.
    fn batch(&mut self, records: Vec<Record>, today: Vec<(usize, WalRecord)>) {
        let first = self.next;
        let mut feed = MediaFeed::default();
        for record in &records {
            let seq = self.next;
            self.next += 1;
            self.fold
                .apply(seq, record, &self.conversation, &self.operations, &mut feed);
            broca_session_store::session::apply(
                &mut self.conversation,
                &mut self.operations,
                seq,
                record,
            )
            .unwrap_or_else(|error| panic!("{} at {seq}: {error}", record.name()));
        }
        self.fold.flush(&mut feed);
        assert_eq!(feed.needs_saved_index, None);
        for (at, record) in today {
            for event in self.events.events(first + at as u64, &record).unwrap() {
                self.today.apply(&event).unwrap();
            }
        }
        if let Some(start) = &feed.start {
            self.store = Some(engine_from(start));
        }
        if let Some(store) = self.store.as_mut() {
            for item in feed.items {
                let event = match item {
                    MediaItem::Event(event) => event,
                    // Limits change no fact the engine holds.
                    MediaItem::RunStart(run) => run.with_limits(None),
                };
                store.apply(&event).unwrap();
            }
        }
    }

    fn assert_same(&self, context: &str) {
        let store = self.store.as_ref().expect("the session captured something");
        assert_eq!(store.facts(), self.today.facts(), "{context}");
    }
}

fn model() -> ModelHandle {
    ModelHandle::new("anthropic", "claude")
}

fn image(name: &str) -> MediaRef {
    serde_json::from_value(json!({
        "kind": "image", "mime": "image/png", "byte_size": 1,
        "sha256": name.repeat(64), "width": 1, "height": 1,
    }))
    .unwrap()
}

fn said(words: &str) -> Message {
    Message::new(Role::User, vec![ContentBlock::Text { text: words.into() }])
}

fn photo(words: &str, name: &str) -> Message {
    Message::new(
        Role::User,
        vec![
            ContentBlock::Text { text: words.into() },
            ContentBlock::Media(image(name)),
        ],
    )
}

/// A tool result with a text part, then one media part per image.
fn result(id: &str, images: &[&str]) -> serde_json::Value {
    let mut parts = vec![json!({"kind": "text", "text": "photos"})];
    for name in images {
        parts.push(json!({"kind": "media", "media": image(name)}));
    }
    json!({"tool_call_id": id, "output": {"kind": "parts", "parts": parts}, "is_error": false})
}

// The session store's records.

fn start(run: &str) -> Record {
    Record::RunStart(Box::new(RunStart {
        named_speaker: None,
        run_id: run.into(),
        model: model(),
        setup: Vec::new(),
        renderer_version: 1,
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
        seeded: None,
        refusal_schedule: None,
        imported_ids: None,
        message_marks: None,
    }))
}

fn end(run: &str) -> Record {
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

fn input(run: &str, placement: Placement, receipt: Option<u64>, message: Message) -> Record {
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

fn facts(ordinal: u64) -> Record {
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

fn step_start(run: &str, step: u64) -> Record {
    Record::StepStart(StepStart {
        run_id: run.into(),
        step_id: step,
        mid: None,
    })
}

fn attempt(run: &str, step: u64) -> Record {
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

fn call_block(id: &str) -> ContentBlock {
    serde_json::from_value(
        json!({"type": "tool_call", "tool_call_id": id, "tool_name": "screen", "input": {}}),
    )
    .unwrap()
}

fn assistant(run: &str, step: u64, calls: &[&str]) -> Record {
    let content = if calls.is_empty() {
        vec![ContentBlock::Text {
            text: "continue".into(),
        }]
    } else {
        calls.iter().map(|id| call_block(id)).collect()
    };
    Record::Assistant(Assistant {
        run_id: run.into(),
        step_id: step,
        provider_message_id: MessageId(format!("answer{step}")),
        message: Message::assistant_from(content, model()),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn step_end(run: &str, step: u64, calls: u32) -> Record {
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

fn batch_start(run: &str, step: u64, calls: &[&str]) -> Record {
    Record::BatchStart(BatchStart {
        run_id: run.into(),
        batch_id: "batch".into(),
        step_id: step,
        planned: calls.iter().map(|id| ToolCallId((*id).into())).collect(),
        mid: None,
    })
}

fn batch_end(run: &str) -> Record {
    Record::BatchEnd(BatchEnd {
        run_id: run.into(),
        batch_id: "batch".into(),
    })
}

fn reply(run: &str, result: &serde_json::Value) -> Record {
    Record::ToolReply(ToolReply {
        run_id: run.into(),
        batch_id: Some("batch".into()),
        result: serde_json::from_value(result.clone()).unwrap(),
        finished_at_ms: None,
        tags_v1: None,
        media: Vec::new(),
    })
}

fn outcome(run: &str, id: &str) -> Record {
    Record::ToolOutcome(ToolOutcome {
        run_id: run.into(),
        batch_id: "batch".into(),
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

fn steer(messages: &[Message]) -> Record {
    Record::Steer(Steer {
        input: messages.to_vec(),
        written_at_ms: None,
        tags_v1: None,
        receipt: None,
        media: Vec::new(),
        message_marks: None,
        named_speaker: None,
    })
}

fn delivery(run: &str, mode: &str, messages: &[Message]) -> Record {
    Record::Delivery(Delivery {
        submission_id: "send".into(),
        run_id: run.into(),
        mode: mode.into(),
        input: messages.to_vec(),
        tool_call_id: None,
        appended_text: None,
        written_at_ms: None,
        tags_v1: None,
        input_listed: Some(true),
    })
}

fn late_event(result: &serde_json::Value) -> Record {
    Record::Event(Event {
        name: "late_result".into(),
        run_id: Some("run".into()),
        body: Some(json!({"call_key": "k", "outcome": "completed", "result": result})),
    })
}

// Today's records.

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

fn started(input: &[Message]) -> WalRecord {
    record(json!({
        "type": "run_started", "run_id": "run",
        "session": {"project_root": "/fixture", "harness": "media", "session": "session"},
        "config": {"model": model(), "tools": [],
            "tool_choice": {"type": "auto"}, "generation": {}, "response_format": {"type": "text"},
            "context_epoch": 0, "renderer_version": 1, "frozen_render_config": {}},
        "input": input, "origin": {"kind": "fresh"}
    }))
}

fn step_started(step: u64) -> WalRecord {
    WalRecord::StepStarted {
        step_id: StepId(step),
    }
}

fn attempt_finished(step: u64) -> WalRecord {
    record(
        json!({"type": "model_attempt_finished", "step_id": step, "attempt": 1,
        "outcome": "complete", "ts_ms": 1, "request_body_len": 0, "request_body_sha256": "a",
        "response_body_len": 0, "response_body_sha256": "b"}),
    )
}

fn step_finished(step: u64, calls: &[&str]) -> WalRecord {
    let content = if calls.is_empty() {
        json!([{"type": "text", "text": "continue"}])
    } else {
        calls
            .iter()
            .map(|id| json!({"type": "tool_call", "tool_call_id": id, "tool_name": "screen", "input": {}}))
            .collect()
    };
    record(json!({"type": "model_step_finished", "step_id": step,
        "assistant_message": {"message_id": format!("answer{step}"), "content": content},
        "usage": broca_protocol::Usage::default()}))
}

fn batch_started(step: u64, calls: &[&str]) -> WalRecord {
    record(
        json!({"type": "tool_batch_started", "step_id": step, "batch_id": "batch",
        "planned_calls": calls}),
    )
}

fn tool_result(result: &serde_json::Value) -> WalRecord {
    record(
        json!({"type": "tool_result", "batch_id": "batch", "result": result,
        "outcome": "completed"}),
    )
}

fn batch_finished() -> WalRecord {
    record(json!({"type": "tool_batch_finished", "batch_id": "batch"}))
}

fn steered(input: &[Message]) -> WalRecord {
    record(json!({"type": "steered", "input": input}))
}

fn queued(input: &[Message]) -> WalRecord {
    record(
        json!({"type": "queued", "submission_id": "send", "request": {
        "model": model(), "input": input}}),
    )
}

fn send_delivered(mode: &str, input: &[Message]) -> WalRecord {
    record(
        json!({"type": "send_delivered", "submission_id": "send", "run_id": "run",
        "mode": mode, "input": input}),
    )
}

fn late(result: &serde_json::Value) -> WalRecord {
    record(
        json!({"type": "late_result", "call_key": "k", "outcome": "completed",
        "result": result}),
    )
}

fn run_finished() -> WalRecord {
    record(json!({"type": "run_finished", "reason": "completed", "usage": {}}))
}

/// A run whose input holds an image, at ordinal 1, and its first step started: the session's
/// first capture.
fn captured() -> BothWays {
    let mut both = BothWays::new();
    let looked = [photo("look", "a")];
    both.batch(
        vec![
            start("run"),
            input("run", Placement::RunInput, None, looked[0].clone()),
            facts(1),
        ],
        vec![(0, started(&looked))],
    );
    both.batch(vec![step_start("run", 1)], vec![(0, step_started(1))]);
    both.assert_same("the first capture");
    both
}

/// A step that answers with `calls`, at ordinal `ordinal`, as both storages write it.
fn answered(both: &mut BothWays, step: u64, ordinal: u64, calls: &[&str]) {
    both.batch(
        vec![attempt("run", step)],
        vec![(0, attempt_finished(step))],
    );
    both.batch(
        vec![
            assistant("run", step, calls),
            step_end("run", step, calls.len() as u32),
            facts(ordinal),
        ],
        vec![(1, step_finished(step, calls))],
    );
}

/// A tool result whose output is a list of parts without an image still takes its call's place
/// in the results message, once the session has an index: today's fold keeps its text-free copy,
/// and so does the engine fed the store's events.
#[test]
fn text_only_parts_results_keep_their_place_both_ways() {
    let mut both = captured();
    answered(&mut both, 1, 2, &["first", "second"]);
    both.batch(
        vec![batch_start("run", 1, &["first", "second"])],
        vec![(0, batch_started(1, &["first", "second"]))],
    );
    let text_only = result("first", &[]);
    let shown = result("second", &["b"]);
    both.batch(
        vec![
            reply("run", &text_only),
            outcome("run", "first"),
            reply("run", &shown),
            outcome("run", "second"),
            facts(3),
            batch_end("run"),
        ],
        vec![
            (1, tool_result(&text_only)),
            (3, tool_result(&shown)),
            (5, batch_finished()),
        ],
    );
    both.assert_same("after the batch");
    both.batch(vec![step_start("run", 2)], vec![(0, step_started(2))]);
    both.assert_same("at the next step");
}

/// A result that arrives after its batch closed is captured at the results message waiting when
/// it arrives, if any, and never shown.
#[test]
fn late_results_take_the_waiting_results_ordinal_both_ways() {
    let mut both = captured();
    answered(&mut both, 1, 2, &["a"]);
    both.batch(
        vec![batch_start("run", 1, &["a"])],
        vec![(0, batch_started(1, &["a"]))],
    );
    // A late result of an older call while this batch's results wait.
    let older = result("older", &["c"]);
    both.batch(vec![late_event(&older)], vec![(0, late(&older))]);
    both.assert_same("a late result while results wait");
    let shown = result("a", &["b"]);
    both.batch(
        vec![
            reply("run", &shown),
            outcome("run", "a"),
            facts(3),
            batch_end("run"),
        ],
        vec![(1, tool_result(&shown)), (3, batch_finished())],
    );
    both.batch(vec![step_start("run", 2)], vec![(0, step_started(2))]);
    // A late result when no results wait.
    let after = result("a", &["d"]);
    both.batch(vec![late_event(&after)], vec![(0, late(&after))]);
    both.assert_same("a late result with no results waiting");
    both.batch(vec![step_start("run", 3)], vec![(0, step_started(3))]);
    both.assert_same("at the next step");
}

/// A steer is placed at the next step's boundary before that step starts, so its image is
/// protected through that step, both ways; a steer accepted before the session's first capture,
/// which the store's engine never held, takes its place ahead of it.
#[test]
fn a_boundary_places_its_steers_before_the_step_starts_both_ways() {
    let mut both = BothWays::new();
    let first = [said("no media yet")];
    both.batch(
        vec![
            start("run"),
            input("run", Placement::RunInput, None, first[0].clone()),
            facts(1),
        ],
        vec![(0, started(&first))],
    );
    both.batch(vec![step_start("run", 1)], vec![(0, step_started(1))]);
    let early = [said("wait")];
    let early_seq = both.next;
    both.batch(vec![steer(&early)], vec![(0, steered(&early))]);
    let shown = [photo("and this", "b"), said("also")];
    let late_seq = both.next;
    both.batch(vec![steer(&shown)], vec![(0, steered(&shown))]);
    both.assert_same("the first capture, a steer");
    answered(&mut both, 1, 2, &[]);
    both.batch(
        vec![
            input("run", Placement::Steer, Some(early_seq), early[0].clone()),
            input("run", Placement::Steer, Some(late_seq), shown[0].clone()),
            input("run", Placement::Steer, Some(late_seq), shown[1].clone()),
            step_start("run", 2),
            facts(3),
            facts(4),
            facts(5),
        ],
        vec![(3, step_started(2))],
    );
    both.assert_same("the boundary that places them");
    answered(&mut both, 2, 6, &[]);
    both.batch(vec![step_start("run", 3)], vec![(0, step_started(3))]);
    both.assert_same("the step after");
}

/// A queued send delivered as turn-scoped system text holds messages too: they are accepted, and
/// placed at the boundary that delivers them, as a steer's are.
#[test]
fn turn_system_deliveries_place_their_messages_both_ways() {
    let mut both = captured();
    answered(&mut both, 1, 2, &[]);
    let sent = [photo("from the queue", "b")];
    let at = both.next;
    both.batch(
        vec![
            delivery("run", "turn_system", &sent),
            input(
                "run",
                Placement::DeliveredTurnSystem,
                Some(at),
                sent[0].clone(),
            ),
            step_start("run", 2),
            facts(3),
        ],
        vec![
            (0, queued(&sent)),
            (0, send_delivered("turn_system", &sent)),
            (2, step_started(2)),
        ],
    );
    both.assert_same("the boundary that delivers it");
    answered(&mut both, 2, 4, &[]);
    both.batch(vec![end("run")], vec![(0, run_finished())]);
    both.assert_same("the run's end");
}
