use broca_protocol::{ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult};
use cortexkit_compaction_view::{rules, tags};
use cortexkit_role_compaction::{
    part::{Delivered, PartBody, Seal, ToolStatus},
    read::{Detail, ReadRequest, Select},
    view::{Edit, EditOp, View, ViewRefusal},
};
use serde_json::json;

use super::{
    entries::{project, Deliveries, EntryFacts},
    render, runtime, state, views,
};

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

fn facts(count: usize) -> Vec<EntryFacts> {
    (0..count)
        .map(|position| EntryFacts {
            mid: format!("m12345678.{position}"),
            written_at: 100 + position as u64,
            completed_at: Some(200 + position as u64),
            ..EntryFacts::default()
        })
        .collect()
}

fn call() -> Message {
    Message::new(
        Role::Assistant,
        vec![
            ContentBlock::Reasoning {
                text: "private".into(),
                signature: Some("signature".into()),
            },
            ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId("call".into()),
                tool_name: "read".into(),
                input: json!({"path":"a"}),
                thought_signature: Some("thought".into()),
            }),
        ],
    )
}

fn result(output: ToolOutput) -> Message {
    Message::new(
        Role::Tool,
        vec![ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId("call".into()),
            output,
            is_error: false,
        })],
    )
}

#[test]
fn compaction_v2_entries_merge_results_without_merging_delivered_text() {
    let messages = vec![
        text(Role::User, "question"),
        call(),
        result(ToolOutput::Json {
            value: json!({"z":2,"a":1}),
        }),
    ];
    let delivered = Delivered::new("ck-mc", "remember").with_template("reminder");
    let deliveries = Deliveries::from([("call".into(), vec![delivered.clone()])]);
    let entries = project(&messages, &facts(3), &deliveries, true);
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.ordinal)
            .collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert!(entries[2].parts.is_empty());
    assert_eq!(entries[0].completed_at, None);
    assert_eq!(entries[2].completed_at, None);
    assert_eq!(entries[1].completed_at, Some(201));
    assert_eq!(entries[1].parts[0].seal, Seal::Signed);
    assert_eq!(entries[1].parts[0].tag, None);
    let part = &entries[1].parts[1];
    assert_eq!(part.id, "1");
    assert_eq!(part.tag, Some(2));
    assert_eq!(part.seal, Seal::Signed);
    let PartBody::Tool(tool) = &part.body else {
        panic!("tool part missing")
    };
    assert_eq!(tool.output.as_deref(), Some(r#"{"a":1,"z":2}"#));
    assert_eq!(tool.delivered, [delivered]);
    assert_eq!(tool.status, ToolStatus::Completed);
    let PartBody::Tool(compact) = part.compact().body else {
        panic!("tool part missing")
    };
    assert_eq!(compact.output, None);
    assert_eq!(compact.delivered[0].text, None);
    assert_eq!(compact.delivered[0].text_bytes, 8);
    assert_eq!(part.digest, part.compact().digest);
}

#[test]
fn compaction_v2_unfinished_tool_prevents_completed_at() {
    let entries = project(&[call()], &facts(1), &Deliveries::new(), true);
    assert_eq!(entries[0].completed_at, None);
    let PartBody::Tool(tool) = &entries[0].parts[1].body else {
        panic!("tool part missing")
    };
    assert_eq!(tool.status, ToolStatus::Running);
    assert_eq!(tool.output, None);
}

#[test]
fn compaction_v2_reply_cleaning_does_not_clean_user_or_tool_text() {
    let messages = vec![
        text(Role::User, "§99§ question"),
        text(Role::Assistant, "§2§ answer §99§"),
        call(),
        result(ToolOutput::Denied {
            reason: "§99§ denied".into(),
        }),
    ];
    let entries = project(&messages, &facts(4), &Deliveries::new(), true);
    assert!(
        matches!(&entries[0].parts[0].body, PartBody::Text { text } if text == "§99§ question")
    );
    assert!(matches!(&entries[1].parts[0].body, PartBody::Text { text } if text == "answer"));
    assert!(
        matches!(&entries[2].parts[1].body, PartBody::Tool(tool) if tool.output.as_deref() == Some("§99§ denied") && tool.status == ToolStatus::Denied)
    );
}

#[test]
fn compaction_v2_view_checks_use_full_original_text_and_deliveries() {
    let messages = vec![text(Role::User, "original §99§")];
    let entries = project(&messages, &facts(1), &Deliveries::new(), true);
    let replace = |value: &str| {
        View::seal(
            1,
            vec![Edit::new(
                1,
                EditOp::Replace {
                    mid: entries[0].mid.clone(),
                    part: "0".into(),
                    field: "text".into(),
                    value: json!(value),
                    expect: entries[0].parts[0].digest.clone(),
                },
            )],
        )
    };
    let rules = rules::defaults("gpt-5");
    assert!(views::check(&entries, None, &replace("kept §99§"), &rules, true).is_ok());
    assert!(matches!(
        views::check(&entries, None, &replace("foreign §98§"), &rules, true),
        Err(ViewRefusal::ForeignTag { tag: 98, .. })
    ));
    let view = replace("replacement");
    assert!(matches!(
        views::check(&entries, Some(&view), &view, &rules, true),
        Err(ViewRefusal::StaleVersion { .. })
    ));
    let mut bad = view;
    bad.checksum = "invalid".into();
    assert!(matches!(
        views::check(&entries, None, &bad, &rules, true),
        Err(ViewRefusal::BadChecksum)
    ));
}

#[test]
fn compaction_v2_marker_checkpoint_keeps_existing_numbers() {
    let messages = vec![
        text(Role::User, "one"),
        text(Role::Assistant, "two"),
        text(Role::User, "three"),
    ];
    let mut entries = project(&messages, &facts(3), &Deliveries::new(), true);
    let before = entries[2].parts[0].tag;
    tags::assign_tags(2, &mut entries[2..]);
    assert_eq!(entries[2].parts[0].tag, before);
    assert_eq!(before, Some(3));
}

#[test]
fn compaction_v2_entries_page_returns_one_large_entry_and_continuation() {
    let messages = vec![
        text(Role::User, "long first entry"),
        text(Role::Assistant, "second"),
    ];
    let entries = project(&messages, &facts(2), &Deliveries::new(), true);
    let request = ReadRequest::new(
        Select::After {
            after_ordinal: None,
            limit: Some(1),
        },
        Detail::Full,
    )
    .with_max_bytes(1);
    let page = views::page(&entries, &request, None);
    assert_eq!(page.entries.len(), 1);
    assert_eq!(page.entries[0].mid, "m12345678.0");
    assert_eq!(page.next_after, Some(1));
    assert_eq!(page.head.newest.unwrap().ordinal, 2);
}

#[test]
fn compaction_v2_render_keeps_provider_blocks_and_applies_delivered_after_output() {
    let mut messages = vec![
        text(Role::User, "question"),
        call(),
        result(ToolOutput::Json {
            value: json!({"a":1}),
        }),
    ];
    // The signed call is kept unchanged; only its result's unsigned text is numbered.
    if let ContentBlock::ToolCall(call) = &mut messages[1].content[1] {
        call.thought_signature = None;
    }
    let deliveries = Deliveries::from([("call".into(), vec![Delivered::new("ck-mc", "remember")])]);
    let entries = project(&messages, &facts(3), &deliveries, true);
    let view = View::seal(
        1,
        vec![Edit::new(
            1,
            EditOp::Replace {
                mid: entries[1].mid.clone(),
                part: "1".into(),
                field: "output".into(),
                value: json!("smaller"),
                expect: entries[1].parts[1].digest.clone(),
            },
        )],
    );
    let rendered =
        render::prompt(&messages, &entries, &view.edits, |_| panic!("no image")).unwrap();
    assert_eq!(rendered[1].content, messages[1].content);
    let ContentBlock::ToolResult(result) = &rendered[2].content[0] else {
        panic!("result missing")
    };
    assert_eq!(
        result.output,
        ToolOutput::Text {
            text: "smaller\n\nremember".into()
        }
    );
    let removal = Edit::new(
        2,
        EditOp::Replace {
            mid: entries[1].mid.clone(),
            part: "1".into(),
            field: "delivered".into(),
            value: json!([]),
            expect: entries[1].parts[1].digest.clone(),
        },
    );
    let mut edits = view.edits;
    edits.push(removal);
    let rendered = render::prompt(&messages, &entries, &edits, |_| panic!("no image")).unwrap();
    let ContentBlock::ToolResult(result) = &rendered[2].content[0] else {
        panic!("result missing")
    };
    assert_eq!(
        result.output,
        ToolOutput::Text {
            text: "smaller".into()
        }
    );
}

#[test]
fn compaction_v2_render_drops_both_halves_of_a_tool_and_preserves_correct_reply_bytes() {
    let messages = vec![
        text(Role::Assistant, "§1§ \n answer  "),
        call(),
        result(ToolOutput::Text {
            text: "done".into(),
        }),
    ];
    let entries = project(&messages, &facts(3), &Deliveries::new(), true);
    let edits = vec![Edit::new(
        1,
        EditOp::Drop {
            mid: entries[1].mid.clone(),
            part: Some("1".into()),
        },
    )];
    let rendered = render::prompt(&messages, &entries, &edits, |_| panic!("no image")).unwrap();
    assert_eq!(rendered.len(), 2);
    assert_eq!(rendered[0].content, messages[0].content);
    assert_eq!(rendered[1].content, vec![messages[1].content[0].clone()]);
}

fn setup() -> cortexkit_role_compaction::setup::SetupRequest {
    use cortexkit_role_compaction::setup::{ModelRef, SetupRequest};
    SetupRequest::new(
        "worker",
        ModelRef::new("openai", "gpt-5"),
        1000,
        100,
        rules::defaults("gpt-5"),
        runtime::detects(),
        1,
    )
}

fn conditions() -> cortexkit_role_compaction::notice::Conditions {
    let setup = setup();
    cortexkit_role_compaction::notice::Conditions::new(
        setup.model,
        setup.context_window,
        setup.output_reserve,
        setup.rules,
    )
}

#[derive(Default)]
struct RecordingPlane {
    calls: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
    answers: std::sync::Mutex<std::collections::VecDeque<serde_json::Value>>,
}

#[async_trait::async_trait]
impl runtime::Plane for RecordingPlane {
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, runtime::CallError> {
        self.calls.lock().unwrap().push((method.into(), params));
        Ok(self
            .answers
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted answer"))
    }
}

#[tokio::test]
async fn compaction_v2_setup_unknown_answer_fails_and_next_setup_restarts_notice_seq() {
    let plane = RecordingPlane::default();
    plane.answers.lock().unwrap().extend([
        json!({"answer":"future"}),
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
    ]);
    let runtime = runtime::Runtime::default();
    assert!(!runtime.setup(&plane, &setup()).await);
    assert!(runtime.setup(&plane, &setup()).await);
    runtime.observe(&project(
        &[text(Role::User, "one")],
        &facts(1),
        &Deliveries::new(),
        true,
    ));
    runtime.flush(&plane, &setup(), &conditions(), None).await;
    assert!(runtime.setup(&plane, &setup()).await);
    runtime.observe(&project(
        &[text(Role::User, "one"), text(Role::User, "two")],
        &facts(2),
        &Deliveries::new(),
        true,
    ));
    runtime.flush(&plane, &setup(), &conditions(), None).await;
    let calls = plane.calls.lock().unwrap();
    let notices: Vec<_> = calls
        .iter()
        .filter(|(method, _)| method == "compaction.notice")
        .collect();
    assert_eq!(notices.len(), 2);
    assert_eq!(notices[0].1["seq"], 1);
    assert_eq!(notices[1].1["seq"], 1);
    assert_eq!(notices[1].1["entries"].as_array().unwrap().len(), 1);
    assert!(
        calls[0].1.get("entries").is_none(),
        "Setup must be content-free"
    );
}

#[tokio::test]
async fn compaction_v2_ride_flushes_changed_entries_and_completion_alone_is_silent() {
    use cortexkit_role_compaction::ride::{RideCause, RideRequest};
    let plane = RecordingPlane::default();
    plane.answers.lock().unwrap().extend([
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
        json!({"answer":"nothing"}),
    ]);
    let runtime = runtime::Runtime::default();
    let mut entries = project(
        &[text(Role::Assistant, "answer")],
        &facts(1),
        &Deliveries::new(),
        true,
    );
    entries[0].completed_at = None;
    runtime.observe(&entries);
    entries[0].completed_at = Some(1000);
    runtime.observe(&entries);
    runtime
        .ride(
            &plane,
            &setup(),
            &RideRequest::new(vec![RideCause::CacheExpired], 1000, conditions()),
        )
        .await;
    runtime.flush(&plane, &setup(), &conditions(), None).await;
    let calls = plane.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        ["compaction.setup", "compaction.notice", "compaction.ride"]
    );
}

#[derive(Default)]
struct MemoryJournal {
    facts: std::sync::Mutex<Vec<String>>,
    fail: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl state::Journal for MemoryJournal {
    async fn accepted(&self, _: Option<&View>, view: &View) -> Result<(), String> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("sync failed".into());
        }
        self.facts
            .lock()
            .unwrap()
            .push(format!("accepted:{}", view.version));
        Ok(())
    }
    async fn applied(&self, step: u64, view: &View) -> Result<(), String> {
        self.facts
            .lock()
            .unwrap()
            .push(format!("applied:{}:{step}", view.version));
        Ok(())
    }
    async fn marker(
        &self,
        view: &View,
        mid: &str,
        ordinal: u64,
        checkpoint: u64,
    ) -> Result<(), String> {
        self.facts.lock().unwrap().push(format!(
            "marker:{}:{mid}:{ordinal}:{checkpoint}",
            view.version
        ));
        Ok(())
    }
}

#[tokio::test]
async fn compaction_v2_acceptance_is_not_application_and_failed_sync_does_not_accept() {
    let entries = project(
        &[text(Role::User, "old"), text(Role::User, "kept")],
        &facts(2),
        &Deliveries::new(),
        true,
    );
    let view = View::seal(
        1,
        vec![Edit::new(
            1,
            EditOp::Cut {
                through: entries[0].mid.clone(),
            },
        )],
    );
    let rules = rules::defaults("gpt-5");
    let journal = MemoryJournal::default();
    let mut state = state::State::default();
    journal
        .fail
        .store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(state
        .accept(&journal, &entries, view.clone(), &rules, true)
        .await
        .is_err());
    assert!(state.latest().is_none());
    journal
        .fail
        .store(false, std::sync::atomic::Ordering::SeqCst);
    state
        .accept(&journal, &entries, view.clone(), &rules, true)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.latest(), Some(&view));
    assert_eq!(state.applied(), None);
    state
        .apply(
            &journal,
            state::Boundary {
                step: 3,
                entries: &entries,
                rules: &rules,
                accepts_images: true,
                next_mid: "m12345678.2",
            },
        )
        .await
        .unwrap();
    assert_eq!(state.applied(), Some(&view));
    assert_eq!(
        *journal.facts.lock().unwrap(),
        ["accepted:1", "applied:1:3", "marker:1:m12345678.1:2:1"]
    );
}

#[tokio::test]
async fn compaction_v2_recovery_finishes_marker_after_applied_barrier() {
    let entries = project(
        &[text(Role::User, "old")],
        &facts(1),
        &Deliveries::new(),
        true,
    );
    let view = View::seal(
        1,
        vec![Edit::new(
            1,
            EditOp::Cut {
                through: entries[0].mid.clone(),
            },
        )],
    );
    let mut state = state::State::default();
    state.restore_accepted(view.clone()).unwrap();
    state.restore_applied(&view.applied()).unwrap();
    let journal = MemoryJournal::default();
    let rules = rules::defaults("gpt-5");
    let events = state
        .apply(
            &journal,
            state::Boundary {
                step: 1,
                entries: &entries,
                rules: &rules,
                accepts_images: true,
                next_mid: "m12345678.1",
            },
        )
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(*journal.facts.lock().unwrap(), ["marker:1:m12345678.1:2:1"]);
    state
        .apply(
            &journal,
            state::Boundary {
                step: 1,
                entries: &entries,
                rules: &rules,
                accepts_images: true,
                next_mid: "m12345678.1",
            },
        )
        .await
        .unwrap();
    assert_eq!(journal.facts.lock().unwrap().len(), 1);
}

#[test]
fn compaction_v2_restore_checks_checksum_and_applied_identity() {
    let mut state = state::State::default();
    let mut view = View::seal(1, vec![]);
    let identity = view.applied();
    view.checksum = "wrong".into();
    assert!(state
        .restore_accepted(view)
        .unwrap_err()
        .contains("checksum mismatch"));
    assert!(state
        .restore_applied(&identity)
        .unwrap_err()
        .contains("no matching stored view"));
}
