use broca_protocol::{ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult};
use cortexkit_compaction_view::{rules, tags};
use cortexkit_role_compaction::{
    part::{Delivered, PartBody, Seal, ToolStatus},
    read::{Detail, ReadRequest, Select},
    view::{Edit, EditOp, View, ViewRefusal},
};
use serde_json::json;

use super::{
    conditions as session_conditions,
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
            ordinal: position as u64 + 1,
            written_at: 100 + position as u64,
            completed_at: Some(200 + position as u64),
            ..EntryFacts::default()
        })
        .collect()
}

#[test]
fn compaction_v2_newest_imported_entry_keeps_its_reported_prompt() {
    let messages = vec![
        text(Role::Assistant, "fallback reply"),
        text(Role::Assistant, "imported reply"),
        text(Role::User, "next question"),
    ];
    let mut facts = facts(3);
    for (fact, prompt) in facts.iter_mut().zip([2_100, 3_000]) {
        let mut usage = cortexkit_role_compaction::read::StepUsage::new();
        usage.prompt_tokens = Some(prompt);
        fact.usage = Some(usage);
    }
    let entries = project(&messages, &facts, &Deliveries::new(), true);
    let fallback = Some((entries[0].mid.as_str(), 1_000));
    assert_eq!(
        super::host::last_prompt_tokens(&entries, fallback),
        Some(3_000)
    );
    assert_eq!(
        super::host::last_prompt_tokens(&entries[..1], fallback),
        Some(1_000)
    );
    assert_eq!(
        super::host::last_prompt_tokens(&entries[..1], Some(("another-entry", 900))),
        Some(2_100)
    );
}

#[test]
fn compaction_v2_fallback_fill_requires_distinct_serving_models() {
    let usage = broca_protocol::Usage {
        input_tokens: Some(1_300),
        cached_input_tokens: Some(500),
        cache_write_tokens: Some(300),
        ..Default::default()
    };
    assert_eq!(super::host::fallback_fill("model", usage, None), None);
    let mut iterations: Vec<broca_protocol::usage::UsageIteration> =
        serde_json::from_value(json!([
            {"model":"model", "input_tokens":600, "cache_read_input_tokens":300,
             "cache_creation_input_tokens":200},
            {"model":"model", "input_tokens":700, "cache_read_input_tokens":200,
             "cache_creation_input_tokens":100}
        ]))
        .unwrap();
    assert_eq!(
        super::host::fallback_fill("model", usage, Some(&iterations)),
        None
    );
    iterations[1].model = Some("fallback-model".into());
    assert_eq!(
        super::host::fallback_fill("fallback-model", usage, Some(&iterations)),
        Some(1_000)
    );
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

fn frozen_ride(id: &str) -> broca_wal::record::RideMetadata {
    broca_wal::record::RideMetadata {
        speaks_for: None,
        call_key: "frozen-carrier-key".into(),
        send_id: id.into(),
        sender: "ck-mc".into(),
        forms: vec!["tool_result".into()],
        form: broca_wal::record::RideForm::Append,
    }
}

#[test]
fn frozen_and_legacy_deliveries_keep_record_order_and_error_status() {
    let messages = vec![
        text(Role::User, "question"),
        call(),
        result(ToolOutput::error("failed")),
    ];
    let deliveries = Deliveries::from([(
        ("m12345678.1".into(), "1".into()),
        vec![
            Delivered::new("ck-mc", "first").with_send_id("first"),
            Delivered::new("ck-mc", "legacy"),
            Delivered::new("ck-mc", "last").with_send_id("last"),
        ],
    )]);
    let entries = project(&messages, &facts(3), &deliveries, true);
    let mut rides = super::entries::RideEnvelopes::default();
    rides.push(&entries[1].mid, "1", Some(frozen_ride("first")));
    rides.push(&entries[1].mid, "1", None);
    rides.push(&entries[1].mid, "1", Some(frozen_ride("last")));
    let rendered = render::deliveries_only_with_rides(&messages, &entries, &rides).unwrap();
    let ContentBlock::ToolResult(result) = &rendered[2].content[0] else {
        panic!("missing result")
    };
    assert!(result.is_error);
    assert_eq!(
        super::entries::tool_output(&result.output).1,
        "failed\n\nfirst\n\nlegacy\n\nlast"
    );
    let only_new = Deliveries::from([(
        (entries[1].mid.clone(), "1".into()),
        vec![Delivered::new("ck-mc", "first").with_send_id("first")],
    )]);
    let entries = project(&messages, &facts(3), &only_new, true);
    let rendered = render::deliveries_only_with_rides(&messages, &entries, &rides).unwrap();
    assert!(matches!(&rendered[2].content[0], ContentBlock::ToolResult(result) if result.is_error));
}

#[test]
fn keyed_legacy_delivery_is_not_a_frozen_ride_and_parts_honor_delivered_edits() {
    use broca_protocol::ToolOutputPart;
    let mut assistant = call();
    let ContentBlock::ToolCall(call) = &mut assistant.content[1] else {
        panic!("missing call")
    };
    // Signed calls may only be removed, not edited. Clear the signature before
    // replacing the delivered list.
    call.thought_signature = None;
    let messages = vec![
        text(Role::User, "question"),
        assistant,
        result(ToolOutput::Parts {
            parts: vec![
                ToolOutputPart::Text {
                    text: "value".into(),
                },
                ToolOutputPart::Text {
                    text: "caption".into(),
                },
            ],
        }),
    ];
    let deliveries = Deliveries::from([(
        ("m12345678.1".into(), "1".into()),
        vec![
            Delivered::new("ck-mc", "first").with_send_id("first"),
            Delivered::new("ck-mc", "second").with_send_id("second"),
        ],
    )]);
    let entries = project(&messages, &facts(3), &deliveries, true);
    let legacy = render::deliveries_only(&messages, &entries).unwrap();
    let ContentBlock::ToolResult(result) = &legacy[2].content[0] else {
        panic!("missing result")
    };
    assert_eq!(
        result.output,
        ToolOutput::Parts {
            parts: vec![ToolOutputPart::Text {
                text: "value\ncaption\n\nfirst\n\nsecond".into()
            }]
        }
    );
    let mut rides = super::entries::RideEnvelopes::default();
    rides.push(&entries[1].mid, "1", Some(frozen_ride("first")));
    rides.push(&entries[1].mid, "1", Some(frozen_ride("second")));
    let edits = [Edit::new(
        1,
        EditOp::Replace {
            mid: entries[1].mid.clone(),
            part: "1".into(),
            field: "delivered".into(),
            value: json!([1]),
            expect: entries[1].parts[1].digest.clone(),
        },
    )];
    let rendered =
        render::prompt_with_rides(&messages, &entries, &rides, &edits, |_| panic!("no image"))
            .unwrap();
    let ContentBlock::ToolResult(result) = &rendered[2].content[0] else {
        panic!("missing result")
    };
    assert_eq!(
        result.output,
        ToolOutput::Parts {
            parts: vec![
                ToolOutputPart::Text {
                    text: "\u{a7}2\u{a7} value".into()
                },
                ToolOutputPart::Text {
                    text: "caption\n\nsecond".into()
                }
            ]
        }
    );
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
    let deliveries =
        Deliveries::from([(("m12345678.1".into(), "1".into()), vec![delivered.clone()])]);
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
    let page = views::page(
        &entries,
        &request,
        cortexkit_role_compaction::read::Head::new().with_newest(views::newest(&entries).unwrap()),
    );
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
    let deliveries = Deliveries::from([(
        ("m12345678.1".into(), "1".into()),
        vec![Delivered::new("ck-mc", "remember")],
    )]);
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
        runtime::detects(true),
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
    ) -> Result<Box<serde_json::value::RawValue>, runtime::CallError> {
        self.calls.lock().unwrap().push((method.into(), params));
        serde_json::value::to_raw_value(
            &self
                .answers
                .lock()
                .unwrap()
                .pop_front()
                .expect("scripted answer"),
        )
        .map_err(|_| runtime::CallError::Unavailable)
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
    async fn accepted(
        &self,
        _: Option<&super::received::ReceivedView>,
        view: &super::received::ReceivedView,
    ) -> Result<(), String> {
        if self.fail.load(std::sync::atomic::Ordering::SeqCst) {
            return Err("sync failed".into());
        }
        self.facts
            .lock()
            .unwrap()
            .push(format!("accepted:{}", view.view().version));
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

#[test]
fn compaction_v2_rides_include_every_simultaneous_cause() {
    use cortexkit_role_compaction::{ride::RideCause, setup::ModelRef};
    let previous = session_conditions::SendContext {
        model: ModelRef::new("openai", "old"),
        account: Some("a".into()),
        surface: "old".into(),
    };
    let current = session_conditions::SendContext {
        model: ModelRef::new("anthropic", "new").with_service_tier("priority"),
        account: Some("b".into()),
        surface: "new".into(),
    };
    let conditions = session_conditions::conditions(current.model.clone(), 1000, 100, true);
    assert_eq!(
        session_conditions::ride_causes(Some(&previous), &current, true, true, 901, &conditions),
        [
            RideCause::CacheExpired,
            RideCause::ChainReset,
            RideCause::ModelChange,
            RideCause::TierChange,
            RideCause::AccountChange,
            RideCause::SurfaceChange,
            RideCause::OverLimit
        ]
    );
    assert!(session_conditions::ride_causes(
        Some(&current),
        &current,
        false,
        false,
        900,
        &conditions
    )
    .is_empty());
    let setup = session_conditions::setup_request("unchanged-preset", &conditions, 100, true);
    assert_eq!(setup.preset, "unchanged-preset");
    assert_eq!(setup.model, current.model);
    assert_eq!(setup.detects.len(), 7);
    assert_eq!(
        setup.rules.history_edit_cost,
        cortexkit_role_compaction::rules::EditCost::FullResend
    );
}

#[test]
fn compaction_v2_plan_version_mismatch_refuses_by_name() {
    assert!(
        session_conditions::check_plan_version(cortexkit_compaction_view::PLAN_VERSION).is_ok()
    );
    let error = session_conditions::check_plan_version(1).unwrap_err();
    assert!(error.contains("compaction_plan_version 1"));
    assert!(error.contains("version 2"));
}

#[derive(Default)]
struct HeldNoticePlane {
    calls: std::sync::Mutex<Vec<(String, serde_json::Value)>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    held: std::sync::atomic::AtomicBool,
}

#[async_trait::async_trait]
impl runtime::Plane for HeldNoticePlane {
    async fn call(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Box<serde_json::value::RawValue>, runtime::CallError> {
        self.calls.lock().unwrap().push((method.into(), params));
        if method == "compaction.notice"
            && !self.held.swap(true, std::sync::atomic::Ordering::SeqCst)
        {
            self.entered.notify_one();
            self.release.notified().await;
        }
        serde_json::value::to_raw_value(&match method {
            "compaction.setup" => json!({"answer":"ready"}),
            "compaction.notice" => json!({"answer":"received"}),
            "compaction.ride" => json!({"answer":"nothing"}),
            _ => panic!("unexpected method"),
        })
        .map_err(|_| runtime::CallError::Unavailable)
    }
}

#[tokio::test]
async fn compaction_v2_ride_waits_for_inflight_notice_and_coalesced_pending_changes() {
    use cortexkit_role_compaction::ride::{RideCause, RideRequest};
    use std::sync::Arc;
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        let runtime = Arc::new(runtime::Runtime::default());
        let plane = Arc::new(HeldNoticePlane::default());
        runtime.observe(&project(
            &[text(Role::User, "one")],
            &facts(1),
            &Deliveries::new(),
            true,
        ));
        let sending = {
            let runtime = runtime.clone();
            let plane = plane.clone();
            tokio::spawn(async move {
                runtime
                    .flush(plane.as_ref(), &setup(), &conditions(), None)
                    .await
            })
        };
        plane.entered.notified().await;
        runtime.observe(&project(
            &[text(Role::User, "one"), text(Role::User, "two")],
            &facts(2),
            &Deliveries::new(),
            true,
        ));
        runtime.observe(&project(
            &[
                text(Role::User, "one"),
                text(Role::User, "two"),
                text(Role::User, "three"),
            ],
            &facts(3),
            &Deliveries::new(),
            true,
        ));
        let riding = {
            let runtime = runtime.clone();
            let plane = plane.clone();
            tokio::spawn(async move {
                runtime
                    .ride(
                        plane.as_ref(),
                        &setup(),
                        &RideRequest::new(vec![RideCause::CacheExpired], 1000, conditions()),
                    )
                    .await
            })
        };
        assert_eq!(plane.calls.lock().unwrap().len(), 2);
        plane.release.notify_one();
        sending.await.unwrap();
        riding.await.unwrap();
        let calls = plane.calls.lock().unwrap();
        assert_eq!(
            calls
                .iter()
                .map(|(method, _)| method.as_str())
                .collect::<Vec<_>>(),
            [
                "compaction.setup",
                "compaction.notice",
                "compaction.notice",
                "compaction.ride"
            ]
        );
        assert_eq!(calls[2].1["seq"], 2);
        assert_eq!(calls[2].1["entries"].as_array().unwrap().len(), 2);
        assert_eq!(calls[2].1["entries"][0]["ordinal"], 2);
        assert_eq!(calls[2].1["entries"][1]["ordinal"], 3);
    })
    .await
    .expect("notice/ride lane must make progress");
}

#[test]
fn compaction_v2_received_edits_keep_unknown_members_and_exact_bytes() {
    use super::received::ReceivedView;
    let raw = serde_json::value::RawValue::from_string(
        r#"{ "op":"drop", "mid":"m12345678.0", "since":1, "future":{"n":1.00} }"#.into(),
    )
    .unwrap();
    let edit: Edit = serde_json::from_str(raw.get()).unwrap();
    let view = View::seal(1, vec![edit]);
    #[derive(serde::Serialize)]
    struct WireView<'a> {
        version: u64,
        checksum: &'a str,
        edits: &'a [Box<serde_json::value::RawValue>],
    }
    let edits = vec![raw.clone()];
    let wire = serde_json::value::to_raw_value(&WireView {
        version: view.version,
        checksum: &view.checksum,
        edits: &edits,
    })
    .unwrap();
    let received = ReceivedView::parse(&wire).unwrap();
    assert_eq!(received.view(), &view);
    assert_eq!(received.edits()[0].get(), raw.get());
    let rebuilt = ReceivedView::rebuilt(
        view.version,
        view.checksum.clone(),
        received.edits().to_vec(),
    )
    .unwrap();
    assert_eq!(rebuilt.edits()[0].get(), raw.get());
    assert!(
        ReceivedView::rebuilt(view.version, "wrong".into(), received.edits().to_vec())
            .unwrap_err()
            .contains("checksum mismatch")
    );
}

#[test]
fn compaction_v2_tool_identity_is_not_a_call_id_reused_in_a_later_turn() {
    let messages = vec![
        call(),
        result(ToolOutput::Text {
            text: "first".into(),
        }),
        call(),
        result(ToolOutput::Denied {
            reason: "second".into(),
        }),
    ];
    let deliveries = Deliveries::from([(
        ("m12345678.0".into(), "1".into()),
        vec![Delivered::new("ck-mc", "first reminder")],
    )]);
    let entries = project(&messages, &facts(4), &deliveries, true);
    let PartBody::Tool(first) = &entries[0].parts[1].body else {
        panic!("first tool missing")
    };
    let PartBody::Tool(second) = &entries[2].parts[1].body else {
        panic!("second tool missing")
    };
    assert_eq!(first.output.as_deref(), Some("first"));
    assert_eq!(first.delivered.len(), 1);
    assert_eq!(second.output.as_deref(), Some("second"));
    assert!(second.delivered.is_empty());
    let rendered = render::prompt(&messages, &entries, &[], |_| panic!("no image")).unwrap();
    let ContentBlock::ToolResult(first) = &rendered[1].content[0] else {
        panic!("first result missing")
    };
    let ContentBlock::ToolResult(second) = &rendered[3].content[0] else {
        panic!("second result missing")
    };
    assert_eq!(
        first.output,
        ToolOutput::Text {
            text: "§1§ first\n\nfirst reminder".into()
        }
    );
    assert_eq!(second.output, ToolOutput::error("§2§ second"));
}

#[test]
fn compaction_v2_duplicate_call_ids_pair_by_position_even_when_the_first_part_is_dropped() {
    let mut assistant = call();
    assistant.content.push(assistant.content[1].clone());
    let mut results = result(ToolOutput::Text {
        text: "first".into(),
    });
    results.content.extend(
        result(ToolOutput::Denied {
            reason: "second".into(),
        })
        .content,
    );
    let messages = vec![assistant, results];
    let entries = project(&messages, &facts(2), &Deliveries::new(), true);
    assert!(
        matches!(&entries[0].parts[1].body, PartBody::Tool(tool) if tool.output.as_deref() == Some("first"))
    );
    assert!(
        matches!(&entries[0].parts[2].body, PartBody::Tool(tool) if tool.output.as_deref() == Some("second"))
    );
    let edits = vec![Edit::new(
        1,
        EditOp::Drop {
            mid: entries[0].mid.clone(),
            part: Some("1".into()),
        },
    )];
    let rendered = render::prompt(&messages, &entries, &edits, |_| panic!("no image")).unwrap();
    assert_eq!(rendered[1].content.len(), 1);
    let ContentBlock::ToolResult(result) = &rendered[1].content[0] else {
        panic!("second result missing")
    };
    assert_eq!(result.output, ToolOutput::error("§2§ second"));
}

#[tokio::test]
async fn compaction_v2_midstep_acceptance_does_not_change_a_resumed_step_view() {
    let messages = vec![text(Role::User, "original")];
    let entries = project(&messages, &facts(1), &Deliveries::new(), true);
    let first = View::seal(1, vec![]);
    let next = View::seal(
        2,
        vec![Edit::new(
            2,
            EditOp::Replace {
                mid: entries[0].mid.clone(),
                part: "0".into(),
                field: "text".into(),
                value: json!("replacement"),
                expect: entries[0].parts[0].digest.clone(),
            },
        )],
    );
    let mut state = state::State::default();
    state.restore_accepted(first.clone()).unwrap();
    state.restore_applied(&first.applied()).unwrap();
    state.start_step(1);
    let before = render::prompt(
        &messages,
        &entries,
        &state.view_for_step(1).unwrap().edits,
        |_| panic!("no image"),
    )
    .unwrap();
    let journal = MemoryJournal::default();
    let rules = rules::defaults("gpt-5");
    state
        .accept(&journal, &entries, next.clone(), &rules, true)
        .await
        .unwrap()
        .unwrap();
    let checkpoint = serde_json::to_vec(&state).unwrap();
    let mut resumed: state::State = serde_json::from_slice(&checkpoint).unwrap();
    resumed.start_step(1);
    assert_eq!(resumed.view_for_step(1), Some(&first));
    let after = render::prompt(
        &messages,
        &entries,
        &resumed.view_for_step(1).unwrap().edits,
        |_| panic!("no image"),
    )
    .unwrap();
    assert_eq!(
        serde_json::to_vec(&before).unwrap(),
        serde_json::to_vec(&after).unwrap()
    );
    resumed.start_step(2);
    resumed
        .apply(
            &journal,
            state::Boundary {
                step: 2,
                entries: &entries,
                rules: &rules,
                accepts_images: true,
                next_mid: "m12345678.1",
            },
        )
        .await
        .unwrap();
    assert_eq!(resumed.view_for_step(2), Some(&next));
    let later = render::prompt(
        &messages,
        &entries,
        &resumed.view_for_step(2).unwrap().edits,
        |_| panic!("no image"),
    )
    .unwrap();
    assert_ne!(
        serde_json::to_vec(&before).unwrap(),
        serde_json::to_vec(&later).unwrap()
    );
}

#[test]
fn compaction_v2_delivery_origin_distinguishes_person_input_from_runner_delivery() {
    use super::entries::delivery_sender;
    use broca_protocol::{BindPrincipal, DeliveryMode};
    let module = BindPrincipal::Reserved {
        module_id: "ck-mc".into(),
    };
    assert_eq!(
        delivery_sender(Some(&module), DeliveryMode::ToolResult).as_deref(),
        Some("ck-mc")
    );
    assert_eq!(
        delivery_sender(Some(&module), DeliveryMode::Steer).as_deref(),
        Some("ck-mc")
    );
    for principal in [
        None,
        Some(BindPrincipal::Direct),
        Some(BindPrincipal::Unverified),
    ] {
        assert_eq!(
            delivery_sender(principal.as_ref(), DeliveryMode::ToolResult).as_deref(),
            Some("broca")
        );
        assert_eq!(
            delivery_sender(principal.as_ref(), DeliveryMode::Steer),
            None
        );
        assert_eq!(
            delivery_sender(principal.as_ref(), DeliveryMode::IdleStart),
            None
        );
    }
}

#[test]
fn compaction_v2_delivered_attribution_joins_submission_records_in_delivery_order() {
    use broca_protocol::{BindPrincipal, DeliveryMode, DeliveryOrder, RunId, SubmissionId};
    use broca_wal::WalRecord;
    let ordered = |submission: &str, sender: Option<BindPrincipal>, template: &str| {
        WalRecord::DeliveryOrdered {
            submission_id: SubmissionId(submission.into()),
            sent_by: sender,
            order: DeliveryOrder {
                modes: vec![DeliveryMode::ToolResult],
                dedupe_key: None,
                expires_at_ms: None,
                template: Some(
                    serde_json::from_value(json!({"id":template,"digest":"0".repeat(64)})).unwrap(),
                ),
            },
        }
    };
    let queued: WalRecord = serde_json::from_value(json!({"type":"queued","submission_id":"queued","principal":{"kind":"reserved","module_id":"queued-module"},"request":{"model":{"provider_module_id":"fake","model_id":"fake"},"input":[]}})).unwrap();
    let delivered = |submission: &str, text: &str| WalRecord::SendDelivered {
        ride: None,
        written_at_ms: None,
        submission_id: SubmissionId(submission.into()),
        run_id: RunId("run".into()),
        mode: DeliveryMode::ToolResult,
        input: None,
        tool_call_id: Some(ToolCallId("call".into())),
        appended_text: Some(format!("\n\n{text}")),
        tags: None,
    };
    let records = vec![
        ordered("person", Some(BindPrincipal::Direct), "person-template"),
        ordered(
            "module",
            Some(BindPrincipal::Reserved {
                module_id: "named-module".into(),
            }),
            "module-template",
        ),
        ordered("queued", None, "queued-template"),
        queued,
        delivered("module", "module first"),
        delivered("person", "person second"),
        delivered("queued", "queued third"),
    ];
    let deliveries = super::entries::delivered_from_records(&records, |run, _, call| {
        assert_eq!(run.0, "run");
        assert_eq!(call.0, "call");
        Some(("mid".into(), "0".into()))
    });
    let texts = &deliveries[&("mid".into(), "0".into())];
    assert_eq!(
        texts
            .iter()
            .map(|text| text.sender.as_str())
            .collect::<Vec<_>>(),
        ["named-module", "broca", "queued-module"]
    );
    assert_eq!(
        texts
            .iter()
            .map(|text| text.template.as_deref())
            .collect::<Vec<_>>(),
        [
            Some("module-template"),
            Some("person-template"),
            Some("queued-template")
        ]
    );
    assert_eq!(
        texts
            .iter()
            .map(|text| text.text.as_deref())
            .collect::<Vec<_>>(),
        [
            Some("module first"),
            Some("person second"),
            Some("queued third")
        ]
    );
}

#[test]
fn compaction_v2_bind_requires_mode3_selected_provider_authority() {
    use super::{authority::check_bind, protocol::wire::not_permitted_reasons as reasons};
    for (mode3, provider, caller, reason) in [
        (
            false,
            Some("ck-mc"),
            Some("ck-mc"),
            reasons::SESSION_NOT_HELD,
        ),
        (true, None, Some("ck-mc"), reasons::NO_COMPACTION_ITEM),
        (
            true,
            Some("ck-mc"),
            Some("other"),
            reasons::PROVIDER_MISMATCH,
        ),
    ] {
        let refusal = check_bind(mode3, provider, caller).unwrap_err();
        assert_eq!(refusal.code, "compaction_not_permitted");
        assert_eq!(refusal.detail, Some(json!({"reason":reason})));
    }
    assert!(check_bind(true, Some("ck-mc"), Some("ck-mc")).is_ok());
}

#[test]
fn compaction_v2_bounded_projection_keeps_native_ordinals_and_marker_number_base() {
    let mut facts = facts(1);
    facts[0].ordinal = 41;
    facts[0].mid = "m12345678.40".into();
    facts[0].tag_base = Some(70);
    let entries = project(
        &[text(Role::User, "kept")],
        &facts,
        &Deliveries::new(),
        true,
    );
    assert_eq!(entries[0].ordinal, 41);
    assert_eq!(entries[0].mid, "m12345678.40");
    assert_eq!(entries[0].parts[0].tag, Some(71));
}

#[tokio::test]
async fn compaction_v2_failed_setup_retries_on_next_send_without_an_applied_view() {
    let plane = RecordingPlane::default();
    plane
        .answers
        .lock()
        .unwrap()
        .extend([json!({"answer":"unknown"}), json!({"answer":"ready"})]);
    let runtime = runtime::Runtime::default();
    runtime.begin_send(false).await;
    assert!(!runtime.ensure_setup(&plane, &setup()).await);
    assert!(!runtime.ensure_setup(&plane, &setup()).await);
    runtime.begin_send(true).await;
    assert!(!runtime.ensure_setup(&plane, &setup()).await);
    assert_eq!(plane.calls.lock().unwrap().len(), 1);
    runtime.begin_send(false).await;
    assert!(runtime.ensure_setup(&plane, &setup()).await);
    assert_eq!(plane.calls.lock().unwrap().len(), 2);
}

#[test]
fn compaction_v2_storage_references_only_byte_identical_previous_edits() {
    use super::{received::ReceivedView, storage};
    use broca_wal::StoredViewEdit;
    let raw = serde_json::value::RawValue::from_string(
        r#"{ "since":1, "op":"drop", "mid":"m12345678.0" }"#.into(),
    )
    .unwrap();
    let edit: Edit = serde_json::from_str(raw.get()).unwrap();
    let first = View::seal(1, vec![edit.clone()]);
    let previous = ReceivedView::rebuilt(1, first.checksum, vec![raw.clone()]).unwrap();
    let next = View::seal(2, vec![edit.clone()]);
    let identical = ReceivedView::rebuilt(2, next.checksum.clone(), vec![raw]).unwrap();
    let encoded = storage::encode(Some(&previous), &identical);
    assert!(matches!(
        encoded.as_slice(),
        [StoredViewEdit::Previous { index: 0 }]
    ));
    assert_eq!(
        storage::rebuild(Some(&previous), 2, next.checksum.clone(), &encoded).unwrap(),
        identical
    );
    let normalized = serde_json::value::to_raw_value(&edit).unwrap();
    let different_bytes = ReceivedView::rebuilt(2, next.checksum, vec![normalized]).unwrap();
    assert!(matches!(
        storage::encode(Some(&previous), &different_bytes).as_slice(),
        [StoredViewEdit::Full { .. }]
    ));
    assert!(matches!(
        storage::encode(None, &previous).as_slice(),
        [StoredViewEdit::Full { .. }]
    ));
}

#[test]
fn compaction_v2_storage_bad_reference_and_checksum_refuse_by_name() {
    use super::{received::ReceivedView, storage};
    use broca_wal::StoredViewEdit;
    let previous = ReceivedView::from(View::seal(1, vec![]));
    let error = storage::rebuild(
        Some(&previous),
        2,
        "unused".into(),
        &[StoredViewEdit::Previous { index: 8 }],
    )
    .unwrap_err();
    assert!(error.contains("compaction view version 2"));
    assert!(error.contains("previous edit index 8"));
    assert!(storage::rebuild(None, 2, "wrong".into(), &[])
        .unwrap_err()
        .contains("checksum mismatch"));
}

#[test]
fn compaction_v2_every_final_result_kind_is_a_delivery_carrier() {
    for output in [
        ToolOutput::Json {
            value: json!({"a":1}),
        },
        ToolOutput::Denied {
            reason: "no".into(),
        },
        ToolOutput::Parts { parts: Vec::new() },
        ToolOutput::error("failed"),
    ] {
        let first = ToolResult {
            tool_call_id: ToolCallId("first".into()),
            output: ToolOutput::Text {
                text: "first".into(),
            },
            is_error: false,
        };
        let last = ToolResult {
            tool_call_id: ToolCallId("last".into()),
            output,
            is_error: false,
        };
        assert_eq!(
            crate::delivery_order::carrier_mode3([&first, &last]),
            Some(&last.tool_call_id)
        );
    }
    let json = ToolResult {
        tool_call_id: ToolCallId("json".into()),
        output: ToolOutput::Json { value: json!(1) },
        is_error: false,
    };
    assert_eq!(
        crate::delivery_order::carrier([&json]),
        None,
        "legacy carrier policy is unchanged"
    );
    assert_eq!(
        crate::delivery_order::carrier_mode3([&json]),
        Some(&json.tool_call_id)
    );
}

#[test]
fn compaction_v2_kept_cache_marker_stays_on_its_native_block() {
    let mut message = Message::new(
        Role::User,
        vec![
            ContentBlock::Text { text: "one".into() },
            ContentBlock::Text { text: "two".into() },
            ContentBlock::Text {
                text: "three".into(),
            },
        ],
    );
    message.cache_prefix_blocks = Some(3);
    let entries = project(&[message.clone()], &facts(1), &Deliveries::new(), true);
    let drop_first = vec![Edit::new(
        1,
        EditOp::Drop {
            mid: entries[0].mid.clone(),
            part: Some("0".into()),
        },
    )];
    let kept = render::prompt(&[message.clone()], &entries, &drop_first, |_| {
        panic!("no image")
    })
    .unwrap();
    assert_eq!(kept[0].cache_prefix_blocks, Some(2));
    let drop_marker = vec![Edit::new(
        1,
        EditOp::Drop {
            mid: entries[0].mid.clone(),
            part: Some("2".into()),
        },
    )];
    let kept = render::prompt(&[message], &entries, &drop_marker, |_| panic!("no image")).unwrap();
    assert_eq!(kept[0].cache_prefix_blocks, Some(0));
}

#[test]
fn compaction_v2_error_output_is_rendered_once_and_delivered_after_its_attribution() {
    let mut assistant = call();
    if let ContentBlock::ToolCall(call) = &mut assistant.content[1] {
        call.thought_signature = None;
    }
    let output = ToolOutput::Error {
        message: "offline".into(),
        module: Some("aft".into()),
        code: Some("module_warming".into()),
        detail: Some("restarting".into()),
        nothing_sent: Some(true),
        retry: None,
    };
    let expected = ToolOutput::error_model_text(
        "offline",
        Some("aft"),
        Some("module_warming"),
        Some("restarting"),
        Some(true),
        None,
    );
    let messages = vec![assistant, result(output)];
    let deliveries = Deliveries::from([(
        ("m12345678.0".into(), "1".into()),
        vec![Delivered::new("ck-mc", "remember")],
    )]);
    let entries = project(&messages, &facts(2), &deliveries, true);
    assert!(
        matches!(&entries[0].parts[1].body, PartBody::Tool(tool) if tool.output.as_deref() == Some(expected.as_str()))
    );
    let kept = render::prompt(&messages, &entries, &[], |_| panic!("no image")).unwrap();
    let ContentBlock::ToolResult(result) = &kept[1].content[0] else {
        panic!("result missing")
    };
    assert!(result.is_error);
    assert_eq!(
        result.output,
        ToolOutput::error(format!("§1§ {expected}\n\nremember"))
    );
    let edits = vec![Edit::new(
        1,
        EditOp::Replace {
            mid: entries[0].mid.clone(),
            part: "1".into(),
            field: "output".into(),
            value: json!("short"),
            expect: entries[0].parts[1].digest.clone(),
        },
    )];
    let replaced = render::prompt(&messages, &entries, &edits, |_| panic!("no image")).unwrap();
    let ContentBlock::ToolResult(result) = &replaced[1].content[0] else {
        panic!("result missing")
    };
    assert_eq!(result.output, ToolOutput::error("short\n\nremember"));
}

#[test]
fn compaction_v2_marker_identity_is_checked_in_log_and_checkpoint() {
    use super::fold;
    use broca_protocol::BindIdentity;
    use broca_wal::WalRecord;
    let original = BindIdentity::new("/synthetic/marker", "runner", "original");
    let fork = BindIdentity::new("/synthetic/marker", "runner", "fork");
    let marker = WalRecord::CompactionMarker {
        harness: original.harness.clone(),
        session: original.session.clone(),
        view_version: 1,
        first_kept_mid: "m12345678.1".into(),
        first_kept_ordinal: 2,
        checkpoint: 1,
    };
    let mut state = state::State::default();
    let error = fold::fold(&mut state, &marker, &fork).unwrap_err();
    assert!(error.contains("compaction marker session mismatch"));
    assert!(error.contains("runner/original"));
    assert!(error.contains("runner/fork"));
    fold::fold(&mut state, &marker, &original).unwrap();
    let decoded: state::State =
        serde_json::from_slice(&serde_json::to_vec(&state).unwrap()).unwrap();
    fold::validate_identity(&decoded, &original).unwrap();
    assert!(fold::validate_identity(&decoded, &fork).is_err());
}

fn request_at<'a>(
    messages: &'a [Message],
    entries: &'a [cortexkit_role_compaction::read::Entry],
    conditions: &'a cortexkit_role_compaction::notice::Conditions,
    current: &'a session_conditions::SendContext,
) -> super::request::At<'a> {
    static RIDES: std::sync::OnceLock<super::entries::RideEnvelopes> = std::sync::OnceLock::new();
    super::request::At {
        step: 1,
        preset: "worker",
        entries,
        rides: RIDES.get_or_init(Default::default),
        stored: messages,
        conditions,
        previous: Some(current),
        current,
        cache_expired: false,
        detects_cache_expired: false,
        chain_reset: false,
        now: 1,
        next_mid: "m12345678.1",
    }
}

/// A proven expiry rides as `cache_expired` only when Setup told the provider broca detects it.
/// Where the session's requests ask for no cache lifetime (ChatGPT's backend), Setup leaves the
/// cause out, and the ride never claims it, even when broca's own estimate says the cache aged
/// out.
#[tokio::test]
async fn compaction_v2_cache_expired_rides_only_where_setup_listed_it() {
    for detects in [false, true] {
        let messages = vec![text(Role::User, "question")];
        let entries = project(&messages, &facts(1), &Deliveries::new(), true);
        let plane = RecordingPlane::default();
        plane.answers.lock().unwrap().extend([
            json!({"answer":"ready"}),
            json!({"answer":"received"}),
            json!({"answer":"nothing"}),
            json!({"answer":"received"}),
        ]);
        let state = tokio::sync::Mutex::new(state::State::default());
        let conditions = conditions();
        let current = send_context();
        let mut at = request_at(&messages, &entries, &conditions, &current);
        at.cache_expired = true;
        at.detects_cache_expired = detects;
        super::request::prepare(
            &state,
            &runtime::Runtime::default(),
            &MemoryJournal::default(),
            &plane,
            at,
            |_| panic!("no image"),
            text_estimate,
        )
        .await
        .unwrap();
        let calls = plane.calls.lock().unwrap();
        let (_, setup) = calls
            .iter()
            .find(|(method, _)| method == "compaction.setup")
            .expect("Setup was sent");
        let listed = setup["detects"]
            .as_array()
            .unwrap()
            .iter()
            .any(|cause| cause == "cache_expired");
        assert_eq!(listed, detects);
        let ride = calls.iter().find(|(method, _)| method == "compaction.ride");
        if detects {
            let (_, ride) = ride.expect("a proven expiry rides");
            assert_eq!(ride["causes"], json!(["cache_expired"]), "{ride}");
        } else {
            assert!(ride.is_none(), "rode with {:?}", ride);
        }
    }
}

#[test]
fn compaction_v2_detects_lists_cache_expired_only_when_asked() {
    use cortexkit_role_compaction::ride::RideCause;
    assert!(runtime::detects(true).contains(&RideCause::CacheExpired));
    let without = runtime::detects(false);
    assert!(!without.contains(&RideCause::CacheExpired));
    assert_eq!(without.len() + 1, runtime::detects(true).len());
}

fn send_context() -> session_conditions::SendContext {
    session_conditions::SendContext {
        model: setup().model,
        account: None,
        surface: "same".into(),
    }
}

fn text_estimate(messages: &[Message]) -> Result<u64, String> {
    Ok(messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|part| match part {
            ContentBlock::Text { text } => Some(text.len() as u64),
            _ => None,
        })
        .sum())
}

#[tokio::test]
async fn compaction_v2_request_setup_failure_stops_before_even_rendering_model_input() {
    let messages = vec![text(Role::User, "question")];
    let entries = project(&messages, &facts(1), &Deliveries::new(), true);
    let plane = RecordingPlane::default();
    plane
        .answers
        .lock()
        .unwrap()
        .push_back(json!({"answer":"unknown"}));
    let state = tokio::sync::Mutex::new(state::State::default());
    let conditions = conditions();
    let current = send_context();
    let outcome = super::request::prepare(
        &state,
        &runtime::Runtime::default(),
        &MemoryJournal::default(),
        &plane,
        request_at(&messages, &entries, &conditions, &current),
        |_| panic!("no image rendering after failed Setup"),
        |_| panic!("no model request rendering after failed Setup"),
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        super::request::Prepared::CompactionUnavailable
    ));
    assert_eq!(
        plane
            .calls
            .lock()
            .unwrap()
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        ["compaction.setup"]
    );
}

#[tokio::test]
async fn compaction_v2_request_over_limit_rides_reestimates_then_refuses() {
    let messages = vec![text(Role::User, &"x".repeat(1000))];
    let entries = project(&messages, &facts(1), &Deliveries::new(), true);
    let plane = RecordingPlane::default();
    plane.answers.lock().unwrap().extend([
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
        json!({"answer":"nothing"}),
    ]);
    let state = tokio::sync::Mutex::new(state::State::default());
    let conditions = conditions();
    let current = send_context();
    let mut estimates = 0;
    let outcome = super::request::prepare(
        &state,
        &runtime::Runtime::default(),
        &MemoryJournal::default(),
        &plane,
        request_at(&messages, &entries, &conditions, &current),
        |_| panic!("no image"),
        |messages| {
            estimates += 1;
            text_estimate(messages)
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        outcome,
        super::request::Prepared::ContextCeiling {
            usable_window: 900,
            ..
        }
    ));
    assert_eq!(estimates, 2);
    let calls = plane.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        ["compaction.setup", "compaction.notice", "compaction.ride"]
    );
    assert_eq!(calls[2].1["causes"], json!(["over_limit"]));
    assert!(calls[2].1["request_tokens"].as_u64().unwrap() > 900);
}

#[tokio::test]
async fn compaction_v2_request_ride_view_applies_to_that_request_before_sending() {
    let messages = vec![text(Role::User, &"x".repeat(1000))];
    let entries = project(&messages, &facts(1), &Deliveries::new(), true);
    let view = View::seal(
        1,
        vec![Edit::new(
            1,
            EditOp::Replace {
                mid: entries[0].mid.clone(),
                part: "0".into(),
                field: "text".into(),
                value: json!("small"),
                expect: entries[0].parts[0].digest.clone(),
            },
        )],
    );
    let plane = RecordingPlane::default();
    plane.answers.lock().unwrap().extend([
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
        json!({"answer":"view", "view":view}),
    ]);
    let state = tokio::sync::Mutex::new(state::State::default());
    let journal = MemoryJournal::default();
    let conditions = conditions();
    let current = send_context();
    let outcome = super::request::prepare(
        &state,
        &runtime::Runtime::default(),
        &journal,
        &plane,
        request_at(&messages, &entries, &conditions, &current),
        |_| panic!("no image"),
        text_estimate,
    )
    .await
    .unwrap();
    let super::request::Prepared::Ready {
        prompt,
        applied,
        request_tokens,
    } = outcome
    else {
        panic!("ride did not free the request")
    };
    assert_eq!(
        prompt[0].content,
        vec![ContentBlock::Text {
            text: "small".into()
        }]
    );
    assert_eq!(applied, Some(view.applied()));
    assert_eq!(request_tokens, 5);
    assert_eq!(
        *journal.facts.lock().unwrap(),
        ["accepted:1", "applied:1:1"]
    );
    assert!(matches!(&messages[0].content[0], ContentBlock::Text { text } if text.len() == 1000));
}

/// The ride (broca's call to the compaction provider before a step, naming why the request is
/// about to change) claims `cache_expired` only when the cache clock proves it. A route with no cache clock,
/// or a session with no send yet, claims nothing: the provider treats `cache_expired` as a free
/// rebuild, so a guess there turns every step into a full resend.
#[test]
fn cache_expired_is_claimed_only_when_proven() {
    use session_conditions::cache_proven_expired;
    assert!(!cache_proven_expired(None, Some(1_000), 10_000_000));
    assert!(!cache_proven_expired(Some(300_000), None, 10_000_000));
    assert!(!cache_proven_expired(Some(300_000), Some(1_000), 300_999));
    assert!(cache_proven_expired(Some(300_000), Some(1_000), 301_000));
    let causes = session_conditions::ride_causes(
        Some(&send_context()),
        &send_context(),
        false,
        false,
        10,
        &conditions(),
    );
    assert!(causes.is_empty(), "{causes:?}");
}

/// The output reserve is the run's own `max_output_tokens` when it froze one, else the model's
/// output cap from the frozen render config. The ChatGPT route never sends the field, and its
/// sessions told the provider a reserve of 0.
#[test]
fn the_output_reserve_falls_back_to_the_models_output_cap() {
    use broca_protocol::request::ModelHandle;
    use broca_wal::RunConfig;
    let config = |max_output_tokens: Option<u64>| RunConfig {
        global_system: None,
        account_route: None,
        route_decision_id: None,
        account_limit_pause: false,
        turn_directives_v1: None,
        render_tags: false,
        computed_tags: None,
        model: ModelHandle::new("openai", "gpt-6.1-sol"),
        tools: Vec::new().into(),
        tool_choice: Default::default(),
        generation: broca_protocol::request::GenerationConfig {
            max_output_tokens,
            ..Default::default()
        },
        response_format: Default::default(),
        provider_options: serde_json::Value::Null,
        cache_tiers: serde_json::Value::Null,
        context_epoch: 0,
        renderer_version: 1,
        frozen_render_config: serde_json::json!({"max_output_tokens": 128000}),
        auth_selection: None,
        tools_were_omitted: false,
        budget: None,
        context_limit: Some(1_050_000),
        work_class: None,
        variant: None,
        service_tier: None,
        stop_when: Vec::new(),
        context_management: None,
    };
    let reserve = |config: RunConfig| {
        super::host::conditions(&config, &[], false, None)
            .unwrap()
            .output_reserve
    };
    assert_eq!(reserve(config(None)), 128_000);
    assert_eq!(reserve(config(Some(4_096))), 4_096);
}
