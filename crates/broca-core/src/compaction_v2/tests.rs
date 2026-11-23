use broca_protocol::{ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult};
use cortexkit_compaction_view::{rules, rules::Floor, tags};
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
                input: json!({"path":"a"}).into(),
                thought_signature: Some("thought".into()),
                provider_state: None,
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

fn frozen_ride(id: &str) -> broca_records::record::RideMetadata {
    broca_records::record::RideMetadata {
        speaks_for: None,
        call_key: "frozen-carrier-key".into(),
        send_id: id.into(),
        sender: "ck-mc".into(),
        forms: vec!["tool_result".into()],
        form: broca_records::record::RideForm::Append,
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

/// A probe placement: with rides rendered after the results, each result keeps only the tool's
/// own output (its part number included), and the tool message holds the ride texts beside it,
/// by the carrying result's place and then delivery order, on both the deliveries-only and the
/// planned render. Without it, each ride is appended to its result as before.
#[test]
fn rides_after_results_are_held_beside_the_tool_message() {
    let call = |id: &str, path: &str| {
        ContentBlock::ToolCall(ToolCall {
            tool_call_id: ToolCallId(id.into()),
            tool_name: "read".into(),
            input: json!({ "path": path }).into(),
            thought_signature: None,
            provider_state: None,
        })
    };
    let answer = |id: &str, text: &str| {
        ContentBlock::ToolResult(ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        })
    };
    let messages = vec![
        text(Role::User, "question"),
        Message::new(Role::Assistant, vec![call("c1", "a"), call("c2", "b")]),
        Message::new(Role::Tool, vec![answer("c1", "one"), answer("c2", "two")]),
    ];
    let deliveries = Deliveries::from([
        (
            ("m12345678.1".into(), "1".into()),
            vec![
                Delivered::new("ck-mc", "third").with_send_id("third"),
                Delivered::new("ck-mc", "fourth").with_send_id("fourth"),
            ],
        ),
        (
            ("m12345678.1".into(), "0".into()),
            vec![Delivered::new("ck-mc", "first").with_send_id("first")],
        ),
    ]);
    let entries = project(&messages, &facts(3), &deliveries, true);
    let mut rides = super::entries::RideEnvelopes::default();
    rides.push(&entries[1].mid, "0", Some(frozen_ride("first")));
    rides.push(&entries[1].mid, "1", Some(frozen_ride("third")));
    rides.push(&entries[1].mid, "1", Some(frozen_ride("fourth")));
    let outputs = |message: &Message| -> Vec<String> {
        message
            .content
            .iter()
            .map(|block| match block {
                ContentBlock::ToolResult(result) => super::entries::tool_output(&result.output).1,
                _ => panic!("a tool message holds results"),
            })
            .collect()
    };
    let renders = |rides: &super::entries::RideEnvelopes| {
        [
            render::deliveries_only_with_rides(&messages, &entries, rides).unwrap(),
            render::prompt_with_rides(&messages, &entries, rides, &[], |_| panic!("no image"))
                .unwrap(),
        ]
    };
    let today = renders(&rides);
    rides.set_after_results(true);
    let after = renders(&rides);
    for (today, after) in today.iter().zip(&after) {
        let (today, after) = (&today[2], &after[2]);
        assert!(today.tags.rides_after_results().is_empty());
        let own: Vec<String> = outputs(today)
            .into_iter()
            .zip(["\n\nfirst", "\n\nthird\n\nfourth"])
            .map(|(text, rides)| text.strip_suffix(rides).unwrap().to_owned())
            .collect();
        assert_eq!(outputs(after), own);
        assert_eq!(
            after.tags.rides_after_results(),
            ["first", "third", "fourth"]
        );
        assert_eq!(after.content.len(), today.content.len());
    }
    // The planned render shows each result's part number in both placements.
    assert!(outputs(&after[1][2])[0].starts_with('\u{a7}'));
}

#[test]
fn compaction_v2_entries_merge_results_without_merging_delivered_text() {
    let messages = vec![
        text(Role::User, "question"),
        call(),
        result(ToolOutput::Json {
            value: json!({"z":2,"a":1}).into(),
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
    assert_eq!(tool.delivered().unwrap(), [delivered]);
    assert_eq!(tool.status, ToolStatus::Completed);
    let PartBody::Tool(compact) = part.compact().body else {
        panic!("tool part missing")
    };
    assert_eq!(compact.output, None);
    let delivered = compact.delivered().unwrap();
    assert_eq!(delivered[0].text, None);
    assert_eq!(delivered[0].text_bytes, 8);
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
    assert!(views::check(&entries, None, &replace("kept §99§"), &rules, Floor::none()).is_ok());
    assert!(matches!(
        views::check(
            &entries,
            None,
            &replace("foreign §98§"),
            &rules,
            Floor::none()
        ),
        Err(ViewRefusal::ForeignTag { tag: 98, .. })
    ));
    let view = replace("replacement");
    assert!(matches!(
        views::check(&entries, Some(&view), &view, &rules, Floor::none()),
        Err(ViewRefusal::StaleVersion { .. })
    ));
    let mut bad = view;
    bad.checksum = "invalid".into();
    assert!(matches!(
        views::check(&entries, None, &bad, &rules, Floor::none()),
        Err(ViewRefusal::BadChecksum)
    ));
}

/// On a moved-in session, a view must cut through the move's entry or a later one; any view is
/// fine on a session nothing moved in, and a cut through an entry the session doesn't hold is
/// left to the view's own checks.
#[test]
fn compaction_v2_a_moved_session_refuses_views_cutting_before_the_move() {
    let moved = views::MovedThrough {
        mid: "m-moved".into(),
        ordinal: 4,
    };
    let cut = |through: &str| {
        View::seal(
            2,
            vec![Edit::new(
                2,
                EditOp::Cut {
                    through: through.into(),
                },
            )],
        )
    };
    let refused = Err(ViewRefusal::CutBeforeMove {
        moved_through: moved.mid.clone(),
    });
    let earlier = cut("m-earlier");
    assert_eq!(views::check_moved(&earlier, Some(&moved), Some(3)), refused);
    let no_cut = View::seal(2, Vec::new());
    assert_eq!(views::check_moved(&no_cut, Some(&moved), None), refused);
    assert_eq!(
        views::check_moved(&cut("m-moved"), Some(&moved), Some(4)),
        Ok(())
    );
    assert_eq!(
        views::check_moved(&cut("m-later"), Some(&moved), Some(9)),
        Ok(())
    );
    assert_eq!(
        views::check_moved(&cut("unknown"), Some(&moved), None),
        Ok(())
    );
    assert_eq!(views::check_moved(&earlier, None, Some(3)), Ok(()));
}

/// A view's inserted image is accepted when its bytes are the PNG its media type names, however
/// the session stated it takes images (a provider repeats an image it inserted before), and
/// refused `unsupported_edit` when they aren't: no request could render it.
#[test]
fn compaction_v2_view_inserted_images_are_checked_as_png() {
    use cortexkit_role_compaction::view::{InsertAt, InsertedImage, InsertedMessage};
    let messages = vec![text(Role::User, "original")];
    let entries = project(&messages, &facts(1), &Deliveries::new(), true);
    let insert = |data: &str| {
        View::seal(
            1,
            vec![Edit::new(
                1,
                EditOp::Insert {
                    at: InsertAt::Start,
                    message: InsertedMessage::new(
                        "memories",
                        cortexkit_role_compaction::read::Role::User,
                        vec!["Memories.".into()],
                    )
                    .with_images(vec![InsertedImage::new("image/png", data)]),
                },
            )],
        )
    };
    let rules = rules::defaults("gpt-5");
    // A 1x1 PNG whose chunk checksums hold, as the media check requires (the media
    // validation tests' fixture).
    let png = {
        use base64::Engine;
        const PNG: &str = "89504e470d0a1a0a0000000d4948445200000001000000010802000000907753de0000000c49444154789c63f8cfc0000003010100c9fe92ef0000000049454e44ae426082";
        let bytes: Vec<u8> = (0..PNG.len() / 2)
            .map(|i| u8::from_str_radix(&PNG[2 * i..2 * i + 2], 16).unwrap())
            .collect();
        base64::engine::general_purpose::STANDARD.encode(bytes)
    };
    let png = png.as_str();
    assert_eq!(
        views::check(&entries, None, &insert(png), &rules, Floor::none()),
        Ok(())
    );
    // Standard padded base64 whose bytes are text, not a PNG.
    assert_eq!(
        views::check(
            &entries,
            None,
            &insert("bm90IGEgcG5n"),
            &rules,
            Floor::none()
        ),
        Err(ViewRefusal::UnsupportedEdit { index: 0 })
    );
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
        let answer = self
            .answers
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted answer");
        // Scripted failures: `{"unanswered": true}` is a call that got no answer (the provider
        // unreachable or too slow), `{"refused": code}` the provider's refusal.
        if answer["unanswered"] == true {
            return Err(runtime::CallError::Unavailable);
        }
        if let Some(code) = answer["refused"].as_str() {
            return Err(runtime::CallError::Refused(
                super::protocol::wire::Refusal::new(code, "refused", None),
            ));
        }
        serde_json::value::to_raw_value(&answer).map_err(|_| runtime::CallError::Unavailable)
    }
}

/// Compaction contract N6: the first notice after Setup carries only what changed since the
/// Setup, here on a fresh runtime, as an actor reopened or a restarted process has.
#[tokio::test]
async fn compaction_v2_first_notice_after_setup_sends_only_what_changed_since() {
    let plane = RecordingPlane::default();
    plane
        .answers
        .lock()
        .unwrap()
        .extend([json!({"answer":"ready"}), json!({"answer":"received"})]);
    let runtime = runtime::Runtime::default();
    // The window at Setup: a question and an assistant entry whose tool call is still running.
    let at_setup = project(
        &[text(Role::User, "question"), call()],
        &facts(2),
        &Deliveries::new(),
        true,
    );
    let mut setup = setup();
    setup.newest = views::newest(&at_setup);
    runtime.observe(&at_setup);
    runtime.flush(&plane, &setup, &conditions(), None).await;
    assert_eq!(
        plane.calls.lock().unwrap().len(),
        1,
        "Setup covers the window, so nothing is left to notice"
    );

    // The call's result arrives after Setup: the assistant entry's digest changes and a new
    // entry follows it. Both go in the next notice; the question does not.
    let after = project(
        &[
            text(Role::User, "question"),
            call(),
            result(ToolOutput::Text {
                text: "contents".into(),
            }),
        ],
        &facts(3),
        &Deliveries::new(),
        true,
    );
    assert_ne!(after[1].digest, at_setup[1].digest);
    runtime.observe(&after);
    runtime.flush(&plane, &setup, &conditions(), None).await;
    let calls = plane.calls.lock().unwrap();
    assert_eq!(
        calls
            .iter()
            .map(|(method, _)| method.as_str())
            .collect::<Vec<_>>(),
        ["compaction.setup", "compaction.notice"]
    );
    let entries = calls[1].1["entries"].as_array().unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry["ordinal"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [2, 3]
    );
    assert_eq!(entries[0]["digest"], json!(after[1].digest));
}

/// Scripted answers at the catalog boundary, including a list kept by the transport after a
/// failed read. Calls still use the ordinary recording rig.
struct CatalogPlane {
    inner: RecordingPlane,
    capabilities: std::sync::Mutex<std::collections::VecDeque<crate::ProviderCapabilities>>,
}

#[async_trait::async_trait]
impl crate::CompactionPlane for CatalogPlane {
    async fn provider_capabilities(&self, _: &str) -> crate::ProviderCapabilities {
        self.capabilities
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted catalog answer")
    }

    async fn setup(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        _: crate::compaction_provider::setup::SetupRequest,
    ) -> Result<crate::compaction_provider::setup::SetupAnswer, crate::CompactionUnavailable> {
        panic!("v2 uses call_v2")
    }

    async fn call_v2(
        &self,
        _: &str,
        _: &broca_protocol::BindIdentity,
        _: Option<&broca_protocol::SessionScope>,
        method: &str,
        params: serde_json::Value,
        _: u64,
    ) -> Result<Box<serde_json::value::RawValue>, runtime::CallError> {
        runtime::Plane::call(&self.inner, method, params).await
    }
}

#[tokio::test]
async fn compaction_v2_kept_capabilities_preserve_setup_notice_and_ride_fields() {
    use cortexkit_role_compaction::{
        ride::{RideCause, RideRequest},
        SignedFloor,
    };
    let capabilities = crate::ProviderCapabilities::Declared(vec![
        cortexkit_role_compaction::COMPACTION_ENTRY_SPEAKS_FOR_PROVIDES.into(),
        cortexkit_role_compaction::COMPACTION_SIGNED_FLOOR_PROVIDES.into(),
        "compaction-notes-tokens/v1".into(),
    ]);
    // The first list is fresh; the second is the transport's kept answer after a failed read.
    let plane = CatalogPlane {
        inner: RecordingPlane::default(),
        capabilities: std::sync::Mutex::new([capabilities.clone(), capabilities].into()),
    };
    for _ in 0..2 {
        plane.inner.answers.lock().unwrap().extend([
            json!({"answer":"ready"}),
            json!({"answer":"received"}),
            json!({"answer":"nothing"}),
        ]);
    }
    let bind = broca_protocol::BindIdentity::new("/project", "test", "s");
    let bound = runtime::BoundPlane {
        plane: Some(&plane),
        provider: "magic",
        bind: &bind,
        scope: None,
    };
    let mut conditions = conditions();
    conditions.signed_floor = Some(SignedFloor::new("m12345678.1"));
    let mut setup = setup();
    setup.signed_floor = conditions.signed_floor.clone();
    let speaker = json!({"agent_id":"agent-a","name":"Ada"});
    for _ in 0..2 {
        let runtime = runtime::Runtime::default();
        let model = broca_protocol::request::ModelHandle::new("stub", "claude-opus-5");
        let config = json!({});
        let globals = vec![text(Role::System, "system instructions")];
        runtime.tokens.lock().unwrap().configure(
            &super::host::Frozen {
                model: &model,
                variant: None,
                service_tier: None,
                frozen_render_config: &config,
                tools: &[],
                global_system: Some(&globals),
                auth_selection: None,
                context_limit: Some(200_000),
                max_output_tokens: Some(1000),
            },
            None,
        );
        let mut marked = facts(1);
        marked[0].speaks_for = Some(serde_json::from_value(speaker.clone()).unwrap());
        runtime.observe(&project(
            &[text(Role::User, "agent words")],
            &marked,
            &Deliveries::new(),
            true,
        ));
        runtime.flush(&bound, &setup, &conditions, None).await;
        runtime
            .ride(
                &bound,
                &setup,
                &RideRequest::new(vec![RideCause::OverLimit], 1000, conditions.clone()),
            )
            .await;
        assert_eq!(runtime.entry_speaks_for(), Some(true));
    }
    let calls = plane.inner.calls.lock().unwrap();
    assert_eq!(calls.len(), 6);
    assert_eq!(&calls[..3], &calls[3..]);
    assert_eq!(calls[0].1["signed_floor"], json!({"through":"m12345678.1"}));
    assert_eq!(
        calls[1].1["conditions"]["signed_floor"],
        calls[0].1["signed_floor"]
    );
    assert_eq!(
        calls[2].1["conditions"]["signed_floor"],
        calls[0].1["signed_floor"]
    );
    assert_eq!(
        calls[1].1["conditions"]["notes_tokens"],
        json!({"count":0,"exact":true})
    );
    assert!(
        calls[1].1["conditions"]["system_tokens"]["count"]
            .as_u64()
            .unwrap()
            > 0
    );
    assert_eq!(calls[1].1["entries"][0]["speaks_for"], speaker);
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
    /// Each applied view's `head_kept`, in order.
    heads: std::sync::Mutex<Vec<bool>>,
    fail: std::sync::atomic::AtomicBool,
}
impl state::Journal for MemoryJournal {
    fn view_refused(&self, publication: &super::diagnostics::Publication, code: &str) {
        publication.warn(
            &broca_protocol::BindIdentity::new("/project", "test", "s"),
            code,
        );
    }
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
    async fn applied(&self, step: u64, view: &View, head_kept: bool) -> Result<(), String> {
        self.heads.lock().unwrap().push(head_kept);
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
    async fn entry_ordinal(&self, _: &str) -> Result<Option<u64>, String> {
        Ok(None)
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
        .accept(&journal, &entries, view.clone(), &rules, Floor::none())
        .await
        .is_err());
    assert!(state.latest().is_none());
    journal
        .fail
        .store(false, std::sync::atomic::Ordering::SeqCst);
    state
        .accept(&journal, &entries, view.clone(), &rules, Floor::none())
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
                floor: Floor::none(),
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
                floor: Floor::none(),
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
                floor: Floor::none(),
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

/// An effort change on the same model rides `effort_change` unless the run sends it in a
/// cache-keeping form; a thinking change always rides it; a change of model is `model_change`
/// only (llm-runner §9.3.6).
#[test]
fn effort_changes_ride_only_when_they_lose_the_cache() {
    use cortexkit_role_compaction::{ride::RideCause, setup::ModelRef};
    let at = |variant: &str, thinking: &str, keeps: bool| session_conditions::SendContext {
        model: ModelRef::new("anthropic", "claude-opus-5-5").with_variant(variant),
        account: None,
        surface: "same".into(),
        thinking: Some(thinking.into()),
        effort_keeps_cache: keeps,
    };
    let conditions = session_conditions::conditions(at("high", "a", true).model, 1000, 100, true);
    let causes = |previous, current| {
        session_conditions::ride_causes(Some(&previous), &current, false, false, 1, &conditions)
    };
    assert!(causes(at("high", "a", true), at("low", "a", true)).is_empty());
    assert_eq!(
        causes(at("high", "a", false), at("low", "a", false)),
        [RideCause::EffortChange]
    );
    assert_eq!(
        causes(at("high", "a", true), at("high", "b", true)),
        [RideCause::EffortChange]
    );
    let mut other = at("low", "a", true);
    other.model = ModelRef::new("anthropic", "claude-sonnet-5-5").with_variant("low");
    assert_eq!(
        causes(at("high", "a", true), other),
        [RideCause::ModelChange]
    );
}

#[test]
fn compaction_v2_rides_include_every_simultaneous_cause() {
    use cortexkit_role_compaction::{ride::RideCause, setup::ModelRef};
    let previous = session_conditions::SendContext {
        model: ModelRef::new("openai", "old"),
        account: Some("a".into()),
        surface: "old".into(),
        thinking: None,
        effort_keeps_cache: false,
    };
    let current = session_conditions::SendContext {
        model: ModelRef::new("anthropic", "new").with_service_tier("priority"),
        account: Some("b".into()),
        surface: "new".into(),
        thinking: None,
        effort_keeps_cache: false,
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
    assert_eq!(setup.detects.len(), 8);
    assert_eq!(setup.view_anchor, Some(true));
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
    assert!(session_conditions::check_plan_version(3).is_ok());
    let error = session_conditions::check_plan_version(1).unwrap_err();
    assert!(error.contains("compaction_plan_version 1"));
    assert!(error.contains("version 2"));
    let newer = cortexkit_compaction_view::PLAN_VERSION + 1;
    assert!(session_conditions::check_plan_version(newer).is_err());
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
        checksum: &view.checksum.to_string(),
        edits: &edits,
    })
    .unwrap();
    let received = ReceivedView::parse(&wire).unwrap();
    assert_eq!(received.view(), &view);
    assert_eq!(received.edits()[0].get(), raw.get());
    let rebuilt = ReceivedView::rebuilt(
        view.version,
        view.checksum.to_string(),
        received.edits().to_vec(),
        None,
        None,
    )
    .unwrap();
    assert_eq!(rebuilt.edits()[0].get(), raw.get());
    assert!(ReceivedView::rebuilt(
        view.version,
        "wrong".into(),
        received.edits().to_vec(),
        None,
        None
    )
    .unwrap_err()
    .contains("checksum mismatch"));
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
    use broca_records::record::StoredViewEdit;
    let raw = serde_json::value::RawValue::from_string(
        r#"{ "since":1, "op":"drop", "mid":"m12345678.0" }"#.into(),
    )
    .unwrap();
    let edit: Edit = serde_json::from_str(raw.get()).unwrap();
    let first = View::seal(1, vec![edit.clone()]);
    let previous =
        ReceivedView::rebuilt(1, first.checksum.to_string(), vec![raw.clone()], None, None)
            .unwrap();
    let next = View::seal(2, vec![edit.clone()]);
    let identical =
        ReceivedView::rebuilt(2, next.checksum.to_string(), vec![raw], None, None).unwrap();
    let encoded = storage::encode(Some(&previous), &identical);
    assert!(matches!(
        encoded.as_slice(),
        [StoredViewEdit::Previous { index: 0 }]
    ));
    assert_eq!(
        storage::rebuild(
            Some(&previous),
            2,
            next.checksum.to_string(),
            &encoded,
            None,
            None
        )
        .unwrap(),
        identical
    );
    let normalized = serde_json::value::to_raw_value(&edit).unwrap();
    let different_bytes =
        ReceivedView::rebuilt(2, next.checksum.to_string(), vec![normalized], None, None).unwrap();
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
    use broca_records::record::StoredViewEdit;
    let previous = ReceivedView::from(View::seal(1, vec![]));
    let error = storage::rebuild(
        Some(&previous),
        2,
        "unused".into(),
        &[StoredViewEdit::Previous { index: 8 }],
        None,
        None,
    )
    .unwrap_err();
    assert!(error.contains("compaction view version 2"));
    assert!(error.contains("previous edit index 8"));
    assert!(storage::rebuild(None, 2, "wrong".into(), &[], None, None)
        .unwrap_err()
        .contains("checksum mismatch"));
}

#[test]
fn compaction_v2_every_final_result_kind_is_a_delivery_carrier() {
    for output in [
        ToolOutput::Json {
            value: json!({"a":1}).into(),
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
        output: ToolOutput::Json {
            value: json!(1).into(),
        },
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
fn compaction_v2_a_marker_naming_another_session_is_refused() {
    use broca_protocol::BindIdentity;
    let original = BindIdentity::new("/synthetic/marker", "runner", "original");
    let fork = BindIdentity::new("/synthetic/marker", "runner", "fork");
    let mut state = state::State::default();
    let error =
        super::fold::restore_marker(&mut state, "runner", "original", 1, &fork).unwrap_err();
    assert!(error.contains("compaction marker session mismatch"));
    assert!(error.contains("runner/original"));
    assert!(error.contains("runner/fork"));
    super::fold::restore_marker(&mut state, "runner", "original", 1, &original).unwrap();
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
        thinking: None,
        effort_keeps_cache: false,
    }
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
    use broca_records::record::RunConfig;
    let config = |max_output_tokens: Option<u64>| RunConfig {
        global_system: None,
        account_route: None,
        route_decision_id: None,
        served_provider: None,
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
        super::host::conditions(&config, &[], false, None, None)
            .unwrap()
            .output_reserve
    };
    assert_eq!(reserve(config(None)), 128_000);
    assert_eq!(reserve(config(Some(4_096))), 4_096);
}

/// Each applied view is recorded with whether it renders the conversation's first message as
/// the view applied before it (or no view) did: a view that edits only later messages keeps the
/// head, and one that rewrites the first message changes it.
#[tokio::test]
async fn an_applied_view_records_whether_it_kept_the_conversations_head() {
    let entries = project(
        &[
            text(Role::User, "first"),
            text(Role::Assistant, "second"),
            text(Role::User, "third"),
            text(Role::Assistant, "fourth"),
        ],
        &facts(4),
        &Deliveries::new(),
        true,
    );
    let replace = |seq: u64, index: usize, value: &str| {
        Edit::new(
            seq,
            EditOp::Replace {
                mid: entries[index].mid.clone(),
                part: "0".into(),
                field: "text".into(),
                value: json!(value),
                expect: entries[index].parts[0].digest.clone(),
            },
        )
    };
    let later = View::seal(1, vec![replace(1, 2, "third, shorter")]);
    let head = View::seal(
        2,
        vec![replace(1, 2, "third, shorter"), replace(2, 0, "a summary")],
    );
    let after = View::seal(
        3,
        vec![
            replace(1, 2, "third, shorter"),
            replace(2, 0, "a summary"),
            replace(3, 3, "fourth, shorter"),
        ],
    );
    let rules = rules::defaults("gpt-5");
    let journal = MemoryJournal::default();
    let mut state = state::State::default();
    for (step, view) in [(1, later), (2, head), (3, after)] {
        state
            .accept(&journal, &entries, view, &rules, Floor::none())
            .await
            .unwrap()
            .unwrap();
        state
            .apply(
                &journal,
                state::Boundary {
                    step,
                    entries: &entries,
                    rules: &rules,
                    floor: Floor::none(),
                    next_mid: "m12345678.5",
                },
            )
            .await
            .unwrap();
    }
    assert_eq!(*journal.heads.lock().unwrap(), [true, false, true]);
}

#[path = "statement_seq_tests.rs"]
mod statement_seq;

/// Setup, notices and rides carry the session's signed floor only to a provider whose catalog
/// entry declares `compaction-signed-floor/v1`, read again at every Setup.
#[tokio::test]
async fn compaction_v2_only_a_declaring_provider_is_sent_the_signed_floor() {
    use cortexkit_role_compaction::ride::{RideCause, RideRequest};
    struct Declaring {
        inner: RecordingPlane,
        provides: std::sync::Mutex<Vec<String>>,
    }
    #[async_trait::async_trait]
    impl runtime::Plane for Declaring {
        async fn call(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> Result<Box<serde_json::value::RawValue>, runtime::CallError> {
            self.inner.call(method, params).await
        }
        async fn provides_signed_floor(&self) -> bool {
            cortexkit_role_compaction::provides_signed_floor(&self.provides.lock().unwrap())
        }
    }
    let floor = cortexkit_role_compaction::SignedFloor::new("m12345678.1");
    let mut conditions = conditions();
    conditions.signed_floor = Some(floor.clone());
    let mut setup = setup();
    setup.signed_floor = Some(floor.clone());
    let ride = RideRequest::new(vec![RideCause::OverLimit], 1000, conditions.clone());
    let entries = |count| {
        project(
            &(0..count)
                .map(|index| text(Role::User, &format!("message {index}")))
                .collect::<Vec<_>>(),
            &facts(count),
            &Deliveries::new(),
            true,
        )
    };
    let plane = Declaring {
        inner: RecordingPlane::default(),
        provides: std::sync::Mutex::new(vec!["compaction-provider/v2".into()]),
    };
    plane.inner.answers.lock().unwrap().extend([
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
        json!({"answer":"nothing"}),
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
        json!({"answer":"nothing"}),
    ]);
    let runtime = runtime::Runtime::default();
    // A provider that doesn't declare the capability hears no floor.
    assert!(runtime.setup(&plane, &setup).await);
    runtime.observe(&entries(1));
    runtime.flush(&plane, &setup, &conditions, None).await;
    runtime.ride(&plane, &setup, &ride).await;
    // Once it declares it, the next Setup and everything after it carry the floor.
    plane
        .provides
        .lock()
        .unwrap()
        .push(cortexkit_role_compaction::COMPACTION_SIGNED_FLOOR_PROVIDES.into());
    assert!(runtime.setup(&plane, &setup).await);
    runtime.observe(&entries(2));
    runtime.flush(&plane, &setup, &conditions, None).await;
    runtime.ride(&plane, &setup, &ride).await;
    let calls = plane.inner.calls.lock().unwrap();
    let floors: Vec<_> = calls
        .iter()
        .map(|(method, params)| {
            let floor = if method == "compaction.setup" {
                &params["signed_floor"]
            } else {
                &params["conditions"]["signed_floor"]
            };
            (method.as_str(), floor.clone())
        })
        .collect();
    let sent = json!({"through": "m12345678.1"});
    assert_eq!(
        floors,
        [
            ("compaction.setup", serde_json::Value::Null),
            ("compaction.notice", serde_json::Value::Null),
            ("compaction.ride", serde_json::Value::Null),
            ("compaction.setup", sent.clone()),
            ("compaction.notice", sent.clone()),
            ("compaction.ride", sent),
        ]
    );
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

/// A failed Setup drops nothing from the queue: the provider didn't read the window, so the
/// entries it named are still owed. (The ready Setup here names no newest entry, so it covers
/// nothing and the notice shows exactly what the failed one left queued.)
#[tokio::test]
async fn compaction_v2_a_failed_setup_drops_nothing_from_the_queue() {
    let plane = RecordingPlane::default();
    plane.answers.lock().unwrap().extend([
        json!({"answer":"future"}),
        json!({"answer":"ready"}),
        json!({"answer":"received"}),
    ]);
    let runtime = runtime::Runtime::default();
    let first = project(
        &[text(Role::User, "one")],
        &facts(1),
        &Deliveries::new(),
        true,
    );
    let mut failed = setup();
    failed.newest = views::newest(&first);
    runtime.observe(&first);
    assert!(!runtime.setup(&plane, &failed).await);
    let second = project(
        &[text(Role::User, "one"), text(Role::User, "two")],
        &facts(2),
        &Deliveries::new(),
        true,
    );
    runtime.observe(&second);
    assert!(runtime.setup(&plane, &setup()).await);
    runtime.flush(&plane, &setup(), &conditions(), None).await;
    let calls = plane.calls.lock().unwrap();
    assert_eq!(calls.len(), 3);
    let entries = calls[2].1["entries"].as_array().unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry["ordinal"].as_u64().unwrap())
            .collect::<Vec<_>>(),
        [1, 2]
    );
}

#[tokio::test]
async fn compaction_v2_unknown_catalog_setup_keeps_attribution_unknown() {
    let plane = CatalogPlane {
        inner: RecordingPlane::default(),
        capabilities: std::sync::Mutex::new([crate::ProviderCapabilities::Unknown].into()),
    };
    plane
        .inner
        .answers
        .lock()
        .unwrap()
        .push_back(json!({"answer":"ready"}));
    let bind = broca_protocol::BindIdentity::new("/project", "test", "s");
    let bound = runtime::BoundPlane {
        plane: Some(&plane),
        provider: "magic",
        bind: &bind,
        scope: None,
    };
    let runtime = runtime::Runtime::default();
    let mut setup = setup();
    setup.signed_floor = Some(cortexkit_role_compaction::SignedFloor::new("m12345678.1"));
    assert!(
        runtime.setup(&bound, &setup).await,
        "unknown capabilities do not block Setup"
    );
    assert_eq!(runtime.entry_speaks_for(), None);
    assert!(plane.inner.calls.lock().unwrap()[0]
        .1
        .get("signed_floor")
        .is_none());
}

#[tokio::test(start_paused = true)]
async fn compaction_v2_catalog_deadline_does_not_learn_false_attribution() {
    struct Slow(RecordingPlane);
    #[async_trait::async_trait]
    impl runtime::Plane for Slow {
        async fn declarations(&self) -> runtime::Declarations {
            std::future::pending().await
        }
        async fn call(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> Result<Box<serde_json::value::RawValue>, runtime::CallError> {
            runtime::Plane::call(&self.0, method, params).await
        }
    }
    let plane = Slow(RecordingPlane::default());
    plane
        .0
        .answers
        .lock()
        .unwrap()
        .push_back(json!({"answer":"ready"}));
    let runtime = runtime::Runtime::default();
    assert!(runtime.setup(&plane, &setup()).await);
    assert_eq!(runtime.entry_speaks_for(), None);
}
