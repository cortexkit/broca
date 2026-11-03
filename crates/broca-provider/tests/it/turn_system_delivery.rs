use broca_core::{delivery_order::OrderedSend, provider::Provider, steer::SteerInbox};
use broca_protocol::{
    ContentBlock, DeliveryMode, DeliveryOrder, Message, Role, RunId, StepId, SubmissionId,
};
use broca_provider::{
    families::{
        anthropic_apikey_frozen_config, gemini_frozen_config, openai_responses_frozen_config,
        AnthropicMessages, Gemini, OpenAiResponses,
    },
    transport::LiveProvider,
    wire::AuthHandle,
};
use broca_wal::WalRecord;

fn text(role: Role, value: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: value.into() }])
}

fn note(id: &str, modes: &[DeliveryMode], template: bool) -> OrderedSend {
    OrderedSend {
        admitted_at_ms: Some(1),
        submission_id: SubmissionId(id.into()),
        principal: None,
        order: DeliveryOrder {
            modes: modes.to_vec(),
            dedupe_key: None,
            expires_at_ms: None,
            template: template.then(|| serde_json::from_value(serde_json::json!({
                "id": "note", "digest": "0000000000000000000000000000000000000000000000000000000000000000", "slots": {}
            })).unwrap()),
        },
        input: vec![text(Role::User, "Keep the summary concise.")],
    }
}

fn boundary(
    inbox: &SteerInbox,
    provider: &dyn Provider,
    preceding: &Message,
) -> (Vec<Message>, Vec<WalRecord>) {
    let mut recorded = Vec::new();
    let input = inbox
        .try_take_at_step_with_system(
            &RunId("r".into()),
            10,
            StepId(1),
            Some(preceding),
            &|preceding, following| provider.turn_system_possible(preceding, following),
            |records| {
                let tags = vec![None; records.len()];
                recorded = records;
                tags
            },
        )
        .unwrap();
    (input, recorded)
}

#[test]
fn responses_delivers_only_templates_as_system_and_freezes_the_exact_input() {
    let provider: Box<dyn Provider> = Box::new(LiveProvider::new(
        OpenAiResponses,
        openai_responses_frozen_config("gpt-4o"),
        AuthHandle::Bearer("fixture".into()),
    ));
    let inbox = SteerInbox::new();
    inbox.accept_ordered(note("template", &[DeliveryMode::TurnSystem], true), |_| {});
    let preceding = text(Role::User, "Summarize the notes.");
    let (input, records) = boundary(&inbox, provider.as_ref(), &preceding);
    assert_eq!(input.len(), 1);
    assert_eq!(input[0].role, Role::System);
    assert!(input[0].turn_system);
    assert!(
        matches!(&records[..], [WalRecord::SendDelivered { mode: DeliveryMode::TurnSystem, input: Some(stored), .. }] if stored.as_ref() == &input)
    );
    let mut opts = broca_protocol::CallOptions::new(vec![preceding]);
    opts.prompt.extend(input);
    let body: serde_json::Value = serde_json::from_slice(&provider.render_request(&opts)).unwrap();
    assert_eq!(body["input"][1]["role"], "developer");
    assert!(!body["instructions"]
        .as_str()
        .unwrap_or("")
        .contains("Keep the summary concise."));
    inbox.accept_ordered(
        note(
            "untrusted",
            &[DeliveryMode::TurnSystem, DeliveryMode::Steer],
            false,
        ),
        |_| {},
    );
    let (input, records) = boundary(&inbox, provider.as_ref(), &text(Role::User, "next"));
    assert_eq!(input[0].role, Role::User);
    assert!(!input[0].turn_system);
    assert!(matches!(
        records[0],
        WalRecord::SendDelivered {
            mode: DeliveryMode::Steer,
            ..
        }
    ));
}

#[test]
fn gemini_pure_turn_system_stays_pending_and_mixed_list_falls_through() {
    let provider = LiveProvider::new(
        Gemini,
        gemini_frozen_config("gemini-2.5-flash"),
        AuthHandle::Bearer("fixture".into()),
    );
    let inbox = SteerInbox::new();
    inbox.accept_ordered(note("pure", &[DeliveryMode::TurnSystem], true), |_| {});
    let preceding = text(Role::User, "Summarize the notes.");
    let (input, records) = boundary(&inbox, &provider, &preceding);
    assert!(input.is_empty() && records.is_empty());
    assert!(inbox.take_consumed().is_empty());
    inbox.accept_ordered(
        note(
            "mixed",
            &[DeliveryMode::TurnSystem, DeliveryMode::Steer],
            true,
        ),
        |_| {},
    );
    let (input, records) = boundary(&inbox, &provider, &preceding);
    assert_eq!(input, vec![text(Role::User, "Keep the summary concise.")]);
    assert!(matches!(
        records[0],
        WalRecord::SendDelivered {
            mode: DeliveryMode::Steer,
            ..
        }
    ));
    assert_eq!(inbox.take_consumed(), vec![SubmissionId("mixed".into())]);
    let (input, records) = boundary(&inbox, &provider, &preceding);
    assert!(
        input.is_empty() && records.is_empty(),
        "the pure send is still pending"
    );
}

#[test]
fn anthropic_requires_frozen_support_and_a_legal_final_position() {
    let preceding = text(Role::User, "Summarize the notes.");
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-4-8");
    let provider = LiveProvider::new(
        AnthropicMessages,
        frozen.clone(),
        AuthHandle::Bearer("fixture".into()),
    );
    let inbox = SteerInbox::new();
    inbox.accept_ordered(
        note(
            "unconfirmed",
            &[DeliveryMode::TurnSystem, DeliveryMode::Steer],
            true,
        ),
        |_| {},
    );
    let (_, records) = boundary(&inbox, &provider, &preceding);
    assert!(matches!(
        records[0],
        WalRecord::SendDelivered {
            mode: DeliveryMode::Steer,
            ..
        }
    ));

    frozen.anthropic_turn_system = true;
    let provider = LiveProvider::new(
        AnthropicMessages,
        frozen,
        AuthHandle::Bearer("fixture".into()),
    );
    inbox.accept_ordered(note("waiting", &[DeliveryMode::TurnSystem], true), |_| {});
    let (input, records) = boundary(
        &inbox,
        &provider,
        &text(Role::Assistant, "Here is the summary."),
    );
    assert!(input.is_empty() && records.is_empty());
    inbox.accept_ordered(note("later-user", &[DeliveryMode::Steer], false), |_| {});
    let (input, records) = boundary(&inbox, &provider, &preceding);
    assert_eq!(
        input.len(),
        1,
        "system text cannot precede another user message"
    );
    assert!(matches!(
        records[0],
        WalRecord::SendDelivered {
            mode: DeliveryMode::Steer,
            ..
        }
    ));
    let (input, records) = boundary(&inbox, &provider, &preceding);
    assert_eq!(input[0].role, Role::System);
    assert!(matches!(
        records[0],
        WalRecord::SendDelivered {
            mode: DeliveryMode::TurnSystem,
            ..
        }
    ));
}

#[test]
fn system_precedence_over_tool_result_is_preserved_until_the_request_boundary() {
    let inbox = SteerInbox::new();
    inbox.set_system_after_tool_result(true);
    inbox.accept_ordered(
        note(
            "system-first",
            &[DeliveryMode::TurnSystem, DeliveryMode::ToolResult],
            true,
        ),
        |_| {},
    );
    let result = broca_protocol::ToolResult {
        tool_call_id: broca_protocol::ToolCallId("call_1".into()),
        output: broca_protocol::ToolOutput::Text {
            text: "notes".into(),
        },
        is_error: false,
    };
    let appends = inbox.deliver_with_tool_results(&RunId("r".into()), 10, [&result], |records| {
        assert!(records.is_empty())
    });
    assert!(appends.is_empty());
    let provider = LiveProvider::new(
        OpenAiResponses,
        openai_responses_frozen_config("gpt-4o"),
        AuthHandle::Bearer("fixture".into()),
    );
    let (input, records) = boundary(
        &inbox,
        &provider,
        &Message::new(Role::Tool, vec![ContentBlock::ToolResult(result)]),
    );
    assert_eq!(input.len(), 1);
    assert!(matches!(
        records[0],
        WalRecord::SendDelivered {
            mode: DeliveryMode::TurnSystem,
            ..
        }
    ));
}

#[test]
fn writer_serializer_pins_the_reader_compatibility_log() {
    use broca_provider::wire::WireFamily;
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-4-8");
    frozen.anthropic_turn_system = true;
    let provider = LiveProvider::new(
        AnthropicMessages,
        frozen.clone(),
        AuthHandle::Bearer("fixture".into()),
    );
    let inbox = SteerInbox::new();
    let preceding = text(Role::User, "Summarize the notes.");
    inbox.accept_ordered(note("compat", &[DeliveryMode::TurnSystem], true), |_| {});
    let (input, mut delivered) = boundary(&inbox, &provider, &preceding);
    let mut records = vec![serde_json::from_value::<WalRecord>(serde_json::json!({
        "type":"run_started", "run_id":"r", "session":{"project_root":"/fixture","harness":"h","session":"s"},
        "config":{"model":{"provider_module_id":"anthropic","model_id":"claude-opus-4-8"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":frozen.renderer_version,"frozen_render_config":frozen},
        "input":[preceding], "origin":{"kind":"fresh"}
    })).unwrap()];
    records.append(&mut delivered);
    records.push(WalRecord::StepStarted { step_id: StepId(1) });
    let mut bytes = Vec::new();
    for (index, record) in records.iter().enumerate() {
        bytes.extend(broca_wal::framing::encode(index as u64 + 1, 1, record).unwrap());
    }
    let mut prompt = vec![preceding];
    prompt.extend(input);
    let opts = broca_protocol::CallOptions::new(prompt);
    let native = AnthropicMessages.render(&frozen, &opts);
    frozen.anthropic_turn_system = false;
    let downgraded = AnthropicMessages.render(&frozen, &opts);
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures");
    for (name, bytes) in [
        ("turn-system-writer.wal", bytes),
        ("turn-system-writer-native.json", native),
        ("turn-system-writer-downgraded.json", downgraded),
    ] {
        let path = root.join(name);
        if std::env::var_os("UPDATE_GOLDENS").is_some() {
            std::fs::write(&path, &bytes).unwrap();
        }
        assert_eq!(bytes, std::fs::read(path).unwrap(), "{name}");
    }
}
