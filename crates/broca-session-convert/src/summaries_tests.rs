//! Reasoning summaries are display facts the new storage records when it places a message, so a
//! sealed page serves the ones the open page served, though the seal prunes the encrypted
//! reasoning they are read from.

use broca_module::serve::SessionReadRequest;
use broca_module::store_read::todays::Todays;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{MemoryBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{Placement, Record};
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use futures::executor::block_on;
use serde_json::Value;

const RUN: &str = r#"{"type":"run_started","run_id":"RUN","session":{"project_root":"/project","harness":"runner","session":"s"},"config":{"model":{"provider_module_id":"p","model_id":"m"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":1},"input":[{"role":"user","content":[{"type":"text","text":"question"}]}],"origin":{"kind":"fresh"}}"#;

fn log(lines: &[String]) -> Vec<FramedRecord> {
    lines
        .iter()
        .enumerate()
        .map(|(index, line)| FramedRecord {
            seq: index as u64 + 1,
            fence: 0,
            record: serde_json::from_str::<WalRecord>(line)
                .unwrap_or_else(|error| panic!("line {index}: {error}")),
        })
        .collect()
}

/// A model step whose message reasons with a summary, then answers.
fn reasoning_step(step: u64) -> String {
    let item = serde_json::json!({
        "type": "reasoning",
        "id": format!("rs_{step}"),
        "summary": [{"type": "summary_text", "text": format!("Weighing step {step}")}],
        "encrypted_content": "c2VjcmV0",
    })
    .to_string();
    serde_json::json!({
        "type": "model_step_finished",
        "step_id": step,
        "assistant_message": {
            "message_id": format!("m{step}"),
            "content": [
                {"type": "redacted_reasoning", "data": item, "provider_format": "open_ai_responses"},
                {"type": "text", "text": "answer"},
            ],
        },
        "usage": {"input_tokens": 1},
        "finish_reason": "stop",
        "retries_used": 0,
    })
    .to_string()
}

/// The reply in each read mode to a read of the whole session.
fn replies(
    read: impl Fn(&SessionReadRequest) -> broca_module::serve::SessionReadResponse,
) -> Vec<broca_module::serve::SessionReadResponse> {
    [
        (false, false, false),
        (true, false, false),
        (false, true, false),
        (false, false, true),
        (true, true, true),
    ]
    .into_iter()
    .map(|(display, tools, rich_ui)| {
        let key = broca_protocol::BindIdentity::new("/summaries", "summaries", "s");
        let mut request = SessionReadRequest::new(key, Some(1), Some(64));
        request.include_display_facts = display;
        request.include_tools = tools;
        request.include_rich_ui = rich_ui;
        read(&request)
    })
    .collect()
}

/// The reply without its storage's own identity (`incarnation`, `history_version`), which a
/// seal changes, and with each reasoning block's encrypted reasoning, which a seal prunes.
fn comparable(reply: &broca_module::serve::SessionReadResponse) -> Value {
    let mut value = serde_json::to_value(reply).unwrap();
    let object = value.as_object_mut().unwrap();
    object.remove("incarnation");
    object.remove("history_version");
    for message in value["messages"].as_array_mut().unwrap() {
        for block in message["message"]["content"].as_array_mut().unwrap() {
            if block["type"] == "redacted_reasoning" {
                block["data"] = Value::Null;
            }
        }
    }
    value
}

#[test]
fn a_sealed_page_serves_the_reasoning_summaries_its_open_page_served() {
    let old = log(&[
        RUN.replace("RUN", "r1"),
        r#"{"type":"step_started","step_id":1}"#.into(),
        reasoning_step(1),
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#.into(),
        RUN.replace("RUN", "r2"),
        r#"{"type":"step_started","step_id":2}"#.into(),
        reasoning_step(2),
        r#"{"type":"run_finished","reason":"completed","usage":{},"retries_used":0}"#.into(),
    ]);
    let lineage = "0b3c3c1e-1f2a-4c1d-9e57-5a0e5f0c0002";
    let converted = crate::convert(&old, Some(lineage)).unwrap();
    let parts = MemoryParts {
        parts: converted.parts.clone(),
    };
    let backend = MemoryBackend::default();
    let addr = SessionAddr("summaries".into());
    let triple = SessionTriple {
        project_root: "/summaries".into(),
        harness: "summaries".into(),
        session: "s".into(),
    };
    let deployment = DeploymentKey::new(1, &[0x5a; 32]);
    let today = Todays::new(&old, Some(lineage));
    let todays = replies(|request| today.read(request).unwrap());
    let mut session = block_on(Session::create(
        &backend,
        addr.clone(),
        triple.clone(),
        &deployment,
        false,
    ))
    .unwrap();
    let mut second_input = None;
    for record in &converted.records {
        let seq = block_on(session.append(std::slice::from_ref(record), false)).unwrap();
        if matches!(record, Record::Input(input) if input.placement == Placement::RunInput) {
            second_input = Some(seq);
        }
    }
    let open = replies(|request| {
        block_on(broca_module::store_read::serve_view(
            &session, &parts, request,
        ))
        .unwrap()
    });
    block_on(session.seal(second_input, 0)).unwrap();
    block_on(session.close()).unwrap();
    let view = block_on(Session::view(&backend, addr, &triple, &deployment)).unwrap();
    let sealed = replies(|request| {
        block_on(broca_module::store_read::serve_view(
            &*view, &parts, request,
        ))
        .unwrap()
    });
    // The open pages serve today's summaries, on both runs' messages.
    let summaries = |reply: &broca_module::serve::SessionReadResponse| {
        reply
            .messages
            .iter()
            .map(|message| message.reasoning_summaries.len())
            .sum::<usize>()
    };
    assert_eq!(summaries(&open[1]), 2);
    for (open, today) in open.iter().zip(&todays) {
        assert_eq!(
            serde_json::to_value(&open.messages).unwrap(),
            serde_json::to_value(&today.messages).unwrap()
        );
    }
    // The seal pruned the first run's encrypted reasoning, and its page still serves its
    // summary, in every read mode.
    let first = &sealed[1].messages[1].message.content[0];
    assert!(
        matches!(first, broca_protocol::ContentBlock::RedactedReasoning { data, .. } if data.is_empty()),
        "the seal prunes the first run's reasoning: {first:?}"
    );
    for (sealed, open) in sealed.iter().zip(&open) {
        assert_eq!(comparable(sealed), comparable(open));
    }
}
