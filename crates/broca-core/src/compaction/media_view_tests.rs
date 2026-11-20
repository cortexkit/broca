use super::*;
use crate::compaction::tests::{altered_media, image};
use broca_protocol::{MediaRef, ToolOutput, ToolOutputPart, ToolResult};

fn media_result(parts: Vec<ToolOutputPart>) -> ContentBlock {
    ContentBlock::ToolResult(ToolResult {
        tool_call_id: broca_protocol::ToolCallId("a".into()),
        output: ToolOutput::Parts { parts },
        is_error: false,
    })
}

fn parts(reference: MediaRef) -> Vec<ToolOutputPart> {
    vec![
        ToolOutputPart::Text {
            text: "long before".into(),
        },
        ToolOutputPart::Media { media: reference },
        ToolOutputPart::Text {
            text: "long after".into(),
        },
    ]
}

fn capture(state: &mut CompactionState, input: &[Message]) {
    state.fold(&serde_json::from_value(json!({"type":"steered", "input": input})).unwrap());
}

#[test]
fn captured_index_accepts_steers_and_tool_results_but_never_provider_replacements() {
    let mut state = after_setup();
    capture(
        &mut state,
        &[Message::new(Role::User, vec![ContentBlock::Media(image())])],
    );
    assert!(state.captured.contains(&image()));
    for (i, kind) in ["tool_result", "late_result"].into_iter().enumerate() {
        let reference = altered_media().remove(i);
        let ContentBlock::ToolResult(result) = media_result(parts(reference.clone())) else {
            unreachable!()
        };
        let record = serde_json::from_value(json!({
            "type": kind, "batch_id": "b", "call_key": "a", "outcome": "completed", "result": result,
        })).unwrap();
        state.fold(&record);
        assert!(state.captured.contains(&reference));
    }
    let mut setup = setup_record(None);
    let WalRecord::CompactionMessage { replacement, .. } = &mut setup else {
        unreachable!()
    };
    let foreign = altered_media().remove(4);
    *replacement = std::sync::Arc::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Media(foreign.clone())],
    )]);
    let bytes = replace_bytes(replacement);
    state.fold(&setup);
    assert!(!state.captured.contains(&foreign));
    let (view, _) = make_view(2, 0, vec![ViewRange::replace(0, 1, bytes)]);
    let applied = WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: CompactionViewChoice::Applied,
    };
    state.fold(&applied);
    assert!(!state.captured.contains(&foreign));
}
