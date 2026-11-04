use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;

use super::{Conversation, FoldError};
use crate::record::{Assistant, Input, Placement, Record, ResultAppend, StepStart, ToolReply};

fn model() -> ModelHandle {
    serde_json::from_value(serde_json::json!({"provider_module_id": "p", "model_id": "m"})).unwrap()
}

fn input(text: &str, placement: Placement) -> Record {
    Record::Input(Input {
        run_id: "r".into(),
        placement,
        message: Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }]),
        written_at_ms: None,
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        pruned: Vec::new(),
    })
}

fn assistant(calls: &[&str]) -> Record {
    let content = if calls.is_empty() {
        vec![ContentBlock::Text {
            text: "done".into(),
        }]
    } else {
        calls
            .iter()
            .map(|id| {
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId((*id).into()),
                    tool_name: "bash".into(),
                    input: serde_json::json!({}),
                    thought_signature: None,
                })
            })
            .collect()
    };
    Record::Assistant(Assistant {
        run_id: "r".into(),
        step_id: 1,
        provider_message_id: MessageId("m".into()),
        message: Message::assistant_from(content, model()),
        usage: None,
        written_at_ms: None,
        last_content_at_ms: None,
        budget_finalize: None,
        tags_v1: None,
        pruned: Vec::new(),
    })
}

fn reply(id: &str, text: &str) -> Record {
    Record::ToolReply(ToolReply {
        run_id: "r".into(),
        batch_id: Some("b".into()),
        result: ToolResult {
            tool_call_id: ToolCallId(id.into()),
            output: ToolOutput::Text { text: text.into() },
            is_error: false,
        },
        finished_at_ms: None,
        tags_v1: None,
    })
}

fn fold(records: &[Record], appends: bool) -> Result<Conversation, FoldError> {
    let mut conversation = Conversation::new(appends);
    for record in records {
        conversation.apply(record)?;
    }
    Ok(conversation)
}

fn result_ids(message: &Message) -> Vec<&str> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.as_str()),
            _ => None,
        })
        .collect()
}

#[test]
fn a_tool_turn_enters_once_every_call_is_answered_with_results_in_call_order() {
    let mut conversation = Conversation::new(false);
    for record in [
        input("hi", Placement::RunInput),
        assistant(&["a", "b"]),
        reply("b", "2"),
    ] {
        conversation.apply(&record).unwrap();
    }
    assert_eq!(
        conversation.messages.len(),
        1,
        "the turn waits for its second reply"
    );
    assert!(conversation.open_tool_turn().is_some());

    conversation.apply(&reply("a", "1")).unwrap();
    assert!(conversation.open_tool_turn().is_none());
    let roles: Vec<Role> = conversation.messages.iter().map(|m| m.role).collect();
    assert_eq!(roles, [Role::User, Role::Assistant, Role::Tool]);
    assert_eq!(result_ids(&conversation.messages[2]), ["a", "b"]);
}

#[test]
fn ledger_records_and_misplaced_records_refuse() {
    let ledger = Record::StepStart(StepStart {
        run_id: "r".into(),
        step_id: 1,
    });
    assert_eq!(
        fold(&[ledger], false).unwrap_err(),
        FoldError::LedgerRecord("StepStart")
    );
    assert_eq!(
        fold(&[reply("x", "")], false).unwrap_err(),
        FoldError::ReplyWithoutCall("x".into())
    );
    assert_eq!(
        fold(&[assistant(&["a"]), reply("a", ""), reply("a", "")], false).unwrap_err(),
        FoldError::ReplyWithoutCall("a".into()),
        "the turn closed at its first reply"
    );
    assert_eq!(
        fold(
            &[assistant(&["a", "b"]), reply("a", ""), reply("a", "")],
            false
        )
        .unwrap_err(),
        FoldError::SecondReply("a".into())
    );
    assert_eq!(
        fold(&[assistant(&["a"]), input("s", Placement::Steer)], false).unwrap_err(),
        FoldError::InputInsideToolTurn
    );
    assert_eq!(
        fold(&[assistant(&["a"]), assistant(&[])], false).unwrap_err(),
        FoldError::AssistantInsideToolTurn
    );
}

#[test]
fn a_new_run_drops_a_turn_its_interrupted_run_left_unanswered() {
    let conversation = fold(
        &[
            input("hi", Placement::RunInput),
            assistant(&["a"]),
            input("again", Placement::RunInput),
        ],
        false,
    )
    .unwrap();
    let roles: Vec<Role> = conversation.messages.iter().map(|m| m.role).collect();
    assert_eq!(roles, [Role::User, Role::User]);
    assert!(conversation.open_tool_turn().is_none());
}

#[test]
fn a_budget_finalize_step_enters_at_once_without_its_calls() {
    let Record::Assistant(mut step) = assistant(&["a"]) else {
        unreachable!()
    };
    step.message.content.push(ContentBlock::Text {
        text: "summary".into(),
    });
    step.budget_finalize = Some(true);
    let conversation = fold(&[Record::Assistant(step)], false).unwrap();
    assert!(conversation.open_tool_turn().is_none());
    assert_eq!(
        conversation.messages[0].content,
        [ContentBlock::Text {
            text: "summary".into()
        }]
    );
}

#[test]
fn delivered_text_joins_its_result_only_where_the_session_stores_it_there() {
    let append = Record::ResultAppend(ResultAppend {
        run_id: "r".into(),
        tool_call_id: ToolCallId("a".into()),
        text: " +note".into(),
        notice: None,
        written_at_ms: None,
    });
    let records = [assistant(&["a"]), reply("a", "out"), append];
    let text = |conversation: &Conversation| match &conversation.messages[1].content[0] {
        ContentBlock::ToolResult(ToolResult {
            output: ToolOutput::Text { text },
            ..
        }) => text.clone(),
        other => panic!("{other:?}"),
    };
    assert_eq!(text(&fold(&records, true).unwrap()), "out +note");
    assert_eq!(text(&fold(&records, false).unwrap()), "out");
}
