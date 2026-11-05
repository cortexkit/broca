use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
use broca_protocol::ids::{MessageId, ToolCallId};
use broca_protocol::request::ModelHandle;

use super::{Conversation, Delivered, FoldError};
use crate::record::{
    Assistant, EntryFacts, Input, Notice, Placement, Record, ResultAppend, StepStart, ToolReply,
};

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

fn facts(ordinal: u64, tag_base: u64) -> Record {
    Record::EntryFacts(EntryFacts {
        ordinal,
        written_at_ms: 1_000 + ordinal,
        tag_base,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: true,
        clean_reply: true,
        mid: format!("m{}", ordinal - 1),
    })
}

fn tag_base(conversation: &Conversation, ordinal: u64) -> Option<u64> {
    let (_, state) = conversation.window_entry(ordinal)?;
    state.facts.as_ref().map(|facts| facts.tag_base)
}

/// Entry facts attach to the message at their ordinal. A held assistant message's facts wait at
/// its reserved ordinal and move with it when its last result places it; its tool message, placed
/// with it, has none of its own. A later record for the same ordinal replaces the first.
#[test]
fn entry_facts_follow_their_message_through_a_tool_turn() {
    let conversation = fold(
        &[
            input("hi", Placement::RunInput),
            facts(1, 0),
            assistant(&["a"]),
            facts(2, 0),
            reply("a", "out"),
            assistant(&[]),
            facts(4, 1),
            facts(4, 2),
        ],
        false,
    )
    .unwrap();
    assert_eq!(conversation.placed, 4);
    assert_eq!(tag_base(&conversation, 1), Some(0));
    assert_eq!(tag_base(&conversation, 2), Some(0));
    assert_eq!(
        tag_base(&conversation, 3),
        None,
        "the tool message has no facts"
    );
    assert_eq!(tag_base(&conversation, 4), Some(2), "the later record wins");

    let held = fold(
        &[
            input("hi", Placement::RunInput),
            assistant(&["a"]),
            facts(2, 5),
        ],
        false,
    )
    .unwrap();
    let (ordinal, _, state) = held.open_entry().expect("the turn is held");
    assert_eq!(ordinal, 2);
    assert_eq!(state.facts.as_ref().map(|f| f.tag_base), Some(5));
    assert!(held.window_entry(2).is_none(), "not placed yet");
}

/// Facts for an ordinal that isn't placed or reserved refuse, and facts for a message that left
/// the window are dropped with it, since only the count of placed messages is kept.
#[test]
fn entry_facts_for_an_unplaced_ordinal_refuse() {
    assert_eq!(
        fold(&[input("hi", Placement::RunInput), facts(2, 0)], false).unwrap_err(),
        FoldError::FactsWithoutEntry(2)
    );
    assert_eq!(
        fold(&[assistant(&["a"]), facts(2, 0)], false).unwrap_err(),
        FoldError::FactsWithoutEntry(2),
        "the held message reserved ordinal 1"
    );
    let mut conversation = fold(&[input("hi", Placement::RunInput)], false).unwrap();
    conversation.clear_window();
    assert!(conversation.entries.is_empty());
    conversation.apply(&facts(1, 0)).unwrap();
    assert_eq!(conversation.placed, 1);
}

/// In a session that keeps delivered text apart from results, the text joins the entry of the
/// newest message that made the call, at the call's position, with its sender.
#[test]
fn delivered_text_attaches_to_the_call_that_made_it() {
    let notice = Notice {
        sender: "reminders".into(),
        template: Some("nudge".into()),
    };
    let append = |id: &str| {
        Record::ResultAppend(ResultAppend {
            run_id: "r".into(),
            tool_call_id: ToolCallId(id.into()),
            text: "\n\nnote".into(),
            notice: Some(notice.clone()),
            written_at_ms: None,
        })
    };
    let conversation = fold(
        &[
            assistant(&["a", "b"]),
            reply("a", "1"),
            reply("b", "2"),
            append("b"),
            append("gone"),
        ],
        false,
    )
    .unwrap();
    let (_, state) = conversation.window_entry(1).unwrap();
    assert_eq!(
        state.delivered,
        [Delivered {
            part: 1,
            notice: Some(notice),
            text: "\n\nnote".into(),
        }],
        "an unknown call's text attaches nowhere"
    );
    let (_, results) = conversation.window_entry(2).unwrap();
    assert!(results.delivered.is_empty());
}
