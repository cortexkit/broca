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
        media: Vec::new(),
        run_id: "r".into(),
        placement,
        message: std::sync::Arc::new(Message::new(
            Role::User,
            vec![ContentBlock::Text { text: text.into() }],
        )),
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
                    input: serde_json::json!({}).into(),
                    thought_signature: None,
                    provider_state: None,
                })
            })
            .collect()
    };
    Record::Assistant(Assistant {
        run_id: "r".into(),
        step_id: 1,
        provider_message_id: MessageId("m".into()),
        message: std::sync::Arc::new(Message::assistant_from(content, model())),
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
        media: Vec::new(),
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
fn the_first_model_answer_excludes_assistant_input_and_survives_a_window_cut() {
    use crate::codec::Message as _;
    let mut imported = input("import", Placement::RunInput);
    let Record::Input(value) = &mut imported else {
        unreachable!()
    };
    std::sync::Arc::make_mut(&mut value.message).role = Role::Assistant;
    let mut conversation = fold(
        &[input("hi", Placement::RunInput), imported, assistant(&[])],
        false,
    )
    .unwrap();
    assert_eq!(conversation.episodes[0].first_model_answer, Some(3));
    let bytes = conversation.encode();
    let decoded = Conversation::decode(&bytes).unwrap();
    assert_eq!(decoded.episodes[0].first_model_answer, Some(3));
    conversation.seal_window(4, None).unwrap();
    assert_eq!(conversation.episodes[0].first_model_answer, Some(3));
}

#[test]
fn the_master_episode_decoder_skips_the_new_checkpoint_boundary() {
    use crate::codec::{Fields, Message as _, Value as _};
    // The Episode declaration from master 77ccd4b85, using the same unchanged codec macros.
    // Keeping its real old field set exercises the old generated decoder, not the new one.
    crate::message! {
        #[derive(Debug,PartialEq)]
        struct MasterEpisode {
            1 req run_id: String,
            2 req start: u64,
            3 opt first_user: u64,
            4 opt render_config: crate::record::PartRef,
            5 opt render_key: crate::codec::Digest,
        }
    }
    let conversation = fold(&[input("hi", Placement::RunInput), assistant(&[])], false).unwrap();
    let checkpoint = crate::checkpoint::Checkpoint {
        covers_through: 3,
        next_seq: 4,
        conversation,
        operational: crate::fold::operations::Operations::default(),
        transcript_base: None,
        ledger_base: None,
        activity_ms: None,
        media: None,
    };
    let bytes = checkpoint.encode();
    let conversation = Fields::new(&bytes, "Checkpoint")
        .find_map(|field| {
            let field = field.unwrap();
            (field.number == 3).then_some(field.raw)
        })
        .unwrap();
    let body = crate::codec::len_payload(conversation, "Conversation").unwrap();
    let mut decoded = 0;
    for field in Fields::new(body, "Conversation") {
        let field = field.unwrap();
        if field.number == 10 || field.number == 12 {
            let before = crate::codec::skipped_unknown_fields();
            let old = MasterEpisode::read(field.raw).unwrap();
            assert_eq!(old.run_id, "r");
            assert_eq!(old.start, 1);
            assert_eq!(crate::codec::skipped_unknown_fields(), before + 1);
            decoded += 1;
        }
    }
    assert_eq!(
        decoded, 1,
        "a real checkpoint included the extended episode"
    );
    assert_eq!(
        crate::checkpoint::Checkpoint::decode(&bytes)
            .unwrap()
            .conversation
            .episodes[0]
            .first_model_answer,
        Some(2)
    );
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
        conversation.held_messages().unwrap().len(),
        1,
        "the turn waits for its second reply"
    );
    assert!(conversation.open_tool_turn().is_some());

    conversation.apply(&reply("a", "1")).unwrap();
    assert!(conversation.open_tool_turn().is_none());
    let roles: Vec<Role> = conversation
        .held_messages()
        .unwrap()
        .iter()
        .map(|m| m.role)
        .collect();
    assert_eq!(roles, [Role::User, Role::Assistant, Role::Tool]);
    assert_eq!(
        result_ids(&conversation.held_messages().unwrap()[2]),
        ["a", "b"]
    );
}

#[test]
fn ledger_records_and_misplaced_records_refuse() {
    let ledger = Record::StepStart(StepStart {
        run_id: "r".into(),
        step_id: 1,
        mid: None,
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
    let roles: Vec<Role> = conversation
        .held_messages()
        .unwrap()
        .iter()
        .map(|m| m.role)
        .collect();
    assert_eq!(roles, [Role::User, Role::User]);
    assert!(conversation.open_tool_turn().is_none());
}

#[test]
fn a_budget_finalize_step_enters_at_once_without_its_calls() {
    let Record::Assistant(mut step) = assistant(&["a"]) else {
        unreachable!()
    };
    std::sync::Arc::make_mut(&mut step.message)
        .content
        .push(ContentBlock::Text {
            text: "summary".into(),
        });
    step.budget_finalize = Some(true);
    let conversation = fold(&[Record::Assistant(step)], false).unwrap();
    assert!(conversation.open_tool_turn().is_none());
    assert_eq!(
        conversation.held_messages().unwrap()[0].content,
        [ContentBlock::Text {
            text: "summary".into()
        }]
    );
}

#[test]
fn delivered_text_joins_its_result_only_where_the_session_stores_it_there() {
    let append = Record::ResultAppend(ResultAppend {
        send_id: None,
        run_id: "r".into(),
        tool_call_id: ToolCallId("a".into()),
        text: " +note".into(),
        notice: None,
        written_at_ms: None,
        ride: None,
    });
    let records = [assistant(&["a"]), reply("a", "out"), append];
    let text =
        |conversation: &Conversation| match &conversation.held_messages().unwrap()[1].content[0] {
            ContentBlock::ToolResult(ToolResult {
                output: ToolOutput::Text { text },
                ..
            }) => text.clone(),
            other => panic!("{other:?}"),
        };
    assert_eq!(text(&fold(&records, true).unwrap()), "out +note");
    assert_eq!(text(&fold(&records, false).unwrap()), "out");
}

/// A ride (text delivered with an envelope) is never joined into the stored result, in either
/// layout: it waits beside the call's entry, envelope and all, for requests to render it.
#[test]
fn a_ride_is_kept_beside_its_call_in_both_layouts() {
    let ride = crate::record::RideEnvelope {
        named_speaker: None,
        call_key: "s:4".into(),
        sender: "reminders".into(),
        forms: vec!["tool_result".into()],
        form: crate::record::RideForm::Append,
        speaks_for: None,
    };
    let append = Record::ResultAppend(ResultAppend {
        send_id: Some("q".into()),
        run_id: "r".into(),
        tool_call_id: ToolCallId("a".into()),
        text: "\n\nnote".into(),
        notice: None,
        written_at_ms: None,
        ride: Some(ride.clone()),
    });
    let records = [assistant(&["a"]), reply("a", "out"), append];
    for appends in [true, false] {
        let conversation = fold(&records, appends).unwrap();
        match &conversation.held_messages().unwrap()[1].content[0] {
            ContentBlock::ToolResult(ToolResult {
                output: ToolOutput::Text { text },
                ..
            }) => assert_eq!(text, "out", "appends in conversation: {appends}"),
            other => panic!("{other:?}"),
        }
        let (_, state) = conversation.window_entry(1).unwrap();
        assert_eq!(
            state.delivered,
            [Delivered {
                part: 0,
                notice: None,
                text: "\n\nnote".into(),
                send_id: Some("q".into()),
                ride: Some(ride.clone()),
            }]
        );
    }
}

fn facts(ordinal: u64, tag_base: u64) -> Record {
    Record::EntryFacts(std::sync::Arc::new(EntryFacts {
        send_id: None,
        named_speaker: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: None,
        legacy_view_seq: None,
        ordinal,
        written_at_ms: Some(1_000 + ordinal),
        tag_base,
        notice: None,
        usage: None,
        generated_under: None,
        native: true,
        render_tags: true,
        clean_reply: true,
        mid: format!("m{}", ordinal - 1),
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        notices: Vec::new(),
    }))
}

fn tag_base(conversation: &Conversation, ordinal: u64) -> Option<u64> {
    let (_, state) = conversation.window_entry(ordinal)?;
    state.facts.as_ref().map(|facts| facts.tag_base)
}

fn delivered(call: &str, text: &str) -> Record {
    Record::ResultAppend(ResultAppend {
        send_id: None,
        run_id: "r".into(),
        tool_call_id: ToolCallId(call.into()),
        text: text.into(),
        notice: None,
        written_at_ms: None,
        ride: None,
    })
}

/// The fold says which placed messages changed since a reader's mark, by the lowest ordinal of
/// each kind of change: restated facts, text delivered beside a call, and text joined into a
/// result. Messages placed since the mark aren't changes; a copy of the fold, and an emptied
/// window, answer every mark with "everything".
#[test]
fn the_fold_names_the_lowest_message_changed_since_a_mark() {
    for appends in [false, true] {
        let mut conversation = fold(
            &[
                input("hi", Placement::RunInput),
                assistant(&["a"]),
                reply("a", "one"),
                assistant(&["b"]),
                reply("b", "two"),
            ],
            appends,
        )
        .unwrap();
        let changes = |conversation: &Conversation, mark| {
            (
                conversation.changes.states_since(mark),
                conversation.changes.messages_since(mark),
            )
        };
        let mark = conversation.changes.mark();
        assert_eq!(changes(&conversation, mark), (None, None));

        // Facts restated for the third message, then the second: the lowest counts.
        conversation.apply(&facts(3, 0)).unwrap();
        conversation.apply(&facts(2, 0)).unwrap();
        assert_eq!(changes(&conversation, mark), (Some(2), None));
        let later = conversation.changes.mark();
        assert_eq!(changes(&conversation, later), (None, None));

        // Text delivered with the newest call: kept beside it, or joined into its result.
        conversation.apply(&delivered("b", " +note")).unwrap();
        let expected = if appends {
            (None, Some(5))
        } else {
            (Some(4), None)
        };
        assert_eq!(changes(&conversation, later), expected, "joins: {appends}");
        assert_eq!(changes(&conversation, mark).0, Some(2));

        // Messages placed since a mark aren't changes to it.
        let placed = conversation.changes.mark();
        conversation
            .apply(&input("more", Placement::RunInput))
            .unwrap();
        assert_eq!(changes(&conversation, placed), (None, None));

        assert_eq!(
            changes(&conversation.clone(), placed),
            (Some(0), Some(0)),
            "a copy is another fold"
        );
        conversation.clear_window();
        assert_eq!(changes(&conversation, placed), (Some(0), Some(0)));
    }
}

/// Past its cap, the record of changes keeps the lowest ordinal changed since every mark it
/// still answers, or a lower one, never a higher one.
#[test]
fn the_record_of_changes_stays_bounded_without_missing_a_change() {
    let mut conversation = fold(&[input("hi", Placement::RunInput)], false).unwrap();
    for _ in 0..200 {
        conversation
            .apply(&input("more", Placement::Steer))
            .unwrap();
    }
    let first = conversation.changes.mark();
    let mut marks = Vec::new();
    for ordinal in 2..=200 {
        marks.push((ordinal, conversation.changes.mark()));
        conversation.apply(&facts(ordinal, 0)).unwrap();
    }
    assert!(conversation.changes.states.0.len() <= super::RESTATED_CAP);
    assert_eq!(conversation.changes.states_since(first), Some(2));
    for (ordinal, mark) in marks {
        let lowest = conversation.changes.states_since(mark).unwrap();
        assert!(
            lowest <= ordinal,
            "since the mark before {ordinal}: {lowest}"
        );
    }
    let newest = conversation.changes.mark();
    assert_eq!(conversation.changes.states_since(newest), None);
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
            send_id: None,
            run_id: "r".into(),
            tool_call_id: ToolCallId(id.into()),
            text: "\n\nnote".into(),
            notice: Some(notice.clone()),
            written_at_ms: None,
            ride: None,
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
            send_id: None,
            ride: None,
        }],
        "an unknown call's text attaches nowhere"
    );
    let (_, results) = conversation.window_entry(2).unwrap();
    assert!(results.delivered.is_empty());
}

/// `records` folded at seqs 1, 2, ... in order.
fn fold_at(records: &[Record]) -> Conversation {
    let mut conversation = Conversation::new(false);
    for (seq, record) in (1..).zip(records) {
        conversation.apply_at(Some(seq), record).unwrap();
    }
    conversation
}

fn turn_records() -> Vec<Record> {
    vec![
        input("hello", Placement::RunInput),
        assistant(&["a", "b"]),
        reply("a", "first result"),
        reply("b", "second result"),
        assistant(&[]),
    ]
}

/// Each window message knows the seqs of the records its content came from; both messages of a
/// tool turn come from the assistant's record and its replies.
#[test]
fn a_message_folded_at_known_seqs_knows_its_sources() {
    let conversation = fold_at(&turn_records());
    let sources: Vec<&[u64]> = conversation
        .messages
        .iter()
        .map(|message| message.sources())
        .collect();
    assert_eq!(sources, [&[1][..], &[2, 3, 4], &[2, 3, 4], &[5]]);
    // A fold that isn't told the seqs knows none.
    let unknown = fold(&turn_records(), false).unwrap();
    assert!(unknown
        .messages
        .iter()
        .all(|message| message.sources().is_empty()));
}

/// A released block keeps its shape (kind, call id, both runner part kinds), its message stops
/// offering its content, and reading it back from its records gives the original.
#[test]
fn a_released_block_keeps_its_shape_and_reads_back_whole() {
    let records = turn_records();
    let mut conversation = fold_at(&records);
    let original = std::sync::Arc::clone(conversation.held_message(3).unwrap().unwrap());
    let shape_before: Vec<_> = conversation
        .window_message(3)
        .unwrap()
        .blocks()
        .map(|block| {
            (
                block.kind(),
                block.tool_call_id().cloned(),
                block.runner_kind(Role::Tool, false),
                block.runner_kind(Role::Tool, true),
                block.text_empty(),
            )
        })
        .collect();
    assert!(conversation.release(3, &[0]));
    let released = conversation.window_message(3).unwrap();
    assert!(released.is_released());
    assert!(released.held().is_none());
    assert_eq!(
        conversation.held_message(3),
        Err(super::NotHeld { ordinal: 3 })
    );
    let shape_after: Vec<_> = released
        .blocks()
        .map(|block| {
            (
                block.kind(),
                block.tool_call_id().cloned(),
                block.runner_kind(Role::Tool, false),
                block.runner_kind(Role::Tool, true),
                block.text_empty(),
            )
        })
        .collect();
    assert_eq!(shape_before, shape_after);
    assert!(matches!(
        released.blocks().next(),
        Some(super::BlockRef::Released(_))
    ));
    assert!(matches!(
        released.blocks().nth(1),
        Some(super::BlockRef::Held(_))
    ));
    let read = conversation
        .read_back(3, &records[1..4])
        .expect("the turn's records give the message back");
    assert_eq!(*read, *original);
    // Releasing again what is already released changes nothing.
    assert!(!conversation.release(3, &[0]));
}

/// The assistant message of a turn reads back from the same records as its results message.
#[test]
fn a_released_assistant_message_reads_back_from_its_turn() {
    let records = turn_records();
    let mut conversation = fold_at(&records);
    let original = std::sync::Arc::clone(conversation.held_message(2).unwrap().unwrap());
    assert!(conversation.release(2, &[0, 1]));
    let read = conversation.read_back(2, &records[1..4]).unwrap();
    assert_eq!(*read, *original);
}

/// The newest message stays held, since the fold may still change it, and so does a message
/// whose records aren't known, since nothing could read it back.
#[test]
fn the_newest_message_and_one_without_sources_are_never_released() {
    let mut conversation = fold_at(&turn_records());
    assert!(!conversation.release(4, &[0]));
    assert!(conversation.window_message(4).unwrap().held().is_some());
    let mut unknown = fold(&turn_records(), false).unwrap();
    assert!(!unknown.release(3, &[0]));
    assert!(unknown.window_message(3).unwrap().held().is_some());
}

/// Records that fold to another message than the one the window keeps are refused, never
/// served as the original.
#[test]
fn a_read_back_that_folds_to_another_message_is_refused() {
    let records = turn_records();
    let mut conversation = fold_at(&records);
    assert!(conversation.release(3, &[0]));
    let wrong = [
        records[1].clone(),
        reply("a", "another result"),
        records[3].clone(),
    ];
    assert_eq!(
        conversation.read_back(3, &wrong),
        Err(super::ReadBackError::Mismatch(3))
    );
}
