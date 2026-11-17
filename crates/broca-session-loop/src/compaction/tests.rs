use broca_core::compaction_v2::received::ReceivedView;
use broca_protocol::BindIdentity;
use broca_session_store::fold::operations::{AppliedView, Operations, ViewSlot};
use broca_session_store::record::{Record, ViewApplied};
use cortexkit_role_compaction::view::View;

/// A view sealed with build metadata, which its checksum covers.
fn built(version: u64, through: &str, new_signed: Option<&str>) -> ReceivedView {
    View::seal_with_policy(
        version,
        vec![],
        Some(through.into()),
        new_signed.map(str::to_owned),
    )
    .into()
}

/// A session whose step applied a view one provider handed over, and which then accepted a
/// newer one from another: `restore` rebuilds both views with their build metadata (a view
/// rebuilt without it would fail its checksum) and names who handed each over, and the copies a
/// seal carries restore to the same state.
#[test]
fn views_restore_with_their_build_metadata_and_applier_across_a_seal() {
    let bind = BindIdentity::new("/project", "test", "s");
    let applied = built(1, "m1", Some("strip"));
    let latest = built(2, "m2", None);
    let records = vec![
        (
            1,
            super::view_record(None, &applied, Some("ck-mc")).unwrap(),
        ),
        (
            2,
            Record::ViewApplied(ViewApplied {
                step_id: 1,
                version: 1,
                checksum: applied.view().checksum.to_string(),
                head_kept: None,
            }),
        ),
        (
            3,
            super::view_record(Some(&applied), &latest, Some("another")).unwrap(),
        ),
    ];
    let operations = Operations {
        view_accepted: Some(ViewSlot {
            version: 2,
            checksum: latest.view().checksum.to_string(),
        }),
        view_applied: Some(AppliedView {
            step_id: 1,
            view: ViewSlot {
                version: 1,
                checksum: applied.view().checksum.to_string(),
            },
        }),
        ..Default::default()
    };
    let state = super::restore(&records, &bind, &operations).unwrap();
    assert_eq!(state.applied(), Some(applied.view()));
    assert_eq!(state.latest(), Some(latest.view()));
    assert_eq!(state.applied_by(), Some("ck-mc"));
    assert_eq!(state.accepted_by(), Some("another"));
    let carried: Vec<(u64, Record)> = super::carried_views(&state, &operations)
        .unwrap()
        .into_iter()
        .enumerate()
        .map(|(seq, record)| (seq as u64 + 1, record))
        .collect();
    let stored: Vec<_> = carried
        .iter()
        .filter_map(|(_, record)| match record {
            Record::View(view) => Some((
                view.version,
                view.built_through.clone(),
                view.new_signed.clone(),
                view.applied_by.clone(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(
        stored,
        [
            (
                1,
                Some("m1".into()),
                Some("strip".into()),
                Some("ck-mc".into())
            ),
            (2, Some("m2".into()), None, Some("another".into())),
        ]
    );
    assert_eq!(
        super::restore(&carried, &bind, &operations).unwrap(),
        state,
        "the copies a seal carries restore the same state"
    );
}

/// One run's records: its input, then a tool turn per call id (the call, then its result).
fn run_with_tool_turns(calls: &[&str]) -> Vec<Record> {
    use broca_protocol::content::{ContentBlock, Message, Role, ToolCall, ToolOutput, ToolResult};
    use broca_protocol::ids::ToolCallId;
    use broca_session_store::record::{Assistant, Input, Placement, ToolReply};
    use std::sync::Arc;
    let mut records = vec![Record::Input(Input {
        run_id: "run".into(),
        placement: Placement::RunInput,
        message: Arc::new(Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "input".into(),
            }],
        )),
        written_at_ms: Some(1),
        submission_id: None,
        notice: None,
        tags_v1: None,
        receipt_seq: None,
        media: Vec::new(),
        pruned: Vec::new(),
    })];
    for (step, call) in calls.iter().enumerate() {
        records.push(Record::Assistant(Assistant {
            run_id: "run".into(),
            step_id: step as u64 + 1,
            provider_message_id: broca_protocol::MessageId(format!("provider-{call}")),
            message: Arc::new(Message::new(
                Role::Assistant,
                vec![ContentBlock::ToolCall(ToolCall {
                    thought_signature: None,
                    tool_call_id: ToolCallId((*call).into()),
                    tool_name: "read".into(),
                    input: serde_json::json!({"path": call}).into(),
                })],
            )),
            usage: None,
            written_at_ms: Some(2),
            last_content_at_ms: None,
            tags_v1: None,
            budget_finalize: None,
            pruned: Vec::new(),
        }));
        records.push(Record::ToolReply(ToolReply {
            run_id: "run".into(),
            batch_id: None,
            result: ToolResult {
                tool_call_id: ToolCallId((*call).into()),
                output: ToolOutput::Text {
                    text: format!("result of {call}").into(),
                },
                is_error: false,
            },
            finished_at_ms: Some(3),
            tags_v1: None,
            media: Vec::new(),
        }));
    }
    records
}

/// A cut whose first kept message is a tool turn's results seals from the assistant message
/// whose call they answer, as the store's seal does. The repetition counters taken over the
/// run's history below the seal then never stop between a call and its result: taken at the
/// results instead, they refused, and every run of the session failed with "a compaction cut
/// between a tool call and its result".
#[test]
fn a_cut_keeping_from_tool_results_seals_from_the_call_that_asked_for_them() {
    use broca_session_store::fold::conversation::Conversation;
    let mut conversation = Conversation::new(false);
    let mut operations = Operations::default();
    for (seq, record) in run_with_tool_turns(&["one", "two"]).iter().enumerate() {
        broca_session_store::session::apply(
            &mut conversation,
            &mut operations,
            seq as u64 + 1,
            record,
        )
        .unwrap();
    }
    // Input 1, then call 2 and its results 3, then call 4 and its results 5.
    assert_eq!(conversation.placed, 5);
    assert!(matches!(
        super::spanning_repetition(&conversation, 5, None),
        Err(crate::LoopError::Unsupported(_))
    ));
    let start = super::turn_start(&conversation, 5);
    assert_eq!(start, 4, "results keep from the call that asked for them");
    assert!(super::spanning_repetition(&conversation, start, None)
        .unwrap()
        .is_some());
    // A cut already at a turn's start stays where it is.
    assert_eq!(super::turn_start(&conversation, 4), 4);
    assert_eq!(super::turn_start(&conversation, 2), 2);
}
