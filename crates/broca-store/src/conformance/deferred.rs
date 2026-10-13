use super::*;

/// A literal three-call transcript. Declined and expired calls have no ToolResult record;
/// the unanswered third call is withdrawn. Prepared keys are their frame positions.
pub(crate) fn records(session: &BindIdentity) -> Vec<FramedRecord> {
    let mut records = vec![run_started(
        1,
        &session.project_root.to_string_lossy(),
        session,
        "approve",
        ModelHandle::new("stub", "m"),
    )];
    let mut turn = tool_turn(2, 1);
    if let WalRecord::ModelStepFinished {
        assistant_message, ..
    } = &mut turn[0].record
    {
        std::sync::Arc::make_mut(assistant_message).content = ["declined", "expired", "withdrawn"]
            .iter()
            .map(|id| {
                ContentBlock::ToolCall(ToolCall {
                    tool_call_id: ToolCallId((*id).into()),
                    tool_name: "edit".into(),
                    input: serde_json::json!({"f":"a"}),
                    thought_signature: None,
                })
            })
            .collect();
    }
    if let WalRecord::ToolBatchStarted { planned_calls, .. } = &mut turn[1].record {
        *planned_calls = ["declined", "expired", "withdrawn"]
            .iter()
            .map(|id| ToolCallId((*id).into()))
            .collect();
    }
    records.extend(turn.into_iter().take(2));
    for (seq, id) in [(4, "declined"), (5, "expired"), (6, "withdrawn")] {
        records.push(framed(seq, serde_json::from_value(serde_json::json!({
            "type":"tool_call_deferred", "batch_id":"b1", "call_id":id, "executor":"broca", "tool":"edit",
            "args_jcs":"{\"f\":\"a\"}", "args_hash":"910b42ae2f4a973d3af5f056c18c62742efb4f7c4af344444e47e071411d6e57",
            "scope":{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"head-1","scope_epoch":3},
            "ask":{"prompt":"Edit a?","expires_at_ms":1790000723000u64,"on_expiry":"deny","material_damage":true,"late_execution":"notify_only"}
        })).unwrap()));
    }
    for (seq, anchor, id, decision, text) in [
        (
            7,
            4,
            "declined",
            broca_wal::DeferredDecision::Declined,
            "pre_tool_declined",
        ),
        (
            8,
            5,
            "expired",
            broca_wal::DeferredDecision::Expired,
            "pre_tool_expired",
        ),
    ] {
        records.push(framed(
            seq,
            WalRecord::ApprovalRecorded {
                call_key: format!("5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:{anchor}"),
                call_id: ToolCallId(id.into()),
                elicitation_id: Some(format!("q-{id}")),
                decision,
                approval_digest: None,
                closing_result: Some(
                    ToolResult {
                        tool_call_id: ToolCallId(id.into()),
                        output: ToolOutput::Error {
                            message: text.into(),
                        },
                        is_error: true,
                    }
                    .into(),
                ),
            },
        ));
    }
    records.push(framed(
        9,
        WalRecord::ToolResult {
            batch_id: BatchId("b1".into()),
            result: broca_core::not_dispatched_result(&ToolCallId("withdrawn".into())).into(),
            outcome: DispatchOutcome::Interrupted,
            reason: None,
            hooks: None,
            provider_held: None,
            withdraw_elicitation: Some(broca_wal::WithdrawElicitation {
                call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:6".into(),
                elicitation_id: None,
            }),
        },
    ));
    records.push(framed(
        10,
        WalRecord::ToolBatchFinished {
            batch_id: BatchId("b1".into()),
        },
    ));
    records.push(framed(11, WalRecord::TurnFinished { step_id: StepId(1) }));
    records.push(framed(
        12,
        WalRecord::RunFinished {
            reason: RunFinishReason::Cancelled,
            ts_ms: None,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    records
}

fn index_run<S: RunIndex>(store: &S, session: &BindIdentity) {
    store
        .index_run_active(&RunIndexRow {
            run_id: session.project_root.to_string_lossy().into_owned(),
            session: session.clone(),
            submission_id: None,
            request_identity_digest: None,
            state: RunIndexState::Active,
            usage: None,
            final_step_finish_reason: None,
            retries_used: 0,
            title: None,
            withdrawals_owed: false,
        })
        .unwrap();
}

pub fn deferred_projection_matches_replay_golden<
    S: SessionStore + RunIndex + PageUsageConformance,
>(
    store: S,
) {
    let session = session("deferred-golden");
    let run_id = session.project_root.to_string_lossy().into_owned();
    let mut records = records(&session);
    index_run(&store, &session);
    let watermark = Persister::project(&store, &session, &records).unwrap();
    assert_eq!(watermark, 11);
    assert!(
        store.lookup_run(&run_id).unwrap().unwrap().withdrawals_owed,
        "projection commit must set debt before a terminal index write"
    );
    store
        .index_run_terminal(&run_id, RunIndexState::Cancelled, None, None, 0)
        .unwrap();
    let flagged = store.runs_with_withdrawals_owed().unwrap();
    assert!(flagged
        .iter()
        .any(|row| row.run_id.0 == run_id && row.session == session));
    let prefix = store.read_prefix_with_watermark(&session).unwrap();
    let pure = replay(&records_only(&records)).unwrap();
    assert_eq!(
        serde_json::to_vec(&prefix.prefix).unwrap(),
        serde_json::to_vec(&pure.prompt).unwrap()
    );
    let outputs: Vec<_> = prefix
        .prefix
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(serde_json::to_value(result).unwrap()),
            _ => None,
        })
        .collect();
    assert_eq!(
        serde_json::Value::Array(outputs),
        serde_json::json!([
            {"tool_call_id":"declined","output":{"kind":"error","message":"pre_tool_declined"},"is_error":true},
            {"tool_call_id":"expired","output":{"kind":"error","message":"pre_tool_expired"},"is_error":true},
            {"tool_call_id":"withdrawn","output":{"kind":"error","message":"tool not run: the run was interrupted before this call was dispatched"},"is_error":true}
        ])
    );
    let control_results: Vec<_> = project_control(&records)
        .into_iter()
        .filter_map(|(_, unit)| match unit {
            broca_protocol::caller::ControlUnit::ToolResult { result } => {
                Some(result.tool_call_id.0)
            }
            _ => None,
        })
        .collect();
    assert_eq!(control_results, ["declined", "expired", "withdrawn"]);
    let snapshot = store
        .read_page_with_watermark(&session, Some(0), 500, broca_core::ReadView::Raw)
        .unwrap();
    let delta =
        broca_core::replay_projection_tail(&broca_core::ProjectionSeed::empty(), &records[..11])
            .unwrap();
    assert_eq!(delta.deferred_calls, snapshot.deferred_calls);
    let merged = read_session_page(&snapshot, &records[11..], Some(0), Some(500)).unwrap();
    let full =
        read_session_page(&StorePageSnapshot::empty(), &records, Some(0), Some(500)).unwrap();
    assert_eq!(merged, full);
    assert!(merged.withdrawals_owed);
    let view = merged.deferred_calls.unwrap();
    assert_eq!(view.calls.len(), 3);
    assert!(view
        .calls
        .values()
        .all(|call| call.state == broca_core::OpenCallState::Terminated));
    assert!(merged.late_approvals.is_empty());

    records.push(framed(
        13,
        WalRecord::LateApprovalRecorded {
            call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:5".into(),
            elicitation_id: "q-expired".into(),
            decision: broca_wal::LateDecision::Approved,
            approval_digest: Some("digest".into()),
            late_execution: Some(broca_wal::LateExecution::Execute),
        },
    ));
    records.push(framed(
        14,
        WalRecord::QuestionWithdrawalAcknowledged {
            call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:6".into(),
            elicitation_id: Some("q-withdrawn".into()),
            answer: broca_wal::WithdrawalAnswer::Acknowledged,
            reason: None,
        },
    ));
    // The actor clears only after its complete WAL view has no debt. A later projection of
    // the same WAL must not resurrect an obligation from the old withdrawn prefix.
    let acknowledged = read_session_page(&snapshot, &records[11..], Some(0), Some(500)).unwrap();
    assert!(!acknowledged.withdrawals_owed);
    assert_eq!(acknowledged.late_approvals.len(), 1);
    assert_eq!(
        acknowledged.late_approvals[0].call_key,
        "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:5"
    );
    assert_eq!(
        acknowledged.messages, full.messages,
        "late records never change the finished prompt"
    );
    store.index_run_withdrawals_owed(&run_id, false).unwrap();
    records.push(run_started(
        15,
        "next",
        &session,
        "next",
        ModelHandle::new("stub", "m"),
    ));
    records.extend(text_turn(16, 1, "next reply"));
    records.push(framed(
        18,
        WalRecord::RunFinished {
            reason: RunFinishReason::Completed,
            ts_ms: None,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
    ));
    let advanced = Persister::project(&store, &session, &records[11..]).unwrap();
    // Discard only the derived projection, preserving the actor-cleared index flag. Full-WAL
    // re-projection must fold acknowledgements before deciding whether debt should be flagged.
    store.reset_projection_for_conformance(&session).unwrap();
    assert_eq!(
        Persister::project(&store, &session, &records).unwrap(),
        advanced
    );
    assert!(advanced > watermark);
    assert!(!store.lookup_run(&run_id).unwrap().unwrap().withdrawals_owed);
    assert!(!store
        .runs_with_withdrawals_owed()
        .unwrap()
        .iter()
        .any(|row| row.run_id.0 == run_id && row.session == session));
    let projected = store
        .read_page_with_watermark(&session, Some(0), 500, broca_core::ReadView::Raw)
        .unwrap();
    let tail: Vec<_> = records
        .iter()
        .filter(|frame| frame.seq > advanced)
        .cloned()
        .collect();
    let page = read_session_page(&projected, &tail, Some(0), Some(500)).unwrap();
    let full =
        read_session_page(&StorePageSnapshot::empty(), &records, Some(0), Some(500)).unwrap();
    assert_eq!(page, full);
    assert_eq!(page.late_approvals.len(), 1);
    assert!(!page.withdrawals_owed);
    assert_eq!(
        store
            .projection_seed(&session)
            .unwrap()
            .unwrap()
            .deferred_calls,
        projected.deferred_calls
    );
    let prompt = page.messages;
    records.push(framed(
        19,
        WalRecord::LateDispatchIntent {
            call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:5".into(),
            approval_digest: "digest".into(),
            batch_id: BatchId("late:expired".into()),
        },
    ));
    let page = read_session_page(&projected, &records[17..], Some(0), Some(500)).unwrap();
    assert_eq!(page.messages, prompt);
    assert_eq!(
        page.late_approvals[0].outcome,
        Some(broca_wal::LateOutcome::Unknown)
    );
    records.push(framed(
        20,
        WalRecord::LateResult {
            call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:5".into(),
            outcome: broca_wal::LateOutcome::NotSent,
            result: ToolResult {
                tool_call_id: ToolCallId("expired".into()),
                is_error: true,
                output: ToolOutput::Error {
                    message: "pre_tool_unavailable".into(),
                },
            }
            .into(),
        },
    ));
    let page = read_session_page(&projected, &records[17..], Some(0), Some(500)).unwrap();
    assert_eq!(page.messages, prompt);
    assert_eq!(
        page.late_approvals[0].outcome,
        Some(broca_wal::LateOutcome::NotSent)
    );
    assert_eq!(
        page.late_approvals[0]
            .result
            .as_ref()
            .unwrap()
            .tool_call_id
            .0,
        "expired"
    );
}

pub fn withdrawal_projection_excludes_answered_calls<S: SessionStore + RunIndex>(store: S) {
    let session = session("answered-withdrawal");
    let run_id = session.project_root.to_string_lossy().into_owned();
    let mut records = records(&session);
    // A consumed approval closed with the reader's not-dispatched result is not an unanswered
    // question, even though its terminal resembles a withdrawal.
    records.insert(
        8,
        framed(
            9,
            WalRecord::ApprovalRecorded {
                call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:6".into(),
                call_id: ToolCallId("withdrawn".into()),
                elicitation_id: Some("q-withdrawn".into()),
                decision: broca_wal::DeferredDecision::Approved,
                approval_digest: Some("digest".into()),
                closing_result: None,
            },
        ),
    );
    for (i, frame) in records.iter_mut().enumerate() {
        frame.seq = i as u64 + 1;
    }
    if let WalRecord::ToolResult {
        withdraw_elicitation,
        ..
    } = &mut records[9].record
    {
        *withdraw_elicitation = None;
    }
    index_run(&store, &session);
    Persister::project(&store, &session, &records).unwrap();
    assert!(!store.lookup_run(&run_id).unwrap().unwrap().withdrawals_owed);
    assert!(!store
        .runs_with_withdrawals_owed()
        .unwrap()
        .iter()
        .any(|row| row.run_id.0 == run_id && row.session == session));
    let snapshot = store
        .read_page_with_watermark(&session, Some(0), 500, broca_core::ReadView::Raw)
        .unwrap();
    let tail: Vec<_> = records
        .iter()
        .filter(|frame| frame.seq > snapshot.watermark_seq)
        .cloned()
        .collect();
    assert!(
        !read_session_page(&snapshot, &tail, Some(0), Some(500))
            .unwrap()
            .withdrawals_owed
    );
}

pub const RETAINED_DEFERRED_MARKER: &str = "retained-deferred-text-should-stay-in-wal-873bc1";

/// The normal approval summary retains arguments, question text and late results.
/// Retained sessions keep that summary in the WAL but still persist debt discovery.
pub fn retained_deferred_projection_keeps_only_withdrawal_flags<
    S: SessionStore + RunIndex + PageUsageConformance,
>(
    store: S,
) {
    for retained in [true, false] {
        let session = session(if retained {
            "retained-deferred"
        } else {
            "forever-deferred"
        });
        let marker = if retained {
            RETAINED_DEFERRED_MARKER
        } else {
            "forever-deferred-content-control"
        };
        let run_id = session.project_root.to_string_lossy().into_owned();
        let mut frames = records(&session);
        if let WalRecord::RunStarted {
            retention,
            title,
            input,
            ..
        } = &mut frames[0].record
        {
            *retention = retained.then_some(3600);
            *title = Some(marker.into());
            std::sync::Arc::make_mut(input)[0].content = vec![ContentBlock::Text {
                text: marker.into(),
            }];
        }
        for frame in &mut frames {
            if let WalRecord::ToolCallDeferred { args_jcs, ask, .. } = &mut frame.record {
                *args_jcs = serde_json::json!({"text": marker}).to_string();
                ask.prompt = marker.into();
            }
        }
        frames.push(framed(
            13,
            WalRecord::LateResult {
                call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:5".into(),
                outcome: broca_wal::LateOutcome::Completed,
                result: ToolResult {
                    tool_call_id: ToolCallId("expired".into()),
                    output: ToolOutput::Text {
                        text: marker.into(),
                    },
                    is_error: false,
                }
                .into(),
            },
        ));
        index_run(&store, &session);
        let watermark = Persister::project(&store, &session, &frames).unwrap();
        assert_eq!(watermark, if retained { 0 } else { 11 });
        assert!(store.lookup_run(&run_id).unwrap().unwrap().withdrawals_owed);
        if retained {
            assert!(store.read_admission_snapshot(&session).unwrap().is_none());
            let page = store
                .read_page_with_watermark(&session, Some(0), 500, broca_core::ReadView::Raw)
                .unwrap();
            assert!(page.rows.is_empty());
            assert!(page.last_title.is_none());
            assert!(page.deferred_calls.is_none());
            assert!(!format!("{page:?}").contains(marker));
            store.index_run_withdrawals_owed(&run_id, false).unwrap();
            let metadata = broca_core::projection_plan::retained_withdrawal_projection(
                None, &frames, None, None,
            )
            .unwrap()
            .unwrap();
            assert!(!format!("{metadata:?}").contains(marker));
            Persister::project_retention_metadata(&store, &session, &metadata).unwrap();
            assert!(
                store.lookup_run(&run_id).unwrap().unwrap().withdrawals_owed,
                "the content-free core seam must also persist debt"
            );
        } else {
            let seed = Persister::projection_seed(&store, &session)
                .unwrap()
                .unwrap();
            assert!(serde_json::to_string(&seed.deferred_calls)
                .unwrap()
                .contains(marker));
            assert_eq!(
                store
                    .read_page_with_watermark(&session, Some(0), 500, broca_core::ReadView::Raw)
                    .unwrap()
                    .last_title
                    .as_deref(),
                Some(marker)
            );
        }
        frames.push(framed(
            14,
            WalRecord::QuestionWithdrawalAcknowledged {
                call_key: "5a5a5a5a-5a5a-5a5a-5a5a-5a5a5a5a5a5a:6".into(),
                elicitation_id: None,
                answer: broca_wal::WithdrawalAnswer::Acknowledged,
                reason: None,
            },
        ));
        store.index_run_withdrawals_owed(&run_id, false).unwrap();
        assert_eq!(
            Persister::project(&store, &session, &frames).unwrap(),
            watermark
        );
        assert!(
            !store.lookup_run(&run_id).unwrap().unwrap().withdrawals_owed,
            "acknowledged debt must not be resurrected by prefix projection"
        );
    }
}
