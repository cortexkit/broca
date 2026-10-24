use super::*;

#[tokio::test]
async fn await_route_drops_within_one_second_on_cancel_stop_and_restart_pause() {
    for mode in ["cancel", "stop", "pause"] {
        let mut rig = Rig::new("pass", "none", false, false);
        let token = CancellationToken::new();
        if mode == "cancel" {
            rig.control.cancel = Some(token.clone());
        } else {
            rig.control.stop_requested = Some(token.clone());
        }
        let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, mode == "pause");
        let interrupt = async {
            tokio::time::timeout(std::time::Duration::from_secs(1), async {
                let mut tick = tokio::time::interval(std::time::Duration::from_millis(2));
                while rig.questions.outstanding.load(Ordering::SeqCst) == 0 {
                    tick.tick().await;
                }
            })
            .await
            .expect("await must open");
            token.cancel();
        };
        let (result, ()) = tokio::time::timeout(std::time::Duration::from_secs(1), async {
            tokio::join!(batch, interrupt)
        })
        .await
        .expect("await route must close within one second");
        assert_eq!(rig.questions.outstanding.load(Ordering::SeqCst), 0);
        assert_eq!(rig.questions.awaits.load(Ordering::SeqCst), 1);
        assert_eq!(
            rig.questions.polls.load(Ordering::SeqCst),
            2,
            "only recovery lookups, no held-call feed polling"
        );
        assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
        if mode == "pause" {
            assert_eq!(result.interruption, Some(BatchInterruption::RestartPause));
            assert!(!rig
                .frames()
                .iter()
                .any(|frame| matches!(frame.record, WalRecord::ToolResult { .. })));
        }
        rig.finish().await;
    }
}

#[tokio::test]
async fn pending_restart_pause_gets_then_awaits_the_same_durable_card() {
    let mut rig = Rig::new("pass", "none", false, false);
    let token = CancellationToken::new();
    rig.control.stop_requested = Some(token.clone());
    let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, true);
    let stop = async {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(2));
        while rig.questions.outstanding.load(Ordering::SeqCst) == 0 {
            tick.tick().await;
        }
        token.cancel();
    };
    // Preparing and syncing a pause is not the one-second transport-close bound.
    // The separate route-drop test measures that bound after cancellation.
    let (paused, ()) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(batch, stop)
    })
    .await
    .unwrap();
    let held = &paused.open[0];
    let id = held.id.clone().unwrap();
    let validates = rig.hooks.validates.load(Ordering::SeqCst);
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: held.prepared.clone(),
            approval: None,
            elicitation_id: Some(id.clone()),
        }],
        ..Default::default()
    };
    rig.control.stop_requested = None;
    *rig.questions.answer.lock().unwrap() = "approve";
    rig.batch(&crate::AllowAll, vec![call("c")], Some(resume), true)
        .await;
    assert_eq!(rig.questions.keys.lock().unwrap().len(), 1);
    assert_eq!(rig.questions.gets.load(Ordering::SeqCst), 1);
    assert_eq!(rig.questions.awaits.load(Ordering::SeqCst), 2);
    assert_eq!(rig.hooks.validates.load(Ordering::SeqCst), validates + 1);
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        rig.questions.reports.lock().unwrap()[0],
        (id, ApprovalExecution::Executed)
    );
    rig.finish().await;
}

#[tokio::test]
async fn branch_c_cancel_before_intent_closes_approved_interrupted_with_zero_ack_and_send() {
    let mut rig = Rig::new("pass", "approve", false, false);
    let token = CancellationToken::new();
    rig.control.cancel = Some(token.clone());
    *rig.tools.interrupt_on_recheck.lock().unwrap() = Some(token);
    rig.batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::ApprovalRecorded {
            decision: broca_wal::DeferredDecision::Approved,
            ..
        }
    )));
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::ToolResult {
            outcome: DispatchOutcome::Interrupted,
            withdraw_elicitation: None,
            ..
        }
    )));
    assert!(!rig
        .frames()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::ToolDispatchIntent { .. })));
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    assert_eq!(rig.questions.ack_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        rig.questions.reports.lock().unwrap()[0].1,
        ApprovalExecution::ApprovedNotExecuted
    );
    rig.finish().await;
}

#[tokio::test]
async fn execution_reports_distinguish_provider_error_not_sent_unknown_and_prevalidation() {
    for (validation, outcome, expected) in [
        ("pass", "completed", ApprovalExecution::Executed),
        ("pass", "provider_error", ApprovalExecution::Executed),
        ("pass", "not_sent", ApprovalExecution::ApprovedNotExecuted),
        ("pass", "unknown", ApprovalExecution::OutcomeUnknown),
        ("deny", "completed", ApprovalExecution::ApprovedNotExecuted),
    ] {
        let rig = Rig::new(validation, "approve", false, false);
        *rig.tools.outcome.lock().unwrap() = outcome;
        rig.batch(&crate::AllowAll, vec![call("c")], None, false)
            .await;
        assert_eq!(
            rig.questions
                .reports
                .lock()
                .unwrap()
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>(),
            [expected],
            "{validation}/{outcome}"
        );
        if outcome == "not_sent" || validation == "deny" {
            assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
        }
        assert_eq!(rig.questions.ack_calls.load(Ordering::SeqCst), 0);
        rig.finish().await;
    }
    let rig = Rig::new("pass", "decline", false, false);
    rig.batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(rig.questions.reports.lock().unwrap().is_empty());
    rig.finish().await;
}

#[tokio::test]
async fn binding_mismatch_closes_unavailable_without_provider_request_or_report() {
    for answer in ["unreadable_binding_missing", "unreadable_binding_type"] {
        let rig = Rig::new("pass", answer, false, false);
        rig.batch(&crate::AllowAll, vec![call("c")], None, false)
            .await;
        assert!(rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::ApprovalRecorded {
                decision: broca_wal::DeferredDecision::Unavailable,
                ..
            }
        )));
        assert!(rig.questions.reports.lock().unwrap().is_empty());
        assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
        rig.finish().await;
    }
}

struct OwnerFeed {
    cards: Mutex<std::collections::HashMap<String, Value>>,
    held_key: Mutex<Option<String>>,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    ack_calls: AtomicUsize,
    requests: AtomicUsize,
}
#[async_trait::async_trait]
impl ElicitationPlane for OwnerFeed {
    async fn request(
        &self,
        _: &BatchCtx,
        key: &str,
        prepared: &WalRecord,
        _: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.held_key
            .lock()
            .unwrap()
            .get_or_insert_with(|| key.into());
        self.cards.lock().unwrap().entry(key.into()).or_insert_with(|| json!({"state":"pending","elicitation_id":format!("q:{key}"),"requester_ref":key,"binding":crate::consent::binding(prepared)}));
        Ok(ApprovalFiling {
            elicitation_id: format!("q:{key}"),
            args_hash: String::new(),
        })
    }
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let records = if cursor.is_none() {
            self.cards
                .lock()
                .unwrap()
                .values()
                .filter(|record| record["state"] == "answered")
                .cloned()
                .collect()
        } else {
            Vec::new()
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "owner".into(),
                seq: 1,
            },
        })
    }
    async fn r#await(
        &self,
        _: &BatchCtx,
        id: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let key = id.strip_prefix("q:").unwrap();
        let held = self.held_key.lock().unwrap().as_deref() == Some(key);
        if held {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let mut cards = self.cards.lock().unwrap();
        let record = cards.get_mut(key).unwrap();
        record["state"] = json!("answered");
        record["answered_choice_id"] = json!("approve");
        Ok(ApprovalQuestion {
            terminal: true,
            record: record.clone(),
        })
    }
    async fn ack(&self, _: &BatchCtx, _: &ApprovalCursor) -> Result<(), ApprovalRouteError> {
        self.ack_calls.fetch_add(1, Ordering::SeqCst);
        self.cards
            .lock()
            .unwrap()
            .retain(|_, record| record["state"] != "answered");
        Ok(())
    }
    async fn report_execution(
        &self,
        _: &BatchCtx,
        _: &str,
        _: ApprovalExecution,
    ) -> Result<(), ApprovalRouteError> {
        Ok(())
    }
}

#[tokio::test]
async fn branch_c_two_sessions_keep_owner_feed_answers_without_ack() {
    let feed = Arc::new(OwnerFeed {
        cards: Mutex::new(Default::default()),
        held_key: Mutex::new(None),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
        ack_calls: AtomicUsize::new(0),
        requests: AtomicUsize::new(0),
    });
    let mut a = Rig::new("pass", "none", false, false);
    a.control.elicitation_plane = Some(feed.clone());
    let mut b = Rig::new("pass", "none", false, false);
    assert_ne!(
        a.writer.lineage_id(),
        b.writer.lineage_id(),
        "separate sessions require distinct consent call-key lineages"
    );
    b.control.elicitation_plane = Some(feed.clone());
    let held = b.batch(&crate::AllowAll, vec![call("b")], None, false);
    let other = async {
        feed.entered.notified().await;
        a.batch(&crate::AllowAll, vec![call("a")], None, false)
            .await;
        assert_eq!(
            feed.cards
                .lock()
                .unwrap()
                .values()
                .filter(|record| record["state"] == "answered")
                .count(),
            1
        );
        feed.release.notify_one();
    };
    tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(held, other)
    })
    .await
    .unwrap();
    assert_eq!(a.tools.provider_requests.load(Ordering::SeqCst), 1);
    assert_eq!(b.tools.provider_requests.load(Ordering::SeqCst), 1);
    assert_eq!(feed.requests.load(Ordering::SeqCst), 2);
    assert_eq!(feed.cards.lock().unwrap().len(), 2);
    assert_eq!(feed.ack_calls.load(Ordering::SeqCst), 0);
    a.finish().await;
    b.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_report_table_includes_expired_metadata_none_and_route_failure() {
    for (mode, expected) in [
        ("completed", ApprovalExecution::Executed),
        ("not_sent", ApprovalExecution::ApprovedNotExecuted),
        ("unknown", ApprovalExecution::OutcomeUnknown),
    ] {
        let rig = expired_run("pass").await;
        *rig.questions.answer.lock().unwrap() = "approve";
        *rig.tools.outcome.lock().unwrap() = mode;
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        assert_eq!(
            rig.questions
                .reports
                .lock()
                .unwrap()
                .iter()
                .map(|(_, value)| *value)
                .collect::<Vec<_>>(),
            [expected]
        );
        if mode == "not_sent" {
            assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
        }
        rig.finish().await;
    }
    let rig = expired_run("pass").await;
    *rig.questions.answer.lock().unwrap() = "terminal";
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::LateApprovalRecorded {
            decision: broca_wal::LateDecision::None,
            ..
        }
    )));
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    assert!(rig.questions.reports.lock().unwrap().is_empty());
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
async fn approved_run() -> Rig {
    let mut rig = Rig::file_backed("pass");
    *rig.questions.answer.lock().unwrap() = "approve";
    rig.control.manifest = Some(Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("approve"), "refuse"),
            sub("pre_tool", Some("validate"), "pass"),
        ]],
        None,
        false,
    )));
    let mut req = hook_tests::request();
    req.session = rig.bind.clone();
    req.scope = Some(
        serde_json::from_value(
            json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1}),
        )
        .unwrap(),
    );
    let events = |content, reason| {
        vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: broca_protocol::MessageId("m".into()),
                    content,
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: reason,
                retries_used: 0,
            }),
        ]
    };
    let provider = crate::StubProvider::new(vec![
        events(
            vec![ContentBlock::ToolCall(call("c"))],
            FinishReason::ToolCalls,
        ),
        events(
            vec![ContentBlock::Text {
                text: "done".into(),
            }],
            FinishReason::Stop,
        ),
    ]);
    let (out, _rx) = mpsc::channel(1000);
    Box::pin(run_body_with_approval_history(
        &mut req,
        replay(&[]).unwrap(),
        None,
        false,
        false,
        false,
        &rig.control,
        &provider,
        &rig.tools,
        &crate::AllowAll,
        &rig.writer,
        || Ok(false),
        None,
        &out,
        None,
        Some(&rig.tools.store),
        None,
    ))
    .await
    .unwrap();
    rig
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn ordinary_crash_cuts_close_at_every_durable_step_and_owe_no_postresult_report() {
    let rig = approved_run().await;
    let bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
    let frames = rig.frames();
    let mut boundaries = vec![0usize];
    let mut offset = 0;
    while offset < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[offset..]) {
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => {
                offset += consumed;
                boundaries[0] = offset;
            }
            broca_wal::framing::DecodeStep::Record { consumed, .. } => {
                offset += consumed;
                boundaries.push(offset);
            }
            _ => panic!("complete fixture WAL"),
        }
    }
    let index = |predicate: fn(&WalRecord) -> bool| {
        frames
            .iter()
            .position(|frame| predicate(&frame.record))
            .unwrap()
    };
    let cuts = [
        (
            "deferred",
            index(|r| matches!(r, WalRecord::ToolCallDeferred { .. })),
        ),
        (
            "id",
            index(
                |r| matches!(r, WalRecord::AwaitingApproval { pending, .. } if pending.iter().any(|entry| entry.elicitation_id.is_some())),
            ),
        ),
        (
            "approved",
            index(|r| {
                matches!(
                    r,
                    WalRecord::ApprovalRecorded {
                        decision: broca_wal::DeferredDecision::Approved,
                        ..
                    }
                )
            }),
        ),
        (
            "intent",
            index(|r| {
                matches!(
                    r,
                    WalRecord::ToolDispatchIntent {
                        call_key: Some(_),
                        ..
                    }
                )
            }),
        ),
        (
            "result",
            index(|r| matches!(r, WalRecord::ToolResult { .. })),
        ),
        (
            "report_execution",
            index(|r| matches!(r, WalRecord::ToolResult { .. })),
        ),
    ];
    assert_eq!(
        rig.questions.reports.lock().unwrap().len(),
        1,
        "fault-free report ran after the result"
    );
    for (name, cut) in cuts {
        let cut_root = rig.root.join(format!("cut-{name}"));
        let store = crate::FileWalStore::new(&cut_root);
        let path = store.wal_path(&rig.bind);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, &bytes[..boundaries[cut + 1]]).unwrap();
        let writer = WalHandle::spawn(store.open(&rig.bind).unwrap(), 1).unwrap();
        // The body runs without admission, which is what seeds the writer's tag state.
        writer.seed_lineage_tags(Some(crate::tags::lineage_tags(
            frames[..=cut].iter().map(|frame| &frame.record),
        )));
        let questions = Arc::new(Questions {
            store: crate::FileWalStore::new(&cut_root),
            memory: None,
            completed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            lineage: writer.lineage_id().unwrap(),
            head: writer.durable_head(),
            keys: Mutex::new(Vec::new()),
            attempts: AtomicUsize::new(0),
            polls: AtomicUsize::new(0),
            answer_gate: Mutex::new(None),
            settlement_pages: AtomicUsize::new(0),
            awaits: AtomicUsize::new(0),
            gets: AtomicUsize::new(0),
            outstanding: AtomicUsize::new(0),
            ack_calls: AtomicUsize::new(0),
            reports: Mutex::new(Vec::new()),
            detaches: Mutex::new(Vec::new()),
            detach_answer: Mutex::new("pending"),
            transient: false,
            cursors: Mutex::new(Vec::new()),
            answer: Mutex::new("approve"),
            stop: None,
        });
        let tools = tools_for_store(crate::FileWalStore::new(&cut_root), rig.bind.clone());
        let control = RunControl {
            elicitation_plane: Some(questions.clone()),
            hook_plane: Some(rig.hooks.clone()),
            ..Default::default()
        };
        let prior = replay(
            &frames[..=cut]
                .iter()
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut req = hook_tests::request();
        req.session = rig.bind.clone();
        let provider = crate::StubProvider::new(vec![vec![
            NormalizedEvent::Control(ProviderControl::AssistantMessage {
                message: AssistantMessage {
                    message_id: broca_protocol::MessageId("after-cut".into()),
                    content: vec![ContentBlock::Text {
                        text: "done".into(),
                    }],
                },
            }),
            NormalizedEvent::Control(ProviderControl::Finished {
                usage: Usage::default(),
                finish_reason: FinishReason::Stop,
                retries_used: 0,
            }),
        ]]);
        let (out, _rx) = mpsc::channel(1000);
        let result = tokio::time::timeout(
            std::time::Duration::from_secs(2),
            Box::pin(run_body_with_approval_history(
                &mut req,
                prior,
                None,
                true,
                false,
                false,
                &control,
                &provider,
                &tools,
                &crate::AllowAll,
                &writer,
                || Ok(false),
                None,
                &out,
                None,
                Some(&store),
                None,
            )),
        )
        .await
        .unwrap()
        .unwrap();
        assert_eq!(
            result.reason,
            if matches!(name, "result" | "report_execution") {
                RunFinishReason::Completed
            } else {
                RunFinishReason::Interrupted
            },
            "{name}"
        );
        assert_eq!(
            tools.provider_requests.load(Ordering::SeqCst),
            0,
            "{name}: recovered calls never dispatch"
        );
        assert_eq!(questions.awaits.load(Ordering::SeqCst), 0);
        assert_eq!(questions.gets.load(Ordering::SeqCst), 0);
        let records = store.read_records(&rig.bind).unwrap();
        let terminals: Vec<_> = records
            .iter()
            .filter_map(|frame| match &frame.record {
                WalRecord::ToolResult {
                    outcome,
                    withdraw_elicitation,
                    ..
                } => Some((*outcome, withdraw_elicitation.is_some())),
                _ => None,
            })
            .collect();
        assert_eq!(terminals.len(), 1, "{name}");
        match name {
            "deferred" | "id" => {
                assert_eq!(terminals[0], (DispatchOutcome::Interrupted, true));
                assert!(questions.reports.lock().unwrap().is_empty());
            }
            "approved" => {
                assert_eq!(terminals[0], (DispatchOutcome::Interrupted, false));
                assert_eq!(
                    questions.reports.lock().unwrap()[0].1,
                    ApprovalExecution::ApprovedNotExecuted
                );
            }
            "intent" => {
                assert_eq!(terminals[0], (DispatchOutcome::Unknown, false));
                assert_eq!(
                    questions.reports.lock().unwrap()[0].1,
                    ApprovalExecution::OutcomeUnknown
                );
            }
            _ => {
                assert_eq!(terminals[0], (DispatchOutcome::Completed, false));
                assert!(
                    questions.reports.lock().unwrap().is_empty(),
                    "post-result crash owes no report"
                );
            }
        }
        writer.shutdown().await.unwrap();
    }
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn one_held_and_one_expired_call_use_one_await_one_page_and_drop_settled_wait() {
    let rig = expired_run("pass").await;
    let prepared = rig
        .frames()
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::ToolCallDeferred { .. } => Some(frame.record.clone()),
            _ => None,
        })
        .unwrap();
    let mut prepared = prepared;
    let WalRecord::ToolCallDeferred { call_id, ask, .. } = &mut prepared else {
        unreachable!()
    };
    *call_id = ToolCallId("held".into());
    ask.expires_at_ms += 60_000;
    let seq = rig.writer.record(prepared.clone()).into_result().unwrap();
    let key = crate::context::call_key(rig.writer.lineage_id().unwrap(), seq);
    let id = format!("q:{key}");
    rig.writer.record(WalRecord::AwaitingApproval {
        batch_id: BatchId("r-b1".into()),
        pending: vec![broca_wal::AwaitingApprovalEntry {
            call_key: key.clone(),
            call_id: ToolCallId("held".into()),
            elicitation_id: Some(id.clone()),
            expires_at_ms: rig.hooks.expires + 60_000,
        }],
    });
    // Freeze a new held call beyond the first card's expired deadline.
    let WalRecord::ToolCallDeferred { ask, .. } = &prepared else {
        unreachable!()
    };
    assert!(ask.expires_at_ms > rig.control.clock.as_ref().unwrap().now_ms());
    rig.writer.sync().await.unwrap();
    rig.questions.keys.lock().unwrap().push(key);
    *rig.questions.answer.lock().unwrap() = "feed_only";
    let polls = rig.questions.polls.load(Ordering::SeqCst);
    let pages = rig.questions.settlement_pages.load(Ordering::SeqCst);
    let awaits = rig.questions.awaits.load(Ordering::SeqCst);
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: Arc::new(prepared),
            approval: None,
            elicitation_id: Some(id),
        }],
        ..Default::default()
    };
    rig.batch(&crate::AllowAll, vec![call("held")], Some(resume), false)
        .await;
    // One empty reconciliation read precedes get. The live wait then reads one
    // nonempty page plus its empty continuation head. Each authorization then
    // reads that retained head once more before minting its capability.
    assert_eq!(rig.questions.polls.load(Ordering::SeqCst), polls + 5);
    assert_eq!(
        rig.questions.settlement_pages.load(Ordering::SeqCst),
        pages + 1
    );
    assert_eq!(rig.questions.awaits.load(Ordering::SeqCst), awaits + 1);
    assert_eq!(rig.questions.outstanding.load(Ordering::SeqCst), 0);
    assert_eq!(rig.frames().iter().filter(|frame| matches!(&frame.record, WalRecord::ApprovalRecorded { call_id, .. } if call_id.0 == "held")).count(), 1);
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 1);
    rig.finish().await;
}

#[tokio::test]
async fn awaiting_marker_names_deferred_call_before_lookup_or_filing_finishes() {
    struct HeldLookup(tokio::sync::Notify);
    #[async_trait::async_trait]
    impl ElicitationPlane for HeldLookup {
        async fn answers(
            &self,
            _: &BatchCtx,
            _: Option<&ApprovalCursor>,
        ) -> Result<ApprovalPage, ApprovalRouteError> {
            self.0.notify_one();
            std::future::pending().await
        }
    }
    let lookup = Arc::new(HeldLookup(tokio::sync::Notify::new()));
    let mut rig = Rig::new("pass", "none", false, false);
    rig.control.elicitation_plane = Some(lookup.clone());
    let stop = CancellationToken::new();
    rig.control.stop_requested = Some(stop.clone());
    let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, true);
    let interrupt = async {
        lookup.0.notified().await;
        stop.cancel();
    };
    let (paused, ()) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(batch, interrupt)
    })
    .await
    .unwrap();
    assert_eq!(paused.interruption, Some(BatchInterruption::RestartPause));
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::AwaitingApproval { pending, .. } if pending.len() == 1 && pending[0].elicitation_id.is_none() && pending[0].call_id.0 == "c")));
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    rig.finish().await;
}
