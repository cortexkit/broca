use super::*;

struct ProvisionalCards {
    calls: AtomicUsize,
    pause: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    filed: Mutex<Option<(String, Value)>>,
    ops: Mutex<Vec<&'static str>>,
}
impl ProvisionalCards {
    fn new(pause: bool) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            pause: std::sync::atomic::AtomicBool::new(pause),
            entered: tokio::sync::Notify::new(),
            filed: Mutex::new(None),
            ops: Mutex::new(Vec::new()),
        }
    }
    fn answer(&self, id: &str, choice: &str) -> Value {
        let (key, binding) = self.filed.lock().unwrap().clone().unwrap();
        json!({"requester_ref":key,"elicitation_id":id,"state":"answered","answered_choice_id":choice,"binding":binding})
    }
}
#[async_trait::async_trait]
impl ElicitationPlane for ProvisionalCards {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        self.ops.lock().unwrap().push("answers");
        let n = self.calls.fetch_add(1, Ordering::SeqCst);
        if n == 1 {
            self.entered.notify_one();
            if self.pause.load(Ordering::SeqCst) {
                return std::future::pending().await;
            }
            return Err(crate::consent::refusal("feed_reloading", true));
        }
        let records = if n == 0 || cursor.is_some() {
            Vec::new()
        } else {
            vec![self.answer("old", "deny"), self.answer("new", "approve")]
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "owner".into(),
                seq: n as u64 + 1,
            },
        })
    }
    async fn request(
        &self,
        _: &BatchCtx,
        key: &str,
        record: &WalRecord,
        _: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        self.ops.lock().unwrap().push("request");
        *self.filed.lock().unwrap() = Some((key.into(), crate::consent::binding(record)));
        Ok(ApprovalFiling {
            elicitation_id: "new".into(),
            args_hash: String::new(),
        })
    }
    async fn get(&self, _: &BatchCtx, _: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.ops.lock().unwrap().push("get");
        Ok(ApprovalQuestion {
            terminal: true,
            record: self.answer("new", "approve"),
        })
    }
    async fn r#await(
        &self,
        _: &BatchCtx,
        _: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.ops.lock().unwrap().push("await");
        Ok(ApprovalQuestion {
            terminal: true,
            record: self.answer("new", "approve"),
        })
    }
    async fn withdraw(&self, _: &BatchCtx, _: &str) -> ApprovalWithdrawal {
        ApprovalWithdrawal::Acknowledged
    }
}

#[tokio::test]
async fn provisional_id_retry_reconciles_conflicting_settlements_before_get() {
    let mut rig = Rig::new("pass", "none", false, false);
    let cards = Arc::new(ProvisionalCards::new(false));
    rig.control.elicitation_plane = Some(cards.clone());
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], None, false)
        .await;
    assert!(
        result.results[0].is_error,
        "a provisional duplicate's approval must not override the older denial"
    );
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    assert!(!cards.ops.lock().unwrap().contains(&"get"));
    assert!(
        matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions"))
    );
    assert!(!rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::ApprovalRecorded {
            decision: broca_wal::DeferredDecision::Approved,
            ..
        }
    )));
    rig.finish().await;
}

#[tokio::test]
async fn provisional_id_resume_reconciles_conflicting_settlements_before_get() {
    let mut rig = Rig::new("pass", "none", false, false);
    let cards = Arc::new(ProvisionalCards::new(true));
    rig.control.elicitation_plane = Some(cards.clone());
    let stop = CancellationToken::new();
    rig.control.stop_requested = Some(stop.clone());
    let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, true);
    let interrupt = async {
        cards.entered.notified().await;
        stop.cancel();
    };
    let (paused, ()) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(batch, interrupt)
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "pause did not reach reconciliation stop: {:?}",
            cards.ops.lock().unwrap()
        )
    });
    assert_eq!(paused.interruption, Some(BatchInterruption::RestartPause));
    assert_eq!(paused.open[0].id.as_deref(), Some("new"));
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::AwaitingApproval { pending, .. } if pending.iter().any(|entry| entry.elicitation_id.as_deref() == Some("new")))));
    cards.pause.store(false, Ordering::SeqCst);
    cards.ops.lock().unwrap().clear();
    rig.control.stop_requested = None;
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: paused.open[0].prepared.clone(),
            approval: None,
            elicitation_id: paused.open[0].id.clone(),
        }],
    };
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), true)
        .await;
    assert!(
        result.results[0].is_error,
        "a resumed provisional id must reconcile before reading the duplicate"
    );
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    assert_eq!(cards.ops.lock().unwrap().first(), Some(&"answers"));
    assert!(
        matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions"))
    );
    assert!(!cards.ops.lock().unwrap().contains(&"get"));
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
struct ConflictingFeed {
    key: String,
    prepared: WalRecord,
    gets: AtomicUsize,
    entered: std::sync::atomic::AtomicBool,
    split: bool,
}
#[cfg(feature = "test-clock")]
impl ConflictingFeed {
    fn record(&self, id: &str, choice: &str) -> Value {
        json!({"state":"answered","requester_ref":self.key,"elicitation_id":id,"answered_choice_id":choice,"binding":crate::consent::binding(&self.prepared)})
    }
}
#[cfg(feature = "test-clock")]
#[async_trait::async_trait]
impl ElicitationPlane for ConflictingFeed {
    async fn get(&self, _: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Ok(ApprovalQuestion {
            terminal: false,
            record: json!({"state":"pending","requester_ref":self.key,"elicitation_id":id}),
        })
    }
    async fn r#await(
        &self,
        _: &BatchCtx,
        _: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.entered.store(true, Ordering::SeqCst);
        std::future::pending().await
    }
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        if self.gets.load(Ordering::SeqCst) == 0 {
            return Ok(ApprovalPage {
                records: Vec::new(),
                cursor: ApprovalCursor {
                    provider_incarnation: "owner".into(),
                    seq: 0,
                },
            });
        }
        while !self.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        let (records, seq) = match cursor.map(|c| c.seq) {
            None if self.split => (vec![self.record("held", "approve")], 1),
            None => (
                vec![self.record("held", "approve"), self.record("other", "deny")],
                1,
            ),
            Some(1) if self.split => (vec![self.record("other", "deny")], 2),
            Some(seq) => (Vec::new(), seq),
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "owner".into(),
                seq,
            },
        })
    }
}

#[cfg(feature = "test-clock")]
async fn conflict_wait(split: bool) -> Rig {
    let mut rig = expired_run("pass").await;
    let mut prepared = rig
        .frames()
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::ToolCallDeferred { .. } => Some(frame.record.clone()),
            _ => None,
        })
        .unwrap();
    let WalRecord::ToolCallDeferred { call_id, ask, .. } = &mut prepared else {
        unreachable!()
    };
    *call_id = ToolCallId("held".into());
    ask.expires_at_ms += 60_000;
    let seq = rig.writer.record(prepared.clone()).into_result().unwrap();
    let key = crate::context::call_key(rig.writer.lineage_id().unwrap(), seq);
    rig.writer.sync().await.unwrap();
    let feed = Arc::new(ConflictingFeed {
        key,
        prepared: prepared.clone(),
        gets: AtomicUsize::new(0),
        entered: std::sync::atomic::AtomicBool::new(false),
        split,
    });
    rig.control.elicitation_plane = Some(feed);
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: Arc::new(prepared),
            approval: None,
            elicitation_id: Some("held".into()),
        }],
    };
    let result = rig
        .batch(&crate::AllowAll, vec![call("held")], Some(resume), false)
        .await;
    assert!(
        matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions")),
        "all decisions for one key must be reduced before approval"
    );
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    assert!(!rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::ApprovalRecorded { call_id, decision: broca_wal::DeferredDecision::Approved, .. } if call_id.0 == "held")));
    rig
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn live_feed_conflicting_answers_refuse_before_any_dispatch() {
    conflict_wait(false).await.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn live_feed_conflicting_continuations_refuse_before_any_dispatch() {
    conflict_wait(true).await.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_feed_conflicting_answers_close_as_named_not_sent() {
    let mut rig = expired_run("pass").await;
    let prepared = rig
        .frames()
        .iter()
        .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
        .unwrap()
        .clone();
    let key = crate::context::call_key(rig.writer.lineage_id().unwrap(), prepared.seq);
    let feed = Arc::new(ConflictingFeed {
        key,
        prepared: prepared.record,
        gets: AtomicUsize::new(1),
        entered: std::sync::atomic::AtomicBool::new(true),
        split: true,
    });
    rig.control.elicitation_plane = Some(feed);
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
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateResult { outcome: broca_wal::LateOutcome::NotSent, result, .. } if matches!(&result.output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions")))));
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    rig.finish().await;
}

#[tokio::test]
async fn every_known_id_unavailable_await_release_detaches_with_original_code() {
    for (mode, code) in [
        ("unreadable_binding_missing", "consent_binding_mismatch"),
        ("unreadable_binding_type", "consent_binding_mismatch"),
        ("unsupported_choice", "consent_choice_unsupported"),
        ("wrong_card", "consent_card_mismatch"),
        ("await_refusal", "route_closed"),
    ] {
        let rig = Rig::new("pass", mode, false, false);
        let result = rig
            .batch(&crate::AllowAll, vec![call("c")], None, false)
            .await;
        assert_eq!(
            rig.questions.detaches.lock().unwrap().len(),
            1,
            "{mode}: every known-id unavailable release must detach"
        );
        assert!(
            matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains(code)),
            "{mode}: retain the original refusal code"
        );
        assert_eq!(rig.questions.outstanding.load(Ordering::SeqCst), 0);
        assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
        assert!(rig.questions.reports.lock().unwrap().is_empty());
        rig.finish().await;
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn local_expiry_detaches_the_known_visible_card() {
    let rig = expired_run("pass").await;
    assert_eq!(rig.questions.detaches.lock().unwrap().len(), 1);
    assert_eq!(rig.questions.outstanding.load(Ordering::SeqCst), 0);
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn approving_detach_readback_stays_not_sent_across_later_admission() {
    let rig = Rig::file_backed("pass");
    *rig.questions.detach_answer.lock().unwrap() = "approve";
    let rig = expire_rig(rig).await;
    assert_eq!(
        rig.questions
            .reports
            .lock()
            .unwrap()
            .iter()
            .map(|(_, v)| *v)
            .collect::<Vec<_>>(),
        [ApprovalExecution::ApprovedNotExecuted]
    );
    *rig.questions.answer.lock().unwrap() = "approve";
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
    history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
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
        rig.tools.provider_requests.load(Ordering::SeqCst),
        0,
        "an approving detach readback must never become executable later"
    );
    assert_eq!(rig.questions.reports.lock().unwrap().len(), 1);
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::LateResult {
            outcome: broca_wal::LateOutcome::NotSent,
            ..
        }
    )));
    assert!(!rig
        .frames()
        .iter()
        .any(|frame| matches!(frame.record, WalRecord::LateDispatchIntent { .. })));
    let bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
    let mut cut = 0;
    while cut < bytes.len() {
        match broca_wal::framing::decode_next(&bytes[cut..]) {
            broca_wal::framing::DecodeStep::Lineage { consumed, .. } => cut += consumed,
            broca_wal::framing::DecodeStep::Record { framed, consumed } => {
                cut += consumed;
                if matches!(framed.record, WalRecord::LateResult { .. }) {
                    break;
                }
            }
            _ => panic!("complete release fixture"),
        }
    }
    let cut_store = crate::FileWalStore::new(rig.root.join("release-result-cut"));
    let path = cut_store.wal_path(&rig.bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, &bytes[..cut]).unwrap();
    let cut_writer = WalHandle::spawn(cut_store.open(&rig.bind).unwrap(), 1).unwrap();
    let mut cut_history =
        approval_gate::History::new(&cut_writer, cut_store.read_records(&rig.bind).unwrap())
            .unwrap();
    let reads = rig.questions.polls.load(Ordering::SeqCst);
    approval_gate::admission(
        &mut cut_history,
        &cut_writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    assert_eq!(
        rig.questions.polls.load(Ordering::SeqCst),
        reads,
        "the terminal-before-approval cut is already closed"
    );
    cut_writer.shutdown().await.unwrap();
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
async fn copied_pending_late_episode(stopped: bool) -> Rig {
    let mut rig = expire_rig(Rig::file_backed("pass")).await;
    record_late_consent(&rig).await;
    let bytes = std::fs::read(rig.tools.store.wal_path(&rig.bind)).unwrap();
    let broca_wal::framing::DecodeStep::Lineage { consumed, .. } =
        broca_wal::framing::decode_next(&bytes)
    else {
        panic!("fixture has a lineage header")
    };
    let mut encoded = bytes[..consumed].to_vec();
    for mut frame in rig.frames() {
        if let WalRecord::RunFinished { reason, .. } = &mut frame.record {
            if stopped {
                *reason = RunFinishReason::Cancelled;
            }
        }
        encoded.extend(broca_wal::framing::encode(frame.seq, frame.fence, &frame.record).unwrap());
    }
    let root = rig
        .root
        .join(if stopped { "stopped-seed" } else { "live-seed" });
    let store = crate::FileWalStore::new(&root);
    let path = store.wal_path(&rig.bind);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, encoded).unwrap();
    let writer = WalHandle::spawn(store.open(&rig.bind).unwrap(), 1).unwrap();
    assert_eq!(writer.lineage_id(), rig.writer.lineage_id());
    let old = std::mem::replace(&mut rig.writer, writer);
    old.shutdown().await.unwrap();
    let keys = rig.questions.keys.lock().unwrap().clone();
    rig.tools = tools_for_store(crate::FileWalStore::new(&root), rig.bind.clone());
    rig.questions = Arc::new(Questions {
        store: crate::FileWalStore::new(&root),
        memory: None,
        completed: rig.tools.completed.clone(),
        lineage: rig.writer.lineage_id().unwrap(),
        head: rig.writer.durable_head(),
        keys: Mutex::new(keys),
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
    rig.control.elicitation_plane = Some(rig.questions.clone());
    rig
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn legacy_stop_facts_seed_requires_full_fold_before_late_dispatch() {
    for stopped in [true, false] {
        let rig = copied_pending_late_episode(stopped).await;
        let full = rig.frames();
        let mut history = approval_gate::History::new(&rig.writer, full.clone()).unwrap();
        let mut legacy = serde_json::to_value(&history.projection).unwrap();
        legacy.as_object_mut().unwrap().remove("stopped_runs");
        history.projection = serde_json::from_value(legacy).unwrap();
        history.frames.retain(|frame| {
            matches!(
                frame.record,
                WalRecord::RunStarted { .. } | WalRecord::ToolBatchStarted { .. }
            )
        });
        let incomplete = approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await;
        assert!(
            matches!(incomplete, Err(RunError::Wal(ref message)) if message.contains("consent_projection_incomplete")),
            "unknown stop facts require a complete authoritative prefix, not an origin-only cache"
        );
        assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
        history.frames = full;
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
            rig.tools.provider_requests.load(Ordering::SeqCst),
            usize::from(!stopped)
        );
        assert_eq!(
            rig.questions.reports.lock().unwrap()[0].1,
            if stopped {
                ApprovalExecution::ApprovedNotExecuted
            } else {
                ApprovalExecution::Executed
            }
        );
        assert_eq!(rig.questions.gets.load(Ordering::SeqCst), 0);
        assert_eq!(rig.questions.awaits.load(Ordering::SeqCst), 0);
        rig.finish().await;
    }
}

#[cfg(feature = "test-clock")]
async fn stopped_expired_sibling(cancel: bool) -> Rig {
    struct LongerSibling(Arc<Hooks>);
    #[async_trait::async_trait]
    impl crate::HookPlane for LongerSibling {
        async fn hook(
            &self,
            provider: &str,
            bind: &BindIdentity,
            hook: HookCall,
        ) -> Result<HookAnswer, crate::HookUnavailable> {
            if matches!(&hook.subject, Subject::PreTool { phase: Phase::Approve, tool_call_id, .. } if tool_call_id == "held")
            {
                return Ok(serde_json::from_value(json!({"answer":"ask","ask":{"prompt":"Allow held?","expires_at_ms":self.0.expires + 60_000,"on_expiry":"deny","material_damage":false,"late_execution":"execute"}})).unwrap());
            }
            crate::HookPlane::hook(self.0.as_ref(), provider, bind, hook).await
        }
    }
    let mut rig = Rig::new("pass", "none", false, false);
    let clock = crate::Clock::for_state_root(&rig.root).unwrap();
    rig.control.clock = Some(clock.clone());
    rig.control.hook_plane = Some(Arc::new(LongerSibling(rig.hooks.clone())));
    rig.control.manifest = Some(Arc::new(frozen(
        vec![vec![
            sub("pre_tool", Some("approve"), "refuse"),
            sub("pre_tool", Some("validate"), "pass"),
        ]],
        None,
        false,
    )));
    let token = CancellationToken::new();
    if cancel {
        rig.control.cancel = Some(token.clone());
    } else {
        rig.control.stop_requested = Some(token.clone());
    }
    let mut req = hook_tests::request();
    req.session = rig.bind.clone();
    req.on_restart = None;
    req.scope = Some(
        serde_json::from_value(
            json!({"owner":{"kind":"reserved","module_id":"agent"},"ref":"s","scope_epoch":1}),
        )
        .unwrap(),
    );
    let events = vec![
        NormalizedEvent::Control(ProviderControl::AssistantMessage {
            message: AssistantMessage {
                message_id: broca_protocol::MessageId("siblings".into()),
                content: vec![
                    ContentBlock::ToolCall(call("c")),
                    ContentBlock::ToolCall(call("held")),
                ],
            },
        }),
        NormalizedEvent::Control(ProviderControl::Finished {
            usage: Usage::default(),
            finish_reason: FinishReason::ToolCalls,
            retries_used: 0,
        }),
    ];
    let provider = crate::StubProvider::new(vec![events]);
    let (out, _rx) = mpsc::channel(1000);
    let run = Box::pin(run_body_with_approval_history(
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
        Some(rig.wal_store()),
        None,
    ));
    let interrupt = async {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(2));
        while rig.questions.outstanding.load(Ordering::SeqCst) < 2 {
            tick.tick().await;
        }
        std::fs::write(
            rig.root.join("test-clock"),
            json!({"now_ms":rig.hooks.expires+1}).to_string(),
        )
        .unwrap();
        clock.refresh().unwrap();
        while !rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::ApprovalRecorded { call_id, decision: broca_wal::DeferredDecision::Expired, .. } if call_id.0 == "c")) { tick.tick().await; }
        token.cancel();
    };
    let (result, ()) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(run, interrupt)
    })
    .await
    .unwrap();
    assert_eq!(
        result.unwrap().reason,
        if cancel {
            RunFinishReason::Cancelled
        } else {
            RunFinishReason::Interrupted
        }
    );
    rig.control.cancel = None;
    rig.control.stop_requested = None;
    *rig.questions.answer.lock().unwrap() = "approve";
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
        rig.tools.provider_requests.load(Ordering::SeqCst),
        0,
        "a stopped episode's expired sibling cannot late-execute"
    );
    assert!(rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::LateResult {
            outcome: broca_wal::LateOutcome::NotSent,
            ..
        }
    )));
    rig
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn cancelled_episode_expired_sibling_never_late_executes() {
    stopped_expired_sibling(true).await.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn nonresuming_stopped_episode_expired_sibling_never_late_executes() {
    stopped_expired_sibling(false).await.finish().await;
}

#[test]
fn stopped_run_projection_is_bounded_by_open_calls_and_prunes_terminals() {
    use crate::replay::deferred_tests::{approval, batch, prepared};
    let mut projection = crate::DeferredCallProjection::new("00000000-0000-0000-0000-000000000000");
    let mut seq = 0;
    for i in 0..128 {
        let id = format!("c{i}");
        let run = RunId(format!("r{i}"));
        let mut records = batch(&[&id]);
        let WalRecord::RunStarted { run_id, .. } = &mut records[0] else {
            unreachable!()
        };
        *run_id = run.clone();
        for record in records {
            seq += 1;
            projection.fold_frames(&[broca_wal::FramedRecord {
                seq,
                fence: 0,
                record,
            }]);
        }
        seq += 1;
        let key = format!("{}:{seq}", projection.lineage_id);
        projection.fold_frames(&[broca_wal::FramedRecord {
            seq,
            fence: 0,
            record: prepared(&id, "b"),
        }]);
        seq += 1;
        projection.fold_frames(&[broca_wal::FramedRecord {
            seq,
            fence: 0,
            record: approval(&id, &key, broca_wal::DeferredDecision::Expired),
        }]);
        seq += 1;
        projection.fold_frames(&[broca_wal::FramedRecord {
            seq,
            fence: 0,
            record: WalRecord::RunFinished {
                reason: RunFinishReason::Cancelled,
                ts_ms: None,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        }]);
        assert_eq!(
            serde_json::to_value(&projection).unwrap()["stopped_runs"],
            json!([run.0])
        );
        assert!(projection
            .obligations()
            .expired_without_late_record
            .is_empty());
        seq += 1;
        projection.fold_frames(&[broca_wal::FramedRecord {
            seq,
            fence: 0,
            record: WalRecord::LateResult {
                call_key: key,
                outcome: broca_wal::LateOutcome::NotSent,
                result: Arc::new(crate::resume::not_dispatched_result(&ToolCallId(id))),
            },
        }]);
        assert_eq!(
            serde_json::to_value(&projection).unwrap()["stopped_runs"],
            json!([]),
            "finished runs cannot accumulate in the stop-facts seed"
        );
    }
}

#[test]
fn released_0_3_199_projection_reader_accepts_stop_facts_cache_bytes() {
    // The v0.3.199 Git tag at c39240ce has these three projection-reader fields.
    // Decode the new cache bytes with that unchanged reader shape: default Serde
    // behavior must ignore the additional stop-facts field, not reject the cache.
    #[derive(serde::Deserialize)]
    struct ReleasedProjection {
        lineage_id: String,
        last_run_id: Option<RunId>,
        calls: std::collections::BTreeMap<String, crate::ProjectedDeferredCall>,
    }
    let mut projection = crate::DeferredCallProjection::new("00000000-0000-0000-0000-000000000000");
    let records = crate::replay::deferred_tests::batch(&["c"]);
    for (i, record) in records.into_iter().enumerate() {
        projection.fold_frames(&[broca_wal::FramedRecord {
            seq: i as u64 + 1,
            fence: 0,
            record,
        }]);
    }
    projection.fold_frames(&[
        broca_wal::FramedRecord {
            seq: 4,
            fence: 0,
            record: crate::replay::deferred_tests::prepared("c", "b"),
        },
        broca_wal::FramedRecord {
            seq: 5,
            fence: 0,
            record: crate::replay::deferred_tests::approval(
                "c",
                "00000000-0000-0000-0000-000000000000:4",
                broca_wal::DeferredDecision::Expired,
            ),
        },
        broca_wal::FramedRecord {
            seq: 6,
            fence: 0,
            record: WalRecord::RunFinished {
                reason: RunFinishReason::Cancelled,
                ts_ms: None,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
            },
        },
    ]);
    let bytes = serde_json::to_vec(&projection).unwrap();
    assert!(serde_json::from_slice::<Value>(&bytes)
        .unwrap()
        .get("stopped_runs")
        .is_some());
    let released: ReleasedProjection = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(released.lineage_id, projection.lineage_id);
    assert_eq!(released.last_run_id, projection.last_run_id);
    assert_eq!(released.calls, projection.calls);
}

#[test]
fn legacy_projection_suffix_remains_unknown_until_complete_origin_fold() {
    use crate::replay::deferred_tests::{approval, batch, prepared};
    for stopped in [true, false] {
        let lineage = "00000000-0000-0000-0000-000000000000";
        let key = format!("{lineage}:4");
        let mut records = batch(&["c"]);
        records.push(prepared("c", "b"));
        records.push(approval("c", &key, broca_wal::DeferredDecision::Expired));
        records.push(WalRecord::RunFinished {
            reason: if stopped {
                RunFinishReason::Cancelled
            } else {
                RunFinishReason::Completed
            },
            ts_ms: None,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        });
        records.push(WalRecord::LateApprovalRecorded {
            call_key: key.clone(),
            elicitation_id: "q".into(),
            decision: broca_wal::LateDecision::Approved,
            approval_digest: None,
            late_execution: Some(broca_wal::LateExecution::Execute),
        });
        let frames: Vec<_> = records
            .into_iter()
            .enumerate()
            .map(|(i, record)| broca_wal::FramedRecord {
                seq: i as u64 + 1,
                fence: 0,
                record,
            })
            .collect();
        let mut full = crate::DeferredCallProjection::new(lineage);
        full.fold_frames(&frames);
        let mut value = serde_json::to_value(&full).unwrap();
        value.as_object_mut().unwrap().remove("stopped_runs");
        let legacy: crate::DeferredCallProjection = serde_json::from_value(value).unwrap();
        let suffix = crate::replay::project_deferred_calls(
            Some(&legacy),
            &[],
            Some(lineage),
            Some(&RunId("r".into())),
        )
        .unwrap();
        assert!(
            suffix.obligations().pending_late_execution.is_empty(),
            "a legacy suffix cannot assume empty stop facts"
        );
        assert!(serde_json::to_value(&suffix)
            .unwrap()
            .get("stopped_runs")
            .is_none());
        let rebuilt =
            crate::replay::project_deferred_calls(Some(&legacy), &frames, Some(lineage), None)
                .unwrap();
        assert_eq!(
            rebuilt.obligations().pending_late_execution.len(),
            usize::from(!stopped)
        );
        assert!(serde_json::to_value(&rebuilt)
            .unwrap()
            .get("stopped_runs")
            .is_some());
    }
}

#[cfg(feature = "test-clock")]
struct IntentSyncGate {
    entered: tokio::sync::Notify,
    released: Mutex<bool>,
    cv: std::sync::Condvar,
    seq: std::sync::atomic::AtomicU64,
}
#[cfg(feature = "test-clock")]
impl IntentSyncGate {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.cv.notify_all();
    }
}
#[cfg(feature = "test-clock")]
struct IntentSyncWal {
    inner: Box<dyn broca_wal::Wal + Send>,
    armed: std::sync::atomic::AtomicBool,
    gate: Arc<IntentSyncGate>,
    ordinary: bool,
}
#[cfg(feature = "test-clock")]
impl broca_wal::Wal for IntentSyncWal {
    fn append(
        &mut self,
        fence: u64,
        record: &WalRecord,
        sync: bool,
    ) -> Result<u64, broca_wal::WalError> {
        let intent = matches!(record, WalRecord::LateDispatchIntent { .. })
            || (self.ordinary
                && matches!(
                    record,
                    WalRecord::ToolDispatchIntent {
                        call_key: Some(_),
                        ..
                    }
                ));
        let seq = self.inner.append(fence, record, sync && !intent)?;
        if intent {
            self.gate.seq.store(seq, Ordering::SeqCst);
            self.armed.store(true, Ordering::SeqCst);
            if sync {
                self.sync()?;
            }
        }
        Ok(seq)
    }
    fn sync(&self) -> Result<(), broca_wal::WalError> {
        if self.armed.swap(false, Ordering::SeqCst) {
            // Hold the real durability barrier after intent append, before its ack.
            self.gate.entered.notify_one();
            let (released, timeout) = self
                .gate
                .cv
                .wait_timeout_while(
                    self.gate.released.lock().unwrap(),
                    broca_test_support::EVENT_HANG_GUARD,
                    |released| !*released,
                )
                .unwrap();
            if timeout.timed_out() && !*released {
                return Err(broca_wal::WalError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    "intent sync gate was not released",
                )));
            }
        }
        self.inner.sync()
    }
    fn replay(&self) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.replay()
    }
    fn tail_after_seq(
        &self,
        seq: u64,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.inner.tail_after_seq(seq)
    }
    fn last_seq(&self) -> u64 {
        self.inner.last_seq()
    }
    fn lineage_id(&self) -> Option<broca_wal::WalLineageId> {
        self.inner.lineage_id()
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_dispatch_cancel_or_stop_during_intent_sync_is_not_sent() {
    late_dispatch_during_intent_sync(DispatchSignalPolicy::Enforce).await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_core_fence_stops_noncompliant_adapter_after_intent_sync() {
    late_dispatch_during_intent_sync(DispatchSignalPolicy::Ignore).await;
}

#[cfg(feature = "test-clock")]
async fn late_dispatch_during_intent_sync(dispatch_signal_policy: DispatchSignalPolicy) {
    for cancel in [true, false] {
        let mut rig = copied_pending_late_episode(false).await;
        rig.tools.dispatch_signal_policy = dispatch_signal_policy;
        rig.writer.shutdown().await.unwrap();
        let gate = Arc::new(IntentSyncGate {
            entered: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            cv: std::sync::Condvar::new(),
            seq: std::sync::atomic::AtomicU64::new(0),
        });
        rig.writer = WalHandle::spawn(
            Box::new(IntentSyncWal {
                inner: rig.tools.store.open(&rig.bind).unwrap(),
                armed: std::sync::atomic::AtomicBool::new(false),
                gate: gate.clone(),
                ordinary: false,
            }),
            1,
        )
        .unwrap();
        rig.control.elicitation_plane = None;
        Arc::get_mut(&mut rig.questions).unwrap().head = rig.writer.durable_head();
        rig.control.elicitation_plane = Some(rig.questions.clone());
        let token = CancellationToken::new();
        if cancel {
            rig.control.cancel = Some(token.clone());
        } else {
            rig.control.stop_requested = Some(token.clone());
        }
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        let admission = approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        );
        let interrupt = async {
            gate.entered.notified().await;
            assert!(
                rig.writer.durable_head().load(Ordering::SeqCst) < gate.seq.load(Ordering::SeqCst),
                "intent must still be unsynced when stop arrives"
            );
            token.cancel();
            gate.release();
        };
        let result =
            tokio::time::timeout(HANG_GUARD, async { tokio::join!(admission, interrupt) }).await;
        gate.release();
        let (result, ()) = result.expect("the intent barrier must be released");
        result.unwrap();
        assert_eq!(
            rig.tools.provider_requests.load(Ordering::SeqCst),
            0,
            "late cancellation during intent sync must win before send"
        );
        assert!(rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::LateResult {
                outcome: broca_wal::LateOutcome::NotSent,
                ..
            }
        )));
        assert_eq!(
            rig.questions.reports.lock().unwrap()[0].1,
            ApprovalExecution::ApprovedNotExecuted
        );
        rig.finish().await;
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_dispatch_cancel_or_stop_during_route_open_is_not_sent() {
    late_dispatch_during_route_open(DispatchSignalPolicy::Enforce).await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_core_fence_stops_noncompliant_adapter_after_route_open() {
    late_dispatch_during_route_open(DispatchSignalPolicy::Ignore).await;
}

#[cfg(feature = "test-clock")]
async fn late_dispatch_during_route_open(dispatch_signal_policy: DispatchSignalPolicy) {
    for cancel in [true, false] {
        let mut rig = copied_pending_late_episode(false).await;
        rig.tools.dispatch_signal_policy = dispatch_signal_policy;
        let token = CancellationToken::new();
        if cancel {
            rig.control.cancel = Some(token.clone());
        } else {
            rig.control.stop_requested = Some(token.clone());
        }
        *rig.tools.interrupt_on_open.lock().unwrap() = Some(token);
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
            rig.tools.provider_requests.load(Ordering::SeqCst),
            0,
            "late cancellation during route open must win before send"
        );
        assert!(rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::LateResult {
                outcome: broca_wal::LateOutcome::NotSent,
                ..
            }
        )));
        assert_eq!(
            rig.questions.reports.lock().unwrap()[0].1,
            ApprovalExecution::ApprovedNotExecuted
        );
        rig.finish().await;
    }
}

#[cfg(feature = "test-clock")]
struct LiveCursorProbe {
    key: String,
    prepared: WalRecord,
    unreadable: bool,
    gets: AtomicUsize,
    entered: std::sync::atomic::AtomicBool,
    phase: AtomicUsize,
    stale_refusals: AtomicUsize,
    skipped: std::sync::atomic::AtomicBool,
    cursors: Mutex<Vec<Option<ApprovalCursor>>>,
}
#[cfg(feature = "test-clock")]
impl LiveCursorProbe {
    fn cursor(incarnation: &str, seq: u64) -> ApprovalCursor {
        ApprovalCursor {
            provider_incarnation: incarnation.into(),
            seq,
        }
    }
    fn approved(&self) -> Value {
        json!({"state":"answered","requester_ref":self.key,"elicitation_id":"held","answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)})
    }
}
#[cfg(feature = "test-clock")]
#[async_trait::async_trait]
impl ElicitationPlane for LiveCursorProbe {
    async fn get(&self, _: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Ok(ApprovalQuestion {
            terminal: false,
            record: json!({"state":"pending","requester_ref":self.key,"elicitation_id":id}),
        })
    }
    async fn r#await(
        &self,
        _: &BatchCtx,
        _: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.entered.store(true, Ordering::SeqCst);
        std::future::pending().await
    }
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        if self.gets.load(Ordering::SeqCst) == 0 {
            return Ok(ApprovalPage {
                records: Vec::new(),
                cursor: Self::cursor("setup", 0),
            });
        }
        while !self.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        // Route I/O must yield even for a refusal, so a broken cursor retry loop
        // cannot starve this fixture's cancellation monitor and deadline.
        tokio::task::yield_now().await;
        self.cursors.lock().unwrap().push(cursor.cloned());
        if cursor.is_some_and(|c| c.provider_incarnation == "old" && c.seq != 0) {
            if self.unreadable {
                self.skipped.store(true, Ordering::SeqCst);
                return Ok(ApprovalPage {
                    records: Vec::new(),
                    cursor: Self::cursor("old", 7),
                });
            }
            self.stale_refusals.fetch_add(1, Ordering::SeqCst);
            return Err(crate::consent::refusal("cursor_incarnation_changed", false));
        }
        if cursor.is_some() {
            return Ok(ApprovalPage {
                records: Vec::new(),
                cursor: Self::cursor("new", 1),
            });
        }
        if self.phase.fetch_add(1, Ordering::SeqCst) == 0 {
            let mut records = Vec::new();
            if self.unreadable {
                let mut bad = self.approved();
                bad.as_object_mut().unwrap().remove("answered_choice_id");
                records.push(bad);
            }
            return Ok(ApprovalPage {
                records,
                cursor: Self::cursor("old", 7),
            });
        }
        Ok(ApprovalPage {
            records: vec![self.approved()],
            cursor: Self::cursor("new", 1),
        })
    }
}

#[cfg(feature = "test-clock")]
async fn live_cursor_probe(unreadable: bool) {
    let mut rig = expired_run("pass").await;
    // The earlier expiry is durable; polling this new held call uses the normal
    // advancing clock rather than the frozen clock used to create that expiry.
    rig.control.clock = None;
    let mut prepared = rig
        .frames()
        .iter()
        .find(|frame| matches!(frame.record, WalRecord::ToolCallDeferred { .. }))
        .unwrap()
        .record
        .clone();
    let WalRecord::ToolCallDeferred { call_id, ask, .. } = &mut prepared else {
        unreachable!()
    };
    *call_id = ToolCallId("held".into());
    ask.expires_at_ms += 60_000;
    let seq = rig.writer.record(prepared.clone()).into_result().unwrap();
    rig.writer.sync().await.unwrap();
    let probe = Arc::new(LiveCursorProbe {
        key: crate::context::call_key(rig.writer.lineage_id().unwrap(), seq),
        prepared: prepared.clone(),
        unreadable,
        gets: AtomicUsize::new(0),
        entered: std::sync::atomic::AtomicBool::new(false),
        phase: AtomicUsize::new(0),
        stale_refusals: AtomicUsize::new(0),
        skipped: std::sync::atomic::AtomicBool::new(false),
        cursors: Mutex::new(Vec::new()),
    });
    rig.control.elicitation_plane = Some(probe.clone());
    let cancel = CancellationToken::new();
    rig.control.cancel = Some(cancel.clone());
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: Arc::new(prepared),
            approval: None,
            elicitation_id: Some("held".into()),
        }],
    };
    let batch = rig.batch(&crate::AllowAll, vec![call("held")], Some(resume), false);
    let monitor = async {
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(2));
        while rig.tools.provider_requests.load(Ordering::SeqCst) == 0 {
            if probe.skipped.load(Ordering::SeqCst)
                || probe.stale_refusals.load(Ordering::SeqCst) > 1
            {
                cancel.cancel();
                break;
            }
            tick.tick().await;
        }
    };
    let (result, ()) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(batch, monitor)
    })
    .await
    .unwrap();
    assert!(
        !result.results[0].is_error,
        "live feed cursor lost the corrected approval"
    );
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 1);
    let cursors = probe.cursors.lock().unwrap().clone();
    if unreadable {
        assert_eq!(
            &cursors[..2],
            &[None, None],
            "unreadable live page is retried at the same feed position"
        );
        assert!(!probe.skipped.load(Ordering::SeqCst));
    } else {
        assert_eq!(probe.stale_refusals.load(Ordering::SeqCst), 1);
        assert_eq!(
            &cursors[..3],
            &[None, Some(LiveCursorProbe::cursor("old", 7)), None],
            "the stale non-zero live cursor must actually be cleared"
        );
    }
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn live_wait_clears_stale_nonzero_cursor_before_reread() {
    live_cursor_probe(false).await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn live_wait_unreadable_page_retries_before_cursor_advance() {
    live_cursor_probe(true).await;
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn ordinary_approved_cancel_during_intent_sync_is_not_sent() {
    let mut rig = Rig::file_backed("pass");
    *rig.questions.answer.lock().unwrap() = "approve";
    rig.writer.shutdown().await.unwrap();
    let gate = Arc::new(IntentSyncGate {
        entered: tokio::sync::Notify::new(),
        released: Mutex::new(false),
        cv: std::sync::Condvar::new(),
        seq: std::sync::atomic::AtomicU64::new(0),
    });
    rig.writer = WalHandle::spawn(
        Box::new(IntentSyncWal {
            inner: rig.tools.store.open(&rig.bind).unwrap(),
            armed: std::sync::atomic::AtomicBool::new(false),
            gate: gate.clone(),
            ordinary: true,
        }),
        1,
    )
    .unwrap();
    rig.control.elicitation_plane = None;
    Arc::get_mut(&mut rig.questions).unwrap().head = rig.writer.durable_head();
    rig.control.elicitation_plane = Some(rig.questions.clone());
    let cancel = CancellationToken::new();
    rig.control.cancel = Some(cancel.clone());
    let batch = rig.batch(&crate::AllowAll, vec![call("c")], None, false);
    let interrupt = async {
        gate.entered.notified().await;
        assert!(rig.writer.durable_head().load(Ordering::SeqCst) < gate.seq.load(Ordering::SeqCst));
        cancel.cancel();
        gate.release();
    };
    let result = broca_test_support::event_timeout(
        "ordinary intent gate to release",
        async { tokio::join!(batch, interrupt) },
        || broca_test_support::record_kinds(rig.frames()),
    )
    .await;
    gate.release();
    let (_result, ()) = result.expect("ordinary intent gate must release");
    assert_eq!(
        rig.tools.provider_requests.load(Ordering::SeqCst),
        0,
        "cancel during ordinary approved intent sync cannot send"
    );
    assert!(rig.frames().iter().any(|f| matches!(
        f.record,
        WalRecord::ToolDispatchIntent {
            call_key: Some(_),
            ..
        }
    )));
    assert!(
        rig.frames().iter().any(|f| matches!(
            f.record,
            WalRecord::ToolResult {
                outcome: DispatchOutcome::Interrupted,
                ..
            }
        )),
        "the ordinary result must record known non-execution, not Unknown or provider completion"
    );
    assert_eq!(
        rig.questions.reports.lock().unwrap()[0].1,
        ApprovalExecution::ApprovedNotExecuted
    );
    rig.finish().await;
}
