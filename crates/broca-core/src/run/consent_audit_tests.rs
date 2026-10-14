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
    let (paused, ()) = tokio::time::timeout(std::time::Duration::from_secs(3), async {
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

struct ConflictingFeed {
    key: String,
    prepared: WalRecord,
    gets: AtomicUsize,
    entered: std::sync::atomic::AtomicBool,
    split: bool,
}
impl ConflictingFeed {
    fn record(&self, id: &str, choice: &str) -> Value {
        json!({"state":"answered","requester_ref":self.key,"elicitation_id":id,"answered_choice_id":choice,"binding":crate::consent::binding(&self.prepared)})
    }
}
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
