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
