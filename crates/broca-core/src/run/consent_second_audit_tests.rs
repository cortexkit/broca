use super::*;

struct SlowHealthyFeed {
    finished: std::sync::atomic::AtomicBool,
    positions: Mutex<Vec<u64>>,
    key: String,
    prepared: WalRecord,
    gets: AtomicUsize,
}
#[async_trait::async_trait]
impl ElicitationPlane for SlowHealthyFeed {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let position = cursor.map_or(0, |cursor| cursor.seq);
        self.positions.lock().unwrap().push(position);
        if !self.finished.load(Ordering::SeqCst) {
            tokio::time::sleep(std::time::Duration::from_millis(450)).await;
        }
        let records = if position < 24 {
            vec![
                json!({"state":"answered","requester_ref":format!("unrelated-{position}"),"elicitation_id":format!("other-{position}"),"answered_choice_id":"deny","binding":{}}),
            ]
        } else {
            self.finished.store(true, Ordering::SeqCst);
            Vec::new()
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "healthy".into(),
                seq: (position + 1).min(24),
            },
        })
    }
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
        id: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        Ok(ApprovalQuestion {
            terminal: true,
            record: json!({"state":"answered","requester_ref":self.key,"elicitation_id":id,"answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}),
        })
    }
}

fn seed_known_call(rig: &Rig) -> (String, WalRecord) {
    let mut prepared = crate::replay::deferred_tests::prepared("c", "r-b1");
    let WalRecord::ToolCallDeferred { ask, .. } = &mut prepared else {
        unreachable!()
    };
    ask.expires_at_ms = rig.hooks.expires;
    let seq = rig.writer.record(prepared.clone()).into_result().unwrap();
    (
        crate::context::call_key(rig.writer.lineage_id().unwrap(), seq),
        prepared,
    )
}

#[tokio::test]
async fn healthy_known_id_feed_longer_than_one_budget_reaches_get_and_await() {
    let mut rig = Rig::new("pass", "none", false, false);
    rig.timeout = std::time::Duration::from_secs(24);
    let (key, prepared) = seed_known_call(&rig);
    rig.writer.sync().await.unwrap();
    let feed = Arc::new(SlowHealthyFeed {
        finished: std::sync::atomic::AtomicBool::new(false),
        positions: Mutex::new(Vec::new()),
        key,
        prepared: prepared.clone(),
        gets: AtomicUsize::new(0),
    });
    rig.control.elicitation_plane = Some(feed.clone());
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: Arc::new(prepared),
            approval: None,
            elicitation_id: Some("known".into()),
        }],
    };
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), false)
        .await;
    assert!(!result.results[0].is_error);
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 1);
    assert_eq!(feed.gets.load(Ordering::SeqCst), 1);
    let positions = feed.positions.lock().unwrap().clone();
    let first_head = positions
        .iter()
        .position(|position| *position == 24)
        .expect("healthy traversal must reach its head");
    assert_eq!(
        positions[..first_head]
            .iter()
            .filter(|position| **position == 0)
            .count(),
        1,
        "a budget retry must continue, not restart at zero"
    );
    rig.finish().await;
}

struct AwaitConflict {
    key: String,
    prepared: WalRecord,
    mode: &'static str,
    awaits: AtomicUsize,
    older_denied: std::sync::atomic::AtomicBool,
    new_approved: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl ElicitationPlane for AwaitConflict {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let records = if cursor.is_some() || !self.older_denied.load(Ordering::SeqCst) {
            Vec::new()
        } else {
            let mut records = vec![
                json!({"state":"answered","requester_ref":self.key,"elicitation_id":"old","answered_choice_id":"deny","binding":crate::consent::binding(&self.prepared)}),
            ];
            if self.new_approved.load(Ordering::SeqCst) {
                records.push(json!({"state":"answered","requester_ref":self.key,"elicitation_id":"new","answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}));
            }
            records
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "owner".into(),
                seq: 1,
            },
        })
    }
    async fn get(&self, _: &BatchCtx, _: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        Ok(ApprovalQuestion {
            terminal: false,
            record: json!({"state":"pending","requester_ref":self.key,"elicitation_id":"new"}),
        })
    }
    async fn r#await(
        &self,
        _: &BatchCtx,
        _: &str,
        _: u64,
    ) -> Result<ApprovalQuestion, ApprovalRouteError> {
        let n = self.awaits.fetch_add(1, Ordering::SeqCst);
        self.older_denied.store(true, Ordering::SeqCst);
        if n == 0 && self.mode != "first" {
            if self.mode == "unreadable" {
                return Ok(ApprovalQuestion {
                    terminal: true,
                    record: json!({"state":"answered","requester_ref":self.key,"elicitation_id":"new","binding":crate::consent::binding(&self.prepared)}),
                });
            }
            return Err(crate::consent::refusal(
                if self.mode == "timeout" {
                    "timeout"
                } else {
                    "route_reloading"
                },
                true,
            ));
        }
        self.new_approved.store(true, Ordering::SeqCst);
        Ok(ApprovalQuestion {
            terminal: true,
            record: json!({"state":"answered","requester_ref":self.key,"elicitation_id":"new","answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}),
        })
    }
}
async fn await_conflict(mode: &'static str) {
    let mut rig = Rig::new("pass", "none", false, false);
    let (key, prepared) = seed_known_call(&rig);
    rig.writer.sync().await.unwrap();
    let cards = Arc::new(AwaitConflict {
        key,
        prepared: prepared.clone(),
        mode,
        awaits: AtomicUsize::new(0),
        older_denied: std::sync::atomic::AtomicBool::new(false),
        new_approved: std::sync::atomic::AtomicBool::new(false),
    });
    rig.control.elicitation_plane = Some(cards.clone());
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: Arc::new(prepared),
            approval: None,
            elicitation_id: Some("new".into()),
        }],
    };
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), false)
        .await;
    assert!(
        matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions")),
        "a filed await approval must check every settlement before intake"
    );
    assert_eq!(
        cards.awaits.load(Ordering::SeqCst),
        if mode == "first" { 1 } else { 2 }
    );
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
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
async fn filed_await_timeout_then_approve_refuses_older_denial() {
    await_conflict("timeout").await;
}
#[tokio::test]
async fn first_filed_await_approval_checks_settlements_after_scan() {
    await_conflict("first").await;
}
#[tokio::test]
async fn filed_retryable_or_unreadable_await_approval_checks_settlements() {
    for mode in ["retry", "unreadable"] {
        await_conflict(mode).await;
    }
}
