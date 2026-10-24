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
    let WalRecord::ToolCallDeferred {
        ask,
        deferred_at_ms,
        ..
    } = &mut prepared
    else {
        unreachable!()
    };
    ask.expires_at_ms = rig.hooks.expires;
    *deferred_at_ms = epoch_millis();
    let seq = rig.writer.record(prepared.clone()).into_result().unwrap();
    (
        crate::context::call_key(rig.writer.lineage_id().unwrap(), seq),
        prepared,
    )
}

#[tokio::test]
async fn healthy_known_id_feed_longer_than_one_budget_reaches_get_and_await() {
    let mut rig = Rig::new("pass", "none", false, false);
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
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(24),
        rig.batch(&crate::AllowAll, vec![call("c")], Some(resume), false),
    )
    .await
    .expect("healthy owner-feed traversal must finish across retry budgets");
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

#[cfg(feature = "test-clock")]
struct LiveAdoptionRace {
    key: String,
    prepared: WalRecord,
    gets: AtomicUsize,
    entered: std::sync::atomic::AtomicBool,
    withdrew: std::sync::atomic::AtomicBool,
}
#[cfg(feature = "test-clock")]
#[async_trait::async_trait]
impl ElicitationPlane for LiveAdoptionRace {
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
        let mut records = if cursor.is_none() {
            vec![
                json!({"state":"answered","requester_ref":self.key,"elicitation_id":"old","answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}),
            ]
        } else {
            Vec::new()
        };
        if cursor.is_none() && self.withdrew.load(Ordering::SeqCst) {
            records.push(json!({"state":"answered","requester_ref":self.key,"elicitation_id":"new","answered_choice_id":"deny","binding":crate::consent::binding(&self.prepared)}));
        }
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "owner".into(),
                seq: 1,
            },
        })
    }
    async fn get(&self, _: &BatchCtx, _: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
        Ok(if self.withdrew.load(Ordering::SeqCst) {
            ApprovalQuestion {
                terminal: true,
                record: json!({"state":"answered","requester_ref":self.key,"elicitation_id":"new","answered_choice_id":"deny","binding":crate::consent::binding(&self.prepared)}),
            }
        } else {
            ApprovalQuestion {
                terminal: false,
                record: json!({"state":"pending","requester_ref":self.key,"elicitation_id":"new"}),
            }
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
    async fn withdraw(&self, _: &BatchCtx, id: &str) -> ApprovalWithdrawal {
        assert_eq!(id, "new");
        self.withdrew.store(true, Ordering::SeqCst);
        ApprovalWithdrawal::NotSent {
            reason: "answered".into(),
        }
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn live_old_approval_proves_recorded_duplicate_withdrawal_before_adoption() {
    let mut rig = expired_run("pass").await;
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
    let cards = Arc::new(LiveAdoptionRace {
        key: crate::context::call_key(rig.writer.lineage_id().unwrap(), seq),
        prepared: prepared.clone(),
        gets: AtomicUsize::new(0),
        entered: std::sync::atomic::AtomicBool::new(false),
        withdrew: std::sync::atomic::AtomicBool::new(false),
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
        .batch(&crate::AllowAll, vec![call("held")], Some(resume), false)
        .await;
    assert!(
        matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions")),
        "live adoption must prove the recorded duplicate did not answer oppositely"
    );
    assert!(cards.withdrew.load(Ordering::SeqCst));
    assert_eq!(cards.gets.load(Ordering::SeqCst), 2);
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    rig.finish().await;
}

struct LongLiveFeed {
    delay: std::time::Duration,
    positions: Mutex<Vec<u64>>,
    head: Option<u64>,
}
#[async_trait::async_trait]
impl ElicitationPlane for LongLiveFeed {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        tokio::time::sleep(self.delay).await;
        let position = cursor.map_or(0, |cursor| cursor.seq);
        self.positions.lock().unwrap().push(position);
        let records = if self.head.is_some_and(|head| position >= head) {
            Vec::new()
        } else {
            vec![
                json!({"state":"answered","requester_ref":format!("unrelated-{position}"),"elicitation_id":format!("id-{position}"),"answered_choice_id":"deny","binding":{}}),
            ]
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "live".into(),
                seq: position + 1,
            },
        })
    }
}
#[tokio::test]
async fn live_feed_traversal_has_total_budget_and_keeps_progress() {
    let rig = Rig::new("pass", "none", false, false);
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    let ctx = BatchCtx {
        batch_id: BatchId("scan".into()),
        bind: rig.bind.clone(),
        run_id: RunId("r".into()),
        scope: None,
    };
    let endless = LongLiveFeed {
        delay: std::time::Duration::from_millis(1),
        positions: Mutex::new(Vec::new()),
        head: None,
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        approval_gate::scan_live_for_test(&endless, &ctx, &mut history),
    )
    .await
    .expect("one live feed traversal must have a total bound");
    assert_eq!(result.unwrap_err().code, "consent_feed_budget_exhausted");
    assert!(endless.positions.lock().unwrap().len() <= 32);
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    rig.finish().await;
}
#[tokio::test]
async fn healthy_live_feed_continues_after_budget_without_restarting() {
    let rig = Rig::new("pass", "none", false, false);
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    let ctx = BatchCtx {
        batch_id: BatchId("scan".into()),
        bind: rig.bind.clone(),
        run_id: RunId("r".into()),
        scope: None,
    };
    let feed = LongLiveFeed {
        delay: std::time::Duration::from_millis(1),
        positions: Mutex::new(Vec::new()),
        head: Some(70),
    };
    let mut budgets = 0;
    loop {
        match approval_gate::scan_live_for_test(&feed, &ctx, &mut history).await {
            Ok(complete) => {
                assert!(complete);
                break;
            }
            Err(error) => {
                assert_eq!(error.code, "consent_feed_budget_exhausted");
                budgets += 1;
                assert!(budgets < 4);
            }
        }
    }
    assert_eq!(budgets, 2);
    assert_eq!(
        *feed.positions.lock().unwrap(),
        (0..=70).collect::<Vec<_>>()
    );
    rig.finish().await;
}

#[tokio::test]
async fn live_feed_slow_pages_share_one_total_time_budget() {
    let rig = Rig::new("pass", "none", false, false);
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    let ctx = BatchCtx {
        batch_id: BatchId("slow-scan".into()),
        bind: rig.bind.clone(),
        run_id: RunId("r".into()),
        scope: None,
    };
    let feed = LongLiveFeed {
        delay: std::time::Duration::from_millis(250),
        positions: Mutex::new(Vec::new()),
        head: None,
    };
    let result = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        approval_gate::scan_live_for_test(&feed, &ctx, &mut history),
    )
    .await
    .expect("slow but healthy pages cannot reset the traversal deadline");
    assert_eq!(result.unwrap_err().code, "consent_feed_budget_exhausted");
    assert!(feed.positions.lock().unwrap().len() <= 4);
    rig.finish().await;
}

#[cfg(feature = "test-clock")]
struct BudgetConflictFeed {
    key: String,
    prepared: WalRecord,
    gets: AtomicUsize,
    entered: std::sync::atomic::AtomicBool,
    positions: Mutex<Vec<u64>>,
}
#[cfg(feature = "test-clock")]
#[async_trait::async_trait]
impl ElicitationPlane for BudgetConflictFeed {
    async fn get(&self, _: &BatchCtx, _: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.gets.fetch_add(1, Ordering::SeqCst);
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
                    provider_incarnation: "budget".into(),
                    seq: 0,
                },
            });
        }
        while !self.entered.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
        tokio::task::yield_now().await;
        let position = cursor.map_or(0, |c| c.seq);
        self.positions.lock().unwrap().push(position);
        let records = if position >= 70 {
            Vec::new()
        } else if position == 0 || position == 69 {
            vec![
                json!({"state":"answered","requester_ref":self.key,"elicitation_id":if position == 0 {"old"} else {"new"},"answered_choice_id":if position == 0 {"approve"} else {"deny"},"binding":crate::consent::binding(&self.prepared)}),
            ]
        } else {
            vec![
                json!({"state":"answered","requester_ref":format!("other-{position}"),"elicitation_id":format!("other-{position}"),"answered_choice_id":"deny","binding":{}}),
            ]
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "budget".into(),
                seq: position + 1,
            },
        })
    }
}
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn live_feed_budget_preserves_prefix_until_later_conflict() {
    let mut rig = expired_run("pass").await;
    // Expiry is already durable; continuation retry deadlines now use real time.
    rig.control.clock = None;
    let mut prepared = rig
        .frames()
        .iter()
        .find(|f| matches!(f.record, WalRecord::ToolCallDeferred { .. }))
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
    let feed = Arc::new(BudgetConflictFeed {
        key: crate::context::call_key(rig.writer.lineage_id().unwrap(), seq),
        prepared: prepared.clone(),
        gets: AtomicUsize::new(0),
        entered: std::sync::atomic::AtomicBool::new(false),
        positions: Mutex::new(Vec::new()),
    });
    rig.control.elicitation_plane = Some(feed.clone());
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
        .batch(&crate::AllowAll, vec![call("held")], Some(resume), false)
        .await;
    assert!(
        matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions"))
    );
    assert_eq!(
        *feed.positions.lock().unwrap(),
        (0..=70).collect::<Vec<_>>()
    );
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    rig.finish().await;
}
