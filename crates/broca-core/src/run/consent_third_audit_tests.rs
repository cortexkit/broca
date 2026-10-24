use super::*;

fn expired_card(rig: &Rig) -> (String, WalRecord, String) {
    let frames = rig.frames();
    let (key, id) = frames
        .iter()
        .find_map(|frame| match &frame.record {
            WalRecord::ApprovalRecorded {
                call_key,
                elicitation_id: Some(id),
                decision: broca_wal::DeferredDecision::Expired,
                ..
            } => Some((call_key.clone(), id.clone())),
            _ => None,
        })
        .unwrap();
    let prepared = frames
        .iter()
        .find(|frame| key.ends_with(&format!(":{}", frame.seq)))
        .unwrap()
        .record
        .clone();
    (key, prepared, id)
}

struct SlowLateFeed {
    key: String,
    prepared: WalRecord,
    id: String,
    positions: Mutex<Vec<u64>>,
}

#[async_trait::async_trait]
impl ElicitationPlane for SlowLateFeed {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let position = cursor.map_or(0, |cursor| cursor.seq);
        tokio::time::sleep(std::time::Duration::from_millis(80)).await;
        self.positions.lock().unwrap().push(position);
        let records = match position {
            0..16 => vec![json!({"requester_ref":"another-call","state":"pending"})],
            16 => vec![
                json!({"state":"answered","requester_ref":self.key,"elicitation_id":self.id,"answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}),
            ],
            _ => vec![],
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "slow-late".into(),
                seq: position + 1,
            },
        })
    }
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn late_admission_continues_healthy_feed_beyond_one_budget() {
    let mut rig = expired_run("pass").await;
    let (key, prepared, id) = expired_card(&rig);
    let feed = Arc::new(SlowLateFeed {
        key: key.clone(),
        prepared,
        id,
        positions: Mutex::new(Vec::new()),
    });
    rig.control.elicitation_plane = Some(feed.clone());
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    for _ in 0..3 {
        approval_gate::admission(
            &mut history,
            &rig.writer,
            &rig.bind,
            &rig.control,
            &rig.tools,
        )
        .await
        .unwrap();
        if rig.tools.provider_requests.load(Ordering::SeqCst) > 0 {
            break;
        }
    }
    assert_eq!(
        rig.tools.provider_requests.load(Ordering::SeqCst),
        1,
        "a healthy late feed must finish across one-second admission budgets"
    );
    assert_eq!(
        feed.positions
            .lock()
            .unwrap()
            .iter()
            .filter(|position| **position == 0)
            .count(),
        1,
        "a budget retry must resume from its last validated late-feed cursor"
    );
    assert_eq!(rig.frames().iter().filter(|frame| matches!(&frame.record, WalRecord::LateApprovalRecorded { call_key, decision: broca_wal::LateDecision::Approved, .. } if call_key == &key)).count(), 1);
    rig.finish().await;
}

struct LateGetRace {
    key: String,
    prepared: WalRecord,
    id: String,
    got: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl ElicitationPlane for LateGetRace {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let records = if self.got.load(Ordering::SeqCst) && cursor.is_none() {
            vec![
                json!({"state":"answered","requester_ref":self.key,"elicitation_id":"opposing","answered_choice_id":"deny","binding":crate::consent::binding(&self.prepared)}),
            ]
        } else {
            vec![]
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "get-race".into(),
                seq: 1,
            },
        })
    }
    async fn get(&self, _: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        assert_eq!(id, self.id);
        self.got.store(true, Ordering::SeqCst);
        Ok(ApprovalQuestion {
            terminal: true,
            record: json!({"state":"answered","requester_ref":self.key,"elicitation_id":id,"answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}),
        })
    }
}

#[tokio::test]
async fn late_approving_get_rechecks_opposing_settlement_before_authorization() {
    let mut rig = expired_run("pass").await;
    let (key, prepared, id) = expired_card(&rig);
    let race = Arc::new(LateGetRace {
        key: key.clone(),
        prepared,
        id,
        got: std::sync::atomic::AtomicBool::new(false),
    });
    rig.control.elicitation_plane = Some(race.clone());
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
    assert!(race.got.load(Ordering::SeqCst));
    assert_eq!(
        rig.tools.provider_requests.load(Ordering::SeqCst),
        0,
        "a late get must not authorize over an opposing owner-feed settlement"
    );
    assert!(!rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateApprovalRecorded { call_key, decision: broca_wal::LateDecision::Approved, .. } if call_key == &key)));
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateResult { call_key, outcome: broca_wal::LateOutcome::NotSent, result } if call_key == &key && matches!(&result.output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions")))));
    rig.finish().await;
}

struct ExpiredAdoptionRace {
    key: String,
    prepared: WalRecord,
    recorded: String,
    entered: std::sync::atomic::AtomicBool,
    withdrew: std::sync::atomic::AtomicBool,
}
#[async_trait::async_trait]
impl ElicitationPlane for ExpiredAdoptionRace {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let records = if self.entered.load(Ordering::SeqCst) && cursor.is_none() {
            vec![
                json!({"state":"answered","requester_ref":self.key,"elicitation_id":"older-approved","answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}),
            ]
        } else {
            vec![]
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "expired-adoption".into(),
                seq: 1,
            },
        })
    }
    async fn get(&self, _: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        if id == self.recorded {
            assert!(
                self.withdrew.load(Ordering::SeqCst),
                "the recorded duplicate is read only after its refused withdrawal"
            );
            Ok(ApprovalQuestion {
                terminal: true,
                record: json!({"state":"answered","requester_ref":self.key,"elicitation_id":id,"answered_choice_id":"deny","binding":crate::consent::binding(&self.prepared)}),
            })
        } else {
            Ok(ApprovalQuestion {
                terminal: false,
                record: json!({"state":"pending","elicitation_id":id}),
            })
        }
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
        if id == self.recorded {
            self.withdrew.store(true, Ordering::SeqCst);
            ApprovalWithdrawal::NotSent {
                reason: "answered".into(),
            }
        } else {
            ApprovalWithdrawal::Acknowledged
        }
    }
}

#[tokio::test]
async fn live_expired_older_approval_proves_recorded_duplicate_before_adoption() {
    let mut rig = expired_run("pass").await;
    let (key, prepared, recorded) = expired_card(&rig);
    rig.control.clock = None;
    let mut held = prepared.clone();
    let WalRecord::ToolCallDeferred { call_id, ask, .. } = &mut held else {
        unreachable!()
    };
    *call_id = ToolCallId("held".into());
    ask.expires_at_ms += 60_000;
    rig.writer.record(held.clone());
    rig.writer.sync().await.unwrap();
    let race = Arc::new(ExpiredAdoptionRace {
        key: key.clone(),
        prepared,
        recorded: recorded.clone(),
        entered: std::sync::atomic::AtomicBool::new(false),
        withdrew: std::sync::atomic::AtomicBool::new(false),
    });
    rig.control.elicitation_plane = Some(race.clone());
    let cancel = CancellationToken::new();
    rig.control.cancel = Some(cancel.clone());
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: Arc::new(held),
            approval: None,
            elicitation_id: Some("waiting-sibling".into()),
        }],
        ..Default::default()
    };
    let interrupt = async {
        loop {
            if rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateApprovalRecorded { call_key, .. } | WalRecord::LateResult { call_key, .. } if call_key == &key)) { cancel.cancel(); return; }
            tokio::task::yield_now().await;
        }
    };
    let (result, ()) = tokio::time::timeout(broca_test_support::EVENT_HANG_GUARD, async {
        tokio::join!(
            rig.batch(&crate::AllowAll, vec![call("held")], Some(resume), false),
            interrupt
        )
    })
    .await
    .expect("expired feed adoption must either settle or refuse");
    assert_eq!(result.interruption, Some(BatchInterruption::CallerCancel));
    assert!(
        race.withdrew.load(Ordering::SeqCst),
        "an older late approval cannot replace the still-answerable recorded duplicate"
    );
    assert!(!rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateApprovalRecorded { call_key, decision: broca_wal::LateDecision::Approved, .. } if call_key == &key)));
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateResult { call_key, outcome: broca_wal::LateOutcome::NotSent, result } if call_key == &key && matches!(&result.output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions")))));
    let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
    assert_eq!(
        history.projection.calls[&key].elicitation_id.as_deref(),
        Some(recorded.as_str())
    );
    rig.control.cancel = None;
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    rig.finish().await;
}
