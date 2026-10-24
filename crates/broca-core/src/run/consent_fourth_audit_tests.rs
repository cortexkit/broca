use super::*;

struct EmptyFeed;
#[async_trait::async_trait]
impl ElicitationPlane for EmptyFeed {
    async fn answers(
        &self,
        _: &BatchCtx,
        _: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        Ok(ApprovalPage {
            records: vec![],
            cursor: ApprovalCursor {
                provider_incarnation: "scope-test".into(),
                seq: 0,
            },
        })
    }
}

async fn capability_for(
    key: &str,
    prepared: &WalRecord,
    id: &str,
) -> crate::consent::AuthorizedAnswer {
    let raw = json!({"state":"answered","requester_ref":key,"elicitation_id":id,"answered_choice_id":"approve","binding":crate::consent::binding(prepared)});
    let candidate = crate::consent::answer(&raw, key, prepared)
        .unwrap()
        .unwrap();
    let ctx = BatchCtx {
        batch_id: BatchId("scope-test".into()),
        bind: BindIdentity::new("/project", "test", "scope"),
        run_id: RunId("r".into()),
        scope: None,
    };
    crate::consent::authorize_candidate(
        &EmptyFeed,
        &ctx,
        key,
        prepared,
        None,
        &candidate.record,
        &Mutex::new(crate::consent::LookupProgress::default()),
    )
    .await
    .unwrap()
}

fn another_binding(prepared: &WalRecord) -> WalRecord {
    let mut other = prepared.clone();
    let WalRecord::ToolCallDeferred { module, .. } = &mut other else {
        unreachable!()
    };
    *module = Some("another-module".into());
    other
}

#[tokio::test]
async fn ordinary_approval_writer_rejects_capability_for_another_call_or_binding() {
    for wrong_binding in [false, true] {
        let rig = Rig::new("pass", "none", false, true);
        let paused = rig
            .batch(&crate::AllowAll, vec![call("c")], None, true)
            .await;
        let mut held = paused.open[0].clone();
        let id = held
            .id
            .get_or_insert_with(|| format!("q:{}", held.key))
            .clone();
        let other = if wrong_binding {
            another_binding(&held.prepared)
        } else {
            (*held.prepared).clone()
        };
        let key = if wrong_binding {
            held.key.as_str()
        } else {
            "another-call"
        };
        let proof = capability_for(key, &other, &id).await;
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        let result =
            approval_gate::approved_decision(&held, proof, &rig.writer, &mut history).await;
        assert!(
            result.is_err(),
            "ordinary approval must check its capability key and binding"
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
}

#[tokio::test]
async fn late_approval_writer_rejects_capability_for_another_call_or_binding() {
    for wrong_binding in [false, true] {
        let rig = expired_run("pass").await;
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
        let prepared = &frames
            .iter()
            .find(|frame| key.ends_with(&format!(":{}", frame.seq)))
            .unwrap()
            .record;
        let other = if wrong_binding {
            another_binding(prepared)
        } else {
            prepared.clone()
        };
        let proof = capability_for(
            if wrong_binding { &key } else { "another-call" },
            &other,
            &id,
        )
        .await;
        let mut history = approval_gate::History::new(&rig.writer, rig.frames()).unwrap();
        let result = approval_gate::record_late_answer(
            &mut history,
            &rig.writer,
            &key,
            proof,
            broca_wal::LateExecution::Execute,
        )
        .await;
        assert!(
            result.is_err(),
            "late approval must check its capability key and binding"
        );
        assert!(!rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::LateApprovalRecorded {
                decision: broca_wal::LateDecision::Approved,
                ..
            }
        )));
        rig.finish().await;
    }
}

struct RetryDuplicateFeed {
    key: String,
    prepared: WalRecord,
    recorded: String,
    deny_on_retry: bool,
    withdrawals: AtomicUsize,
    tail_reads: AtomicUsize,
}
#[async_trait::async_trait]
impl ElicitationPlane for RetryDuplicateFeed {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let withdrawals = self.withdrawals.load(Ordering::SeqCst);
        if withdrawals >= 2 {
            self.tail_reads.fetch_add(1, Ordering::SeqCst);
        }
        let deny = self.deny_on_retry && withdrawals > 0;
        let closed = withdrawals >= 2;
        let since = cursor.map_or(0, |cursor| cursor.seq);
        let mut records = vec![];
        if since < 1 {
            records.push(json!({"state":"answered","requester_ref":self.key,"elicitation_id":"A","answered_choice_id":"approve","binding":crate::consent::binding(&self.prepared)}));
        }
        if deny && since < 2 {
            records.push(json!({"state":"answered","requester_ref":self.key,"elicitation_id":"C","answered_choice_id":"deny","binding":crate::consent::binding(&self.prepared)}));
        }
        let closed_seq = if deny { 3 } else { 2 };
        if closed && since < closed_seq {
            records.push(json!({"state":"withdrawn","requester_ref":self.key,"elicitation_id":self.recorded,"binding":crate::consent::binding(&self.prepared)}));
        }
        let head = if closed {
            closed_seq
        } else if deny {
            2
        } else {
            1
        };
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "retry-tail".into(),
                seq: head,
            },
        })
    }
    async fn withdraw(&self, _: &BatchCtx, id: &str) -> ApprovalWithdrawal {
        assert_eq!(id, self.recorded);
        if self.withdrawals.fetch_add(1, Ordering::SeqCst) == 0 {
            ApprovalWithdrawal::Retry(crate::consent::refusal("withdraw_busy", true))
        } else {
            ApprovalWithdrawal::Acknowledged
        }
    }
    async fn get(&self, _: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        assert_eq!(id, self.recorded);
        assert!(self.withdrawals.load(Ordering::SeqCst) >= 2);
        Ok(ApprovalQuestion {
            terminal: true,
            record: json!({"state":"withdrawn","requester_ref":self.key,"elicitation_id":id,"binding":crate::consent::binding(&self.prepared)}),
        })
    }
}

async fn late_retry_tail(deny: bool) {
    let mut rig = expired_run("pass").await;
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
    let feed = Arc::new(RetryDuplicateFeed {
        key: key.clone(),
        prepared,
        recorded: id.clone(),
        deny_on_retry: deny,
        withdrawals: AtomicUsize::new(0),
        tail_reads: AtomicUsize::new(0),
    });
    rig.control.elicitation_plane = Some(feed.clone());
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
    assert_eq!(feed.withdrawals.load(Ordering::SeqCst), 1);
    assert!(history.projection.calls[&key].late_approval.is_none());
    assert_eq!(rig.tools.provider_requests.load(Ordering::SeqCst), 0);
    approval_gate::admission(
        &mut history,
        &rig.writer,
        &rig.bind,
        &rig.control,
        &rig.tools,
    )
    .await
    .unwrap();
    if deny {
        assert_eq!(
            rig.tools.provider_requests.load(Ordering::SeqCst),
            0,
            "C's denial after A's completed scan must defeat later duplicate closure"
        );
        assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateResult { call_key, outcome: broca_wal::LateOutcome::NotSent, result } if call_key == &key && matches!(&result.output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions")))));
        assert_eq!(
            history.projection.calls[&key].elicitation_id.as_deref(),
            Some(id.as_str())
        );
    } else {
        assert_eq!(
            rig.tools.provider_requests.load(Ordering::SeqCst),
            1,
            "a proven withdrawn duplicate is not an opposing unanswered settlement"
        );
        assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::LateApprovalRecorded { call_key, decision: broca_wal::LateDecision::Approved, .. } if call_key == &key)));
    }
    assert!(
        feed.tail_reads.load(Ordering::SeqCst) > 0,
        "every minting attempt must read the retained-cursor tail after duplicate closure"
    );
    rig.finish().await;
}

#[tokio::test]
async fn late_retry_reads_new_denial_after_duplicate_proof_failure() {
    late_retry_tail(true).await;
}

#[tokio::test]
async fn late_retry_accepts_proven_withdrawn_duplicate_after_fresh_tail() {
    late_retry_tail(false).await;
}

#[tokio::test]
async fn held_retry_reads_new_denial_after_duplicate_proof_failure() {
    let mut rig = Rig::new("pass", "none", false, true);
    let paused = rig
        .batch(&crate::AllowAll, vec![call("c")], None, true)
        .await;
    let held = &paused.open[0];
    let id = held.id.clone().unwrap_or_else(|| format!("q:{}", held.key));
    let WalRecord::ToolCallDeferred { call_id, ask, .. } = &*held.prepared else {
        unreachable!()
    };
    rig.writer.record(WalRecord::AwaitingApproval {
        batch_id: BatchId("r-b1".into()),
        pending: vec![broca_wal::AwaitingApprovalEntry {
            call_key: held.key.clone(),
            call_id: call_id.clone(),
            elicitation_id: Some(id.clone()),
            expires_at_ms: ask.expires_at_ms,
        }],
    });
    rig.writer.sync().await.unwrap();
    let feed = Arc::new(RetryDuplicateFeed {
        key: held.key.clone(),
        prepared: (*held.prepared).clone(),
        recorded: id.clone(),
        deny_on_retry: true,
        withdrawals: AtomicUsize::new(0),
        tail_reads: AtomicUsize::new(0),
    });
    rig.control.stop_requested = None;
    rig.control.elicitation_plane = Some(feed.clone());
    let resume = ResumeCtx {
        batch_already_started: true,
        existing: Default::default(),
        prepared: vec![crate::replay::PreparedCall {
            record_index: 0,
            prepared: held.prepared.clone(),
            approval: None,
            elicitation_id: Some(id),
        }],
        ..Default::default()
    };
    let result = rig
        .batch(&crate::AllowAll, vec![call("c")], Some(resume), true)
        .await;
    assert_eq!(
        rig.tools.provider_requests.load(Ordering::SeqCst),
        0,
        "held retries cannot authorize A from the completed snapshot before C's denial"
    );
    assert!(
        matches!(&result.results[0].output, ToolOutput::Error { message } if message.contains("consent_conflicting_decisions"))
    );
    assert!(feed.tail_reads.load(Ordering::SeqCst) > 0);
    assert!(!rig.frames().iter().any(|frame| matches!(
        frame.record,
        WalRecord::ApprovalRecorded {
            decision: broca_wal::DeferredDecision::Approved,
            ..
        }
    )));
    rig.finish().await;
}
