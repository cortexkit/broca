use super::*;
use crate::tool_plane::*;
use broca_protocol::{BatchId, BindIdentity, RunId};
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Mutex,
    },
};

fn ctx() -> BatchCtx {
    BatchCtx {
        batch_id: BatchId("b".into()),
        bind: BindIdentity::new("/p", "test", "s"),
        run_id: RunId("r".into()),
        scope: None,
    }
}
fn prepared() -> WalRecord {
    crate::replay::deferred_tests::prepared("c", "b")
}
fn record(id: &str, choice: &str) -> Value {
    json!({"state":"answered","elicitation_id":id,"requester_ref":"key","answered_choice_id":choice,"binding":binding(&prepared())})
}
struct Absent;
#[async_trait::async_trait]
impl ElicitationPlane for Absent {}
fn unavailable<T: std::fmt::Debug>(result: Result<T, ApprovalRouteError>) {
    let error = result.unwrap_err();
    assert_eq!(error.code, "elicitation_unavailable");
    assert!(!error.retryable);
}
#[tokio::test]
async fn await_default_refuses_nonretryably() {
    unavailable(Absent.r#await(&ctx(), "id", 120_000).await);
}
#[tokio::test]
async fn ack_default_refuses_nonretryably() {
    unavailable(
        Absent
            .ack(
                &ctx(),
                &ApprovalCursor {
                    provider_incarnation: "i".into(),
                    seq: 1,
                },
            )
            .await,
    );
}
#[tokio::test]
async fn report_execution_default_refuses_nonretryably() {
    unavailable(
        Absent
            .report_execution(&ctx(), "id", ApprovalExecution::Executed)
            .await,
    );
}
#[tokio::test]
async fn detach_default_refuses_nonretryably() {
    unavailable(Absent.detach(&ctx(), "id").await);
}

struct Fake {
    pages: Mutex<VecDeque<Vec<Value>>>,
    events: Mutex<Vec<String>>,
    records: Mutex<Vec<WalRecord>>,
    failure: Mutex<Option<ApprovalRouteError>>,
    answered_withdrawal: bool,
    withdrawal_reason: Option<&'static str>,
    pending_readback: bool,
    reports: Mutex<Vec<ApprovalExecution>>,
    ack_calls: AtomicUsize,
}
impl Fake {
    fn new(pages: Vec<Vec<Value>>) -> Self {
        Self {
            pages: Mutex::new(pages.into()),
            events: Mutex::new(Vec::new()),
            records: Mutex::new(Vec::new()),
            failure: Mutex::new(None),
            answered_withdrawal: false,
            withdrawal_reason: None,
            pending_readback: false,
            reports: Mutex::new(Vec::new()),
            ack_calls: AtomicUsize::new(0),
        }
    }
    async fn durable(&self, record: WalRecord) -> Result<(), ApprovalRouteError> {
        self.events.lock().unwrap().push(match &record {
            WalRecord::AwaitingApproval { pending, .. } => {
                format!("sync:{}", pending[0].elicitation_id.as_deref().unwrap())
            }
            WalRecord::QuestionWithdrawalAcknowledged { .. } => "sync:acknowledgement".into(),
            _ => panic!("unexpected recovery record"),
        });
        self.records.lock().unwrap().push(record);
        Ok(())
    }
}
#[async_trait::async_trait]
impl ElicitationPlane for Fake {
    async fn answers(
        &self,
        _: &BatchCtx,
        _: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        let mut events = self.events.lock().unwrap();
        events.push("answers".into());
        let records = self.pages.lock().unwrap().pop_front().unwrap_or_default();
        Ok(ApprovalPage {
            records,
            cursor: ApprovalCursor {
                provider_incarnation: "owner".into(),
                seq: events.len() as u64,
            },
        })
    }
    async fn request(
        &self,
        _: &BatchCtx,
        _: &str,
        _: &WalRecord,
        _: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        let mut events = self.events.lock().unwrap();
        assert_eq!(
            events.last().map(String::as_str),
            Some("answers"),
            "lookup precedes every refile"
        );
        events.push("request".into());
        if let Some(error) = self.failure.lock().unwrap().take() {
            return Err(error);
        }
        Ok(ApprovalFiling {
            elicitation_id: "new".into(),
            args_hash: String::new(),
        })
    }
    async fn withdraw(&self, _: &BatchCtx, id: &str) -> ApprovalWithdrawal {
        if id == "new" {
            assert!(
                self.events
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|event| event == "sync:new"),
                "returned id must be durable before withdraw"
            );
        }
        self.events.lock().unwrap().push(format!("withdraw:{id}"));
        if let Some(reason) = self.withdrawal_reason {
            return ApprovalWithdrawal::NotSent {
                reason: reason.into(),
            };
        }
        if self.answered_withdrawal {
            ApprovalWithdrawal::NotSent {
                reason: "answered".into(),
            }
        } else {
            ApprovalWithdrawal::Acknowledged
        }
    }
    async fn get(&self, _: &BatchCtx, id: &str) -> Result<ApprovalQuestion, ApprovalRouteError> {
        self.events.lock().unwrap().push("get".into());
        Ok(ApprovalQuestion {
            terminal: !self.pending_readback,
            record: if self.pending_readback {
                json!({"state":"pending","requester_ref":"key","elicitation_id":id})
            } else {
                record(id, "approve")
            },
        })
    }
    async fn report_execution(
        &self,
        _: &BatchCtx,
        _: &str,
        value: ApprovalExecution,
    ) -> Result<(), ApprovalRouteError> {
        assert!(
            self.events
                .lock()
                .unwrap()
                .iter()
                .any(|event| event == "sync:acknowledgement"),
            "report follows durable close-out"
        );
        self.reports.lock().unwrap().push(value);
        Ok(())
    }
    async fn ack(&self, _: &BatchCtx, _: &ApprovalCursor) -> Result<(), ApprovalRouteError> {
        self.ack_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn duplicate_answered_during_withdrawal_refuses_conflicting_decisions() {
    for reason in [None, Some("route_busy")] {
        let mut fake = Fake::new(vec![vec![], vec![record("old", "deny")], vec![]]);
        fake.answered_withdrawal = true;
        fake.withdrawal_reason = reason;
        let error = recover(&fake, &ctx(), "key", &prepared(), 0, |r| fake.durable(r))
            .await
            .unwrap_err();
        assert_eq!(error.code, "consent_conflicting_decisions");
        assert!(!error.retryable);
        assert!(fake.events.lock().unwrap().contains(&"get".into()));
        assert!(!fake.events.lock().unwrap().contains(&"sync:old".into()));
    }
}

#[tokio::test]
async fn unproven_duplicate_withdrawal_does_not_adopt_an_older_answer() {
    let mut fake = Fake::new(vec![vec![], vec![record("old", "approve")], vec![]]);
    fake.withdrawal_reason = Some("route_busy");
    fake.pending_readback = true;
    let error = recover(&fake, &ctx(), "key", &prepared(), 0, |r| fake.durable(r))
        .await
        .unwrap_err();
    assert!(error.retryable);
    assert_eq!(error.code, "consent_withdrawal_unproven");
    assert!(!fake.events.lock().unwrap().contains(&"sync:old".into()));
}

#[tokio::test]
async fn settled_lookup_precedes_refile_and_recovers_without_request() {
    let fake = Fake::new(vec![vec![record("old", "approve")], vec![]]);
    let card = recover(&fake, &ctx(), "key", &prepared(), 0, |r| fake.durable(r))
        .await
        .unwrap();
    assert_eq!(card.id, "old");
    assert_eq!(card.answer.unwrap().decision, ConsentDecision::Approved);
    assert_eq!(
        *fake.events.lock().unwrap(),
        ["answers", "answers", "sync:old"]
    );
    assert_eq!(fake.ack_calls.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn second_lookup_adopts_settlement_and_withdraws_duplicate_after_id_sync() {
    let fake = Fake::new(vec![vec![], vec![record("old", "approve")], vec![]]);
    let card = recover(&fake, &ctx(), "key", &prepared(), 0, |r| fake.durable(r))
        .await
        .unwrap();
    assert_eq!(card.id, "old");
    assert_eq!(
        *fake.events.lock().unwrap(),
        [
            "answers",
            "request",
            "sync:new",
            "answers",
            "answers",
            "withdraw:new",
            "sync:old"
        ]
    );
}
#[tokio::test]
async fn conflicting_settled_decisions_refuse_before_refile() {
    let fake = Fake::new(vec![
        vec![record("a", "approve")],
        vec![record("b", "deny")],
        vec![],
    ]);
    let error = recover(&fake, &ctx(), "key", &prepared(), 0, |r| fake.durable(r))
        .await
        .unwrap_err();
    assert_eq!(error.code, "consent_conflicting_decisions");
    assert!(!error.retryable);
    assert!(fake.records.lock().unwrap().is_empty());
}
#[tokio::test]
async fn crash_recovered_approval_closes_debt_and_reports_without_approval_record() {
    let fake = Fake::new(vec![vec![record("old", "approve")], vec![]]);
    close_out(&fake, &ctx(), "key", &prepared(), None, 0, |r| {
        fake.durable(r)
    })
    .await
    .unwrap();
    assert_eq!(
        *fake.reports.lock().unwrap(),
        [ApprovalExecution::ApprovedNotExecuted]
    );
    assert_eq!(fake.records.lock().unwrap().len(), 2);
    assert!(!fake.records.lock().unwrap().iter().any(|r| matches!(
        r,
        WalRecord::ApprovalRecorded { .. } | WalRecord::ToolDispatchIntent { .. }
    )));
    assert!(!fake
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event.starts_with("withdraw")));
}
#[tokio::test]
async fn recorded_card_answered_while_down_gets_one_not_executed_report() {
    let mut fake = Fake::new(vec![]);
    fake.answered_withdrawal = true;
    close_out(&fake, &ctx(), "key", &prepared(), Some("old"), 0, |r| {
        fake.durable(r)
    })
    .await
    .unwrap();
    assert_eq!(
        *fake.events.lock().unwrap(),
        ["withdraw:old", "get", "sync:acknowledgement"]
    );
    assert_eq!(
        *fake.reports.lock().unwrap(),
        [ApprovalExecution::ApprovedNotExecuted]
    );
}
#[tokio::test]
async fn crash_refile_terminal_refusal_settles_but_retryable_refusal_leaves_debt() {
    for retryable in [false, true] {
        let fake = Fake::new(vec![vec![]]);
        *fake.failure.lock().unwrap() = Some(refusal("expired", retryable));
        let result = close_out(&fake, &ctx(), "key", &prepared(), None, 0, |r| {
            fake.durable(r)
        })
        .await;
        assert_eq!(result.is_err(), retryable);
        assert_eq!(fake.records.lock().unwrap().len(), usize::from(!retryable));
    }
}
#[test]
fn binding_mismatch_and_nonapprove_choices_never_authorize() {
    for field in ["args_hash", "tool", "module", "schema_pin", "extra"] {
        let mut changed = record("id", "approve");
        changed["binding"][field] = json!("different");
        assert_eq!(
            answer(&changed, "key", &prepared()).unwrap_err().code,
            "consent_binding_mismatch"
        );
    }
    assert_eq!(
        answer(&record("id", "unknown"), "key", &prepared())
            .unwrap_err()
            .code,
        "consent_choice_unsupported"
    );
}
#[test]
fn expired_choice_metadata_is_never_approval() {
    let mut expired = record("id", "approve");
    expired["state"] = json!("expired");
    assert_eq!(
        answer(&expired, "key", &prepared())
            .unwrap()
            .unwrap()
            .decision,
        ConsentDecision::None
    );
}

#[tokio::test]
async fn crash_pending_refile_is_durable_before_withdrawal_and_acknowledgement() {
    let fake = Fake::new(vec![vec![], vec![]]);
    close_out(&fake, &ctx(), "key", &prepared(), None, 0, |r| {
        fake.durable(r)
    })
    .await
    .unwrap();
    assert_eq!(
        *fake.events.lock().unwrap(),
        [
            "answers",
            "request",
            "sync:new",
            "answers",
            "withdraw:new",
            "sync:acknowledgement"
        ]
    );
    assert!(fake.reports.lock().unwrap().is_empty());
}

#[tokio::test]
async fn retryable_report_resends_the_same_value_and_terminal_refusal_stops() {
    struct Reporter {
        values: Mutex<Vec<ApprovalExecution>>,
        retryable: bool,
    }
    #[async_trait::async_trait]
    impl ElicitationPlane for Reporter {
        async fn report_execution(
            &self,
            _: &BatchCtx,
            _: &str,
            value: ApprovalExecution,
        ) -> Result<(), ApprovalRouteError> {
            self.values.lock().unwrap().push(value);
            Err(refusal("unavailable", self.retryable))
        }
    }
    for retryable in [false, true] {
        let reporter = Reporter {
            values: Mutex::new(Vec::new()),
            retryable,
        };
        report(
            &reporter,
            &ctx(),
            "id",
            ApprovalExecution::OutcomeUnknown,
            None,
        )
        .await;
        assert_eq!(
            *reporter.values.lock().unwrap(),
            vec![ApprovalExecution::OutcomeUnknown; if retryable { 3 } else { 1 }]
        );
    }
}

#[tokio::test]
async fn maximum_call_key_fits_consent_printable_ascii_bound() {
    let root =
        broca_test_support::scratch_root().join(format!("consent-key-{}", std::process::id()));
    let store = crate::FileWalStore::new(&root);
    let writer =
        crate::WalHandle::spawn(crate::WalStore::open(&store, &ctx().bind).unwrap(), 1).unwrap();
    let key = crate::context::call_key(writer.lineage_id().unwrap(), u64::MAX);
    assert_eq!(key.len(), 57);
    assert!(key.bytes().all(|byte| (0x21..=0x7e).contains(&byte)));
    writer.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn request_return_before_id_sync_child() {
    let Some(root) = std::env::var_os("BROCA_CONSENT_CRASH_CHILD") else {
        return;
    };
    struct DiskCard(std::path::PathBuf);
    #[async_trait::async_trait]
    impl ElicitationPlane for DiskCard {
        async fn answers(
            &self,
            _: &BatchCtx,
            _: Option<&ApprovalCursor>,
        ) -> Result<ApprovalPage, ApprovalRouteError> {
            Ok(ApprovalPage {
                records: Vec::new(),
                cursor: ApprovalCursor {
                    provider_incarnation: "disk".into(),
                    seq: 0,
                },
            })
        }
        async fn request(
            &self,
            _: &BatchCtx,
            _: &str,
            _: &WalRecord,
            _: u64,
        ) -> Result<ApprovalFiling, ApprovalRouteError> {
            let mut card = record("old", "approve");
            card["state"] = json!("pending");
            let path = self.0.join("card.json");
            std::fs::write(&path, serde_json::to_vec(&card).unwrap()).unwrap();
            std::fs::OpenOptions::new()
                .write(true)
                .open(path)
                .unwrap()
                .sync_all()
                .unwrap();
            Ok(ApprovalFiling {
                elicitation_id: "old".into(),
                args_hash: String::new(),
            })
        }
    }
    let root = std::path::PathBuf::from(root);
    let _ = recover(
        &DiskCard(root.clone()),
        &ctx(),
        "key",
        &prepared(),
        0,
        |_record| async {
            std::fs::write(root.join("request-returned"), b"returned before id sync").unwrap();
            std::future::pending::<Result<(), ApprovalRouteError>>().await
        },
    )
    .await;
}

#[tokio::test]
async fn process_kill_between_request_return_and_id_sync_recovers_without_second_card() {
    let root =
        broca_test_support::scratch_root().join(format!("consent-kill-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let mut child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "consent::tests::request_return_before_id_sync_child",
            "--nocapture",
        ])
        .env("BROCA_CONSENT_CRASH_CHILD", &root)
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .unwrap();
    let reached = tokio::time::timeout(Duration::from_secs(2), async {
        let mut tick = tokio::time::interval(Duration::from_millis(5));
        while !root.join("request-returned").exists() {
            tick.tick().await;
        }
    })
    .await;
    child.kill().unwrap();
    let status = child.wait().unwrap();
    assert!(
        reached.is_ok(),
        "child must reach the request-return/id-sync crash gap"
    );
    assert!(!status.success(), "child must actually be killed");
    let mut card: Value =
        serde_json::from_slice(&std::fs::read(root.join("card.json")).unwrap()).unwrap();
    card["state"] = json!("answered");
    let fake = Fake::new(vec![vec![card], vec![]]);
    close_out(&fake, &ctx(), "key", &prepared(), None, 0, |r| {
        fake.durable(r)
    })
    .await
    .unwrap();
    assert!(!fake
        .events
        .lock()
        .unwrap()
        .iter()
        .any(|event| event == "request"));
    assert_eq!(
        *fake.reports.lock().unwrap(),
        [ApprovalExecution::ApprovedNotExecuted]
    );
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn close_out_adopted_id_survives_projection_without_reopening_call() {
    use crate::WalStore;
    let root = broca_test_support::scratch_root()
        .join(format!("consent-projection-{}", std::process::id()));
    let store = crate::FileWalStore::new(&root);
    let writer = crate::WalHandle::spawn(store.open(&ctx().bind).unwrap(), 1).unwrap();
    let prepared = prepared();
    let seq = writer.record(prepared.clone()).into_result().unwrap();
    let key = crate::context::call_key(writer.lineage_id().unwrap(), seq);
    writer.record(WalRecord::ToolResult {
        batch_id: ctx().batch_id,
        result: std::sync::Arc::new(crate::resume::not_dispatched_result(
            &broca_protocol::ToolCallId("c".into()),
        )),
        outcome: broca_wal::DispatchOutcome::Interrupted,
        reason: None,
        hooks: None,
        provider_held: None,
        withdraw_elicitation: Some(broca_wal::WithdrawElicitation {
            call_key: key.clone(),
            elicitation_id: None,
        }),
    });
    writer.record(id_record(&ctx(), &key, &prepared, "new"));
    writer.record(id_record(&ctx(), &key, &prepared, "old"));
    writer.sync().await.unwrap();
    let mut projection = crate::DeferredCallProjection::new(writer.lineage_id().unwrap());
    projection.fold_frames(&store.read_records(&ctx().bind).unwrap());
    assert_eq!(
        projection.calls[&key].elicitation_id.as_deref(),
        Some("old")
    );
    assert_eq!(
        projection.calls[&key].state,
        crate::OpenCallState::Terminated
    );
    assert_eq!(
        projection.obligations().withdrawals[0]
            .withdrawal
            .elicitation_id
            .as_deref(),
        Some("old")
    );
    writer.shutdown().await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}

struct ProgressPages {
    pages: Mutex<std::collections::VecDeque<Result<ApprovalPage, ApprovalRouteError>>>,
    cursors: Mutex<Vec<Option<ApprovalCursor>>>,
}
#[async_trait::async_trait]
impl ElicitationPlane for ProgressPages {
    async fn answers(
        &self,
        _: &BatchCtx,
        cursor: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        self.cursors.lock().unwrap().push(cursor.cloned());
        self.pages
            .lock()
            .unwrap()
            .pop_front()
            .expect("scripted progress page")
    }
}
fn progress_page(records: Vec<Value>, seq: u64) -> Result<ApprovalPage, ApprovalRouteError> {
    Ok(ApprovalPage {
        records,
        cursor: ApprovalCursor {
            provider_incarnation: "stable".into(),
            seq,
        },
    })
}
#[tokio::test]
async fn lookup_retry_preserves_prior_settlements_and_cursor() {
    let feed = ProgressPages {
        pages: Mutex::new(
            vec![
                progress_page(vec![record("old", "deny")], 1),
                Err(refusal("feed_busy", true)),
                progress_page(vec![record("new", "approve")], 2),
                progress_page(Vec::new(), 2),
            ]
            .into(),
        ),
        cursors: Mutex::new(Vec::new()),
    };
    let progress = Mutex::new(LookupProgress::default());
    assert!(
        lookup_with_progress(&feed, &ctx(), "key", &prepared(), &progress)
            .await
            .unwrap_err()
            .retryable
    );
    let error = lookup_with_progress(&feed, &ctx(), "key", &prepared(), &progress)
        .await
        .unwrap_err();
    assert_eq!(error.code, "consent_conflicting_decisions");
    let cursors = feed.cursors.lock().unwrap().clone();
    assert_eq!(cursors[0], None);
    assert_eq!(cursors[1], cursors[2]);
    assert_eq!(cursors[2].as_ref().unwrap().seq, 1);
}
#[tokio::test]
async fn lookup_cursor_change_restarts_progress_and_discards_old_settlements() {
    let feed = ProgressPages {
        pages: Mutex::new(
            vec![
                progress_page(vec![record("old", "deny")], 1),
                Err(refusal("cursor_incarnation_changed", false)),
                progress_page(vec![record("new", "approve")], 2),
                progress_page(Vec::new(), 2),
            ]
            .into(),
        ),
        cursors: Mutex::new(Vec::new()),
    };
    let progress = Mutex::new(LookupProgress::default());
    let found = lookup_with_progress(&feed, &ctx(), "key", &prepared(), &progress)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(found.id, "new");
    assert_eq!(found.decision, ConsentDecision::Approved);
    assert_eq!(feed.cursors.lock().unwrap()[2], None);
}

#[tokio::test]
async fn readback_candidate_survives_snapshot_retry_and_cursor_change() {
    let feed = ProgressPages {
        pages: Mutex::new(
            vec![
                Err(refusal("feed_busy", true)),
                Err(refusal("cursor_incarnation_changed", false)),
                progress_page(Vec::new(), 1),
            ]
            .into(),
        ),
        cursors: Mutex::new(Vec::new()),
    };
    let progress = Mutex::new(LookupProgress::default());
    let candidate = ConsentAnswer {
        id: "new".into(),
        decision: ConsentDecision::Approved,
    };
    assert!(
        verify_readback(
            &feed,
            &ctx(),
            "key",
            &prepared(),
            candidate.clone(),
            &progress
        )
        .await
        .unwrap_err()
        .retryable
    );
    assert_eq!(progress.lock().unwrap().expected(), Some(candidate.clone()));
    assert_eq!(
        verify_readback(
            &feed,
            &ctx(),
            "key",
            &prepared(),
            candidate.clone(),
            &progress
        )
        .await
        .unwrap(),
        candidate
    );
    assert_eq!(*feed.cursors.lock().unwrap(), [None, None, None]);
}

#[test]
fn live_candidate_cannot_erase_an_opposing_buffered_readback() {
    let mut progress = LookupProgress::default();
    let current = ConsentAnswer {
        id: "new".into(),
        decision: ConsentDecision::Approved,
    };
    progress
        .seed_candidate("key", &prepared(), &current)
        .unwrap();
    let older = ConsentAnswer {
        id: "old".into(),
        decision: ConsentDecision::Declined,
    };
    assert_eq!(
        progress
            .seed_candidate("key", &prepared(), &older)
            .unwrap_err()
            .code,
        "consent_conflicting_decisions"
    );
    assert_eq!(progress.expected(), Some(current));
}
