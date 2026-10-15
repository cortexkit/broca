use super::tests::{min_config, text_req};
use super::*;
use crate::opener::DefaultOpener;
use crate::registry::LineageRegistry;
use broca_core::tool_plane::{
    ApprovalCursor, ApprovalFiling, ApprovalPage, ApprovalRouteError, ApprovalWithdrawal,
    ElicitationPlane,
};
use broca_core::{
    AllowAll, BatchCtx, DispatchResult, FileLeaseStore, FileWalStore, StartMode, StubProvider,
    ToolManifest, ToolPlane, ToolPlaneError,
};
use broca_protocol::{StepId, ToolCall, ToolCallId};
use std::sync::Mutex;

#[derive(Default)]
struct Questions {
    retrying: std::sync::atomic::AtomicBool,
    requests: Mutex<Vec<String>>,
    withdrawals: Mutex<Vec<String>>,
    replies: Mutex<std::collections::VecDeque<ApprovalWithdrawal>>,
}
#[async_trait::async_trait]
impl ElicitationPlane for Questions {
    async fn request(
        &self,
        ctx: &BatchCtx,
        key: &str,
        prepared: &WalRecord,
        _: u64,
    ) -> Result<ApprovalFiling, ApprovalRouteError> {
        let WalRecord::ToolCallDeferred {
            scope, args_hash, ..
        } = prepared
        else {
            panic!("not Prepared")
        };
        assert_eq!(ctx.scope.as_ref(), Some(scope));
        assert_eq!(ctx.run_id.0, "r");
        self.requests.lock().unwrap().push(key.into());
        Ok(ApprovalFiling {
            elicitation_id: "recovered-id".into(),
            args_hash: args_hash.clone(),
        })
    }
    async fn answers(
        &self,
        _: &BatchCtx,
        _: Option<&ApprovalCursor>,
    ) -> Result<ApprovalPage, ApprovalRouteError> {
        Ok(ApprovalPage {
            records: Vec::new(),
            cursor: ApprovalCursor {
                provider_incarnation: "p".into(),
                seq: 0,
            },
        })
    }
    async fn withdraw(&self, _: &BatchCtx, id: &str) -> ApprovalWithdrawal {
        self.withdrawals.lock().unwrap().push(id.into());
        if self.retrying.load(Ordering::Acquire) {
            return ApprovalWithdrawal::Retry(ApprovalRouteError {
                code: "elicitation_unavailable".into(),
                retryable: true,
            });
        }
        self.replies
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or(ApprovalWithdrawal::Acknowledged)
    }
}
struct NoTools;
#[async_trait::async_trait]
impl ToolPlane for NoTools {
    async fn batch_started(
        &self,
        _: &BatchCtx,
        _: &[ToolCall],
        _: &ToolManifest,
    ) -> Result<(), ToolPlaneError> {
        Ok(())
    }
    async fn dispatch(&self, _: &BatchCtx, _: &ToolCall) -> DispatchResult {
        panic!("a closed or unanswered call must never execute")
    }
    async fn batch_finished(&self, _: &BatchCtx) -> Result<(), ToolPlaneError> {
        Ok(())
    }
}
struct Executor {
    wal: Arc<FileWalStore>,
    provider: StubProvider,
    questions: Option<Arc<Questions>>,
}
#[async_trait::async_trait]
impl RunExecutor for Executor {
    fn elicitation_plane(&self) -> Option<Arc<dyn ElicitationPlane>> {
        self.questions
            .clone()
            .map(|plane| plane as Arc<dyn ElicitationPlane>)
    }
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        let mode = match job.kind {
            RunKind::FreshOrResume => StartMode::FreshOrResume,
            RunKind::AppendEpisode => StartMode::AppendEpisode,
            RunKind::ResumeAfterAuth => StartMode::ResumeAfterAuth,
        };
        broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
            job.req,
            &job.writer,
            mode,
            &self.provider,
            &NoTools,
            &AllowAll,
            self.wal.as_ref(),
            None,
            job.control,
            &out,
            job.on_admitted,
            Some(&job.prior_snapshot),
        )
        .await
    }
}
struct Rig {
    root: std::path::PathBuf,
    key: LineageKey,
    wal: Arc<FileWalStore>,
    store: Arc<broca_store::SqliteStore>,
    executor: Arc<Executor>,
    registry: LineageRegistry,
    full_reads: Arc<std::sync::atomic::AtomicUsize>,
}
impl Rig {
    fn new(writer: bool) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "approval-module-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let key = LineageKey::new(root.join("project"), "runner", "session");
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open(root.join("store.db")).unwrap());
        let executor = Arc::new(Executor {
            wal: wal.clone(),
            provider: StubProvider::new(vec![]),
            questions: writer.then(|| Arc::new(Questions::default())),
        });
        let full_reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counted = Arc::new(CountedWal {
            count_when: None,
            wal: wal.clone(),
            full_reads: full_reads.clone(),
        });
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(root.join("leases"))),
            counted.clone(),
            executor.clone(),
        )
        .with_run_index(store.clone())
        .with_materialized_store(store.clone());
        let registry =
            LineageRegistry::new_with_reader(Arc::new(opener), counted, Some(store.clone()));
        Self {
            root,
            key,
            wal,
            store,
            executor,
            registry,
            full_reads,
        }
    }
    fn seed(&self, on_restart: bool, paused: bool, approved: bool, closed: bool) -> String {
        self.seed_at(on_restart, paused, approved, closed, u64::MAX)
    }
    fn seed_at(
        &self,
        on_restart: bool,
        paused: bool,
        approved: bool,
        closed: bool,
        expiry: u64,
    ) -> String {
        let req = text_req(&self.key, "r");
        use broca_core::Provider;
        let mut config = min_config();
        config.renderer_version = self.executor.provider.renderer_version();
        config.frozen_render_config = self.executor.provider.render_config_blob();
        let scope: broca_protocol::SessionScope = serde_json::from_value(
            serde_json::json!({"owner":"agent","ref":"agent:a","scope_epoch":1}),
        )
        .unwrap();
        let mut file = self.wal.open(&self.key).unwrap();
        let records = vec![
            WalRecord::RunStarted { retention: None, hooks: None, title: None, on_restart: on_restart.then_some(broca_protocol::request::OnRestart::Resume), keep_warm: None, marker_order_version: None, manifest: None, scope: Some(scope.clone()), send_id: None, request_identity_digest: None, project_id: None, principal: None, ts_ms: None, run_id: RunId("r".into()), session: self.key.clone(), config: Box::new(config), input: req.input.into(), origin: broca_wal::RunOrigin::Fresh, submission_id: None },
            WalRecord::ModelStepFinished { hooks: None, step_id: StepId(1), assistant_message: broca_protocol::AssistantMessage { message_id: broca_protocol::MessageId("m".into()), content: vec![broca_protocol::ContentBlock::ToolCall(ToolCall { thought_signature: None, tool_call_id: ToolCallId("c".into()), tool_name: "edit".into(), input: serde_json::json!({}) })] }.into(), usage: Default::default(), finish_reason: Some(broca_protocol::FinishReason::ToolCalls), retries_used: 0, service_tier_used: None, speed_used: None },
            WalRecord::ToolBatchStarted { batch_id: broca_protocol::BatchId("b".into()), step_id: StepId(1), planned_calls: vec![ToolCallId("c".into())] },
            WalRecord::ToolCallDeferred { batch_id: broca_protocol::BatchId("b".into()), call_id: ToolCallId("c".into()), executor: broca_wal::DeferredExecutor::Broca, module: None, tool: "edit".into(), schema_pin: None, args_jcs: "{}".into(), args_hash: "44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a".into(), scope, ask: serde_json::from_value(serde_json::json!({"prompt":"Allow?","expires_at_ms":expiry,"on_expiry":"deny","material_damage":false,"late_execution":"notify_only"})).unwrap() },
        ];
        for record in records {
            file.append(1, &record, true).unwrap();
        }
        drop(file);
        let frames = self.wal.read_records(&self.key).unwrap();
        let range = self
            .wal
            .read_records_range(&self.key, 0, None, None)
            .unwrap();
        let key = format!(
            "{}:{}",
            range.lineage_id.unwrap(),
            frames.last().unwrap().seq
        );
        if approved {
            self.append(WalRecord::ApprovalRecorded {
                call_key: key.clone(),
                call_id: ToolCallId("c".into()),
                elicitation_id: Some("answered-id".into()),
                decision: broca_wal::DeferredDecision::Approved,
                approval_digest: Some("digest".into()),
                closing_result: None,
            });
        }
        if paused {
            self.append(WalRecord::RunPaused {
                run_id: RunId("r".into()),
                ts_ms: None,
                reason: broca_protocol::PauseReason::Restart,
                error: None,
            });
        }
        if closed {
            self.append(Self::closed_result(&key, false));
            self.append(WalRecord::ToolBatchFinished {
                batch_id: broca_protocol::BatchId("b".into()),
            });
            self.append(WalRecord::TurnFinished { step_id: StepId(1) });
            self.append(WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                ts_ms: None,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            });
        }
        self.store
            .index_run_active(&RunIndexRow {
                run_id: "r".into(),
                session: self.key.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: if paused {
                    RunIndexState::Paused
                } else {
                    RunIndexState::Active
                },
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
                title: None,
                withdrawals_owed: false,
            })
            .unwrap();
        key
    }
    fn closed_result(key: &str, writer: bool) -> WalRecord {
        WalRecord::ToolResult {
            hooks: None,
            batch_id: broca_protocol::BatchId("b".into()),
            result: broca_core::not_dispatched_result(&ToolCallId("c".into())).into(),
            outcome: broca_wal::DispatchOutcome::Interrupted,
            reason: None,
            withdraw_elicitation: writer.then(|| broca_wal::WithdrawElicitation {
                call_key: key.into(),
                elicitation_id: None,
            }),
            provider_held: None,
        }
    }
    fn append(&self, record: WalRecord) {
        self.wal
            .open(&self.key)
            .unwrap()
            .append(1, &record, true)
            .unwrap();
    }
    fn frames(&self) -> Vec<broca_wal::FramedRecord> {
        self.wal.read_records(&self.key).unwrap()
    }
    fn row(&self) -> RunIndexRow {
        self.store.lookup_run("r").unwrap().unwrap()
    }
    async fn eventually(&self, test: impl Fn() -> bool) {
        broca_test_support::event_timeout(
            "eventually",
            async {
                while !test() {
                    tokio::task::yield_now().await;
                }
            },
            || {
                format!(
                    "run_index={:?}; {}",
                    self.store.lookup_run("r"),
                    broca_test_support::record_kinds(self.frames().iter().map(|f| &f.record))
                )
            },
        )
        .await
        .expect("approval state did not converge");
    }
    async fn stop(&self) {
        assert!(self.registry.stop_and_seal().await);
        assert_eq!(
            self.registry.actor_panic_count(),
            0,
            "the session actor panicked during teardown"
        );
    }
}

#[tokio::test]
async fn crash_seal_reader_closes_prepared_and_flags_unprojected_debt() {
    for approved in [false, true] {
        let rig = Rig::new(false);
        rig.seed(true, false, approved, false);
        let marked = rig.registry.mark_crash_orphans(
            rig.wal.as_ref(),
            vec![(RunId("r".into()), rig.key.clone())],
            std::time::SystemTime::now() + Duration::from_secs(1),
        );
        assert_eq!(
            rig.registry.seal_marked_crash_orphans(marked).await.sealed,
            1
        );
        assert_eq!(rig.row().state, RunIndexState::Interrupted);
        assert_eq!(rig.row().withdrawals_owed, !approved);
        assert!(rig.frames().iter().any(|frame| matches!(
            &frame.record,
            WalRecord::ToolResult {
                withdraw_elicitation: None,
                outcome: broca_wal::DispatchOutcome::Interrupted,
                ..
            }
        )));
        assert!(broca_core::replay(
            &rig.frames()
                .into_iter()
                .map(|frame| frame.record)
                .collect::<Vec<_>>()
        )
        .unwrap()
        .open_tool_batch
        .is_none());
        rig.stop().await;
    }
}

#[tokio::test]
async fn crash_orphan_repair_flags_reader_debt_before_projection() {
    let rig = Rig::new(false);
    rig.seed(true, false, false, true);
    let marked = rig.registry.mark_crash_orphans(
        rig.wal.as_ref(),
        vec![(RunId("r".into()), rig.key.clone())],
        std::time::SystemTime::now() + Duration::from_secs(1),
    );
    let repaired = rig.registry.seal_marked_crash_orphans(marked).await;
    assert_eq!(repaired.index_repaired, 1);
    assert!(rig.row().withdrawals_owed);
    rig.stop().await;
}

#[tokio::test]
async fn paused_cancel_finalizer_flags_debt_before_projection_and_withdraws() {
    let rig = Rig::new(true);
    let key = rig.seed(true, true, false, false);
    let questions = rig.executor.questions.as_ref().unwrap();
    questions
        .replies
        .lock()
        .unwrap()
        .push_back(ApprovalWithdrawal::Retry(ApprovalRouteError {
            code: "elicitation_unavailable".into(),
            retryable: true,
        }));
    assert_eq!(
        rig.registry
            .route_cancel(&rig.key, RunId("r".into()))
            .await
            .unwrap(),
        CancelOutcome::Ack
    );
    rig.eventually(|| rig.row().state == RunIndexState::Cancelled)
        .await;
    assert!(rig.row().withdrawals_owed);
    rig.eventually(|| !rig.row().withdrawals_owed).await;
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::ToolResult { withdraw_elicitation: Some(withdrawal), .. } if withdrawal.call_key == key)));
    assert_eq!(questions.withdrawals.lock().unwrap().len(), 2);
    assert!(rig.executor.provider.calls().is_empty());
    rig.stop().await;
}

#[tokio::test]
async fn refused_reader_restart_resume_seals_prepared_calls() {
    let rig = Rig::new(false);
    rig.seed(true, true, false, false);
    assert_eq!(
        rig.registry
            .resume_restart_pauses(rig.wal.clone(), vec![("r".into(), rig.key.clone())])
            .await,
        (0, 1)
    );
    assert!(rig.row().withdrawals_owed);
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::RunFinished { error: Some(error), .. } if error.provider_code.as_deref() == Some("restart_resume_refused"))));
    assert!(rig.executor.provider.calls().is_empty());
    rig.stop().await;
}

#[tokio::test]
async fn unmarked_running_approval_honours_on_restart() {
    for resume in [false, true] {
        let rig = Rig::new(true);
        let key = rig.seed(resume, false, false, false);
        rig.registry.route_status(&rig.key).await.unwrap();
        if resume {
            rig.eventually(|| {
                !rig.executor
                    .questions
                    .as_ref()
                    .unwrap()
                    .requests
                    .lock()
                    .unwrap()
                    .is_empty()
            })
            .await;
            assert_eq!(
                rig.executor
                    .questions
                    .as_ref()
                    .unwrap()
                    .requests
                    .lock()
                    .unwrap()[0],
                key
            );
            assert_eq!(rig.row().state, RunIndexState::Active, "{:?}", rig.frames());
            assert!(rig
                .executor
                .questions
                .as_ref()
                .unwrap()
                .withdrawals
                .lock()
                .unwrap()
                .is_empty());
            rig.registry
                .route_cancel(&rig.key, RunId("r".into()))
                .await
                .unwrap();
            rig.eventually(|| rig.row().state == RunIndexState::Cancelled)
                .await;
        } else {
            assert_eq!(rig.row().state, RunIndexState::Interrupted);
            assert!(rig.frames().iter().any(|frame| matches!(
                &frame.record,
                WalRecord::ToolResult {
                    withdraw_elicitation: Some(_),
                    ..
                }
            )));
        }
        assert!(rig.executor.provider.calls().is_empty());
        rig.stop().await;
    }
}

#[tokio::test]
async fn withdrawal_refiles_same_key_retries_and_stops_for_each_terminal_class() {
    for reason in [
        None,
        Some("answered"),
        Some("expired"),
        Some("elicitation_not_owner"),
        Some("elicitation_not_found"),
        Some("scope_ended"),
    ] {
        let rig = Rig::new(true);
        let key = rig.seed(true, false, false, true);
        rig.store
            .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
            .unwrap();
        rig.store.index_run_withdrawals_owed("r", true).unwrap();
        let questions = rig.executor.questions.as_ref().unwrap();
        questions.replies.lock().unwrap().extend([
            ApprovalWithdrawal::Retry(ApprovalRouteError {
                code: "scope_not_synced".into(),
                retryable: true,
            }),
            reason.map_or(ApprovalWithdrawal::Acknowledged, |reason| {
                ApprovalWithdrawal::NotSent {
                    reason: reason.into(),
                }
            }),
        ]);
        rig.registry.route_status(&rig.key).await.unwrap();
        rig.eventually(|| !rig.row().withdrawals_owed).await;
        assert_eq!(
            questions.withdrawals.lock().unwrap().as_slice(),
            ["recovered-id", "recovered-id"]
        );
        assert!(questions
            .requests
            .lock()
            .unwrap()
            .iter()
            .all(|requested| requested == &key));
        assert_eq!(rig.frames().iter().filter(|frame| matches!(&frame.record, WalRecord::QuestionWithdrawalAcknowledged { call_key, .. } if call_key == &key)).count(), 1);
        rig.registry.route_status(&rig.key).await.unwrap();
        assert_eq!(questions.withdrawals.lock().unwrap().len(), 2);
        rig.stop().await;
    }
}

#[tokio::test]
async fn approved_closed_call_has_no_debt_and_sends_no_withdrawal() {
    let rig = Rig::new(true);
    rig.seed(true, false, true, true);
    rig.registry.route_status(&rig.key).await.unwrap();
    assert!(!rig.row().withdrawals_owed);
    assert!(rig
        .executor
        .questions
        .as_ref()
        .unwrap()
        .withdrawals
        .lock()
        .unwrap()
        .is_empty());
    rig.stop().await;
}

#[test]
fn stale_marker_cannot_reopen_an_approved_or_closed_call() {
    let rig = Rig::new(false);
    let key = rig.seed(true, false, true, false);
    rig.append(WalRecord::AwaitingApproval {
        batch_id: broca_protocol::BatchId("b".into()),
        pending: vec![broca_wal::AwaitingApprovalEntry {
            call_key: key,
            call_id: ToolCallId("c".into()),
            elicitation_id: Some("stale".into()),
            expires_at_ms: u64::MAX,
        }],
    });
    let range = rig.wal.read_records_range(&rig.key, 0, None, None).unwrap();
    let projection =
        crate::approval::projection_from_frames(range.lineage_id, &rig.frames()).unwrap();
    assert!(crate::approval::awaiting_approval(&projection, &RunId("r".into())).is_none());
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn writer_seal_records_withdrawal_and_flags_debt_before_projection() {
    let rig = Rig::new(true);
    rig.seed(true, false, false, false);
    let questions = rig.executor.questions.as_ref().unwrap();
    questions.retrying.store(true, Ordering::Release);
    let marked = rig.registry.mark_crash_orphans(
        rig.wal.as_ref(),
        vec![(RunId("r".into()), rig.key.clone())],
        std::time::SystemTime::now() + Duration::from_secs(1),
    );
    assert_eq!(
        rig.registry.seal_marked_crash_orphans(marked).await.sealed,
        1
    );
    assert!(rig.row().withdrawals_owed);
    assert!(rig.frames().iter().any(|frame| matches!(
        &frame.record,
        WalRecord::ToolResult {
            withdraw_elicitation: Some(_),
            ..
        }
    )));
    questions.retrying.store(false, Ordering::Release);
    rig.eventually(|| !rig.row().withdrawals_owed).await;
    rig.stop().await;
}

#[tokio::test]
async fn expired_withdrawal_is_acknowledged_without_any_route_op() {
    let rig = Rig::new(true);
    rig.seed_at(true, false, false, true, 1);
    rig.store
        .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
        .unwrap();
    rig.store.index_run_withdrawals_owed("r", true).unwrap();
    rig.registry.route_status(&rig.key).await.unwrap();
    rig.eventually(|| !rig.row().withdrawals_owed).await;
    let questions = rig.executor.questions.as_ref().unwrap();
    assert!(questions.requests.lock().unwrap().is_empty());
    assert!(questions.withdrawals.lock().unwrap().is_empty());
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::QuestionWithdrawalAcknowledged { reason: Some(reason), answer: broca_wal::WithdrawalAnswer::NotSent, .. } if reason == "expired")));
    rig.stop().await;
}

#[test]
fn unread_debt_is_not_empty_debt() {
    for frames in [
        Vec::new(),
        vec![broca_wal::FramedRecord {
            seq: 1,
            fence: 1,
            record: WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                ts_ms: None,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
        }],
    ] {
        assert!(crate::approval::complete_frames(
            broca_wal::WalRangeRead::without_anchors(frames),
            1
        )
        .is_err());
    }
    assert!(crate::approval::complete_frames(
        broca_wal::WalRangeRead::absent_after_archive_lookup(),
        1
    )
    .is_err());
}

#[tokio::test]
async fn pending_status_uses_call_states_and_the_latest_filing_marker() {
    use crate::serve::ServeHandler;
    let rig = Rig::new(true);
    let key = rig.seed(true, false, false, false);
    rig.registry.route_status(&rig.key).await.unwrap();
    // A complete marker frame on disk may still be awaiting fsync. Wait on the served,
    // durable status instead of treating a raw WAL read as proof that filing was synced.
    let status = broca_test_support::event_timeout("pending_status_uses_call_states_and_the_latest_filing_marker", async {
        loop {
            let status = rig.registry.run_status(&rig.key, RunId("r".into())).await.unwrap();
            if matches!(&status, broca_wire::RunStatusResponse::Active { awaiting_approval: Some(pending), .. }
                if pending.pending.iter().any(|entry| entry.elicitation_id.is_some())) { break status; }
            tokio::task::yield_now().await;
        }
    }, || format!("run_index={:?}; {}", rig.store.lookup_run("r"), broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record)))).await.expect("filing marker was not reported durably");
    let page = rig
        .registry
        .read(crate::serve::SessionReadRequest::new(
            rig.key.clone(),
            Some(0),
            Some(100),
        ))
        .await
        .unwrap();
    assert!(
        matches!(page.open_batch, Some(broca_wire::OpenBatchView::Open { calls, .. }) if calls.len() == 1 && calls[0].state == broca_core::OpenCallState::Deferred)
    );

    assert!(
        matches!(status, broca_wire::RunStatusResponse::Active { awaiting_approval: Some(pending), .. }
        if pending.pending.len() == 1 && pending.pending[0].call_key == key && pending.pending[0].elicitation_id.as_deref() == Some("recovered-id"))
    );
    rig.registry
        .route_cancel(&rig.key, RunId("r".into()))
        .await
        .unwrap();
    rig.eventually(|| rig.row().state == RunIndexState::Cancelled)
        .await;
    rig.stop().await;
}

#[tokio::test]
async fn projected_approval_metadata_matches_wal_session_read() {
    use crate::serve::{ServeHandler, SessionReadRequest};
    use broca_core::Persister;
    for decision in [
        None,
        Some(broca_wal::DeferredDecision::Declined),
        Some(broca_wal::DeferredDecision::Expired),
    ] {
        let rig = Rig::new(false);
        let key = rig.seed(true, false, false, decision.is_none());
        if let Some(decision) = decision {
            let result = broca_core::not_dispatched_result(&ToolCallId("c".into()));
            rig.append(WalRecord::ApprovalRecorded {
                call_key: key.clone(),
                call_id: ToolCallId("c".into()),
                elicitation_id: Some("el".into()),
                decision,
                approval_digest: None,
                closing_result: Some(result.into()),
            });
            rig.append(WalRecord::ToolBatchFinished {
                batch_id: broca_protocol::BatchId("b".into()),
            });
            rig.append(WalRecord::TurnFinished { step_id: StepId(1) });
            rig.append(WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                ts_ms: None,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            });
            if decision == broca_wal::DeferredDecision::Expired {
                rig.append(WalRecord::LateApprovalRecorded {
                    call_key: key.clone(),
                    elicitation_id: "el".into(),
                    decision: broca_wal::LateDecision::Approved,
                    approval_digest: Some("digest".into()),
                    late_execution: Some(broca_wal::LateExecution::NotifyOnly),
                });
            }
        }
        let request = || SessionReadRequest::new(rig.key.clone(), Some(0), Some(100));
        let before = rig.registry.read(request()).await.unwrap();
        let range = rig.wal.read_records_range(&rig.key, 0, None, None).unwrap();
        let watermark = rig.store.project_range(&rig.key, &range).unwrap();
        assert!(
            watermark >= 7,
            "premise: the projection covered the terminal"
        );
        let after = rig.registry.read(request()).await.unwrap();
        assert_eq!(after, before);
        assert_eq!(
            after.deferred_calls.as_ref().unwrap()[0].state,
            broca_core::OpenCallState::Terminated
        );
        assert_eq!(
            after.late_approvals.as_ref().unwrap().len(),
            usize::from(decision == Some(broca_wal::DeferredDecision::Expired))
        );
        rig.stop().await;
    }
}

#[tokio::test]
async fn retry_keeps_discovery_flag_until_full_wal_debt_is_empty() {
    let rig = Rig::new(true);
    rig.seed(true, false, false, true);
    rig.store
        .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
        .unwrap();
    rig.store.index_run_withdrawals_owed("r", true).unwrap();
    let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
    let questions = rig.executor.questions.as_ref().unwrap();
    let lock = Arc::new(Mutex::new(()));
    questions.retrying.store(true, Ordering::Release);
    let clock = broca_core::Clock::default();
    let pass = || {
        crate::approval::service_pass(
            rig.wal.clone(),
            rig.key.clone(),
            writer.clone(),
            Some(rig.store.clone()),
            lock.clone(),
            questions.as_ref(),
            &clock,
        )
    };
    assert!(!pass().await.unwrap());
    assert!(
        rig.row().withdrawals_owed,
        "retry leaves an unanswered question discoverable"
    );
    questions.retrying.store(false, Ordering::Release);
    assert!(pass().await.unwrap());
    assert!(!rig.row().withdrawals_owed);
    assert!(pass().await.unwrap());
    assert_eq!(questions.withdrawals.lock().unwrap().len(), 2);
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
}

struct UnknownRead {
    wal: Arc<FileWalStore>,
    fail_at: usize,
    reads: std::sync::atomic::AtomicUsize,
}
impl WalStore for UnknownRead {
    fn open(
        &self,
        key: &LineageKey,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.wal.open(key)
    }
    fn read_records(
        &self,
        key: &LineageKey,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.wal.read_records(key)
    }
    fn read_records_range(
        &self,
        key: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        let range = self.wal.read_records_range(key, after, through, anchor)?;
        if self.reads.fetch_add(1, Ordering::Relaxed) == self.fail_at {
            Ok(broca_wal::WalRangeRead::without_anchors(
                match range.into_records() {
                    broca_wal::WalRangeRecords::Complete(frames) => frames,
                    _ => panic!("fixture WAL must be complete"),
                },
            ))
        } else {
            Ok(range)
        }
    }
}

#[tokio::test]
async fn an_unknown_full_wal_read_never_clears_the_discovery_flag() {
    for fail_at in [0, 1] {
        let rig = Rig::new(true);
        rig.seed(true, false, false, true);
        rig.store
            .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
            .unwrap();
        rig.store.index_run_withdrawals_owed("r", true).unwrap();
        let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
        let questions = rig.executor.questions.as_ref().unwrap();
        let wal = Arc::new(UnknownRead {
            wal: rig.wal.clone(),
            fail_at,
            reads: std::sync::atomic::AtomicUsize::new(0),
        });
        let lock = Arc::new(Mutex::new(()));
        let clock = broca_core::Clock::default();
        let pass = || {
            crate::approval::service_pass(
                wal.clone(),
                rig.key.clone(),
                writer.clone(),
                Some(rig.store.clone()),
                lock.clone(),
                questions.as_ref(),
                &clock,
            )
        };
        assert!(pass().await.is_err());
        assert!(rig.row().withdrawals_owed);
        assert_eq!(questions.withdrawals.lock().unwrap().len(), fail_at);
        assert!(pass().await.unwrap());
        assert!(!rig.row().withdrawals_owed);
        assert_eq!(questions.withdrawals.lock().unwrap().len(), 1);
        let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
    }
}

#[tokio::test]
async fn dispatched_refused_declined_and_approved_closed_calls_send_no_withdrawal() {
    for kind in ["dispatched", "refused", "declined", "approved_closed"] {
        let rig = Rig::new(true);
        let key = rig.seed(true, false, kind != "declined", false);
        let mut result = Rig::closed_result(&key, false);
        if kind == "declined" {
            rig.append(WalRecord::ApprovalRecorded {
                call_key: key.clone(),
                call_id: ToolCallId("c".into()),
                elicitation_id: Some("el".into()),
                decision: broca_wal::DeferredDecision::Declined,
                approval_digest: None,
                closing_result: Some(
                    broca_core::not_dispatched_result(&ToolCallId("c".into())).into(),
                ),
            });
        } else {
            if kind == "dispatched" {
                rig.append(WalRecord::ToolDispatchIntent {
                    batch_id: broca_protocol::BatchId("b".into()),
                    tool_call_id: ToolCallId("c".into()),
                    tool_name: Some("edit".into()),
                    args: serde_json::json!({}).into(),
                    module_id: None,
                    schema_pin: None,
                    hooks: None,
                    call_key: Some(key.clone()),
                    approval_digest: Some("digest".into()),
                });
            }
            if let WalRecord::ToolResult {
                outcome,
                reason,
                result,
                ..
            } = &mut result
            {
                if kind != "approved_closed" {
                    *outcome = broca_wal::DispatchOutcome::Completed;
                    *result = broca_protocol::ToolResult {
                        tool_call_id: ToolCallId("c".into()),
                        output: broca_protocol::content::ToolOutput::Text { text: kind.into() },
                        is_error: kind == "refused",
                    }
                    .into();
                }
                if kind == "refused" {
                    *reason = Some("pre_tool_denied".into());
                }
            }
            rig.append(result);
        }
        rig.append(WalRecord::ToolBatchFinished {
            batch_id: broca_protocol::BatchId("b".into()),
        });
        rig.append(WalRecord::TurnFinished { step_id: StepId(1) });
        rig.append(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Interrupted,
            ts_ms: None,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
        let projection =
            crate::approval::read_projection(rig.wal.as_ref(), &rig.key, &writer).unwrap();
        crate::approval::record_terminal_debt(rig.store.as_ref(), &projection, "r").unwrap();
        rig.store
            .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
            .unwrap();
        let questions = rig.executor.questions.as_ref().unwrap();
        assert!(crate::approval::service_pass(
            rig.wal.clone(),
            rig.key.clone(),
            writer.clone(),
            Some(rig.store.clone()),
            Arc::new(Mutex::new(())),
            questions.as_ref(),
            &broca_core::Clock::default()
        )
        .await
        .unwrap());
        assert!(!rig.row().withdrawals_owed, "{kind}");
        assert!(questions.withdrawals.lock().unwrap().is_empty(), "{kind}");
        assert!(questions.requests.lock().unwrap().is_empty(), "{kind}");
        let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
    }
}

#[tokio::test]
async fn idle_writes_skip_withdrawal_reads_but_new_debt_wakes_the_task() {
    let rig = Rig::new(true);
    rig.seed(true, false, false, true);
    let template = rig.frames();
    std::fs::remove_file(rig.wal.wal_path(&rig.key)).unwrap();
    let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counted = Arc::new(CountedWal {
        count_when: None,
        wal: rig.wal.clone(),
        full_reads: reads.clone(),
    });
    let task = crate::approval::WithdrawalTask::spawn_with_pending(
        counted,
        rig.key.clone(),
        writer.clone(),
        None,
        Arc::new(Mutex::new(())),
        rig.executor.questions.as_ref().unwrap().clone(),
        broca_core::Clock::default(),
        false,
    );
    writer.record(WalRecord::StepStarted {
        step_id: StepId(99),
    });
    writer.sync().await.unwrap();
    assert!(!task.pending(writer.durable_head().load(Ordering::Acquire)));
    tokio::task::yield_now().await;
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "ordinary idle writes scheduled a full withdrawal scan"
    );
    for frame in template.iter().take(4) {
        writer.record(frame.record.clone());
    }
    writer.sync().await.unwrap();
    let key = format!("{}:5", writer.lineage_id().unwrap());
    writer.record(Rig::closed_result(&key, true));
    writer.sync().await.unwrap();
    task.wake();
    let sent = broca_test_support::event_timeout(
        "idle_writes_skip_withdrawal_reads_but_new_debt_wakes_the_task",
        async {
            while rig
                .executor
                .questions
                .as_ref()
                .unwrap()
                .withdrawals
                .lock()
                .unwrap()
                .is_empty()
            {
                tokio::task::yield_now().await;
            }
        },
        || {
            format!(
                "run_index={:?}; {}",
                rig.store.lookup_run("r"),
                broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record))
            )
        },
    )
    .await;
    task.stop().await;
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
    assert!(
        sent.is_ok(),
        "new owed debt was not serviced within the first backoff bound"
    );
    assert!(reads.load(Ordering::SeqCst) > 0);
}

struct ReadGate {
    entered: tokio::sync::Notify,
    finished: tokio::sync::Notify,
    released: Mutex<bool>,
    changed: std::sync::Condvar,
}
impl ReadGate {
    fn new() -> Self {
        Self {
            entered: tokio::sync::Notify::new(),
            finished: tokio::sync::Notify::new(),
            released: Mutex::new(false),
            changed: std::sync::Condvar::new(),
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct BlockingRead {
    wal: Arc<FileWalStore>,
    at: usize,
    reads: std::sync::atomic::AtomicUsize,
    gate: Arc<ReadGate>,
}
impl WalStore for BlockingRead {
    fn open(
        &self,
        key: &LineageKey,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.wal.open(key)
    }
    fn read_records(
        &self,
        key: &LineageKey,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.wal.read_records(key)
    }
    fn read_records_range(
        &self,
        key: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        let block = self.reads.fetch_add(1, Ordering::SeqCst) == self.at;
        if block {
            self.gate.entered.notify_one();
            let mut released = self.gate.released.lock().unwrap();
            while !*released {
                released = self.gate.changed.wait(released).unwrap();
            }
        }
        let result = self.wal.read_records_range(key, after, through, anchor);
        if block {
            self.gate.finished.notify_one();
        }
        result
    }
}

#[tokio::test]
async fn withdrawal_stop_joins_read_and_flag_jobs_before_writer_shutdown() {
    for at in [0, 1] {
        let rig = Rig::new(true);
        rig.seed(true, false, false, true);
        rig.store
            .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
            .unwrap();
        rig.store.index_run_withdrawals_owed("r", true).unwrap();
        let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
        let gate = Arc::new(ReadGate::new());
        let wal = Arc::new(BlockingRead {
            wal: rig.wal.clone(),
            at,
            reads: std::sync::atomic::AtomicUsize::new(0),
            gate: gate.clone(),
        });
        let task = crate::approval::WithdrawalTask::spawn(
            wal,
            rig.key.clone(),
            writer.clone(),
            Some(rig.store.clone()),
            Arc::new(Mutex::new(())),
            rig.executor.questions.as_ref().unwrap().clone(),
            broca_core::Clock::default(),
        );
        broca_test_support::event_timeout(
            "withdrawal_stop_joins_read_and_flag_jobs_before_writer_shutdown",
            gate.entered.notified(),
            || {
                format!(
                    "run_index={:?}; {}",
                    rig.store.lookup_run("r"),
                    broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record))
                )
            },
        )
        .await
        .unwrap();
        let mut stopping = tokio::spawn(task.stop());
        let early = tokio::time::timeout(Duration::from_millis(50), &mut stopping).await;
        let returned_early = early.is_ok();
        let owners_at_return = Arc::strong_count(&writer);
        gate.release();
        if let Ok(result) = early {
            result.unwrap();
        } else {
            stopping.await.unwrap();
        }
        broca_test_support::event_timeout(
            "withdrawal_stop_joins_read_and_flag_jobs_before_writer_shutdown",
            gate.finished.notified(),
            || {
                format!(
                    "run_index={:?}; {}",
                    rig.store.lookup_run("r"),
                    broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record))
                )
            },
        )
        .await
        .unwrap();
        assert!(
            !returned_early,
            "stop returned while blocking job {at} held {owners_at_return} writer owners"
        );
        assert_eq!(
            Arc::strong_count(&writer),
            1,
            "the actor must have sole writer ownership before shutdown"
        );
        assert!(
            rig.row().withdrawals_owed,
            "a cancelled flag job must not clear discovery after stop"
        );
        let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
    }
}

#[tokio::test]
async fn reader_unmarked_running_prepared_calls_close_regardless_of_restart_policy() {
    for resume in [false, true] {
        for approved in [false, true] {
            let rig = Rig::new(false);
            rig.seed(resume, false, approved, false);
            let status = broca_test_support::event_timeout(
                "reader_unmarked_running_prepared_calls_close_regardless_of_restart_policy",
                rig.registry.route_status(&rig.key),
                || {
                    format!(
                        "run_index={:?}; {}",
                        rig.store.lookup_run("r"),
                        broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record))
                    )
                },
            )
            .await;
            let completed = status.is_ok();
            rig.stop().await;
            assert!(
                completed,
                "reader open must seal rather than retry a refused Prepared resume"
            );
            assert_eq!(rig.row().state, RunIndexState::Interrupted);
            assert_eq!(rig.row().withdrawals_owed, !approved);
            assert!(rig.frames().iter().any(|frame| matches!(
                &frame.record,
                WalRecord::ToolResult {
                    withdraw_elicitation: None,
                    outcome: broca_wal::DispatchOutcome::Interrupted,
                    ..
                }
            )));
        }
    }
}

#[tokio::test]
async fn invalid_request_and_requester_refusal_retain_question_debt() {
    for reason in [
        "elicitation_invalid_request",
        "elicitation_requester_refused",
    ] {
        let rig = Rig::new(true);
        rig.seed(true, false, false, true);
        let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
        let questions = rig.executor.questions.as_ref().unwrap();
        questions
            .replies
            .lock()
            .unwrap()
            .push_back(ApprovalWithdrawal::NotSent {
                reason: reason.into(),
            });
        let empty = crate::approval::service_pass(
            rig.wal.clone(),
            rig.key.clone(),
            writer.clone(),
            None,
            Arc::new(Mutex::new(())),
            questions.as_ref(),
            &broca_core::Clock::default(),
        )
        .await
        .unwrap();
        let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
        assert!(!empty, "{reason} is not a withdrawal stop class");
        assert!(!rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::QuestionWithdrawalAcknowledged { .. }
        )));
    }
}

struct CountedWal {
    count_when: Option<Arc<std::sync::atomic::AtomicBool>>,
    wal: Arc<FileWalStore>,
    full_reads: Arc<std::sync::atomic::AtomicUsize>,
}
impl WalStore for CountedWal {
    fn open(
        &self,
        key: &LineageKey,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.wal.open(key)
    }
    fn read_records(
        &self,
        key: &LineageKey,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        if self
            .count_when
            .as_ref()
            .is_none_or(|window| window.load(Ordering::SeqCst))
        {
            self.full_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.wal.read_records(key)
    }
    fn read_records_range(
        &self,
        key: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        if after == 0
            && self
                .count_when
                .as_ref()
                .is_none_or(|window| window.load(Ordering::SeqCst))
        {
            self.full_reads.fetch_add(1, Ordering::SeqCst);
        }
        self.wal.read_records_range(key, after, through, anchor)
    }
    fn read_records_range_from_valid_anchor(
        &self,
        key: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: &broca_wal::WalByteAnchor,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        self.wal
            .read_records_range_from_valid_anchor(key, after, through, anchor)
    }
    fn flush_for_read(&self, key: &LineageKey) -> Result<Option<u64>, broca_wal::WalError> {
        self.wal.flush_for_read(key)
    }
    fn file_identity(&self, key: &LineageKey) -> Result<Option<u64>, broca_wal::WalError> {
        self.wal.file_identity(key)
    }
    fn wal_present(&self, key: &LineageKey) -> Option<bool> {
        self.wal.wal_present(key)
    }
    fn lineage_exists(
        &self,
        key: &LineageKey,
        meta: Option<&broca_core::AdmissionStoreSnapshot>,
    ) -> Result<bool, broca_wal::WalError> {
        self.wal.lineage_exists(key, meta)
    }
}

#[tokio::test]
async fn active_approval_status_polls_do_not_read_the_wal() {
    use crate::serve::ServeHandler;
    let rig = Rig::new(true);
    rig.seed(true, false, false, false);
    rig.registry.route_status(&rig.key).await.unwrap();
    broca_test_support::event_timeout("active_approval_status_polls_do_not_read_the_wal", async {
        loop {
            if matches!(rig.registry.run_status(&rig.key, RunId("r".into())).await.unwrap(), broca_wire::RunStatusResponse::Active { awaiting_approval: Some(pending), .. } if pending.pending[0].elicitation_id.is_some()) { break; }
            tokio::task::yield_now().await;
        }
    }, || format!("run_index={:?}; {}", rig.store.lookup_run("r"), broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record)))).await.unwrap();
    rig.full_reads.store(0, Ordering::SeqCst);
    for _ in 0..5 {
        assert!(matches!(
            rig.registry
                .run_status(&rig.key, RunId("r".into()))
                .await
                .unwrap(),
            broca_wire::RunStatusResponse::Active {
                awaiting_approval: Some(_),
                ..
            }
        ));
    }
    let reads = rig.full_reads.load(Ordering::SeqCst);
    rig.registry
        .route_cancel(&rig.key, RunId("r".into()))
        .await
        .unwrap();
    rig.eventually(|| rig.row().state == RunIndexState::Cancelled)
        .await;
    rig.stop().await;
    assert_eq!(reads, 0, "active status reread the entire WAL");
}

#[derive(Default)]
struct HoldingNoApproval {
    window: Option<Arc<std::sync::atomic::AtomicBool>>,
}

#[async_trait::async_trait]
impl RunExecutor for HoldingNoApproval {
    async fn execute(
        &self,
        job: RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        job.writer.record(
            serde_json::from_value(serde_json::json!({
                "type":"run_started", "run_id":job.req.run_id.0, "session":job.req.session,
                "config":min_config(), "input":job.req.input, "origin":broca_wal::RunOrigin::Fresh
            }))
            .unwrap(),
        );
        job.writer.sync().await.unwrap();
        if let Some(admit) = job.on_admitted {
            let _ = admit.send(Ok(job.req.run_id.clone()));
        }
        job.control.cancel.as_ref().unwrap().cancelled().await;
        job.writer.record(WalRecord::RunFinished {
            reason: broca_protocol::RunFinishReason::Cancelled,
            ts_ms: None,
            usage: Default::default(),
            error: None,
            retries_used: 0,
        });
        job.writer.sync().await.unwrap();
        if let Some(window) = &self.window {
            window.store(true, Ordering::SeqCst);
        }
        Ok(RunSummary {
            reason: broca_protocol::RunFinishReason::Cancelled,
            paused: None,
            steps: 0,
            usage: Default::default(),
            retries_used: 0,
            final_step_finish_reason: None,
        })
    }
}

#[tokio::test]
async fn active_status_without_an_approval_reader_is_still_reported() {
    use crate::serve::ServeHandler;
    let rig = Rig::new(false);
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(rig.root.join("leases"))),
        rig.wal.clone(),
        Arc::new(HoldingNoApproval::default()),
    )
    .with_run_index(rig.store.clone());
    let registry = LineageRegistry::new(Arc::new(opener));
    registry
        .route_send(&rig.key, text_req(&rig.key, "r"), RunKind::FreshOrResume)
        .await
        .unwrap();
    let status = registry.run_status(&rig.key, RunId("r".into())).await;
    registry
        .route_cancel(&rig.key, RunId("r".into()))
        .await
        .unwrap();
    assert!(registry.stop_and_seal().await);
    assert_eq!(registry.actor_panic_count(), 0);
    assert!(
        matches!(
            status,
            Ok(broca_wire::RunStatusResponse::Active {
                awaiting_approval: None,
                ..
            })
        ),
        "status depended on an approval reader: {status:?}"
    );
}

#[tokio::test]
async fn missing_origin_is_expired_without_blocking_other_owed_calls() {
    let rig = Rig::new(true);
    rig.seed(true, false, false, true);
    let source = rig.frames();
    let started = source[0].record.clone();
    let mut bad = source[3].record.clone();
    if let WalRecord::ToolCallDeferred { call_id, ask, .. } = &mut bad {
        *call_id = ToolCallId("bad".into());
        ask.expires_at_ms = 1;
    }
    let good = source[3].record.clone();
    std::fs::remove_file(rig.wal.wal_path(&rig.key)).unwrap();
    let mut file = rig.wal.open(&rig.key).unwrap();
    file.append(1, &bad, true).unwrap();
    let bad_key = format!("{}:1", file.lineage_id().unwrap());
    file.append(
        1,
        &WalRecord::ToolResult {
            batch_id: broca_protocol::BatchId("b".into()),
            result: broca_core::not_dispatched_result(&ToolCallId("bad".into())).into(),
            outcome: broca_wal::DispatchOutcome::Interrupted,
            hooks: None,
            reason: None,
            withdraw_elicitation: Some(broca_wal::WithdrawElicitation {
                call_key: bad_key.clone(),
                elicitation_id: None,
            }),
            provider_held: None,
        },
        true,
    )
    .unwrap();
    file.append(1, &started, true).unwrap();
    file.append(1, &good, true).unwrap();
    let good_key = format!("{}:4", file.lineage_id().unwrap());
    file.append(1, &Rig::closed_result(&good_key, true), true)
        .unwrap();
    drop(file);
    let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
    let result = crate::approval::service_pass(
        rig.wal.clone(),
        rig.key.clone(),
        writer.clone(),
        None,
        Arc::new(Mutex::new(())),
        rig.executor.questions.as_ref().unwrap().as_ref(),
        &broca_core::Clock::default(),
    )
    .await;
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
    assert!(result.unwrap());
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::QuestionWithdrawalAcknowledged { call_key, reason: Some(reason), .. } if call_key == &bad_key && reason == "expired")));
    assert!(rig.frames().iter().any(|frame| matches!(&frame.record, WalRecord::QuestionWithdrawalAcknowledged { call_key, answer: broca_wal::WithdrawalAnswer::Acknowledged, .. } if call_key == &good_key)));
}

struct FailingLookup(Arc<broca_store::SqliteStore>);
impl RunIndex for FailingLookup {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        self.0.index_run_active(row)
    }
    fn index_run_terminal(
        &self,
        id: &str,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        reason: Option<broca_protocol::FinishReason>,
        retries: u32,
    ) -> Result<(), String> {
        self.0.index_run_terminal(id, state, usage, reason, retries)
    }
    fn lookup_run(&self, _: &str) -> Result<Option<RunIndexRow>, String> {
        Err("injected index read failure".into())
    }
}
#[tokio::test]
async fn expired_question_acknowledgement_does_not_settle_failed_index_cleanup() {
    let rig = Rig::new(true);
    rig.seed_at(true, false, false, true, 1);
    rig.store
        .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
        .unwrap();
    rig.store.index_run_withdrawals_owed("r", true).unwrap();
    let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
    let result = crate::approval::service_pass(
        rig.wal.clone(),
        rig.key.clone(),
        writer.clone(),
        Some(Arc::new(FailingLookup(rig.store.clone()))),
        Arc::new(Mutex::new(())),
        rig.executor.questions.as_ref().unwrap().as_ref(),
        &broca_core::Clock::default(),
    )
    .await;
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
    assert!(
        !result.unwrap(),
        "acknowledged questions still require retrying failed discovery cleanup"
    );
    assert!(
        rig.row().withdrawals_owed,
        "failed bookkeeping must remain discoverable"
    );
    assert!(rig
        .executor
        .questions
        .as_ref()
        .unwrap()
        .withdrawals
        .lock()
        .unwrap()
        .is_empty());
}

fn seed_late_entries(rig: &Rig) -> Vec<String> {
    rig.seed(true, false, false, true);
    let template = rig.frames();
    std::fs::remove_file(rig.wal.wal_path(&rig.key)).unwrap();
    let mut file = rig.wal.open(&rig.key).unwrap();
    let mut keys = Vec::new();
    for i in 0..3 {
        let batch = format!("late-source-{i}");
        let mut started = template[0].record.clone();
        if let WalRecord::RunStarted { run_id, .. } = &mut started {
            *run_id = RunId(format!("r-{i}"));
        }
        file.append(1, &started, true).unwrap();
        file.append(1, &template[1].record, true).unwrap();
        file.append(
            1,
            &WalRecord::ToolBatchStarted {
                batch_id: broca_protocol::BatchId(batch.clone()),
                step_id: StepId(1),
                planned_calls: vec![ToolCallId("c".into())],
            },
            true,
        )
        .unwrap();
        let mut prepared = template[3].record.clone();
        if let WalRecord::ToolCallDeferred { batch_id, ask, .. } = &mut prepared {
            *batch_id = broca_protocol::BatchId(batch.clone());
            ask.expires_at_ms = 1;
        }
        let seq = file.append(1, &prepared, true).unwrap();
        let key = format!("{}:{seq}", file.lineage_id().unwrap());
        file.append(
            1,
            &WalRecord::ApprovalRecorded {
                call_key: key.clone(),
                call_id: ToolCallId("c".into()),
                elicitation_id: Some(format!("el-{i}")),
                decision: broca_wal::DeferredDecision::Expired,
                approval_digest: None,
                closing_result: Some(
                    broca_core::not_dispatched_result(&ToolCallId("c".into())).into(),
                ),
            },
            true,
        )
        .unwrap();
        file.append(
            1,
            &WalRecord::ToolBatchFinished {
                batch_id: broca_protocol::BatchId(batch),
            },
            true,
        )
        .unwrap();
        file.append(1, &WalRecord::TurnFinished { step_id: StepId(1) }, true)
            .unwrap();
        file.append(
            1,
            &WalRecord::RunFinished {
                reason: broca_protocol::RunFinishReason::Interrupted,
                ts_ms: None,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
            true,
        )
        .unwrap();
        file.append(
            1,
            &WalRecord::LateApprovalRecorded {
                call_key: key.clone(),
                elicitation_id: format!("el-{i}"),
                decision: broca_wal::LateDecision::Approved,
                approval_digest: Some("digest".into()),
                late_execution: Some(broca_wal::LateExecution::Execute),
            },
            true,
        )
        .unwrap();
        file.append(
            1,
            &WalRecord::LateDispatchIntent {
                call_key: key.clone(),
                approval_digest: "digest".into(),
                batch_id: broca_protocol::BatchId(format!("late:{key}")),
            },
            true,
        )
        .unwrap();
        file.append(
            1,
            &WalRecord::LateResult {
                call_key: key.clone(),
                outcome: broca_wal::LateOutcome::Completed,
                result: broca_protocol::ToolResult {
                    tool_call_id: ToolCallId("c".into()),
                    output: broca_protocol::content::ToolOutput::Text {
                        text: "x".repeat(2048),
                    },
                    is_error: false,
                }
                .into(),
            },
            true,
        )
        .unwrap();
        keys.push(key);
    }
    keys
}

#[tokio::test]
async fn late_read_pages_are_atomic_bounded_and_nonrepeating() {
    use crate::serve::{ServeHandler, SessionReadRequest};
    let rig = Rig::new(false);
    seed_late_entries(&rig);
    let page = rig.registry.route_read(&rig.key, None, None).await.unwrap();
    // Bound the approval projection itself: the outer response pager also limits bytes and
    // could hide an unbounded allocation of late results before it trims the response.
    let (late, next) = crate::approval::late_page(&page.late_approvals, None, 256).unwrap();
    assert_eq!(late.len(), 1, "late projection ignored max_bytes");
    assert_eq!(next.as_ref(), Some(&late[0].call_key));
    let mut request = SessionReadRequest::new(rig.key.clone(), None, Some(100));
    request.max_bytes = Some(256);
    let first = rig.registry.read(request.clone()).await.unwrap();
    assert_eq!(
        first.late_approvals.as_ref().unwrap().len(),
        1,
        "late results ignored max_bytes"
    );
    assert_eq!(
        first.late_approvals.as_ref().unwrap()[0]
            .result
            .as_ref()
            .unwrap()
            .output,
        broca_protocol::content::ToolOutput::Text {
            text: "x".repeat(2048)
        }
    );
    request.late_after = first.next_late_after.clone();
    assert!(request.late_after.is_some());
    let second = rig.registry.read(request.clone()).await.unwrap();
    assert!(second.messages.is_empty());
    assert_ne!(first.late_approvals, second.late_approvals);
    request.late_after = second.next_late_after;
    let last = rig.registry.read(request).await.unwrap();
    assert_eq!(last.late_approvals.as_ref().unwrap().len(), 1);
    assert!(last.next_late_after.is_none());
    let ordinary = rig
        .registry
        .read(SessionReadRequest::new(rig.key.clone(), Some(1), Some(1)))
        .await
        .unwrap();
    assert!(
        ordinary.late_approvals.is_none(),
        "message continuation repeated late approvals"
    );
}

#[tokio::test]
async fn a_missing_late_cursor_is_invalid_params_not_an_empty_page() {
    use crate::serve::{ServeError, ServeHandler, SessionReadRequest};
    let rig = Rig::new(false);
    seed_late_entries(&rig);
    let mut request = SessionReadRequest::new(rig.key.clone(), None, None);
    request.late_after = Some("gone:1".into());
    assert!(
        matches!(rig.registry.read(request).await, Err(ServeError::InvalidParams { field, .. }) if field == "late_after")
    );
}

#[tokio::test]
async fn an_incomplete_tail_explicitly_marks_approval_metadata_not_reported() {
    use crate::serve::{ServeHandler, SessionReadRequest};
    let rig = Rig::new(false);
    rig.seed(true, false, false, true);
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(rig.root.join("leases"))),
        rig.wal.clone(),
        rig.executor.clone(),
    );
    let unknown = Arc::new(UnknownRead {
        wal: rig.wal.clone(),
        fail_at: 0,
        reads: std::sync::atomic::AtomicUsize::new(0),
    });
    let registry =
        LineageRegistry::new_with_reader(Arc::new(opener), unknown, Some(rig.store.clone()));
    let page = registry
        .read(SessionReadRequest::new(rig.key.clone(), None, None))
        .await
        .unwrap();
    assert!(
        matches!(page.open_batch, Some(broca_wire::OpenBatchView::NotReported { reason }) if reason == "incomplete_tail")
    );
}

#[tokio::test]
async fn reopen_clears_acknowledged_discovery_without_more_question_ops() {
    let rig = Rig::new(true);
    let key = rig.seed(true, false, false, true);
    rig.append(WalRecord::QuestionWithdrawalAcknowledged {
        call_key: key,
        elicitation_id: None,
        answer: broca_wal::WithdrawalAnswer::NotSent,
        reason: Some("expired".into()),
    });
    rig.store
        .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
        .unwrap();
    rig.store.index_run_withdrawals_owed("r", true).unwrap();
    rig.registry.route_status(&rig.key).await.unwrap();
    let cleared = broca_test_support::event_timeout(
        "reopen_clears_acknowledged_discovery_without_more_question_ops",
        async {
            while rig.row().withdrawals_owed {
                tokio::task::yield_now().await;
            }
        },
        || {
            format!(
                "run_index={:?}; {}",
                rig.store.lookup_run("r"),
                broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record))
            )
        },
    )
    .await;
    rig.stop().await;
    assert!(
        cleared.is_ok(),
        "boot opened a flagged row but left its completed discovery flag stranded"
    );
    assert!(rig
        .executor
        .questions
        .as_ref()
        .unwrap()
        .withdrawals
        .lock()
        .unwrap()
        .is_empty());
}

struct TerminalWindow {
    store: Arc<broca_store::SqliteStore>,
    window: Arc<std::sync::atomic::AtomicBool>,
}
impl RunIndex for TerminalWindow {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        self.store.index_run_active(row)
    }
    fn lookup_run(&self, id: &str) -> Result<Option<RunIndexRow>, String> {
        self.store.lookup_run(id)
    }
    fn index_run_terminal(
        &self,
        id: &str,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        reason: Option<broca_protocol::FinishReason>,
        retries: u32,
    ) -> Result<(), String> {
        let result = self
            .store
            .index_run_terminal(id, state, usage, reason, retries);
        self.window.store(false, Ordering::SeqCst);
        result
    }
}

#[tokio::test]
async fn ordinary_terminal_indexing_does_not_scan_for_prepared_calls() {
    let rig = Rig::new(false);
    let reads = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let window = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let wal = Arc::new(CountedWal {
        wal: rig.wal.clone(),
        full_reads: reads.clone(),
        count_when: Some(window.clone()),
    });
    let index = Arc::new(TerminalWindow {
        store: rig.store.clone(),
        window: window.clone(),
    });
    let executor = Arc::new(HoldingNoApproval {
        window: Some(window.clone()),
    });
    let registry = LineageRegistry::new(Arc::new(
        DefaultOpener::new(
            Arc::new(FileLeaseStore::new(rig.root.join("leases"))),
            wal,
            executor,
        )
        .with_run_index(index),
    ));
    registry
        .route_send(&rig.key, text_req(&rig.key, "r"), RunKind::FreshOrResume)
        .await
        .unwrap();
    registry
        .route_cancel(&rig.key, RunId("r".into()))
        .await
        .unwrap();
    rig.eventually(|| rig.row().state == RunIndexState::Cancelled)
        .await;
    assert!(registry.stop_and_seal().await);
    assert_eq!(registry.actor_panic_count(), 0);
    assert_eq!(
        reads.load(Ordering::SeqCst),
        0,
        "ordinary terminal indexing paid a full-WAL approval scan"
    );
}

#[tokio::test]
async fn durable_approval_cache_matches_seed_and_ignores_stale_markers() {
    let rig = Rig::new(false);
    let key = rig.seed(true, false, false, false);
    let lineage = rig
        .wal
        .read_records_range(&rig.key, 0, None, None)
        .unwrap()
        .lineage_id;
    let before = rig.frames();
    let live = crate::approval_state::ApprovalState::default();
    for frame in &before {
        live.observe(frame.seq, frame.fence, lineage, &frame.record);
    }
    assert_eq!(
        live.awaiting(&RunId("r".into()), 4).unwrap().pending.len(),
        1
    );
    rig.append(WalRecord::ApprovalRecorded {
        call_key: key.clone(),
        call_id: ToolCallId("c".into()),
        elicitation_id: Some("answered".into()),
        decision: broca_wal::DeferredDecision::Approved,
        approval_digest: Some("digest".into()),
        closing_result: None,
    });
    rig.append(WalRecord::AwaitingApproval {
        batch_id: broca_protocol::BatchId("b".into()),
        pending: vec![broca_wal::AwaitingApprovalEntry {
            call_key: key,
            call_id: ToolCallId("c".into()),
            elicitation_id: Some("stale".into()),
            expires_at_ms: u64::MAX,
        }],
    });
    let all = rig.frames();
    for frame in &all[before.len()..] {
        live.observe(frame.seq, frame.fence, lineage, &frame.record);
    }
    let prefix = crate::approval::projection_from_frames(lineage, &before).unwrap();
    let seeded = crate::approval_state::ApprovalState::default();
    seeded.seed(
        lineage,
        6,
        Some(RunId("r".into())),
        &all[before.len()..],
        Some(&prefix),
        true,
    );
    assert!(live.awaiting(&RunId("r".into()), 6).is_none());
    assert_eq!(
        seeded.awaiting(&RunId("r".into()), 6),
        live.awaiting(&RunId("r".into()), 6)
    );
    assert!(
        live.awaiting(&RunId("r".into()), 7).is_none(),
        "uncovered durable head must be not reported"
    );
    live.healthy.store(false, Ordering::Release);
    live.wait_covered(u64::MAX).await;
    assert!(live.awaiting(&RunId("r".into()), 6).is_none());
    assert_eq!(live.prepared(&RunId("r".into()), 6), None);
}

#[tokio::test]
async fn actor_job_drain_releases_writer_before_reporting_completion() {
    let rig = Rig::new(false);
    let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
    let jobs = Arc::new(crate::approval::ActorJobs::default());
    let job = jobs.track(writer.clone());
    let gate = Arc::new(ReadGate::new());
    let held = gate.clone();
    let blocking = tokio::task::spawn_blocking(move || {
        let _job = job;
        held.entered.notify_one();
        let mut release = held.released.lock().unwrap();
        while !*release {
            release = held.changed.wait(release).unwrap();
        }
    });
    gate.entered.notified().await;
    let mut draining = Box::pin(jobs.join());
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut draining)
            .await
            .is_err()
    );
    gate.release();
    draining.await;
    assert_eq!(Arc::strong_count(&writer), 1);
    blocking.await.unwrap();
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
}

#[tokio::test]
async fn dropping_withdrawal_task_cancels_its_idle_worker() {
    let rig = Rig::new(true);
    let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
    let task = crate::approval::WithdrawalTask::spawn_with_pending(
        rig.wal.clone(),
        rig.key.clone(),
        writer.clone(),
        None,
        Arc::new(Mutex::new(())),
        rig.executor.questions.as_ref().unwrap().clone(),
        broca_core::Clock::default(),
        false,
    );
    let cancelled = task.cancellation();
    drop(task);
    assert!(
        cancelled.is_cancelled(),
        "implicit drop left the worker running"
    );
    rig.eventually(|| Arc::strong_count(&writer) == 1).await;
    let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
}

struct RecoveringCleanupIndex {
    store: Arc<broca_store::SqliteStore>,
    fail: std::sync::atomic::AtomicBool,
    fail_clear: bool,
    failures: std::sync::atomic::AtomicUsize,
}
impl RunIndex for RecoveringCleanupIndex {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        self.store.index_run_active(row)
    }
    fn index_run_terminal(
        &self,
        id: &str,
        state: RunIndexState,
        usage: Option<broca_protocol::Usage>,
        reason: Option<broca_protocol::FinishReason>,
        retries: u32,
    ) -> Result<(), String> {
        self.store
            .index_run_terminal(id, state, usage, reason, retries)
    }
    fn lookup_run(&self, id: &str) -> Result<Option<RunIndexRow>, String> {
        if !self.fail_clear && self.fail.load(Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err("temporary cleanup lookup failure".into());
        }
        self.store.lookup_run(id)
    }
    fn index_run_withdrawals_owed(&self, id: &str, owed: bool) -> Result<(), String> {
        if !owed && self.fail_clear && self.fail.load(Ordering::SeqCst) {
            self.failures.fetch_add(1, Ordering::SeqCst);
            return Err("temporary cleanup write failure".into());
        }
        self.store.index_run_withdrawals_owed(id, owed)
    }
}

#[tokio::test]
async fn cleanup_failure_retries_without_traffic_after_question_acknowledgement() {
    for fail_clear in [false, true] {
        let rig = Rig::new(true);
        rig.seed_at(true, false, false, true, 1);
        rig.store
            .index_run_terminal("r", RunIndexState::Interrupted, None, None, 0)
            .unwrap();
        rig.store.index_run_withdrawals_owed("r", true).unwrap();
        let index = Arc::new(RecoveringCleanupIndex {
            store: rig.store.clone(),
            fail: std::sync::atomic::AtomicBool::new(true),
            fail_clear,
            failures: std::sync::atomic::AtomicUsize::new(0),
        });
        let writer = Arc::new(WalHandle::spawn(rig.wal.open(&rig.key).unwrap(), 1).unwrap());
        let task = crate::approval::WithdrawalTask::spawn(
            rig.wal.clone(),
            rig.key.clone(),
            writer.clone(),
            Some(index.clone()),
            Arc::new(Mutex::new(())),
            rig.executor.questions.as_ref().unwrap().clone(),
            broca_core::Clock::default(),
        );
        rig.eventually(|| index.failures.load(Ordering::SeqCst) > 0)
            .await;
        assert!(rig.frames().iter().any(|frame| matches!(
            frame.record,
            WalRecord::QuestionWithdrawalAcknowledged { .. }
        )));
        index.fail.store(false, Ordering::SeqCst);
        let repaired = broca_test_support::event_timeout(
            "cleanup_failure_retries_without_traffic_after_question_acknowledgement",
            async {
                while rig.row().withdrawals_owed {
                    tokio::task::yield_now().await;
                }
            },
            || {
                format!(
                    "run_index={:?}; {}",
                    rig.store.lookup_run("r"),
                    broca_test_support::record_kinds(rig.frames().iter().map(|f| &f.record))
                )
            },
        )
        .await;
        task.stop().await;
        let _ = Arc::try_unwrap(writer).ok().unwrap().shutdown().await;
        assert!(
            repaired.is_ok(),
            "cleanup failure parked its stale flag instead of retrying"
        );
        assert!(rig
            .executor
            .questions
            .as_ref()
            .unwrap()
            .withdrawals
            .lock()
            .unwrap()
            .is_empty());
        assert_eq!(
            rig.frames()
                .iter()
                .filter(|frame| matches!(
                    frame.record,
                    WalRecord::QuestionWithdrawalAcknowledged { .. }
                ))
                .count(),
            1
        );
    }
}

/// Counts run-cache entries visited per ToolDispatchIntent/ToolResult, not pending-call comparisons.
#[test]
fn approval_cache_tool_update_work_is_independent_of_prior_runs() {
    let rig = Rig::new(false);
    rig.seed(true, false, false, false);
    let template = rig.frames()[0].record.clone();
    let cache = crate::approval_state::ApprovalState::default();
    for i in 0..500 {
        let mut started = template.clone();
        if let WalRecord::RunStarted { run_id, .. } = &mut started {
            *run_id = RunId(format!("run-{i}"));
        }
        cache.observe(i * 2 + 1, 1, None, &started);
        cache.observe(
            i * 2 + 2,
            1,
            None,
            &WalRecord::RunFinished {
                ts_ms: None,
                reason: broca_protocol::RunFinishReason::Completed,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
        );
    }
    let mut active = template;
    if let WalRecord::RunStarted { run_id, .. } = &mut active {
        *run_id = RunId("active".into());
    }
    cache.observe(1001, 1, None, &active);
    cache.observe(
        1002,
        1,
        None,
        &WalRecord::ToolDispatchIntent {
            batch_id: broca_protocol::BatchId("b".into()),
            tool_call_id: ToolCallId("c".into()),
            tool_name: Some("edit".into()),
            args: serde_json::json!({}).into(),
            module_id: None,
            schema_pin: None,
            hooks: None,
            call_key: None,
            approval_digest: None,
        },
    );
    assert_eq!(
        cache.close_visits(),
        1,
        "tool intent visited historical runs"
    );
    cache.observe(1003, 1, None, &Rig::closed_result("unused", false));
    assert_eq!(
        cache.close_visits(),
        2,
        "tool result visited historical runs"
    );
    assert_eq!(
        cache.retained_runs(),
        1,
        "approval cache retained historical runs"
    );
    assert_eq!(cache.prepared(&RunId("run-0".into()), 1003), None);
    cache.finished(&RunId("active".into()));
    assert_eq!(
        cache.retained_runs(),
        0,
        "finished run remained in active status cache"
    );
}
