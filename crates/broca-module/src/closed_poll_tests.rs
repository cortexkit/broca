use super::*;
use crate::opener::DefaultOpener;
use crate::serve::{ServeHandler, SessionReadRequest};
use crate::LineageRegistry;
use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, MaterializedStore};
use broca_protocol::{
    AssistantMessage, BindIdentity, ContentBlock, FinishReason, MessageId, RunFinishReason, StepId,
    Usage,
};
use std::sync::atomic::AtomicBool;

struct TerminalExecutor(RunFinishReason);

#[async_trait]
impl RunExecutor for TerminalExecutor {
    async fn execute(
        &self,
        job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        job.writer.record(WalRecord::RunStarted {
            tags: None,
            session_class: None,
            retention: None,
            hooks: None,
            title: Some("A durable title".into()),
            on_restart: None,
            keep_warm: None,
            marker_order_version: None,
            manifest: None,
            scope: None,
            send_id: None,
            request_identity_digest: None,
            project_id: None,
            principal: None,
            ts_ms: None,
            run_id: job.req.run_id.clone(),
            session: job.req.session.clone(),
            config: Box::new(tests::min_config()),
            input: job.req.input.clone().into(),
            origin: broca_wal::RunOrigin::Fresh,
            submission_id: None,
        });
        job.writer.sync().await?;
        if let Some(ack) = job.on_admitted {
            let _ = ack.send(Ok(job.req.run_id.clone()));
        }
        job.writer.record(WalRecord::ModelStepFinished {
            tags: None,
            hooks: None,
            step_id: StepId(1),
            assistant_message: AssistantMessage {
                message_id: MessageId("answer".into()),
                content: vec![ContentBlock::Text {
                    text: "durable answer".into(),
                }],
            }
            .into(),
            usage: Usage::default(),
            finish_reason: Some(FinishReason::Stop),
            retries_used: 2,
            service_tier_used: None,
            speed_used: None,
        });
        if self.0 == RunFinishReason::Interrupted {
            job.writer.record(WalRecord::ToolDispatchIntent {
                hooks: None,
                batch_id: broca_protocol::BatchId("batch".into()),
                tool_call_id: broca_protocol::ToolCallId("ambiguous-call".into()),
                tool_name: Some("edit".into()),
                args: serde_json::json!({}).into(),
                module_id: None,
                schema_pin: None,
                approval_digest: None,
                call_key: None,
            });
        }
        job.writer.record(WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: self.0,
            usage: Usage::default(),
            error: None,
            retries_used: 2,
        });
        job.writer.sync().await?;
        Ok(RunSummary {
            reason: self.0,
            paused: None,
            steps: 1,
            usage: Usage::default(),
            retries_used: 2,
            final_step_finish_reason: Some(FinishReason::Stop),
        })
    }
}

struct LaggingIndex {
    store: Arc<broca_store::SqliteStore>,
    lag: AtomicBool,
}

impl RunIndex for LaggingIndex {
    fn index_run_active(&self, row: &RunIndexRow) -> Result<(), String> {
        self.store.index_run_active(row)
    }
    fn index_run_terminal(
        &self,
        id: &str,
        state: RunIndexState,
        usage: Option<Usage>,
        finish: Option<FinishReason>,
        retries: u32,
    ) -> Result<(), String> {
        self.store
            .index_run_terminal(id, state, usage, finish, retries)
    }
    fn lookup_run(&self, id: &str) -> Result<Option<RunIndexRow>, String> {
        self.store.lookup_run(id).map(|row| {
            row.map(|mut row| {
                if self.lag.load(Ordering::Relaxed) {
                    row.state = RunIndexState::Active;
                }
                row
            })
        })
    }
}

async fn answers(registry: &LineageRegistry, key: &BindIdentity, run: &RunId) -> Vec<Vec<u8>> {
    vec![
        serde_json::to_vec(&registry.status(key).await.unwrap()).unwrap(),
        serde_json::to_vec(&registry.run_status(key, run.clone()).await.unwrap()).unwrap(),
        serde_json::to_vec(&registry.run_result(key, run.clone()).await.unwrap()).unwrap(),
        serde_json::to_vec(
            &registry
                .read(SessionReadRequest::new(key.clone(), None, None))
                .await
                .unwrap(),
        )
        .unwrap(),
    ]
}

#[tokio::test]
async fn closed_one_shot_polls_leave_actor_closed_and_match_live_answers() {
    for reason in [RunFinishReason::Completed, RunFinishReason::Interrupted] {
        let dir = broca_test_support::scratch_root()
            .join(format!("closed-polls-{}-{reason:?}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let key = BindIdentity::new(dir.as_path(), "runner", "one-shot");
        let wal = Arc::new(FileWalStore::new(dir.as_path().join("wal")));
        let store = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
        let index = Arc::new(LaggingIndex {
            store: store.clone(),
            lag: AtomicBool::new(false),
        });
        let opener = Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.as_path().join("leases"))),
                wal.clone(),
                Arc::new(TerminalExecutor(reason)),
            )
            .with_idle_ttl(Duration::from_secs(3600))
            .with_run_index(index.clone())
            .with_materialized_store(store.clone()),
        );
        let registry =
            LineageRegistry::new_with_reader(opener.clone(), wal.clone(), Some(store.clone()));
        let run = RunId("finished".into());
        let sent = registry
            .route_submission_with_materialization(
                &key,
                run.clone(),
                tests::text_req(&key, &run.0).to_submission(),
                RunKind::FreshOrResume,
                SendKey::RunId,
                true,
            )
            .await
            .unwrap();
        assert!(matches!(sent, SendOutcome::Active { .. }));
        broca_test_support::event_timeout(
            "finished one-shot actor closes",
            async {
                while registry.slot_state(&key).is_some() {
                    tokio::time::sleep(Duration::from_millis(5)).await;
                }
            },
            || format!("slot={:?}", registry.slot_state(&key)),
        )
        .await
        .expect("one-shot actor closes");

        let closed = answers(&registry, &key, &run).await;
        for _ in 0..5 {
            assert_eq!(answers(&registry, &key, &run).await, closed);
            assert_eq!(
                registry.slot_counts().open,
                0,
                "poll must not reopen a closed actor"
            );
            assert!(registry.slot_state(&key).is_none());
        }
        let result: serde_json::Value = serde_json::from_slice(&closed[2]).unwrap();
        let status: serde_json::Value = serde_json::from_slice(&closed[1]).unwrap();
        assert_eq!(status["usage"], result["usage"]);
        assert_eq!(
            result["usage"]["steps"], 1,
            "indexed status must retain completed-step usage from the WAL"
        );
        assert_eq!(result["usage"]["unreported_steps"], 1);
        assert_eq!(result["usage"]["by_model"][0]["steps"], 1);
        assert_eq!(result["usage"]["by_model"][0]["unreported_steps"], 1);
        assert!(result["usage"].get("input_tokens").is_none());
        assert!(result["usage"].get("output_tokens").is_none());
        assert_eq!(
            result["state"],
            if reason == RunFinishReason::Completed {
                "completed"
            } else {
                "interrupted"
            }
        );
        if reason == RunFinishReason::Completed {
            assert_eq!(result["final_message"]["text"], "durable answer");
        } else {
            let status: serde_json::Value = serde_json::from_slice(&closed[1]).unwrap();
            assert_eq!(
                status["indeterminate_tool_calls"],
                serde_json::json!(["ambiguous-call"])
            );
        }
        let page: serde_json::Value = serde_json::from_slice(&closed[3]).unwrap();
        assert_eq!(page["messages"].as_array().unwrap().len(), 2);
        // One-shot payloads are not projected. The page must include the WAL-only answer.
        assert_eq!(
            store
                .read_page_with_watermark(&key, None, 20, broca_core::ReadView::Raw)
                .unwrap()
                .prefix_len,
            0
        );

        index.lag.store(true, Ordering::Relaxed);
        assert_eq!(
            index.lookup_run(&run.0).unwrap().unwrap().state,
            RunIndexState::Active
        );
        assert_eq!(
            answers(&registry, &key, &run).await,
            closed,
            "stale index cannot hide the WAL terminal"
        );
        assert_eq!(registry.slot_counts().open, 0);

        // Explicitly open an independent coordinator to compare the exact serialized replies.
        let live = LineageRegistry::new_with_reader(opener, wal, Some(store));
        live.actor_for(&key).await.unwrap();
        assert_eq!(answers(&live, &key, &run).await, closed);
        index.lag.store(false, Ordering::Relaxed);
        assert_eq!(answers(&live, &key, &run).await, closed);
        assert!(live.stop_and_seal().await);

        let follow_up = registry
            .route_submission_with_materialization(
                &key,
                RunId("next".into()),
                tests::text_req(&key, "next").to_submission(),
                RunKind::AppendEpisode,
                SendKey::RunId,
                false,
            )
            .await
            .unwrap();
        assert!(matches!(follow_up, SendOutcome::Active { .. }));
        assert_eq!(
            registry.slot_counts().open,
            1,
            "send still opens its writer actor"
        );
        assert!(registry.stop_and_seal().await);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

struct NoOpen;

#[async_trait]
impl crate::registry::LineageOpener for NoOpen {
    async fn open(
        &self,
        _: &BindIdentity,
        _: u64,
        _: crate::registry::RegistryHandle,
    ) -> Result<crate::registry::ActorHandle, crate::registry::OpenError> {
        panic!("observer must not open an actor");
    }
}

struct UnprovenWal;

impl WalStore for UnprovenWal {
    fn live_wal_bytes(&self, _: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        Ok(Some(0))
    }
    fn open(
        &self,
        _: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        panic!("observer must not open a WAL writer");
    }
    fn read_records(
        &self,
        _: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        panic!("observer must use bounded WAL reads");
    }
    fn read_records_range(
        &self,
        _: &BindIdentity,
        _: u64,
        _: Option<u64>,
        _: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()))
    }
}

#[tokio::test]
async fn closed_polls_refuse_unproven_history_without_opening_an_actor() {
    let key = BindIdentity::new("/closed-poll", "runner", "unproven");
    let registry = LineageRegistry::new_with_reader(Arc::new(NoOpen), Arc::new(UnprovenWal), None);
    let run = RunId("unproven".into());
    for error in [
        registry.status(&key).await.unwrap_err(),
        registry.run_status(&key, run.clone()).await.unwrap_err(),
        registry.run_result(&key, run).await.unwrap_err(),
    ] {
        assert!(
            matches!(
                error,
                crate::serve::ServeError::Open(_, Some(broca_protocol::ErrorClass::Transient))
            ),
            "{error:?}"
        );
        assert!(
            error.to_string().contains("coverage is unproven"),
            "{error:?}"
        );
    }
    assert!(registry.slot_state(&key).is_none());
}

#[tokio::test]
async fn closed_polls_distinguish_absent_runs_from_a_pending_crash_orphan() {
    let dir =
        broca_test_support::scratch_root().join(format!("closed-unsealed-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = BindIdentity::new(&dir, "runner", "unsealed");
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let registry = LineageRegistry::new_with_reader(Arc::new(NoOpen), wal.clone(), None);
    let run = RunId("unsealed".into());
    assert_eq!(
        serde_json::to_value(registry.run_status(&key, run.clone()).await.unwrap()).unwrap()
            ["state"],
        "unknown"
    );
    assert!(matches!(
        registry.status(&key).await.unwrap(),
        crate::serve::StatusResponse::Idle
    ));
    let start = unsealed_start(&key, &run);
    let mut writer = wal.open(&key).unwrap();
    writer.append(1, &start, true).unwrap();
    drop(writer);
    let sweep = registry.mark_crash_orphans(
        wal.as_ref(),
        vec![(run.clone(), key.clone())],
        std::time::SystemTime::now() + Duration::from_secs(1),
    );
    assert_eq!(sweep.marked, 1);
    assert!(matches!(
        registry.status(&key).await,
        Err(crate::serve::ServeError::Open(
            _,
            Some(broca_protocol::ErrorClass::Transient)
        ))
    ));
    assert!(matches!(
        registry.run_status(&key, run).await,
        Err(crate::serve::ServeError::Open(
            _,
            Some(broca_protocol::ErrorClass::Transient)
        ))
    ));
    assert!(registry.slot_state(&key).is_none());
    std::fs::remove_dir_all(dir).unwrap();
}

fn unsealed_start(key: &BindIdentity, run: &RunId) -> WalRecord {
    WalRecord::RunStarted {
        tags: None,
        session_class: None,
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: run.clone(),
        session: key.clone(),
        config: Box::new(tests::min_config()),
        input: Vec::new().into(),
        origin: broca_wal::RunOrigin::Fresh,
        submission_id: None,
    }
}

#[tokio::test]
async fn closed_unmarked_orphan_status_makes_progress_after_boot_sealing() {
    for run_keyed in [false, true] {
        let dir = broca_test_support::scratch_root()
            .join(format!("closed-orphan-{}-{run_keyed}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let key = BindIdentity::new(&dir, "runner", "unmarked");
        let run = RunId("unsealed".into());
        let wal = Arc::new(FileWalStore::new(dir.join("wal")));
        let mut writer = wal.open(&key).unwrap();
        writer.append(1, &unsealed_start(&key, &run), true).unwrap();
        drop(writer);
        let opener = DefaultOpener::new(
            Arc::new(FileLeaseStore::new(dir.join("leases"))),
            wal.clone(),
            Arc::new(TerminalExecutor(RunFinishReason::Completed)),
        )
        .with_idle_ttl(Duration::from_secs(3600));
        let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None);
        let sweep =
            registry.mark_crash_orphans(wal.as_ref(), Vec::new(), std::time::SystemTime::now());
        registry.seal_marked_crash_orphans(sweep).await;
        assert!(registry.slot_state(&key).is_none());
        if !run_keyed {
            registry
                .status(&key)
                .await
                .expect("session status recovers the unmarked orphan");
        }
        let status = registry
            .run_status(&key, run.clone())
            .await
            .expect("run status must make recovery progress");
        assert_eq!(
            serde_json::to_value(status).unwrap()["state"],
            "interrupted"
        );
        let records = wal.read_records(&key).unwrap();
        assert!(records.iter().any(|frame| matches!(
            frame.record,
            WalRecord::RunFinished {
                reason: RunFinishReason::Interrupted,
                ..
            }
        )));
        assert_eq!(
            records
                .iter()
                .filter(|frame| matches!(frame.record, WalRecord::RunStarted { .. }))
                .count(),
            1,
            "status recovery must seal, not start a replacement episode"
        );
        assert!(registry.stop_and_seal().await);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn status_orphan_lease_refusal_does_not_leave_a_pending_boot_mark() {
    let dir = broca_test_support::scratch_root()
        .join(format!("held-status-orphan-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = BindIdentity::new(&dir, "runner", "held");
    let run = RunId("held-run".into());
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let mut writer = wal.open(&key).unwrap();
    writer.append(1, &unsealed_start(&key, &run), true).unwrap();
    drop(writer);
    let leases = Arc::new(FileLeaseStore::new(dir.join("leases")));
    let held =
        broca_core::LeaseStore::acquire(leases.as_ref(), &broca_core::lease_key_for(&key)).unwrap();
    let opener = DefaultOpener::new(
        leases,
        wal.clone(),
        Arc::new(TerminalExecutor(RunFinishReason::Completed)),
    );
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal, None);
    for _ in 0..2 {
        assert!(
            matches!(
                registry.route_run_state(&key, run.clone()).await,
                Err(crate::registry::OpenError::LeaseHeld(_))
            ),
            "the next poll must retry the lease, not wait forever on a stranded seal mark"
        );
    }
    drop(held);
    assert!(registry.stop_and_seal().await);
    std::fs::remove_dir_all(dir).unwrap();
}

struct StatusCostWal {
    inner: Arc<FileWalStore>,
    from_zero: AtomicU64,
    decoded: AtomicU64,
}

impl StatusCostWal {
    fn count_range(&self, after: u64, range: &broca_wal::WalRangeRead) {
        if after == 0 {
            self.from_zero.fetch_add(1, Ordering::Relaxed);
        }
        let count = match range.records() {
            broca_wal::WalRangeRecords::Complete(records)
            | broca_wal::WalRangeRecords::Partial(records)
            | broca_wal::WalRangeRecords::Unknown(records) => records.len(),
            broca_wal::WalRangeRecords::Absent => 0,
        };
        self.decoded.fetch_add(count as u64, Ordering::Relaxed);
    }
}

impl WalStore for StatusCostWal {
    fn open(
        &self,
        key: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.inner.open(key)
    }
    fn read_records(
        &self,
        key: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        let records = self.inner.read_records(key)?;
        self.from_zero.fetch_add(1, Ordering::Relaxed);
        self.decoded
            .fetch_add(records.len() as u64, Ordering::Relaxed);
        Ok(records)
    }
    fn read_records_range(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        let range = self.inner.read_records_range(key, after, through, anchor)?;
        self.count_range(after, &range);
        Ok(range)
    }
    fn read_records_range_from_valid_anchor(
        &self,
        key: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: &broca_wal::WalByteAnchor,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        let range = self
            .inner
            .read_records_range_from_valid_anchor(key, after, through, anchor)?;
        if let Some(range) = &range {
            self.count_range(after, range);
        }
        Ok(range)
    }
    fn live_wal_bytes(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.live_wal_bytes(key)
    }
    fn wal_present(&self, key: &BindIdentity) -> Option<bool> {
        self.inner.wal_present(key)
    }
    fn file_identity(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.file_identity(key)
    }
    fn flush_for_read(&self, key: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.flush_for_read(key)
    }
    fn is_retention_lineage(&self, key: &BindIdentity) -> Result<bool, broca_wal::WalError> {
        self.inner.is_retention_lineage(key)
    }
}

#[tokio::test]
async fn large_closed_session_status_poll_cost_is_independent_of_history() {
    let dir = broca_test_support::scratch_root()
        .join(format!("large-closed-status-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let key = BindIdentity::new(&dir, "runner", "worker");
    let inner = Arc::new(FileWalStore::new(dir.join("wal")));
    let mut writer = inner.open(&key).unwrap();
    for episode in 0..400 {
        let run = RunId(format!("episode-{episode}"));
        writer
            .append(1, &unsealed_start(&key, &run), false)
            .unwrap();
        writer
            .append(
                1,
                &WalRecord::ModelStepFinished {
                    tags: None,
                    hooks: None,
                    step_id: StepId(1),
                    assistant_message: AssistantMessage {
                        message_id: MessageId(format!("answer-{episode}")),
                        content: vec![ContentBlock::Text {
                            text: "x".repeat(8192),
                        }],
                    }
                    .into(),
                    usage: Usage::default(),
                    finish_reason: Some(FinishReason::Stop),
                    retries_used: 0,
                    service_tier_used: None,
                    speed_used: None,
                },
                false,
            )
            .unwrap();
        writer
            .append(
                1,
                &WalRecord::RunFinished {
                    interrupt_reason: None,
                    ts_ms: None,
                    reason: RunFinishReason::Completed,
                    usage: Usage::default(),
                    error: None,
                    retries_used: 0,
                },
                false,
            )
            .unwrap();
    }
    writer.sync().unwrap();
    drop(writer);
    assert!(
        inner.live_wal_bytes(&key).unwrap().unwrap() > crate::registry::STATUS_WAL_READ_MAX_BYTES
    );
    let store = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    let range = inner.read_records_range(&key, 0, None, None).unwrap();
    broca_core::Persister::project_range(store.as_ref(), &key, &range).unwrap();
    let wal = Arc::new(StatusCostWal {
        inner,
        from_zero: AtomicU64::new(0),
        decoded: AtomicU64::new(0),
    });
    let opener = DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        Arc::new(TerminalExecutor(RunFinishReason::Completed)),
    )
    .with_materialized_store(store.clone())
    .with_run_index(store.clone())
    .with_idle_ttl(Duration::from_secs(3600));
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), Some(store));
    // The compatibility fallback pays ordinary actor recovery once, not on every poll.
    registry.status(&key).await.unwrap();
    wal.from_zero.store(0, Ordering::Relaxed);
    wal.decoded.store(0, Ordering::Relaxed);
    for _ in 0..20 {
        assert!(matches!(
            registry.status(&key).await.unwrap(),
            crate::serve::StatusResponse::Idle
        ));
    }
    assert_eq!(
        wal.from_zero.load(Ordering::Relaxed),
        0,
        "closed status must not reread from sequence zero on each poll"
    );
    assert_eq!(
        wal.decoded.load(Ordering::Relaxed),
        0,
        "closed status must not decode history on each poll"
    );
    assert_eq!(
        registry.slot_counts().open,
        1,
        "large logs retain the legacy actor fallback"
    );
    assert!(registry.stop_and_seal().await);
    std::fs::remove_dir_all(dir).unwrap();
}
