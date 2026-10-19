struct PauseBeforeCancel {
    wal: Arc<FlakyReadWalStore>,
    release: tokio::sync::Notify,
    attempts: AtomicUsize,
}

#[async_trait]
impl RunExecutor for PauseBeforeCancel {
    async fn execute(
        &self,
        mut job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        if self.attempts.fetch_add(1, Ordering::SeqCst) > 0 {
            return broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
                job.req, &job.writer, broca_core::StartMode::FreshOrResume,
                &broca_core::StubProvider::new(vec![]), &broca_core::StubToolPlane::new(),
                &broca_core::AllowAll, self.wal.as_ref(), None, job.control, &out,
                job.on_admitted, Some(&job.prior_snapshot),
            ).await;
        }
        job.writer.record(WalRecord::RunStarted { tags: None, session_class: None, retention: None, hooks: None, title: None, on_restart: None, keep_warm: None,
        marker_order_version: None, manifest: None, scope: None, send_id: None,
        request_identity_digest: None, project_id: None, principal: None,
        ts_ms: Some(100), run_id: job.req.run_id.clone(), session: job.req.session.clone(),
        config: Box::new(min_config()), input: job.req.input.clone().into(),
        origin: broca_wal::RunOrigin::Fresh, submission_id: None, });
        job.writer.sync().await?;
        let _ = job.on_admitted.take().unwrap().send(Ok(job.req.run_id.clone()));
        self.release.notified().await;
        job.writer.record(WalRecord::RunPaused {
            run_id: job.req.run_id.clone(), ts_ms: Some(200),
            reason: broca_protocol::PauseReason::Restart, error: None,
        });
        job.writer.sync().await?;
        // The next read belongs to the paused cancel finalizer, not the executor.
        self.wal.fail_next_read();
        Ok(RunSummary {
            reason: RunFinishReason::Completed,
            paused: Some(broca_protocol::PauseReason::Restart), steps: 0,
            usage: Usage::default(), retries_used: 0, final_step_finish_reason: None,
        })
    }
}

#[tokio::test]
async fn acknowledged_cancel_retries_paused_finalizer_after_one_read_failure() {
    let dir = broca_test_support::scratch_root().join(format!("cancel-pause-retry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let key = BindIdentity::new(dir.join("project"), "runner", "session");
    let wal = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
    let executor = Arc::new(PauseBeforeCancel {
        wal: wal.clone(), release: tokio::sync::Notify::new(), attempts: AtomicUsize::new(0),
    });
    let registry = LineageRegistry::new(Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))), wal.clone(), executor.clone(),
    ).with_idle_ttl(Duration::from_millis(30))));
    registry.route_send(&key, text_req(&key, "r"), RunKind::FreshOrResume).await.unwrap();
    assert_eq!(registry.route_cancel(&key, RunId("r".into())).await.unwrap(), CancelOutcome::Ack);
    executor.release.notify_one();
    broca_test_support::event_timeout("acknowledged_cancel_retries_paused_finalizer_after_one_read_failure", async {
        loop {
            if wal.durable_records(&key).iter().any(|frame| matches!(
                frame.record, WalRecord::RunFinished { reason: RunFinishReason::Cancelled, .. }
            )) { break; }
            tokio::task::yield_now().await;
        }
    }, || broca_test_support::record_kinds(wal.durable_records(&key).iter().map(|f| &f.record))).await.expect("acknowledged cancellation must finish without another command");
    assert_eq!(executor.attempts.load(Ordering::SeqCst), 2);
    assert!(registry.stop_and_seal().await);
    let _ = std::fs::remove_dir_all(dir);
}

#[tokio::test]
async fn cancel_lookup_failure_is_error_but_healthy_absence_is_not_active() {
    let dir = broca_test_support::scratch_root().join(format!("cancel-lookup-error-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let key = BindIdentity::new(dir.join("project"), "runner", "session");
    let wal = Arc::new(FlakyReadWalStore::new(dir.join("wal")));
    let registry = LineageRegistry::new(Arc::new(crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))), wal.clone(),
        Arc::new(QuickDone { close_count: Arc::new(AtomicUsize::new(0)) }),
    ).with_idle_ttl(Duration::from_secs(60))));
    registry.actor_for(&key).await.unwrap();
    registry.route_status(&key).await.unwrap();
    wal.fail_next_read();
    assert!(matches!(crate::serve::ServeHandler::cancel(&registry, &key, RunId("missing".into())).await,
        Err(crate::serve::ServeError::Open(_, _))));
    assert_eq!(crate::serve::ServeHandler::cancel(&registry, &key, RunId("missing".into())).await.unwrap(),
        crate::serve::CancelResponse::NotActive);
    assert!(registry.stop_and_seal().await);
    let _ = std::fs::remove_dir_all(dir);
}
