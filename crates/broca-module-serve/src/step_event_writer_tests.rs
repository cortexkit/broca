struct StepEventCapture {
    tx: tokio::sync::mpsc::UnboundedSender<(String, Vec<u8>)>,
}

#[async_trait::async_trait]
impl broca_module_serve::step_events::StepEventPublisher for StepEventCapture {
    async fn publish(&self, subject: &str, body: Vec<u8>) -> Result<(), String> {
        self.tx.send((subject.to_owned(), body)).map_err(|error| error.to_string())
    }
}

#[tokio::test]
async fn step_events_publish_frozen_decision_on_every_admitted_step() {
    use broca_core::{LeaseStore as _, WalStore as _};
    if !run_catalog_drift_child("step_events_publish_frozen_decision_on_every_admitted_step") {
        return;
    }
    install_queued_ultrafast_catalog();
    let root = fresh_vault_root("step-event-admission");
    let http = tier_http_fixture(vec![None, None, None], true, None, None).await;
    let (mut live, subc) = stand_in_executor(&root).await;
    let store = Arc::new(FileWalStore::with_config(root.join("wal"), broca_wal::WalConfig::OVERHAUL));
    live.wal_store = store.clone();
    live.provider_base_url = Some(http.url.clone());
    let mut req = drift_request(&root);
    req.model = broca_protocol::ModelHandle::new("openai", "test-ultrafast");
    let session = req.session.clone();
    let (tx, mut published) = tokio::sync::mpsc::unbounded_channel();
    let events = broca_module_serve::step_events::StepEvents::start(
        broca_module_serve::step_events::Config { step_events_enabled: true },
        "fixture", "broca", Arc::new(StepEventCapture { tx }),
    ).unwrap().unwrap();
    let (records, _records_inbox) = tokio::sync::broadcast::channel(16);
    let observer = events.observe(broca_core::RecordObserver::new(records,
        Arc::new(std::sync::atomic::AtomicUsize::new(0))));
    let leases = broca_core::FileLeaseStore::new(root.join("leases"));
    let lease = leases.acquire(&broca_core::lease_key_for(&session)).unwrap();
    let (writer, lease) = broca_core::WalHandle::open_and_spawn_observed(
        store.clone(), session.clone(), lease, Some(observer),
    ).await.unwrap();
    let writer = Arc::new(writer);

    for (index, decision, expected_steps) in [(0, Some("caller-decision"), 2), (1, None, 1)] {
        req.run_id = RunId(format!("step-events-{index}"));
        req.route_decision_id = decision.map(str::to_owned);
        let run = req.run_id.clone();
        let head = writer.sync().await.unwrap();
        let prior_snapshot = broca_core::load_prior_wal_snapshot(
            store.as_ref(), &session, head, None::<&broca_store::SqliteStore>,
        ).unwrap();
        let (admitted, admission) = tokio::sync::oneshot::channel();
        let job = RunJob {
            queued_service_tier: None, media: vec![], media_profile: None,
            conversation_key: None, req: req.clone(), auth: None, writer: writer.clone(),
            kind: if index == 0 { RunKind::FreshOrResume } else { RunKind::AppendEpisode },
            skip_materialization: true, control: Default::default(), prior_snapshot,
            on_admitted: Some(admitted),
        };
        let (out, _output) = tokio::sync::mpsc::channel(64);
        let summary = broca_test_support::wait_for("step-event provider run", live.execute(job, out),
            || format!("run={}", run.0)).await.unwrap();
        assert_eq!(summary.reason, broca_protocol::RunFinishReason::Completed);
        assert_eq!(admission.await.unwrap().unwrap(), run);
        let frames = store.read_records(&session).unwrap();
        let frozen = frames.iter().find_map(|frame| match &frame.record {
            broca_wal::WalRecord::RunStarted { run_id, config, .. } if run_id == &run => Some(config),
            _ => None,
        }).unwrap();
        assert_eq!(frozen.route_decision_id.as_deref(), decision);
        for _ in 0..expected_steps {
            let (subject, body) = broca_test_support::wait_for("published durable step", published.recv(),
                || format!("run={}", run.0)).await.unwrap();
            assert_eq!(subject, "ck.fixture.live.broca.step.finished.v1");
            let event: cortexkit_role_llm_runner::step_events::StepFinished = serde_json::from_slice(&body).unwrap();
            event.check().unwrap();
            if let Some(dir) = std::env::var_os("BROCA_STEP_EVENT_SAMPLES_DIR") {
                let dir = std::path::PathBuf::from(dir);
                std::fs::create_dir_all(&dir).unwrap();
                std::fs::write(dir.join(if decision.is_some() { "step.finished.v1.json" } else { "step.finished-without-decision.v1.json" }), &body).unwrap();
            }
            assert_eq!(event.route_decision_id.as_deref(), decision);
            if decision.is_none() {
                assert!(serde_json::from_slice::<serde_json::Value>(&body).unwrap().get("route_decision_id").is_none());
            }
            let recorded = frames.iter().find_map(|frame| match &frame.record {
                broca_wal::WalRecord::ModelAttemptFinished { attempt_id: Some(id), reservation: Some(reservation), sent_at_ms, content_progress, .. }
                    if event.attempt_id.as_deref() == Some(id.as_str()) && reservation.run_id == run => {
                        Some((*sent_at_ms, content_progress.first_content_at_ms, content_progress.last_content_at_ms))
                    }
                _ => None,
            }).unwrap();
            let (Some(sent), Some(first), Some(last)) = recorded else { panic!("fixture content stamps were not recorded"); };
            assert!(sent <= first && first <= last);
            assert_eq!(event.sent_to_last_content_ms, Some(last - sent));
            assert_eq!(event.first_to_last_content_ms, Some(last - first));
        }
    }
    Arc::try_unwrap(writer).ok().expect("run retained writer").shutdown().await.unwrap();
    drop(lease);
    drop(events);
    drop(live);
    drop(store);
    subc.abort();
    std::fs::remove_dir_all(root).unwrap();
}
