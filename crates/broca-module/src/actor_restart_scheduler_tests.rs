use super::*;

#[test]
fn durable_recovery_keeps_replay_state_off_caller_frames() {
    fn future_size<'a, W: WalStore + 'static, F: std::future::Future>(
        _: impl FnOnce(&'a mut LineageActor<W>) -> F,
    ) -> usize {
        std::mem::size_of::<F>()
    }
    // Recovery also runs inside actor command handling on a worker's small stack.
    // Its orchestration future must not embed the full replay/admission state.
    let production = future_size(LineageActor::<FileWalStore>::recover_durable_work);
    let probe = future_size(LineageActor::<ProbeWal>::recover_durable_work);
    eprintln!("recovery future bytes: production={production}, probe={probe}");
    assert!(
        production <= 4096 && probe <= 4096,
        "recovery must keep large replay state on the heap: production={production}, probe={probe} bytes"
    );
}

struct Running {
    task: tokio::task::JoinHandle<Fixture>,
    tx: mpsc::Sender<LineageCmd>,
    executor: Arc<ProbeExecutor>,
    store: Arc<ProbeWal>,
    key: BindIdentity,
    seal: CancellationToken,
}

impl Running {
    async fn start(refuse: bool) -> Self {
        let fixture = Fixture::new(Seed::RunControlRestartPaused, true).await;
        Self::from_fixture(fixture, refuse).await
    }

    async fn from_fixture(fixture: Fixture, refuse: bool) -> Self {
        fixture.executor.media_probe.store(1, Ordering::SeqCst);
        fixture
            .executor
            .gate_first_probe
            .store(true, Ordering::SeqCst);
        fixture
            .executor
            .refuse_first_probe
            .store(refuse, Ordering::SeqCst);
        let running = Self {
            tx: fixture.actor.self_tx.clone(),
            executor: fixture.executor.clone(),
            store: fixture.wal_store.clone(),
            key: fixture.actor.key.clone(),
            seal: fixture.actor.registry.seal_signal(),
            task: tokio::spawn(async move {
                let mut fixture = fixture;
                Box::pin(fixture.actor.run()).await;
                fixture
            }),
        };
        running.executor.entered.notified().await;
        running
    }

    async fn barrier(&self) {
        let (reply, receive) = oneshot::channel();
        self.tx.send(LineageCmd::Status { reply }).await.unwrap();
        receive.await.unwrap();
    }

    async fn complete(&self, count: u32) {
        broca_test_support::event_timeout(
            "scheduled core completions",
            async {
                self.executor
                    .completions
                    .acquire_many(count)
                    .await
                    .unwrap()
                    .forget();
            },
            || {
                format!(
                    "manifests={:?}, errors={:?}, WAL={:?}",
                    self.executor.manifests.lock().unwrap(),
                    self.executor.errors.lock().unwrap(),
                    self.store.read_records(&self.key)
                )
            },
        )
        .await
        .expect("the production scheduler completes the expected runs");
        self.barrier().await;
    }

    async fn close(self) {
        self.seal.cancel();
        let fixture = self.task.await.unwrap();
        fixture.close().await;
    }
}

async fn accept(running: &Running, id: &str, delivery: broca_protocol::Delivery, texts: &[&str]) {
    let (cmd, reply) = control(id, delivery, texts);
    running.tx.send(cmd).await.unwrap();
    assert!(matches!(
        reply.await.unwrap(),
        SendOutcome::Active { .. } | SendOutcome::Pending { .. }
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn scheduler_steer_between_sync_selection_and_prefix_read_is_delivered_once() {
    let running = Running::start(false).await;
    accept(
        &running,
        "first",
        broca_protocol::Delivery::Steer,
        &["first steer"],
    )
    .await;
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, wait) = std::sync::mpsc::channel();
    *running.executor.core_read_gate.lock().unwrap() = Some((entered.clone(), wait));
    running.executor.release.notify_one();
    entered.notified().await;
    accept(
        &running,
        "second",
        broca_protocol::Delivery::Steer,
        &["second steer"],
    )
    .await;
    release.send(()).unwrap();
    running.complete(1).await;
    let requests = running.executor.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    let request = String::from_utf8_lossy(&requests[0]);
    assert_eq!(request.matches("first steer").count(), 1);
    assert_eq!(request.matches("second steer").count(), 1);
    assert_eq!(requests, *running.executor.replay_requests.lock().unwrap());
    assert!(running.executor.errors.lock().unwrap().is_empty());
    running.close().await;
}

#[tokio::test]
async fn scheduler_multiple_multimessage_steers_keep_identical_boundary_inputs_and_queue() {
    for bounded in [false, true] {
        let fixture = Fixture::new(Seed::RunControlRestartPaused, true).await;
        fixture
            .executor
            .bounded_core_snapshot
            .store(bounded, Ordering::SeqCst);
        fixture.actor.writer.as_ref().unwrap().record(serde_json::from_value(serde_json::json!({
            "type":"steered", "input":[{"role":"user","content":[{"type":"text","text":"identical"}]},{"role":"user","content":[{"type":"text","text":"paused steer"}]}]
        })).unwrap()).into_result().unwrap();
        fixture.actor.writer.as_ref().unwrap().sync().await.unwrap();
        let running = Running::from_fixture(fixture, false).await;
        accept(
            &running,
            "first",
            broca_protocol::Delivery::Steer,
            &["identical", "live A"],
        )
        .await;
        accept(
            &running,
            "second",
            broca_protocol::Delivery::Steer,
            &["live B", "identical"],
        )
        .await;
        accept(
            &running,
            "queued",
            broca_protocol::Delivery::Queue,
            &["queue only"],
        )
        .await;
        running.executor.release.notify_one();
        running.complete(2).await;
        let requests = running.executor.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        let first = String::from_utf8_lossy(&requests[0]);
        assert_eq!(first.matches("identical").count(), 3);
        for text in ["paused steer", "live A", "live B"] {
            assert_eq!(first.matches(text).count(), 1);
        }
        assert!(!first.contains("queue only"));
        assert_eq!(
            String::from_utf8_lossy(&requests[1])
                .matches("queue only")
                .count(),
            1
        );
        assert_eq!(requests, *running.executor.replay_requests.lock().unwrap());
        if bounded {
            assert!(running.store.suffix_reads.load(Ordering::SeqCst) > 0);
        }
        running.close().await;
    }
}

#[tokio::test]
async fn scheduler_ready_media_seed_resumes_when_only_its_forward_suffix_is_readable() {
    let fixture = Fixture::new(Seed::RunControlRestartPaused, true).await;
    fixture.actor.writer.as_ref().unwrap().record(serde_json::from_value(serde_json::json!({
        "type":"steered","input":[{"role":"user","content":[{"type":"media","kind":"image","mime":"image/png","byte_size":1,"sha256":"a".repeat(64),"width":1,"height":1}]}]
    })).unwrap()).into_result().unwrap();
    fixture.actor.writer.as_ref().unwrap().sync().await.unwrap();
    let running = Running::from_fixture(fixture, false).await;
    accept(
        &running,
        "live",
        broca_protocol::Delivery::Steer,
        &["text after earlier capture"],
    )
    .await;
    running
        .store
        .next_range_must_be_suffix
        .store(true, Ordering::SeqCst);
    running.executor.release.notify_one();
    running.complete(1).await;
    assert!(running.executor.errors.lock().unwrap().is_empty());
    let requests = running.executor.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(
        String::from_utf8_lossy(&requests[0])
            .matches("text after earlier capture")
            .count(),
        1
    );
    assert_eq!(requests, *running.executor.replay_requests.lock().unwrap());
    running.close().await;
}

fn control(
    id: &str,
    delivery: broca_protocol::Delivery,
    texts: &[&str],
) -> (LineageCmd, oneshot::Receiver<SendOutcome>) {
    let input: Vec<_> = texts
        .iter()
        .map(|text| serde_json::json!({"role":"user","content":[{"type":"text","text":text}]}))
        .collect();
    let request = serde_json::from_value(
        serde_json::json!({"model":{"provider_module_id":"fake","model_id":"fake"},"input":input}),
    )
    .unwrap();
    let (reply, receive) = oneshot::channel();
    (
        LineageCmd::Send {
            session_class: None,
            retention: None,
            stop_sequences_named: false,
            conversation_key: None,
            title: None,
            run_id: RunId(id.into()),
            request: Box::new(request),
            kind: RunKind::FreshOrResume,
            key: SendKey::SendId(id.into()),
            skip_materialization: false,
            delivery,
            plan: None,
            order: None,
            reply,
        },
        receive,
    )
}

async fn settings(running: &Running) -> Vec<Receipt> {
    let mut receipts = Vec::new();
    for mutation in [Mutation::Refresh, Mutation::Policy, Mutation::Flush] {
        let (mut cmd, receipt) = command(mutation, &Default::default());
        if let LineageCmd::SessionRefresh { manifest, .. } = &mut cmd {
            let composition = serde_json::json!({"providers":[]});
            let digest =
                cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
            let plan = serde_json::json!({"tool_items":[],"system_text_items":[],"step_transform_items":[],"composition":composition,"composition_digest":digest});
            broca_fetch_plan::parse_plan_with(
                &plan,
                broca_fetch_plan::CompactionSupport::Supported,
                broca_fetch_plan::OutputRefsSupport::Supported,
            )
            .expect("the refresh carries an admissible empty plan");
            manifest.plan = broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap();
            manifest.composition_digest = digest;
        }
        if let LineageCmd::SessionRefreshPolicy { generation, .. } = &mut cmd {
            *generation = 2;
        }
        running.tx.send(cmd).await.unwrap();
        receipts.push(receipt);
    }
    running.barrier().await;
    receipts
}

async fn ordered_settings(refuse: bool) {
    let running = Running::start(refuse).await;
    let receipts = settings(&running).await;
    let (queue, receive) = control("queued", broca_protocol::Delivery::Queue, &["queued input"]);
    running.tx.send(queue).await.unwrap();
    assert!(matches!(
        receive.await.unwrap(),
        SendOutcome::Pending { .. }
    ));
    running.executor.release.notify_one();
    // Receipts must be produced by the production scheduler, not a test draining its lanes.
    let mut receipts = receipts.into_iter();
    let Receipt::Refresh(refresh) = receipts.next().unwrap() else {
        unreachable!()
    };
    assert!(
        matches!(refresh.await.unwrap(), SessionRefreshOutcome::Accepted { run_in_progress, .. } if run_in_progress != refuse)
    );
    for receipt in receipts {
        receipt.accepted().await;
    }
    running.complete(if refuse { 1 } else { 2 }).await;
    let manifests = running.executor.manifests.lock().unwrap().clone();
    assert_eq!(
        manifests.last().unwrap().as_deref(),
        Some("next"),
        "settings must precede queued successor admission"
    );
    let records = running.store.read_records(&running.key).unwrap();
    let order: Vec<_> = records
        .iter()
        .filter_map(|frame| match &frame.record {
            WalRecord::SessionRefreshAccepted { generation: 2, .. } => Some("refresh"),
            WalRecord::SessionRefreshPolicyChanged { send_id, .. }
                if send_id == "nested-policy" =>
            {
                Some("policy")
            }
            WalRecord::SessionPrefixFlushed { send_id, .. } if send_id == "nested-flush" => {
                Some("flush")
            }
            WalRecord::RunStarted { run_id, .. } if run_id.0 == "queued" => Some("successor"),
            _ => None,
        })
        .collect();
    assert_eq!(order, ["refresh", "policy", "flush", "successor"]);
    let successor = records
        .iter()
        .position(
            |f| matches!(&f.record, WalRecord::RunStarted { run_id, .. } if run_id.0 == "queued"),
        )
        .unwrap();
    let flush = records.iter().position(|f| matches!(&f.record, WalRecord::SessionPrefixFlushed { send_id, .. } if send_id == "nested-flush")).unwrap();
    assert!(flush < successor);
    running.close().await;
}

#[tokio::test]
async fn scheduler_applies_deferred_settings_before_queue_after_refused_resume() {
    ordered_settings(true).await;
}

#[tokio::test]
async fn scheduler_applies_deferred_settings_before_queue_after_successful_resume() {
    ordered_settings(false).await;
}

#[tokio::test]
async fn scheduler_saturated_settings_do_not_block_run_control() {
    let running = Running::start(false).await;
    let mut receipts = Vec::new();
    for _ in 0..64 {
        let (cmd, receipt) = command(Mutation::Flush, &Default::default());
        running.tx.send(cmd).await.unwrap();
        receipts.push(receipt);
    }
    let progress = broca_test_support::event_timeout(
        "status behind saturated deferred settings",
        running.barrier(),
        || "admission must keep receiving run control while settings are full".into(),
    )
    .await;
    // Release even on a failed assertion so the scheduler is not abandoned behind its gate.
    if progress.is_err() {
        running.executor.release.notify_one();
    }
    progress.expect("status must progress without releasing admission");
    let (cmd, Receipt::Flush(refused)) = command(Mutation::Flush, &Default::default()) else {
        unreachable!()
    };
    running.tx.send(cmd).await.unwrap();
    assert!(
        matches!(refused.await.unwrap(), SessionFlushPrefixOutcome::Error(error) if error.contains("session.flush_prefix") && error.contains("retry"))
    );
    let (steer, reply) = control("live", broca_protocol::Delivery::Steer, &["live steer"]);
    running.tx.send(steer).await.unwrap();
    assert!(matches!(reply.await.unwrap(), SendOutcome::Active { .. }));
    let (queue, reply) = control("later", broca_protocol::Delivery::Queue, &["queued send"]);
    running.tx.send(queue).await.unwrap();
    assert!(matches!(reply.await.unwrap(), SendOutcome::Pending { .. }));
    running.executor.release.notify_one();
    for receipt in receipts {
        receipt.accepted().await;
    }
    running.complete(2).await;
    running.close().await;
}

#[tokio::test]
async fn scheduler_deferred_settings_precede_newer_normal_settings() {
    let running = Running::start(false).await;
    let mut receipts = settings(&running).await.into_iter();
    running.executor.release.notify_one();
    receipts.next().unwrap().accepted().await;
    let (reply, receive) = oneshot::channel();
    running
        .tx
        .send(LineageCmd::SessionRefreshPolicy {
            generation: 2,
            policy: "on_prefix_rebuild".into(),
            send_id: "newer-policy".into(),
            reply,
        })
        .await
        .unwrap();
    for receipt in receipts {
        receipt.accepted().await;
    }
    assert_eq!(receive.await.unwrap(), SessionRefreshPolicyOutcome::Changed);
    running.complete(1).await;
    let records = running.store.read_records(&running.key).unwrap();
    let flush = records.iter().position(|f| matches!(&f.record, WalRecord::SessionPrefixFlushed { send_id, .. } if send_id == "nested-flush")).unwrap();
    let newer = records.iter().position(|f| matches!(&f.record, WalRecord::SessionRefreshPolicyChanged { send_id, .. } if send_id == "newer-policy")).unwrap();
    assert!(flush < newer);
    running.close().await;
}

#[tokio::test]
async fn scheduler_shutdown_answers_and_applies_parked_settings() {
    let running = Running::start(false).await;
    let receipts = settings(&running).await;
    running.seal.cancel();
    for receipt in receipts {
        receipt.accepted().await;
    }
    let records = running.store.read_records(&running.key).unwrap();
    assert!(records.iter().any(|f| matches!(&f.record, WalRecord::SessionPrefixFlushed { send_id, .. } if send_id == "nested-flush")));
    running.close().await;
}

#[tokio::test]
async fn scheduler_cancel_progresses_with_a_full_deferred_settings_lane() {
    let running = Running::start(false).await;
    let mut receipts = Vec::new();
    for _ in 0..64 {
        let (cmd, receipt) = command(Mutation::Flush, &Default::default());
        running.tx.send(cmd).await.unwrap();
        receipts.push(receipt);
    }
    let (reply, receive) = oneshot::channel();
    running
        .tx
        .send(LineageCmd::Cancel {
            run_id: RunId("prior".into()),
            at: broca_wal::record::RunCancelAt::Now,
            reply,
        })
        .await
        .unwrap();
    let response =
        broca_test_support::event_timeout("cancel behind full deferred settings", receive, || {
            "cancel must not wait for dependency admission".into()
        })
        .await
        .expect("cancel progresses")
        .unwrap();
    assert_eq!(response, CancelOutcome::Ack);
    running.executor.release.notify_one();
    for receipt in receipts {
        receipt.accepted().await;
    }
    running.close().await;
}
