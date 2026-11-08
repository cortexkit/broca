use super::*;
use broca_provider::wire::WireFamily;

pub(super) fn manifest(identity: &str) -> broca_wal::PlanManifest {
    broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text("{}").unwrap(),
        plan_identity: identity.into(),
        composition_digest: "future".into(),
        tools: Arc::new(vec![]),
        system_text: "future system text".into(),
        system_digest: "future".into(),
        join_version: 1,
        role_versions: Default::default(),
        fetches: vec![],
        declarations: vec![],
    }
}

struct Provider {
    frozen: broca_provider::wire::FrozenRenderConfig,
    requests: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    replay_requests: Arc<std::sync::Mutex<Vec<Vec<u8>>>>,
    wal_store: Arc<ProbeWal>,
    session: BindIdentity,
}

#[async_trait::async_trait]
impl broca_core::Provider for Provider {
    fn supports_media_renderer(&self) -> bool {
        true
    }
    fn renderer_version(&self) -> u32 {
        self.frozen.renderer_version
    }
    fn render_config_blob(&self) -> serde_json::Value {
        serde_json::to_value(&self.frozen).unwrap()
    }
    fn render_request(&self, opts: &broca_protocol::CallOptions) -> Vec<u8> {
        broca_provider::families::OpenAiChat.render(&self.frozen, opts)
    }
    async fn step(
        &self,
        _: broca_protocol::StepId,
        opts: &broca_protocol::CallOptions,
    ) -> mpsc::Receiver<broca_protocol::NormalizedEvent> {
        let records = self.wal_store.read_records(&self.session).unwrap();
        let mut replay = broca_core::replay(
            &records
                .into_iter()
                .map(|frame| frame.record)
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let mut oracle = opts.clone();
        if let Some(config) = replay.run_config.as_ref() {
            oracle.tools = config.tools.clone();
            oracle.tool_choice = config.tool_choice.clone().into();
            oracle.generation = config.generation.clone().into();
        }
        replay.prompt.append(&mut replay.pending_steers);
        oracle.prompt = replay.prompt;
        self.replay_requests
            .lock()
            .unwrap()
            .push(self.render_request(&oracle));
        self.requests
            .lock()
            .unwrap()
            .push(self.render_request(opts));
        let (tx, rx) = mpsc::channel(4);
        tx.try_send(broca_protocol::NormalizedEvent::Control(
            broca_protocol::provider::ProviderControl::AssistantMessage {
                message: broca_protocol::AssistantMessage {
                    message_id: broca_protocol::MessageId("resumed".into()),
                    content: vec![broca_protocol::ContentBlock::Text {
                        text: "resumed answer".into(),
                    }],
                },
            },
        ))
        .unwrap();
        tx.try_send(broca_protocol::NormalizedEvent::Control(
            broca_protocol::provider::ProviderControl::Finished {
                usage: Default::default(),
                finish_reason: broca_protocol::FinishReason::Stop,
                retries_used: 0,
            },
        ))
        .unwrap();
        rx
    }
}

pub(super) async fn execute_probe(
    executor: &ProbeExecutor,
    mut job: crate::executor::RunJob,
    out: mpsc::Sender<broca_core::OutputEvent>,
) -> Result<RunSummary, RunError> {
    assert!(
        !job.writer.is_overhaul(),
        "the probe must exercise today's legacy format"
    );
    if executor.media_probe.load(Ordering::SeqCst) == 1 {
        if let Err(error) = broca_core::media_request::initialize_media_checkpoint(
            &job.writer,
            executor.wal_store.clone(),
            &job.req.session,
            &job.prior_snapshot,
        )
        .await
        {
            let error = RunError::Wal(error);
            if let Some(ack) = job.on_admitted {
                let _ = ack.send(Err(error.clone()));
            }
            return Err(error);
        }
    }
    if executor.bounded_core_snapshot.load(Ordering::SeqCst) {
        let head = job.writer.sync().await.unwrap();
        let mut snapshot = broca_core::load_prior_wal_snapshot(
            executor.wal_store.as_ref(),
            &job.req.session,
            head,
            None::<&broca_store::SqliteStore>,
        )
        .unwrap();
        snapshot.base_watermark = head;
        snapshot.checkpoint_at_base = Some(snapshot.checkpoint_at_head.clone());
        snapshot.tail = Arc::new(Vec::new());
        job.prior_snapshot = snapshot;
    }
    *executor.wal_store.core_read_gate.lock().unwrap() =
        executor.core_read_gate.lock().unwrap().take();
    let config = &job
        .prior_snapshot
        .checkpoint_at_head
        .last_episode
        .as_ref()
        .unwrap()
        .config;
    let provider = Provider {
        frozen: serde_json::from_value(config.frozen_render_config.clone()).unwrap(),
        requests: executor.requests.clone(),
        replay_requests: executor.replay_requests.clone(),
        wal_store: executor.wal_store.clone(),
        session: job.req.session.clone(),
    };
    let mode = match job.kind {
        RunKind::FreshOrResume => broca_core::StartMode::FreshOrResume,
        RunKind::AppendEpisode => broca_core::StartMode::AppendEpisode,
        RunKind::ResumeAfterAuth => broca_core::StartMode::ResumeAfterAuth,
    };
    let result = broca_core::run_loop_on_writer_admitted_with_prior_snapshot(
        job.req,
        &job.writer,
        mode,
        &provider,
        &broca_core::StubToolPlane::new(),
        &broca_core::AllowAll,
        executor.wal_store.as_ref(),
        None,
        job.control,
        &out,
        job.on_admitted,
        Some(&job.prior_snapshot),
    )
    .await;
    executor.completed_probe.notify_one();
    result
}

#[path = "actor_run_control_admission_tests.rs"]
mod run_control;

#[path = "actor_restart_scheduler_tests.rs"]
mod scheduler;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mutation {
    Warm,
    Refresh,
    Policy,
    Flush,
}

impl Mutation {
    fn written(self, records: &[broca_wal::FramedRecord]) -> bool {
        records.iter().any(|frame| match (&frame.record, self) {
            (WalRecord::CacheWarmed { .. }, Self::Warm) => true,
            (WalRecord::SessionRefreshAccepted { generation: 2, .. }, Self::Refresh) => true,
            (WalRecord::SessionRefreshPolicyChanged { send_id, .. }, Self::Policy) => {
                send_id == "nested-policy"
            }
            (WalRecord::SessionPrefixFlushed { send_id, .. }, Self::Flush) => {
                send_id == "nested-flush"
            }
            _ => false,
        })
    }
}

enum Receipt {
    Warm(oneshot::Receiver<WarmOutcome>),
    Refresh(oneshot::Receiver<SessionRefreshOutcome>),
    Policy(oneshot::Receiver<SessionRefreshPolicyOutcome>),
    Flush(oneshot::Receiver<SessionFlushPrefixOutcome>),
}

impl Receipt {
    async fn accepted(self) {
        match self {
            Self::Warm(reply) => assert!(matches!(reply.await.unwrap(), WarmOutcome::Warmed(_))),
            Self::Refresh(reply) => assert!(matches!(
                reply.await.unwrap(),
                SessionRefreshOutcome::Accepted { .. }
            )),
            Self::Policy(reply) => assert!(matches!(
                reply.await.unwrap(),
                SessionRefreshPolicyOutcome::Changed
            )),
            Self::Flush(reply) => assert!(matches!(
                reply.await.unwrap(),
                SessionFlushPrefixOutcome::Flushed { .. }
            )),
        }
    }
}

fn command(mutation: Mutation, slot: &crate::keep_warm::WarmSlot) -> (LineageCmd, Receipt) {
    match mutation {
        Mutation::Warm => {
            let (reply, receive) = oneshot::channel();
            let pending = PendingWarm::new(
                PendingWarmParts {
                    run_id: RunId("prior".into()),
                    step_id: broca_protocol::StepId(1),
                    request_body_sha256: "a".repeat(64),
                    ttl_ms: 60_000,
                },
                reply,
                slot.acquire().unwrap(),
            );
            (
                LineageCmd::WarmFinished {
                    warm: Box::new((
                        pending,
                        Ok(broca_core::WarmSent {
                            warm_body_sha256: "b".repeat(64),
                            sent_at_ms: 1,
                            usage: Default::default(),
                            served_account_ref: None,
                            ratelimit_headers: vec![],
                            http_status: 200,
                        }),
                    )),
                },
                Receipt::Warm(receive),
            )
        }
        Mutation::Refresh => {
            let (reply, receive) = oneshot::channel();
            (
                LineageCmd::SessionRefresh {
                    generation: 2,
                    policy: "immediately".into(),
                    manifest: Box::new(manifest("next")),
                    reply,
                },
                Receipt::Refresh(receive),
            )
        }
        Mutation::Policy => {
            let (reply, receive) = oneshot::channel();
            (
                LineageCmd::SessionRefreshPolicy {
                    generation: 1,
                    policy: "immediately".into(),
                    send_id: "nested-policy".into(),
                    reply,
                },
                Receipt::Policy(receive),
            )
        }
        Mutation::Flush => {
            let (reply, receive) = oneshot::channel();
            (
                LineageCmd::SessionFlushPrefix {
                    send_id: "nested-flush".into(),
                    reply,
                },
                Receipt::Flush(receive),
            )
        }
    }
}

async fn start_probe(actor: &mut LineageActor<ProbeWal>) -> Result<RunId, StartRunFailure> {
    let episode = actor
        .prior_snapshot
        .as_ref()
        .unwrap()
        .checkpoint_at_head
        .last_episode
        .as_ref()
        .unwrap();
    let request = submission_from_config(episode.config.clone(), episode.input.clone());
    actor
        .start_run(
            RunId("prior".into()),
            request,
            RunKind::FreshOrResume,
            None,
            SendIdentity {
                message_marks: None,
                display_sent_by: None,
                title: None,
                conversation_key: None,
                send_id: None,
                request_identity_digest: None,
            },
            false,
        )
        .await
}

async fn prepare(mutation: Mutation, media_on: bool) -> Fixture {
    let seed = if media_on {
        Seed::MediaRestartPaused
    } else {
        Seed::MediaOffRestartPaused
    };
    let fixture = Fixture::new(seed, true).await;
    assert!(!fixture.actor.writer.as_ref().unwrap().is_overhaul());
    fixture
        .executor
        .media_probe
        .store(if media_on { 1 } else { 2 }, Ordering::SeqCst);
    let snapshot = fixture.actor.prior_snapshot.as_ref().unwrap();
    assert!(snapshot.checkpoint_at_head.media.is_none());
    broca_core::run::seed_display_placement(
        fixture.actor.writer.as_ref().unwrap(),
        snapshot,
        fixture.wal_store.as_ref(),
        None,
        &fixture.actor.key,
    );
    println!("{mutation:?}: legacy root, media initialization={media_on}");
    fixture
}

async fn exercise(mutation: Mutation, media_on: bool) {
    let mut fixture = prepare(mutation, media_on).await;
    let episode = fixture
        .actor
        .prior_snapshot
        .as_ref()
        .unwrap()
        .checkpoint_at_head
        .last_episode
        .as_ref()
        .unwrap();
    let frozen = serde_json::from_value(episode.config.frozen_render_config.clone()).unwrap();
    let mut expected_options = broca_protocol::CallOptions::new(episode.input.clone());
    expected_options.tools = episode.config.tools.clone();
    expected_options.tool_choice = episode.config.tool_choice.clone().into();
    expected_options.generation = episode.config.generation.clone().into();
    let expected = broca_provider::families::OpenAiChat.render(&frozen, &expected_options);
    let tx = fixture.actor.self_tx.clone();
    let terminal = fixture.actor.terminal_tx.clone();
    let slot = fixture.actor.warm_in_flight.clone();
    let executor = fixture.executor.clone();
    let store = fixture.wal_store.clone();
    let key = fixture.actor.key.clone();
    let interleave = async move {
        executor.entered.notified().await;
        let (cmd, receipt) = command(mutation, &slot);
        // Warm completions use the terminal lane in production, not the normal command lane.
        if matches!(mutation, Mutation::Warm) {
            terminal.send(cmd).unwrap();
        } else {
            tx.send(cmd).await.unwrap();
        }
        let (reply, status) = oneshot::channel();
        tx.send(LineageCmd::Status { reply }).await.unwrap();
        status.await.unwrap();
        let before = store.read_records(&key).unwrap();
        let written = mutation.written(&before);
        println!("{mutation:?}: appended during the admission wait={written}");
        executor.release.notify_one();
        (receipt, written)
    };
    let (admission, (receipt, written)) = broca_test_support::event_timeout(
        "legacy restart admission interleaving",
        async { tokio::join!(start_probe(&mut fixture.actor), interleave) },
        || format!("{mutation:?}: restart admission did not finish"),
    )
    .await
    .expect("the executor gate is released after the status barrier");
    if !written && matches!(mutation, Mutation::Warm) {
        let deferred = fixture.actor.terminal_rx.recv().await.unwrap();
        assert!(!fixture.actor.handle_cmd(deferred).await);
    }
    receipt.accepted().await;
    let after = fixture.wal_store.read_records(&fixture.actor.key).unwrap();
    assert!(
        mutation.written(&after),
        "{mutation:?}: the command was dropped"
    );
    let diagnostic = admission.as_ref().err().map(ToString::to_string);
    assert!(
        admission.is_ok(),
        "{mutation:?}: appended during wait={written}, resumed admission={diagnostic:?}"
    );
    broca_test_support::event_timeout(
        "resumed model request",
        fixture.executor.completed_probe.notified(),
        || "the resumed core loop did not finish".into(),
    )
    .await
    .expect("the resumed run finishes");
    {
        let requests = fixture.executor.requests.lock().unwrap();
        assert_eq!(
            requests.as_slice(),
            &[expected],
            "{mutation:?}: the frozen resumed request changed"
        );
    }
    if media_on {
        assert!(
            !written,
            "{mutation:?}: a nested write raced media initialization"
        );
    }
    println!("{mutation:?}: exact frozen request preserved; media initialization={media_on}");
    fixture.close().await;
}

#[tokio::test]
async fn legacy_restart_admission_warm_finished_waits_on_the_terminal_lane() {
    exercise(Mutation::Warm, true).await;
}
#[tokio::test]
async fn legacy_restart_admission_defers_refresh_until_media_is_initialized() {
    exercise(Mutation::Refresh, true).await;
}
#[tokio::test]
async fn legacy_restart_admission_defers_refresh_policy_until_media_is_initialized() {
    exercise(Mutation::Policy, true).await;
}
#[tokio::test]
async fn legacy_restart_admission_defers_flush_until_media_is_initialized() {
    exercise(Mutation::Flush, true).await;
}
#[tokio::test]
async fn legacy_restart_admission_media_off_keeps_frozen_request_bytes() {
    for mutation in [
        Mutation::Warm,
        Mutation::Refresh,
        Mutation::Policy,
        Mutation::Flush,
    ] {
        exercise(mutation, false).await;
    }
}

async fn queued_metadata(refuse_admission: bool) {
    let mut fixture = prepare(Mutation::Refresh, true).await;
    if refuse_admission {
        fixture.executor.media_probe.store(0, Ordering::SeqCst);
    }
    let tx = fixture.actor.self_tx.clone();
    let executor = fixture.executor.clone();
    let store = fixture.wal_store.clone();
    let key = fixture.actor.key.clone();
    let slot = fixture.actor.warm_in_flight.clone();
    let interleave = async move {
        executor.entered.notified().await;
        let mut receipts = Vec::new();
        for mutation in [Mutation::Refresh, Mutation::Policy, Mutation::Flush] {
            let (mut cmd, receipt) = command(mutation, &slot);
            if let LineageCmd::SessionRefreshPolicy { generation, .. } = &mut cmd {
                *generation = 2;
            }
            tx.send(cmd).await.unwrap();
            receipts.push(receipt);
        }
        let (reply, status) = oneshot::channel();
        tx.send(LineageCmd::Status { reply }).await.unwrap();
        status.await.unwrap();
        let before = store.read_records(&key).unwrap();
        let wrote_before = [Mutation::Refresh, Mutation::Policy, Mutation::Flush]
            .iter()
            .any(|kind| kind.written(&before));
        executor.release.notify_one();
        (receipts, wrote_before)
    };
    let (admission, (receipts, wrote_before)) = broca_test_support::event_timeout(
        "queued restart metadata",
        async { tokio::join!(start_probe(&mut fixture.actor), interleave) },
        || "the restart admission or status barrier did not finish".into(),
    )
    .await
    .expect("the dependency gate is released after all metadata commands");
    assert_eq!(admission.is_err(), refuse_admission);
    assert!(
        !wrote_before,
        "metadata reached the WAL before admission completed"
    );
    for receipt in receipts {
        receipt.accepted().await;
    }
    let after = fixture.wal_store.read_records(&fixture.actor.key).unwrap();
    let order = after
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
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(order, ["refresh", "policy", "flush"]);
    if !refuse_admission {
        broca_test_support::event_timeout(
            "resumed core completion",
            fixture.executor.completed_probe.notified(),
            || "the resumed core loop did not finish".into(),
        )
        .await
        .expect("the resumed core loop completes");
    } else {
        assert!(fixture.executor.requests.lock().unwrap().is_empty());
    }
    fixture.close().await;
}

#[tokio::test]
async fn legacy_restart_metadata_keeps_fifo_and_replies_after_successful_admission() {
    queued_metadata(false).await;
}
#[tokio::test]
async fn legacy_restart_metadata_keeps_fifo_and_replies_after_refused_admission() {
    queued_metadata(true).await;
}
