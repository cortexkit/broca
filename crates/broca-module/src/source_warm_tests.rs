use super::*;
use broca_core::{ExportIndex, FileWalStore, Provider};
use broca_protocol::{CallOptions, ModelHandle, RunId, StepId, Usage};
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};
use broca_wal::record::{AttemptEnvelope, AttemptSent, BillingAttemptOutcome};
use std::{future::Future, time::Duration};

const SOURCE: &str = "claude-opus-5-5";
const FALLBACK: &str = "claude-opus-4-8";

// Keep the test runnable while the WAL thread works. A paused runtime must not auto-advance
// unrelated timers just because a filesystem operation is waiting on another OS thread.
pub(crate) async fn drive<F: Future>(future: F) -> F::Output {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            result = &mut future => return result,
            _ = tokio::task::yield_now() => assert!(std::time::Instant::now() < deadline, "source warmer stalled"),
        }
    }
}

struct Executor {
    provider: TestProvider,
    costs: broca_core::keep_warm::WarmCosts,
    quota: AtomicBool,
}

struct TestProvider {
    frozen: FrozenRenderConfig,
    fallback: AtomicBool,
    bodies: Mutex<Vec<Vec<u8>>>,
    gate: Mutex<Option<Arc<tokio::sync::Notify>>>,
}

#[async_trait::async_trait]
impl Provider for TestProvider {
    async fn step(
        &self,
        _: StepId,
        _: &CallOptions,
    ) -> tokio::sync::mpsc::Receiver<broca_protocol::NormalizedEvent> {
        unreachable!()
    }

    fn set_step_route(&self, route: broca_core::refusal_fallback::StepRoute) {
        self.fallback.store(
            route == broca_core::refusal_fallback::StepRoute::Fallback,
            Ordering::SeqCst,
        );
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        let mut frozen = self.frozen.clone();
        if self.fallback.load(Ordering::SeqCst) {
            frozen.wire_model_id = FALLBACK.into();
        }
        AnthropicMessages.render(&frozen, opts)
    }

    async fn warm(&self, base: &[u8]) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
        self.warm_with_hook(base, None).await
    }

    async fn warm_with_hook(
        &self,
        base: &[u8],
        hook: Option<Arc<dyn broca_core::run::AttemptHook>>,
    ) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
        let body = broca_provider::cache_warm::anthropic_warm_body(base).unwrap();
        let model = serde_json::from_slice::<serde_json::Value>(&body).unwrap()["model"]
            .as_str()
            .unwrap()
            .to_owned();
        let id = if let Some(hook) = &hook {
            Some(
                hook.before_send(
                    broca_core::run::PhysicalSend {
                        attempt: 0,
                        send_index: 1,
                        endpoint_host: "api.anthropic.com".into(),
                        model_sent: model,
                        request_body_bytes: body.len() as u64,
                        compressed: false,
                        account_ref: None,
                        speed_requested: None,
                        auth_refreshed: false,
                        warm: Some(broca_core::cache_warm::WarmSendPolicy {
                            provider: None,
                            ttl_ms: broca_provider::cache_warm::marker_ttl_ms(&self.frozen),
                            auth_refreshed: false,
                        }),
                    },
                    Default::default(),
                )
                .await
                .map_err(broca_core::WarmFailure::Provider)?,
            )
        } else {
            None
        };
        self.bodies.lock().unwrap().push(body.clone());
        let gate = self.gate.lock().unwrap().clone();
        if let Some(gate) = gate {
            gate.notified().await;
        }
        let sent = broca_core::WarmSent {
            warm_body_sha256: broca_core::render::request_digest_hex(&body),
            sent_at_ms: epoch_ms(),
            usage: Usage {
                cached_input_tokens: Some(1000),
                ..Default::default()
            },
            served_account_ref: None,
            ratelimit_headers: vec![],
            http_status: 200,
        };
        if let Some(hook) = hook {
            hook.warm_finished(
                id.unwrap(),
                AttemptEnvelope {
                    sent: AttemptSent::Yes,
                    outcome: BillingAttemptOutcome::Complete,
                    sent_at_ms: Some(sent.sent_at_ms),
                    usage: Some(sent.usage),
                    http_status: Some(200),
                    ..Default::default()
                },
                Some(sent.clone()),
                sent.warm_body_sha256.clone(),
            )
            .await
            .map_err(broca_core::WarmFailure::Provider)?;
        }
        Ok(sent)
    }
}

#[async_trait::async_trait]
impl RunExecutor for Executor {
    async fn execute(
        &self,
        _: crate::executor::RunJob,
        _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        unreachable!()
    }
    fn warm_costs(
        &self,
        _: &RunConfig,
        _: u64,
        _: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        Ok(self.costs)
    }
    async fn warm_quota(
        &self,
        _: &RunConfig,
        _: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        if self.quota.load(Ordering::SeqCst) {
            Ok(())
        } else {
            Err("quota".into())
        }
    }
    async fn warm(
        &self,
        job: crate::executor::WarmJob,
    ) -> Result<broca_core::WarmSent, crate::executor::WarmError> {
        let result =
            crate::executor::send_warm_with_hook(&self.provider, job.base, job.attempt_hook).await;
        assert!(result.is_ok(), "source warm failed: {result:?}");
        result
    }
}

struct Fixture {
    root: std::path::PathBuf,
    key: BindIdentity,
    writer: Arc<WalHandle>,
    store: Arc<FileWalStore>,
    executor: Arc<Executor>,
    records: Vec<WalRecord>,
    events: Events,
    task: Option<Task>,
    export: Arc<broca_store::SqliteStore>,
    ttl: u64,
}

impl Fixture {
    async fn new(head: bool, overhaul: bool, ttl: u64, bust: f64) -> Self {
        let root = broca_test_support::scratch_root().join(format!(
            "source-warm-{}",
            broca_core::attempt_id::mint_attempt_id().unwrap().as_str()
        ));
        let key = BindIdentity::new(root.join("project"), "runner", "source");
        let store = Arc::new(FileWalStore::with_config(
            root.join("wal"),
            if overhaul {
                broca_wal::WalConfig::OVERHAUL
            } else {
                broca_wal::WalConfig::default()
            },
        ));
        let tenure = broca_core::lease::WalLeaseDomain::new(root.join("leases"))
            .acquire(&key)
            .unwrap();
        let writer = Arc::new(
            WalHandle::open_and_spawn_owned(store.clone(), key.clone(), tenure, None)
                .await
                .unwrap(),
        );
        let mut frozen = anthropic_frozen_config(SOURCE);
        frozen.variant = Some(broca_provider::variant::FrozenVariant {
            name: "high".into(),
            request: broca_provider::variant::VariantRequest::Anthropic {
                thinking: Some(serde_json::json!({"type":"adaptive"})),
                effort: Some("high".into()),
            },
        });
        frozen.cache = Some(broca_provider::wire::CachePolicy {
            version: 1,
            class: broca_provider::wire::CacheClass::Markers {
                syntax: broca_provider::wire::MarkerSyntax::AnthropicCacheControl,
                ttl: if ttl == 300_000 {
                    broca_provider::wire::CacheTtl::FiveMinutes
                } else {
                    broca_provider::wire::CacheTtl::OneHour
                },
                max_breakpoints: 4,
                lookback_blocks: 20,
                tail: None,
                omit_ttl: false,
            },
        });
        let executor = Arc::new(Executor {
            provider: TestProvider {
                frozen: frozen.clone(),
                fallback: AtomicBool::new(true),
                bodies: Default::default(),
                gate: Default::default(),
            },
            costs: broca_core::keep_warm::WarmCosts { warm: 1.0, bust },
            quota: AtomicBool::new(true),
        });
        let mut started = crate::move_install::tests::started(&key, "r");
        if let WalRecord::RunStarted {
            config,
            input,
            session_class,
            ts_ms,
            ..
        } = &mut started
        {
            config.model = ModelHandle::new("anthropic", SOURCE);
            config.frozen_render_config = serde_json::to_value(&frozen).unwrap();
            *input = Arc::new(prompt());
            *session_class = Some(if head { "head" } else { "worker" }.into());
            *ts_ms = Some(epoch_ms());
        }
        let mut records = vec![started, WalRecord::StepStarted { step_id: StepId(1) }];
        let mut opts = CallOptions::new(prompt());
        opts.target_model = Some(ModelHandle::new("anthropic", SOURCE));
        let digest =
            broca_core::render::request_digest_hex(&AnthropicMessages.render(&frozen, &opts));
        records.push(
            serde_json::from_value(serde_json::json!({
                "type":"model_attempt_finished", "step_id":1, "attempt":0,
                "request_body_len":1, "request_body_sha256":digest, "http_status":200,
                "response_body_len":1, "response_body_sha256":"refused", "outcome":"complete",
                "ts_ms":epoch_ms(), "sent_at_ms":epoch_ms()
            }))
            .unwrap(),
        );
        records.push(WalRecord::ModelStepRefused {
            step_id: StepId(1),
            usage: Usage {
                cached_input_tokens: Some(1000),
                ..Default::default()
            },
            served_model: None,
            usage_iterations: None,
            discarded_blocks: 1,
            schedule: broca_core::refusal_fallback::on_source_refusal(
                None,
                &ModelHandle::new("anthropic", SOURCE),
                &ModelHandle::new("anthropic", FALLBACK),
                Some(epoch_ms()),
            ),
            ts_ms: Some(epoch_ms()),
        });
        for record in &records {
            writer.record(record.clone());
        }
        drive(writer.sync()).await.unwrap();
        Self {
            root,
            key,
            writer,
            store,
            executor,
            records,
            events: Events::default(),
            task: None,
            export: Arc::new(broca_store::SqliteStore::open_in_memory().unwrap()),
            ttl,
        }
    }

    async fn start(&mut self) {
        let end = self
            .records
            .iter()
            .position(|r| matches!(r, WalRecord::ModelStepRefused { .. }))
            .unwrap();
        let rebuilt = broca_core::cache_warm::last_recorded_request(&self.records[..end])
            .expect("rebuild the source request");
        assert_eq!(
            rebuilt.request_body_sha256,
            broca_core::render::request_digest_hex(
                &AnthropicMessages.render(&self.executor.provider.frozen, &rebuilt.opts)
            ),
            "source fixture records the actual source request: rebuilt={}",
            String::from_utf8_lossy(
                &AnthropicMessages.render(&self.executor.provider.frozen, &rebuilt.opts)
            )
        );
        let events = self.events.subscribe();
        let head = self.writer.durable_head().load(Ordering::Acquire);
        let ttl = self.ttl;
        let task = spawn(
            Runtime {
                session: self.key.clone(),
                writer: self.writer.clone(),
                store: self.store.clone(),
                executor: self.executor.clone(),
                declare: Arc::new(move |_| {
                    Some(crate::CacheDeclaration {
                        ttl_ms: Some(ttl),
                        ttl_options_ms: vec![],
                        read_refreshes_ttl: Some(true),
                        warm: Ok(()),
                        basis: None,
                    })
                }),
                slot: WarmSlot::default(),
                stopping: CancellationToken::new(),
                export: Some((
                    self.export.clone(),
                    Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 1)),
                )),
            },
            events,
            self.records.clone(),
            head,
        );
        // The task's initial reconstruction is synchronous up to the first select.
        tokio::task::yield_now().await;
        self.task = Some(task);
    }

    async fn record(&mut self, record: WalRecord) {
        self.writer.record(record.clone());
        drive(self.writer.sync()).await.unwrap();
        self.records.push(record.clone());
        self.events
            .observe(self.writer.durable_head().load(Ordering::Acquire), &record);
        tokio::task::yield_now().await;
    }

    async fn warm(&self, count: usize) {
        tokio::time::advance(Duration::from_millis(self.ttl - lead_ms(self.ttl))).await;
        self.await_warm(count).await;
    }

    async fn await_warm(&self, count: usize) {
        drive(async {
            loop {
                if self
                    .task
                    .as_ref()
                    .unwrap()
                    .completed_warms
                    .load(Ordering::Acquire)
                    >= count as u64
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(self.warms().len(), count);
    }

    fn warms(&self) -> Vec<WalRecord> {
        self.store
            .read_records(&self.key)
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .filter(|r| matches!(r, WalRecord::CacheWarmed { .. }))
            .collect()
    }

    async fn stop(&mut self) {
        if let Some(task) = self.task.take() {
            drive(task.stop()).await;
        }
    }

    async fn restart(mut self) -> Self {
        self.stop().await;
        let Self {
            root,
            key,
            writer,
            store,
            executor,
            events,
            export,
            ttl,
            ..
        } = self;
        drive(
            Arc::try_unwrap(writer)
                .ok()
                .expect("scheduler released writer")
                .shutdown(),
        )
        .await
        .unwrap();
        let tenure = broca_core::lease::WalLeaseDomain::new(root.join("leases"))
            .acquire(&key)
            .unwrap();
        let writer = Arc::new(
            drive(WalHandle::open_and_spawn_owned(
                store.clone(),
                key.clone(),
                tenure,
                None,
            ))
            .await
            .unwrap(),
        );
        let records = store
            .read_records(&key)
            .unwrap()
            .into_iter()
            .map(|f| f.record)
            .collect();
        Self {
            root,
            key,
            writer,
            store,
            executor,
            events,
            export,
            ttl,
            records,
            task: None,
        }
    }

    async fn fallback_answer(&mut self) {
        let mut fallback = self.executor.provider.frozen.clone();
        fallback.wire_model_id = FALLBACK.into();
        let digest = broca_core::render::request_digest_hex(
            &AnthropicMessages.render(&fallback, &CallOptions::new(prompt())),
        );
        let mut attempt = self.records[2].clone();
        if let WalRecord::ModelAttemptFinished {
            request_body_sha256,
            attempt,
            ..
        } = &mut attempt
        {
            *request_body_sha256 = digest;
            *attempt = 1;
        }
        self.record(attempt).await;
        let schedule = self
            .records
            .iter()
            .find_map(|record| match record {
                WalRecord::ModelStepRefused { schedule, .. } => Some(schedule),
                _ => None,
            })
            .unwrap();
        let schedule = broca_core::refusal_fallback::on_step_committed(
            Some(schedule),
            broca_core::refusal_fallback::CommittedStep {
                route: broca_core::refusal_fallback::StepRoute::Fallback,
                refused: false,
                served_by_server_side_fallback: false,
            },
            Some(epoch_ms()),
        );
        self.record(serde_json::from_value(serde_json::json!({
            "type":"model_step_finished", "step_id":1,
            "assistant_message":{"message_id":"fallback-answer","content":[{"type":"text","text":"ok"}]},
            "usage":{}, "route":{"model":{"provider_module_id":"anthropic","model_id":FALLBACK},"reason":"refusal"},
            "refusal_fallback":schedule
        })).unwrap()).await;
    }
}

fn prompt() -> Vec<broca_protocol::Message> {
    vec![
        broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text {
                text: "hello".into(),
            }],
        ),
        broca_protocol::Message::assistant_from(
            vec![
                broca_protocol::ContentBlock::Reasoning {
                    text: "reasoning to preserve".into(),
                    signature: Some("source-signature".into()),
                },
                broca_protocol::ContentBlock::Text {
                    text: "earlier answer".into(),
                },
            ],
            ModelHandle::new("anthropic", SOURCE),
        ),
    ]
}

fn finished() -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        reason: broca_protocol::RunFinishReason::Completed,
        ts_ms: Some(epoch_ms()),
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    }
}

#[tokio::test(start_paused = true)]
async fn source_round_warms_once_per_ttl_on_the_source_bytes_then_compaction_stops_it() {
    let mut f = Fixture::new(false, false, 300_000, 1000.0).await;
    f.fallback_answer().await;
    f.start().await;
    tokio::time::advance(Duration::from_millis(269_000)).await;
    assert!(f.warms().is_empty());
    tokio::time::advance(Duration::from_millis(1_000)).await;
    f.await_warm(1).await;
    f.warm(2).await;
    let base = broca_core::cache_warm::last_recorded_request(&f.records[..3]).unwrap();
    let expected =
        String::from_utf8(AnthropicMessages.render(&f.executor.provider.frozen, &base.opts))
            .unwrap();
    let value: serde_json::Value = serde_json::from_str(&expected).unwrap();
    let expected = expected
        .replacen(
            &format!("\"max_tokens\":{},", value["max_tokens"]),
            "\"max_tokens\":0,",
            1,
        )
        .replacen("\"stream\":true,", "", 1)
        .into_bytes();
    assert_eq!(
        *f.executor.provider.bodies.lock().unwrap(),
        vec![expected.clone(), expected]
    );
    assert!(
        String::from_utf8_lossy(&f.executor.provider.bodies.lock().unwrap()[0])
            .contains("source-signature")
    );
    assert!(
        String::from_utf8_lossy(&f.executor.provider.bodies.lock().unwrap()[0])
            .contains("cache_control")
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&f.executor.provider.bodies.lock().unwrap()[0])
            .unwrap()["model"],
        SOURCE
    );
    let compaction = serde_json::from_value(
        serde_json::json!({"type":"compaction_view_applied","provider":"compact","view_version":1,"view":"{}","choice":{"kind":"applied"}}),
    );
    // Use a real applied-view record's shape rather than invoking a compaction provider.
    f.record(compaction.unwrap()).await;
    tokio::time::advance(Duration::from_secs(1000)).await;
    assert_eq!(f.warms().len(), 2);
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_round_worker_stops_at_run_end_and_a_short_round_sends_nothing() {
    let mut f = Fixture::new(false, false, 300_000, 1000.0).await;
    f.start().await;
    f.warm(1).await;
    f.record(finished()).await;
    tokio::time::advance(Duration::from_secs(1000)).await;
    assert_eq!(f.warms().len(), 1);
    f.stop().await;
    let mut short = Fixture::new(true, false, 300_000, 1000.0).await;
    short.start().await;
    let source_answer: WalRecord = serde_json::from_value(serde_json::json!({"type":"model_step_finished","step_id":2,"assistant_message":{"message_id":"source-answer","content":[]},"usage":{}})).unwrap();
    short.record(source_answer).await;
    tokio::time::advance(Duration::from_secs(1000)).await;
    assert!(short.warms().is_empty());
    short.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_head_idle_warming_is_capped_by_catalog_costs() {
    let mut f = Fixture::new(true, false, 3_600_000, 4.0).await;
    f.record(finished()).await;
    f.start().await;
    for count in 1..=3 {
        f.warm(count).await;
    }
    tokio::time::advance(Duration::from_secs(7200)).await;
    drive(async {
        while f.task.as_ref().unwrap().holds_idle.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(f.warms().len(), 3);
    assert!(!f.task.as_ref().unwrap().holds_idle.load(Ordering::Acquire));
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_overhaul_warm_reservation_names_source_model() {
    let mut f = Fixture::new(false, true, 300_000, 1000.0).await;
    f.start().await;
    f.warm(1).await;
    let warms = f.warms();
    let WalRecord::CacheWarmed {
        reservation: Some(reservation),
        trigger,
        ..
    } = &warms[0]
    else {
        panic!("missing billing reservation");
    };
    assert_eq!(reservation.model_sent, SOURCE);
    assert_eq!(trigger.as_deref(), Some("source_keep_warm"));
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_legacy_warm_and_export_follow_the_refused_digest_not_the_fallback_step() {
    let mut f = Fixture::new(true, false, 300_000, 1000.0).await;
    f.fallback_answer().await;
    f.record(finished()).await;
    f.start().await;
    f.warm(1).await;
    let warms = f.warms();
    let WalRecord::CacheWarmed {
        request_body_sha256,
        reservation,
        trigger,
        ..
    } = &warms[0]
    else {
        unreachable!()
    };
    let WalRecord::ModelAttemptFinished {
        request_body_sha256: refused_digest,
        ..
    } = &f.records[2]
    else {
        unreachable!()
    };
    assert_eq!(request_body_sha256, refused_digest);
    assert!(reservation.is_none());
    assert_eq!(trigger.as_deref(), Some("source_keep_warm"));
    let facts = drive(async {
        loop {
            let facts = f.export.read_page(None, 100).unwrap().facts;
            if facts.iter().any(|f| f.segment.as_session().unwrap().warm) {
                break facts;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let warm = facts
        .iter()
        .find(|f| f.segment.as_session().unwrap().warm)
        .unwrap()
        .segment
        .as_session()
        .unwrap();
    assert_eq!(warm.model, SOURCE);
    assert!(facts
        .iter()
        .any(|f| f.segment.as_session().unwrap().model == FALLBACK));
    let sample = serde_json::json!({"cache_warmed_record": &warms[0], "export_fact": warm});
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.cortexkit/internal-docs/investigations/source-warm-serializer-sample.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, serde_json::to_vec_pretty(&sample).unwrap()).unwrap();
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_converted_prefix_is_refused_before_any_warm_send() {
    let mut f = Fixture::new(false, false, 300_000, 1000.0).await;
    f.start().await;
    f.warm(1).await;
    let mut converted = broca_core::cache_warm::last_recorded_request(&f.records[..3]).unwrap();
    converted.opts.prompt[1].content.clear();
    converted.opts.target_model = Some(ModelHandle::new("anthropic", FALLBACK));
    let result = crate::executor::send_warm(
        &f.executor.provider,
        crate::executor::WarmBase::Recorded(Box::new(converted)),
    )
    .await;
    assert!(matches!(
        result,
        Err(crate::executor::WarmError::PrefixUnavailable(_))
    ));
    assert_eq!(f.executor.provider.bodies.lock().unwrap().len(), 1);
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_schedule_rebuilds_from_log_after_shutdown_and_restart_pause() {
    let mut f = Fixture::new(true, false, 300_000, 1000.0).await;
    f.record(finished()).await;
    f.start().await;
    f.warm(1).await;
    let mut f = f.restart().await;
    f.start().await;
    f.warm(2).await;
    f.record(WalRecord::RunPaused {
        run_id: RunId("r".into()),
        reason: broca_protocol::PauseReason::Restart,
        error: None,
        ts_ms: Some(epoch_ms()),
    })
    .await;
    tokio::time::advance(Duration::from_secs(300)).await;
    assert_eq!(f.warms().len(), 2);
    let mut f = f.restart().await;
    f.start().await;
    f.record(WalRecord::ResumeStarted {
        episode: RunId("r".into()),
        resumed_from_seq: 0,
        replayed: Default::default(),
        redone: vec![],
        indeterminate: vec![],
    })
    .await;
    f.warm(3).await;
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_head_run_boundary_does_not_cancel_and_duplicate_an_in_flight_warm() {
    let mut f = Fixture::new(true, false, 300_000, 1000.0).await;
    let gate = Arc::new(tokio::sync::Notify::new());
    *f.executor.provider.gate.lock().unwrap() = Some(gate.clone());
    f.start().await;
    tokio::time::advance(Duration::from_secs(270)).await;
    drive(async {
        while f.executor.provider.bodies.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let mut next_run = f.records[0].clone();
    if let WalRecord::RunStarted { run_id, .. } = &mut next_run {
        *run_id = RunId("next-head-turn".into());
    }
    f.record(next_run).await;
    gate.notify_one();
    f.await_warm(1).await;
    assert_eq!(f.executor.provider.bodies.lock().unwrap().len(), 1);
    assert_eq!(f.warms().len(), 1);
    f.stop().await;
}

impl Fixture {
    async fn finish_fallback_round(&mut self) {
        use broca_core::refusal_fallback::{on_step_committed, CommittedStep, StepRoute};
        let mut schedule = self
            .records
            .iter()
            .find_map(|record| match record {
                WalRecord::ModelStepRefused { schedule, .. } => Some(schedule.clone()),
                _ => None,
            })
            .unwrap();
        for reply in 1..=10 {
            if reply > 1 {
                let mut started = self.records[0].clone();
                if let WalRecord::RunStarted { run_id, input, .. } = &mut started {
                    *run_id = RunId(format!("fallback-round-{reply}"));
                    *input = Arc::new(vec![broca_protocol::Message::new(
                        broca_protocol::Role::User,
                        vec![broca_protocol::ContentBlock::Text {
                            text: format!("reply {reply}"),
                        }],
                    )]);
                }
                self.record(started).await;
                self.record(WalRecord::StepStarted { step_id: StepId(1) })
                    .await;
            }
            let mut fallback = self.executor.provider.frozen.clone();
            fallback.wire_model_id = FALLBACK.into();
            let mut opts = CallOptions::new(broca_core::replay(&self.records).unwrap().prompt);
            opts.target_model = Some(ModelHandle::new("anthropic", FALLBACK));
            let digest =
                broca_core::render::request_digest_hex(&AnthropicMessages.render(&fallback, &opts));
            let mut attempt = self.records[2].clone();
            if let WalRecord::ModelAttemptFinished {
                request_body_sha256,
                attempt,
                ..
            } = &mut attempt
            {
                *request_body_sha256 = digest;
                *attempt = u32::from(reply == 1);
            }
            self.record(attempt).await;
            schedule = on_step_committed(
                Some(&schedule),
                CommittedStep {
                    route: StepRoute::Fallback,
                    refused: false,
                    served_by_server_side_fallback: false,
                },
                Some(epoch_ms()),
            )
            .unwrap();
            self.record(serde_json::from_value(serde_json::json!({
                "type":"model_step_finished", "step_id":1,
                "assistant_message":{"message_id":format!("answer-{reply}"),"content":[{"type":"text","text":"fallback reply"}]},
                "usage":{}, "route":{"model":{"provider_module_id":"anthropic","model_id":FALLBACK},"reason":"schedule"},
                "refusal_fallback":schedule
            })).unwrap()).await;
            self.record(finished()).await;
        }
        assert!(matches!(
            schedule.phase,
            broca_wal::RefusalFallbackPhase::Probe { clean: 0 }
        ));
    }

    async fn stopped_or_warmed(&self, previous_warms: usize) {
        drive(async {
            while self
                .task
                .as_ref()
                .unwrap()
                .holds_idle
                .load(Ordering::Acquire)
                && self
                    .task
                    .as_ref()
                    .unwrap()
                    .completed_warms
                    .load(Ordering::Acquire)
                    <= previous_warms as u64
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }

    async fn pause_resume(&mut self) {
        self.record(WalRecord::RunPaused {
            run_id: RunId("r".into()),
            reason: broca_protocol::PauseReason::Restart,
            error: None,
            ts_ms: Some(epoch_ms()),
        })
        .await;
        self.record(WalRecord::ResumeStarted {
            episode: RunId("r".into()),
            resumed_from_seq: 0,
            replayed: Default::default(),
            redone: vec![],
            indeterminate: vec![],
        })
        .await;
        drive(async {
            while !self
                .task
                .as_ref()
                .unwrap()
                .holds_idle
                .load(Ordering::Acquire)
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
    }
}

#[tokio::test(start_paused = true)]
async fn source_review_final_fallback_reply_stops_a_round_before_first_expiry() {
    let mut f = Fixture::new(true, false, 300_000, 1000.0).await;
    f.start().await;
    f.finish_fallback_round().await;
    tokio::time::advance(Duration::from_secs(270)).await;
    f.stopped_or_warmed(0).await;
    assert!(
        f.warms().is_empty(),
        "the committed tenth fallback reply ended the round"
    );
    assert!(!f.task.as_ref().unwrap().holds_idle.load(Ordering::Acquire));
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_review_probe_phase_on_restart_does_not_rebuild_a_warm_candidate() {
    let mut f = Fixture::new(true, false, 300_000, 1000.0).await;
    f.start().await;
    f.warm(1).await;
    f.finish_fallback_round().await;
    let mut f = f.restart().await;
    f.start().await;
    tokio::time::advance(Duration::from_secs(270)).await;
    f.stopped_or_warmed(1).await;
    assert_eq!(
        f.warms().len(),
        1,
        "a durable Probe phase is not an active fallback round"
    );
    f.stop().await;
}

async fn source_review_resume_budget_case(restart: bool) {
    let mut f = Fixture::new(true, false, 300_000, 3.0).await;
    f.start().await;
    f.warm(1).await;
    f.pause_resume().await;
    f.warm(2).await;
    f.pause_resume().await;
    if restart {
        f = f.restart().await;
        f.start().await;
    }
    tokio::time::advance(Duration::from_secs(270)).await;
    f.stopped_or_warmed(2).await;
    assert_eq!(
        f.warms().len(),
        2,
        "resuming one episode cannot buy a fresh rewrite allowance"
    );
    assert!(!f.task.as_ref().unwrap().holds_idle.load(Ordering::Acquire));
    f.stop().await;
}

#[tokio::test(start_paused = true)]
async fn source_review_repeated_resumes_do_not_replenish_live_rent_budget() {
    source_review_resume_budget_case(false).await;
}

#[tokio::test(start_paused = true)]
async fn source_review_repeated_resumes_do_not_replenish_replayed_rent_budget() {
    source_review_resume_budget_case(true).await;
}

#[tokio::test(start_paused = true)]
async fn source_review_direct_compaction_message_permanently_stops_the_prefix() {
    let mut f = Fixture::new(true, false, 300_000, 1000.0).await;
    f.start().await;
    f.warm(1).await;
    let replacement = Arc::new(vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "replacement history".into(),
        }],
    )]);
    let record = serde_json::from_value(serde_json::json!({
        "type":"compaction_message", "provider":"compact", "kind":"step", "compaction_id":"history-replacement", "version":1,
        "range":{"from":0,"to":2}, "replacement":replacement,
        "sha256":broca_wal::record::compaction_replacement_sha256(&replacement).unwrap()
    })).unwrap();
    f.record(record).await;
    tokio::time::advance(Duration::from_secs(270)).await;
    f.stopped_or_warmed(1).await;
    assert_eq!(f.warms().len(), 1);
    let mut f = f.restart().await;
    f.start().await;
    tokio::time::advance(Duration::from_secs(270)).await;
    f.stopped_or_warmed(1).await;
    assert_eq!(f.warms().len(), 1);
    f.stop().await;
}
