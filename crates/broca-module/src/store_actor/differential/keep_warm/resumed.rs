//! A restarted driver's first tool wait, with no transport sends in the new process: the store
//! actor rebuilds the warm's base from the durable history.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::keep_warm_wait::KeepWarmDriver as _;
use broca_core::keep_warm_wait::WarmTick;
use broca_core::{OutputEvent, RunError, RunSummary};
use broca_protocol::RunId;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wal::{FramedRecord, WalRecord};
use serde_json::json;
use tokio::sync::{mpsc, Notify};

use super::super::warm::{warmable, Probe};
use crate::executor::{RunExecutor, RunJob, StoreRunMode, WarmError, WarmJob};
use crate::store_actor::tests::bind;
use crate::store_actor::StoreActor;

cipher_tests!(
    first_resumed_wait_warms_the_recorded_base,
    resumed_warm_waits_for_the_media_fold,
    mismatched_recorded_digest_never_warms,
    cold_cache_predecessor_rebuilds_the_previous_run_lazily
);

mod idle;

#[derive(Default)]
struct Warming {
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
    warm_configs: Mutex<Vec<broca_wal::RunConfig>>,
    quota_configs: Mutex<Vec<broca_wal::RunConfig>>,
    clock: Option<Arc<std::sync::atomic::AtomicU64>>,
    no_cache: bool,
    fail: bool,
    store: Option<crate::store_actor::tests::LoopExecutor>,
}

async fn cold_cache_predecessor_rebuilds_the_previous_run_lazily(cipher: Cipher) {
    let side = super::side(false, cipher).await;
    let mut records = history(true);
    let expected =
        broca_core::cache_warm::last_recorded_request_with_lineage(&records, None).unwrap();
    records.push(record(
        json!({"type":"run_finished", "reason":"completed", "usage":{}}),
    ));
    let mut next = records[0].clone();
    if let WalRecord::RunStarted { run_id, .. } = &mut next {
        *run_id = RunId("r2".into());
    }
    records.push(next);
    let actor = reopened(&side, &records, cipher, Arc::new(Warming::default())).await;
    let (writer, mut link) = broca_session_loop::shared::ActorWriter::new(&actor.session, None);
    let loader = writer.cache_predecessor_loader();
    assert!(
        futures::poll!(Box::pin(link.next())).is_pending(),
        "installing a loader sends no read"
    );
    let loaded = loader.load();
    tokio::pin!(loaded);
    assert!(futures::poll!(&mut loaded).is_pending());
    let broca_session_loop::shared::Request::CachePredecessor { reply } =
        link.next().await.unwrap()
    else {
        panic!("the comparison asks its owner for a predecessor");
    };
    let _ = reply.send(actor.cache_predecessor().await);
    let recovered = loaded.await.unwrap().unwrap();
    assert_eq!(
        recovered.request.run_id.0, "r1",
        "the new run has no durable attempt yet"
    );
    assert_eq!(recovered.request.step_id.0, 2);
    assert_eq!(
        recovered.request.request_body_sha256,
        expected.request_body_sha256
    );
    let live = broca_provider::LiveProvider::new(
        broca_provider::families::AnthropicMessages,
        serde_json::from_value(recovered.request.config.frozen_render_config).unwrap(),
        broca_provider::wire::AuthHandle::None,
    );
    let bytes = broca_core::provider::Provider::render_request(&live, &recovered.request.opts);
    assert_eq!(
        broca_core::render::request_digest_hex(&bytes),
        expected.request_body_sha256
    );
    assert!(recovered.locator.ends_with(&format!(
        ":{}",
        actor
            .session
            .ledger_tail_until(|r| matches!(r, Record::Attempt(_)))
            .await
            .unwrap()
            .last()
            .unwrap()
            .0
    )));
}

#[async_trait]
impl RunExecutor for Warming {
    async fn execute_on_store(
        &self,
        job: crate::executor::StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> crate::executor::StoreRunEnd {
        self.store
            .as_ref()
            .expect("a real-run test installs a loop")
            .execute_on_store(job, out)
            .await
    }
    fn warm_costs(
        &self,
        _: &broca_wal::RunConfig,
        _: u64,
        _: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        Ok(broca_core::keep_warm::WarmCosts {
            warm: 0.4,
            bust: 1.0,
        })
    }
    async fn execute(
        &self,
        _: RunJob,
        _: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("the resumed tool wait calls no model")
    }

    async fn warm(&self, job: WarmJob) -> Result<broca_core::WarmSent, WarmError> {
        self.warm_configs.lock().unwrap().push(job.config.clone());
        if self.fail {
            return Err(WarmError::Unavailable("test refusal".into()));
        }
        let frozen = serde_json::from_value(job.config.frozen_render_config.clone()).unwrap();
        let probe = Probe {
            live: broca_provider::LiveProvider::new(
                broca_provider::families::AnthropicMessages,
                frozen,
                broca_provider::wire::AuthHandle::Bearer("t".into()),
            ),
            sent: Arc::clone(&self.sent),
        };
        let mut sent = crate::executor::send_warm(&probe, job.base).await?;
        if let Some(clock) = &self.clock {
            sent.sent_at_ms = clock.load(std::sync::atomic::Ordering::SeqCst);
        }
        if self.no_cache {
            sent.usage.cached_input_tokens = Some(0);
        }
        Ok(sent)
    }

    async fn warm_quota(
        &self,
        config: &broca_wal::RunConfig,
        _: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        self.quota_configs.lock().unwrap().push(config.clone());
        Ok(())
    }
}

fn record(value: serde_json::Value) -> WalRecord {
    serde_json::from_value(value).unwrap()
}

/// Two recorded tool steps distinguish the last request from the run's first request. The last
/// turn can still be held, or already placed with replies while its post-tool wait continues.
fn history(closed: bool) -> Vec<WalRecord> {
    let frozen = warmable();
    let mut records = vec![record(json!({
        "type": "run_started", "run_id": "r1", "ts_ms": crate::actor::epoch_ms(),
        "session": bind(), "keep_warm": "auto",
        "config": {
            "model": {"provider_module_id":"anthropic", "model_id":"claude-opus-4-8"},
            "tools": [{"name":"read", "description":"Read", "input_schema":{"type":"object"}, "execution_mode":"parallel"}],
            "tool_choice":{"type":"auto"}, "generation":{}, "response_format":{"type":"text"},
            "context_epoch":0, "renderer_version":frozen.renderer_version,
            "frozen_render_config": frozen,
        },
        "input":[{"role":"user", "content":[{"type":"text", "text":"hello"}]}],
        "origin":{"kind":"fresh"},
    }))];
    for step in 1..=2 {
        records.push(record(json!({"type":"step_started", "step_id":step})));
        if step == 2 {
            records.push(record(
                json!({"type":"output_reservation_clamped", "step_id":step,
                "max_output_tokens":256, "estimated_input_tokens":100, "context_limit":4096}),
            ));
        }
        let config = match &records[0] {
            WalRecord::RunStarted { config, .. } => config.clone(),
            _ => unreachable!(),
        };
        let (_, _, bytes) = crate::wal_render::render_step_with_lineage(
            None,
            &records,
            &config,
            "r1",
            step,
            (step == 2).then_some(256),
            None,
        )
        .unwrap();
        records.push(record(json!({"type":"model_attempt_finished", "step_id":step, "attempt":0,
            "outcome":"complete", "http_status":200, "ts_ms":crate::actor::epoch_ms(),
            "sent_at_ms":crate::actor::epoch_ms(), "request_body_len":bytes.len(),
            "request_body_sha256":broca_core::render::request_digest_hex(&bytes),
            "response_body_len":0, "response_body_sha256":broca_core::render::request_digest_hex(b""),
        })));
        records.push(record(json!({"type":"model_step_finished", "step_id":step,
            "assistant_message":{"message_id":format!("m{step}"), "content":[{
                "type":"tool_call", "tool_call_id":format!("c{step}"), "tool_name":"read", "input":{},
            }]}, "usage":{"input_tokens":100, "output_tokens":20},
            "finish_reason":"tool_calls", "retries_used":0,
        })));
        records.push(record(
            json!({"type":"tool_batch_started", "batch_id":format!("b{step}"),
            "step_id":step, "planned_calls":[format!("c{step}")]}),
        ));
        if step == 1 || closed {
            records.push(record(json!({"type":"tool_result", "batch_id":format!("b{step}"),
                "result":{"tool_call_id":format!("c{step}"), "output":{"kind":"text", "text":"answer"}, "is_error":false},
                "outcome":"unknown"})));
            records.push(record(
                json!({"type":"tool_batch_finished", "batch_id":format!("b{step}")}),
            ));
            records.push(record(json!({"type":"turn_finished", "step_id":step})));
        }
    }
    records
}

async fn reopened(
    side: &super::Side,
    records: &[WalRecord],
    cipher: Cipher,
    executor: Arc<Warming>,
) -> StoreActor<LocalBackend> {
    let frames: Vec<_> = records
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, record)| FramedRecord {
            seq: i as u64 + 1,
            fence: 0,
            record,
        })
        .collect();
    let converted = broca_session_convert::convert(&frames, None).unwrap();
    assert!(converted.lost.is_empty(), "{:?}", converted.lost);
    reopened_converted(side, converted, cipher, executor).await
}

async fn reopened_converted(
    side: &super::Side,
    converted: broca_session_convert::Converted,
    cipher: Cipher,
    executor: Arc<Warming>,
) -> StoreActor<LocalBackend> {
    let root = side.dir.join("resumed-store");
    let addr = SessionAddr(broca_core::session_addr(&bind()));
    let triple = SessionTriple {
        project_root: "/project".into(),
        harness: "store-actor".into(),
        session: "s1".into(),
    };
    let deployment = DeploymentKey::new(1, &[7; 32]).creating(cipher);
    let mut session = Session::create(
        LocalBackend::new(&root),
        addr.clone(),
        triple.clone(),
        &deployment,
        true,
    )
    .await
    .unwrap();
    session.append(&converted.records, true).await.unwrap();
    drop(session);
    let session = Session::load(LocalBackend::new(&root), addr, &triple, &deployment)
        .await
        .unwrap();
    let mut actor = StoreActor::new(
        bind(),
        session,
        executor,
        Box::new(MemoryParts {
            parts: converted.parts,
        }),
    )
    .with_readers(Arc::new(move || LocalBackend::new(&root)));
    actor.registry = Some(side.registry.handle());
    actor
}

async fn finish_media(actor: &mut StoreActor<LocalBackend>) {
    if let Some(build) = actor.media_build.take() {
        actor.media_built(build.await).await;
    }
}

async fn first_resumed_wait_warms_the_recorded_base(cipher: Cipher) {
    for closed in [false, true] {
        let side = super::side(false, cipher).await;
        let records = history(closed);
        let recorded =
            broca_core::cache_warm::last_recorded_request_with_lineage(&records, None).unwrap();
        let executor = Arc::new(Warming::default());
        let mut actor = reopened(&side, &records, cipher, Arc::clone(&executor)).await;
        let mut media = Some(actor.run_media().await);
        let (driver, mut warming) = actor
            .keep_warm_for(
                &super::super::request("ignored"),
                StoreRunMode::Resume,
                &mut media,
            )
            .await
            .unwrap();
        warming.refresh(actor.session.operations.run.as_ref().unwrap(), &actor.parts);
        assert!(
            driver.step_seal().unwrap().last_sent().is_none(),
            "no model sent in this process"
        );
        finish_media(&mut actor).await;
        let run = RunId("r1".into());
        let warm = driver.warm_run(&run);
        tokio::pin!(warm);
        let kept = tokio::select! {
            tick = &mut warm => panic!("warm ended before recording: {}", matches!(tick, WarmTick::Sent)),
            kept = warming.records.recv() => kept.unwrap(),
        };
        let Record::Warmed(store_warm) = kept.record.clone() else {
            unreachable!()
        };
        actor.record_kept_warm(kept).await;
        assert!(matches!(warm.await, WarmTick::Sent));
        assert_eq!(store_warm.step_id, 2);
        assert_eq!(
            super::hex(&store_warm.request_body_sha256),
            recorded.request_body_sha256
        );
        assert_eq!(
            actor.session.conversation.placed,
            if closed { 5 } else { 3 },
            "the live conversation is unchanged"
        );

        broca_test_support::pin!(
            "Recorded bases warm identical bytes",
            *executor.sent.lock().unwrap()
        );
        broca_test_support::pin!(
            "the warm's record",
            (
                store_warm.trigger,
                store_warm.run_id,
                store_warm.step_id,
                super::hex(&store_warm.request_body_sha256),
                super::hex(&store_warm.warm_body_sha256),
                store_warm.usage,
                store_warm.ttl_ms,
                store_warm
                    .expires_at_ms
                    .map(|expiry| expiry - store_warm.sent_at_ms)
            )
        );
    }
}

async fn resumed_warm_waits_for_the_media_fold(cipher: Cipher) {
    let side = super::side(false, cipher).await;
    let records = history(false);
    let executor = Arc::new(Warming::default());
    let gate = Arc::new(Notify::new());
    let mut actor = reopened(&side, &records, cipher, Arc::clone(&executor))
        .await
        .with_media_build_gate(Arc::clone(&gate));
    let mut media = Some(actor.run_media().await);
    let (driver, mut warming) = actor
        .keep_warm_for(
            &super::super::request("ignored"),
            StoreRunMode::Resume,
            &mut media,
        )
        .await
        .unwrap();
    warming.refresh(actor.session.operations.run.as_ref().unwrap(), &actor.parts);
    let run = RunId("r1".into());
    let warm = driver.warm_run(&run);
    tokio::pin!(warm);
    assert!(futures::poll!(&mut warm).is_pending());
    assert!(
        executor.sent.lock().unwrap().is_empty(),
        "RecordedMedia cannot send before readiness"
    );
    assert!(warming.records.try_recv().is_err());
    gate.notify_one();
    finish_media(&mut actor).await;
    let kept = tokio::select! {
        _ = &mut warm => panic!("warm ended before recording"),
        kept = warming.records.recv() => kept.unwrap(),
    };
    actor.record_kept_warm(kept).await;
    assert!(matches!(warm.await, WarmTick::Sent));
    assert_eq!(executor.sent.lock().unwrap().len(), 1);
}

async fn mismatched_recorded_digest_never_warms(cipher: Cipher) {
    let side = super::side(false, cipher).await;
    let mut records = history(false);
    let attempt = records
        .iter_mut()
        .rev()
        .find(|record| matches!(record, WalRecord::ModelAttemptFinished { .. }))
        .unwrap();
    if let WalRecord::ModelAttemptFinished {
        request_body_sha256,
        ..
    } = attempt
    {
        *request_body_sha256 = "ff".repeat(32);
    }
    let executor = Arc::new(Warming::default());
    let mut actor = reopened(&side, &records, cipher, Arc::clone(&executor)).await;
    let mut media = Some(actor.run_media().await);
    let (driver, mut warming) = actor
        .keep_warm_for(
            &super::super::request("ignored"),
            StoreRunMode::Resume,
            &mut media,
        )
        .await
        .unwrap();
    warming.refresh(actor.session.operations.run.as_ref().unwrap(), &actor.parts);
    finish_media(&mut actor).await;
    let tick = driver.warm_run(&RunId("r1".into())).await;
    let WarmTick::Stop { reason, detail } = tick else {
        panic!("a digest mismatch must stop")
    };
    assert_eq!(reason, "unsupported");
    let detail = detail.unwrap();
    assert!(detail.contains("PrefixUnavailable"), "{detail}");
    assert!(
        detail.contains(&"ff".repeat(32)),
        "the refusal names the recorded digest: {detail}"
    );
    assert!(executor.sent.lock().unwrap().is_empty());
    assert!(warming.records.try_recv().is_err());
}
