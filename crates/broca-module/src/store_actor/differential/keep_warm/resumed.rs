//! A restarted driver's first tool wait, with no transport sends in the new process: the store
//! actor rebuilds the warm's base from the durable history.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::keep_warm_wait::KeepWarmDriver as _;
use broca_core::keep_warm_wait::WarmTick;
use broca_core::OutputEvent;
use broca_protocol::RunId;
use broca_session_loop::parts::MemoryParts;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use serde_json::json;
use tokio::sync::{mpsc, Notify};

use super::super::warm::{warmable, Probe};
use crate::executor::{RunExecutor, StoreRunMode, WarmError, WarmJob};
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
    warm_configs: Mutex<Vec<broca_records::record::RunConfig>>,
    quota_configs: Mutex<Vec<broca_records::record::RunConfig>>,
    clock: Option<Arc<std::sync::atomic::AtomicU64>>,
    no_cache: bool,
    fail: bool,
    store: Option<crate::store_actor::tests::LoopExecutor>,
    /// When set, each warm signals the first notification as it starts, then waits for the
    /// second before it sends.
    gate: Option<(Arc<Notify>, Arc<Notify>)>,
}

async fn cold_cache_predecessor_rebuilds_the_previous_run_lazily(cipher: Cipher) {
    let side = super::side(cipher).await;
    // Run r1's two tool steps, then a run r2 that started and sent nothing yet.
    let history = converted("cold-predecessor");
    let expected = history.recorded_sha256.clone().unwrap();
    let actor = reopened(&side, history, cipher, Arc::new(Warming::default())).await;
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
    assert_eq!(recovered.request.request_body_sha256, expected);
    let live = broca_provider::LiveProvider::new(
        broca_provider::families::AnthropicMessages,
        serde_json::from_value(recovered.request.config.frozen_render_config).unwrap(),
        broca_provider::wire::AuthHandle::None,
    );
    let bytes = broca_core::provider::Provider::render_request(&live, &recovered.request.opts);
    assert_eq!(broca_core::render::request_digest_hex(&bytes), expected);
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
        _: &broca_records::record::RunConfig,
        _: u64,
        _: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        // Two warms reach one rewrite's cost, so `auto` stops after the second.
        Ok(broca_core::keep_warm::WarmCosts {
            warm: 0.5,
            bust: 1.0,
        })
    }

    async fn warm(&self, job: WarmJob) -> Result<broca_core::WarmSent, WarmError> {
        if let Some((started, release)) = &self.gate {
            started.notify_one();
            release.notified().await;
        }
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
        config: &broca_records::record::RunConfig,
        _: &std::collections::BTreeMap<String, String>,
    ) -> Result<(), String> {
        self.quota_configs.lock().unwrap().push(config.clone());
        Ok(())
    }
}

/// A session converted from the old storage, as the converter that ran at the storage cutover
/// wrote it: its records, its setup parts, the digest of the request its last recorded attempt
/// sent, and the request bodies a test compares with. The fixtures were written from today's
/// histories by that converter, which no longer exists; this is how those sessions are stored.
struct Converted {
    records: Vec<Record>,
    parts: std::collections::BTreeMap<broca_session_store::codec::Digest, Vec<u8>>,
    recorded_sha256: Option<String>,
    bodies: Vec<Vec<u8>>,
}

/// The converted history `name`. Every history has a run started with two recorded tool steps,
/// which distinguish the run's last request from its first:
/// - `open-tool-wait`: the second step's call is still held by its tool.
/// - `closed-tool-wait`: its reply is placed, and the run's post-tool wait continues.
/// - `cold-predecessor`: `closed-tool-wait`, finished, then a run `r2` that sent nothing yet.
/// - `mismatched-digest`: `open-tool-wait` whose last attempt records another request's digest.
/// - `finished-head`: `closed-tool-wait`, finished, in a session of class `head`.
/// - `finished-head-unclassed`: `finished-head` with no session class.
/// - `finished-head-keep-warm-off`: `finished-head` whose run turned keep-warm off.
/// - `moved-account`: `finished-head` whose second step moved from account A to B after a
///   usage limit; `bodies` are the requests its three attempts sent.
///
/// Times are moved forward by the age of the fixture, so its last request is as recent as when
/// it was written and a cache it warmed is still alive.
fn converted(name: &str) -> Converted {
    let text = match name {
        "open-tool-wait" => include_str!("resumed/fixtures/open-tool-wait.json"),
        "closed-tool-wait" => include_str!("resumed/fixtures/closed-tool-wait.json"),
        "cold-predecessor" => include_str!("resumed/fixtures/cold-predecessor.json"),
        "mismatched-digest" => include_str!("resumed/fixtures/mismatched-digest.json"),
        "finished-head" => include_str!("resumed/fixtures/finished-head.json"),
        "finished-head-unclassed" => include_str!("resumed/fixtures/finished-head-unclassed.json"),
        "finished-head-keep-warm-off" => {
            include_str!("resumed/fixtures/finished-head-keep-warm-off.json")
        }
        "moved-account" => include_str!("resumed/fixtures/moved-account.json"),
        other => panic!("no converted history {other}"),
    };
    let fixture: serde_json::Value = serde_json::from_str(text).unwrap();
    let bytes = |value: &serde_json::Value| -> Vec<u8> {
        let text = value.as_str().unwrap();
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect()
    };
    let age = crate::actor::epoch_ms() - fixture["generated_at_ms"].as_u64().unwrap();
    let records = fixture["records"]
        .as_array()
        .unwrap()
        .iter()
        .map(|record| {
            let kind = u32::try_from(record["kind"].as_u64().unwrap()).unwrap();
            let mut record = Record::decode(kind, &bytes(&record["body"])).unwrap();
            match &mut record {
                Record::RunStart(start) => {
                    start.ts_ms = start.ts_ms.map(|ts| ts + age);
                    start.written_at_ms = start.written_at_ms.map(|ts| ts + age);
                }
                Record::Attempt(attempt) => {
                    attempt.ts_ms += age;
                    attempt.sent_at_ms = attempt.sent_at_ms.map(|ts| ts + age);
                }
                _ => {}
            }
            record
        })
        .collect();
    let parts = fixture["parts"]
        .as_array()
        .unwrap()
        .iter()
        .map(|part| {
            let digest: [u8; 32] = bytes(&part[0]).try_into().unwrap();
            (broca_session_store::codec::Digest(digest), bytes(&part[1]))
        })
        .collect();
    Converted {
        records,
        parts,
        recorded_sha256: fixture["recorded_sha256"].as_str().map(str::to_owned),
        bodies: fixture["bodies"]
            .as_array()
            .unwrap()
            .iter()
            .map(bytes)
            .collect(),
    }
}

async fn reopened(
    side: &super::Side,
    converted: Converted,
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
        let side = super::side(cipher).await;
        let history = converted(if closed {
            "closed-tool-wait"
        } else {
            "open-tool-wait"
        });
        let recorded = history.recorded_sha256.clone().unwrap();
        let executor = Arc::new(Warming::default());
        let mut actor = reopened(&side, history, cipher, Arc::clone(&executor)).await;
        let mut media = Some(actor.run_media().await);
        let (driver, mut warming) = actor
            .keep_warm_for(None, StoreRunMode::Resume, &mut media)
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
        assert_eq!(super::hex(&store_warm.request_body_sha256), recorded);
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
    let side = super::side(cipher).await;
    let history = converted("open-tool-wait");
    let executor = Arc::new(Warming::default());
    let gate = Arc::new(Notify::new());
    let mut actor = reopened(&side, history, cipher, Arc::clone(&executor))
        .await
        .with_media_build_gate(Arc::clone(&gate));
    let mut media = Some(actor.run_media().await);
    let (driver, mut warming) = actor
        .keep_warm_for(None, StoreRunMode::Resume, &mut media)
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
    let side = super::side(cipher).await;
    let history = converted("mismatched-digest");
    let executor = Arc::new(Warming::default());
    let mut actor = reopened(&side, history, cipher, Arc::clone(&executor)).await;
    let mut media = Some(actor.run_media().await);
    let (driver, mut warming) = actor
        .keep_warm_for(None, StoreRunMode::Resume, &mut media)
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
