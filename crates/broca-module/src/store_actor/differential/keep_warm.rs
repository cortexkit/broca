//! Keep-warm on a session kept in the new storage, through the module's registry, against the
//! same run on a session on today's storage.
//!
//! Both modules run the same scripted model, whose transport notes each request it sends on the
//! seal its run gives it, as the real transport does. The model's frozen render config is a
//! first-party Anthropic one with one-hour cache markers, so the session's cache policy allows a
//! warm, read with the provider families' own rules, as the serving layer installs them. A send
//! whose keep-warm is `auto` calls a tool, which is held while the scheduler warms: the warm
//! re-sends the request the run's transport last sent, and its record goes in the session.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary, WalStore as _};
use broca_protocol::{BindIdentity, RunId};
use broca_session_store::keys::Cipher;
use broca_session_store::record::Record;
use tokio::sync::{mpsc, Notify};

use super::warm::{warmable, FamilyDeclarer};
use super::{request, TodayLoop, IDLE};
use crate::actor::{LineageStatus, SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunJob, WarmError, WarmJob};
use crate::registry::LineageRegistry;

cipher_tests!(
    a_keep_warm_resends_the_last_request_and_records_it_alike,
    keep_warm_step_events_match
);

mod resumed;

/// How long after a request a warm of its cache is sent: short, so the tool wait sees one.
const WARM_AFTER_MS: u64 = 200;

/// Today's loop and the new loop, warming through a provider that answers the first warm and
/// never answers a later one, so each run records exactly one warm however long its wait lasts.
struct Loops {
    today: TodayLoop,
    store: crate::store_actor::tests::LoopExecutor,
    /// The bytes of each warm sent.
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl RunExecutor for Loops {
    async fn execute(
        &self,
        job: RunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        self.today.execute(job, out).await
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        self.store.execute_on_store(job, out).await
    }

    fn warm_costs(
        &self,
        _config: &broca_wal::RunConfig,
        _prefix_tokens: u64,
        _ttl_ms: u64,
    ) -> Result<broca_core::keep_warm::WarmCosts, String> {
        Ok(broca_core::keep_warm::WarmCosts {
            warm: 0.01,
            bust: 10.0,
        })
    }

    fn warm_after_send_ms(&self, _config: &broca_wal::RunConfig) -> Option<u64> {
        Some(WARM_AFTER_MS)
    }

    async fn warm(&self, job: WarmJob) -> Result<broca_core::WarmSent, WarmError> {
        let crate::executor::WarmBase::InFlight(sent) = job.base else {
            panic!("a warm while the run waits re-sends the bytes its transport sent");
        };
        let first = {
            let mut warms = self.sent.lock().unwrap();
            warms.push(sent.bytes().to_vec());
            warms.len() == 1
        };
        if !first {
            std::future::pending::<()>().await;
        }
        let id = if let Some(hook) = &job.attempt_hook {
            Some(
                hook.before_send(
                    broca_core::run::PhysicalSend {
                        attempt: 0,
                        send_index: 1,
                        endpoint_host: "api.anthropic.com".into(),
                        model_sent: job.config.model.model_id.clone(),
                        request_body_bytes: sent.bytes().len() as u64,
                        compressed: false,
                        account_ref: None,
                        auth_refreshed: false,
                        speed_requested: None,
                        warm: Some(broca_core::cache_warm::WarmSendPolicy {
                            provider: None,
                            ttl_ms: Some(3_600_000),
                            auth_refreshed: false,
                        }),
                    },
                    Default::default(),
                )
                .await
                .map_err(|e| WarmError::Provider(Box::new(e)))?,
            )
        } else {
            None
        };
        let sent = broca_core::WarmSent {
            request_body_bytes: Some(sent.bytes().len() as u64),
            warm_body_sha256: broca_core::render::request_digest_hex(sent.bytes()),
            sent_at_ms: crate::actor::epoch_ms(),
            usage: broca_protocol::Usage {
                input_tokens: Some(3),
                cached_input_tokens: Some(1_000),
                ..Default::default()
            },
            served_account_ref: None,
            ratelimit_headers: Vec::new(),
            http_status: 200,
        };
        if let (Some(hook), Some(id)) = (job.attempt_hook, id) {
            hook.warm_finished(
                id,
                broca_wal::record::AttemptEnvelope {
                    sent: broca_wal::record::AttemptSent::Yes,
                    outcome: broca_wal::record::BillingAttemptOutcome::Complete,
                    sent_at_ms: Some(sent.sent_at_ms),
                    finished_at_ms: Some(crate::actor::epoch_ms()),
                    usage: Some(sent.usage),
                    ..Default::default()
                },
                Some(sent.clone()),
                sent.warm_body_sha256.clone(),
            )
            .await
            .map_err(|e| WarmError::Provider(Box::new(e)))?;
        }
        Ok(sent)
    }
}

/// One module over a scratch state root, with its session's keep-warm send held in its tool
/// call until released.
struct Side {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    runtime: Option<Arc<crate::store_open::StoreRuntime>>,
    wal: Arc<FileWalStore>,
    key: BindIdentity,
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
    /// Signalled when the tool call is dispatched, and what releases it.
    tool: (Arc<Notify>, Arc<Notify>),
}

impl Drop for Side {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A module whose sessions start in the new storage when `store`; the new storage creates
/// sessions in mode `cipher`.
async fn side(store: bool, cipher: Cipher) -> Side {
    side_events(store, cipher, None).await
}

async fn side_events(
    store: bool,
    cipher: Cipher,
    events: Option<Arc<crate::step_events::StepEvents>>,
) -> Side {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-keep-warm-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let steps = || {
        vec![
            crate::store_actor::tests::read_step("a1"),
            crate::store_actor::tests::text_step("a2", "done"),
        ]
    };
    let render_config = Some(serde_json::to_value(warmable()).unwrap());
    let tool = (Arc::new(Notify::new()), Arc::new(Notify::new()));
    let planes = crate::store_actor::tests::Planes {
        tool_gate: Some(tool.clone()),
        ..Default::default()
    };
    let wal = Arc::new(if events.is_some() {
        FileWalStore::with_config(dir.join("wal"), broca_wal::WalConfig::OVERHAUL)
    } else {
        FileWalStore::new(dir.join("wal"))
    });
    let mut store_loop = super::executor(steps(), None);
    store_loop.provider.render_config = render_config.clone();
    store_loop.provider.notes_sends = true;
    store_loop.planes = planes.clone();
    let sent = Arc::default();
    let executor: Arc<dyn RunExecutor> = Arc::new(Loops {
        today: TodayLoop {
            provider: super::Gated {
                inner: broca_core::provider::StubProvider::new(steps()),
                gate: None,
                render_config,
                notes_sends: true,
            },
            wal: Arc::clone(&wal),
            planes,
        },
        store: store_loop,
        sent: Arc::clone(&sent),
    });
    let runtime = Arc::new(
        crate::store_open::StoreRuntime::open(
            &dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher),
            Arc::clone(&executor),
            IDLE,
        )
        .await
        .unwrap(),
    );
    let mut opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        executor,
    )
    .with_idle_ttl(IDLE)
    .with_store_runtime(Arc::clone(&runtime));
    if let Some(events) = events {
        opener = opener.with_step_events(events);
    }
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None);
    registry.set_cache_declarer(Arc::new(FamilyDeclarer));
    let registry = if store {
        registry.with_store_new_sessions()
    } else {
        registry
    };
    Side {
        key: BindIdentity::new(dir.join("project"), "store-actor", "s1"),
        dir,
        registry,
        runtime: store.then_some(runtime),
        wal,
        sent,
        tool,
    }
}

/// What a warm's record says, the same on either storage: its trigger, run, step, the digests of
/// the request it warmed and of the warm itself, its usage and its lifetime counted from its send.
type Warm = (
    Option<String>,
    String,
    u64,
    String,
    String,
    broca_protocol::Usage,
    Option<u64>,
    Option<u64>,
);

fn hex(digest: &broca_session_store::codec::Digest) -> String {
    digest.0.iter().map(|byte| format!("{byte:02x}")).collect()
}

impl Side {
    /// Sends a keep-warm send, waits until its run warmed once while its tool call is held, then
    /// releases the call and waits for the session to be idle.
    async fn keep_warm_once(&self) {
        let mut first = request("hello");
        first.model = broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8");
        first.keep_warm = Some(broca_protocol::request::KeepWarm::Auto);
        let outcome = self
            .registry
            .route_submission_with_order(
                &self.key,
                RunId("r1".into()),
                first,
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                None,
                false,
                None,
            )
            .await
            .unwrap();
        assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
        self.tool.0.notified().await;
        broca_test_support::wait_until("the run warmed while its tool call ran", || async {
            match self.warms().await.len() {
                0 => Err("no durable warm yet".to_owned()),
                _ => Ok(()),
            }
        })
        .await;
        self.tool.1.notify_one();
        broca_test_support::wait_until("the session is idle", || async {
            match self.registry.route_status(&self.key).await {
                Ok(LineageStatus::Idle) => Ok(()),
                other => Err(format!("{other:?}")),
            }
        })
        .await;
    }

    /// The warms the session recorded.
    async fn warms(&self) -> Vec<Warm> {
        match &self.runtime {
            None => self
                .wal
                .read_records(&self.key)
                .unwrap()
                .into_iter()
                .filter_map(|frame| match frame.record {
                    broca_wal::WalRecord::CacheWarmed {
                        trigger,
                        run_id,
                        step_id,
                        request_body_sha256,
                        warm_body_sha256,
                        usage,
                        sent_at_ms,
                        ttl_ms,
                        expires_at_ms,
                        ..
                    } => Some((
                        trigger,
                        run_id.0,
                        step_id.0,
                        request_body_sha256,
                        warm_body_sha256,
                        usage,
                        ttl_ms,
                        expires_at_ms.map(|expires| expires - sent_at_ms),
                    )),
                    _ => None,
                })
                .collect(),
            Some(runtime) => {
                let view = runtime.current_view(&self.key).await.unwrap().unwrap();
                view.open_log_records()
                    .await
                    .unwrap()
                    .into_iter()
                    .filter_map(|(_, record)| match record {
                        Record::Warmed(warmed) => {
                            assert_eq!(warmed.reached_cache, Some(true));
                            Some((
                                warmed.trigger,
                                warmed.run_id,
                                warmed.step_id,
                                hex(&warmed.request_body_sha256),
                                hex(&warmed.warm_body_sha256),
                                warmed.usage,
                                warmed.ttl_ms,
                                warmed
                                    .expires_at_ms
                                    .map(|expires| expires - warmed.sent_at_ms),
                            ))
                        }
                        _ => None,
                    })
                    .collect()
            }
        }
    }
}

/// A run whose keep-warm is `auto` warms while its tool call runs, as today's does: the warm
/// re-sends the bytes the run's transport sent for the step that called the tool, and its record
/// names that step, the request's digest, the warm's usage and the policy's lifetime counted from
/// the warm's own send. The run then goes on to its end.
async fn a_keep_warm_resends_the_last_request_and_records_it_alike(cipher: Cipher) {
    let today = side(false, cipher).await;
    let store = side(true, cipher).await;
    today.keep_warm_once().await;
    store.keep_warm_once().await;
    assert_eq!(
        *store.sent.lock().unwrap(),
        *today.sent.lock().unwrap(),
        "the same bytes warmed"
    );
    let warms = today.warms().await;
    assert_eq!(warms.len(), 1, "{warms:?}");
    let (trigger, _, step_id, request, warmed, _, ttl_ms, lifetime) = &warms[0];
    assert_eq!(trigger.as_deref(), Some("keep_warm"));
    assert_eq!(*step_id, 1, "the step that called the tool");
    assert_eq!(request, warmed, "the warm re-sent the request it names");
    assert_eq!((*ttl_ms, *lifetime), (Some(3_600_000), Some(3_600_000)));
    assert_eq!(store.warms().await, warms);
    assert!(today.registry.stop_and_seal().await);
    assert!(store.registry.stop_and_seal().await);
}

async fn keep_warm_step_events_match(cipher: Cipher) {
    use super::step_events::{normalized, sample, Capture};
    use crate::step_events::StepEvents;
    let mut bodies = Vec::new();
    for new_storage in [false, true] {
        let (calls, mut inbox) = mpsc::unbounded_channel();
        let events = Arc::new(
            StepEvents::start("fixture", "broca", Arc::new(Capture { calls, fail: false }))
                .unwrap(),
        );
        let side = side_events(new_storage, cipher, Some(events)).await;
        side.keep_warm_once().await;
        assert!(side.registry.stop_and_seal().await);
        let (subject, body) =
            broca_test_support::wait_for("durable keep-warm event", inbox.recv(), || {
                format!("new={new_storage}")
            })
            .await
            .unwrap();
        assert_eq!(subject, "ck.fixture.live.broca.step.finished.v1");
        let event =
            cortexkit_role_llm_runner::step_events::decode_step_finished(&subject, &body).unwrap();
        assert_eq!(event.runner, "broca");
        assert_eq!(event.harness, side.key.harness);
        assert_eq!(event.session, side.key.session);
        assert_eq!(event.session_ref, broca_core::agent_session_ref(&side.key));
        assert_eq!(event.input_tokens, Some(3));
        assert_eq!(event.cached_input_tokens, Some(1_000));
        assert_eq!(
            serde_json::to_value(&event.cache).unwrap(),
            serde_json::json!({"ttl_ms":3_600_000,"warming":{"on":true}})
        );
        assert_eq!(
            event.request_body_bytes,
            Some(side.sent.lock().unwrap()[0].len() as u64)
        );
        if let Some(runtime) = &side.runtime {
            let view = runtime.current_view(&side.key).await.unwrap().unwrap();
            let warms: Vec<_> = view
                .open_log_records()
                .await
                .unwrap()
                .into_iter()
                .filter_map(|(seq, record)| match record {
                    Record::Warmed(warmed) => Some((seq, warmed)),
                    _ => None,
                })
                .collect();
            assert_eq!(warms.len(), 1);
            let (seq, warmed) = &warms[0];
            assert_eq!(event.seq, *seq);
            assert_eq!(event.attempt_id, warmed.attempt_id);
            assert_eq!(event.started_at_ms, warmed.sent_at_ms);
            assert_eq!(
                Some(event.finished_at_ms),
                warmed.billing.as_ref().unwrap()["finished_at_ms"].as_u64()
            );
            let incarnation: String = view
                .head()
                .incarnation
                .0
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();
            assert_eq!(event.log_incarnation, incarnation);
        } else {
            let records = side.wal.read_records(&side.key).unwrap();
            let warms: Vec<_> = records
                .iter()
                .filter_map(|frame| match &frame.record {
                    broca_wal::WalRecord::CacheWarmed {
                        attempt_id,
                        envelope: Some(envelope),
                        ..
                    } => Some((frame.seq, attempt_id, envelope)),
                    _ => None,
                })
                .collect();
            assert_eq!(warms.len(), 1);
            assert_eq!(event.seq, warms[0].0);
            assert_eq!(
                event.attempt_id.as_deref(),
                warms[0].1.as_ref().map(|id| id.as_str())
            );
            assert_eq!(Some(event.started_at_ms), warms[0].2.sent_at_ms);
            assert_eq!(Some(event.finished_at_ms), warms[0].2.finished_at_ms);
            let writer = records
                .iter()
                .find_map(|frame| match &frame.record {
                    broca_wal::WalRecord::WriterOpened { writer_id, .. } => Some(writer_id),
                    _ => None,
                })
                .unwrap();
            let writer: String = writer.iter().map(|b| format!("{b:02x}")).collect();
            let lineage = side.wal.open(&side.key).unwrap().lineage_id().unwrap();
            assert_eq!(event.log_incarnation, format!("{lineage}.{writer}"));
        }
        tokio::task::yield_now().await;
        assert!(inbox.try_recv().is_err(), "a warm published twice");
        let storage = if new_storage { "store" } else { "today" };
        let mode = if cipher == Cipher::None {
            "plain"
        } else {
            "encrypted"
        };
        sample(&format!("keep-warm-{storage}-{mode}.json"), &body);
        bodies.push(body);
    }
    assert_eq!(normalized(&bodies[0]), normalized(&bodies[1]));
}
