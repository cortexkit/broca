//! `session.warm` on a session kept in the new storage, through the module's registry.
//!
//! The module runs a scripted model whose frozen render config is a first-party Anthropic one
//! with one-hour cache markers, so the session's cache policy allows a warm, read with the
//! provider families' own rules, as the serving layer installs them. Each refusal decided before
//! anything is sent is pinned. A warm the store sends re-sends the request its loop would send
//! next, rendered by the real Anthropic transport and checked against the digest the actor
//! computed; its record dates the session's cache, which a restarted module reads back.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_protocol::{BindIdentity, CallOptions, NormalizedEvent, RunId, StepId};
use broca_provider::families::{anthropic_frozen_config, AnthropicMessages};
use broca_provider::wire::{
    AuthHandle, CacheClass, CachePolicy, CacheTtl, FrozenRenderConfig, MarkerSyntax,
};
use broca_session_store::keys::Cipher;
use serde_json::json;
use tokio::sync::mpsc;

use super::{request, IDLE};
use crate::actor::{LineageStatus, SendKey, SendOutcome, WarmOutcome};
use crate::cache::{CacheDeclaration, CacheDeclarer};
use crate::executor::{RunExecutor, RunJob, RunKind, StoreRunEnd, StoreRunJob, WarmError, WarmJob};
use crate::registry::LineageRegistry;

cipher_tests!(
    warm_refusals_answer_alike,
    a_warm_resends_the_next_request_and_dates_the_cache,
);

/// A first-party Anthropic config frozen with one-hour markers and adaptive thinking, which a
/// warm may refresh.
pub(super) fn warmable() -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    });
    // Manual thinking can't accompany a zero-output request; adaptive thinking can.
    frozen.variant = Some(broca_provider::variant::FrozenVariant {
        name: "high".into(),
        request: broca_provider::variant::VariantRequest::Anthropic {
            thinking: Some(json!({"type": "adaptive", "display": "summarized"})),
            effort: Some("high".into()),
        },
    });
    frozen
}

/// Declares a session's cache with the provider families' own rules, as the serving layer does.
pub(super) struct FamilyDeclarer;

impl CacheDeclarer for FamilyDeclarer {
    fn declare(&self, config: &broca_wal::RunConfig) -> Option<CacheDeclaration> {
        let frozen: FrozenRenderConfig =
            serde_json::from_value(config.frozen_render_config.clone()).ok()?;
        let declared = broca_provider::cache_warm::cache_declaration(
            &config.model.provider_module_id,
            &frozen,
            &config.tool_choice,
            &config.response_format,
        );
        Some(CacheDeclaration {
            ttl_ms: declared.ttl_ms,
            ttl_options_ms: declared.ttl_options_ms,
            read_refreshes_ttl: declared.read_refreshes_ttl,
            warm: declared.warm,
            basis: declared.basis.map(str::to_owned),
            requested_ttl_ms: declared.requested_ttl_ms,
        })
    }
}

/// The provider a warm goes through: the real Anthropic transport renders the request, and the
/// warm is answered here instead of over the network, keeping the bytes it was asked to send.
pub(super) struct Probe {
    pub(super) live: broca_provider::LiveProvider<AnthropicMessages>,
    pub(super) sent: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl broca_core::provider::Provider for Probe {
    async fn step(&self, _step_id: StepId, _opts: &CallOptions) -> mpsc::Receiver<NormalizedEvent> {
        unreachable!("a warm calls no model step")
    }

    fn render_request(&self, opts: &CallOptions) -> Vec<u8> {
        broca_core::provider::Provider::render_request(&self.live, opts)
    }

    fn set_media_request_renderer(
        &self,
        renderer: Arc<broca_core::provider::MediaRequestRenderer>,
    ) {
        broca_core::provider::Provider::set_media_request_renderer(&self.live, renderer);
    }

    async fn warm(&self, base: &[u8]) -> Result<broca_core::WarmSent, broca_core::WarmFailure> {
        self.sent.lock().unwrap().push(base.to_vec());
        Ok(broca_core::WarmSent {
            request_body_bytes: None,
            warm_body_sha256: broca_core::render::request_digest_hex(base),
            sent_at_ms: crate::actor::epoch_ms(),
            usage: broca_protocol::Usage {
                input_tokens: Some(3),
                cached_input_tokens: Some(1_000),
                ..Default::default()
            },
            served_account_ref: None,
            ratelimit_headers: Vec::new(),
            http_status: 200,
        })
    }
}

/// The new loop, and a warm through [`Probe`].
struct Loops {
    store: crate::store_actor::tests::LoopExecutor,
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
}

#[async_trait]
impl RunExecutor for Loops {
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
        _job: RunJob,
        _out: mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("a store session's runs come through execute_on_store")
    }

    async fn execute_on_store(
        &self,
        job: StoreRunJob,
        out: mpsc::Sender<OutputEvent>,
    ) -> StoreRunEnd {
        self.store.execute_on_store(job, out).await
    }

    async fn warm(&self, job: WarmJob) -> Result<broca_core::WarmSent, WarmError> {
        let frozen: FrozenRenderConfig =
            serde_json::from_value(job.config.frozen_render_config.clone())
                .map_err(|error| WarmError::Unsupported(error.to_string()))?;
        let probe = Probe {
            live: broca_provider::LiveProvider::new(
                AnthropicMessages,
                frozen,
                AuthHandle::Bearer("t".into()),
            ),
            sent: Arc::clone(&self.sent),
        };
        crate::executor::send_warm(&probe, job.base).await
    }
}

/// A module over the state root `dir` whose sessions start in the new storage when `store`.
/// With `declares`, it reads sessions' cache policies; with `config`, its model freezes that
/// render config.
struct Side {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    key: BindIdentity,
    sent: Arc<Mutex<Vec<Vec<u8>>>>,
}

impl Drop for Side {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn scratch() -> std::path::PathBuf {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-warm-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The module over the state root `dir`; its new storage creates sessions in mode `cipher`.
async fn registry_at(
    dir: &std::path::Path,
    store: bool,
    declares: bool,
    config: Option<&FrozenRenderConfig>,
    steps: Vec<Vec<NormalizedEvent>>,
    sent: &Arc<Mutex<Vec<Vec<u8>>>>,
    cipher: Cipher,
) -> LineageRegistry {
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let render_config = config.map(|config| serde_json::to_value(config).unwrap());
    let mut store_loop = super::executor(steps, None);
    store_loop.provider.render_config = render_config;
    let executor: Arc<dyn RunExecutor> = Arc::new(Loops {
        store: store_loop,
        sent: Arc::clone(sent),
    });
    let runtime = crate::store_open::StoreRuntime::open(
        dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher),
        Arc::clone(&executor),
        IDLE,
    )
    .await
    .unwrap();
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            crate::opener::DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                executor,
            )
            .with_idle_ttl(IDLE)
            .with_store_runtime(Arc::new(runtime)),
        ),
        wal,
        None,
    );
    if declares {
        registry.set_cache_declarer(Arc::new(FamilyDeclarer));
    }
    if store {
        registry.with_store_new_sessions()
    } else {
        registry
    }
}

/// A side whose session ran one send to its end. A store side's session is in mode `cipher`.
async fn side(
    store: bool,
    declares: bool,
    config: Option<&FrozenRenderConfig>,
    steps: Vec<Vec<NormalizedEvent>>,
    cipher: Cipher,
) -> Side {
    let dir = scratch();
    let sent = Arc::default();
    let registry = registry_at(&dir, store, declares, config, steps, &sent, cipher).await;
    let key = BindIdentity::new(dir.join("project"), "store-actor", "s1");
    let side = Side {
        dir,
        registry,
        key,
        sent,
    };
    let mut first = request("hello");
    first.model = broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8");
    let outcome = side
        .registry
        .route_submission_with_order(
            &side.key,
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
    broca_test_support::wait_until("the session is idle", || async {
        match side.registry.route_status(&side.key).await {
            Ok(LineageStatus::Idle) => Ok(()),
            other => Err(format!("{other:?}")),
        }
    })
    .await;
    if store {
        assert_eq!(super::media::stored_mode(&side.dir, &side.key), cipher);
    }
    side
}

impl Side {
    async fn warm(&self, lineage_id: Option<&str>) -> WarmOutcome {
        self.registry
            .route_warm(&self.key, lineage_id.map(str::to_owned))
            .await
            .unwrap()
    }
}

/// A model step that answers `text`, after an attempt that reached the provider at `sent_at_ms`.
fn attempted_step(id: &str, text: &str, sent_at_ms: u64) -> Vec<NormalizedEvent> {
    let attempt = serde_json::from_value(json!({
        "attempt": 0,
        "request_body_len": 10,
        "request_body_sha256": "0".repeat(64),
        "http_status": 200,
        "response_body_len": 0,
        "response_body_sha256": "0".repeat(64),
        "outcome": "complete",
        "ts_ms": sent_at_ms,
        "sent_at_ms": sent_at_ms,
    }))
    .unwrap();
    let mut step = vec![NormalizedEvent::Control(
        broca_protocol::ProviderControl::AttemptFinished { attempt },
    )];
    step.extend(crate::store_actor::tests::text_step(id, text));
    step
}

#[tokio::test]
async fn startup_rearms_an_untouched_automatic_head() {
    let dir = scratch();
    let config = warmable();
    let sent = Arc::default();
    let registry = registry_at(
        &dir,
        true,
        true,
        Some(&config),
        vec![attempted_step("a1", "hello", crate::actor::epoch_ms())],
        &sent,
        Cipher::None,
    )
    .await;
    let key = BindIdentity::new(dir.join("project"), "store-actor", "automatic-head");
    let mut first = request("hello");
    first.model = broca_protocol::ModelHandle::new("anthropic", "claude-opus-4-8");
    first.keep_warm = Some(broca_protocol::request::KeepWarm::Auto);
    assert!(matches!(
        registry
            .route_submission_with_order(
                &key,
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
                Some("head".into()),
                false,
                None,
            )
            .await
            .unwrap(),
        SendOutcome::Active { .. }
    ));
    broca_test_support::wait_until("the automatic head is idle", || async {
        if registry.handle().has_idle_warm(&key) {
            Ok(())
        } else {
            Err("no idle warm timer".into())
        }
    })
    .await;
    assert!(registry.stop_and_seal().await);
    let reboot = registry_at(
        &dir,
        true,
        true,
        Some(&config),
        Vec::new(),
        &sent,
        Cipher::None,
    )
    .await;
    assert!(reboot.slot_state(&key).is_none());
    assert!(!reboot.handle().has_idle_warm(&key));
    let sweep = reboot.recover_store_crash_orphans().await.unwrap();
    assert!(sweep.failed.is_empty(), "{sweep:?}");
    assert_eq!(sweep.recovered, 1);
    assert!(
        reboot.handle().has_idle_warm(&key),
        "startup restores the timer without a session command"
    );
    assert!(reboot.stop_and_seal().await);
    std::fs::remove_dir_all(dir).unwrap();
}

/// The refusals decided before anything is sent answer alike: a warm naming a lineage, a module
/// that can't read the session's cache policy, a session whose policy allows no warm, and a cache
/// whose lifetime is unknown because no request of the session reached it.
async fn warm_refusals_answer_alike(cipher: Cipher) {
    let steps = || vec![crate::store_actor::tests::text_step("a1", "hi")];
    let config = warmable();
    let unknown_clock = |detail: &broca_wire::WarmRefusalDetail| {
        matches!(detail, broca_wire::WarmRefusalDetail::CacheExpired { .. })
    };
    let unsupported = |detail: &broca_wire::WarmRefusalDetail| {
        matches!(
            detail,
            broca_wire::WarmRefusalDetail::WarmUnsupported { .. }
        )
    };
    for (declares, config, what, expected) in [
        (
            true,
            Some(&config),
            "a cache whose clock is unknown",
            &unknown_clock as &dyn Fn(&broca_wire::WarmRefusalDetail) -> bool,
        ),
        (
            false,
            Some(&config),
            "a module that can't read the cache policy",
            &unsupported,
        ),
        (
            true,
            None,
            "a session that froze no cache policy",
            &unsupported,
        ),
    ] {
        let store = side(true, declares, config, steps(), cipher).await;
        let new = store.warm(None).await;
        let WarmOutcome::Refused(refusal) = &new else {
            panic!("{what}: {new:?}");
        };
        assert!(expected(&refusal.detail), "{what}: {new:?}");
        broca_test_support::pin!(what, new);
        assert!(
            store.sent.lock().unwrap().is_empty(),
            "{what}: nothing sent"
        );
    }
    let store = side(true, true, Some(&config), steps(), cipher).await;
    let named = |outcome: WarmOutcome| match outcome {
        WarmOutcome::Refused(refusal) => refusal.detail,
        other => panic!("refused: {other:?}"),
    };
    assert_eq!(
        named(store.warm(Some("another-lineage")).await),
        broca_wire::WarmRefusalDetail::LineageChanged {
            requested: "another-lineage".into()
        },
    );
}

/// A warm of a session whose last request reached the cache re-sends the request its loop would
/// send next: the bytes the real transport renders from the session's state, with the digest the
/// actor computed. It answers with the policy's lifetime counted from the warm's own send, and
/// its record dates the cache: a module restarted on the same state root warms the session again
/// from that clock.
async fn a_warm_resends_the_next_request_and_dates_the_cache(cipher: Cipher) {
    let now = crate::actor::epoch_ms();
    let config = warmable();
    let store = side(
        true,
        true,
        Some(&config),
        vec![attempted_step("a1", "hi", now - 60_000)],
        cipher,
    )
    .await;
    let outcome = store.warm(None).await;
    let WarmOutcome::Warmed(reply) = outcome else {
        panic!("warmed: {outcome:?}");
    };
    let sent = store.sent.lock().unwrap().clone();
    assert_eq!(sent.len(), 1, "one warm sent");
    assert_eq!(
        reply.request_body_sha256,
        broca_core::render::request_digest_hex(&sent[0]),
        "the request warmed is the one the transport rendered"
    );
    let body: serde_json::Value = serde_json::from_slice(&sent[0]).unwrap();
    let texts: Vec<String> = body["messages"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|message| message["content"].as_array().cloned().unwrap_or_default())
        .filter_map(|block| block["text"].as_str().map(str::to_owned))
        .collect();
    assert_eq!(texts, ["hello", "hi"], "the session's next request");
    assert_eq!(reply.ttl_ms, 3_600_000);
    assert_eq!(reply.cache_read_tokens, Some(1_000));
    assert_eq!(reply.expiry_basis, broca_wire::EXPIRY_BASIS_ESTIMATED);
    assert!(store.registry.stop_and_seal().await);
    // The warm's record dates the cache: the session's clock now reads the warm's own send,
    // not the request a minute before it.
    let runtime = crate::store_open::StoreRuntime::open(
        &store.dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]).creating(cipher),
        Arc::new(super::super::tests::executor(Vec::new(), None)),
        IDLE,
    )
    .await
    .unwrap();
    let view = runtime.current_view(&store.key).await.unwrap().unwrap();
    let clock = broca_session_loop::writer::Writer::operations(&***view)
        .last_cache_send
        .clone()
        .expect("the cache is dated");
    assert_eq!(clock.sent_at_ms, reply.expires_at_ms - reply.ttl_ms);
    assert!(clock.sent_at_ms >= now);

    let restarted = registry_at(
        &store.dir,
        true,
        true,
        Some(&config),
        Vec::new(),
        &store.sent,
        cipher,
    )
    .await;
    let again = restarted.route_warm(&store.key, None).await.unwrap();
    let WarmOutcome::Warmed(again) = again else {
        panic!("warmed after a restart: {again:?}");
    };
    assert_eq!(again.request_body_sha256, reply.request_body_sha256);
    assert!(again.expires_at_ms >= reply.expires_at_ms);
    assert!(restarted.stop_and_seal().await);
}
