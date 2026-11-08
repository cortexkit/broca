//! A session kept in the new storage while the deployment key hasn't loaded: refused
//! `key_unavailable`, never served from today's storage, and served once the key loads, without
//! a restart. A run that ends on such a session writes its spend facts to the usage export.

use std::sync::Arc;
use std::time::Duration;

use broca_core::{FileLeaseStore, FileWalStore};
use broca_protocol::{BindIdentity, RunId};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use sha2::Digest;

use super::tests::{executor, request, text_step};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunKind};
use crate::opener::DefaultOpener;
use crate::registry::{LineageRegistry, OpenError};
use crate::store_billing::StoreBilling;
use crate::store_open::{StoreRuntimeSetup, StoreSlot};

/// The deployment key the held session was created under.
const KEY: [u8; 32] = [0x5a; 32];

/// Long enough that no actor closes while a test runs.
const OPEN: Duration = Duration::from_secs(600);

/// A module over a scratch state root holding one session in the new storage (`held`), whose
/// deployment key hasn't loaded, beside a session on today's storage (`today`).
struct Module {
    dir: std::path::PathBuf,
    registry: LineageRegistry,
    slot: Arc<StoreSlot>,
    setup: StoreRuntimeSetup,
    held: BindIdentity,
    today: BindIdentity,
}

impl Drop for Module {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn triple(key: &BindIdentity) -> SessionTriple {
    SessionTriple {
        project_root: key.project_root.display().to_string(),
        harness: key.harness.clone(),
        session: key.session.clone(),
    }
}

async fn module(name: &str, billing: Option<StoreBilling>) -> Module {
    let dir =
        broca_test_support::scratch_root().join(format!("store-key-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let held = BindIdentity::new(dir.join("project"), "store-actor", "held");
    let today = BindIdentity::new(dir.join("project"), "store-actor", "today");
    Session::create(
        LocalBackend::new(dir.join(broca_wal::SESSION_STORAGE_DIR)),
        SessionAddr(broca_core::session_addr(&held)),
        triple(&held),
        &DeploymentKey::new(1, &KEY),
        false,
    )
    .await
    .unwrap();
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let executor: Arc<dyn RunExecutor> = Arc::new(executor(vec![text_step("a1", "hello")], None));
    let slot = Arc::new(StoreSlot::awaiting_key(&dir, 1));
    let registry = LineageRegistry::new_with_reader(
        Arc::new(
            DefaultOpener::new(
                Arc::new(FileLeaseStore::new(dir.join("leases"))),
                wal.clone(),
                Arc::clone(&executor),
            )
            .with_idle_ttl(OPEN)
            .with_store_slot(Arc::clone(&slot)),
        ),
        wal,
        None,
    );
    let setup = StoreRuntimeSetup {
        state_root: dir.clone(),
        executor,
        idle_ttl: OPEN,
        import_roots: None,
        billing,
    };
    Module {
        dir,
        registry,
        slot,
        setup,
        held,
        today,
    }
}

impl Module {
    fn wal_file(&self, key: &BindIdentity) -> std::path::PathBuf {
        self.dir
            .join("wal")
            .join(format!("{}.wal", broca_core::session_addr(key)))
    }

    async fn load_key(&self) {
        self.slot
            .load(&self.setup, DeploymentKey::new(1, &KEY))
            .await
            .expect("the runtime opens under the key");
    }
}

/// The refusal a session kept in the new storage gets while its key hasn't loaded.
fn assert_key_unavailable(error: OpenError) {
    match error {
        OpenError::Classified { reason, class } => {
            assert!(reason.starts_with("key_unavailable:"), "{reason}");
            assert!(reason.contains("deployment key 1"), "{reason}");
            assert_eq!(class, broca_protocol::ErrorClass::Transient);
        }
        other => panic!("expected the transient key_unavailable refusal, got {other:?}"),
    }
}

/// With no key loaded, a session kept in the new storage is refused `key_unavailable` on open
/// and on a read, before today's storage is asked: the refusal is the transient
/// `key_unavailable` one, not the `NewerWriter` refusal today's storage gives a session it doesn't
/// keep, and no WAL is created for it. A session on today's storage at the same root is served
/// as before.
#[tokio::test]
async fn a_held_session_is_refused_key_unavailable_and_never_served_from_todays_storage() {
    let module = module("refused", None).await;
    assert!(!module.slot.key_loaded());
    assert_key_unavailable(
        module
            .registry
            .actor_for(&module.held)
            .await
            .err()
            .expect("the held session is refused"),
    );
    assert_key_unavailable(
        module
            .registry
            .store_holding(&module.held)
            .err()
            .expect("a read of the held session is refused"),
    );
    assert!(
        !module.wal_file(&module.held).exists(),
        "nothing was written for the held session on today's storage"
    );
    module
        .registry
        .actor_for(&module.today)
        .await
        .expect("a session on today's storage opens without the key");
    assert_eq!(module.slot.take_refused(), vec![module.held.clone()]);
}

/// The key loading after the module started serves the held session in the same module, with
/// no restart, and the refusals made before it are handed back, so the module can open those
/// sessions and run the recovery their refused opens put off.
#[tokio::test]
async fn the_key_loading_later_serves_a_held_session_without_a_restart() {
    let module = module("later", None).await;
    assert!(module.registry.actor_for(&module.held).await.is_err());
    module.load_key().await;
    assert!(module.slot.key_loaded());
    module
        .registry
        .actor_for(&module.held)
        .await
        .expect("the held session is served once the key has loaded");
    assert_eq!(module.slot.take_refused(), vec![module.held.clone()]);
    assert!(module.slot.take_refused().is_empty());
}

/// A key the slot doesn't name is not installed: the held session stays refused.
#[tokio::test]
async fn a_key_with_another_id_is_not_installed() {
    let module = module("other-id", None).await;
    assert!(module
        .slot
        .load(&module.setup, DeploymentKey::new(2, &KEY))
        .await
        .is_err());
    assert!(!module.slot.key_loaded());
    assert_key_unavailable(module.registry.actor_for(&module.held).await.err().unwrap());
}

/// The fact id today's runs are exported under: SHA-256 over the source instance, the bind
/// triple and the run, each length-prefixed, then the segment ordinal.
fn todays_fact_id(source: &str, session: &BindIdentity, run_id: &str, ordinal: u64) -> String {
    let mut input = Vec::new();
    for value in [
        source.to_string(),
        session.project_root.to_string_lossy().into_owned(),
        session.harness.clone(),
        session.session.clone(),
        run_id.to_string(),
    ] {
        input.extend_from_slice(&(value.len() as u64).to_le_bytes());
        input.extend_from_slice(value.as_bytes());
    }
    input.extend_from_slice(&ordinal.to_le_bytes());
    sha2::Sha256::digest(input)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// A run that ends on a held session, served once the key has loaded, writes exactly one spend
/// fact to the usage export it was wired with, under today's fact id rule.
#[tokio::test]
async fn a_run_ending_on_a_held_session_writes_one_spend_fact_under_todays_fact_id() {
    let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
    let billing = StoreBilling {
        index: Arc::clone(&export) as Arc<dyn broca_core::ExportIndex>,
        resolver: Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 4)),
    };
    let module = module("billing", Some(billing)).await;
    module.load_key().await;
    let outcome = module
        .registry
        .route_submission_with_order(
            &module.held,
            RunId("r1".into()),
            request("hi"),
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
        .expect("the held session takes the send");
    assert!(matches!(outcome, SendOutcome::Active { .. }));
    let facts = broca_test_support::wait_until("the run's spend fact", || {
        let export = Arc::clone(&export);
        async move {
            let facts = export.read_export_page(None, 100).unwrap().facts;
            if facts.is_empty() {
                Err("no fact yet".to_string())
            } else {
                Ok(facts)
            }
        }
    })
    .await;
    assert_eq!(facts.len(), 1, "one fact for the one run");
    let fact = facts[0].segment.as_session().expect("a session's fact");
    assert_eq!(fact.run_id.0, "r1");
    assert_eq!(fact.session, module.held);
    assert_eq!(
        fact.fact_id,
        todays_fact_id(
            export.source_instance_id(),
            &module.held,
            "r1",
            fact.segment_ordinal
        )
    );
    // The actor settles the run billed once the insert committed; reopening it bills nothing
    // more, so the run still has one fact.
    let runtime = module.slot.runtime().unwrap();
    broca_test_support::wait_until("the run settled billed", || {
        let runtime = Arc::clone(&runtime);
        let held = module.held.clone();
        async move {
            let view = runtime
                .current_view(&held)
                .await
                .map_err(|error| format!("{error}"))?
                .ok_or("no view")?;
            if view.operations.billing_owed.is_empty() {
                Ok(())
            } else {
                Err("billing still owed".to_string())
            }
        }
    })
    .await;
    assert_eq!(export.read_export_page(None, 100).unwrap().facts.len(), 1);
}

/// How long the new storage's check of a session costs on a state root that keeps no session
/// there, the case of every open on today's storage: one `symlink_metadata` of a head that
/// doesn't exist. A measurement, so it runs only when asked: `--ignored --nocapture`.
#[test]
#[ignore = "a measurement; run with --ignored --nocapture"]
fn holds_cost_on_a_root_without_the_new_storage() {
    let dir = broca_test_support::scratch_root()
        .join(format!("store-key-holds-cost-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let slot = StoreSlot::awaiting_key(&dir, 1);
    let sessions: Vec<_> = (0..1_000)
        .map(|n| BindIdentity::new(dir.join("project"), "runner", format!("s{n}")))
        .collect();
    const ROUNDS: usize = 100;
    let started = std::time::Instant::now();
    for _ in 0..ROUNDS {
        for session in &sessions {
            assert!(slot.holding(session).unwrap().is_none());
        }
    }
    let per_check = started.elapsed() / (ROUNDS * sessions.len()) as u32;
    println!(
        "StoreSlot::holding on a root without the new storage: {per_check:?} per check \
         over {} checks",
        ROUNDS * sessions.len()
    );
    std::fs::remove_dir_all(&dir).unwrap();
}
