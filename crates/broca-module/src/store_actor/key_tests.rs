//! A session kept in the new storage while the deployment key hasn't loaded: refused
//! `key_unavailable`, never served from today's storage, and served once the key loads, without
//! a restart. A run that ends on such a session writes its spend facts to the usage export.

use std::sync::Arc;
use std::time::Duration;

use broca_core::{FileLeaseStore, FileWalStore};
use broca_protocol::{BindIdentity, RunId};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
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
    module_with(name, billing, Cipher::XChaCha20Poly1305).await
}

/// [`module`] whose new storage creates sessions in mode `new_sessions` and takes every new
/// session a creating send opens.
async fn module_with(name: &str, billing: Option<StoreBilling>, new_sessions: Cipher) -> Module {
    module_placing(
        name,
        billing,
        new_sessions,
        LineageRegistry::with_store_new_sessions,
    )
    .await
}

/// [`module_with`], with `place` choosing which new sessions the registry starts in the new
/// storage.
async fn module_placing(
    name: &str,
    billing: Option<StoreBilling>,
    new_sessions: Cipher,
    place: impl FnOnce(LineageRegistry) -> LineageRegistry,
) -> Module {
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
    let slot = Arc::new(StoreSlot::awaiting_key(&dir, 1).creating(new_sessions));
    let registry = place(LineageRegistry::new_with_reader(
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
    ));
    let setup = StoreRuntimeSetup {
        state_root: dir.clone(),
        executor,
        idle_ttl: OPEN,
        import_roots: None,
        billing,
        run_events: None,
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

impl Module {
    /// Creates the session `name` beside the held one, in mode `cipher`, as a module creating
    /// sessions in that mode would have.
    async fn create(&self, name: &str, cipher: Cipher) -> BindIdentity {
        let session = BindIdentity::new(self.dir.join("project"), "store-actor", name);
        let deployment = DeploymentKey::new(1, &KEY).creating(cipher);
        Session::create(
            LocalBackend::new(self.dir.join(broca_wal::SESSION_STORAGE_DIR)),
            SessionAddr(broca_core::session_addr(&session)),
            triple(&session),
            &deployment,
            false,
        )
        .await
        .unwrap()
        .close()
        .await
        .unwrap();
        session
    }

    /// The mode the stored head of `session` names.
    fn stored_mode(&self, session: &BindIdentity) -> Cipher {
        let (stored, _) = LocalBackend::new(self.dir.join(broca_wal::SESSION_STORAGE_DIR))
            .read_stored_head(&SessionAddr(broca_core::session_addr(session)))
            .unwrap()
            .unwrap();
        broca_session_store::head::stored_cipher(&stored).unwrap()
    }

    /// Rewrites the head of `session` to name mode `cipher` (`None` names none, which reads as
    /// encrypted), keeping the rest of its bytes: the change no writer makes.
    async fn flip_mode(&self, session: &BindIdentity, cipher: Option<Cipher>) {
        use broca_session_store::backend::SessionBackend;
        use broca_session_store::codec::Message;
        let backend = LocalBackend::new(self.dir.join(broca_wal::SESSION_STORAGE_DIR));
        let addr = SessionAddr(broca_core::session_addr(session));
        let (stored, version) = backend.read_stored_head(&addr).unwrap().unwrap();
        // The head's body follows its 40-byte tag or check.
        let (prefix, body) = stored.split_at(40);
        let mut head = broca_session_store::head::Head::decode(body).unwrap();
        head.cipher = cipher;
        let mut flipped = prefix.to_vec();
        flipped.extend(head.encode());
        backend
            .write_head(&addr, &flipped, Some(version), version + 1)
            .await
            .unwrap();
    }
}

/// Whether `error` is the open refusing a head as damage: it failed its check in the mode it
/// names.
fn refused_as_damage(error: &OpenError) -> bool {
    matches!(error, OpenError::Open(reason) if reason.contains("Head(Authentication)"))
}

/// A plain session is served while the deployment key hasn't loaded, by the runtime the module
/// opened at its start without the key, and an encrypted session at the same root is refused
/// `key_unavailable` until the key loads.
#[tokio::test]
async fn a_plain_session_is_served_before_the_key_loads_and_an_encrypted_one_is_refused() {
    let module = module("plain", None).await;
    let plain = module.create("plain", Cipher::None).await;
    module
        .slot
        .open_without_key(&module.setup)
        .await
        .expect("the runtime opens without the key");
    assert!(!module.slot.key_loaded());
    module
        .registry
        .actor_for(&plain)
        .await
        .expect("the plain session is served without the key");
    assert!(module.registry.store_holding(&plain).unwrap().is_some());
    assert_key_unavailable(
        module
            .registry
            .actor_for(&module.held)
            .await
            .err()
            .expect("the encrypted session is refused"),
    );
    assert_eq!(module.slot.take_refused(), vec![module.held.clone()]);
    module.load_key().await;
    module
        .registry
        .actor_for(&module.held)
        .await
        .expect("the encrypted session is served once the key has loaded");
}

/// A head whose mode was flipped is never served. An encrypted head that names plain mode is
/// routed as plain before the key loads, and its open refuses it as damage: its bytes fail the
/// plain check. A plain head that names no mode reads as encrypted: refused `key_unavailable`
/// before the key loads, and as damage after, since it wraps no key.
#[tokio::test]
async fn a_flipped_head_mode_is_refused_as_damage_both_ways() {
    let module = module("flipped", None).await;
    let plain = module.create("was-plain", Cipher::None).await;
    module.flip_mode(&module.held, Some(Cipher::None)).await;
    module.flip_mode(&plain, None).await;
    assert_eq!(module.stored_mode(&module.held), Cipher::None);
    assert_eq!(module.stored_mode(&plain), Cipher::XChaCha20Poly1305);
    module.slot.open_without_key(&module.setup).await.unwrap();

    let error = module.registry.actor_for(&module.held).await.err().unwrap();
    assert!(refused_as_damage(&error), "{error:?}");
    assert_key_unavailable(module.registry.actor_for(&plain).await.err().unwrap());

    module.load_key().await;
    for session in [&module.held, &plain] {
        let error = module.registry.actor_for(session).await.err().unwrap();
        assert!(refused_as_damage(&error), "{}: {error:?}", session.session);
    }
}

/// A head's creating send while the new storage can't create its session, because it creates
/// encrypted sessions and the deployment key hasn't loaded, is refused `key_unavailable`,
/// transient, and never started on today's storage: nothing of it is created in either storage.
/// Once the key loads, the same send starts the head in the new storage's durable root.
#[tokio::test]
async fn a_head_is_refused_transient_until_the_new_storage_can_create_it() {
    let module = module("head", None).await;
    module.slot.open_without_key(&module.setup).await.unwrap();
    let head = BindIdentity::new(module.dir.join("project"), "store-actor", "new-head");
    let send = || {
        module.registry.route_submission_with_class(
            &head,
            RunId("r1".into()),
            request("first"),
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            None,
            Some(crate::registry::HEAD_SESSION_CLASS.into()),
            false,
        )
    };
    assert_key_unavailable(send().await.expect_err("the head is refused"));
    let address = broca_core::session_addr(&head);
    assert_eq!(
        broca_wal::session_storage_class(&module.dir, &address).unwrap(),
        None
    );
    let wal = FileWalStore::new(module.dir.join("wal"));
    for class in broca_wal::StorageClass::ALL {
        assert!(!wal.class_wal_path(class, &head).exists(), "{class:?}");
    }
    assert_eq!(module.registry.slot_state(&head), None);

    module.load_key().await;
    let outcome = send().await.expect("the head opens once the key loads");
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    assert_eq!(
        broca_wal::session_storage_class(&module.dir, &address).unwrap(),
        Some(broca_wal::StorageClass::Durable)
    );
    for class in broca_wal::StorageClass::ALL {
        assert!(!wal.class_wal_path(class, &head).exists(), "{class:?}");
    }
}

/// A worker or sidekick is held to the same rule as a head: a creating send naming a class that
/// starts in the new storage, sent before the deployment key loads (the first seconds after a
/// start), is refused `key_unavailable`, transient, and leaves nothing in either storage. Before,
/// it started on today's storage, where it stayed, and today's storage refuses plan members it
/// doesn't serve, such as `readers` (the apps a plan lets read the session). Once the key loads,
/// the same send starts it in the new storage.
#[tokio::test]
async fn a_listed_class_is_refused_transient_until_the_new_storage_can_create_it() {
    let module = module_placing(
        "sidekick-early",
        None,
        Cipher::XChaCha20Poly1305,
        |registry| registry.with_store_new_session_classes(["sidekick"]),
    )
    .await;
    module.slot.open_without_key(&module.setup).await.unwrap();
    let session = BindIdentity::new(module.dir.join("project"), "store-actor", "early-sidekick");
    let send = || {
        module.registry.route_submission_with_class(
            &session,
            RunId("r1".into()),
            request("first"),
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            None,
            Some("sidekick".into()),
            false,
        )
    };
    assert_key_unavailable(send().await.expect_err("the sidekick is refused"));
    let address = broca_core::session_addr(&session);
    assert_eq!(
        broca_wal::session_storage_class(&module.dir, &address).unwrap(),
        None
    );
    let wal = FileWalStore::new(module.dir.join("wal"));
    for class in broca_wal::StorageClass::ALL {
        assert!(!wal.class_wal_path(class, &session).exists(), "{class:?}");
    }

    module.load_key().await;
    let outcome = send().await.expect("the sidekick opens once the key loads");
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    assert!(broca_wal::session_storage_class(&module.dir, &address)
        .unwrap()
        .is_some());
    for class in broca_wal::StorageClass::ALL {
        assert!(!wal.class_wal_path(class, &session).exists(), "{class:?}");
    }
}

/// A plan naming `readers` (the apps it lets read the session), as every worker plan core sends
/// does, takes the same retryable refusal while the key loads. Its check runs before the session
/// opens, and it used to read "can't create in the new storage now" as "will stay on today's
/// storage", refusing the launch for good with `invalid_params` (`plan.readers is not supported
/// for a session on today's storage`).
#[tokio::test]
async fn a_worker_plan_with_readers_is_refused_transient_until_the_key_loads() {
    readers_plan_waits_for_the_key("mason-early", false).await;
}

/// The same, when whether the session has a log on today's storage can't be read: every new
/// session starts in the new storage, so not knowing is no reason to refuse its readers for good.
/// A worker launched 21 s after a restart (15 Nov 22:14:52Z) got the permanent refusal this way.
#[tokio::test]
async fn a_readers_plan_waits_for_the_key_when_todays_logs_cant_be_read() {
    readers_plan_waits_for_the_key("mason-unreadable", true).await;
}

async fn readers_plan_waits_for_the_key(name: &str, unreadable_wal: bool) {
    let module = module_placing(name, None, Cipher::XChaCha20Poly1305, |registry| {
        registry.with_store_new_session_classes(["mason"])
    })
    .await;
    module.slot.open_without_key(&module.setup).await.unwrap();
    let session = BindIdentity::new(module.dir.join("project"), "store-actor", "early-mason");
    if unreadable_wal {
        // A file where today's log folder should be: every lookup there fails.
        let _ = std::fs::remove_dir_all(module.dir.join("wal"));
        std::fs::write(module.dir.join("wal"), b"not a folder").unwrap();
        assert_eq!(module.registry.wal_written(&session), None);
    }
    let composition = serde_json::json!({
        "providers": [{"provider": "fixture", "tools": []}],
        "step_transform_items": [],
    });
    let plan = serde_json::json!({
        "tool_items": [],
        "system_text_items": [{"provider": "fixture", "params": {"text": "you work"}}],
        "step_transform_items": [],
        "composition_digest":
            cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap(),
        "composition": composition,
        "readers": ["occipital"],
    });
    let manifest = Arc::new(broca_wal::PlanManifest {
        rich_ui: None,
        plan: broca_wal::OpaqueJson::from_json_text(plan.to_string()).unwrap(),
        plan_identity: "plan-1".into(),
        composition_digest: plan["composition_digest"].as_str().unwrap().into(),
        fetches: vec![],
        tools: Arc::new(vec![]),
        system_digest: "you work".into(),
        system_text: "you work".into(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: vec![],
    });
    let send = || {
        let mut request = request("first");
        request.principal = Some(broca_protocol::BindPrincipal::Reserved {
            module_id: "prefrontal-core".into(),
        });
        request.tools = None;
        request.policy_allowed = Default::default();
        module.registry.route_submission_with_class(
            &session,
            RunId("r1".into()),
            request,
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            Some(crate::actor::SendPlan {
                identity: "plan-1".into(),
                manifest: Some(manifest.clone()),
                model_check: None,
            }),
            None,
            None,
            None,
            Some("mason".into()),
            false,
        )
    };
    // The send's front check, which runs before its admission, refuses it the same way.
    let front = || {
        crate::serve::ServeHandler::plan_readers_supported(
            &module.registry,
            &session,
            Some("mason"),
        )
    };
    match front() {
        Err(crate::serve::ServeError::Open(
            reason,
            Some(broca_protocol::ErrorClass::Transient),
        )) => {
            assert!(reason.starts_with("key_unavailable:"), "{reason}")
        }
        other => panic!("expected the transient key_unavailable refusal, got {other:?}"),
    }
    assert_key_unavailable(send().await.expect_err("the mason is refused"));
    let address = broca_core::session_addr(&session);
    assert_eq!(
        broca_wal::session_storage_class(&module.dir, &address).unwrap(),
        None
    );

    if unreadable_wal {
        // The window passes: today's logs can be read again by the time the key loads.
        std::fs::remove_file(module.dir.join("wal")).unwrap();
    }
    module.load_key().await;
    assert!(
        matches!(front(), Ok(true)),
        "readers are supported once the key loads"
    );
    let outcome = send().await.expect("the mason opens once the key loads");
    assert!(matches!(outcome, SendOutcome::Active { .. }), "{outcome:?}");
    assert!(broca_wal::session_storage_class(&module.dir, &address)
        .unwrap()
        .is_some());
}

/// Production starts new sessions in the new storage one class at a time: a creating send naming
/// a listed class starts there, and one naming another class, or none, starts on today's storage.
#[tokio::test]
async fn only_listed_classes_start_new_sessions_in_the_new_storage() {
    let module = module_placing("classes", None, Cipher::XChaCha20Poly1305, |registry| {
        registry.with_store_new_session_classes(["sidekick"])
    })
    .await;
    module.load_key().await;
    let wal = FileWalStore::new(module.dir.join("wal"));
    for (name, class, in_store) in [
        ("a-sidekick", Some("sidekick"), true),
        ("a-mason", Some("mason"), false),
        ("no-class", None, false),
    ] {
        let session = BindIdentity::new(module.dir.join("project"), "store-actor", name);
        let outcome = module
            .registry
            .route_submission_with_class(
                &session,
                RunId(format!("r-{name}")),
                request("first"),
                RunKind::FreshOrResume,
                SendKey::Minted,
                false,
                broca_protocol::Delivery::Queue,
                None,
                None,
                None,
                None,
                class.map(str::to_owned),
                false,
            )
            .await
            .unwrap_or_else(|error| panic!("{name}: {error:?}"));
        // This module's executor runs only store sessions, so only that row's run is checked;
        // a session on today's storage is checked by where its log was written.
        if in_store {
            assert!(
                matches!(outcome, SendOutcome::Active { .. }),
                "{name}: {outcome:?}"
            );
        }
        let stored =
            broca_wal::session_storage_class(&module.dir, &broca_core::session_addr(&session))
                .unwrap();
        let has_wal = broca_wal::StorageClass::ALL
            .into_iter()
            .any(|storage| wal.class_wal_path(storage, &session).exists());
        assert_eq!(
            stored.is_some(),
            in_store,
            "{name}: kept in the new storage"
        );
        assert_eq!(has_wal, !in_store, "{name}: has a log on today's storage");
    }
}

/// With new sessions plain, a send that creates a session creates it plain, before the key has
/// loaded, and the encrypted session created before keeps its mode once served.
#[tokio::test]
async fn a_module_creating_plain_sessions_creates_the_next_one_plain() {
    let module = module_with("creates-plain", None, Cipher::None).await;
    module.slot.open_without_key(&module.setup).await.unwrap();
    let fresh = BindIdentity::new(module.dir.join("project"), "store-actor", "fresh");
    let outcome = module
        .registry
        .route_submission_with_order(
            &fresh,
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
        .expect("a plain session is created without the key");
    assert!(matches!(outcome, SendOutcome::Active { .. }));
    assert!(!module.slot.key_loaded());
    assert_eq!(module.stored_mode(&fresh), Cipher::None);
    assert!(
        !module.wal_file(&fresh).exists(),
        "the session was created in the new storage, not today's"
    );
    module.load_key().await;
    module.registry.actor_for(&module.held).await.unwrap();
    assert_eq!(module.stored_mode(&module.held), Cipher::XChaCha20Poly1305);
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
