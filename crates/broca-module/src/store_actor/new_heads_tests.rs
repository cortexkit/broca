//! A send creating an agent head starts it in the new storage, in the root backups capture,
//! whether or not the module creates other sessions there; every other send still starts its
//! session on today's storage. A head the new storage can't start is refused, before anything is
//! created in either storage. While a session in the new storage has no run, only the scope whose
//! stamped `route.bind` reserved its address may start it, refused as today's storage refuses
//! everyone else, and a session whose creating send was refused is deleted when its actor closes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_core::scope_admission::OwnerMismatch;
use broca_core::{FileLeaseStore, FileWalStore, ScopeRefusal, WalStore as _};
use broca_protocol::{BindIdentity, BindPrincipal, NormalizedEvent, RunId, SendScope};
use broca_store::session_address::ScopeReservation;
use broca_wal::StorageClass;
use serde_json::json;

use super::differential::{BothLoops, TodayLoop};
use super::tests::{executor, request, text_step, Gated, Inventory};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunKind};
use crate::registry::{LineageRegistry, OpenError, HEAD_SESSION_CLASS};

pub(super) const IDLE: Duration = Duration::from_millis(100);

/// A module over a scratch state root, configured as production configures one: its registry
/// creates no session in the new storage but a head. Its opener serves the new storage when
/// `store`, and serves today's storage alone otherwise.
pub(super) struct Module {
    dir: PathBuf,
    pub(super) registry: LineageRegistry,
    wal: Arc<FileWalStore>,
    inventory: Inventory,
    pub(super) key: BindIdentity,
}

impl Drop for Module {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub(super) async fn module(
    store: bool,
    steps: Vec<Vec<NormalizedEvent>>,
    reservation: Option<&ScopeReservation>,
) -> Module {
    use std::sync::atomic::{AtomicU64, Ordering};
    static N: AtomicU64 = AtomicU64::new(0);
    let dir = broca_test_support::scratch_root().join(format!(
        "store-new-heads-{}-{}",
        std::process::id(),
        N.fetch_add(1, Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let wal = Arc::new(FileWalStore::new(dir.join("wal")));
    let executor: Arc<dyn RunExecutor> = Arc::new(BothLoops {
        today: TodayLoop {
            provider: Gated {
                render_config: None,
                notes_sends: false,
                hold: None,
                inner: broca_core::provider::StubProvider::new(steps.clone()),
                gate: None,
            },
            wal: Arc::clone(&wal),
            planes: Default::default(),
        },
        store: executor(steps, None),
    });
    let mut opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        wal.clone(),
        Arc::clone(&executor),
    )
    .with_idle_ttl(IDLE);
    if store {
        let runtime = crate::store_open::StoreRuntime::open(
            &dir,
            broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
            executor,
            IDLE,
        )
        .await
        .unwrap();
        opener = opener.with_store_runtime(Arc::new(runtime));
    }
    let registry = LineageRegistry::new_with_reader(Arc::new(opener), wal.clone(), None);
    let inventory = Inventory::open(&dir, &dir);
    registry.set_session_address_inventory(Arc::clone(&inventory.store));
    let key = BindIdentity::new(
        dir.join("project"),
        "broca",
        "alfonso:head:agent_0f1e2d3c4b5a6978",
    );
    // A bind names the session first, recording its address: stamped with the scope
    // `reservation` names when it is given, which reserves the session for that scope.
    inventory.reserve(&key, reservation);
    Module {
        dir,
        registry,
        wal,
        inventory,
        key,
    }
}

/// A `session.send` with no delivery list and no conversation key, routed as the serve layer
/// routes one, with the label `class`, the retention policy `retention` and the ordered delivery
/// `order` when given.
pub(super) async fn send(
    module: &Module,
    run_id: &str,
    request: broca_protocol::SubmissionRequest,
    class: Option<&str>,
    retention: Option<u64>,
    order: Option<serde_json::Value>,
) -> Result<SendOutcome, OpenError> {
    module
        .registry
        .route_submission_with_order(
            &module.key,
            RunId(run_id.into()),
            request,
            RunKind::FreshOrResume,
            SendKey::Minted,
            false,
            broca_protocol::Delivery::Queue,
            None,
            None,
            None,
            retention,
            class.map(str::to_owned),
            false,
            order.map(|order| serde_json::from_value(order).expect("an order decodes")),
        )
        .await
}

impl Module {
    /// Waits until the session's actor has closed and left the registry.
    pub(super) async fn closed(&self) {
        for _ in 0..1000 {
            if self.registry.slot_state(&self.key).is_none() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        panic!("the session's actor never closed");
    }

    /// The class of the new storage's root holding the session's head, if one does.
    fn store_class(&self) -> Option<StorageClass> {
        broca_wal::session_storage_class(&self.dir, &broca_core::session_addr(&self.key)).unwrap()
    }

    /// Whether any of the new storage's roots holds a head, a log or an object of the session.
    fn store_leftovers(&self) -> Vec<PathBuf> {
        let addr = broca_core::session_addr(&self.key);
        StorageClass::ALL
            .into_iter()
            .flat_map(|class| {
                let root = class.store_root(&self.dir);
                ["heads", "logs", "objects"].map(|folder| root.join(folder).join(&addr))
            })
            .filter(|path| path.exists())
            .collect()
    }

    /// The class of today's folder holding the session's WAL, if one does.
    fn wal_class(&self) -> Option<StorageClass> {
        StorageClass::ALL
            .into_iter()
            .find(|class| self.wal.class_wal_path(*class, &self.key).is_file())
    }

    fn floor(&self) -> String {
        std::fs::read_to_string(self.dir.join(broca_wal::FEATURE_FLOOR_FILENAME))
            .unwrap_or_default()
    }
}

fn core() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    }
}

fn other() -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: "magic-context".into(),
    }
}

/// A route stamped with `owner`'s scope `scope_ref` at `epoch`, as core stamps its head's.
fn stamped(owner: BindPrincipal, scope_ref: &str, epoch: u64) -> SendScope {
    SendScope::Stamped(broca_protocol::BindScope {
        scope: broca_protocol::SessionScope {
            owner,
            agent_id_frozen: false,
            scope_ref: scope_ref.into(),
            scope_epoch: epoch,
            flow_id: None,
            sender: None,
            agent_id: None,
            run_id: None,
        },
        owner_authorized: true,
        kind: broca_protocol::ScopeKind::Head,
        parent: None,
        parent_state: None,
    })
}

/// A send from `principal` over a route scoped `scope`, naming no tools of its own.
fn sent_by(
    text: &str,
    principal: BindPrincipal,
    scope: SendScope,
) -> broca_protocol::SubmissionRequest {
    let mut sent = request(text);
    sent.tools = None;
    sent.policy_allowed = Default::default();
    sent.principal = Some(principal);
    sent.scope = scope;
    sent
}

/// A head's creating send starts the session in the new storage's durable root, with no WAL and
/// the feature floor raised, though the module creates no other session there. Its actor closes
/// when idle, and the head's next send reopens it from that root.
#[tokio::test]
async fn a_head_starts_in_the_durable_root_of_the_new_storage_and_reopens_from_it() {
    let module = module(
        true,
        vec![text_step("a1", "hello"), text_step("a2", "again")],
        None,
    )
    .await;
    let first = send(
        &module,
        "r1",
        request("first"),
        Some(HEAD_SESSION_CLASS),
        None,
        None,
    )
    .await
    .unwrap();
    assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
    module.closed().await;
    assert_eq!(module.store_class(), Some(StorageClass::Durable));
    assert_eq!(module.wal.wal_present(&module.key), Some(false));
    assert!(module.floor().contains(broca_wal::SESSION_STORAGE_FEATURE));
    let mut later = request("second");
    later.tools = None;
    let second = send(&module, "r2", later, Some(HEAD_SESSION_CLASS), None, None)
        .await
        .unwrap();
    assert!(matches!(second, SendOutcome::Active { .. }), "{second:?}");
    module.closed().await;
    assert_eq!(module.store_class(), Some(StorageClass::Durable));
    assert_eq!(module.wal.wal_present(&module.key), Some(false));
}

/// A creating send without the head label and one with another label still start their sessions
/// on today's storage, and the new storage holds nothing of them. So does one setting retention:
/// it reaches today's open, which refuses it only because this module keeps no retention clock.
#[tokio::test]
async fn every_other_creating_send_still_starts_on_todays_storage() {
    for (label, class) in [("unlabelled", None), ("worker", Some("worker"))] {
        let module = module(true, vec![text_step("a1", "hello")], None).await;
        let first = send(&module, "r1", request("first"), class, None, None)
            .await
            .unwrap();
        assert!(
            matches!(first, SendOutcome::Active { .. }),
            "{label}: {first:?}"
        );
        module.closed().await;
        assert_eq!(module.wal_class(), Some(StorageClass::Ordinary), "{label}");
        assert_eq!(module.store_class(), None, "{label}");
        assert!(module.store_leftovers().is_empty(), "{label}");
    }
    let module = module(true, vec![text_step("a1", "hello")], None).await;
    let retained = send(&module, "r1", request("first"), None, Some(3600), None).await;
    assert!(
        matches!(&retained, Err(OpenError::Open(reason)) if reason == "retention service is not configured"),
        "{retained:?}"
    );
    assert!(module.store_leftovers().is_empty());
}

/// A head whose creating send carries media is refused `not_served_on_new_storage`, permanently,
/// naming that feature.
/// Nothing is created in either storage: no head, log or object in any of the new storage's roots,
/// no WAL, no feature floor, and no actor.
#[tokio::test]
async fn a_head_the_new_storage_does_not_serve_is_refused_and_creates_nothing() {
    let mut media = request("look");
    media.content =
        serde_json::from_value(json!([{"type": "upload", "upload_id": "up-1"}])).unwrap();
    let cases = [("a send carrying media", media, None, None)];
    for (feature, sent, retention, order) in cases {
        let module = module(true, Vec::new(), None).await;
        let refused = send(
            &module,
            "r1",
            sent,
            Some(HEAD_SESSION_CLASS),
            retention,
            order,
        )
        .await;
        let error = match refused {
            Err(error @ OpenError::NotServedOnNewStorage { .. }) => error,
            other => panic!("{feature}: expected the head refused, got {other:?}"),
        };
        assert!(
            matches!(error, OpenError::NotServedOnNewStorage { feature: named } if named == feature),
            "{error:?}"
        );
        let body = crate::serve::admission_refusal::admission_refusal(
            &crate::serve::ServeError::from(error),
        )
        .expect("the refusal has a wire body");
        let body = serde_json::to_value(body).unwrap();
        assert_eq!(body["code"], "not_served_on_new_storage", "{body}");
        assert_eq!(
            body["detail"],
            json!({"class": "permanent", "feature": feature}),
            "{body}"
        );
        assert!(
            body["message"].as_str().unwrap().starts_with(feature),
            "{body}"
        );
        assert!(module.store_leftovers().is_empty(), "{feature}");
        assert_eq!(
            module.wal.wal_present(&module.key),
            Some(false),
            "{feature}"
        );
        assert!(!module.floor().contains(broca_wal::SESSION_STORAGE_FEATURE));
        assert_eq!(module.registry.slot_state(&module.key), None, "{feature}");
    }
}

/// [`send`] of a head's creating send, asking to be delivered as `delivery`, with the run kind
/// `kind` and the ordered delivery `order` when given.
async fn head_send(
    module: &Module,
    request: broca_protocol::SubmissionRequest,
    delivery: broca_protocol::Delivery,
    kind: RunKind,
    order: Option<serde_json::Value>,
) -> SendOutcome {
    module
        .registry
        .route_submission_with_order(
            &module.key,
            RunId("r1".into()),
            request,
            kind,
            SendKey::Minted,
            false,
            delivery,
            None,
            None,
            None,
            None,
            Some(HEAD_SESSION_CLASS.into()),
            false,
            order.map(|order| serde_json::from_value(order).expect("an order decodes")),
        )
        .await
        .expect("the head opens")
}

/// A head created by a send that the new storage served only on a session it already held (an
/// ordered delivery list, starting a run or waiting for one, and an interrupt) or not at all (a
/// send resuming after sign-in, which has no paused run to resume on a new session) starts in the
/// new storage, and is answered as today's storage answers the same send.
#[tokio::test]
async fn a_head_created_by_an_ordered_send_an_interrupt_or_a_sign_in_resume_answers_alike() {
    use broca_protocol::Delivery;
    let cases = [
        (
            "an ordered send starting a run",
            Delivery::Queue,
            RunKind::FreshOrResume,
            Some(json!({"modes": ["idle_start"]})),
        ),
        (
            "an ordered send waiting for a run",
            Delivery::Queue,
            RunKind::FreshOrResume,
            Some(json!({"modes": ["steer"]})),
        ),
        (
            "an interrupt",
            Delivery::Interrupt,
            RunKind::FreshOrResume,
            None,
        ),
        (
            "a send resuming after sign-in",
            Delivery::Queue,
            RunKind::ResumeAfterAuth,
            None,
        ),
    ];
    for (label, delivery, kind, order) in cases {
        let today = module(false, vec![text_step("a1", "hello")], None).await;
        let store = module(true, vec![text_step("a1", "hello")], None).await;
        let old = head_send(&today, request("first"), delivery, kind, order.clone()).await;
        let new = head_send(&store, request("first"), delivery, kind, order).await;
        assert!(
            matches!(
                old,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{label}: {old:?}"
        );
        assert_eq!(new, old, "{label}: today's storage answers alike");
        store.closed().await;
        assert_eq!(store.store_class(), Some(StorageClass::Durable), "{label}");
        assert_eq!(store.wal.wal_present(&store.key), Some(false), "{label}");
    }
}

/// A later `session.send` from another module to a head its reserving owner already started is
/// answered by the session's history, through the serve layer, exactly as the WAL storage
/// answers it, and never with the reservation's "nobody started this yet" refusal. A head in the
/// session store (`broca-session-store`) has no WAL however many runs it has, so the serve
/// layer's first-send precheck must not read the missing WAL as "no run yet".
#[tokio::test]
async fn a_started_head_answers_later_sends_from_its_history_not_its_reservation() {
    use crate::serve::{SendRequest, ServeError, ServeHandler};
    let reservation = ScopeReservation {
        owner: core(),
        scope_ref: "head-1".into(),
    };
    let steps = || vec![text_step("a1", "hello"), text_step("a2", "again")];
    let today = module(false, steps(), Some(&reservation)).await;
    let store = module(true, steps(), Some(&reservation)).await;
    let mut answers = Vec::new();
    for module in [&today, &store] {
        let sent = sent_by("owner", core(), stamped(core(), "head-1", 1));
        let first = send(module, "r1", sent, Some(HEAD_SESSION_CLASS), None, None)
            .await
            .unwrap();
        assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
        module.closed().await;
        let later = SendRequest {
            session_class: None,
            retention: None,
            conversation_key: None,
            session: module.key.clone(),
            run_id: RunId("r2".into()),
            send_id: None,
            title: None,
            request: Box::new(sent_by("from the app", other(), SendScope::Unstamped)),
            skip_materialization: false,
            kind: RunKind::FreshOrResume,
            delivery: broca_protocol::Delivery::Queue,
            stop_sequences_named: false,
            plan: None,
            order: None,
        };
        answers.push(match ServeHandler::send(&module.registry, later).await {
            Ok(_) => "admitted".to_owned(),
            Err(ServeError::ScopeRefused(ScopeRefusal::OwnerMismatch(mismatch))) => {
                assert!(
                    !matches!(&*mismatch, OwnerMismatch::Reserved { .. }),
                    "a started head was refused as if no one had started it: {mismatch:?}"
                );
                format!("{mismatch:?}")
            }
            Err(other) => format!("{other:?}"),
        });
        module.closed().await;
    }
    assert_eq!(answers[1], answers[0], "today's storage answers alike");
    assert_eq!(store.store_class(), Some(StorageClass::Durable));
    assert_eq!(store.wal.wal_present(&store.key), Some(false));
}

/// The refusal a reserved session gives a send from any scope but the one that reserved it.
fn assert_reserved_for_core(outcome: &SendOutcome, label: &str) {
    match outcome {
        SendOutcome::ScopeRefused(ScopeRefusal::OwnerMismatch(mismatch)) => assert!(
            matches!(&**mismatch, OwnerMismatch::Reserved { owner, .. } if *owner == core()),
            "{label}: {mismatch:?}"
        ),
        other => panic!("{label}: expected the reservation's refusal, got {other:?}"),
    }
}

/// A head whose address core's stamped bind reserved is core's before any run: an unstamped
/// first send, one stamped with another owner's scope, and one stamped with another of core's
/// scopes are each refused as today's storage refuses them, with the same outcome. Each refused
/// send created the session to admit it, and the session is gone once its actor closes. Core's
/// first send, under the reserved scope at a later epoch, is admitted and freezes that epoch.
#[tokio::test]
async fn only_the_reserving_scope_starts_a_reserved_head_as_on_todays_storage() {
    let steps = || vec![text_step("a1", "hello")];
    let reservation = ScopeReservation {
        owner: core(),
        scope_ref: "head-1".into(),
    };
    let today = module(false, steps(), Some(&reservation)).await;
    let store = module(true, steps(), Some(&reservation)).await;
    for (label, principal, scope) in [
        ("unstamped", other(), SendScope::Unstamped),
        ("another owner", other(), stamped(other(), "head-1", 1)),
        (
            "another of core's scopes",
            core(),
            stamped(core(), "head-2", 1),
        ),
    ] {
        let mut outcomes = Vec::new();
        for module in [&today, &store] {
            let sent = sent_by("mine", principal.clone(), scope.clone());
            outcomes.push(
                send(module, "r1", sent, Some(HEAD_SESSION_CLASS), None, None)
                    .await
                    .unwrap(),
            );
        }
        assert_reserved_for_core(&outcomes[1], label);
        assert_eq!(
            outcomes[1], outcomes[0],
            "{label}: today's storage answers alike"
        );
        store.closed().await;
        assert!(
            store.store_leftovers().is_empty(),
            "{label}: the refused send left {:?}",
            store.store_leftovers()
        );
        assert_eq!(store.wal.wal_present(&store.key), Some(false), "{label}");
    }
    for module in [&today, &store] {
        let sent = sent_by("owner", core(), stamped(core(), "head-1", 5));
        let first = send(module, "r1", sent, Some(HEAD_SESSION_CLASS), None, None)
            .await
            .unwrap();
        assert!(matches!(first, SendOutcome::Active { .. }), "{first:?}");
        module.closed().await;
        let frozen = module
            .registry
            .session_ownership_for_bind(&module.key)
            .await
            .expect("the session's owner reads")
            .scope
            .expect("the first send froze its scope");
        assert_eq!(
            (frozen.owner, frozen.scope_ref.as_str(), frozen.scope_epoch),
            (core(), "head-1", 5)
        );
    }
    assert_eq!(store.store_class(), Some(StorageClass::Durable));
}

/// A reservation whose row no longer decodes refuses a head's first send as transient, never as
/// unreserved, on the new storage as on today's, and the session that send created is gone once
/// its actor closes.
#[tokio::test]
async fn an_undecodable_reservation_refuses_a_head_send_as_transient() {
    let reservation = ScopeReservation {
        owner: core(),
        scope_ref: "head-1".into(),
    };
    let today = module(false, vec![text_step("a1", "hello")], Some(&reservation)).await;
    let store = module(true, vec![text_step("a1", "hello")], Some(&reservation)).await;
    let mut outcomes = Vec::new();
    for module in [&today, &store] {
        module.inventory.damage(&module.key);
        let sent = sent_by("mine", other(), SendScope::Unstamped);
        outcomes.push(
            send(module, "r1", sent, Some(HEAD_SESSION_CLASS), None, None)
                .await
                .unwrap(),
        );
    }
    match &outcomes[1] {
        SendOutcome::Transient(reason) => assert!(
            reason.starts_with("cannot read the session's scope reservation:"),
            "{reason}"
        ),
        other => panic!("expected a transient refusal, got {other:?}"),
    }
    assert_eq!(outcomes[1], outcomes[0], "today's storage answers alike");
    store.closed().await;
    assert!(store.store_leftovers().is_empty());
}
