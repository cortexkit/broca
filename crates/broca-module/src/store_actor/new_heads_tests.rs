//! A send creating an agent head starts it in the session store, in its durable class (the folder
//! backups copy). A head the session store can't start is refused, before anything is created.
//! While a session in the session store has no run, only the scope whose stamped `route.bind`
//! reserved its address may start it, and a session whose creating send was refused is deleted
//! when its actor closes.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_core::scope_admission::OwnerMismatch;
use broca_core::{FileLeaseStore, ScopeRefusal};
use broca_protocol::{BindIdentity, BindPrincipal, NormalizedEvent, RunId, SendScope};
use broca_records::session_storage::StorageClass;
use broca_store::session_address::ScopeReservation;
use serde_json::json;

use super::tests::{executor, request, text_step, Inventory};
use crate::actor::{SendKey, SendOutcome};
use crate::executor::{RunExecutor, RunKind};
use crate::registry::{LineageRegistry, OpenError, HEAD_SESSION_CLASS};

pub(super) const IDLE: Duration = Duration::from_millis(100);

/// A module over a scratch state root whose registry creates no session in the session store but a
/// head. Its opener serves the session store when `store`.
pub(super) struct Module {
    dir: PathBuf,
    pub(super) registry: LineageRegistry,
    inventory: Inventory,
    pub(super) key: BindIdentity,
}

impl Drop for Module {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

pub(super) async fn module(
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
    let executor: Arc<dyn RunExecutor> = Arc::new(executor(steps, None));
    let runtime = crate::store_open::StoreRuntime::open(
        &dir,
        broca_session_store::keys::DeploymentKey::new(1, &[7; 32]),
        Arc::clone(&executor),
        IDLE,
    )
    .await
    .unwrap();
    let opener = crate::opener::DefaultOpener::new(
        Arc::new(FileLeaseStore::new(dir.join("leases"))),
        executor,
        Arc::new(crate::store_open::StoreSlot::loaded(Arc::new(runtime))),
    )
    .with_idle_ttl(IDLE);
    let registry = LineageRegistry::new(Arc::new(opener));
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

    /// The class of the session store's root holding the session's head, if one does.
    fn store_class(&self) -> Option<StorageClass> {
        broca_records::session_storage::session_storage_class(
            &self.dir,
            &broca_core::session_addr(&self.key),
        )
        .unwrap()
    }

    /// Whether any of the session store's roots holds a head, a log or an object of the session.
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

    fn floor(&self) -> String {
        std::fs::read_to_string(self.dir.join(broca_records::floor::FEATURE_FLOOR_FILENAME))
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

/// A head's creating send starts the session in the session store's durable root, with no WAL and
/// the feature floor raised, though the module creates no other session there. Its actor closes
/// when idle, and the head's next send reopens it from that root.
#[tokio::test]
async fn a_head_starts_in_the_durable_root_of_the_new_storage_and_reopens_from_it() {
    let module = module(
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
    assert!(module.floor().contains(broca_wal::SESSION_STORAGE_FEATURE));
    let mut later = request("second");
    later.tools = None;
    let second = send(&module, "r2", later, Some(HEAD_SESSION_CLASS), None, None)
        .await
        .unwrap();
    assert!(matches!(second, SendOutcome::Active { .. }), "{second:?}");
    module.closed().await;
    assert_eq!(module.store_class(), Some(StorageClass::Durable));
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
        let store = module(vec![text_step("a1", "hello")], None).await;
        let new = head_send(&store, request("first"), delivery, kind, order).await;
        assert!(
            matches!(
                new,
                SendOutcome::Active { .. } | SendOutcome::Pending { .. }
            ),
            "{label}: {new:?}"
        );
        broca_test_support::pin!(label, new);
        store.closed().await;
        assert_eq!(store.store_class(), Some(StorageClass::Durable), "{label}");
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
    let store = module(steps(), Some(&reservation)).await;
    let mut answers = Vec::new();
    for module in [&store] {
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
    broca_test_support::pin!("the later send's answer", answers[0]);
    assert_eq!(store.store_class(), Some(StorageClass::Durable));
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
/// scopes are each refused, each answer pinned. Each refused
/// send created the session to admit it, and the session is gone once its actor closes. Core's
/// first send, under the reserved scope at a later epoch, is admitted and freezes that epoch.
#[tokio::test]
async fn only_the_reserving_scope_starts_a_reserved_head_as_on_todays_storage() {
    let steps = || vec![text_step("a1", "hello")];
    let reservation = ScopeReservation {
        owner: core(),
        scope_ref: "head-1".into(),
    };
    let store = module(steps(), Some(&reservation)).await;
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
        for module in [&store] {
            let sent = sent_by("mine", principal.clone(), scope.clone());
            outcomes.push(
                send(module, "r1", sent, Some(HEAD_SESSION_CLASS), None, None)
                    .await
                    .unwrap(),
            );
        }
        assert_reserved_for_core(&outcomes[0], label);
        broca_test_support::pin!(label, outcomes[0]);
        store.closed().await;
        assert!(
            store.store_leftovers().is_empty(),
            "{label}: the refused send left {:?}",
            store.store_leftovers()
        );
    }
    for module in [&store] {
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
/// unreserved, and the session that send created is gone once its actor closes.
#[tokio::test]
async fn an_undecodable_reservation_refuses_a_head_send_as_transient() {
    let reservation = ScopeReservation {
        owner: core(),
        scope_ref: "head-1".into(),
    };
    let store = module(vec![text_step("a1", "hello")], Some(&reservation)).await;
    let mut outcomes = Vec::new();
    for module in [&store] {
        module.inventory.damage(&module.key);
        let sent = sent_by("mine", other(), SendScope::Unstamped);
        outcomes.push(
            send(module, "r1", sent, Some(HEAD_SESSION_CLASS), None, None)
                .await
                .unwrap(),
        );
    }
    match &outcomes[0] {
        SendOutcome::Transient(reason) => assert!(
            reason.starts_with("cannot read the session's scope reservation:"),
            "{reason}"
        ),
        other => panic!("expected a transient refusal, got {other:?}"),
    }
    broca_test_support::pin!("the refusal", outcomes[0]);
    store.closed().await;
    assert!(store.store_leftovers().is_empty());
}
