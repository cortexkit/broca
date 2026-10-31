use super::*;
use broca_core::SessionRefIndex;
use broca_module::registry::{ActorHandle, LineageKey, LineageOpener, OpenError, RegistryHandle};
use broca_module::LineageRegistry;
use broca_protocol::BindIdentity;
use rusqlite::{Connection, TransactionBehavior};
use std::sync::atomic::{AtomicUsize, Ordering};

struct NeverOpen;

#[async_trait]
impl LineageOpener for NeverOpen {
    async fn open(
        &self,
        _: &LineageKey,
        _: u64,
        _: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        panic!("address resolution must not open an actor")
    }
}

struct Fixture {
    root: PathBuf,
    index: Arc<broca_store::SqliteStore>,
    registry: LineageRegistry,
    inventory: SessionInventory,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "inventory-readiness-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst),
        ));
        std::fs::create_dir_all(&root).unwrap();
        let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
        let registry =
            LineageRegistry::new_with_sources(Arc::new(NeverOpen), None, Some(index.clone()));
        registry.set_session_address_inventory(index.clone());
        let inventory = SessionInventory::new(root.clone(), index.clone());
        registry.set_session_inventory_readiness(Arc::new(inventory.clone()));
        Self {
            root,
            index,
            registry,
            inventory,
        }
    }

    fn bind(&self, name: &str) -> BindIdentity {
        BindIdentity::new(self.root.join("project"), "runner", name)
    }

    fn resolve(
        &self,
        name: &str,
    ) -> tokio::task::JoinHandle<Result<BindIdentity, SessionAddressError>> {
        let bind = self.bind(name);
        let registry = self.registry.clone();
        tokio::spawn(async move { registry.resolve_session_address(&bind, None).await })
    }
}

async fn waiting(
    observer: &mut watch::Receiver<usize>,
    count: usize,
    bind: &mut tokio::task::JoinHandle<Result<BindIdentity, SessionAddressError>>,
) {
    while *observer.borrow_and_update() < count {
        tokio::select! {
            result = &mut *bind => panic!("bind finished before inventory readiness: {result:?}"),
            changed = observer.changed() => changed.unwrap(),
        }
    }
}

#[tokio::test]
async fn held_migration_serves_known_pairs_and_signals_new_pairs_single_flight() {
    let fixture = Fixture::new();
    let known = fixture.bind("known");
    fixture
        .index
        .record_session(&broca_core::session_addr(&known), &known)
        .unwrap();
    let mut blocker = Connection::open(fixture.root.join("run-index.db")).unwrap();
    let held = blocker
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let mut observer = fixture.inventory.inner.waiting.subscribe();
    let mut first = fixture.resolve("new-first");
    waiting(&mut observer, 1, &mut first).await;
    assert_eq!(
        fixture
            .registry
            .resolve_session_address(&known, None)
            .await
            .unwrap(),
        known
    );
    let mut second = fixture.resolve("new-second");
    waiting(&mut observer, 2, &mut second).await;
    assert!(!first.is_finished());
    assert!(!second.is_finished());
    assert_eq!(fixture.inventory.inner.attempts.load(Ordering::SeqCst), 1);
    drop(held);
    assert_eq!(first.await.unwrap().unwrap(), fixture.bind("new-first"));
    assert_eq!(second.await.unwrap().unwrap(), fixture.bind("new-second"));
    assert!(
        fixture
            .index
            .session_refs(std::iter::empty())
            .unwrap()
            .complete
    );
}

#[tokio::test]
async fn failed_migration_refuses_new_pairs_and_can_be_retried() {
    let fixture = Fixture::new();
    std::fs::write(fixture.root.join("wal"), b"not a directory").unwrap();
    let error = fixture.resolve("failed").await.unwrap().unwrap_err();
    assert_eq!(error.code(), "session_address_unavailable");
    assert!(matches!(
        error,
        SessionAddressError::Unavailable(InventoryUnavailable::MigrationFailed(_))
    ));
    std::fs::remove_file(fixture.root.join("wal")).unwrap();
    assert_eq!(
        fixture.resolve("recovered").await.unwrap().unwrap(),
        fixture.bind("recovered")
    );
    assert_eq!(fixture.inventory.inner.attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn bounded_wait_expires_without_cancelling_shared_migration() {
    let fixture = Fixture::new();
    let mut blocker = Connection::open(fixture.root.join("run-index.db")).unwrap();
    let held = blocker
        .transaction_with_behavior(TransactionBehavior::Immediate)
        .unwrap();
    let mut observer = fixture.inventory.inner.waiting.subscribe();
    let mut bind = fixture.resolve("waited");
    waiting(&mut observer, 1, &mut bind).await;
    tokio::time::pause();
    // Tokio's timer wheel rounds deadlines to milliseconds. Cross the final
    // tick rather than depending on the sub-millisecond instant of pause().
    tokio::time::advance(INVENTORY_READINESS_WAIT + std::time::Duration::from_millis(1)).await;
    assert_eq!(
        bind.await.unwrap(),
        Err(SessionAddressError::Unavailable(
            InventoryUnavailable::WaitExpired
        ))
    );
    drop(held);
    tokio::time::resume();
    fixture.inventory.wait().await.unwrap();
    assert_eq!(fixture.inventory.inner.attempts.load(Ordering::SeqCst), 1);
    assert_eq!(
        fixture.resolve("after-timeout").await.unwrap().unwrap(),
        fixture.bind("after-timeout")
    );
}

#[tokio::test]
async fn a_mid_run_gap_marker_is_repaired_by_the_next_new_pair_bind() {
    let fixture = Fixture::new();
    broca_store::session_refs::migrate(
        &fixture.root.join("run-index.db"),
        &broca_core::ReadOnlyWalReader::new(&fixture.root),
    )
    .unwrap();
    assert_eq!(
        fixture.resolve("first").await.unwrap().unwrap(),
        fixture.bind("first")
    );
    assert_eq!(fixture.inventory.inner.attempts.load(Ordering::SeqCst), 0);
    let marker = fixture
        .root
        .join(broca_core::wal_writer::SESSION_PROJECTION_INCOMPLETE);
    std::fs::write(&marker, b"").unwrap();
    assert!(
        !fixture
            .index
            .session_refs(std::iter::empty())
            .unwrap()
            .complete
    );
    assert_eq!(
        fixture.resolve("after-gap").await.unwrap().unwrap(),
        fixture.bind("after-gap")
    );
    assert!(!marker.exists());
    assert!(
        fixture
            .index
            .session_refs(std::iter::empty())
            .unwrap()
            .complete
    );
    assert_eq!(fixture.inventory.inner.attempts.load(Ordering::SeqCst), 1);
}
