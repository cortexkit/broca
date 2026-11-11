// Tests of the new storage's three class roots: where the opener creates a session, which root
// a session is opened from, and the refusal of a session whose head is in two roots.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary, WalStore as _};
use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use broca_wal::StorageClass;

use super::{triple, StoreRuntime, StoreSlot};
use crate::executor::{RunExecutor, RunJob};
use crate::opener::DefaultOpener;
use crate::registry::{LineageOpener as _, StoreCreation};
use crate::LineageRegistry;

/// An executor for sessions whose tests run nothing.
struct NoRuns;

#[async_trait::async_trait]
impl RunExecutor for NoRuns {
    async fn execute(
        &self,
        _: RunJob,
        _: tokio::sync::mpsc::Sender<OutputEvent>,
    ) -> Result<RunSummary, RunError> {
        unreachable!("these tests run no model step")
    }
}

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32])
}

fn state_root(name: &str) -> PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("store-open-classes-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn key(root: &Path, name: &str) -> BindIdentity {
    BindIdentity::new(root.join("project"), "runner", name)
}

async fn runtime(root: &Path) -> Arc<StoreRuntime> {
    Arc::new(
        StoreRuntime::open(
            root,
            deployment(),
            Arc::new(NoRuns),
            Duration::from_millis(100),
        )
        .await
        .unwrap(),
    )
}

/// A module over `root` whose today's storage is `wal_store` and whose new storage `runtime`
/// creates sessions: its opener, and the registry over it.
fn module(
    root: &Path,
    wal_store: Arc<FileWalStore>,
    runtime: Arc<StoreRuntime>,
) -> (
    Arc<DefaultOpener<FileLeaseStore, FileWalStore>>,
    LineageRegistry,
) {
    let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
    let index = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
    let retention = Arc::new(
        crate::retention::RetentionService::new(
            wal_store.clone(),
            index.clone(),
            leases.clone(),
            index,
            Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1)),
        )
        .unwrap(),
    );
    let opener = Arc::new(
        DefaultOpener::new(leases, wal_store.clone(), Arc::new(NoRuns)).with_store_runtime(runtime),
    );
    let registry = LineageRegistry::new_with_reader(opener.clone(), wal_store, None);
    registry.set_retention_service(retention);
    (opener, registry)
}

/// Creates the session `key` in the root of `class`, as a session placed there has been.
async fn create_in(root: &Path, class: StorageClass, key: &BindIdentity) {
    drop(
        Session::create(
            LocalBackend::new(class.store_root(root)),
            SessionAddr(broca_core::session_addr(key)),
            triple(key),
            &deployment(),
            false,
        )
        .await
        .unwrap(),
    );
}

#[tokio::test]
async fn a_finished_administrative_install_retires_its_recovery_candidate() {
    let root = state_root("installed-candidate");
    let runtime = runtime(&root).await;
    let (_, registry) = module(
        &root,
        Arc::new(FileWalStore::new(root.join("wal"))),
        runtime,
    );
    registry.recover_store_crash_orphans().await.unwrap();
    let key = key(&root, "installed");
    let class = StorageClass::Ordinary;
    crate::store_open::recovery::register_candidate(
        &class.store_root(&root),
        &SessionAddr(broca_core::session_addr(&key)),
    )
    .unwrap();
    create_in(&root, class, &key).await;
    let sweep = registry.recover_store_crash_orphans().await.unwrap();
    assert!(sweep.failed.is_empty(), "{sweep:?}");
    assert_eq!(sweep.recovered, 1);
    assert_eq!(
        registry
            .recover_store_crash_orphans()
            .await
            .unwrap()
            .inspected,
        0
    );
    assert!(registry.stop_and_seal().await);
    std::fs::remove_dir_all(root).unwrap();
}

/// Both storages place a new session by one rule: a retention policy places it in the
/// ephemeral class whatever its label, a label in `backup_classes` (`head` by default) in the
/// durable class, and anything else in the ordinary class. The new storage creates it in that
/// class's root, and today's storage its WAL in that class's folder. After a restart with
/// another `backup_classes`, a session stays in the root it was created in, and only new sessions
/// follow the new setting. An import is placed durable by the same rule, whatever the setting.
#[tokio::test]
async fn both_storages_place_a_new_session_by_one_rule() {
    let root = state_root("placement");
    let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
    let runtime = runtime(&root).await;
    let (opener, registry) = module(&root, wal_store.clone(), runtime.clone());
    let backup_classes = wal_store.backup_classes();
    assert_eq!(backup_classes, ["head"], "the default backup classes");
    let cases = [
        (
            "retained-head",
            Some(3600),
            Some("head"),
            StorageClass::Ephemeral,
        ),
        ("retained", Some(3600), None, StorageClass::Ephemeral),
        ("head", None, Some("head"), StorageClass::Durable),
        ("worker", None, Some("worker"), StorageClass::Ordinary),
        ("unlabelled", None, None, StorageClass::Ordinary),
    ];
    for (gen, (name, retention, class, expected)) in cases.into_iter().enumerate() {
        assert_eq!(
            broca_wal::placement(
                retention.is_some(),
                broca_wal::selected_for_backup(class, &backup_classes)
            ),
            expected,
            "{name}"
        );
        // The new storage, as a send the store actor serves creates the session.
        let stored = key(&root, &format!("store-{name}"));
        let handle = registry.handle();
        handle.mark_store_eligible(&stored, StoreCreation::Send);
        opener
            .open_with_class(
                &stored,
                gen as u64 + 1,
                handle,
                retention,
                class.map(str::to_owned),
            )
            .await
            .unwrap();
        assert_eq!(runtime.class_of(&stored).unwrap(), Some(expected), "{name}");
        // Today's storage.
        let today = key(&root, &format!("today-{name}"));
        drop(wal_store.open_with_class(&today, retention, class).unwrap());
        for other in StorageClass::ALL {
            assert_eq!(
                wal_store.class_wal_path(other, &today).is_file(),
                other == expected,
                "{name}: today's {other} folder"
            );
        }
    }

    // A module restarted with `head` no longer listed keeps the durable session in the durable
    // root, and creates a new `head` session in the ordinary root.
    let changed = Arc::new(FileWalStore::new(root.join("wal")).with_backup_classes(Vec::new()));
    let restarted = self::runtime(&root).await;
    let (opener, registry) = module(&root, changed, restarted.clone());
    let held = key(&root, "store-head");
    assert!(opener.store_holding(&held).unwrap().is_some());
    assert_eq!(
        restarted.class_of(&held).unwrap(),
        Some(StorageClass::Durable)
    );
    let fresh = key(&root, "store-fresh-head");
    let handle = registry.handle();
    handle.mark_store_eligible(&fresh, StoreCreation::Send);
    opener
        .open_with_class(&fresh, 1, handle, None, Some("head".into()))
        .await
        .unwrap();
    assert_eq!(
        restarted.class_of(&fresh).unwrap(),
        Some(StorageClass::Ordinary)
    );
    assert_eq!(
        broca_wal::session_storage_class(&root, &broca_core::session_addr(&held)).unwrap(),
        Some(StorageClass::Durable)
    );
    // An import brings an agent head onto broca: it is durable without a label, even with no
    // backup classes configured, and a retention policy would still place it ephemeral.
    for (gen, (name, retention, expected)) in [
        ("imported", None, StorageClass::Durable),
        ("imported-retained", Some(3600), StorageClass::Ephemeral),
    ]
    .into_iter()
    .enumerate()
    {
        let imported = key(&root, name);
        let handle = registry.handle();
        handle.mark_store_eligible(&imported, StoreCreation::Import);
        opener
            .open_with_class(&imported, gen as u64 + 2, handle, retention, None)
            .await
            .unwrap();
        assert_eq!(
            restarted.class_of(&imported).unwrap(),
            Some(expected),
            "{name}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// A session in each root opens from that root through the opener, stays there, and today's
/// storage refuses it by name.
#[tokio::test]
async fn a_session_in_each_root_reopens_from_its_root() {
    let root = state_root("reopen");
    for class in StorageClass::ALL {
        create_in(&root, class, &key(&root, class.name())).await;
    }
    let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
    let runtime = runtime(&root).await;
    let (_, registry) = module(&root, wal_store.clone(), runtime.clone());
    for class in StorageClass::ALL {
        let session = key(&root, class.name());
        assert_eq!(runtime.class_of(&session).unwrap(), Some(class));
        registry.actor_for(&session).await.unwrap();
        assert_eq!(
            broca_wal::session_storage_class(&root, &broca_core::session_addr(&session)).unwrap(),
            Some(class),
            "it opened from its own root"
        );
        assert!(wal_store.check_move(&session, false).is_err(), "{class}");
        assert!(
            !wal_store.class_wal_path(class, &session).exists(),
            "{class}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// A head in two roots is damage: every way of reaching the session refuses it by name, and
/// neither head is touched or preferred.
#[tokio::test]
async fn a_head_in_two_roots_is_refused_by_name() {
    let root = state_root("two-roots");
    let session = key(&root, "twice");
    let address = broca_core::session_addr(&session);
    for class in [StorageClass::Durable, StorageClass::Ordinary] {
        create_in(&root, class, &session).await;
    }
    let heads: Vec<Vec<u8>> = [StorageClass::Durable, StorageClass::Ordinary]
        .into_iter()
        .map(|class| {
            std::fs::read(broca_wal::session_storage_head(&root, class, &address)).unwrap()
        })
        .collect();
    let wal_store = Arc::new(FileWalStore::new(root.join("wal")));
    let runtime = runtime(&root).await;
    let (_, registry) = module(&root, wal_store.clone(), runtime.clone());
    // The refusal's text, which names it.
    fn refusal<T>(result: Result<T, impl ToString>) -> String {
        match result {
            Ok(_) => panic!("a session with a head in two roots was served"),
            Err(error) => error.to_string(),
        }
    }
    let named = |error: String| {
        assert!(
            error.contains(broca_wal::SESSION_IN_TWO_CLASSES),
            "not refused by name: {error}"
        );
    };
    named(refusal(registry.actor_for(&session).await));
    named(refusal(runtime.holds(&session)));
    named(refusal(runtime.current_view(&session).await));
    named(refusal(
        StoreSlot::loaded(runtime.clone()).holding(&session),
    ));
    // Today's storage doesn't take it either.
    assert!(wal_store.check_move(&session, true).is_err());
    assert!(!wal_store.wal_path(&session).exists());
    for (class, head) in [StorageClass::Durable, StorageClass::Ordinary]
        .into_iter()
        .zip(heads)
    {
        assert_eq!(
            std::fs::read(broca_wal::session_storage_head(&root, class, &address)).unwrap(),
            head,
            "the {class} head is untouched"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
