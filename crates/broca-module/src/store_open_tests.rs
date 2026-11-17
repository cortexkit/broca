// Tests of the new storage's three class roots: where the opener creates a session, which root
// a session is opened from, and the refusal of a session whose head is in two roots.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_core::FileLeaseStore;
use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::Session;
use broca_wal::StorageClass;

use super::{triple, StoreRuntime, StoreSlot};
use crate::executor::RunExecutor;
use crate::opener::DefaultOpener;
use crate::registry::{LineageOpener as _, StoreCreation};
use crate::LineageRegistry;

/// An executor for sessions whose tests run nothing.
struct NoRuns;

#[async_trait::async_trait]
impl RunExecutor for NoRuns {}

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

/// A module over `root` whose storage `runtime` creates sessions: its opener, and the registry
/// over it.
fn module(
    root: &Path,
    runtime: Arc<StoreRuntime>,
) -> (Arc<DefaultOpener<FileLeaseStore>>, LineageRegistry) {
    module_backing_up(root, runtime, vec!["head".into()])
}

/// [`module`], whose new sessions labelled one of `backup_classes` start in the durable root.
fn module_backing_up(
    root: &Path,
    runtime: Arc<StoreRuntime>,
    backup_classes: Vec<String>,
) -> (Arc<DefaultOpener<FileLeaseStore>>, LineageRegistry) {
    let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
    let retention =
        Arc::new(crate::retention::RetentionService::new(root, leases.clone()).unwrap());
    let opener = Arc::new(
        DefaultOpener::new(leases, Arc::new(NoRuns))
            .with_backup_classes(backup_classes)
            .with_store_runtime(runtime),
    );
    let registry = LineageRegistry::new(opener.clone());
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

#[cfg(feature = "test-clock")]
async fn closed_read_fixture(
    name: &str,
    max_bytes: u64,
) -> (PathBuf, Arc<StoreRuntime>, LineageRegistry, BindIdentity) {
    use broca_session_store::record::Record;
    let root = state_root(name);
    let key = key(&root, name);
    let memory = broca_session_store::backend::MemoryBackend::default();
    let addr = SessionAddr(broca_core::session_addr(&key));
    let mut source = Session::create(&memory, addr.clone(), triple(&key), &deployment(), false)
        .await
        .unwrap();
    let mut shape = broca_session_store::synthetic::Shape::production(64);
    shape.runs = 4;
    broca_session_store::synthetic::write(&mut source, &shape)
        .await
        .unwrap();
    let mut records = source.open_log_records().await.unwrap();
    for (_, record) in &mut records {
        if let Record::RunStart(start) = record {
            start.run_choices = Some(serde_json::json!({"config": {
                "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
                "response_format": {"type": "text"}, "context_epoch": 0
            }}));
        }
        if let Record::EntryFacts(facts) = record {
            if facts.native {
                Arc::make_mut(facts).sender = None;
            }
        }
    }
    let mut session = Session::create(
        LocalBackend::new(StorageClass::Ordinary.store_root(&root)),
        addr,
        triple(&key),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    session
        .append(
            &records
                .into_iter()
                .map(|(_, record)| record)
                .collect::<Vec<_>>(),
            true,
        )
        .await
        .unwrap();
    session.close().await.unwrap();
    let runtime = Arc::new(
        StoreRuntime::open(
            &root,
            deployment(),
            Arc::new(NoRuns),
            Duration::from_secs(600),
        )
        .await
        .unwrap()
        .with_view_bytes_for_test(max_bytes),
    );
    let (_, registry) = module(&root, runtime.clone());
    (root, runtime, registry, key)
}

/// Only a session kept in the ephemeral root can expire, so the expiry check every operation
/// makes reads no log for a session kept anywhere else. It used to load the session's whole
/// view, also for a running session whose actor then served the operation itself.
#[cfg(feature = "test-clock")]
#[tokio::test]
async fn the_expiry_check_reads_no_log_outside_the_ephemeral_root() {
    use std::sync::atomic::Ordering;
    let (root, runtime, registry, key) =
        closed_read_fixture("expiry-reads", crate::store_reads::ViewCache::MAX_BYTES).await;
    runtime.log_reads.store(0, Ordering::Relaxed);
    for _ in 0..3 {
        registry.check_expired(&key, None).await.unwrap();
    }
    assert_eq!(runtime.log_reads.load(Ordering::Relaxed), 0);
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn cached_closed_pages_read_the_log_once() {
    use crate::serve::ServeHandler;
    use std::sync::atomic::Ordering;
    let (root, runtime, registry, key) =
        closed_read_fixture("cached-pages", crate::store_reads::ViewCache::MAX_BYTES).await;
    runtime.log_reads.store(0, Ordering::Relaxed);
    runtime.checkpoint_frame_reads.store(0, Ordering::Relaxed);
    for from in [1, 8, 15, 1] {
        let page = registry
            .read(crate::serve::SessionReadRequest::new(
                key.clone(),
                Some(from),
                Some(7),
            ))
            .await
            .unwrap();
        assert_eq!(page.messages.len(), 7);
        assert_eq!(
            runtime.log_reads.load(Ordering::Relaxed),
            1,
            "page from {from}"
        );
        // The view's stream head takes the checkpoint back from the log's first frame, once.
        assert_eq!(
            runtime.checkpoint_frame_reads.load(Ordering::Relaxed),
            1,
            "page from {from}"
        );
        assert!(registry.slot_state(&key).is_none());
    }
    eprintln!("cached closed session.read: pages=4 log_reads=1 checkpoint_frame_reads=1");
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn stored_read_future_stays_small() {
    fn future_bytes<F: std::future::Future>(_: impl FnOnce() -> F) -> usize {
        std::mem::size_of::<F>()
    }
    let root = state_root("read-future-size");
    let runtime = runtime(&root).await;
    let (_, registry) = module(&root, runtime.clone());
    let key = key(&root, "size");
    let bytes = future_bytes(|| {
        runtime.read_inner(
            &registry,
            &key,
            crate::store_reads::StoreReadRequest::Page(Default::default()),
        )
    });
    eprintln!("stored read future bytes={bytes}");
    assert!(bytes < 20 * 1024, "stored read future uses {bytes} bytes");
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn a_failed_cached_head_does_not_hide_a_stale_history_version() {
    use crate::serve::{ServeError, ServeHandler, SessionReadRequest};
    use broca_session_store::record::Record;

    let (root, runtime, registry, key) = closed_read_fixture(
        "failed-cached-head",
        crate::store_reads::ViewCache::MAX_BYTES,
    )
    .await;
    let mut session = Session::load(
        LocalBackend::new(StorageClass::Ordinary.store_root(&root)),
        SessionAddr(broca_core::session_addr(&key)),
        &triple(&key),
        &deployment(),
    )
    .await
    .unwrap();
    let records = session.open_log_records().await.unwrap();
    let mut start = records
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::RunStart(start) => Some(start.clone()),
            _ => None,
        })
        .unwrap();
    let mut end = records
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end.clone()),
            _ => None,
        })
        .unwrap();
    start.run_id = "invalid-control-error".into();
    end.run_id = start.run_id.clone();
    end.reason = broca_protocol::RunFinishReason::Error;
    end.usage = None;
    // The storage fold retains opaque error JSON, but control projection requires a
    // ProviderError. This leaves a readable snapshot with a genuinely failing stream head.
    end.error = Some(serde_json::json!("not a ProviderError").into());
    session
        .append(&[Record::RunStart(start), Record::RunEnd(end)], true)
        .await
        .unwrap();
    session.close().await.unwrap();
    let view = runtime.current_view(&key).await.unwrap().unwrap();
    assert!(matches!(
        view.stream_head(),
        Err(ServeError::Open(
            _,
            Some(broca_protocol::ErrorClass::Permanent)
        ))
    ));
    let mut request = SessionReadRequest::new(key.clone(), Some(1), Some(7));
    request.history_version = Some(0);
    let result = registry.read(request).await;
    assert!(
        matches!(result, Err(ServeError::HistoryChanged)),
        "{result:?}"
    );
    let valid = SessionReadRequest::new(key.clone(), Some(1), Some(7));
    assert!(matches!(registry.read(valid).await,
        Err(ServeError::Open(message, Some(broca_protocol::ErrorClass::Permanent)))
            if message.contains("a run end's error")));
    assert!(registry.slot_state(&key).is_none());
    drop(view);
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn uncached_closed_pages_load_once_per_read() {
    use crate::serve::ServeHandler;
    use std::sync::atomic::Ordering;
    let (root, runtime, registry, key) = closed_read_fixture("uncached-pages", 0).await;
    for from in [1, 8, 15, 1] {
        runtime.log_reads.store(0, Ordering::Relaxed);
        runtime.checkpoint_frame_reads.store(0, Ordering::Relaxed);
        let page = registry
            .read(crate::serve::SessionReadRequest::new(
                key.clone(),
                Some(from),
                Some(7),
            ))
            .await
            .unwrap();
        assert_eq!(page.messages.len(), 7);
        assert_eq!(
            runtime.log_reads.load(Ordering::Relaxed),
            1,
            "page from {from}"
        );
        assert_eq!(
            runtime.checkpoint_frame_reads.load(Ordering::Relaxed),
            1,
            "page from {from}"
        );
        assert_eq!(runtime.views.usage(), (0, 0));
        assert!(registry.slot_state(&key).is_none());
    }
    eprintln!(
        "uncached closed session.read: pages=4 log_reads=4 checkpoint_frame_reads=4 (one each per read)"
    );
    drop(registry);
    drop(runtime);
    std::fs::remove_dir_all(root).unwrap();
}

#[cfg(feature = "test-clock")]
#[tokio::test]
async fn raw_sender_pages_reuse_the_stored_position_with_or_without_an_actor() {
    use std::sync::atomic::Ordering;
    let (root, runtime, registry, key) =
        closed_read_fixture("raw-sender-pages", crate::store_reads::ViewCache::MAX_BYTES).await;
    let request =
        serde_json::from_value(serde_json::json!({"form": "raw", "from_ordinal": 1, "limit": 7}))
            .unwrap();
    runtime.log_reads.store(0, Ordering::Relaxed);
    runtime.checkpoint_frame_reads.store(0, Ordering::Relaxed);
    let first = crate::history::read(&registry, &key, &request)
        .await
        .unwrap();
    assert!(first["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|entry| entry.get("sender").is_some()));
    assert_eq!(runtime.log_reads.load(Ordering::Relaxed), 1);
    assert_eq!(runtime.checkpoint_frame_reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        first,
        crate::history::read(&registry, &key, &request)
            .await
            .unwrap()
    );
    assert_eq!(runtime.log_reads.load(Ordering::Relaxed), 1);
    assert_eq!(runtime.checkpoint_frame_reads.load(Ordering::Relaxed), 1);
    assert!(registry.slot_state(&key).is_none());

    let handle = registry.actor_for(&key).await.unwrap();
    broca_test_support::wait_until("the actor's control projection serving reads", || async {
        let (reply, answer) = tokio::sync::oneshot::channel();
        handle
            .send(crate::actor::LineageCmd::StoreRead {
                request: Box::new(crate::store_reads::StoreReadRequest::Page(
                    Default::default(),
                )),
                reply,
            })
            .await
            .unwrap_or_else(|_| panic!("the actor stopped before answering a read"));
        match answer.await.unwrap() {
            crate::store_reads::StoreReadOutcome::Served(_) => Ok(()),
            _ => Err("the actor is still preparing its projection".into()),
        }
    })
    .await;
    runtime.log_reads.store(0, Ordering::Relaxed);
    runtime.checkpoint_frame_reads.store(0, Ordering::Relaxed);
    let open = crate::history::read(&registry, &key, &request)
        .await
        .unwrap();
    assert_eq!(open["messages"], first["messages"]);
    // The view the closed pages loaded is still the session's position, and the expiry check no
    // longer loads a view of its own, so the open session's pages read nothing more.
    assert_eq!(
        runtime.log_reads.load(Ordering::Relaxed),
        0,
        "the first page with the actor open reuses the closed pages' view"
    );
    assert_eq!(runtime.checkpoint_frame_reads.load(Ordering::Relaxed), 0);
    for _ in 0..3 {
        assert_eq!(
            open,
            crate::history::read(&registry, &key, &request)
                .await
                .unwrap()
        );
    }
    assert_eq!(
        runtime.log_reads.load(Ordering::Relaxed),
        0,
        "later sender pages reuse the view and its index"
    );
    assert_eq!(runtime.checkpoint_frame_reads.load(Ordering::Relaxed), 0);
    assert_eq!(registry.slot_state(&key), Some("open"));
    eprintln!("raw sender history: closed pages=2 log_reads=1 checkpoint_frame_reads=1; open pages=4 log_reads=0 checkpoint_frame_reads=0");
    // The actor owns files until the registry shuts it down; the shared scratch root reclaims them.
    drop(handle);
    drop(registry);
    drop(runtime);
    let _ = root;
}

/// A registered session that moved to another machine owes this machine no recovery: the boot
/// sweep retires its registration rather than failing on it at every restart.
#[tokio::test]
async fn a_moved_session_retires_its_recovery_candidate() {
    use broca_session_store::backend::SessionBackend as _;
    use broca_session_store::session::moves;
    let root = state_root("moved-candidate");
    let runtime = runtime(&root).await;
    let (_, registry) = module(&root, runtime);
    registry.recover_store_crash_orphans().await.unwrap();
    let key = key(&root, "moved");
    let class = StorageClass::Ordinary;
    let addr = SessionAddr(broca_core::session_addr(&key));
    crate::store_open::recovery::register_candidate(&class.store_root(&root), &addr).unwrap();
    create_in(&root, class, &key).await;
    let store = LocalBackend::new(class.store_root(&root));
    let (_, version) = store.read_head(&addr).await.unwrap().unwrap();
    moves::freeze(&store, &addr, version, "cut", &deployment())
        .await
        .unwrap();
    moves::release(&store, &addr, "cut", "other-machine", &deployment())
        .await
        .unwrap();
    let sweep = registry.recover_store_crash_orphans().await.unwrap();
    assert!(sweep.failed.is_empty(), "{sweep:?}");
    assert_eq!((sweep.recovered, sweep.retired), (0, 1), "{sweep:?}");
    assert_eq!(registry.slot_state(&key), None, "the sweep opened no actor");
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

/// A registered session whose stored state owes recovery nothing is retired by the boot sweep
/// without being opened, and a later sweep has nothing left to inspect.
#[tokio::test]
async fn a_finished_administrative_install_retires_its_recovery_candidate() {
    let root = state_root("installed-candidate");
    let runtime = runtime(&root).await;
    let (_, registry) = module(&root, runtime);
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
    assert_eq!((sweep.recovered, sweep.retired), (0, 1), "{sweep:?}");
    assert_eq!(registry.slot_state(&key), None, "the sweep opened no actor");
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

/// A new session is placed by one rule: a retention policy places it in the ephemeral class
/// whatever its label, a label in `backup_classes` (`head` by default) in the durable class, and
/// anything else in the ordinary class, and it is created in that class's root. After a restart
/// with another `backup_classes`, a session stays in the root it was created in, and only new
/// sessions follow the new setting. An import is placed durable by the same rule, whatever the
/// setting.
#[tokio::test]
async fn a_new_session_is_placed_by_one_rule() {
    let root = state_root("placement");
    let runtime = runtime(&root).await;
    let (opener, registry) = module(&root, runtime.clone());
    let backup_classes = ["head".to_owned()];
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
    }

    // A module restarted with `head` no longer listed keeps the durable session in the durable
    // root, and creates a new `head` session in the ordinary root.
    let restarted = self::runtime(&root).await;
    let (opener, registry) = module_backing_up(&root, restarted.clone(), Vec::new());
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

/// A session in each root opens from that root through the opener, and stays there.
#[tokio::test]
async fn a_session_in_each_root_reopens_from_its_root() {
    let root = state_root("reopen");
    for class in StorageClass::ALL {
        create_in(&root, class, &key(&root, class.name())).await;
    }
    let runtime = runtime(&root).await;
    let (_, registry) = module(&root, runtime.clone());
    for class in StorageClass::ALL {
        let session = key(&root, class.name());
        assert_eq!(runtime.class_of(&session).unwrap(), Some(class));
        registry.actor_for(&session).await.unwrap();
        assert_eq!(
            broca_wal::session_storage_class(&root, &broca_core::session_addr(&session)).unwrap(),
            Some(class),
            "it opened from its own root"
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
    let runtime = runtime(&root).await;
    let (_, registry) = module(&root, runtime.clone());
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

/// An import's own retention decides where its session is created, through the registry's
/// import route: in the ephemeral root when the import asks for retention, in the durable root
/// otherwise.
#[tokio::test]
async fn an_import_asking_for_retention_creates_its_session_in_the_ephemeral_root() {
    use crate::store_import::{ImportCommand, ImportRoots, StoreCommand, StoreOutcome};
    use sha2::Digest as _;
    let root = state_root("import-retention");
    let runtime = self::runtime(&root).await;
    let (_, registry) = module(&root, runtime.clone());
    let folder = root.join("imports");
    std::fs::create_dir_all(&folder).unwrap();
    let export = serde_json::to_vec(&serde_json::json!({
        "info": {"id": "ses_head"},
        "messages": [
            {"id": "msg_u1", "type": "user", "time": {"created": 1000}, "text": "Hello."},
            {"id": "msg_a1", "type": "assistant", "time": {"created": 2000, "completed": 3000},
             "content": [{"type": "text", "text": "Hi."}]}
        ]
    }))
    .unwrap();
    let core = broca_protocol::BindPrincipal::Reserved {
        module_id: "prefrontal-core".into(),
    };
    for (name, retention, expected) in [
        ("kept", None, StorageClass::Durable),
        ("retained", Some(3_600), StorageClass::Ephemeral),
    ] {
        let path = folder.join(format!("{name}.json"));
        std::fs::write(&path, &export).unwrap();
        let params = serde_json::from_value(serde_json::json!({
            "import_id": name,
            "source": {
                "harness": "opencode2",
                "path": path.display().to_string(),
                "session": "ses_head",
                "sha256": format!("{:x}", sha2::Sha256::digest(&export)),
            },
            "origin": {"kind": "import", "source_ref": "thalamus:ses_head"},
            "retention": retention,
        }))
        .unwrap();
        let session = key(&root, name);
        let outcome = registry
            .route_store_command(
                &session,
                StoreCommand::Import(Box::new(ImportCommand {
                    params,
                    principal: core.clone(),
                    // An import arrives on a route scoped to its owner's head.
                    scope: Some(broca_protocol::SessionScope {
                        owner: core.clone(),
                        scope_ref: format!("head-{name}"),
                        scope_epoch: 1,
                        flow_id: None,
                        sender: None,
                        agent_id: None,
                        run_id: None,
                        agent_id_frozen: false,
                    }),
                    roots: Arc::new(ImportRoots::new(vec![folder.clone()], Some(folder.clone()))),
                    // Now, so the retained session isn't already past its deadline.
                    now_ms: broca_core::clock::epoch_millis().unwrap(),
                })),
            )
            .await
            .unwrap();
        assert!(
            matches!(outcome, StoreOutcome::Imported(_)),
            "{name}: {outcome:?}"
        );
        assert_eq!(
            runtime.class_of(&session).unwrap(),
            Some(expected),
            "{name}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}
