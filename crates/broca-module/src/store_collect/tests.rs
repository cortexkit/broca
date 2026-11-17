use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_core::{FileLeaseStore, LeaseStore as _};
use broca_protocol::BindIdentity;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::codec::{hex, Id128};
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::{Session, GRACE_MS};
use broca_wal::StorageClass;
use tokio_util::sync::CancellationToken;

use super::{StoreCollectSweep, SweepPass};
use crate::executor::RunExecutor;
use crate::store_open::{triple, StoreRuntime, StoreSlot};

/// An executor for sessions whose tests run nothing.
struct NoRuns;

#[async_trait::async_trait]
impl RunExecutor for NoRuns {}

/// When the finished sessions' seals committed.
const SEALED_AT: u64 = 1_000;

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32])
}

fn state_root(name: &str) -> PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("store-collect-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    root
}

fn key(root: &Path, name: &str) -> BindIdentity {
    BindIdentity::new(root.join("project"), "runner", name)
}

/// The new storage's runtime over `root`, with its deployment key loaded.
async fn slot(root: &Path) -> Arc<StoreSlot> {
    let runtime = StoreRuntime::open(
        root,
        deployment(),
        Arc::new(NoRuns),
        Duration::from_millis(100),
    )
    .await
    .unwrap();
    Arc::new(StoreSlot::loaded(Arc::new(runtime)))
}

/// A session created in the root of `class`, sealed once at `sealed_at` and closed, as a
/// finished worker leaves its session: nobody opens it again. Returns the log the seal replaced.
async fn finished(root: &Path, class: StorageClass, key: &BindIdentity, sealed_at: u64) -> Id128 {
    let mut session = Session::create(
        LocalBackend::new(class.store_root(root)),
        SessionAddr(broca_core::session_addr(key)),
        triple(key),
        &deployment(),
        false,
    )
    .await
    .unwrap();
    let (replaced, _) = session.open_log();
    session.seal(None, sealed_at).await.unwrap();
    session.close().await.unwrap();
    replaced
}

/// Whether the session `key` in the root of `class` still holds the log `log`: its file,
/// `logs/<address>/<log>` in the root.
fn holds(root: &Path, class: StorageClass, key: &BindIdentity, log: &Id128) -> bool {
    class
        .store_root(root)
        .join("logs")
        .join(broca_core::session_addr(key))
        .join(hex(&log.0))
        .is_file()
}

/// The sweep over `root`, skipping the sessions named in `served`, at a fixed time.
fn sweep(
    root: &Path,
    slot: Arc<StoreSlot>,
    leases: Arc<FileLeaseStore>,
    served: &[BindIdentity],
    now_ms: u64,
) -> StoreCollectSweep {
    let served: BTreeSet<String> = served.iter().map(broca_core::session_addr).collect();
    StoreCollectSweep::new(
        root,
        slot,
        leases,
        Arc::new(move |key| served.contains(&broca_core::session_addr(key))),
    )
    .with_clock(Arc::new(move || now_ms))
}

#[tokio::test]
async fn a_real_collection_pass_publishes_phase_timings() {
    if !crate::phase_metrics_tests::isolated(
        "store_collect::tests::a_real_collection_pass_publishes_phase_timings",
    ) {
        return;
    }
    let root = state_root("phase-timings");
    let key = key(&root, "finished");
    let replaced = finished(&root, StorageClass::Ordinary, &key, SEALED_AT).await;
    let sweep = sweep(
        &root,
        slot(&root).await,
        Arc::new(FileLeaseStore::new(root.join("leases"))),
        &[],
        SEALED_AT + GRACE_MS,
    );
    let before = broca_session_store::metrics::snapshot();
    let pass = sweep.sweep_once(&CancellationToken::new());
    assert_eq!(pass.collected, 1);
    assert!(!holds(&root, StorageClass::Ordinary, &key, &replaced));
    let after = broca_session_store::metrics::snapshot();
    for phase in ["collection", "collection_sweep"] {
        assert_eq!(after[phase].count, before[phase].count + 1, "{phase}");
        assert!(after[phase].total_us > before[phase].total_us, "{phase}");
        assert!(after[phase].last_us > 0, "{phase}");
    }
    let _ = std::fs::remove_dir_all(root);
}

/// Finished sessions nobody opens again are collected by the sweep once their seal's grace
/// period is over, at most the bound a pass, each pass going on where the last one stopped. A
/// session still inside its grace period keeps its files. A session an actor serves, or whose
/// lease another owner holds, is left for a later pass, and collected once it is free.
#[tokio::test]
async fn the_sweep_collects_closed_sessions_a_bounded_number_per_pass() {
    let root = state_root("bounded");
    let mut finished_sessions = Vec::new();
    for (name, class) in [
        ("a", StorageClass::Ordinary),
        ("b", StorageClass::Durable),
        ("c", StorageClass::Ephemeral),
    ] {
        let key = key(&root, name);
        let replaced = finished(&root, class, &key, SEALED_AT).await;
        finished_sessions.push((key, class, replaced));
    }
    let fresh = key(&root, "fresh");
    let fresh_log = finished(&root, StorageClass::Ordinary, &fresh, SEALED_AT + 1).await;
    let held = key(&root, "held");
    let held_log = finished(&root, StorageClass::Ordinary, &held, SEALED_AT).await;
    let served = key(&root, "served");
    let served_log = finished(&root, StorageClass::Durable, &served, SEALED_AT).await;

    let slot = slot(&root).await;
    let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
    let lease = leases.acquire(&broca_core::lease_key_for(&held)).unwrap();
    let sweep = sweep(
        &root,
        slot,
        Arc::clone(&leases),
        std::slice::from_ref(&served),
        SEALED_AT + GRACE_MS,
    )
    .with_bounds(16, 2);
    let stop = CancellationToken::new();

    // The first pass stops at its bound; the second goes on to the end of the list.
    let first = sweep.sweep_once(&stop);
    assert_eq!(first.collected, 2, "{first:?}");
    assert_eq!(first.changes, 2, "{first:?}");
    let second = sweep.sweep_once(&stop);
    assert_eq!(second.collected, 1, "{second:?}");
    assert_eq!(first.read + second.read, 6, "{first:?} {second:?}");
    assert_eq!(first.skipped + second.skipped, 2, "{first:?} {second:?}");
    for (key, class, replaced) in &finished_sessions {
        assert!(!holds(&root, *class, key, replaced), "{key:?}");
    }
    assert!(holds(&root, StorageClass::Ordinary, &fresh, &fresh_log));
    assert!(holds(&root, StorageClass::Ordinary, &held, &held_log));
    assert!(holds(&root, StorageClass::Durable, &served, &served_log));

    // The next pass starts from the first session again, and finds only the held ones due.
    assert_eq!(
        sweep.sweep_once(&stop),
        SweepPass {
            read: 6,
            skipped: 2,
            ..SweepPass::default()
        }
    );

    // Once its lease is free, the held session is collected.
    drop(lease);
    let freed = sweep.sweep_once(&stop);
    assert_eq!(freed.collected, 1, "{freed:?}");
    assert!(!holds(&root, StorageClass::Ordinary, &held, &held_log));
    assert!(holds(&root, StorageClass::Ordinary, &fresh, &fresh_log));
    assert!(holds(&root, StorageClass::Durable, &served, &served_log));

    // A collected session loads as it stood, with nothing left waiting.
    let (key, class, _) = &finished_sessions[0];
    let session = Session::load(
        LocalBackend::new(class.store_root(&root)),
        SessionAddr(broca_core::session_addr(key)),
        &triple(key),
        &deployment(),
    )
    .await
    .unwrap();
    assert!(session.head().pending.is_empty());
    let _ = std::fs::remove_dir_all(&root);
}

/// A pass reads at most its bound of session heads, and the next pass reads the ones after
/// them, so every session is reached however many there are.
#[tokio::test]
async fn a_pass_reads_its_bound_of_heads_and_the_next_goes_on_after_them() {
    let root = state_root("heads");
    let mut logs = Vec::new();
    for name in ["a", "b", "c", "d", "e"] {
        let key = key(&root, name);
        let replaced = finished(&root, StorageClass::Ordinary, &key, SEALED_AT).await;
        logs.push((key, replaced));
    }
    let slot = slot(&root).await;
    let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
    let sweep = sweep(&root, slot, leases, &[], SEALED_AT + GRACE_MS).with_bounds(2, 16);
    let stop = CancellationToken::new();

    let passes: Vec<SweepPass> = (0..4).map(|_| sweep.sweep_once(&stop)).collect();
    let read: Vec<usize> = passes.iter().map(|pass| pass.read).collect();
    let collected: Vec<usize> = passes.iter().map(|pass| pass.collected).collect();
    assert_eq!(read, [2, 2, 1, 2]);
    assert_eq!(collected, [2, 2, 1, 0]);
    for (key, replaced) in &logs {
        assert!(
            !holds(&root, StorageClass::Ordinary, key, replaced),
            "{key:?}"
        );
    }
    let _ = std::fs::remove_dir_all(&root);
}

/// A pass lists session folders without checking each for a head, so a folder a creation or a
/// move left without one is read like any other and found to hold nothing due; it neither fails
/// the pass nor stops it before the sessions after it.
#[tokio::test]
async fn a_session_folder_without_a_head_holds_nothing_due() {
    let root = state_root("headless");
    let key = key(&root, "z");
    let replaced = finished(&root, StorageClass::Ordinary, &key, SEALED_AT).await;
    std::fs::create_dir_all(
        StorageClass::Ordinary
            .store_root(&root)
            .join("heads")
            .join("0000000000000000"),
    )
    .unwrap();
    let slot = slot(&root).await;
    let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
    let sweep = sweep(&root, slot, leases, &[], SEALED_AT + GRACE_MS);
    let pass = sweep.sweep_once(&CancellationToken::new());
    assert_eq!((pass.read, pass.collected, pass.failed), (2, 1, 0));
    assert!(!holds(&root, StorageClass::Ordinary, &key, &replaced));
    let _ = std::fs::remove_dir_all(&root);
}
