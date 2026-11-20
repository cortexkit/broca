// Tests of session retention: the rule a session's records decide its deadline by, and the sweep
// deleting an expired session with everything it owns, under its lease.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_core::FileLeaseStore;
use broca_protocol::{BindIdentity, PauseReason, RunFinishReason};
use broca_session_loop::queue::{Receipt, SteerReceipt};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{Moved, Pause, QueuedSend, Record, RunEnd, RunStart};
use broca_session_store::session::Session;
use broca_wal::StorageClass;

use super::store_policy;
use crate::executor::RunExecutor;
use crate::opener::DefaultOpener;
use crate::registry::{LineageOpener as _, StoreCreation};
use crate::retention::RetentionService;
use crate::store_open::StoreRuntime;
use crate::LineageRegistry;

/// An executor for sessions whose tests run nothing.
struct NoRuns;

#[async_trait::async_trait]
impl RunExecutor for NoRuns {}

/// Retention seconds of the sessions these tests seed: a session whose last run ended at
/// `ENDED_MS` expires at `DEADLINE_MS`.
const SECONDS: u64 = 60;
const STARTED_MS: u64 = 1_000;
const ENDED_MS: u64 = 2_000;
const DEADLINE_MS: u64 = ENDED_MS + SECONDS * 1_000;

fn deployment() -> DeploymentKey {
    DeploymentKey::new(1, &[7; 32])
}

fn addr(key: &BindIdentity) -> SessionAddr {
    SessionAddr(broca_core::session_addr(key))
}

fn triple(key: &BindIdentity) -> SessionTriple {
    SessionTriple {
        project_root: key.project_root.display().to_string(),
        harness: key.harness.clone(),
        session: key.session.clone(),
    }
}

/// A module over one state root with both storages, the retention service, and the registry
/// that sweeps.
struct Rig {
    root: PathBuf,
    service: Arc<RetentionService>,
    runtime: Arc<StoreRuntime>,
    opener: Arc<DefaultOpener<FileLeaseStore>>,
    registry: LineageRegistry,
}

impl Rig {
    async fn new(name: &str, idle_ttl: Duration) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("retention-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
        let service = Arc::new(RetentionService::new(&root, leases.clone()).unwrap());
        // The runtime exports spend, as broca's does: only then does owed spend hold a session.
        let export = Arc::new(broca_store::SqliteStore::open_in_memory().unwrap());
        let runtime = Arc::new(
            StoreRuntime::open(&root, deployment(), Arc::new(NoRuns), idle_ttl)
                .await
                .unwrap()
                .with_billing(crate::store_billing::StoreBilling {
                    index: export as Arc<dyn broca_core::ExportIndex>,
                    resolver: Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::MeteredApi, 4)),
                }),
        );
        let opener = Arc::new(
            DefaultOpener::new(leases, Arc::new(NoRuns)).with_store_runtime(runtime.clone()),
        );
        let registry = LineageRegistry::new(opener.clone());
        registry.set_retention_service(service.clone());
        Self {
            root,
            service,
            runtime,
            opener,
            registry,
        }
    }

    fn key(&self, name: &str) -> BindIdentity {
        BindIdentity::new(self.root.join("project"), "runner", name)
    }

    fn ephemeral(&self) -> PathBuf {
        StorageClass::Ephemeral.store_root(&self.root)
    }

    /// Creates the session `key` in the ephemeral root holding `records`. With `seal`, the
    /// session is sealed once, so it has objects (segments, directory and id-index pages) as well
    /// as a log.
    async fn seed(&self, key: &BindIdentity, records: &[Record], seal: bool) {
        self.seed_in(StorageClass::Ephemeral, key, records, seal)
            .await
    }

    /// [`Rig::seed`], in the root of storage class `class`.
    async fn seed_in(
        &self,
        class: StorageClass,
        key: &BindIdentity,
        records: &[Record],
        seal: bool,
    ) {
        let backend = LocalBackend::new(class.store_root(&self.root));
        let mut session = Session::create(&backend, addr(key), triple(key), &deployment(), false)
            .await
            .unwrap();
        session.append(records, true).await.unwrap();
        if seal {
            session.seal(None, ENDED_MS).await.unwrap();
        }
        session.close().await.unwrap();
    }

    fn holds(&self, key: &BindIdentity) -> bool {
        self.runtime.class_of(key).unwrap() == Some(StorageClass::Ephemeral)
    }

    /// The folders of the session `key` in the ephemeral root.
    fn folders(&self, key: &BindIdentity) -> Vec<PathBuf> {
        ["heads", "logs", "objects"]
            .into_iter()
            .map(|folder| self.ephemeral().join(folder).join(addr(key).0))
            .collect()
    }
}

impl Drop for Rig {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn store_start(run: &str, retention: Option<u64>, ts: u64) -> Record {
    Record::RunStart(Box::new(RunStart {
        imported_ids: None,
        message_marks: None,
        named_speaker: None,
        run_id: run.into(),
        model: serde_json::from_value(
            serde_json::json!({"provider_module_id": "p", "model_id": "m"}),
        )
        .unwrap(),
        setup: Vec::new(),
        renderer_version: 1,
        submission_id: None,
        send_id: None,
        request_identity_digest: None,
        ts_ms: Some(ts),
        written_at_ms: Some(ts),
        session_class: None,
        retention_secs: retention,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }))
}

fn store_end(run: &str, ts: u64) -> Record {
    Record::RunEnd(RunEnd {
        run_id: run.into(),
        reason: RunFinishReason::Completed,
        usage: None,
        ts_ms: Some(ts),
        retries_used: 0,
        interrupt_reason: None,
        error: None,
    })
}

/// The export holds run `run`'s spend.
fn store_billed(run: &str) -> Record {
    Record::Billed(broca_session_store::record::Billed { run_id: run.into() })
}

fn store_queued(retention: Option<u64>, ts: u64) -> Record {
    let receipt = Receipt {
        retention,
        ..Receipt::default()
    };
    Record::QueuedSend(QueuedSend {
        media: Vec::new(),
        message_marks: None,
        named_speaker: None,
        submission_id: "queued".into(),
        send_id: None,
        input: Vec::new(),
        setup: Vec::new(),
        written_at_ms: Some(ts),
        ts_ms: Some(ts),
        request: None,
        receipt: Some(serde_json::to_value(receipt).unwrap().into()),
        delivery: None,
    })
}

fn store_steered(retention: Option<u64>, ts: u64) -> Record {
    let receipt = SteerReceipt {
        retention,
        ts_ms: Some(ts),
        ..SteerReceipt::default()
    };
    Record::Steer(broca_session_loop::queue::steer(Vec::new(), Some(ts), &receipt, None).unwrap())
}

fn store_paused(run: &str, ts: u64) -> Record {
    store_paused_for(run, ts, PauseReason::ToolHeld)
}

fn store_paused_for(run: &str, ts: u64, reason: PauseReason) -> Record {
    Record::Pause(Pause {
        run_id: run.into(),
        reason,
        ts_ms: Some(ts),
        error: None,
        account_limit: None,
    })
}

fn seqs(records: Vec<Record>) -> Vec<(u64, Record)> {
    records
        .into_iter()
        .enumerate()
        .map(|(n, record)| (n as u64 + 1, record))
        .collect()
}

/// A session's retention policy, last activity and deadline, as its run starts, queued sends,
/// steers, ends and pauses decide them.
#[test]
fn the_retention_rule_reads_a_session_history() {
    let start = store_start;
    let end = store_end;
    let queued = store_queued;
    let steered = store_steered;
    let paused = store_paused;
    let cases = vec![
        (
            "a finished run",
            vec![start("r1", Some(60), 1_000), end("r1", 2_000)],
            Some((60, 2_000, Some(62_000))),
        ),
        (
            "a send queued after it shortens the policy and is activity",
            vec![
                start("r1", Some(60), 1_000),
                end("r1", 2_000),
                queued(Some(30), 2_500),
            ],
            Some((30, 2_500, Some(32_500))),
        ),
        (
            "a steer into the running run shortens it",
            vec![
                start("r1", Some(60), 1_000),
                steered(Some(10), 1_500),
                end("r1", 2_000),
            ],
            Some((10, 2_000, Some(12_000))),
        ),
        (
            "a later run that sets none keeps it",
            vec![
                start("r1", Some(60), 1_000),
                end("r1", 2_000),
                start("r2", None, 5_000),
                end("r2", 6_000),
            ],
            Some((60, 6_000, Some(66_000))),
        ),
        (
            "a running run has no deadline",
            vec![start("r1", Some(60), 1_000)],
            Some((60, 1_000, None)),
        ),
        (
            "a paused run has no deadline",
            vec![start("r1", Some(60), 1_000), paused("r1", 1_500)],
            Some((60, 1_500, None)),
        ),
        (
            "a first run that set none keeps the session forever",
            vec![start("r1", None, 1_000), end("r1", 2_000)],
            None,
        ),
        ("no run yet", vec![queued(Some(60), 1_000)], None),
    ];
    for (name, records, expected) in cases {
        assert_eq!(store_policy(&seqs(records)), expected, "{name}");
    }
}

/// An expired session in the ephemeral root is deleted by the sweep with everything it owns: its
/// head and kept head versions, its logs and objects, and a move's leftover stream. Its address
/// then reads as absent, and a later send there starts a new session.
#[tokio::test]
async fn an_expired_session_in_the_new_storage_is_deleted_with_everything_it_owns() {
    let rig = Rig::new("expired", Duration::from_secs(600)).await;
    let key = rig.key("expired");
    rig.seed(
        &key,
        &[
            store_start("r1", Some(SECONDS), STARTED_MS),
            store_end("r1", ENDED_MS),
            store_billed("r1"),
            Record::Moved(Moved {
                move_id: "store-move:1".into(),
                source_activity_ms: ENDED_MS,
                source_lineage: None,
            }),
        ],
        true,
    )
    .await;
    let owned = rig.folders(&key);
    for path in &owned {
        assert!(path.exists(), "{} before the sweep", path.display());
    }
    let stream = rig
        .root
        .join("session-store-moves/streams")
        .join(addr(&key).0);
    std::fs::create_dir_all(stream.parent().unwrap()).unwrap();
    std::fs::write(&stream, b"ciphertext stream fixture").unwrap();
    assert!(
        LocalBackend::new(rig.ephemeral())
            .head_versions(&addr(&key))
            .unwrap()
            .len()
            > 1,
        "the session keeps earlier head versions"
    );

    assert_eq!(rig.registry.sweep_once(DEADLINE_MS).await.unwrap(), 1);
    assert!(!stream.exists());
    for path in &owned {
        assert!(!path.exists(), "{} is left", path.display());
    }
    assert!(rig.runtime.current_view(&key).await.unwrap().is_none());

    // A later send at the address starts a new session, with none of the deleted one's history.
    let handle = rig.registry.handle();
    handle.mark_store_eligible(&key, StoreCreation::Send);
    rig.opener
        .open_with_class(&key, 1, handle, Some(SECONDS), None)
        .await
        .unwrap();
    assert!(rig.holds(&key));
    let view = rig.runtime.current_view(&key).await.unwrap().unwrap();
    assert!(view.operations.owner.first_start.is_none(), "a new session");
}

/// A session whose policy hasn't expired is kept until its deadline; one whose lease is held is
/// kept by the sweep past it, and so is one whose run's spend isn't exported yet, since only
/// the session records it; one with a run still going, or whose first run set no retention,
/// has no deadline at all. Only the expired session nobody uses is deleted.
#[tokio::test]
async fn unexpired_and_leased_sessions_in_the_new_storage_are_kept() {
    let rig = Rig::new("kept", Duration::from_secs(600)).await;
    let finished = [
        store_start("r1", Some(SECONDS), STARTED_MS),
        store_end("r1", ENDED_MS),
        store_billed("r1"),
    ];
    let idle = rig.key("idle");
    let open = rig.key("open");
    let owed = rig.key("owed");
    let running = rig.key("running");
    let forever = rig.key("forever");
    rig.seed(&idle, &finished, false).await;
    rig.seed(&open, &finished, false).await;
    rig.seed(&owed, &finished[..2], false).await;
    rig.seed(
        &running,
        &[store_start("r1", Some(SECONDS), STARTED_MS)],
        false,
    )
    .await;
    rig.seed(
        &forever,
        &[
            store_start("r1", None, STARTED_MS),
            store_end("r1", ENDED_MS),
        ],
        false,
    )
    .await;
    let leases = FileLeaseStore::new(rig.root.join("leases"));
    let _lease =
        broca_core::LeaseStore::acquire(&leases, &broca_core::lease_key_for(&open)).unwrap();

    assert_eq!(rig.registry.sweep_once(DEADLINE_MS - 1).await.unwrap(), 0);
    for key in [&idle, &open, &owed, &running, &forever] {
        assert!(rig.holds(key), "{} before its deadline", key.session);
    }

    assert_eq!(rig.registry.sweep_once(DEADLINE_MS).await.unwrap(), 1);
    assert!(!rig.holds(&idle), "the expired session nobody uses");
    for key in [&open, &owed, &running, &forever] {
        assert!(rig.holds(key), "{} at the deadline", key.session);
    }
    assert_eq!(
        rig.registry.sweep_once(DEADLINE_MS * 1_000).await.unwrap(),
        0
    );
    for key in [&open, &owed, &running, &forever] {
        assert!(rig.holds(key), "{} long after", key.session);
    }
}

/// A fork's session in the ephemeral root, which holds only its fork's records, is kept until
/// its retention time past the fork's idle time after its newest record, and then deleted by the
/// same sweep, with no deletion deferred for want of a reading.
#[tokio::test]
async fn a_forks_session_is_deleted_once_its_records_say_it_may_be() {
    use broca_session_store::record::{Fork, ForkEvent};
    let rig = Rig::new("fork", Duration::from_secs(600)).await;
    let key = rig.key("head:fork:fork-1");
    let fact = |event, at_ms| Fork {
        fork_id: "fork-1".into(),
        event,
        at_ms,
        ask_id: None,
        attempt: None,
        question: None,
        answer: None,
        finish: None,
        usage: None,
        principal: None,
        base: None,
        admission_id: None,
        idle_ms: None,
        retain_ms: None,
        detail: None,
    };
    const IDLE_MS: u64 = 600_000;
    const RETAIN_MS: u64 = 3_600_000;
    rig.seed(
        &key,
        &[
            Record::Fork(Fork {
                principal: Some("colliculus".into()),
                base: Some(serde_json::json!({"run_id": "r1"})),
                idle_ms: Some(IDLE_MS),
                retain_ms: Some(RETAIN_MS),
                ..fact(ForkEvent::Prepared, STARTED_MS)
            }),
            Record::Fork(fact(ForkEvent::Ended, ENDED_MS)),
        ],
        false,
    )
    .await;
    let deadline = ENDED_MS + IDLE_MS + RETAIN_MS;
    let records = LocalBackend::new(rig.ephemeral());
    let position = Session::stored_position(&records, &addr(&key), &deployment())
        .await
        .unwrap()
        .unwrap();
    let view = Session::view_at(
        &records,
        addr(&key),
        &triple(&key),
        &deployment(),
        &position,
    )
    .await
    .unwrap();
    assert_eq!(
        store_policy(&view.records_from(0).await.unwrap()),
        Some((RETAIN_MS / 1_000, ENDED_MS, Some(deadline)))
    );
    drop(view);

    assert_eq!(rig.registry.sweep_once(deadline - 1).await.unwrap(), 0);
    assert!(
        rig.holds(&key),
        "the fork's answers are kept until its deadline"
    );
    assert_eq!(rig.registry.sweep_once(deadline).await.unwrap(), 1);
    assert!(
        !rig.holds(&key),
        "the fork's session is deleted at its deadline"
    );
    for path in rig.folders(&key) {
        assert!(!path.exists(), "{} is left", path.display());
    }
}

/// The operator deletes a session in either storage class that holds no work, however it was
/// kept: its head, logs and objects go, and its expiry marker stays, so reads of its runs answer
/// expired. A session with a run going or paused, a queued send or spend still to export is
/// refused by what it holds;
/// one an actor holds open, by its lease; and an address with no session has nothing to delete.
#[tokio::test]
async fn the_operator_deletes_a_session_that_holds_no_work() {
    use crate::retention::store::OperatorDeletion;
    let rig = Rig::new("operator", Duration::from_secs(600)).await;
    let billed = || {
        Record::Billed(broca_session_store::record::Billed {
            run_id: "r1".into(),
        })
    };
    let finished = [
        store_start("r1", None, STARTED_MS),
        store_end("r1", ENDED_MS),
        billed(),
    ];
    let durable = rig.key("durable");
    let owed = rig.key("owed");
    let ephemeral = rig.key("ephemeral");
    let running = rig.key("running");
    let paused = rig.key("paused");
    let signed_out = rig.key("signed-out");
    let queued = rig.key("queued");
    let open = rig.key("open");
    rig.seed_in(StorageClass::Durable, &durable, &finished, true)
        .await;
    rig.seed(&ephemeral, &finished, false).await;
    rig.seed_in(
        StorageClass::Durable,
        &running,
        &[store_start("r1", None, STARTED_MS)],
        false,
    )
    .await;
    rig.seed_in(
        StorageClass::Durable,
        &paused,
        &[
            store_start("r1", None, STARTED_MS),
            store_paused("r1", ENDED_MS),
        ],
        false,
    )
    .await;
    rig.seed_in(
        StorageClass::Durable,
        &signed_out,
        &[
            store_start("r1", None, STARTED_MS),
            store_paused_for("r1", ENDED_MS, PauseReason::AuthRequired),
        ],
        false,
    )
    .await;
    rig.seed_in(
        StorageClass::Durable,
        &queued,
        &[
            store_start("r1", None, STARTED_MS),
            store_end("r1", ENDED_MS),
            billed(),
            store_queued(None, ENDED_MS),
        ],
        false,
    )
    .await;
    rig.seed_in(
        StorageClass::Durable,
        &owed,
        &[
            store_start("r1", None, STARTED_MS),
            store_end("r1", ENDED_MS),
        ],
        false,
    )
    .await;
    rig.seed_in(StorageClass::Durable, &open, &finished, false)
        .await;
    let leases = FileLeaseStore::new(rig.root.join("leases"));
    let _lease =
        broca_core::LeaseStore::acquire(&leases, &broca_core::lease_key_for(&open)).unwrap();
    let delete_with = |key: BindIdentity, paused_run: bool| {
        let (service, runtime) = (rig.service.clone(), rig.runtime.clone());
        async move {
            service
                .delete_store_session_for_operator(&runtime, &key, DEADLINE_MS, paused_run)
                .await
                .unwrap()
        }
    };
    let delete = |key: BindIdentity| delete_with(key, false);
    let durable_folders: Vec<PathBuf> = ["heads", "logs", "objects"]
        .into_iter()
        .map(|folder| {
            StorageClass::Durable
                .store_root(&rig.root)
                .join(folder)
                .join(addr(&durable).0)
        })
        .collect();
    for path in &durable_folders {
        assert!(path.exists(), "{} before the deletion", path.display());
    }

    for key in [&durable, &ephemeral] {
        assert_eq!(
            delete(key.clone()).await,
            OperatorDeletion::Deleted,
            "{}",
            key.session
        );
        assert!(
            rig.runtime.class_of(key).unwrap().is_none(),
            "{}",
            key.session
        );
        assert!(rig.runtime.current_view(key).await.unwrap().is_none());
        assert_eq!(
            rig.runtime
                .expired_at(key, Some("r1"), DEADLINE_MS)
                .await
                .unwrap(),
            Some(DEADLINE_MS),
            "a read of the deleted run answers expired"
        );
        assert_eq!(delete(key.clone()).await, OperatorDeletion::Absent);
    }
    for path in &durable_folders {
        assert!(!path.exists(), "{} is left", path.display());
    }
    assert_eq!(
        delete(running.clone()).await,
        OperatorDeletion::Busy("a run is active or paused")
    );
    assert_eq!(
        delete(paused.clone()).await,
        OperatorDeletion::Busy("a run is active or paused")
    );
    assert_eq!(
        delete(signed_out.clone()).await,
        OperatorDeletion::Busy("a run is active or paused")
    );
    // Asked to delete a paused run, the operator deletion still keeps a running run and one
    // waiting on calls a provider holds: broca owes that provider a `tool.withdraw` for each
    // call, which deleting the session would lose.
    assert_eq!(
        delete_with(running.clone(), true).await,
        OperatorDeletion::Busy("a run is active or paused")
    );
    assert_eq!(
        delete_with(paused.clone(), true).await,
        OperatorDeletion::Busy("a run is active or paused")
    );
    assert_eq!(
        delete_with(signed_out.clone(), true).await,
        OperatorDeletion::Deleted
    );
    assert!(rig.runtime.class_of(&signed_out).unwrap().is_none());
    assert_eq!(
        rig.runtime
            .expired_at(&signed_out, Some("r1"), DEADLINE_MS)
            .await
            .unwrap(),
        Some(DEADLINE_MS),
        "a read of the deleted paused run answers expired"
    );
    assert_eq!(
        delete(queued.clone()).await,
        OperatorDeletion::Busy("sends are queued")
    );
    assert_eq!(
        delete(owed.clone()).await,
        OperatorDeletion::Busy("a run's spend is not exported yet")
    );
    assert_eq!(delete(open.clone()).await, OperatorDeletion::Held);
    for key in [&running, &paused, &queued, &owed, &open] {
        assert!(
            rig.runtime.class_of(key).unwrap().is_some(),
            "{} is kept",
            key.session
        );
    }
    assert_eq!(delete(rig.key("never")).await, OperatorDeletion::Absent);
}
