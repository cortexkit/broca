// Tests of retention for sessions kept in the new storage: the rule both storages decide by, and
// the sweep deleting an expired session there, with everything it owns, under its lease.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use broca_core::{FileLeaseStore, FileWalStore, OutputEvent, RunError, RunSummary};
use broca_protocol::{BindIdentity, PauseReason, RunFinishReason, RunId};
use broca_session_loop::queue::{Receipt, SteerReceipt};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::record::{Moved, Pause, QueuedSend, Record, RunEnd, RunStart};
use broca_session_store::session::Session;
use broca_store::SqliteStore;
use broca_wal::{FramedRecord, StorageClass, WalRecord};

use super::store_policy;
use crate::executor::{RunExecutor, RunJob};
use crate::opener::DefaultOpener;
use crate::registry::{LineageOpener as _, StoreCreation};
use crate::retention::{policy, RetentionService};
use crate::store_open::StoreRuntime;
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
    wal: Arc<FileWalStore>,
    service: Arc<RetentionService>,
    runtime: Arc<StoreRuntime>,
    opener: Arc<DefaultOpener<FileLeaseStore, FileWalStore>>,
    registry: LineageRegistry,
}

impl Rig {
    async fn new(name: &str, idle_ttl: Duration) -> Self {
        let root = broca_test_support::scratch_root()
            .join(format!("retention-store-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let wal = Arc::new(FileWalStore::new(root.join("wal")));
        let index = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
        let leases = Arc::new(FileLeaseStore::new(root.join("leases")));
        let resolver = Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
        let service = Arc::new(
            RetentionService::new(
                wal.clone(),
                index.clone(),
                leases.clone(),
                index.clone(),
                resolver,
            )
            .unwrap(),
        );
        let runtime = Arc::new(
            StoreRuntime::open(&root, deployment(), Arc::new(NoRuns), idle_ttl)
                .await
                .unwrap(),
        );
        let opener = Arc::new(
            DefaultOpener::new(leases, wal.clone(), Arc::new(NoRuns))
                .with_store_runtime(runtime.clone()),
        );
        let registry = LineageRegistry::new_with_reader(opener.clone(), wal.clone(), None);
        registry.set_retention_service(service.clone());
        Self {
            root,
            wal,
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
        let backend = LocalBackend::new(self.ephemeral());
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
        receipt: Some(serde_json::to_value(receipt).unwrap()),
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
    Record::Pause(Pause {
        run_id: run.into(),
        reason: PauseReason::ToolHeld,
        ts_ms: Some(ts),
        error: None,
        account_limit: None,
    })
}

fn wal_record(fixture: &str) -> WalRecord {
    serde_json::from_str(fixture).unwrap()
}

fn wal_start(key: &BindIdentity, run: &str, retention: Option<u64>, ts: u64) -> WalRecord {
    let mut record = wal_record(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/run_started.json"
    ));
    let WalRecord::RunStarted {
        session,
        run_id,
        retention: value,
        ts_ms,
        ..
    } = &mut record
    else {
        unreachable!("the fixture is a run start")
    };
    *session = key.clone();
    *run_id = RunId(run.into());
    *value = retention;
    *ts_ms = Some(ts);
    record
}

fn wal_end(ts: u64) -> WalRecord {
    WalRecord::RunFinished {
        interrupt_reason: None,
        ts_ms: Some(ts),
        reason: RunFinishReason::Completed,
        usage: Default::default(),
        error: None,
        retries_used: 0,
    }
}

fn wal_queued(retention: Option<u64>, ts: u64) -> WalRecord {
    let mut record = wal_record(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/queued.json"
    ));
    let WalRecord::Queued {
        retention: value,
        ts_ms,
        ..
    } = &mut record
    else {
        unreachable!("the fixture is a queued send")
    };
    *value = retention;
    *ts_ms = Some(ts);
    record
}

fn wal_steered(retention: Option<u64>, ts: u64) -> WalRecord {
    let mut record = wal_record(include_str!(
        "../../broca-wal/tests/golden/pre_step_transforms/steered.json"
    ));
    let WalRecord::Steered {
        retention: value,
        ts_ms,
        ..
    } = &mut record
    else {
        unreachable!("the fixture is a steer")
    };
    *value = retention;
    *ts_ms = Some(ts);
    record
}

fn wal_paused(run: &str, ts: u64) -> WalRecord {
    WalRecord::RunPaused {
        run_id: RunId(run.into()),
        ts_ms: Some(ts),
        reason: PauseReason::ToolHeld,
        error: None,
        account_limit: None,
    }
}

fn framed(records: Vec<WalRecord>) -> Vec<FramedRecord> {
    records
        .into_iter()
        .enumerate()
        .map(|(n, record)| FramedRecord {
            seq: n as u64 + 1,
            fence: 1,
            record,
        })
        .collect()
}

fn seqs(records: Vec<Record>) -> Vec<(u64, Record)> {
    records
        .into_iter()
        .enumerate()
        .map(|(n, record)| (n as u64 + 1, record))
        .collect()
}

/// One history, written by either storage, gives one retention policy: today's records and the
/// new storage's say the same run starts, sends, steers, ends and pauses, with the same retention
/// and times, and both storages decide through one rule.
#[test]
fn both_storages_decide_one_history_by_one_rule() {
    let key = BindIdentity::new("/project", "runner", "s");
    let start = |run: &str, retention: Option<u64>, ts: u64| {
        (
            wal_start(&key, run, retention, ts),
            store_start(run, retention, ts),
        )
    };
    let end = |run: &str, ts: u64| (wal_end(ts), store_end(run, ts));
    let queued =
        |retention: Option<u64>, ts: u64| (wal_queued(retention, ts), store_queued(retention, ts));
    let steered = |retention: Option<u64>, ts: u64| {
        (wal_steered(retention, ts), store_steered(retention, ts))
    };
    let paused = |run: &str, ts: u64| (wal_paused(run, ts), store_paused(run, ts));
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
    for (name, steps, expected) in cases {
        let (wal, store): (Vec<_>, Vec<_>) = steps.into_iter().unzip();
        assert_eq!(policy(&framed(wal)), expected, "{name}: today's storage");
        assert_eq!(
            store_policy(&seqs(store)),
            expected,
            "{name}: the new storage"
        );
    }
}

/// An expired session in the new storage's ephemeral root, moved there from today's storage, is
/// deleted by the sweep with everything it owns: its head and kept head versions, its logs and
/// objects, and what its move left on today's storage (the retired WAL, its identity sidecar,
/// today's media manifest of the session, and the `moved` marker). Its address then reads as
/// absent in both storages, and a later send there starts a new session.
#[tokio::test]
async fn an_expired_session_in_the_new_storage_is_deleted_with_everything_it_owns() {
    let rig = Rig::new("expired", Duration::from_secs(600)).await;
    let key = rig.key("moved");
    // What the move left on today's storage: the WAL it retired, with its identity sidecar, the
    // session's media manifest, and the marker naming the move.
    let lineage = {
        let _lease = rig.service.lease(&key).unwrap();
        let mut wal = rig.wal.open_with_retention(&key, Some(SECONDS)).unwrap();
        wal.append(1, &wal_start(&key, "r1", Some(SECONDS), STARTED_MS), true)
            .unwrap();
        wal.append(1, &wal_end(ENDED_MS), true).unwrap();
        rig.service.remember_session(&key).unwrap();
        wal.lineage_id().unwrap().to_string()
    };
    let bytes = b"%PDF-1.7\nretention fixture\n%%EOF";
    let reference = broca_core::media_validation::validate_media(bytes, "application/pdf").unwrap();
    broca_core::media_store::MediaStore::open(
        &rig.root,
        broca_core::media_store::MediaStoreClass::Ephemeral,
    )
    .unwrap()
    .session(&key)
    .unwrap()
    .register_and_append(&reference, bytes, || Ok(()))
    .unwrap();
    let retired = rig.wal.ephemeral_wal_path(&key);
    let marker = rig.wal.wal_path(&key).with_extension("moved");
    std::fs::create_dir_all(marker.parent().unwrap()).unwrap();
    broca_wal::migration::atomic_json(
        &rig.root,
        &marker,
        &broca_wal::migration::Moved {
            cut_id: "store-move:1".into(),
            holder: "session-store".into(),
            lineage_id: lineage,
        },
    )
    .unwrap();
    rig.seed(
        &key,
        &[
            store_start("r1", Some(SECONDS), STARTED_MS),
            store_end("r1", ENDED_MS),
            Record::Moved(Moved {
                move_id: "store-move:1".into(),
                source_activity_ms: ENDED_MS,
                source_lineage: None,
            }),
        ],
        true,
    )
    .await;
    let mut owned = rig.folders(&key);
    owned.extend([
        retired.clone(),
        retired.with_extension("session"),
        marker,
        rig.root.join("media-ephemeral/refs").join(addr(&key).0),
    ]);
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
    assert_eq!(
        broca_wal::store_ownership(&rig.root, &addr(&key).0).unwrap(),
        broca_wal::StoreOwnership::Today
    );
    assert!(rig.runtime.current_view(&key).await.unwrap().is_none());
    rig.wal.check_move(&key, true).unwrap();

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
/// kept by the sweep past it; one with a run still going, or whose first
/// run set no retention, has no deadline at all. Only the expired session nobody uses is deleted.
#[tokio::test]
async fn unexpired_and_leased_sessions_in_the_new_storage_are_kept() {
    let rig = Rig::new("kept", Duration::from_secs(600)).await;
    let finished = [
        store_start("r1", Some(SECONDS), STARTED_MS),
        store_end("r1", ENDED_MS),
    ];
    let idle = rig.key("idle");
    let open = rig.key("open");
    let running = rig.key("running");
    let forever = rig.key("forever");
    rig.seed(&idle, &finished, false).await;
    rig.seed(&open, &finished, false).await;
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
    for key in [&idle, &open, &running, &forever] {
        assert!(rig.holds(key), "{} before its deadline", key.session);
    }

    assert_eq!(rig.registry.sweep_once(DEADLINE_MS).await.unwrap(), 1);
    assert!(!rig.holds(&idle), "the expired session nobody uses");
    for key in [&open, &running, &forever] {
        assert!(rig.holds(key), "{} at the deadline", key.session);
    }
    assert_eq!(
        rig.registry.sweep_once(DEADLINE_MS * 1_000).await.unwrap(),
        0
    );
    for key in [&open, &running, &forever] {
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
