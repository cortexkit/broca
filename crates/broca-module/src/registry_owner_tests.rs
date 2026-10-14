use super::*;
use crate::serve::ServeHandler;
use broca_core::{FileWalStore, MaterializedStore, Persister, WalStore};
use broca_protocol::{BindPrincipal, RunFinishReason, SendScope, SessionScope, Usage};
use broca_wal::{FramedRecord, WalRecord};

struct NeverOpener;

#[async_trait]
impl LineageOpener for NeverOpener {
    async fn open(
        &self,
        _: &LineageKey,
        _: u64,
        _: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        panic!("owner authorization must not open an actor")
    }
}

fn reserved(module_id: &str) -> BindPrincipal {
    BindPrincipal::Reserved {
        module_id: module_id.into(),
    }
}

fn scope(owner: &str) -> SessionScope {
    SessionScope {
        owner: reserved(owner),
        scope_ref: "work".into(),
        scope_epoch: 7,
        flow_id: Some("flow".into()),
        sender: Some(reserved("basal")),
        agent_id: None,
        run_id: None,
    }
}

fn started(
    session: &LineageKey,
    principal: Option<BindPrincipal>,
    scope: Option<SessionScope>,
) -> WalRecord {
    let mut record = super::tests::started_record(session);
    let WalRecord::RunStarted {
        principal: first,
        scope: frozen,
        ..
    } = &mut record
    else {
        unreachable!()
    };
    *first = principal;
    *frozen = scope;
    record
}

fn finished() -> WalRecord {
    WalRecord::RunFinished {
        reason: RunFinishReason::Cancelled,
        ts_ms: None,
        usage: Usage::default(),
        error: None,
        retries_used: 0,
    }
}

struct Fixture {
    root: std::path::PathBuf,
    session: LineageKey,
    wal: Arc<FileWalStore>,
}

impl Fixture {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = broca_test_support::scratch_root().join(format!(
            "broca-owner-read-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        Self {
            session: BindIdentity::new(root.join("project"), "runner", "session"),
            wal: Arc::new(FileWalStore::new(root.join("wal"))),
            root,
        }
    }

    fn append(&self, records: &[WalRecord]) {
        let mut wal = self.wal.open(&self.session).unwrap();
        for record in records {
            wal.append(1, record, false).unwrap();
        }
        wal.sync().unwrap();
    }

    fn sqlite(&self) -> Arc<broca_store::SqliteStore> {
        Arc::new(broca_store::SqliteStore::open(self.root.join("store.db")).unwrap())
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
#[ignore = "manual timing probe on a 15 MB production-framed WAL"]
async fn owner_authorization_on_15mb_wal() {
    let fixture = Fixture::new();
    let mut records = vec![started(
        &fixture.session,
        Some(reserved("basal")),
        Some(scope("core")),
    )];
    for step in 1..=256 {
        records.push(WalRecord::StepStarted {
            step_id: broca_protocol::StepId(step),
        });
        let mut record = super::tests::assistant_record();
        let WalRecord::ModelStepFinished {
            step_id,
            assistant_message,
            ..
        } = &mut record
        else {
            unreachable!()
        };
        *step_id = broca_protocol::StepId(step);
        *assistant_message = Arc::new(broca_protocol::AssistantMessage {
            message_id: broca_protocol::MessageId(format!("m{step}")),
            content: vec![broca_protocol::ContentBlock::Text {
                text: "x".repeat(61_000),
            }],
        });
        records.push(record);
    }
    records.push(finished());
    fixture.append(&records);
    let store = fixture.sqlite();
    let range = fixture
        .wal
        .read_records_range(&fixture.session, 0, None, None)
        .unwrap();
    store.project_range(&fixture.session, &range).unwrap();
    let bytes = std::fs::metadata(fixture.wal.wal_path(&fixture.session))
        .unwrap()
        .len();
    assert!(
        (15_000_000..17_000_000).contains(&bytes),
        "representative WAL size: {bytes}"
    );
    let registry =
        LineageRegistry::new_with_reader(Arc::new(NeverOpener), fixture.wal.clone(), Some(store));
    let started_at = std::time::Instant::now();
    registry
        .authorize_session_owner(
            &fixture.session,
            Some(&reserved("core")),
            &SendScope::Unstamped,
        )
        .await
        .unwrap();
    let cold_us = started_at.elapsed().as_micros();
    let repeated_at = std::time::Instant::now();
    registry
        .authorize_session_owner(
            &fixture.session,
            Some(&reserved("core")),
            &SendScope::Unstamped,
        )
        .await
        .unwrap();
    println!(
        "OWNER_AUTH bytes={bytes} records={} cold_authorization_us={cold_us} repeated_authorization_us={}",
        records.len(),
        repeated_at.elapsed().as_micros()
    );
    assert_eq!(registry.slot_counts().open, 0);
}

struct CountingWal {
    inner: Arc<FileWalStore>,
    reads: Mutex<Vec<u64>>,
    fault: AtomicU64,
}

impl WalStore for CountingWal {
    fn open(&self, _: &LineageKey) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        panic!("authorization never opens a WAL writer")
    }

    fn read_records(&self, session: &LineageKey) -> Result<Vec<FramedRecord>, broca_wal::WalError> {
        self.reads.lock().unwrap().push(0);
        self.inner.read_records(session)
    }

    fn read_records_range(
        &self,
        session: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.reads.lock().unwrap().push(after);
        if self.fault.load(Ordering::Relaxed) == 2 {
            return Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()));
        }
        if self.fault.load(Ordering::Relaxed) == 5 && through.is_some() {
            return self
                .inner
                .read_records_range(session, after, Some(after), anchor);
        }
        self.inner
            .read_records_range(session, after, through, anchor)
    }

    fn flush_for_read(&self, session: &LineageKey) -> Result<Option<u64>, broca_wal::WalError> {
        match self.fault.load(Ordering::Relaxed) {
            1 => {
                return Err(broca_wal::WalError::Io(std::io::Error::other(
                    "injected authorization barrier failure",
                )))
            }
            3 => panic!("injected authorization barrier task failure"),
            4 => return Ok(Some(u64::MAX)),
            _ => {}
        }
        self.inner.flush_for_read(session)
    }

    fn file_identity(&self, session: &LineageKey) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.file_identity(session)
    }
}

struct SnapshotStore {
    snapshot: Mutex<Result<Option<broca_core::AdmissionStoreSnapshot>, String>>,
    reads: AtomicU64,
}

impl MaterializedStore for SnapshotStore {
    fn read_prefix_with_watermark(
        &self,
        _: &LineageKey,
    ) -> Result<broca_core::StorePrefix, String> {
        panic!("authorization does not read transcript content")
    }

    fn read_admission_snapshot(
        &self,
        _: &LineageKey,
    ) -> Result<Option<broca_core::AdmissionStoreSnapshot>, String> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        self.snapshot.lock().unwrap().clone()
    }
}

fn snapshot(fixture: &Fixture) -> broca_core::AdmissionStoreSnapshot {
    let range = fixture
        .wal
        .read_records_range(&fixture.session, 0, None, None)
        .unwrap();
    let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
        panic!("complete fixture WAL")
    };
    let checkpoint = broca_core::AdmissionCheckpointV1::from_frames(records).unwrap();
    broca_core::AdmissionStoreSnapshot {
        watermark_seq: checkpoint.at_seq,
        lineage_id: range.lineage_id.map(|id| id.to_string()),
        anchor: range.anchor_at(checkpoint.at_seq),
        checkpoint: Some(checkpoint),
    }
}

fn registry(
    fixture: &Fixture,
    snapshot: Result<Option<broca_core::AdmissionStoreSnapshot>, String>,
) -> (LineageRegistry, Arc<CountingWal>, Arc<SnapshotStore>) {
    let wal = Arc::new(CountingWal {
        inner: fixture.wal.clone(),
        reads: Mutex::new(Vec::new()),
        fault: AtomicU64::new(0),
    });
    let store = Arc::new(SnapshotStore {
        snapshot: Mutex::new(snapshot),
        reads: AtomicU64::new(0),
    });
    let registry =
        LineageRegistry::new_with_reader(Arc::new(NeverOpener), wal.clone(), Some(store.clone()));
    (registry, wal, store)
}

async fn decision(
    registry: &LineageRegistry,
    session: &LineageKey,
    caller: Option<&BindPrincipal>,
) -> Result<(), broca_core::ScopeRefusal> {
    match registry
        .authorize_session_owner(session, caller, &SendScope::Unstamped)
        .await
    {
        Ok(()) => Ok(()),
        Err(crate::serve::ServeError::ScopeRefused(refusal)) => Err(refusal),
        Err(error) => panic!("ownership history must be readable: {error}"),
    }
}

fn install_live(
    registry: &LineageRegistry,
    session: &LineageKey,
    checkpoint: &broca_core::AdmissionCheckpointV1,
) -> Arc<AtomicU64> {
    let head = Arc::new(AtomicU64::new(checkpoint.at_seq));
    let (tx, _rx) = mpsc::channel(1);
    let handle = ActorHandle {
        gen: 3,
        tx,
        durable_head: head.clone(),
        run_in_flight: Arc::new(AtomicU64::new(0)),
        approvals: Arc::default(),
        first_episode: Arc::default(),
    };
    registry
        .inner
        .slots
        .lock()
        .unwrap()
        .insert(session.clone(), Entry::Open { gen: 3, handle });
    registry
        .handle()
        .note_session_ownership(session, 3, checkpoint);
    head
}

#[tokio::test]
async fn owner_sources_match_full_wal_without_opening_an_actor() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let projected = snapshot(&fixture);
    let checkpoint = projected.checkpoint.as_ref().unwrap();
    let ownership = checkpoint.ownership.as_ref().unwrap();
    let callers = [
        Some(reserved("basal")),
        Some(reserved("core")),
        Some(reserved("foreign")),
        Some(BindPrincipal::Direct),
        None,
    ];
    for path in [
        "live",
        "memory",
        "store",
        "no_snapshot",
        "failed_store",
        "old_snapshot",
    ] {
        let source = match path {
            "failed_store" => Err("injected store failure".into()),
            "old_snapshot" => {
                let mut old = projected.clone();
                old.checkpoint.as_mut().unwrap().ownership = None;
                Ok(Some(old))
            }
            "no_snapshot" | "live" | "memory" => Ok(None),
            _ => Ok(Some(projected.clone())),
        };
        let (registry, wal, store) = registry(&fixture, source);
        if path == "live" {
            install_live(&registry, &fixture.session, checkpoint);
        } else if path == "memory" {
            registry.owner_ownership(&fixture.session).await.unwrap();
            wal.reads.lock().unwrap().clear();
            store.reads.store(0, Ordering::Relaxed);
        }
        let checkpoint_seq = registry
            .inner
            .owner_snapshots
            .lock()
            .unwrap()
            .get(&fixture.session)
            .map_or(checkpoint.at_seq, |snapshot| snapshot.watermark_seq);
        for caller in &callers {
            let expected = broca_core::scope_admission::decide_owner_access(
                ownership,
                caller.as_ref(),
                &SendScope::Unstamped,
            );
            assert_eq!(
                decision(&registry, &fixture.session, caller.as_ref()).await,
                expected,
                "{path}: {caller:?}"
            );
        }
        let reads = wal.reads.lock().unwrap();
        match path {
            "live" => {
                assert!(
                    reads.is_empty(),
                    "live ownership must require no WAL reads: {reads:?}"
                );
                assert_eq!(store.reads.load(Ordering::Relaxed), 0);
            }
            "memory" | "store" => assert!(
                reads.iter().all(|after| *after == checkpoint_seq),
                "checkpoint path must never scan the prefix: {path}: {reads:?}"
            ),
            _ => assert!(
                reads.contains(&0),
                "missing or unusable snapshot must read the full WAL"
            ),
        }
        if path != "live" {
            assert_eq!(registry.slot_counts().open, 0);
        }
    }
}

#[tokio::test]
async fn ownership_tail_freezes_scope_after_the_store_watermark() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(&fixture.session, Some(reserved("core")), None),
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        super::tests::assistant_record(),
        finished(),
    ]);
    let store = fixture.sqlite();
    store
        .project_range(
            &fixture.session,
            &fixture
                .wal
                .read_records_range(&fixture.session, 0, None, None)
                .unwrap(),
        )
        .unwrap();
    let old = store
        .read_admission_snapshot(&fixture.session)
        .unwrap()
        .unwrap();
    let mut freeze = started(
        &fixture.session,
        Some(reserved("core")),
        Some(scope("core")),
    );
    if let WalRecord::RunStarted { run_id, .. } = &mut freeze {
        run_id.0 = "scope-freeze".into();
    }
    fixture.append(&[freeze, finished()]);
    let current = snapshot(&fixture);
    let foreign = reserved("foreign");
    let before = broca_core::scope_admission::decide_owner_access(
        old.checkpoint.as_ref().unwrap().ownership.as_ref().unwrap(),
        Some(&foreign),
        &SendScope::Unstamped,
    );
    let expected = broca_core::scope_admission::decide_owner_access(
        current
            .checkpoint
            .as_ref()
            .unwrap()
            .ownership
            .as_ref()
            .unwrap(),
        Some(&foreign),
        &SendScope::Unstamped,
    );
    assert_ne!(
        before, expected,
        "refusal must now name the newly frozen scope"
    );
    let wal = Arc::new(CountingWal {
        inner: fixture.wal.clone(),
        reads: Mutex::new(Vec::new()),
        fault: AtomicU64::new(0),
    });
    let registry =
        LineageRegistry::new_with_reader(Arc::new(NeverOpener), wal.clone(), Some(store));
    assert_eq!(
        decision(&registry, &fixture.session, Some(&foreign)).await,
        expected
    );
    assert!(
        wal.reads
            .lock()
            .unwrap()
            .iter()
            .all(|after| *after == old.watermark_seq),
        "only the suffix may be decoded"
    );
    assert_eq!(registry.slot_counts().open, 0);
}

#[tokio::test]
async fn ownership_tail_changes_an_unprotected_snapshot_to_protected() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let current = snapshot(&fixture);
    let mut empty = current.clone();
    empty.watermark_seq = 0;
    empty.anchor = None;
    empty.checkpoint = Some(broca_core::AdmissionCheckpointV1::from_frames(&[]).unwrap());
    let foreign = reserved("foreign");
    assert_eq!(
        broca_core::scope_admission::decide_owner_access(
            empty
                .checkpoint
                .as_ref()
                .unwrap()
                .ownership
                .as_ref()
                .unwrap(),
            Some(&foreign),
            &SendScope::Unstamped
        ),
        Ok(())
    );
    let (registry, _, _) = registry(&fixture, Ok(Some(empty)));
    let expected = broca_core::scope_admission::decide_owner_access(
        current
            .checkpoint
            .as_ref()
            .unwrap()
            .ownership
            .as_ref()
            .unwrap(),
        Some(&foreign),
        &SendScope::Unstamped,
    );
    assert!(expected.is_err());
    assert_eq!(
        decision(&registry, &fixture.session, Some(&foreign)).await,
        expected
    );
}

#[tokio::test]
async fn live_and_memory_ownership_do_not_hide_new_scope_records() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(&fixture.session, Some(reserved("core")), None),
        finished(),
    ]);
    let old = snapshot(&fixture);
    let (registry, wal, _) = registry(&fixture, Ok(Some(old.clone())));
    let head = install_live(
        &registry,
        &fixture.session,
        old.checkpoint.as_ref().unwrap(),
    );
    registry.inner.owner_snapshots.lock().unwrap().insert(
        fixture.session.clone(),
        OwnershipSnapshot::from_store(old.clone()).unwrap(),
    );
    let mut freeze = started(
        &fixture.session,
        Some(reserved("core")),
        Some(scope("core")),
    );
    if let WalRecord::RunStarted { run_id, .. } = &mut freeze {
        run_id.0 = "new-scope".into();
    }
    fixture.append(&[freeze, finished()]);
    let current = snapshot(&fixture);
    head.store(current.watermark_seq, Ordering::Release);
    let foreign = reserved("foreign");
    let expected = broca_core::scope_admission::decide_owner_access(
        current
            .checkpoint
            .as_ref()
            .unwrap()
            .ownership
            .as_ref()
            .unwrap(),
        Some(&foreign),
        &SendScope::Unstamped,
    );
    assert_eq!(
        decision(&registry, &fixture.session, Some(&foreign)).await,
        expected
    );
    assert!(wal
        .reads
        .lock()
        .unwrap()
        .iter()
        .all(|after| *after == old.watermark_seq));
}

#[tokio::test]
async fn ownership_rejects_stale_actor_generation_and_replaced_lineage_snapshot() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let current = snapshot(&fixture);
    let foreign = reserved("foreign");
    let expected = broca_core::scope_admission::decide_owner_access(
        current
            .checkpoint
            .as_ref()
            .unwrap()
            .ownership
            .as_ref()
            .unwrap(),
        Some(&foreign),
        &SendScope::Unstamped,
    );
    let (registry, wal, _) = registry(&fixture, Err("store unavailable".into()));
    install_live(
        &registry,
        &fixture.session,
        current.checkpoint.as_ref().unwrap(),
    );
    let mut stale = current.clone();
    stale.lineage_id = Some("replaced-lineage".into());
    let owner = stale
        .checkpoint
        .as_mut()
        .unwrap()
        .ownership
        .as_mut()
        .unwrap();
    owner.first_principal = Some(foreign.clone());
    owner.scope = None;
    registry.inner.live_ownership.lock().unwrap().insert(
        fixture.session.clone(),
        LiveOwnership {
            generation: 2,
            head: stale.watermark_seq,
            ownership: owner.for_owner_access(),
        },
    );
    registry.inner.owner_snapshots.lock().unwrap().insert(
        fixture.session.clone(),
        OwnershipSnapshot::from_store(stale).unwrap(),
    );
    assert_eq!(
        decision(&registry, &fixture.session, Some(&foreign)).await,
        expected
    );
    assert!(
        wal.reads.lock().unwrap().contains(&0),
        "a replaced lineage must fall back to the complete WAL"
    );
}

#[tokio::test]
async fn retained_ownership_is_not_persisted_as_a_content_checkpoint() {
    let fixture = Fixture::new();
    let mut start = started(
        &fixture.session,
        Some(reserved("basal")),
        Some(scope("core")),
    );
    if let WalRecord::RunStarted { retention, .. } = &mut start {
        *retention = Some(3600);
    }
    fixture.append(&[
        start,
        WalRecord::StepStarted {
            step_id: broca_protocol::StepId(1),
        },
        super::tests::assistant_record(),
        finished(),
    ]);
    let store = fixture.sqlite();
    store
        .project_range(
            &fixture.session,
            &fixture
                .wal
                .read_records_range(&fixture.session, 0, None, None)
                .unwrap(),
        )
        .unwrap();
    let metadata = store.read_admission_snapshot(&fixture.session).unwrap();
    assert!(
        metadata.is_none_or(|snapshot| snapshot.checkpoint.is_none()),
        "retained ownership must not persist a content-bearing checkpoint"
    );
    let registry =
        LineageRegistry::new_with_reader(Arc::new(NeverOpener), fixture.wal.clone(), Some(store));
    assert!(
        decision(&registry, &fixture.session, Some(&reserved("foreign")))
            .await
            .is_err()
    );
    assert_eq!(
        decision(&registry, &fixture.session, Some(&reserved("core"))).await,
        Ok(())
    );
}

#[tokio::test]
async fn budgeted_and_legacy_owner_snapshots_match_the_full_wal() {
    for first in [
        Some(reserved("basal")),
        None,
        Some(BindPrincipal::Direct),
        Some(BindPrincipal::Unverified),
    ] {
        let fixture = Fixture::new();
        let frozen = matches!(&first, Some(BindPrincipal::Reserved { .. })).then(|| scope("core"));
        let mut start = started(&fixture.session, first, frozen);
        let WalRecord::RunStarted { config, .. } = &mut start else {
            unreachable!()
        };
        config.budget = Some(broca_protocol::BudgetConfig {
            nudges: Vec::new(),
            finalize_at_tool_calls: 99,
            finalize_text: "finalize".into(),
        });
        fixture.append(&[start, finished()]);
        let current = snapshot(&fixture);
        let ownership = current
            .checkpoint
            .as_ref()
            .unwrap()
            .ownership
            .as_ref()
            .unwrap();
        let (registry, wal, _) = registry(&fixture, Ok(Some(current.clone())));
        for caller in [
            Some(reserved("core")),
            Some(reserved("foreign")),
            Some(BindPrincipal::Direct),
            None,
        ] {
            let expected = broca_core::scope_admission::decide_owner_access(
                ownership,
                caller.as_ref(),
                &SendScope::Unstamped,
            );
            assert_eq!(
                decision(&registry, &fixture.session, caller.as_ref()).await,
                expected
            );
        }
        assert!(
            wal.reads
                .lock()
                .unwrap()
                .iter()
                .all(|after| *after == current.watermark_seq),
            "a complete budget checkpoint must not require replaying its prefix"
        );
    }
}

#[tokio::test]
async fn owner_snapshot_cap_evicts_oldest_and_misses_recover_from_store_or_wal() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let projected = snapshot(&fixture);
    let cached = OwnershipSnapshot::from_store(projected.clone()).unwrap();
    for use_store in [true, false] {
        let source = use_store.then(|| projected.clone());
        let (registry, wal, store) = registry(&fixture, Ok(source));
        {
            let mut cache = registry.inner.owner_snapshots.lock().unwrap();
            cache.insert(fixture.session.clone(), cached.clone());
            for index in 0..MAX_OWNER_SNAPSHOTS {
                let mut key = fixture.session.clone();
                key.session = format!("filler-{index}");
                cache.insert(key, cached.clone());
            }
            assert_eq!(
                cache.entries.len(),
                MAX_OWNER_SNAPSHOTS,
                "owner snapshot entries must stay bounded"
            );
            assert_eq!(
                cache.oldest.len(),
                MAX_OWNER_SNAPSHOTS,
                "eviction ordering must also stay bounded"
            );
            assert!(
                cache.get(&fixture.session).is_none(),
                "oldest session must be evicted"
            );
            let mut oldest = fixture.session.clone();
            oldest.session = "filler-0".into();
            cache.insert(oldest.clone(), cached.clone());
            assert_eq!(
                cache.oldest.len(),
                MAX_OWNER_SNAPSHOTS,
                "updates must not add duplicate ordering entries"
            );
            let mut newest = fixture.session.clone();
            newest.session = "newest".into();
            cache.insert(newest, cached.clone());
            assert!(
                cache.get(&oldest).is_none(),
                "updating an entry preserves oldest-first eviction"
            );
        }
        let expected_owner = broca_core::scope_admission::decide_owner_access(
            &cached.ownership,
            Some(&reserved("core")),
            &SendScope::Unstamped,
        );
        let expected_foreign = broca_core::scope_admission::decide_owner_access(
            &cached.ownership,
            Some(&reserved("foreign")),
            &SendScope::Unstamped,
        );
        assert_eq!(
            decision(&registry, &fixture.session, Some(&reserved("core"))).await,
            expected_owner
        );
        assert_eq!(
            decision(&registry, &fixture.session, Some(&reserved("foreign"))).await,
            expected_foreign
        );
        assert!(
            store.reads.load(Ordering::Relaxed) > 0,
            "evicted entries must retry the store source"
        );
        let reads = wal.reads.lock().unwrap();
        if use_store {
            assert!(reads.iter().all(|after| *after == projected.watermark_seq));
        } else {
            assert!(
                reads.contains(&0),
                "a missing store snapshot must recover from the WAL"
            );
        }
    }
}

#[tokio::test]
async fn owner_caches_do_not_retain_first_episode_content() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let mut projected = snapshot(&fixture);
    let record: WalRecord = serde_json::from_str(include_str!(
        "../../broca-wal/tests/fixtures/plan_manifest_run_started.json"
    ))
    .unwrap();
    let WalRecord::RunStarted {
        manifest: Some(manifest),
        ..
    } = record
    else {
        panic!("fixture carries provider-supplied content")
    };
    let manifest = Arc::new(*manifest);
    let content = Arc::downgrade(&manifest);
    {
        let ownership = projected
            .checkpoint
            .as_mut()
            .unwrap()
            .ownership
            .as_mut()
            .unwrap();
        let first = ownership.first_episode.as_mut().unwrap();
        first.tool_names.push("provider-tool-content".into());
        first.manifest = Some(manifest.clone());
    }
    let checkpoint = projected.checkpoint.as_ref().unwrap();
    let ownership = checkpoint.ownership.as_ref().unwrap();
    let (registry, _, _) = registry(&fixture, Ok(None));
    install_live(&registry, &fixture.session, checkpoint);
    registry.inner.owner_snapshots.lock().unwrap().insert(
        fixture.session.clone(),
        OwnershipSnapshot {
            watermark_seq: projected.watermark_seq,
            anchor: projected.anchor,
            lineage_id: projected.lineage_id.clone(),
            ownership: ownership.clone(),
        },
    );
    drop(projected);
    drop(manifest);
    assert!(
        content.upgrade().is_none(),
        "owner caches must not keep provider content alive"
    );
    let live = registry.inner.live_ownership.lock().unwrap();
    assert!(live
        .get(&fixture.session)
        .unwrap()
        .ownership
        .first_episode
        .is_none());
    drop(live);
    let snapshots = registry.inner.owner_snapshots.lock().unwrap();
    assert!(snapshots
        .get(&fixture.session)
        .unwrap()
        .ownership
        .first_episode
        .is_none());
}

#[tokio::test]
async fn live_ownership_is_removed_on_close_and_late_publication_is_ignored() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let projected = snapshot(&fixture);
    let checkpoint = projected.checkpoint.as_ref().unwrap();
    for close in ["commit", "drop", "finalize", "abort"] {
        let (registry, _, _) = registry(&fixture, Ok(Some(projected.clone())));
        install_live(&registry, &fixture.session, checkpoint);
        registry.handle().finalize_exit(&fixture.session, 2);
        assert!(
            registry
                .inner
                .live_ownership
                .lock()
                .unwrap()
                .contains_key(&fixture.session),
            "a stale finalizer cannot erase live ownership"
        );
        if close == "finalize" {
            registry.handle().finalize_exit(&fixture.session, 3);
        } else {
            let handle = {
                let slots = registry.inner.slots.lock().unwrap();
                let Some(Entry::Open { handle, .. }) = slots.get(&fixture.session) else {
                    panic!("installed actor is open")
                };
                handle.clone()
            };
            let token = registry.handle().begin_close(&fixture.session, 3).unwrap();
            registry
                .handle()
                .note_session_ownership(&fixture.session, 2, checkpoint);
            match close {
                "commit" => token.commit(),
                "drop" => drop(token),
                "abort" => {
                    token.abort(handle);
                    assert!(
                        registry
                            .inner
                            .live_ownership
                            .lock()
                            .unwrap()
                            .contains_key(&fixture.session),
                        "an aborted close keeps the actor's ownership"
                    );
                    registry.handle().finalize_exit(&fixture.session, 3);
                }
                _ => unreachable!(),
            }
        }
        registry
            .handle()
            .note_session_ownership(&fixture.session, 3, checkpoint);
        assert!(
            !registry
                .inner
                .live_ownership
                .lock()
                .unwrap()
                .contains_key(&fixture.session),
            "closed actors and late publishers must not retain live ownership"
        );
        assert_eq!(
            decision(&registry, &fixture.session, Some(&reserved("core"))).await,
            Ok(())
        );
        assert!(
            decision(&registry, &fixture.session, Some(&reserved("foreign")))
                .await
                .is_err()
        );
        assert!(
            registry.inner.live_ownership.lock().unwrap().is_empty(),
            "read-only recovery must not resurrect an actor"
        );
    }
}

fn assert_transient(result: Result<(), crate::serve::ServeError>) {
    assert!(
        matches!(
            result,
            Err(crate::serve::ServeError::Open(
                _,
                Some(broca_protocol::ErrorClass::Transient)
            ))
        ),
        "expected transient storage refusal, got {result:?}"
    );
}

#[tokio::test]
async fn owner_barrier_failure_refuses_cancel_and_retract_without_writes() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let path = fixture.wal.wal_path(&fixture.session);
    let before = std::fs::read(&path).unwrap();
    for operation in ["cancel", "retract"] {
        for fault in [1, 3, 4, 5] {
            let (registry, wal, _) = registry(&fixture, Ok(None));
            wal.fault.store(fault, Ordering::Relaxed);
            assert_transient(
                registry
                    .authorize_session_owner(
                        &fixture.session,
                        Some(&reserved("stranger")),
                        &SendScope::Unstamped,
                    )
                    .await,
            );
            assert_eq!(
                std::fs::read(&path).unwrap(),
                before,
                "{operation}: no writes on refusal"
            );
            wal.fault.store(0, Ordering::Relaxed);
            assert!(matches!(
                registry
                    .authorize_session_owner(
                        &fixture.session,
                        Some(&reserved("stranger")),
                        &SendScope::Unstamped
                    )
                    .await,
                Err(crate::serve::ServeError::ScopeRefused(_))
            ));
            registry
                .authorize_session_owner(
                    &fixture.session,
                    Some(&reserved("core")),
                    &SendScope::Unstamped,
                )
                .await
                .unwrap();
            let fresh = BindIdentity::new(fixture.root.join("project"), "runner", "absent");
            registry
                .authorize_session_owner(&fresh, Some(&reserved("stranger")), &SendScope::Unstamped)
                .await
                .unwrap();
        }
    }
}

#[tokio::test]
async fn advanced_owner_head_refuses_failed_suffix_and_full_revalidation() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(
            &fixture.session,
            Some(reserved("basal")),
            Some(scope("core")),
        ),
        finished(),
    ]);
    let (registry, wal, _) = registry(&fixture, Ok(None));
    let checkpoint = snapshot(&fixture).checkpoint.unwrap();
    registry.owner_ownership(&fixture.session).await.unwrap();
    let head = install_live(&registry, &fixture.session, &checkpoint);
    assert!(matches!(
        registry
            .authorize_session_owner(
                &fixture.session,
                Some(&reserved("stranger")),
                &SendScope::Unstamped
            )
            .await,
        Err(crate::serve::ServeError::ScopeRefused(_))
    ));
    registry.owner_ownership(&fixture.session).await.unwrap();
    fixture.append(&[super::tests::assistant_record()]);
    head.fetch_add(1, Ordering::Release);
    wal.fault.store(2, Ordering::Relaxed);
    let before = std::fs::read(fixture.wal.wal_path(&fixture.session)).unwrap();
    assert_transient(
        registry
            .authorize_session_owner(
                &fixture.session,
                Some(&reserved("stranger")),
                &SendScope::Unstamped,
            )
            .await,
    );
    assert!(
        wal.reads.lock().unwrap().contains(&0),
        "full revalidation attempted"
    );
    assert_eq!(
        std::fs::read(fixture.wal.wal_path(&fixture.session)).unwrap(),
        before
    );
    wal.fault.store(0, Ordering::Relaxed);
    registry
        .authorize_session_owner(
            &fixture.session,
            Some(&reserved("core")),
            &SendScope::Unstamped,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn cold_flow_sender_refuses_degraded_read_but_warm_sender_remains_authoritative() {
    let fixture = Fixture::new();
    fixture.append(&[
        started(&fixture.session, Some(reserved("core")), None),
        finished(),
    ]);
    let (registry, wal, _) = registry(&fixture, Ok(None));
    wal.fault.store(1, Ordering::Relaxed);
    assert_transient(
        registry
            .authorize_session(&fixture.session, Some(&reserved("basal")))
            .await,
    );
    wal.fault.store(0, Ordering::Relaxed);
    assert!(matches!(
        registry
            .authorize_session(&fixture.session, Some(&reserved("basal")))
            .await,
        Err(crate::serve::ServeError::ScopeRefused(_))
    ));
    wal.fault.store(1, Ordering::Relaxed);
    assert!(matches!(
        registry
            .authorize_session(&fixture.session, Some(&reserved("basal")))
            .await,
        Err(crate::serve::ServeError::ScopeRefused(_))
    ));
    let fresh = BindIdentity::new(fixture.root.join("project"), "runner", "absent");
    registry
        .authorize_session(&fresh, Some(&reserved("basal")))
        .await
        .unwrap();
}
