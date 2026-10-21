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
        interrupt_reason: None,
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
async fn session_read_rejects_equal_watermark_prefix_after_wal_recreation() {
    recreated_prefix_session_read(false, false).await;
}

#[tokio::test]
async fn session_read_replays_and_repairs_unidentified_prefix() {
    recreated_prefix_session_read(true, false).await;
}

#[tokio::test]
async fn session_read_model_replays_and_repairs_unidentified_prefix() {
    recreated_prefix_session_read(true, true).await;
}

async fn recreated_prefix_session_read(unidentified: bool, model: bool) {
    let fixture = Fixture::new();
    let mut old = started(&fixture.session, None, None);
    if let WalRecord::RunStarted { input, .. } = &mut old {
        *input = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text {
                text: "old conversation".into(),
            }],
        )]
        .into();
    }
    fixture.append(&[
        old,
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
    let stale = store.read_prefix_with_watermark(&fixture.session).unwrap();
    if unidentified {
        let conn = rusqlite::Connection::open(fixture.root.join("store.db")).unwrap();
        conn.execute("UPDATE meta SET lineage_id=NULL", []).unwrap();
    }
    let stale_prefix = store.read_prefix_with_watermark(&fixture.session).unwrap();
    assert_eq!(stale_prefix.lineage_id.is_none(), unidentified);
    std::fs::remove_file(fixture.wal.wal_path(&fixture.session)).unwrap();
    let mut new = started(&fixture.session, None, None);
    if let WalRecord::RunStarted { input, .. } = &mut new {
        *input = vec![broca_protocol::Message::new(
            broca_protocol::Role::User,
            vec![broca_protocol::ContentBlock::Text {
                text: "new conversation".into(),
            }],
        )]
        .into();
    }
    fixture.append(&[new, super::tests::assistant_record(), finished()]);
    let replacement = fixture
        .wal
        .read_records_range(&fixture.session, 0, None, None)
        .unwrap();
    assert_eq!(stale.watermark_seq, 3);
    assert_eq!(fixture.wal.read_records(&fixture.session).unwrap().len(), 3);
    let lineage = replacement.lineage_id.unwrap();
    assert_ne!(
        stale.lineage_id.as_deref(),
        Some(lineage.to_string().as_str())
    );
    assert_eq!(
        store.read_prefix_with_watermark(&fixture.session).unwrap(),
        stale_prefix
    );

    let registry = LineageRegistry::new_with_reader(
        Arc::new(NeverOpener),
        fixture.wal.clone(),
        Some(store.clone()),
    );
    if model {
        let page = registry
            .route_read_model_page(&fixture.session, &broca_core::ModelPageRequest::default())
            .await
            .unwrap();
        assert_eq!(page.lineage_id, Some(lineage.to_string()));
        assert_eq!(
            page.messages[0].message.content,
            vec![broca_protocol::ContentBlock::Text {
                text: "new conversation".into()
            }]
        );
        assert_eq!(
            store
                .read_prefix_with_watermark(&fixture.session)
                .unwrap()
                .lineage_id,
            Some(lineage.to_string()),
            "model reads must persist repaired lineage too"
        );
    }
    let page = registry
        .route_read_page(&fixture.session, Some(0), Some(100), true)
        .await
        .unwrap()
        .page;
    assert_eq!(page.lineage_id, Some(lineage));
    assert_eq!(page.messages.len(), 2);
    assert_eq!(
        page.messages[0].message.content,
        vec![broca_protocol::ContentBlock::Text {
            text: "new conversation".into()
        }]
    );
    assert_eq!(
        store
            .read_prefix_with_watermark(&fixture.session)
            .unwrap()
            .lineage_id,
        Some(lineage.to_string())
    );
    let repeated = registry
        .route_read_page(&fixture.session, Some(0), Some(100), true)
        .await
        .unwrap()
        .page;
    assert_eq!(repeated.messages, page.messages);
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

#[cfg(unix)]
#[tokio::test]
async fn bind_and_owner_checks_reuse_large_wal_ownership_without_prefix_reads() {
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
        if let WalRecord::ModelStepFinished {
            step_id,
            assistant_message,
            ..
        } = &mut record
        {
            *step_id = broca_protocol::StepId(step);
            *assistant_message = Arc::new(broca_protocol::AssistantMessage {
                message_id: broca_protocol::MessageId(format!("m{step}")),
                content: vec![broca_protocol::ContentBlock::Text {
                    text: "x".repeat(61_000),
                }],
            });
        }
        records.push(record);
    }
    fixture.append(&records);
    let stored = snapshot(&fixture);
    fixture.append(&[finished()]);
    let current = snapshot(&fixture);
    let expected = current
        .checkpoint
        .as_ref()
        .unwrap()
        .ownership
        .as_ref()
        .unwrap();
    let bytes = std::fs::metadata(fixture.wal.wal_path(&fixture.session))
        .unwrap()
        .len();
    assert!(
        (15_000_000..17_000_000).contains(&bytes),
        "WAL bytes: {bytes}"
    );

    for source in ["live_cold", "closed_cold", "store", "live_cached"] {
        let (registry, wal, _) =
            registry(&fixture, Ok((source == "store").then(|| stored.clone())));
        if source.starts_with("live") {
            install_live(
                &registry,
                &fixture.session,
                current.checkpoint.as_ref().unwrap(),
            );
            if source == "live_cold" {
                registry
                    .inner
                    .live_ownership
                    .lock()
                    .unwrap()
                    .remove(&fixture.session);
            } else {
                let slots = registry.inner.slots.lock().unwrap();
                if let Some(Entry::Open { handle, .. }) = slots.get(&fixture.session) {
                    handle
                        .first_episode
                        .set(expected.first_episode.clone().unwrap())
                        .unwrap();
                }
            }
        }
        assert_eq!(
            registry.owner_ownership(&fixture.session).await.unwrap(),
            expected.for_owner_access(),
            "{source}"
        );
        // Without a live actor, the lookup decodes the WAL, flushes the WAL to make the
        // observed head durable, then rereads exactly that prefix. A live actor supplies
        // an already-durable head, so the lookup needs only one decode.
        let cold_full_reads = wal
            .requests
            .lock()
            .unwrap()
            .iter()
            .filter(|(after, through)| {
                *after == 0 && through.is_none_or(|seq| seq == current.watermark_seq)
            })
            .count();
        assert_eq!(
            cold_full_reads,
            match source {
                "live_cold" => 1,
                "closed_cold" => 2,
                _ => 0,
            },
            "{source}"
        );

        for _ in 0..3 {
            wal.requests.lock().unwrap().clear();
            wal.decoded_seqs.lock().unwrap().clear();
            assert_eq!(
                registry
                    .session_ownership_for_bind(&fixture.session)
                    .await
                    .as_ref(),
                Some(expected),
                "{source}"
            );
            assert_eq!(
                registry.owner_ownership(&fixture.session).await.unwrap(),
                expected.for_owner_access(),
                "{source}"
            );
            let requests = wal.requests.lock().unwrap();
            assert!(requests.iter().all(|(after, through)| {
                *after >= current.watermark_seq - 1 || (*after == 0 && *through == Some(1))
            }), "only the final-frame suffix or the bounded first episode may be read: {source}: {requests:?}");
            let decoded = wal.decoded_seqs.lock().unwrap();
            assert!(
                decoded
                    .iter()
                    .all(|seq| *seq == 1 || *seq >= current.watermark_seq),
                "decoded an unchanged prefix: {source}: {decoded:?}"
            );
            if source == "live_cached" {
                assert!(requests.is_empty(), "live cache must need no WAL reads");
            }
        }
    }
}

fn plan_manifest(identity: &str, transforms: bool) -> Box<broca_wal::PlanManifest> {
    Box::new(broca_wal::PlanManifest {
        plan: broca_wal::OpaqueJson::from_json_text(if transforms {
            serde_json::json!({"step_transform_items": [{
                "provider": "p",
                "subscriptions": [{"hook": "pre_tool", "phase": "approve", "ops": [],
                    "on_unavailable": "refuse", "budget_ms": 1000}]
            }]})
            .to_string()
        } else {
            "{}".into()
        })
        .unwrap(),
        plan_identity: identity.into(),
        composition_digest: String::new(),
        fetches: vec![],
        tools: Arc::new(vec![]),
        system_text: String::new(),
        system_digest: String::new(),
        join_version: 1,
        role_versions: Default::default(),
        declarations: vec![],
    })
}

fn plan_start(fixture: &Fixture, identity: &str, transforms: bool) -> WalRecord {
    let mut start = started(
        &fixture.session,
        Some(reserved("core")),
        Some(scope("core")),
    );
    if let WalRecord::RunStarted { manifest, .. } = &mut start {
        *manifest = Some(plan_manifest(identity, transforms));
    }
    start
}

#[tokio::test]
async fn bind_sources_preserve_frozen_plan_hooks_and_approval_answers() {
    let fixture = Fixture::new();
    fixture.append(&[plan_start(&fixture, "owner-test-plan", true)]);
    let stored = snapshot(&fixture);
    fixture.append(&[finished()]);
    let current = snapshot(&fixture);
    for source in ["live", "store", "anchored_frame"] {
        let (registry, _, _) = registry(&fixture, Ok((source == "store").then(|| stored.clone())));
        if source == "live" {
            install_live(
                &registry,
                &fixture.session,
                current.checkpoint.as_ref().unwrap(),
            );
            let slots = registry.inner.slots.lock().unwrap();
            if let Some(Entry::Open { handle, .. }) = slots.get(&fixture.session) {
                handle
                    .first_episode
                    .set(
                        current
                            .checkpoint
                            .as_ref()
                            .unwrap()
                            .ownership
                            .as_ref()
                            .unwrap()
                            .first_episode
                            .clone()
                            .unwrap(),
                    )
                    .unwrap();
            }
        } else if source == "anchored_frame" {
            registry.owner_ownership(&fixture.session).await.unwrap();
        }
        for _ in 0..2 {
            assert_eq!(
                registry.plan_state(&fixture.session).await,
                crate::serve::PlanState::Admitted {
                    frozen: Some("owner-test-plan".into())
                },
                "{source}"
            );
            assert!(
                registry.frozen_plan_requires_hooks(&fixture.session).await,
                "{source}"
            );
            assert!(
                registry
                    .frozen_plan_requires_approval(&fixture.session)
                    .await,
                "{source}"
            );
            assert_eq!(
                registry
                    .session_ownership_for_bind(&fixture.session)
                    .await
                    .as_ref(),
                current.checkpoint.as_ref().unwrap().ownership.as_ref(),
                "{source}"
            );
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn stale_first_episode_positions_revalidate_recreated_or_truncated_wals() {
    for replacement in ["recreated", "truncated"] {
        let fixture = Fixture::new();
        fixture.append(&[plan_start(&fixture, "old-plan", false), finished()]);
        let (registry, wal, _) = registry(&fixture, Ok(None));
        registry.owner_ownership(&fixture.session).await.unwrap();
        let path = fixture.wal.wal_path(&fixture.session);
        if replacement == "recreated" {
            std::fs::remove_file(&path).unwrap();
        } else {
            // Keep the inode and lineage header, but replace every record after it.
            // A saved byte position is not proof that the replacement frame is the original,
            // even though both frames have the same sequence number.
            let bytes = std::fs::read(&path).unwrap();
            let header_end =
                broca_wal::HEADER_LEN + u32::from_le_bytes(bytes[..4].try_into().unwrap()) as usize;
            std::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .unwrap()
                .set_len(header_end as u64)
                .unwrap();
        }
        fixture.append(&[
            plan_start(
                &fixture,
                "replacement-plan-with-a-different-frame-length",
                true,
            ),
            finished(),
        ]);
        wal.requests.lock().unwrap().clear();
        assert_eq!(
            registry.plan_state(&fixture.session).await,
            crate::serve::PlanState::Admitted {
                frozen: Some("replacement-plan-with-a-different-frame-length".into())
            },
            "{replacement}"
        );
        assert!(wal.requests.lock().unwrap().contains(&(0, None)),
            "a stale first-frame anchor or lineage must fall back to full revalidation: {replacement}");
        assert!(registry.frozen_plan_requires_hooks(&fixture.session).await);
        assert!(
            registry
                .frozen_plan_requires_approval(&fixture.session)
                .await
        );
    }
}

#[tokio::test]
async fn bind_unknown_history_is_not_reported_as_unowned() {
    let fixture = Fixture::new();
    fixture.append(&[plan_start(&fixture, "plan", true), finished()]);
    let projected = snapshot(&fixture);
    let (registry, wal, _) = registry(&fixture, Ok(Some(projected)));
    wal.fault.store(2, Ordering::Relaxed);
    assert!(registry
        .session_ownership_for_bind(&fixture.session)
        .await
        .is_none());

    let (registry, _, _) = self::registry(&fixture, Err("store unavailable".into()));
    let path = fixture.wal.wal_path(&fixture.session);
    std::fs::write(&path, []).unwrap();
    assert!(registry
        .session_ownership_for_bind(&fixture.session)
        .await
        .is_none());
    let registry = LineageRegistry::new(Arc::new(NeverOpener));
    assert!(registry
        .session_ownership_for_bind(&fixture.session)
        .await
        .is_none());
}

struct CountingWal {
    inner: Arc<FileWalStore>,
    reads: Mutex<Vec<u64>>,
    decoded_seqs: Mutex<Vec<u64>>,
    requests: Mutex<Vec<(u64, Option<u64>)>>,
    fault: AtomicU64,
}

impl WalStore for CountingWal {
    fn check_move(&self, session: &LineageKey, write: bool) -> Result<(), broca_wal::WalError> {
        self.inner.check_move(session, write)
    }

    fn open(&self, _: &LineageKey) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        panic!("authorization never opens a WAL writer")
    }

    fn read_records(&self, session: &LineageKey) -> Result<Vec<FramedRecord>, broca_wal::WalError> {
        self.reads.lock().unwrap().push(0);
        self.requests.lock().unwrap().push((0, None));
        let records = self.inner.read_records(session)?;
        self.decoded_seqs
            .lock()
            .unwrap()
            .extend(records.iter().map(|frame| frame.seq));
        Ok(records)
    }

    fn read_records_range(
        &self,
        session: &LineageKey,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.reads.lock().unwrap().push(after);
        self.requests.lock().unwrap().push((after, through));
        let fault = self.fault.load(Ordering::Relaxed);
        let suffix_fault = after > 0 && matches!(fault, 6 | 8);
        let first_fault = after == 0 && through == Some(1) && matches!(fault, 7 | 9);
        if (suffix_fault || first_fault)
            && self
                .fault
                .compare_exchange(fault, 0, Ordering::Relaxed, Ordering::Relaxed)
                .is_ok()
        {
            // Fail only this read. A retry from another snapshot would succeed and would
            // hide the failed read, which the refusal tests below must detect.
            return if fault >= 8 {
                Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()))
            } else {
                Err(broca_wal::WalError::Io(std::io::Error::other(
                    "injected one-shot ownership read failure",
                )))
            };
        }
        if self.fault.load(Ordering::Relaxed) == 2 {
            return Ok(broca_wal::WalRangeRead::without_anchors(Vec::new()));
        }
        if self.fault.load(Ordering::Relaxed) == 5 && through.is_some() {
            return self
                .inner
                .read_records_range(session, after, Some(after), anchor);
        }
        let range = self
            .inner
            .read_records_range(session, after, through, anchor)?;
        if let broca_wal::WalRangeRecords::Complete(records) = range.records() {
            self.decoded_seqs
                .lock()
                .unwrap()
                .extend(records.iter().map(|frame| frame.seq));
        }
        Ok(range)
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
        decoded_seqs: Mutex::new(Vec::new()),
        requests: Mutex::new(Vec::new()),
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
        decoded_seqs: Mutex::new(Vec::new()),
        requests: Mutex::new(Vec::new()),
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
            max_input_tokens: None,
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
            first_episode_position: None,
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

#[tokio::test]
async fn ownership_suffix_read_failure_never_tries_another_snapshot() {
    for fault in [6, 8] {
        for bind in [false, true] {
            let fixture = Fixture::new();
            fixture.append(&[plan_start(&fixture, "plan", true), finished()]);
            let projected = snapshot(&fixture);
            let (registry, wal, store) = registry(&fixture, Ok(None));
            registry.owner_ownership(&fixture.session).await.unwrap();
            *store.snapshot.lock().unwrap() = Ok(Some(projected));
            store.reads.store(0, Ordering::Relaxed);
            wal.requests.lock().unwrap().clear();
            wal.fault.store(fault, Ordering::Relaxed);
            if bind {
                assert!(
                    registry
                        .session_ownership_for_bind(&fixture.session)
                        .await
                        .is_none(),
                    "a failed suffix cannot fall through to a different snapshot: fault {fault}"
                );
            } else {
                assert!(
                    registry.owner_ownership(&fixture.session).await.is_err(),
                    "a failed suffix cannot fall through to a different snapshot: fault {fault}"
                );
            }
            assert_eq!(store.reads.load(Ordering::Relaxed), 0);
            assert_eq!(wal.requests.lock().unwrap().len(), 1);
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn ownership_first_episode_read_failure_never_tries_another_snapshot() {
    for fault in [7, 9] {
        let fixture = Fixture::new();
        fixture.append(&[plan_start(&fixture, "plan", true), finished()]);
        let projected = snapshot(&fixture);
        let (registry, wal, store) = registry(&fixture, Ok(None));
        registry.owner_ownership(&fixture.session).await.unwrap();
        *store.snapshot.lock().unwrap() = Ok(Some(projected));
        store.reads.store(0, Ordering::Relaxed);
        wal.requests.lock().unwrap().clear();
        wal.fault.store(fault, Ordering::Relaxed);
        assert!(registry.session_ownership_for_bind(&fixture.session).await.is_none(),
            "a failed first-episode read cannot fall through to a different snapshot: fault {fault}");
        assert_eq!(store.reads.load(Ordering::Relaxed), 0);
        assert_eq!(wal.requests.lock().unwrap().len(), 2);
    }
}

#[tokio::test]
async fn move_fenced_ownership_never_reuses_a_corrupt_cached_prefix() {
    for fence in ["moving", "moved"] {
        for source in ["memory", "store", "live"] {
            let fixture = Fixture::new();
            fixture.append(&[plan_start(&fixture, "plan", true), finished()]);
            let projected = snapshot(&fixture);
            let (registry, wal, _) =
                registry(&fixture, Ok((source == "store").then(|| projected.clone())));
            if source == "live" {
                install_live(
                    &registry,
                    &fixture.session,
                    projected.checkpoint.as_ref().unwrap(),
                );
            } else if source == "memory" {
                registry
                    .session_ownership_for_bind(&fixture.session)
                    .await
                    .unwrap();
            }
            let path = fixture.wal.wal_path(&fixture.session);
            if fence == "moving" {
                broca_wal::migration::atomic_json(
                    &fixture.root,
                    &path.with_extension("moving"),
                    &broca_wal::migration::Freeze {
                        cut_id: "cut-a".into(),
                        end_offset: 0,
                        lineage_id: "lineage".into(),
                    },
                )
                .unwrap();
            } else {
                broca_wal::migration::atomic_json(
                    &fixture.root,
                    &path.with_extension("moved"),
                    &broca_wal::migration::Moved {
                        cut_id: "cut-a".into(),
                        holder: "remote".into(),
                        lineage_id: "lineage".into(),
                    },
                )
                .unwrap();
            }
            let mut bytes = std::fs::read(&path).unwrap();
            bytes[0] ^= 0xff;
            std::fs::write(&path, &bytes).unwrap();
            wal.requests.lock().unwrap().clear();
            assert!(
                registry.owner_ownership(&fixture.session).await.is_err(),
                "{source}"
            );
            assert!(
                registry
                    .session_ownership_for_bind(&fixture.session)
                    .await
                    .is_none(),
                "{source}"
            );
            assert!(wal
                .requests
                .lock()
                .unwrap()
                .iter()
                .all(|(after, _)| *after == 0));
            assert_eq!(
                std::fs::read(&path).unwrap(),
                bytes,
                "readers cannot repair corruption"
            );
        }
    }
}
