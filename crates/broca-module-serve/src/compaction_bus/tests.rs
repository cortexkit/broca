use super::*;
use broca_core::{FileWalStore, WalStore};
use broca_protocol::{BindIdentity, Message as NativeMessage, Role};
use broca_wal::{CompactionMessageKind, CompactionRange, WalRecord};

struct CountingWal {
    inner: Arc<FileWalStore>,
    full: AtomicU64,
    ranges: AtomicU64,
    tails: AtomicU64,
}
impl WalStore for CountingWal {
    fn open(
        &self,
        session: &BindIdentity,
    ) -> Result<Box<dyn broca_wal::Wal + Send>, broca_wal::WalError> {
        self.inner.open(session)
    }
    fn read_records(
        &self,
        session: &BindIdentity,
    ) -> Result<Vec<broca_wal::FramedRecord>, broca_wal::WalError> {
        self.full.fetch_add(1, Ordering::Relaxed);
        self.inner.read_records(session)
    }
    fn read_records_range(
        &self,
        session: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.ranges.fetch_add(1, Ordering::Relaxed);
        self.inner
            .read_records_range(session, after, through, anchor)
    }
    fn read_records_tail_bounded(
        &self,
        session: &BindIdentity,
        bytes: u64,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        self.tails.fetch_add(1, Ordering::Relaxed);
        self.inner.read_records_tail_bounded(session, bytes)
    }
    fn file_identity(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.file_identity(session)
    }
    fn live_wal_bytes(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.live_wal_bytes(session)
    }
    fn flush_for_read(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.flush_for_read(session)
    }
}

struct NoActor;
#[async_trait::async_trait]
impl broca_module::registry::LineageOpener for NoActor {
    async fn open(
        &self,
        _: &BindIdentity,
        _: u64,
        _: broca_module::registry::RegistryHandle,
    ) -> Result<broca_module::registry::ActorHandle, broca_module::registry::OpenError> {
        Err(broca_module::registry::OpenError::Open(
            "lookup unexpectedly opened an actor".into(),
        ))
    }
}
struct NoConnector;
#[async_trait::async_trait]
impl BusConnector for NoConnector {
    async fn connect(&self) -> Result<crate::bus_events::BusConnection, String> {
        Err("not attached in lookup test".into())
    }
}

fn compaction_record(kind: CompactionMessageKind, text: &str) -> WalRecord {
    let replacement = vec![NativeMessage::new(
        Role::User,
        vec![broca_protocol::ContentBlock::Text { text: text.into() }],
    )];
    WalRecord::CompactionMessage {
        provider: "magic-context".into(),
        kind,
        request_id: None,
        cursor: None,
        compaction_id: "fixture".into(),
        version: 1,
        range: CompactionRange { from: 0, to: 0 },
        sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
        replacement: Arc::new(replacement),
        call_when: None,
        stability: None,
        ceiling: None,
        tag_checkpoint: None,
        ride_wait_ms: None,
        view_features: None,
        view_due: None,
    }
}
fn message(session: &BindIdentity, version: u64) -> cortexkit_bus_trait::Message {
    let ready = ViewReady::new(
        broca_core::agent_session_ref(session),
        version,
        0,
        "a".repeat(64),
    );
    cortexkit_bus_trait::Message {
        subject: event_subject("box_test", "magic-context", VIEW_READY_EVENT),
        id: ready.message_id(),
        digest: ready.digest.parse().unwrap(),
        headers: ready.to_headers(),
    }
}

struct Fixture {
    store: Arc<SqliteStore>,
    wal: Arc<CountingWal>,
    session: BindIdentity,
    writer: Box<dyn broca_wal::Wal + Send>,
    service: Service,
}
impl Fixture {
    fn new(name: &str) -> Self {
        use broca_core::SessionRefIndex;
        let root = broca_test_support::scratch_root().join(name);
        std::fs::create_dir_all(&root).unwrap();
        let store = Arc::new(SqliteStore::open(root.join("run-index.db")).unwrap());
        let inner = Arc::new(FileWalStore::new(root.join("wal")));
        let session = BindIdentity::new("/fixture", "runner", name);
        store
            .record_session(&broca_core::session_addr(&session), &session)
            .unwrap();
        let mut writer = inner.open(&session).unwrap();
        writer
            .append(
                1,
                &compaction_record(CompactionMessageKind::Setup, "head"),
                true,
            )
            .unwrap();
        let padding = compaction_record(CompactionMessageKind::Step, &"history ".repeat(8192));
        for _ in 0..200 {
            writer.append(1, &padding, false).unwrap();
        }
        writer.sync().unwrap();
        assert!(inner.live_wal_bytes(&session).unwrap().unwrap() > 10 * 1024 * 1024);
        let range = inner.read_records_range(&session, 0, None, None).unwrap();
        broca_core::Persister::project_range(store.as_ref(), &session, &range).unwrap();
        assert!(store.compaction_facts(&session).unwrap().unwrap().setup);
        let wal = Arc::new(CountingWal {
            inner,
            full: AtomicU64::new(0),
            ranges: AtomicU64::new(0),
            tails: AtomicU64::new(0),
        });
        let registry = LineageRegistry::new_with_reader(Arc::new(NoActor), wal.clone(), None);
        let service = Service::new(
            store.clone(),
            registry,
            wal.clone(),
            Arc::default(),
            Arc::new(NoConnector),
            CancellationToken::new(),
        );
        Self {
            store,
            wal,
            session,
            writer,
            service,
        }
    }
    #[cfg(any(unix, windows))]
    fn notice(&mut self, applied: Option<u64>, refused: Option<u64>) {
        use broca_core::compaction_provider::notice::{AdvanceNotice, Event};
        let mut notice = AdvanceNotice::new(
            broca_core::agent_session_ref(&self.session),
            1,
            self.writer.lineage_id().unwrap().to_string(),
            0,
            "b".repeat(64),
            10,
            "m",
        );
        notice.applied_view_version = applied;
        if let Some(version) = refused {
            notice.events.push(Event::ViewRefused {
                view_version: version,
                reason: "provider_constraint".into(),
            });
        }
        self.writer
            .append(
                1,
                &WalRecord::CompactionNotice {
                    seq: 1,
                    headers: notice.to_headers(),
                },
                true,
            )
            .unwrap();
    }
}

#[tokio::test]
#[cfg(any(unix, windows))]
async fn twenty_announcements_and_reconcile_passes_do_zero_full_reads_of_a_large_log() {
    let fixture = Fixture::new("compaction-large-log-lookup");
    for version in 1..=20 {
        fixture
            .service
            .handle(&message(&fixture.session, version), "box_test")
            .await
            .unwrap();
    }
    for version in 21..=25 {
        let receipt = ViewReceipt {
            ready: ViewReady::new(
                broca_core::agent_session_ref(&fixture.session),
                version,
                0,
                "c".repeat(64),
            ),
            lineage_id: "retired-lineage".into(),
            received_at_ms: 1,
        };
        fixture.store.receive_compaction_view(&receipt).unwrap();
        fixture.service.reconcile_one(receipt).await.unwrap();
    }
    assert_eq!(
        fixture.wal.full.load(Ordering::Relaxed),
        0,
        "compaction lookup decoded full history"
    );
    assert_eq!(
        fixture.wal.ranges.load(Ordering::Relaxed),
        0,
        "compaction lookup used a range with a full-read fallback"
    );
    assert_eq!(
        fixture.wal.tails.load(Ordering::Relaxed),
        0,
        "current projection unnecessarily read the WAL tail"
    );
}

#[tokio::test]
#[cfg(any(unix, windows))]
async fn behind_projection_folds_one_bounded_suffix_and_caches_applied_and_refused_versions() {
    let mut fixture = Fixture::new("compaction-behind-lookup");
    fixture.notice(Some(19), Some(20));
    for version in 1..=20 {
        fixture
            .service
            .handle(&message(&fixture.session, version), "box_test")
            .await
            .unwrap();
    }
    assert!(
        fixture
            .store
            .compaction_receipts("", 256)
            .unwrap()
            .is_empty(),
        "resolved announcements wrote receipts"
    );
    assert_eq!(fixture.wal.full.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.wal.ranges.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.wal.tails.load(Ordering::Relaxed), 1);
    let facts = fixture
        .store
        .compaction_facts(&fixture.session)
        .unwrap()
        .unwrap();
    assert_eq!((facts.noticed, facts.refused), (19, 20));
}

#[tokio::test]
async fn oversized_tail_returns_unknown_without_any_full_read_fallback() {
    let mut fixture = Fixture::new("compaction-oversized-lookup");
    fixture
        .writer
        .append(
            1,
            &compaction_record(
                CompactionMessageKind::Step,
                &"x".repeat(FACTS_TAIL_BYTES as usize * 2),
            ),
            true,
        )
        .unwrap();
    let error = fixture
        .service
        .handle(&message(&fixture.session, 1), "box_test")
        .await
        .unwrap_err();
    assert!(error.contains("bounded WAL suffix"), "{error}");
    assert_eq!(fixture.wal.full.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.wal.ranges.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.wal.tails.load(Ordering::Relaxed), 1);
    assert!(fixture
        .store
        .compaction_receipts("", 256)
        .unwrap()
        .is_empty());
}

#[tokio::test]
#[cfg(any(unix, windows))]
async fn replaced_file_identity_invalidates_cached_facts_without_a_full_read() {
    let fixture = Fixture::new("compaction-replaced-lookup");
    let path = fixture
        .wal
        .inner
        .resolve_live_path(&fixture.session)
        .unwrap()
        .unwrap();
    let replacement = path.with_extension("replacement");
    std::fs::copy(&path, &replacement).unwrap();
    drop(fixture.writer);
    std::fs::rename(replacement, path).unwrap();
    for version in 1..=2 {
        fixture
            .service
            .handle(&message(&fixture.session, version), "box_test")
            .await
            .unwrap();
    }
    assert_eq!(fixture.wal.full.load(Ordering::Relaxed), 0);
    assert_eq!(fixture.wal.ranges.load(Ordering::Relaxed), 0);
    assert_eq!(
        fixture.wal.tails.load(Ordering::Relaxed),
        1,
        "replacement reused an old file proof"
    );
    let facts = fixture
        .store
        .compaction_facts(&fixture.session)
        .unwrap()
        .unwrap();
    assert_eq!(
        facts.file_id,
        fixture.wal.file_identity(&fixture.session).unwrap()
    );
}

/// Hands out queued `view_ready` messages, answers "nothing waiting" once they are gone,
/// and checks at each ACK that the durable receipt and the inbox already hold the view.
struct QueuedCursor {
    queue: std::collections::VecDeque<cortexkit_bus_trait::Message>,
    in_flight: Option<ViewReady>,
    store: Arc<SqliteStore>,
    inbox: Arc<broca_core::ViewReadyInbox>,
    acked: Arc<Mutex<Vec<u64>>>,
    all_acked: Arc<tokio::sync::Notify>,
    expected: usize,
}
#[async_trait::async_trait]
impl StreamCursor for QueuedCursor {
    async fn next(
        &mut self,
    ) -> cortexkit_bus_trait::BusResult<Option<cortexkit_bus_trait::StreamDelivery>> {
        assert!(
            self.in_flight.is_none(),
            "next() before the delivery settled"
        );
        let Some(message) = self.queue.pop_front() else {
            return Ok(None);
        };
        self.in_flight = Some(ViewReady::from_headers(&message.headers).unwrap());
        Ok(Some(cortexkit_bus_trait::StreamDelivery {
            stream_seq: self.expected as u64 - self.queue.len() as u64,
            delivery_count: 1,
            message,
        }))
    }
    async fn ack(&mut self) -> cortexkit_bus_trait::BusResult<()> {
        use broca_core::CompactionViews;
        let ready = self.in_flight.take().expect("ACK without a delivery");
        let receipts = self.store.compaction_receipts("", 256).unwrap();
        assert_eq!(
            receipts.first().map(|r| &r.ready),
            Some(&ready),
            "ACK preceded the durable receipt"
        );
        assert_eq!(
            self.inbox.newest_ready(&ready.session_ref, "ignored", None),
            Some(ready.clone()),
            "ACK preceded the inbox enqueue"
        );
        let mut acked = self.acked.lock().unwrap();
        acked.push(ready.view_version);
        if acked.len() == self.expected {
            self.all_acked.notify_one();
        }
        Ok(())
    }
    async fn nak(&mut self, _: Duration) -> cortexkit_bus_trait::BusResult<()> {
        unreachable!("the consumer never naks a view_ready")
    }
    async fn term(&mut self, _: Option<&str>) -> cortexkit_bus_trait::BusResult<()> {
        unreachable!("the consumer never terms a view_ready")
    }
    async fn in_progress(&mut self) -> cortexkit_bus_trait::BusResult<()> {
        unreachable!("the consumer never reports progress")
    }
}

struct NoPublisher;
#[async_trait::async_trait]
impl broca_store::bus_events::BusPublisher for NoPublisher {
    async fn publish(
        &self,
        _: &broca_store::bus_events::Publication,
    ) -> Result<cortexkit_bus_trait::PublishReceipt, String> {
        Err("the compaction consumer publishes nothing".into())
    }
}

/// Attaches the `box_test` account once, with the one cursor it was given.
struct QueuedConnector(Mutex<Option<QueuedCursor>>);
#[async_trait::async_trait]
impl BusConnector for QueuedConnector {
    async fn connect(&self) -> Result<crate::bus_events::BusConnection, String> {
        Ok(crate::bus_events::BusConnection {
            publisher: Arc::new(NoPublisher),
            account: cortexkit_bus_naming::AccountNames::derive("box_test").unwrap(),
            refresh_at_ms: u64::MAX,
        })
    }
    async fn compaction_consumer(&self) -> Result<Box<dyn StreamCursor>, String> {
        let cursor = self
            .0
            .lock()
            .unwrap()
            .take()
            .ok_or("cursor already taken")?;
        Ok(Box::new(cursor))
    }
}

/// A backlog of announcements drains without the one-second retry wait between them: the
/// wait is for a failed pass, not for every delivery. The clock is paused, so any wait the
/// consumer makes shows as virtual time passing.
#[tokio::test(start_paused = true)]
async fn queued_view_ready_backlog_is_receipted_and_acked_without_retry_waits() {
    let mut fixture = Fixture::new("compaction-backlog-consume");
    const QUEUED: usize = 5;
    let acked = Arc::new(Mutex::new(Vec::new()));
    let all_acked = Arc::new(tokio::sync::Notify::new());
    let cursor = QueuedCursor {
        queue: (1..=QUEUED as u64)
            .map(|version| message(&fixture.session, version))
            .collect(),
        in_flight: None,
        store: fixture.store.clone(),
        inbox: fixture.service.inbox.clone(),
        acked: acked.clone(),
        all_acked: all_acked.clone(),
        expected: QUEUED,
    };
    fixture.service.connector = Arc::new(QueuedConnector(Mutex::new(Some(cursor))));
    let service = &fixture.service;
    let started = tokio::time::Instant::now();
    let elapsed = tokio::join!(service.consume(), async {
        tokio::time::timeout(Duration::from_secs(5), all_acked.notified())
            .await
            .expect("five queued view_ready messages were not all acked within five seconds");
        let elapsed = started.elapsed();
        service.stop.cancel();
        elapsed
    })
    .1;
    assert_eq!(*acked.lock().unwrap(), vec![1, 2, 3, 4, 5]);
    assert_eq!(service.metrics.acked.load(Ordering::Relaxed), QUEUED as u64);
    assert_eq!(service.metrics.last_error.lock().unwrap().clone(), None);
    assert!(
        elapsed < Duration::from_secs(1),
        "the consumer waited between deliveries: {elapsed:?} passed for {QUEUED} messages"
    );
}
