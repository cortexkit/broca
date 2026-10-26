// Included in executor::tests to pass an already-loaded prior_snapshot through RunJob and
// observe whether LiveLoopExecutor rereads that persisted history during admission.

struct AdmissionWalReads {
    inner: FileWalStore,
    full: std::sync::atomic::AtomicUsize,
    ranged: std::sync::atomic::AtomicUsize,
}

impl ExecutorWalStore for AdmissionWalReads {
    fn file_store(&self) -> &FileWalStore {
        &self.inner
    }
}

impl WalStore for AdmissionWalReads {
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
        self.full.fetch_add(1, Ordering::SeqCst);
        self.inner.read_records(session)
    }

    fn read_records_range(
        &self,
        session: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: Option<&broca_wal::WalByteAnchor>,
    ) -> Result<broca_wal::WalRangeRead, broca_wal::WalError> {
        self.ranged.fetch_add(1, Ordering::SeqCst);
        self.inner
            .read_records_range(session, after, through, anchor)
    }
}

#[tokio::test]
async fn fresh_episode_admission_does_not_add_a_full_wal_read() {
    let root = fresh_vault_root("fresh-admission-read-budget");
    let (stand_in, server) = stand_in_executor(&root).await;
    let store = Arc::new(AdmissionWalReads {
        inner: FileWalStore::new(root.join("wal")),
        full: Default::default(),
        ranged: Default::default(),
    });
    let executor = Arc::new(LiveLoopExecutor::new(
        store.clone(),
        stand_in.session_store.clone(),
        stand_in.plane.clone(),
        Some("http://127.0.0.1:9".into()),
    ));
    let mut req = drift_request(&root);
    req.model = broca_protocol::request::ModelHandle::new("openai", "gpt-test");
    req.run_id = broca_protocol::RunId("fresh-episode".into());
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "next turn".into(),
        }],
    )];
    let mut wal = store.open(&req.session).unwrap();
    wal.append(
        1,
        &run_started_with_user_text(&req.session, "previous-episode", "earlier turn"),
        true,
    )
    .unwrap();
    wal.append(1, &run_finished(2, None).record, true).unwrap();
    let prior_snapshot = broca_core::load_prior_wal_snapshot(
        store.as_ref(),
        &req.session,
        2,
        None::<&broca_store::SqliteStore>,
    )
    .unwrap();
    assert_eq!(prior_snapshot.head_seq, 2);
    assert!(
        store.full.load(Ordering::SeqCst) + store.ranged.load(Ordering::SeqCst) > 0,
        "snapshot setup really read the persisted history"
    );
    store.full.store(0, Ordering::SeqCst);
    store.ranged.store(0, Ordering::SeqCst);
    let (ack, admitted) = tokio::sync::oneshot::channel();
    let job = RunJob {
        media: vec![],
        media_profile: None,
        conversation_key: None,
        req,
        auth: None,
        writer: Arc::new(broca_core::WalHandle::spawn(wal, 1).unwrap()),
        kind: RunKind::AppendEpisode,
        skip_materialization: true,
        control: Default::default(),
        prior_snapshot,
        on_admitted: Some(ack),
    };
    let (out, _events) = tokio::sync::mpsc::channel(64);
    let task = tokio::spawn(async move { executor.execute(job, out).await });
    let ack = broca_test_support::wait_for("durable fresh-episode admission", admitted, || {
        format!("executor finished: {}", task.is_finished())
    })
    .await;
    // Admission is the boundary under test. Do not wait for or depend on a provider response.
    task.abort();
    let _ = task.await;
    ack.expect("executor sends its admission ack")
        .expect("the fresh episode is admitted");
    assert_eq!(
        store.full.load(Ordering::SeqCst),
        0,
        "admission must not reread the full WAL"
    );
    assert_eq!(
        store.ranged.load(Ordering::SeqCst),
        0,
        "the supplied snapshot also avoids a redundant ranged read"
    );
    drop(stand_in);
    server.abort();
    let _ = std::fs::remove_dir_all(root);
}
