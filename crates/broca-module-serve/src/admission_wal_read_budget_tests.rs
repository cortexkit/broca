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
    fn uploads(&self) -> Option<broca_core::media_uploads::Uploads> {
        self.inner.uploads()
    }
    fn file_identity(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.file_identity(session)
    }
    fn live_wal_bytes(&self, session: &BindIdentity) -> Result<Option<u64>, broca_wal::WalError> {
        self.inner.live_wal_bytes(session)
    }
    fn read_records_range_from_valid_anchor(
        &self,
        session: &BindIdentity,
        after: u64,
        through: Option<u64>,
        anchor: &broca_wal::WalByteAnchor,
    ) -> Result<Option<broca_wal::WalRangeRead>, broca_wal::WalError> {
        self.ranged.fetch_add(1, Ordering::SeqCst);
        self.inner
            .read_records_range_from_valid_anchor(session, after, through, anchor)
    }
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
async fn media_free_sparse_resume_keeps_master_request_bytes_and_zero_extra_wal_reads() {
    let root = fresh_vault_root("media-free-sparse-resume");
    let (stand_in, subc_server) = stand_in_executor(&root).await;
    let http = resume_http_fixture().await;
    let store = Arc::new(AdmissionWalReads {
        inner: FileWalStore::new(root.join("wal")),
        full: Default::default(),
        ranged: Default::default(),
    });
    let materialized = stand_in.session_store.clone();
    let executor = LiveLoopExecutor::new(
        store.clone(),
        materialized.clone(),
        stand_in.plane.clone(),
        Some(http.url.clone()),
    );
    let mut req = drift_request(&root);
    req.model = broca_protocol::ModelHandle::new("openai", "gpt-test");
    req.input = vec![broca_protocol::Message::new(
        broca_protocol::Role::User,
        vec![broca_protocol::ContentBlock::Text {
            text: "resume this exact text".into(),
        }],
    )];
    let resolved = broca_catalog::live::resolve_provider(
        "openai/gpt-test",
        Some(&http.url),
        2048,
        &AuthMode::None,
        &Default::default(),
    )
    .unwrap();
    let config = drift_run_config(&req, &resolved);
    let mut started = run_started_with_user_text(&req.session, &req.run_id.0, "");
    if let broca_wal::WalRecord::RunStarted {
        config: frozen,
        input,
        ..
    } = &mut started
    {
        **frozen = config.clone();
        *input = req.input.clone().into();
    }
    let mut previous = run_started_with_user_text(&req.session, "previous", "earlier turn");
    if let broca_wal::WalRecord::RunStarted { config: frozen, .. } = &mut previous {
        **frozen = config.clone();
    }
    let records = vec![previous,
        serde_json::from_value(serde_json::json!({"type":"model_step_finished","step_id":1,"usage":{},"assistant_message":{"message_id":"old","content":[{"type":"text","text":"previous answer"}]}})).unwrap(),
        serde_json::from_value(serde_json::json!({"type":"turn_finished","step_id":1})).unwrap(),
        serde_json::from_value(serde_json::json!({"type":"run_finished","reason":"completed","usage":{}})).unwrap(),
        started, broca_wal::WalRecord::StepStarted { step_id: broca_protocol::StepId(1) }];
    let mut wal = store.open(&req.session).unwrap();
    for record in &records {
        wal.append(1, record, true).unwrap();
    }
    let prefix = store
        .inner
        .read_records_range(&req.session, 0, Some(4), None)
        .unwrap();
    broca_core::Persister::project_range(materialized.as_ref(), &req.session, &prefix).unwrap();
    let snapshot = broca_core::load_prior_wal_snapshot(
        store.as_ref(),
        &req.session,
        6,
        Some(materialized.as_ref()),
    )
    .unwrap();
    assert_eq!(snapshot.base_watermark, 3);
    assert_eq!(snapshot.tail.len(), 3);
    let (_, _, expected) =
        broca_module_serve::wal_render::render_step(&records, &config, &req.run_id.0, 1).unwrap();
    req.input.clear();
    let writer = Arc::new(broca_core::WalHandle::spawn(wal, 1).unwrap());
    store.full.store(0, Ordering::SeqCst);
    store.ranged.store(0, Ordering::SeqCst);
    let (out, _events) = mpsc::channel(128);
    let summary = tokio::time::timeout(
        Duration::from_secs(15),
        executor.execute(
            RunJob {
                media: Vec::new(),
                media_profile: None,
                conversation_key: None,
                req,
                auth: None,
                writer: writer.clone(),
                kind: RunKind::FreshOrResume,
                skip_materialization: false,
                control: Default::default(),
                prior_snapshot: snapshot,
                on_admitted: None,
            },
            out,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(summary.reason, RunFinishReason::Completed);
    assert_eq!(
        store.full.load(Ordering::SeqCst),
        0,
        "a media-free sparse resume adds no prefix decode"
    );
    // The completed step is materialized by one tail read in the media-free executor too.
    assert_eq!(
        store.ranged.load(Ordering::SeqCst),
        1,
        "only the ordinary final materialization tail read is allowed"
    );
    {
        let requests = http.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].body, expected);
    }
    Arc::try_unwrap(writer)
        .unwrap_or_else(|_| panic!("executor released writer"))
        .shutdown()
        .await
        .unwrap();
    subc_server.abort();
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
