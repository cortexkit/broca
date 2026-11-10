use super::*;
use broca_session_store::{
    backend::{LocalBackend, SessionAddr},
    keys::{Cipher, DeploymentKey},
    session::Session,
};
use broca_wal::StorageClass;

struct NoRuns;
#[async_trait]
impl broca_module::executor::RunExecutor for NoRuns {
    async fn execute(
        &self,
        job: broca_module::executor::RunJob,
        _: tokio::sync::mpsc::Sender<broca_core::OutputEvent>,
    ) -> Result<broca_core::RunSummary, broca_core::RunError> {
        panic!("move executed a model step for {:?}", job.req.session)
    }
}

async fn rig(path: PathBuf, door: Arc<FakeDoor>, key: DeploymentKey) -> Rig {
    std::fs::create_dir_all(&path).unwrap();
    broca_wal::migration::durable_dir(&path, &path.join("wal")).unwrap();
    let runtime = Arc::new(
        broca_module::store_open::StoreRuntime::open(
            &path,
            key,
            Arc::new(NoRuns),
            std::time::Duration::from_secs(60),
        )
        .await
        .unwrap(),
    );
    let wal = Arc::new(FileWalStore::new(path.join("wal")));
    let index = Arc::new(broca_store::SqliteStore::open(path.join("run-index.db")).unwrap());
    broca_store::session_refs::migrate(
        &path.join("run-index.db"),
        &broca_core::ReadOnlyWalReader::new(&path),
    )
    .unwrap();
    let registry = Arc::new(LineageRegistry::new_with_sources(
        Arc::new(
            broca_module::opener::DefaultOpener::new(
                Arc::new(broca_core::FileLeaseStore::new(path.join("leases"))),
                wal.clone(),
                Arc::new(NoRuns),
            )
            .with_store_runtime(runtime),
        ),
        Some(wal.clone()),
        Some(index.clone()),
    ));
    registry.set_session_address_inventory(index.clone());
    let service = Arc::new(AgentDataService::new(
        path.clone(),
        registry.as_ref().clone(),
        wal.clone(),
        index.clone(),
        door.clone(),
    ));
    Rig {
        root: path,
        wal,
        index,
        registry,
        service,
        door,
    }
}

async fn seed(rig: &Rig, session: &BindIdentity, terminal: bool) {
    let runtime = rig.registry.store_runtime().unwrap();
    let backend = LocalBackend::new(StorageClass::Durable.store_root(&rig.root));
    let mut writer = Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(session)),
        broca_session_convert::stage::triple(session),
        runtime.deployment(),
        false,
    )
    .await
    .unwrap();
    let source = Rig::new("store-fixture-wal", Arc::new(FakeDoor::default()));
    source.seed(session, "synthetic-run", terminal);
    let mut records = source.wal.read_records(session).unwrap();
    for frame in &mut records {
        if let WalRecord::ModelStepFinished { written_at_ms, .. } = &mut frame.record {
            *written_at_ms = Some(2);
        }
        if let WalRecord::RunStarted { config, .. } = &mut frame.record {
            config.frozen_render_config = json!({"family_id":broca_provider::spec::WireFamilyId::OpenAiChat,"renderer_version":1,"wire_model_id":"m","max_output_tokens":16,
                "reasoning":broca_provider::policy::ReasoningPolicy::None,"tool_id":broca_provider::policy::ToolIdPolicy::Passthrough,
                "system_prompt":broca_provider::policy::SystemPromptPolicy::SystemMessage,"usage":broca_provider::policy::UsagePolicy::OpenAiUsage});
        }
    }
    let converted = broca_session_convert::convert(&records, None).unwrap();
    let key = runtime
        .deployment()
        .setup_key_for(runtime.deployment().new_sessions())
        .unwrap();
    let store = broca_session_store::setup::SetupStore::new(&backend, &key);
    for record in &converted.records {
        if let broca_session_store::record::Record::SetupStub(stub) = record {
            for part in &stub.parts {
                use broca_session_loop::parts::Parts as _;
                store
                    .put(part.kind, &converted.parts[&part.digest])
                    .await
                    .unwrap();
                runtime
                    .parts_for(StorageClass::Durable, runtime.deployment().new_sessions())
                    .clone()
                    .put(
                        part.kind,
                        &serde_json::from_slice(&converted.parts[&part.digest]).unwrap(),
                    )
                    .unwrap();
            }
        }
    }
    writer.append(&converted.records, true).await.unwrap();
    writer.close().await.unwrap();
}

#[tokio::test]
async fn store_move_plain_and_encrypted_roundtrip_release_and_move_back() {
    for cipher in [Cipher::XChaCha20Poly1305, Cipher::None] {
        let key = DeploymentKey::new(7, &[4; 32]).creating(cipher);
        let door = Arc::new(FakeDoor::default());
        let source = rig(root("store-source"), door.clone(), key).await;
        let receiver = rig(
            root("store-receiver"),
            door.clone(),
            DeploymentKey::new(7, &[4; 32]).creating(cipher),
        )
        .await;
        let session = BindIdentity::new("broca-unscoped-v1", "broca", "store-move");
        seed(&source, &session, true).await;
        let mut reader = Wire::new(&source, Value::Null, false).await;
        reader.bind_session(&session).await;
        let read_before = reader.management("session.read", json!({})).await.unwrap();
        source.registry.actor_for(&session).await.unwrap();
        assert_eq!(source.registry.slot_state(&session), Some("open"));
        let params = source.params(CUT, std::slice::from_ref(&session));
        let mut from = Wire::new(&source, entorhinal(), true).await;
        let mut to = Wire::new(&receiver, entorhinal(), true).await;
        let prepared = from.call("prepare", params.clone()).await.unwrap();
        assert_eq!(source.registry.slot_state(&session), None);
        assert!(prepared["items"][0]["format"]
            .as_str()
            .unwrap()
            .starts_with("broca-store-export/1"));
        assert!(matches!(
            source.registry.check_move(&session, true),
            Err(OpenError::SessionMoving { .. })
        ));
        source.registry.check_move(&session, false).unwrap();
        assert_code(
            reader
                .management(
                    "session.send",
                    json!({"prompt":"hi", "model":{"provider":"stub","model":"m"}}),
                )
                .await,
            "session_moving",
        );
        let frozen_read = reader.management("session.read", json!({})).await.unwrap();
        assert_eq!(frozen_read["messages"], read_before["messages"]);
        from.call("export", params.clone()).await.unwrap();
        door.seal(CUT);
        to.call("import", params.clone()).await.unwrap();
        to.call("activate", params.clone()).await.unwrap();
        from.call(
            "release",
            json!({"cutId":CUT,"refs":params["refs"],"holder":"receiver"}),
        )
        .await
        .unwrap();
        assert!(
            matches!(source.registry.check_move(&session, false), Err(OpenError::SessionMoved { holder }) if holder == "receiver")
        );
        assert_code(
            reader.management("session.read", json!({})).await,
            "session_moved",
        );
        assert_code(
            reader
                .management(
                    "session.send",
                    json!({"prompt":"hi", "model":{"provider":"stub","model":"m"}}),
                )
                .await,
            "session_moved",
        );
        let runtime = receiver.registry.store_runtime().unwrap();
        let backend = LocalBackend::new(StorageClass::Durable.store_root(&receiver.root));
        let addr = SessionAddr(broca_core::session_addr(&session));
        let writer = Session::load(
            &backend,
            addr.clone(),
            &broca_session_convert::stage::triple(&session),
            runtime.deployment(),
        )
        .await
        .unwrap();
        writer.close().await.unwrap();
        let back = source.params(
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            std::slice::from_ref(&session),
        );
        to.call("prepare", back.clone()).await.unwrap();
        to.call("export", back.clone()).await.unwrap();
        door.seal("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
        assert_eq!(
            from.call("diverged", back.clone()).await.unwrap()["refs"][0]["state"],
            "advanced"
        );
        from.call("import", back.clone()).await.unwrap();
        from.call("activate", back).await.unwrap();
        source.registry.check_move(&session, true).unwrap();
        let again = source.params(
            "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            std::slice::from_ref(&session),
        );
        from.call("prepare", again.clone()).await.unwrap();
        from.call("export", again.clone()).await.unwrap();
        door.seal("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
        to.call("release", json!({"cutId":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb","refs":again["refs"],"holder":"source"})).await.unwrap();
        to.call("import", again.clone()).await.unwrap();
        to.call("activate", again).await.unwrap();
        receiver.registry.check_move(&session, true).unwrap();
    }
}

#[tokio::test]
async fn store_stream_recovers_crashes_before_and_during_admit_but_refuses_a_lost_copy() {
    let door = Arc::new(FakeDoor::default());
    let source = rig(
        root("store-stream-recovery"),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let session = source.session("stream");
    seed(&source, &session, true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let reference = params["refs"][0]["ref"].as_str().unwrap().to_owned();
    let cache = source
        .root
        .join("session-store-moves/streams")
        .join(broca_core::session_addr(&session));
    let mut from = Wire::new(&source, entorhinal(), true).await;
    from.call("prepare", params.clone()).await.unwrap();
    let first = std::fs::read(&cache).unwrap();
    // Process death after the durable copy but before admission leaves no remote bytes.
    from.call("abort", params).await.unwrap();
    let second = source.params(
        "11111111111111111111111111111111",
        std::slice::from_ref(&session),
    );
    from.call("prepare", second.clone()).await.unwrap();
    let cached = std::fs::read(&cache).unwrap();
    assert_eq!(first.len(), cached.len());
    // A process death during admission leaves a partial frame. Its cached tail finishes
    // first, so the following snapshot can still start on a valid frame boundary.
    door.streams
        .lock()
        .unwrap()
        .insert(reference.clone(), cached[..cached.len() / 2].to_vec());
    from.call("abort", second).await.unwrap();
    let third = source.params(
        "22222222222222222222222222222222",
        std::slice::from_ref(&session),
    );
    from.call("prepare", third.clone()).await.unwrap();
    from.call("export", third.clone()).await.unwrap();
    let admitted = door.streams.lock().unwrap()[&reference].clone();
    assert!(admitted.starts_with(&cached));
    assert!(admitted.len() > cached.len());
    broca_session_store::session::moves::last_export(&admitted).unwrap();
    from.call("abort", third).await.unwrap();
    std::fs::remove_file(&cache).unwrap();
    let lost = source.params(
        "33333333333333333333333333333333",
        std::slice::from_ref(&session),
    );
    let refusal = assert_code(from.call("prepare", lost).await, "unavailable");
    assert!(refusal.to_string().contains("move_stream_lost"));
    assert!(!source
        .wal
        .wal_path(&session)
        .with_extension("moving")
        .exists());
}

#[tokio::test]
async fn store_missing_key_refusal_names_transfer_and_local_grant_before_staging() {
    let door = Arc::new(FakeDoor::default());
    let source = rig(
        root("store-key-source"),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let receiver = rig(
        root("store-key-receiver"),
        door.clone(),
        DeploymentKey::new(8, &[4; 32]),
    )
    .await;
    let session = source.session("store-key");
    seed(&source, &session, true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let mut from = Wire::new(&source, entorhinal(), true).await;
    from.call("prepare", params.clone()).await.unwrap();
    from.call("export", params.clone()).await.unwrap();
    door.seal(CUT);
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    let refusal = assert_code(to.call("import", params).await, "unavailable");
    let message = refusal.to_string();
    for word in [
        "deployment_key_missing",
        "opaque:broca-deployment:1",
        "reserved:broca",
        "exact read",
        "grants do not travel",
    ] {
        assert!(message.contains(word), "{message}");
    }
    assert!(!receiver.root.join("session-store-moves/staging").exists());
}

#[tokio::test]
async fn store_busy_refuses_prepare_and_bad_verification_digests_refuse_before_staging() {
    let door = Arc::new(FakeDoor::default());
    let source = rig(
        root("store-busy"),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let session = source.session("busy");
    seed(&source, &session, false).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let mut from = Wire::new(&source, entorhinal(), true).await;
    assert_eq!(
        from.call("busy", json!({"refs":params["refs"]}))
            .await
            .unwrap()["refs"][0]["reason_code"],
        "run_active"
    );
    let error = assert_code(from.call("prepare", params.clone()).await, "busy");
    assert!(error.to_string().contains("run_active"));
    assert!(!source
        .wal
        .wal_path(&session)
        .with_extension("moving")
        .exists());

    let session = source.session("bad-digests");
    seed(&source, &session, true).await;
    let params = source.params(
        "cccccccccccccccccccccccccccccccc",
        std::slice::from_ref(&session),
    );
    from.call("prepare", params.clone()).await.unwrap();
    from.call("export", params.clone()).await.unwrap();
    let reference = params["refs"][0]["ref"].as_str().unwrap();
    let bytes = door.streams.lock().unwrap()[reference].clone();
    let runtime = source.registry.store_runtime().unwrap();
    let mut export = broca_session_store::session::moves::Export::decode(
        broca_session_store::session::moves::last_export(&bytes).unwrap(),
        &SessionAddr(broca_core::session_addr(&session)),
        runtime.deployment(),
    )
    .unwrap();
    export.checks.insert("next_request".into(), "wrong".into());
    door.plant(
        "cccccccccccccccccccccccccccccccc",
        reference,
        broca_session_store::session::moves::append_stream(&[], &export),
    );
    let receiver = rig(
        root("store-bad-receiver"),
        door,
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    assert_code(to.call("import", params).await, "digest_mismatch");
    assert!(!receiver.root.join("session-store-moves/staging").exists());
}

fn pending(rig: &Rig, op: &str, previous: contract::step::Step, params: Value) {
    use broca_module::cut_state::{CutState, CutStore, Intent};
    let path = CutStore::new(&rig.root).path(CUT);
    let mut state: CutState = broca_wal::migration::read_json(&path).unwrap().unwrap();
    state.step = previous;
    state.replies.remove(&format!("agent_data.{op}"));
    state.intent = Some(Intent {
        op: op.into(),
        request: params,
    });
    broca_wal::migration::atomic_json(&rig.root, &path, &state).unwrap();
}

#[tokio::test]
async fn store_prepare_refuses_an_unverifiable_next_request_before_freezing() {
    use broca_session_store::record::Record;
    let source = rig(
        root("store-unverifiable"),
        Arc::new(FakeDoor::default()),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let session = source.session("no-render-config");
    seed(&source, &session, true).await;
    let runtime = source.registry.store_runtime().unwrap();
    let backend = LocalBackend::new(StorageClass::Durable.store_root(&source.root));
    let mut writer = Session::load(
        &backend,
        SessionAddr(broca_core::session_addr(&session)),
        &broca_session_convert::stage::triple(&session),
        runtime.deployment(),
    )
    .await
    .unwrap();
    let records = writer.records_from(0).await.unwrap();
    let mut start = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::RunStart(start) => Some((**start).clone()),
            _ => None,
        })
        .unwrap();
    let mut end = records
        .iter()
        .find_map(|(_, record)| match record {
            Record::RunEnd(end) => Some(end.clone()),
            _ => None,
        })
        .unwrap();
    start.run_id = "no-render".into();
    start.setup.clear();
    end.run_id = start.run_id.clone();
    writer
        .append(
            &[Record::RunStart(Box::new(start)), Record::RunEnd(end)],
            true,
        )
        .await
        .unwrap();
    writer.close().await.unwrap();
    let mut from = Wire::new(&source, entorhinal(), true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let error = assert_code(from.call("prepare", params).await, "unavailable");
    assert!(error.to_string().contains("no frozen render config"));
    source.registry.check_move(&session, true).unwrap();
}

#[tokio::test]
async fn store_operation_crash_cuts_resume_and_abort_only_the_owned_freeze() {
    use contract::step::Step;
    let door = Arc::new(FakeDoor::default());
    let source_root = root("store-cuts-source");
    let receiver_root = root("store-cuts-receiver");
    let source = rig(
        source_root.clone(),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let session = source.session("crash-cuts");
    seed(&source, &session, true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let mut from = Wire::new(&source, entorhinal(), true).await;
    let prepared = from.call("prepare", params.clone()).await.unwrap();
    pending(&source, "prepare", Step::None, params.clone());
    let source = rig(
        source_root.clone(),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let mut from = Wire::new(&source, entorhinal(), true).await;
    assert_eq!(
        from.call("prepare", params.clone()).await.unwrap(),
        prepared
    );
    from.call("export", params.clone()).await.unwrap();
    pending(&source, "export", Step::Prepared, params.clone());
    let source = rig(source_root, door.clone(), DeploymentKey::new(7, &[4; 32])).await;
    let mut from = Wire::new(&source, entorhinal(), true).await;
    from.call("export", params.clone()).await.unwrap();
    door.seal(CUT);
    let receiver = rig(
        receiver_root.clone(),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    to.call("import", params.clone()).await.unwrap();
    pending(&receiver, "import", Step::None, params.clone());
    let receiver = rig(
        receiver_root.clone(),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    receiver.service.reconcile_boot().await.unwrap();
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    to.call("activate", params.clone()).await.unwrap();
    pending(&receiver, "activate", Step::Imported, params.clone());
    let receiver = rig(receiver_root, door.clone(), DeploymentKey::new(7, &[4; 32])).await;
    receiver.service.reconcile_boot().await.unwrap();
    receiver.registry.check_move(&session, true).unwrap();
    let release = json!({"cutId":CUT,"refs":params["refs"],"holder":"receiver"});
    from.call("release", release.clone()).await.unwrap();
    pending(&source, "release", Step::Exported, release.clone());
    let source = rig(
        source.root.clone(),
        door.clone(),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    source.service.reconcile_boot().await.unwrap();
    assert!(matches!(
        source.registry.check_move(&session, true),
        Err(OpenError::SessionMoved { .. })
    ));
    let abort = receiver.params(
        "dddddddddddddddddddddddddddddddd",
        std::slice::from_ref(&session),
    );
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    to.call("prepare", abort.clone()).await.unwrap();
    to.call("abort", abort).await.unwrap();
    receiver.registry.check_move(&session, true).unwrap();
}
