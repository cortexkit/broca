use super::*;
use broca_session_store::{
    backend::{LocalBackend, SessionAddr},
    keys::{Cipher, DeploymentKey},
    session::Session,
};
use broca_wal::StorageClass;

struct NoRuns;
#[async_trait]
impl broca_module::executor::RunExecutor for NoRuns {}

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
    let registry = Arc::new(LineageRegistry::new(Arc::new(
        broca_module::opener::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(path.join("leases"))),
            Arc::new(NoRuns),
        )
        .with_store_runtime(runtime),
    )));
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

/// The session triple a session's log names, as broca derives it from its bind identity.
fn triple(session: &BindIdentity) -> broca_session_store::head::SessionTriple {
    broca_session_store::head::SessionTriple {
        project_root: session.project_root.display().to_string(),
        harness: session.harness.clone(),
        session: session.session.clone(),
    }
}

/// A session's records and setup parts, as the storage keeps them: a run started on a stub
/// model, either finished after one answer (`finished.json`) or still running with nothing
/// answered (`running.json`). The records name no session, so one history serves every one.
struct Seed {
    records: Vec<broca_session_store::record::Record>,
    parts: std::collections::BTreeMap<broca_session_store::codec::Digest, Vec<u8>>,
}

fn seed_history(terminal: bool) -> Seed {
    let text = if terminal {
        include_str!("fixtures/finished.json")
    } else {
        include_str!("fixtures/running.json")
    };
    let fixture: Value = serde_json::from_str(text).unwrap();
    let bytes = |value: &Value| -> Vec<u8> {
        let text = value.as_str().unwrap();
        (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect()
    };
    Seed {
        records: fixture["records"]
            .as_array()
            .unwrap()
            .iter()
            .map(|record| {
                let kind = u32::try_from(record["kind"].as_u64().unwrap()).unwrap();
                broca_session_store::record::Record::decode(kind, &bytes(&record["body"])).unwrap()
            })
            .collect(),
        parts: fixture["parts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|part| {
                let digest: [u8; 32] = bytes(&part[0]).try_into().unwrap();
                (broca_session_store::codec::Digest(digest), bytes(&part[1]))
            })
            .collect(),
    }
}

/// Stores `session` as binding it would, with its ref in the session-ref index.
async fn seed(rig: &Rig, session: &BindIdentity, terminal: bool) {
    store_session(rig, session, terminal).await;
    // Binding a session records its ref in the index in production; moves find it there.
    broca_core::SessionRefIndex::record_session(
        rig.index.as_ref(),
        &broca_core::session_addr(session),
        session,
    )
    .unwrap();
}

/// Writes `session`'s records into the store, and nothing into the session-ref index.
async fn store_session(rig: &Rig, session: &BindIdentity, terminal: bool) {
    store_session_on(rig, session, terminal, false).await;
}

/// As [`store_session`], for a session on the compaction contract when `contract` is set: its
/// runs carry a compaction plan version, so a message renders only with its `EntryFacts` record
/// (its id, time and part numbers).
async fn store_session_on(rig: &Rig, session: &BindIdentity, terminal: bool, contract: bool) {
    let runtime = rig.registry.store_runtime().unwrap();
    let backend = LocalBackend::new(StorageClass::Durable.store_root(&rig.root));
    let mut writer = Session::create(
        &backend,
        SessionAddr(broca_core::session_addr(session)),
        triple(session),
        runtime.deployment(),
        false,
    )
    .await
    .unwrap();
    let mut seeded = seed_history(terminal);
    if contract {
        for record in &mut seeded.records {
            if let broca_session_store::record::Record::RunStart(start) = record {
                start.compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
            }
        }
    }
    let key = runtime
        .deployment()
        .setup_key_for(runtime.deployment().new_sessions())
        .unwrap();
    let store = broca_session_store::setup::SetupStore::new(&backend, &key);
    for record in &seeded.records {
        if let broca_session_store::record::Record::SetupStub(stub) = record {
            for part in &stub.parts {
                use broca_session_loop::parts::Parts as _;
                store
                    .put(part.kind, &seeded.parts[&part.digest])
                    .await
                    .unwrap();
                runtime
                    .parts_for(StorageClass::Durable, runtime.deployment().new_sessions())
                    .clone()
                    .put(
                        part.kind,
                        &serde_json::from_slice(&seeded.parts[&part.digest]).unwrap(),
                    )
                    .unwrap();
            }
        }
    }
    writer.append(&seeded.records, true).await.unwrap();
    writer.close().await.unwrap();
}

/// A move finds a session through the session-ref index alone, never by searching the stored
/// sessions: a ref is a one-way hash, so a search reads every stored head, and on a machine
/// with about 190,000 sessions that took 37 s, past the mover's 30 s deadline. So a stored
/// session the index doesn't name is not this machine's to move.
#[tokio::test]
async fn busy_and_prepare_find_a_stored_session_through_the_ref_index_alone() {
    let door = Arc::new(FakeDoor::default());
    let source = rig(
        root("store-busy-index"),
        door,
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let indexed = source.session("indexed");
    seed(&source, &indexed, true).await;
    let unindexed = source.session("unindexed");
    store_session(&source, &unindexed, true).await;
    let mut from = Wire::new(&source, entorhinal(), true).await;

    let params = source.params(CUT, std::slice::from_ref(&indexed));
    let busy = from
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(busy["refs"][0]["state"], "idle", "{busy}");

    let params = source.params(CUT, std::slice::from_ref(&unindexed));
    assert_code(
        from.call("busy", json!({"refs":params["refs"]})).await,
        "unknown_ref",
    );
    assert_code(from.call("prepare", params).await, "unknown_ref");
}

/// The move check renders the request a next message would send. On a session on the
/// compaction contract a message renders only with its `EntryFacts` record (its id, time and
/// part numbers), so the check writes one for its made-up next message as the run loop writes
/// one for a real message; without it `busy` and `prepare` refused every such session as
/// unavailable.
#[tokio::test]
async fn busy_and_prepare_answer_for_a_session_on_the_compaction_contract() {
    let door = Arc::new(FakeDoor::default());
    let source = rig(
        root("store-busy-contract"),
        door,
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let session = source.session("contract");
    store_session_on(&source, &session, true, true).await;
    broca_core::SessionRefIndex::record_session(
        source.index.as_ref(),
        &broca_core::session_addr(&session),
        &session,
    )
    .unwrap();
    let mut from = Wire::new(&source, entorhinal(), true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let busy = from
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(busy["refs"][0]["state"], "idle", "{busy}");
    let prepared = from.call("prepare", params).await.unwrap();
    assert!(
        prepared["items"][0]["format"]
            .as_str()
            .is_some_and(|format| format.starts_with("broca-store-export/1")),
        "{prepared}"
    );
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
        let refused_read = assert_code(
            reader.management("session.read", json!({})).await,
            "session_held_elsewhere",
        );
        let refused = assert_code(
            reader
                .management(
                    "session.send",
                    json!({"prompt":"hi", "model":{"provider":"stub","model":"m"}}),
                )
                .await,
            "session_held_elsewhere",
        );
        // The release named its holder by backup author id, which is not the box id the
        // contract's `holder` carries, so the refusal names no holder at all.
        assert_eq!(refused["detail"], json!({}), "{refused}");
        assert!(!refused.to_string().contains("receiver"), "{refused}");
        // Reader samples: the exact refusal bodies a reader of a moved session gets.
        if let Some(dir) = std::env::var_os("BROCA_HELD_ELSEWHERE_SAMPLES") {
            let dir = std::path::PathBuf::from(dir);
            std::fs::create_dir_all(&dir).unwrap();
            for (name, body) in [("read", &refused_read), ("send", &refused)] {
                std::fs::write(
                    dir.join(format!("{name}-refused.json")),
                    serde_json::to_vec_pretty(body).unwrap(),
                )
                .unwrap();
            }
        }
        let runtime = receiver.registry.store_runtime().unwrap();
        let backend = LocalBackend::new(StorageClass::Durable.store_root(&receiver.root));
        let addr = SessionAddr(broca_core::session_addr(&session));
        let writer = Session::load(
            &backend,
            addr.clone(),
            &triple(&session),
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
        &triple(&session),
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
    // Drop only the render config: prepare reads the run's other setup parts earlier, when it
    // checks what the latest run waits on, and a run start missing them is refused there, before
    // the render-config check this test is about.
    start
        .setup
        .retain(|part| part.kind != broca_session_store::record::PartKind::RenderConfig);
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
