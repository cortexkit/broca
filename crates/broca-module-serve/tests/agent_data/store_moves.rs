use super::*;
use broca_session_store::{
    backend::{LocalBackend, SessionAddr},
    keys::{Cipher, DeploymentKey},
    session::Session,
};
use broca_wal::StorageClass;

/// A rig whose new storage opens without its deployment key, as broca's does at boot before the
/// vault answers, and the slot and setup that load the key later. With `open_late`, the storage
/// opens only after the agent-data service is built, as when its first open failed at boot and
/// the key loader opened it later.
async fn rig_awaiting_key(
    path: PathBuf,
    door: Arc<FakeDoor>,
    open_late: bool,
) -> (
    Rig,
    Arc<broca_module::store_open::StoreSlot>,
    broca_module::store_open::StoreRuntimeSetup,
) {
    std::fs::create_dir_all(&path).unwrap();
    let slot = Arc::new(broca_module::store_open::StoreSlot::awaiting_key(&path, 7));
    let setup = broca_module::store_open::StoreRuntimeSetup {
        state_root: path.clone(),
        executor: Arc::new(NoRuns),
        idle_ttl: std::time::Duration::from_secs(60),
        import_roots: None,
        billing: None,
        run_events: None,
        send_states: None,
    };
    if !open_late {
        slot.open_without_key(&setup).await.unwrap();
    }
    let index = Arc::new(broca_store::SqliteStore::open(path.join("run-index.db")).unwrap());
    // Boot's census: every session already stored here is in the session-ref index.
    broca_store::session_refs::migrate(
        &path.join("run-index.db"),
        &broca_store::session_refs::StoredSessions::new(&path),
    )
    .unwrap();
    let registry = Arc::new(LineageRegistry::new(Arc::new(
        broca_module::opener::DefaultOpener::new(
            Arc::new(broca_core::FileLeaseStore::new(path.join("leases"))),
            Arc::new(NoRuns),
        )
        .with_store_slot(slot.clone()),
    )));
    registry.set_session_address_inventory(index.clone());
    let service = Arc::new(AgentDataService::new(
        path.clone(),
        registry.as_ref().clone(),
        index.clone(),
        door.clone(),
    ));
    if open_late {
        slot.open_without_key(&setup).await.unwrap();
    }
    let rig = Rig {
        root: path,
        index,
        registry,
        service,
        door,
    };
    (rig, slot, setup)
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
        // The encrypted round's release names the receiving computer's box id (agent-data
        // 0.3.3 `holderBox`); the plain round's names only its backup author id, as older
        // drivers send.
        const BOX: &str = "0123456789abcdef0123456789abcdef";
        let holder_box = (cipher == Cipher::XChaCha20Poly1305).then_some(BOX);
        let mut release = json!({"cutId":CUT,"refs":params["refs"],"holder":"receiver"});
        if let Some(holder_box) = holder_box {
            release["holderBox"] = json!(holder_box);
        }
        from.call("release", release).await.unwrap();
        assert!(
            matches!(source.registry.check_move(&session, false), Err(OpenError::SessionMoved { holder }) if holder.as_deref() == holder_box)
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
        // The refusal names the box id when the release gave one. The backup author id is not
        // the box id the contract's `holder` carries, so it is never named.
        match holder_box {
            Some(holder_box) => {
                assert_eq!(
                    refused["detail"],
                    json!({"holder": holder_box}),
                    "{refused}"
                )
            }
            None => assert_eq!(refused["detail"], json!({}), "{refused}"),
        }
        assert!(!refused.to_string().contains("receiver"), "{refused}");
        // Reader samples: the exact refusal bodies a reader of a moved session gets.
        if let Some(dir) = std::env::var_os("BROCA_HELD_ELSEWHERE_SAMPLES") {
            let dir = std::path::PathBuf::from(dir);
            std::fs::create_dir_all(&dir).unwrap();
            let suffix = if holder_box.is_some() {
                "-holder-box"
            } else {
                ""
            };
            for (name, body) in [("read", &refused_read), ("send", &refused)] {
                std::fs::write(
                    dir.join(format!("{name}-refused{suffix}.json")),
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
        // The computer the session left holds nothing the cut moving it back lacks: that
        // cut's stream starts with the one this computer sent.
        assert_eq!(
            from.call("diverged", back.clone()).await.unwrap()["refs"][0]["state"],
            "at_cut"
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
    assert!(!source.fence(&session).with_extension("moving").exists());
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
    assert!(!source.fence(&session).with_extension("moving").exists());

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
    // `busy` reads the session's state and builds no export, so the next request it never
    // renders can't refuse it; only `prepare`, which exports, checks that request.
    let busy = from
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(busy["refs"][0]["state"], "idle", "{busy}");
    let error = assert_code(from.call("prepare", params).await, "unavailable");
    assert!(error.to_string().contains("no frozen render config"));
    source.registry.check_move(&session, true).unwrap();
}

/// A session kept with retention, which lives in the ephemeral class, is refused a move: a copy on
/// another computer would outlive its deletion deadline here.
#[tokio::test]
async fn store_prepare_refuses_a_retention_session() {
    let source = rig(
        root("store-retention"),
        Arc::new(FakeDoor::default()),
        DeploymentKey::new(7, &[4; 32]),
    )
    .await;
    let session = source.session("retained");
    store_session_in(&source, &session, true, false, StorageClass::Ephemeral).await;
    broca_core::SessionRefIndex::record_session(
        source.index.as_ref(),
        &broca_core::session_addr(&session),
        &session,
    )
    .unwrap();
    let mut from = Wire::new(&source, entorhinal(), true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    assert_code(from.call("prepare", params).await, "not_exportable");
    source.registry.check_move(&session, true).unwrap();
}

/// The agent-data service finds a stored session even when the session store opened only after the
/// service was built: it reads the storage's runtime at each request, not once at boot.
#[tokio::test]
async fn agent_data_finds_a_store_that_opened_after_it_was_built() {
    let door = Arc::new(FakeDoor::default());
    let path = root("store-opened-late");
    let source = rig(path.clone(), door.clone(), DeploymentKey::new(7, &[4; 32])).await;
    let session = source.session("late");
    seed(&source, &session, true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    drop(source);

    let (late, slot, setup) = rig_awaiting_key(path, door, true).await;
    slot.load(&setup, DeploymentKey::new(7, &[4; 32]))
        .await
        .unwrap();
    let mut from = Wire::new(&late, entorhinal(), true).await;
    let busy = from
        .call("busy", json!({"refs":params["refs"]}))
        .await
        .unwrap();
    assert_eq!(busy["refs"][0]["state"], "idle", "{busy}");
}

/// After a move lands here, `diverged` for its cut answers `at_cut` only while the session holds
/// nothing newer than what the move brought. Work done here since makes it `advanced`, though the
/// move's cached stream still matches the cut byte for byte.
#[tokio::test]
async fn diverged_sees_work_done_here_after_the_move_landed() {
    let key = || DeploymentKey::new(7, &[4; 32]);
    let door = Arc::new(FakeDoor::default());
    let source = rig(root("store-diverged-source"), door.clone(), key()).await;
    let receiver = rig(root("store-diverged-receiver"), door.clone(), key()).await;
    let session = source.session("diverged-here");
    seed(&source, &session, true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let mut from = Wire::new(&source, entorhinal(), true).await;
    from.call("prepare", params.clone()).await.unwrap();
    from.call("export", params.clone()).await.unwrap();
    door.seal(CUT);
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    to.call("import", params.clone()).await.unwrap();
    to.call("activate", params.clone()).await.unwrap();
    let state = |reply: Value| reply["refs"][0]["state"].clone();
    assert_eq!(
        state(to.call("diverged", params.clone()).await.unwrap()),
        "at_cut"
    );

    let run_id = seed_history(true)
        .records
        .iter()
        .find_map(|record| match record {
            broca_session_store::record::Record::RunStart(start) => Some(start.run_id.clone()),
            _ => None,
        })
        .unwrap();
    let runtime = receiver.registry.store_runtime().unwrap();
    let backend = LocalBackend::new(StorageClass::Durable.store_root(&receiver.root));
    let mut writer = Session::load(
        &backend,
        SessionAddr(broca_core::session_addr(&session)),
        &triple(&session),
        runtime.deployment(),
    )
    .await
    .unwrap();
    writer
        .append(
            &[broca_session_store::record::Record::Billed(
                broca_session_store::record::Billed { run_id },
            )],
            true,
        )
        .await
        .unwrap();
    writer.close().await.unwrap();
    assert_eq!(
        state(to.call("diverged", params).await.unwrap()),
        "advanced"
    );
}

/// A receiver that stopped mid-activation of an encrypted session can't finish it at boot: the
/// session can't be read before the storage key loads, so the activation stays pending and the
/// session fenced. Once the key loads, the pending activation finishes without a retry of the
/// move.
#[tokio::test]
async fn an_encrypted_activation_left_at_boot_finishes_once_the_key_loads() {
    use contract::step::Step;
    let key = || DeploymentKey::new(7, &[4; 32]).creating(Cipher::XChaCha20Poly1305);
    let door = Arc::new(FakeDoor::default());
    let source = rig(root("store-key-source"), door.clone(), key()).await;
    let session = source.session("key-activation");
    seed(&source, &session, true).await;
    let params = source.params(CUT, std::slice::from_ref(&session));
    let mut from = Wire::new(&source, entorhinal(), true).await;
    from.call("prepare", params.clone()).await.unwrap();
    from.call("export", params.clone()).await.unwrap();
    door.seal(CUT);
    let receiver_root = root("store-key-receiver");
    let receiver = rig(receiver_root.clone(), door.clone(), key()).await;
    let mut to = Wire::new(&receiver, entorhinal(), true).await;
    to.call("import", params.clone()).await.unwrap();
    to.call("activate", params.clone()).await.unwrap();
    pending(&receiver, "activate", Step::Imported, params.clone());

    let (receiver, slot, setup) = rig_awaiting_key(receiver_root, door.clone(), false).await;
    receiver.service.reconcile_boot().await.unwrap();
    assert!(
        receiver.registry.check_move(&session, true).is_err(),
        "the activation finished before the key could read the session"
    );
    slot.load(&setup, key()).await.unwrap();
    receiver.service.reconcile_pending_cuts().await.unwrap();
    receiver.registry.check_move(&session, true).unwrap();
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

/// As [`store_session`], for a session on the compaction contract when `contract` is set: its
/// runs carry a compaction plan version, so a message renders only with its `EntryFacts` record
/// (its id, time and part numbers).
async fn store_session_on(rig: &Rig, session: &BindIdentity, terminal: bool, contract: bool) {
    store_session_in(rig, session, terminal, contract, StorageClass::Durable).await;
}

/// As [`store_session_on`], kept in storage class `class`.
async fn store_session_in(
    rig: &Rig,
    session: &BindIdentity,
    terminal: bool,
    contract: bool,
    class: StorageClass,
) {
    let mut seeded = seed_history(terminal);
    if contract {
        for record in &mut seeded.records {
            if let broca_session_store::record::Record::RunStart(start) = record {
                start.compaction_plan_version = Some(broca_core::compaction_v2::PLAN_VERSION);
            }
        }
    }
    store_session_with(rig, session, seeded, class).await;
}

async fn seed(rig: &Rig, session: &BindIdentity, terminal: bool) {
    rig.seed(session, terminal).await;
}
