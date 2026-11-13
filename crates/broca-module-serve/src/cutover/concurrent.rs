use super::*;
use crate::session_conversion::parallel::Probe;
use std::collections::BTreeMap;
use std::sync::atomic::Ordering;

fn identities(count: usize) -> Vec<BindIdentity> {
    (0..count)
        .map(|i| BindIdentity::new("/project", "runner", format!("parallel-{i}")))
        .collect()
}

fn write_sessions(rig: &Rig, sessions: &[BindIdentity]) {
    for session in sessions {
        let path = rig.wal_store.wal_path(session);
        let mut records = session_records(&path);
        for record in &mut records {
            if let WalRecord::RunStarted {
                session: identity, ..
            } = record
            {
                *identity = session.clone();
            }
        }
        let mut wal = broca_wal::FileWal::open(&path).unwrap();
        for record in records {
            wal.append(0, &record, false).unwrap();
        }
        wal.sync().unwrap();
    }
}

fn probe(permits: usize) -> Arc<Probe> {
    Arc::new(Probe {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(permits),
        sources: Mutex::default(),
        crashes: BTreeMap::new(),
        crash_barrier: None,
        active: Mutex::default(),
        overlap: Default::default(),
    })
}

async fn entered(probe: &Probe, count: usize) {
    broca_test_support::wait_until("concurrent conversion workers enter", || async {
        let seen = probe.sources.lock().unwrap().len();
        if seen >= count {
            Ok(())
        } else {
            Err(format!("entered {seen}, expected {count}"))
        }
    })
    .await;
}

fn run_live(rig: &Rig, engine: Engine) -> tokio::task::JoinHandle<Result<(), String>> {
    let runtime = rig.runtime.clone();
    let registry = rig.registry.clone();
    tokio::spawn(async move {
        engine
            .run_live(&deployment(), Some(&runtime), Some(&registry))
            .await
    })
}

fn assert_progress(engine: &Engine) {
    assert_eq!(
        engine.health_for_test().progress,
        engine.ledger.progress().unwrap()
    );
}

fn assert_rows_equal(sequential: &Engine, concurrent: &Engine, roots: [&Path; 2]) {
    let a = sequential.ledger.rows().unwrap();
    let b = concurrent.ledger.rows().unwrap();
    assert_eq!(a.len(), b.len());
    for (mut expected, mut actual) in a.into_iter().zip(b) {
        // The clones intentionally have different backup roots; the relative backup is stored
        // unchanged. All other ledger fields, including the source fingerprint, must agree.
        expected.source.backup = Path::new(&expected.source.backup)
            .strip_prefix(roots[0])
            .unwrap()
            .to_str()
            .unwrap()
            .into();
        actual.source.backup = Path::new(&actual.source.backup)
            .strip_prefix(roots[1])
            .unwrap()
            .to_str()
            .unwrap()
            .into();
        assert_eq!(actual, expected);
    }
}

async fn assert_stored_equal(a: &Rig, b: &Rig, sessions: &[BindIdentity]) {
    for identity in sessions {
        let a = a.runtime.current_view(identity).await.unwrap().unwrap();
        let b = b.runtime.current_view(identity).await.unwrap().unwrap();
        assert_eq!(
            serde_json::to_value(&a.conversation.messages).unwrap(),
            serde_json::to_value(&b.conversation.messages).unwrap()
        );
        assert_eq!(a.operations.source_lineage, b.operations.source_lineage);
        assert_eq!(a.operations.history_version, b.operations.history_version);
        assert_eq!(broca_module::store_read::transcript_len(&a), 4);
        assert_eq!(broca_module::store_read::transcript_len(&b), 4);
    }
    for (display, tools, rich_ui) in [(false, false, false), (true, true, true)] {
        for identity in sessions {
            let mut request =
                broca_module::serve::SessionReadRequest::new(identity.clone(), Some(1), Some(10));
            request.include_display_facts = display;
            request.include_tools = tools;
            request.include_rich_ui = rich_ui;
            let mut replies = Vec::new();
            for rig in [a, b] {
                let view = rig.runtime.current_view(identity).await.unwrap().unwrap();
                let response = broca_module::store_read::serve_view(
                    &view,
                    rig.runtime
                        .parts_for(StorageClass::Ordinary, view.head().cipher()),
                    &request,
                )
                .await
                .unwrap();
                let mut value = serde_json::to_value(response).unwrap();
                // These two fields contain fresh storage IDs rather than converted content.
                value.as_object_mut().unwrap().remove("incarnation");
                value.as_object_mut().unwrap().remove("head");
                replies.push(value);
            }
            assert_eq!(
                replies[0].as_object().unwrap().len(),
                replies[1].as_object().unwrap().len()
            );
            for (field, expected) in replies[0].as_object().unwrap() {
                assert_eq!(
                    replies[1].get(field),
                    Some(expected),
                    "session.read field {field}"
                );
            }
        }
    }
}

#[tokio::test]
async fn concurrent_conversion_matches_sequential_ledger_and_stored_fields() {
    let sequential = Rig::start("conversion-sequential").await;
    let concurrent = Rig::start("conversion-concurrent").await;
    let sessions = identities(4);
    write_sessions(&sequential, &sessions);
    for identity in &sessions {
        let source = sequential.wal_store.wal_path(identity);
        let destination = concurrent.wal_store.wal_path(identity);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::copy(source, destination).unwrap();
    }
    let mut serial = engine(&sequential, false, true);
    serial.set_parallelism_for_test(1);
    serial
        .run_live(
            &deployment(),
            Some(&sequential.runtime),
            Some(&sequential.registry),
        )
        .await
        .unwrap();
    let gate = probe(0);
    let mut parallel = engine(&concurrent, false, true);
    parallel.parallel_probe = Some(gate.clone());
    let task = run_live(&concurrent, parallel.clone());
    entered(&gate, 2).await;
    assert_eq!(
        gate.sources.lock().unwrap().len(),
        2,
        "the serving limit is two"
    );
    assert!(parallel
        .ledger
        .rows()
        .unwrap()
        .iter()
        .all(|row| row.step == "discovered"));
    gate.release.add_permits(4);
    task.await.unwrap().unwrap();
    assert_eq!(gate.sources.lock().unwrap().len(), 4);
    assert_rows_equal(
        &serial,
        &parallel,
        [
            &sequential
                .dir
                .with_extension("converted-sources")
                .canonicalize()
                .unwrap(),
            &concurrent
                .dir
                .with_extension("converted-sources")
                .canonicalize()
                .unwrap(),
        ],
    );
    assert_stored_equal(&sequential, &concurrent, &sessions).await;
    assert_progress(&serial);
    assert_progress(&parallel);
    assert_eq!(parallel.health_for_test().live_remaining, 0);
    assert_eq!(
        parallel
            .ledger
            .progress()
            .unwrap()
            .outcomes
            .get("converted"),
        Some(&4)
    );
}

#[tokio::test]
async fn concurrent_conversion_crash_recovers_each_sessions_own_ledger_boundary() {
    for (first, second) in [
        ("recovering", "staging"),
        ("verifying", "verified"),
        ("publishing", "awaiting_backup"),
        ("backing_up", "done"),
    ] {
        let rig = Rig::start(&format!("parallel-crash-{first}-{second}")).await;
        let sessions = identities(2);
        write_sessions(&rig, &sessions);
        let before: Vec<_> = sessions
            .iter()
            .map(|s| std::fs::read(rig.wal_store.wal_path(s)).unwrap())
            .collect();
        let mut gate = probe(2);
        let mutable = Arc::get_mut(&mut gate).unwrap();
        for (identity, step) in sessions.iter().zip([first, second]) {
            mutable.crashes.insert(
                format!("wal/{}.wal", broca_core::session_addr(identity)),
                step,
            );
        }
        mutable.crash_barrier = Some(std::sync::Barrier::new(2));
        let mut conversion = engine(&rig, false, true);
        conversion.parallel_probe = Some(gate);
        let error = conversion
            .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
            .await
            .unwrap_err();
        assert!([first, second].contains(&error.as_str()), "{error}");
        for (identity, step) in sessions.iter().zip([first, second]) {
            let id = format!("wal/{}.wal", broca_core::session_addr(identity));
            assert_eq!(conversion.ledger.row(&id).unwrap().unwrap().step, step);
        }
        assert_progress(&conversion);
        conversion.parallel_probe = None;
        conversion
            .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
            .await
            .unwrap();
        assert_progress(&conversion);
        for (identity, bytes) in sessions.iter().zip(before) {
            let id = format!("wal/{}.wal", broca_core::session_addr(identity));
            let row = conversion.ledger.row(&id).unwrap().unwrap();
            assert_eq!(row.step, "done");
            assert_eq!(row.outcome.as_deref(), Some("converted"));
            assert_eq!(std::fs::read(row.source.backup).unwrap(), bytes);
            let view = rig.runtime.current_view(identity).await.unwrap().unwrap();
            assert_eq!(broca_module::store_read::transcript_len(&view), 4);
            assert_eq!(rig.registry.conversion_refusal(identity), None);
        }
        assert_eq!(conversion.health_for_test().live_remaining, 0);
    }
}

#[tokio::test]
async fn concurrent_conversion_stopping_drains_two_fenced_sessions_without_admitting_more() {
    let rig = Rig::start("parallel-stop").await;
    let sessions = identities(4);
    write_sessions(&rig, &sessions);
    let gate = Arc::new(crate::session_conversion::BackgroundProbe {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
        sessions: Mutex::default(),
    });
    let mut conversion = engine(&rig, true, false);
    conversion.background_fenced = Some(gate.clone());
    let task = {
        let conversion = conversion.clone();
        let runtime = rig.runtime.clone();
        let registry = rig.registry.clone();
        tokio::spawn(async move {
            conversion
                .run_archives(
                    &deployment(),
                    Some(&runtime),
                    Some(&registry),
                    Duration::ZERO,
                )
                .await
        })
    };
    broca_test_support::wait_until("two idle conversion fences are held", || async {
        let count = gate.sessions.lock().unwrap().len();
        if count == 2 {
            Ok(())
        } else {
            Err(format!("held {count} fences"))
        }
    })
    .await;
    rig.registry.begin_stop();
    gate.release.add_permits(2);
    task.await.unwrap().unwrap();
    assert_eq!(gate.sessions.lock().unwrap().len(), 2);
    let rows = conversion.ledger.rows().unwrap();
    assert_eq!(rows.iter().filter(|row| row.step == "done").count(), 2);
    assert_eq!(
        rows.iter().filter(|row| row.step == "discovered").count(),
        2
    );
    assert_eq!(conversion.health_for_test().live_background_converted, 2);
    assert_eq!(conversion.health_for_test().live_remaining, 2);
    assert_progress(&conversion);
    for identity in &sessions {
        assert_eq!(rig.registry.conversion_refusal(identity), None);
    }
}

#[tokio::test]
async fn concurrent_conversion_failed_session_keeps_its_row_and_others_convert() {
    let rig = Rig::start("parallel-failure").await;
    let sessions = identities(4);
    write_sessions(&rig, &sessions);
    let bad = sessions[0].clone();
    let before = std::fs::read(rig.wal_store.wal_path(&bad)).unwrap();
    let mover = rig.mover().with_run_rows(Arc::new(move |session| {
        if session == &bad {
            let mut row = settled_row(session, "r1");
            row.usage = None;
            Ok((vec![row], false))
        } else {
            Ok((Vec::new(), true))
        }
    }));
    let conversion = Engine::configured(
        &rig.dir,
        Config {
            archives: false,
            live_at_startup: true,
            backup_root: Some(rig.dir.with_extension("converted-sources")),
        },
    )
    .unwrap()
    .unwrap();
    conversion.set_mover(Arc::new(mover));
    conversion
        .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
        .await
        .unwrap();
    for (index, identity) in sessions.iter().enumerate() {
        let id = format!("wal/{}.wal", broca_core::session_addr(identity));
        let row = conversion.ledger.row(&id).unwrap().unwrap();
        if index == 0 {
            assert_eq!(row.outcome.as_deref(), Some("failed"));
            assert_eq!(row.refusal.as_deref(), Some("reconcile_owed"));
            assert!(row
                .detail
                .as_deref()
                .is_some_and(|detail| !detail.is_empty()));
            assert_eq!(
                std::fs::read(rig.wal_store.wal_path(identity)).unwrap(),
                before
            );
            assert!(!Path::new(&row.source.backup).exists());
        } else {
            assert_eq!(row.step, "done");
            assert_eq!(row.outcome.as_deref(), Some("converted"));
            assert!(rig.runtime.current_view(identity).await.unwrap().is_some());
        }
    }
    assert_eq!(conversion.health_for_test().live_remaining, 1);
    assert_eq!(conversion.health_for_test().failed_sessions.len(), 1);
    assert_progress(&conversion);
}

#[tokio::test]
async fn concurrent_conversion_converts_one_containers_members_together_and_moves_it_once() {
    let rig = Rig::start("parallel-containers").await;
    let sessions = identities(4);
    write_sessions(&rig, &sessions);
    let mut originals = BTreeMap::new();
    for (index, members) in sessions.chunks(2).enumerate() {
        let path = rig
            .dir
            .join(format!("wal-archive/fold-{}.ark", 2000 + index));
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        let mut writer = broca_wal::archive::ArchiveWriter::create(&path)
            .unwrap()
            .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
        for identity in members {
            let wal = rig.wal_store.wal_path(identity);
            writer
                .append(
                    &broca_core::session_addr(identity),
                    &std::fs::read(&wal).unwrap(),
                )
                .unwrap();
            std::fs::remove_file(wal).unwrap();
        }
        writer.seal().unwrap();
        originals.insert(
            path.strip_prefix(&rig.dir)
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned(),
            std::fs::read(path).unwrap(),
        );
    }
    let gate = probe(0);
    let mut conversion = engine(&rig, true, false);
    conversion.parallel_probe = Some(gate.clone());
    let task = {
        let conversion = conversion.clone();
        let runtime = rig.runtime.clone();
        let registry = rig.registry.clone();
        tokio::spawn(async move {
            conversion
                .run_archives(
                    &deployment(),
                    Some(&runtime),
                    Some(&registry),
                    Duration::ZERO,
                )
                .await
        })
    };
    entered(&gate, 2).await;
    let first = gate.sources.lock().unwrap().clone();
    // The queue holds one container's members first; they are different sessions, so both
    // workers take them at once rather than one container each.
    assert_eq!(
        first[0].path, first[1].path,
        "members of one container convert together"
    );
    assert_ne!(first[0].member, first[1].member);
    assert!(gate.overlap.load(Ordering::Relaxed));
    assert!(
        broca_core::lease::acquire_archive_pass(&rig.dir).is_err(),
        "external folds remain excluded"
    );
    // Released together, the members of each container finish together; the container's
    // lock lets exactly one of them move it, after both are done.
    gate.release.add_permits(4);
    task.await.unwrap().unwrap();
    assert_eq!(gate.sources.lock().unwrap().len(), 4);
    assert!(gate
        .active
        .lock()
        .unwrap()
        .values()
        .all(|count| *count == 0));
    for row in conversion.ledger.rows().unwrap() {
        assert_eq!(row.step, "done");
        assert_eq!(row.outcome.as_deref(), Some("converted"));
        assert_eq!(
            std::fs::read(&row.source.backup).unwrap(),
            originals[&row.source.path]
        );
        assert!(!rig.dir.join(&row.source.path).exists());
    }
    assert!(broca_core::lease::acquire_archive_pass(&rig.dir).is_ok());
    assert_progress(&conversion);
}
