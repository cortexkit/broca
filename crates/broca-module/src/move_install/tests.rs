use super::*;
use broca_core::{ExportIndex, WalStore};
use broca_protocol::{RunFinishReason, RunId, Usage};
use broca_wal::{RunConfig, RunOrigin, WalRecord};
fn root(tag: &str) -> std::path::PathBuf {
    let root = broca_test_support::scratch_root()
        .join(format!("move-install-{}-{tag}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    root
}
pub(crate) fn started(session: &BindIdentity, id: &str) -> WalRecord {
    WalRecord::RunStarted {
        tags: None,
        session_class: None,
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        run_id: RunId(id.into()),
        session: session.clone(),
        ts_ms: Some(1),
        config: Box::new(RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            model: broca_protocol::ModelHandle::new("stub", "m"),
            tools: vec![].into(),
            tool_choice: broca_protocol::ToolChoice::Auto,
            generation: Default::default(),
            response_format: Default::default(),
            provider_options: Value::Null,
            cache_tiers: Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: vec![],
            context_management: None,
        }),
        input: Default::default(),
        origin: RunOrigin::Fresh,
        submission_id: None,
    }
}
pub(crate) fn episode(wal: &mut dyn broca_wal::Wal, session: &BindIdentity, id: &str) {
    wal.append(1, &started(session, id), true).unwrap();
    wal.append(
        1,
        &WalRecord::RunFinished {
            interrupt_reason: None,
            reason: RunFinishReason::Completed,
            ts_ms: Some(1),
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        },
        true,
    )
    .unwrap();
}

#[test]
fn whole_ref_set_divergence_refuses_before_any_installation_or_floor() {
    let root = root("preflight");
    let wal_root = root.join("wal");
    broca_wal::migration::durable_dir(&root, &wal_root).unwrap();
    let a = BindIdentity::new("/p", "runner", "a");
    let b = BindIdentity::new("/p", "runner", "b");
    let staged_a = root.join("a.stage");
    let staged_b = root.join("b.stage");
    for (path, session) in [(&staged_a, &a), (&staged_b, &b)] {
        let mut writer = broca_wal::FileWal::open(path).unwrap();
        episode(&mut writer, session, &session.session);
    }
    let live_a = wal_root.join("a.wal");
    let live_b = wal_root.join("b.wal");
    std::fs::write(&live_b, b"advanced").unwrap();
    let bytes_a = std::fs::read(&staged_a).unwrap();
    let bytes_b = std::fs::read(&staged_b).unwrap();
    let digest_a = broca_wal::migration::digest(&bytes_a);
    let digest_b = broca_wal::migration::digest(&bytes_b);
    let specs = [
        Installation {
            session: &a,
            staged: &staged_a,
            live: &live_a,
            local_archive: None,
            end_offset: bytes_a.len() as u64,
            digest: &digest_a,
        },
        Installation {
            session: &b,
            staged: &staged_b,
            live: &live_b,
            local_archive: None,
            end_offset: bytes_b.len() as u64,
            digest: &digest_b,
        },
    ];
    let export = broca_store::SqliteStore::open_in_memory().unwrap();
    let drops = std::sync::atomic::AtomicUsize::new(0);
    let result = install_verified(&root, "cut", &specs, &export, |_| {
        drops.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    });
    assert!(
        !live_a.exists(),
        "ref A was installed before checking ref B"
    );
    assert!(staged_a.exists());
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 0);
    assert_eq!(export.high_water(), 0);
    assert_eq!(result.unwrap_err().code.as_str(), "local_advanced");
    assert_eq!(std::fs::read(live_b).unwrap(), b"advanced");
}

#[test]
fn imported_floor_excludes_foreign_runs_and_preserves_local_billing_after_recovery() {
    let root = root("floor");
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/p", "runner", "a");
    let old = {
        let mut writer = wal.open(&session).unwrap();
        episode(writer.as_mut(), &session, "local");
        std::fs::read(wal.wal_path(&session)).unwrap()
    };
    let incoming_path = root.join("incoming.wal");
    std::fs::write(&incoming_path, &old).unwrap();
    {
        let mut writer = broca_wal::FileWal::open(&incoming_path).unwrap();
        episode(&mut writer, &session, "imported");
    }
    let incoming = std::fs::read(incoming_path).unwrap();
    let digest = broca_wal::migration::digest(&incoming);
    let staged = broca_wal::migration::staging_path(wal.root(), "cut", "a").unwrap();
    broca_wal::migration::stage(&root, &staged, &incoming, &digest).unwrap();
    let live = wal.wal_path(&session);
    broca_wal::migration::atomic_json(
        &root,
        &live.with_extension("moved"),
        &broca_wal::migration::Moved {
            cut_id: "old".into(),
            holder: "remote".into(),
            lineage_id: "old-lineage".into(),
        },
    )
    .unwrap();
    let specs = [Installation {
        session: &session,
        staged: &staged,
        live: &live,
        local_archive: None,
        end_offset: incoming.len() as u64,
        digest: &digest,
    }];
    let export = broca_store::SqliteStore::open(root.join("run-index.db")).unwrap();
    let result = install_verified(&root, "cut", &specs, &export, |_| {
        Err("crash after rename".into())
    });
    assert_eq!(result.unwrap_err().code.as_str(), "unavailable");
    assert_eq!(std::fs::read(&live).unwrap(), incoming);
    assert!(!staged.exists());
    drop(export);
    let export = broca_store::SqliteStore::open(root.join("run-index.db")).unwrap();
    install_verified(&root, "cut", &specs, &export, |_| Ok(())).unwrap();
    assert!(!live.with_extension("moved").exists());
    let records = broca_wal::replay_file(&live, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    let resolver = |_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1);
    export
        .emit_run(&records, &RunId("imported".into()), &resolver)
        .unwrap();
    assert_eq!(
        export.high_water(),
        0,
        "imported run minted local facts after installation recovery"
    );
    export
        .emit_run(&records, &RunId("local".into()), &resolver)
        .unwrap();
    assert!(
        export.high_water() > 0,
        "pre-existing local run was suppressed"
    );
}

#[test]
fn incoming_identity_digest_and_reader_format_are_checked_before_staging() {
    let root = root("incoming");
    let store = broca_core::FileWalStore::new(root.join("wal"));
    let session = BindIdentity::new("/p", "runner", "a");
    {
        let mut writer = store.open(&session).unwrap();
        episode(writer.as_mut(), &session, "imported");
    }
    let bytes = std::fs::read(store.wal_path(&session)).unwrap();
    let digest = broca_wal::migration::digest(&bytes);
    let reference = broca_wire::agent_data::session_ref("runner", "/p", "a");
    assert_eq!(
        verify_received(&reference, &bytes, bytes.len() as u64, &digest).unwrap(),
        session
    );
    assert_eq!(
        verify_received("other", &bytes, bytes.len() as u64, &digest)
            .unwrap_err()
            .code
            .as_str(),
        "invalid_request"
    );
    assert_eq!(
        verify_received(&reference, &bytes, bytes.len() as u64, "bad")
            .unwrap_err()
            .code
            .as_str(),
        "digest_mismatch"
    );
    assert_eq!(
        check_format("broca-wal/v3;features=future/v9")
            .unwrap_err()
            .code
            .as_str(),
        "incompatible_version"
    );
    check_format("broca-wal/v3;features=").unwrap();
    assert!(!root.join("wal-staging").exists());
}

#[test]
fn mixed_run_started_triples_resolve_by_ref_not_record_position() {
    let root = root("mixed-triples");
    let store = broca_core::FileWalStore::new(root.join("wal"));
    let a = BindIdentity::new("/before", "runner", "s");
    let b = BindIdentity::new("/after", "runner", "s");
    {
        let mut writer = store.open(&a).unwrap();
        episode(writer.as_mut(), &a, "old");
        episode(writer.as_mut(), &b, "new");
    }
    let bytes = std::fs::read(store.wal_path(&a)).unwrap();
    let digest = broca_wal::migration::digest(&bytes);
    for session in [&a, &b] {
        let reference = broca_wire::agent_data::session_ref(
            &session.harness,
            &session.project_root.to_string_lossy(),
            &session.session,
        );
        let resolved = verify_received(&reference, &bytes, bytes.len() as u64, &digest).unwrap();
        assert_eq!(&resolved, session);
        assert_eq!(store.wal_path(&resolved), store.wal_path(session));
    }
    let error = verify_received("unknown-ref", &bytes, bytes.len() as u64, &digest).unwrap_err();
    assert_eq!(error.code.as_str(), "invalid_request");
    assert!(error.message.contains("unknown-ref"));
}

#[test]
fn rekey_without_a_run_for_the_new_triple_is_refused_without_cross_module_lookup() {
    let root = root("rekey-no-run");
    let store = broca_core::FileWalStore::new(root.join("wal"));
    let old = BindIdentity::new("/before", "runner", "s");
    let current = BindIdentity::new("/after", "runner", "s");
    {
        let mut writer = store.open(&old).unwrap();
        episode(writer.as_mut(), &old, "old");
    }
    std::fs::rename(store.wal_path(&old), store.wal_path(&current)).unwrap();
    let bytes = std::fs::read(store.wal_path(&current)).unwrap();
    let digest = broca_wal::migration::digest(&bytes);
    let reference = broca_wire::agent_data::session_ref("runner", "/after", "s");
    let error = verify_received(&reference, &bytes, bytes.len() as u64, &digest).unwrap_err();
    assert_eq!(error.code.as_str(), "invalid_request");
    assert!(error.message.contains(&reference));
    assert!(!root.join("wal-staging").exists());
}

#[test]
fn mixed_identity_import_excludes_every_historical_run_and_bills_fresh_receiver_run() {
    let root = root("mixed-billing");
    broca_wal::migration::durable_dir(&root, &root).unwrap();
    let historical = BindIdentity::new("/p", "runner", "s");
    let current = BindIdentity::new("/p", "broca", "s");
    let incoming_path = root.join("incoming.wal");
    {
        let mut writer = broca_wal::FileWal::open(&incoming_path).unwrap();
        episode(&mut writer, &historical, "old");
        episode(&mut writer, &current, "new");
    }
    let records =
        broca_wal::replay_file(&incoming_path, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    let resolver = |_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1);
    let source = broca_store::SqliteStore::open(root.join("source.db")).unwrap();
    for id in ["old", "new"] {
        source
            .emit_run(&records, &RunId(id.into()), &resolver)
            .unwrap();
    }
    assert_eq!(
        source.high_water(),
        2,
        "both historical episodes produce real spend facts"
    );
    let bytes = std::fs::read(incoming_path).unwrap();
    let digest = broca_wal::migration::digest(&bytes);
    let wal = broca_core::FileWalStore::new(root.join("wal"));
    let staged = broca_wal::migration::staging_path(
        wal.root(),
        "mixed-cut",
        &broca_core::session_addr(&current),
    )
    .unwrap();
    broca_wal::migration::stage(&root, &staged, &bytes, &digest).unwrap();
    let live = wal.wal_path(&current);
    let receiver_path = root.join("receiver.db");
    let receiver = broca_store::SqliteStore::open(&receiver_path).unwrap();
    install_verified(
        &root,
        "mixed-cut",
        &[Installation {
            session: &current,
            staged: &staged,
            live: &live,
            local_archive: None,
            end_offset: bytes.len() as u64,
            digest: &digest,
        }],
        &receiver,
        |_| Ok(()),
    )
    .unwrap();
    drop(receiver);
    let receiver = broca_store::SqliteStore::open(receiver_path).unwrap();
    let installed = broca_wal::replay_file(&live, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    for id in ["old", "new"] {
        receiver
            .emit_run(&installed, &RunId(id.into()), &resolver)
            .unwrap();
        assert_eq!(
            receiver.high_water(),
            0,
            "foreign run {id} escaped its historical session's billing floor"
        );
    }
    episode(
        wal.open(&current).unwrap().as_mut(),
        &current,
        "fresh-receiver",
    );
    let installed = broca_wal::replay_file(&live, broca_wal::DEFAULT_MAX_READ_BYTES).unwrap();
    receiver
        .emit_run(&installed, &RunId("fresh-receiver".into()), &resolver)
        .unwrap();
    assert_eq!(
        receiver.high_water(),
        1,
        "a fresh receiver run must still bill"
    );
}

#[cfg(unix)]
#[test]
fn already_renamed_activation_repays_the_staging_source_barrier() {
    use broca_wal::migration::{
        self,
        io_trace::{self, Event},
    };
    use std::{cell::Cell, rc::Rc};
    let root = root("activation-source-barrier");
    let session = BindIdentity::new("/p", "runner", "incoming");
    let incoming = root.join("incoming.wal");
    {
        let mut writer = broca_wal::FileWal::open(&incoming).unwrap();
        episode(&mut writer, &session, "foreign");
    }
    let bytes = std::fs::read(&incoming).unwrap();
    let staged = root.join("wal-staging/cut/incoming.wal");
    let live = root.join("wal/incoming.wal");
    let digest = migration::digest(&bytes);
    migration::stage(&root, &staged, &bytes, &digest).unwrap();
    let specs = [Installation {
        session: &session,
        staged: &staged,
        live: &live,
        local_archive: None,
        end_offset: bytes.len() as u64,
        digest: &digest,
    }];
    let export = broca_store::SqliteStore::open(root.join("run-index.db")).unwrap();
    let source = staged.parent().unwrap().to_owned();
    let drops = Cell::new(0);
    {
        let target = source.clone();
        let _trace = io_trace::install(move |event| {
            if event == Event::BeforeDirectorySync(target.clone()) {
                return Err(std::io::Error::other(
                    "injected rename source-directory barrier",
                ));
            }
            Ok(())
        });
        assert!(install_verified(&root, "cut", &specs, &export, |_| {
            drops.set(drops.get() + 1);
            Ok(())
        })
        .is_err());
    }
    assert!(!staged.exists());
    assert_eq!(std::fs::read(&live).unwrap(), bytes);
    assert_eq!(drops.get(), 0);
    {
        let target = source.clone();
        let _trace = io_trace::install(move |event| {
            if event == Event::BeforeDirectorySync(target.clone()) {
                return Err(std::io::Error::other(
                    "injected recovery source-directory barrier",
                ));
            }
            Ok(())
        });
        let error = install_verified(&root, "cut", &specs, &export, |_| {
            drops.set(drops.get() + 1);
            Ok(())
        })
        .unwrap_err();
        assert!(error
            .message
            .contains("injected recovery source-directory barrier"));
    }
    assert_eq!(
        drops.get(),
        0,
        "projection cleanup must follow the source-directory barrier"
    );
    let synced = Rc::new(Cell::new(false));
    let observed = synced.clone();
    let _trace = io_trace::install(move |event| {
        if event == Event::DirectorySync(source.clone()) {
            observed.set(true);
        }
        Ok(())
    });
    install_verified(&root, "cut", &specs, &export, |_| {
        assert!(
            synced.get(),
            "recovery must finish staging removal before projection cleanup"
        );
        drops.set(drops.get() + 1);
        Ok(())
    })
    .unwrap();
    assert_eq!(drops.get(), 1);
    assert_eq!(std::fs::read(&live).unwrap(), bytes);
}
