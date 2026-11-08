use super::*;

#[tokio::test]
async fn repair_one_wrong_root_restores_full_session_read_and_preserves_stub() {
    use broca_core::SessionRefIndex;
    let (rig, old, real, real_bytes) = stale_inventory_fixture().await;
    let history = rig
        .wal_store
        .read_records_range(&real, 0, None, None)
        .unwrap();
    rig.store.project_range(&old, &history).unwrap();
    let hint = BindIdentity::new(&real.project_root, &real.harness, "root-hint");
    rig.store
        .record_session(&broca_core::session_addr(&hint), &hint)
        .unwrap();
    drop(rig.wal_store.open(&old).unwrap());
    let stub_bytes = std::fs::read(rig.wal_store.wal_path(&old)).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(rig.wal_store.wal_path(&real))
        .unwrap()
        .set_times(
            std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH + Duration::from_secs(10)),
        )
        .unwrap();
    let complete = broca_store::session_refs::migrate(
        &rig.dir.join("store.db"),
        &broca_core::ReadOnlyWalReader::new(&rig.dir),
    )
    .unwrap();
    assert!(complete.complete);
    assert!(matches!(
        rig.registry.read(read_request(&old, None, Some(20))).await,
        Err(broca_module::serve::ServeError::SessionLogMissing { .. })
    ));
    let mut findings = Vec::new();
    assert_eq!(
        rig.store
            .audit_session_refs(|finding| {
                findings.push(serde_json::to_value(finding)?);
                Ok(())
            })
            .unwrap(),
        1
    );
    assert_eq!(findings[0]["state"], "forked_stub");
    assert_eq!(
        findings[0]["alternatives"][0]["address"],
        broca_core::session_addr(&real)
    );
    let plan = rig
        .store
        .preview_session_root_repair(&old.harness, &old.session)
        .unwrap();
    assert_eq!(
        plan.repoint_runs.len(),
        1,
        "the one wrong-root ref is not a no-op"
    );
    rig.store.apply_session_root_repair(&plan).unwrap();
    let resolved = rig
        .registry
        .resolve_session_address(&real, None, None)
        .await
        .unwrap();
    assert_eq!(resolved, real);
    let page = rig
        .registry
        .read(read_request(&resolved, None, Some(20)))
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 2);
    assert!(page.messages[0]
        .message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::Text { text } if text == "real content")));
    assert!(page.messages[1]
        .message
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::Text { text } if text == "real answer")));
    assert!(!rig.wal_store.wal_path(&old).exists());
    let aside: Vec<_> = std::fs::read_dir(rig.dir.join("wal/repair-aside"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .collect();
    assert_eq!(aside.len(), 1);
    assert_eq!(std::fs::read(&aside[0]).unwrap(), stub_bytes);
    assert_eq!(
        std::fs::read(rig.wal_store.wal_path(&real)).unwrap(),
        real_bytes
    );
    assert_eq!(rig.store.audit_session_refs(|_| Ok(())).unwrap(), 0);
    let repeated = rig
        .store
        .preview_session_root_repair(&old.harness, &old.session)
        .unwrap();
    assert!(repeated.stale_identity.is_none());
    rig.store.apply_session_root_repair(&repeated).unwrap();
    assert_eq!(std::fs::read(&aside[0]).unwrap(), stub_bytes);
}

#[tokio::test]
async fn legacy_null_lineage_stub_read_refuses_and_repair_restores_all_runs() {
    use broca_core::SessionRefIndex;
    let rig = Rig::new("legacy-null-lineage-fork", Arc::new(NoopExec));
    let old = BindIdentity::new(rig.dir.join("old"), "broca", "legacy-fork");
    let real = BindIdentity::new(rig.dir.join("new"), "broca", "legacy-fork");
    let mut records = Vec::new();
    for n in 0..3 {
        let run = format!("legacy-{n}");
        records.push(started(&old, &run, "legacy question"));
        records.push(assistant_text(0, "legacy answer"));
        records.push(WalRecord::TurnFinished { step_id: StepId(0) });
        records.push(finished_ok());
        rig.store
            .index_run_active(&RunIndexRow {
                run_id: run,
                session: old.clone(),
                submission_id: None,
                request_identity_digest: None,
                state: RunIndexState::Completed,
                usage: None,
                final_step_finish_reason: None,
                retries_used: 0,
                title: None,
                withdrawals_owed: false,
            })
            .unwrap();
    }
    broca_store::conformance::write_legacy_filewal(&rig.wal_store.wal_path(&real), &records);
    let history = rig
        .wal_store
        .read_records_range(&real, 0, None, None)
        .unwrap();
    assert!(history.lineage_id.is_none());
    let broca_wal::WalRangeRecords::Complete(frames) = history.records() else {
        panic!("complete legacy history required");
    };
    assert_eq!(frames.len(), 12);
    let hint = BindIdentity::new(&real.project_root, "broca", "root-hint");
    rig.store
        .record_session(&broca_core::session_addr(&hint), &hint)
        .unwrap();
    let migrated = broca_store::session_refs::migrate(
        &rig.dir.join("store.db"),
        &broca_core::ReadOnlyWalReader::new(&rig.dir),
    )
    .unwrap();
    assert!(migrated.complete);
    std::fs::OpenOptions::new()
        .write(true)
        .open(rig.wal_store.wal_path(&real))
        .unwrap()
        .set_times(
            std::fs::FileTimes::new().set_modified(std::time::UNIX_EPOCH + Duration::from_secs(10)),
        )
        .unwrap();
    drop(rig.wal_store.open(&old).unwrap());
    let original = std::fs::read(rig.wal_store.wal_path(&real)).unwrap();
    let stub = std::fs::read(rig.wal_store.wal_path(&old)).unwrap();
    assert_eq!(stub.len(), 69);
    assert!(rig
        .store
        .session_log_evidence(&old)
        .unwrap()
        .lineages
        .is_empty());
    rig.registry
        .set_session_address_inventory(rig.store.clone());
    let read = rig.registry.read(read_request(&old, None, Some(20))).await;
    assert!(
        matches!(
            &read,
            Err(broca_module::serve::ServeError::SessionLogMissing { .. })
        ),
        "{read:?}"
    );
    let plan = rig
        .store
        .preview_session_root_repair("broca", "legacy-fork")
        .unwrap();
    assert_eq!(plan.repoint_runs.len(), 3);
    let aside = plan.move_stub_aside.as_ref().unwrap().to.clone();
    rig.store.apply_session_root_repair(&plan).unwrap();
    let resolved = rig
        .registry
        .resolve_session_address(&real, None, None)
        .await
        .unwrap();
    assert_eq!(resolved, real);
    let page = rig
        .registry
        .read(read_request(&resolved, None, Some(20)))
        .await
        .unwrap();
    assert_eq!(page.messages.len(), 6);
    assert_eq!(std::fs::read(&aside).unwrap(), stub);
    assert_eq!(
        std::fs::read(rig.wal_store.wal_path(&real)).unwrap(),
        original
    );
    assert!(rig
        .wal_store
        .read_records_range(&real, 0, None, None)
        .unwrap()
        .lineage_id
        .is_none());
}
