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
        .resolve_session_address(&real, None)
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
