use super::*;

const PRIVATE_CUT: &str = "private-retention-cut-content";

fn seed_cut_artifacts(rig: &Rig) -> (String, Vec<String>) {
    let id = rig.seed(1, 100, Some(200));
    let _lease = rig.service.lease(&rig.key).unwrap();
    let names = add_cut_artifacts(rig, &id);
    (id, names)
}

fn add_cut_artifacts(rig: &Rig, id: &str) -> Vec<String> {
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let mut wal = rig
        .service
        .wal
        .open_with_retention(&rig.key, Some(1))
        .unwrap();
    let cut = fs::metadata(&path).unwrap().len() as usize;
    let mut removed = started(&rig.key, "removed-run", Some(1), 300);
    let WalRecord::RunStarted { title, .. } = &mut removed else {
        unreachable!()
    };
    *title = Some(PRIVATE_CUT.into());
    wal.append(1, &removed, true).unwrap();
    wal.append(1, &finished(400), true).unwrap();
    drop(wal);
    let mut bytes = fs::read(&path).unwrap();
    bytes[cut + 4] = 0;
    fs::write(&path, bytes).unwrap();
    drop(
        rig.service
            .wal
            .open_with_retention(&rig.key, Some(1))
            .unwrap(),
    );
    let owner_prefix = format!("{}.cut-{id}-", path.file_name().unwrap().to_str().unwrap());
    let cuts = broca_wal::recovery::manifests(&path, 1_000_000)
        .unwrap()
        .into_iter()
        .filter(|cut| cut.recovery.sidecar_basename.starts_with(&owner_prefix))
        .collect::<Vec<_>>();
    assert_eq!(cuts.len(), 1);
    let raw = cuts[0].recovery.sidecar_basename.clone();
    assert!(fs::read(path.parent().unwrap().join(&raw))
        .unwrap()
        .windows(PRIVATE_CUT.len())
        .any(|bytes| bytes == PRIVATE_CUT.as_bytes()));
    let base = format!(
        "{}.cut-{id}-orphan",
        path.file_name().unwrap().to_str().unwrap()
    );
    let mut names = vec![raw.clone(), format!("{raw}.json")];
    for name in [
        base.clone(),
        format!("{base}.tmp"),
        format!("{base}.json.tmp"),
    ] {
        fs::write(path.parent().unwrap().join(&name), PRIVATE_CUT).unwrap();
        names.push(name);
    }
    names.sort();
    names
}

#[test]
fn leased_completion_removes_raw_manifest_and_orphan_cut_content() {
    let rig = Rig::overhaul();
    let (id, names) = seed_cut_artifacts(&rig);
    let _lease = rig.service.lease(&rig.key).unwrap();
    let captured = rig
        .service
        .capture_recovery_artifacts(&rig.service.wal.ephemeral_wal_path(&rig.key))
        .unwrap();
    assert_eq!(captured, names);
    let stone = Tombstone {
        recovery_artifacts: captured,
        lineage_id: id,
        expired_at_ms: 1200,
        seq: 1,
        completed_at_ms: None,
        next_tag: None,
        next_notice_seq: None,
    };
    rig.service.remember_session(&rig.key).unwrap();
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    // Exercise the leased disk state machine without enabling the separately
    // gated background rollout for overhaul roots.
    rig.service
        .complete_deletion(&rig.key, stone, 1300)
        .unwrap();
    assert!(!rig.service.wal.ephemeral_wal_path(&rig.key).exists());
    for name in names {
        assert!(
            !rig.service.wal.ephemeral_root().join(&name).exists(),
            "deleted transcript bytes survived in {name}"
        );
    }
    assert!(rig.service.tombstones(&rig.key).unwrap()[0]
        .completed_at_ms
        .is_some());
}

fn prepare_stone(rig: &Rig, id: String, names: &[String]) -> Tombstone {
    let captured = rig
        .service
        .capture_recovery_artifacts(&rig.service.wal.ephemeral_wal_path(&rig.key))
        .unwrap();
    assert_eq!(captured, names);
    let stone = Tombstone {
        lineage_id: id,
        expired_at_ms: 1200,
        seq: 1,
        completed_at_ms: None,
        next_tag: None,
        next_notice_seq: None,
        recovery_artifacts: captured,
    };
    rig.service.remember_session(&rig.key).unwrap();
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    stone
}

#[test]
fn leased_cut_removal_failure_keeps_completion_retryable() {
    let rig = Rig::overhaul();
    let (id, names) = seed_cut_artifacts(&rig);
    let _lease = rig.service.lease(&rig.key).unwrap();
    let stone = prepare_stone(&rig, id, &names);
    let blocked = rig.service.wal.ephemeral_root().join(&names[0]);
    let bytes = fs::read(&blocked).unwrap();
    fs::remove_file(&blocked).unwrap();
    fs::create_dir(&blocked).unwrap();
    assert!(rig
        .service
        .complete_deletion(&rig.key, stone, 1300)
        .is_err());
    let pending = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
    assert!(pending.completed_at_ms.is_none());
    fs::remove_dir(&blocked).unwrap();
    fs::write(&blocked, bytes).unwrap();
    fs::remove_file(rig.service.wal.ephemeral_root().join(&names[1])).unwrap();
    rig.service
        .complete_deletion(&rig.key, pending, 1400)
        .unwrap();
    for name in names {
        assert!(!rig.service.wal.ephemeral_root().join(name).exists());
    }
    assert!(rig.service.tombstones(&rig.key).unwrap()[0]
        .completed_at_ms
        .is_some());
}

#[test]
fn overhaul_retention_pass_still_preserves_wal_and_cut_files() {
    let rig = Rig::overhaul();
    let (_, names) = seed_cut_artifacts(&rig);
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let wal = fs::read(&path).unwrap();
    let files: Vec<_> = names
        .iter()
        .map(|name| fs::read(rig.service.wal.ephemeral_root().join(name)).unwrap())
        .collect();
    let _lease = rig.service.lease(&rig.key).unwrap();
    assert!(rig
        .service
        .delete_expired_typed(&rig.key, u64::MAX)
        .unwrap()
        .is_none());
    assert!(rig.service.resume_deletions(u64::MAX).unwrap().is_empty());
    assert_eq!(fs::read(path).unwrap(), wal);
    for (name, bytes) in names.iter().zip(files) {
        assert_eq!(
            fs::read(rig.service.wal.ephemeral_root().join(name)).unwrap(),
            bytes
        );
    }
    assert!(rig.service.tombstones(&rig.key).unwrap().is_empty());
}

#[test]
#[ignore = "subprocess entry point exercised by leased_completion_crash_boundaries_preserve_fresh_lineage_cuts"]
fn retention_completion_child() {
    let root =
        PathBuf::from(std::env::var_os("BROCA_RETENTION_TEST_ROOT").expect("child root required"));
    let key: BindIdentity =
        serde_json::from_str(&std::env::var("BROCA_RETENTION_TEST_KEY").unwrap()).unwrap();
    let target: usize = std::env::var("BROCA_RETENTION_TEST_CUT")
        .unwrap()
        .parse()
        .unwrap();
    let wal = Arc::new(FileWalStore::with_config(
        root.join("wal"),
        broca_wal::WalConfig::OVERHAUL,
    ));
    let store = Arc::new(
        SqliteStore::open(root.join("run-index.db"))
            .unwrap()
            .with_wal_config(broca_wal::WalConfig::OVERHAUL),
    );
    let leases = Arc::new(broca_core::FileLeaseStore::new(root.join("leases")));
    let resolver = Arc::new(|_: &str, _: &str| (broca_core::ChargeBasis::Unknown, 1));
    let service = RetentionService::new(wal, store.clone(), leases, store, resolver).unwrap();
    let _lease = service.lease(&key).unwrap();
    let stone = service.tombstones(&key).unwrap().pop().unwrap();
    let mut step = 0;
    DELETION_BOUNDARY.with(|slot| {
        *slot.borrow_mut() = Some(Box::new(move |label| {
            if step == target {
                let mut file = File::create(root.join("deletion.ready.tmp")).unwrap();
                file.write_all(label.as_bytes()).unwrap();
                file.sync_all().unwrap();
                fs::rename(root.join("deletion.ready.tmp"), root.join("deletion.ready")).unwrap();
                sync_dir(&root).unwrap();
                loop {
                    std::thread::park();
                }
            }
            step += 1;
        }))
    });
    service.complete_deletion(&key, stone, 1300).unwrap();
    panic!("deletion boundary {target} was not reached");
}

#[test]
fn leased_completion_crash_boundaries_preserve_fresh_lineage_cuts() {
    struct Child(std::process::Child);
    impl Drop for Child {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
    let trace = Arc::new(Mutex::new(Vec::<String>::new()));
    {
        let rig = Rig::overhaul();
        let (id, names) = seed_cut_artifacts(&rig);
        let _lease = rig.service.lease(&rig.key).unwrap();
        let stone = prepare_stone(&rig, id, &names);
        let recorded = trace.clone();
        DELETION_BOUNDARY.with(|slot| {
            *slot.borrow_mut() = Some(Box::new(move |label| {
                recorded.lock().unwrap().push(label.into())
            }))
        });
        rig.service
            .complete_deletion(&rig.key, stone, 1300)
            .unwrap();
        DELETION_BOUNDARY.with(|slot| *slot.borrow_mut() = None);
        assert_eq!(
            trace
                .lock()
                .unwrap()
                .iter()
                .filter(|label| label.as_str() == "artifact_removed")
                .count(),
            names.len()
        );
    }
    let labels = trace.lock().unwrap().clone();
    eprintln!(
        "leased retention completion: {} crash boundaries",
        labels.len()
    );
    assert_eq!(labels.first().map(String::as_str), Some("prepared"));
    assert_eq!(labels.last().map(String::as_str), Some("completed"));
    for label in [
        "moved",
        "expired_index",
        "unlinked",
        "artifacts_synced",
        "completion_synced",
    ] {
        assert!(labels.iter().any(|found| found == label));
    }
    for (cut, label) in labels.iter().enumerate() {
        let rig = Rig::overhaul();
        let (id, names) = seed_cut_artifacts(&rig);
        let original: Vec<_> = names
            .iter()
            .map(|name| {
                (
                    name.clone(),
                    fs::read(rig.service.wal.ephemeral_root().join(name)).unwrap(),
                )
            })
            .collect();
        {
            let _lease = rig.service.lease(&rig.key).unwrap();
            prepare_stone(&rig, id.clone(), &names);
        }
        let output = File::create(rig.root.join("deletion-child.log")).unwrap();
        let mut child = Child(
            std::process::Command::new(std::env::current_exe().unwrap())
                .args([
                    "--ignored",
                    "--exact",
                    "retention::tests::recovery_artifacts::retention_completion_child",
                    "--nocapture",
                ])
                .env("BROCA_RETENTION_TEST_ROOT", &rig.root)
                .env(
                    "BROCA_RETENTION_TEST_KEY",
                    serde_json::to_string(&rig.key).unwrap(),
                )
                .env("BROCA_RETENTION_TEST_CUT", cut.to_string())
                .stdout(output.try_clone().unwrap())
                .stderr(output)
                .spawn()
                .unwrap(),
        );
        broca_test_support::wait_until_sync("leased deletion disk boundary", || {
            if rig.root.join("deletion.ready").exists() {
                return Ok(());
            }
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "child stopped at cut {cut}: {}",
                fs::read_to_string(rig.root.join("deletion-child.log")).unwrap()
            );
            Err(format!("waiting for deletion boundary {cut}"))
        });
        assert_eq!(
            fs::read_to_string(rig.root.join("deletion.ready")).unwrap(),
            *label
        );
        child.0.kill().unwrap();
        child.0.wait().unwrap();
        let _lease = rig.service.lease(&rig.key).unwrap();
        if label == "artifact_removed" {
            // Before the cleanup directory barrier, a power cut may lose every
            // unlink. Reinstall that allowed disk image before retrying.
            for (name, bytes) in &original {
                fs::write(rig.service.wal.ephemeral_root().join(name), bytes).unwrap();
            }
        }
        let fresh = if cut > 0 {
            // Establish a new identity without asking recovery to infer one from
            // the old lineage's still-pending artifacts.
            drop(broca_wal::FileWal::open(rig.service.wal.ephemeral_wal_path(&rig.key)).unwrap());
            let mut wal = rig
                .service
                .wal
                .open_with_retention(&rig.key, Some(1))
                .unwrap();
            wal.append(1, &started(&rig.key, "fresh-lineage", Some(1), 5000), true)
                .unwrap();
            wal.append(1, &finished(5100), true).unwrap();
            let fresh_id = wal.lineage_id().unwrap().to_string();
            drop(wal);
            assert_ne!(fresh_id, id);
            let fresh_names = add_cut_artifacts(&rig, &fresh_id);
            assert!(fresh_names.iter().all(|name| !names.contains(name)));
            Some((
                fs::read(rig.service.wal.ephemeral_wal_path(&rig.key)).unwrap(),
                fresh_names
                    .iter()
                    .map(|name| {
                        (
                            name.clone(),
                            fs::read(rig.service.wal.ephemeral_root().join(name)).unwrap(),
                        )
                    })
                    .collect::<Vec<_>>(),
            ))
        } else {
            None
        };
        let saved = rig.service.tombstones(&rig.key).unwrap().pop().unwrap();
        assert_eq!(saved.recovery_artifacts, names);
        rig.service
            .complete_deletion(&rig.key, saved, 1400)
            .unwrap();
        for name in &names {
            assert!(
                !rig.service.wal.ephemeral_root().join(name).exists(),
                "old cut bytes survived boundary {cut}"
            );
        }
        if let Some((wal, files)) = fresh {
            assert_eq!(
                fs::read(rig.service.wal.ephemeral_wal_path(&rig.key)).unwrap(),
                wal,
                "fresh WAL changed at boundary {cut}"
            );
            for (name, bytes) in files {
                assert_eq!(
                    fs::read(rig.service.wal.ephemeral_root().join(name)).unwrap(),
                    bytes,
                    "fresh cut file changed at boundary {cut}"
                );
            }
        }
        assert!(rig.service.tombstones(&rig.key).unwrap()[0]
            .completed_at_ms
            .is_some());
    }
}

#[test]
fn legacy_deletion_keeps_its_tombstone_bytes_and_ignores_cut_named_files() {
    let rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    let _lease = rig.service.lease(&rig.key).unwrap();
    let path = rig.service.wal.ephemeral_wal_path(&rig.key);
    let unrelated = path.parent().unwrap().join(format!(
        "{}.cut-unrelated",
        path.file_name().unwrap().to_str().unwrap()
    ));
    fs::write(&unrelated, PRIVATE_CUT).unwrap();
    assert!(rig
        .service
        .capture_recovery_artifacts(&path)
        .unwrap()
        .is_empty());
    let stone = Tombstone {
        lineage_id: id,
        expired_at_ms: 1200,
        seq: 1,
        completed_at_ms: None,
        next_tag: None,
        next_notice_seq: None,
        recovery_artifacts: Vec::new(),
    };
    assert!(serde_json::to_value(&stone)
        .unwrap()
        .get("recovery_artifacts")
        .is_none());
    rig.service.write_tombstone(&rig.key, &stone).unwrap();
    rig.service.complete(&rig.key, stone, 1300).unwrap();
    assert_eq!(fs::read(unrelated).unwrap(), PRIVATE_CUT.as_bytes());
}

#[test]
fn leased_completion_removes_both_names_of_a_power_lost_wal_rename() {
    let rig = Rig::overhaul();
    let (id, names) = seed_cut_artifacts(&rig);
    let _lease = rig.service.lease(&rig.key).unwrap();
    let stone = prepare_stone(&rig, id.clone(), &names);
    let live = rig.service.wal.ephemeral_wal_path(&rig.key);
    let moved = rig.service.deleting(&rig.key, &id);
    fs::hard_link(&live, &moved).unwrap();
    sync_dir(&rig.service.wal.ephemeral_root()).unwrap();
    rig.service
        .complete_deletion(&rig.key, stone, 1300)
        .unwrap();
    assert!(
        !live.exists() && !moved.exists(),
        "an allowed two-name rename image must not retain deleted transcript bytes"
    );
    for name in names {
        assert!(!rig.service.wal.ephemeral_root().join(name).exists());
    }
}
