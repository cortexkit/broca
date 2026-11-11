use super::*;

#[test]
fn indexed_deletion_recovery_does_not_walk_the_expiry_graveyard() {
    let rig = Rig::new();
    let ephemeral = rig.root.join("wal-ephemeral");
    std::fs::create_dir_all(&ephemeral).unwrap();
    for n in 0..512 {
        std::fs::create_dir(ephemeral.join(format!("{:016x}.expired", n + 1_000_000))).unwrap();
    }
    rig.service
        .directory_walks
        .store(0, std::sync::atomic::Ordering::Relaxed);
    assert!(rig
        .service
        .resume_deletions(rig.service.clock.now_ms())
        .unwrap()
        .is_empty());
    assert_eq!(
        rig.service
            .directory_walks
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
        "the durable pending-deletion inventory is authoritative, even at first boot"
    );
}

#[test]
fn old_tombstones_are_indexed_once_without_exposing_expired_content() {
    let rig = Rig::new();
    let id = rig.seed(1, 100, Some(200));
    rig.service.remember_session(&rig.key).unwrap();
    rig.service
        .write_tombstone(
            &rig.key,
            &Tombstone {
                lineage_id: id.clone(),
                expired_at_ms: 1200,
                seq: 1,
                completed_at_ms: None,
                next_tag: None,
                next_notice_seq: None,
                recovery_artifacts: Vec::new(),
            },
        )
        .unwrap();
    rig.service
        .index
        .deletion_pending(&id, &rig.key, false)
        .unwrap();
    assert!(rig.service.resume_deletions(1300).unwrap().is_empty());
    assert_eq!(rig.service.expired(&rig.key, None).unwrap(), Some(1200));
    rig.service.rebuild_deletion_inventory().unwrap();
    assert_eq!(
        rig.service.resume_deletions(1300).unwrap(),
        vec![rig.key.clone()]
    );
    rig.service
        .directory_walks
        .store(0, std::sync::atomic::Ordering::Relaxed);
    rig.service.rebuild_deletion_inventory().unwrap();
    assert_eq!(
        rig.service
            .directory_walks
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

/// A filename-volume probe, not a model for the production database's 55 GB of history.
/// Empty WALs hold no session yet; expired directories without sidecars model old completed
/// deletions. Disjoint address ranges isolate directory reads and checks for missing sidecars.
#[test]
#[ignore = "production-volume startup measurement; creates 122038 expiry directories"]
fn startup_retention_phase_costs_at_production_filename_volume() {
    let rig = Rig::new();
    let ephemeral = rig.root.join("wal-ephemeral");
    std::fs::create_dir_all(&ephemeral).unwrap();
    std::fs::create_dir_all(rig.root.join("wal")).unwrap();
    for n in 0..122_038 {
        std::fs::create_dir(ephemeral.join(format!("{:016x}.expired", n + 1_000_000))).unwrap();
    }
    for n in 0..9959 {
        std::fs::write(ephemeral.join(format!("{:016x}.wal", n + 2_000_000)), b"").unwrap();
    }
    for n in 0..15_373 {
        std::fs::write(
            ephemeral.join(format!("{:016x}.session", n + 3_000_000)),
            b"{}",
        )
        .unwrap();
    }
    for n in 0..13_376 {
        std::fs::write(
            rig.root
                .join("wal")
                .join(format!("{:016x}.wal", n + 4_000_000)),
            b"",
        )
        .unwrap();
    }
    let start = std::time::Instant::now();
    let recovered = rig
        .service
        .resume_deletions(rig.service.clock.now_ms())
        .unwrap();
    let deletion_ms = start.elapsed().as_millis();
    let foreground_walks = rig
        .service
        .directory_walks
        .load(std::sync::atomic::Ordering::Relaxed);
    let start = std::time::Instant::now();
    rig.service.rebuild_deletion_inventory().unwrap();
    let compatibility_ms = start.elapsed().as_millis();
    let start = std::time::Instant::now();
    let enrolled = rig.service.recover_enrollment().unwrap();
    let enrollment_ms = start.elapsed().as_millis();
    eprintln!("synthetic retention startup: expired=122038 ephemeral_wals=9959 sidecars=15373 portable_wals=13376 deletion_recovery_ms={deletion_ms} compatibility_background_ms={compatibility_ms} enrollment_background_ms={enrollment_ms} foreground_walks={foreground_walks} recovered={} enrolled={enrolled}", recovered.len());
    assert!(recovered.is_empty());
    assert_eq!(foreground_walks, 0);
}
