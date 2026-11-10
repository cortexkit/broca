use super::*;

const PRIVATE_CUT: &str = "private-retention-cut-content";

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
