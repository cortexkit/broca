use super::*;

impl LegacyFork {
    fn seed_stale_projection(&self) {
        let wal = broca_core::FileWalStore::new(self.root.join("wal"));
        let range = wal
            .read_records_range(&self.live, 0, Some(11), None)
            .unwrap();
        self.store.project_range(&self.stale, &range).unwrap();
        let conn = self.store.conn.lock().unwrap();
        let meta: (Option<String>, i64, String) = conn
            .query_row(
                "SELECT lineage_id,watermark_seq,last_run_id FROM meta WHERE session=?1",
                [SqliteStore::key(&self.stale)],
                |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
            )
            .unwrap();
        assert_eq!(meta, (None, 11, "legacy-2".into()));
        assert_eq!(
            conn.query_row(
                "SELECT count(*) FROM message WHERE session=?1",
                [SqliteStore::key(&self.stale)],
                |row| row.get::<_, i64>(0),
            )
            .unwrap(),
            6
        );
        for table in PROJECTION_TABLES {
            assert_eq!(
                conn.query_row(
                    &format!("SELECT count(*) FROM {table} WHERE session=?1"),
                    [SqliteStore::key(&self.live)],
                    |row| row.get::<_, i64>(0),
                )
                .unwrap(),
                0,
                "live key must have no {table} projection"
            );
        }
    }

    fn message_bytes(&self, identity: &BindIdentity) -> Vec<(i64, Vec<u8>)> {
        self.store
            .conn
            .lock()
            .unwrap()
            .prepare("SELECT ord,CAST(json AS BLOB) FROM message WHERE session=?1 ORDER BY ord")
            .unwrap()
            .query_map([SqliteStore::key(identity)], |row| {
                Ok((row.get(0)?, row.get(1)?))
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap()
    }
}

#[test]
fn null_lineage_stale_only_projection_matches_the_verified_legacy_log() {
    let f = LegacyFork::new(true);
    f.seed_stale_projection();
    let stale_before = f.message_bytes(&f.stale);
    let wal = broca_core::FileWalStore::new(f.root.join("wal"));
    let real_before = std::fs::read(wal.wal_path(&f.live)).unwrap();
    let stub_before = std::fs::read(wal.wal_path(&f.stale)).unwrap();
    let plan = f.preview().unwrap();
    assert!(!plan.live_meta_present);
    assert_eq!(plan.stale_meta.as_ref().unwrap().lineage_id, None);
    assert_eq!(
        plan.stale_projection_evidence,
        Some(ProjectionEvidence {
            kind: ProjectionEvidenceKind::NullLineagePrefixMatch,
            compared_message_rows: 6,
        })
    );
    assert_eq!(f.message_bytes(&f.stale), stale_before);
    assert!(f.message_bytes(&f.live).is_empty());
    let aside = plan.move_stub_aside.as_ref().unwrap().to.clone();
    f.store.apply_session_root_repair(&plan).unwrap();
    assert!(f.message_bytes(&f.stale).is_empty());
    assert!(f.message_bytes(&f.live).is_empty());
    assert!(f.preview().unwrap().stale_identity.is_none());
    assert_eq!(std::fs::read(aside).unwrap(), stub_before);
    assert_eq!(std::fs::read(wal.wal_path(&f.live)).unwrap(), real_before);
    let range = wal.read_records_range(&f.live, 0, None, None).unwrap();
    f.store.project_range(&f.live, &range).unwrap();
    assert_eq!(f.message_bytes(&f.live), stale_before);
}

#[test]
fn null_lineage_stale_only_projection_refuses_a_byte_different_log_message() {
    let f = LegacyFork::new(true);
    f.seed_stale_projection();
    f.store
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE message SET json=json || ' ' WHERE session=?1 AND ord=5",
            [SqliteStore::key(&f.stale)],
        )
        .unwrap();
    let before = f.message_bytes(&f.stale);
    let error = f.preview().unwrap_err().to_string();
    assert!(
        error.contains("null_lineage_log_prefix_mismatch: stale message ord 5"),
        "{error}"
    );
    assert_eq!(f.message_bytes(&f.stale), before);
    assert!(f.message_bytes(&f.live).is_empty());
    assert!(!f.root.join("wal/repair-aside").exists());
}

#[test]
fn null_lineage_matching_stale_and_live_rows_must_also_match_the_log() {
    let f = LegacyFork::new(true);
    f.seed_stale_projection();
    let wal = broca_core::FileWalStore::new(f.root.join("wal"));
    let range = wal.read_records_range(&f.live, 0, None, None).unwrap();
    f.store.project_range(&f.live, &range).unwrap();
    f.store
        .conn
        .lock()
        .unwrap()
        .execute("UPDATE message SET json=json || ' ' WHERE ord=5", [])
        .unwrap();
    let stale_before = f.message_bytes(&f.stale);
    let live_before = f.message_bytes(&f.live);
    assert_eq!(stale_before, live_before);
    let error = f.preview().unwrap_err().to_string();
    assert!(
        error.contains("null_lineage_log_prefix_mismatch: stale message ord 5"),
        "{error}"
    );
    assert_eq!(f.message_bytes(&f.stale), stale_before);
    assert_eq!(f.message_bytes(&f.live), live_before);
    assert!(!f.root.join("wal/repair-aside").exists());
}

#[test]
fn null_lineage_stale_only_projection_requires_last_run_in_the_log() {
    for last_run in [Some("not-in-the-log"), None] {
        let f = LegacyFork::new(true);
        f.seed_stale_projection();
        f.store
            .conn
            .lock()
            .unwrap()
            .execute(
                "UPDATE meta SET last_run_id=?1 WHERE session=?2",
                params![last_run, SqliteStore::key(&f.stale)],
            )
            .unwrap();
        let before = f.message_bytes(&f.stale);
        let error = f.preview().unwrap_err().to_string();
        assert!(error.contains("null_lineage_last_run_missing"), "{error}");
        assert_eq!(f.message_bytes(&f.stale), before);
        assert!(f.message_bytes(&f.live).is_empty());
        assert!(!f.root.join("wal/repair-aside").exists());
    }
}
