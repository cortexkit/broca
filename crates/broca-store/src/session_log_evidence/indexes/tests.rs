use super::*;

#[test]
fn schema_and_explicit_preparation_share_idempotent_indexes() {
    let store = SqliteStore::open_in_memory().unwrap();
    assert!(store
        .preview_session_log_indexes()
        .unwrap()
        .iter()
        .filter(|index| index.applicable)
        .all(|index| index.exists));
    store
        .conn
        .lock()
        .unwrap()
        .execute_batch("DROP INDEX session_log_evidence_runs")
        .unwrap();
    assert!(store
        .preview_session_log_indexes()
        .unwrap()
        .iter()
        .any(|index| index.applicable && !index.exists));
    store.prepare_session_log_indexes().unwrap();
    let version: i64 = store
        .conn
        .lock()
        .unwrap()
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    store.prepare_session_log_indexes().unwrap();
    let after: i64 = store
        .conn
        .lock()
        .unwrap()
        .query_row("PRAGMA schema_version", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        version, after,
        "second preparation must not rebuild indexes"
    );
    assert!(store
        .preview_session_log_indexes()
        .unwrap()
        .iter()
        .filter(|index| index.applicable)
        .all(|index| index.exists));
}

#[test]
fn optional_message_lineage_index_is_installed_from_the_same_definition() {
    let store = SqliteStore::open_in_memory().unwrap();
    assert!(
        !store
            .preview_session_log_indexes()
            .unwrap()
            .iter()
            .find(|index| index.name == "session_log_evidence_message")
            .unwrap()
            .applicable
    );
    store
        .conn
        .lock()
        .unwrap()
        .execute_batch("ALTER TABLE message ADD COLUMN lineage_id TEXT")
        .unwrap();
    store.prepare_session_log_indexes().unwrap();
    let plan = store.preview_session_log_indexes().unwrap();
    let message = plan
        .iter()
        .find(|index| index.name == "session_log_evidence_message")
        .unwrap();
    assert!(message.applicable && message.exists);
}

#[test]
fn identity_indexes_preserve_malformed_json_rows_without_indexing_them() {
    let store = SqliteStore::open_in_memory().unwrap();
    {
        let conn = store.conn.lock().unwrap();
        conn.execute_batch("DROP INDEX session_log_evidence_runs; DROP INDEX session_log_evidence_deletion_identity;").unwrap();
        conn.execute("INSERT INTO run_index(run_id,session,state) VALUES('malformed','not-json','completed')", []).unwrap();
        conn.execute(
            "INSERT INTO retention_deletion(lineage_id,session) VALUES('malformed','not-json')",
            [],
        )
        .unwrap();
    }
    store.prepare_session_log_indexes().unwrap();
    let conn = store.conn.lock().unwrap();
    assert_eq!(
        conn.query_row(
            "SELECT count(*) FROM run_index WHERE run_id='malformed'",
            [],
            |row| row.get::<_, i64>(0)
        )
        .unwrap(),
        1
    );
    let session = broca_protocol::BindIdentity::new("/project", "broca", "unrelated");
    assert!(
        !crate::session_log_evidence::evidence(&conn, &session)
            .unwrap()
            .content
    );
}
