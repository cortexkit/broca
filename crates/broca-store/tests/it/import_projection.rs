use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use broca_core::{
    read_session_page, run_import, run_import_with_store, FileWalStore, ImportOrigin,
    ImportRequest, MaterializedStore, SessionReadMessage, SessionReadPage, SessionStore,
    StorePageSnapshot, WalHandle, WalStore,
};
use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::{BindIdentity, RunFinishReason, RunId};
use broca_store::SqliteStore;
use broca_wal::WalRecord;

static NEXT_DIR: AtomicU64 = AtomicU64::new(1);

fn temp_dir(label: &str) -> PathBuf {
    let path = broca_test_support::scratch_root().join(format!(
        "broca-store-import-{label}-{}-{}",
        std::process::id(),
        NEXT_DIR.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&path).expect("create temp directory");
    path
}

fn text(role: Role, value: impl Into<String>) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: value.into() }])
}

fn request(
    session: &BindIdentity,
    run_id: &str,
    source_ref: &str,
    messages: Vec<Message>,
) -> ImportRequest {
    ImportRequest {
        project_id: None,
        principal: None,
        run_id: RunId(run_id.into()),
        session: session.clone(),
        messages,
        origin: ImportOrigin {
            source_ref: source_ref.into(),
        },
    }
}

fn store_backed_page(
    store: &SqliteStore,
    wal_store: &FileWalStore,
    session: &BindIdentity,
    from_ordinal: u64,
    limit: u64,
) -> SessionReadPage {
    let snapshot = store
        .read_page_with_watermark(
            session,
            Some(from_ordinal),
            limit,
            broca_core::context::ReadView::Raw,
        )
        .expect("read materialized page");
    let tail = wal_store
        .read_records_range(
            session,
            snapshot.watermark_seq,
            None,
            snapshot.anchor.as_ref(),
        )
        .expect("read bounded WAL tail");
    let tail = match tail.into_records() {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Partial(records)
        | broca_wal::WalRangeRecords::Unknown(records) => records,
        broca_wal::WalRangeRecords::Absent => Vec::new(),
    };
    read_session_page(&snapshot, &tail, Some(from_ordinal), Some(limit))
        .expect("merge materialized page and WAL tail")
}

fn full_replay_page(
    wal_store: &FileWalStore,
    session: &BindIdentity,
    from_ordinal: u64,
    limit: u64,
) -> SessionReadPage {
    read_session_page(
        &StorePageSnapshot::empty(),
        &wal_store.read_records(session).expect("read complete WAL"),
        Some(from_ordinal),
        Some(limit),
    )
    .expect("read page by full WAL replay")
}

#[tokio::test]
async fn import_materializes_paged_history_and_retry_is_idempotent() {
    let root = temp_dir("paged");
    let wal_store = FileWalStore::new(root.join("wal"));
    let store = Arc::new(SqliteStore::open(root.join("store.db")).expect("open SQLite store"));
    let session = BindIdentity::new("/project", "runner", "materialized-import");
    let writer =
        WalHandle::spawn(wal_store.open(&session).expect("open WAL"), 1).expect("spawn writer");
    let messages: Vec<_> = (0..11)
        .map(|ordinal| {
            let role = if ordinal % 2 == 0 {
                Role::User
            } else {
                Role::Assistant
            };
            text(role, format!("message-{ordinal}"))
        })
        .collect();
    let import_request = request(
        &session,
        "import-run",
        "fixture:materialized",
        messages.clone(),
    );

    let first = run_import_with_store(
        import_request.clone(),
        &writer,
        &wal_store,
        Some(store.clone() as Arc<dyn SessionStore>),
    )
    .await
    .expect("import history");
    assert!(!first.already_imported);

    let materialized = store
        .read_page_with_watermark(&session, Some(0), 100, broca_core::context::ReadView::Raw)
        .expect("read complete materialized prefix");
    assert_eq!(materialized.rows.len(), messages.len());
    assert_eq!(materialized.prefix_len, messages.len() as u64);
    assert_eq!(materialized.watermark_seq, 1);
    assert_eq!(materialized.last_episode_usage, None);
    assert_eq!(
        materialized
            .anchor
            .map(|anchor| (anchor.file_id, anchor.seq)),
        wal_store.file_identity(&session).unwrap().map(|id| (id, 1))
    );
    assert_eq!(
        materialized
            .rows
            .iter()
            .map(|row| row.ordinal)
            .collect::<Vec<_>>(),
        (0..messages.len() as u64).collect::<Vec<_>>()
    );

    let tail = wal_store
        .read_records_range(
            &session,
            materialized.watermark_seq,
            None,
            materialized.anchor.as_ref(),
        )
        .expect("read post-projection WAL tail");
    let tail = match tail.into_records() {
        broca_wal::WalRangeRecords::Complete(records)
        | broca_wal::WalRangeRecords::Partial(records)
        | broca_wal::WalRangeRecords::Unknown(records) => records,
        broca_wal::WalRangeRecords::Absent => Vec::new(),
    };
    assert_eq!(
        tail.len(),
        1,
        "the imported RunStarted must not be replayed"
    );
    assert!(matches!(
        tail[0].record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Completed,
            ..
        }
    ));

    for (from, limit) in [(3, 4), (9, 5)] {
        assert_eq!(
            store_backed_page(&store, &wal_store, &session, from, limit),
            full_replay_page(&wal_store, &session, from, limit)
        );
    }

    let mut concatenated = Vec::<SessionReadMessage>::new();
    let mut from = 0;
    loop {
        let page = store_backed_page(&store, &wal_store, &session, from, 3);
        concatenated.extend(page.messages);
        let Some(next) = page.next_from_ordinal else {
            break;
        };
        from = next;
    }
    assert_eq!(
        concatenated,
        full_replay_page(&wal_store, &session, 0, 100).messages
    );

    let retry = run_import_with_store(
        import_request,
        &writer,
        &wal_store,
        Some(store.clone() as Arc<dyn SessionStore>),
    )
    .await
    .expect("retry completed import");
    assert!(retry.already_imported);
    assert_eq!(
        store
            .read_page_with_watermark(&session, Some(0), 100, broca_core::context::ReadView::Raw)
            .expect("read projection after retry"),
        materialized,
        "the watermark, anchor, metadata, and rows must remain unchanged"
    );
    assert_eq!(
        wal_store.read_records(&session).expect("read WAL").len(),
        2,
        "retry must not append or duplicate the import"
    );

    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

#[tokio::test]
async fn repaired_dangling_import_projection_matches_final_wal() {
    let root = temp_dir("repair");
    let session = BindIdentity::new("/project", "runner", "repaired-import");

    let source_wal = FileWalStore::new(root.join("source-wal"));
    let source_writer = WalHandle::spawn(source_wal.open(&session).expect("open source WAL"), 1)
        .expect("spawn writer");
    run_import(
        request(
            &session,
            "dangling-run",
            "fixture:dangling",
            vec![text(Role::User, "obsolete prompt")],
        ),
        &source_writer,
        &source_wal,
    )
    .await
    .expect("create source import");
    let _ = source_writer.shutdown().await;
    let dangling_start = source_wal
        .read_records(&session)
        .expect("read source import")
        .remove(0)
        .record;

    let wal_store = FileWalStore::new(root.join("target-wal"));
    let mut raw_wal = wal_store.open(&session).expect("open target WAL");
    raw_wal
        .append(1, &dangling_start, true)
        .expect("write dangling import start");
    drop(raw_wal);

    let replacement = vec![
        text(Role::User, "replacement-0"),
        text(Role::Assistant, "replacement-1"),
        text(Role::User, "replacement-2"),
    ];
    let store = Arc::new(SqliteStore::open(root.join("store.db")).expect("open SQLite store"));
    let writer = WalHandle::spawn(wal_store.open(&session).expect("reopen target WAL"), 1)
        .expect("spawn writer");
    run_import_with_store(
        request(
            &session,
            "replacement-run",
            "fixture:replacement",
            replacement.clone(),
        ),
        &writer,
        &wal_store,
        Some(store.clone() as Arc<dyn SessionStore>),
    )
    .await
    .expect("repair and project import");

    let frames = wal_store.read_records(&session).expect("read repaired WAL");
    assert_eq!(frames.len(), 4);
    assert!(matches!(
        frames[1].record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        }
    ));
    let snapshot = store
        .read_page_with_watermark(&session, Some(0), 100, broca_core::context::ReadView::Raw)
        .expect("read repaired materialized prefix");
    assert_eq!(snapshot.watermark_seq, 3);
    assert_eq!(snapshot.prefix_len, replacement.len() as u64);
    assert_eq!(snapshot.rows.len(), replacement.len());
    assert_eq!(
        store_backed_page(&store, &wal_store, &session, 0, 100),
        full_replay_page(&wal_store, &session, 0, 100)
    );

    let _ = writer.shutdown().await;
    let _ = std::fs::remove_dir_all(root);
}

/// Import reads the target WAL (to find a dangling seed to seal) and then appends; it never
/// writes back a record it read. So a dangling seed held in a gated frame keeps its version and
/// `requires` list byte for byte after the repair, rather than being re-encoded as an ordinary
/// version-1 frame that decodes to the same record.
#[tokio::test]
async fn import_repair_keeps_a_gated_dangling_seed_byte_for_byte() {
    let root = temp_dir("gated-seed");
    let session = BindIdentity::new("/project", "runner", "gated-seed");

    let source_wal = FileWalStore::new(root.join("source-wal"));
    let source_writer = WalHandle::spawn(source_wal.open(&session).expect("open source WAL"), 1)
        .expect("spawn writer");
    run_import(
        request(
            &session,
            "dangling-run",
            "fixture:dangling",
            vec![text(Role::User, "obsolete prompt")],
        ),
        &source_writer,
        &source_wal,
    )
    .await
    .expect("create source import");
    let _ = source_writer.shutdown().await;
    let dangling_start = source_wal
        .read_records(&session)
        .expect("read source import")
        .remove(0)
        .record;

    let wal_store = FileWalStore::new(root.join("target-wal"));
    drop(wal_store.open(&session).expect("create target WAL"));
    let path = wal_store.wal_path(&session);
    let gated = broca_wal::framing::encode_with_requires(
        1,
        1,
        &[broca_wal::framing::TEST_KNOWN_FEATURE],
        &dangling_start,
    )
    .unwrap();
    let mut before = std::fs::read(&path).unwrap();
    before.extend(&gated);
    std::fs::write(&path, &before).unwrap();

    let writer = WalHandle::spawn(wal_store.open(&session).expect("reopen target WAL"), 1)
        .expect("spawn writer");
    run_import(
        request(
            &session,
            "replacement-run",
            "fixture:replacement",
            vec![text(Role::User, "replacement")],
        ),
        &writer,
        &wal_store,
    )
    .await
    .expect("repair the dangling seed and import");
    let _ = writer.shutdown().await;

    let after = std::fs::read(&path).unwrap();
    assert!(
        after.starts_with(&before),
        "import rewrote the gated frame it read"
    );
    let frames = wal_store.read_records(&session).expect("read repaired WAL");
    assert_eq!(frames.len(), 4, "seed, its seal, and the new episode");
    assert_eq!(frames[0].record, dangling_start);
    assert!(matches!(
        frames[1].record,
        WalRecord::RunFinished {
            reason: RunFinishReason::Interrupted,
            ..
        }
    ));
    let _ = std::fs::remove_dir_all(root);
}
