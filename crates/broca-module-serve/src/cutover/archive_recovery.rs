use super::*;
use broca_core::{ExportIndex, RunIndex};

async fn orphan(
    tag: &str,
    usage: bool,
    exported: bool,
) -> (Rig, Arc<broca_store::SqliteStore>, PathBuf, Vec<u8>) {
    orphan_with_layout(tag, usage, exported, false).await
}

async fn orphan_with_layout(
    tag: &str,
    usage: bool,
    exported: bool,
    legacy: bool,
) -> (Rig, Arc<broca_store::SqliteStore>, PathBuf, Vec<u8>) {
    let mut rig = Rig::start(tag).await;
    let records = if usage {
        let mut records = session_records(&rig.wal());
        let second = records
            .iter()
            .position(|record| {
                matches!(record,
            WalRecord::RunStarted { run_id, .. } if run_id.0 == "r2")
            })
            .unwrap();
        records.truncate(second - 1);
        records
    } else {
        vec![
            run_started("r1", 1_000),
            record(json!({"type": "step_started", "step_id": 1})),
        ]
    };
    rig.write(&records);
    let index = Arc::new(broca_store::SqliteStore::open(rig.dir.join("run-index.db")).unwrap());
    let mut row = settled_row(&key(), "r1");
    row.state = broca_core::RunIndexState::Active;
    row.usage = None;
    index.index_run_active(&row).unwrap();
    let resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync> =
        Arc::new(|_: &str, _: &str| (broca_core::billing::ChargeBasis::MeteredApi, 4));
    if exported {
        let mut framed = broca_wal::replay_file(rig.wal(), u64::MAX).unwrap();
        framed.push(FramedRecord {
            seq: framed.last().unwrap().seq + 1,
            fence: 1,
            record: record(
                json!({"type": "run_finished", "reason": "completed", "ts_ms": 1_003,
                "usage": {"input_tokens": 3, "output_tokens": 1}, "retries_used": 0}),
            ),
        });
        index
            .emit_run(
                &framed,
                &broca_protocol::RunId("r1".into()),
                resolver.as_ref(),
            )
            .unwrap();
        assert_eq!(facts(&rig), 1);
    }
    let billing = broca_module::store_billing::StoreBilling {
        index: index.clone(),
        resolver,
    };
    rig.runtime = Arc::new(
        broca_module::store_open::StoreRuntime::open(
            &rig.dir,
            deployment(),
            Arc::new(NoRuns),
            Duration::from_millis(100),
        )
        .await
        .unwrap()
        .with_billing(billing),
    );
    let mut bytes = std::fs::read(rig.wal()).unwrap();
    if legacy {
        let mut frames = Vec::new();
        let mut offset = 0;
        while offset < bytes.len() {
            let consumed = match broca_wal::framing::decode_next(&bytes[offset..]) {
                broca_wal::framing::DecodeStep::Record { consumed, .. } => {
                    frames.extend_from_slice(&bytes[offset..offset + consumed]);
                    consumed
                }
                broca_wal::framing::DecodeStep::Lineage { consumed, .. } => consumed,
                _ => panic!("the fixture contains only complete verified frames"),
            };
            offset += consumed;
        }
        bytes = frames;
    }
    let archive = rig.dir.join("wal-archive/fold-1000.ark");
    std::fs::create_dir_all(archive.parent().unwrap()).unwrap();
    let mut writer = broca_wal::archive::ArchiveWriter::create(&archive)
        .unwrap()
        .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
    writer
        .append(&broca_core::session_addr(&key()), &bytes)
        .unwrap();
    writer.seal().unwrap();
    let original = std::fs::read(&archive).unwrap();
    std::fs::remove_file(rig.wal()).unwrap();
    (rig, index, archive, original)
}

#[tokio::test]
async fn an_archived_orphan_without_lineage_keeps_its_legacy_identity_after_recovery() {
    let (rig, _index, _archive, original) =
        orphan_with_layout("orphan-legacy-header", false, false, true).await;
    let engine = engine(&rig, true, false);
    engine
        .run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    assert_eq!(std::fs::read(&row.source.backup).unwrap(), original);
    let view = rig.runtime.current_view(&key()).await.unwrap().unwrap();
    assert!(view.operations.billing_owed.is_empty());
    assert_eq!(
        view.operations.run.as_ref().unwrap().finished,
        Some(broca_protocol::caller::RunFinishReason::Interrupted)
    );
}

fn facts(rig: &Rig) -> u64 {
    rusqlite::Connection::open(rig.dir.join("run-index.db"))
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM export_facts WHERE run_id='r1'",
            [],
            |row| row.get(0),
        )
        .unwrap()
}

#[tokio::test]
async fn an_archive_orphan_is_recovered_and_billed_only_on_the_staged_copy() {
    let (rig, _index, _archive, original) = orphan("cutover-archive-orphan", false, false).await;
    let engine = engine(&rig, true, false);
    engine
        .run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    assert_eq!(std::fs::read(&row.source.backup).unwrap(), original);
    let view = rig.runtime.current_view(&key()).await.unwrap().unwrap();
    assert!(view.operations.billing_owed.is_empty());
    assert_eq!(
        view.operations.run.as_ref().unwrap().finished,
        Some(broca_protocol::caller::RunFinishReason::Interrupted)
    );
    assert_eq!(
        facts(&rig),
        1,
        "the normal exporter retains this run's zero-usage segment"
    );
    let usage: String = rusqlite::Connection::open(rig.dir.join("run-index.db"))
        .unwrap()
        .query_row(
            "SELECT json_extract(segment_json,'$.usage') FROM export_facts WHERE run_id='r1'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        serde_json::from_str::<broca_protocol::Usage>(&usage).unwrap(),
        broca_protocol::Usage::default()
    );
    let state: String = rusqlite::Connection::open(rig.dir.join("run-index.db"))
        .unwrap()
        .query_row("SELECT state FROM run_index WHERE run_id='r1'", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_eq!(state, "active", "the legacy projection is not rewritten");
}

#[tokio::test]
async fn recovered_usage_is_exported_without_duplicating_an_existing_segment() {
    for exported in [false, true] {
        let (rig, _index, _archive, original) = orphan(
            if exported {
                "orphan-dedup"
            } else {
                "orphan-usage"
            },
            true,
            exported,
        )
        .await;
        let engine = engine(&rig, true, false);
        engine
            .run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
            .await
            .unwrap();
        let row = engine.ledger.rows().unwrap().remove(0);
        assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
        assert_eq!(
            facts(&rig),
            1,
            "one segment, whether or not legacy exported it first"
        );
        let usage: String = rusqlite::Connection::open(rig.dir.join("run-index.db"))
            .unwrap()
            .query_row(
                "SELECT json_extract(segment_json,'$.usage') FROM export_facts WHERE run_id='r1'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        let usage: broca_protocol::Usage = serde_json::from_str(&usage).unwrap();
        assert_eq!(usage.input_tokens, Some(3));
        assert_eq!(usage.output_tokens, Some(1));
        assert_eq!(std::fs::read(&row.source.backup).unwrap(), original);
        assert!(rig
            .runtime
            .current_view(&key())
            .await
            .unwrap()
            .unwrap()
            .operations
            .billing_owed
            .is_empty());
    }
}

#[tokio::test]
async fn failed_recovered_billing_stays_owed_and_is_visible_without_publication() {
    let (rig, _index, archive, original) = orphan("orphan-billing-failed", true, false).await;
    rusqlite::Connection::open(rig.dir.join("run-index.db")).unwrap().execute_batch(
        "CREATE TRIGGER fail_export BEFORE INSERT ON export_facts BEGIN SELECT RAISE(ABORT,'synthetic export failure'); END;"
    ).unwrap();
    let engine = engine(&rig, true, false);
    engine
        .run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
        .await
        .unwrap();
    let row = engine.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("failed"), "{row:?}");
    assert_eq!(row.refusal.as_deref(), Some("billing_failed"), "{row:?}");
    assert_eq!(facts(&rig), 0);
    assert_eq!(std::fs::read(archive).unwrap(), original);
    assert!(!broca_wal::session_storage_head(
        &rig.dir,
        StorageClass::Ordinary,
        &broca_core::session_addr(&key())
    )
    .exists());
    let staging = rig
        .dir
        .join("conversion-staging")
        .join(broca_core::session_addr(&key()));
    let view = broca_session_store::session::Session::view(
        broca_session_store::backend::LocalBackend::new(staging),
        broca_session_store::backend::SessionAddr(broca_core::session_addr(&key())),
        &broca_session_convert::stage::triple(&key()),
        &deployment(),
    )
    .await
    .unwrap();
    assert_eq!(view.operations.billing_owed, vec!["r1".to_owned()]);
}

#[tokio::test]
async fn an_archive_recovery_reuses_its_exact_tail_across_failed_staging_and_restart() {
    let (rig, _index, archive, original) = orphan("orphan-retry", false, false).await;
    let mut first = engine(&rig, true, false);
    first.crash = Some("staging");
    assert_eq!(
        first
            .run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
            .await
            .unwrap_err(),
        "staging"
    );
    let row = first.ledger.rows().unwrap().remove(0);
    let ready = rig
        .dir
        .join("conversion-recovery")
        .join(broca_core::session_addr(&key()))
        .join(&row.source_sha256)
        .join("recovered.wal");
    let tail = std::fs::read(&ready).unwrap();
    assert_eq!(std::fs::read(&archive).unwrap(), original);
    drop(first);
    let mut second = engine(&rig, true, false);
    second.crash = Some("verified");
    assert_eq!(
        second
            .run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
            .await
            .unwrap_err(),
        "verified"
    );
    assert_eq!(second.ledger.rows().unwrap()[0].step, "verified");
    assert_eq!(std::fs::read(&ready).unwrap(), tail);
    assert_eq!(std::fs::read(&archive).unwrap(), original);
    drop(second);
    let last = engine(&rig, true, false);
    last.run_archives(&deployment(), Some(&rig.runtime), None, Duration::ZERO)
        .await
        .unwrap();
    let row = last.ledger.rows().unwrap().remove(0);
    assert_eq!(row.outcome.as_deref(), Some("converted"), "{row:?}");
    assert_eq!(std::fs::read(&row.source.backup).unwrap(), original);
    assert!(!ready.exists());
}
