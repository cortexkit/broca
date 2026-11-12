use super::*;
use crate::conversion_ledger::{Ledger, Row};
use crate::session_conversion::parallel::Probe;
use std::collections::BTreeMap;

const PANIC_TEXT: &str = "runner tags must cover a message's blocks one for one";

#[derive(Clone, Copy)]
enum Mode {
    Startup,
    Background,
    Offline,
}

fn write_sources(rig: &Rig, archived: bool) -> Vec<BindIdentity> {
    let sessions: Vec<_> = (0..3)
        .map(|index| BindIdentity::new("/project", "runner", format!("panic-{index}")))
        .collect();
    for (index, identity) in sessions.iter().enumerate() {
        let path = rig.wal_store.wal_path(identity);
        let mut records = session_records(&path);
        for record in &mut records {
            if let WalRecord::RunStarted { session, tags, .. } = record {
                *session = identity.clone();
                if index == 1 {
                    // The input has one block. Preserve the real broken invariant rather
                    // than replacing the converter with a test-only panic implementation.
                    *tags = Some(Arc::new(vec![broca_protocol::tags::MessageTags::new(
                        vec![1, 2],
                    )]));
                }
            }
        }
        let mut wal = broca_wal::FileWal::open(&path)
            .unwrap()
            .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
        for record in records {
            wal.append(0, &record, false).unwrap();
        }
        wal.sync().unwrap();
        drop(wal);
        if archived {
            let archive = rig
                .dir
                .join(format!("wal-archive/fold-{}.ark", 3000 + index));
            std::fs::create_dir_all(archive.parent().unwrap()).unwrap();
            let mut writer = broca_wal::archive::ArchiveWriter::create(&archive)
                .unwrap()
                .with_feature_floor(rig.dir.join(broca_wal::FEATURE_FLOOR_FILENAME));
            writer
                .append(
                    &broca_core::session_addr(identity),
                    &std::fs::read(&path).unwrap(),
                )
                .unwrap();
            writer.seal().unwrap();
            std::fs::remove_file(path).unwrap();
        }
    }
    sessions
}

fn configured(rig: &Rig, mode: Mode) -> Engine {
    let engine = Engine::configured(
        &rig.dir,
        Config {
            archives: !matches!(mode, Mode::Startup),
            live_at_startup: matches!(mode, Mode::Startup),
            backup_root: Some(rig.dir.with_extension("converted-sources")),
        },
    )
    .unwrap()
    .unwrap();
    engine.set_mover(Arc::new(rig.mover()));
    engine
}

async fn run_serving(rig: &Rig, mode: Mode) {
    let probe = Arc::new(Probe {
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Semaphore::new(0),
        sources: Mutex::default(),
        crashes: BTreeMap::new(),
        crash_barrier: None,
        active: Mutex::default(),
        overlap: Default::default(),
    });
    let mut engine = configured(rig, mode);
    engine.parallel_probe = Some(probe.clone());
    let runtime = rig.runtime.clone();
    let registry = rig.registry.clone();
    let task = tokio::spawn(async move {
        match mode {
            Mode::Startup => {
                engine
                    .run_live(&deployment(), Some(&runtime), Some(&registry))
                    .await
            }
            Mode::Background => {
                engine
                    .run_archives(
                        &deployment(),
                        Some(&runtime),
                        Some(&registry),
                        Duration::ZERO,
                    )
                    .await
            }
            Mode::Offline => unreachable!(),
        }
        .unwrap();
        assert!(!engine.health_for_test().live_converting);
        assert_eq!(engine.health_for_test().failed_sessions.len(), 1);
        assert_eq!(
            engine.health_for_test().progress,
            engine.ledger.progress().unwrap()
        );
    });
    broca_test_support::wait_until("two conversion workers enter", || async {
        let count = probe.sources.lock().unwrap().len();
        if count == 2 {
            Ok(())
        } else {
            Err(format!("{count} workers entered"))
        }
    })
    .await;
    probe.release.add_permits(3);
    task.await.unwrap();
    assert_eq!(probe.sources.lock().unwrap().len(), 3);
}

async fn run(rig: &Rig, mode: Mode) {
    if matches!(mode, Mode::Offline) {
        crate::session_conversion::rehearse(&rig.dir, &rig.dir.with_extension("converted-sources"))
            .await
            .unwrap();
    } else {
        run_serving(rig, mode).await;
    }
}

async fn assert_results(
    rig: &Rig,
    sessions: &[BindIdentity],
    originals: &BTreeMap<String, Vec<u8>>,
) -> Vec<Row> {
    let ledger = Ledger::open(&rig.dir).unwrap();
    let rows = ledger.rows().unwrap();
    assert_eq!(rows.len(), 3);
    for (index, identity) in sessions.iter().enumerate() {
        let address = broca_core::session_addr(identity);
        let row = rows
            .iter()
            .find(|row| {
                row.source.member.as_deref() == Some(&address)
                    || row.source.path == format!("wal/{address}.wal")
            })
            .unwrap();
        if index == 1 {
            assert_eq!(row.outcome.as_deref(), Some("failed"));
            assert_eq!(row.refusal.as_deref(), Some("panic"));
            assert!(row.detail.as_deref().unwrap().contains(PANIC_TEXT));
            assert_eq!(
                row.step,
                if row.source.member.is_some() {
                    "discovered"
                } else {
                    "recovering"
                }
            );
            assert_eq!(
                std::fs::read(rig.dir.join(&row.source.path)).unwrap(),
                originals[&row.source.path]
            );
            assert!(!Path::new(&row.source.backup).exists());
            assert!(rig.runtime.current_view(identity).await.unwrap().is_none());
        } else {
            assert_eq!(row.step, "done");
            assert_eq!(row.outcome.as_deref(), Some("converted"));
            assert_eq!(row.refusal, None);
            assert!(!rig.dir.join(&row.source.path).exists());
            assert_eq!(
                std::fs::read(&row.source.backup).unwrap(),
                originals[&row.source.path]
            );
            let view = rig.runtime.current_view(identity).await.unwrap().unwrap();
            assert_eq!(broca_module::store_read::transcript_len(&view), 4);
        }
    }
    rows
}

async fn exercise(mode: Mode, tag: &str) {
    let rig = Rig::start(tag).await;
    let backup = rig.dir.with_extension("converted-sources");
    let _ = std::fs::remove_dir_all(&backup);
    let archived = !matches!(mode, Mode::Startup);
    let sessions = write_sources(&rig, archived);
    let originals: BTreeMap<_, _> = (0..3)
        .map(|index| {
            let path = if archived {
                format!("wal-archive/fold-{}.ark", 3000 + index)
            } else {
                format!("wal/{}.wal", broca_core::session_addr(&sessions[index]))
            };
            let bytes = std::fs::read(rig.dir.join(&path)).unwrap();
            (path, bytes)
        })
        .collect();
    run(&rig, mode).await;
    let before = assert_results(&rig, &sessions, &originals).await;

    // Observe actual ledger writes on restart without relying on timestamp resolution.
    // Final rows must not be touched, but the failed source must be tried again.
    let connection =
        rusqlite::Connection::open(rig.dir.join(crate::conversion_ledger::FILE)).unwrap();
    connection
        .execute_batch(
            "CREATE TABLE retry_observations (source_id TEXT NOT NULL);
         CREATE TRIGGER observe_retry AFTER UPDATE ON sources BEGIN
             INSERT INTO retry_observations VALUES (NEW.source_id);
         END;",
        )
        .unwrap();
    if matches!(mode, Mode::Offline) {
        run(&rig, mode).await;
    } else {
        // Only the failed source remains eligible. Calling run_serving again would wait
        // for a second worker that has no source to process.
        let engine = configured(&rig, mode);
        match mode {
            Mode::Startup => {
                engine
                    .run_live(&deployment(), Some(&rig.runtime), Some(&rig.registry))
                    .await
            }
            Mode::Background => {
                engine
                    .run_archives(
                        &deployment(),
                        Some(&rig.runtime),
                        Some(&rig.registry),
                        Duration::ZERO,
                    )
                    .await
            }
            Mode::Offline => unreachable!(),
        }
        .unwrap();
    }
    let after = assert_results(&rig, &sessions, &originals).await;
    assert_eq!(after, before);
    let observed = connection
        .prepare("SELECT source_id FROM retry_observations")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(0))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    let failed = after
        .iter()
        .find(|row| row.outcome.as_deref() == Some("failed"))
        .unwrap();
    assert!(!observed.is_empty(), "the nonterminal failure is retried");
    assert!(
        observed.iter().all(|id| id == &failed.source.id),
        "final sessions were revisited: {observed:?}"
    );
    std::fs::remove_dir_all(backup).unwrap();
}

#[tokio::test]
async fn conversion_panic_startup_is_session_local() {
    exercise(Mode::Startup, "panic-startup").await;
}

#[tokio::test]
async fn conversion_panic_background_is_session_local() {
    exercise(Mode::Background, "panic-background").await;
}

#[tokio::test]
async fn conversion_panic_offline_is_session_local() {
    exercise(Mode::Offline, "panic-offline").await;
}
