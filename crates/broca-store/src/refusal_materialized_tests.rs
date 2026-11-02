use super::*;
use broca_core::refusal_fallback::{route, StepRoute};
use broca_protocol::{ModelHandle, StepId};
use broca_wal::{FramedRecord, WalRecord};
use serde_json::json;

fn frame(seq: u64, record: WalRecord) -> FramedRecord {
    FramedRecord {
        seq,
        fence: 1,
        record,
    }
}

fn fixture(session: &BindIdentity) -> Vec<FramedRecord> {
    let mut start = crate::conformance::run_started(
        1,
        "first",
        session,
        "hello",
        ModelHandle::new("anthropic", "source"),
    );
    if let WalRecord::RunStarted { ts_ms, .. } = &mut start.record {
        *ts_ms = Some(1_000);
    }
    let schedule = json!({"source":{"provider_module_id":"anthropic", "model_id":"source"},
        "fallback":{"provider_module_id":"openai", "model_id":"fallback"},
        "phase":{"kind":"fallback", "remaining":9}, "re_refusals":0, "updated_at_ms":1_002});
    let mut armed = schedule.clone();
    armed["phase"]["remaining"] = json!(10);
    vec![
        start,
        frame(2, WalRecord::StepStarted { step_id: StepId(1) }),
        frame(
            3,
            serde_json::from_value(json!({"type":"model_step_refused", "step_id":1,
            "usage":{"input_tokens":5,"output_tokens":2}, "discarded_blocks":1,
            "schedule":armed, "ts_ms":1_001}))
            .unwrap(),
        ),
        frame(
            4,
            serde_json::from_value(json!({"type":"model_step_finished", "step_id":1,
            "assistant_message":{"message_id":"m1", "content":[{"type":"text","text":"answer"}]},
            "usage":{"input_tokens":5,"output_tokens":2}, "refusal_fallback":schedule,
            "route":{"model":{"provider_module_id":"openai","model_id":"fallback"},
                "reason":"refusal"}}))
            .unwrap(),
        ),
        frame(
            5,
            WalRecord::RunFinished {
                interrupt_reason: None,
                ts_ms: Some(1_003),
                reason: broca_protocol::RunFinishReason::Completed,
                usage: Default::default(),
                error: None,
                retries_used: 0,
            },
        ),
    ]
}

fn full(records: &[FramedRecord]) -> broca_core::ReplayState {
    broca_core::replay(&records.iter().map(|f| f.record.clone()).collect::<Vec<_>>()).unwrap()
}

fn merged(
    store: &SqliteStore,
    session: &BindIdentity,
    records: &[FramedRecord],
) -> broca_core::ReplayState {
    let prefix = store.read_prefix_with_watermark(session).unwrap();
    assert_eq!(prefix.watermark_seq, 4);
    let tail = records
        .iter()
        .filter(|f| f.seq > prefix.watermark_seq)
        .map(|f| f.record.clone())
        .collect::<Vec<_>>();
    let expected = full(records);
    broca_core::context::resume_state_from_store(
        &prefix,
        &tail,
        expected.run_config,
        expected.run_id,
        None,
    )
    .unwrap()
}

#[test]
fn refusal_materialized_cross_episode_matches_full_schedule_and_route() {
    let store = SqliteStore::open_in_memory().unwrap();
    let session = crate::conformance::session("refusal-carry");
    let mut records = fixture(&session);
    store.project(&session, &records).unwrap();
    let mut second = crate::conformance::run_started(
        6,
        "second",
        &session,
        "next",
        ModelHandle::new("anthropic", "source"),
    );
    if let WalRecord::RunStarted { ts_ms, .. } = &mut second.record {
        *ts_ms = Some(2_000);
    }
    records.push(second);
    let expected = full(&records);
    let prior = merged(&store, &session, &records);
    assert_eq!(prior.refusal_fallback, expected.refusal_fallback);
    assert_eq!(
        route(prior.refusal_fallback.as_ref(), true),
        StepRoute::Fallback
    );
    assert_eq!(
        route(prior.refusal_fallback.as_ref(), true),
        route(expected.refusal_fallback.as_ref(), true)
    );
    assert_eq!(prior.refused_steps, expected.refused_steps);
}

#[test]
fn refusal_materialized_same_episode_retains_refused_steps() {
    let store = SqliteStore::open_in_memory().unwrap();
    let session = crate::conformance::session("refusal-resume");
    let records = fixture(&session);
    store.project(&session, &records).unwrap();
    let prior = merged(&store, &session, &records);
    let expected = full(&records);
    assert_eq!(prior.refused_steps, expected.refused_steps);
    assert_eq!(prior.refused_steps, [StepId(1)].into_iter().collect());
    assert_eq!(prior.refusal_fallback, expected.refusal_fallback);
}

#[test]
fn refusal_materialized_admission_expiry_and_model_change_stay_in_tail() {
    for (model, at) in [("source", 1_002 + 86_400_001), ("different", 2_000)] {
        let store = SqliteStore::open_in_memory().unwrap();
        let session = crate::conformance::session("refusal-admission");
        let mut records = fixture(&session);
        store.project(&session, &records).unwrap();
        let before = store.read_prefix_with_watermark(&session).unwrap();
        let mut second = crate::conformance::run_started(
            6,
            "second",
            &session,
            "next",
            ModelHandle::new("anthropic", model),
        );
        if let WalRecord::RunStarted { ts_ms, .. } = &mut second.record {
            *ts_ms = Some(at);
        }
        records.push(second);
        store.project(&session, &records).unwrap();
        let after = store.read_prefix_with_watermark(&session).unwrap();
        assert_eq!(after.watermark_seq, before.watermark_seq);
        assert_eq!(
            after.refusal, before.refusal,
            "unmaterialized admissions must not pre-clear the seed"
        );
        let prior = merged(&store, &session, &records);
        assert_eq!(prior.refusal_fallback, full(&records).refusal_fallback);
        assert!(prior.refusal_fallback.is_none());
        assert!(prior.refused_steps.is_empty());
        assert_eq!(
            route(prior.refusal_fallback.as_ref(), true),
            StepRoute::Source
        );
    }
}

#[test]
fn refusal_materialized_incremental_projection_carries_state() {
    let store = SqliteStore::open_in_memory().unwrap();
    let session = crate::conformance::session("refusal-incremental");
    let records = fixture(&session);
    store.project(&session, &records).unwrap();
    let seed = store.projection_seed(&session).unwrap().unwrap();
    assert_eq!(seed.refusal.schedule, full(&records[..4]).refusal_fallback);
    // The terminal tail has no refusal record. Continuing projection must still
    // remember all refused steps from the episode, rather than only suffix facts.
    let delta = broca_core::replay_projection_tail(&seed, &records[4..]).unwrap();
    assert_eq!(
        delta.refusal.refused_steps,
        [StepId(1)].into_iter().collect()
    );
    assert_eq!(delta.refusal.schedule, seed.refusal.schedule);
}

#[test]
fn refusal_materialized_old_reader_and_replacing_writer_are_compatible() {
    let store = SqliteStore::open_in_memory().unwrap();
    let session = crate::conformance::session("refusal-rollback");
    store.project(&session, &fixture(&session)).unwrap();
    let conn = store.conn.lock().unwrap();
    // These are the old reader's named columns and unchanged JSON type. The extra
    // column is invisible to it, and its episode decoder accepts additive members.
    let (watermark, episodes): (i64, String) = conn
        .query_row(
            "SELECT watermark_seq, episode_metadata_json FROM meta WHERE session=?1",
            [SqliteStore::key(&session)],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .unwrap();
    assert_eq!(watermark, 4);
    let mut value: serde_json::Value = serde_json::from_str(&episodes).unwrap();
    value[0]["future_optional_member"] = json!(true);
    // EpisodeRenderProjection is unchanged from the legacy reader; it deliberately
    // has no deny_unknown_fields attribute.
    let decoded: Vec<broca_core::context::EpisodeRenderProjection> =
        serde_json::from_value(value).unwrap();
    assert_eq!(decoded.len(), 1);
    let columns = conn
        .prepare("PRAGMA table_info(meta)")
        .unwrap()
        .query_map([], |row| row.get::<_, String>(1))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .into_iter()
        .filter(|column| column != "refusal_replay_json")
        .collect::<Vec<_>>()
        .join(",");
    conn.execute(
        &format!(
            "INSERT OR REPLACE INTO meta ({columns}) SELECT {columns} FROM meta WHERE session=?1"
        ),
        [SqliteStore::key(&session)],
    )
    .unwrap();
    drop(conn);
    assert!(
        store
            .read_prefix_with_watermark(&session)
            .unwrap()
            .refusal
            .is_none(),
        "an old replacing writer must leave unknown state, never a stale seed"
    );
    assert!(store.projection_seed(&session).unwrap().is_none());
    let mut records = fixture(&session);
    records.push(crate::conformance::run_started(
        6,
        "second",
        &session,
        "next",
        ModelHandle::new("anthropic", "source"),
    ));
    records.push(frame(7, WalRecord::StepStarted { step_id: StepId(1) }));
    let mut answer = records[3].clone();
    answer.seq = 8;
    records.push(answer);
    let mut terminal = records[4].clone();
    terminal.seq = 9;
    records.push(terminal);
    store.project(&session, &records).unwrap();
    assert!(store
        .read_prefix_with_watermark(&session)
        .unwrap()
        .refusal
        .is_some());
}

#[test]
fn refusal_materialized_old_updating_writer_cannot_reuse_stale_checkpoint() {
    let store = SqliteStore::open_in_memory().unwrap();
    let session = crate::conformance::session("refusal-old-update");
    store.project(&session, &fixture(&session)).unwrap();
    // Older projection code uses ON CONFLICT DO UPDATE, not just row replacement.
    // It can leave unknown columns intact while advancing the watermark.
    store
        .conn
        .lock()
        .unwrap()
        .execute(
            "UPDATE meta SET watermark_seq=watermark_seq+1 WHERE session=?1",
            [SqliteStore::key(&session)],
        )
        .unwrap();
    assert!(store
        .read_prefix_with_watermark(&session)
        .unwrap()
        .refusal
        .is_none());
    assert!(store.projection_seed(&session).unwrap().is_none());
}

#[test]
fn refusal_materialized_nullable_column_add_does_not_rewrite_table_pages() {
    let root =
        broca_test_support::scratch_root().join(format!("refusal-column-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("store.db");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=DELETE; CREATE TABLE meta(session TEXT PRIMARY KEY, watermark_seq INTEGER);
        WITH RECURSIVE n(i) AS (VALUES(1) UNION ALL SELECT i+1 FROM n WHERE i<10000)
        INSERT INTO meta (session, watermark_seq) SELECT printf('session-%08d',i), i FROM n;").unwrap();
    let page_size: usize = conn
        .query_row("PRAGMA page_size", [], |r| r.get(0))
        .unwrap();
    let before = std::fs::read(&path).unwrap();
    let changes = conn.total_changes();
    conn.execute_batch("ALTER TABLE meta ADD COLUMN refusal_replay_json TEXT")
        .unwrap();
    assert_eq!(conn.total_changes(), changes);
    let after = std::fs::read(&path).unwrap();
    assert_eq!(before.len(), after.len());
    assert_eq!(
        &before[page_size..],
        &after[page_size..],
        "only sqlite_schema on the first page changes; table and index pages stay byte-identical"
    );
    assert_eq!(
        conn.query_row(
            "SELECT COUNT(*) FROM meta WHERE refusal_replay_json IS NULL",
            [],
            |r| r.get::<_, u64>(0)
        )
        .unwrap(),
        10_000
    );
}

#[test]
fn refusal_materialized_ten_thousand_step_replay_cost() {
    use broca_wal::Wal;
    let root = broca_test_support::scratch_root()
        .join(format!("refusal-replay-cost-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("generated.wal");
    let session = crate::conformance::session("refusal-replay-cost");
    let mut writer = broca_wal::FileWal::open(&path).unwrap();
    let start = crate::conformance::run_started(
        1,
        "worker",
        &session,
        "hello",
        ModelHandle::new("stub", "worker"),
    );
    writer.append(1, &start.record, false).unwrap();
    for step in 1..=10_000 {
        writer
            .append(
                1,
                &WalRecord::StepStarted {
                    step_id: StepId(step),
                },
                false,
            )
            .unwrap();
        let record = serde_json::from_value(json!({"type":"model_step_finished", "step_id":step,
            "assistant_message":{"message_id":format!("m{step}"), "content":[{"type":"text","text":"x".repeat(128)}]},
            "usage":{"input_tokens":100,"output_tokens":32}})).unwrap();
        writer.append(1, &record, false).unwrap();
    }
    writer
        .append(1, &fixture(&session)[4].record, true)
        .unwrap();
    drop(writer);
    let bytes = std::fs::metadata(&path).unwrap().len();
    let started = std::time::Instant::now();
    let frames = broca_wal::replay_file(&path, bytes).unwrap();
    let read_decode_us = started.elapsed().as_micros();
    let replay_started = std::time::Instant::now();
    let state = full(&frames);
    let replay_us = replay_started.elapsed().as_micros();
    assert_eq!(state.completed_steps, 10_000);
    let report = json!({"steps":10_000,"frames":frames.len(),"bytes":bytes,
        "read_decode_us":read_decode_us,"replay_us":replay_us,
        "total_us":started.elapsed().as_micros(),"cache":"generated file, warm page cache"});
    println!("refusal legacy full replay cost: {report}");
    if let Some(path) = std::env::var_os("BROCA_REFUSAL_REPLAY_COST_REPORT") {
        let path = std::path::PathBuf::from(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, serde_json::to_vec_pretty(&report).unwrap()).unwrap();
    }
}
