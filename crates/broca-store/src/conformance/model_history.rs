use super::*;
use crate::SqliteStore;
use broca_core::{ModelPageRequest, ReadView, StorePageSnapshot};

fn repeated_episodes(session: &BindIdentity) -> Vec<FramedRecord> {
    let mut records = Vec::new();
    for episode in 0..3 {
        records.push(run_started(
            records.len() as u64 + 1,
            &format!("run-{episode}"),
            session,
            "question",
            ModelHandle::new("stub", "m"),
        ));
        for step in 0..6 {
            let mut turn = tool_turn(records.len() as u64 + 1, step);
            for frame in &mut turn {
                if let WalRecord::ToolResult { result, .. } = &mut frame.record {
                    std::sync::Arc::make_mut(result).is_error = true;
                }
            }
            records.extend(turn);
        }
        records.extend(text_turn(records.len() as u64 + 1, 6, "done"));
        records.push(framed(
            records.len() as u64 + 1,
            WalRecord::RunFinished {
                reason: RunFinishReason::Completed,
                ts_ms: None,
                usage: Usage::default(),
                error: None,
                retries_used: 0,
                interrupt_reason: None,
            },
        ));
    }
    records
}

fn full_page(records: &[FramedRecord], request: &ModelPageRequest) -> broca_core::ModelPage {
    // A legacy snapshot without its compaction checkpoint forces the independent full-replay
    // branch, even if the optimized tail-only reader could reconstruct this particular page.
    let snapshot = StorePageSnapshot {
        watermark_seq: 1,
        prefix_len: 1,
        ..StorePageSnapshot::empty()
    };
    broca_core::read_session_model_page(&snapshot, records, request, || Ok(records.to_vec()))
        .unwrap()
}

#[test]
fn model_tool_pages_match_full_replay_without_reading_the_complete_wal() {
    for cut in [None, Some(0), Some(14)] {
        compare_model_tool_pages(cut);
    }
}

fn compare_model_tool_pages(cut: Option<u64>) {
    let session = session("model-guidance-window");
    let mut records = repeated_episodes(&session);
    if let Some(cut) = cut {
        records.insert(
            1,
            super::compaction::compaction_message(0, broca_wal::CompactionMessageKind::Setup),
        );
        if cut > 0 {
            let mut update =
                super::compaction::compaction_message(0, broca_wal::CompactionMessageKind::Step);
            if let WalRecord::CompactionMessage { range, .. } = &mut update.record {
                range.to = cut;
            }
            records.insert(records.len() - 3, update);
        }
        for (i, frame) in records.iter_mut().enumerate() {
            frame.seq = i as u64 + 1;
        }
    }
    let store = SqliteStore::open_in_memory().unwrap();
    let full = full_page(
        &records,
        &ModelPageRequest {
            from_ordinal: Some(0),
            limit: Some(500),
            ..Default::default()
        },
    );
    assert!(
        full.messages
            .iter()
            .any(|entry| serde_json::to_string(&entry.message)
                .unwrap()
                .contains("Tool repetition guidance:")),
        "fixture must exercise repeated-call guidance"
    );
    // Project every completed turn separately, including seams inside each episode. Each
    // request then combines that materialized prefix with all later episodes in the WAL tail.
    for end in 1..=records.len() {
        if !matches!(records[end - 1].record, WalRecord::TurnFinished { .. }) {
            continue;
        }
        Persister::project(&store, &session, &records[..end]).unwrap();
        for from in [
            None,
            Some(0),
            Some(5),
            Some(12),
            Some(16),
            Some(22),
            Some(39),
            Some(42),
            Some(1000),
        ] {
            for limit in [1, 2, 7, 500] {
                let snapshot = MaterializedStore::read_page_with_watermark(
                    &store,
                    &session,
                    from,
                    limit,
                    ReadView::Model,
                )
                .unwrap();
                let tail: Vec<_> = records
                    .iter()
                    .filter(|frame| frame.seq > snapshot.watermark_seq)
                    .cloned()
                    .collect();
                for max_bytes in [None, Some(600)] {
                    let request = ModelPageRequest {
                        from_ordinal: from,
                        limit: Some(limit),
                        max_bytes,
                        ..Default::default()
                    };
                    let actual =
                        broca_core::read_session_model_page(&snapshot, &tail, &request, || {
                            assert!(
                                tail.iter().any(|frame| matches!(
                                    frame.record,
                                    WalRecord::CompactionMessage {
                                        kind: broca_wal::CompactionMessageKind::Step,
                                        ..
                                    }
                                )),
                                "only a tail compaction may move the stored window"
                            );
                            Ok(records.clone())
                        })
                        .unwrap();
                    let mut expected = full_page(&records, &request);
                    expected.lineage_id = snapshot.lineage_id.clone();
                    assert_eq!(
                        actual, expected,
                        "projection end={end}, from={from:?}, limit={limit}, bytes={max_bytes:?}"
                    );
                }
            }
        }
    }
}

#[test]
fn missing_episode_metadata_means_replay_and_projection_repairs_it() {
    let session = session("model-guidance-legacy");
    let records = repeated_episodes(&session);
    let store = SqliteStore::open_in_memory().unwrap();
    Persister::project(&store, &session, &records).unwrap();
    store
        .conn
        .lock()
        .unwrap()
        .execute("UPDATE meta SET episode_spans_complete=0", [])
        .unwrap();
    assert!(Persister::projection_seed(&store, &session)
        .unwrap()
        .is_none());
    let request = ModelPageRequest {
        from_ordinal: Some(22),
        limit: Some(2),
        ..Default::default()
    };
    let snapshot = MaterializedStore::read_page_with_watermark(
        &store,
        &session,
        Some(0),
        u64::MAX,
        ReadView::Model,
    )
    .unwrap();
    assert!(snapshot.model_history.is_none());
    let reads = std::cell::Cell::new(0);
    let actual = broca_core::read_session_model_page(&snapshot, &[], &request, || {
        reads.set(reads.get() + 1);
        Ok(records.clone())
    })
    .unwrap();
    let mut expected = full_page(&records, &request);
    expected.head = actual.head;
    expected.lineage_id = actual.lineage_id.clone();
    assert_eq!(actual, expected);
    assert_eq!(reads.get(), 1);
    Persister::project(&store, &session, &records).unwrap();
    let repaired = MaterializedStore::read_page_with_watermark(
        &store,
        &session,
        request.from_ordinal,
        2,
        ReadView::Model,
    )
    .unwrap();
    assert!(repaired.model_history.is_some());
}

#[test]
fn model_store_reads_begin_at_the_pages_episode_not_at_ordinal_zero() {
    let session = session("model-guidance-bounds");
    let records = repeated_episodes(&session);
    let store = SqliteStore::open_in_memory().unwrap();
    Persister::project(&store, &session, &records).unwrap();
    let snapshot =
        MaterializedStore::read_page_with_watermark(&store, &session, Some(22), 2, ReadView::Model)
            .unwrap();
    assert_eq!(snapshot.rows.first().unwrap().ordinal, 15);
    assert!(snapshot.rows.len() < snapshot.prefix_len as usize);
    assert!(snapshot
        .rows
        .iter()
        .any(|row| row.message.role == Role::Tool));
}

#[test]
fn stale_episode_metadata_replays_and_repairs_instead_of_reusing_old_counters() {
    let session = session("model-guidance-stale-watermark");
    let records = repeated_episodes(&session);
    let store = SqliteStore::open_in_memory().unwrap();
    Persister::project(&store, &session, &records).unwrap();
    store
        .conn
        .lock()
        .unwrap()
        .execute("UPDATE meta SET episode_spans_seq=watermark_seq-1", [])
        .unwrap();
    let snapshot = MaterializedStore::read_page_with_watermark(
        &store,
        &session,
        Some(0),
        u64::MAX,
        ReadView::Model,
    )
    .unwrap();
    assert!(
        snapshot.model_history.is_none(),
        "stale spans must not be exposed as a complete model snapshot"
    );
    assert!(Persister::projection_seed(&store, &session)
        .unwrap()
        .is_none());
    let request = ModelPageRequest {
        from_ordinal: Some(22),
        limit: Some(2),
        ..Default::default()
    };
    let reads = std::cell::Cell::new(0);
    let actual = broca_core::read_session_model_page(&snapshot, &[], &request, || {
        reads.set(reads.get() + 1);
        Ok(records.clone())
    })
    .unwrap();
    let mut expected = full_page(&records, &request);
    expected.head = actual.head;
    expected.lineage_id = actual.lineage_id.clone();
    assert_eq!(actual, expected);
    assert_eq!(reads.get(), 1);
    Persister::project(&store, &session, &records).unwrap();
    let repaired = MaterializedStore::read_page_with_watermark(
        &store,
        &session,
        Some(0),
        u64::MAX,
        ReadView::Model,
    )
    .unwrap();
    assert!(repaired.model_history.is_some());
    assert!(Persister::projection_seed(&store, &session)
        .unwrap()
        .is_some());
}
