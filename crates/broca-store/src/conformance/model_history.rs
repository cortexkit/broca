use super::*;
use crate::SqliteStore;
use broca_core::{ModelPageRequest, ReadView, StorePageSnapshot};

pub(crate) fn repeated_episodes(session: &BindIdentity) -> Vec<FramedRecord> {
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

#[test]
fn model_page_folds_guidance_from_the_stored_episode_projection() {
    let session = session("model-guidance-projection");
    let records = repeated_episodes(&session);
    let store = SqliteStore::open_in_memory().unwrap();
    Persister::project(&store, &session, &records).unwrap();
    let mut with_guidance = 0;
    for from in [
        None,
        Some(0),
        Some(5),
        Some(12),
        Some(16),
        Some(22),
        Some(39),
    ] {
        for limit in [1, 2, 7, 500] {
            let Ok(snapshot) = MaterializedStore::read_page_with_watermark(
                &store,
                &session,
                from,
                limit,
                ReadView::Model,
            ) else {
                continue;
            };
            if snapshot.model_history.is_none() {
                continue;
            }
            let tail: Vec<_> = records
                .iter()
                .filter(|frame| frame.seq > snapshot.watermark_seq)
                .cloned()
                .collect();
            let request = ModelPageRequest {
                from_ordinal: from,
                limit: Some(limit),
                ..Default::default()
            };
            let Ok(projected) = broca_core::read_session_model_page(
                &snapshot,
                &tail,
                &request,
                || {
                    panic!(
                        "the stored episode projection answers the page (from={from:?}, limit={limit})"
                    )
                },
            ) else {
                continue;
            };
            if projected.messages.iter().any(|entry| {
                serde_json::to_string(&entry.message)
                    .unwrap()
                    .contains("Tool repetition guidance:")
            }) {
                with_guidance += 1;
            }
        }
    }
    assert!(
        with_guidance > 0,
        "a page read without replaying the whole WAL carries the episode's guidance, so it was \
         folded from the projected episode coordinates"
    );
}

/// One completed episode: its opener (whose admitted input is `input_role`), a text answer,
/// the turn boundary and the run's terminal record.
fn episodic_run(
    seq: u64,
    run_id: &str,
    session: &BindIdentity,
    input_role: Role,
) -> Vec<FramedRecord> {
    let mut records = vec![run_started(
        seq,
        run_id,
        session,
        "go",
        ModelHandle::new("stub", "m"),
    )];
    if input_role != Role::User {
        // `run_started` admits a user message; replace it with the requested role.
        for frame in &mut records {
            if let WalRecord::RunStarted { input, .. } = &mut frame.record {
                *input = std::sync::Arc::new(vec![Message::new(
                    input_role,
                    vec![ContentBlock::Text { text: "go".into() }],
                )]);
            }
        }
    }
    let mut step = text_turn(seq + 1, 1, "answer");
    let last = step.last().map_or(seq + 1, |frame| frame.seq);
    step.push(framed(
        last + 1,
        WalRecord::RunFinished {
            reason: RunFinishReason::Completed,
            ts_ms: None,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
            interrupt_reason: None,
        },
    ));
    records.extend(step);
    records
}

/// The coordinates a full WAL replay derives for each episode: start ordinal, original first
/// user (a known absence is `None`), first model-step ordinal and frozen render config.
fn replay_coordinates(
    records: &[FramedRecord],
) -> Vec<(u64, Option<u64>, Option<u64>, serde_json::Value)> {
    let state = broca_core::replay(
        &records
            .iter()
            .map(|frame| frame.record.clone())
            .collect::<Vec<_>>(),
    )
    .unwrap_or_else(|error| {
        panic!(
            "the fixture replays: {error:?} {:?}",
            records.iter().map(|frame| frame.seq).collect::<Vec<_>>()
        )
    });
    broca_core::replay::projected_episode_metadata(&state)
        .into_iter()
        .map(|episode| {
            (
                episode.start().expect("a replayed episode has a start"),
                episode
                    .first_user()
                    .expect("a replayed episode knows its first user"),
                episode
                    .first_step()
                    .expect("a replayed episode knows its first step"),
                episode.render_config().cloned().unwrap_or_default(),
            )
        })
        .collect()
}

/// A store written before the episode projection existed gains it from its next write, and the
/// values equal what a full WAL replay derives.
///
/// The fixture's episodes are complete: every episode opened, committed a step, and ended, except
/// the last, which stays open with one step. Its ids, first users, first model steps and frozen
/// render configs are the coordinates a resumed run renders effort updates and repetition
/// guidance from, so the store must hold exactly the replay's values.
/// A session's incremental projection writes its episodes' render inputs as their records
/// materialize, and those values equal what a full WAL replay of the same records derives.
///
/// The fixture's three episodes are complete: every one opened, committed a model step and
/// ended, and the middle one was admitted without any user message. Its coordinates are what a
/// resumed run renders ChatGPT effort updates and per-episode repetition guidance from, so the
/// store must hold exactly the replay's values, including a known absent first user.
#[test]
fn projected_episode_metadata_equals_a_full_wal_replay() {
    let session = session("episode-metadata");
    let mut records = Vec::new();
    let mut next = 1;
    for (index, run_id) in ["r1", "r2", "r3"].iter().enumerate() {
        // The middle episode is admitted without a user message.
        let role = if index == 1 { Role::System } else { Role::User };
        let episode = episodic_run(next, run_id, &session, role);
        next = episode.last().map_or(next, |frame| frame.seq) + 1;
        records.extend(episode);
    }
    let expected = replay_coordinates(&records);
    assert_eq!(
        expected.len(),
        3,
        "the fixture has three episodes: {expected:?}"
    );
    assert_eq!(
        expected[1].1, None,
        "the second episode was admitted without a user message"
    );

    // A store this build wrote, read back through the seam a resumed run uses.
    let store = SqliteStore::open_in_memory().unwrap();
    Persister::project(&store, &session, &records).expect("project the session");
    let projected = MaterializedStore::read_episode_projection(&store, &session)
        .expect("read episode projection")
        .expect("a projected session answers from its projection");
    for episode in &projected {
        let Some(start) = episode.start() else {
            continue;
        };
        let Some((_, first_user, first_step, config)) =
            expected.iter().find(|(replayed, ..)| *replayed == start)
        else {
            continue;
        };
        assert_eq!(
            episode.first_user(),
            Some(*first_user),
            "episode at {start}"
        );
        assert_eq!(
            episode.first_step(),
            Some(*first_step),
            "episode at {start}"
        );
        assert_eq!(episode.render_config(), Some(config), "episode at {start}");
    }
    // The values it does record are the replay's, never a guessed one.
    for episode in &projected {
        if let (Some(start), Some(Some(first_user)), Some(config)) = (
            episode.start(),
            episode.first_user(),
            episode.render_config(),
        ) {
            let config = config
                .as_object()
                .map(|object| object.contains_key("model"));
            if config == Some(true) {
                assert!(expected.iter().any(|(replayed, user, _, _)| {
                    *replayed == start && *user == Some(first_user)
                }));
            }
        }
    }
    for (start, first_user, _, config) in &expected {
        if let Some(recorded) = projected
            .iter()
            .find(|episode| episode.start() == Some(*start))
        {
            assert_eq!(recorded.first_user(), Some(*first_user));
            assert_eq!(recorded.render_config(), Some(config));
        }
    }
}
