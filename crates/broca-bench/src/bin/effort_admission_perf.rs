//! Compare store-seeded prior assembly with forced WAL replay on read-only WAL copies.
//! The first directory is read only; copied WALs and temporary SQLite stores go in the second.
//! `cargo run --release -p broca-bench --bin effort_admission_perf -- <live-wal-dir> <scratch-dir>`
//! Reports warmed medians (nine samples); fixture construction is outside the timings.
use broca_core::{
    load_prior_wal_snapshot, replay, resume_state_from_store, FileWalStore, MaterializedStore,
    Persister, StorePrefix, WalStore,
};
use broca_wal::{replay_file, WalRecord, DEFAULT_MAX_READ_BYTES};
use std::{hint::black_box, path::PathBuf, time::Instant};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args_os().skip(1);
    let root = PathBuf::from(args.next().ok_or("missing WAL directory")?);
    let scratch = PathBuf::from(args.next().ok_or("missing scratch directory")?);
    std::fs::create_dir_all(&scratch)?;
    let mut paths = std::fs::read_dir(root)?
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .path()
                .extension()
                .is_some_and(|extension| extension == "wal")
        })
        .map(|entry| Ok((entry.metadata()?.len(), entry.path())))
        .collect::<Result<Vec<_>, std::io::Error>>()?;
    paths.sort_by_key(|entry| std::cmp::Reverse(entry.0));
    let mut selected = 0;
    for (_, source) in paths {
        let copy = scratch.join(source.file_name().ok_or("missing filename")?);
        if !copy.exists() {
            std::fs::copy(&source, &copy)?;
        }
        let mut permissions = std::fs::metadata(&copy)?.permissions();
        permissions.set_readonly(true);
        std::fs::set_permissions(&copy, permissions)?;
        let frames = match replay_file(&copy, DEFAULT_MAX_READ_BYTES) {
            Ok(frames) => frames,
            Err(_) => continue,
        };
        let codex_mason = frames
            .iter()
            .rev()
            .find_map(|frame| match &frame.record {
                WalRecord::RunStarted {
                    session, config, ..
                } => Some(
                    (session.harness.contains("mason")
                        || session.session.starts_with("alfonso:bg_"))
                        && config.frozen_render_config["family_id"] == "openai_responses_chatgpt",
                ),
                _ => None,
            })
            .unwrap_or(false);
        if !codex_mason {
            continue;
        }
        let rows: Vec<_> = frames.iter().map(|frame| frame.record.clone()).collect();
        let boundary = rows
            .iter()
            .rposition(|record| matches!(record, WalRecord::TurnFinished { .. }))
            .ok_or("no materialization boundary")?
            + 1;
        let full = replay(&rows)?;
        let projected = replay(&rows[..boundary])?;
        let prefix = StorePrefix {
            prefix: projected.prompt,
            completed_steps: projected.completed_steps,
            usage: projected.usage,
            retries_used: projected.retries_used,
            last_step_usage: projected.last_step_usage,
            last_step_finish_reason: projected.last_step_finish_reason,
            watermark_seq: frames[boundary - 1].seq,
            context_epoch: 0,
        };
        let budget_seed = broca_core::AdmissionCheckpointV1::from_frames(&frames[..boundary])?
            .last_episode
            .and_then(|episode| episode.budget_replay_state);
        let tail = &rows[boundary..];
        let fast = || {
            resume_state_from_store(
                black_box(&prefix),
                black_box(tail),
                full.run_config.clone(),
                full.run_id.clone(),
                budget_seed.clone(),
            )
            .expect("store prior build")
        };
        assert_eq!(
            fast().prompt,
            full.prompt,
            "copied fixture must have equivalent prompt"
        );
        let session = frames
            .iter()
            .find_map(|frame| match &frame.record {
                WalRecord::RunStarted { session, .. } => Some(session.clone()),
                _ => None,
            })
            .ok_or("missing session")?;
        let wal_store = FileWalStore::new(&scratch);
        let store = broca_store::SqliteStore::open(scratch.join(format!(
            "{}-{}.db",
            copy.file_stem().unwrap().to_string_lossy(),
            std::process::id()
        )))?;
        let range = wal_store.read_records_range(&session, 0, None, None)?;
        store.project_range(&session, &range)?;
        let fast_end_to_end = || {
            let snapshot = load_prior_wal_snapshot(
                &wal_store,
                &session,
                frames.last().unwrap().seq,
                Some(&store),
            )
            .expect("checkpoint and tail read");
            assert!(
                snapshot.base_watermark > 0,
                "benchmark must use a persisted checkpoint"
            );
            assert!(
                snapshot.tail.len() < frames.len(),
                "fast path must read a suffix, not the whole WAL"
            );
            let prefix = store
                .read_prefix_with_watermark(&session)
                .expect("materialized prefix read");
            let tail = snapshot
                .tail
                .iter()
                .filter(|frame| frame.seq > prefix.watermark_seq)
                .map(|frame| frame.record.clone())
                .collect::<Vec<_>>();
            let budget = snapshot
                .checkpoint_at_base
                .as_ref()
                .and_then(|checkpoint| checkpoint.last_episode.as_ref())
                .and_then(|episode| episode.budget_replay_state.clone());
            let episode = snapshot
                .checkpoint_at_head
                .last_episode
                .as_ref()
                .expect("last episode");
            let mut prior = resume_state_from_store(
                &prefix,
                &tail,
                Some(episode.config.clone()),
                Some(episode.run_id.clone()),
                budget,
            )
            .expect("end-to-end store prior");
            snapshot
                .checkpoint_at_head
                .restore_unchanged_effort(&mut prior);
            prior
        };
        assert_eq!(fast_end_to_end().prompt, full.prompt);
        let forced = || {
            let rows: Vec<_> = replay_file(&copy, DEFAULT_MAX_READ_BYTES)
                .expect("read copied WAL")
                .into_iter()
                .map(|frame| frame.record)
                .collect();
            replay(black_box(&rows)).expect("full prior build")
        };
        let median = |build: &dyn Fn() -> broca_core::ReplayState| {
            drop(build());
            let mut times = Vec::new();
            for _ in 0..9 {
                let begin = Instant::now();
                let state = build();
                times.push(begin.elapsed());
                black_box(&state);
                drop(state);
            }
            times.sort();
            times[4].as_secs_f64() * 1000.0
        };
        println!("{} bytes={} frames={} prompt_messages={} tail_frames={} fast_prepared_ms={:.3} fast_end_to_end_ms={:.3} forced_full_ms={:.3}", copy.file_name().unwrap().to_string_lossy(), std::fs::metadata(&copy)?.len(), frames.len(), full.prompt.len(), tail.len(), median(&fast), median(&fast_end_to_end), median(&forced));
        selected += 1;
        if selected == 2 {
            break;
        }
    }
    if selected != 2 {
        return Err(format!("found only {selected} Codex mason WALs").into());
    }
    Ok(())
}
