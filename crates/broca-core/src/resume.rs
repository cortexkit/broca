//! What a resume writes before it does anything else.
//!
//! A resume picks up an episode that has no durable `RunFinished`: the process that ran it
//! crashed or was stopped. Before the resume calls a provider, dispatches a tool or seals the
//! episode, it writes one informational [`WalRecord::ResumeStarted`] saying what it found.
//!
//! When the episode holds an indeterminate tool call (a durable dispatch intent with no
//! result, so the call may or may not have run), the resume never sends it again and makes no
//! model call. It closes the open batch instead: a `ToolResult` with outcome `Unknown` for each
//! indeterminate call, an `Interrupted` result for each planned call that never left the
//! process, and the batch's commit markers. Those records go in one barrier with the resume
//! record, and the caller then seals the episode `Interrupted` in a barrier of its own. The
//! owner's next send renders every call of that batch paired with its result, so no request
//! ever carries a tool call with no result, and the model learns the outcome is uncertain
//! before it would repeat the call.
//!
//! Both the run loop and the module's crash-orphan pass resume through these functions, so the
//! close-out has one shape however the resume is reached.

use std::sync::Arc;

use broca_protocol::{
    content::{ToolOutput, ToolResult},
    ToolCallId,
};
use broca_wal::{DispatchOutcome, ResumeRedo, ResumeReplayed, WalRecord, WithdrawElicitation};

use crate::replay::{OpenCallState, ReplayResolution, ReplayState};

/// One transient offset-index read, bounded by the writer's durable head. The adapter loads
/// messages and frozen episode inputs from their WAL locators, not database content copies.
/// Boundary, projection scalars, message rows and tail must describe the same index snapshot.
/// This value is deliberately not serializable: both the message prefix and the projection's
/// admission checkpoint contain hydrated request/config content. Persist locators and scalars,
/// not this snapshot.
#[derive(Clone)]
pub struct OffsetResumeSnapshot {
    pub boundary: crate::replay::SeedBoundary,
    pub projection: crate::replay::ProjectionSeed,
    /// Absent for the dense, zero-based prefix. A window selects absolute transcript rows
    /// from the same index transaction as the boundary and projection.
    pub window: Option<crate::context::OffsetPromptWindow>,
    pub prefix: Vec<broca_protocol::Message>,
    pub tail: Vec<crate::replay::PositionedFrame>,
}

pub struct OffsetResumeState {
    pub prior: ReplayState,
    pub snapshot: crate::run::PriorWalSnapshot,
    pub placement: crate::placement::Placement,
}

/// Opt-in reader seam for overhaul roots. The offset store must explicitly supply this reader
/// to the run entry point; ordinary run entry points never select it.
pub trait OffsetResumeReader: Send + Sync {
    /// Hydrate the requested message rows and episode metadata using checked WAL locators.
    /// Implementations must verify source sequences/identities and bound reads to `safe_head`.
    fn snapshot(&self, safe_head: u64)
        -> Result<OffsetResumeSnapshot, crate::replay::SeedFallback>;
    fn read_at(&self, offset: u64, len: u64) -> std::io::Result<Vec<u8>>;
    /// Report whether reconstruction accepted the validated seed or fell back to the complete WAL.
    fn reconstructed(&self, _fallback: Option<crate::replay::SeedFallback>) {}
}

/// Validate snapshot consistency and the boundary before using continuation state. A compacted
/// prefix must supply its complete selected window; missing coordinates or continuation state
/// return a fallback so the caller reads the complete WAL rather than producing a partial prompt.
pub fn reconstruct_from_offsets(
    read_at: &dyn Fn(u64, u64) -> std::io::Result<Vec<u8>>,
    snapshot: OffsetResumeSnapshot,
    safe_head: u64,
) -> Result<OffsetResumeState, crate::replay::SeedFallback> {
    use crate::replay::{
        replay_projection_tail, replay_seeded_at_validated_boundary, SeedFallback,
    };
    let bad = |reason| SeedFallback { reason };
    let mut seed = snapshot.projection;
    let window = snapshot.window;
    if seed.watermark_seq != snapshot.boundary.seq
        || seed.admission_checkpoint.at_seq != seed.watermark_seq
        || (window.is_none()
            && (seed.prefix_len != snapshot.prefix.len() as u64
                || snapshot
                    .prefix
                    .iter()
                    .enumerate()
                    .any(|(ord, m)| m.ordinal != Some(ord as u64))))
        || snapshot
            .tail
            .last()
            .map_or(seed.watermark_seq, |p| p.framed.seq)
            != safe_head
        || snapshot
            .tail
            .iter()
            .filter(|p| matches!(p.framed.record, WalRecord::RunStarted { .. }))
            .count()
            > 1
    {
        return Err(bad("offset resume snapshot does not reach durable head"));
    }
    #[cfg(feature = "test-attachments")]
    if !seed.admission_checkpoint.media_seed_complete() {
        return Err(bad("offset resume media seed needs full replay"));
    }
    if !seed.admission_checkpoint.ownership_complete()
        || seed.admission_checkpoint.has_deferred_calls != Some(false)
        || seed.admission_checkpoint.input_budget.is_none()
        || seed.admission_checkpoint.output_reservation.is_none()
        || (window.is_none() && !matches!(
            seed.admission_checkpoint.compaction,
            Some(crate::compaction::CompactionCheckpoint::None)
        ))
        || crate::replay::tail_contains_compaction_setup(
            &snapshot
                .tail
                .iter()
                .map(|p| p.framed.clone())
                .collect::<Vec<_>>(),
        )
        || !crate::replay::budget_replay_seed_is_valid(&seed)
        // A refreeze can replace an earlier episode's render input. This composer does not
        // replace previously projected inputs after a refreeze, so it requires the complete WAL.
        || snapshot.tail.iter().any(|p| matches!(p.framed.record,
            WalRecord::RunConfigRefrozen { .. } | WalRecord::RunAuthRefrozen { .. }))
    {
        return Err(bad("offset resume prefix needs full replay"));
    }
    if let Some(window) = &window {
        crate::context::validate_offset_prompt_window(
            window,
            &snapshot.prefix,
            &seed,
            &snapshot.boundary,
        )?;
        seed.projection_episodes = Some(
            window
                .episodes
                .iter()
                .map(|e| e.projection.clone())
                .collect(),
        );
    }
    let episode = seed.admission_checkpoint.last_episode.as_ref();
    let mut state = replay_seeded_at_validated_boundary(
        read_at,
        &snapshot.boundary,
        &snapshot.tail,
        seed.trailing_origin.clone(),
        episode.map(|e| e.run_id.clone()),
        episode.and_then(|e| e.config.budget.clone()),
        episode.and_then(|e| e.budget_replay_state.clone()),
        seed.prefix_len,
        seed.prefix_len != 0,
        seed.tag_seed,
        Vec::new(),
    )?;
    let frames: Vec<_> = snapshot.tail.into_iter().map(|p| p.framed).collect();
    let mut placement = crate::projection::ControlProjectionState::from_projection_seed(&seed)
        .ok_or_else(|| bad("offset resume placement seed is incomplete"))?
        .placement()
        .clone();
    for frame in &frames {
        placement
            .observe(&frame.record)
            .map_err(|_| bad("offset resume placement tail is invalid"))?;
    }
    // Projection composition retains the existing counter/reset and episode-coordinate rules.
    // Only the checked WAL tail is folded; the hydrated request-message prefix is reused.
    let delta = replay_projection_tail(&seed, &frames)
        .map_err(|_| bad("offset resume projection composition failed"))?;
    let checkpoint = delta.admission_checkpoint;
    // A tail can introduce the first media capture into a previously media-free prefix.
    // Recheck its folded state: a valid selected window is not a media-state completeness proof.
    #[cfg(feature = "test-attachments")]
    if !checkpoint.media_seed_complete() {
        return Err(bad("offset resume media seed needs full replay"));
    }
    if window.is_none()
        && !matches!(
            checkpoint.compaction,
            Some(crate::compaction::CompactionCheckpoint::None)
        )
    {
        return Err(bad("offset resume compaction needs full replay"));
    }
    let episodes = delta
        .episodes
        .ok_or_else(|| bad("offset resume has no episode coordinates"))?;
    if (window.is_none() && episodes.first().and_then(|e| e.start()) != Some(0))
        || episodes.iter().enumerate().any(|(i, e)| {
            let end = episodes
                .get(i + 1)
                .and_then(|e| e.start())
                .unwrap_or(state.extent);
            e.start().is_none_or(|start| {
                start > end
                    || end > state.extent
                    || e.first_user()
                        .flatten()
                        .is_some_and(|ord| ord < start || ord >= end)
                    || e.first_step()
                        .flatten()
                        .is_some_and(|ord| ord < start || ord >= end)
            })
        })
    {
        return Err(bad(
            "offset resume episode coordinates are outside the prompt",
        ));
    }
    state.render_episodes = crate::run::episode_render_inputs(&episodes)
        .map_err(|_| bad("offset resume episode coordinates overflow"))?;
    if state.render_episodes.is_none() {
        return Err(bad("offset resume episode coordinates are incomplete"));
    }
    state.episode_step_spans = Some(
        episodes
            .iter()
            .enumerate()
            .filter_map(|(i, e)| {
                e.first_step().flatten().map(|start| {
                    let end = episodes
                        .get(i + 1)
                        .and_then(|e| e.start())
                        .unwrap_or(state.extent);
                    start as usize..end as usize
                })
            })
            .collect(),
    );
    let mut prefix = snapshot.prefix;
    if let Some(to) = delta.truncate_prefix_to {
        prefix.truncate(to as usize);
    }
    prefix.reserve(state.prompt.len() + 256);
    prefix.append(&mut state.prompt);
    if window.is_none() && prefix.len() as u64 != state.extent {
        return Err(bad("offset resume prompt extent is inconsistent"));
    }
    state.prompt = prefix;
    if let Some(window) = window {
        if delta.truncate_prefix_to.is_some() {
            return Err(bad("offset prompt window was abandoned"));
        }
        crate::context::finish_offset_prompt_window(
            &mut state,
            window,
            &seed,
            &checkpoint,
            &episodes,
        )?;
    }
    state.message_runs = vec![None; state.prompt.len()];
    state.completed_steps = delta.completed_steps;
    state.usage = delta.usage;
    state.retries_used = delta.retries_used;
    state.last_step_usage = delta.last_step_usage;
    state.last_step_finish_reason = delta.last_step_finish_reason;
    state.provider_call_started |= state.completed_steps != 0;
    let current = checkpoint.last_episode.as_ref();
    state.run_config = current.map(|e| e.config.clone());
    state.run_id = current.map(|e| e.run_id.clone());
    state.on_restart = current.and_then(|e| e.on_restart);
    state.keep_warm = current.and_then(|e| e.keep_warm);
    state.cancel_at = current.and_then(|e| e.cancel_at);
    state.marker_order_version = current.and_then(|e| e.marker_order_version);
    state.input_budget = checkpoint
        .input_budget
        .clone()
        .ok_or_else(|| bad("offset resume input budget is incomplete"))?;
    state.output_reservation = checkpoint
        .output_reservation
        .clone()
        .ok_or_else(|| bad("offset resume output reservation is incomplete"))?;
    state.manifest = checkpoint.ownership.as_ref().and_then(|o| {
        o.latest_manifest
            .clone()
            .or_else(|| o.first_episode.as_ref().and_then(|e| e.manifest.clone()))
    });
    Ok(OffsetResumeState {
        prior: state,
        snapshot: crate::run::PriorWalSnapshot {
            base_watermark: seed.watermark_seq,
            head_seq: safe_head,
            checkpoint_at_base: Some(seed.admission_checkpoint),
            checkpoint_at_head: checkpoint,
            tail: Arc::new(frames),
        },
        placement,
    })
}

#[cfg(test)]
mod offset_resume_tests {
    use super::*;
    use crate::replay::{PositionedFrame, ProjectionSeed, SeedBoundary};
    use broca_protocol::{
        caller::RunFinishReason, content::AssistantMessage, ContentBlock, Message, Role, StepId,
        Usage,
    };
    use broca_wal::{framing, FramedRecord};
    use sha2::{Digest, Sha256};

    fn start(run: &str) -> WalRecord {
        let mut record = crate::replay::projection_boundary_tests::run_started(1, run).record;
        if let WalRecord::RunStarted { input, .. } = &mut record {
            *input = vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: run.into() }],
            )]
            .into();
        }
        record
    }

    fn step(id: u64) -> WalRecord {
        WalRecord::ModelStepFinished {
            fallback: Default::default(),
            written_at_ms: None,
            hooks: None,
            service_tier_used: None,
            speed_used: None,
            step_id: StepId(id),
            assistant_message: Arc::new(AssistantMessage {
                message_id: broca_protocol::MessageId(format!("m{id}")),
                content: vec![ContentBlock::Text {
                    text: format!("answer {id}"),
                }],
            }),
            usage: Usage {
                input_tokens: Some(20),
                output_tokens: Some(id),
                ..Usage::default()
            },
            finish_reason: Some(broca_protocol::FinishReason::Stop),
            retries_used: 0,
            tags: None,
        }
    }

    fn finish() -> WalRecord {
        WalRecord::RunFinished {
            interrupt_reason: None,
            ts_ms: None,
            reason: RunFinishReason::Completed,
            usage: Usage::default(),
            error: None,
            retries_used: 0,
        }
    }

    fn corpus() -> Vec<WalRecord> {
        vec![
            start("old"),
            WalRecord::StepStarted { step_id: StepId(1) },
            step(1),
            finish(),
            start("current"),
            WalRecord::StepStarted { step_id: StepId(1) },
            step(1),
            finish(),
        ]
    }

    fn encode(records: &[WalRecord]) -> (Vec<u8>, Vec<PositionedFrame>, Vec<u64>) {
        let mut bytes = Vec::new();
        let mut positions = Vec::new();
        let mut ends = Vec::new();
        for (i, record) in records.iter().enumerate() {
            let frame = FramedRecord {
                seq: i as u64 + 1,
                fence: 1,
                record: record.clone(),
            };
            positions.push(PositionedFrame {
                off: bytes.len() as u64,
                framed: frame.clone(),
            });
            bytes.extend(framing::encode(frame.seq, frame.fence, &frame.record).unwrap());
            ends.push(bytes.len() as u64);
        }
        (bytes, positions, ends)
    }

    fn reader(bytes: &[u8]) -> impl Fn(u64, u64) -> std::io::Result<Vec<u8>> + '_ {
        move |off, len| {
            let start = (off as usize).min(bytes.len());
            Ok(bytes[start..(start.saturating_add(len as usize)).min(bytes.len())].to_vec())
        }
    }

    fn snapshot(
        bytes: &[u8],
        positions: &[PositionedFrame],
        ends: &[u64],
        boundary: usize,
    ) -> OffsetResumeSnapshot {
        let frames: Vec<_> = positions[..=boundary]
            .iter()
            .map(|p| p.framed.clone())
            .collect();
        let prior =
            crate::replay::replay(&frames.iter().map(|f| f.record.clone()).collect::<Vec<_>>())
                .unwrap();
        let delta =
            crate::replay::replay_projection_tail(&ProjectionSeed::empty(), &frames).unwrap();
        let mut projection = ProjectionSeed::empty();
        projection.watermark_seq = frames.last().unwrap().seq;
        projection.prefix_len = prior.extent;
        projection.completed_steps = prior.completed_steps;
        projection.usage = prior.usage;
        projection.retries_used = prior.retries_used;
        projection.last_step_usage = prior.last_step_usage;
        projection.last_step_finish_reason = prior.last_step_finish_reason;
        projection.last_run_id = prior.run_id;
        projection.trailing_origin = prior.prompt.iter().rev().find_map(|m| m.origin.clone());
        projection.tag_seed = crate::tags::TagSeed::from_prefix(&prior.prompt);
        projection.admission_checkpoint = delta.admission_checkpoint;
        projection.projection_episodes = delta.episodes;
        projection.open_episode_span = delta.open_episode_span;
        OffsetResumeSnapshot {
            window: None,
            boundary: SeedBoundary {
                lineage_id: None,
                seq: positions[boundary].framed.seq,
                frame_off: positions[boundary].off,
                frame_end: ends[boundary],
                frame_identity: Sha256::digest(
                    &bytes[positions[boundary].off as usize..ends[boundary] as usize],
                )
                .to_vec(),
                qualified: true,
            },
            projection,
            prefix: prior.prompt,
            tail: positions[boundary + 1..].to_vec(),
        }
    }

    fn assert_continuation(actual: &ReplayState, expected: &ReplayState) {
        assert_eq!(actual.prompt, expected.prompt);
        assert_eq!(actual.run_config, expected.run_config);
        assert_eq!(actual.run_id, expected.run_id);
        assert_eq!(actual.completed_steps, expected.completed_steps);
        assert_eq!(actual.usage, expected.usage);
        assert_eq!(actual.retries_used, expected.retries_used);
        assert_eq!(actual.last_step_usage, expected.last_step_usage);
        assert_eq!(
            actual.last_step_finish_reason,
            expected.last_step_finish_reason
        );
        assert_eq!(actual.resolution, expected.resolution);
        assert_eq!(actual.render_episodes, expected.render_episodes);
        assert_eq!(actual.episode_step_spans, expected.episode_step_spans);
        assert_eq!(actual.open_tool_batch, expected.open_tool_batch);
        assert_eq!(actual.tool_call_count, expected.tool_call_count);
        assert_eq!(actual.pending_steers, expected.pending_steers);
        assert_eq!(actual.input_budget, expected.input_budget);
        assert_eq!(actual.output_reservation, expected.output_reservation);
        assert_eq!(
            resume_started_record(actual, 8, planned_redo(actual)),
            resume_started_record(expected, 8, planned_redo(expected))
        );
    }

    #[test]
    fn offset_resume_matches_complete_continuation_at_every_cut() {
        let records = corpus();
        for cut in 4..=records.len() {
            let (bytes, positions, ends) = encode(&records[..cut]);
            let loaded = reconstruct_from_offsets(
                &reader(&bytes),
                snapshot(&bytes, &positions, &ends, 2),
                cut as u64,
            )
            .unwrap();
            let expected = crate::replay::replay(&records[..cut]).unwrap();
            assert_continuation(&loaded.prior, &expected);
            assert_eq!(loaded.snapshot.head_seq, cut as u64);
        }
    }

    #[test]
    fn offset_resume_preserves_open_tool_turn_and_redo_at_every_cut() {
        let mut records = corpus()[..4].to_vec();
        records.extend(crate::replay::deferred_tests::batch(&["a", "b"]));
        records.push(crate::replay::deferred_tests::terminal("b", None));
        records.push(crate::replay::deferred_tests::terminal("a", None));
        records.extend(crate::replay::deferred_tests::finished());
        for cut in 5..=records.len() {
            let (bytes, positions, ends) = encode(&records[..cut]);
            let loaded = reconstruct_from_offsets(
                &reader(&bytes),
                snapshot(&bytes, &positions, &ends, 2),
                cut as u64,
            )
            .unwrap();
            let expected = crate::replay::replay(&records[..cut]).unwrap();
            assert_continuation(&loaded.prior, &expected);
        }
    }

    #[test]
    fn offset_resume_rejects_invalid_boundary_and_incomplete_snapshot() {
        let records = corpus();
        let (bytes, positions, ends) = encode(&records);
        for case in 0..8 {
            let mut candidate = snapshot(&bytes, &positions, &ends, 2);
            match case {
                0 => candidate.boundary.frame_identity[0] ^= 1,
                1 => candidate.boundary.qualified = false,
                2 => candidate.tail[0].off += 1,
                3 => candidate.tail[0].framed.seq += 1,
                4 => candidate.projection.admission_checkpoint.has_deferred_calls = Some(true),
                5 => {
                    candidate.prefix.pop();
                }
                6 => candidate.tail.pop().map(|_| ()).unwrap(),
                _ => {
                    candidate.projection.projection_episodes.as_mut().unwrap()[0].first_step =
                        Some(Some(10000))
                }
            }
            assert!(
                reconstruct_from_offsets(&reader(&bytes), candidate, 8).is_err(),
                "case {case}"
            );
        }
        let candidate = snapshot(&bytes, &positions, &ends, 2);
        assert!(
            reconstruct_from_offsets(&reader(&bytes[..ends[1] as usize]), candidate, 8).is_err()
        );
    }

    #[test]
    fn offset_resume_decodes_only_the_boundary_and_single_episode_tail() {
        let mut records = corpus();
        let mut history = Vec::new();
        for _ in 0..1000 {
            history.extend(records[..4].iter().cloned());
        }
        history.append(&mut records);
        let (bytes, positions, ends) = encode(&history);
        let boundary = positions.len() - 6;
        let candidate = snapshot(&bytes, &positions, &ends, boundary);
        let decoded = std::cell::Cell::new(0);
        let tail = crate::projection::decode_positioned_frames(
            &reader(&bytes),
            ends[boundary],
            bytes.len() as u64,
            Some(&decoded),
        )
        .unwrap();
        assert_eq!(tail.len(), 5);
        let checked_reads = std::cell::RefCell::new(Vec::new());
        let boundary_range = (
            candidate.boundary.frame_off,
            candidate.boundary.frame_end - candidate.boundary.frame_off,
        );
        let read_at = |off, len| {
            checked_reads.borrow_mut().push((off, len));
            reader(&bytes)(off, len)
        };
        reconstruct_from_offsets(
            &read_at,
            OffsetResumeSnapshot { tail, ..candidate },
            positions.len() as u64,
        )
        .unwrap();
        assert_eq!(decoded.get(), 5);
        assert_eq!(
            *checked_reads.borrow(),
            vec![boundary_range, (0, framing::LINEAGE_FRAME_LEN as u64)],
            "read only the boundary and lineage header, never the history range"
        );
    }

    fn compacted_tools(replaced_turns: usize, with_view: bool) -> Vec<WalRecord> {
        let mut first = start("window");
        if let WalRecord::RunStarted { input, .. } = &mut first {
            *input = vec![
                Message::new(
                    Role::System,
                    vec![ContentBlock::Text {
                        text: "system".into(),
                    }],
                ),
                Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "question".into(),
                    }],
                ),
            ]
            .into();
        }
        let mut records = vec![first];
        for turn in 1..=5 {
            records.push(WalRecord::StepStarted {
                step_id: StepId(turn),
            });
            let mut batch = crate::replay::deferred_tests::batch(&["call"]);
            batch.remove(0);
            if let WalRecord::ModelStepFinished {
                assistant_message,
                step_id,
                ..
            } = &mut batch[0]
            {
                *step_id = StepId(turn);
                let ContentBlock::ToolCall(call) = &mut Arc::make_mut(assistant_message).content[0]
                else {
                    unreachable!()
                };
                call.tool_name = "read".into();
            }
            if let WalRecord::ToolBatchStarted { step_id, .. } = &mut batch[1] {
                *step_id = StepId(turn);
            }
            records.extend(batch);
            let mut result = crate::replay::deferred_tests::terminal("call", None);
            if let WalRecord::ToolResult {
                result, outcome, ..
            } = &mut result
            {
                *result = crate::replay::deferred_tests::result("call", "same result");
                *outcome = broca_wal::DispatchOutcome::Completed;
            }
            records.push(result);
            records.push(WalRecord::ToolBatchFinished {
                batch_id: broca_protocol::BatchId("b".into()),
            });
            if turn as usize == replaced_turns {
                let replacement = Arc::new(vec![Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: "summary".into(),
                    }],
                )]);
                records.push(WalRecord::CompactionMessage {
                    provider: "compactor".into(),
                    kind: broca_wal::CompactionMessageKind::Setup,
                    compaction_id: "window".into(),
                    version: 1,
                    range: broca_wal::CompactionRange {
                        from: 1,
                        to: 2 + 2 * turn,
                    },
                    sha256: broca_wal::compaction_replacement_sha256(&replacement).unwrap(),
                    replacement,
                    request_id: None,
                    cursor: None,
                    call_when: None,
                    stability: None,
                    ceiling: None,
                    tag_checkpoint: None,
                    ride_wait_ms: None,
                });
                if with_view {
                    use crate::compaction_provider::{
                        lineage::MessageRef,
                        view::{CompactionView, ViewRange},
                    };
                    let base = 2 + 2 * turn;
                    let summary = serde_json::to_string(&vec![Message::new(
                        Role::User,
                        vec![ContentBlock::Text {
                            text: "summary".into(),
                        }],
                    )])
                    .unwrap();
                    let ranges = if turn >= 3 {
                        vec![
                            ViewRange::replace(0, 4, summary.clone()),
                            ViewRange::keep(4, 6),
                            ViewRange::replace(6, base, summary),
                        ]
                    } else {
                        vec![ViewRange::replace(0, base, summary)]
                    };
                    let view = CompactionView::new(
                        "session",
                        2,
                        "lineage",
                        MessageRef {
                            ordinal: base - 1,
                            mid: format!("m{}", base - 1),
                        },
                        ranges,
                    );
                    records.push(WalRecord::CompactionViewApplied {
                        provider: "compactor".into(),
                        view_version: 2,
                        view: broca_wal::OpaqueJson::from_json_text(
                            serde_json::to_string(&view).unwrap(),
                        )
                        .unwrap(),
                        choice: broca_wal::CompactionViewChoice::Applied,
                    });
                }
            }
            records.push(WalRecord::TurnFinished {
                step_id: StepId(turn),
            });
        }
        records
    }

    fn window_snapshot(
        bytes: &[u8],
        positions: &[PositionedFrame],
        ends: &[u64],
        boundary: usize,
    ) -> OffsetResumeSnapshot {
        let mut candidate = snapshot(bytes, positions, ends, boundary);
        let episodes = candidate
            .projection
            .projection_episodes
            .as_ref()
            .unwrap()
            .clone();
        candidate.projection.open_episode = episodes.last().cloned();
        let compaction = candidate
            .projection
            .admission_checkpoint
            .compaction
            .as_ref()
            .unwrap()
            .state()
            .unwrap();
        let base = compaction.view.as_ref().map_or_else(
            || compaction.applied.as_ref().unwrap().range.to,
            |v| v.view.covers_through.ordinal + 1,
        );
        let mut fold = crate::tool_repetition::RepetitionFold::default();
        let mut guidance = Vec::new();
        for message in candidate.prefix.iter().skip(2) {
            fold.push(message);
            if let Some(checkpoint) = fold.checkpoint() {
                guidance.push(crate::context::OffsetGuidanceCheckpoint {
                    ordinal: message.ordinal.unwrap() + 1,
                    at_seq: candidate.projection.watermark_seq,
                    fold: checkpoint,
                });
            }
        }
        let newest = candidate.prefix.last().unwrap().ordinal;
        candidate.prefix.retain(|m| {
            compaction.view.as_ref().map_or_else(
                || m.role == Role::System || m.ordinal >= Some(base),
                |v| crate::run::held_prompt::held_by(&v.view, m),
            ) || m.ordinal == newest
        });
        candidate.window = Some(crate::context::OffsetPromptWindow {
            base,
            systems: vec![0],
            guidance,
            episodes: episodes
                .into_iter()
                .map(|projection| crate::context::HeldEpisode {
                    projection,
                    next_start: None,
                })
                .collect(),
        });
        candidate
    }

    fn file_window(
        records: &[WalRecord],
        label: &str,
    ) -> (Vec<u8>, Vec<PositionedFrame>, Vec<u64>, String) {
        use crate::WalStore;
        let root = broca_test_support::scratch_root()
            .join(format!("offset-window-{}-{label}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        std::fs::write(
            root.join(broca_wal::state_format::MARKER_FILENAME),
            b"broca-state/2\n",
        )
        .unwrap();
        let store = crate::FileWalStore::with_config(
            root.join("wal"),
            broca_wal::classify_state_root(&root).unwrap(),
        );
        let WalRecord::RunStarted { session, .. } = &records[0] else {
            unreachable!()
        };
        let mut wal = store.open(session).unwrap();
        let lineage = wal.lineage_id().unwrap().to_string();
        for record in records {
            let mut record = record.clone();
            if let WalRecord::CompactionViewApplied { view, .. } = &mut record {
                let mut value: serde_json::Value =
                    serde_json::from_str(view.as_json_text()).unwrap();
                value["lineage_id"] = lineage.clone().into();
                *view = broca_wal::OpaqueJson::from_json_text(value.to_string()).unwrap();
            }
            wal.append(1, &record, true).unwrap();
        }
        drop(wal);
        let bytes = std::fs::read(store.wal_path(session)).unwrap();
        let mut positions = Vec::new();
        let mut ends = Vec::new();
        let mut lineage = None;
        let mut off = 0;
        while off < bytes.len() {
            match framing::decode_next(&bytes[off..]) {
                framing::DecodeStep::Record { framed, consumed } => {
                    positions.push(PositionedFrame {
                        off: off as u64,
                        framed: *framed,
                    });
                    off += consumed;
                    ends.push(off as u64);
                }
                framing::DecodeStep::Lineage {
                    lineage_id,
                    consumed,
                } => {
                    lineage = Some(lineage_id.to_string());
                    off += consumed;
                }
                _ => panic!("real WAL fixture must decode"),
            }
        }
        std::fs::remove_dir_all(root).unwrap();
        (bytes, positions, ends, lineage.unwrap())
    }

    fn rendered_window(state: &ReplayState, full: bool) -> broca_protocol::CallOptions {
        let mut options = broca_protocol::CallOptions::new(state.prompt.clone());
        if full {
            for span in state.episode_step_spans.as_ref().unwrap() {
                crate::run::restore_repetition_guidance(&mut options.prompt[span.clone()]);
            }
        }
        options.render_episodes = Arc::new(state.render_episodes.clone().unwrap().into());
        crate::compaction::model_options(&options, state.compaction.state().unwrap()).unwrap()
    }

    #[test]
    fn offset_resume_window_preserves_file_wal_request_bytes_at_every_cut() {
        for (replaced, with_view) in [(1, false), (2, false), (3, false), (1, true), (3, true)] {
            let records = compacted_tools(replaced, with_view);
            let boundary = records.iter().position(|r| matches!(r, WalRecord::TurnFinished { step_id } if step_id.0 as usize == replaced)).unwrap();
            for cut in boundary + 1..=records.len() {
                let (bytes, positions, ends, lineage) =
                    file_window(&records[..cut], &format!("{replaced}-{with_view}-{cut}"));
                let mut candidate = window_snapshot(&bytes, &positions, &ends, boundary);
                candidate.boundary.lineage_id = Some(lineage);
                assert_eq!(
                    candidate.prefix.len(),
                    if with_view && replaced == 3 { 4 } else { 2 },
                    "only selected rows and newest anchor are resident below the cut"
                );
                let loaded =
                    reconstruct_from_offsets(&reader(&bytes), candidate, cut as u64).unwrap();
                let full = crate::replay::replay(
                    &positions
                        .iter()
                        .map(|p| p.framed.record.clone())
                        .collect::<Vec<_>>(),
                )
                .unwrap();
                let actual = rendered_window(&loaded.prior, false);
                let expected = rendered_window(&full, true);
                assert_eq!(
                    crate::canonical_request_bytes(&actual),
                    crate::canonical_request_bytes(&expected),
                    "replacement {replaced}, cut {cut}"
                );
                assert_eq!(actual.render_episodes, expected.render_episodes);
                assert_eq!(loaded.prior.extent, full.extent);
                assert_eq!(
                    loaded.prior.held.as_ref().unwrap().observer,
                    crate::tool_repetition::RepetitionObserver::from_prompt(&full.prompt[2..])
                );
                if full.prompt.len() >= 6 && replaced == 1 {
                    let text = String::from_utf8(crate::canonical_request_bytes(&actual)).unwrap();
                    assert!(text.contains("returned exactly the same result 2 times"));
                }
            }
        }
    }

    #[test]
    #[cfg(feature = "test-attachments")]
    fn offset_resume_window_rejects_incomplete_media_seed_before_reusing_prefix() {
        let mut records = compacted_tools(1, false);
        if let WalRecord::RunStarted { input, .. } = &mut records[0] {
            Arc::make_mut(input)[1]
                .content
                .push(ContentBlock::Media(broca_protocol::MediaRef {
                    kind: broca_protocol::MediaKind::Image,
                    mime: "image/png".into(),
                    byte_size: 69,
                    sha256: "a".repeat(64),
                    width: Some(1),
                    height: Some(1),
                }));
        }
        let boundary = records
            .iter()
            .position(|record| matches!(record, WalRecord::TurnFinished { .. }))
            .unwrap();
        let (bytes, positions, ends, lineage) = file_window(&records, "media-seed");
        let make = || {
            let mut candidate = window_snapshot(&bytes, &positions, &ends, boundary);
            candidate.boundary.lineage_id = Some(lineage.clone());
            candidate
        };
        let valid = make();
        assert!(valid.window.is_some());
        assert!(valid.projection.admission_checkpoint.media_seed_complete());
        reconstruct_from_offsets(&reader(&bytes), valid, records.len() as u64).unwrap();
        for case in 0..3 {
            let mut candidate = make();
            let seed = &mut candidate.projection.admission_checkpoint.media;
            *seed = match case {
                0 => None,
                1 => Some(serde_json::from_value(serde_json::json!({"version":99})).unwrap()),
                _ => {
                    let mut value = serde_json::to_value(seed.as_ref().unwrap()).unwrap();
                    value["index"]["boundary"] = serde_json::json!(999);
                    Some(serde_json::from_value(value).unwrap())
                }
            };
            let before = crate::media_request::seed_fallback_count();
            let error = reconstruct_from_offsets(&reader(&bytes), candidate, records.len() as u64)
                .err()
                .expect("an incomplete media seed must request full replay");
            assert_eq!(error.reason, "offset resume media seed needs full replay");
            assert!(crate::media_request::seed_fallback_count() > before);
        }
    }

    #[test]
    #[cfg(feature = "test-attachments")]
    fn offset_resume_window_checks_media_seed_after_tail_capture() {
        let mut records = compacted_tools(1, false);
        let boundary = records
            .iter()
            .position(|record| matches!(record, WalRecord::TurnFinished { .. }))
            .unwrap();
        let image = Message::new(
            Role::User,
            vec![ContentBlock::Media(broca_protocol::MediaRef {
                kind: broca_protocol::MediaKind::Image,
                mime: "image/png".into(),
                byte_size: 69,
                sha256: "b".repeat(64),
                width: Some(1),
                height: Some(1),
            })],
        );
        records.push(
            serde_json::from_value(serde_json::json!({"type":"steered","input":[image]})).unwrap(),
        );
        records.push(WalRecord::StepStarted { step_id: StepId(6) });
        let (bytes, positions, ends, lineage) = file_window(&records, "media-tail-seed");
        let make = || {
            let mut candidate = window_snapshot(&bytes, &positions, &ends, boundary);
            candidate.boundary.lineage_id = Some(lineage.clone());
            candidate
        };
        let mut valid = make();
        let mut metadata = crate::media_request::MediaCheckpoint::empty();
        for frame in &positions[..=boundary] {
            metadata
                .fold(frame.framed.seq, &frame.framed.record)
                .unwrap();
        }
        valid.projection.admission_checkpoint.media = Some(
            crate::media_request::DurableMediaSeed::Ready(Box::new(metadata)),
        );
        reconstruct_from_offsets(&reader(&bytes), valid, records.len() as u64).unwrap();
        let missing = make();
        assert!(
            missing
                .projection
                .admission_checkpoint
                .media_seed_complete(),
            "the prefix contains no captures yet"
        );
        let before = crate::media_request::seed_fallback_count();
        let error = reconstruct_from_offsets(&reader(&bytes), missing, records.len() as u64)
            .err()
            .expect("a new tail capture needs a complete media seed");
        assert_eq!(error.reason, "offset resume media seed needs full replay");
        assert!(crate::media_request::seed_fallback_count() > before);
    }

    #[test]
    fn offset_resume_window_rejects_unproven_selection_and_guidance() {
        let records = compacted_tools(1, false);
        let boundary = records
            .iter()
            .position(|r| matches!(r, WalRecord::TurnFinished { .. }))
            .unwrap();
        let (bytes, positions, ends, lineage) = file_window(&records, "refusals");
        for case in 0..8 {
            let mut candidate = window_snapshot(&bytes, &positions, &ends, boundary);
            candidate.boundary.lineage_id = Some(lineage.clone());
            match case {
                0 => candidate.boundary.frame_identity[0] ^= 1,
                1 => candidate.boundary.qualified = false,
                2 => candidate.window.as_mut().unwrap().base += 1,
                3 => candidate.prefix.pop().map(|_| ()).unwrap(),
                4 => candidate.window.as_mut().unwrap().guidance.clear(),
                5 => {
                    candidate.window.as_mut().unwrap().guidance[0]
                        .fold
                        .rules_version += 1
                }
                6 => candidate.window.as_mut().unwrap().guidance[0].at_seq -= 1,
                _ => candidate.window.as_mut().unwrap().episodes.clear(),
            }
            let error = reconstruct_from_offsets(&reader(&bytes), candidate, records.len() as u64)
                .err()
                .expect("unproven windows must fall back");
            if case == 5 {
                assert_eq!(
                    error.reason,
                    "offset prompt window repetition rules changed"
                );
            }
            if case == 4 {
                assert_eq!(
                    error.reason,
                    "offset prompt window lacks repetition checkpoint"
                );
            }
        }
    }

    #[test]
    fn offset_resume_window_preserves_episode_boundaries_in_the_tail() {
        let mut records = compacted_tools(1, true);
        let boundary = records
            .iter()
            .position(|r| matches!(r, WalRecord::TurnFinished { .. }))
            .unwrap();
        records.extend([
            finish(),
            start("after-view"),
            WalRecord::StepStarted { step_id: StepId(1) },
            step(1),
        ]);
        for cut in records.len() - 3..=records.len() {
            let (bytes, positions, ends, lineage) =
                file_window(&records[..cut], &format!("episode-{cut}"));
            let mut candidate = window_snapshot(&bytes, &positions, &ends, boundary);
            candidate.boundary.lineage_id = Some(lineage);
            let loaded = reconstruct_from_offsets(&reader(&bytes), candidate, cut as u64).unwrap();
            let full = crate::replay::replay(
                &positions
                    .iter()
                    .map(|p| p.framed.record.clone())
                    .collect::<Vec<_>>(),
            )
            .unwrap();
            assert_eq!(
                crate::canonical_request_bytes(&rendered_window(&loaded.prior, false)),
                crate::canonical_request_bytes(&rendered_window(&full, true))
            );
            assert_eq!(loaded.prior.tags_await_setup, full.tags_await_setup);
            assert_eq!(loaded.prior.completed_steps, full.completed_steps);
        }
    }

    #[test]
    #[ignore = "explicit resume-cost measurement at the campaign's two history sizes"]
    fn offset_resume_cost_at_1k_and_150k_messages() {
        for n in [1_000, 150_000] {
            let mut records = corpus();
            if let WalRecord::RunStarted { input, .. } = &mut records[0] {
                *input = (0..n - 2)
                    .map(|i| {
                        Message::new(
                            Role::User,
                            vec![ContentBlock::Text {
                                text: format!("history {i}"),
                            }],
                        )
                    })
                    .collect::<Vec<_>>()
                    .into();
            }
            let (bytes, positions, ends) = encode(&records[..6]);
            let mut candidate = snapshot(&bytes, &positions, &ends, 2);
            let start = std::time::Instant::now();
            // Include request-source decoding, not just concatenation of an already hydrated
            // vector. This fixture's history inputs share a single RunStarted source frame.
            let broca_wal::framing::DecodeStep::Record { framed, .. } =
                framing::decode_next(&bytes[..ends[0] as usize])
            else {
                panic!("history source decodes")
            };
            let WalRecord::RunStarted { input, .. } = framed.record else {
                panic!("history source is RunStarted")
            };
            let mut prefix = input.to_vec();
            for (ord, message) in prefix.iter_mut().enumerate() {
                message.ordinal = Some(ord as u64);
            }
            prefix.push(candidate.prefix.last().unwrap().clone());
            candidate.prefix = prefix;
            let loaded = reconstruct_from_offsets(&reader(&bytes), candidate, 6).unwrap();
            let elapsed = start.elapsed();
            assert_eq!(loaded.prior.prompt.len(), n);
            eprintln!("offset resume {n} messages: {:.2} ms (request-source decode + composition; index construction excluded; historical baseline {:.2} ms)", elapsed.as_secs_f64() * 1000.0, if n == 1000 { 5.24 } else { 384.58 });
        }
    }
}

/// The text of the result a resume writes for an indeterminate call. It says plainly that no
/// result was recorded, because that is all the runner knows: the call may have run.
pub const OUTCOME_UNKNOWN_TEXT: &str = "outcome_unknown: this tool call was sent, but no result \
     was recorded before the run stopped, so it may or may not have run. It will not be sent \
     again.";

/// The text of the result a resume writes for a planned call of the same batch that never left
/// the process. It definitely did not run.
pub const NOT_DISPATCHED_TEXT: &str =
    "tool not run: the run was interrupted before this call was dispatched";

/// The result a resume records for an indeterminate call: an error whose text says the call has
/// no recorded result. Written with outcome [`DispatchOutcome::Unknown`], which every reader
/// already treats as indeterminate, so the call stays marked as possibly run.
pub fn outcome_unknown_result(tool_call_id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: ToolOutput::error(OUTCOME_UNKNOWN_TEXT),
        is_error: true,
    }
}

/// The result for a planned call that never left the process because the run stopped first. A
/// resume writes it for such a call of an open batch, and a run asked to stop writes it for each
/// call it no longer sends. Recorded with outcome [`DispatchOutcome::Interrupted`]: the call
/// definitely did not run and is never sent later.
pub fn not_dispatched_result(tool_call_id: &ToolCallId) -> ToolResult {
    ToolResult {
        tool_call_id: tool_call_id.clone(),
        output: ToolOutput::error(NOT_DISPATCHED_TEXT),
        is_error: true,
    }
}

/// A provider-held call has a known owner, so crash close-out must not claim an unknown outcome.
pub fn provider_held_interrupted_result(tool_call_id: &ToolCallId) -> ToolResult {
    provider_held_close_result(tool_call_id, broca_protocol::RunFinishReason::Interrupted).0
}

/// Close a provider's known obligation without pretending that its outcome is unknown.
pub fn provider_held_close_result(
    tool_call_id: &ToolCallId,
    reason: broca_protocol::RunFinishReason,
) -> (ToolResult, DispatchOutcome, &'static str) {
    let (code, text, outcome) = match reason {
        broca_protocol::RunFinishReason::Cancelled => (
            "provider_held_cancelled",
            "cancelled",
            DispatchOutcome::Interrupted,
        ),
        _ => (
            "provider_held_interrupted",
            "interrupted",
            DispatchOutcome::Interrupted,
        ),
    };
    (
        ToolResult {
            tool_call_id: tool_call_id.clone(),
            output: ToolOutput::error(format!(
                "{code}: the run was {text} while the provider held the call"
            )),
            is_error: true,
        },
        outcome,
        code,
    )
}

/// Closing is episode-wide, even after the last held call has its terminal. Only outcomes,
/// not provider-controlled result text, identify a close-out. Ignore earlier episodes.
pub fn provider_held_closing_reason<'a>(
    prior: &ReplayState,
    records: impl DoubleEndedIterator<Item = &'a WalRecord>,
) -> Option<broca_protocol::RunFinishReason> {
    use broca_protocol::RunFinishReason;
    let episode: Vec<_> = records
        .rev()
        .take_while(|record| !matches!(record, WalRecord::RunStarted { .. }))
        .collect();
    let held: Vec<_> = episode
        .iter()
        .filter_map(|record| match record {
            WalRecord::ToolCallProviderHeld { tool_call_id, .. } => Some(tool_call_id),
            _ => None,
        })
        .collect();
    if held.is_empty() || prior.run_finished_durable {
        return None;
    }
    if prior.cancel_at.is_some() {
        return Some(RunFinishReason::Cancelled);
    }
    episode.iter().find_map(|record| match record {
        WalRecord::ToolResult {
            result, outcome, ..
        } if held.contains(&&result.tool_call_id)
            && matches!(
                outcome,
                DispatchOutcome::Interrupted | DispatchOutcome::MaxSteps
            ) =>
        {
            Some(*outcome)
        }
        _ => None,
    })?;
    Some(RunFinishReason::Interrupted)
}

/// Provider obligations in the last episode that have not delivered a result. Include durable
/// close-out terminals so a crash before the seal does not lose their best-effort withdrawal.
pub fn closing_held_replies<'a>(
    records: impl DoubleEndedIterator<Item = &'a WalRecord>,
) -> Vec<crate::tool_plane::ProviderHeldReply> {
    let episode: Vec<_> = records
        .rev()
        .take_while(|record| !matches!(record, WalRecord::RunStarted { .. }))
        .collect();
    episode.iter().filter_map(|record| match record {
        WalRecord::ToolCallProviderHeld { tool_call_id, call_key, module, reason, detail_digest, .. }
            if !episode.iter().any(|record| matches!(record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Completed, .. } if result.tool_call_id == *tool_call_id)) => {
            Some(crate::tool_plane::ProviderHeldReply { tool_call_id: tool_call_id.clone(), call_key: call_key.clone(), module: module.clone(), reason: reason.clone(), detail_digest: detail_digest.clone() })
        },
        _ => None,
    }).collect()
}

/// Spawn only after syncing the seal; a failed or hanging withdrawal cannot postpone it.
pub fn withdraw_provider_held(
    lifecycle: Option<std::sync::Arc<dyn crate::tool_plane::ProviderHeldLifecycle>>,
    calls: Vec<crate::tool_plane::ProviderHeldReply>,
) {
    let Some(lifecycle) = lifecycle else {
        return;
    };
    for call in calls {
        let lifecycle = lifecycle.clone();
        tokio::spawn(async move {
            match tokio::time::timeout(
                std::time::Duration::from_secs(120),
                lifecycle.withdraw_held(&call),
            )
            .await
            {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    tracing::warn!(call_key = call.call_key, %error, "provider-held withdrawal failed")
                }
                Err(_) => tracing::warn!(
                    call_key = call.call_key,
                    "provider-held withdrawal timed out"
                ),
            }
        });
    }
}

/// The resume record for the last episode of `prior`, resumed from the record at
/// `resumed_from_seq`, about to redo `redone`.
///
/// The indeterminate list is replay's own: every call with an intent and no result, and every
/// call already carrying an `Unknown` result, including one a crashed earlier resume wrote.
pub fn resume_started_record(
    prior: &ReplayState,
    resumed_from_seq: u64,
    redone: Vec<ResumeRedo>,
) -> WalRecord {
    let indeterminate = match &prior.resolution {
        ReplayResolution::Interrupted { indeterminate } => indeterminate.clone(),
        ReplayResolution::LineageIdle { .. }
        | ReplayResolution::Resume { .. }
        | ReplayResolution::AuthRequired { .. }
        | ReplayResolution::ProviderHeld { .. } => Vec::new(),
    };
    let tool_results = prior.open_tool_batch.as_ref().map_or(0, |batch| {
        batch
            .calls
            .iter()
            .filter(|(_, state)| *state == OpenCallState::Terminated)
            .count() as u64
    });
    WalRecord::ResumeStarted {
        episode: prior
            .run_id
            .clone()
            .unwrap_or_else(|| broca_protocol::RunId(String::new())),
        resumed_from_seq,
        replayed: ResumeReplayed {
            // A held resume loads only part of the transcript; the record still reports what
            // the resume reconstructed, the whole transcript at the store prefix and WAL tail.
            messages: prior
                .held
                .as_ref()
                .map_or(prior.prompt.len() as u64, |held| held.replayed_messages),
            model_steps: prior.completed_steps,
            tool_results,
        },
        redone,
        indeterminate,
    }
}

/// What a resume that continues the episode will do again, read from replay's resolution: the
/// planned calls it dispatches because they never left the process, or, with no tool turn
/// pending, the model step it requests again because none of its output was durable. Empty for
/// every resolution the resume seals instead of continuing.
pub fn planned_redo(prior: &ReplayState) -> Vec<ResumeRedo> {
    match &prior.resolution {
        ReplayResolution::Resume {
            pending: Some(pending),
        } => pending
            .redispatch
            .iter()
            .map(|tool_call_id| ResumeRedo::ToolDispatch {
                tool_call_id: tool_call_id.clone(),
            })
            .collect(),
        ReplayResolution::Resume { pending: None } => vec![ResumeRedo::ModelStep {
            step_id: broca_protocol::StepId(prior.completed_steps + 1),
        }],
        ReplayResolution::LineageIdle { .. }
        | ReplayResolution::Interrupted { .. }
        | ReplayResolution::AuthRequired { .. }
        | ReplayResolution::ProviderHeld { .. } => Vec::new(),
    }
}

/// How a sealing caller closes an unanswered Prepared call. Reader builds choose the ungated
/// not-dispatched result; writer builds supply the question's durable filing identity.
pub enum DeferredCloseKind {
    NotDispatched,
    WithdrawElicitation(WithdrawElicitation),
}

/// Refuse a reader resume before it can drop a Prepared batch and enter a model step.
pub fn refuse_prepared_resume(
    pending: Option<&crate::replay::PendingTurn>,
) -> Result<(), crate::run::RunError> {
    if pending.is_some_and(|pending| !pending.deferred.is_empty() || !pending.approved.is_empty()) {
        return Err(prepared_resume_refusal());
    }
    Ok(())
}

pub(crate) fn prepared_resume_refusal() -> crate::run::RunError {
    crate::run::RunError::Config(
        "deferred_call_open: this reader cannot resume a Prepared approval call".into(),
    )
}

/// The records that close an indeterminate batch, or any batch retaining a Deferred/Approved
/// call, regardless of the resolution: one terminal per unfinished call, then commit markers.
/// Reader close-out cannot withdraw pending approval elicitations; a writer must still do so.
/// The generated records leave observation times unset until enqueued by the WAL writer,
/// rather than reconstructing them from an earlier dispatch.
///
/// A call that already has a terminal gets nothing, so a resume after a crash inside an earlier
/// resume's close-out writes only what that close-out did not make durable, and never a second
/// result for one call.
pub fn close_out_open_batch(
    prior: &ReplayState,
    mut deferred_close: impl FnMut(&ToolCallId) -> DeferredCloseKind,
) -> Vec<WalRecord> {
    let Some(batch) = &prior.open_tool_batch else {
        return Vec::new();
    };
    if !matches!(prior.resolution, ReplayResolution::Interrupted { .. })
        && !batch.calls.iter().any(|(_, state)| {
            matches!(
                state,
                OpenCallState::Deferred
                    | OpenCallState::Approved
                    | OpenCallState::ProviderHeld
                    | OpenCallState::HeldSuccessor
            )
        })
    {
        return Vec::new();
    }
    let reason = if prior.cancel_at.is_some() {
        broca_protocol::RunFinishReason::Cancelled
    } else {
        broca_protocol::RunFinishReason::Interrupted
    };
    let mut records = Vec::with_capacity(batch.calls.len() + 2);
    for (tool_call_id, state) in &batch.calls {
        let withdraw_elicitation = if *state == OpenCallState::Deferred {
            match deferred_close(tool_call_id) {
                DeferredCloseKind::NotDispatched => None,
                DeferredCloseKind::WithdrawElicitation(withdrawal) => Some(withdrawal),
            }
        } else {
            None
        };
        let (result, outcome) = match state {
            OpenCallState::Terminated => continue,
            OpenCallState::Dispatched => (
                outcome_unknown_result(tool_call_id),
                DispatchOutcome::Unknown,
            ),
            OpenCallState::ProviderHeld
                if matches!(reason, broca_protocol::RunFinishReason::Cancelled) =>
            {
                let (result, outcome, _) = provider_held_close_result(tool_call_id, reason);
                (result, outcome)
            }
            OpenCallState::ProviderHeld => (
                provider_held_interrupted_result(tool_call_id),
                DispatchOutcome::Interrupted,
            ),
            OpenCallState::NotSent
            | OpenCallState::Deferred
            | OpenCallState::Approved
            | OpenCallState::HeldSuccessor => (
                not_dispatched_result(tool_call_id),
                DispatchOutcome::Interrupted,
            ),
        };
        records.push(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: batch.batch_id.clone(),
            result: Arc::new(result),
            outcome,
            reason: (*state == OpenCallState::ProviderHeld)
                .then(|| provider_held_close_result(tool_call_id, reason).2.into()),
            withdraw_elicitation,
            provider_held: None,
        });
    }
    records.push(WalRecord::ToolBatchFinished {
        batch_id: batch.batch_id.clone(),
    });
    records.push(WalRecord::TurnFinished {
        step_id: batch.step_id,
    });
    records
}

/// Close a known closing episode even when every held terminal survived but the batch markers
/// or seal did not. Only the batch is copied: close-out needs no reconstructed conversation.
pub fn close_out_open_batch_for(
    prior: &ReplayState,
    reason: broca_protocol::RunFinishReason,
    deferred_close: impl FnMut(&ToolCallId) -> DeferredCloseKind,
) -> Vec<WalRecord> {
    let mut closing = crate::replay::replay(&[]).expect("empty WAL has a valid replay");
    closing.open_tool_batch = prior.open_tool_batch.clone();
    closing.resolution = ReplayResolution::Interrupted {
        indeterminate: Vec::new(),
    };
    closing.cancel_at = (reason == broca_protocol::RunFinishReason::Cancelled)
        .then_some(broca_wal::record::RunCancelAt::Now);
    close_out_open_batch(&closing, deferred_close)
}

/// The records that keep a tool turn whose remaining calls were never dispatched, written when a
/// caller seals the episode `Interrupted` instead of resuming it.
///
/// Replay holds a model step that asked for tools out of the conversation until its batch
/// commits. A seal that wrote only `RunFinished` would therefore drop that already-written
/// assistant message from history, and dropping or rewriting a written message would need a new
/// lineage. Instead the turn is kept and completed: the batch is opened if it never was, each
/// planned call without a terminal gets [`not_dispatched_result`] with outcome
/// [`DispatchOutcome::Interrupted`], and the batch's commit markers follow. These are the records
/// a hard stop writes for calls it never dispatched: a definite "never ran" outcome, never
/// `Unknown`, so no reader reports the call as indeterminate and no later run sends it.
///
/// The caller must write these records and its terminal behind ONE durability barrier. A batch
/// committed with only synthetic results and no terminal reads to replay as a finished run, so a
/// crash between two barriers would end the episode with the wrong reason.
///
/// Empty unless replay would resume the turn by dispatching its calls (`Resume`, or a pause with
/// a pending turn). An indeterminate batch, or one holding a Prepared call, is closed by
/// [`close_out_open_batch`] instead.
pub fn close_out_pending_turn(prior: &ReplayState) -> Vec<WalRecord> {
    let pending = match &prior.resolution {
        ReplayResolution::Resume {
            pending: Some(pending),
        }
        | ReplayResolution::AuthRequired {
            pending: Some(pending),
            ..
        } => pending,
        ReplayResolution::ProviderHeld { .. }
        | ReplayResolution::Interrupted { .. }
        | ReplayResolution::LineageIdle { .. }
        | ReplayResolution::Resume { pending: None }
        | ReplayResolution::AuthRequired { pending: None, .. } => return Vec::new(),
    };
    if !pending.deferred.is_empty() || !pending.approved.is_empty() {
        return Vec::new();
    }
    let mut records = Vec::with_capacity(pending.planned.len() + 3);
    let batch_id = match &pending.batch_id {
        Some(batch_id) => batch_id.clone(),
        None => {
            // The id the run loop would have given this step's batch, so the kept turn reads
            // the same as one the loop committed itself.
            let run_id = prior.run_id.as_ref().map_or("", |run_id| run_id.0.as_str());
            let batch_id = broca_protocol::BatchId(format!("{run_id}-b{}", pending.step_id.0));
            records.push(WalRecord::ToolBatchStarted {
                batch_id: batch_id.clone(),
                step_id: pending.step_id,
                planned_calls: pending.planned.clone(),
            });
            batch_id
        }
    };
    for tool_call_id in &pending.planned {
        if pending.existing.iter().any(|(id, _)| id == tool_call_id) {
            continue;
        }
        records.push(WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            hooks: None,
            batch_id: batch_id.clone(),
            result: Arc::new(not_dispatched_result(tool_call_id)),
            outcome: DispatchOutcome::Interrupted,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        });
    }
    records.push(WalRecord::ToolBatchFinished { batch_id });
    records.push(WalRecord::TurnFinished {
        step_id: pending.step_id,
    });
    records
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::{
        deferred_tests::{approval, batch, finished, prepared, terminal},
        replay,
    };
    use broca_protocol::{BatchId, PauseReason, RunFinishReason, RunId};
    use broca_wal::DeferredDecision;

    #[test]
    fn seal_closes_prepared_calls_whatever_the_resolution() {
        let mut records = batch(&["deferred", "approved", "fresh", "done"]);
        records.extend([
            prepared("deferred", "b"),
            prepared("approved", "b"),
            approval("approved", "key-approved", DeferredDecision::Approved),
            terminal("done", None),
        ]);
        let prior = replay(&records).unwrap();
        let resolutions = [
            prior.resolution.clone(),
            ReplayResolution::Interrupted {
                indeterminate: vec![],
            },
            ReplayResolution::AuthRequired {
                run_id: RunId("r".into()),
                reason: PauseReason::Restart,
                error: None,
                pending: match &prior.resolution {
                    ReplayResolution::Resume { pending } => pending.clone(),
                    _ => unreachable!(),
                },
            },
            ReplayResolution::LineageIdle {
                last_reason: RunFinishReason::Interrupted,
            },
        ];
        for resolution in resolutions {
            let mut prior = prior.clone();
            prior.resolution = resolution;
            let close = close_out_open_batch(&prior, |_| DeferredCloseKind::NotDispatched);
            assert_eq!(close.len(), 5);
            for (record, id) in close.iter().take(3).zip(["deferred", "approved", "fresh"]) {
                let WalRecord::ToolResult {
                    result,
                    outcome,
                    withdraw_elicitation,
                    ..
                } = record
                else {
                    panic!("missing terminal")
                };
                assert_eq!(
                    **result,
                    not_dispatched_result(&broca_protocol::ToolCallId(id.into()))
                );
                assert_eq!(*outcome, DispatchOutcome::Interrupted);
                assert!(withdraw_elicitation.is_none());
                assert!(
                    record.required_features().is_empty(),
                    "reader close-out must stay ungated"
                );
            }
            assert!(
                matches!(&close[3], WalRecord::ToolBatchFinished { batch_id } if batch_id == &BatchId("b".into()))
            );
            assert!(
                matches!(&close[4], WalRecord::TurnFinished { step_id } if *step_id == broca_protocol::StepId(1))
            );
            let mut closed = records.clone();
            closed.extend(close);
            assert!(replay(&closed).unwrap().open_tool_batch.is_none());
        }
    }

    #[test]
    fn writer_close_kind_only_withdraws_deferred_calls_and_keeps_unknown_siblings() {
        let mut records = batch(&["deferred", "approved", "dispatched"]);
        records.extend([
            prepared("deferred", "b"),
            prepared("approved", "b"),
            approval("approved", "key-approved", DeferredDecision::Approved),
        ]);
        let mut prior = replay(&records).unwrap();
        prior.open_tool_batch.as_mut().unwrap().calls[2].1 = OpenCallState::Dispatched;
        prior.resolution = ReplayResolution::Interrupted {
            indeterminate: vec![ToolCallId("dispatched".into())],
        };
        for elicitation_id in [None, Some("question".into())] {
            let close = close_out_open_batch(&prior, |id| {
                assert_eq!(id.0, "deferred");
                DeferredCloseKind::WithdrawElicitation(WithdrawElicitation {
                    call_key: "key-deferred".into(),
                    elicitation_id: elicitation_id.clone(),
                })
            });
            assert!(
                matches!(&close[0], WalRecord::ToolResult { withdraw_elicitation: Some(w), .. } if w.elicitation_id == elicitation_id)
            );
            assert!(matches!(
                &close[1],
                WalRecord::ToolResult {
                    withdraw_elicitation: None,
                    outcome: DispatchOutcome::Interrupted,
                    ..
                }
            ));
            assert!(
                matches!(&close[2], WalRecord::ToolResult { withdraw_elicitation: None, outcome: DispatchOutcome::Unknown, result, .. }
                if **result == outcome_unknown_result(&broca_protocol::ToolCallId("dispatched".into())))
            );
        }
    }

    #[test]
    fn ordinary_resume_without_prepared_calls_does_not_close_batch() {
        let prior = replay(&batch(&["fresh"])).unwrap();
        assert!(close_out_open_batch(&prior, |_| panic!("not a deferred call")).is_empty());
        assert!(refuse_prepared_resume(None).is_ok());
        if let ReplayResolution::Resume { pending } = &prior.resolution {
            assert!(refuse_prepared_resume(pending.as_ref()).is_ok());
        }
    }

    #[test]
    fn provider_held_close_out_survives_every_terminal_prefix() {
        use crate::replay::provider_held_tests::{held, intent};
        for sequential in [false, true] {
            for cancel in [false, true] {
                let mut records = batch(&["a", "b"]);
                records.extend([intent("a"), intent("b"), held(sequential)]);
                if !sequential {
                    let mut second = held(false);
                    if let WalRecord::ToolCallProviderHeld {
                        tool_call_id,
                        call_key,
                        ..
                    } = &mut second
                    {
                        *tool_call_id = ToolCallId("b".into());
                        *call_key = "lineage:5".into();
                    }
                    records.push(second);
                }
                if cancel {
                    records.push(WalRecord::RunCancelRequested {
                        run_id: broca_protocol::RunId("r".into()),
                        at: broca_wal::record::RunCancelAt::StepBoundary,
                    });
                }
                let reason = if cancel {
                    RunFinishReason::Cancelled
                } else {
                    RunFinishReason::Interrupted
                };
                let close = close_out_open_batch_for(&replay(&records).unwrap(), reason, |_| {
                    DeferredCloseKind::NotDispatched
                });
                for cut in 1..=close.len() {
                    let mut cut_records = records.clone();
                    cut_records.extend_from_slice(&close[..cut]);
                    let prior = replay(&cut_records).unwrap();
                    assert_eq!(
                        provider_held_closing_reason(&prior, cut_records.iter()),
                        Some(reason),
                        "closed held terminals mark the episode even without an open held sibling"
                    );
                    let repair = close_out_open_batch_for(&prior, reason, |_| {
                        DeferredCloseKind::NotDispatched
                    });
                    cut_records.extend(repair);
                    cut_records.push(WalRecord::RunFinished {
                        ts_ms: None,
                        interrupt_reason: None,
                        reason: if cancel {
                            RunFinishReason::Cancelled
                        } else {
                            RunFinishReason::Interrupted
                        },
                        usage: prior.usage,
                        retries_used: 0,
                        error: None,
                    });
                    for id in ["a", "b"] {
                        let results: Vec<_> = cut_records
                            .iter()
                            .filter_map(|record| match record {
                                WalRecord::ToolResult {
                                    result,
                                    outcome,
                                    reason,
                                    ..
                                } if result.tool_call_id.0 == id => Some((result, outcome, reason)),
                                _ => None,
                            })
                            .collect();
                        assert_eq!(
                            results.len(),
                            1,
                            "every planned call gets exactly one terminal"
                        );
                        let (result, outcome, code) = results[0];
                        if sequential && id == "b" {
                            assert_eq!(**result, not_dispatched_result(&ToolCallId("b".into())));
                            assert_eq!(
                                *outcome,
                                DispatchOutcome::Interrupted,
                                "closed-out held successors are never Unknown"
                            );
                        } else {
                            let expected_code = if cancel {
                                "provider_held_cancelled"
                            } else {
                                "provider_held_interrupted"
                            };
                            assert_eq!(code.as_deref(), Some(expected_code));
                            assert_eq!(*outcome, DispatchOutcome::Interrupted);
                            assert_eq!(result.output, ToolOutput::error(format!("{expected_code}: the run was {} while the provider held the call", if cancel { "cancelled" } else { "interrupted" })));
                        }
                    }
                    let repaired = replay(&cut_records).unwrap();
                    assert!(repaired.open_tool_batch.is_none());
                    assert_eq!(
                        provider_held_closing_reason(&repaired, cut_records.iter()),
                        None
                    );
                    assert!(repaired.prompt.iter().any(|message| message
                        .content
                        .iter()
                        .any(|block| matches!(block, broca_protocol::ContentBlock::ToolCall(_)))));
                }
            }
        }
    }

    #[test]
    fn provider_held_closing_precedence_uses_outcomes_not_reasons() {
        use crate::replay::provider_held_tests::{prefix, terminal};
        let mut records = prefix(false);
        let mut stopped = terminal("a", DispatchOutcome::MaxSteps);
        if let WalRecord::ToolResult { reason, .. } = &mut stopped {
            *reason = Some("unrelated text".into());
        }
        records.push(stopped);
        let prior = replay(&records).unwrap();
        assert_eq!(
            provider_held_closing_reason(&prior, records.iter()),
            Some(RunFinishReason::Interrupted)
        );
        records.push(WalRecord::RunCancelRequested {
            run_id: broca_protocol::RunId("r".into()),
            at: broca_wal::record::RunCancelAt::Now,
        });
        assert_eq!(
            provider_held_closing_reason(&replay(&records).unwrap(), records.iter()),
            Some(RunFinishReason::Cancelled)
        );
        records.retain(|record| {
            !matches!(
                record,
                WalRecord::RunCancelRequested { .. } | WalRecord::ToolResult { .. }
            )
        });
        let mut delivered = terminal("a", DispatchOutcome::Completed);
        if let WalRecord::ToolResult { reason, .. } = &mut delivered {
            *reason = Some("provider_held_stopped".into());
        }
        records.push(delivered);
        assert_eq!(
            provider_held_closing_reason(&replay(&records).unwrap(), records.iter()),
            None
        );
    }

    /// A sealed tool turn keeps every written message: the step whose batch never opened gets a
    /// batch, each call a never-executed result, and replay then commits the turn to the prompt.
    #[test]
    fn pending_turn_close_out_keeps_a_step_whose_batch_never_opened() {
        let mut records = batch(&["first", "second"]);
        records.pop(); // the step is durable, its ToolBatchStarted is not
        let prior = replay(&records).unwrap();
        let close = close_out_pending_turn(&prior);
        let planned = vec![ToolCallId("first".into()), ToolCallId("second".into())];
        assert!(
            matches!(&close[0], WalRecord::ToolBatchStarted { batch_id, step_id, planned_calls }
                if batch_id == &BatchId("r-b1".into())
                    && *step_id == broca_protocol::StepId(1)
                    && planned_calls == &planned),
            "{close:#?}"
        );
        for (record, id) in close[1..3].iter().zip(&planned) {
            assert!(
                matches!(record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Interrupted, batch_id, .. }
                    if **result == not_dispatched_result(id) && batch_id == &BatchId("r-b1".into())),
                "{record:#?}"
            );
        }
        assert!(matches!(&close[3], WalRecord::ToolBatchFinished { .. }));
        assert!(matches!(&close[4], WalRecord::TurnFinished { .. }));
        assert_eq!(close.len(), 5);
        let mut closed = records.clone();
        closed.extend(close);
        let after = replay(&closed).unwrap();
        assert!(after.open_tool_batch.is_none());
        assert_eq!(
            after.prompt.len(),
            prior.prompt.len() + 2,
            "the step and its results join the transcript"
        );
        assert_eq!(after.prompt[..prior.prompt.len()], prior.prompt[..]);
    }

    /// A batch that already holds a result keeps it: only the calls without one are closed, in
    /// the durable batch, and no second batch is opened.
    #[test]
    fn pending_turn_close_out_reuses_an_open_batch_and_keeps_its_results() {
        let mut records = batch(&["done", "fresh"]);
        records.push(terminal("done", None));
        let prior = replay(&records).unwrap();
        let close = close_out_pending_turn(&prior);
        assert_eq!(close.len(), 3, "{close:#?}");
        assert!(
            matches!(&close[0], WalRecord::ToolResult { result, batch_id, .. }
                if **result == not_dispatched_result(&ToolCallId("fresh".into()))
                    && batch_id == &BatchId("b".into()))
        );
        let mut closed = records.clone();
        closed.extend(close);
        assert!(replay(&closed).unwrap().open_tool_batch.is_none());
    }

    /// Only a turn replay would resume by dispatching is closed here: nothing pending, an
    /// indeterminate batch and a Prepared call are left to their own paths.
    #[test]
    fn pending_turn_close_out_leaves_other_resolutions_alone() {
        let mut idle = batch(&["done"]);
        idle.push(terminal("done", None));
        idle.extend(finished());
        assert!(close_out_pending_turn(&replay(&idle).unwrap()).is_empty());
        let mut deferred = batch(&["deferred"]);
        deferred.push(prepared("deferred", "b"));
        assert!(close_out_pending_turn(&replay(&deferred).unwrap()).is_empty());
        let mut prior = replay(&batch(&["sent"])).unwrap();
        prior.resolution = ReplayResolution::Interrupted {
            indeterminate: vec![ToolCallId("sent".into())],
        };
        assert!(close_out_pending_turn(&prior).is_empty());
    }

    #[tokio::test]
    async fn reader_core_resume_refuses_restart_paused_prepared_batch() {
        use crate::{
            AllowAll, FileLeaseStore, FileWalStore, RunRequest, StubProvider, StubToolPlane,
            WalStore,
        };
        use broca_protocol::{
            request::{GenerationConfig, ModelHandle, ToolChoice},
            BindIdentity,
        };
        use std::io::Write;

        for approved in [false, true] {
            let root = broca_test_support::scratch_root().join(format!(
                "broca-reader-refusal-{}-{approved}",
                std::process::id()
            ));
            std::fs::create_dir_all(root.join("project")).unwrap();
            let session = BindIdentity::new(root.join("project"), "runner", "approval");
            let store = FileWalStore::new(root.join("wal"));
            let mut records = batch(&["call"]);
            if let WalRecord::RunStarted {
                session: stored_session,
                ..
            } = &mut records[0]
            {
                *stored_session = session.clone();
            }
            records.push(prepared("call", "b"));
            if approved {
                records.push(approval("call", "key-call", DeferredDecision::Approved));
            }
            records.push(WalRecord::RunPaused {
                run_id: RunId("r".into()),
                ts_ms: Some(1),
                reason: PauseReason::Restart,
                error: None,
            });
            let mut wal = store.open(&session).unwrap();
            wal.append(1, &records[0], true).unwrap();
            drop(wal);
            // Exercise the reader after decoding; feature placement is separately blocked until
            // every reader slice lands, so these payload frames bypass only the feature table.
            let mut file = std::fs::OpenOptions::new()
                .append(true)
                .open(store.wal_path(&session))
                .unwrap();
            for (index, record) in records.iter().enumerate().skip(1) {
                file.write_all(
                    &broca_wal::framing::encode_payload(
                        (index + 1) as u64,
                        1,
                        &serde_json::to_vec(record).unwrap(),
                    )
                    .unwrap(),
                )
                .unwrap();
            }
            file.sync_all().unwrap();
            drop(file);
            let before = std::fs::read(store.wal_path(&session)).unwrap();
            let request = RunRequest {
                input_written_at_ms: None,
                directives: None,
                session_class: None,
                retention: None,
                title: None,
                on_restart: None,
                keep_warm: None,
                scope: None,
                send_id: None,
                request_identity_digest: None,
                project_id: None,
                principal: None,
                run_id: RunId("r".into()),
                session: session.clone(),
                model: ModelHandle::new("stub", "m"),
                input: vec![],
                tools: vec![],
                policy_allowed: Default::default(),
                tool_choice: ToolChoice::Auto,
                generation: GenerationConfig::default(),
                cache: None,
                stop_when: vec![],
                submission_id: None,
                budget: None,
                context_limit: None,
                work_class: None,
                context_management: None,
                variant: None,
                service_tier: None,
                inherit_frozen_cache: false,
                structured_output: None,
            };
            let provider = StubProvider::new(vec![]);
            let plane = StubToolPlane::new()
                .with_tool("edit", |_| panic!("reader dispatched a Prepared call"));
            let lease = FileLeaseStore::new(root.join("lease"));
            let (out, _events) = tokio::sync::mpsc::channel(64);
            let error =
                crate::run::run_loop(request, &provider, &plane, &lease, &AllowAll, &store, &out)
                    .await
                    .unwrap_err();
            assert!(
                matches!(error, crate::run::RunError::Config(ref reason) if reason.starts_with("deferred_call_open:")),
                "unexpected refusal: {error:?}"
            );
            assert_eq!(
                std::fs::read(store.wal_path(&session)).unwrap(),
                before,
                "refusal writes no gated records or model steps"
            );
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
