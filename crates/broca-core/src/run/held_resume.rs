//! Resuming a session under an applied compaction view from the messages the run holds.
//!
//! A run under an applied view holds its system messages, the view's keep ranges and every
//! message from the view's coverage end on (see [`super::held_prompt::held_by`]). A resume reads
//! exactly those rows from the materialized store, the projected metadata of the episodes they
//! belong to (plus the newest one, which is resumed), and those episodes' model-step rows to
//! rebuild their repetition guidance. Everything past the store's watermark comes from the WAL
//! tail admission already read, merged once. No other stored row is read, so the cost does not
//! grow with the discarded history.
//!
//! The answer is all or nothing: when the store cannot supply something completely (a gap, a
//! watermark that moved, an uncomputed projection value), the caller replays the whole WAL
//! instead, which renders the same bytes.

use std::sync::Arc;

use broca_protocol::{BindIdentity, Message};
use broca_wal::{FramedRecord, WalRecord};

use super::held_prompt::{held_by, GuidanceEpisode, HeldResume};
use super::repetition_guidance::GuidanceSource;
use crate::compaction_provider::view::{CompactionView, ViewRange};
use crate::context::{HeldRowsRequest, PrefixSeed, SessionStore};
use crate::tool_repetition::RepetitionObserver;
use crate::ReplayState;

/// How many stored model-step rows one guidance page reads.
const GUIDANCE_PAGE_ROWS: u64 = 256;

/// What a held resume could do with the store.
pub(super) enum HeldLoad {
    /// The resumed state, holding only the view's held set.
    Prior(Box<ReplayState>),
    /// The store keeps no held read or no materialized prefix: merge the complete prefix as
    /// before (cheap, since there is nothing materialized to skip).
    Merge,
    /// The store cannot answer completely; replay the whole WAL. The reason is logged.
    Replay(String),
}

/// Everything a held resume reads besides the store, captured by admission.
pub(super) struct HeldInput {
    pub store: Arc<dyn SessionStore>,
    pub session: BindIdentity,
    pub view: CompactionView,
    /// The admission snapshot's records past its base watermark.
    pub tail: Arc<Vec<FramedRecord>>,
    pub base_watermark: u64,
    pub head_seq: u64,
    pub writer_lineage: Option<String>,
    pub budgeted: bool,
    pub run_config: Option<crate::RunConfig>,
    pub run_id: Option<crate::RunId>,
    pub budget_state: Option<crate::BudgetReplayStateV1>,
}

/// Load the resumed state from the held rows. Blocking: it reads the store.
pub(super) fn load(input: HeldInput) -> HeldLoad {
    match load_held(&input) {
        Ok(Some(prior)) => HeldLoad::Prior(Box::new(prior)),
        Ok(None) => HeldLoad::Merge,
        Err(reason) => HeldLoad::Replay(reason),
    }
}

/// The rows of `view`'s held set, as the store is asked for them.
pub(super) fn held_rows(view: &CompactionView) -> HeldRowsRequest {
    HeldRowsRequest {
        keep: view
            .ranges
            .iter()
            .filter_map(|range| match range {
                ViewRange::Keep { from, to, .. } => Some(*from..*to),
                _ => None,
            })
            .collect(),
        suffix_from: view.covers_through.ordinal.saturating_add(1),
    }
}

fn load_held(input: &HeldInput) -> Result<Option<ReplayState>, String> {
    let Some(prefix) = input
        .store
        .read_held_prefix(&input.session, &held_rows(&input.view))?
    else {
        return Ok(None);
    };
    if prefix.prefix_len == 0 {
        return Ok(None);
    }
    // The same checks a merge of the complete prefix makes before trusting it.
    let lineage = match (
        prefix.lineage_id.as_deref(),
        input.writer_lineage.as_deref(),
    ) {
        (Some(stored), Some(observed)) if stored == observed => stored.to_owned(),
        _ => return Err("store prefix belongs to another WAL lineage".into()),
    };
    if !(input.base_watermark..=input.head_seq).contains(&prefix.watermark_seq) {
        return Err("store watermark is outside the admission snapshot".into());
    }
    if input.budgeted && prefix.watermark_seq != input.base_watermark {
        return Err("budgeted store prefix moved past the admission snapshot".into());
    }
    let tail: Vec<WalRecord> = input
        .tail
        .iter()
        .filter(|frame| frame.seq > prefix.watermark_seq)
        .map(|frame| frame.record.clone())
        .collect();
    if crate::context::materialized_zero_step_seed_was_interrupted(prefix.completed_steps, &tail) {
        return Err("the materialized seed was abandoned".into());
    }
    let projected = prefix
        .episodes
        .ok_or("store keeps no usable episode projection")?;
    let seed = PrefixSeed {
        prefix_len: prefix.prefix_len,
        completed_steps: prefix.completed_steps,
        usage: prefix.usage,
        retries_used: prefix.retries_used,
        last_step_usage: prefix.last_step_usage,
        last_step_finish_reason: prefix.last_step_finish_reason,
        trailing_origin: prefix.trailing_origin,
        tag_seed: prefix.tag_seed,
    };
    let (mut state, tail_episodes) = crate::context::merge_prefix_and_tail(
        prefix.rows,
        &seed,
        &tail,
        input.run_config.clone(),
        input.run_id.clone(),
        input.budget_state.clone(),
    )
    .map_err(|error| error.to_string())?;
    let extent = state.extent;
    if extent < seed.prefix_len {
        return Err("WAL tail ends before the store's prefix".into());
    }
    let tail_start = state.prompt.len() - tail_episodes.prompt_len;
    // Raw copies for the guidance fold; the held copies are decorated in place.
    let raw_tail: Vec<Message> = state.prompt[tail_start..].to_vec();
    let replayed_messages = seed.prefix_len + raw_tail.len() as u64;
    let tag_high = seed
        .tag_seed
        .high
        .max(broca_protocol::tags::high_after(&state.prompt));

    let episodes = episodes(&projected, &tail_episodes, seed.prefix_len, extent)?;
    // The newest message stays held until the first step boundary releases it, as it does in a
    // run that loaded the whole transcript: that boundary's notice names it.
    let newest = state.prompt.last().and_then(|message| message.ordinal);
    state
        .prompt
        .retain(|message| held_by(&input.view, message) || message.ordinal == newest);
    let active = episodes.len() - 1;
    let guidance: Vec<GuidanceEpisode> = episodes
        .into_iter()
        .enumerate()
        .filter(|(index, episode)| {
            *index == active
                || state.prompt.iter().any(|message| {
                    message.ordinal.is_some_and(|ordinal| {
                        episode.render.prompt_start as u64 <= ordinal && ordinal < episode.end
                    })
                })
        })
        .map(|(_, episode)| episode)
        .collect();

    let source = GuidanceSource {
        store: input.store.as_ref(),
        session: &input.session,
        lineage_id: &lineage,
        watermark_seq: prefix.watermark_seq,
        prefix_len: seed.prefix_len,
        tail: &raw_tail,
        page_size: GUIDANCE_PAGE_ROWS,
    };
    let undecorated = state.prompt.clone();
    let mut observer = RepetitionObserver::new();
    for (index, episode) in guidance.iter().enumerate() {
        let folded =
            source.restore_episode(Some(episode.first_step), episode.end, &mut state.prompt)?;
        if index + 1 == guidance.len() {
            observer = folded;
        }
    }
    let originals = undecorated
        .into_iter()
        .zip(&state.prompt)
        .filter(|(original, shown)| original.content != shown.content)
        .map(|(original, _)| original)
        .collect();
    state.prompt.shrink_to_fit();
    state.message_runs = Vec::new();
    state.episode_step_spans = None;
    state.render_episodes = Some(
        guidance
            .iter()
            .map(|episode| episode.render.clone())
            .collect(),
    );
    state.held = Some(Box::new(HeldResume {
        episodes: guidance,
        observer,
        originals,
        tag_high,
        replayed_messages,
    }));
    Ok(Some(state))
}

/// Every episode the held prompt can belong to, in lineage order: the projected ones the store
/// selected, then those the WAL tail starts. Each carries its first model step (from the
/// projection, or from the tail when the projection's newest episode took its first step there)
/// and its end.
fn episodes(
    projected: &[crate::context::HeldEpisode],
    tail: &crate::context::TailEpisodes,
    prefix_len: u64,
    extent: u64,
) -> Result<Vec<GuidanceEpisode>, String> {
    let tail_first_start = tail
        .render
        .first()
        .map(|episode| episode.prompt_start as u64);
    // Tail spans index the tail's own prompt, which starts at `prefix_len`.
    let tail_steps: Vec<u64> = tail
        .spans
        .iter()
        .map(|span| prefix_len + span.start as u64)
        .collect();
    let mut out = Vec::with_capacity(projected.len() + tail.render.len());
    for (index, held) in projected.iter().enumerate() {
        let newest = index + 1 == projected.len();
        let render = super::episode_render_inputs(std::slice::from_ref(&held.projection))?
            .and_then(|mut episodes| episodes.pop())
            .ok_or("projected episode metadata is incomplete")?;
        let start = render.prompt_start as u64;
        let end = if newest {
            tail_first_start.unwrap_or(extent)
        } else {
            held.next_start
                .ok_or("projected episode has no successor")?
        };
        if start >= end || start >= prefix_len {
            return Err("projected episode lies outside the materialized prefix".into());
        }
        let first_step = match held.projection.first_step() {
            Some(Some(first)) => Some(first),
            // The episode open at the watermark may take its first step in the tail.
            Some(None) if newest => tail_steps
                .first()
                .copied()
                .filter(|first| *first >= prefix_len && *first < end),
            Some(None) => None,
            None => return Err("projected episode first step is uncomputed".into()),
        };
        out.push(GuidanceEpisode {
            render,
            first_step,
            end,
        });
    }
    for (index, render) in tail.render.iter().enumerate() {
        let start = render.prompt_start as u64;
        let end = tail
            .render
            .get(index + 1)
            .map_or(extent, |next| next.prompt_start as u64);
        if start < prefix_len
            || out
                .last()
                .is_some_and(|last| last.render.prompt_start as u64 >= start)
        {
            return Err("WAL tail episodes overlap the materialized prefix".into());
        }
        out.push(GuidanceEpisode {
            render: render.clone(),
            first_step: tail_steps
                .iter()
                .copied()
                .find(|first| start <= *first && *first < end),
            end,
        });
    }
    if out.is_empty() {
        return Err("a resumed lineage has no episode".into());
    }
    Ok(out)
}
