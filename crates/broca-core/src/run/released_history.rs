//! Reading messages a running session released back by absolute ordinal, so a newer compaction
//! view is decided against the bytes a run that held its whole transcript would see.
//!
//! Rows below the materialized store's prefix come from the store, page by page, at one
//! watermark. Anything the store cannot supply (no store, a message past its prefix, an episode
//! projection that was never computed, a gap, or a watermark that moved during the read) is read
//! from the WAL instead, which is the authority: the whole WAL is replayed, since it cannot be
//! read by ordinal. An answer is complete or an error, never a shorter history.

use std::ops::Range;
use std::sync::Arc;

use broca_diagnostics::{spawn_blocking, JobKind};
use broca_protocol::{BindIdentity, Message};

use super::held_prompt::{GuidanceEpisode, GuidanceEpisodes};
use crate::compaction::{OriginalMessages, ReleasedHistory, ReleasedMessages, ReleasedNeeds};
use crate::context::{ReadView, SessionStore};
use crate::wal_writer::{WalHandle, WalStore};

/// How the run shows a message it holds: runner tags rendered by the run's frozen render
/// config, or the computed tags an earlier run wrote cleaned out of its text.
#[derive(Clone)]
pub(crate) struct PromptForm {
    pub render_tags: bool,
    pub render_config: serde_json::Value,
}

impl PromptForm {
    /// The same rendering `render_new_tags` gives a message when it joins the prompt.
    fn render(&self, message: &mut Message) {
        if self.render_tags {
            broca_protocol::tags::render_tags_into(
                std::slice::from_mut(message),
                broca_protocol::tags::TagRendering::for_render_config(&self.render_config),
            );
        } else {
            broca_protocol::tags::clean_computed_into(std::slice::from_mut(message));
        }
    }
}

/// The run loop's reader of released messages.
pub(crate) struct RunHistory<'a> {
    pub wal: Option<&'a dyn WalStore>,
    pub store: Option<Arc<dyn SessionStore>>,
    pub writer: &'a WalHandle,
    pub session: BindIdentity,
    pub form: PromptForm,
}

#[async_trait::async_trait]
impl ReleasedHistory for RunHistory<'_> {
    async fn load(&self, needs: &ReleasedNeeds) -> Result<ReleasedMessages, String> {
        if let (Some(store), Some(lineage)) = (&self.store, self.writer.lineage_id()) {
            let store = Arc::clone(store);
            let session = self.session.clone();
            let lineage = lineage.to_string();
            let wanted = needs.clone();
            let form = self.form.clone();
            let read = spawn_blocking(JobKind::Projection, move || {
                from_store(store.as_ref(), &session, &lineage, &wanted, &form)
            })
            .await
            .map_err(|error| format!("store read task failed: {error}"))
            .and_then(|read| read);
            match read {
                Ok(loaded) => return Ok(loaded),
                Err(reason) => tracing::info!(target: "compaction", %reason,
                    "released history is read from the WAL"),
            }
        }
        let wal = self
            .wal
            .ok_or("no WAL reader for the released messages a view needs")?;
        from_wal(wal, &self.session, needs, &self.form)
    }
}

/// Replay the whole WAL and take the needed messages from its transcript.
fn from_wal(
    wal: &dyn WalStore,
    session: &BindIdentity,
    needs: &ReleasedNeeds,
    form: &PromptForm,
) -> Result<ReleasedMessages, String> {
    let records: Vec<_> = wal
        .read_records(session)
        .map_err(|error| error.to_string())?
        .into_iter()
        .map(|frame| frame.record)
        .collect();
    let replay = crate::replay(&records).map_err(|error| error.to_string())?;
    let render = replay
        .render_episodes
        .as_deref()
        .ok_or("WAL replay has no episode render inputs")?;
    let episodes =
        GuidanceEpisodes::new(render, replay.episode_step_spans.as_deref(), replay.extent)
            .into_episodes();
    let prompt = replay.prompt;
    build(needs, &episodes, form, |range| {
        let start = prompt.partition_point(|m| m.ordinal.is_none_or(|o| o < range.start));
        let rows = prompt[start..]
            .iter()
            .take_while(|m| m.ordinal.is_some_and(|o| o < range.end))
            .cloned()
            .collect();
        contiguous(rows, range, "WAL")
    })
}

/// Read the needed messages from the store's materialized prefix, with the episode metadata of
/// its episode projection. An error means the store cannot answer completely.
fn from_store(
    store: &dyn SessionStore,
    session: &BindIdentity,
    lineage: &str,
    needs: &ReleasedNeeds,
    form: &PromptForm,
) -> Result<ReleasedMessages, String> {
    let first = needs
        .prompt
        .iter()
        .chain(&needs.stored)
        .map(|range| range.start)
        .min()
        .ok_or("nothing to read")?;
    let mut pages = StorePages {
        store,
        session,
        lineage,
        at: None,
    };
    // A page read before and after the projection read pins it to the pages' watermark.
    pages.rows(first..first + 1)?;
    let projection = store
        .read_episode_projection(session)?
        .ok_or("store keeps no episode projection")?;
    let render = super::episode_render_inputs(&projection)?
        .ok_or("store episode projection is incomplete")?;
    let episodes: Vec<GuidanceEpisode> = render
        .iter()
        .zip(&projection)
        .enumerate()
        .map(|(index, (render, projected))| GuidanceEpisode {
            render: render.clone(),
            first_step: projected.first_step().flatten(),
            // The newest projected episode's end is not known here; holding it again bounds
            // it by the next episode the run knows (`GuidanceEpisodes::restore`).
            end: projection
                .get(index + 1)
                .and_then(|next| next.start())
                .unwrap_or(u64::MAX),
        })
        .collect();
    let loaded = build(needs, &episodes, form, |range| pages.rows(range))?;
    pages.rows(first..first + 1)?;
    Ok(loaded)
}

/// Raw store pages, all at the watermark of the first one read.
struct StorePages<'a> {
    store: &'a dyn SessionStore,
    session: &'a BindIdentity,
    lineage: &'a str,
    /// The watermark and prefix length of the first page.
    at: Option<(u64, u64)>,
}

impl StorePages<'_> {
    fn rows(&mut self, range: Range<u64>) -> Result<Vec<Message>, String> {
        let mut rows = Vec::new();
        let mut next = range.start;
        while next < range.end {
            let page = self.store.read_page_with_watermark(
                self.session,
                Some(next),
                range.end - next,
                ReadView::Raw,
            )?;
            if page.lineage_id.as_deref() != Some(self.lineage) {
                return Err("store page belongs to another lineage".into());
            }
            let at = *self.at.get_or_insert((page.watermark_seq, page.prefix_len));
            if at != (page.watermark_seq, page.prefix_len) {
                return Err("store watermark moved while reading released history".into());
            }
            if range.end > at.1 {
                return Err("released message is past the store's prefix".into());
            }
            let before = next;
            for row in page.rows {
                if row.ordinal == next && next < range.end {
                    let mut message = row.message;
                    message.ordinal = Some(row.ordinal);
                    rows.push(message);
                    next += 1;
                }
            }
            if next == before {
                return Err(format!("store gap at ordinal {next}"));
            }
        }
        Ok(rows)
    }
}

/// `rows` when they are exactly the messages of `range`, in order.
fn contiguous(rows: Vec<Message>, range: Range<u64>, source: &str) -> Result<Vec<Message>, String> {
    if rows.len() as u64 != range.end - range.start
        || rows
            .iter()
            .zip(range.clone())
            .any(|(row, ordinal)| row.ordinal != Some(ordinal))
    {
        return Err(format!(
            "{source} cannot supply released messages {}..{}",
            range.start, range.end
        ));
    }
    Ok(rows)
}

/// The messages `needs` names, given the session's episodes and a reader of stored rows by
/// ordinal range. A prompt-form message gets the repetition guidance its episode's fold gives it
/// (folded from the episode's first model step, over raw rows) and then its runner tags, the
/// order in which the run itself decorates a message.
fn build(
    needs: &ReleasedNeeds,
    episodes: &[GuidanceEpisode],
    form: &PromptForm,
    mut rows: impl FnMut(Range<u64>) -> Result<Vec<Message>, String>,
) -> Result<ReleasedMessages, String> {
    let mut out = ReleasedMessages::default();
    for range in &needs.stored {
        out.stored.extend(rows(range.clone())?);
    }
    // Index of the episode an ordinal belongs to; `None` before the first episode.
    let episode_of = |ordinal: u64| {
        episodes
            .partition_point(|episode| episode.render.prompt_start as u64 <= ordinal)
            .checked_sub(1)
    };
    let mut by_episode: Vec<(Option<usize>, Vec<Range<u64>>)> = Vec::new();
    for range in &needs.prompt {
        let mut start = range.start;
        while start < range.end {
            let episode = episode_of(start);
            let next_start = episodes
                .get(episode.map_or(0, |index| index + 1))
                .map_or(u64::MAX, |next| next.render.prompt_start as u64);
            let end = range.end.min(next_start);
            match by_episode.last_mut() {
                Some((last, ranges)) if *last == episode => ranges.push(start..end),
                _ => by_episode.push((episode, std::iter::once(start..end).collect())),
            }
            start = end;
        }
    }
    for (episode, ranges) in by_episode {
        let fold_end = ranges.iter().map(|range| range.end).max().unwrap_or(0);
        let first_step = episode
            .and_then(|index| episodes[index].first_step)
            .filter(|first| *first < fold_end);
        let mut undecorated = OriginalMessages::default();
        let decorated = match first_step {
            Some(first) => {
                let mut raw = rows(first..fold_end)?;
                super::restore_guidance_with_originals(
                    &mut raw,
                    first as usize,
                    Some(&mut undecorated),
                );
                raw
            }
            None => Vec::new(),
        };
        for range in ranges {
            let below_fold = range.end.min(first_step.unwrap_or(range.end));
            let mut stored_rows = Vec::new();
            if range.start < below_fold {
                stored_rows = rows(range.start..below_fold)?;
            }
            let from_fold = range.start.max(below_fold)..range.end;
            let folded = from_fold.clone().map(|ordinal| {
                let shown = decorated[(ordinal - first_step.unwrap_or(0)) as usize].clone();
                let stored = undecorated
                    .get(ordinal)
                    .cloned()
                    .unwrap_or_else(|| shown.clone());
                (shown, stored)
            });
            for (mut shown, mut stored) in stored_rows
                .into_iter()
                .map(|row| (row.clone(), row))
                .chain(folded)
            {
                form.render(&mut shown);
                if shown.content != stored.content {
                    stored.ordinal = shown.ordinal;
                    out.originals.push(stored);
                }
                out.prompt.push(shown);
            }
        }
        if let Some(index) = episode {
            out.episodes.push(episodes[index].clone());
        }
    }
    Ok(out)
}
