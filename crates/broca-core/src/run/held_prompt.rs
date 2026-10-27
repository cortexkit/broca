//! Absolute transcript positions are independent of the sparse prompt's resident length.

use broca_protocol::{Message, Role};

pub(super) fn release_read_metadata(prior: &mut crate::ReplayState) {
    // Transcript readers rebuild these indexes independently; the executing run never uses them.
    prior.message_runs = Vec::new();
    prior.run_finals.clear();
    prior.committed_tool_turns = Vec::new();
    prior.open_tool_turn = None;
    prior.open_tool_run = None;
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) struct GuidanceEpisode {
    pub render: broca_protocol::request::EpisodeRenderInput,
    pub first_step: Option<u64>,
    pub end: u64,
}

/// What a resume that loaded only the held set of an applied compaction view hands the run
/// loop, in place of what the loop would derive from a whole transcript.
#[derive(Clone, Debug)]
pub(crate) struct HeldResume {
    /// The guidance episodes: the newest one (the episode being resumed) and every one that
    /// holds a message of the prompt, in lineage order, with their first model steps and ends.
    pub episodes: Vec<GuidanceEpisode>,
    /// The newest episode's repetition observer, folded over its model steps.
    pub observer: crate::tool_repetition::RepetitionObserver,
    /// The undecorated copy of every held message the repetition-guidance fold changed.
    pub originals: Vec<Message>,
    /// The highest runner tag the whole transcript shows: where numbering continues.
    pub tag_high: u64,
    /// How many messages a merge of the complete prefix with the WAL tail would hold; the
    /// resume record reports it as the replayed transcript.
    pub replayed_messages: u64,
}

pub(super) struct GuidanceEpisodes {
    episodes: Vec<GuidanceEpisode>,
}

impl GuidanceEpisodes {
    /// Episodes a held resume already knows, in lineage order (see [`HeldResume::episodes`]).
    pub(super) fn known(episodes: Vec<GuidanceEpisode>) -> Self {
        Self { episodes }
    }

    pub(super) fn new(
        render: &[broca_protocol::request::EpisodeRenderInput],
        spans: Option<&[std::ops::Range<usize>]>,
        extent: u64,
    ) -> Self {
        Self {
            episodes: render
                .iter()
                .enumerate()
                .map(|(index, episode)| {
                    let start = episode.prompt_start as u64;
                    let end = render
                        .get(index + 1)
                        .map_or(extent, |next| next.prompt_start as u64);
                    GuidanceEpisode {
                        render: episode.clone(),
                        end,
                        first_step: spans
                            .into_iter()
                            .flatten()
                            .map(|span| span.start as u64)
                            .find(|first| start <= *first && *first < end),
                    }
                })
                .collect(),
        }
    }

    pub(super) fn active_step(&mut self, ordinal: u64) {
        if let Some(active) = self.episodes.last_mut() {
            active.first_step.get_or_insert(ordinal);
        }
    }

    /// Know again the earlier episodes of messages a newer view holds after they were released,
    /// so [`Self::retain`] keeps their render inputs while those messages are held. An episode
    /// already known keeps its entry; the active episode stays last.
    ///
    /// An inserted episode ends no later than the next known episode starts. That bounds an
    /// episode whose end its source could not tell (the newest one a store projected): every
    /// held message's own episode is known, so no held message falls between.
    pub(super) fn restore(&mut self, episodes: Vec<GuidanceEpisode>) {
        for mut episode in episodes {
            let start = episode.render.prompt_start;
            if let Err(at) = self
                .episodes
                .binary_search_by_key(&start, |known| known.render.prompt_start)
            {
                if let Some(next) = self.episodes.get(at) {
                    episode.end = episode.end.min(next.render.prompt_start as u64);
                    self.episodes.insert(at, episode);
                }
            }
        }
    }

    pub(super) fn into_episodes(self) -> Vec<GuidanceEpisode> {
        self.episodes
    }

    pub(super) fn retain(
        &mut self,
        prompt: &[Message],
        extent: u64,
    ) -> broca_protocol::request::RenderHistory {
        let active = self
            .episodes
            .last()
            .map(|episode| episode.render.prompt_start);
        if let Some(episode) = self.episodes.last_mut() {
            episode.end = extent;
        }
        self.episodes.retain(|episode| {
            debug_assert!(episode.first_step.is_none_or(|first| first < episode.end));
            Some(episode.render.prompt_start) == active
                || prompt.iter().any(|message| {
                    message.ordinal.is_some_and(|ordinal| {
                        episode.render.prompt_start as u64 <= ordinal && ordinal < episode.end
                    })
                })
        });
        self.episodes
            .iter()
            .map(|episode| episode.render.clone())
            .collect::<Vec<_>>()
            .into()
    }

    #[cfg(test)]
    pub(super) fn episodes(&self) -> &[GuidanceEpisode] {
        &self.episodes
    }
}

#[cfg(test)]
pub(crate) type Probe = dyn Fn(&[Message], u64, usize, &[GuidanceEpisode], usize) + Send + Sync;

#[cfg(test)]
pub(crate) type ReleaseProbe =
    dyn Fn(&[Message], u64, &crate::compaction::CompactionState) + Send + Sync;

#[derive(Default)]
pub(super) struct PromptPosition {
    pub(super) stamped_len: u64,
    pub(super) held_stamped: usize,
}

impl PromptPosition {
    pub(super) fn restored(extent: u64, held: usize) -> Self {
        Self {
            stamped_len: extent,
            held_stamped: held,
        }
    }

    pub(super) fn extent(&self, prompt: &[Message]) -> u64 {
        self.stamped_len + (prompt.len() - self.held_stamped) as u64
    }

    pub(super) fn stamp(&mut self, prompt: &mut [Message]) -> usize {
        let appended = prompt.len() - self.held_stamped;
        for message in &mut prompt[self.held_stamped..] {
            message.ordinal = Some(self.stamped_len);
            self.stamped_len += 1;
        }
        self.held_stamped = prompt.len();
        appended
    }

    pub(super) fn released(&mut self, held: usize) {
        self.held_stamped = held;
    }
}

pub(crate) fn held_by(
    view: &crate::compaction_provider::view::CompactionView,
    message: &Message,
) -> bool {
    use crate::compaction_provider::view::ViewRange;
    message.role == Role::System
        || message.ordinal.is_some_and(|ordinal| {
            ordinal > view.covers_through.ordinal || view.ranges.iter().any(|range| {
            matches!(range, ViewRange::Keep { from, to, .. } if *from <= ordinal && ordinal < *to)
        })
        })
}

/// Hold again the released messages a newly applied view keeps (`reheld`, read back in prompt
/// form to decide it), merged in by ordinal with their stored originals, before [`release`]
/// trims the prompt to that view. Returns whether the prompt changed. Every message already in
/// `prompt` is stamped at a step boundary, so the inserted messages are counted as stamped and
/// the next ordinal stays `extent`.
pub(super) fn rehold(
    prompt: &mut Vec<Message>,
    originals: Option<&mut crate::compaction::OriginalMessages>,
    position: &mut PromptPosition,
    episodes: &mut GuidanceEpisodes,
    reheld: crate::compaction::ReleasedMessages,
) -> bool {
    debug_assert_eq!(position.extent(prompt), position.stamped_len);
    episodes.restore(reheld.episodes);
    let mut inserted = 0;
    let mut merged = Vec::with_capacity(prompt.len() + reheld.prompt.len());
    let mut extra = reheld.prompt.into_iter().peekable();
    for message in prompt.drain(..) {
        if let Some(ordinal) = message.ordinal {
            while let Some(next) = extra.next_if(|next| next.ordinal <= Some(ordinal)) {
                // A message the prompt already holds is never duplicated.
                if next.ordinal != Some(ordinal) {
                    merged.push(next);
                    inserted += 1;
                }
            }
        }
        merged.push(message);
    }
    for message in extra {
        merged.push(message);
        inserted += 1;
    }
    *prompt = merged;
    if let Some(originals) = originals {
        originals.insert_originals(reheld.originals, prompt);
    }
    position.held_stamped += inserted;
    inserted > 0
}

pub(super) fn release(
    prompt: &mut Vec<Message>,
    state: Option<&crate::compaction::CompactionState>,
    originals: Option<&mut crate::compaction::OriginalMessages>,
    position: &mut PromptPosition,
) -> bool {
    let Some(view) = state.and_then(|state| state.view.as_ref()) else {
        return false;
    };
    debug_assert_eq!(position.extent(prompt), position.stamped_len);
    let before = prompt.len();
    prompt.retain(|message| held_by(&view.view, message));
    if let Some(originals) = originals {
        originals.release(&view.view, prompt.len());
    }
    position.released(prompt.len());
    if prompt.len() != before {
        // Retaining rows alone would leave the discarded transcript's allocation resident.
        prompt.shrink_to_fit();
        return true;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn running_state_releases_read_only_transcript_indexes() {
        let mut prior = crate::replay(&[]).unwrap();
        let run = crate::replay::RunAttribution {
            run_id: "r".into(),
            episode: "r".into(),
            is_final: true,
            delivery: None,
        };
        prior.message_runs = vec![Some(run.clone())];
        prior.run_finals.insert(0, run.clone());
        prior.committed_tool_turns = vec![(0, 1)];
        prior.open_tool_turn = Some((Message::new(Role::Assistant, vec![]), 1));
        prior.open_tool_run = Some(run);
        release_read_metadata(&mut prior);
        assert!(prior.message_runs.is_empty());
        assert!(prior.run_finals.is_empty());
        assert!(prior.committed_tool_turns.is_empty());
        assert!(prior.open_tool_turn.is_none());
        assert!(prior.open_tool_run.is_none());
    }

    #[test]
    fn guidance_metadata_keeps_intersections_and_an_empty_active_episode() {
        let render = [0, 10, 20].map(|start| broca_protocol::request::EpisodeRenderInput {
            prompt_start: start,
            first_user: Some(start),
            frozen_config: serde_json::json!({"episode":start}),
        });
        let mut episodes = GuidanceEpisodes::new(&render, Some(&[1..10, 11..20, 21..30]), 30);
        let mut prompt = vec![Message::new(Role::User, vec![])];
        prompt[0].ordinal = Some(0);
        let history = episodes.retain(&prompt, 30);
        assert_eq!(
            history
                .episodes
                .iter()
                .map(|episode| episode.prompt_start)
                .collect::<Vec<_>>(),
            vec![0, 20]
        );
        assert_eq!(
            episodes
                .episodes()
                .iter()
                .map(|episode| (episode.render.first_user, episode.first_step, episode.end))
                .collect::<Vec<_>>(),
            vec![(Some(0), Some(1), 10), (Some(20), Some(21), 30)]
        );
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn restored_episodes_are_ordered_bounded_and_never_replace_known_ones() {
        let render = |start: usize| broca_protocol::request::EpisodeRenderInput {
            prompt_start: start,
            first_user: Some(start),
            frozen_config: serde_json::json!({"episode":start}),
        };
        let mut episodes = GuidanceEpisodes::new(&[render(10), render(30)], Some(&[11..20]), 40);
        let episode = |start: usize, end: u64| GuidanceEpisode {
            render: render(start),
            first_step: Some(start as u64 + 1),
            end,
        };
        episodes.restore(vec![
            episode(0, 10),
            episode(20, u64::MAX),
            episode(10, 99),
            episode(50, u64::MAX),
        ]);
        assert_eq!(
            episodes
                .episodes()
                .iter()
                .map(|episode| (episode.render.prompt_start, episode.end))
                .collect::<Vec<_>>(),
            vec![(0, 10), (10, 30), (20, 30), (30, 40)],
            "an unknown end stops at the next known episode; the active one stays last"
        );
    }

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn reheld_messages_join_in_ordinal_order_without_moving_the_next_ordinal() {
        let mut prompt = vec![Message::new(Role::User, vec![]); 3];
        for (message, ordinal) in prompt.iter_mut().zip([0, 5, 6]) {
            message.ordinal = Some(ordinal);
        }
        let mut position = PromptPosition::restored(7, 3);
        let render = |start: usize| broca_protocol::request::EpisodeRenderInput {
            prompt_start: start,
            first_user: Some(start),
            frozen_config: serde_json::json!({"episode":start}),
        };
        let mut episodes = GuidanceEpisodes::new(&[render(4)], Some(&[5..7]), 7);
        let reheld = |ordinal: u64| {
            let mut message = Message::new(Role::Assistant, vec![]);
            message.ordinal = Some(ordinal);
            message
        };
        let mut originals = crate::compaction::OriginalMessages::default();
        assert!(rehold(
            &mut prompt,
            Some(&mut originals),
            &mut position,
            &mut episodes,
            crate::compaction::ReleasedMessages {
                prompt: vec![reheld(2), reheld(3), reheld(5)],
                originals: vec![reheld(3)],
                stored: Vec::new(),
                episodes: vec![GuidanceEpisode {
                    render: render(0),
                    first_step: Some(1),
                    end: u64::MAX,
                }],
            },
        ));
        assert_eq!(
            episodes
                .episodes()
                .iter()
                .map(|episode| (episode.render.prompt_start, episode.end))
                .collect::<Vec<_>>(),
            vec![(0, 4), (4, 7)],
            "the reheld messages' episode is known again"
        );
        assert_eq!(
            prompt
                .iter()
                .map(|message| message.ordinal.unwrap())
                .collect::<Vec<_>>(),
            vec![0, 2, 3, 5, 6],
            "a message already held is not duplicated"
        );
        assert_eq!(prompt[3].role, Role::User);
        assert_eq!(originals.len(), 1);
        assert_eq!(position.extent(&prompt), 7);
        prompt.push(Message::new(Role::User, vec![]));
        position.stamp(&mut prompt);
        assert_eq!(prompt.last().unwrap().ordinal, Some(7));
    }

    #[test]
    fn new_messages_start_at_extent_even_when_the_newest_message_was_released() {
        let mut prompt = vec![Message::new(Role::User, vec![])];
        prompt[0].ordinal = Some(0);
        let mut position = PromptPosition::restored(100, 1);
        prompt.push(Message::new(Role::User, vec![]));
        assert_eq!(position.extent(&prompt), 101);
        assert_eq!(position.stamp(&mut prompt), 1);
        assert_eq!(prompt[1].ordinal, Some(100));
        assert_eq!(position.stamped_len, 101);
        prompt.truncate(1);
        position.released(1);
        prompt.push(Message::new(Role::User, vec![]));
        position.stamp(&mut prompt);
        assert_eq!(prompt[1].ordinal, Some(101));
    }

    #[test]
    fn setup_without_an_applied_view_never_releases_history() {
        let mut prompt = vec![Message::new(Role::User, vec![]); 10];
        let mut position = PromptPosition::default();
        position.stamp(&mut prompt);
        assert!(!release(
            &mut prompt,
            Some(&crate::compaction::CompactionState::default()),
            None,
            &mut position
        ));
        assert_eq!(prompt.len(), 10);
        assert_eq!(position.extent(&prompt), 10);
    }

    #[test]
    fn release_retains_systems_keep_ranges_suffix_and_only_their_originals() {
        use crate::compaction_provider::{
            lineage::MessageRef,
            view::{CompactionView, ViewRange},
        };
        let mut prompt = vec![Message::new(Role::User, vec![]); 10];
        prompt[2].role = Role::System;
        let mut position = PromptPosition::default();
        position.stamp(&mut prompt);
        let mut originals = crate::compaction::OriginalMessages::default();
        for message in &mut prompt {
            let original = message.clone();
            message.content.push(broca_protocol::ContentBlock::Text {
                text: "decorated".into(),
            });
            originals.retain_change(message.ordinal.unwrap() as usize, original, message);
        }
        let state = crate::compaction::CompactionState {
            view: Some(crate::compaction::view::AppliedView {
                view: CompactionView::new(
                    "session",
                    2,
                    "lineage",
                    MessageRef {
                        ordinal: 8,
                        mid: "m8".into(),
                    },
                    vec![ViewRange::keep(0, 1), ViewRange::replace(1, 9, "[]")],
                ),
                choice: broca_wal::CompactionViewChoice::Applied,
            }),
            ..Default::default()
        };
        assert!(release(
            &mut prompt,
            Some(&state),
            Some(&mut originals),
            &mut position
        ));
        assert_eq!(
            prompt
                .iter()
                .map(|message| message.ordinal)
                .collect::<Vec<_>>(),
            vec![Some(0), Some(2), Some(9)]
        );
        assert_eq!(originals.len(), 3);
        assert_eq!(position.extent(&prompt), 10);
        assert!(prompt.capacity() < 10);
    }
}
