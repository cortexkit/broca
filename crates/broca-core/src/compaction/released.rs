//! Judging a newer view after the run released history.
//!
//! Once a view applies, a running session keeps only the messages that view's requests send
//! (`held_prompt::held_by`): system messages, the view's keep ranges and everything after its
//! coverage. A newer view can keep, edit, uncover or copy messages the run let go of. Its
//! decision must see exactly the bytes a run that never released anything would see, so those
//! messages are read back by ordinal before the view is decided. They stay only when the view
//! applies and keeps them; every other row read for the decision is dropped with it.

use std::ops::Range;

use broca_protocol::Message;

use super::{native_schema_message, view::ordinal_range, OriginalMessages, StoredTranscript};
use crate::compaction_provider::view::{CompactionView, ViewRange};

/// The ordinals a decision on a newer view reads that the live prompt does not hold, as
/// half-open ranges in increasing order.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct ReleasedNeeds {
    /// Read in prompt form (runner tags and the episode's repetition guidance folded in, as a
    /// request renders the message): the view's keep ranges, the message its coverage ends at
    /// and every message after its coverage.
    pub prompt: Vec<Range<u64>>,
    /// Read only as stored (what `session.read` returns): the replace ranges whose
    /// replacement could be a copy of a message only the model may author, which is judged
    /// against the stored messages it stands for.
    pub stored: Vec<Range<u64>>,
}

impl ReleasedNeeds {
    pub(crate) fn is_empty(&self) -> bool {
        self.prompt.is_empty() && self.stored.is_empty()
    }
}

/// Messages read back by ordinal for one decision.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ReleasedMessages {
    /// [`ReleasedNeeds::prompt`]'s messages in prompt form, by increasing ordinal.
    pub prompt: Vec<Message>,
    /// The stored form of each message in `prompt` whose prompt form differs from it.
    pub originals: Vec<Message>,
    /// [`ReleasedNeeds::stored`]'s messages as stored, by increasing ordinal.
    pub stored: Vec<Message>,
    /// The episodes the messages of `prompt` belong to, so a view that keeps them renders
    /// their episode decorations as a run that held them all along does.
    pub episodes: Vec<crate::run::held_prompt::GuidanceEpisode>,
}

impl ReleasedMessages {
    /// Only the messages `view` holds once it applies, with their stored originals. Rows read
    /// only to judge the view are dropped here rather than kept resident.
    pub(crate) fn held_by(mut self, view: &CompactionView) -> Self {
        self.prompt
            .retain(|message| crate::run::held_prompt::held_by(view, message));
        self.originals
            .retain(|message| crate::run::held_prompt::held_by(view, message));
        self.stored = Vec::new();
        self
    }
}

/// Reads released messages back by absolute ordinal. The run loop supplies one that reads the
/// materialized store and falls back to the WAL.
#[async_trait::async_trait]
pub(crate) trait ReleasedHistory: Send + Sync {
    /// Every message `needs` names, or an error when neither the store nor the WAL can
    /// supply one of them. A partial answer is never returned.
    async fn load(&self, needs: &ReleasedNeeds) -> Result<ReleasedMessages, String>;
}

/// What deciding `view` reads beyond the messages of `prompt`, the live (possibly released)
/// prompt whose stored copy is `stored`, in a session of `extent` messages.
///
/// Empty when `prompt` holds everything the decision reads, which is always the case before
/// the first view applies.
pub(crate) fn released_needs(
    view: &CompactionView,
    prompt: &[Message],
    stored: StoredTranscript<'_>,
    extent: u64,
) -> ReleasedNeeds {
    let mut needs = ReleasedNeeds::default();
    let covered = view.covers_through.ordinal;
    for range in &view.ranges {
        match range {
            ViewRange::Keep { from, to, .. } => {
                absent(prompt, *from, (*to).min(extent), &mut needs.prompt);
            }
            ViewRange::Replace { from, to, bytes }
                if replacement_judged_against_stored(bytes, stored) =>
            {
                absent(prompt, *from, (*to).min(extent), &mut needs.stored);
            }
            _ => {}
        }
    }
    // The coverage's last message is looked up by `dispose_fetched` even when a replace range
    // stands for it, and every message after the coverage is sent as written.
    absent(
        prompt,
        covered,
        covered.saturating_add(1).min(extent),
        &mut needs.prompt,
    );
    absent(prompt, covered.saturating_add(1), extent, &mut needs.prompt);
    needs.prompt = normalized(std::mem::take(&mut needs.prompt));
    needs.stored = subtract(normalized(std::mem::take(&mut needs.stored)), &needs.prompt);
    needs
}

/// Whether `assemble_judged` reads the stored messages a replace range stands for: only when
/// one of its replacement messages could be an unchanged copy of model-authored content.
/// Bytes that do not decode are rejected before anything is read.
fn replacement_judged_against_stored(bytes: &str, stored: StoredTranscript<'_>) -> bool {
    let Ok(values) = serde_json::from_str::<Vec<serde_json::Value>>(bytes) else {
        return false;
    };
    values
        .iter()
        .filter_map(native_schema_message)
        .any(|candidate| super::view::carries_model_authorship(&candidate, stored))
}

/// Append to `out` the ordinals of `[from, to)` that `prompt` does not hold.
fn absent(prompt: &[Message], from: u64, to: u64, out: &mut Vec<Range<u64>>) {
    if from >= to {
        return;
    }
    let mut next = from;
    for ordinal in ordinal_range(prompt, from, to)
        .iter()
        .filter_map(|message| message.ordinal)
    {
        if ordinal > next {
            out.push(next..ordinal);
        }
        next = ordinal.saturating_add(1);
    }
    if next < to {
        out.push(next..to);
    }
}

/// Sort and merge overlapping or touching ranges.
fn normalized(mut ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    ranges.sort_by_key(|range| range.start);
    let mut out: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        match out.last_mut() {
            Some(last) if range.start <= last.end => last.end = last.end.max(range.end),
            _ => out.push(range),
        }
    }
    out
}

/// `ranges` without the ordinals of `minus`; both sorted and merged.
fn subtract(ranges: Vec<Range<u64>>, minus: &[Range<u64>]) -> Vec<Range<u64>> {
    let mut out = Vec::new();
    for range in ranges {
        let mut start = range.start;
        for cut in minus {
            if cut.start >= range.end || cut.end <= start {
                continue;
            }
            if cut.start > start {
                out.push(start..cut.start);
            }
            start = start.max(cut.end);
        }
        if start < range.end {
            out.push(start..range.end);
        }
    }
    out
}

/// The transcript one decision reads: the live prompt with the messages read back for it
/// merged in by ordinal, and the stored copy of each.
pub(crate) struct Judging {
    pub prompt: Vec<Message>,
    originals: OriginalMessages,
    extent: u64,
}

impl Judging {
    pub(crate) fn new(
        prompt: &[Message],
        stored: StoredTranscript<'_>,
        loaded: &ReleasedMessages,
    ) -> Self {
        let mut extra: Vec<&Message> = loaded.prompt.iter().chain(&loaded.stored).collect();
        extra.sort_by_key(|message| message.ordinal);
        let mut merged = Vec::with_capacity(prompt.len() + extra.len());
        let mut extra = extra.into_iter().peekable();
        for message in prompt {
            if let Some(ordinal) = message.ordinal {
                while let Some(next) = extra.next_if(|next| next.ordinal < Some(ordinal)) {
                    merged.push(next.clone());
                }
            }
            merged.push(message.clone());
        }
        merged.extend(extra.cloned());
        let originals = stored.judging_originals(&merged, &loaded.originals);
        Self {
            prompt: merged,
            originals,
            extent: stored.extent(),
        }
    }

    pub(crate) fn stored(&self) -> StoredTranscript<'_> {
        StoredTranscript::new(&self.prompt, Some(&self.originals)).with_extent(self.extent)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::single_range_in_vec_init)]
    fn needs_cover_kept_uncovered_and_judged_ordinals_the_prompt_lacks() {
        use crate::compaction_provider::{lineage::MessageRef, view::CompactionView};
        use broca_protocol::Role;
        let mut prompt = vec![Message::new(Role::User, vec![]); 4];
        for (message, ordinal) in prompt.iter_mut().zip([0, 3, 4, 9]) {
            message.ordinal = Some(ordinal);
        }
        let view = |replacement: Message| {
            CompactionView::new(
                "session",
                3,
                "lineage",
                MessageRef {
                    ordinal: 4,
                    mid: "m4".into(),
                },
                vec![
                    ViewRange::replace(0, 2, serde_json::to_string(&[replacement]).unwrap()),
                    ViewRange::keep(2, 5),
                ],
            )
        };
        let stored = StoredTranscript::plain(&prompt).with_extent(10);
        let summary = Message::new(Role::User, vec![]);
        assert_eq!(
            released_needs(&view(summary), &prompt, stored, 10),
            ReleasedNeeds {
                prompt: vec![2..3, 5..9],
                stored: vec![],
            },
            "a plain summary is not judged against the messages it replaces"
        );
        let mut copy = Message::new(Role::Assistant, vec![]);
        copy.origin = Some(
            serde_json::from_value(
                serde_json::json!({"provider_module_id":"stub","model_id":"model"}),
            )
            .unwrap(),
        );
        assert_eq!(
            released_needs(&view(copy), &prompt, stored, 10).stored,
            vec![1..2],
            "a copy claiming model authorship is judged against the stored messages"
        );
    }

    #[test]
    fn absent_ranges_skip_held_ordinals() {
        let mut prompt = vec![Message::new(broca_protocol::Role::User, vec![]); 3];
        for (message, ordinal) in prompt.iter_mut().zip([0, 4, 9]) {
            message.ordinal = Some(ordinal);
        }
        let mut out = Vec::new();
        absent(&prompt, 0, 12, &mut out);
        assert_eq!(out, vec![1..4, 5..9, 10..12]);
        assert_eq!(
            subtract(normalized(vec![0..3, 2..8]), &[1..2, 5..6]),
            vec![0..1, 2..5, 6..8]
        );
    }
}
