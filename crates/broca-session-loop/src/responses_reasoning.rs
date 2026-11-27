//! Request-local suppression of reasoning items the Responses API already discarded.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use broca_protocol::request::RenderHistory;
use broca_protocol::{CallOptions, InputTransformation};
use broca_session_store::record::Record;

/// A session's removed Responses reasoning IDs, projected from existing attempt telemetry.
/// The first reporting step is relative to the current run: `None` means an earlier run.
#[derive(Default)]
pub struct ResponsesReasoningRemovals {
    known: HashMap<String, Option<u64>>,
    rendered: Option<RenderedHistory>,
}

struct RenderedHistory {
    base: Arc<RenderHistory>,
    with_removals: Arc<RenderHistory>,
}

impl ResponsesReasoningRemovals {
    pub(crate) fn applies(config: &serde_json::Value) -> bool {
        matches!(
            config.get("family_id").and_then(serde_json::Value::as_str),
            Some("openai_responses" | "openai_responses_chatgpt")
        )
    }

    pub(crate) fn observe_record(&mut self, run_id: &str, record: &Record) {
        let Record::Attempt(attempt) = record else {
            return;
        };
        let Some(entries) = attempt
            .telemetry
            .as_ref()
            .and_then(crate::billing::reported_input_transformations)
        else {
            return;
        };
        let step = (attempt.run_id == run_id).then_some(attempt.step_id);
        for id in entries.iter().filter_map(removed_id) {
            self.known
                .entry(id.to_owned())
                .and_modify(|first| *first = (*first).min(step))
                .or_insert(step);
        }
    }

    /// Other transformations still warn normally. The first report of a reasoning ID both
    /// adds it to the suppression set and makes it worth warning about.
    pub(crate) fn observe_live(&mut self, step: u64, entry: &InputTransformation) -> bool {
        let Some(id) = removed_id(entry) else {
            return true;
        };
        if self.known.contains_key(id) {
            return false;
        }
        self.known.insert(id.to_owned(), Some(step));
        true
    }

    pub(crate) fn apply(&mut self, opts: &mut CallOptions, step: u64) {
        if self.known.is_empty() {
            return;
        }
        // A retry of the same step keeps the request it had before any of its own attempts.
        let removed = self
            .known
            .iter()
            .filter(|(_, first)| first.is_none_or(|first| first < step))
            .map(|(id, _)| id);
        let count = removed.clone().count();
        if count == 0 {
            return;
        }
        // The set only grows as a run advances. Reuse the shared episode list until either an
        // item becomes visible to a later step or a seal or view changes the base history.
        if let Some(cached) = self.rendered.as_ref().filter(|cached| {
            Arc::ptr_eq(&cached.base, &opts.render_episodes)
                && cached.with_removals.removed_responses_reasoning.len() == count
        }) {
            opts.render_episodes = Arc::clone(&cached.with_removals);
            return;
        }
        let base = Arc::clone(&opts.render_episodes);
        Arc::make_mut(&mut opts.render_episodes).removed_responses_reasoning =
            removed.cloned().collect::<HashSet<_>>();
        self.rendered = Some(RenderedHistory {
            base,
            with_removals: Arc::clone(&opts.render_episodes),
        });
    }
}

/// Responses identifies the removed reasoning item with `id`, not a transcript position.
/// Missing identifiers and any other kind of transformation are diagnostic evidence only.
fn removed_id(entry: &InputTransformation) -> Option<&str> {
    match (
        entry.kind.as_deref(),
        entry.action.as_deref(),
        entry.id.as_deref(),
    ) {
        (Some("reasoning"), Some("removed"), Some(id)) if !id.is_empty() => Some(id),
        _ => None,
    }
}
