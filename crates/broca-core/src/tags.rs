//! Runner tags: the numbers a session's parts show the model, under two sets of rules.
//!
//! Under the stored-ledger rules (`runner-tags/v1`) every part broca stored got a number when
//! its record was appended: the session's [`WalHandle`](crate::WalHandle) assigned them under
//! the lock that orders the record's sequence number, so they increase in WAL order, and the
//! next number is the highest any record holds, plus one. Retention deletion records the next
//! number on its tombstone so a lineage created after it keeps counting from there.
//!
//! Under the computed rules (`runner-tags/v2`, llm-runner 0.4.0 §9.6) no record stores a
//! number. A run frozen under them numbers the parts that count, in the order a request
//! renders the transcript, after its checkpoint ([`TagNumbering`]): the replay of the WAL and
//! the live loop drive the same numbering, so a read, a resume and the live request agree. The
//! first such run on a session the ledger numbered takes the ledger's high-water as its
//! checkpoint, so nothing a reader has seen is renumbered. A run keeps the rules it was frozen
//! under for its whole life, so a run in flight at the switch finishes under the ledger.

use serde::{Deserialize, Serialize};

/// What a lineage's records say about tags, as folded from its WAL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum LineageTags {
    /// No episode has started. The first `RunStarted` turns tags on and takes `floor` as its
    /// first tag: 1 for a new session, or the number a deleted predecessor reached.
    Unstarted { floor: u64 },
    /// The lineage started without tags (admitted before tags existed). Nothing is tagged.
    Off,
    /// The lineage is tagged; `next` is the tag the next part gets.
    On { next: u64 },
    /// A run frozen under the computed rules (`runner-tags/v2`) has started: no record is
    /// stamped any more, and numbers are computed from the transcript. `floor` is the stored
    /// ledger's high-water at the switch (0 when it never stored one), which every computed
    /// number stays above, so nothing the ledger handed out is ever shown again for another
    /// part.
    Computed { floor: u64 },
}

impl LineageTags {
    /// The state of an empty lineage.
    pub const fn unstarted() -> Self {
        LineageTags::Unstarted { floor: 1 }
    }
}

/// The computed rules a run frozen with `config` writes its messages by, or `None` for a run
/// frozen under the stored-ledger rules. The model's text is cleaned only when the run renders
/// tags and is not a `final_text` structured-output run, whose final text is its output and is
/// never cleaned.
pub fn computed_rules(
    config: &broca_records::record::RunConfig,
) -> Option<broca_protocol::tags::ComputedRules> {
    rules_for(
        config.computed_tags,
        config.render_tags,
        &config.frozen_render_config,
    )
}

/// [`computed_rules`] from a run's frozen members: its computed tag rules, whether it renders
/// tags, and its provider's frozen render config (which holds a structured-output run's mode).
pub fn rules_for(
    computed_tags: Option<broca_records::record::ComputedTagRules>,
    render_tags: bool,
    frozen_render_config: &serde_json::Value,
) -> Option<broca_protocol::tags::ComputedRules> {
    computed_tags?;
    Some(broca_protocol::tags::ComputedRules {
        render: render_tags,
        clean: render_tags && !structured_final_text(frozen_render_config),
    })
}

/// Whether the run's structured output is its final text.
fn structured_final_text(frozen_render_config: &serde_json::Value) -> bool {
    broca_protocol::structured_output::FrozenStructuredOutput::from_frozen_render_config(Some(
        frozen_render_config,
    ))
    .is_some_and(|frozen| frozen.mode == broca_protocol::structured_output::Mode::FinalText)
}

/// Whether a session's frozen plan asks its runs to show runner tags to the model: the plan's
/// `render_tags` member is `true`. Absent or `false` means off.
pub fn plan_renders_tags(manifest: &broca_records::record::PlanManifest) -> bool {
    serde_json::from_str::<serde_json::Value>(manifest.plan.as_json_text())
        .ok()
        .and_then(|plan| plan.get("render_tags").and_then(serde_json::Value::as_bool))
        == Some(true)
}

/// What numbering needs to know about the transcript before a stretch it numbers: the
/// highest number shown so far, and the computed rules of the run the stretch continues, when
/// the stretch starts inside a run whose start lies before it (a WAL tail after materialized
/// rows).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TagSeed {
    pub high: u64,
    pub rules: Option<broca_protocol::tags::ComputedRules>,
}

impl TagSeed {
    /// The seed after `prefix`, a transcript prefix as it was numbered.
    pub fn from_prefix(prefix: &[broca_protocol::Message]) -> Self {
        Self {
            high: broca_protocol::tags::high_after(prefix),
            rules: prefix.last().and_then(Self::rules_of),
        }
    }

    fn rules_of(message: &broca_protocol::Message) -> Option<broca_protocol::tags::ComputedRules> {
        let computed = message.part_tags()?.computed?;
        Some(broca_protocol::tags::ComputedRules {
            render: computed.render,
            clean: computed.clean,
        })
    }
}

/// Numbers a transcript under the computed rules as it grows, in the order it is rendered:
/// the replay of a WAL and the live run loop both drive one, over the same messages in the
/// same order, so both give each counted part the same number. Messages written under the stored-ledger
/// rules keep the numbers their records stored; numbering continues above the highest of them.
#[derive(Debug, Clone)]
pub struct TagNumbering {
    high: u64,
    numbered: usize,
    rules: Option<broca_protocol::tags::ComputedRules>,
    seed_high: u64,
}

impl TagNumbering {
    /// Start after `seed`, with `numbered` messages of the transcript already numbered.
    pub fn new(seed: TagSeed, numbered: usize) -> Self {
        Self {
            high: seed.high,
            numbered,
            rules: seed.rules,
            seed_high: seed.high,
        }
    }

    /// Numbering for a live run whose earlier transcript is `prior` (already numbered) and
    /// whose frozen rules are `rules` with `computed_tags`: the run's own messages, appended
    /// after `prior`, are numbered by them.
    pub fn for_run(
        prior: &[broca_protocol::Message],
        rules: Option<broca_protocol::tags::ComputedRules>,
        computed_tags: Option<broca_records::record::ComputedTagRules>,
    ) -> Self {
        let mut numbering = Self::new(TagSeed::from_prefix(prior), prior.len());
        numbering.rules = rules;
        if let Some(computed) = computed_tags {
            numbering.high = numbering.high.max(computed.checkpoint);
        }
        numbering
    }

    /// Number the messages appended to `prompt` since the last call. Refused, with nothing
    /// renumbered, when the numbers would pass `u64::MAX`.
    pub fn number_new(
        &mut self,
        prompt: &mut [broca_protocol::Message],
    ) -> Result<(), broca_protocol::tags::NumberingOverflow> {
        if prompt.len() < self.numbered {
            // The transcript lost its tail (a recovery rewrote it): continue after what is left.
            self.numbered = prompt.len();
            self.high = self.seed_high.max(broca_protocol::tags::high_after(prompt));
        }
        // Messages are counted as numbered one at a time, so a refusal leaves the ones after the
        // refused message for the next call rather than skipping them.
        let start = self.numbered;
        for message in &mut prompt[start..] {
            match (message.part_tags(), self.rules) {
                (Some(tags), _) => self.high = self.high.max(tags.high()),
                (None, Some(rules)) => {
                    broca_protocol::tags::number_message(message, rules, &mut self.high)?
                }
                (None, None) => {}
            }
            self.numbered += 1;
        }
        self.numbered = prompt.len();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    /// A run cleans the model's text exactly when it renders tags under the computed rules,
    /// except a `final_text` structured-output run, whose final text is its output.
    #[test]
    fn only_a_tag_rendering_run_that_is_not_final_text_output_cleans() {
        let computed = Some(broca_records::record::ComputedTagRules { checkpoint: 0 });
        let plain = serde_json::json!({"render_config": "stateless"});
        let final_text = serde_json::json!({
            "render_config": "stateless",
            "structured_output": {
                "mode": "final_text", "name": "result", "schema_jcs": "{}",
                "enforcement": "enforced", "delivery": "strict",
            },
        });
        let submit_tool = serde_json::json!({
            "render_config": "stateless",
            "structured_output": {
                "mode": "submit_tool", "name": "result", "schema_jcs": "{}",
                "enforcement": "enforced", "delivery": "strict",
            },
        });
        let clean = |computed, render, config: &serde_json::Value| {
            rules_for(computed, render, config).map(|rules| rules.clean)
        };
        assert_eq!(clean(computed, true, &plain), Some(true));
        assert_eq!(clean(computed, false, &plain), Some(false));
        assert_eq!(clean(computed, true, &final_text), Some(false));
        assert_eq!(clean(computed, true, &submit_tool), Some(true));
        assert_eq!(
            clean(None, true, &plain),
            None,
            "the stored ledger never cleans"
        );
    }
}
