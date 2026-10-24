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

use std::sync::Arc;

use broca_protocol::tags::{MessageTags, PartTags};
use broca_wal::WalRecord;
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

    /// Fold one record, in WAL order.
    pub fn fold(&mut self, record: &WalRecord) {
        if starts_computed_run(record) {
            if !matches!(self, LineageTags::Computed { .. }) {
                *self = LineageTags::Computed {
                    floor: self.ledger_high(),
                };
            }
            return;
        }
        let highest = record
            .runner_tags()
            .iter()
            .filter_map(MessageTags::max_tag)
            .max();
        match self {
            LineageTags::Unstarted { floor } => {
                if matches!(record, WalRecord::RunStarted { .. }) {
                    *self = match (record.runner_tags().is_empty(), highest) {
                        // A first episode written without tags: an untagged lineage.
                        (true, _) => LineageTags::Off,
                        (false, highest) => LineageTags::On {
                            next: highest.map_or(*floor, |tag| tag.saturating_add(1).max(*floor)),
                        },
                    };
                }
            }
            LineageTags::On { next } => {
                if let Some(tag) = highest {
                    *next = (*next).max(tag.saturating_add(1));
                }
            }
            LineageTags::Off | LineageTags::Computed { .. } => {}
        }
    }

    /// The highest number the stored ledger handed out on this lineage, or a deleted
    /// predecessor's: 0 when it handed out none. Under the computed rules, the floor kept at
    /// the switch.
    pub fn ledger_high(self) -> u64 {
        match self {
            LineageTags::Unstarted { floor } => floor.saturating_sub(1),
            LineageTags::On { next } => next.saturating_sub(1),
            LineageTags::Off => 0,
            LineageTags::Computed { floor } => floor,
        }
    }

    /// The tag the next part of this lineage gets, if the lineage is (or will be) tagged.
    pub fn next(self) -> Option<u64> {
        match self {
            LineageTags::Unstarted { floor } => Some(floor),
            LineageTags::On { next } => Some(next),
            LineageTags::Off | LineageTags::Computed { .. } => None,
        }
    }

    /// Raise an unstarted lineage's first tag to at least `floor` (a deleted predecessor's
    /// next tag). A started lineage already counts from its own records.
    pub fn with_floor(self, floor: u64) -> Self {
        match self {
            LineageTags::Unstarted { floor: own } => LineageTags::Unstarted {
                floor: own.max(floor),
            },
            other => other,
        }
    }
}

/// Whether `record` starts a run frozen under the computed tag rules.
fn starts_computed_run(record: &WalRecord) -> bool {
    matches!(record, WalRecord::RunStarted { config, .. } if config.computed_tags.is_some())
}

/// The computed rules a run frozen with `config` writes its messages by, or `None` for a run
/// frozen under the stored-ledger rules. The model's text is cleaned only when the run renders
/// tags and is not a `final_text` structured-output run, whose final text is its output and is
/// never cleaned.
pub fn computed_rules(
    config: &broca_wal::RunConfig,
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
    computed_tags: Option<broca_wal::ComputedTagRules>,
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
pub fn plan_renders_tags(manifest: &broca_wal::PlanManifest) -> bool {
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

    /// The seed after a prefix whose last message is `last` and whose highest number is
    /// `high`: what a store that keeps only per-row tags can tell without reading every row.
    pub fn after_last(last: Option<&broca_protocol::Message>) -> Self {
        Self {
            high: last
                .and_then(broca_protocol::Message::part_tags)
                .map_or(0, PartTags::high),
            rules: last.and_then(Self::rules_of),
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
        computed_tags: Option<broca_wal::ComputedTagRules>,
    ) -> Self {
        let mut numbering = Self::new(TagSeed::from_prefix(prior), prior.len());
        numbering.rules = rules;
        if let Some(computed) = computed_tags {
            numbering.high = numbering.high.max(computed.checkpoint);
        }
        numbering
    }

    /// Number `prompt[from..]`, messages a replay held back (steers waiting for their step
    /// boundary) and a rebuild appends, by the rules of the run `config` froze.
    pub fn number_appended(
        prompt: &mut [broca_protocol::Message],
        from: usize,
        config: &broca_wal::RunConfig,
    ) -> Result<(), broca_protocol::tags::NumberingOverflow> {
        let mut numbering = Self::new(TagSeed::from_prefix(&prompt[..from]), from);
        numbering.rules = computed_rules(config);
        numbering.number_new(prompt)
    }

    /// A run starts: from here on messages are numbered by its rules, after its checkpoint.
    pub fn start_run(&mut self, config: &broca_wal::RunConfig) {
        self.rules = computed_rules(config);
        if let Some(rules) = config.computed_tags {
            self.high = self.high.max(rules.checkpoint);
        }
    }

    /// The highest number shown so far.
    pub fn high(&self) -> u64 {
        self.high
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

    /// Anchor numbering at a compaction provider's Setup checkpoint: the cut message (`m<n>`,
    /// `n` its transcript ordinal; `prompt[0]` is ordinal `ordinal_base`) and every message
    /// after it are numbered after `checkpoint.tag_checkpoint`. Which anchor applies follows
    /// llm-runner's shared precedence (`tags::anchor`): an in-effect marker carrying a
    /// checkpoint for the session wins over the Setup checkpoint. Broca's host keeps no native
    /// compaction markers (compaction-provider §9), so its callers pass none and the Setup
    /// checkpoint applies. Answers whether numbering was re-anchored.
    pub fn anchor_setup(
        &mut self,
        prompt: &mut [broca_protocol::Message],
        ordinal_base: u64,
        markers: &[cortexkit_role_llm_runner::tags::Marker],
        session: &str,
        checkpoint: &broca_wal::SetupTagCheckpoint,
    ) -> Result<bool, broca_protocol::tags::NumberingOverflow> {
        let numbered = self.numbered.min(prompt.len());
        let index_of = |cut: &str| {
            cut.strip_prefix('m')
                .and_then(|ordinal| ordinal.parse::<u64>().ok())
                .and_then(|ordinal| ordinal.checked_sub(ordinal_base))
                .and_then(|index| usize::try_from(index).ok())
                .filter(|index| *index < numbered)
        };
        let setup = cortexkit_role_llm_runner::tags::SetupCheckpoint::new(
            checkpoint.tag_checkpoint,
            checkpoint.cut.clone(),
        );
        let anchor =
            cortexkit_role_llm_runner::tags::anchor(session, markers, Some(&setup), |cut| {
                index_of(cut).is_some()
            });
        let (anchor, cut) = match anchor {
            Ok(cortexkit_role_llm_runner::tags::Anchor::Marker { checkpoint, cut })
            | Ok(cortexkit_role_llm_runner::tags::Anchor::Setup { checkpoint, cut }) => {
                (checkpoint, cut)
            }
            // The cut message is not in this stretch of the transcript, or the session is a
            // fork: nothing to re-anchor here.
            _ => return Ok(false),
        };
        let Some(index) = index_of(&cut) else {
            return Ok(false);
        };
        let mut high = prompt[index]
            .part_tags()
            .and_then(|tags| tags.computed)
            .map_or_else(
                || broca_protocol::tags::high_after(&prompt[..index]),
                |computed| computed.base,
            )
            .max(anchor);
        for message in &mut prompt[index..numbered] {
            let rules = message
                .part_tags()
                .and_then(|tags| tags.computed)
                .map(|computed| broca_protocol::tags::ComputedRules {
                    render: computed.render,
                    clean: computed.clean,
                });
            match rules {
                Some(rules) => broca_protocol::tags::number_message(message, rules, &mut high)?,
                None => high = high.max(message.part_tags().map_or(0, PartTags::high)),
            }
        }
        self.high = high;
        Ok(true)
    }
}

/// Fold a complete WAL from its origin.
pub fn lineage_tags<'a>(records: impl IntoIterator<Item = &'a WalRecord>) -> LineageTags {
    let mut tags = LineageTags::unstarted();
    for record in records {
        tags.fold(record);
    }
    tags
}

/// The number of parts in each message `record` carries, in record order, when it is a record
/// whose parts get tags; `None` for every other record.
pub(crate) fn tag_shape(record: &WalRecord) -> Option<Vec<usize>> {
    match record {
        WalRecord::RunStarted { input, .. } | WalRecord::Steered { input, .. } => {
            Some(input.iter().map(|message| message.content.len()).collect())
        }
        WalRecord::SendDelivered {
            input: Some(input), ..
        } => Some(input.iter().map(|message| message.content.len()).collect()),
        WalRecord::ModelStepFinished {
            assistant_message, ..
        } => Some(vec![assistant_message.content.len()]),
        WalRecord::ToolResult { .. }
        | WalRecord::ToolNoDispatch { .. }
        | WalRecord::BudgetFired { .. }
        | WalRecord::ApprovalRecorded {
            closing_result: Some(_),
            ..
        } => Some(vec![1]),
        _ => None,
    }
}

/// Write `tags` into `record`'s tag field. `tags` has one entry per message of `record`.
pub(crate) fn set_record_tags(record: &mut WalRecord, tags: Vec<MessageTags>) {
    match record {
        WalRecord::RunStarted { tags: field, .. }
        | WalRecord::Steered { tags: field, .. }
        | WalRecord::SendDelivered { tags: field, .. } => {
            *field = Some(Arc::new(tags));
        }
        WalRecord::ModelStepFinished { tags: field, .. }
        | WalRecord::ToolResult { tags: field, .. }
        | WalRecord::ToolNoDispatch { tags: field, .. }
        | WalRecord::BudgetFired { tags: field, .. }
        | WalRecord::ApprovalRecorded { tags: field, .. } => {
            *field = tags.into_iter().next().map(Box::new);
        }
        _ => {}
    }
}

/// Assign tags from `state` to a record about to be appended, in place. Returns the tags it
/// assigned, one entry per message. A record that already holds tags keeps them, and the
/// counter moves past them.
pub(crate) fn stamp(state: &mut LineageTags, record: &mut WalRecord) -> Option<Vec<MessageTags>> {
    // A run under the computed rules stores no number, and neither does any record after one.
    if starts_computed_run(record) || matches!(state, LineageTags::Computed { .. }) {
        state.fold(record);
        return None;
    }
    if !record.runner_tags().is_empty() {
        let mut probe = match *state {
            LineageTags::Unstarted { floor } => LineageTags::Unstarted { floor },
            other => other,
        };
        probe.fold(record);
        *state = probe;
        return None;
    }
    let shape = tag_shape(record)?;
    let first = match (*state, &*record) {
        (LineageTags::Unstarted { floor }, WalRecord::RunStarted { .. }) => floor,
        (LineageTags::On { next }, _) => next,
        // Nothing before a lineage's first episode carries parts, and an untagged lineage stays
        // untagged.
        _ => return None,
    };
    let mut next = first;
    let tags: Vec<MessageTags> = shape
        .into_iter()
        .map(|parts| {
            let start = next;
            next = next.saturating_add(parts as u64);
            MessageTags::new((start..next).collect())
        })
        .collect();
    set_record_tags(record, tags.clone());
    *state = LineageTags::On { next };
    Some(tags)
}

/// Attach one record's `tags` to `message`, the message that record carries.
pub fn attach(message: &mut broca_protocol::Message, tags: Option<&MessageTags>) {
    attach_parts(message, tags.map(PartTags::from));
}

/// Attach per-part `tags` to `message`. They cover its blocks one for one: a record's tags are
/// assigned from the same blocks, so a mismatch is a broken invariant, never a partial tagging.
pub fn attach_parts(message: &mut broca_protocol::Message, tags: Option<PartTags>) {
    let Some(tags) = tags else {
        return;
    };
    assert_eq!(
        tags.parts.len(),
        message.content.len(),
        "runner tags must cover a message's blocks one for one"
    );
    message.set_tags(Some(Arc::new(tags)));
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::content::{ContentBlock, Message, Role, ToolOutput, ToolResult};
    use broca_protocol::{BatchId, ToolCallId};
    use broca_wal::record::DispatchOutcome;

    fn result(id: &str) -> WalRecord {
        WalRecord::ToolResult {
            finished_at_ms: None,
            tags: None,
            batch_id: BatchId("b".into()),
            result: Arc::new(ToolResult {
                tool_call_id: ToolCallId(id.into()),
                output: ToolOutput::Text { text: "r".into() },
                is_error: false,
            }),
            outcome: DispatchOutcome::Completed,
            hooks: None,
            reason: None,
            withdraw_elicitation: None,
            provider_held: None,
        }
    }

    fn two_block_steer() -> WalRecord {
        steer(2)
    }

    fn steer(blocks: usize) -> WalRecord {
        WalRecord::Steered {
            written_at_ms: None,
            tags: None,
            retention: None,
            ts_ms: None,
            send_id: None,
            request_identity_digest: None,
            delivery: None,
            title: None,
            input: Arc::new(vec![Message::new(
                Role::User,
                (0..blocks)
                    .map(|i| ContentBlock::Text {
                        text: format!("{i}"),
                    })
                    .collect(),
            )]),
            hooks: None,
        }
    }

    /// A first episode with one single-block user message, written without tags.
    fn run_started() -> WalRecord {
        let mut record = crate::replay::projection_boundary_tests::run_started(1, "r").record;
        if let WalRecord::RunStarted { input, .. } = &mut record {
            *input = Arc::new(vec![Message::new(
                Role::User,
                vec![ContentBlock::Text { text: "hi".into() }],
            )]);
        }
        record
    }

    /// A first episode frozen under the computed rules, rendering tags, with one user text.
    fn computed_run_started(checkpoint: u64) -> WalRecord {
        let mut record = run_started();
        if let WalRecord::RunStarted { config, .. } = &mut record {
            config.render_tags = true;
            config.computed_tags = Some(broca_wal::ComputedTagRules { checkpoint });
        }
        record
    }

    /// The switch from the stored ledger: the first run under the computed rules keeps the
    /// ledger's high-water as the lineage's floor, and from then on nothing is stamped.
    #[test]
    fn the_switch_keeps_the_ledger_high_water_and_stops_stamping() {
        let mut state = LineageTags::On { next: 10 };
        assert_eq!(state.ledger_high(), 9);
        let mut started = computed_run_started(9);
        assert_eq!(stamp(&mut state, &mut started), None);
        assert!(started.runner_tags().is_empty());
        assert_eq!(state, LineageTags::Computed { floor: 9 });
        let mut res = result("a");
        assert_eq!(stamp(&mut state, &mut res), None);
        assert!(res.runner_tags().is_empty());
        // A later run under the computed rules keeps the floor.
        state.fold(&computed_run_started(12));
        assert_eq!(state, LineageTags::Computed { floor: 9 });
        assert_eq!(LineageTags::unstarted().ledger_high(), 0);
        assert_eq!(LineageTags::Off.ledger_high(), 0);
    }

    fn user(text: &str) -> broca_protocol::Message {
        broca_protocol::Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
    }

    const RENDER: broca_protocol::tags::ComputedRules = broca_protocol::tags::ComputedRules {
        render: true,
        clean: true,
    };

    fn numbers(prompt: &[broca_protocol::Message]) -> Vec<Option<u64>> {
        prompt
            .iter()
            .flat_map(|message| message.part_tags().unwrap().parts.clone())
            .collect()
    }

    /// A Setup checkpoint anchors numbering at its cut: the cut message and every later one are
    /// numbered after the checkpoint, earlier ones keep theirs.
    #[test]
    fn a_setup_checkpoint_renumbers_from_its_cut() {
        let mut prompt = vec![user("a"), user("b"), user("c")];
        let mut numbering = TagNumbering::new(TagSeed::default(), 0);
        numbering.rules = Some(RENDER);
        numbering.number_new(&mut prompt).unwrap();
        assert_eq!(numbers(&prompt), vec![Some(1), Some(2), Some(3)]);
        let checkpoint = broca_wal::SetupTagCheckpoint {
            tag_checkpoint: 500,
            cut: "m1".into(),
        };
        assert!(numbering
            .anchor_setup(&mut prompt, 0, &[], "s", &checkpoint)
            .unwrap());
        assert_eq!(numbers(&prompt), vec![Some(1), Some(501), Some(502)]);
        assert_eq!(numbering.high(), 502);
        prompt.push(user("d"));
        numbering.number_new(&mut prompt).unwrap();
        assert_eq!(numbers(&prompt)[3], Some(503));
        // A cut outside this stretch of the transcript cannot anchor numbering.
        let gone = broca_wal::SetupTagCheckpoint {
            tag_checkpoint: 900,
            cut: "m40".into(),
        };
        assert!(!numbering
            .anchor_setup(&mut prompt, 0, &[], "s", &gone)
            .unwrap());
        assert_eq!(numbers(&prompt)[3], Some(503));
    }

    /// An in-effect marker carrying a checkpoint for the session wins over the Setup
    /// checkpoint (llm-runner §9.6), at the marker's own cut.
    #[test]
    fn an_in_effect_marker_checkpoint_wins_over_the_setup_checkpoint() {
        use cortexkit_role_llm_runner::tags::{Marker, MarkerCheckpoint};
        let mut prompt = vec![user("a"), user("b"), user("c")];
        let mut numbering = TagNumbering::new(TagSeed::default(), 0);
        numbering.rules = Some(RENDER);
        numbering.number_new(&mut prompt).unwrap();
        let checkpoint = broca_wal::SetupTagCheckpoint {
            tag_checkpoint: 500,
            cut: "m0".into(),
        };
        let markers = [Marker::new("m2", true).with_checkpoint(MarkerCheckpoint::new(40, "s"))];
        assert!(numbering
            .anchor_setup(&mut prompt, 0, &markers, "s", &checkpoint)
            .unwrap());
        assert_eq!(numbers(&prompt), vec![Some(1), Some(2), Some(41)]);
        // A marker no longer in effect is no anchor: the Setup checkpoint applies.
        let mut prompt = vec![user("a"), user("b"), user("c")];
        let mut numbering = TagNumbering::new(TagSeed::default(), 0);
        numbering.rules = Some(RENDER);
        numbering.number_new(&mut prompt).unwrap();
        let reverted = [Marker::new("m2", false).with_checkpoint(MarkerCheckpoint::new(40, "s"))];
        assert!(numbering
            .anchor_setup(&mut prompt, 0, &reverted, "s", &checkpoint)
            .unwrap());
        assert_eq!(numbers(&prompt), vec![Some(501), Some(502), Some(503)]);
    }

    /// A WAL cut right after the Setup record, before any request rendered a number, replays to
    /// the anchored numbers, and so does every later read: the anchor is the recorded Setup
    /// checkpoint, never memory.
    #[test]
    fn a_crash_after_setup_replays_the_anchored_numbers() {
        let setup = WalRecord::CompactionMessage {
            provider: "provider".into(),
            kind: broca_wal::CompactionMessageKind::Setup,
            compaction_id: "head".into(),
            version: 1,
            range: broca_wal::CompactionRange { from: 0, to: 0 },
            replacement: Arc::new(Vec::new()),
            sha256: broca_wal::compaction_replacement_sha256(&[]).unwrap(),
            request_id: None,
            cursor: None,
            call_when: None,
            stability: None,
            ceiling: None,
            tag_checkpoint: Some(broca_wal::SetupTagCheckpoint {
                tag_checkpoint: 500,
                cut: "m0".into(),
            }),
            ride_wait_ms: None,
        };
        let records = vec![
            computed_run_started(0),
            WalRecord::StepStarted {
                step_id: broca_protocol::StepId(0),
            },
            setup,
        ];
        // The checkpoint is must-understand: a build that cannot read it refuses the frame.
        assert!(records[2]
            .required_features()
            .contains(&broca_wal::framing::RUNNER_TAGS_V2_FEATURE));
        let replayed = crate::replay::replay(&records).unwrap();
        assert_eq!(numbers(&replayed.prompt), vec![Some(501)]);
        // Before the Setup answer was durable the same run numbers from the session start.
        let unanchored = crate::replay::replay(&records[..2]).unwrap();
        assert_eq!(numbers(&unanchored.prompt), vec![Some(1)]);
    }

    /// A run cleans the model's text exactly when it renders tags under the computed rules,
    /// except a `final_text` structured-output run, whose final text is its output.
    #[test]
    fn only_a_tag_rendering_run_that_is_not_final_text_output_cleans() {
        let computed = Some(broca_wal::ComputedTagRules { checkpoint: 0 });
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

    #[test]
    fn an_untagged_first_episode_keeps_the_lineage_untagged() {
        let mut state = LineageTags::unstarted();
        let started = run_started();
        state.fold(&started);
        assert_eq!(state, LineageTags::Off);
        let mut later = result("a");
        assert_eq!(stamp(&mut state, &mut later), None);
        assert!(later.runner_tags().is_empty());
    }

    #[test]
    fn stamping_counts_parts_in_wal_order_from_the_floor() {
        let mut state = LineageTags::unstarted().with_floor(40);
        let mut started = run_started();
        let assigned = stamp(&mut state, &mut started).expect("first episode is tagged");
        assert_eq!(assigned, vec![MessageTags::new(vec![40])]);
        let mut two = steer(2);
        assert_eq!(
            stamp(&mut state, &mut two),
            Some(vec![MessageTags::new(vec![41, 42])])
        );
        let mut res = result("a");
        stamp(&mut state, &mut res);
        assert_eq!(res.runner_tags(), &[MessageTags::new(vec![43])]);
        assert_eq!(state, LineageTags::On { next: 44 });

        // Folding the written records gives the writer's own next tag back.
        let folded = lineage_tags([&started, &two, &res]);
        assert_eq!(folded, LineageTags::On { next: 44 });
    }

    fn scratch(tag: &str) -> std::path::PathBuf {
        let dir = broca_test_support::scratch_root()
            .join(format!("broca-tags-{}-{tag}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    /// Every tag in `store`'s WAL for `session`, in WAL order.
    fn wal_tags(store: &crate::FileWalStore, session: &broca_protocol::BindIdentity) -> Vec<u64> {
        use crate::WalStore;
        store
            .read_records(session)
            .unwrap()
            .iter()
            .flat_map(|frame| frame.record.runner_tags().to_vec())
            .flat_map(|tags| tags.parts)
            .collect()
    }

    /// Open a writer on an existing WAL the way admission does: fold the lineage's tag state
    /// from its records and seed the writer with it.
    fn reopen(
        store: &crate::FileWalStore,
        session: &broca_protocol::BindIdentity,
    ) -> crate::WalHandle {
        use crate::WalStore;
        let records = store.read_records(session).unwrap();
        let checkpoint = crate::AdmissionCheckpointV1::from_frames(&records).unwrap();
        let writer = crate::WalHandle::spawn(store.open(session).unwrap(), 1).unwrap();
        assert_eq!(
            writer.lineage_tags(),
            None,
            "a non-empty WAL starts unknown"
        );
        writer.seed_lineage_tags(checkpoint.tags);
        writer
    }

    fn strictly_increasing(tags: &[u64]) -> bool {
        tags.windows(2).all(|pair| pair[0] < pair[1])
    }

    /// Tags keep counting, never repeating, across a restart (a new writer over the same WAL,
    /// seeded from its records), a resumed episode, and an agent-data move (the WAL's bytes
    /// copied to another root and continued there).
    #[tokio::test]
    async fn tags_are_strictly_increasing_across_restart_resume_and_a_moved_session() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "tags");
        let store = crate::FileWalStore::new(scratch("origin"));

        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        assert_eq!(writer.lineage_tags(), Some(LineageTags::unstarted()));
        let (_, input) = writer.record_tagged(run_started());
        assert_eq!(input, Some(vec![MessageTags::new(vec![1])]));
        writer.record(two_block_steer());
        writer.record(result("a"));
        writer.shutdown().await.unwrap();
        assert_eq!(wal_tags(&store, &session), vec![1, 2, 3, 4]);

        // A restart: a fresh writer over the same WAL.
        let writer = reopen(&store, &session);
        assert_eq!(writer.lineage_tags(), Some(LineageTags::On { next: 5 }));
        writer.record(result("b"));
        writer.shutdown().await.unwrap();

        // A resume of the episode after a crash, through another restart.
        let writer = reopen(&store, &session);
        writer.record(result("c"));
        writer.record(two_block_steer());
        writer.shutdown().await.unwrap();
        let before_move = wal_tags(&store, &session);
        assert_eq!(before_move, vec![1, 2, 3, 4, 5, 6, 7, 8]);

        // An agent-data move: the WAL's bytes, as they are, under another root.
        let moved = crate::FileWalStore::new(scratch("moved"));
        let destination = moved.wal_path(&session);
        std::fs::create_dir_all(destination.parent().unwrap()).unwrap();
        std::fs::copy(store.wal_path(&session), &destination).unwrap();
        let writer = reopen(&moved, &session);
        writer.record(result("d"));
        writer.shutdown().await.unwrap();
        let after_move = wal_tags(&moved, &session);
        assert_eq!(&after_move[..before_move.len()], before_move.as_slice());
        assert_eq!(after_move.last(), Some(&9));
        assert!(strictly_increasing(&after_move), "{after_move:?}");
        assert!(moved.read_records(&session).unwrap().len() > 1);
    }

    /// A writer reopened over a non-empty WAL knows nothing of its lineage's tags until an
    /// admission seeds it. It refuses a record with parts rather than write it untagged, and
    /// its next barrier fails, so nothing can claim the record durable.
    #[tokio::test]
    async fn an_unseeded_writer_refuses_a_record_with_parts() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "unseeded");
        let store = crate::FileWalStore::new(scratch("unseeded"));
        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        writer.record(run_started());
        writer.shutdown().await.unwrap();

        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        assert_eq!(writer.lineage_tags(), None);
        // A record without parts is written as before.
        assert!(matches!(
            writer.record(WalRecord::TurnFinished {
                step_id: broca_protocol::StepId(1)
            }),
            crate::RecordOutcome::Accepted(_)
        ));
        assert!(matches!(
            writer.record(result("a")),
            crate::RecordOutcome::Refused(_)
        ));
        assert!(writer.is_poisoned());
        assert!(writer.sync().await.is_err());
        assert!(!store
            .read_records(&session)
            .unwrap()
            .iter()
            .any(|frame| matches!(frame.record, WalRecord::ToolResult { .. })));
    }

    /// A lineage whose first episode was written without tags (by a build before tags) stays
    /// untagged: later records keep their exact, ungated bytes.
    #[tokio::test]
    async fn a_lineage_started_untagged_is_never_tagged() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "untagged");
        let store = crate::FileWalStore::new(scratch("untagged"));
        {
            let mut wal = store.open(&session).unwrap();
            wal.append(1, &run_started(), true).unwrap();
        }
        let writer = reopen(&store, &session);
        assert_eq!(writer.lineage_tags(), Some(LineageTags::Off));
        writer.record(result("a"));
        writer.shutdown().await.unwrap();
        assert!(wal_tags(&store, &session).is_empty());
        let plain = store.read_records(&session).unwrap();
        assert!(plain.iter().all(|frame| !frame
            .record
            .required_features()
            .contains(&broca_wal::framing::RUNNER_TAGS_FEATURE)));
    }

    /// A lineage created at an address whose previous lineage retention deleted starts past
    /// every tag the deleted one used, as its tombstone recorded.
    #[tokio::test]
    async fn a_lineage_after_retention_deletion_never_reuses_a_tag() {
        use crate::WalStore;
        let session = broca_protocol::BindIdentity::new("/tmp/p", "h", "deleted");
        let store = crate::FileWalStore::new(scratch("deleted"));
        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        writer.record(run_started());
        writer.record(result("a"));
        writer.shutdown().await.unwrap();
        let deleted = wal_tags(&store, &session);
        let next = lineage_tags(
            store
                .read_records(&session)
                .unwrap()
                .iter()
                .map(|frame| &frame.record),
        )
        .next()
        .unwrap();

        // Retention moves the WAL away and leaves a tombstone naming the next tag.
        std::fs::remove_file(store.wal_path(&session)).unwrap();
        let stone = store.tombstone_path(&session, "deleted-lineage");
        std::fs::create_dir_all(stone.parent().unwrap()).unwrap();
        std::fs::write(
            &stone,
            serde_json::json!({"lineage_id":"deleted-lineage","expired_at_ms":1,"seq":1,"next_tag":next})
                .to_string(),
        )
        .unwrap();
        assert_eq!(store.retired_tag_floor(&session).unwrap(), next);

        let writer = crate::WalHandle::spawn(store.open(&session).unwrap(), 1).unwrap();
        writer.seed_lineage_tags(Some(
            LineageTags::unstarted().with_floor(store.retired_tag_floor(&session).unwrap()),
        ));
        writer.record(run_started());
        writer.shutdown().await.unwrap();
        let fresh = wal_tags(&store, &session);
        assert_eq!(fresh, vec![next]);
        assert!(deleted.iter().all(|tag| *tag < fresh[0]));
    }
}
