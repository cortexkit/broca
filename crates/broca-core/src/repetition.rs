//! Repetition guidance reconstructed from raw tool results without changing durable history.

use crate::tool_repetition::{result_bytes, RepetitionFold, RepetitionObserver};
use broca_protocol::{ContentBlock, Message, ToolCall, ToolOutput, ToolResult};

pub mod store_guidance;

/// Whether folding `prompt` with a single observer produces guidance for any tool result.
pub(crate) fn repetition_guidance_fires(prompt: &[Message]) -> bool {
    let mut fold = RepetitionFold::default();
    prompt.iter().any(|message| {
        fold.push(message)
            .iter()
            .any(|(_, verdict)| verdict.guidance().is_some())
    })
}

pub(crate) fn append_repetition_guidance(result: &mut ToolResult, guidance: &str) {
    const LABEL: &str = "Tool repetition guidance: ";
    let suffix = format!("\n\n{LABEL}{guidance}");

    match &mut result.output {
        ToolOutput::Parts { parts } => {
            parts.push(broca_protocol::ToolOutputPart::Text { text: suffix })
        }
        ToolOutput::Text { text } => text.push_str(&suffix),
        ToolOutput::Denied { reason } => reason.push_str(&suffix),
        ToolOutput::Error { message, .. } => message.push_str(&suffix),
        ToolOutput::Json { value } => {
            // Structured outputs have no metadata slot. Preserve their JSON text before the note;
            // this copy is provider-facing only, while the durable result remains structured.
            let text = format!("{value}{suffix}");
            result.output = ToolOutput::Text { text };
        }
    }
}

fn attach_repetition_guidance_to_result(
    observer: &mut RepetitionObserver,
    call: &ToolCall,
    result: &mut ToolResult,
) {
    let bytes = result_bytes(&result.output);
    let verdict = observer.observe(call, bytes.as_ref(), result.is_error);
    if let Some(guidance) = verdict.guidance() {
        append_repetition_guidance(result, &guidance);
    }
}

pub(crate) fn attach_repetition_guidance<'a>(
    observer: &mut RepetitionObserver,
    calls: impl Iterator<Item = &'a ToolCall>,
    results: &mut [ToolResult],
) {
    for (call, result) in calls.zip(results) {
        attach_repetition_guidance_to_result(observer, call, result);
    }
}

/// Recreate guidance that an uninterrupted run retained in its in-memory prompt. Replay deliberately
/// reconstructs only the tool-owned output from the WAL, so this folds raw result copies in source
/// order and decorates those copies without changing durable history.
///
/// Pass one episode's model-step messages, excluding other episodes' messages and the
/// episode's initial input. Each call starts fresh repetition counters. The slice must
/// contain raw, undecorated tool outputs: applying this fold twice would duplicate warnings.
/// Offline renderers use this same fold so their reconstructed inputs receive exactly the
/// decoration the run loop sends to the provider.
pub fn restore_repetition_guidance(prompt: &mut [Message]) {
    restore_guidance_with_originals(prompt, 0, None);
}

/// [`restore_repetition_guidance`] for a run whose earlier steps aren't in `prompt`: counting
/// starts from `seed`, the run's [`repetition_checkpoint`] just before `prompt`'s first message,
/// so the warnings are the ones the whole run would get. False, with nothing changed, when the
/// checkpoint was taken under other warning rules or doesn't read as one.
pub fn restore_repetition_guidance_seeded(
    prompt: &mut [Message],
    seed: &crate::tool_repetition::RepetitionCheckpoint,
) -> bool {
    let Some(mut guidance) = RepetitionGuidance::seeded(seed) else {
        return false;
    };
    for message in prompt.iter_mut() {
        guidance.guide(message);
    }
    true
}

/// [`restore_repetition_guidance`] one message at a time, for a caller that keeps a run's
/// guided prompt and adds each new message as it comes: guiding a run's model-step messages
/// one by one, in order, gives each the warnings the whole-slice fold gives it.
pub struct RepetitionGuidance(RepetitionFold);

impl Default for RepetitionGuidance {
    fn default() -> Self {
        Self::new()
    }
}

impl RepetitionGuidance {
    /// Fresh counters, for a run's first model-step message.
    pub fn new() -> Self {
        Self(RepetitionFold::default())
    }

    /// Counters continuing from `seed`, as [`restore_repetition_guidance_seeded`] starts. `None`
    /// when the checkpoint was taken under other warning rules or doesn't read as one.
    pub fn seeded(seed: &crate::tool_repetition::RepetitionCheckpoint) -> Option<Self> {
        RepetitionFold::seeded(seed).map(Self)
    }

    /// Counts `message`, raw, then adds to its tool results the warnings due.
    pub fn guide(&mut self, message: &mut Message) {
        for (index, verdict) in self.0.push(message) {
            if let Some(guidance) = verdict.guidance() {
                if let ContentBlock::ToolResult(result) = &mut message.content[index] {
                    append_repetition_guidance(result, &guidance);
                }
            }
        }
    }
}

/// What [`repetition_checkpoint`] found.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepetitionCheckpointTaken {
    /// The counters after the steps given.
    Taken(crate::tool_repetition::RepetitionCheckpoint),
    /// An assistant's calls among the steps still wait for their results. A checkpoint keeps no
    /// call text to pair them with, so none can be taken there.
    CallsWaiting,
    /// The seed doesn't read as a checkpoint under today's warning rules.
    SeedRejected,
}

/// The repetition counters after a run's raw, undecorated steps `prompt`, from its first step,
/// continuing from `seed` when the run's steps before `prompt` were counted already.
pub fn repetition_checkpoint(
    prompt: &[Message],
    seed: Option<&crate::tool_repetition::RepetitionCheckpoint>,
) -> RepetitionCheckpointTaken {
    let mut fold = match seed {
        Some(seed) => match RepetitionFold::seeded(seed) {
            Some(fold) => fold,
            None => return RepetitionCheckpointTaken::SeedRejected,
        },
        None => RepetitionFold::default(),
    };
    for message in prompt {
        fold.push(message);
    }
    match fold.checkpoint() {
        Some(checkpoint) => RepetitionCheckpointTaken::Taken(checkpoint),
        None => RepetitionCheckpointTaken::CallsWaiting,
    }
}

pub(crate) fn restore_guidance_with_originals(
    prompt: &mut [Message],
    base: usize,
    mut originals: Option<&mut crate::compaction::OriginalMessages>,
) -> RepetitionObserver {
    let mut fold = RepetitionFold::default();
    for (index, message) in prompt.iter_mut().enumerate() {
        let verdicts = fold.push(message);
        let original = originals
            .as_ref()
            .filter(|_| !verdicts.is_empty())
            .map(|_| message.clone());
        for (block_index, verdict) in verdicts {
            if let Some(guidance) = verdict.guidance() {
                if let ContentBlock::ToolResult(result) = &mut message.content[block_index] {
                    append_repetition_guidance(result, &guidance);
                }
            }
        }
        if let (Some(originals), Some(original)) = (originals.as_deref_mut(), original) {
            originals.retain_change(base + index, original, message);
        }
    }
    fold.into_observer()
}
