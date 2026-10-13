//! The resume invariants every kill is checked against.
//!
//! Given the durable log a kill left (`prefix`) and the log after the resume ran (`after`),
//! each check below states one property that must hold whatever the kill point was.
//!
//! Which checks lean on replay, and which do not:
//!
//! - Derived from `replay(prefix)`: the expected terminal reason (check 3), whether the resume
//!   may call the provider or dispatch at all (check 5), the resume record's expected episode,
//!   counts, redo list and indeterminate list (check 6), and which calls must get an
//!   `outcome_unknown` result (check 7). These use the same classification the resume itself
//!   uses, so a wrong classification in replay would be mirrored here, not caught.
//! - Read from the logs and the scripted provider and plane directly: append-only log with
//!   gapless sequence numbers, unreused ids, the lineage id, one terminal per run and never
//!   Cancelled, no second dispatch of a call the prefix shows may have left, no duplicate
//!   intent or result, byte-identical re-renders, and a sendable conversation.
//!
//! The independent cross-check for the replay-derived part is the hand-written table in
//! `tests/it/hand_written_expectations.rs`: for every cut of a one-tool-call run it states the
//! terminal reason, whether the provider is called, whether the call is dispatched, and the
//! `outcome_unknown` count, without consulting replay.

use std::collections::{BTreeMap, HashMap, HashSet};

use broca_core::{replay, ReplayResolution, ReplayState};
use broca_protocol::{caller::RunFinishReason, content::ContentBlock, StepId, ToolCallId};
use broca_wal::{DispatchOutcome, ResumeRedo, WalRecord};

use crate::scenario::{check_tool_pairing, HarnessPlane, ScriptedProvider};
use crate::wal::LogImage;

/// What a check needs to know about one kill and the resume after it.
pub struct CheckInput<'a> {
    /// Names the kill in every failure message.
    pub label: &'a str,
    pub prefix: &'a LogImage,
    pub after: &'a LogImage,
    pub reason: RunFinishReason,
    pub expected_uninterrupted: RunFinishReason,
    pub provider: &'a ScriptedProvider,
    pub plane: &'a HarnessPlane,
    /// The request bytes of the uninterrupted run, by 0-based step index.
    pub reference_sent: &'a BTreeMap<usize, Vec<u8>>,
}

/// What a resume that continues the episode must list as redone, from replay's resolution:
/// the planned calls that never left the process, or the model step that has no durable
/// output. A resume that seals redoes nothing.
fn expected_redo(prior: &ReplayState) -> Vec<ResumeRedo> {
    match &prior.resolution {
        ReplayResolution::Resume {
            pending: Some(pending),
        } => pending
            .redispatch
            .iter()
            .map(|id| ResumeRedo::ToolDispatch {
                tool_call_id: id.clone(),
            })
            .collect(),
        ReplayResolution::Resume { pending: None } => vec![ResumeRedo::ModelStep {
            step_id: StepId(prior.completed_steps + 1),
        }],
        _ => Vec::new(),
    }
}

/// Run every check. Panics naming the kill and the property that failed.
pub fn check(input: &CheckInput<'_>) {
    let label = input.label;
    let prefix = input.prefix.records();
    let after_framed = input.after.framed();
    let after: Vec<WalRecord> = after_framed.iter().map(|f| f.record.clone()).collect();
    let prior = replay(&prefix).unwrap_or_else(|e| panic!("{label}: the prefix must replay: {e}"));
    let appended = &after[prefix.len().min(after.len())..];

    // 1. Ids and ordinals are never reused or renumbered.
    //    The resume appends only: what was durable stays exactly as it was.
    let prefix_framed = input.prefix.framed();
    assert!(
        after_framed.len() >= prefix_framed.len()
            && after_framed[..prefix_framed.len()] == prefix_framed[..],
        "{label}: the resume rewrote, dropped or renumbered durable records"
    );
    //    Sequence numbers stay 1-based and gapless across the kill.
    for (index, framed) in after_framed.iter().enumerate() {
        assert_eq!(
            framed.seq,
            index as u64 + 1,
            "{label}: record {index} carries seq {} (a sequence number was reused or skipped)",
            framed.seq
        );
    }
    //    Run ids, step ids within an episode and assistant message ids are never reused.
    let mut run_ids = HashSet::new();
    let mut message_ids = HashSet::new();
    let mut steps_in_episode = HashSet::new();
    for record in &after {
        match record {
            WalRecord::RunStarted { run_id, .. } => {
                assert!(
                    run_ids.insert(run_id.clone()),
                    "{label}: run id {} reused",
                    run_id.0
                );
                steps_in_episode.clear();
            }
            WalRecord::ModelStepFinished {
                step_id,
                assistant_message,
                ..
            } => {
                assert!(
                    steps_in_episode.insert(*step_id),
                    "{label}: step {} finished twice in one episode",
                    step_id.0
                );
                assert!(
                    message_ids.insert(assistant_message.message_id.clone()),
                    "{label}: assistant message id {} reused",
                    assistant_message.message_id.0
                );
            }
            _ => {}
        }
    }
    //    Every message the prefix had committed keeps its place and ordinal.
    let resumed =
        replay(&after).unwrap_or_else(|e| panic!("{label}: the resumed log must replay: {e}"));
    assert!(
        resumed.prompt.len() >= prior.prompt.len()
            && resumed.prompt[..prior.prompt.len()] == prior.prompt[..],
        "{label}: a committed message moved or was renumbered by the resume"
    );

    // 2. A lineage change is visible. A log file carries a lineage id (16 bytes minted when the
    //    file is created, naming that one log). The resumed log must keep the id it had at the
    //    kill: a resume that replaced the file would mint a new id, which this check sees.
    assert!(
        input.after.lineage_id().is_some(),
        "{label}: the resumed log has no lineage id"
    );
    if let Some(at_kill) = input.prefix.lineage_id() {
        assert_eq!(
            input.after.lineage_id(),
            Some(at_kill),
            "{label}: the resume silently changed the log's lineage id"
        );
    }

    // 3. Each run gets exactly one terminal, and Interrupted stays distinct from Cancelled.
    let mut terminals: Vec<(String, Vec<RunFinishReason>)> = Vec::new();
    for record in &after {
        match record {
            WalRecord::RunStarted { run_id, .. } => terminals.push((run_id.0.clone(), Vec::new())),
            WalRecord::RunFinished { reason, .. } => {
                if let Some((_, reasons)) = terminals.last_mut() {
                    reasons.push(*reason);
                }
            }
            _ => {}
        }
    }
    for (run_id, reasons) in &terminals {
        assert_eq!(
            reasons.len(),
            1,
            "{label}: run {run_id} has terminals {reasons:?}"
        );
        assert_ne!(
            reasons[0],
            RunFinishReason::Cancelled,
            "{label}: run {run_id} ended Cancelled though nobody cancelled it"
        );
    }
    let predicted = match &prior.resolution {
        ReplayResolution::Interrupted { .. } => RunFinishReason::Interrupted,
        ReplayResolution::LineageIdle { last_reason } => *last_reason,
        _ => input.expected_uninterrupted,
    };
    assert_eq!(
        input.reason, predicted,
        "{label}: the resume ended {:?}",
        input.reason
    );

    // 4. A tool call is dispatched at most once, and never again once it may have left.
    let mut already_left: HashSet<ToolCallId> = HashSet::new();
    for record in &prefix {
        match record {
            WalRecord::ToolDispatchIntent { tool_call_id, .. }
            | WalRecord::ToolNoDispatch { tool_call_id, .. } => {
                already_left.insert(tool_call_id.clone());
            }
            WalRecord::ToolResult { result, .. } => {
                already_left.insert(result.tool_call_id.clone());
            }
            _ => {}
        }
    }
    let mut dispatch_counts: HashMap<ToolCallId, usize> = HashMap::new();
    for id in input.plane.dispatched() {
        *dispatch_counts.entry(id).or_default() += 1;
    }
    for (id, count) in &dispatch_counts {
        assert!(*count <= 1, "{label}: {} dispatched {count} times", id.0);
        assert!(
            !already_left.contains(id),
            "{label}: {} was dispatched again though the prefix records it may have run",
            id.0
        );
    }
    //    No call carries two intents or two terminals in the resumed log.
    let mut intents: HashMap<(String, String), usize> = HashMap::new();
    let mut terminal_records: HashMap<(String, String), usize> = HashMap::new();
    for record in &after {
        match record {
            WalRecord::ToolDispatchIntent {
                batch_id,
                tool_call_id,
                ..
            } => {
                *intents
                    .entry((batch_id.0.clone(), tool_call_id.0.clone()))
                    .or_default() += 1
            }
            WalRecord::ToolNoDispatch {
                batch_id,
                tool_call_id,
                ..
            } => {
                *terminal_records
                    .entry((batch_id.0.clone(), tool_call_id.0.clone()))
                    .or_default() += 1
            }
            WalRecord::ToolResult {
                batch_id, result, ..
            } => {
                *terminal_records
                    .entry((batch_id.0.clone(), result.tool_call_id.0.clone()))
                    .or_default() += 1
            }
            _ => {}
        }
    }
    for ((batch, id), count) in intents.iter().chain(terminal_records.iter()) {
        assert_eq!(
            *count, 1,
            "{label}: call {id} of batch {batch} recorded {count} times"
        );
    }

    // 5. Recorded outputs replay without calling a provider. The resume never requests a step
    //    whose output is already in the log, a resume that seals the episode makes no provider
    //    call at all, and any step the resume does request has exactly the bytes the
    //    uninterrupted run sent for it.
    assert!(
        input.provider.rejected().is_empty(),
        "{label}: the provider refused a prompt: {:?}",
        input.provider.rejected()
    );
    let sent = input.provider.sent();
    let resumes_an_episode = prior.run_id.is_some() && !prior.run_finished_durable;
    if resumes_an_episode {
        for step in sent.keys() {
            assert!(
                *step as u64 >= prior.completed_steps,
                "{label}: step {} was requested again though its output is durable",
                step + 1
            );
        }
    }
    if matches!(
        prior.resolution,
        ReplayResolution::Interrupted { .. } | ReplayResolution::LineageIdle { .. }
    ) {
        assert_eq!(
            input.provider.request_count(),
            0,
            "{label}: a resume that seals the episode called the provider"
        );
        assert!(
            input.plane.dispatched().is_empty(),
            "{label}: a resume that seals the episode dispatched a tool"
        );
    }
    for (step, bytes) in &sent {
        let reference = input
            .reference_sent
            .get(step)
            .unwrap_or_else(|| panic!("{label}: the uninterrupted run never sent step {step}"));
        assert!(
            bytes == reference,
            "{label}: step {} re-rendered different bytes than the uninterrupted run",
            step + 1
        );
    }

    // 6. One resume record per resume, written before anything else the resume writes, saying
    //    what replay found; none when there was no unfinished episode to resume. The resume
    //    record never changes the reconstructed conversation and never appears in a provider
    //    request.
    let resume_records: Vec<&WalRecord> = appended
        .iter()
        .filter(|record| matches!(record, WalRecord::ResumeStarted { .. }))
        .collect();
    if resumes_an_episode {
        assert_eq!(
            resume_records.len(),
            1,
            "{label}: expected one resume record, got {resume_records:?}"
        );
        let expected_indeterminate = match &prior.resolution {
            ReplayResolution::Interrupted { indeterminate } => indeterminate.clone(),
            _ => Vec::new(),
        };
        let last_seq = input.prefix.framed().last().map_or(0, |framed| framed.seq);
        match &appended[0] {
            WalRecord::ResumeStarted {
                episode,
                resumed_from_seq,
                replayed,
                redone,
                indeterminate,
            } => {
                assert_eq!(
                    Some(episode),
                    prior.run_id.as_ref(),
                    "{label}: resume record episode"
                );
                assert_eq!(*resumed_from_seq, last_seq, "{label}: resume record seq");
                assert_eq!(
                    replayed.model_steps, prior.completed_steps,
                    "{label}: replayed steps"
                );
                assert_eq!(
                    replayed.messages,
                    prior.prompt.len() as u64,
                    "{label}: replayed messages"
                );
                assert_eq!(
                    indeterminate, &expected_indeterminate,
                    "{label}: indeterminate calls"
                );
                assert_eq!(redone, &expected_redo(&prior), "{label}: redone");
            }
            other => {
                panic!("{label}: the resume's first record is {other:?}, not its resume record")
            }
        }
    } else {
        assert!(
            resume_records.is_empty(),
            "{label}: a resume record with nothing to resume"
        );
    }
    let without_resume_records: Vec<WalRecord> = after
        .iter()
        .filter(|record| !matches!(record, WalRecord::ResumeStarted { .. }))
        .cloned()
        .collect();
    assert_eq!(
        replay(&without_resume_records).expect("replays").prompt,
        resumed.prompt,
        "{label}: a resume record changed the reconstructed conversation"
    );
    for bytes in sent.values() {
        assert!(
            !String::from_utf8_lossy(bytes).contains("resume_started"),
            "{label}: a resume record reached a request"
        );
    }

    // 7. When replay found an indeterminate call (an intent with no result: it may have run),
    //    the resume never dispatches it again, writes exactly one Unknown result for it, and
    //    seals the episode. The conversation any later request is built from then pairs every
    //    tool call with a result.
    if let ReplayResolution::Interrupted { indeterminate } = &prior.resolution {
        for id in indeterminate {
            let unknown = after
                .iter()
                .filter(|record| matches!(record, WalRecord::ToolResult { result, outcome: DispatchOutcome::Unknown, .. } if &result.tool_call_id == id))
                .count();
            assert_eq!(
                unknown, 1,
                "{label}: {} has {unknown} outcome_unknown results",
                id.0
            );
        }
        assert!(
            matches!(
                after.last(),
                Some(WalRecord::RunFinished {
                    reason: RunFinishReason::Interrupted,
                    ..
                })
            ),
            "{label}: the close-out must end with the Interrupted seal"
        );
        let seal_at = after.len() - 1;
        assert!(
            !after[prefix.len()..seal_at]
                .iter()
                .any(|record| matches!(record, WalRecord::RunFinished { .. })),
            "{label}: a terminal was written before the close-out's results"
        );
        assert!(
            resumed.open_tool_batch.is_none(),
            "{label}: the close-out left the batch open"
        );
    }
    check_tool_pairing(&resumed.prompt).unwrap_or_else(|why| {
        panic!("{label}: the conversation after the resume is unsendable: {why}")
    });
    // Every tool result in the conversation answers a call that is in it.
    let calls: HashSet<&ToolCallId> = resumed
        .prompt
        .iter()
        .flat_map(|m| m.content.iter())
        .filter_map(|b| match b {
            ContentBlock::ToolCall(call) => Some(&call.tool_call_id),
            _ => None,
        })
        .collect();
    for block in resumed.prompt.iter().flat_map(|m| m.content.iter()) {
        if let ContentBlock::ToolResult(result) = block {
            assert!(
                calls.contains(&result.tool_call_id),
                "{label}: orphan tool result"
            );
        }
    }
}
