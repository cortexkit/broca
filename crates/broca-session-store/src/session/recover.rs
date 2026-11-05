//! Crash recovery: repairing a damaged open log as one change.
//!
//! [`crate::damage::analyse`] decides what a damaged log holds. When it finds debris (damage
//! past the durable bound, which only unsynced writes can leave), loading refuses with
//! [`SessionError::NeedsRecovery`], and [`Session::recover`] repairs the session as one change:
//!
//! 1. **Intent in the head.** One compare-and-swap sets the head's residence to `Recovering(C)`
//!    and mints a writer generation. The damaged log is never written to again. A crash from here
//!    to the commit leaves that residence; the next recovery deletes what C created and runs
//!    again on the unchanged log.
//! 2. **Keep.** Every ledger record that authenticated is kept, wherever it lies: each is a fact
//!    that was really written. The transcript keeps every record before the first damaged
//!    region, then each later record whose back-link chains to the last one kept. From the first
//!    that doesn't chain, the rest are dropped, so the conversation never skips a message.
//! 3. **Close open work, if anything may have been lost.** When any region can't be proven
//!    loss-free, every run still open ends interrupted, every call with no kept result gets
//!    `outcome_unknown` (and a transcript result, if its call part is kept), every queued send
//!    ends `delivery_unknown`, and a damage notice is placed after every kept entry. Nothing
//!    that may have run is sent again. A loss-free recovery closes nothing: its open work is a
//!    crash's, which the run loop resolves as after any crash.
//! 4. **Transcript change.** If transcript records may have been lost, the history version moves
//!    and the compaction provider is owed `transcript_changed` from the first entry the loss can
//!    change.
//! 5. **Seqs.** Records the recovery writes take seqs above every seq the damaged bytes could
//!    hold, so no seq is issued twice.
//! 6. **Write and commit.** A ledger segment holds the kept ledger records and a final batch:
//!    `ChangeAborted` for a change the damaged log shows never committed, `RecoveryCut`, and the
//!    recovery's `ChangeIntent`. A new open log holds a checkpoint, the kept transcript records
//!    and one recovery batch: `WriterOpened`, closing results, the damage notice and the ledger's
//!    closing records. The damaged bytes are saved for an operator for a week. Then one
//!    compare-and-swap makes the new log the session's open log.
//!
//! Not built yet: rebuilding a damaged checkpoint (recovery refuses as `CheckpointLost`), entry
//! facts for the records a recovery writes, steers pending inside an interrupted run, withdrawals
//! owed for provider-held calls, the crash-loop limit, and showing the damage notice at system
//! authority and keeping it in view until a model step has seen it (the run loop's part).

use std::collections::{BTreeMap, BTreeSet};

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role, ToolOutput, ToolResult};
use broca_protocol::ids::ToolCallId;
use sha2::{Digest as _, Sha256};

use crate::backend::{SessionAddr, SessionBackend};
use crate::checkpoint::Checkpoint;
use crate::codec::{hex, Digest, Id128, Message as _};
use crate::damage::{self, Lost, Verdict};
use crate::fold::conversation::Conversation;
use crate::frame::FRAME_OVERHEAD;
use crate::head::{self, PendingDeletion, Residence, SessionTriple};
use crate::idindex::OpenIds;
use crate::keys::{random_id, DeploymentKey};
use crate::log::{ChainBase, LogEncoder};
use crate::record::{
    CallOutcome, ChangeAborted, ChangeIntent, ChangeKind, Input, Placement, Record, RecoveryCut,
    RunEnd, SendEnded, Stream, ToolOutcome, ToolReply, WriterOpened,
};

use super::seal::{Created, Sealed, GRACE_MS};
use super::{chain_base, log_bind, Session, SessionError};

/// How long the damaged bytes a recovery removed are kept for an operator.
pub const REMOVED_KEEP_MS: u64 = 7 * 24 * 60 * 60 * 1000;

/// What a recovery did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Recovery {
    /// The log opens as it is: nothing to repair.
    NotNeeded,
    Recovered(Recovered),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recovered {
    pub change: Id128,
    /// The first damaged byte of the replaced log.
    pub at: u64,
    /// Some record may have been lost, so open work was closed.
    pub loss_aware: bool,
    /// Transcript records that authenticated but were dropped, because an earlier record of the
    /// transcript may be missing.
    pub transcript_dropped: usize,
    pub interrupted_runs: Vec<String>,
    pub unknown_calls: Vec<ToolCallId>,
    /// Calls whose ledger outcome was lost but whose transcript result was kept: their outcome
    /// is written from that result.
    pub derived_outcomes: Vec<ToolCallId>,
    pub ended_sends: Vec<String>,
    /// Records written from now on take seqs above this.
    pub removed_through: u64,
    /// The new history version, when transcript records may have been lost.
    pub history_version: Option<u64>,
}

/// What the kept records say about one call.
#[derive(Default)]
struct Call {
    run_id: Option<String>,
    batch_id: Option<String>,
    tool_name: Option<String>,
    call_key: Option<String>,
    /// Its ledger intent was kept.
    intent: bool,
    /// Its ledger outcome was kept.
    outcome: bool,
    /// Its ledger records belong to the damaged log: a ledger record there names it, its batch
    /// was open at the log's checkpoint, or the message that made it comes after everything the
    /// checkpoint sealed. Otherwise its outcome was sealed before this log, and it is settled.
    in_log: bool,
}

/// The run a ledger record names, if it names one.
fn ledger_run(record: &Record) -> Option<&str> {
    Some(match record {
        Record::RunStart(r) => &r.run_id,
        Record::RunEnd(r) => &r.run_id,
        Record::StepStart(r) => &r.run_id,
        Record::StepEnd(r) => &r.run_id,
        Record::BatchStart(r) => &r.run_id,
        Record::BatchEnd(r) => &r.run_id,
        Record::ToolIntent(r) => &r.run_id,
        Record::ToolOutcome(r) => &r.run_id,
        Record::CancelRequest(r) => &r.run_id,
        _ => return None,
    })
}

fn fold_error(error: impl std::fmt::Debug) -> SessionError {
    SessionError::Fold(format!("recovery: {error:?}"))
}

fn digest(bytes: &[u8]) -> Digest {
    Digest(Sha256::digest(bytes).into())
}

impl<'b, B: SessionBackend> Session<'b, B> {
    /// Repairs the session at `addr` if its open log holds debris, or finishes a recovery a
    /// crash interrupted. Afterwards, load the session as usual.
    pub async fn recover(
        backend: &'b B,
        addr: SessionAddr,
        triple: &SessionTriple,
        deployment: &DeploymentKey,
        now_ms: u64,
    ) -> Result<Recovery, SessionError> {
        let (stored, mut version) = backend
            .read_head(&addr)
            .await?
            .ok_or(SessionError::Missing)?;
        let (mut head, key) = head::open(&stored, deployment).map_err(SessionError::Head)?;
        if &head.triple != triple {
            return Err(SessionError::TripleMismatch);
        }
        let resuming = match head.residence {
            Residence::Recovering(change) => {
                // That recovery never committed: what it created is unreferenced.
                backend.delete_change(&addr, &change, version).await?;
                backend.delete_log(&addr, &change, version).await?;
                true
            }
            Residence::Local(()) => false,
            // Loading finishes a creation, which has no records to recover.
            Residence::Creating(_) => return Ok(Recovery::NotNeeded),
        };
        let open = head.open_log.clone().ok_or(SessionError::Backend(
            crate::backend::BackendError::Corrupt("local head without a log"),
        ))?;
        let end = backend.log_end(&addr, &open.change).await?;
        let bytes = backend.read_log(&addr, &open.change, 0..end).await?;
        let frame_key = key.frame_key();
        let bind = log_bind(&head.session_id, &open.change);
        let verdict =
            damage::analyse(&bytes, &frame_key, &bind, open.synced_len, chain_base, None)?;
        let debris = match verdict {
            Verdict::Debris(debris) => debris,
            Verdict::Opens(_) => {
                if resuming {
                    head.residence = Residence::Local(());
                    backend
                        .write_head(&addr, &head::seal(&head, &key), Some(version), version + 1)
                        .await?;
                }
                return Ok(Recovery::NotNeeded);
            }
            Verdict::Corrupt { at, durable_bound } => {
                return Err(SessionError::Corrupt { at, durable_bound })
            }
        };
        let Some(body) = &debris.checkpoint else {
            return Err(SessionError::CheckpointLost);
        };
        let checkpoint = Checkpoint::decode(body).map_err(SessionError::Checkpoint)?;
        let first_damage = debris.at();

        // The intent, in the head: nothing writes to the damaged log again.
        let change = random_id();
        head.residence = Residence::Recovering(change);
        head.generation = random_id();
        version = backend
            .write_head(&addr, &head::seal(&head, &key), Some(version), version + 1)
            .await?;

        // Keep every ledger record, and the transcript up to the first record that doesn't
        // chain to the last one kept.
        let mut transcript: Vec<(u64, u64, Record)> = Vec::new();
        let mut dropped: Vec<Record> = Vec::new();
        let mut ledger: Vec<(u64, Record)> = Vec::new();
        let mut last_transcript = checkpoint.transcript_base;
        for frame in &debris.harvested {
            let Some(record) = &frame.record else {
                continue;
            };
            match record.stream() {
                Stream::Ledger if frame.header.seq > checkpoint.covers_through => {
                    ledger.push((frame.header.seq, record.clone()));
                }
                Stream::Ledger => {}
                Stream::Transcript => {
                    let chains = dropped.is_empty()
                        && (frame.at < first_damage
                            || frame.header.prev_in_stream == last_transcript);
                    if chains {
                        last_transcript = Some(frame.header.seq);
                        transcript.push((
                            frame.header.seq,
                            frame.header.batch_first,
                            record.clone(),
                        ));
                    } else {
                        dropped.push(record.clone());
                    }
                }
            }
        }
        let transcript_loss = !dropped.is_empty()
            || debris
                .regions
                .iter()
                .any(|region| region.transcript == Lost::CantProve);
        let loss_aware = !debris.loss_free();
        let unattributed_risk = debris
            .regions
            .iter()
            .any(|region| region.ledger == Lost::CantProve);

        let mut conversation = checkpoint.conversation.clone();
        for (_, _, record) in &transcript {
            conversation.apply(record).map_err(fold_error)?;
        }
        let mut operations = checkpoint.operational.clone();
        for (_, record) in &ledger {
            operations.apply(record).map_err(fold_error)?;
        }

        // A change whose intent the damaged log holds never committed: the head's open log is
        // still this one.
        let mut final_batch = Vec::new();
        if let Some(crashed) = operations.open_change {
            backend.delete_change(&addr, &crashed, version).await?;
            backend.delete_log(&addr, &crashed, version).await?;
            final_batch.push(Record::ChangeAborted(ChangeAborted { change: crashed }));
        }

        // Where the transcript loss starts, by seq and by served ordinal.
        let (transcript_from, ordinal, history_version) = if transcript_loss {
            let from = last_transcript.unwrap_or(0) + 1;
            let ordinal = first_changed_entry(&conversation, dropped.first());
            (Some(from), Some(ordinal), Some(head.history_version + 1))
        } else {
            (None, None, None)
        };

        // Close open work.
        let mut closing_transcript = Vec::new();
        let mut closing_ledger = Vec::new();
        let mut recovered = Recovered {
            change,
            at: first_damage,
            loss_aware,
            transcript_dropped: dropped.len(),
            interrupted_runs: Vec::new(),
            unknown_calls: Vec::new(),
            derived_outcomes: Vec::new(),
            ended_sends: Vec::new(),
            removed_through: 0,
            history_version,
        };
        let mut notice = None;
        if loss_aware {
            let calls = known_calls(
                &checkpoint.operational,
                &conversation,
                &ledger,
                &transcript,
                checkpoint.covers_through,
            );
            let replied = replied_calls(&conversation, &transcript);
            let latest_run = operations.run.as_ref().map(|run| run.run_id.clone());
            let mut lost_parts = Vec::new();
            let open_turn: Vec<ToolCallId> = conversation
                .open_tool_turn()
                .map(call_ids)
                .unwrap_or_default();
            let in_transcript = transcript_calls(&conversation);
            for (id, call) in &calls {
                let run_id = call
                    .run_id
                    .clone()
                    .or_else(|| latest_run.clone())
                    .unwrap_or_default();
                let batch_id = call.batch_id.clone().unwrap_or_default();
                let has_reply = replied.contains(&id.0);
                if !call.outcome && (!has_reply || call.in_log) {
                    let (outcome, result_digest, is_error, reason) = if has_reply {
                        // Only its transcript result was kept: its outcome comes from that.
                        let result = transcript_result(&transcript, id).expect("replied");
                        recovered.derived_outcomes.push(id.clone());
                        let bytes = serde_json::to_vec(&result).expect("results serialize");
                        (
                            CallOutcome::Completed,
                            digest(&bytes),
                            result.is_error,
                            None,
                        )
                    } else {
                        recovered.unknown_calls.push(id.clone());
                        (
                            CallOutcome::Unknown,
                            digest(UNKNOWN_TEXT.as_bytes()),
                            true,
                            Some("storage_damage".to_string()),
                        )
                    };
                    closing_ledger.push(Record::ToolOutcome(ToolOutcome {
                        run_id: run_id.clone(),
                        batch_id: batch_id.clone(),
                        tool_call_id: id.clone(),
                        outcome,
                        result_digest,
                        is_error,
                        reason,
                        finished_at_ms: Some(now_ms),
                        detail: None,
                        hooks: None,
                    }));
                }
                if open_turn.contains(id) && !has_reply {
                    let text = if call.outcome {
                        LOST_OUTPUT_TEXT
                    } else {
                        UNKNOWN_TEXT
                    };
                    closing_transcript.push(Record::ToolReply(ToolReply {
                        run_id,
                        batch_id: call.batch_id.clone(),
                        result: ToolResult {
                            tool_call_id: id.clone(),
                            output: ToolOutput::error(text),
                            is_error: true,
                        },
                        finished_at_ms: Some(now_ms),
                        tags_v1: None,
                    }));
                } else if call.intent && !in_transcript.contains(&id.0) {
                    lost_parts.push(lost_part_line(id, call));
                }
            }
            for pending in &operations.queue {
                recovered.ended_sends.push(pending.submission_id.clone());
                closing_ledger.push(Record::SendEnded(SendEnded {
                    submission_id: pending.submission_id.clone(),
                    reason: Some(serde_json::json!({
                        "code": "delivery_unknown",
                        "cause": "storage_damage",
                    })),
                }));
            }
            for run_id in open_runs(&operations, &ledger, &transcript) {
                recovered.interrupted_runs.push(run_id.clone());
                closing_ledger.push(Record::RunEnd(RunEnd {
                    run_id,
                    reason: RunFinishReason::Interrupted,
                    usage: None,
                    ts_ms: Some(now_ms),
                    retries_used: 0,
                    interrupt_reason: Some("storage_damage".into()),
                    error: None,
                }));
            }
            notice = Some(Record::Input(Input {
                run_id: latest_run.unwrap_or_default(),
                placement: Placement::DamageNotice,
                message: Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: notice_text(&lost_parts),
                    }],
                ),
                written_at_ms: Some(now_ms),
                submission_id: None,
                notice: None,
                tags_v1: None,
                receipt_seq: None,
                pruned: Vec::new(),
            }));
        }

        // Seqs above every seq the removed bytes could hold.
        let highest = debris
            .harvested
            .iter()
            .filter(|frame| frame.record.is_some())
            .map(|frame| frame.header.seq)
            .max()
            .unwrap_or(0)
            .max(head.next_seq.saturating_sub(1))
            .max(checkpoint.next_seq.saturating_sub(1));
        let removed_len = bytes.len() as u64 - first_damage;
        let ceiling = highest + removed_len.div_ceil(FRAME_OVERHEAD as u64);
        let before_damage = debris
            .harvested
            .iter()
            .filter(|frame| frame.record.is_some() && frame.at < first_damage)
            .map(|frame| frame.header.seq + 1)
            .max()
            .unwrap_or(checkpoint.next_seq);
        let removed_from = transcript_from.unwrap_or(u64::MAX).min(before_damage);
        recovered.removed_through = ceiling;

        let name = hex(&change.0);
        let saved = format!("{name}/removed");
        final_batch.push(Record::RecoveryCut(RecoveryCut {
            change,
            at: first_damage,
            durable_bound: debris.durable_bound,
            removed_from,
            removed_through: ceiling,
            loss_aware,
            transcript_from,
            ordinal,
            history_version,
            unattributed_risk,
            saved: saved.clone(),
        }));
        final_batch.push(Record::ChangeIntent(ChangeIntent {
            change,
            kind: ChangeKind::Recover,
            cut: None,
        }));
        let mut seq = ceiling;
        for record in final_batch {
            seq += 1;
            operations.apply(&record).map_err(fold_error)?;
            ledger.push((seq, record));
        }
        let intent_seq = seq;
        let mut settled = operations;
        settled.open_change = None;

        let mut session = Session {
            backend,
            addr: addr.clone(),
            head,
            version,
            key: key.clone(),
            encoder: LogEncoder::new(frame_key.clone(), bind, 0, ChainBase::default()),
            log: open.change,
            poisoned: false,
            conversation: checkpoint.conversation.clone(),
            operations: settled.clone(),
            open_ids: OpenIds::default(),
            window_facts: BTreeMap::new(),
        };
        let created = async {
            let ledger_sealed = session
                .seal_ledger(&name, &ledger, &settled, now_ms)
                .await?;
            backend
                .put_object(&addr, &saved, &bytes[first_damage as usize..], version)
                .await?;

            // The new open log: a checkpoint, the kept transcript batch by batch, then the
            // recovery batch.
            let mut encoder = LogEncoder::new(
                frame_key.clone(),
                log_bind(&session.head.session_id, &change),
                intent_seq + 1,
                ChainBase {
                    transcript: checkpoint.transcript_base,
                    ledger: Some(intent_seq),
                },
            );
            backend.create_log(&addr, &change, version).await?;
            let mut out = encoder
                .checkpoint(
                    &Checkpoint {
                        covers_through: intent_seq,
                        next_seq: intent_seq + 1,
                        conversation: checkpoint.conversation.clone(),
                        operational: settled.clone(),
                        transcript_base: checkpoint.transcript_base,
                        ledger_base: Some(intent_seq),
                    }
                    .encode(),
                )?
                .into_written(&mut encoder);
            let mut conversation = checkpoint.conversation.clone();
            let mut window_facts = BTreeMap::new();
            let mut batch_start = 0;
            while batch_start < transcript.len() {
                let first = transcript[batch_start].1;
                let batch_end = transcript[batch_start..]
                    .iter()
                    .position(|(_, batch, _)| *batch != first)
                    .map_or(transcript.len(), |offset| batch_start + offset);
                let kept: Vec<(u64, &Record)> = transcript[batch_start..batch_end]
                    .iter()
                    .map(|(seq, _, record)| (*seq, record))
                    .collect();
                for (seq, record) in &kept {
                    conversation.apply(record).map_err(fold_error)?;
                    if let Record::EntryFacts(facts) = record {
                        window_facts.insert(*seq, facts.ordinal);
                    }
                }
                out.extend(encoder.copy_batch(&kept)?.into_written(&mut encoder));
                batch_start = batch_end;
            }
            let mut batch = vec![Record::WriterOpened(WriterOpened {
                generation: session.head.generation,
            })];
            batch.extend(closing_transcript);
            batch.extend(notice);
            batch.extend(closing_ledger);
            let first = encoder.next_seq();
            let mut operations = settled.clone();
            let mut open_ids = OpenIds::default();
            for (offset, record) in batch.iter().enumerate() {
                match record.stream() {
                    Stream::Transcript => conversation.apply(record).map_err(fold_error)?,
                    Stream::Ledger => {
                        operations.apply(record).map_err(fold_error)?;
                        open_ids.add(first + offset as u64, record);
                    }
                }
            }
            out.extend(encoder.batch(&batch)?.into_written(&mut encoder));
            backend
                .append_log(&addr, &change, &out, 0, version, true)
                .await?;
            encoder.synced();
            backend.sync_objects(&addr, &change, version).await?;
            Ok::<_, SessionError>(Created {
                sealed: Sealed {
                    change,
                    transcript_records: 0,
                    ledger_records: ledger.len(),
                    kept_records: transcript.len(),
                    transcript_bytes: 0,
                    ledger_bytes: ledger_sealed.bytes,
                    index_entries: ledger_sealed.index_entries,
                    merged_entries: ledger_sealed.merged_entries,
                    forced_merges: ledger_sealed.forced_merges,
                },
                encoder,
                conversation,
                operations,
                window_facts,
                open_ids,
                transcript_dir: session.head.transcript_dir.clone(),
                ledger_dir: ledger_sealed.ledger_dir,
                id_index: ledger_sealed.id_index,
                retired_index: ledger_sealed.retired_index,
                replaced: ledger_sealed.replaced,
                history_version,
                abandoned: None,
                cold_since_ms: None,
            })
        }
        .await?;

        // The commit. The saved bytes are kept a week, under a deletion entry of their own.
        session.head.residence = Residence::Local(());
        session.head.pending.push(PendingDeletion {
            change: random_id(),
            unreferenced_at_ms: (now_ms + REMOVED_KEEP_MS).saturating_sub(GRACE_MS),
            logs: Vec::new(),
            objects: vec![saved],
            retired_index: None,
            abandoned: None,
        });
        session.commit(change, intent_seq, created, now_ms).await?;
        Ok(Recovery::Recovered(recovered))
    }
}

/// The closing result of a call whose outcome was lost.
const UNKNOWN_TEXT: &str = "outcome_unknown: storage damage removed this tool call's records. It \
    may or may not have run. Check its effects before repeating it.";

/// The closing result of a call whose outcome was kept but whose result was lost.
const LOST_OUTPUT_TEXT: &str = "This tool call finished, but storage damage removed its output.";

fn notice_text(lost_parts: &[String]) -> String {
    let mut text = String::from(
        "Storage damage removed records at the end of this session's history. Actions after the \
         last message shown may have run without being recorded. Check the state of anything you \
         were changing before you continue.",
    );
    if !lost_parts.is_empty() {
        text.push_str(" These tool calls have records, but the messages that made them were lost:");
        for line in lost_parts {
            text.push_str("\n- ");
            text.push_str(line);
        }
    }
    text
}

fn lost_part_line(id: &ToolCallId, call: &Call) -> String {
    let outcome = if call.outcome {
        "finished"
    } else {
        "outcome_unknown"
    };
    format!(
        "{} (call {}): {outcome}",
        call.tool_name.as_deref().unwrap_or("a tool"),
        call.call_key.as_deref().unwrap_or(&id.0),
    )
}

fn call_ids(message: &Message) -> Vec<ToolCallId> {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call.tool_call_id.clone()),
            _ => None,
        })
        .collect()
}

/// Calls whose call part is on the kept transcript: in the window or the turn still open.
fn transcript_calls(conversation: &Conversation) -> BTreeSet<String> {
    conversation
        .messages
        .iter()
        .chain(conversation.open_tool_turn())
        .flat_map(call_ids)
        .map(|id| id.0)
        .collect()
}

/// Calls with a kept transcript result: in the window, or replied in the turn still open.
fn replied_calls(
    conversation: &Conversation,
    transcript: &[(u64, u64, Record)],
) -> BTreeSet<String> {
    let mut replied: BTreeSet<String> = conversation
        .messages
        .iter()
        .flat_map(|message| &message.content)
        .filter_map(|block| match block {
            ContentBlock::ToolResult(result) => Some(result.tool_call_id.0.clone()),
            _ => None,
        })
        .collect();
    for (_, _, record) in transcript {
        if let Record::ToolReply(reply) = record {
            replied.insert(reply.result.tool_call_id.0.clone());
        }
    }
    replied
}

fn transcript_result(transcript: &[(u64, u64, Record)], id: &ToolCallId) -> Option<ToolResult> {
    transcript
        .iter()
        .rev()
        .find_map(|(_, _, record)| match record {
            Record::ToolReply(reply) if &reply.result.tool_call_id == id => {
                Some(reply.result.clone())
            }
            _ => None,
        })
}

/// Every call a kept record or the folded state names, with what the kept records say about
/// it, in the order first named. A call named only by a closed entry in the window is settled
/// already and left out.
fn known_calls(
    operations: &crate::fold::operations::Operations,
    conversation: &Conversation,
    ledger: &[(u64, Record)],
    transcript: &[(u64, u64, Record)],
    covers_through: u64,
) -> Vec<(ToolCallId, Call)> {
    fn slot<'c>(calls: &'c mut Vec<(ToolCallId, Call)>, id: &ToolCallId) -> &'c mut Call {
        let at = match calls.iter().position(|(known, _)| known == id) {
            Some(at) => at,
            None => {
                calls.push((id.clone(), Call::default()));
                calls.len() - 1
            }
        };
        &mut calls[at].1
    }
    let mut calls: Vec<(ToolCallId, Call)> = Vec::new();
    if let Some(run) = operations.run.as_ref().filter(|run| run.finished.is_none()) {
        if let Some(batch) = &run.open_batch {
            for call in &batch.calls {
                let known = slot(&mut calls, &call.id);
                known.run_id = Some(run.run_id.clone());
                known.in_log = true;
            }
        }
    }
    for (_, record) in ledger {
        if let Some(id) = match record {
            Record::ToolIntent(r) => Some(&r.tool_call_id),
            Record::ToolHeld(r) => Some(&r.tool_call_id),
            Record::ToolOutcome(r) => Some(&r.tool_call_id),
            _ => None,
        } {
            slot(&mut calls, id).in_log = true;
        }
        match record {
            Record::BatchStart(start) => {
                for id in &start.planned {
                    let call = slot(&mut calls, id);
                    call.run_id = Some(start.run_id.clone());
                    call.batch_id = Some(start.batch_id.clone());
                    call.in_log = true;
                }
            }
            Record::ToolIntent(intent) => {
                let call = slot(&mut calls, &intent.tool_call_id);
                call.run_id = Some(intent.run_id.clone());
                call.batch_id = Some(intent.batch_id.clone());
                if intent.tool_name.is_some() {
                    call.tool_name = intent.tool_name.clone();
                }
                if intent.call_key.is_some() {
                    call.call_key = intent.call_key.clone();
                }
                call.intent = true;
            }
            Record::ToolHeld(held) => {
                let call = slot(&mut calls, &held.tool_call_id);
                call.batch_id = Some(held.batch_id.clone());
                call.call_key = Some(held.call_key.clone());
            }
            Record::ToolOutcome(outcome) => slot(&mut calls, &outcome.tool_call_id).outcome = true,
            _ => {}
        }
    }
    for (seq, _, record) in transcript {
        if let Record::Assistant(assistant) = record {
            for id in call_ids(&assistant.message) {
                let call = slot(&mut calls, &id);
                call.run_id.get_or_insert_with(|| assistant.run_id.clone());
                call.in_log |= *seq > covers_through;
            }
        }
    }
    if let Some(turn) = conversation.open_tool_turn() {
        for id in call_ids(turn) {
            slot(&mut calls, &id);
        }
    }
    calls
}

/// Runs a kept record or the folded state names that no kept record ends.
fn open_runs(
    operations: &crate::fold::operations::Operations,
    ledger: &[(u64, Record)],
    transcript: &[(u64, u64, Record)],
) -> Vec<String> {
    let mut named: Vec<String> = Vec::new();
    let mut ended: BTreeSet<String> = BTreeSet::new();
    let mut name = |id: &str| {
        if !named.iter().any(|known| known == id) {
            named.push(id.to_string());
        }
    };
    if let Some(run) = &operations.run {
        name(&run.run_id);
        if run.finished.is_some() {
            ended.insert(run.run_id.clone());
        }
    }
    for (_, record) in ledger {
        if let Some(id) = ledger_run(record) {
            name(id);
        }
        if let Record::RunEnd(end) = record {
            ended.insert(end.run_id.clone());
        }
    }
    for (_, _, record) in transcript {
        match record {
            Record::Input(input) if !input.run_id.is_empty() => name(&input.run_id),
            Record::Assistant(assistant) => name(&assistant.run_id),
            Record::ToolReply(reply) => name(&reply.run_id),
            _ => {}
        }
    }
    named.retain(|id| !ended.contains(id));
    named
}

/// The ordinal of the first served entry a transcript loss can change: the entry holding the
/// first dropped record, or the next entry when nothing kept says otherwise. The assistant
/// message still waiting for results is that next entry, at its reserved ordinal.
fn first_changed_entry(conversation: &Conversation, first_dropped: Option<&Record>) -> u64 {
    let next = conversation.placed + 1;
    match first_dropped {
        Some(Record::EntryFacts(facts)) => facts.ordinal.min(next),
        Some(Record::ResultAppend(append)) => conversation
            .messages
            .iter()
            .rposition(|message| call_ids(message).contains(&append.tool_call_id))
            .map_or(next, |index| conversation.window_start() + index as u64),
        _ => next,
    }
}

#[cfg(test)]
mod tests;
