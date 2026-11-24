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
//! **A damaged checkpoint** is rebuilt, not cut: cutting at the log's first batch would cut the
//! whole log. Every change that creates an open log keeps a copy of that log's checkpoint in the
//! ledger segment it seals, so the newest ledger segment holds it. A session's first log, which
//! no ledger segment precedes, started from empty state. With the rebuilt checkpoint, the records
//! after it chain to its bases, so a damaged checkpoint alone loses nothing.
//!
//! Not built yet: sends waiting to steer an interrupted run, withdrawals
//! owed for provider-held calls, the crash-loop limit, and showing the damage notice at system
//! authority and keeping it in view until a model step has seen it (the run loop's part).

use super::media::OpenLogIndex;
use std::collections::{BTreeMap, BTreeSet, HashMap};

use broca_protocol::caller::RunFinishReason;
use broca_protocol::content::{ContentBlock, Message, Role, ToolOutput, ToolResult};
use broca_protocol::ids::ToolCallId;
use sha2::{Digest as _, Sha256};

use crate::backend::{SessionAddr, SessionBackend};
use crate::checkpoint::Checkpoint;
use crate::codec::{hex, Digest, Id128, Message as _};
use crate::damage::{self, Lost, Verdict};
use crate::fold::conversation::{BlockKind, Conversation, WindowMessage};
use crate::head::{self, PendingDeletion, Residence, SessionTriple};
use crate::idindex::{self, OpenIds};
use crate::keys::{random_id, DeploymentKey};
use crate::log::{ChainBase, LogEncoder};
use crate::placement::{entry_facts, lifetime_tag_high, runner_notice};
use crate::record::{
    CallOutcome, ChangeAborted, ChangeIntent, ChangeKind, Input, Placement, Record, RecoveryCut,
    RunEnd, SendEnded, Stream, ToolOutcome, ToolReply, WriterOpened,
};

use super::seal::{Created, Sealed, GRACE_MS};
use super::{chain_base, log_bind, MediaFeed, MediaFold, Session, SessionError};
use crate::directory::{Pages, FANOUT};
use crate::segment::Segment;

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

/// The transcript names a turn by step. Ledger records can still name its batch even when
/// nothing kept connects that batch to a step.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
enum Turn<'a> {
    Step(&'a str, u64),
    Batch(&'a str, &'a str),
}

impl<'a> Turn<'a> {
    fn run_id(self) -> &'a str {
        match self {
            Self::Step(run, _) | Self::Batch(run, _) => run,
        }
    }
}

type CallKey<'a> = (Turn<'a>, &'a ToolCallId);

/// What the kept records say about one call in one turn.
#[derive(Default)]
struct Call<'a> {
    batch_id: Option<&'a str>,
    tool_name: Option<&'a str>,
    call_key: Option<&'a str>,
    on_transcript: bool,
    reply: Option<&'a ToolResult>,
    /// Its ledger intent was kept.
    intent: bool,
    /// Its ledger outcome was kept.
    outcome: bool,
    /// Its ledger records belong to the damaged log: a ledger record there names it, its batch
    /// was open at the log's checkpoint, or the message that made it comes after everything the
    /// checkpoint sealed. Otherwise its outcome was sealed before this log, and it is settled.
    in_log: bool,
}

/// Keep first-seen order when writing closing records and the notice.
#[derive(Default)]
struct Calls<'a> {
    indices: HashMap<CallKey<'a>, usize>,
    entries: Vec<(CallKey<'a>, Call<'a>)>,
}

impl<'a> Calls<'a> {
    fn slot(
        &mut self,
        turn: Turn<'a>,
        id: &'a ToolCallId,
        batch: Option<&'a str>,
    ) -> &mut Call<'a> {
        let key = (turn, id);
        let at = *self.indices.entry(key).or_insert_with(|| {
            self.entries.push((key, Call::default()));
            self.entries.len() - 1
        });
        let call = &mut self.entries[at].1;
        call.batch_id = call.batch_id.or(batch);
        call
    }
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

/// Folds a record the recovery keeps into the media fold it continues, when the damaged log's
/// checkpoint named one, and the record comes after what that fold was current through. Nothing
/// is handed over: the recovery's session is loaded again before anyone reads its media.
fn fold_media(
    media: &mut Option<MediaFold>,
    through: u64,
    seq: u64,
    record: &Record,
    conversation: &Conversation,
    operations: &crate::fold::operations::Operations,
) {
    if let Some(fold) = media.as_mut().filter(|_| seq > through) {
        fold.apply(
            seq,
            record,
            conversation,
            operations,
            &mut MediaFeed::default(),
        );
    }
}

fn digest(bytes: &[u8]) -> Digest {
    Digest(Sha256::digest(bytes).into())
}

fn kept_record((seq, _, record): &(u64, u64, Record)) -> (u64, &Record) {
    (*seq, record)
}

impl<B: SessionBackend> Session<B> {
    /// Repairs the session at `addr` if its open log holds debris, or finishes a recovery a
    /// crash interrupted. Afterwards, load the session as usual.
    pub async fn recover(
        backend: B,
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
            Residence::Moving(cut_id) => return Err(SessionError::SessionMoving { cut_id }),
            Residence::Moved(moved) => {
                return Err(SessionError::SessionMoved {
                    holder: moved.holder_box,
                })
            }
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
        let (debris, body) = match debris.checkpoint.clone() {
            Some(body) => (debris, body),
            None => {
                let body = rebuild_checkpoint(&backend, &addr, &head, &key, &open.change).await?;
                // Analyse again from the rebuilt checkpoint's chain bases, so a damaged
                // checkpoint alone reads as losing nothing.
                let base = chain_base(&body).map_err(SessionError::Fold)?;
                let verdict = damage::analyse(
                    &bytes,
                    &frame_key,
                    &bind,
                    open.synced_len,
                    chain_base,
                    Some(base),
                )?;
                match verdict {
                    Verdict::Debris(debris) => (debris, body),
                    Verdict::Corrupt { at, durable_bound } => {
                        return Err(SessionError::Corrupt { at, durable_bound })
                    }
                    Verdict::Opens(_) => return Err(SessionError::CheckpointLost),
                }
            }
        };
        let checkpoint = Checkpoint::decode(&body).map_err(SessionError::Checkpoint)?;
        let first_damage = debris.at();
        // The media fold continues from the damaged log's checkpoint over the records the
        // recovery keeps, as a load would; the recovery's cut leaves no saved index.
        let (mut media, media_through, media_object) =
            match super::media_index::recovery_start(&checkpoint) {
                Some((fold, through, object)) => (Some(fold), through, object),
                None => (None, 0, None),
            };

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

        // In seq order: a session's enrollment in the current compaction contract changes how
        // the conversation folds the transcript records after it (`Conversation::enrolled`).
        let mut conversation = checkpoint.conversation.clone();
        let mut operations = checkpoint.operational.clone();
        let mut reply_fallbacks =
            super::model_fallback::ReplyFallbacks::from_operations(&operations);
        let mut activity =
            OpenLogIndex::from_checkpoint(checkpoint.activity_ms, checkpoint.next_seq);
        conversation.enrolled(operations.enrolled_through());
        let mut kept_transcript = transcript.iter().map(kept_record).peekable();
        for (seq, record) in &ledger {
            reply_fallbacks.note(*seq, record);
            while let Some((kept_seq, kept)) =
                kept_transcript.next_if(|(transcript_seq, _)| transcript_seq < seq)
            {
                fold_media(
                    &mut media,
                    media_through,
                    kept_seq,
                    kept,
                    &conversation,
                    &operations,
                );
                conversation
                    .apply_at(Some(kept_seq), kept)
                    .map_err(fold_error)?;
                activity.note(kept_seq, kept);
            }
            fold_media(
                &mut media,
                media_through,
                *seq,
                record,
                &conversation,
                &operations,
            );
            operations.apply(record).map_err(fold_error)?;
            activity.note(*seq, record);
            conversation.enrolled(operations.enrolled_through());
        }
        for (seq, kept) in kept_transcript {
            fold_media(
                &mut media,
                media_through,
                seq,
                kept,
                &conversation,
                &operations,
            );
            conversation.apply_at(Some(seq), kept).map_err(fold_error)?;
            activity.note(seq, kept);
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
            let (calls, open_turn) = known_calls(
                &checkpoint.operational,
                &conversation,
                &ledger,
                &transcript,
                checkpoint.covers_through,
            );
            let latest_run = operations.run.as_ref().map(|run| run.run_id.clone());
            let mut lost_parts = Vec::new();
            for ((turn, id), call) in calls.entries {
                let run_id = turn.run_id();
                if !call.outcome && (call.reply.is_none() || call.in_log) {
                    let (outcome, result_digest, is_error, reason) =
                        if let Some(result) = call.reply {
                            // Only its transcript result was kept: its outcome comes from that.
                            recovered.derived_outcomes.push(id.clone());
                            let bytes = serde_json::to_vec(result).expect("results serialize");
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
                        yielded_by: None,
                        run_id: run_id.to_owned(),
                        batch_id: call.batch_id.unwrap_or_default().to_owned(),
                        tool_call_id: id.clone(),
                        outcome,
                        result_digest,
                        is_error,
                        reason,
                        finished_at_ms: Some(now_ms),
                        detail: None,
                        hooks: None,
                        call_key: None,
                    }));
                }
                if open_turn == Some(turn) && call.on_transcript && call.reply.is_none() {
                    let text = if call.outcome {
                        LOST_OUTPUT_TEXT
                    } else {
                        UNKNOWN_TEXT
                    };
                    closing_transcript.push(Record::ToolReply(ToolReply {
                        run_id: run_id.to_owned(),
                        batch_id: call.batch_id.map(str::to_owned),
                        result: ToolResult {
                            tool_call_id: id.clone(),
                            output: ToolOutput::error(text),
                            is_error: true,
                        },
                        finished_at_ms: Some(now_ms),
                        tags_v1: None,
                        media: Vec::new(),
                    }));
                } else if call.intent && !call.on_transcript {
                    lost_parts.push(lost_part_line(id, &call));
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
                media: Vec::new(),
                run_id: latest_run.unwrap_or_default(),
                placement: Placement::DamageNotice,
                message: std::sync::Arc::new(Message::new(
                    Role::User,
                    vec![ContentBlock::Text {
                        text: notice_text(&lost_parts).into(),
                    }],
                )),
                written_at_ms: Some(now_ms),
                submission_id: None,
                notice: None,
                tags_v1: None,
                receipt_seq: None,
                pruned: Vec::new(),
            }));
        }

        // Seqs above every seq the removed bytes could hold: no frame is smaller than the
        // overhead its session's key seals a frame with.
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
        let ceiling = highest + removed_len.div_ceil(frame_key.frame_overhead() as u64);
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
            fold_media(
                &mut media,
                media_through,
                seq,
                &record,
                &conversation,
                &operations,
            );
            operations.apply(&record).map_err(fold_error)?;
            ledger.push((seq, record));
        }
        let intent_seq = seq;
        if let Some(fold) = media.as_mut() {
            fold.flush(&mut MediaFeed::default());
        }
        let media = super::media_index::recovery_commit(media, intent_seq);
        let mut settled = operations;
        settled.open_change = None;

        let activity_ms = activity.newest_activity_ms;
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
            operations: Box::new(settled.clone()),
            open_ids: OpenIds::new(idindex::hash_key(&key)),
            window: OpenLogIndex::from_checkpoint(activity_ms, intent_seq + 1),
            imported: Default::default(),
            enrollment: Default::default(),
            sealed_entries: Default::default(),
            observer: None,
            kept: super::kept::KeptRecords::from_next(intent_seq + 1, false),
            media: Default::default(),
            memory_only: Default::default(),
            memory_refused: Default::default(),
            retired_tag_floor: 1,
            interner: Default::default(),
            reply_fallbacks,
        };
        let checkpoint_body = Checkpoint {
            covers_through: intent_seq,
            next_seq: intent_seq + 1,
            conversation: checkpoint.conversation.clone(),
            operational: settled.clone(),
            transcript_base: checkpoint.transcript_base,
            ledger_base: Some(intent_seq),
            activity_ms,
            media: media.member(),
        }
        .encode();
        let created = async {
            let ledger_sealed = session
                .seal_ledger(&name, &ledger, &settled, &checkpoint_body, now_ms)
                .await?;
            session
                .backend()
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
            session
                .backend()
                .create_log(&addr, &change, version)
                .await?;
            let mut out = encoder
                .checkpoint(&checkpoint_body)?
                .into_written(&mut encoder);
            let mut conversation = checkpoint.conversation.clone();
            let mut window = OpenLogIndex::from_checkpoint(activity_ms, intent_seq + 1);
            let mut batch_start = 0;
            while batch_start < transcript.len() {
                let first = transcript[batch_start].1;
                let batch_end = transcript[batch_start..]
                    .iter()
                    .position(|(_, batch, _)| *batch != first)
                    .map_or(transcript.len(), |offset| batch_start + offset);
                let kept: Vec<(u64, &Record)> = transcript[batch_start..batch_end]
                    .iter()
                    .map(kept_record)
                    .collect();
                for (seq, record) in &kept {
                    conversation
                        .apply_at(Some(*seq), record)
                        .map_err(fold_error)?;
                    window.note(*seq, record);
                }
                out.extend(encoder.copy_batch(&kept)?.into_written(&mut encoder));
                batch_start = batch_end;
            }
            let mut batch = vec![Record::WriterOpened(WriterOpened {
                generation: session.head.generation,
            })];
            let first = encoder.next_seq();
            let prefix = session.message_id_prefix();
            let on_contract = settled.on_contract();
            let tag_base = if loss_aware && on_contract {
                lifetime_tag_high(&conversation, &settled).map_err(SessionError::Fold)?
            } else {
                0
            };
            let mut ordinal = conversation.placed;
            batch.extend(closing_transcript);
            if let Some(record @ Record::ToolReply(reply)) = batch.last() {
                // The last reply places both the already-reserved assistant and its results.
                ordinal += 2;
                let mid = results_mid(reply, &ledger, &checkpoint.operational)
                    .map(str::to_owned)
                    .unwrap_or_else(|| format!("{prefix}.{}", first + batch.len() as u64));
                batch.push(repair_entry_facts(
                    record,
                    ordinal,
                    mid,
                    tag_base,
                    on_contract,
                    &conversation,
                ));
            }
            if let Some(record) = notice {
                // Results have no compaction numbers of their own; the notice starts at the
                // same base. Its nonempty text takes one number on the contract.
                if on_contract && tag_base >= 9_007_199_254_740_991 {
                    return Err(SessionError::Fold(
                        "compaction tag numbers exceed the protocol integer range".into(),
                    ));
                }
                let mid = format!("{prefix}.{}", first + batch.len() as u64 + 1);
                let facts = repair_entry_facts(
                    &record,
                    ordinal + 1,
                    mid,
                    tag_base,
                    on_contract,
                    &conversation,
                );
                batch.extend([record, facts]);
            }
            batch.extend(closing_ledger);
            let mut operations = settled.clone();
            for (offset, record) in batch.iter().enumerate() {
                window.note(first + offset as u64, record);
                match record.stream() {
                    Stream::Transcript => conversation
                        .apply_at(Some(first + offset as u64), record)
                        .map_err(fold_error)?,
                    Stream::Ledger => operations.apply(record).map_err(fold_error)?,
                }
            }
            let encoded = encoder.batch(&batch)?;
            let mut open_ids = OpenIds::new(idindex::hash_key(&key));
            for ((offset, record), place) in batch
                .iter()
                .enumerate()
                .zip(encoded.places(encoder.offset()))
            {
                if record.stream() == Stream::Ledger {
                    open_ids.add(first + offset as u64, record, place);
                } else {
                    open_ids.place_transcript(first + offset as u64, record, place);
                }
            }
            out.extend(encoded.into_written(&mut encoder));
            session
                .backend()
                .append_log(&addr, &change, &out, 0, version, true)
                .await?;
            encoder.synced();
            session
                .backend()
                .sync_objects(&addr, &change, version)
                .await?;
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
                window,
                open_ids,
                transcript_dir: session.head.transcript_dir.clone(),
                ledger_dir: ledger_sealed.ledger_dir,
                id_index: ledger_sealed.id_index,
                retired_index: ledger_sealed.retired_index,
                // An index the new checkpoint no longer names is deleted with the rest.
                replaced: ledger_sealed
                    .replaced
                    .into_iter()
                    .chain(media_object)
                    .collect(),
                history_version,
                abandoned: None,
                cold_since_ms: None,
                media: Some(media),
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

/// Rebuilds the checkpoint of open log `log`: the copy in the newest ledger segment, which the
/// change that created the log sealed, or empty state for the session's first log. Refuses as
/// `CheckpointLost` when the newest segment belongs to another change or holds no copy.
async fn rebuild_checkpoint<B: SessionBackend>(
    backend: &B,
    addr: &SessionAddr,
    head: &head::Head,
    key: &crate::keys::DataKey,
    log: &Id128,
) -> Result<Vec<u8>, SessionError> {
    let frame_key = key.frame_key();
    let pages = Pages {
        backend,
        addr,
        session: head.session_id,
        key: &frame_key,
        fanout: FANOUT,
    };
    let newest = pages
        .find_before(head.ledger_dir.as_ref(), u64::MAX)
        .await
        .map_err(SessionError::Directory)?;
    let Some(entry) = newest else {
        // The session's first log, which creation started from empty state.
        return Ok(super::stored::first_log_checkpoint(head).encode());
    };
    if entry.object != format!("{}/l", hex(&log.0)) {
        return Err(SessionError::CheckpointLost);
    }
    let bytes = backend
        .get_object(addr, &entry.object)
        .await?
        .ok_or(SessionError::CheckpointLost)?;
    let segment = Segment::open(&bytes, &frame_key, entry.object.as_bytes())
        .map_err(SessionError::Segment)?;
    segment
        .index
        .log_start
        .map(|body| body.0)
        .ok_or(SessionError::CheckpointLost)
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

fn lost_part_line(id: &ToolCallId, call: &Call<'_>) -> String {
    let outcome = if call.outcome {
        "finished"
    } else {
        "outcome_unknown"
    };
    format!(
        "{} (call {}): {outcome}",
        call.tool_name.unwrap_or("a tool"),
        call.call_key.unwrap_or(&id.0),
    )
}

fn results_mid<'a>(
    reply: &ToolReply,
    ledger: &'a [(u64, Record)],
    checkpoint: &'a crate::fold::operations::Operations,
) -> Option<&'a str> {
    let id = reply.batch_id.as_deref()?;
    ledger
        .iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::BatchStart(start) if start.run_id == reply.run_id && start.batch_id == id => {
                start.mid.as_deref()
            }
            _ => None,
        })
        .or_else(|| {
            let run = checkpoint
                .run
                .as_ref()
                .filter(|run| run.run_id == reply.run_id)?;
            let batch = run
                .open_batch
                .as_ref()
                .filter(|batch| batch.batch_id == id)?;
            batch.results_mid.as_deref()
        })
}

fn repair_entry_facts(
    record: &Record,
    ordinal: u64,
    mid: String,
    tag_base: u64,
    on_contract: bool,
    conversation: &Conversation,
) -> Record {
    let (run_id, written_at_ms, notice) = match record {
        Record::ToolReply(reply) => (&reply.run_id, reply.finished_at_ms, None),
        Record::Input(input) => (
            &input.run_id,
            input.written_at_ms,
            on_contract.then(runner_notice),
        ),
        _ => unreachable!("repair facts follow a closing reply or the damage notice"),
    };
    let render_tags = on_contract
        && conversation
            .open_entry
            .iter()
            .chain(conversation.entries.iter().rev())
            .filter_map(|entry| entry.facts.as_deref())
            .find(|facts| facts.run_id.as_deref() == Some(run_id.as_str()))
            .is_some_and(|facts| facts.render_tags);
    let mut facts = entry_facts(record, ordinal, mid, written_at_ms, tag_base, render_tags);
    facts.run_id = (!run_id.is_empty()).then(|| run_id.as_str().into());
    facts.notice = notice.map(Into::into);
    // Recovery didn't render under the interrupted run's frozen tag rules. Leave its runner
    // base absent so a read cannot mistake a guessed number for one the model saw.
    Record::EntryFacts(facts.into())
}

/// The ids of a window message's blocks of `kind`: a tool call's own id, or the call a result
/// answers. Ids are kept when the content is released, so this reads any window message.
fn window_ids(kind: BlockKind) -> impl Fn(&WindowMessage) -> Vec<ToolCallId> {
    move |message| {
        message
            .blocks()
            .filter(|block| block.kind() == kind)
            .filter_map(|block| block.tool_call_id().cloned())
            .collect()
    }
}

/// Attach replies to the assistant that opened their turn, including replies that name no batch.
fn transcript_turns(
    transcript: &[(u64, u64, Record)],
) -> impl Iterator<Item = (Turn<'_>, u64, &Record)> {
    let mut turn = None;
    transcript.iter().filter_map(move |(seq, _, record)| {
        match record {
            Record::Assistant(assistant) => {
                turn = Some(Turn::Step(&assistant.run_id, assistant.step_id));
            }
            Record::Input(_) => turn = None,
            _ => {}
        }
        turn.map(|turn| (turn, *seq, record))
    })
}

/// Every call a kept record or the folded state names, with what the kept records say about
/// it, in the order first named. A model-facing id can recur, so only evidence from the same
/// turn can settle a call. Also returns the turn that still needs replies, if there is one.
fn known_calls<'a>(
    operations: &'a crate::fold::operations::Operations,
    conversation: &Conversation,
    ledger: &'a [(u64, Record)],
    transcript: &'a [(u64, u64, Record)],
    covers_through: u64,
) -> (Calls<'a>, Option<Turn<'a>>) {
    let open_batch = operations
        .run
        .as_ref()
        .filter(|run| run.finished.is_none())
        .and_then(|run| Some((run.run_id.as_str(), run.open_batch.as_ref()?)));
    let mut steps = BTreeMap::new();
    if let Some((run, batch)) = open_batch {
        steps.insert((run, batch.batch_id.as_str()), batch.step_id);
    }
    for (_, record) in ledger {
        if let Record::BatchStart(start) = record {
            steps.insert(
                (start.run_id.as_str(), start.batch_id.as_str()),
                start.step_id,
            );
        }
    }
    // If the batch start was lost, a kept reply can still link its named batch to the step.
    for (turn, _, record) in transcript_turns(transcript) {
        if let (Turn::Step(run, step), Record::ToolReply(reply)) = (turn, record) {
            if let Some(batch) = reply.batch_id.as_deref().filter(|_| reply.run_id == run) {
                steps.entry((run, batch)).or_insert(step);
            }
        }
    }
    let by_batch = |run: &'a str, batch: &'a str| {
        steps
            .get(&(run, batch))
            .map_or(Turn::Batch(run, batch), |step| Turn::Step(run, *step))
    };

    let mut calls = Calls::default();
    let mut batch_owners = BTreeMap::new();
    if let Some((run, batch)) = open_batch {
        let batch_id = batch.batch_id.as_str();
        batch_owners.insert(batch_id, run);
        for call in &batch.calls {
            let known = calls.slot(Turn::Step(run, batch.step_id), &call.id, Some(batch_id));
            known.intent |= call.intent;
            known.outcome |= call.terminal.is_some();
            known.call_key = call.key.as_deref().or(known.call_key);
            known.in_log = true;
        }
    }
    for (_, record) in ledger {
        match record {
            Record::BatchStart(start) => {
                let (run, batch) = (start.run_id.as_str(), start.batch_id.as_str());
                batch_owners.insert(batch, run);
                for id in &start.planned {
                    calls.slot(by_batch(run, batch), id, Some(batch)).in_log = true;
                }
            }
            Record::ToolIntent(intent) => {
                let (run, batch) = (intent.run_id.as_str(), intent.batch_id.as_str());
                batch_owners.insert(batch, run);
                let call = calls.slot(by_batch(run, batch), &intent.tool_call_id, Some(batch));
                call.tool_name = intent.tool_name.as_deref().or(call.tool_name);
                call.call_key = intent.call_key.as_deref().or(call.call_key);
                call.intent = true;
                call.in_log = true;
            }
            Record::ToolHeld(held) => {
                let batch = held.batch_id.as_str();
                // The held record has no run id. Don't attribute it to a run without its
                // preceding batch or intent; the held record itself is still kept in the ledger.
                let Some(&run) = batch_owners.get(batch) else {
                    continue;
                };
                let call = calls.slot(by_batch(run, batch), &held.tool_call_id, Some(batch));
                call.call_key = Some(&held.call_key);
                call.in_log = true;
            }
            Record::ToolOutcome(outcome) => {
                let (run, batch) = (outcome.run_id.as_str(), outcome.batch_id.as_str());
                let call = calls.slot(by_batch(run, batch), &outcome.tool_call_id, Some(batch));
                call.outcome = true;
                call.in_log = true;
            }
            _ => {}
        }
    }
    let mut last_turn = None;
    for (turn, seq, record) in transcript_turns(transcript) {
        match record {
            Record::Assistant(assistant) => {
                last_turn = Some(turn);
                for block in &assistant.message.content {
                    if let ContentBlock::ToolCall(tool) = block {
                        let call = calls.slot(turn, &tool.tool_call_id, None);
                        call.on_transcript = true;
                        call.in_log |= seq > covers_through;
                    }
                }
            }
            Record::ToolReply(reply) => {
                let call = calls.slot(turn, &reply.result.tool_call_id, reply.batch_id.as_deref());
                call.reply = Some(&reply.result);
            }
            _ => {}
        }
    }
    // A checkpoint never splits a tool turn, so any open turn began at a kept assistant record.
    (
        calls,
        last_turn.filter(|_| conversation.open_tool_turn().is_some()),
    )
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
            .rposition(|message| {
                window_ids(BlockKind::ToolCall)(message).contains(&append.tool_call_id)
            })
            .map_or(next, |index| conversation.window_start() + index as u64),
        _ => next,
    }
}

#[cfg(test)]
mod tests;
