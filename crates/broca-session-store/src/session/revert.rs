//! Revert: a person edits an earlier message of theirs and sends it again. The transcript branch
//! ends just before that message, and a new run starts there with the edited input. Nothing
//! sealed is rewritten, and the ledger is never cut: the runs a revert abandons keep their spend,
//! their tool side effects and their accepted sends.
//!
//! A revert is a change (see `seal`): intent in the old log, every new object created under the
//! change's id, then one compare-and-swap of the head.
//!
//! - **What it starts at.** The request names the message by id, never by ordinal, since an
//!   ordinal is taken again after a revert. The message must be a run's own input, the only place
//!   a run can start again.
//! - **What must be settled first.** No run active or paused, no tool turn waiting for results,
//!   no send in the queue. A revert never decides on its own what happens to open work; the
//!   caller ends, cancels or retracts it first.
//! - **The resend comes with it.** The new run's start, its input and the input's entry facts are
//!   written into the new log before the commit, so a crash can never leave the branch cut with no
//!   edited message. A retry finds the resend's send id already used and is answered from it.
//! - **The new log.** If the message is in the open log, the records before it are copied into
//!   the new log. If it's sealed, the transcript directory is cut at it (pages and segments after
//!   it dropped, the segment holding it ended just before it) and the new log starts with no
//!   window: the session is *window-pending* until a window can be copied back in, which isn't
//!   built yet.
//! - **The ledger.** Every ledger record of the old log is sealed, as a ledger seal does, with a
//!   `TranscriptReverted` record that only the revert's own ledger segment holds. Its fold moves
//!   the history version and owes the compaction provider a `transcript_changed` notice.
//! - **Deletion.** The old log, and the objects the cut branch stopped naming, are deleted after
//!   the grace period. Ledger-named objects are never among them.

use std::collections::BTreeMap;

use crate::backend::SessionBackend;
use crate::checkpoint::Checkpoint;
use crate::codec::{hex, Id128, Message as _};
use crate::directory::{Pages, FANOUT};
use crate::fold::conversation::Conversation;
use crate::fold::operations::Resolution;
use crate::idindex::{IdKind, IndexPages, OpenIds, Retired};
use crate::keys::random_id;
use crate::log::{ChainBase, LogEncoder};
use crate::record::{ChangeIntent, ChangeKind, Placement, Record, Stream, TranscriptReverted};
use crate::segment::Segment;

use super::seal::{Created, Sealed};
use super::{apply, log_bind, Session, SessionError};

/// Why a revert was refused. Nothing was written.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RevertRefusal {
    /// A run is active or paused, or a tool turn waits for results.
    RunOpen,
    /// Sends wait in the queue.
    QueueNotEmpty,
    /// No message on the current branch has the id.
    UnknownMessage,
    /// The message isn't a run's own input.
    NotRunInput,
    /// The resend isn't a run start with a send id, the run's input, and its entry facts.
    BadResend(&'static str),
}

/// What a revert did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Reverted {
    Applied {
        change: Id128,
        /// The edited message's ordinal: the reverted message's, now the resend's.
        ordinal: u64,
        history_version: u64,
        /// The resend's message id.
        mid: String,
        window_pending: bool,
    },
    /// A retry: the resend's send id already started a run, at this seq.
    Already { seq: u64 },
}

/// Where the message a revert starts at was placed.
struct Located {
    /// The seq of its input record: the first transcript record the branch drops.
    seq: u64,
    /// The conversation just before it.
    before: Conversation,
    /// The branch's last transcript record before it, which the new log's back-links continue.
    prev_transcript: Option<u64>,
    sealed: bool,
}

/// Folds transcript `records` onto `conversation` until the record that places message
/// `ordinal`, and returns where it is if it's a run's own input.
fn locate<'a>(
    mut conversation: Conversation,
    records: impl Iterator<Item = (u64, &'a Record)>,
    ordinal: u64,
    mut prev: Option<u64>,
    sealed: bool,
) -> Result<Option<Located>, SessionError> {
    for (seq, record) in records {
        if record.stream() != Stream::Transcript {
            continue;
        }
        let places_next =
            conversation.open_tool_turn().is_none() && conversation.placed + 1 == ordinal;
        if places_next {
            match record {
                Record::Input(input) => {
                    return Ok((input.placement == Placement::RunInput).then_some(Located {
                        seq,
                        before: conversation,
                        prev_transcript: prev,
                        sealed,
                    }));
                }
                // An assistant message places the ordinal, or reserves it while its calls wait.
                Record::Assistant(_) => return Ok(None),
                _ => {}
            }
        }
        conversation
            .apply(record)
            .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
        prev = Some(seq);
        if conversation.placed >= ordinal {
            return Ok(None);
        }
    }
    Ok(None)
}

/// The resend's send id, once its shape is checked.
fn check_resend(resend: &[Record]) -> Result<String, SessionError> {
    let bad = |why| SessionError::RevertRefused(RevertRefusal::BadResend(why));
    let [Record::RunStart(start), Record::Input(input), Record::EntryFacts(_)] = resend else {
        return Err(bad("a run start, its input and the input's entry facts"));
    };
    if input.placement != Placement::RunInput || input.run_id != start.run_id {
        return Err(bad("the input is the run's own"));
    }
    start
        .send_id
        .clone()
        .ok_or_else(|| bad("the run start has a send id"))
}

impl<'b, B: SessionBackend> Session<'b, B> {
    /// Ends the branch before the message `target` names, and starts the run `resend` describes
    /// there: its run start (with a send id), its input, and the input's entry facts, whose
    /// ordinal and id the revert fills in.
    pub async fn revert(
        &mut self,
        target: &str,
        mut resend: Vec<Record>,
        now_ms: u64,
    ) -> Result<Reverted, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        if self.operations.open_change.is_some() {
            return Err(SessionError::ChangeOpen);
        }
        let send_id = check_resend(&resend)?;
        // A retry is answered before the target is resolved: after the commit, the target is
        // no longer on the branch.
        let used = self.find_id(IdKind::SendId, &send_id).await?;
        if let Some((seq, _)) = used
            .into_iter()
            .find(|(_, record)| matches!(record, Record::RunStart(_)))
        {
            return Ok(Reverted::Already { seq });
        }
        let refuse = |refusal| Err(SessionError::RevertRefused(refusal));
        let run_open = self
            .operations
            .run
            .as_ref()
            .is_some_and(|run| !matches!(run.resolution(), Resolution::Idle(_)));
        if run_open || self.conversation.open_tool_turn().is_some() {
            return refuse(RevertRefusal::RunOpen);
        }
        if !self.operations.queue.is_empty() {
            return refuse(RevertRefusal::QueueNotEmpty);
        }
        let Some(ordinal) = self.find_message(target).await? else {
            return refuse(RevertRefusal::UnknownMessage);
        };
        let (records, batches, checkpoint) = self.read_open_log().await?;
        let located = if ordinal >= self.conversation.window_start() {
            locate(
                checkpoint.conversation.clone(),
                records.iter().map(|(seq, record)| (*seq, record)),
                ordinal,
                checkpoint.transcript_base,
                false,
            )?
        } else {
            self.locate_sealed(ordinal).await?
        };
        let Some(located) = located else {
            return refuse(RevertRefusal::NotRunInput);
        };
        let through = records
            .iter()
            .rev()
            .find(|(_, record)| record.stream() == Stream::Transcript)
            .map(|(seq, _)| *seq)
            .or(self.head.transcript_dir.as_ref().map(|dir| dir.last_seq))
            .unwrap_or(located.seq);

        let change = random_id();
        let intent = Record::ChangeIntent(ChangeIntent {
            change,
            kind: ChangeKind::Revert,
            cut: Some(located.seq),
        });
        let intent_seq = self.append(std::slice::from_ref(&intent), true).await?;
        let mut records = records;
        records.push((intent_seq, intent));
        let created = self
            .create_revert(
                change,
                intent_seq,
                through,
                ordinal,
                located,
                &records,
                &batches,
                &checkpoint,
                &mut resend,
                now_ms,
            )
            .await;
        let created = match created {
            Ok(created) => created,
            Err(error) => {
                self.abort(change).await;
                return Err(error);
            }
        };
        let history_version = created.history_version.expect("a revert moves the version");
        let window_pending = created.conversation.window_pending;
        let Some(Record::EntryFacts(facts)) = resend.last() else {
            unreachable!("checked above");
        };
        let mid = facts.mid.clone();
        self.commit(change, intent_seq, created, now_ms).await?;
        Ok(Reverted::Applied {
            change,
            ordinal,
            history_version,
            mid,
            window_pending,
        })
    }

    /// Where a sealed message was placed: its segment's leading state folded up to it.
    async fn locate_sealed(&self, ordinal: u64) -> Result<Option<Located>, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: FANOUT,
        };
        let damaged = || SessionError::Fold(format!("no sealed segment places message {ordinal}"));
        let root = self.head.transcript_dir.as_ref();
        let entry = pages
            .find_ordinal(root, ordinal)
            .await
            .map_err(SessionError::Directory)?
            .ok_or_else(damaged)?;
        let bytes = self
            .backend
            .get_object(&self.addr, &entry.object)
            .await?
            .ok_or_else(damaged)?;
        let segment =
            Segment::open(&bytes, &key, entry.object.as_bytes()).map_err(SessionError::Segment)?;
        let leading = segment.index.leading.as_ref().ok_or_else(damaged)?;
        let before = Conversation::decode(&leading.0).map_err(SessionError::Checkpoint)?;
        let mut records = Vec::new();
        for position in 0..segment.index.blocks.len() {
            records.extend(
                segment
                    .block(position)
                    .map_err(SessionError::Segment)?
                    .into_iter()
                    .filter(|(seq, _)| entry.keeps(*seq)),
            );
        }
        // The branch's last transcript record before this segment ends the segment before it.
        let prev = pages
            .find_before(root, entry.first_seq)
            .await
            .map_err(SessionError::Directory)?
            .map(|previous| previous.last_seq);
        locate(
            before,
            records.iter().map(|(seq, record)| (*seq, record)),
            ordinal,
            prev,
            true,
        )
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_revert(
        &mut self,
        change: Id128,
        intent_seq: u64,
        through: u64,
        ordinal: u64,
        located: Located,
        records: &[(u64, Record)],
        batches: &[std::ops::Range<usize>],
        checkpoint: &Checkpoint,
        resend: &mut [Record],
        now_ms: u64,
    ) -> Result<Created, SessionError> {
        let name = hex(&change.0);
        let key = self.key.frame_key();
        let history_version = self.head.history_version + 1;
        let reverted_seq = intent_seq + 1;
        let reverted = Record::TranscriptReverted(TranscriptReverted {
            change,
            from_seq: located.seq,
            through_seq: through,
            ordinal,
            history_version,
        });
        let mut ledger: Vec<(u64, Record)> = records
            .iter()
            .filter(|(_, record)| record.stream() == Stream::Ledger)
            .cloned()
            .collect();
        ledger.push((reverted_seq, reverted.clone()));
        let mut settled = self.operations.clone();
        settled.open_change = None;
        settled
            .apply(&reverted)
            .map_err(|e| SessionError::Fold(format!("operations: {e:?}")))?;
        let ledger_sealed = self.seal_ledger(&name, &ledger, &settled, now_ms).await?;
        let replaced = ledger_sealed.replaced;

        // The transcript: cut the directory at the message if it's sealed; otherwise keep the
        // open log's records before it.
        let pages = Pages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: FANOUT,
        };
        let (transcript_dir, abandoned, mut conversation, base) = if located.sealed {
            let old = self.head.transcript_dir.as_ref();
            let last_kept = ordinal.checked_sub(1).filter(|last| *last >= 1);
            let root = pages
                .truncate(
                    old,
                    located.seq,
                    last_kept,
                    &format!("{name}/dt"),
                    self.version,
                )
                .await
                .map_err(SessionError::Directory)?;
            let gone = pages
                .abandoned(old, located.seq)
                .await
                .map_err(SessionError::Directory)?;
            let list = format!("{name}/ab");
            let mut refs = gone.segments;
            refs.extend(gone.pages);
            IndexPages {
                backend: self.backend,
                addr: &self.addr,
                session: self.head.session_id,
                key: &key,
            }
            .write(&list, &Retired { refs }, self.version)
            .await
            .map_err(SessionError::Index)?;
            let mut start = located.before;
            start.clear_window();
            start.window_pending = true;
            (root, Some(list), start, located.prev_transcript)
        } else {
            (
                self.head.transcript_dir.clone(),
                None,
                checkpoint.conversation.clone(),
                checkpoint.transcript_base,
            )
        };

        let mut encoder = LogEncoder::new(
            key.clone(),
            log_bind(&self.head.session_id, &change),
            reverted_seq + 1,
            ChainBase {
                transcript: base,
                ledger: Some(reverted_seq),
            },
        );
        self.backend
            .create_log(&self.addr, &change, self.version)
            .await?;
        let mut bytes = encoder
            .checkpoint(
                &Checkpoint {
                    covers_through: reverted_seq,
                    next_seq: reverted_seq + 1,
                    conversation: conversation.clone(),
                    operational: settled.clone(),
                    transcript_base: base,
                    ledger_base: Some(reverted_seq),
                }
                .encode(),
            )?
            .into_written(&mut encoder);
        let mut window_facts = BTreeMap::new();
        let mut kept_records = 0;
        if !located.sealed {
            for batch in batches {
                let kept: Vec<(u64, &Record)> = records[batch.clone()]
                    .iter()
                    .filter(|(seq, record)| {
                        record.stream() == Stream::Transcript && *seq < located.seq
                    })
                    .map(|(seq, record)| (*seq, record))
                    .collect();
                if kept.is_empty() {
                    continue;
                }
                for (seq, record) in &kept {
                    conversation
                        .apply(record)
                        .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
                    if let Record::EntryFacts(facts) = record {
                        window_facts.insert(*seq, facts.ordinal);
                    }
                }
                kept_records += kept.len();
                bytes.extend(encoder.copy_batch(&kept)?.into_written(&mut encoder));
            }
        }
        // The resend, with its entry facts' ordinal and id, which only the revert knows.
        let first = encoder.next_seq();
        let generation = hex(&self.head.generation.0);
        for (offset, record) in resend.iter_mut().enumerate() {
            if let Record::EntryFacts(facts) = record {
                facts.ordinal = ordinal;
                facts.mid = format!("g{}.{}", &generation[..16], first + offset as u64);
            }
        }
        let mut operations = settled;
        let mut open_ids = OpenIds::default();
        for (offset, record) in resend.iter().enumerate() {
            let seq = first + offset as u64;
            apply(&mut conversation, &mut operations, record).map_err(SessionError::Fold)?;
            if record.stream() == Stream::Ledger {
                open_ids.add(seq, record);
            }
            if let Record::EntryFacts(facts) = record {
                window_facts.insert(seq, facts.ordinal);
            }
        }
        if conversation.placed != ordinal {
            return Err(SessionError::Fold(format!(
                "the resend placed message {}, not {ordinal}",
                conversation.placed
            )));
        }
        bytes.extend(encoder.batch(resend)?.into_written(&mut encoder));
        self.backend
            .append_log(&self.addr, &change, &bytes, 0, self.version, true)
            .await?;
        encoder.synced();
        self.backend
            .sync_objects(&self.addr, &change, self.version)
            .await?;
        Ok(Created {
            sealed: Sealed {
                change,
                transcript_records: 0,
                ledger_records: ledger.len(),
                kept_records,
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
            transcript_dir,
            ledger_dir: ledger_sealed.ledger_dir,
            id_index: ledger_sealed.id_index,
            retired_index: ledger_sealed.retired_index,
            replaced,
            history_version: Some(history_version),
            abandoned,
        })
    }
}

#[cfg(test)]
mod tests;
