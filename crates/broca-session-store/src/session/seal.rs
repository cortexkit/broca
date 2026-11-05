//! Sealing: moving records out of the open log into immutable segments, as one change.
//!
//! A change replaces the open log, and runs in three steps:
//! 1. **Intent.** It appends `ChangeIntent` to the open log as a barrier. That closes the log:
//!    the only record it accepts afterwards is the change's `ChangeAborted`.
//! 2. **Create.** It writes every object it creates under its own id, and its new open log,
//!    named by that id. All of it is durable before the next step.
//! 3. **Commit.** One compare-and-swap of the head makes the new log the open log and names the
//!    new segments in the directories. The head also records what the change stopped naming (the
//!    old log and the directory pages it rewrote), which is deleted after a grace period, so a
//!    reader that read the old head can finish first.
//!
//! A crash before the commit leaves the old head, whose open log ends with the intent. Loading
//! sees the unsettled intent and undoes the change. A failure before the commit undoes it at
//! once, unless another writer has taken the head over, in which case this writer stops and the
//! next load undoes it.
//!
//! A **cut** seals the transcript records before the first entry the compaction view keeps, and
//! every ledger record. A **ledger seal** seals only the ledger records and keeps the whole
//! transcript open, so a session that runs long without compacting still loads quickly.
//!
//! Every seal also files the sealed ledger records' ids in the id index (see `idindex`): a new run
//! at level 0, and a bounded share of any merge in progress. The new index root commits with the
//! head, so an id moves from the open log to the index in the same change as its record.
//!
//! Records keep their seqs and bytes. The transcript records that stay open are copied into the
//! new log batch by batch, so a batch is still all-or-nothing there. Opaque reasoning of finished
//! runs is pruned from sealed transcript records (see `prune`); the current run's isn't. Providers
//! attach a signature or ciphertext to their reasoning (Anthropic thinking signatures, OpenAI
//! encrypted reasoning) and some require it back on later requests of the same run, so resuming
//! a run may need its own.

use std::collections::BTreeSet;

use crate::backend::SessionBackend;
use crate::checkpoint::Checkpoint;
use crate::codec::{hex, Id128, Message as _};
use crate::directory::{DirEntry, Pages, FANOUT};
use crate::fold::conversation::Conversation;
use crate::head::{self, LastChange, OpenLog, PendingDeletion};
use crate::idindex::{self, IndexPages, IndexRoot, Retired};
use crate::keys::random_id;
use crate::log::{self, ChainBase, LogEncoder};
use crate::record::{ChangeAborted, ChangeIntent, ChangeKind, Record, Stream};
use crate::segment::{self, SegmentMeta};

use super::{log_bind, Session, SessionError};

/// How long what a change stopped naming is kept, so a reader that read the head just before
/// the commit can finish reading.
pub const GRACE_MS: u64 = 10 * 60 * 1000;

const PRUNE_CONTEXT: &[u8] = b"broca-prune-v1";

/// What a seal wrote.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sealed {
    pub change: Id128,
    pub transcript_records: usize,
    pub ledger_records: usize,
    /// Transcript records copied into the new open log.
    pub kept_records: usize,
    pub transcript_bytes: u64,
    pub ledger_bytes: u64,
    /// Ids the sealed ledger records named, filed at level 0 of the id index.
    pub index_entries: usize,
    /// Entries the id index's merges wrote in this change.
    pub merged_entries: u64,
    /// Merges that had to finish in this change because their level was full.
    pub forced_merges: u32,
}

/// What the create step built, which the commit then names.
struct Created {
    sealed: Sealed,
    encoder: LogEncoder,
    conversation: Conversation,
    transcript_dir: Option<crate::directory::PageRef>,
    ledger_dir: Option<crate::directory::PageRef>,
    id_index: String,
    /// The id-index list of runs merges retired, deleted after the grace period.
    retired_index: Option<String>,
    replaced: Vec<String>,
}

impl<'b, B: SessionBackend> Session<'b, B> {
    /// Seals the open log. With `cut`, the seq of the first record a compaction view keeps,
    /// the transcript records before it are sealed too; it must start an entry (a user input or
    /// an assistant message) with no assistant message waiting for replies before it.
    pub async fn seal(&mut self, cut: Option<u64>, now_ms: u64) -> Result<Sealed, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        if self.operations.open_change.is_some() {
            return Err(SessionError::ChangeOpen);
        }
        let (records, batches, checkpoint) = self.read_open_log().await?;
        if let Some(cut) = cut {
            self.check_cut(&records, &checkpoint, cut)?;
        }

        let change = random_id();
        let kind = if cut.is_some() {
            ChangeKind::Cut
        } else {
            ChangeKind::LedgerSeal
        };
        let intent = Record::ChangeIntent(ChangeIntent { change, kind, cut });
        let intent_seq = self.append(std::slice::from_ref(&intent), true).await?;
        let mut records = records;
        records.push((intent_seq, intent));

        let created = self
            .create_objects(
                change,
                cut,
                intent_seq,
                &records,
                &batches,
                &checkpoint,
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
        self.commit(change, intent_seq, created, now_ms).await
    }

    /// The open log's records, its batches and its checkpoint, read back from the backend.
    async fn read_open_log(
        &self,
    ) -> Result<(Vec<(u64, Record)>, Vec<std::ops::Range<usize>>, Checkpoint), SessionError> {
        let open = self.head.open_log.clone().ok_or(SessionError::Backend(
            crate::backend::BackendError::Corrupt("local head without a log"),
        ))?;
        let bytes = self
            .backend
            .read_log(&self.addr, &self.log, 0..self.encoder.offset())
            .await?;
        let mut checkpoint = None;
        let read = log::read(
            &bytes,
            &self.key.frame_key(),
            &log_bind(&self.head.session_id, &self.log),
            open.synced_len,
            |body| {
                let decoded = Checkpoint::decode(body).map_err(|e| e.to_string())?;
                let base = ChainBase {
                    transcript: decoded.transcript_base,
                    ledger: decoded.ledger_base,
                };
                checkpoint = Some(decoded);
                Ok(base)
            },
        )?;
        if read.end != log::LogEnd::Clean {
            return Err(SessionError::Damaged(read.end));
        }
        let checkpoint = checkpoint.ok_or(SessionError::Damaged(log::LogEnd::Clean))?;
        Ok((read.records, read.batches, checkpoint))
    }

    fn check_cut(
        &self,
        records: &[(u64, Record)],
        checkpoint: &Checkpoint,
        cut: u64,
    ) -> Result<(), SessionError> {
        let starts_entry = records.iter().any(|(seq, record)| {
            *seq == cut && matches!(record, Record::Input(_) | Record::Assistant(_))
        });
        let before = conversation_at(checkpoint, records, cut).map_err(SessionError::Fold)?;
        if !starts_entry || before.open_tool_turn().is_some() {
            return Err(SessionError::BadCut(cut));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_objects(
        &mut self,
        change: Id128,
        cut: Option<u64>,
        intent_seq: u64,
        records: &[(u64, Record)],
        batches: &[std::ops::Range<usize>],
        checkpoint: &Checkpoint,
        now_ms: u64,
    ) -> Result<Created, SessionError> {
        let name = hex(&change.0);
        let key = self.key.frame_key();
        let sealed_before = cut.unwrap_or(0);
        let below = |seq: u64, record: &Record| {
            record.stream() == Stream::Transcript && seq < sealed_before
        };

        // The transcript records below the cut, with finished runs' opaque reasoning pruned.
        let unfinished = self
            .operations
            .run
            .as_ref()
            .filter(|run| run.finished.is_none())
            .map(|run| run.run_id.clone());
        let prune_key = self.key.derive(PRUNE_CONTEXT);
        let transcript: Vec<(u64, Record)> = records
            .iter()
            .filter(|(seq, record)| below(*seq, record))
            .map(|(seq, record)| {
                let mut record = record.clone();
                if record_run(&record).is_some_and(|run| Some(run) != unfinished.as_deref()) {
                    crate::prune::prune_record(&mut record, &prune_key);
                }
                (*seq, record)
            })
            .collect();
        let ledger: Vec<(u64, Record)> = records
            .iter()
            .filter(|(_, record)| record.stream() == Stream::Ledger)
            .cloned()
            .collect();

        // The conversation state at the cut. A checkpoint's messages are always empty: the
        // window's messages are rebuilt from the open log's own records.
        let at_cut = match cut {
            Some(cut) => {
                let mut at =
                    conversation_at(checkpoint, records, cut).map_err(SessionError::Fold)?;
                at.messages.clear();
                at
            }
            None => checkpoint.conversation.clone(),
        };
        // The commit settles the intent, so state carried past it has no open change.
        let mut settled = self.operations.clone();
        settled.open_change = None;

        let pages = Pages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: FANOUT,
        };
        let mut sealed = Sealed {
            change,
            transcript_records: transcript.len(),
            ledger_records: ledger.len(),
            kept_records: 0,
            transcript_bytes: 0,
            ledger_bytes: 0,
            index_entries: 0,
            merged_entries: 0,
            forced_merges: 0,
        };
        let mut replaced = Vec::new();
        let mut transcript_dir = self.head.transcript_dir.clone();
        if !transcript.is_empty() {
            let object = format!("{name}/t");
            let (placed_before, placed_after) =
                placed_counts(&checkpoint.conversation, &transcript).map_err(SessionError::Fold)?;
            let ordinals =
                (placed_after > placed_before[0]).then(|| (placed_before[0] + 1, placed_after));
            let bytes = segment::build(
                &key,
                object.as_bytes(),
                &transcript,
                SegmentMeta {
                    session_id: self.head.session_id,
                    created_at_ms: now_ms,
                    prefixes: Vec::new(),
                    leading: Some(checkpoint.conversation.encode()),
                    trailer: None,
                    placed_before,
                },
            )
            .map_err(SessionError::Segment)?;
            sealed.transcript_bytes = bytes.len() as u64;
            self.backend
                .put_object(&self.addr, &object, &bytes, self.version)
                .await?;
            let appended = pages
                .append(
                    transcript_dir.as_ref(),
                    entry(object, &transcript, bytes.len(), now_ms, ordinals),
                    &format!("{name}/dt"),
                    self.version,
                )
                .await
                .map_err(SessionError::Directory)?;
            replaced.extend(appended.replaced);
            transcript_dir = Some(appended.root);
        }
        let object = format!("{name}/l");
        let bytes = segment::build(
            &key,
            object.as_bytes(),
            &ledger,
            SegmentMeta {
                session_id: self.head.session_id,
                created_at_ms: now_ms,
                prefixes: Vec::new(),
                leading: None,
                trailer: Some(settled.encode()),
                placed_before: Vec::new(),
            },
        )
        .map_err(SessionError::Segment)?;
        sealed.ledger_bytes = bytes.len() as u64;
        self.backend
            .put_object(&self.addr, &object, &bytes, self.version)
            .await?;
        let appended = pages
            .append(
                self.head.ledger_dir.as_ref(),
                entry(object, &ledger, bytes.len(), now_ms, None),
                &format!("{name}/dl"),
                self.version,
            )
            .await
            .map_err(SessionError::Directory)?;
        replaced.extend(appended.replaced);
        let ledger_dir = Some(appended.root);

        // The sealed ledger records' ids, filed in the id index.
        let index = IndexPages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
        };
        let root: IndexRoot = match &self.head.id_index {
            Some(name) => index.read(name).await.map_err(SessionError::Index)?,
            None => IndexRoot::default(),
        };
        let entries = idindex::entries_for(&idindex::hash_key(&self.key), &ledger);
        sealed.index_entries = entries.len();
        let updated = index
            .update(root, entries, &name, self.version)
            .await
            .map_err(SessionError::Index)?;
        sealed.merged_entries = updated.merged_entries;
        sealed.forced_merges = updated.forced;
        let id_index = format!("{name}/ix");
        index
            .write(&id_index, &updated.root, self.version)
            .await
            .map_err(SessionError::Index)?;
        replaced.extend(self.head.id_index.clone());
        let retired_index = if updated.retired.is_empty() {
            None
        } else {
            let object = format!("{name}/ixd");
            index
                .write(
                    &object,
                    &Retired {
                        refs: updated.retired,
                    },
                    self.version,
                )
                .await
                .map_err(SessionError::Index)?;
            Some(object)
        };

        // The new open log: a checkpoint, then the transcript records that stay open, copied
        // batch by batch with their seqs.
        let transcript_base = transcript
            .last()
            .map(|(seq, _)| *seq)
            .or(checkpoint.transcript_base);
        let mut encoder = LogEncoder::new(
            key.clone(),
            log_bind(&self.head.session_id, &change),
            self.encoder.next_seq(),
            ChainBase {
                transcript: transcript_base,
                ledger: Some(intent_seq),
            },
        );
        self.backend
            .create_log(&self.addr, &change, self.version)
            .await?;
        let mut bytes = encoder
            .checkpoint(
                &Checkpoint {
                    covers_through: intent_seq,
                    next_seq: self.encoder.next_seq(),
                    conversation: at_cut.clone(),
                    operational: settled,
                    transcript_base,
                    ledger_base: Some(intent_seq),
                }
                .encode(),
            )?
            .into_written(&mut encoder);
        let mut conversation = at_cut;
        for batch in batches {
            let kept: Vec<(u64, &Record)> = records[batch.clone()]
                .iter()
                .filter(|(seq, record)| {
                    record.stream() == Stream::Transcript && !below(*seq, record)
                })
                .map(|(seq, record)| (*seq, record))
                .collect();
            if kept.is_empty() {
                continue;
            }
            for (_, record) in &kept {
                conversation
                    .apply(record)
                    .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
            }
            sealed.kept_records += kept.len();
            bytes.extend(encoder.copy_batch(&kept)?.into_written(&mut encoder));
        }
        self.backend
            .append_log(&self.addr, &change, &bytes, 0, self.version, true)
            .await?;
        encoder.synced();
        self.backend
            .sync_objects(&self.addr, &change, self.version)
            .await?;
        Ok(Created {
            sealed,
            encoder,
            conversation,
            transcript_dir,
            ledger_dir,
            id_index,
            retired_index,
            replaced,
        })
    }

    /// The commit: one compare-and-swap of the head. A refusal or an unknown outcome poisons the
    /// writer, which never undoes a change it can't tell didn't commit: the next load decides.
    async fn commit(
        &mut self,
        change: Id128,
        intent_seq: u64,
        created: Created,
        now_ms: u64,
    ) -> Result<Sealed, SessionError> {
        let mut head = self.head.clone();
        head.open_log = Some(OpenLog {
            change,
            synced_len: created.encoder.offset(),
        });
        head.last_change = Some(LastChange { change, intent_seq });
        head.transcript_dir = created.transcript_dir;
        head.ledger_dir = created.ledger_dir;
        head.id_index = Some(created.id_index);
        head.next_seq = created.encoder.next_seq();
        head.pending.push(PendingDeletion {
            change,
            unreferenced_at_ms: now_ms,
            logs: vec![self.log],
            objects: created.replaced,
            retired_index: created.retired_index,
        });
        self.poisoned = true;
        let version = self
            .backend
            .write_head(
                &self.addr,
                &head::seal(&head, &self.key),
                Some(self.version),
                self.version + 1,
            )
            .await?;
        self.head = head;
        self.version = version;
        self.log = change;
        self.encoder = created.encoder;
        self.conversation = created.conversation;
        self.operations.open_change = None;
        // Every ledger record the old log held is sealed, and its ids are in the index now.
        self.open_ids.clear();
        self.poisoned = false;
        Ok(created.sealed)
    }

    /// Undoes a change that failed before its commit: deletes what it created and reopens the
    /// log. If that fails too (another writer took the head, or the backend is down), the writer
    /// is poisoned and the next load undoes the change.
    async fn abort(&mut self, change: Id128) {
        let undone = async {
            self.backend
                .delete_change(&self.addr, &change, self.version)
                .await?;
            self.backend
                .delete_log(&self.addr, &change, self.version)
                .await?;
            Ok::<_, SessionError>(())
        }
        .await;
        let reopened = match undone {
            Ok(()) => self
                .append(&[Record::ChangeAborted(ChangeAborted { change })], true)
                .await
                .map(|_| ()),
            Err(error) => Err(error),
        };
        if reopened.is_err() {
            self.poisoned = true;
        }
    }

    /// Deletes what committed changes stopped naming, once it is `GRACE_MS` old, and drops
    /// those entries from the head. Returns how many changes' deletions ran.
    pub async fn collect(&mut self, now_ms: u64) -> Result<usize, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        if self.operations.open_change.is_some() {
            return Err(SessionError::ChangeOpen);
        }
        let due: Vec<PendingDeletion> = self
            .head
            .pending
            .iter()
            .filter(|pending| pending.unreferenced_at_ms.saturating_add(GRACE_MS) <= now_ms)
            .cloned()
            .collect();
        if due.is_empty() {
            return Ok(0);
        }
        let key = self.key.frame_key();
        let index = IndexPages {
            backend: self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
        };
        for pending in &due {
            // A retired-runs list is read before anything it names is deleted, and deleted
            // last, so a collection that stops halfway runs again from the list.
            if let Some(list) = &pending.retired_index {
                match index.read::<Retired>(list).await {
                    Ok(retired) => {
                        index
                            .delete_retired(&retired.refs, self.version)
                            .await
                            .map_err(SessionError::Index)?;
                        self.backend
                            .delete_object(&self.addr, list, self.version)
                            .await?;
                    }
                    Err(idindex::IndexError::Missing(_)) => {}
                    Err(error) => return Err(SessionError::Index(error)),
                }
            }
            for log in &pending.logs {
                self.backend
                    .delete_log(&self.addr, log, self.version)
                    .await?;
            }
            for object in &pending.objects {
                self.backend
                    .delete_object(&self.addr, object, self.version)
                    .await?;
            }
        }
        let done: BTreeSet<Id128> = due.iter().map(|pending| pending.change).collect();
        let mut head = self.head.clone();
        head.pending
            .retain(|pending| !done.contains(&pending.change));
        self.version = self
            .backend
            .write_head(
                &self.addr,
                &head::seal(&head, &self.key),
                Some(self.version),
                self.version + 1,
            )
            .await?;
        self.head = head;
        Ok(due.len())
    }
}

/// The checkpoint's conversation state folded over the log's transcript records before `cut`.
fn conversation_at(
    checkpoint: &Checkpoint,
    records: &[(u64, Record)],
    cut: u64,
) -> Result<Conversation, String> {
    let mut conversation = checkpoint.conversation.clone();
    if !conversation.messages.is_empty() {
        return Err("a checkpoint carries window messages".into());
    }
    for (seq, record) in records {
        if *seq >= cut {
            break;
        }
        if record.stream() == Stream::Transcript {
            conversation
                .apply(record)
                .map_err(|e| format!("conversation: {e:?}"))?;
        }
    }
    Ok(conversation)
}

/// The run a transcript record belongs to.
fn record_run(record: &Record) -> Option<&str> {
    match record {
        Record::Assistant(assistant) => Some(&assistant.run_id),
        Record::Input(input) => Some(&input.run_id),
        _ => None,
    }
}

/// How many messages the conversation had placed before each of `transcript`'s records,
/// folding from `start`, and how many after the last. Only the count is kept, so the fold drops
/// each placed message at once.
fn placed_counts(
    start: &Conversation,
    transcript: &[(u64, Record)],
) -> Result<(Vec<u64>, u64), String> {
    let mut conversation = start.clone();
    conversation.messages.clear();
    let mut counts = Vec::with_capacity(transcript.len());
    for (_, record) in transcript {
        counts.push(conversation.placed);
        conversation
            .apply(record)
            .map_err(|e| format!("conversation: {e:?}"))?;
        conversation.messages.clear();
    }
    Ok((counts, conversation.placed))
}

fn entry(
    object: String,
    records: &[(u64, Record)],
    size: usize,
    now_ms: u64,
    ordinals: Option<(u64, u64)>,
) -> DirEntry {
    DirEntry {
        object,
        first_seq: records.first().map_or(0, |(seq, _)| *seq),
        last_seq: records.last().map_or(0, |(seq, _)| *seq),
        size: size as u64,
        sealed_at_ms: now_ms,
        first_ordinal: ordinals.map(|(first, _)| first),
        last_ordinal: ordinals.map(|(_, last)| last),
    }
}

#[cfg(test)]
mod tests;
