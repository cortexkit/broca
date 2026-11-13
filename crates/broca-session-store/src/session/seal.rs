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

use super::media::OpenLogIndex;
use std::collections::BTreeSet;

use crate::backend::SessionBackend;
use crate::checkpoint::Checkpoint;
use crate::codec::{hex, Id128, Message as _};
use crate::directory::{DirEntry, Pages, FANOUT};
use crate::fold::conversation::Conversation;
use crate::fold::operations::Operations;
use crate::head::{self, LastChange, OpenLog, PendingDeletion};
use crate::idindex::{self, IndexPages, IndexRoot, OpenIds, Retired};
use crate::keys::random_id;
use crate::log::{self, ChainBase, LogEncoder};
use crate::record::{ChangeAborted, ChangeIntent, ChangeKind, Record, Stream};
use crate::segment::{self, SegmentMeta};

use super::{log_bind, Session, SessionError};

/// How long what a change stopped naming is kept, so a reader that read the head just before
/// the commit can finish reading, and a backup's walk of the store that began before the commit
/// still finds every file of the head version current when it began, which restore may choose.
/// It is the period head versions are kept for (`backend::HEAD_VERSION_KEEP_MS`), and both count
/// from the change: the head versions from before it, which name its deletions, come due for
/// pruning when its deletions come due, and the head write that ends a collection prunes them.
pub const GRACE_MS: u64 = crate::backend::HEAD_VERSION_KEEP_MS;

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
pub(super) struct Created {
    pub(super) sealed: Sealed,
    pub(super) encoder: LogEncoder,
    /// The session's state once the change commits.
    pub(super) conversation: Conversation,
    pub(super) operations: Operations,
    /// The new log's `EntryFacts` records, by seq, and its ledger records' ids.
    pub(super) window: OpenLogIndex,
    pub(super) open_ids: OpenIds,
    pub(super) transcript_dir: Option<crate::directory::PageRef>,
    pub(super) ledger_dir: Option<crate::directory::PageRef>,
    pub(super) id_index: String,
    /// The id-index list of runs merges retired, deleted after the grace period.
    pub(super) retired_index: Option<String>,
    pub(super) replaced: Vec<String>,
    /// A revert's new history version, and its list of abandoned objects.
    pub(super) history_version: Option<u64>,
    pub(super) abandoned: Option<String>,
    /// Set by the cold step: the head records when the session went cold.
    pub(super) cold_since_ms: Option<u64>,
    /// What the change saves of the session's media, which the commit installs.
    pub(super) media: Option<super::media_index::MediaCommit>,
}

/// The open log as read back: its records, its batches, and its checkpoint.
pub(super) type OpenLogRead = (Vec<(u64, Record)>, Vec<std::ops::Range<usize>>, Checkpoint);

/// What sealing a change's ledger records wrote.
pub(super) struct LedgerSealed {
    pub(super) ledger_dir: Option<crate::directory::PageRef>,
    pub(super) replaced: Vec<String>,
    pub(super) id_index: String,
    pub(super) retired_index: Option<String>,
    pub(super) bytes: u64,
    pub(super) index_entries: usize,
    pub(super) merged_entries: u64,
    pub(super) forced_merges: u32,
}

/// What a seal starts the new open log with besides its checkpoint.
#[derive(Debug, Default)]
pub struct Carry<'a> {
    /// Ledger records the caller needs whole in the open log and that the seal would otherwise
    /// move out, such as full copies of compaction views whose stored edits are changes against
    /// earlier views. They take fresh seqs after the checkpoint, and they're part of the change,
    /// so the log never exists without them.
    pub records: &'a [Record],
    /// For a cut through a run: that run's repetition-warning counters at the cut, as the loop
    /// computes them (the store doesn't read them). The new log's conversation keeps them, so a
    /// request still counts the run's sealed steps. See `Conversation::window_repetition`.
    pub window_repetition: Option<serde_json::Value>,
}

impl<B: SessionBackend> Session<B> {
    /// Seals the open log. With `cut`, the seq of the first record a compaction view keeps,
    /// the transcript records before it are sealed too; it must start an entry (a user input or
    /// an assistant message) with no assistant message waiting for replies before it.
    pub async fn seal(&mut self, cut: Option<u64>, now_ms: u64) -> Result<Sealed, SessionError> {
        self.seal_carrying(cut, &Carry::default(), now_ms).await
    }

    /// [`Session::seal`], starting the new open log with what `carry` names.
    pub async fn seal_carrying(
        &mut self,
        cut: Option<u64>,
        carry: &Carry<'_>,
        now_ms: u64,
    ) -> Result<Sealed, SessionError> {
        // A seal holds the open log's records and the state it builds across its writes, tens of
        // kilobytes in a debug build. Boxed, so every future that awaits a seal (a store actor's,
        // a run's) carries one pointer for it.
        Box::pin(self.seal_carrying_boxed(cut, carry, now_ms)).await
    }

    /// The work of [`Session::seal_carrying`], which boxes it.
    async fn seal_carrying_boxed(
        &mut self,
        cut: Option<u64>,
        carry: &Carry<'_>,
        now_ms: u64,
    ) -> Result<Sealed, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        if self.operations.open_change.is_some() {
            return Err(SessionError::ChangeOpen);
        }
        if carry
            .records
            .iter()
            .any(|record| record.stream() != Stream::Ledger)
        {
            return Err(SessionError::Fold(
                "a seal carries only ledger records into the new log".into(),
            ));
        }
        self.check_views_carried(carry)?;
        let open = self.read_open_log().await?;
        if let Some(cut) = cut {
            self.check_cut(&open.0, &open.2, cut)?;
        }
        let kind = if cut.is_some() {
            ChangeKind::Cut
        } else {
            ChangeKind::LedgerSeal
        };
        // Sealing owns several copies of folded state; keep that future off callers' stacks.
        Box::pin(self.seal_read(kind, cut, false, carry, open, now_ms)).await
    }

    /// [`Session::seal_carrying`] at or before the open-log entry with ordinal `ordinal`: the
    /// transcript records before it are sealed. A compaction cut names its first kept entry this
    /// way. A log can only start at an input or an assistant message with no tool turn open
    /// before it, so when that entry is a tool turn's results, the seal falls at the nearest
    /// earlier entry that can start one: it may keep more than the cut keeps, never less.
    ///
    /// `None` when that entry is the open log's first message, so no message would leave the
    /// window: nothing is written. A caller sealing at every cut it sees can ask again for the
    /// same cut without rewriting the log each time.
    pub async fn seal_at_entry(
        &mut self,
        ordinal: u64,
        carry: &Carry<'_>,
        now_ms: u64,
    ) -> Result<Option<Sealed>, SessionError> {
        let cut = {
            // Dropped before the seal, which reads the open log again itself.
            let (records, _, checkpoint) = self.read_open_log().await?;
            let cut = placing_seq(&checkpoint, &records, ordinal)?;
            let places_before_cut = records.iter().any(|(seq, record)| {
                *seq < cut && matches!(record, Record::Input(_) | Record::Assistant(_))
            });
            if !places_before_cut {
                return Ok(None);
            }
            cut
        };
        self.seal_carrying(Some(cut), carry, now_ms).await.map(Some)
    }

    /// Every seal moves the ledger out of the open log, and with it the `View` records the
    /// session's accepted and applied views are stored in. Those are stored as changes against
    /// earlier views, which only the compaction code can turn back into whole views, so the store
    /// can't keep them itself: the caller carries a copy of each view the session names, and a
    /// `ViewApplied` naming the applied one. A seal that would lose either refuses before it
    /// writes anything.
    pub(super) fn check_views_carried(&self, carry: &Carry<'_>) -> Result<(), SessionError> {
        let view = |slot: &crate::fold::operations::ViewSlot| {
            carry.records.iter().any(|record| {
                matches!(record, Record::View(view)
                    if view.version == slot.version && view.checksum == slot.checksum)
            })
        };
        let accepted = self.operations.view_accepted.as_ref().is_none_or(view);
        let applied = self.operations.view_applied.as_ref().is_none_or(|applied| {
            view(&applied.view)
                && carry.records.iter().any(|record| {
                    matches!(record, Record::ViewApplied(named)
                        if named.version == applied.view.version
                            && named.checksum == applied.view.checksum)
                })
        });
        if accepted && applied {
            Ok(())
        } else {
            Err(SessionError::ViewNotCarried)
        }
    }

    /// A change's new log starts from a checkpoint whose operational state must still name the
    /// session's enrollment in the current compaction contract, as it names a view the session
    /// holds (see [`Session::check_views_carried`]): the enrollment record moves out of the open
    /// log with the rest of the ledger, and only the fold says the session enrolled. A change
    /// that would lose it refuses before it commits.
    pub(super) fn check_enrollment_carried(
        &self,
        created: Created,
    ) -> Result<Created, SessionError> {
        if created.operations.enrolled == self.operations.enrolled {
            Ok(created)
        } else {
            Err(SessionError::EnrollmentNotCarried)
        }
    }

    /// Runs a seal over an open log already read. `prune_window` also prunes finished runs'
    /// opaque reasoning from the transcript records it copies into the new log; the caller
    /// passes it only when no run is unfinished.
    pub(super) async fn seal_read(
        &mut self,
        kind: ChangeKind,
        cut: Option<u64>,
        prune_window: bool,
        carry: &Carry<'_>,
        open: OpenLogRead,
        now_ms: u64,
    ) -> Result<Sealed, SessionError> {
        let (records, batches, checkpoint) = open;
        let addr = self.addr.clone();
        let _timing = crate::metrics::Phase::Seal
            .start()
            .with_session(|| Some(addr.0));
        let change = random_id();
        let intent = Record::ChangeIntent(ChangeIntent { change, kind, cut });
        let intent_seq = self.append(std::slice::from_ref(&intent), true).await?;
        let mut records = records;
        records.push((intent_seq, intent));

        let created = self
            .create_objects(
                change,
                cut,
                prune_window,
                carry,
                intent_seq,
                &records,
                &batches,
                &checkpoint,
                now_ms,
            )
            .await;
        let mut created = match created.and_then(|created| self.check_enrollment_carried(created)) {
            Ok(created) => created,
            Err(error) => {
                self.abort(change).await;
                return Err(error);
            }
        };
        if kind == ChangeKind::Cold {
            created.cold_since_ms = Some(now_ms);
        }
        // The carried records took the last seqs of the new log.
        let carried = created.encoder.next_seq() - carry.records.len() as u64;
        let sealed = self.commit(change, intent_seq, created, now_ms).await?;
        // The commit made every record before it durable.
        self.observed(super::Appended::Synced);
        if !carry.records.is_empty() {
            self.observed(super::Appended::Records {
                first: carried,
                records: carry.records,
                durable: true,
            });
        }
        crate::metrics::sealed(&sealed);
        Ok(sealed)
    }

    /// The open log's records, its batches and its checkpoint, read back from the backend.
    pub(super) async fn read_open_log(&self) -> Result<OpenLogRead, SessionError> {
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
        if !starts_entry || before.open_tool_turn().is_some() || owes_facts(records, cut) {
            return Err(SessionError::BadCut(cut));
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    async fn create_objects(
        &mut self,
        change: Id128,
        cut: Option<u64>,
        prune_window: bool,
        carry: &Carry<'_>,
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
                // The runs whose settings still shape a request after the cut stay with the
                // conversation the new log starts from.
                let first_kept = at.placed + 1;
                let starts_run = records.iter().find_map(|(seq, record)| match record {
                    Record::Input(input) if *seq == cut => Some(input.run_id.as_str()),
                    _ => None,
                });
                at.seal_window(first_kept, starts_run)
                    .map_err(|error| SessionError::Fold(format!("conversation: {error:?}")))?;
                at.window_repetition = carry.window_repetition.clone();
                at
            }
            None => checkpoint.conversation.clone(),
        };
        // The window's records are folded again below, as the session folded them.
        let mut at_cut = at_cut;
        at_cut.enrolled(self.operations.enrolled_through());
        // The commit settles the intent, so state carried past it has no open change.
        let mut settled = self.operations.clone();
        settled.open_change = None;
        // The media index current through the intent, saved with the new checkpoint; an index the
        // checkpoint no longer names is deleted with the rest of what the change replaced.
        let (mut media, media_replaced) = self.media_for_seal(&name, intent_seq).await?;

        let pages = Pages {
            backend: &self.backend,
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
        replaced.extend(media_replaced);
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
                    log_start: None,
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
        // The new open log starts with this checkpoint. The ledger segment keeps a copy, so a
        // damaged checkpoint can be rebuilt.
        let transcript_base = transcript
            .last()
            .map(|(seq, _)| *seq)
            .or(checkpoint.transcript_base);
        let checkpoint_body = Checkpoint {
            covers_through: intent_seq,
            next_seq: self.encoder.next_seq(),
            conversation: at_cut.clone(),
            operational: settled.clone(),
            transcript_base,
            ledger_base: Some(intent_seq),
            activity_ms: self.window.newest_activity_ms,
            media: media.member(),
        }
        .encode();
        let ledger_sealed = self
            .seal_ledger(&name, &ledger, &settled, &checkpoint_body, now_ms)
            .await?;
        sealed.ledger_bytes = ledger_sealed.bytes;
        sealed.index_entries = ledger_sealed.index_entries;
        sealed.merged_entries = ledger_sealed.merged_entries;
        sealed.forced_merges = ledger_sealed.forced_merges;
        replaced.extend(ledger_sealed.replaced);

        // The new open log: the checkpoint, then the transcript records that stay open, copied
        // batch by batch with their seqs.
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
            .checkpoint(&checkpoint_body)?
            .into_written(&mut encoder);
        let settled_after = settled;
        let mut conversation = at_cut;
        let mut window =
            OpenLogIndex::from_checkpoint(self.window.newest_activity_ms, self.encoder.next_seq());
        for batch in batches {
            let kept: Vec<(u64, Record)> = records[batch.clone()]
                .iter()
                .filter(|(seq, record)| {
                    record.stream() == Stream::Transcript && !below(*seq, record)
                })
                .map(|(seq, record)| {
                    let mut record = record.clone();
                    if prune_window && record_run(&record).is_some() {
                        crate::prune::prune_record(&mut record, &prune_key);
                    }
                    (*seq, record)
                })
                .collect();
            let kept: Vec<(u64, &Record)> =
                kept.iter().map(|(seq, record)| (*seq, record)).collect();
            if kept.is_empty() {
                continue;
            }
            for (seq, record) in &kept {
                conversation
                    .apply(record)
                    .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
                window.note(*seq, record);
            }
            sealed.kept_records += kept.len();
            bytes.extend(encoder.copy_batch(&kept)?.into_written(&mut encoder));
        }
        // The carried ledger records follow as one batch with fresh seqs, above every record the
        // seal read, and fold into the state the change installs.
        let mut settled_after = settled_after;
        let mut open_ids = OpenIds::new(idindex::hash_key(&self.key));
        let carried = carry.records;
        if !carried.is_empty() {
            let first = encoder.next_seq();
            let encoded = encoder.batch(carried)?;
            let places: Vec<_> = encoded.places(encoder.offset()).collect();
            bytes.extend(encoded.into_written(&mut encoder));
            for ((offset, record), place) in carried.iter().enumerate().zip(places) {
                let seq = first + offset as u64;
                window.note(seq, record);
                open_ids.add(seq, record, place);
                media.fold(seq, record, &conversation, &settled_after);
                super::apply(&mut conversation, &mut settled_after, seq, record)
                    .map_err(SessionError::Fold)?;
            }
        }
        media.flush();
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
            operations: settled_after,
            window,
            open_ids,
            transcript_dir,
            ledger_dir: ledger_sealed.ledger_dir,
            id_index: ledger_sealed.id_index,
            retired_index: ledger_sealed.retired_index,
            replaced,
            history_version: None,
            abandoned: None,
            cold_since_ms: None,
            media: Some(media),
        })
    }

    /// Seals a change's ledger records into one ledger segment, with `settled` (the operational
    /// state once the change commits) as its trailer, and files their ids in the id index.
    pub(super) async fn seal_ledger(
        &self,
        name: &str,
        ledger: &[(u64, Record)],
        settled: &Operations,
        log_start: &[u8],
        now_ms: u64,
    ) -> Result<LedgerSealed, SessionError> {
        let key = self.key.frame_key();
        let pages = Pages {
            backend: &self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
            fanout: FANOUT,
        };
        let mut replaced = Vec::new();
        let object = format!("{name}/l");
        let bytes = segment::build(
            &key,
            object.as_bytes(),
            ledger,
            SegmentMeta {
                session_id: self.head.session_id,
                created_at_ms: now_ms,
                prefixes: Vec::new(),
                leading: None,
                trailer: Some(settled.encode()),
                log_start: Some(log_start.to_vec()),
                placed_before: Vec::new(),
            },
        )
        .map_err(SessionError::Segment)?;
        let size = bytes.len() as u64;
        self.backend
            .put_object(&self.addr, &object, &bytes, self.version)
            .await?;
        let appended = pages
            .append(
                self.head.ledger_dir.as_ref(),
                entry(object, ledger, bytes.len(), now_ms, None),
                &format!("{name}/dl"),
                self.version,
            )
            .await
            .map_err(SessionError::Directory)?;
        replaced.extend(appended.replaced);
        let index = IndexPages {
            backend: &self.backend,
            addr: &self.addr,
            session: self.head.session_id,
            key: &key,
        };
        let root: IndexRoot = match &self.head.id_index {
            Some(name) => index.read(name).await.map_err(SessionError::Index)?,
            None => IndexRoot::default(),
        };
        let entries = idindex::entries_for(&idindex::hash_key(&self.key), ledger);
        let index_entries = entries.len();
        let updated = index
            .update(root, entries, name, self.version)
            .await
            .map_err(SessionError::Index)?;
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
        Ok(LedgerSealed {
            ledger_dir: Some(appended.root),
            replaced,
            id_index,
            retired_index,
            bytes: size,
            index_entries,
            merged_entries: updated.merged_entries,
            forced_merges: updated.forced,
        })
    }

    /// The commit: one compare-and-swap of the head. A refusal or an unknown outcome poisons the
    /// writer, which never undoes a change it can't tell didn't commit: the next load decides.
    pub(super) async fn commit(
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
        if let Some(version) = created.history_version {
            head.history_version = version;
        }
        if created.cold_since_ms.is_some() {
            head.cold_since_ms = created.cold_since_ms;
        }
        head.pending.push(PendingDeletion {
            change,
            unreferenced_at_ms: now_ms,
            logs: vec![self.log],
            objects: created.replaced,
            retired_index: created.retired_index,
            abandoned: created.abandoned,
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
        self.operations = created.operations;
        // Every ledger record the old log held is sealed, and its ids are in the index now;
        // facts of messages that left the window are found through the directory.
        self.open_ids = created.open_ids;
        self.window = created.window;
        if let Some(media) = created.media {
            self.media.install(media);
        }
        // The new log's records up to here were copied or carried, not appended, so none is kept;
        // a read asking for them reads the log.
        self.kept = super::kept::KeptRecords::from_next(self.encoder.next_seq(), false);
        self.poisoned = false;
        Ok(created.sealed)
    }

    /// Undoes a change that failed before its commit: deletes what it created and reopens the
    /// log. If that fails too (another writer took the head, or the backend is down), the writer
    /// is poisoned and the next load undoes the change.
    pub(super) async fn abort(&mut self, change: Id128) {
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
    /// those entries from the head. Returns how many changes' deletions ran; 0, with nothing
    /// read or written, when none is due.
    pub async fn collect(&mut self, now_ms: u64) -> Result<usize, SessionError> {
        let _timing = crate::metrics::Phase::Collection
            .start()
            .with_session(|| Some(self.addr.0.clone()));
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        if self.operations.open_change.is_some() {
            return Err(SessionError::ChangeOpen);
        }
        let due = super::collect::due(&self.head, now_ms);
        if due.is_empty() {
            return Ok(0);
        }
        super::collect::delete(
            &self.backend,
            &self.addr,
            self.head.session_id,
            &self.key,
            self.version,
            &due,
        )
        .await?;
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

/// Whether a message before `cut` is still waiting for its `EntryFacts` there: messages placed
/// together are written before all their facts, so a log can't start between them (see
/// `segment::groups`). A log with no facts at all owes none.
fn owes_facts(records: &[(u64, Record)], cut: u64) -> bool {
    let tracks_facts = records
        .iter()
        .any(|(_, record)| matches!(record, Record::EntryFacts(_)));
    let mut owed = 0usize;
    for (_, record) in records.iter().take_while(|(seq, _)| *seq < cut) {
        match record {
            Record::Input(_) | Record::Assistant(_) => owed += usize::from(tracks_facts),
            Record::EntryFacts(_) => owed = owed.saturating_sub(1),
            _ => {}
        }
    }
    owed > 0
}

/// The checkpoint's conversation state folded over the log's transcript records before `cut`.
/// The seq of the latest open-log record a log can start at that places an entry with ordinal at
/// most `ordinal`: an input or an assistant message, with no tool turn open before it and no
/// earlier message still waiting for its `EntryFacts` (see `segment::groups`: messages placed
/// together come before all their facts, and a log starting between them would leave a sealed
/// message's facts in the open log). Every entry the log knows counts: those it placed, and an
/// assistant message waiting for its results.
fn placing_seq(
    checkpoint: &Checkpoint,
    records: &[(u64, Record)],
    ordinal: u64,
) -> Result<u64, SessionError> {
    let known = |conversation: &Conversation| {
        conversation.placed + u64::from(conversation.open_tool_turn().is_some())
    };
    let tracks_facts = records
        .iter()
        .any(|(_, record)| matches!(record, Record::EntryFacts(_)));
    let mut owed = 0usize;
    let mut conversation = checkpoint.conversation.clone();
    let mut start = None;
    let mut reached = false;
    for (seq, record) in records {
        if record.stream() != Stream::Transcript {
            continue;
        }
        let owed_before = owed;
        match record {
            Record::Input(_) | Record::Assistant(_) => owed += usize::from(tracks_facts),
            Record::EntryFacts(_) => owed = owed.saturating_sub(1),
            _ => {}
        }
        let before = known(&conversation);
        let open_before = conversation.open_tool_turn().is_some() || owed_before > 0;
        conversation
            .apply(record)
            .map_err(|e| SessionError::Fold(format!("conversation: {e:?}")))?;
        let after = known(&conversation);
        if after > before && before < ordinal {
            if matches!(record, Record::Input(_) | Record::Assistant(_)) && !open_before {
                start = Some(*seq);
            }
            reached |= after >= ordinal;
        }
        if reached {
            break;
        }
    }
    match (reached, start) {
        (true, Some(seq)) => Ok(seq),
        (true, None) => Err(SessionError::Fold(format!(
            "no entry the open log holds up to {ordinal} can start a log"
        ))),
        (false, _) => Err(SessionError::Fold(format!(
            "the open log places no entry {ordinal}"
        ))),
    }
}

fn conversation_at(
    checkpoint: &Checkpoint,
    records: &[(u64, Record)],
    cut: u64,
) -> Result<Conversation, String> {
    let mut conversation = checkpoint.conversation.clone();
    if !conversation.messages.is_empty() || !conversation.entries.is_empty() {
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
    conversation.clear_window();
    let mut counts = Vec::with_capacity(transcript.len());
    for (_, record) in transcript {
        counts.push(conversation.placed);
        conversation
            .apply(record)
            .map_err(|e| format!("conversation: {e:?}"))?;
        conversation.clear_window();
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
