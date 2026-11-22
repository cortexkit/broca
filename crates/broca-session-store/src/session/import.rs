//! Appending an import's seeded history: the history of a session another runner hosted, written
//! as the first records of a new session.
//!
//! An import may hold more records than one log batch can (its frame header counts them in 16
//! bits, [`MAX_BATCH_RECORDS`]), so it is appended in as many batches as it needs. A crash between
//! them would leave part of it, so an import is never appended to a session anything reads: it
//! is written into a replacement of the empty session ([`Session::stage_replacement`]) in a
//! private place, which is published whole over the empty session once every batch is durable.
//! Until then the live session stays empty, so a crash at any point leaves no part of the import.
//!
//! What the batches name is stored first, each as a session object under the open log, made
//! durable before the first batch, as a tool result's media is:
//! - the bytes of every medium the imported messages carry;
//! - the id map: each imported message's source id with its ordinal, which the first batch's
//!   seeded run start names. A message id broca mints names where its message is
//!   (`find_message`), and an imported id doesn't, so reads resolve an imported id through the map;
//! - the receipt: what the import answered, so a retry is answered the same after the source
//!   it read is gone.

use std::sync::Arc;

use super::media::MediaError;
use super::{Appended, Session, SessionError};
use crate::backend::{SessionBackend, Version};
use crate::codec::{hex, Message as _};
use crate::head::{Head, PendingDeletion, Residence};
use crate::keys::random_id;
use crate::record::{ImportReceipt, ImportedId, ImportedIdMap, ImportedIds, Record};

/// The most records one log batch holds: a frame's header counts its batch's records in 16 bits.
pub const MAX_BATCH_RECORDS: usize = u16::MAX as usize;

/// The bytes of one medium of a message in a batch about to be appended: an input message's, or
/// one of the messages a queued send or a steer carries.
#[derive(Debug, Clone)]
pub struct NewInputMedia {
    /// The record's position in the batch.
    pub record: usize,
    /// The message's position in the record's messages: 0 for an `Input`, which holds one.
    pub message: u32,
    /// The message's content block holding the medium.
    pub block: u32,
    /// For a medium inside a tool result block: its position in the result's parts.
    pub part: Option<u32>,
    pub bytes: Vec<u8>,
}

/// Whether `mid` has the form of an id broca mints or converts (`g<generation>.<seq>`,
/// `m<position>`), which `find_message` resolves by where it points. An imported id of that form
/// could meet one broca mints later, so an import refuses it.
pub fn is_broca_id_form(mid: &str) -> bool {
    if let Some(rest) = mid.strip_prefix('g') {
        return rest
            .split_once('.')
            .is_some_and(|(_, seq)| super::read::canonical_u64(seq).is_some());
    }
    if let Some(rest) = mid.strip_prefix('m') {
        let digits = rest.split_once('.').map_or(rest, |(_, digits)| digits);
        return super::read::canonical_u64(digits).is_some();
    }
    false
}

/// A session's loaded id map, kept once read: it never changes after the import wrote it.
pub(crate) type IdMapCache = std::sync::Mutex<Option<Arc<ImportedIdMap>>>;

/// The name of the id map object `ids` points at.
pub(crate) fn id_map_name(ids: &ImportedIds) -> String {
    format!(
        "{}/a/{}.{}-imported-ids",
        hex(&ids.log.0),
        &hex(&ids.generation.0)[..16],
        ids.seq,
    )
}

/// The name of the receipt object beside the id map `ids` points at.
pub(crate) fn receipt_name(ids: &ImportedIds) -> String {
    format!(
        "{}/a/{}.{}-import-receipt",
        hex(&ids.log.0),
        &hex(&ids.generation.0)[..16],
        ids.seq,
    )
}

/// An import ready to append (see [`Session::prepare_import`]).
pub struct PreparedImport {
    /// The seq the import's first record takes.
    pub first: u64,
    /// The import's records as they are appended.
    pub records: Vec<Record>,
    map: Arc<ImportedIdMap>,
}

impl<B: SessionBackend> Session<B> {
    /// The name of the id map object `ids` points at.
    fn id_map_name(&self, ids: &ImportedIds) -> String {
        id_map_name(ids)
    }

    /// The name of the receipt object beside the id map `ids` points at.
    fn receipt_name(&self, ids: &ImportedIds) -> String {
        receipt_name(ids)
    }

    /// Appends an import's seeded history, each batch synced, after storing what it names (see
    /// [`Session::prepare_import`]). It takes as many batches as it needs, each of at most
    /// [`MAX_BATCH_RECORDS`] records, so a crash may leave only some of them: an import is
    /// appended to a replacement in staging (see [`Session::stage_replacement`]), never to a
    /// session anything reads. Returns the seq of the first record and the records as appended.
    pub async fn append_import(
        &mut self,
        records: &[Record],
        media: Vec<NewInputMedia>,
        ids: Vec<(String, u64)>,
        receipt: &ImportReceipt,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        let prepared = self.prepare_import(records, media, ids, receipt).await?;
        for batch in prepared.records.chunks(MAX_BATCH_RECORDS) {
            self.append_import_batch(&prepared, batch).await?;
        }
        Ok((prepared.first, prepared.records))
    }

    /// Stores, synced, what an import's records name: each medium `media` lists, checked against
    /// the reference its message holds; the id map of `ids` (source id and ordinal); and the
    /// import's `receipt`. Appends nothing: the records come back with the first, the seeded run
    /// start, naming where the id map and receipt are, and each input naming its stored media,
    /// for [`Session::append_import_batch`] to append in order from the session's next seq.
    pub async fn prepare_import(
        &mut self,
        records: &[Record],
        media: Vec<NewInputMedia>,
        ids: Vec<(String, u64)>,
        receipt: &ImportReceipt,
    ) -> Result<PreparedImport, SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        let first = self.next_seq();
        let mut records = records.to_vec();
        // An import's media are its messages' only; the objects are synced with the id map's
        // and receipt's below.
        if let Some(new) = media
            .iter()
            .find(|new| !matches!(records.get(new.record), Some(Record::Input(_))))
        {
            return Err(SessionError::Media(MediaError::NotMedia {
                record: new.record,
                part: new.block as usize,
            }));
        }
        self.store_input_media(&mut records, first, media).await?;
        let Some(Record::RunStart(start)) = records.first_mut() else {
            return Err(SessionError::Fold(
                "an import's batch starts with its seeded run start".into(),
            ));
        };
        let pointer = ImportedIds {
            log: self.log,
            generation: self.head.generation,
            seq: first,
        };
        let mut ids: Vec<ImportedId> = ids
            .into_iter()
            .map(|(mid, ordinal)| ImportedId { mid, ordinal })
            .collect();
        ids.sort_by(|a, b| a.mid.cmp(&b.mid));
        if ids.windows(2).any(|pair| pair[0].mid == pair[1].mid) {
            return Err(SessionError::Fold("an imported message id repeats".into()));
        }
        let map = ImportedIdMap { ids };
        for (name, body) in [
            (self.id_map_name(&pointer), map.encode()),
            (self.receipt_name(&pointer), receipt.encode()),
        ] {
            let sealed = self.key.frame_key().seal_unit(name.as_bytes(), &body);
            self.backend
                .put_object(&self.addr, &name, &sealed, self.version)
                .await?;
        }
        start.imported_ids = Some(pointer);
        self.backend
            .sync_objects(&self.addr, &self.log, self.version)
            .await?;
        Ok(PreparedImport {
            first,
            records,
            map: Arc::new(map),
        })
    }

    /// Appends `batch`, synced: the next records of `prepared` (`prepared.records` in order, at
    /// most [`MAX_BATCH_RECORDS`] at a time). Returns the seq of its first record.
    pub async fn append_import_batch(
        &mut self,
        prepared: &PreparedImport,
        batch: &[Record],
    ) -> Result<u64, SessionError> {
        let at = self.append(batch, true).await?;
        *self.imported.lock().expect("id map cache") = Some(Arc::clone(&prepared.map));
        Ok(at)
    }

    /// The version of the head this writer last committed.
    pub fn head_version(&self) -> Version {
        self.version
    }

    /// A replacement for this session, created in `staging`, a private backend nothing else
    /// reads, for an import to be written into before it is published over this session (see
    /// the module doc). It is this session as identified and keyed (session id, bind triple,
    /// incarnation, data key, mode and conversation layout) with an empty log of its own; its
    /// head lists this session's open log, which the replacement no longer names, for deletion
    /// from `now_ms` on, after the usual grace period. Only a session that never sealed or
    /// changed its log is replaced: everything else it stores would be left unnamed.
    pub async fn stage_replacement<S: SessionBackend>(
        &self,
        staging: S,
        now_ms: u64,
    ) -> Result<Session<S>, SessionError> {
        let head = &self.head;
        if head.last_change.is_some()
            || head.transcript_dir.is_some()
            || head.ledger_dir.is_some()
            || head.id_index.is_some()
            || !head.pending.is_empty()
        {
            return Err(SessionError::Fold(
                "only a session that never changed its log is replaced".into(),
            ));
        }
        let log = random_id();
        let replacement = Head {
            format: head.format,
            session_id: head.session_id,
            triple: head.triple.clone(),
            incarnation: head.incarnation,
            history_version: self.operations.history_version,
            residence: Residence::Creating(log),
            features: head.features.clone(),
            keys: head.keys.clone(),
            generation: random_id(),
            next_seq: 1,
            open_log: None,
            last_change: None,
            transcript_dir: None,
            ledger_dir: None,
            pending: vec![PendingDeletion {
                change: random_id(),
                unreferenced_at_ms: now_ms,
                logs: vec![self.log],
                objects: Vec::new(),
                retired_index: None,
                abandoned: None,
            }],
            id_index: None,
            cold_since_ms: None,
            appends_in_conversation: head.appends_in_conversation,
            cipher: head.cipher,
        };
        Session::create_from_head(staging, self.addr.clone(), replacement, &self.key, log).await
    }

    /// Takes `reloaded`, this session loaded again after its head moved to a history written
    /// elsewhere (an import's replacement published over it), in this writer's place. What the
    /// load can't know carries over: the observer of this writer's appends, and the retired-tag
    /// floor (the tag floor the address's expiry markers carry across a deletion, which the
    /// opener raised on this writer). The observer is told the history was replaced, since none of the new
    /// history's records were appended through it.
    pub fn replace_with(&mut self, reloaded: Session<B>) {
        let observer = self.observer.take();
        let floor = self.retired_tag_floor;
        *self = reloaded;
        self.observer = observer;
        self.raise_retired_tag_floor(floor);
        self.observed(Appended::Replaced);
    }

    /// The receipt the session's import kept, or `None` for a session that imported nothing. A
    /// receipt the session names but can't read is damage.
    pub async fn import_receipt(&self) -> Result<Option<ImportReceipt>, SessionError> {
        let Some(pointer) = self.operations.imported_ids.clone() else {
            return Ok(None);
        };
        let name = self.receipt_name(&pointer);
        let bytes = self.read_import_object(&name).await?;
        ImportReceipt::decode(&bytes)
            .map(Some)
            .map_err(|error| SessionError::ImportedIdsDamaged(format!("{name}: {error}")))
    }

    /// The decrypted bytes of an import's object `name`; missing or unopenable is damage.
    async fn read_import_object(&self, name: &str) -> Result<Vec<u8>, SessionError> {
        let damaged = |reason: &str| SessionError::ImportedIdsDamaged(format!("{name}: {reason}"));
        let sealed = self
            .backend
            .get_object(&self.addr, name)
            .await?
            .ok_or_else(|| damaged("missing"))?;
        self.key
            .frame_key()
            .open_unit(name.as_bytes(), &sealed)
            .ok_or_else(|| damaged("doesn't open under the session key"))
    }

    /// The ordinal the import's id map gives an imported message's source id, or `None` when
    /// the session imported nothing or no imported message had that id. A map the session names
    /// but can't read is damage.
    pub(super) async fn imported_ordinal(&self, mid: &str) -> Result<Option<u64>, SessionError> {
        let Some(pointer) = self.operations.imported_ids.clone() else {
            return Ok(None);
        };
        let cached = self.imported.lock().expect("id map cache").clone();
        let map = match cached {
            Some(map) => map,
            None => {
                self.storage_read()?;
                let name = self.id_map_name(&pointer);
                let damaged =
                    |reason: &str| SessionError::ImportedIdsDamaged(format!("{name}: {reason}"));
                let bytes = self.read_import_object(&name).await?;
                let map = ImportedIdMap::decode(&bytes)
                    .map_err(|error| damaged(&format!("doesn't decode: {error}")))?;
                if map.ids.windows(2).any(|pair| pair[0].mid >= pair[1].mid) {
                    return Err(damaged("not sorted by id"));
                }
                let map = Arc::new(map);
                *self.imported.lock().expect("id map cache") = Some(map.clone());
                map
            }
        };
        Ok(map
            .ids
            .binary_search_by(|id| id.mid.as_str().cmp(mid))
            .ok()
            .map(|at| map.ids[at].ordinal))
    }

    /// On a session an import moved in, the entry the move's first view cuts through
    /// (`Operations::move_cut`), as its id and ordinal: no later view may cut before it (the
    /// compaction contract's MV7). `None` on any other session, and when the session no longer
    /// holds that entry.
    pub async fn moved_through(&self) -> Result<Option<(String, u64)>, SessionError> {
        let Some(mid) = self.operations.move_cut.clone() else {
            return Ok(None);
        };
        Ok(self
            .entry_by_mid(&mid)
            .await?
            .map(|entry| (mid, entry.ordinal)))
    }
}

#[cfg(test)]
mod tests;
