//! Appending an import's seeded batch: the history of a session another runner hosted, written
//! as the first records of a new session.
//!
//! An import is one batch, synced, so a crash leaves either none of it or all of it. What the
//! batch names is stored first, each as a session object under the open log, made durable
//! before the batch, as a tool result's media is:
//! - the bytes of every medium the imported messages carry;
//! - the id map: each imported message's source id with its ordinal, which the batch's seeded
//!   run start names. A message id broca mints names where its message is (`find_message`), and
//!   an imported id doesn't, so reads resolve an imported id through the map;
//! - the receipt: what the import answered, so a retry is answered the same after the source
//!   it read is gone. A crash before the batch leaves the session empty, and the retry writes
//!   both objects again under the new attempt's names.

use std::sync::Arc;

use super::media::{bytes_match, input_medium, MediaError};
use super::{Session, SessionError};
use crate::backend::SessionBackend;
use crate::codec::{hex, Message as _};
use crate::record::{
    ImportReceipt, ImportedId, ImportedIdMap, ImportedIds, InputMedia, MediaObject, Record,
};

/// The bytes of one medium of an input message in a batch about to be appended.
#[derive(Debug, Clone)]
pub struct NewInputMedia {
    /// The `Input` record's position in the batch.
    pub record: usize,
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

impl<B: SessionBackend> Session<B> {
    /// The name of the id map object `ids` points at.
    fn id_map_name(&self, ids: &ImportedIds) -> String {
        format!(
            "{}/a/{}.{}-imported-ids",
            hex(&ids.log.0),
            &hex(&ids.generation.0)[..16],
            ids.seq,
        )
    }

    /// The name of the receipt object beside the id map `ids` points at.
    fn receipt_name(&self, ids: &ImportedIds) -> String {
        format!(
            "{}/a/{}.{}-import-receipt",
            hex(&ids.log.0),
            &hex(&ids.generation.0)[..16],
            ids.seq,
        )
    }

    /// Appends an import's seeded batch, synced, after storing what it names: each medium
    /// `media` lists, checked against the reference its message holds; the id map of `ids`
    /// (source id and ordinal); and the import's `receipt`. The batch's first record, its seeded
    /// run start, names where the id map and receipt are. Returns the seq of the first record
    /// and the records as appended.
    pub async fn append_import(
        &mut self,
        records: &[Record],
        media: Vec<NewInputMedia>,
        ids: Vec<(String, u64)>,
        receipt: &ImportReceipt,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        let first = self.next_seq();
        let mut records = records.to_vec();
        // Objects this batch writes, by digest: later media of the batch with the same bytes
        // point at the same object. The open log's index learns them only once the batch is
        // appended, so a failed import leaves it naming nothing the log doesn't.
        let mut stored: std::collections::HashMap<String, MediaObject> =
            std::collections::HashMap::new();
        for new in media {
            let (record, block, part) = (new.record, new.block, new.part);
            let not_media = || {
                SessionError::Media(MediaError::NotMedia {
                    record,
                    part: block as usize,
                })
            };
            let Some(Record::Input(input)) = records.get(record) else {
                return Err(not_media());
            };
            let reference = input_medium(&input.message, block, part)
                .cloned()
                .ok_or_else(not_media)?;
            if !bytes_match(&reference, &new.bytes) {
                return Err(SessionError::Media(MediaError::Mismatch {
                    record,
                    part: block as usize,
                }));
            }
            let known = self
                .window
                .media
                .get(&reference.sha256)
                .or_else(|| stored.get(&reference.sha256))
                .cloned();
            let object = match known {
                Some(object) => object,
                None => {
                    let object = MediaObject {
                        part: 0,
                        log: self.log,
                        generation: self.head.generation,
                        seq: first + record as u64,
                    };
                    let name = self.media_name_at(
                        &object.log,
                        &object.generation,
                        object.seq,
                        &reference.sha256,
                    );
                    let sealed = self.key.frame_key().seal_unit(name.as_bytes(), &new.bytes);
                    self.backend
                        .put_object(&self.addr, &name, &sealed, self.version)
                        .await?;
                    stored.insert(reference.sha256.clone(), object.clone());
                    object
                }
            };
            let Some(Record::Input(input)) = records.get_mut(record) else {
                unreachable!("checked above");
            };
            input.media.push(InputMedia {
                block,
                part,
                log: object.log,
                generation: object.generation,
                seq: object.seq,
            });
        }
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
        let at = self.append(&records, true).await?;
        *self.imported.lock().expect("id map cache") = Some(Arc::new(map));
        Ok((at, records))
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
}

#[cfg(test)]
mod tests;
