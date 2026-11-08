//! Media bytes, stored as append objects: a tool result's media parts, and the media an input
//! message carries, or a message a queued send or a steer carries before it is placed.
//!
//! Each medium's bytes live in one object, written under the open log by the append whose record
//! first names them, and made durable before that record, so no record ever names bytes a crash
//! could lose. The record keeps, for each medium, where its object is ([`MediaObject`],
//! [`crate::record::InputMedia`], [`crate::record::SendMedia`]), so a reader names the object
//! without a lookup. Bytes the open log already names are pointed at again, not written again;
//! bytes it doesn't name are written again, so a branch after a revert never names an object
//! created on the abandoned one. The copy that places a queued send's or a steer's message points
//! at the object its accepting record stored.

use std::collections::BTreeMap;

use broca_protocol::content::{MediaRef, ToolOutput, ToolOutputPart, ToolResult};
use sha2::{Digest as _, Sha256};

use super::{Session, SessionError};
use crate::backend::SessionBackend;
use crate::codec::hex;
use crate::record::{InputMedia, MediaObject, Record, SendMedia};

/// What the session keeps about the open log's records, rebuilt whenever the open log is
/// replaced and kept current on every append.
#[derive(Debug, Default)]
pub(crate) struct OpenLogIndex {
    /// The ordinal each `EntryFacts` record names, by that record's seq and, for a message whose
    /// id an earlier record announced (a step's or a batch's start), by that record's seq too,
    /// so `find_message` finds a new message's id before it's sealed.
    pub facts: BTreeMap<u64, u64>,
    /// The object each media digest the open log names is stored as.
    pub media: BTreeMap<String, MediaObject>,
    /// The newest time any of the open log's records says something happened in the session
    /// ([`Record::activity_ms`]), which `session.head` serves as the session's last update.
    pub newest_activity_ms: Option<u64>,
}

impl OpenLogIndex {
    /// Takes in one record of the open log.
    pub fn note(&mut self, seq: u64, record: &Record) {
        if let Some(at) = record.activity_ms() {
            self.newest_activity_ms =
                Some(self.newest_activity_ms.map_or(at, |newest| newest.max(at)));
        }
        match record {
            Record::EntryFacts(facts) => {
                self.facts.insert(seq, facts.ordinal);
                if let Some(announced) = super::read::minted_seq(&facts.mid) {
                    self.facts.entry(announced).or_insert(facts.ordinal);
                }
            }
            Record::ToolReply(reply) => {
                for object in &reply.media {
                    if let Some(media) = media_part(&reply.result, object.part) {
                        self.media
                            .entry(media.sha256.clone())
                            .or_insert_with(|| object.clone());
                    }
                }
            }
            _ => {
                for (sha256, object) in stored_media(record) {
                    self.media.entry(sha256).or_insert(object);
                }
            }
        }
    }
}

/// The media whose bytes a record of messages stores: an input message's, or a queued send's or
/// a steer's messages', each by digest with the object its bytes are (`part` is 0). In record
/// order; a digest the record names twice is listed twice.
pub fn stored_media(record: &Record) -> Vec<(String, MediaObject)> {
    let object = |log, generation, seq| MediaObject {
        part: 0,
        log,
        generation,
        seq,
    };
    match record {
        Record::Input(input) => input
            .media
            .iter()
            .filter_map(|stored| {
                input_medium(&input.message, stored.block, stored.part).map(|medium| {
                    (
                        medium.sha256.clone(),
                        object(stored.log, stored.generation, stored.seq),
                    )
                })
            })
            .collect(),
        _ => {
            let Some((messages, media)) = sent_messages(record) else {
                return Vec::new();
            };
            media
                .iter()
                .filter_map(|stored| {
                    messages
                        .get(stored.message as usize)
                        .and_then(|message| input_medium(message, stored.block, stored.part))
                        .map(|medium| {
                            (
                                medium.sha256.clone(),
                                object(stored.log, stored.generation, stored.seq),
                            )
                        })
                })
                .collect()
        }
    }
}

/// Every medium `message` holds, with where: its content block and, for a medium inside a tool
/// result there, its position in the result's parts.
pub fn message_media(
    message: &broca_protocol::content::Message,
) -> Vec<(u32, Option<u32>, &MediaRef)> {
    use broca_protocol::content::ContentBlock;
    let mut found = Vec::new();
    for (block, content) in message.content.iter().enumerate() {
        let block = block as u32;
        match content {
            ContentBlock::Media(media) => found.push((block, None, media)),
            ContentBlock::ToolResult(result) => {
                found.extend(media_parts(result).map(|(part, media)| (block, Some(part), media)))
            }
            _ => {}
        }
    }
    found
}

/// The messages a queued send or a steer carries before they are placed, with where their media
/// are stored. `None` for any other record.
pub fn sent_messages(
    record: &Record,
) -> Option<(&[broca_protocol::content::Message], &[SendMedia])> {
    match record {
        Record::QueuedSend(send) => Some((&send.input, &send.media)),
        Record::Steer(steer) => Some((&steer.input, &steer.media)),
        _ => None,
    }
}

/// The bytes of one media part of a tool result in a batch about to be appended.
#[derive(Debug, Clone)]
pub struct NewMedia {
    /// The `ToolReply` record's position in the batch.
    pub record: usize,
    /// The media part's position in the result's parts.
    pub part: usize,
    pub bytes: Vec<u8>,
}

/// Why a media append or read was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaError {
    /// The named record isn't a tool result, an input message, a queued send or a steer, or the
    /// named part or block isn't a medium.
    NotMedia { record: usize, part: usize },
    /// The bytes don't match the part's reference: another digest or size.
    Mismatch { record: usize, part: usize },
    /// A record says the part is stored, but the session has no such object.
    Missing(String),
    /// The object didn't decrypt under the session key and its name, or its bytes don't match
    /// the reference.
    Corrupt(String),
    /// The writer asked to store media bytes keeps no media objects. Nothing was appended.
    NoStore,
}

fn media_part(result: &ToolResult, part: u32) -> Option<&MediaRef> {
    match &result.output {
        ToolOutput::Parts { parts } => match parts.get(part as usize) {
            Some(ToolOutputPart::Media { media }) => Some(media),
            _ => None,
        },
        _ => None,
    }
}

/// The medium an input message holds at content block `block`: the block itself, or with
/// `part`, that media part of the tool result the block is.
pub(super) fn input_medium(
    message: &broca_protocol::content::Message,
    block: u32,
    part: Option<u32>,
) -> Option<&MediaRef> {
    use broca_protocol::content::ContentBlock;
    match (message.content.get(block as usize)?, part) {
        (ContentBlock::Media(media), None) => Some(media),
        (ContentBlock::ToolResult(result), Some(part)) => media_part(result, part),
        _ => None,
    }
}

/// A medium a record of the session names, found by its digest ([`Session::named_media`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NamedMedium {
    pub reference: MediaRef,
    /// Where a record stored the medium's bytes: the open log, the writer generation and the
    /// seq of the record that wrote its object. `None` when the record naming it stored none.
    stored: Option<(crate::codec::Id128, crate::codec::Id128, u64)>,
}

impl NamedMedium {
    /// Whether a record stored the medium's bytes.
    pub fn stored(&self) -> bool {
        self.stored.is_some()
    }
}

/// The media parts of a tool result, with their positions in its parts.
fn media_parts(result: &ToolResult) -> impl Iterator<Item = (u32, &MediaRef)> {
    let parts: &[ToolOutputPart] = match &result.output {
        ToolOutput::Parts { parts } => parts,
        _ => &[],
    };
    parts
        .iter()
        .enumerate()
        .filter_map(|(part, block)| match block {
            ToolOutputPart::Media { media } => Some((part as u32, media)),
            _ => None,
        })
}

/// Where a record stored a medium's bytes: the open log, writer generation and seq its object is
/// named by.
type StoredAt = (crate::codec::Id128, crate::codec::Id128, u64);

/// Every place the input message `message` names the medium with digest `sha256`, each with
/// where `stored` says the bytes at that content block (and tool result part) are.
fn named_in_message(
    message: &broca_protocol::content::Message,
    sha256: &str,
    stored: impl Fn(u32, Option<u32>) -> Option<StoredAt>,
    named: &mut Vec<NamedMedium>,
) {
    for (block, part, media) in message_media(message) {
        if media.sha256 == sha256 {
            named.push(NamedMedium {
                reference: media.clone(),
                stored: stored(block, part),
            });
        }
    }
}

/// Every place `record` names the medium with digest `sha256`, each with where the record stored
/// its bytes, if it did: an input message, a queued send's or a steer's messages, or a tool
/// result.
fn named_in(record: &Record, sha256: &str) -> Vec<NamedMedium> {
    let mut named = Vec::new();
    match record {
        Record::Input(input) => {
            let stored = |block, part| {
                input
                    .media
                    .iter()
                    .find(|object| object.block == block && object.part == part)
                    .map(|object| (object.log, object.generation, object.seq))
            };
            named_in_message(&input.message, sha256, stored, &mut named);
        }
        Record::ToolReply(reply) => {
            for (part, media) in media_parts(&reply.result).filter(|(_, m)| m.sha256 == sha256) {
                let stored = reply
                    .media
                    .iter()
                    .find(|object| object.part == part)
                    .map(|object| (object.log, object.generation, object.seq));
                named.push(NamedMedium {
                    reference: media.clone(),
                    stored,
                });
            }
        }
        _ => {
            if let Some((messages, media)) = sent_messages(record) {
                for (index, message) in messages.iter().enumerate() {
                    let stored = |block, part| {
                        media
                            .iter()
                            .find(|object| {
                                object.message as usize == index
                                    && object.block == block
                                    && object.part == part
                            })
                            .map(|object| (object.log, object.generation, object.seq))
                    };
                    named_in_message(message, sha256, stored, &mut named);
                }
            }
        }
    }
    named
}

fn matches(media: &MediaRef, bytes: &[u8]) -> bool {
    media.byte_size == bytes.len() as u64 && media.sha256 == hex(&Sha256::digest(bytes))
}

impl<B: SessionBackend> Session<B> {
    /// The name of the object `object` points at, for bytes whose digest is `sha256`. The digest
    /// is hashed with a key derived from the session key, so a name says nothing about the
    /// content outside the session, and the same bytes always get the same name.
    fn media_name(&self, object: &MediaObject, sha256: &str) -> String {
        self.media_name_at(&object.log, &object.generation, object.seq, sha256)
    }

    /// The name of the media object written under `log` by writer `generation`, named by the
    /// record at `seq`, for bytes whose digest is `sha256`.
    pub(super) fn media_name_at(
        &self,
        log: &crate::codec::Id128,
        generation: &crate::codec::Id128,
        seq: u64,
        sha256: &str,
    ) -> String {
        let key = self.key.derive(b"media-object-name");
        let h = crate::prune::keyed_digest(&key, sha256.as_bytes()).0;
        format!(
            "{}/a/{}.{}-media-{}",
            hex(&log.0),
            &hex(&generation.0)[..16],
            seq,
            &hex(&h)[..32],
        )
    }

    /// Appends `records` as [`append`](Self::append) does, after storing the bytes of each media
    /// part `media` names. Each part's bytes must match its reference. Bytes the open log already
    /// names are pointed at; others are written as new objects, all durable before the batch is
    /// appended. The batch's tool results record where each part's bytes are. Returns the seq
    /// of the first record and the records as appended.
    pub async fn append_media(
        &mut self,
        records: &[Record],
        media: Vec<NewMedia>,
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        if media.is_empty() {
            let first = self.append(records, barrier).await?;
            return Ok((first, records.to_vec()));
        }
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        let first = self.next_seq();
        let mut records = records.to_vec();
        let mut written = false;
        for new in media {
            let (record, part) = (new.record, new.part);
            let Some(Record::ToolReply(reply)) = records.get(record) else {
                return Err(SessionError::Media(MediaError::NotMedia { record, part }));
            };
            let Some(reference) = media_part(&reply.result, part as u32).cloned() else {
                return Err(SessionError::Media(MediaError::NotMedia { record, part }));
            };
            if !matches(&reference, &new.bytes) {
                return Err(SessionError::Media(MediaError::Mismatch { record, part }));
            }
            let object = match self.window.media.get(&reference.sha256) {
                Some(stored) => MediaObject {
                    part: part as u32,
                    ..stored.clone()
                },
                None => {
                    let object = MediaObject {
                        part: part as u32,
                        log: self.log,
                        generation: self.head.generation,
                        seq: first + record as u64,
                    };
                    let name = self.media_name(&object, &reference.sha256);
                    let sealed = self.key.frame_key().seal_unit(name.as_bytes(), &new.bytes);
                    self.backend
                        .put_object(&self.addr, &name, &sealed, self.version)
                        .await?;
                    written = true;
                    object
                }
            };
            let Some(Record::ToolReply(reply)) = records.get_mut(record) else {
                unreachable!("checked above");
            };
            reply.media.push(object);
        }
        if written {
            self.backend
                .sync_objects(&self.addr, &self.log, self.version)
                .await?;
        }
        let first = self.append(&records, barrier).await?;
        Ok((first, records))
    }

    /// Appends `records` as [`append`](Self::append) does, after storing the bytes of each medium
    /// `media` lists: a medium of an input message, or of a message a queued send or a steer
    /// carries. Each medium's bytes must match the reference its message holds. Bytes the open
    /// log already names are pointed at; others are written as new objects, all durable before
    /// the batch is appended. The batch's records record where each medium's bytes are. Returns
    /// the seq of the first record and the records as appended.
    pub async fn append_input_media(
        &mut self,
        records: &[Record],
        media: Vec<super::NewInputMedia>,
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        if self.poisoned {
            return Err(SessionError::Poisoned);
        }
        let first = self.next_seq();
        let mut records = records.to_vec();
        if self.store_input_media(&mut records, first, media).await? {
            self.backend
                .sync_objects(&self.addr, &self.log, self.version)
                .await?;
        }
        let at = self.append(&records, barrier).await?;
        Ok((at, records))
    }

    /// Writes the object of each medium `media` lists that the open log doesn't name yet, under
    /// the record of `records` naming it, which takes seq `first` plus its position, and records
    /// in that record where the medium's bytes are. The objects aren't synced: the caller syncs
    /// them before appending `records`. Returns whether any object was written.
    pub(super) async fn store_input_media(
        &mut self,
        records: &mut [Record],
        first: u64,
        media: Vec<super::NewInputMedia>,
    ) -> Result<bool, SessionError> {
        // Objects written here, by digest: later media of the batch with the same bytes point at
        // the same object. The open log's index learns them only once the batch is appended, so
        // a failed append leaves it naming nothing the log doesn't.
        let mut stored: std::collections::HashMap<String, StoredAt> =
            std::collections::HashMap::new();
        let mut written = false;
        for new in media {
            let (record, message, block, part) = (new.record, new.message, new.block, new.part);
            let not_media = || {
                SessionError::Media(MediaError::NotMedia {
                    record,
                    part: block as usize,
                })
            };
            let reference = match records.get(record) {
                Some(Record::Input(input)) if message == 0 => {
                    input_medium(&input.message, block, part)
                }
                Some(other) => sent_messages(other)
                    .and_then(|(messages, _)| messages.get(message as usize))
                    .and_then(|message| input_medium(message, block, part)),
                None => None,
            }
            .cloned()
            .ok_or_else(not_media)?;
            if !matches(&reference, &new.bytes) {
                return Err(SessionError::Media(MediaError::Mismatch {
                    record,
                    part: block as usize,
                }));
            }
            let known = self
                .window
                .media
                .get(&reference.sha256)
                .map(|object| (object.log, object.generation, object.seq))
                .or_else(|| stored.get(&reference.sha256).copied());
            let (log, generation, seq) = match known {
                Some(at) => at,
                None => {
                    let at = (self.log, self.head.generation, first + record as u64);
                    let name = self.media_name_at(&at.0, &at.1, at.2, &reference.sha256);
                    let sealed = self.key.frame_key().seal_unit(name.as_bytes(), &new.bytes);
                    self.backend
                        .put_object(&self.addr, &name, &sealed, self.version)
                        .await?;
                    stored.insert(reference.sha256.clone(), at);
                    written = true;
                    at
                }
            };
            match &mut records[record] {
                Record::Input(input) => input.media.push(InputMedia {
                    block,
                    part,
                    log,
                    generation,
                    seq,
                }),
                Record::QueuedSend(send) => send.media.push(SendMedia {
                    message,
                    block,
                    part,
                    log,
                    generation,
                    seq,
                }),
                Record::Steer(steer) => steer.media.push(SendMedia {
                    message,
                    block,
                    part,
                    log,
                    generation,
                    seq,
                }),
                _ => unreachable!("checked above"),
            }
        }
        Ok(written)
    }

    /// The bytes of a media part in the window, checked against its reference. Every message
    /// in the window is in the open log, so its media is found by digest; bytes the open log
    /// doesn't name read as `Missing`.
    pub async fn window_media(&self, reference: &MediaRef) -> Result<Vec<u8>, SessionError> {
        match self.window.media.get(&reference.sha256) {
            Some(object) => self.read_media(object, reference).await,
            None => Err(SessionError::Media(MediaError::Missing(
                reference.sha256.clone(),
            ))),
        }
    }

    /// The bytes of a stored tool result's media part `part`, checked against its reference:
    /// for a record read from sealed history, whose media the open log may no longer name.
    pub async fn record_media(
        &self,
        reply: &crate::record::ToolReply,
        part: u32,
    ) -> Result<Vec<u8>, SessionError> {
        let not_media = || {
            SessionError::Media(MediaError::NotMedia {
                record: 0,
                part: part as usize,
            })
        };
        let reference = media_part(&reply.result, part).ok_or_else(not_media)?;
        let object = reply
            .media
            .iter()
            .find(|object| object.part == part)
            .ok_or_else(not_media)?;
        self.read_media(object, reference).await
    }

    async fn read_media(
        &self,
        object: &MediaObject,
        reference: &MediaRef,
    ) -> Result<Vec<u8>, SessionError> {
        let name = self.media_name(object, &reference.sha256);
        self.read_named_media(name, reference).await
    }

    /// The bytes of a stored input message's medium at content block `block` (and, inside a
    /// tool result there, part `part`), checked against its reference.
    pub async fn input_media(
        &self,
        input: &crate::record::Input,
        block: u32,
        part: Option<u32>,
    ) -> Result<Vec<u8>, SessionError> {
        let not_media = || {
            SessionError::Media(MediaError::NotMedia {
                record: 0,
                part: block as usize,
            })
        };
        let reference = input_medium(&input.message, block, part).ok_or_else(not_media)?;
        let object = input
            .media
            .iter()
            .find(|object| object.block == block && object.part == part)
            .ok_or_else(not_media)?;
        let name = self.media_name_at(
            &object.log,
            &object.generation,
            object.seq,
            &reference.sha256,
        );
        self.read_named_media(name, reference).await
    }

    /// The medium with digest `sha256` that a record of the session names: a media block of an
    /// input message, a media part of a tool result inside one, or a media part of a tool
    /// result. Every record is looked at, sealed or in the open log. A record that stored the
    /// medium's bytes is preferred; when none did, the first record naming the digest is
    /// returned, with no bytes to read. `None` when no record names the digest.
    pub async fn named_media(&self, sha256: &str) -> Result<Option<NamedMedium>, SessionError> {
        let mut unstored = None;
        for (_, record) in self.records_from(0).await? {
            for medium in named_in(&record, sha256) {
                if medium.stored.is_some() {
                    return Ok(Some(medium));
                }
                unstored.get_or_insert(medium);
            }
        }
        Ok(unstored)
    }

    /// The bytes of `medium`, checked against its reference: `Missing` when no record stored
    /// them. Reading them touches storage, so a session answering from memory only refuses.
    pub async fn named_media_bytes(&self, medium: &NamedMedium) -> Result<Vec<u8>, SessionError> {
        let Some((log, generation, seq)) = &medium.stored else {
            return Err(SessionError::Media(MediaError::Missing(
                medium.reference.sha256.clone(),
            )));
        };
        self.storage_read()?;
        let name = self.media_name_at(log, generation, *seq, &medium.reference.sha256);
        self.read_named_media(name, &medium.reference).await
    }

    async fn read_named_media(
        &self,
        name: String,
        reference: &MediaRef,
    ) -> Result<Vec<u8>, SessionError> {
        let sealed = self
            .backend
            .get_object(&self.addr, &name)
            .await?
            .ok_or_else(|| SessionError::Media(MediaError::Missing(name.clone())))?;
        let bytes = self
            .key
            .frame_key()
            .open_unit(name.as_bytes(), &sealed)
            .ok_or_else(|| SessionError::Media(MediaError::Corrupt(name.clone())))?;
        if !matches(reference, &bytes) {
            return Err(SessionError::Media(MediaError::Corrupt(name)));
        }
        Ok(bytes)
    }

    /// The name of the object the open log stores `reference`'s bytes under. Tests only.
    #[cfg(test)]
    pub(crate) fn media_object_name(&self, reference: &MediaRef) -> Option<String> {
        self.window
            .media
            .get(&reference.sha256)
            .map(|object| self.media_name(object, &reference.sha256))
    }
}

#[cfg(test)]
mod tests;
