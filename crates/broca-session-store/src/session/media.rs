//! A tool result's media bytes, stored as append objects (design 11.1, 11.5).
//!
//! Each media part's bytes live in one object, written under the open log by the append whose
//! record first names them, and made durable before that record (I8). The record keeps, for each
//! media part, where its object is ([`MediaObject`]), so a reader names the object without a
//! lookup. Bytes the open log already names are pointed at again, not written again; bytes it
//! doesn't name are written again, so a branch after a revert never names an object created on
//! the abandoned one.

use std::collections::BTreeMap;

use broca_protocol::content::{MediaRef, ToolOutput, ToolOutputPart, ToolResult};
use sha2::{Digest as _, Sha256};

use super::{Session, SessionError};
use crate::backend::SessionBackend;
use crate::codec::hex;
use crate::record::{MediaObject, Record};

/// What the session keeps about the open log's records, rebuilt whenever the open log is
/// replaced and kept current on every append.
#[derive(Debug, Default)]
pub(crate) struct OpenLogIndex {
    /// The ordinal each `EntryFacts` record names, by that record's seq, so `find_message`
    /// finds a new message's id before it's sealed.
    pub facts: BTreeMap<u64, u64>,
    /// The object each media digest the open log names is stored as.
    pub media: BTreeMap<String, MediaObject>,
}

impl OpenLogIndex {
    /// Takes in one record of the open log.
    pub fn note(&mut self, seq: u64, record: &Record) {
        match record {
            Record::EntryFacts(facts) => {
                self.facts.insert(seq, facts.ordinal);
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
            _ => {}
        }
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
    /// The named record isn't a tool result, or the named part isn't a media part.
    NotMedia { record: usize, part: usize },
    /// The bytes don't match the part's reference: another digest or size.
    Mismatch { record: usize, part: usize },
    /// A record says the part is stored, but the session has no such object.
    Missing(String),
    /// The object didn't decrypt under the session key and its name, or its bytes don't match
    /// the reference.
    Corrupt(String),
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

fn matches(media: &MediaRef, bytes: &[u8]) -> bool {
    media.byte_size == bytes.len() as u64 && media.sha256 == hex(&Sha256::digest(bytes))
}

impl<B: SessionBackend> Session<B> {
    /// The name of the object `object` points at, for bytes whose digest is `sha256`. The digest
    /// is hashed with a key derived from the session key, so a name says nothing about the
    /// content outside the session, and the same bytes always get the same name.
    fn media_name(&self, object: &MediaObject, sha256: &str) -> String {
        let key = self.key.derive(b"media-object-name");
        let h = crate::prune::keyed_digest(&key, sha256.as_bytes()).0;
        format!(
            "{}/a/{}.{}-media-{}",
            hex(&object.log.0),
            &hex(&object.generation.0)[..16],
            object.seq,
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
