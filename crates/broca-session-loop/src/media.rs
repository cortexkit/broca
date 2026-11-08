//! A tool's media, held from the moment the tool plane captures it until the record that names
//! it is appended.
//!
//! The tool plane captures each media part as its result arrives ([`MediaCapture`]). The bytes
//! stay here, validated, until the loop appends the call's `ToolReply`; that append stores them
//! as the session's own objects, durable before the record (design 11.1, I8). Only bytes a
//! result still names when it's appended are stored: a part the provenance check replaced with
//! a marker was never captured for its call, so nothing is held for it.

use std::collections::HashMap;
use std::sync::Mutex;

use broca_core::tool_media::{CaptureFailure, MediaCapture};
use broca_protocol::content::{ToolOutput, ToolOutputPart};
use broca_protocol::{BatchId, MediaRef, ToolCallId};
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::{InputMedia, MediaObject, Record, SendMedia};
use broca_session_store::session::{NewInputMedia, NewMedia, Session, SessionError};

use crate::writer::Writer;

/// Each captured part's reference and bytes, in capture order.
type Held = Vec<(MediaRef, Vec<u8>)>;

/// Captured media not yet stored, by batch and call.
#[derive(Debug, Default)]
pub struct PendingMedia {
    held: Mutex<HashMap<(BatchId, ToolCallId), Held>>,
}

#[async_trait::async_trait]
impl MediaCapture for PendingMedia {
    async fn capture(
        &self,
        batch_id: &BatchId,
        call_id: &ToolCallId,
        bytes: Vec<u8>,
        declared_mime: String,
    ) -> Result<MediaRef, CaptureFailure> {
        let reference = broca_core::media_validation::validate_media(&bytes, &declared_mime)
            .map_err(|reason| CaptureFailure {
                reason,
                media: None,
            })?;
        self.held
            .lock()
            .expect("pending media lock")
            .entry((batch_id.clone(), call_id.clone()))
            .or_default()
            .push((reference.clone(), bytes));
        Ok(reference)
    }

    fn captured_for(&self, batch_id: &BatchId, call_id: &ToolCallId) -> Vec<MediaRef> {
        self.held
            .lock()
            .expect("pending media lock")
            .get(&(batch_id.clone(), call_id.clone()))
            .map(|held| {
                held.iter()
                    .map(|(reference, _)| reference.clone())
                    .collect()
            })
            .unwrap_or_default()
    }
}

impl PendingMedia {
    /// The held bytes of every media part `records`' tool results name, and the calls they
    /// were held for.
    fn named(&self, records: &[Record]) -> (Vec<NewMedia>, Vec<(BatchId, ToolCallId)>) {
        let held = self.held.lock().expect("pending media lock");
        let (mut media, mut calls) = (Vec::new(), Vec::new());
        for (index, record) in records.iter().enumerate() {
            let Record::ToolReply(reply) = record else {
                continue;
            };
            let Some(batch) = &reply.batch_id else {
                continue;
            };
            let key = (BatchId(batch.clone()), reply.result.tool_call_id.clone());
            let Some(captured) = held.get(&key) else {
                continue;
            };
            calls.push(key.clone());
            let ToolOutput::Parts { parts } = &reply.result.output else {
                continue;
            };
            for (part, block) in parts.iter().enumerate() {
                let ToolOutputPart::Media { media: reference } = block else {
                    continue;
                };
                if let Some((_, bytes)) = captured.iter().find(|(held, _)| held == reference) {
                    media.push(NewMedia {
                        record: index,
                        part,
                        bytes: bytes.clone(),
                    });
                }
            }
        }
        (media, calls)
    }

    /// Appends `records` to `session`, storing the bytes of each media part they name that was
    /// captured for its call. Each call has one result, so once it's appended nothing more is
    /// held for that call. Returns the seq of the first record.
    pub async fn append<B: SessionBackend>(
        &self,
        session: &mut Session<B>,
        records: &[Record],
        barrier: bool,
    ) -> Result<u64, SessionError> {
        let (media, calls) = self.named(records);
        let first = session.append_media(records, media, barrier).await?.0;
        let mut held = self.held.lock().expect("pending media lock");
        for call in calls {
            held.remove(&call);
        }
        Ok(first)
    }
}

/// A send's media, held by the session's actor from the moment it stages them until a record
/// naming them is appended: the send's queued record or steer, or the start of the run that
/// places its messages. That append stores the bytes as the session's own objects, durable
/// before the records. A send whose media an earlier record already stored, as a queued send's
/// record stores them, is held by where those objects are instead, and the messages that place
/// it point at them.
#[derive(Debug, Default)]
pub struct HeldInputMedia {
    /// Bytes no record stores yet, by digest.
    bytes: HashMap<String, std::sync::Arc<Vec<u8>>>,
    /// Objects a record already stored, by digest.
    stored: HashMap<String, MediaObject>,
}

impl HeldInputMedia {
    /// Holds `bytes`, the bytes `reference` names.
    pub fn hold(&mut self, reference: &MediaRef, bytes: std::sync::Arc<Vec<u8>>) {
        self.bytes.insert(reference.sha256.clone(), bytes);
    }

    /// Holds the media `record` stored, by where their objects are.
    pub fn hold_stored(&mut self, record: &Record) {
        for (sha256, object) in broca_session_store::session::stored_media(record) {
            self.stored.entry(sha256).or_insert(object);
        }
    }

    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty() && self.stored.is_empty()
    }

    /// Lets go of everything held.
    pub fn clear(&mut self) {
        self.bytes.clear();
        self.stored.clear();
    }

    /// For each medium `records`' messages hold that no record of theirs stores yet and that is
    /// held: the pointer filled in for one held by its object, and the bytes to store for one
    /// held by its bytes, with the digests of the held media named. `None` when no held medium
    /// is named.
    fn prepare(&self, records: &[Record]) -> Option<Prepared> {
        let mut out = records.to_vec();
        let mut media = Vec::new();
        let mut named = Vec::new();
        for (index, record) in out.iter_mut().enumerate() {
            let messages: Vec<(u32, &broca_protocol::content::Message)> = match &*record {
                Record::Input(input) => vec![(0, &input.message)],
                other => match broca_session_store::session::sent_messages(other) {
                    Some((messages, _)) => (0u32..).zip(messages.iter()).collect(),
                    None => continue,
                },
            };
            let mut pointers = Vec::new();
            for (message, content) in messages {
                for (block, part, reference) in broca_session_store::session::message_media(content)
                {
                    if stores(record, message, block, part) {
                        continue;
                    }
                    if let Some(object) = self.stored.get(&reference.sha256) {
                        pointers.push(SendMedia {
                            message,
                            block,
                            part,
                            log: object.log,
                            generation: object.generation,
                            seq: object.seq,
                        });
                        named.push(reference.sha256.clone());
                    } else if let Some(bytes) = self.bytes.get(&reference.sha256) {
                        media.push(NewInputMedia {
                            record: index,
                            message,
                            block,
                            part,
                            bytes: bytes.as_ref().clone(),
                        });
                        named.push(reference.sha256.clone());
                    }
                }
            }
            match record {
                Record::Input(input) => {
                    input
                        .media
                        .extend(pointers.into_iter().map(|pointer| InputMedia {
                            block: pointer.block,
                            part: pointer.part,
                            log: pointer.log,
                            generation: pointer.generation,
                            seq: pointer.seq,
                        }))
                }
                Record::QueuedSend(send) => send.media.extend(pointers),
                Record::Steer(steer) => steer.media.extend(pointers),
                _ => {}
            }
        }
        (!named.is_empty()).then_some(Prepared {
            records: out,
            media,
            named,
        })
    }

    /// Appends `records` through `writer`, storing or pointing at every held medium their
    /// messages name (see [`Writer::append_input_media`]). Once they are appended, those media
    /// are no longer held: the session names them. Returns the seq of the first record and the
    /// records as appended.
    pub async fn append<W: Writer>(
        &mut self,
        writer: &mut W,
        records: &[Record],
        barrier: bool,
    ) -> Result<(u64, Vec<Record>), SessionError> {
        let Some(prepared) = self.prepare(records) else {
            let first = writer.append(records, barrier).await?;
            return Ok((first, records.to_vec()));
        };
        let appended = writer
            .append_input_media(&prepared.records, prepared.media, barrier)
            .await?;
        for sha256 in prepared.named {
            self.bytes.remove(&sha256);
            self.stored.remove(&sha256);
        }
        Ok(appended)
    }
}

/// A batch with the held media it names filled in (see [`HeldInputMedia::prepare`]).
struct Prepared {
    records: Vec<Record>,
    /// The bytes to store, of the media held by their bytes.
    media: Vec<NewInputMedia>,
    /// The digest of each held medium the batch names.
    named: Vec<String>,
}

/// Whether `record` already says where the medium at (`message`, `block`, `part`) is stored.
fn stores(record: &Record, message: u32, block: u32, part: Option<u32>) -> bool {
    match record {
        Record::Input(input) => input
            .media
            .iter()
            .any(|stored| stored.block == block && stored.part == part),
        other => broca_session_store::session::sent_messages(other).is_some_and(|(_, media)| {
            media.iter().any(|stored| {
                stored.message == message && stored.block == block && stored.part == part
            })
        }),
    }
}
