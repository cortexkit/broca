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
use broca_session_store::record::Record;
use broca_session_store::session::{NewMedia, Session, SessionError};

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
        session: &mut Session<'_, B>,
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
