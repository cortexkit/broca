use std::{collections::BTreeMap, sync::Arc};

use broca_protocol::{ContentBlock, MediaMarkerReason, MediaRef, Message};
use broca_records::record::WalRecord;
use serde::{Deserialize, Serialize};

/// Exact, metadata-only captures, indexed by digest. Repeated uses of a capture take one entry.
/// This is persisted with the compaction checkpoint because a sparse prompt can omit the
/// original message even while a replacement still shows its media.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CapturedMedia(Arc<BTreeMap<String, Vec<MediaRef>>>);

impl CapturedMedia {
    /// The heap the references hold, behind the `Arc` the engine's clones share (see
    /// `media_request::held`).
    pub(crate) fn heap_bytes(&self) -> usize {
        use crate::media_request::held::{btree_map, media_ref, vec};
        2 * std::mem::size_of::<usize>()
            + btree_map(&*self.0, |key, value| {
                key.capacity() + vec(value, media_ref)
            })
    }

    /// Detect capture-bearing records before allocating an index. Most records, including
    /// every record in a media-free session, need no capture collection at all.
    pub(crate) fn captures_any(record: &WalRecord) -> bool {
        match record {
            WalRecord::RunStarted { input, .. } | WalRecord::Steered { input, .. } => input
                .iter()
                .any(|message| super::media_refs(&message.content).next().is_some()),
            WalRecord::Queued { request, .. } => request
                .input
                .iter()
                .any(|message| super::media_refs(&message.content).next().is_some()),
            WalRecord::ToolResult { result, .. } | WalRecord::LateResult { result, .. } => {
                matches!(&result.output, broca_protocol::ToolOutput::Parts { parts }
                    if parts.iter().any(|part| matches!(part, broca_protocol::ToolOutputPart::Media { .. })))
            }
            _ => false,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(crate) fn contains(&self, reference: &MediaRef) -> bool {
        self.0
            .get(&reference.sha256)
            .is_some_and(|refs| refs.contains(reference))
    }

    pub(crate) fn insert(&mut self, reference: &MediaRef) {
        if !self.contains(reference) {
            let references = Arc::make_mut(&mut self.0)
                .entry(reference.sha256.clone())
                .or_default();
            // Most digests have one reference for the session's life: room for exactly that.
            if references.capacity() == 0 {
                references.reserve_exact(1);
            }
            references.push(reference.clone());
        }
    }

    pub(crate) fn fold(&mut self, record: &WalRecord) {
        match record {
            WalRecord::RunStarted { input, .. } | WalRecord::Steered { input, .. } => {
                self.messages(input);
            }
            WalRecord::Queued { request, .. } => self.messages(&request.input),
            WalRecord::ToolResult { result, .. } | WalRecord::LateResult { result, .. } => {
                if let broca_protocol::ToolOutput::Parts { parts } = &result.output {
                    for part in parts {
                        if let broca_protocol::ToolOutputPart::Media { media } = part {
                            self.insert(media);
                        }
                    }
                }
            }
            // Provider-written replacements, views and model output cannot capture objects.
            _ => {}
        }
    }

    fn messages(&mut self, messages: &[Message]) {
        for reference in messages
            .iter()
            .flat_map(|message| super::media_refs(&message.content))
        {
            self.insert(reference);
        }
    }

    pub(crate) fn enforce(&self, messages: &mut [Message]) {
        enforce_by(messages, |reference| self.contains(reference));
    }
}

/// Replace each medium of `messages` that `captured` refuses with its provenance marker: a
/// prompt may show only media the session captured.
pub(crate) fn enforce_by(messages: &mut [Message], captured: impl Fn(&MediaRef) -> bool) {
    for block in messages.iter_mut().flat_map(|message| &mut message.content) {
        match block {
            ContentBlock::Media(reference) if !captured(reference) => {
                *block = ContentBlock::Text {
                    text: broca_protocol::content::media_not_shown_marker(
                        reference,
                        MediaMarkerReason::Provenance,
                    )
                    .into(),
                };
            }
            ContentBlock::ToolResult(result) => {
                if let broca_protocol::ToolOutput::Parts { parts } = &mut result.output {
                    for part in parts {
                        if let broca_protocol::ToolOutputPart::Media { media } = part {
                            if !captured(media) {
                                *part = broca_protocol::ToolOutputPart::Text {
                                    text: broca_protocol::content::media_not_shown_marker(
                                        media,
                                        MediaMarkerReason::Provenance,
                                    )
                                    .into(),
                                };
                            }
                        }
                    }
                }
            }
            _ => {}
        }
    }
}
