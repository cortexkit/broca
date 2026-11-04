//! Pruning opaque reasoning from records that leave the open log.
//!
//! Below a compaction cut nothing is rendered again, so a seal drops the opaque provider payloads
//! of finished runs: encrypted reasoning, redacted thinking, thinking signatures, Gemini thought
//! signatures and the provider state attached to text. These are about a quarter of all stored
//! bytes and nothing reads them once they're below the cut.
//!
//! Each removed payload leaves a stub on its record: which content block and field held it, its
//! size, and a keyed digest of it. So a history read can say truthfully that reasoning was there
//! and was removed, and two stubs can be compared without revealing anything. The visible
//! reasoning text stays. The compaction provider (the module that summarises and trims a
//! session's history) checks each message part by a checksum over a neutral form of the part. That
//! form holds the visible text but never the opaque payload, so pruning changes no checksum, while
//! dropping the text would.
//!
//! Block positions never change: a pruned block stays in place with its opaque field emptied,
//! because part ids are block positions. A record carrying stubs must be rendered without its
//! opaque parts, so the stubs are encoded as must-understand: a reader that doesn't know them
//! refuses the record instead of rendering emptied reasoning as if it were complete.

use broca_protocol::content::{ContentBlock, Message};
use sha2::{Digest as _, Sha256};

use crate::codec::Digest;

crate::values! {
    /// Which opaque field of a content block was removed.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
    pub enum OpaqueField {
        1 ReasoningSignature,
        2 RedactedReasoningData,
        3 TextProviderState,
        4 ToolCallThoughtSignature,
    }
}

crate::message! {
    /// What is left of one removed payload.
    #[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
    pub struct PrunedOpaque {
        /// The content block's position in its message.
        1 req block: u32,
        2 req field: OpaqueField,
        /// The removed payload's size in bytes, as the provider sent it.
        3 req size: u64,
        /// HMAC-SHA256 of the payload under the session's pruning key.
        4 req digest: Digest,
    }
}

/// HMAC-SHA256, the keyed digest of a removed payload.
fn keyed_digest(key: &[u8; 32], payload: &[u8]) -> Digest {
    let mut inner = Sha256::new();
    let mut outer = Sha256::new();
    let mut block = [0u8; 64];
    block[..32].copy_from_slice(key);
    inner.update(block.map(|byte| byte ^ 0x36));
    inner.update(payload);
    outer.update(block.map(|byte| byte ^ 0x5c));
    outer.update(inner.finalize());
    Digest(outer.finalize().into())
}

fn stub(key: &[u8; 32], block: usize, field: OpaqueField, payload: &str) -> PrunedOpaque {
    PrunedOpaque {
        block: block as u32,
        field,
        size: payload.len() as u64,
        digest: keyed_digest(key, payload.as_bytes()),
    }
}

/// Removes every opaque payload from `message`, in place, and returns a stub for each. Visible
/// text, tool calls and tool results are untouched, and no block moves.
pub fn prune(message: &mut Message, key: &[u8; 32]) -> Vec<PrunedOpaque> {
    let mut stubs = Vec::new();
    for (index, block) in message.content.iter_mut().enumerate() {
        match block {
            ContentBlock::Reasoning { signature, .. } => {
                if let Some(signature) = signature.take() {
                    stubs.push(stub(
                        key,
                        index,
                        OpaqueField::ReasoningSignature,
                        &signature,
                    ));
                }
            }
            ContentBlock::RedactedReasoning { data, .. } if !data.is_empty() => {
                let data = std::mem::take(data);
                stubs.push(stub(key, index, OpaqueField::RedactedReasoningData, &data));
            }
            ContentBlock::TextWithState { provider_state, .. }
                if !provider_state.data.is_empty() =>
            {
                let data = std::mem::take(&mut provider_state.data);
                stubs.push(stub(key, index, OpaqueField::TextProviderState, &data));
            }
            ContentBlock::ToolCall(call) => {
                if let Some(signature) = call.thought_signature.take() {
                    stubs.push(stub(
                        key,
                        index,
                        OpaqueField::ToolCallThoughtSignature,
                        &signature,
                    ));
                }
            }
            _ => {}
        }
    }
    stubs
}

/// Prunes a transcript record's message, if it has one, and keeps the stubs on the record. The
/// caller decides that the record's run is finished: an unfinished run's reasoning is never
/// pruned, since resuming it may need its signed reasoning.
pub fn prune_record(record: &mut crate::record::Record, key: &[u8; 32]) {
    use crate::record::Record;
    match record {
        Record::Assistant(assistant) => {
            let stubs = prune(&mut assistant.message, key);
            assistant.pruned.extend(stubs);
        }
        Record::Input(input) => {
            let stubs = prune(&mut input.message, key);
            input.pruned.extend(stubs);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use broca_protocol::content::{ContentBlock, Message, Role};

    use super::{prune, OpaqueField};

    #[test]
    fn opaque_payloads_leave_stubs_and_blocks_keep_their_places() {
        let mut message = Message::new(
            Role::Assistant,
            vec![
                ContentBlock::Reasoning {
                    text: "visible".into(),
                    signature: Some("c2lnbmF0dXJl".into()),
                },
                ContentBlock::Text {
                    text: "answer".into(),
                },
                ContentBlock::RedactedReasoning {
                    data: "encrypted".into(),
                    provider_format: None,
                },
            ],
        );
        let key = [3; 32];
        let stubs = prune(&mut message, &key);
        assert_eq!(stubs.len(), 2);
        assert_eq!(
            (stubs[0].block, stubs[0].field),
            (0, OpaqueField::ReasoningSignature)
        );
        assert_eq!(
            (stubs[1].block, stubs[1].field),
            (2, OpaqueField::RedactedReasoningData)
        );
        assert_eq!(stubs[1].size, 9);
        assert_eq!(message.content.len(), 3);
        assert!(matches!(
            &message.content[0],
            ContentBlock::Reasoning { text, signature: None } if text == "visible"
        ));
        assert!(
            matches!(&message.content[2], ContentBlock::RedactedReasoning { data, .. } if data.is_empty())
        );
        // The same payload under the same key gives the same digest; another key doesn't.
        let mut again = Message::new(
            Role::Assistant,
            vec![ContentBlock::RedactedReasoning {
                data: "encrypted".into(),
                provider_format: None,
            }],
        );
        assert_eq!(prune(&mut again.clone(), &key)[0].digest, stubs[1].digest);
        assert_ne!(prune(&mut again, &[4; 32])[0].digest, stubs[1].digest);
    }
}
