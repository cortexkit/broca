//! Caller-facing message vocabulary, shared by reads and compaction content identities.

use crate::{ContentBlock, Message};
use serde::Serialize;
use serde_json::Value;

/// A message's caller-facing content before read-time media markers or payload references.
pub struct MessageContentIdentity {
    pub inline: Value,
    pub digest: String,
}

/// Compute the identity a descriptors read serves. Provider-only text state is not caller
/// content, and model text is cleaned according to the run that wrote it. Media retains its
/// stored metadata here: markers and references are read-time substitutions, not identity.
pub fn message_content_identity(message: &Message) -> MessageContentIdentity {
    let exposed = crate::tags::exposed_message(message);
    let inline =
        serde_json::to_value(CallerMessageRef(&exposed)).expect("a stored message serializes");
    let digest = cortexkit_role_llm_runner::content_digest::content_digest(&inline);
    MessageContentIdentity { inline, digest }
}

/// Serialize in the caller vocabulary without changing text cleaning or media presentation.
/// Callers choose those policies before applying this borrowed view.
pub struct CallerMessageRef<'a>(pub &'a Message);

impl Serialize for CallerMessageRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        if !self
            .0
            .content
            .iter()
            .any(|block| matches!(block, ContentBlock::TextWithState { .. }))
        {
            return self.0.serialize(serializer);
        }
        let mut fields = 2;
        fields += usize::from(self.0.cache_prefix_blocks.is_some());
        fields += usize::from(self.0.origin.is_some());
        let mut state = serializer.serialize_struct("Message", fields)?;
        state.serialize_field("role", &self.0.role)?;
        state.serialize_field("content", &CallerContentRef(&self.0.content))?;
        if let Some(cache_prefix_blocks) = self.0.cache_prefix_blocks {
            state.serialize_field("cache_prefix_blocks", &cache_prefix_blocks)?;
        }
        if let Some(origin) = &self.0.origin {
            state.serialize_field("origin", origin)?;
        }
        state.end()
    }
}

/// The same caller vocabulary for content blocks, without a message envelope.
pub struct CallerContentRef<'a>(pub &'a [ContentBlock]);

impl Serialize for CallerContentRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeSeq;

        let mut sequence = serializer.serialize_seq(Some(self.0.len()))?;
        for block in self.0 {
            sequence.serialize_element(&CallerBlockRef(block))?;
        }
        sequence.end()
    }
}

struct CallerBlockRef<'a>(&'a ContentBlock);

impl Serialize for CallerBlockRef<'_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;

        match self.0 {
            ContentBlock::TextWithState { text, .. } => {
                let mut state = serializer.serialize_struct("ContentBlock", 2)?;
                state.serialize_field("type", "text")?;
                state.serialize_field("text", text)?;
                state.end()
            }
            block => block.serialize(serializer),
        }
    }
}
