//! One message of the conversation's window, as the conversation holds it.
//!
//! A window message's content can be released from memory while the message stays in the
//! window: a long worker's view drops or shortens most of its older parts, and the run never
//! sends them again, so holding them costs memory and buys nothing. The content stays in the
//! open log, in the records that placed the message, and is read back when something needs the
//! original.
//!
//! So a window message offers two kinds of read. Its role, its flags and each block's shape
//! ([`BlockRef`]: kind, tool call id, whether its text is empty) are always there, held or not.
//! Its content is reached only through [`WindowMessage::held`], which answers `None` when some
//! of it isn't held, so every reader of content says what it does then.

use std::sync::Arc;

use broca_protocol::content::{ContentBlock, Message, Role};
use broca_protocol::ids::ToolCallId;
use broca_protocol::tags::v1::PartKind;

use crate::codec::{DecodeError, Raw, Value, Wire};

/// A window message: its content, while it is held.
#[derive(Debug, Clone, PartialEq)]
pub struct WindowMessage(Arc<Message>);

/// A window message whose content isn't held in memory: it has to be read back from the open log
/// before anything that needs the original can use it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NotHeld {
    /// The message's ordinal.
    pub ordinal: u64,
}

/// The kind of a content block, one per [`ContentBlock`] variant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockKind {
    Media,
    Text,
    TextWithState,
    Reasoning,
    RedactedReasoning,
    ToolCall,
    ToolResult,
    ProviderFallback,
}

impl BlockKind {
    pub fn of(block: &ContentBlock) -> Self {
        match block {
            ContentBlock::Media(_) => Self::Media,
            ContentBlock::Text { .. } => Self::Text,
            ContentBlock::TextWithState { .. } => Self::TextWithState,
            ContentBlock::Reasoning { .. } => Self::Reasoning,
            ContentBlock::RedactedReasoning { .. } => Self::RedactedReasoning,
            ContentBlock::ToolCall(_) => Self::ToolCall,
            ContentBlock::ToolResult(_) => Self::ToolResult,
            ContentBlock::ProviderFallback { .. } => Self::ProviderFallback,
        }
    }
}

/// What a released block keeps: enough to number it under either numbering, to find a call or
/// a result by its id, and to know what kind of block to read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlockShape {
    pub kind: BlockKind,
    /// For a text block: whether its text is empty, which decides whether the compaction
    /// numbering gives it a number. False for every other kind.
    pub text_empty: bool,
    /// The call's id on a tool call, the call it answers on a tool result.
    pub tool_call_id: Option<ToolCallId>,
    /// Whether the block held media (see [`WindowMessage::has_media`]).
    pub has_media: bool,
    /// The part kind the runner's own numbering gives the block
    /// (`broca_protocol::tags::part_kind`), as written and with replies cleaned: a run freezes
    /// one of the two, and the kind can depend on the content.
    pub runner_kinds: [Option<PartKind>; 2],
}

/// One block of a window message: the block itself while it is held, its shape once released.
#[derive(Debug, Clone, Copy)]
pub enum BlockRef<'a> {
    Held(&'a ContentBlock),
    Released(&'a BlockShape),
}

impl<'a> BlockRef<'a> {
    pub fn kind(&self) -> BlockKind {
        match self {
            Self::Held(block) => BlockKind::of(block),
            Self::Released(shape) => shape.kind,
        }
    }

    /// The call's id on a tool call, the call it answers on a tool result.
    pub fn tool_call_id(&self) -> Option<&'a ToolCallId> {
        match self {
            Self::Held(ContentBlock::ToolCall(call)) => Some(&call.tool_call_id),
            Self::Held(ContentBlock::ToolResult(result)) => Some(&result.tool_call_id),
            Self::Held(_) => None,
            Self::Released(shape) => shape.tool_call_id.as_ref(),
        }
    }

    /// The part kind the runner's own numbering gives the block in a message of `role`, with
    /// replies cleaned or not (see `broca_protocol::tags::part_kind`).
    pub fn runner_kind(&self, role: Role, clean: bool) -> Option<PartKind> {
        match self {
            Self::Held(block) => broca_protocol::tags::part_kind(role, block, clean),
            Self::Released(shape) => shape.runner_kinds[usize::from(clean)],
        }
    }

    /// For a text block, whether its text is empty; false for every other kind.
    pub fn text_empty(&self) -> bool {
        match self {
            Self::Held(ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. }) => {
                text.is_empty()
            }
            Self::Held(_) => false,
            Self::Released(shape) => shape.text_empty,
        }
    }
}

impl WindowMessage {
    /// A message whose content is held.
    pub fn new(message: Arc<Message>) -> Self {
        Self(message)
    }

    /// The message with all its content, or `None` when some of it has been released from
    /// memory and has to be read back from the open log first.
    pub fn held(&self) -> Option<&Arc<Message>> {
        Some(&self.0)
    }

    pub fn role(&self) -> Role {
        self.0.role
    }

    /// Whether the message is a turn-scoped system message (see [`Message::turn_system`]).
    pub fn turn_system(&self) -> bool {
        self.0.turn_system
    }

    /// Whether any block of the message holds media: a media block, or a tool result with a
    /// media part. A released block keeps this.
    pub fn has_media(&self) -> bool {
        !crate::session::message_media(&self.0).is_empty()
    }

    /// The message's blocks, in order, each held or released.
    pub fn blocks(&self) -> impl ExactSizeIterator<Item = BlockRef<'_>> + '_ {
        self.0.content.iter().map(BlockRef::Held)
    }

    /// The message to change in place, or `None` when some of its content is not held. Only the
    /// fold changes a window message, and only its newest one, which is never released.
    pub(crate) fn held_mut(&mut self) -> Option<&mut Message> {
        Some(Arc::make_mut(&mut self.0))
    }

    /// The shared allocation, for memory accounting: two holders of one message count it once.
    pub(crate) fn allocation(&self) -> &Arc<Message> {
        &self.0
    }
}

impl crate::held::HeldBytes for WindowMessage {
    fn heap_bytes(&self) -> usize {
        self.0.heap_bytes()
    }
}

/// Written exactly as the message it holds.
impl Value for WindowMessage {
    const WIRE: Wire = <Arc<Message> as Value>::WIRE;
    const NAME: &'static str = <Arc<Message> as Value>::NAME;
    fn write(&self, out: &mut Vec<u8>) {
        self.0.write(out);
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        <Arc<Message> as Value>::read(raw).map(Self)
    }
}

#[cfg(any(test, fuzzing))]
impl crate::fuzzing::gen::Gen for WindowMessage {
    fn gen(g: &mut crate::fuzzing::gen::G<'_>) -> Self {
        Self(crate::fuzzing::gen::gen(g))
    }
}
