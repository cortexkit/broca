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
//!
//! A message also knows the open-log seqs of the records its content came from
//! ([`WindowMessage::sources`]), which is what reading it back reads. That is runtime
//! bookkeeping: it is never written, and a message read from a checkpoint has none, so it is
//! never released.

use std::sync::Arc;

use broca_protocol::content::{ContentBlock, Message, Role, ToolOutput, ToolOutputPart};
use broca_protocol::ids::ToolCallId;
use broca_protocol::tags::v1::PartKind;

use crate::codec::{DecodeError, Raw, Value, Wire};

/// A window message: its content while it is held, or its held blocks and the shapes of the
/// released ones.
#[derive(Debug, Clone)]
pub struct WindowMessage {
    repr: Repr,
    /// The seqs of the open-log records the message's content came from, oldest first; empty
    /// when they aren't known (see the module's notes).
    sources: Arc<[u64]>,
}

#[derive(Debug, Clone, PartialEq)]
enum Repr {
    Held(Arc<Message>),
    Released(Arc<Released>),
}

/// A message some of whose blocks were released: `message` holds the blocks still held, in
/// order, and every other field as it was; `layout` places each of the original blocks.
#[derive(Debug, PartialEq)]
struct Released {
    message: Message,
    layout: Box<[Slot]>,
}

#[derive(Debug, PartialEq)]
enum Slot {
    /// Held, at this index of `Released::message`'s blocks.
    Held(usize),
    Released(BlockShape),
}

/// Two window messages are equal when they hold the same message, or released the same blocks
/// of it. Where their content came from isn't part of what they are.
impl PartialEq for WindowMessage {
    fn eq(&self, other: &Self) -> bool {
        self.repr == other.repr
    }
}

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
    /// SHA-256 of the block as JSON, so a block read back can be checked to be the one released.
    pub digest: [u8; 32],
}

impl BlockShape {
    /// The shape of `block`, in a message of `role`.
    pub fn of(role: Role, block: &ContentBlock) -> Self {
        let held = BlockRef::Held(block);
        Self {
            kind: held.kind(),
            text_empty: held.text_empty(),
            tool_call_id: held.tool_call_id().cloned(),
            has_media: holds_media(block),
            runner_kinds: [false, true].map(|clean| held.runner_kind(role, clean)),
            digest: block_digest(block),
        }
    }
}

/// SHA-256 of `block` as JSON. A block always serializes; one that somehow didn't would digest
/// as nothing, which no block read back matches.
fn block_digest(block: &ContentBlock) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    serde_json::to_vec(block)
        .map(|bytes| Sha256::digest(bytes).into())
        .unwrap_or_default()
}

/// Whether `block` holds media: a media block, or a tool result with a media part.
fn holds_media(block: &ContentBlock) -> bool {
    match block {
        ContentBlock::Media(_) => true,
        ContentBlock::ToolResult(result) => match &result.output {
            ToolOutput::Parts { parts } => parts
                .iter()
                .any(|part| matches!(part, ToolOutputPart::Media { .. })),
            _ => false,
        },
        _ => false,
    }
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

/// The blocks of a window message, in order (see [`WindowMessage::blocks`]).
pub struct Blocks<'a> {
    message: &'a WindowMessage,
    next: usize,
}

impl<'a> Iterator for Blocks<'a> {
    type Item = BlockRef<'a>;

    fn next(&mut self) -> Option<BlockRef<'a>> {
        let block = self.message.block(self.next)?;
        self.next += 1;
        Some(block)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let left = self.message.block_count() - self.next;
        (left, Some(left))
    }
}

impl ExactSizeIterator for Blocks<'_> {}

impl WindowMessage {
    /// A message whose content is held, with no known sources.
    pub fn new(message: Arc<Message>) -> Self {
        Self::from_records(message, Arc::from([]))
    }

    /// A message whose content is held, placed by the open-log records at `sources`.
    pub(crate) fn from_records(message: Arc<Message>, sources: Arc<[u64]>) -> Self {
        Self {
            repr: Repr::Held(message),
            sources,
        }
    }

    /// The message with all its content, or `None` when some of it has been released from
    /// memory and has to be read back from the open log first.
    pub fn held(&self) -> Option<&Arc<Message>> {
        match &self.repr {
            Repr::Held(message) => Some(message),
            Repr::Released(_) => None,
        }
    }

    /// Whether some of the message's blocks are released.
    pub fn is_released(&self) -> bool {
        matches!(self.repr, Repr::Released(_))
    }

    /// The seqs of the open-log records the message's content came from, oldest first; empty
    /// when they aren't known, as for a message read from a checkpoint.
    pub fn sources(&self) -> &[u64] {
        &self.sources
    }

    /// The fields every block of the message shares, whatever is held.
    fn fields(&self) -> &Message {
        match &self.repr {
            Repr::Held(message) => message,
            Repr::Released(released) => &released.message,
        }
    }

    pub fn role(&self) -> Role {
        self.fields().role
    }

    /// Whether the message is a turn-scoped system message (see [`Message::turn_system`]).
    pub fn turn_system(&self) -> bool {
        self.fields().turn_system
    }

    /// Whether any block of the message holds media: a media block, or a tool result with a
    /// media part. A released block keeps this.
    pub fn has_media(&self) -> bool {
        self.blocks().any(|block| match block {
            BlockRef::Held(block) => holds_media(block),
            BlockRef::Released(shape) => shape.has_media,
        })
    }

    fn block_count(&self) -> usize {
        match &self.repr {
            Repr::Held(message) => message.content.len(),
            Repr::Released(released) => released.layout.len(),
        }
    }

    fn block(&self, index: usize) -> Option<BlockRef<'_>> {
        match &self.repr {
            Repr::Held(message) => message.content.get(index).map(BlockRef::Held),
            Repr::Released(released) => released.layout.get(index).map(|slot| match slot {
                Slot::Held(at) => BlockRef::Held(&released.message.content[*at]),
                Slot::Released(shape) => BlockRef::Released(shape),
            }),
        }
    }

    /// The message's blocks, in order, each held or released.
    pub fn blocks(&self) -> Blocks<'_> {
        Blocks {
            message: self,
            next: 0,
        }
    }

    /// The message with the blocks at `released` (indexes into its blocks) released as well,
    /// or `None` when it can't be: its sources aren't known, so it could never be read back.
    /// Blocks already released stay released, and indexes past the last block are ignored.
    pub fn releasing(&self, released: &[usize]) -> Option<Self> {
        if self.sources.is_empty() {
            return None;
        }
        let role = self.role();
        let mut message = Message::clone(self.fields());
        message.content = Vec::new();
        let mut layout = Vec::with_capacity(self.block_count());
        for (index, block) in self.blocks().enumerate() {
            layout.push(match block {
                BlockRef::Released(shape) => Slot::Released(shape.clone()),
                BlockRef::Held(block) if released.contains(&index) => {
                    Slot::Released(BlockShape::of(role, block))
                }
                BlockRef::Held(block) => {
                    message.content.push(block.clone());
                    Slot::Held(message.content.len() - 1)
                }
            });
        }
        if layout.iter().all(|slot| matches!(slot, Slot::Held(_))) {
            return Some(self.clone());
        }
        Some(Self {
            repr: Repr::Released(Arc::new(Released {
                message,
                layout: layout.into_boxed_slice(),
            })),
            sources: Arc::clone(&self.sources),
        })
    }

    /// Whether `original`, read back from this message's sources, is the message this one
    /// stands for: the same fields, and each held block the same as the original's.
    pub fn stands_for(&self, original: &Message) -> bool {
        let fields = self.fields();
        original.role == fields.role
            && original.turn_system == fields.turn_system
            && original.cache_prefix_blocks == fields.cache_prefix_blocks
            && original.origin == fields.origin
            && original.content.len() == self.block_count()
            && self
                .blocks()
                .zip(&original.content)
                .all(|(block, read)| match block {
                    BlockRef::Held(held) => held == read,
                    BlockRef::Released(shape) => *shape == BlockShape::of(original.role, read),
                })
    }

    /// The message to change in place, or `None` when some of its content is not held. Only the
    /// fold changes a window message, and only its newest one, which is never released.
    pub(crate) fn held_mut(&mut self) -> Option<&mut Message> {
        match &mut self.repr {
            Repr::Held(message) => Some(Arc::make_mut(message)),
            Repr::Released(_) => None,
        }
    }

    /// Adds `seq` to the message's sources: a later record changed its content, so reading it
    /// back reads that record too. A message with no known sources stays without them.
    pub(crate) fn add_source(&mut self, seq: u64) {
        if !self.sources.is_empty() {
            self.sources = self.sources.iter().copied().chain([seq]).collect();
        }
    }

    /// The shared allocation's address, for memory accounting: two holders of one message
    /// count it once.
    pub(crate) fn allocation(&self) -> usize {
        match &self.repr {
            Repr::Held(message) => Arc::as_ptr(message) as usize,
            Repr::Released(released) => Arc::as_ptr(released) as usize,
        }
    }
}

impl crate::held::HeldBytes for WindowMessage {
    fn heap_bytes(&self) -> usize {
        let sources = self.sources.len() * size_of::<u64>();
        sources
            + match &self.repr {
                Repr::Held(message) => message.heap_bytes(),
                Repr::Released(released) => {
                    let layout = released.layout.len() * size_of::<Slot>()
                        + released
                            .layout
                            .iter()
                            .map(|slot| match slot {
                                Slot::Released(BlockShape {
                                    tool_call_id: Some(id),
                                    ..
                                }) => id.0.capacity(),
                                _ => 0,
                            })
                            .sum::<usize>();
                    size_of::<Released>() + released.message.heap_bytes() + layout
                }
            }
    }
}

/// Written exactly as the message it holds. A message with released blocks is never written:
/// every writer of window messages (a checkpoint, a seal, a revert, a move copy) reads it back
/// first, so writing one is a bug that would store a hollow message.
impl Value for WindowMessage {
    const WIRE: Wire = <Arc<Message> as Value>::WIRE;
    const NAME: &'static str = <Arc<Message> as Value>::NAME;
    fn write(&self, out: &mut Vec<u8>) {
        match &self.repr {
            Repr::Held(message) => message.write(out),
            Repr::Released(_) => {
                panic!("a window message with released blocks reached a writer unread")
            }
        }
    }
    fn read(raw: Raw<'_>) -> Result<Self, DecodeError> {
        <Arc<Message> as Value>::read(raw).map(Self::new)
    }
}

#[cfg(any(test, fuzzing))]
impl crate::fuzzing::gen::Gen for WindowMessage {
    fn gen(g: &mut crate::fuzzing::gen::G<'_>) -> Self {
        Self::new(crate::fuzzing::gen::gen(g))
    }
}
