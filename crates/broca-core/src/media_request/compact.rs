//! The compact forms the image engine keeps its state in. A session can hold thousands of
//! images for its whole life, so each distinct reference is kept once, in a table that the
//! captures and message copies name positions of, and a message copy keeps only the few facts
//! a text-free copy has. Both rebuild exactly what they stand for.

use broca_protocol::{
    ContentBlock, MediaRef, Message, Role, ToolCallId, ToolOutput, ToolOutputPart, ToolResult,
};

/// A reference's position in [`References`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct RefId(u32);

/// Every distinct reference the session has captured, each kept once. It never shrinks: a
/// reference stays even after an undo or a recovery removed the message that held it, so the
/// table is also the set of references the session may render.
#[derive(Debug, Clone, Default)]
pub(crate) struct References {
    refs: Vec<MediaRef>,
    /// Positions in `refs`, ordered by digest and then by position: finds a digest's
    /// references.
    by_digest: Vec<RefId>,
}

impl References {
    pub(crate) fn get(&self, id: RefId) -> &MediaRef {
        &self.refs[id.0 as usize]
    }

    /// The references with `digest`, in the order they were first captured.
    pub(crate) fn of_digest(&self, digest: &str) -> &[RefId] {
        let start = self
            .by_digest
            .partition_point(|id| self.get(*id).sha256.as_str() < digest);
        let len = self.by_digest[start..].partition_point(|id| self.get(*id).sha256 == digest);
        &self.by_digest[start..start + len]
    }

    /// The position of `reference`, when it was captured.
    pub(crate) fn find(&self, reference: &MediaRef) -> Option<RefId> {
        self.of_digest(&reference.sha256)
            .iter()
            .copied()
            .find(|id| self.get(*id) == reference)
    }

    /// The position of `reference`, which joins the table when it is new.
    pub(crate) fn intern(&mut self, reference: &MediaRef) -> RefId {
        if let Some(id) = self.find(reference) {
            return id;
        }
        let id = RefId(u32::try_from(self.refs.len()).expect("fewer than 2^32 references"));
        // After the digest's earlier references, so each digest's stay in position order.
        let at = self
            .by_digest
            .partition_point(|other| self.get(*other).sha256 <= reference.sha256);
        self.refs.push(reference.clone());
        self.by_digest.insert(at, id);
        id
    }

    /// Every reference, in the order first captured.
    pub(crate) fn iter(&self) -> impl Iterator<Item = &MediaRef> {
        self.refs.iter()
    }

    /// The heap the table holds (see `media_request::held`).
    pub(crate) fn heap_bytes(&self) -> usize {
        use super::held::{media_ref, vec};
        vec(&self.refs, media_ref) + vec(&self.by_digest, |_| 0)
    }
}

/// A text-free copy of a message that the engine keeps, to find where its media sit when a
/// later request is assembled (see `SessionMedia::capture_message`). A copy is kept as its
/// [`Shape`] when that holds all of it, and whole otherwise.
#[derive(Debug, Clone)]
pub(crate) enum MessageCopy {
    Shaped(Shape),
    Whole(Box<Message>),
}

impl MessageCopy {
    /// The copy to keep of `message`, which sits at `ordinal` (none for a message not placed
    /// yet). It is kept as its shape only when `shape` allows it and the shape rebuilds the
    /// same message; its references then join `refs`.
    pub(crate) fn keep(
        message: Message,
        ordinal: Option<u64>,
        refs: &mut References,
        shape: bool,
    ) -> Self {
        if shape && message.ordinal == ordinal && Shape::holds(&message) {
            let shaped = Shape::of(&message, refs);
            if shaped.message(refs, ordinal) == message {
                return Self::Shaped(shaped);
            }
        }
        Self::Whole(Box::new(message))
    }

    /// The message this copy stands for, kept at `ordinal`. A whole copy keeps its own.
    pub(crate) fn message(&self, refs: &References, ordinal: Option<u64>) -> Message {
        match self {
            Self::Shaped(shape) => shape.message(refs, ordinal),
            Self::Whole(message) => (**message).clone(),
        }
    }

    /// The copy of a message not placed yet is now kept at `ordinal`.
    pub(crate) fn place(&mut self, ordinal: u64) {
        if let Self::Whole(message) = self {
            message.ordinal = Some(ordinal);
        }
    }

    /// The heap the copy holds, but for its references, which [`References`] holds once.
    pub(crate) fn heap_bytes(&self) -> usize {
        use super::held::{boxed, message};
        match self {
            Self::Shaped(shape) => boxed(&shape.blocks, |block| match block {
                Block::Result(result) => {
                    std::mem::size_of::<ResultShape>()
                        + result.call.0.capacity()
                        + boxed(&result.parts, |_| 0)
                }
                Block::Empty | Block::Media(_) => 0,
            }),
            Self::Whole(whole) => std::mem::size_of::<Message>() + message(whole),
        }
    }
}

/// The shape of a text-free message copy: its role, and for each block whether it is empty
/// text, a medium, or a tool result whose parts are each empty text or a medium. A copy with
/// anything else (text, other blocks, other tool output, an origin, a cache prefix or turn
/// template text) has no shape.
#[derive(Debug, Clone)]
pub(crate) struct Shape {
    role: Role,
    blocks: Box<[Block]>,
}

#[derive(Debug, Clone)]
enum Block {
    Empty,
    Media(RefId),
    Result(Box<ResultShape>),
}

#[derive(Debug, Clone)]
struct ResultShape {
    call: ToolCallId,
    is_error: bool,
    /// Each part's medium; `None` for an empty text part.
    parts: Box<[Option<RefId>]>,
}

impl Shape {
    /// Whether `message` has a shape.
    fn holds(message: &Message) -> bool {
        !message.turn_system
            && message.cache_prefix_blocks.is_none()
            && message.origin.is_none()
            && message.content.iter().all(|block| match block {
                ContentBlock::Text { text } => text.is_empty(),
                ContentBlock::Media(_) => true,
                ContentBlock::ToolResult(result) => match &result.output {
                    ToolOutput::Parts { parts } => parts.iter().all(|part| match part {
                        ToolOutputPart::Text { text } => text.is_empty(),
                        ToolOutputPart::Media { .. } => true,
                    }),
                    _ => false,
                },
                _ => false,
            })
    }

    /// The shape of `message`, which [`Shape::holds`]; its references join `refs`.
    fn of(message: &Message, refs: &mut References) -> Self {
        let blocks = message
            .content
            .iter()
            .map(|block| match block {
                ContentBlock::Media(reference) => Block::Media(refs.intern(reference)),
                ContentBlock::ToolResult(result) => {
                    let parts = match &result.output {
                        ToolOutput::Parts { parts } => parts
                            .iter()
                            .map(|part| match part {
                                ToolOutputPart::Media { media } => Some(refs.intern(media)),
                                ToolOutputPart::Text { .. } => None,
                            })
                            .collect(),
                        _ => Box::default(),
                    };
                    Block::Result(Box::new(ResultShape {
                        call: result.tool_call_id.clone(),
                        is_error: result.is_error,
                        parts,
                    }))
                }
                _ => Block::Empty,
            })
            .collect();
        Self {
            role: message.role,
            blocks,
        }
    }

    /// The message of this shape at `ordinal`.
    fn message(&self, refs: &References, ordinal: Option<u64>) -> Message {
        let content = self
            .blocks
            .iter()
            .map(|block| match block {
                Block::Empty => ContentBlock::Text {
                    text: String::new().into(),
                },
                Block::Media(id) => ContentBlock::Media(refs.get(*id).clone()),
                Block::Result(result) => ContentBlock::ToolResult(ToolResult {
                    tool_call_id: result.call.clone(),
                    output: ToolOutput::Parts {
                        parts: result
                            .parts
                            .iter()
                            .map(|part| match part {
                                Some(id) => ToolOutputPart::Media {
                                    media: refs.get(*id).clone(),
                                },
                                None => ToolOutputPart::Text {
                                    text: String::new().into(),
                                },
                            })
                            .collect(),
                    },
                    is_error: result.is_error,
                }),
            })
            .collect();
        let mut message = Message::new(self.role, content);
        message.ordinal = ordinal;
        message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::MediaKind;

    fn reference(digest: &str, byte_size: u64) -> MediaRef {
        MediaRef {
            kind: MediaKind::Image,
            mime: "image/png".into(),
            byte_size,
            sha256: digest.into(),
            width: Some(1),
            height: Some(1),
        }
    }

    #[test]
    fn a_reference_is_kept_once_and_found_by_digest_in_the_order_captured() {
        let mut refs = References::default();
        let b = refs.intern(&reference(&"b".repeat(64), 1));
        let a = refs.intern(&reference(&"a".repeat(64), 1));
        // The same digest with other metadata is another reference of that digest.
        let b2 = refs.intern(&reference(&"b".repeat(64), 2));
        assert_eq!(refs.intern(&reference(&"b".repeat(64), 1)), b);
        assert_eq!(refs.of_digest(&"b".repeat(64)), [b, b2]);
        assert_eq!(refs.of_digest(&"a".repeat(64)), [a]);
        assert!(refs.of_digest(&"c".repeat(64)).is_empty());
        assert_eq!(refs.find(&reference(&"b".repeat(64), 2)), Some(b2));
        assert_eq!(refs.find(&reference(&"b".repeat(64), 3)), None);
        assert_eq!(refs.iter().count(), 3);
    }

    /// A copy with only empty text, media and media-only tool results is kept as its shape
    /// and rebuilds the same message; any other copy is kept whole, as it is.
    #[test]
    fn a_message_copy_rebuilds_the_message_it_keeps() {
        let mut refs = References::default();
        let photo = reference(&"a".repeat(64), 1);
        let mut shaped = Message::new(
            Role::User,
            vec![
                ContentBlock::Text {
                    text: String::new().into(),
                },
                ContentBlock::Media(photo.clone()),
                ContentBlock::ToolResult(ToolResult {
                    tool_call_id: ToolCallId("call".into()),
                    output: ToolOutput::Parts {
                        parts: vec![
                            ToolOutputPart::Text {
                                text: String::new().into(),
                            },
                            ToolOutputPart::Media {
                                media: photo.clone(),
                            },
                        ],
                    },
                    is_error: true,
                }),
            ],
        );
        shaped.ordinal = Some(4);
        let copy = MessageCopy::keep(shaped.clone(), Some(4), &mut refs, true);
        assert!(matches!(copy, MessageCopy::Shaped(_)));
        assert_eq!(copy.message(&refs, Some(4)), shaped);

        let mut with_origin = shaped.clone();
        with_origin.origin = Some(broca_protocol::request::ModelHandle::new("p", "m"));
        let mut with_text = shaped.clone();
        with_text.content[0] = ContentBlock::Text {
            text: "kept".into(),
        };
        let mut held_elsewhere = shaped.clone();
        held_elsewhere.ordinal = Some(5);
        for message in [with_origin, with_text, held_elsewhere] {
            let copy = MessageCopy::keep(message.clone(), Some(4), &mut refs, true);
            assert!(matches!(copy, MessageCopy::Whole(_)), "{message:?}");
            assert_eq!(copy.message(&refs, Some(4)), message);
        }
        // An engine that doesn't ask for shapes keeps every copy whole.
        let copy = MessageCopy::keep(shaped.clone(), Some(4), &mut refs, false);
        assert!(matches!(copy, MessageCopy::Whole(_)));
    }
}
