//! Content-free facts that broca's image engine reads from a session's records.
//!
//! "Image" here means any medium a request can hold: an image, a PDF, later audio and video.
//! For each medium in a request, broca must know which capture it is, whether it is still
//! protected from aging, and whether it has aged out. The engine that decides this
//! (`broca_core::media_request::SessionMedia`, with `broca_provider`'s `MediaAging` for aging)
//! needs very little of each record: ids, conversation positions, media references and a few
//! flags. A [`MediaEvent`] carries exactly that, so a storage can feed the engine without
//! rebuilding another storage's records. No event holds a message's text, a tool's input, a
//! tool list or a run's config.
//!
//! The types live in this crate because both the engine and the session store depend on it,
//! and neither depends on the other.

use serde::{Deserialize, Serialize};

use crate::{
    ContentBlock, MediaRef, Message, Role, StepId, ToolCallId, ToolOutput, ToolOutputPart,
};

/// Where one medium sits in a message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaSlot {
    /// The content block that holds it: a media block, or a tool result block.
    pub block: usize,
    /// For a medium inside a tool result block: the call the result answers, and the medium's
    /// position among the result's parts. `None` for a media block.
    pub tool_part: Option<(ToolCallId, usize)>,
    pub reference: MediaRef,
}

/// A message reduced to what the engine reads of it: its role, its number of content blocks,
/// and where each medium in it sits. The engine keeps skeletons like this, never the messages,
/// to work out which capture a prompt position means. The role matters because a system
/// message is never part of the conversation the engine selects from.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaMessage {
    pub role: Role,
    pub blocks: usize,
    /// In block order, then part order, as the message holds them.
    pub slots: Vec<MediaSlot>,
}

impl MediaMessage {
    /// The skeleton of `message`. Text, reasoning, tool calls and every other non-media
    /// content are left out.
    pub fn of(message: &Message) -> Self {
        let mut slots = Vec::new();
        for (block, value) in message.content.iter().enumerate() {
            match value {
                ContentBlock::Media(reference) => slots.push(MediaSlot {
                    block,
                    tool_part: None,
                    reference: reference.clone(),
                }),
                ContentBlock::ToolResult(result) => {
                    if let ToolOutput::Parts { parts } = &result.output {
                        for (part, value) in parts.iter().enumerate() {
                            if let ToolOutputPart::Media { media } = value {
                                slots.push(MediaSlot {
                                    block,
                                    tool_part: Some((result.tool_call_id.clone(), part)),
                                    reference: media.clone(),
                                });
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        Self {
            role: message.role,
            blocks: message.content.len(),
            slots,
        }
    }

    pub fn holds_media(&self) -> bool {
        !self.slots.is_empty()
    }

    /// A message with this skeleton's shape: empty text in every block that holds no medium,
    /// and each medium at its slot, inside a tool result block for a tool part. Its `ordinal`
    /// is unset. Refused when a slot names a block past the message's last.
    pub fn to_message(&self) -> Result<Message, String> {
        let mut message = Message::new(
            self.role,
            vec![
                ContentBlock::Text {
                    text: String::new()
                };
                self.blocks
            ],
        );
        for slot in &self.slots {
            let Some(block) = message.content.get_mut(slot.block) else {
                return Err(format!(
                    "media slot names block {} of a message with {} blocks",
                    slot.block, self.blocks
                ));
            };
            match &slot.tool_part {
                Some((call, part)) => {
                    if !matches!(block, ContentBlock::ToolResult(_)) {
                        *block = ContentBlock::ToolResult(crate::ToolResult {
                            tool_call_id: call.clone(),
                            output: ToolOutput::Parts { parts: Vec::new() },
                            is_error: false,
                        });
                    }
                    if let ContentBlock::ToolResult(result) = block {
                        if let ToolOutput::Parts { parts } = &mut result.output {
                            if parts.len() <= *part {
                                parts.resize_with(part + 1, || ToolOutputPart::Text {
                                    text: String::new(),
                                });
                            }
                            parts[*part] = ToolOutputPart::Media {
                                media: slot.reference.clone(),
                            };
                        }
                    }
                }
                None => *block = ContentBlock::Media(slot.reference.clone()),
            }
        }
        Ok(message)
    }
}

/// One input message of a run that holds a medium.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaInput {
    /// The message's position among the run's input messages.
    pub message: usize,
    /// The message's position in the whole conversation, counted from 0 as
    /// [`Message::ordinal`] counts it.
    pub ordinal: u64,
    pub skeleton: MediaMessage,
}

/// One medium among a tool result's parts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MediaPart {
    /// The medium's position among the result's parts.
    pub part: usize,
    pub reference: MediaRef,
}

/// The model's media limits and costs: the `media` member of a run's frozen render config,
/// exactly as recorded. It is opaque here: broca-provider decodes it as its `MediaProfile`,
/// which decides when aging runs and what each medium costs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MediaLimits(pub serde_json::Value);

/// One fact of a session's history that the image engine acts on, in the order the records
/// that state it were written.
///
/// A capture is named by `(record, message, block, part)`: `record` is the record the event
/// comes from, so every event that captures media carries it. Positions in the conversation
/// (ordinals) are carried too: the engine never works them out itself.
///
/// A record of the old storage's compaction protocol, where the compactor replaces a range of
/// the transcript with its own messages (`compaction_message`, `compaction_view_applied`), has
/// no event. It changes which images show through compaction code the engine does not run on
/// events. A session that holds one continues from an index saved after it, never from
/// events. The current compaction contract's views need no event: the engine ignores them,
/// since an image a view hides is simply not in the prompt.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub enum MediaEvent {
    /// A run starts. Per-run state resets, and the run's input messages are placed. Their
    /// media are a person's, except media inside a tool result, which are a tool's.
    RunStart {
        record: u64,
        /// The inputs that hold a medium, in input order. An input without one captures
        /// nothing and is left out.
        inputs: Vec<MediaInput>,
        /// The run's media limits, when its frozen render config has them.
        limits: Option<MediaLimits>,
    },
    /// Messages accepted into a running run before they are placed: a steer (a message a
    /// person sends while the run works, which joins at the next step boundary), or a queued
    /// send delivered as messages. Their media are a person's, protected through the first
    /// step that starts after they are accepted.
    Accepted {
        record: u64,
        /// One skeleton per accepted message, in order, including a message that holds no
        /// medium: each takes one conversation position when the messages are placed.
        messages: Vec<MediaMessage>,
    },
    /// Every accepted message not yet placed is placed, in the order accepted, at
    /// consecutive ordinals starting at `first`. At a step boundary it comes before that
    /// step's [`MediaEvent::StepStart`].
    Placed { first: u64 },
    /// A tool batch starts, with its calls in planned order. Each result sits at its call's
    /// position in the batch's results message.
    BatchStart { calls: Vec<ToolCallId> },
    /// A tool result whose output is a list of parts. A result whose output is anything else
    /// has no event.
    ToolResult {
        record: u64,
        call: ToolCallId,
        /// The ordinal of the results message: the ordinal of the assistant message waiting
        /// for these results, plus one. `None` when no assistant message is waiting.
        ordinal: Option<u64>,
        /// False for a late result: a result that arrived after its batch closed. Its media
        /// are captured but never shown, and it adds nothing to the results message.
        shown: bool,
        /// The media among the parts, in part order; possibly none. A shown result without
        /// media still marks its place in the results message.
        media: Vec<MediaPart>,
    },
    /// A model step starts. The step counter moves, and the media protected for the previous
    /// step lose their protection. A repeated start of the same step (a retry) changes
    /// nothing.
    StepStart { step: StepId },
    /// A model attempt ends. Aging may run.
    AttemptEnd,
    /// A model step ends. Aging may run, and no step is open after it.
    StepEnd,
    /// The run's media limits change inside the run.
    LimitsChanged { limits: MediaLimits },
    /// The transcript is cut back to its first `len` messages, at ordinals `0..len`: an undo
    /// to an earlier message, or a storage recovery that removed the messages after a damaged
    /// point. The media of the removed messages are forgotten, with their aging, and so is
    /// every accepted message not yet placed. The cut also ends the run's state: the open
    /// batch, and the protection of the current step's media. A medium that had aged out and
    /// whose message remains stays aged. The request prefix, the start of the provider request
    /// that the provider has already written to its prompt cache, shows that medium as a
    /// not-shown marker; showing it again would change those bytes and lose the cache.
    Truncated { len: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn image(digit: char) -> MediaRef {
        MediaRef {
            kind: crate::MediaKind::Image,
            mime: "image/png".into(),
            byte_size: 1,
            sha256: digit.to_string().repeat(64),
            width: Some(1),
            height: Some(1),
        }
    }

    #[test]
    fn a_skeleton_keeps_every_media_position_and_no_text() {
        let message = Message::new(
            Role::User,
            vec![
                ContentBlock::Text {
                    text: "words the skeleton must not keep".into(),
                },
                ContentBlock::Media(image('a')),
                ContentBlock::ToolResult(crate::ToolResult {
                    tool_call_id: ToolCallId("call".into()),
                    output: ToolOutput::Parts {
                        parts: vec![
                            ToolOutputPart::Text {
                                text: "tool text".into(),
                            },
                            ToolOutputPart::Media { media: image('b') },
                            ToolOutputPart::Text {
                                text: "trailing".into(),
                            },
                        ],
                    },
                    is_error: true,
                }),
            ],
        );
        let skeleton = MediaMessage::of(&message);
        assert_eq!(skeleton.blocks, 3);
        assert_eq!(
            skeleton.slots,
            vec![
                MediaSlot {
                    block: 1,
                    tool_part: None,
                    reference: image('a'),
                },
                MediaSlot {
                    block: 2,
                    tool_part: Some((ToolCallId("call".into()), 1)),
                    reference: image('b'),
                },
            ]
        );
        let rebuilt = skeleton.to_message().unwrap();
        assert_eq!(MediaMessage::of(&rebuilt), skeleton);
        let text = serde_json::to_string(&rebuilt).unwrap();
        assert!(!text.contains("words") && !text.contains("tool text"));
    }

    #[test]
    fn a_slot_past_the_last_block_is_refused() {
        let skeleton = MediaMessage {
            role: Role::User,
            blocks: 1,
            slots: vec![MediaSlot {
                block: 1,
                tool_part: None,
                reference: image('a'),
            }],
        };
        assert!(skeleton.to_message().is_err());
    }
}
