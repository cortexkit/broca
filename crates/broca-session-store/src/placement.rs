//! Building durable entry facts for messages placed by the runner or by log recovery.

use broca_protocol::tags::compaction::{numbered_parts, numbered_parts_of, NumberingKind};

use crate::fold::conversation::{BlockKind, Conversation, WindowMessage};
use crate::fold::operations::Operations;
use crate::record::{EntryFacts, Notice, Placement, Record};

/// The identity and baseline facts shared by placement writers. Optional display and run facts
/// belong to the writer that has evidence for them.
pub fn entry_facts(
    record: &Record,
    ordinal: u64,
    mid: String,
    written_at_ms: Option<u64>,
    tag_base: u64,
    render_tags: bool,
) -> EntryFacts {
    EntryFacts {
        ordinal,
        written_at_ms,
        tag_base,
        notice: None,
        usage: None,
        generated_under: None,
        native: native(record),
        render_tags,
        clean_reply: false,
        mid,
        run_id: None,
        created_at_ms: None,
        delivery_mode: None,
        delivery_tool_call_id: None,
        client_message_id: None,
        sender: None,
        marks: None,
        no_display_facts: None,
        reasoning_summaries: Vec::new(),
        rich_ui: None,
        runner_tag_base: None,
        legacy_view_seq: None,
        notices: Vec::new(),
        named_speaker: None,
        send_id: None,
        kept_notice: None,
    }
}

/// A model step, a tool-results message or a notice from broca, rather than caller input.
pub fn native(record: &Record) -> bool {
    match record {
        Record::Input(input) => matches!(
            input.placement,
            Placement::BudgetNudge | Placement::DamageNotice
        ),
        Record::Assistant(_) | Record::ToolReply(_) => true,
        _ => false,
    }
}

pub fn runner_notice() -> Notice {
    Notice {
        sender: "broca".into(),
        template: None,
    }
}

/// How many compaction part numbers a message takes, whether or not its content is held.
pub fn window_numbered_parts(message: &WindowMessage) -> u64 {
    numbered_parts_of(
        message.role(),
        message.blocks().map(|block| match block.kind() {
            BlockKind::Text | BlockKind::TextWithState if !block.text_empty() => {
                NumberingKind::Text
            }
            BlockKind::ToolCall => NumberingKind::ToolCall,
            _ => NumberingKind::Other,
        }),
    )
}

/// The highest part number the session has taken: the newest entry's base plus its numbered
/// parts, or the marker's checkpoint with no window, never below the enrollment or revert floor.
pub fn lifetime_tag_high(
    conversation: &Conversation,
    operations: &Operations,
) -> Result<u64, String> {
    let newest = match (&conversation.open_entry, conversation.open_tool_turn()) {
        (Some(entry), Some(message)) => {
            Some((conversation.placed + 1, numbered_parts(message), entry))
        }
        _ => conversation
            .messages
            .last()
            .zip(conversation.entries.last())
            .map(|(message, entry)| (conversation.placed, window_numbered_parts(message), entry)),
    };
    let high = match (newest, operations.enrolled.as_ref()) {
        // Enrollment counted older entries whose facts don't carry compaction numbers.
        (Some((ordinal, _, _)), Some(enrolled)) if ordinal <= enrolled.through_ordinal => {
            enrolled.tag_high
        }
        (Some((_, parts, entry)), _) => {
            let facts = entry
                .facts
                .as_ref()
                .ok_or("entry facts: the newest message has none")?;
            facts.tag_base + parts
        }
        (None, _) => conversation
            .marker
            .as_ref()
            .map_or(0, |marker| marker.checkpoint),
    };
    Ok(high.max(operations.tag_high_water))
}
