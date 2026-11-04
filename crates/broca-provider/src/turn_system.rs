//! Render-edge authority for stored, turn-scoped template text.

use std::borrow::Cow;

use broca_protocol::{CallOptions, ContentBlock, Role};

use crate::{spec::WireFamilyId, wire::FrozenRenderConfig};

/// Admission's single source of route support. Anthropic authority requires its own
/// catalog's explicit published fact; compatible hosts and absent facts fail closed.
pub fn catalog_support(
    family: WireFamilyId,
    provider: &str,
    model: &crate::spec::ModelSpec,
) -> bool {
    match family {
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => true,
        WireFamilyId::AnthropicMessages => {
            provider == "anthropic" && model.mid_conversation_system == Some(true)
        }
        _ => false,
    }
}

/// Only confirmed routes may receive system authority in the conversation.
pub fn supported(frozen: &FrozenRenderConfig) -> bool {
    match frozen.family_id {
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => true,
        WireFamilyId::AnthropicMessages => frozen.anthropic_turn_system,
        _ => false,
    }
}

/// Whether a newly delivered template can end this request at system authority.
pub fn possible(frozen: &FrozenRenderConfig, prompt: &[broca_protocol::Message]) -> bool {
    possible_after(frozen, prompt.last(), false)
}

pub(crate) fn possible_after(
    frozen: &FrozenRenderConfig,
    preceding: Option<&broca_protocol::Message>,
    followed_by_user: bool,
) -> bool {
    if !supported(frozen) {
        return false;
    }
    if frozen.family_id != WireFamilyId::AnthropicMessages {
        return true;
    }
    !followed_by_user
        && preceding.is_some_and(|message| {
            !message.turn_system && matches!(message.role, Role::User | Role::Tool)
        })
}

/// Downgrade only the rendered copy: history retains its bytes, role and delivery mode.
/// A borrowed result leaves every legacy request on its original rendering path.
pub(crate) fn prepare<'a>(
    frozen: &FrozenRenderConfig,
    opts: &'a CallOptions,
) -> Cow<'a, CallOptions> {
    if supported(frozen) || !opts.prompt.iter().any(|message| message.turn_system) {
        return Cow::Borrowed(opts);
    }
    let mut rendered = opts.clone();
    for message in &mut rendered.prompt {
        if message.turn_system {
            message.role = Role::User;
            let text = text(message);
            message.content = vec![ContentBlock::Text {
                text: format!("<context-note>{text}</context-note>"),
            }];
        }
    }
    Cow::Owned(rendered)
}

pub(crate) fn text(message: &broca_protocol::Message) -> String {
    message
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::Text { text } => Some(text.as_str()),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join("\n\n")
}
