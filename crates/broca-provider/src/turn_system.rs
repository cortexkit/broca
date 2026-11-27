//! Render-edge authority for stored, turn-scoped template text.

use std::borrow::Cow;

use broca_protocol::{CallOptions, ContentBlock, Role};

use crate::{spec::WireFamilyId, wire::FrozenRenderConfig};

pub mod tool_addition;

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

/// Places that can still carry an Anthropic note after the view and echo rules are applied.
/// At a fresh run's boundary the new input follows the prompt, so its current end is not legal.
pub fn rendering_anchors(
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
    end_is_legal: bool,
) -> std::collections::BTreeSet<u64> {
    let view = crate::families::compaction_view(frozen.family_id, opts);
    let visible = crate::families::anthropic_messages::rendered_note_positions(frozen, &view);
    visible
        .iter()
        .enumerate()
        .filter_map(|(at, &index)| {
            let entry = &view.prompt[index];
            let ordinal = entry.ordinal?;
            let next = visible.get(at + 1).map(|&next| &view.prompt[next]);
            (matches!(entry.role, Role::User | Role::Tool)
                && next.is_none_or(|message| message.ordinal != Some(ordinal))
                && next.map_or(end_is_legal, |message| message.role == Role::Assistant))
            .then_some(ordinal + 1)
        })
        .collect()
}

/// Whether a newly delivered template can end this request at system authority.
pub fn possible(frozen: &FrozenRenderConfig, prompt: &[broca_protocol::Message]) -> bool {
    possible_after(frozen, prompt.last(), false)
}

/// Whether a template delivered right after `preceding` can go out at system authority on the
/// route `frozen` names; `followed_by_user` says another send in the same request will place a
/// user message after it.
pub fn possible_after(
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
pub fn prepare<'a>(frozen: &FrozenRenderConfig, opts: &'a CallOptions) -> Cow<'a, CallOptions> {
    if supported(frozen) || !opts.prompt.iter().any(|message| message.turn_system) {
        return Cow::Borrowed(opts);
    }
    let mut rendered = opts.clone();
    for message in &mut rendered.prompt {
        if message.turn_system
            && (!message.tags.plan_note || message.tags.legacy_plan_note)
            && message.tool_addition.is_none()
        {
            message.role = Role::User;
            let text = text(message);
            message.content = vec![ContentBlock::Text {
                text: format!("<context-note>{text}</context-note>").into(),
            }];
        }
    }
    Cow::Owned(rendered)
}

/// Anthropic accepts a system message in the conversation only right after a user message (a
/// tool result included), and only right before an assistant message or at the end of the
/// request. Delivery checks that place against the history it sees, but the request a later
/// step renders can differ: a compaction view may drop the messages around the template, which
/// can leave it after a model reply, before a user message, or next to another template. Such a
/// template goes out as a user note instead, so the request stays one the API accepts. Only the
/// rendered copy changes, and a template in a place the API accepts keeps its bytes. System
/// messages that render at the top of the request are skipped, as the request body leaves them
/// out of the conversation.
///
/// `renders` says whether the prompt message at an index reaches the request at all. The
/// renderer leaves out a message whose content renders empty, such as a reply whose every block
/// a view dropped, so a template's neighbour in the prompt need not be its neighbour in the
/// request.
pub fn place<'a>(
    frozen: &FrozenRenderConfig,
    opts: &'a CallOptions,
    renders: impl Fn(usize) -> bool,
) -> Cow<'a, CallOptions> {
    if frozen.family_id != WireFamilyId::AnthropicMessages || !supported(frozen) {
        return Cow::Borrowed(opts);
    }
    let is_template =
        |message: &broca_protocol::Message| message.turn_system && message.role == Role::System;
    let conversation: Vec<usize> = (0..opts.prompt.len())
        .filter(|&index| {
            let message = &opts.prompt[index];
            (message.role != Role::System || message.turn_system) && renders(index)
        })
        .collect();
    // Whether the message rendered last is a user message: a tool result, a person's message,
    // or a template already turned into a user note. A template that stays a system message
    // is not one.
    let mut after_user = false;
    let mut misplaced = Vec::new();
    for (position, &index) in conversation.iter().enumerate() {
        let message = &opts.prompt[index];
        // Plan notes are validated against the request view before insertion, not rewritten.
        if (message.tags.plan_note && !message.tags.legacy_plan_note)
            || message.tool_addition.is_some()
        {
            after_user = false;
            continue;
        }
        if !is_template(message) {
            after_user = matches!(message.role, Role::User | Role::Tool);
            continue;
        }
        // The next message as it will render: a template there never renders as an assistant
        // message, whichever way it goes out.
        let before_reply = conversation.get(position + 1).is_none_or(|&next| {
            let next = &opts.prompt[next];
            next.role == Role::Assistant && !is_template(next)
        });
        if after_user && before_reply {
            after_user = false;
        } else {
            misplaced.push(index);
            after_user = true;
        }
    }
    if misplaced.is_empty() {
        return Cow::Borrowed(opts);
    }
    let mut rendered = opts.clone();
    for index in misplaced {
        // The same user note an unsupported route renders (`prepare`).
        let message = &mut rendered.prompt[index];
        let text = text(message);
        message.role = Role::User;
        message.content = vec![ContentBlock::Text {
            text: format!("<context-note>{text}</context-note>").into(),
        }];
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
