//! The durable `Message.ordinal` (a reduction-transform's per-message key) is render
//! INPUT-INVISIBLE: it must never affect the rendered request bytes. It exists only so an
//! external transform can key its durable state on a stable per-message coordinate; a
//! renderer reads role/content/origin, never ordinal. If ordinal ever leaked into the wire
//! bytes, stamping it (which happens every time a prompt is finalized) would bust the
//! prompt cache. This pins that render(with ordinals) == render(without) for every family.

use broca_protocol::{
    content::{stamp_ordinals, ContentBlock, Message, Role},
    request::CallOptions,
};
use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
    openai_chat_frozen_config, openai_responses_frozen_config, AnthropicMessages, BedrockConverse,
    Gemini, OpenAiChat, OpenAiResponses,
};
use broca_provider::wire::{FrozenRenderConfig, WireFamily};

/// A representative multi-turn prompt: system + user + assistant + tool result, so the
/// stamping touches every role.
fn prompt() -> Vec<Message> {
    vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text { text: "sys".into() }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::Text {
                text: "hi there".into(),
            }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "again".into(),
            }],
        ),
    ]
}

/// Render a prompt (unstamped) and the SAME prompt after `stamp_ordinals`, for one family,
/// asserting byte-identity.
fn assert_ordinal_invisible<F: WireFamily>(family: &F, frozen: &FrozenRenderConfig) {
    let without = family.render(frozen, &CallOptions::new(prompt()));

    let mut stamped = prompt();
    stamp_ordinals(&mut stamped);
    // Every message now carries an ordinal (the reduction key).
    assert!(
        stamped.iter().all(|m| m.ordinal.is_some()),
        "stamp_ordinals must populate every ordinal"
    );
    let with = family.render(frozen, &CallOptions::new(stamped));

    assert_eq!(
        without, with,
        "rendered bytes must be identical with and without ordinals stamped — ordinal is a \
         reduction key, never a render input"
    );
}

#[test]
fn anthropic_render_ignores_ordinal() {
    assert_ordinal_invisible(
        &AnthropicMessages,
        &anthropic_frozen_config("claude-opus-4-8"),
    );
}

#[test]
fn openai_chat_render_ignores_ordinal() {
    assert_ordinal_invisible(&OpenAiChat, &openai_chat_frozen_config("gpt-4o"));
}

#[test]
fn openai_responses_render_ignores_ordinal() {
    assert_ordinal_invisible(&OpenAiResponses, &openai_responses_frozen_config("gpt-4o"));
}

#[test]
fn gemini_render_ignores_ordinal() {
    assert_ordinal_invisible(&Gemini, &gemini_frozen_config("gemini-2.5-flash"));
}

#[test]
fn bedrock_render_ignores_ordinal() {
    assert_ordinal_invisible(
        &BedrockConverse,
        &bedrock_frozen_config("amazon.nova-lite-v1:0"),
    );
}

/// The opaque `mid` token is `m<ordinal>` and `#`-free (a consumer composes block ids as
/// `<mid>#<index>`, so a `#` in the mid would break that separator).
#[test]
fn mid_is_derived_from_ordinal_and_hash_free() {
    let mut p = prompt();
    assert!(p[0].mid().is_none(), "unstamped message has no mid");
    stamp_ordinals(&mut p);
    assert_eq!(p[0].mid().as_deref(), Some("m0"));
    assert_eq!(p[3].mid().as_deref(), Some("m3"));
    assert!(
        p.iter().all(|m| !m.mid().unwrap().contains('#')),
        "a mid must never contain '#' (it is the block-id separator)"
    );
}
