use broca_core::plan_notes::{self, Note};
use broca_protocol::{CallOptions, ContentBlock, Message, Role};
use broca_provider::{
    families::{anthropic_apikey_frozen_config, AnthropicMessages},
    wire::WireFamily,
};

fn text(role: Role, value: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: value.into() }])
}

fn opts() -> CallOptions {
    let mut prompt = vec![
        text(Role::User, "one"),
        text(Role::Assistant, "answer"),
        text(Role::User, "two"),
    ];
    broca_protocol::content::stamp_ordinals(&mut prompt);
    CallOptions::new(prompt)
}

fn bytes(opts: &CallOptions, supported: bool) -> String {
    let mut frozen = anthropic_apikey_frozen_config("claude-opus-5-5");
    frozen.cache = None;
    frozen.anthropic_turn_system = supported;
    String::from_utf8(AnthropicMessages.render(&frozen, opts)).unwrap()
}

#[test]
fn unmarked_plan_notes_match_0149_one_and_two_at_the_same_anchor() {
    let opts = opts();
    let one = plan_notes::view_for(&opts, [(1, Note::LegacyText("owner"))], true).unwrap();
    let two = plan_notes::view_for(
        &opts,
        [
            (1, Note::LegacyText("owner")),
            (1, Note::LegacyText("removed")),
        ],
        true,
    )
    .unwrap();
    assert_eq!(
        bytes(&one, true),
        include_str!("fixtures/notes_0149_one.json").trim_end()
    );
    assert_eq!(
        bytes(&two, true),
        include_str!("fixtures/notes_0149_two.json").trim_end()
    );
}

#[test]
fn ordinary_turn_system_template_has_0149_authority_and_bytes() {
    let mut opts = opts();
    let mut template = text(Role::System, "owner");
    template.turn_system = true;
    opts.prompt.insert(1, template);
    assert_eq!(
        bytes(&opts, true),
        include_str!("fixtures/notes_0149_one.json").trim_end()
    );
    assert_eq!(
        bytes(&opts, false),
        include_str!("fixtures/template_0149_unsupported.json").trim_end()
    );
}
