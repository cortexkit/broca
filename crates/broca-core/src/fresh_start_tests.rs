use broca_protocol::content::{ContentBlock, Message, Role};

use super::*;

fn text(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

fn plan(system: &str) -> PlanManifest {
    serde_json::from_value(serde_json::json!({
        "plan": "{}", "plan_identity": "plan", "composition_digest": "composition",
        "fetches": [], "tools": [], "system_text": system, "system_digest": "system",
        "join_version": 1, "role_versions": {}
    }))
    .unwrap()
}

#[test]
fn a_new_episode_keeps_its_previous_system_text_when_the_send_supplies_none() {
    let previous = [text(Role::System, "rules")];
    let mut input = vec![text(Role::User, "hi")];
    let globals = new_episode_globals(&mut input, None, Some(&previous)).unwrap();
    assert_eq!(globals, previous);
    assert_eq!(input, [text(Role::User, "hi")]);
}

#[test]
fn a_new_episode_accepts_the_same_system_text_again_and_refuses_different_text() {
    let previous = [text(Role::System, "rules")];
    let mut same = vec![text(Role::System, "rules"), text(Role::User, "hi")];
    let globals = new_episode_globals(&mut same, None, Some(&previous)).unwrap();
    assert_eq!(globals, previous);
    assert_eq!(
        same,
        [text(Role::User, "hi")],
        "the system text leaves the input"
    );

    let mut changed = vec![text(Role::System, "other rules"), text(Role::User, "hi")];
    assert!(matches!(
        new_episode_globals(&mut changed, None, Some(&previous)),
        Err(RunError::SystemPromptChanged)
    ));
}

#[test]
fn a_frozen_plan_supplies_the_system_text_whatever_the_send_carries() {
    let previous = [text(Role::System, "rules")];
    let mut input = vec![text(Role::System, "other rules"), text(Role::User, "hi")];
    let globals = new_episode_globals(&mut input, Some(&plan("plan rules")), Some(&previous));
    assert_eq!(globals.unwrap(), [text(Role::System, "plan rules")]);
    assert_eq!(input, [text(Role::User, "hi")]);
}

#[test]
fn an_appended_episode_strips_a_repeated_system_prompt_and_refuses_a_new_one() {
    let prefix = [text(Role::System, "rules"), text(Role::User, "first")];
    let mut repeat = vec![text(Role::System, "rules"), text(Role::User, "second")];
    append_system_messages(&mut repeat, &prefix, false).unwrap();
    assert_eq!(repeat, [text(Role::User, "second")]);

    let mut changed = vec![text(Role::System, "other"), text(Role::User, "second")];
    assert!(matches!(
        append_system_messages(&mut changed, &prefix, false),
        Err(RunError::SystemPromptChanged)
    ));

    let imported = [text(Role::User, "imported")];
    let mut first = vec![text(Role::System, "rules"), text(Role::User, "second")];
    append_system_messages(&mut first, &imported, true).unwrap();
    assert_eq!(
        first.len(),
        2,
        "after an import the harness sets the system prompt"
    );
    let mut first = vec![text(Role::System, "rules"), text(Role::User, "second")];
    assert!(matches!(
        append_system_messages(&mut first, &imported, false),
        Err(RunError::SystemPromptChanged)
    ));
}
