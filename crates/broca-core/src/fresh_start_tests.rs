use broca_protocol::content::{ContentBlock, Message, Role};

use super::*;

#[test]
fn temperature_model_policy_omits_unsupported_and_clamps_known_ranges() {
    use crate::provider::TemperaturePolicy;
    for (policy, requested, expected) in [
        (
            TemperaturePolicy {
                supported: Some(false),
                min: None,
                max: None,
            },
            Some(0.1),
            None,
        ),
        (
            TemperaturePolicy {
                supported: Some(true),
                min: Some(0.25),
                max: Some(1.5),
            },
            Some(0.0),
            Some(0.25),
        ),
        (
            TemperaturePolicy {
                supported: None,
                min: Some(0.25),
                max: Some(1.5),
            },
            Some(2.0),
            Some(1.5),
        ),
        (
            TemperaturePolicy {
                supported: Some(true),
                min: Some(0.0),
                max: Some(2.0),
            },
            Some(2.0),
            Some(2.0),
        ),
        (
            TemperaturePolicy {
                supported: Some(true),
                min: None,
                max: None,
            },
            Some(2.0),
            Some(1.0),
        ),
        (TemperaturePolicy::default(), None, None),
    ] {
        assert_eq!(
            policy.apply(requested),
            expected,
            "{policy:?}, {requested:?}"
        );
    }
}

#[test]
fn temperature_known_minimum_clamps_at_admission() {
    let policy = crate::provider::TemperaturePolicy {
        supported: Some(true),
        min: Some(0.25),
        max: Some(1.5),
    };
    assert_eq!(policy.apply(Some(0.0)), Some(0.25));
}

#[test]
fn temperature_known_maximum_clamps_at_admission() {
    let policy = crate::provider::TemperaturePolicy {
        supported: None,
        min: Some(0.25),
        max: Some(1.5),
    };
    assert_eq!(policy.apply(Some(2.0)), Some(1.5));
}

#[test]
fn temperature_admission_uses_the_conservative_range_without_model_facts() {
    let provider = crate::provider::StubProvider::new(vec![]);
    let generation = GenerationConfig {
        temperature: Some(2.0),
        top_p: Some(0.75),
        ..Default::default()
    };
    let admitted = admitted_generation(&generation, &provider).unwrap();
    assert_eq!(admitted.temperature, Some(1.0));
    assert_eq!(admitted.top_p, Some(0.75));
}

#[test]
fn temperature_admission_refuses_out_of_range_and_non_finite_internal_values() {
    let provider = crate::provider::StubProvider::new(vec![]);
    for value in [-0.1, 2.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
        let generation = GenerationConfig {
            temperature: Some(value),
            ..Default::default()
        };
        assert!(
            matches!(admitted_generation(&generation, &provider), Err(RunError::Config(message))
            if message.contains("generation.temperature")),
            "accepted {value}"
        );
    }
}

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
