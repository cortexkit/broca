use super::conversation::Conversation;
use super::operations::{Budget, OpenBatch, Operations, Run};
use super::{Bound, FieldSpec};

/// Every fold state, by name.
const STATES: &[(&str, &[FieldSpec])] = &[
    ("Conversation", Conversation::SCHEMA),
    ("Operations", Operations::SCHEMA),
    ("Run", Run::SCHEMA),
    ("Budget", Budget::SCHEMA),
    ("OpenBatch", OpenBatch::SCHEMA),
];

/// Each field bounded as `Declared`, with the state struct whose own fields carry the bounds.
const DECLARED: &[(&str, &str, &str)] =
    &[("Operations", "run", "Run"), ("Run", "budget", "Budget")];

#[test]
fn every_state_field_is_declared_once_with_an_owner_and_a_reset_rule() {
    for (state, schema) in STATES {
        assert!(!schema.is_empty(), "{state} declares no fields");
        for (index, field) in schema.iter().enumerate() {
            assert!(
                !field.owner.is_empty() && !field.reset.is_empty(),
                "{state}.{} has no owner or reset rule",
                field.name
            );
            assert!(
                schema[..index]
                    .iter()
                    .all(|earlier| earlier.name != field.name),
                "{state}.{} is declared twice",
                field.name
            );
        }
    }
}

/// A field bounded as `Declared` must name a state struct whose fields are declared, so a
/// growing collection can't hide inside a nested struct.
#[test]
fn declared_fields_point_at_declared_states() {
    for (state, schema) in STATES {
        for field in schema.iter().filter(|f| f.bound == Bound::Declared) {
            let child = DECLARED
                .iter()
                .find(|(parent, name, _)| parent == state && *name == field.name)
                .map(|(_, _, child)| *child)
                .unwrap_or_else(|| {
                    panic!("{state}.{} is Declared but maps to no state", field.name)
                });
            assert!(
                STATES.iter().any(|(name, _)| *name == child),
                "{state}.{} maps to {child}, which isn't a declared state",
                field.name
            );
        }
    }
    for (parent, name, _) in DECLARED {
        let (_, schema) = STATES.iter().find(|(state, _)| state == parent).unwrap();
        let field = schema.iter().find(|f| f.name == *name).unwrap();
        assert_eq!(field.bound, Bound::Declared, "{parent}.{name}");
    }
}

/// Only the conversation holds the window; operational state never holds content that grows
/// with the conversation.
#[test]
fn only_the_conversation_holds_the_window() {
    for (state, schema) in STATES {
        for field in schema.iter().filter(|f| f.bound == Bound::Window) {
            assert_eq!(
                *state, "Conversation",
                "{state}.{} holds the window",
                field.name
            );
        }
    }
}
