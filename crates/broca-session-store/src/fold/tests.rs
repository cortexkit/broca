use super::conversation::Conversation;
use super::media::{Captured, MediaFold, MediaIndexRef};
use super::operations::{
    Budget, EffectiveChange, Enrollment, ManifestRef, ModelTotals, OpenBatch, Operations, Owner,
    PendingChange, Plan, Run, SessionUsage, SetupInForce, UsageTotals,
};
use super::{Bound, FieldSpec};

/// Every fold state, by name.
const STATES: &[(&str, &[FieldSpec])] = &[
    ("Conversation", Conversation::SCHEMA),
    ("Operations", Operations::SCHEMA),
    ("Run", Run::SCHEMA),
    ("Budget", Budget::SCHEMA),
    ("OpenBatch", OpenBatch::SCHEMA),
    ("SetupInForce", SetupInForce::SCHEMA),
    ("Owner", Owner::SCHEMA),
    ("SessionUsage", SessionUsage::SCHEMA),
    ("UsageTotals", UsageTotals::SCHEMA),
    ("ModelTotals", ModelTotals::SCHEMA),
    ("Plan", Plan::SCHEMA),
    ("PendingChange", PendingChange::SCHEMA),
    ("EffectiveChange", EffectiveChange::SCHEMA),
    ("ManifestRef", ManifestRef::SCHEMA),
    ("Enrollment", Enrollment::SCHEMA),
    ("MediaFold", MediaFold::SCHEMA),
    ("Captured", Captured::SCHEMA),
    ("MediaIndexRef", MediaIndexRef::SCHEMA),
];

/// Each field bounded as `Declared`, with the state struct whose own fields carry the bounds.
const DECLARED: &[(&str, &str, &str)] = &[
    ("Operations", "run", "Run"),
    ("Operations", "setup", "SetupInForce"),
    ("Operations", "owner", "Owner"),
    ("Operations", "session_usage", "SessionUsage"),
    ("Operations", "plan", "Plan"),
    ("Operations", "enrolled", "Enrollment"),
    ("Plan", "pending", "PendingChange"),
    ("Plan", "effective", "EffectiveChange"),
    ("Plan", "current", "ManifestRef"),
    ("Plan", "prefix", "ManifestRef"),
    ("Plan", "accepted", "ManifestRef"),
    ("PendingChange", "manifest", "ManifestRef"),
    ("SessionUsage", "totals", "UsageTotals"),
    ("ModelTotals", "totals", "UsageTotals"),
    ("Run", "budget", "Budget"),
    ("MediaFold", "captured", "Captured"),
    ("MediaIndexRef", "fold", "MediaFold"),
];

#[test]
fn checkpoint_source_lineage_is_optional_but_must_be_understood_when_present() {
    use crate::codec::{skip_unknown, Fields, Message as _};
    // The previous operational reader knows fields through 26; it skips later optional fields
    // and refuses later must-understand fields by the same codec rule used on checkpoint load.
    let old_reader = |bytes: &[u8]| -> Result<(), crate::codec::DecodeError> {
        for field in Fields::new(bytes, "old Operations") {
            let field = field?;
            if field.number > 26 {
                skip_unknown(field, "old Operations")?;
            }
        }
        Ok(())
    };
    let old = Operations::default();
    let bytes = old.encode();
    old_reader(&bytes).unwrap();
    assert_eq!(Operations::decode(&bytes).unwrap().encode(), old.encode());
    let moved = Operations {
        source_lineage: Some("0190a6b2-0000-7000-8000-000000000001".into()),
        ..old
    };
    let bytes = moved.encode();
    assert!(matches!(
        old_reader(&bytes).unwrap_err().kind,
        crate::codec::DecodeErrorKind::MustUnderstand { field: 27 }
    ));
    assert_eq!(Operations::decode(&bytes).unwrap().encode(), moved.encode());
}

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
