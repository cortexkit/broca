//! Pins the shape of fusiform's served payloads, so day-one drift is separable from a bug.
//!
//! Broca is cutting its model facts over to the fusiform module. On the first live fetch the
//! response will differ from the currently embedded snapshot in a large and entirely legitimate
//! way — measured 2026-08-13 against fusiform's live document: 183 vs 184 providers, +77 models,
//! −39 withdrawn, and 17 `limit.context` changes. Several of those are corrections toward exact
//! powers of two (128000→131072, 256000→262144) which read as noise and are not: context limit
//! is the prompt-reduction pressure signal, so a changed value changes what gets compacted,
//! which changes the prompt.
//!
//! That is the problem this fixture exists for. Without a pinned wire shape, the first diff mixes
//! "the source moved" with "the mapper is wrong", on the day that distinction is hardest to make
//! and most consequential to get wrong. This test fails when the SHAPE changes; the values are
//! expected to move and are not pinned here.
//!
//! Provenance, since a fixture nobody can re-derive is a fixture nobody can trust:
//!   source        ../fusiform/crates/fusiform-module/fixtures/served-payloads.json
//!   minted by     fusiform's real serve path over a real store (not hand-written)
//!   fusiform      6d88e52, fusiform-protocol 0.7.1
//!   vendored at   broca 250b968
//!
//! One row is different and fusiform flagged it: `synthetic/unknown-reasoning` is NOT cut from a
//! real upstream document, because no model in the live document omits a capability key — the
//! absent case does not occur upstream and therefore cannot be sampled from it. It is declared
//! synthetic in its own id so it cannot be mistaken for measured data. The alternative was
//! leaving the unknown-capability wire shape unpinned until the upstream produces one, which
//! means the first real occurrence is also the first test of that path.

use serde_json::Value;

const FIXTURE: &str = include_str!("../fixtures/fusiform-served-payloads.json");

/// Every capability fact must be able to arrive as JSON `null`, on the wire, not just in
/// fusiform's domain types.
///
/// This is the assertion the cutover turns on. `serde_json::Value::as_bool()` returns `None` for
/// a null exactly as it does for an absent key, so a parser written with `unwrap_or(false)`
/// converts "the producer said it does not know" into "the producer said false" — and for
/// `reasoning` that silently strips thinking blocks from a capable model.
///
/// Fusiform first told us only `reasoning` could arrive null, then built this fixture and found
/// their own wire contradicted it: all five capability facts are `Option` and all five serialize
/// as null. They had been describing the upstream document's behaviour, not their own contract.
/// So this test pins all five, and a future fixture refresh that quietly drops one of the null
/// arms fails here rather than in a parser six months later.
#[test]
fn every_capability_fact_can_arrive_null_on_the_wire() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("vendored fixture parses");
    let case = fixture
        .as_object()
        .expect("fixture is an object of named cases")
        .iter()
        .find(|(name, _)| name.contains("unknown"))
        .map(|(_, case)| case)
        .expect("fixture carries the unknown-capability case");

    let model = case
        .pointer("/response/models")
        .and_then(Value::as_object)
        .and_then(|models| models.values().next())
        .and_then(Value::as_object)
        .expect("the case carries exactly one model object");

    let capabilities = [
        "capability.reasoning",
        "capability.tool_call",
        "capability.attachment",
        "capability.input_modalities",
        "capability.output_modalities",
    ];

    for key in capabilities {
        let value = model
            .get(key)
            .unwrap_or_else(|| panic!("{key} is absent from the wire shape entirely"));
        assert!(
            value.is_null(),
            "{key} must be able to arrive as an explicit null: a parser cannot distinguish \
             unknown from false unless the producer can say unknown"
        );
    }
}

/// The keys broca actually reads must be present in the served shape.
///
/// Narrow on purpose: this asserts the fields on broca's serving path, not fusiform's full
/// vocabulary. Pinning fields we do not consume would make their fixture refreshes fail here for
/// no reason, and a test that fails for reasons unrelated to its subject gets weakened rather
/// than read.
///
/// `limit.output` is deliberately NOT in this list even though broca now reads it: it only
/// picks the default cap when a send sets none, a missing fact decodes as unknown and selects
/// the fallback, so its absence from a served row is not a shape break.
#[test]
fn the_served_shape_carries_the_facts_broca_consumes() {
    let fixture: Value = serde_json::from_str(FIXTURE).expect("vendored fixture parses");
    let cases = fixture.as_object().expect("fixture is an object of cases");

    let mut checked = 0usize;
    for (name, case) in cases {
        let Some(models) = case.pointer("/response/models").and_then(Value::as_object) else {
            continue;
        };
        for (model_id, model) in models {
            let model = model.as_object().expect("each model is an object");
            for key in ["existence", "limit.context", "capability.reasoning"] {
                assert!(
                    model.contains_key(key),
                    "{name}: {model_id} is missing {key}, which broca reads at admission"
                );
            }
            checked += 1;
        }
    }

    // A fixture that stopped carrying model rows would satisfy every assertion above by having
    // nothing to assert against. Refuse that rather than reporting a pass over nothing.
    assert!(
        checked > 0,
        "no model rows found in the fixture: the assertions above proved nothing"
    );
}
