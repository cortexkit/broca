use super::*;
use cortexkit_llm_tools::errors::{refusal_spend, RefusalSpend, SpendViolation};
use cortexkit_role_classifier::errors::ErrorClass;

/// What a vector's `expect` names, read as `refusal_spend`'s outcome.
fn expected(expect: &Value) -> Result<RefusalSpend, SpendViolation> {
    match (expect["spend"].as_str(), expect["error"].as_str()) {
        (Some("measured"), _) => Ok(RefusalSpend::Measured(
            expect["cost_usd"]
                .as_f64()
                .expect("a measured vector names its cost"),
        )),
        (Some("charge_ceiling"), _) => Ok(RefusalSpend::ChargeCeiling),
        (Some("no_spend"), _) => Ok(RefusalSpend::NoSpend),
        (Some("undeclared"), _) => Ok(RefusalSpend::Undeclared),
        (_, Some("both_present")) => Err(SpendViolation::BothPresent),
        (_, Some("unknown_not_true")) => Err(SpendViolation::UnknownNotTrue),
        (_, Some("no_spend_not_true")) => Err(SpendViolation::NoSpendNotTrue),
        (_, Some("invalid_cost")) => Err(SpendViolation::InvalidCost),
        (_, Some("unmeasured_with_spend")) => Err(SpendViolation::UnmeasuredWithSpend),
        other => panic!("unknown vector expectation {other:?}"),
    }
}

/// Every published refusal-spend vector decodes, re-encodes through broca's refusal encoder to
/// the same JSON, and reads back as the vector expects.
#[test]
fn refusal_spend_vectors_round_trip_through_the_encoder() {
    let vectors: Value =
        serde_json::from_str(cortexkit_llm_tools::vectors::REFUSAL_SPEND).expect("vectors parse");
    let cases = vectors["cases"].as_array().expect("vectors list cases");
    assert_eq!(cases.len(), 14, "the published file has 14 cases");
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let refusal: Refusal = serde_json::from_value(case["value"].clone())
            .unwrap_or_else(|error| panic!("{name}: the refusal decodes: {error}"));
        let encoded = crate::system_one::refusal_body(&refusal);
        let reencoded: Value = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            reencoded, case["value"],
            "{name}: encoding keeps the refusal"
        );
        let decoded: Refusal = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(
            refusal_spend(&decoded),
            expected(&case["expect"]),
            "{name}: the spend reads as the vector expects"
        );
    }
}

/// The marks broca writes produce the vectors' valid shapes byte for byte (as JSON values).
#[test]
fn marks_produce_the_published_valid_shapes() {
    let vectors: Value = serde_json::from_str(cortexkit_llm_tools::vectors::REFUSAL_SPEND).unwrap();
    let case = |name: &str| {
        vectors["cases"]
            .as_array()
            .unwrap()
            .iter()
            .find(|case| case["name"] == name)
            .unwrap()["value"]
            .clone()
    };
    let unmarked = |name: &str| {
        let mut value = case(name);
        for key in [COST_USD_KEY, COST_UNKNOWN_KEY, NO_SPEND_KEY] {
            value["detail"].as_object_mut().unwrap().remove(key);
        }
        serde_json::from_value::<Refusal>(value).unwrap()
    };
    for (name, spend) in [
        ("measured-provider-error", Spend::Measured(0.0042)),
        ("unknown-transient", Spend::Unknown),
        ("no-spend-predispatch", Spend::Nothing),
    ] {
        let marked = spend.mark(unmarked(name));
        assert_eq!(serde_json::to_value(&marked).unwrap(), case(name), "{name}");
        let body = spend.mark_body(serde_json::to_vec(&unmarked(name)).unwrap());
        assert_eq!(
            serde_json::from_slice::<Value>(&body).unwrap(),
            case(name),
            "{name}"
        );
    }
    // `cost_unmeasured` is its own mark: marking it never adds a member.
    for spend in [Spend::Nothing, Spend::Unknown, Spend::Measured(0.1)] {
        let marked = spend.mark(unmarked("unmeasured"));
        assert_eq!(serde_json::to_value(&marked).unwrap(), case("unmeasured"));
        assert_eq!(refusal_spend(&marked), Ok(RefusalSpend::ChargeCeiling));
        let body = serde_json::to_vec(&unmarked("unmeasured")).unwrap();
        assert_eq!(spend.mark_body(body.clone()), body);
    }
}

#[test]
fn a_mark_replaces_any_earlier_mark_and_never_doubles() {
    let refusal = Refusal::new("provider_error", "invented");
    for first in [Spend::Nothing, Spend::Unknown, Spend::Measured(0.5)] {
        for second in [Spend::Nothing, Spend::Unknown, Spend::Measured(0.25)] {
            let marked = second.mark(first.mark(refusal.clone()));
            let read = refusal_spend(&marked).expect("one mark only");
            let want = match second {
                Spend::Nothing => RefusalSpend::NoSpend,
                Spend::Unknown => RefusalSpend::ChargeCeiling,
                Spend::Measured(cost) => RefusalSpend::Measured(cost),
            };
            assert_eq!(read, want);
            let body: Refusal = serde_json::from_slice(
                &second.mark_body(serde_json::to_vec(&first.mark(refusal.clone())).unwrap()),
            )
            .unwrap();
            assert_eq!(refusal_spend(&body), Ok(want));
            // A refusal already marked keeps its mark.
            assert_eq!(
                refusal_spend(&second.mark_undeclared(first.mark(refusal.clone()))),
                refusal_spend(&first.mark(refusal.clone()))
            );
        }
    }
    assert_eq!(
        refusal_spend(&Spend::Nothing.mark_undeclared(refusal.clone())),
        Ok(RefusalSpend::NoSpend)
    );
}

#[test]
fn a_cost_the_contract_refuses_falls_back_to_unknown_never_free() {
    for cost in [-0.01, f64::NAN, f64::INFINITY] {
        let marked = Spend::Measured(cost).mark(Refusal::new("provider_error", "invented"));
        assert_eq!(refusal_spend(&marked), Ok(RefusalSpend::ChargeCeiling));
    }
}

/// A body built without a `detail` (a tool-provider `ErrorBody`) gains one holding the mark.
#[test]
fn a_body_without_detail_gains_one() {
    let body =
        serde_json::to_vec(&subc_protocol::ErrorBody::new("unknown_method", "invented")).unwrap();
    let marked: Value = serde_json::from_slice(&Spend::Nothing.mark_body(body)).unwrap();
    assert_eq!(marked["detail"][NO_SPEND_KEY], Value::Bool(true));
    assert_eq!(marked["code"], "unknown_method");
}

/// A marked item error stores and replays its mark; an unmarked one (stored before marks
/// existed, or a `classifier/v1` error) encodes exactly as the role's `ItemError` does.
#[test]
fn item_errors_keep_their_mark_and_unmarked_ones_keep_their_bytes() {
    let error = ItemError::for_code("invalid_item", "invented").with_retry_after_ms(5);
    let plain = serde_json::to_string(&error).unwrap();
    assert_eq!(
        serde_json::to_string(&MarkedItemError::new(error.clone(), None)).unwrap(),
        plain
    );
    let legacy: MarkedItemError = serde_json::from_str(&plain).unwrap();
    assert_eq!(
        refusal_spend(&legacy.clone().into_refusal()),
        Ok(RefusalSpend::Undeclared)
    );
    for (spend, want) in [
        (ItemSpend::Nothing, RefusalSpend::NoSpend),
        (ItemSpend::Unknown, RefusalSpend::ChargeCeiling),
    ] {
        let marked = MarkedItemError::new(error.clone(), Some(spend));
        let stored = serde_json::to_string(&marked).unwrap();
        let read: MarkedItemError = serde_json::from_str(&stored).unwrap();
        assert_eq!(read, marked);
        // The role's own decoder still reads the stored error.
        assert_eq!(serde_json::from_str::<ItemError>(&stored).unwrap(), error);
        let refusal = read.clone().into_refusal();
        assert_eq!(refusal_spend(&refusal), Ok(want));
        assert_eq!(refusal.detail.class, ErrorClass::Permanent);
        assert_eq!(refusal.detail.retry_after_ms, Some(5));
        assert_eq!(
            serde_json::to_string(&read.unmarked()).unwrap(),
            plain,
            "a classifier reply carries no mark"
        );
    }
    let unmeasured = ItemError::for_code(COST_UNMEASURED, "invented");
    let marked = MarkedItemError::new(unmeasured, Some(ItemSpend::Unknown));
    assert_eq!(
        refusal_spend(&marked.into_refusal()),
        Ok(RefusalSpend::ChargeCeiling)
    );
}

/// Every line in the model tools' sources that builds a refusal body, or a stored or unsent
/// item error a refusal is later built from, without going through a helper that requires a
/// spend: the file, a piece of the line, how many lines carry it, and the spend it states.
/// Refusals built as a `Refusal` cannot reach a caller unmarked: the route's `error` takes the
/// spend, and `SystemOne::execute_tool` marks every refusal it returns by whether a provider
/// request was sent. Refusals of the provider's own operations (`tool.catalog`, `late_results`
/// and `late_results.ack`) answer no model call and are listed as carrying no member. The
/// permissions route is an operator-only operation, not a model tool, so it is not listed.
const SITES: &[(&str, &str, usize, &str)] = &[
    (
        "route.rs",
        "to_vec(&error).expect(\"call refusal encodes\")",
        1,
        "no spend: mark_body",
    ),
    (
        "route.rs",
        "\"code\":\"tool_schema_changed\"",
        1,
        "no spend: literal no_spend",
    ),
    (
        "route.rs",
        "\"code\":\"tool_semantics_changed\"",
        1,
        "no spend: literal no_spend",
    ),
    (
        "route.rs",
        "to_vec(&subc_protocol::ErrorBody::new(",
        1,
        "no spend: mark_body (unknown_method)",
    ),
    (
        "route.rs",
        "ErrorBody::new(\"invalid_request\"",
        1,
        "no member: a tool.catalog refusal answers no model call",
    ),
    (
        "route.rs",
        "to_vec(&body).expect(\"catalog refusal encodes\")",
        1,
        "no member: the same tool.catalog body",
    ),
    (
        "route.rs",
        "refusal_body(&refusal)",
        1,
        "the route's `error`: its caller states the spend",
    ),
    ("route.rs", "to_value(composition)", 1, "not a refusal"),
    ("route.rs", "to_value(&pricing)", 1, "not a refusal"),
    (
        "route.rs",
        "Outcome::Failed(body) =>",
        1,
        "reads a stored outcome",
    ),
    (
        "held.rs",
        "result.body.as_bytes().to_vec()",
        1,
        "a stored final error, replayed with its own mark",
    ),
    (
        "held.rs",
        "to_value(Spend::Nothing.mark(",
        1,
        "no spend: not_started",
    ),
    (
        "held.rs",
        "refusal_body(&spend.mark(",
        1,
        "the held helper: its caller states the spend",
    ),
    (
        "held.rs",
        "refusal_body(&Spend::Nothing.mark(",
        1,
        "no spend: a store failure before any send",
    ),
    (
        "held.rs",
        "refusal_body(&Spend::Unknown.mark(",
        1,
        "unknown: a held result that was lost",
    ),
    (
        "held.rs",
        "refusal_body(&super::admission::refusal(code, message))",
        1,
        "no member: a late_results refusal answers no model call",
    ),
    (
        "held.rs",
        "refusal_body(&crate::system_one::internal_refusal(&format!(",
        1,
        "no member: a late_results store failure answers no model call",
    ),
    (
        "private_content.rs",
        "to_vec(&super::spend::Spend::Nothing.mark(refusal))",
        1,
        "no spend",
    ),
    (
        "private_content.rs",
        "to_vec(&ConsentRefusal {",
        1,
        "no spend: ConsentDetail.no_spend",
    ),
    (
        "classify.rs",
        "Slot::Unsent(MarkedItemError::new(",
        2,
        "marked item errors",
    ),
    (
        "classify.rs",
        "Outcome::Failed(stored_error_json(&error, mark(ItemSpend::Nothing)))",
        1,
        "no spend: over the ceiling, never sent",
    ),
    (
        "classify.rs",
        "Outcome::Failed(stored_error_json(&error, mark(spend)))",
        1,
        "the provider reply's spend",
    ),
    (
        "classify.rs",
        "Outcome::Failed(item_error_json(&ItemError::new(",
        1,
        "cost_unmeasured: the code is its mark",
    ),
    (
        "classify.rs",
        "Outcome::Failed(text) => serde_json::from_str::<ItemError>(text)",
        1,
        "reads a stored outcome",
    ),
    (
        "classify.rs",
        "Outcome::Failed(text) => {",
        1,
        "reads a stored outcome",
    ),
    (
        "classify.rs",
        "Outcome::Failed(_) =>",
        1,
        "counts a stored outcome",
    ),
    (
        "classify.rs",
        "Some(Slot::Unsent(error)) =>",
        1,
        "reads an unsent error",
    ),
];

/// What a line that builds a refusal body or an item error outside the helpers looks like.
const CONSTRUCTORS: &[&str] = &[
    "refusal_body(",
    "ErrorBody::new(",
    "\"code\":",
    "to_vec(&",
    "as_bytes().to_vec()",
    "to_value(",
    "Outcome::Failed(",
    "Slot::Unsent(",
];

/// A refusal construction site that is not in [`SITES`] fails here: add it with the spend it
/// states, and make the code state it.
#[test]
fn every_refusal_construction_site_is_listed_with_its_spend() {
    let sources = [
        ("route.rs", include_str!("route.rs")),
        ("held.rs", include_str!("held.rs")),
        ("held_images.rs", include_str!("held_images.rs")),
        ("private_content.rs", include_str!("private_content.rs")),
        ("admission.rs", include_str!("admission.rs")),
        ("consent.rs", include_str!("consent.rs")),
        ("mod.rs", include_str!("mod.rs")),
        ("classify.rs", include_str!("../system_one/classify.rs")),
    ];
    let mut seen = vec![0usize; SITES.len()];
    for (file, source) in sources {
        // Test modules sit after the first `#[cfg(test)]` at the start of a line.
        let code = source.split("\n#[cfg(test)]").next().unwrap();
        for (number, line) in code.lines().enumerate() {
            if !CONSTRUCTORS.iter().any(|pattern| line.contains(pattern)) {
                continue;
            }
            let matches: Vec<usize> = SITES
                .iter()
                .enumerate()
                .filter(|(_, (site_file, needle, _, _))| {
                    *site_file == file && line.contains(needle)
                })
                .map(|(index, _)| index)
                .collect();
            assert_eq!(
                matches.len(),
                1,
                "{file}:{}: a refusal construction site without a listed spend: {}",
                number + 1,
                line.trim()
            );
            seen[matches[0]] += 1;
        }
    }
    for ((file, needle, count, _), seen) in SITES.iter().zip(seen) {
        assert_eq!(
            seen, *count,
            "{file}: `{needle}` is listed {count} times but found {seen}"
        );
    }
}
