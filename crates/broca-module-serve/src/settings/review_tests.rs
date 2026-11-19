use serde_json::{json, Value};
use subc_config::{RefusalCode, Schema};

use super::{review, READERS, SCHEMA_ONLY};

fn schema() -> Schema {
    Schema::new(serde_json::from_str(super::super::SCHEMA).unwrap()).unwrap()
}

/// A key the schema gains without a reader here would reach broca unreviewed.
#[test]
fn every_schema_key_has_one_reviewer() {
    let schema = schema();
    let mut keys: Vec<&str> = schema.normalized()["properties"]
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    keys.sort_unstable();
    let mut reviewed: Vec<&str> = READERS
        .iter()
        .flat_map(|reader| reader.keys.iter().copied())
        .chain(SCHEMA_ONLY.iter().copied())
        .collect();
    reviewed.sort_unstable();
    assert_eq!(reviewed, keys);
}

#[test]
fn the_defaults_pass_review() {
    let tuple = review(&schema(), &json!({})).unwrap();
    assert_eq!(tuple.effective, schema().defaults());
}

#[test]
fn a_view_a_reader_refuses_is_refused_at_its_key() {
    let refusal = review(
        &schema(),
        &json!({"account_selection": {"routing_module": "   "}}),
    )
    .unwrap_err();
    assert_eq!(refusal.code, RefusalCode::ValueInvalid);
    assert_eq!(refusal.path, "/account_selection");
}

#[test]
fn a_reader_reading_two_keys_names_the_one_the_view_sets() {
    let providers: Vec<Value> = (0..17).map(|n| json!(format!("p{n}"))).collect();
    let refusal = review(&schema(), &json!({"private_content_providers": providers}));
    // Whether the schema or the reader refuses, the refusal names the key the view set.
    assert_eq!(refusal.unwrap_err().path, "/private_content_providers");
}

#[test]
fn a_schema_refusal_passes_through() {
    let view = json!({"no_such_key": true});
    let context = subc_config::ValidationContext::new("broca", None);
    assert_eq!(
        review(&schema(), &view).unwrap_err(),
        schema()
            .validate_layer(subc_config::Layer::User, &view, &context)
            .unwrap_err()
    );
}

#[test]
fn a_valid_view_passes_with_its_sources() {
    let tuple = review(
        &schema(),
        &json!({"service_tier": {"enabled": true}, "chatgpt_force_https": true}),
    )
    .unwrap();
    assert_eq!(tuple.effective["service_tier"]["enabled"], json!(true));
    assert!(tuple.sources.iter().any(
        |(path, layer)| path.starts_with("/service_tier") && *layer == subc_config::Layer::User
    ));
}
