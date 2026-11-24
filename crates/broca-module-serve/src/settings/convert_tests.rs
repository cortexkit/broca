//! Every shape broca has accepted converts to the schema's shape, and a converted document
//! converts to itself.

use serde_json::{json, Map, Value};
use subc_config::{Layer, Schema, ValidationContext};

use super::convert;

fn object(value: Value) -> Map<String, Value> {
    value.as_object().unwrap().clone()
}

/// `old` converts to `new`, and `new` converts to itself with no notes.
fn converts(old: Value, new: Value) {
    let converted = convert(&object(old.clone())).unwrap();
    assert_eq!(Value::Object(converted.document.clone()), new, "{old}");
    let again = convert(&converted.document).unwrap();
    assert_eq!(again.document, converted.document, "{new}");
    assert!(again.notes.is_empty(), "{:?}", again.notes);
    let schema = Schema::new(serde_json::from_str(super::super::SCHEMA).unwrap()).unwrap();
    schema
        .validate_layer(Layer::User, &new, &ValidationContext::new("broca", None))
        .unwrap_or_else(|refusal| panic!("{new} is refused: {refusal:?}"));
}

#[test]
fn the_same_account_wait_takes_its_tagged_form() {
    converts(
        json!({"max_same_account_wait": 30}),
        json!({"max_same_account_wait": {"kind": "minutes", "minutes": 30}}),
    );
    converts(
        json!({"max_same_account_wait": "max"}),
        json!({"max_same_account_wait": {"kind": "max"}}),
    );
    assert!(convert(&object(json!({"max_same_account_wait": "forever"}))).is_err());
}

#[test]
fn tool_name_aliases_take_their_tagged_form() {
    converts(
        json!({"tool_name_aliases": {
            "oauth:anthropic": {"names": {"read": "Read"}},
            "chatgpt:openai": {"prefix_all": "mcp_"},
        }}),
        json!({"tool_name_aliases": {
            "oauth:anthropic": {"kind": "names", "names": {"read": "Read"}},
            "chatgpt:openai": {"kind": "prefix_all", "prefix": "mcp_"},
        }}),
    );
    for refused in [
        json!({"tool_name_aliases": {"r": {"names": {}, "prefix_all": "x"}}}),
        json!({"tool_name_aliases": {"r": {"other": 1}}}),
    ] {
        assert!(convert(&object(refused.clone())).is_err(), "{refused}");
    }
}

#[test]
fn cache_rows_take_their_tagged_form() {
    converts(
        json!({"cache": {"provider_ttl": {
            "anthropic": {"primary": "1h", "ephemeral": "5m"},
            "openai": {"retention": "24h"},
            "google": {},
        }}}),
        json!({"cache": {"provider_ttl": {
            "anthropic": {"kind": "markers", "primary": "1h", "ephemeral": "5m"},
            "openai": {"kind": "retention", "retention": "24h"},
            "google": {"kind": "retention", "retention": ""},
        }}}),
    );
    assert!(convert(&object(
        json!({"cache": {"provider_ttl": {"a": {"primary": "1h", "retention": "x"}}}})
    ))
    .is_err());
}

#[test]
fn every_charge_basis_spelling_lands_under_provider_then_method() {
    converts(
        json!({
            "billing": {
                "version": 3,
                "charge_basis": {
                    "anthropic": {"oauth": "subscription_included"},
                    "apikey:openai": "metered_api",
                },
                "charge_basis_mapping": {"google:antigravity": "subscription_included"},
            },
            "charge_basis": {"openai:chatgpt": "subscription_included"},
            "charge_basis_mapping_version": 2,
        }),
        json!({"billing": {"charge_basis": {
            "anthropic": {"oauth": "subscription_included"},
            "google": {"antigravity": "subscription_included"},
            "openai": {"apikey": "metered_api", "chatgpt": "subscription_included"},
        }}}),
    );
    // A root row replaces a billing row with the same key.
    let converted = convert(&object(json!({
        "billing": {"charge_basis": {"apikey:openai": "metered_api"}},
        "charge_basis": {"apikey:openai": "unknown"},
    })))
    .unwrap();
    assert_eq!(
        converted.document["billing"],
        json!({"charge_basis": {"openai": {"apikey": "unknown"}}})
    );
    for refused in [
        json!({"charge_basis": {"openai:nosuch": "metered_api"}}),
        json!({"billing": {"charge_basis": {"anthropic": {"nosuch": "unknown"}}}}),
        json!({"billing": {"other": 1}}),
    ] {
        assert!(convert(&object(refused.clone())).is_err(), "{refused}");
    }
}

#[test]
fn context_management_takes_its_tagged_form() {
    converts(
        json!({"context_management": {
            "default": "none",
            "projects": {"/synthetic": "none"},
        }}),
        json!({"context_management": {
            "default": {"kind": "none"},
            "projects": {"/synthetic": {"kind": "none"}},
        }}),
    );
    let converted = convert(&object(json!({"context_management": {
        "projects": {"/a": "compaction", "/b": {"transform": "ck-mc"}},
    }})))
    .unwrap();
    assert_eq!(
        converted.document["context_management"]["projects"],
        json!({"/a": {"kind": "compaction"}, "/b": {"kind": "transform", "provider": "ck-mc"}})
    );
}

#[test]
fn removed_keys_go_with_a_note_each() {
    let converted = convert(&object(json!({
        "vault": {"module": "claustrum"},
        "media": {"enabled": true},
        "wal_archive_idle_retention_days": 30,
        "session_conversion": {"enabled": false},
        "account_limit_pause": true,
        "compaction_signals": false,
        "bus_run_events_enabled": true,
    })))
    .unwrap();
    assert_eq!(
        Value::Object(converted.document),
        json!({"bus_run_events_enabled": true})
    );
    assert_eq!(converted.notes.len(), 6, "{:?}", converted.notes);
}

#[test]
fn removed_classifier_routing_is_rewritten_with_a_notice() {
    let (text, notes) = super::super::rewrite::rewritten_text(
        r#"{
            // Settings from a build that let operators disable decision routing.
            "classifier_routing": {"enabled": false},
            "bus_run_events_enabled": true,
        }"#,
    )
    .unwrap()
    .expect("the old settings need conversion");
    assert_eq!(
        notes,
        ["removed classifier_routing: decision requests without a model always use routing"]
    );
    assert!(super::super::rewrite::rewritten_text(&text)
        .unwrap()
        .is_none());
    let settings = super::super::Settings::parse(&text).unwrap();
    assert_eq!(
        settings.keys().collect::<Vec<_>>(),
        ["bus_run_events_enabled"]
    );
    let document = serde_json::from_str(settings.text()).unwrap();
    super::super::schema()
        .validate_layer(
            Layer::User,
            &document,
            &ValidationContext::new("broca", None),
        )
        .unwrap();
}

#[test]
fn import_roots_pass_through() {
    converts(
        json!({"import": {"roots": []}}),
        json!({"import": {"roots": []}}),
    );
    converts(
        json!({"import": {"roots": ["/srv/heads"]}}),
        json!({"import": {"roots": ["/srv/heads"]}}),
    );
}

#[test]
fn keys_in_unchanged_shapes_pass_through() {
    let file = json!({
        "bus_run_events_enabled": true,
        "service_tier": {"enabled": true},
        "llm_tools": {"advertise_tags": true},
        "anthropic_fallbacks": {"refusal_resend": {"enabled": true}},
        "providers": {"typesafe": {"base_url": "http://127.0.0.1:9/v1"}},
    });
    converts(file.clone(), file);
}

/// A file shaped like the live config files at the time the schema was written: removed keys
/// beside keys whose shapes stay and one whose shape changed.
#[test]
fn a_file_in_the_shapes_in_use_converts_and_validates() {
    let converted = convert(&object(json!({
        "anthropic_fallbacks": {"server_side": true, "refusal_resend": {"enabled": true}},
        "bus_run_events_enabled": true,
        "llm_tools": {"advertise_tags": true, "image_references": true},
        "service_tier": {"enabled": true},
        "session_conversion": {"archives": false, "live_at_startup": false},
        "tool_name_aliases": {"oauth:example": {"prefix_all": "client_"}},
        "vault": {"credential_ids": {"apikey:example-route": "apikey:example"}},
        "wal_archive_idle_retention_days": 7,
    })))
    .unwrap();
    assert_eq!(
        Value::Object(converted.document.clone()),
        json!({
            "anthropic_fallbacks": {"server_side": true, "refusal_resend": {"enabled": true}},
            "bus_run_events_enabled": true,
            "llm_tools": {"advertise_tags": true, "image_references": true},
            "service_tier": {"enabled": true},
            "tool_name_aliases": {"oauth:example": {"kind": "prefix_all", "prefix": "client_"}},
        })
    );
    assert_eq!(converted.notes.len(), 4, "{:?}", converted.notes);
    let schema = Schema::new(serde_json::from_str(super::super::SCHEMA).unwrap()).unwrap();
    schema
        .validate_layer(
            Layer::User,
            &Value::Object(converted.document),
            &ValidationContext::new("broca", None),
        )
        .unwrap();
}
