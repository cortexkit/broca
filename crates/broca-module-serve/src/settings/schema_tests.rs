//! broca's settings schema is one the daemon's settings contract accepts, and its defaults
//! are the defaults broca's readers already use.

use serde_json::{json, Value};
use subc_config::{Layer, Schema, ValidationContext};

use super::{Settings, SCHEMA};
use crate::config::{
    AnthropicFallbackConfig, ImportConfig, ServiceTierConfig, SessionStorageConfig,
    StorageSyncConfig,
};

fn schema() -> Schema {
    Schema::new(serde_json::from_str(SCHEMA).unwrap())
        .unwrap_or_else(|refusal| panic!("the contract refuses the schema: {refusal:?}"))
}

fn context() -> ValidationContext {
    ValidationContext::new("broca", None)
}

/// The keys whose shape or meaning the schema changes: today's readers can't read their schema
/// defaults, or read them as something else.
const NEW_SHAPES: [&str; 7] = [
    "account_selection",
    "import",
    "max_same_account_wait",
    "tool_name_aliases",
    "cache",
    "billing",
    "context_management",
];

#[test]
fn the_schema_defaults_are_the_readers_defaults() {
    let mut defaults = schema().defaults();
    assert_eq!(defaults["session_storage"], json!({"encryption": "on"}));
    let object = defaults.as_object_mut().unwrap();
    for key in NEW_SHAPES {
        object
            .remove(key)
            .unwrap_or_else(|| panic!("{key} has a default"));
    }
    let from_schema = Settings::parse(&defaults.to_string()).unwrap();
    let empty = Settings::default();
    macro_rules! same {
        ($($reader:ty),+ $(,)?) => {$(
            assert_eq!(
                <$reader>::from_settings(&from_schema).unwrap(),
                <$reader>::from_settings(&empty).unwrap(),
                stringify!($reader),
            );
        )+};
    }
    same!(
        StorageSyncConfig,
        ServiceTierConfig,
        SessionStorageConfig,
        AnthropicFallbackConfig,
        crate::bus_events::Config,
        crate::session_class::BackupClasses,
    );
    let providers = |settings| {
        format!(
            "{:?}",
            crate::provider_overrides::ProviderOverrides::from_settings(settings).unwrap()
        )
    };
    assert_eq!(providers(&from_schema), providers(&empty));
    let tools = |settings| crate::config::LlmToolsConfig::from_settings(settings).unwrap();
    let (schema_tools, empty_tools) = (tools(&from_schema), tools(&empty));
    assert_eq!(
        (
            schema_tools.advertise_tags,
            schema_tools.image_references,
            schema_tools.private_content_providers
        ),
        (
            empty_tools.advertise_tags,
            empty_tools.image_references,
            empty_tools.private_content_providers
        ),
    );
}

#[test]
fn the_new_shapes_default_to_what_broca_does_today() {
    let defaults = schema().defaults();
    assert_eq!(
        defaults["max_same_account_wait"],
        json!({"kind": "minutes", "minutes": 15})
    );
    assert_eq!(
        broca_core::SameAccountWait::default(),
        broca_core::SameAccountWait::Minutes(15)
    );
    let shipped: Value = crate::refusal_fallback_config::DEFAULT_MAPPINGS
        .iter()
        .map(|(source, fallback)| ((*source).to_owned(), json!(fallback)))
        .collect::<serde_json::Map<_, _>>()
        .into();
    assert_eq!(
        defaults["anthropic_fallbacks"]["refusal_resend"]["models"],
        shipped
    );
    assert_eq!(
        defaults["context_management"],
        json!({"default": {"kind": "none"}, "projects": {}})
    );
    for key in ["tool_name_aliases", "providers"] {
        assert_eq!(defaults[key], json!({}), "{key}");
    }
    assert_eq!(defaults["cache"], json!({"provider_ttl": {}}));
    // Empty roots will mean broca's own import folder, as an absent list does today. Today an
    // empty list means no folder at all.
    assert_eq!(defaults["import"], json!({"roots": []}));
    // An empty routing module will mean: find the one routing provider, as an absent one does
    // today. Today an empty name is refused.
    assert_eq!(defaults["account_selection"], json!({"routing_module": ""}));
    assert_eq!(
        crate::account_routing::AccountSelectionConfig::from_settings(&Settings::default())
            .unwrap()
            .routing_module,
        None
    );
    assert_eq!(
        ImportConfig::from_settings(&Settings::default()).unwrap(),
        ImportConfig { roots: None }
    );
    assert_eq!(defaults["billing"], json!({"charge_basis": {}}));
}

#[test]
fn a_file_in_todays_unchanged_shapes_is_a_valid_user_layer() {
    let file = json!({
        "bus_run_events_enabled": true,
        "service_tier": {"enabled": true},
        "storage_sync": {"full_flush": false},
        "llm_tools": {"advertise_tags": true, "image_references": true},
        "backup_classes": ["head", "worker"],
        "providers": {"typesafe": {"base_url": "http://127.0.0.1:9/v1"}},
        "anthropic_fallbacks": {"refusal_resend": {"enabled": true}},
        "session_storage": {"deployment_key_id": 1, "encryption": "on"},
    });
    schema()
        .validate_layer(Layer::User, &file, &context())
        .unwrap();
    for refused in [
        json!({"storage_sync": {"full_flush": true, "extra": 1}}),
        json!({"no_such_key": true}),
        json!({"max_same_account_wait": "max"}),
        json!({"session_storage": {"encryption": "maybe"}}),
    ] {
        assert!(
            schema()
                .validate_layer(Layer::User, &refused, &context())
                .is_err(),
            "{refused}"
        );
    }
}

#[test]
fn every_new_shape_takes_each_of_its_forms() {
    let file = json!({
        "max_same_account_wait": {"kind": "max"},
        "tool_name_aliases": {
            "oauth:anthropic": {"kind": "names", "names": {"read": "Read"}},
            "chatgpt:openai": {"kind": "prefix_all", "prefix": "mcp_"},
        },
        "cache": {"provider_ttl": {
            "anthropic": {"kind": "markers", "primary": "1h", "ephemeral": "5m"},
            "openai": {"kind": "retention", "retention": "24h"},
        }},
        "billing": {"charge_basis": {"anthropic": {"oauth": "subscription_included"}}},
        "context_management": {"projects": {"/synthetic": {"kind": "none"}}},
    });
    schema()
        .validate_layer(Layer::User, &file, &context())
        .unwrap();
}

#[test]
fn only_the_user_layer_may_set_broca() {
    let file = json!({"bus_run_events_enabled": true});
    for layer in [Layer::Workspace, Layer::Project] {
        assert!(schema().validate_layer(layer, &file, &context()).is_err());
    }
}
