use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::CallOptions,
};
use broca_provider::families::{
    anthropic_frozen_config, openai_chat_frozen_config, openai_responses_chatgpt_frozen_config,
    OpenAiChat,
};
use broca_provider::wire::{
    CacheClass, CachePolicy, CacheTail, CacheTtl, FrozenRenderConfig, MarkerSyntax, WireFamily,
};
use serde_json::{json, Value};

const PRE_CACHE_FROZEN_CONFIG: &str =
    include_str!("../fixtures/frozen_render_config_pre_cache.json");

fn marker_policy() -> CachePolicy {
    CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::OneHour,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: false,
        },
    }
}

#[test]
fn old_frozen_config_without_cache_deserializes_with_no_policy() {
    let frozen: FrozenRenderConfig =
        serde_json::from_str(PRE_CACHE_FROZEN_CONFIG).expect("pre-cache config deserializes");

    assert_eq!(frozen.cache, None);
    assert_eq!(frozen.reasoning_effort, None);
    assert_eq!(frozen.service_tier, None);
}

#[test]
fn frozen_chatgpt_tier_controls_round_trip_equal() {
    let mut frozen = openai_responses_chatgpt_frozen_config("gpt-5.6-sol");
    frozen.reasoning_effort = Some("max".into());
    frozen.service_tier = Some("priority".into());

    let serialized = serde_json::to_string(&frozen).expect("serialize ChatGPT tier controls");
    let restored: FrozenRenderConfig =
        serde_json::from_str(&serialized).expect("deserialize ChatGPT tier controls");

    assert_eq!(restored, frozen);
}

#[test]
fn frozen_config_with_policy_round_trips_equal() {
    let mut frozen = anthropic_frozen_config("claude-opus-4-8");
    frozen.cache = Some(marker_policy());

    let serialized = serde_json::to_string(&frozen).expect("serialize policy config");
    let restored: FrozenRenderConfig =
        serde_json::from_str(&serialized).expect("deserialize policy config");

    assert_eq!(restored, frozen);
}

#[test]
fn caller_cache_scope_round_trips_and_renders_identically_on_resume() {
    let mut frozen = openai_chat_frozen_config("gpt-4.1");
    frozen.prompt_cache_key = Some("alfonso-classify-v1".into());
    let opts = CallOptions::new(vec![Message::new(
        Role::User,
        vec![ContentBlock::Text {
            text: "classify this request".into(),
        }],
    )]);
    let first = OpenAiChat.render(&frozen, &opts);

    let restored: FrozenRenderConfig =
        serde_json::from_slice(&serde_json::to_vec(&frozen).unwrap()).unwrap();
    let resumed = OpenAiChat.render(&restored, &opts);

    assert_eq!(
        resumed, first,
        "the frozen caller scope must survive a resume"
    );
}

/// The tail declaration is additive: a policy that carries none must serialize to the exact
/// bytes it did before the field existed, and survive a decode/re-encode unchanged. Anything
/// else would move every already-frozen config's bytes, which is the one thing this field must
/// not do.
#[test]
fn cache_policy_tail_defaults_to_none_and_preserves_pre_field_bytes() {
    let pre_field_bytes = serde_json::to_vec(&marker_policy()).unwrap();
    let pre_field_value: Value = serde_json::from_slice(&pre_field_bytes).unwrap();
    assert!(
        pre_field_value.get("tail").is_none(),
        "a policy with no tail declaration must retain the pre-field frozen shape"
    );

    let decoded: CachePolicy = serde_json::from_slice(&pre_field_bytes).unwrap();
    assert_eq!(decoded, marker_policy());
    assert_eq!(
        serde_json::to_vec(&decoded).unwrap(),
        pre_field_bytes,
        "decoding and re-encoding a pre-field policy must preserve its bytes"
    );
}

#[test]
fn declared_volatile_tail_survives_the_frozen_round_trip() {
    let mut policy = marker_policy();
    let CacheClass::Markers { ref mut tail, .. } = policy.class else {
        panic!("the marker fixture is a markers policy");
    };
    *tail = Some(CacheTail::Volatile);

    let value = serde_json::to_value(&policy).expect("serialize policy");
    assert_eq!(value.get("tail"), Some(&json!("volatile")));
    let restored: CachePolicy = serde_json::from_value(value).expect("deserialize policy");
    assert_eq!(restored, policy);
}

#[test]
fn cache_policy_deserialization_accepts_future_fields() {
    let policy: CachePolicy = serde_json::from_value(json!({
        "version": 1,
        "class": "markers",
        "syntax": "anthropic_cache_control",
        "ttl": "1h",
        "max_breakpoints": 4,
        "lookback_blocks": 20,
        "future_top_level": "ignored",
        "future_object": {"nested": true}
    }))
    .expect("future policy fields are additive");

    assert_eq!(policy, marker_policy());
}

/// `omit_ttl` is additive like `tail`: an explicit policy (false) keeps the frozen shape it had
/// before the field existed, and a recorded policy without the field reads back as false, so a
/// run frozen earlier still writes `ttl` on its markers after a resume.
#[test]
fn omit_ttl_is_absent_when_false_and_round_trips_when_true() {
    let explicit = serde_json::to_value(marker_policy()).unwrap();
    assert!(explicit.get("omit_ttl").is_none());

    let mut default = marker_policy();
    let CacheClass::Markers {
        ref mut omit_ttl,
        ref mut ttl,
        ..
    } = default.class
    else {
        panic!("the marker fixture is a markers policy");
    };
    *omit_ttl = true;
    *ttl = CacheTtl::FiveMinutes;
    let value = serde_json::to_value(&default).unwrap();
    assert_eq!(value.get("omit_ttl"), Some(&json!(true)));
    assert_eq!(
        serde_json::from_value::<CachePolicy>(value).unwrap(),
        default
    );
}
