//! Tool schemas on routes served by Moonshot's API (the Kimi routes). That API refuses a tool
//! whose schema has an `enum` node without a `type` ("not a valid moonshot flavored json
//! schema ... type is not defined"), so a config frozen for such a route carries
//! `moonshot_schema_types` and the OpenAI Chat render adds the missing types. Every other
//! route, and a Kimi run frozen before the flag existed, renders exactly the bytes it did.

use broca_protocol::{request::Tool, CallOptions};
use broca_provider::{
    families::{
        openai_chat_frozen_config, openai_chat_renderer_version, OpenAiChat,
        OPENAI_CHAT_RENDERER_VERSION,
    },
    policy::ReasoningEchoPolicy,
    spec::WireFamilyId,
    wire::{FrozenRenderConfig, WireFamily},
};
use serde_json::{json, Value};

use crate::renderer_version_reproduction::openai_chat_v4_call_options;

/// Two cases pinned by the code before the flag existed (OpenAI Chat v5), both offering a
/// tool shaped like a federation call tool (`surface` and `op` string enums without a `type`,
/// plus an enum in array `items` and in `anyOf` branches): the first on an ordinary route, the
/// second on a Kimi route.
fn pins() -> Vec<Value> {
    serde_json::from_str(include_str!("../fixtures/openai_chat_v5_enum_schemas.json"))
        .expect("pins are JSON")
}

fn request(pin: &Value) -> CallOptions {
    let mut opts = openai_chat_v4_call_options();
    opts.tools = serde_json::from_value::<Vec<Tool>>(pin["tools"].clone())
        .expect("pinned tools decode")
        .into();
    opts
}

fn render(frozen: &FrozenRenderConfig, opts: &CallOptions) -> String {
    String::from_utf8(OpenAiChat.render(frozen, opts)).expect("rendered bytes are UTF-8")
}

/// The pinned Kimi config as a run started on this build freezes it.
fn current_kimi_config(pin: &Value) -> FrozenRenderConfig {
    let mut frozen: FrozenRenderConfig =
        serde_json::from_value(pin["frozen"].clone()).expect("pinned config decodes");
    frozen.moonshot_schema_types = true;
    frozen.renderer_version = openai_chat_renderer_version(true);
    frozen
}

#[test]
fn a_kimi_route_types_the_enum_properties_of_tool_schemas() {
    let pins = pins();
    let opts = request(&pins[1]);
    let body: Value = serde_json::from_str(&render(&current_kimi_config(&pins[1]), &opts)).unwrap();
    let parameters = &body["tools"][0]["function"]["parameters"];
    let properties = &parameters["properties"];
    assert_eq!(
        properties["surface"],
        json!({"enum": ["tool", "management"], "type": "string"})
    );
    assert_eq!(
        properties["op"],
        json!({"enum": ["call", "status", "cancel"], "type": "string"})
    );
    assert_eq!(properties["targets"]["items"]["type"], "string");
    assert_eq!(properties["priority"]["anyOf"][0]["type"], "integer");
    assert_eq!(properties["priority"]["anyOf"][1]["type"], "string");
    // A node that already declares its type, and the keywords around the enums, are unchanged.
    assert_eq!(
        properties["module"],
        json!({"type": "string", "minLength": 1})
    );
    assert_eq!(parameters["additionalProperties"], json!(false));
    assert_eq!(
        parameters["required"],
        json!(["machine_fingerprint", "surface", "module"])
    );

    // Only the schema changes: with the declared schema put back, the body is the v5 bytes.
    let mut restored = body.clone();
    restored["tools"][0]["function"]["parameters"] = opts.tools[0].input_schema.clone();
    assert_eq!(
        serde_json::to_string(&restored).unwrap(),
        pins[1]["body"].as_str().unwrap()
    );
    // The stored tool keeps its declared schema.
    assert!(opts.tools[0].input_schema["properties"]["surface"]
        .get("type")
        .is_none());
}

#[test]
fn a_route_without_the_flag_renders_its_tool_schemas_as_declared() {
    let pins = pins();
    let frozen = openai_chat_frozen_config("gpt-4o");
    // Without the flag an OpenAI Chat config still freezes v5, the version before the flag.
    assert_eq!(frozen.renderer_version, 5);
    assert_eq!(openai_chat_renderer_version(false), 5);
    assert!(!frozen.moonshot_schema_types);
    assert_eq!(
        render(&frozen, &request(&pins[0])),
        pins[0]["body"].as_str().unwrap(),
        "a non-Kimi route must render the bytes it rendered before the flag existed"
    );
}

#[test]
fn a_config_without_the_flag_records_no_new_member() {
    let frozen = openai_chat_frozen_config("gpt-4o");
    let recorded = serde_json::to_value(&frozen).unwrap();
    assert!(
        recorded.get("moonshot_schema_types").is_none(),
        "an unset flag must be omitted, so a non-Kimi run records the config it did before"
    );
    let pins = pins();
    let kimi_v5: FrozenRenderConfig = serde_json::from_value(pins[1]["frozen"].clone()).unwrap();
    assert!(!kimi_v5.moonshot_schema_types);
    assert_eq!(serde_json::to_value(&kimi_v5).unwrap(), pins[1]["frozen"]);

    let flagged = current_kimi_config(&pins[1]);
    assert_eq!(
        serde_json::to_value(&flagged).unwrap()["moonshot_schema_types"],
        json!(true)
    );
}

#[test]
fn a_kimi_run_frozen_at_v5_renders_its_recorded_bytes() {
    let pins = pins();
    let mut frozen: FrozenRenderConfig = serde_json::from_value(pins[1]["frozen"].clone()).unwrap();
    assert_eq!(frozen.family_id, WireFamilyId::OpenAiChat);
    assert_eq!(frozen.renderer_version, 5);
    assert!(broca_provider::families::reproduces_renderer_version(
        WireFamilyId::OpenAiChat,
        5
    ));
    let opts = request(&pins[1]);
    assert_eq!(render(&frozen, &opts), pins[1]["body"].as_str().unwrap());
    // Rendered as if stamped with today's version, the config still lacks the flag, so the
    // bytes are the same.
    frozen.renderer_version = OPENAI_CHAT_RENDERER_VERSION;
    assert_eq!(render(&frozen, &opts), pins[1]["body"].as_str().unwrap());
}

/// The start record of a Kimi run begun on this build: v6, with the flag, in both the run's
/// renderer version and its frozen render config. Its counterpart for an ordinary OpenAI Chat
/// run, which stays at v5 without the member, is `ordinary_run_started_bytes_stay_pinned`.
#[test]
fn a_kimi_run_started_bytes_record_v6_and_the_flag() {
    let mut frozen = openai_chat_frozen_config("k3.1-pro");
    frozen.base_url = Some("https://api.kimi.ai/coding/v1".into());
    frozen.reasoning_echo = Some(ReasoningEchoPolicy::KIMI);
    frozen.moonshot_schema_types = true;
    frozen.renderer_version = openai_chat_renderer_version(frozen.moonshot_schema_types);
    // A run start stores the frozen render config as a JSON value, whose keys serialize sorted.
    assert_eq!(
        serde_json::to_value(&frozen).unwrap().to_string(),
        r#"{"base_url":"https://api.kimi.ai/coding/v1","family_id":"openai_chat","max_output_tokens":2048,"moonshot_schema_types":true,"reasoning":{"mode":"none"},"reasoning_echo":{"format":"reasoning_content","turns":"every_assistant"},"renderer_version":6,"system_prompt":{"mode":"system_message"},"token_limit_field":"max_tokens","tool_id":"passthrough","usage":"open_ai_usage","wire_model_id":"k3.1-pro"}"#
    );
}
