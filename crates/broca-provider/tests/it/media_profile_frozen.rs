//! The media profile is an optional frozen render field: absent configs keep their exact
//! serialized shape and request bytes, a recorded profile requires the media-content/v1 WAL feature,
//! and the profile a run froze is the one it keeps while the catalog moves underneath it.

use broca_protocol::{
    content::{ContentBlock, MediaKind, Message, Role},
    request::CallOptions,
    ModelHandle,
};
use broca_provider::families::{frozen_config_for_family, resolve_frozen};
use broca_provider::media_profile::{freeze_media_profile, MediaProfileId};
use broca_provider::registry::wire_family;
use broca_provider::spec::{AuthKind, InputModalities, ModelSpec, ProviderSpec, WireFamilyId};
use broca_provider::wire::FrozenRenderConfig;
use broca_wal::WalRecord;
use serde_json::{json, Value};

const FAMILIES: [WireFamilyId; 7] = [
    WireFamilyId::AnthropicMessages,
    WireFamilyId::OpenAiChat,
    WireFamilyId::OpenAiResponses,
    WireFamilyId::Gemini,
    WireFamilyId::BedrockConverse,
    WireFamilyId::GoogleCodeAssist,
    WireFamilyId::OpenAiResponsesChatgpt,
];

fn provider(id: &str, family: WireFamilyId) -> ProviderSpec {
    ProviderSpec {
        id: id.into(),
        wire_family: family,
        base_url: String::new(),
        auth_kind: AuthKind::Bearer,
        static_headers: Default::default(),
        static_options: Value::Null,
        quirks: broca_provider::families::default_quirks(family),
        raw: Value::Null,
    }
}

fn model(id: &str, modalities: Value) -> ModelSpec {
    let mut model: ModelSpec = serde_json::from_value(json!({ "id": id })).unwrap();
    model.input_modalities = InputModalities::from_value(&modalities);
    model.limits.context = Some(1_000_000);
    model
}

fn with_media(family: WireFamilyId) -> FrozenRenderConfig {
    let mut frozen = frozen_config_for_family(family, "gemini-3-pro");
    // Code Assist binds every request to a cloud project, which admission supplies.
    if family == WireFamilyId::GoogleCodeAssist {
        frozen.project_id = Some("media-project".into());
    }
    let provider_id = match family {
        WireFamilyId::AnthropicMessages => "anthropic",
        WireFamilyId::Gemini => "google",
        _ => "openai",
    };
    freeze_media_profile(
        &mut frozen,
        &provider(provider_id, family),
        &model("gemini-3-pro", json!(["text", "image", "pdf"])),
    );
    frozen
}

#[test]
fn a_config_without_media_omits_the_field_and_reads_back_as_absent() {
    let provider = provider("anthropic", WireFamilyId::AnthropicMessages);
    let model = model("claude", json!(["text", "image"]));
    // The catalog path does not freeze a profile by itself; admission adds it.
    let resolved = resolve_frozen(&provider, &model, 2048, 2).unwrap();
    assert_eq!(resolved.media, None);
    for family in FAMILIES {
        let frozen = frozen_config_for_family(family, "m");
        let blob = serde_json::to_value(&frozen).unwrap();
        assert!(
            blob.get("media").is_none(),
            "{family:?} wrote an absent media profile"
        );
        let decoded: FrozenRenderConfig = serde_json::from_value(blob).unwrap();
        assert_eq!(decoded.media, None);

        let frozen = with_media(family);
        let blob = serde_json::to_value(&frozen).unwrap();
        assert!(blob.get("media").is_some(), "{family:?}");
        let decoded: FrozenRenderConfig = serde_json::from_value(blob).unwrap();
        assert_eq!(
            decoded, frozen,
            "{family:?} profile must round-trip exactly"
        );
    }
}

#[test]
fn a_frozen_media_profile_requires_the_media_content_feature() {
    let frozen = with_media(WireFamilyId::AnthropicMessages);
    assert!(frozen.media.is_some());
    let started: WalRecord = serde_json::from_value(json!({
        "type": "run_started", "run_id": "media",
        "session": {"project_root": "/tmp/media-profile", "harness": "test", "session": "media"},
        "config": {
            "model": ModelHandle::new("anthropic", "claude"),
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0,
            "renderer_version": frozen.renderer_version, "frozen_render_config": frozen
        },
        "input": [], "origin": {"kind": "fresh"}
    }))
    .unwrap();
    let refrozen: WalRecord = serde_json::from_value(json!({
        "type": "run_config_refrozen",
        "renderer_version": frozen.renderer_version,
        "frozen_render_config": frozen
    }))
    .unwrap();
    for record in [started, refrozen] {
        assert!(
            record.required_features().contains(&"media-content/v1"),
            "a media profile must be understood before resuming the run: {:?}",
            record.required_features()
        );
    }
}

#[test]
fn the_frozen_profile_survives_a_catalog_change_and_a_switch_refreezes_it() {
    let provider = provider("anthropic", WireFamilyId::AnthropicMessages);
    let mut admitted = frozen_config_for_family(WireFamilyId::AnthropicMessages, "claude");
    freeze_media_profile(
        &mut admitted,
        &provider,
        &model("claude", json!(["text", "image"])),
    );
    // The blob is what RunStarted records and what a resume restores.
    let recorded = serde_json::to_value(&admitted).unwrap();

    // The catalog later stops listing image input for the same model. The recorded profile is
    // decoded, not re-derived, so the admitted run still accepts images.
    let changed = model("claude", json!(["text"]));
    let resumed: FrozenRenderConfig = serde_json::from_value(recorded).unwrap();
    let profile = resumed.media.as_ref().unwrap();
    assert_eq!(profile.profile, MediaProfileId::Anthropic);
    assert!(profile.accepts_modality(MediaKind::Image));
    assert_eq!(resumed, admitted);

    // A model switch freezes the new model's profile into the refrozen config.
    let mut switched = frozen_config_for_family(WireFamilyId::AnthropicMessages, "claude");
    freeze_media_profile(&mut switched, &provider, &changed);
    let profile = switched.media.as_ref().unwrap();
    assert!(!profile.accepts_modality(MediaKind::Image));
    assert_ne!(switched.media, admitted.media);
}

#[test]
fn a_media_profile_does_not_change_text_only_request_bytes() {
    let prompt = vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text { text: "sys".into() }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "hello".into(),
            }],
        ),
        Message::new(
            Role::Assistant,
            vec![ContentBlock::Text {
                text: "hi there".into(),
            }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "again".into(),
            }],
        ),
    ];
    for family_id in FAMILIES {
        let family = wire_family(family_id).expect("every family is registered");
        let mut without = with_media(family_id);
        without.media = None;
        let with = with_media(family_id);
        assert!(with.media.is_some());
        let mut opts = CallOptions::new(prompt.clone());
        // Code Assist derives its request id from the run and step, so every render needs them.
        opts.step_context = Some(broca_protocol::StepContext {
            session_address: None,
            credential_id: None,
            account_ref: None,
            run_id: "media".into(),
            step_index: 1,
            max_output_tokens: None,
        });
        assert_eq!(
            family.render(&without, &opts),
            family.render(&with, &opts),
            "{family_id:?}: a session without media must render byte-identically with or \
             without a frozen media profile"
        );
    }
}

#[test]
fn ordinary_run_started_bytes_stay_pinned() {
    let frozen = frozen_config_for_family(WireFamilyId::OpenAiChat, "ordinary");
    let started: WalRecord = serde_json::from_value(json!({
        "type": "run_started", "run_id": "ordinary",
        "session": {"project_root": "/ordinary", "harness": "test", "session": "ordinary"},
        "config": {
            "model": ModelHandle::new("openai", "ordinary"),
            "tools": [], "tool_choice": {"type": "auto"}, "generation": {},
            "response_format": {"type": "text"}, "context_epoch": 0,
            "renderer_version": frozen.renderer_version, "frozen_render_config": frozen
        },
        "input": [], "origin": {"kind": "fresh"}
    }))
    .unwrap();
    assert_eq!(
        serde_json::to_string(&started).unwrap(),
        r#"{"type":"run_started","run_id":"ordinary","session":{"project_root":"/ordinary","harness":"test","session":"ordinary"},"config":{"model":{"provider_module_id":"openai","model_id":"ordinary"},"tools":[],"tool_choice":{"type":"auto"},"generation":{},"response_format":{"type":"text"},"context_epoch":0,"renderer_version":5,"frozen_render_config":{"family_id":"openai_chat","max_output_tokens":2048,"reasoning":{"mode":"none"},"renderer_version":5,"system_prompt":{"mode":"system_message"},"token_limit_field":"max_tokens","tool_id":"passthrough","usage":"open_ai_usage","wire_model_id":"ordinary"}},"input":[],"origin":{"kind":"fresh"}}"#
    );
}
