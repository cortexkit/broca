//! Every wire family must honor `FrozenRenderConfig.base_url` in its endpoint URL,
//! so the e2e harness can point any `LiveProvider<Family>` at a mock server. base_url
//! lives in the endpoint (not the rendered body), so this never affects the render
//! goldens — it only redirects where the request is sent. A trailing slash on the
//! base is trimmed before the family path is appended.

use broca_provider::families::{
    anthropic_frozen_config, bedrock_frozen_config, gemini_frozen_config,
    openai_chat_frozen_config, openai_responses_frozen_config,
};
use broca_provider::families::{
    AnthropicMessages, BedrockConverse, Gemini, OpenAiChat, OpenAiResponses,
};
use broca_provider::wire::{AuthHandle, FrozenRenderConfig, WireFamily};

const MOCK: &str = "http://127.0.0.1:8801/v1";

fn url_of(family: &dyn WireFamily, frozen: &FrozenRenderConfig) -> String {
    family
        .endpoint(frozen, &AuthHandle::None, b"{}".to_vec())
        .url
}

#[test]
fn openai_chat_honors_base_url() {
    let mut f = openai_chat_frozen_config("m");
    f.base_url = Some(MOCK.to_string());
    assert_eq!(
        url_of(&OpenAiChat, &f),
        "http://127.0.0.1:8801/v1/chat/completions"
    );
}

#[test]
fn openai_responses_honors_base_url() {
    let mut f = openai_responses_frozen_config("m");
    f.base_url = Some(MOCK.to_string());
    assert_eq!(
        url_of(&OpenAiResponses, &f),
        "http://127.0.0.1:8801/v1/responses"
    );
}

#[test]
fn anthropic_honors_base_url() {
    let mut f = anthropic_frozen_config("m");
    f.base_url = Some("http://127.0.0.1:8801".to_string());
    assert_eq!(
        url_of(&AnthropicMessages, &f),
        "http://127.0.0.1:8801/v1/messages"
    );
}

#[test]
fn gemini_honors_base_url() {
    let mut f = gemini_frozen_config("gemini-2.0-flash");
    f.base_url = Some("http://127.0.0.1:8801/v1beta".to_string());
    assert_eq!(
        url_of(&Gemini, &f),
        "http://127.0.0.1:8801/v1beta/models/gemini-2.0-flash:streamGenerateContent?alt=sse"
    );
}

#[test]
fn bedrock_honors_base_url_for_url_and_host() {
    let mut f = bedrock_frozen_config("anthropic.claude-3-sonnet");
    f.base_url = Some("http://127.0.0.1:8801".to_string());
    let req = BedrockConverse.endpoint(&f, &AuthHandle::None, b"{}".to_vec());
    assert_eq!(
        req.url,
        "http://127.0.0.1:8801/model/anthropic.claude-3-sonnet/converse-stream"
    );
    // The host header must track the overridden host (SigV4 signs over it).
    let host = req
        .headers
        .iter()
        .find(|(k, _)| k == "host")
        .map(|(_, v)| v.as_str());
    assert_eq!(host, Some("127.0.0.1:8801"));
}

#[test]
fn bedrock_base_url_host_uses_only_the_authority() {
    const MODEL_PATH: &str = "/model/anthropic.claude-3-sonnet/converse-stream";
    let cases = [
        ("https://proxy.example", "proxy.example"),
        ("https://proxy.example:8443", "proxy.example:8443"),
        ("https://[::1]", "[::1]"),
        ("https://[::1]:8080", "[::1]:8080"),
        ("https://proxy.example/bedrock", "proxy.example"),
        ("https://proxy.example:8443/bedrock", "proxy.example:8443"),
        ("proxy.example", "proxy.example"),
        ("https://proxy.example/", "proxy.example"),
    ];

    for (base, expected_host) in cases {
        let mut frozen = bedrock_frozen_config("anthropic.claude-3-sonnet");
        frozen.base_url = Some(base.to_string());
        let request = BedrockConverse.endpoint(&frozen, &AuthHandle::None, b"{}".to_vec());
        let trimmed = base.trim_end_matches('/');
        let host = request
            .headers
            .iter()
            .find(|(name, _)| name == "host")
            .map(|(_, value)| value.as_str());

        assert_eq!(request.url, format!("{trimmed}{MODEL_PATH}"), "{base}");
        assert_eq!(host, Some(expected_host), "{base}");
    }
}

#[test]
fn bedrock_without_override_keeps_the_region_derived_endpoint() {
    let frozen = bedrock_frozen_config("anthropic.claude-3-sonnet");
    let request = BedrockConverse.endpoint(&frozen, &AuthHandle::None, b"{}".to_vec());
    let host = request
        .headers
        .iter()
        .find(|(name, _)| name == "host")
        .map(|(_, value)| value.as_str());

    assert_eq!(
        request.url,
        "https://bedrock-runtime.us-east-1.amazonaws.com/model/anthropic.claude-3-sonnet/converse-stream"
    );
    assert_eq!(host, Some("bedrock-runtime.us-east-1.amazonaws.com"));
}

/// The catalog path resolves a Bedrock provider with NO explicit region to
/// `aws_region: Some("")` (the auth kind carries an empty region string). An empty
/// region must fall back to the default host, not build the malformed
/// `bedrock-runtime..amazonaws.com`. (No base_url override, so the host is region-derived.)
#[test]
fn bedrock_empty_region_falls_back_to_default_host() {
    let mut f = bedrock_frozen_config("amazon.nova-lite-v1:0");
    f.base_url = None;
    f.aws_region = Some(String::new());
    let req = BedrockConverse.endpoint(&f, &AuthHandle::Bearer("k".into()), b"{}".to_vec());
    assert_eq!(
        req.url,
        "https://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.nova-lite-v1%3A0/converse-stream",
        "an empty region must use the default region, not produce a malformed host"
    );
    let host = req
        .headers
        .iter()
        .find(|(k, _)| k == "host")
        .map(|(_, v)| v.as_str());
    assert_eq!(host, Some("bedrock-runtime.us-east-1.amazonaws.com"));
}

/// Bedrock accepts a long-term BEARER api key (the newer AWS Bedrock API key), not
/// only SigV4. The Bearer arm must attach `Authorization: Bearer <key>`, and the
/// SigV4 arm must still attach its signed headers — so the live bearer-key path works
/// without breaking the existing signing path.
#[test]
fn bedrock_attaches_bearer_and_still_supports_sigv4() {
    let f = bedrock_frozen_config("anthropic.claude-3-5-sonnet-20240620-v1:0");

    // Bearer → Authorization: Bearer <key>.
    let bearer = BedrockConverse.endpoint(
        &f,
        &AuthHandle::Bearer("bedrock-api-key-xyz".into()),
        b"{}".to_vec(),
    );
    let auth_header = bearer
        .headers
        .iter()
        .find(|(k, _)| k == "authorization")
        .map(|(_, v)| v.as_str());
    assert_eq!(
        auth_header,
        Some("Bearer bedrock-api-key-xyz"),
        "Bearer arm attaches the Authorization header"
    );

    // SigV4 → its signed headers are still attached (unchanged).
    let sigv4 = BedrockConverse.endpoint(
        &f,
        &AuthHandle::SigV4 {
            region: "us-east-1".into(),
            service: "bedrock-runtime".into(),
            signed_headers: vec![("x-amz-date".into(), "20250101T000000Z".into())],
        },
        b"{}".to_vec(),
    );
    assert!(
        sigv4
            .headers
            .iter()
            .any(|(k, v)| k == "x-amz-date" && v == "20250101T000000Z"),
        "SigV4 arm still attaches signed headers"
    );
    // The bearer path must NOT have leaked a signed header, and vice versa.
    assert!(
        !bearer.headers.iter().any(|(k, _)| k == "x-amz-date"),
        "Bearer path carries no SigV4 signed headers"
    );
    assert!(
        !sigv4.headers.iter().any(|(k, _)| k == "authorization"),
        "SigV4 path carries no bearer Authorization header"
    );
}

#[test]
fn trailing_slash_on_base_is_trimmed() {
    let mut f = openai_chat_frozen_config("m");
    f.base_url = Some("http://127.0.0.1:8801/v1/".to_string());
    assert_eq!(
        url_of(&OpenAiChat, &f),
        "http://127.0.0.1:8801/v1/chat/completions"
    );
}

#[test]
fn no_base_url_falls_back_to_family_default() {
    // The hardcoded defaults still apply when base_url is None (live-API behavior
    // unchanged).
    let f = anthropic_frozen_config("m");
    assert_eq!(
        url_of(&AnthropicMessages, &f),
        "https://api.anthropic.com/v1/messages"
    );
    let f = openai_responses_frozen_config("m");
    assert_eq!(
        url_of(&OpenAiResponses, &f),
        "https://api.openai.com/v1/responses"
    );
}
