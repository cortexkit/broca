//! Credential-safe media carrier probe. No flags (or `--dry-run`) renders but sends nothing.
//! Live use is reserved for the Mac operator: see `docs/media-families.md`.

#[path = "media-live/plan.rs"]
mod plan;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use broca_core::media_request::{MediaId, MediaLocation};
use broca_module_serve::{devtools, CredentialId};
use broca_protocol::{
    AssistantMessage, BindIdentity, CallOptions, ContentBlock, ExecutionMode, MediaKind, MediaRef,
    Message, ModelHandle, NormalizedEvent, ProviderControl, Role, StepId, Tool, ToolCall,
    ToolCallId, ToolChoice, ToolOutput, ToolOutputPart, ToolResult, Usage,
};
use broca_provider::{
    media_profile::{MediaCarrier, MediaProfile, MediaProfileId},
    media_request::{render_prepared, Occurrence, PreparedBundle, PreparedMedia},
    registry::wire_family,
    spec::WireFamilyId,
    transport::build_single_attempt_http_client,
    wire::{AuthHandle, FrozenRenderConfig, HttpRequest},
    ProviderHttpClient,
};
use plan::{Args, Route, MAX_REQUESTS};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const RESPONSE_LIMIT: usize = 4 * 1024 * 1024;
const TOOL_NAME: &str = "synthetic_media";
const IMAGE: &[u8] = include_bytes!("media-live/fixtures/image.png");
const PDF: &[u8] = include_bytes!("media-live/fixtures/document.pdf");
const USER_WAV: &[u8] = include_bytes!("media-live/fixtures/user.wav");
const TOOL_WAV: &[u8] = include_bytes!("media-live/fixtures/tool.wav");
const USER_MP3: &[u8] = include_bytes!("media-live/fixtures/user.mp3");
const TOOL_MP3: &[u8] = include_bytes!("media-live/fixtures/tool.mp3");
const TOOL_IMAGE: &[u8] = include_bytes!("media-live/fixtures/tool-image.png");
const TOOL_PDF: &[u8] = include_bytes!("media-live/fixtures/tool-document.pdf");

#[derive(Clone)]
struct Fixture {
    reference: MediaRef,
    base64: String,
}

impl Fixture {
    fn new(kind: MediaKind, mime: &str, bytes: &[u8]) -> Self {
        Self {
            reference: MediaRef {
                kind,
                mime: mime.into(),
                byte_size: bytes.len() as u64,
                sha256: format!("{:x}", Sha256::digest(bytes)),
                width: (kind == MediaKind::Image).then_some(640),
                height: (kind == MediaKind::Image).then_some(160),
            },
            base64: STANDARD.encode(bytes),
        }
    }
}

fn fixtures() -> Vec<Fixture> {
    vec![
        Fixture::new(MediaKind::Image, "image/png", IMAGE),
        Fixture::new(MediaKind::Document, "application/pdf", PDF),
        Fixture::new(MediaKind::Audio, "audio/wav", USER_WAV),
        Fixture::new(MediaKind::Audio, "audio/wav", TOOL_WAV),
        Fixture::new(MediaKind::Audio, "audio/mpeg", USER_MP3),
        Fixture::new(MediaKind::Audio, "audio/mpeg", TOOL_MP3),
        Fixture::new(MediaKind::Image, "image/png", TOOL_IMAGE),
        Fixture::new(MediaKind::Document, "application/pdf", TOOL_PDF),
    ]
}

fn text(value: impl Into<String>) -> ContentBlock {
    ContentBlock::Text { text: value.into() }
}

fn tool() -> Tool {
    Tool {
        name: TOOL_NAME.into(),
        description: "Return synthetic media, never session data.".into(),
        input_schema: json!({"type":"object","properties":{},"additionalProperties":false}),
        module: None,
        execution_mode: ExecutionMode::Sequential,
        reply: None,
    }
}

/// Padding gives automatic prompt caches enough stable input to report a hit. It contains
/// neither the fixture answers nor anything read from the operator's machine.
fn initial_history() -> Vec<Message> {
    let padding = (0..1024)
        .map(|n| format!("Synthetic cache line {n:04}: north east south west.\n"))
        .collect::<String>();
    vec![Message::new(Role::System, vec![text(format!(
        "Read attached media, not filenames. Never invent an unreadable value. Use uppercase words and decimal numerals for spoken numbers. {padding}"
    ))])]
}

fn add_exchange(
    history: &mut Vec<Message>,
    frozen: &FrozenRenderConfig,
    fixtures: &[Fixture],
    stage: usize,
) -> BTreeMap<&'static str, &'static str> {
    let profile = frozen.media.as_ref().expect("probe profile");
    let chatgpt = frozen.family_id == WireFamilyId::OpenAiResponsesChatgpt;
    let mut selected = vec![if stage == 0 {
        ("image", "VIOLET 73", 0)
    } else {
        ("image", "ORANGE 19", 6)
    }];
    if profile.document.user_message != MediaCarrier::Marker {
        selected.push(if stage == 0 {
            ("document", "COBALT 41", 1)
        } else {
            ("document", "INDIGO 62", 7)
        });
    }
    let mut user = vec![];
    let mut parts = vec![ToolOutputPart::Text {
        text: "Synthetic capture; inspect the media itself.".into(),
    }];
    let mut checks = BTreeMap::new();
    for (label, answer, index) in selected {
        // The baseline establishes person-media rendering; the second request exercises the
        // tool carrier. Both remain in the third request's cache prefix.
        if stage == 0 {
            user.push(ContentBlock::Media(fixtures[index].reference.clone()));
        } else {
            parts.push(ToolOutputPart::Media {
                media: fixtures[index].reference.clone(),
            });
        }
        checks.insert(label, answer);
    }
    if chatgpt {
        let index = if stage == 0 { 2 } else { 4 };
        user.push(ContentBlock::Media(fixtures[index].reference.clone()));
        parts.push(ToolOutputPart::Media {
            media: fixtures[index + 1].reference.clone(),
        });
        checks.insert(
            "user_audio",
            if stage == 0 { "SILVER 26" } else { "GOLDEN 32" },
        );
        checks.insert(
            "tool_audio",
            if stage == 0 { "AMBER 58" } else { "SCARLET 84" },
        );
    }
    user.push(text(format!(
        "Inspect the {} media in this exchange. Reply only with {}. Fill each value from the matching media, not from previous answers.",
        if stage == 0 { "user" } else { "tool" },
        checks.keys().map(|label| format!("{label}=<value>")).collect::<Vec<_>>().join("; ")
    )));
    history.push(Message::new(Role::User, user));
    if stage != 0 || chatgpt {
        history.push(Message::new(
            Role::Assistant,
            vec![ContentBlock::ToolCall(ToolCall {
                tool_call_id: ToolCallId(format!("synthetic_{stage}")),
                tool_name: TOOL_NAME.into(),
                input: json!({}),
                thought_signature: matches!(
                    profile.profile,
                    MediaProfileId::Gemini3 | MediaProfileId::CodeAssistGemini3
                )
                .then(|| "skip_thought_signature_validator".into()),
            })],
        ));
        history.push(Message::new(
            Role::Tool,
            vec![ContentBlock::ToolResult(ToolResult {
                tool_call_id: ToolCallId(format!("synthetic_{stage}")),
                output: ToolOutput::Parts { parts },
                is_error: false,
            })],
        ));
    }
    checks
}

fn bundle(history: &[Message], profile: &MediaProfile, fixtures: &[Fixture]) -> PreparedBundle {
    let mut media = Vec::new();
    let mut add = |reference: &MediaRef, location: MediaLocation, tool: bool| {
        let fixture = fixtures
            .iter()
            .find(|f| f.reference == *reference)
            .expect("synthetic fixture");
        let kind = profile.kind(reference.kind);
        media.push(PreparedMedia {
            id: Occurrence {
                capture: MediaId {
                    record: 0,
                    message: location.message,
                    block: location.block,
                    part: location.part,
                },
                repetition: 0,
            },
            location,
            reference: reference.clone(),
            carrier: if tool {
                kind.tool_result
            } else {
                kind.user_message
            },
            protected: false,
            tool,
            base64: fixture.base64.clone(),
        });
    };
    for (message, entry) in history.iter().enumerate() {
        for (block, content) in entry.content.iter().enumerate() {
            match content {
                ContentBlock::Media(reference) => add(
                    reference,
                    MediaLocation {
                        message,
                        block,
                        part: None,
                    },
                    false,
                ),
                ContentBlock::ToolResult(ToolResult {
                    output: ToolOutput::Parts { parts },
                    ..
                }) => {
                    for (part, value) in parts.iter().enumerate() {
                        if let ToolOutputPart::Media { media } = value {
                            add(
                                media,
                                MediaLocation {
                                    message,
                                    block,
                                    part: Some(part),
                                },
                                true,
                            );
                        }
                    }
                }
                _ => {}
            }
        }
    }
    PreparedBundle {
        prompt: history.to_vec(),
        media,
    }
}

fn render(
    frozen: &FrozenRenderConfig,
    history: &[Message],
    fixtures: &[Fixture],
) -> Result<Vec<u8>, &'static str> {
    let family = wire_family(frozen.family_id).ok_or("wire family unavailable")?;
    let mut options = CallOptions::new(history.to_vec());
    options.step_context = Some(broca_protocol::StepContext {
        run_id: "media-live-check".into(),
        step_index: history.len() as u64,
        session_address: None,
        credential_id: None,
        account_ref: None,
    });
    options.tools = Arc::new(vec![tool()]);
    options.tool_choice = Arc::new(ToolChoice::None);
    let mut prepared = bundle(
        history,
        frozen.media.as_ref().ok_or("media profile missing")?,
        fixtures,
    );
    render_prepared(&*family, frozen, &options, Some(&mut prepared), &|prompt| {
        prompt.to_vec()
    })
    .map_err(|_| "prepared rendering failed")
}

#[derive(Default)]
struct Observation {
    status: Option<u16>,
    code: &'static str,
    usage: Usage,
    assistant: Option<AssistantMessage>,
    complete: bool,
}

fn safe_code(body: &[u8]) -> &'static str {
    let value: Value = serde_json::from_slice(body).unwrap_or(Value::Null);
    let code = value
        .pointer("/error/code")
        .or_else(|| value.pointer("/error/type"))
        .and_then(Value::as_str);
    match code {
        Some("invalid_request_error") => "invalid_request_error",
        Some("unsupported_value") => "unsupported_value",
        Some("unsupported_content_type") => "unsupported_content_type",
        Some("invalid_api_key") => "invalid_api_key",
        Some("rate_limit_exceeded") => "rate_limit_exceeded",
        Some("insufficient_quota") => "insufficient_quota",
        _ => "other_redacted",
    }
}

async fn send_once(
    client: &ProviderHttpClient,
    frozen: &FrozenRenderConfig,
    request: HttpRequest,
    budget: &mut plan::Budget,
) -> Result<Observation, &'static str> {
    budget.take()?;
    let url = reqwest::Url::parse(&request.url).map_err(|_| "invalid_request_url")?;
    let mut builder = client
        .for_host(url.host_str() == Some("api.anthropic.com"))
        .post(url);
    for (name, value) in &request.headers {
        builder = builder.header(name, value);
    }
    // Neither HTTP failures nor truncated streams are retried. Even a timeout spends its slot.
    let mut response = builder
        .body(request.body)
        .send()
        .await
        .map_err(|_| "transport_failed")?;
    let status = response.status().as_u16();
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(|_| "response_read_failed")? {
        if bytes.len() + chunk.len() > RESPONSE_LIMIT {
            return Err("response_limit");
        }
        bytes.extend_from_slice(&chunk);
    }
    let mut observation = Observation {
        status: Some(status),
        code: "none",
        ..Default::default()
    };
    if !(200..300).contains(&status) {
        observation.code = safe_code(&bytes);
        return Ok(observation);
    }
    let family = wire_family(frozen.family_id).ok_or("wire family unavailable")?;
    let mut normalizer = family.normalizer(frozen, StepId(budget.sent as u64));
    let mut events = normalizer
        .push(&bytes)
        .map_err(|_| "normalization_failed")?;
    events.extend(normalizer.finish().map_err(|_| "normalization_failed")?);
    for event in events {
        match event {
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                observation.assistant = Some(message)
            }
            NormalizedEvent::Control(
                ProviderControl::StepFinished { usage, .. }
                | ProviderControl::Finished { usage, .. },
            ) => {
                observation.usage = usage;
                observation.complete = true;
            }
            NormalizedEvent::Control(ProviderControl::Error { .. }) => {
                observation.code = "stream_error_redacted";
                observation.complete = false;
            }
            _ => {}
        }
    }
    Ok(observation)
}

fn check_answer(
    observation: &Observation,
    checks: &BTreeMap<&str, &str>,
) -> BTreeMap<String, bool> {
    let answer = observation
        .assistant
        .as_ref()
        .map(|message| {
            message
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                        Some(text.as_str())
                    }
                    _ => None,
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .unwrap_or_default()
        .to_uppercase();
    checks
        .iter()
        .map(|(label, expected)| {
            let expected = format!("{}={expected}", label.to_uppercase());
            (
                (*label).to_owned(),
                observation.complete
                    && observation.code == "none"
                    && answer.split(';').any(|part| part.trim() == expected),
            )
        })
        .collect()
}

fn cache_stable(usages: &[Usage]) -> bool {
    matches!((usages.get(1).and_then(|u| u.cached_input_tokens), usages.get(2).and_then(|u| u.cached_input_tokens)), (Some(before), Some(after)) if before > 0 && after >= before)
}

fn prefix_stable(previous: &[u8], next: &[u8], family: WireFamilyId) -> bool {
    let (Ok(previous), Ok(next)) = (
        serde_json::from_slice::<Value>(previous),
        serde_json::from_slice::<Value>(next),
    ) else {
        return false;
    };
    let pointer = match family {
        WireFamilyId::AnthropicMessages
        | WireFamilyId::BedrockConverse
        | WireFamilyId::OpenAiChat => "/messages",
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt => "/input",
        WireFamilyId::Gemini => "/contents",
        WireFamilyId::GoogleCodeAssist => "/request/contents",
    };
    match (
        previous.pointer(pointer).and_then(Value::as_array),
        next.pointer(pointer).and_then(Value::as_array),
    ) {
        (Some(before), Some(after)) => !before.is_empty() && after.starts_with(before),
        _ => false,
    }
}

fn auth(route: &Route, token: String) -> Result<AuthHandle, &'static str> {
    if route.family == WireFamilyId::OpenAiResponsesChatgpt {
        let claims = token
            .split('.')
            .nth(1)
            .ok_or("invalid_chatgpt_credential")?;
        let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .decode(claims)
            .map_err(|_| "invalid_chatgpt_credential")?;
        let claims: Value =
            serde_json::from_slice(&bytes).map_err(|_| "invalid_chatgpt_credential")?;
        let account = claims
            .pointer("/https:~1~1api.openai.com~1auth/chatgpt_account_id")
            .and_then(Value::as_str)
            .ok_or("chatgpt_account_missing")?;
        return Ok(AuthHandle::Headers(vec![
            ("authorization".into(), format!("Bearer {token}")),
            ("ChatGPT-Account-Id".into(), account.into()),
            ("session-id".into(), "media-live-check".into()),
        ]));
    }
    if route
        .credential
        .as_deref()
        .is_some_and(|name| name.starts_with("apikey:"))
    {
        let header = match route.family {
            WireFamilyId::AnthropicMessages => Some("x-api-key"),
            WireFamilyId::Gemini => Some("x-goog-api-key"),
            _ => None,
        };
        if let Some(header) = header {
            return Ok(AuthHandle::ApiKeyHeader {
                header: header.into(),
                value: token,
            });
        }
    }
    Ok(AuthHandle::Bearer(token))
}

fn preview(
    route: &Route,
    frozen: &FrozenRenderConfig,
    fixtures: &[Fixture],
) -> Result<(), &'static str> {
    let mut history = initial_history();
    for stage in 0..MAX_REQUESTS {
        if stage < 2 {
            add_exchange(&mut history, frozen, fixtures, stage);
        } else {
            history.push(Message::new(
                Role::User,
                vec![text(
                    "Repeat the preceding media values in the same format.",
                )],
            ));
        }
        let body = render(frozen, &history, fixtures)?;
        let prepared = bundle(
            &history,
            frozen.media.as_ref().ok_or("media profile missing")?,
            fixtures,
        );
        println!(
            "{}",
            json!({"mode":"dry_run", "family":route.family, "model":route.model, "profile":frozen.media.as_ref().map(|p| p.profile), "request":stage + 1, "purpose":if stage == 2 {"cache_follow_up"} else {"media"}, "request_bytes":body.len(), "request_sha256":format!("{:x}", Sha256::digest(&body)), "media": prepared.media.iter().map(|m| json!({"origin":if m.tool {"tool"} else {"user"}, "carrier":m.carrier, "kind":m.reference.kind,"mime":m.reference.mime,"bytes":m.reference.byte_size,"sha256":m.reference.sha256})).collect::<Vec<_>>() })
        );
        if stage < 2 {
            history.push(Message::new(
                Role::Assistant,
                vec![text("Dry-run answer placeholder.")],
            ));
        }
    }
    Ok(())
}

async fn run_live(
    args: &Args,
    route: &Route,
    frozen: &FrozenRenderConfig,
    fixtures: &[Fixture],
) -> Result<(), &'static str> {
    let credential = route
        .credential
        .as_deref()
        .and_then(CredentialId::parse)
        .ok_or("named_credential_required")?;
    let identity = BindIdentity::new(
        std::env::current_dir().map_err(|_| "working_directory_unavailable")?,
        "media-live-check",
        "media-live-check",
    );
    let token = devtools::credential(&credential, &args.subc_path(), &identity)
        .await
        .map_err(|_| "named_credential_read_failed")?;
    let auth = auth(route, token)?;
    let family = wire_family(route.family).ok_or("wire family unavailable")?;
    let client = build_single_attempt_http_client();
    let mut budget = plan::Budget::default();
    let mut history = initial_history();
    let mut usages = vec![];
    let mut checks = BTreeMap::new();
    let mut previous_body: Option<Vec<u8>> = None;
    let mut prefixes_stable = true;
    for stage in 0..MAX_REQUESTS {
        if stage < 2 {
            checks = add_exchange(&mut history, frozen, fixtures, stage);
        } else {
            history.push(Message::new(
                Role::User,
                vec![text(
                    "Repeat the preceding media values in the same format.",
                )],
            ));
        }
        let body = render(frozen, &history, fixtures)?;
        if let Some(previous) = &previous_body {
            prefixes_stable &= prefix_stable(previous, &body, route.family);
        }
        previous_body = Some(body.clone());
        let request = family.endpoint(frozen, &auth, body);
        if !request.url.starts_with("https://") {
            return Err("live_endpoint_must_be_https");
        }
        let observation = tokio::time::timeout(
            Duration::from_secs(args.timeout_secs),
            send_once(&client, frozen, request, &mut budget),
        )
        .await
        .map_err(|_| "request_timeout")??;
        let answers = check_answer(&observation, &checks);
        usages.push(observation.usage);
        println!(
            "{}",
            json!({"mode":"live", "family":route.family,"model":route.model,"request":stage + 1,"status":observation.status,"provider_code":observation.code,"usage":observation.usage,"media_checks":answers,"rendered_prefix_stable":prefixes_stable,"cache_stable":if stage == 2 {Some(prefixes_stable && cache_stable(&usages))} else {None}})
        );
        // Never log or save answers. Keep the assistant response in memory so the next
        // request extends the actual rendered prefix, including any provider replay state.
        match observation.assistant {
            Some(message) if observation.complete && observation.code == "none" => {
                history.push(Message::assistant_from(
                    message.content,
                    ModelHandle::new("media-live-check", &route.model),
                ))
            }
            _ => break,
        }
    }
    println!(
        "{}",
        json!({"result":"pending_operator_review","family":route.family,"model":route.model,"requests_sent":budget.sent,"rendered_prefix_stable":prefixes_stable,"cache_stable":prefixes_stable && cache_stable(&usages),"production_gates_changed":false})
    );
    Ok(())
}

async fn run() -> Result<(), &'static str> {
    let args = Args::parse(std::env::args().skip(1))?;
    let routes = args.routes()?;
    println!(
        "{}",
        json!({"mode":if args.live {"live"} else {"dry_run"},"families":routes.len(),"total_planned_requests":routes.len() * MAX_REQUESTS,"max_requests_per_family":MAX_REQUESTS,"retries":0})
    );
    let fixtures = fixtures();
    // Validate and render the whole plan before any credential access or network activity.
    let prepared = routes
        .iter()
        .map(|route| {
            let frozen = route.frozen()?;
            preview(route, &frozen, &fixtures)?;
            Ok(frozen)
        })
        .collect::<Result<Vec<_>, &'static str>>()?;
    if args.live {
        for (route, frozen) in routes.iter().zip(&prepared) {
            if let Err(code) = run_live(&args, route, frozen, &fixtures).await {
                println!(
                    "{}",
                    json!({"family":route.family,"model":route.model,"provider_code":code,"result":"inconclusive","production_gates_changed":false})
                );
            }
        }
    }
    Ok(())
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    if let Err(code) = run().await {
        eprintln!("media-live-check: {code}; no credential or response details are logged");
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use broca_protocol::MessageId;

    fn route(family: WireFamilyId) -> Route {
        serde_json::from_value(json!({"family":family, "model":"gemini-3-test"})).unwrap()
    }

    #[test]
    fn prepared_requests_contain_fixture_bytes_on_every_family() {
        let fixtures = fixtures();
        for route in Args::default().routes().unwrap() {
            let frozen = route.frozen().unwrap();
            let mut history = initial_history();
            for stage in 0..2 {
                add_exchange(&mut history, &frozen, &fixtures, stage);
                let body =
                    String::from_utf8(render(&frozen, &history, &fixtures).unwrap()).unwrap();
                assert!(
                    body.contains(&fixtures[0].base64),
                    "{:?} image",
                    route.family
                );
                if stage == 1 {
                    assert!(
                        body.contains(&fixtures[6].base64),
                        "tool image must be new evidence"
                    );
                }
                assert!(!body.contains("reason=mime"), "{:?} MIME", route.family);
                // OpenAI Chat-compatible APIs' string-only tool-result slot keeps a carrier
                // marker even when the payload is carried in the following user message.
                if stage == 0
                    || frozen.media.as_ref().unwrap().image.tool_result
                        != MediaCarrier::FollowUpUserMessage
                {
                    assert!(
                        !body.contains("reason=carrier"),
                        "{:?} carrier",
                        route.family
                    );
                }
                if route.family == WireFamilyId::OpenAiResponsesChatgpt {
                    for fixture in &fixtures[2..if stage == 0 { 4 } else { 6 }] {
                        assert!(
                            body.contains(&fixture.base64),
                            "ChatGPT audio {}",
                            fixture.reference.mime
                        );
                    }
                    assert!(body.contains("input_audio"));
                }
            }
        }
    }

    #[test]
    fn checker_requires_fixed_media_values_not_http_success() {
        let checks = BTreeMap::from([("image", "VIOLET 73"), ("document", "COBALT 41")]);
        let mut observation = Observation {
            code: "none",
            complete: true,
            status: Some(200),
            ..Default::default()
        };
        assert!(check_answer(&observation, &checks)
            .values()
            .all(|value| !value));
        observation.assistant = Some(AssistantMessage {
            message_id: MessageId("synthetic".into()),
            content: vec![text("image=VIOLET 73; document=COBALT 41")],
        });
        assert!(check_answer(&observation, &checks)
            .values()
            .all(|value| *value));
        observation.assistant.as_mut().unwrap().content =
            vec![text("image=VIOLET 730; document=COBALT 41")];
        assert!(!check_answer(&observation, &checks)["image"]);
        observation.complete = false;
        assert!(check_answer(&observation, &checks)
            .values()
            .all(|value| !value));
    }

    #[test]
    fn cache_verdict_requires_reported_positive_non_decreasing_hits() {
        let usage = |cached_input_tokens| Usage {
            cached_input_tokens,
            ..Default::default()
        };
        assert!(!cache_stable(&[]));
        assert!(!cache_stable(&[
            usage(None),
            usage(Some(0)),
            usage(Some(0))
        ]));
        assert!(!cache_stable(&[
            usage(None),
            usage(Some(1024)),
            usage(Some(512))
        ]));
        assert!(!cache_stable(&[
            usage(None),
            usage(Some(1024)),
            usage(None)
        ]));
        assert!(cache_stable(&[
            usage(None),
            usage(Some(1024)),
            usage(Some(2048))
        ]));
    }

    #[test]
    fn cache_prefix_check_rejects_changed_or_missing_prior_items() {
        assert!(prefix_stable(
            br#"{"input":[{"role":"user","content":"one"}]}"#,
            br#"{"input":[{"role":"user","content":"one"},{"role":"user","content":"two"}]}"#,
            WireFamilyId::OpenAiResponses
        ));
        assert!(!prefix_stable(
            br#"{"input":[{"role":"user","content":"one"}]}"#,
            br#"{"input":[{"role":"user","content":"changed"}]}"#,
            WireFamilyId::OpenAiResponses
        ));
        assert!(!prefix_stable(b"{}", b"{}", WireFamilyId::OpenAiResponses));
    }

    #[test]
    fn errors_never_echo_unrecognized_provider_codes_or_messages() {
        let secret = "not-a-real-credential-do-not-print";
        let body = json!({"error":{"code":secret,"message":secret,"token":secret}}).to_string();
        assert_eq!(safe_code(body.as_bytes()), "other_redacted");
        assert_eq!(
            safe_code(b"invalid response containing a credential"),
            "other_redacted"
        );
        assert_eq!(
            safe_code(br#"{"error":{"type":"invalid_request_error"}}"#),
            "invalid_request_error"
        );
    }

    #[tokio::test]
    async fn single_attempt_transport_does_not_follow_redirects_or_spend_a_fourth_slot() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = [0; 4096];
            stream.read(&mut request).await.unwrap();
            let reply = format!("HTTP/1.1 307 Temporary Redirect\r\nLocation: http://{addr}/second\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
            stream.write_all(reply.as_bytes()).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        });
        let frozen = route(WireFamilyId::OpenAiChat).frozen().unwrap();
        let request = || HttpRequest {
            method: broca_provider::wire::HttpMethod::Post,
            url: format!("http://{addr}/first"),
            headers: vec![],
            body: b"{}".to_vec(),
        };
        let client = build_single_attempt_http_client();
        let mut budget = plan::Budget::default();
        let observation = send_once(&client, &frozen, request(), &mut budget)
            .await
            .unwrap();
        assert_eq!(observation.status, Some(307));
        assert_eq!(budget.sent, 1);
        budget.take().unwrap();
        budget.take().unwrap();
        assert_eq!(
            send_once(&client, &frozen, request(), &mut budget)
                .await
                .err(),
            Some("family_request_cap")
        );
        server.await.unwrap();
    }
}
