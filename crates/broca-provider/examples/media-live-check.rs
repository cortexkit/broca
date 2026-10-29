//! Credential-safe media carrier probe. No flags (or `--dry-run`) renders but sends nothing.
//! Live use is reserved for the Mac operator: see `docs/media-families.md`.

#[path = "media-live/error_detail.rs"]
mod error_detail;
#[path = "media-live/plan.rs"]
mod plan;

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use base64::{engine::general_purpose::STANDARD, Engine as _};
use broca_core::media_request::{MediaId, MediaLocation};
use broca_module_serve::{
    claude_code_identity, devtools, state_root::resolve_state_root, CredentialId,
};
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
use error_detail::ErrorDetail;
use plan::{Args, Route, MAX_REQUESTS};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

const RESPONSE_LIMIT: usize = 4 * 1024 * 1024;
const TOOL_NAME: &str = "synthetic_media";
const IMAGE: &[u8] = include_bytes!("media-live/fixtures/image.png");
const PDF: &[u8] = include_bytes!("media-live/fixtures/document.pdf");
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
    let mut selected = vec![if stage == 0 {
        ("image", "VIOLET 73", 0)
    } else {
        ("image", "ORANGE 19", 2)
    }];
    if profile.document.user_message != MediaCarrier::Marker {
        selected.push(if stage == 0 {
            ("document", "COBALT 41", 1)
        } else {
            ("document", "INDIGO 62", 3)
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
    user.push(text(format!(
        "Inspect the {} media in this exchange. Reply only with {}. Fill each value from the matching media, not from previous answers.",
        if stage == 0 { "user" } else { "tool" },
        checks.keys().map(|label| format!("{label}=<value>")).collect::<Vec<_>>().join("; ")
    )));
    history.push(Message::new(Role::User, user));
    if stage != 0 {
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
        max_output_tokens: None,
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

fn probe_identity() -> Result<BindIdentity, &'static str> {
    Ok(BindIdentity::new(
        std::env::current_dir().map_err(|_| "working_directory_unavailable")?,
        "media-live-check",
        "media-live-check",
    ))
}

fn oauth_config(
    route: &Route,
    frozen: &FrozenRenderConfig,
    identity: &BindIdentity,
    install_id: &str,
    account_id: Option<&str>,
) -> Result<FrozenRenderConfig, &'static str> {
    let mut resolved = frozen.clone();
    resolved.claude_code_persona = Some(
        claude_code_identity::fresh_persona(
            install_id,
            account_id,
            route
                .credential
                .as_deref()
                .ok_or("named_credential_required")?,
            identity,
            None,
            1,
        )
        .map_err(|_| "oauth_persona_unavailable")?,
    );
    Ok(resolved)
}

fn preview_config(
    route: &Route,
    frozen: &FrozenRenderConfig,
) -> Result<FrozenRenderConfig, &'static str> {
    let mut preview = frozen.clone();
    if route.is_anthropic_oauth() {
        // Preview uses an explicit synthetic install seed and the production None-account
        // fallback. Live execution rebuilds this persona from the vault reply and real seed.
        preview = oauth_config(
            route,
            &preview,
            &probe_identity()?,
            "media-live-offline-preview",
            None,
        )?;
    }
    if preview.family_id == WireFamilyId::GoogleCodeAssist
        && preview.project_id.as_deref().is_none_or(str::is_empty)
    {
        // The envelope requires a project field even for an offline preview. An empty field
        // lets the real renderer run without inventing an account project. Live execution
        // still sees the original unresolved config and skips before credential access.
        preview.project_id = Some(String::new());
    }
    Ok(preview)
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
    mut error_detail: Option<&mut ErrorDetail>,
    redactions: &[&str],
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
        if let Some(detail) = error_detail.as_deref_mut() {
            detail
                .record(
                    &bytes,
                    "http_error",
                    Some(observation.code),
                    "provider error fields unavailable",
                    redactions,
                )
                .map_err(|_| "error_detail_write_failed")?;
        }
        return Ok(observation);
    }
    let wire_errors_recorded = match error_detail.as_deref_mut() {
        Some(detail) => detail
            .record_wire_errors(&bytes, redactions)
            .map_err(|_| "error_detail_write_failed")?,
        None => false,
    };
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
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                observation.code = "stream_error_redacted";
                observation.complete = false;
                if let Some(detail) = error_detail
                    .as_deref_mut()
                    .filter(|_| !wire_errors_recorded)
                {
                    detail
                        .record(
                            &bytes,
                            &format!("{:?}", error.class),
                            error.provider_code.as_deref(),
                            &error.message,
                            redactions,
                        )
                        .map_err(|_| "error_detail_write_failed")?;
                }
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
    let preview = preview_config(route, frozen)?;
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
        let body = render(&preview, &history, fixtures)?;
        let prepared = bundle(
            &history,
            frozen.media.as_ref().ok_or("media profile missing")?,
            fixtures,
        );
        println!(
            "{}",
            json!({"mode":"dry_run", "family":route.family, "model":route.model, "profile":frozen.media.as_ref().map(|p| p.profile), "request":stage + 1, "purpose":if stage == 2 {"cache_follow_up"} else {"media"}, "live_skip":route.live_skip_reason(frozen), "request_bytes":body.len(), "request_sha256":format!("{:x}", Sha256::digest(&body)), "media": prepared.media.iter().map(|m| json!({"origin":if m.tool {"tool"} else {"user"}, "carrier":m.carrier, "kind":m.reference.kind,"mime":m.reference.mime,"bytes":m.reference.byte_size,"sha256":m.reference.sha256})).collect::<Vec<_>>() })
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
    mut error_detail: Option<&mut ErrorDetail>,
) -> Result<(), &'static str> {
    if let Some(reason) = route.live_skip_reason(frozen) {
        println!(
            "{}",
            json!({"mode":"live", "family":route.family,"model":route.model,"status":"skipped","reason":reason,"missing_field":route.missing_live_field(frozen),"requests_sent":0,"production_gates_changed":false})
        );
        return Ok(());
    }
    let credential = route
        .credential
        .as_deref()
        .and_then(CredentialId::parse)
        .ok_or("named_credential_required")?;
    let identity = probe_identity()?;
    let secret = devtools::credential_with_metadata(&credential, &args.subc_path(), &identity)
        .await
        .map_err(|_| "named_credential_read_failed")?;
    let mut resolved = frozen.clone();
    if route.is_anthropic_oauth() {
        let state = resolve_state_root().map_err(|_| "install_identity_unavailable")?;
        let install_id = claude_code_identity::load_or_create_install_id(&state.path)
            .map_err(|_| "install_identity_unavailable")?;
        resolved = oauth_config(
            route,
            &resolved,
            &identity,
            &install_id,
            secret.account_id.as_deref(),
        )?;
    }
    let frozen = &resolved;
    let auth = auth(route, secret.token.clone())?;
    let redactions = [
        secret.token.as_str(),
        secret.account_id.as_deref().unwrap_or_default(),
    ];
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
            send_once(
                &client,
                frozen,
                request,
                &mut budget,
                error_detail.as_deref_mut(),
                &redactions,
            ),
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
    println!("{}", plan_summary(&routes, args.live)?);
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
        let mut error_detail = args
            .error_detail
            .as_deref()
            .map(ErrorDetail::open)
            .transpose()
            .map_err(|_| "error_detail_open_failed")?;
        for (route, frozen) in routes.iter().zip(&prepared) {
            if let Err(code) =
                run_live(&args, route, frozen, &fixtures, error_detail.as_mut()).await
            {
                println!(
                    "{}",
                    json!({"family":route.family,"model":route.model,"provider_code":code,"result":"inconclusive","production_gates_changed":false})
                );
            }
        }
    }
    Ok(())
}

fn plan_summary(routes: &[Route], live: bool) -> Result<Value, &'static str> {
    let not_live_checked = if routes
        .iter()
        .any(|route| route.family == WireFamilyId::BedrockConverse)
    {
        vec![]
    } else {
        vec![json!({"family":"bedrock_converse", "reason":"no_credential"})]
    };
    let live_skips = routes
        .iter()
        .filter_map(|route| match route.frozen() {
            Ok(frozen) => route
                .live_skip_reason(&frozen)
                .map(|reason| Ok(json!({"family":route.family,"reason":reason,"missing_field":route.missing_live_field(&frozen)}))),
            Err(error) => Some(Err(error)),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(
        json!({"mode":if live {"live"} else {"dry_run"},"families":routes.len(),"total_planned_requests":routes.len() * MAX_REQUESTS,"max_requests_per_family":MAX_REQUESTS,"retries":0,"not_live_checked":not_live_checked,"live_skips":live_skips}),
    )
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
    fn default_summary_caps_six_families_at_eighteen_and_names_unchecked_bedrock() {
        let summary = plan_summary(&Args::default().routes().unwrap(), false).unwrap();
        assert_eq!(summary["families"], 6);
        assert_eq!(summary["total_planned_requests"], 18);
        assert_eq!(summary["max_requests_per_family"], 3);
        assert_eq!(summary["mode"], "dry_run");
        assert_eq!(
            summary["not_live_checked"],
            json!([{"family":"bedrock_converse", "reason":"no_credential"}])
        );
        assert_eq!(
            summary["live_skips"],
            json!([
                {"family":"google_codeassist", "reason":"project_id_required", "missing_field":"project_id"}
            ])
        );
    }

    #[tokio::test]
    async fn codeassist_without_project_skips_before_even_parsing_a_credential_name() {
        let route = route(WireFamilyId::GoogleCodeAssist);
        assert!(route.credential.is_none());
        let frozen = route.frozen().unwrap();
        let mut history = initial_history();
        add_exchange(&mut history, &frozen, &fixtures(), 0);
        let preview: Value = serde_json::from_slice(
            &render(
                &preview_config(&route, &frozen).unwrap(),
                &history,
                &fixtures(),
            )
            .unwrap(),
        )
        .unwrap();
        assert_eq!(preview["project"], "");
        assert!(frozen.project_id.is_none());
        assert!(
            run_live(&Args::default(), &route, &frozen, &fixtures(), None)
                .await
                .is_ok()
        );
    }

    #[test]
    fn oauth_uses_vault_account_id_and_the_production_credential_fallback() {
        let route: Route = serde_json::from_value(json!({
            "family":"anthropic_messages", "model":"claude-haiku-5-5", "credential":"oauth:anthropic"
        }))
        .unwrap();
        let frozen = route.frozen().unwrap();
        assert_eq!(route.live_skip_reason(&frozen), None);
        assert_eq!(route.missing_live_field(&frozen), None);
        let uuid = "00000000-0000-4000-8000-000000000001";
        for (account, expected_uuid, device_account) in [
            (Some(uuid), Some(uuid), uuid),
            (Some("opaque-account"), None, "opaque-account"),
            (None, None, "oauth:anthropic"),
            (Some(""), None, "oauth:anthropic"),
        ] {
            let configured = oauth_config(
                &route,
                &frozen,
                &probe_identity().unwrap(),
                "synthetic-install",
                account,
            )
            .unwrap();
            let persona = configured.claude_code_persona.as_ref().unwrap();
            assert_eq!(persona.account_uuid.as_deref(), expected_uuid);
            assert_eq!(
                persona.device_id,
                broca_provider::wire::ClaudeCodePersona::derive_device_id(
                    "synthetic-install",
                    device_account
                )
            );
            assert_eq!(persona.prompt_ordinal, Some(1));
            assert!(persona.prompt_id.is_some());
            assert_eq!(route.live_skip_reason(&configured), None);
        }
    }

    #[test]
    fn resolved_oauth_uses_production_persona_headers_and_api_keys_still_work() {
        let route: Route = serde_json::from_value(json!({
            "family":"anthropic_messages", "model":"claude-haiku-5-5", "credential":"oauth:anthropic"
        })).unwrap();
        let frozen = oauth_config(
            &route,
            &route.frozen().unwrap(),
            &probe_identity().unwrap(),
            "synthetic-install",
            Some("00000000-0000-4000-8000-000000000001"),
        )
        .unwrap();
        assert_eq!(route.live_skip_reason(&frozen), None);
        let family = wire_family(route.family).unwrap();
        let fixtures = fixtures();
        let mut history = initial_history();
        add_exchange(&mut history, &frozen, &fixtures, 0);
        let request = family.endpoint(
            &frozen,
            &auth(&route, "synthetic-oauth-token".into()).unwrap(),
            render(&frozen, &history, &fixtures).unwrap(),
        );
        let header = |name: &str| {
            request
                .headers
                .iter()
                .find(|(key, _)| key.eq_ignore_ascii_case(name))
                .map(|(_, value)| value.as_str())
        };
        assert!(request.url.ends_with("/v1/messages?beta=true"));
        assert_eq!(
            header("authorization"),
            Some("Bearer synthetic-oauth-token")
        );
        assert_eq!(header("x-app"), Some("cli"));
        assert!(header("anthropic-beta")
            .unwrap()
            .contains("oauth-2025-04-20"));
        assert!(header("user-agent").unwrap().starts_with("claude-cli/"));
        assert!(header("x-api-key").is_none());
        assert!(
            serde_json::from_slice::<Value>(&request.body).unwrap()["metadata"]["user_id"]
                .is_string()
        );

        let api: Route = serde_json::from_value(json!({
            "family":"anthropic_messages", "model":"claude-haiku-5-5", "credential":"apikey:anthropic"
        })).unwrap();
        let frozen = api.frozen().unwrap();
        assert_eq!(api.live_skip_reason(&frozen), None);
        let request = family.endpoint(
            &frozen,
            &auth(&api, "synthetic-api-key".into()).unwrap(),
            render(&frozen, &history, &fixtures).unwrap(),
        );
        assert!(request
            .headers
            .iter()
            .any(|(key, value)| key == "x-api-key" && value == "synthetic-api-key"));
        assert!(!request
            .headers
            .iter()
            .any(|(key, _)| key == "authorization"));
    }

    #[test]
    fn prepared_requests_contain_fixture_bytes_on_every_family() {
        let fixtures = fixtures();
        for route in Args::default().routes().unwrap() {
            let frozen = route.frozen().unwrap();
            let preview = preview_config(&route, &frozen).unwrap();
            let mut history = initial_history();
            for stage in 0..2 {
                add_exchange(&mut history, &frozen, &fixtures, stage);
                let body =
                    String::from_utf8(render(&preview, &history, &fixtures).unwrap()).unwrap();
                assert!(
                    body.contains(&fixtures[0].base64),
                    "{:?} image",
                    route.family
                );
                if stage == 1 {
                    assert!(
                        body.contains(&fixtures[2].base64),
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
                    assert!(!body.contains("input_audio"));
                    assert!(!body.contains("audio/"));
                    assert!(!body.contains("input_file"));
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
    async fn provider_error_detail_is_opt_in_and_console_code_stays_redacted() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let secret = "synthetic-private-token";
        let body = json!({"headers":{"authorization":"header-secret"}, "error":{"type":"invalid_request_error","code":"private-code","message":format!("Rejected {secret}")}}).to_string();
        let server = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.unwrap();
                let mut request = [0; 4096];
                stream.read(&mut request).await.unwrap();
                let reply = format!("HTTP/1.1 400 Bad Request\r\nContent-Length: {}\r\nx-provider-private: header-secret\r\nConnection: close\r\n\r\n{body}", body.len());
                stream.write_all(reply.as_bytes()).await.unwrap();
            }
        });
        let path = broca_test_support::scratch_root().join("media-http-error-detail.jsonl");
        let mut detail = ErrorDetail::open(&path).unwrap();
        let frozen = route(WireFamilyId::OpenAiChat).frozen().unwrap();
        let request = || HttpRequest {
            method: broca_provider::wire::HttpMethod::Post,
            url: format!("http://{addr}/error"),
            headers: vec![],
            body: b"{}".to_vec(),
        };
        let client = build_single_attempt_http_client();
        let mut budget = plan::Budget::default();
        let default = send_once(&client, &frozen, request(), &mut budget, None, &[secret])
            .await
            .unwrap();
        assert_eq!(default.status, Some(400));
        assert_eq!(default.code, "other_redacted");
        assert_eq!(std::fs::read(&path).unwrap(), b"");
        let opted_in = send_once(
            &client,
            &frozen,
            request(),
            &mut budget,
            Some(&mut detail),
            &[secret],
        )
        .await
        .unwrap();
        assert_eq!(opted_in.code, "other_redacted");
        let text = std::fs::read_to_string(&path).unwrap();
        let record: Value = serde_json::from_str(text.trim()).unwrap();
        assert_eq!(record["type"], "invalid_request_error");
        assert_eq!(record["code"], "private-code");
        assert_eq!(record["message"], "Rejected [redacted]");
        assert!(!text.contains("header-secret"));
        assert!(!text.contains(secret));
        server.await.unwrap();
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
        let observation = send_once(&client, &frozen, request(), &mut budget, None, &[])
            .await
            .unwrap();
        assert_eq!(observation.status, Some(307));
        assert_eq!(budget.sent, 1);
        budget.take().unwrap();
        budget.take().unwrap();
        assert_eq!(
            send_once(&client, &frozen, request(), &mut budget, None, &[])
                .await
                .err(),
            Some("family_request_cap")
        );
        server.await.unwrap();
    }
}
