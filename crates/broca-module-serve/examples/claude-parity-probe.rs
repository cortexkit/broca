//! Live Claude OAuth persona check: `cargo run -p broca-module-serve --example claude-parity-probe
//! -- [--dry-run] <credential-id> [model]`.
//!
//! Sends one short two-step tool run on a Claude subscription credential and reports, per step,
//! the HTTP status, Anthropic's `request-id` and the prompt-cache read and write. Step one is
//! rendered with the recorded 2.1.280 persona; step two answers its tool call with the
//! 2.1.302 persona. Both use the production transport (request gzip on) and share one large
//! fixed system prefix, so step two's cache read measures the version transition.
//!
//! The credential is fetched from claustrum through the handle in broca's dev-tools handle file
//! and is never printed: request headers are listed without `authorization`, and an error
//! response is reduced to its `error.type` and `error.message`. `--dry-run` fetches no
//! credential and sends nothing; it prints the two requests as they would be sent.
use std::{sync::Arc, time::Duration};

use broca_module_serve::{claude_code_identity, vault_config::load_devtools_handle, CredentialId};
use broca_protocol::{
    request::{CallOptions, Tool, ToolChoice},
    BindIdentity, ContentBlock, Message, Role, ToolCall, ToolCallId, ToolOutput, ToolResult,
};
use broca_provider::{
    families::{anthropic_frozen_config, AnthropicMessages},
    wire::{
        AuthHandle, CacheClass, CachePolicy, CacheTtl, ClaudeCodePersona, FrozenRenderConfig,
        HttpRequest, MarkerSyntax, WireFamily,
    },
};
use serde_json::{json, Value};

const TOOL: &str = "get_secret_number";

fn text_message(role: Role, text: String) -> Message {
    Message::new(role, vec![ContentBlock::Text { text }])
}

/// About 16k tokens of inert, fixed text: long enough to be cached, identical on both steps.
fn fixed_prefix() -> String {
    let mut prefix = String::from(
        "You are a careful assistant. The numbered reference rows below are inert data; never \
         analyze or repeat them.\n",
    );
    for row in 0..1100 {
        prefix.push_str(&format!(
            "reference {row:05}: amber cobalt delta echo forest granite harbor ivory.\n"
        ));
    }
    prefix
}

fn request_options() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        text_message(Role::System, fixed_prefix()),
        text_message(
            Role::User,
            format!("Call {TOOL} exactly once, then reply with only the number it returns."),
        ),
    ]);
    opts.tools = vec![Tool {
        name: TOOL.into(),
        description: "Return the secret number.".into(),
        input_schema: json!({"type": "object", "properties": {}}),
        module: None,
        execution_mode: Default::default(),
    }]
    .into();
    opts.tool_choice = ToolChoice::Auto.into();
    opts
}

fn frozen(model: &str, persona: Option<ClaudeCodePersona>) -> FrozenRenderConfig {
    let mut frozen = anthropic_frozen_config(model);
    frozen.max_output_tokens = 2048;
    // broca's default Anthropic cache policy: markers with the provider's 5-minute lifetime.
    frozen.cache = Some(CachePolicy {
        version: 1,
        class: CacheClass::Markers {
            syntax: MarkerSyntax::AnthropicCacheControl,
            ttl: CacheTtl::FiveMinutes,
            max_breakpoints: 4,
            lookback_blocks: 20,
            tail: None,
            omit_ttl: true,
        },
    });
    frozen.claude_code_persona = persona;
    frozen
}

/// The request without its credential, for printing.
fn describe(step: u32, shape: &str, request: &HttpRequest) {
    println!("step {step} ({shape}): POST {}", request.url);
    for (name, value) in &request.headers {
        if !name.eq_ignore_ascii_case("authorization") {
            println!("  {name}: {value}");
        }
    }
    let body: Value = serde_json::from_slice(&request.body).unwrap_or(Value::Null);
    println!("  body bytes: {}", request.body.len());
    println!(
        "  system[0]: {}",
        body["system"][0]["text"]
            .as_str()
            .map(|text| text.chars().take(160).collect::<String>())
            .unwrap_or_default()
    );
}

/// What one streamed response carried: the assistant content and the cache usage.
struct Answer {
    content: Vec<ContentBlock>,
    input_tokens: u64,
    cache_read: u64,
    cache_write: u64,
}

/// Observe response ids without replacing the transport, encoder or Anthropic normalizer.
struct ProbeFamily(Arc<std::sync::Mutex<Option<String>>>);
impl WireFamily for ProbeFamily {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        AnthropicMessages.shape(frozen, opts)
    }
    fn render(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Vec<u8> {
        AnthropicMessages.render(frozen, opts)
    }
    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        AnthropicMessages.endpoint(frozen, auth, body)
    }
    fn normalizer(
        &self,
        frozen: &FrozenRenderConfig,
        step: broca_protocol::StepId,
    ) -> Box<dyn broca_provider::wire::StreamNormalizer> {
        Box::new(ProbeNormalizer {
            inner: AnthropicMessages.normalizer(frozen, step),
            request_id: self.0.clone(),
        })
    }
}
struct ProbeNormalizer {
    inner: Box<dyn broca_provider::wire::StreamNormalizer>,
    request_id: Arc<std::sync::Mutex<Option<String>>>,
}
impl broca_provider::wire::StreamNormalizer for ProbeNormalizer {
    fn push(
        &mut self,
        chunk: &[u8],
    ) -> Result<Vec<broca_protocol::NormalizedEvent>, broca_provider::wire::NormalizeError> {
        self.inner.push(chunk)
    }
    fn finish(
        &mut self,
    ) -> Result<Vec<broca_protocol::NormalizedEvent>, broca_provider::wire::NormalizeError> {
        self.inner.finish()
    }
    fn observe_response_headers(&mut self, headers: &broca_provider::wire::ResponseHeaders) {
        *self.request_id.lock().unwrap() = headers.get("request-id").map(str::to_owned);
        self.inner.observe_response_headers(headers);
    }
    fn content_progress(&self) -> broca_protocol::provider::ContentProgress {
        self.inner.content_progress()
    }
    fn progress_marker(&self) -> u64 {
        self.inner.progress_marker()
    }
    fn liveness_marker(&self) -> Option<u64> {
        self.inner.liveness_marker()
    }
    fn last_event_type(&self) -> Option<&str> {
        self.inner.last_event_type()
    }
    fn keepalives_after_content(&self) -> Option<u64> {
        self.inner.keepalives_after_content()
    }
    fn event_type_counts(&self) -> Option<&[(String, u64)]> {
        self.inner.event_type_counts()
    }
    fn input_transformations(&self) -> Option<Vec<broca_protocol::InputTransformation>> {
        self.inner.input_transformations()
    }
    fn input_transformations_unmeasured(&self) -> bool {
        self.inner.input_transformations_unmeasured()
    }
}

async fn send(
    step: u32,
    shape: &str,
    frozen: &FrozenRenderConfig,
    auth: &AuthHandle,
    opts: &CallOptions,
) -> Result<Answer, Box<dyn std::error::Error>> {
    use broca_core::Provider;
    use broca_protocol::{NormalizedEvent, ProviderControl, StepId};
    let request_id = Arc::new(std::sync::Mutex::new(None));
    let provider = broca_provider::LiveProvider::new(
        ProbeFamily(request_id.clone()),
        frozen.clone(),
        auth.clone(),
    );
    let mut events = provider.step(StepId(u64::from(step)), opts).await;
    let exchange = async {
        let mut answer = Answer {
            content: Vec::new(),
            input_tokens: 0,
            cache_read: 0,
            cache_write: 0,
        };
        let mut status = None;
        let mut finished = false;
        while let Some(event) = events.recv().await {
            match event {
                NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                    status = attempt.http_status
                }
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    answer.content = message.content
                }
                NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => {
                    answer.input_tokens = usage.input_tokens.unwrap_or(0);
                    answer.cache_read = usage.cached_input_tokens.unwrap_or(0);
                    answer.cache_write = usage.cache_write_tokens.unwrap_or(0);
                    finished = true;
                }
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                    println!(
                        "step {step} ({shape}): status={:?} request_id={} error_class={:?}",
                        error.status,
                        request_id.lock().unwrap().as_deref().unwrap_or("-"),
                        error.class
                    );
                    return Err(format!("step {step} failed").into());
                }
                _ => {}
            }
        }
        if !finished {
            return Err("stream ended without a terminal success".into());
        }
        println!("step {step} ({shape}): status={} request_id={} input_tokens={} cache_read={} cache_write={}",
            status.map_or("-".into(), |status| status.to_string()), request_id.lock().unwrap().as_deref().unwrap_or("-"),
            answer.input_tokens, answer.cache_read, answer.cache_write);
        Ok(answer)
    };
    tokio::time::timeout(Duration::from_secs(180), exchange).await?
}

/// The tool result answering every tool call in `content`.
fn tool_results(content: &[ContentBlock]) -> Message {
    let results = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(ContentBlock::ToolResult(ToolResult {
                tool_call_id: call.tool_call_id.clone(),
                output: ToolOutput::Text { text: "42".into() },
                is_error: false,
            })),
            _ => None,
        })
        .collect();
    Message::new(Role::Tool, results)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args: Vec<String> = std::env::args().skip(1).collect();
    let dry_run = args.first().is_some_and(|arg| arg == "--dry-run");
    if dry_run {
        args.remove(0);
    }
    let credential = args
        .first()
        .cloned()
        .ok_or("usage: claude-parity-probe [--dry-run] <credential-id> [model]")?;
    let model = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "claude-sonnet-5-5".into());
    let id = CredentialId::parse(&credential).ok_or("invalid credential id")?;
    let session = BindIdentity::new(
        std::env::current_dir()?,
        "claude-parity-probe",
        format!("claude-parity-probe-{}", std::process::id()),
    );

    let (auth, account_id) = if dry_run {
        (AuthHandle::Bearer("dry-run-token".into()), None)
    } else {
        let handle = load_devtools_handle(&id)?;
        let subc = std::env::var_os("SUBC_SOCKET")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                    .join(".local/share/cortexkit/run/subc-connection.json")
            });
        let conn = broca_subc::SubcConnection::connect(&subc).await?;
        let identity = BindIdentity::new(
            std::env::current_dir()?,
            "claude-parity-probe",
            "claude-parity-probe",
        );
        let vault = broca_subc::VaultClient::new(
            Arc::new(conn),
            "claustrum",
            &identity,
            Duration::from_secs(10),
        );
        let secret = vault
            .get(&handle, None, false)
            .await
            .result
            .map_err(|error| format!("vault credential.get: {error}"))?;
        (AuthHandle::Bearer(secret.token), secret.account_id)
    };

    // The persona as broca's daemon would freeze it for this credential: the device id from
    // this installation's id, the session id from this probe's session.
    let state_root = broca_module_serve::state_root::resolve_state_root()?;
    let install_id = if dry_run {
        "0".repeat(64)
    } else {
        claude_code_identity::load_or_create_install_id(&state_root.path)?
    };
    let persona = claude_code_identity::fresh_persona(
        &install_id,
        account_id.as_deref(),
        &credential,
        &session,
        claude_code_identity::first_durable_cc_version_suffix(&[], &request_options().prompt),
        1,
    )?;
    println!(
        "credential={credential} model={model} account_uuid={} dry_run={dry_run}",
        if persona.account_uuid.is_some() {
            "known"
        } else {
            "unknown"
        }
    );

    let mut opts = request_options();
    let mut old_persona = persona.clone();
    old_persona.version = "2.1.280".into();
    old_persona.prompt_ordinal = None;
    old_persona.cc_version_suffix =
        broca_provider::families::anthropic_messages::cc_version_suffix_for_transcript(
            &opts.prompt,
            "2.1.280",
        );
    let old_shape = frozen(&model, Some(old_persona));
    let new_shape = frozen(&model, Some(persona));
    let first = AnthropicMessages.endpoint(
        &old_shape,
        &auth,
        AnthropicMessages.render(&old_shape, &opts),
    );

    let answer = if dry_run {
        describe(1, "2.1.280", &first);
        Answer {
            content: vec![ContentBlock::ToolCall(ToolCall {
                thought_signature: None,
                tool_call_id: ToolCallId("toolu_dry_run".into()),
                tool_name: TOOL.into(),
                input: json!({}),
            })],
            input_tokens: 0,
            cache_read: 0,
            cache_write: 0,
        }
    } else {
        send(1, "2.1.280", &old_shape, &auth, &opts).await?
    };
    if !answer
        .content
        .iter()
        .any(|block| matches!(block, ContentBlock::ToolCall(_)))
    {
        return Err("step 1 did not call the tool, so there is no second step".into());
    }
    let results = tool_results(&answer.content);
    opts.prompt
        .push(Message::new(Role::Assistant, answer.content));
    opts.prompt.push(results);
    let second = AnthropicMessages.endpoint(
        &new_shape,
        &auth,
        AnthropicMessages.render(&new_shape, &opts),
    );
    if dry_run {
        describe(2, "2.1.302", &second);
    } else {
        send(2, "2.1.302", &new_shape, &auth, &opts).await?;
    }
    Ok(())
}
