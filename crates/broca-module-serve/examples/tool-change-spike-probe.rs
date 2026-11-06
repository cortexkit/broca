//! Prompt-cache and tool/instruction-change spike. No vault or network access in --dry-run.
//!
//! Usage: tool-change-spike-probe [--dry-run] --provider openai|kimi|deepseek|xai|gemini
//! --credential <id> --model <model> --out <file.jsonl> [--case 1|2|3|4|all]
//! [--max-requests N]. The output contains prompt text and model responses, never headers.
//! Gemini uses VALIDATED with a fixed mode so only its allowed list changes; support
//! depends on model/API version. Its appended instruction changes systemInstruction
//! at request level, not a nonexistent mid-conversation system role.

use broca_module_serve::{vault_config::load_devtools_handle, CredentialId};
use broca_protocol::{
    request::{CallOptions, Tool},
    BindIdentity, ContentBlock, Message, Role,
};
use broca_provider::{
    families::{
        gemini_frozen_config, openai_chat_frozen_config, openai_responses_frozen_config, Gemini,
        OpenAiChat, OpenAiResponses,
    },
    wire::{AuthHandle, FrozenRenderConfig, HttpRequest, WireFamily},
};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::{
    io::Write,
    sync::{Arc, Mutex},
    time::Duration,
};

const REQUEST_CAP: usize = 30;
const NEW_TOOL: &str = "lookup_orbit_code";
const CHANGED_TOOL: &str = "lookup_station";
const INSTRUCTION: &str = "From now on, end every plain-text reply with the word KITE.";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    Openai,
    Kimi,
    Deepseek,
    Xai,
    Gemini,
}

impl Provider {
    fn family(self) -> &'static dyn WireFamily {
        match self {
            Self::Openai => &OpenAiResponses,
            Self::Gemini => &Gemini,
            _ => &OpenAiChat,
        }
    }

    fn frozen(self, model: &str) -> FrozenRenderConfig {
        let mut frozen = match self {
            Self::Openai => openai_responses_frozen_config(model),
            Self::Gemini => gemini_frozen_config(model),
            _ => openai_chat_frozen_config(model),
        };
        frozen.max_output_tokens = 2048;
        frozen.base_url = match self {
            // The only Kimi key is the coding plan, served on its own endpoint.
            Self::Kimi => Some("https://api.kimi.ai/coding/v1".into()),
            Self::Deepseek => Some("https://api.deepseek.com/v1".into()),
            Self::Xai => Some("https://api.x.ai/v1".into()),
            _ => None,
        };
        frozen
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "openai" => Ok(Self::Openai),
            "kimi" => Ok(Self::Kimi),
            "deepseek" => Ok(Self::Deepseek),
            "xai" => Ok(Self::Xai),
            "gemini" => Ok(Self::Gemini),
            _ => Err(format!(
                "unknown provider {value}; expected openai|kimi|deepseek|xai|gemini"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Kimi => "kimi",
            Self::Deepseek => "deepseek",
            Self::Xai => "xai",
            Self::Gemini => "gemini",
        }
    }

    fn route(self) -> &'static str {
        match self {
            Self::Openai => "openai-responses",
            Self::Gemini => "gemini",
            _ => "openai-chat",
        }
    }
}

struct Args {
    dry_run: bool,
    provider: Provider,
    credential: String,
    model: String,
    out: PathBuf,
    case: Option<u8>,
    max_requests: usize,
}

impl Args {
    fn parse(args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut args = args;
        let mut dry_run = false;
        let mut provider = None;
        let mut credential = None;
        let mut model = None;
        let mut out = None;
        let mut case = None;
        let mut max_requests = REQUEST_CAP;
        while let Some(flag) = args.next() {
            if flag == "--dry-run" {
                dry_run = true;
                continue;
            }
            if !matches!(
                flag.as_str(),
                "--provider" | "--credential" | "--model" | "--out" | "--case" | "--max-requests"
            ) {
                return Err(format!(
                    "unknown option {flag}; see the example's usage comment"
                ));
            }
            let value = args
                .next()
                .ok_or_else(|| format!("{flag} requires a value"))?;
            match flag.as_str() {
                "--provider" => provider = Some(Provider::parse(&value)?),
                "--credential" => credential = Some(value),
                "--model" => model = Some(value),
                "--out" => out = Some(PathBuf::from(value)),
                "--case" => {
                    case = match value.as_str() {
                        "all" => None,
                        "1" => Some(1),
                        "2" => Some(2),
                        "3" => Some(3),
                        "4" => Some(4),
                        _ => return Err("--case must be 1|2|3|4|all".into()),
                    };
                }
                "--max-requests" => {
                    max_requests = value
                        .parse()
                        .map_err(|_| "--max-requests requires an integer".to_string())?;
                    if !(1..=REQUEST_CAP).contains(&max_requests) {
                        return Err(
                            "--max-requests must be between 1 and the hard cap of 30".into()
                        );
                    }
                }
                _ => unreachable!(),
            }
        }
        let result = Self {
            dry_run,
            provider: provider.ok_or("--provider is required")?,
            credential: credential
                .filter(|v| !v.is_empty())
                .ok_or("--credential is required")?,
            model: model
                .filter(|v| !v.is_empty())
                .ok_or("--model is required")?,
            out: out.ok_or("--out is required")?,
            case,
            max_requests,
        };
        let minimum = result.variants().len() * 4;
        if minimum > result.max_requests {
            return Err(format!("selected cases need at least {minimum} requests; narrow --case or raise --max-requests"));
        }
        Ok(result)
    }

    fn variants(&self) -> Vec<Variant> {
        let mut variants = Vec::new();
        for case in 1..=4 {
            if self.case.is_some_and(|selected| selected != case) {
                continue;
            }
            variants.push(Variant {
                case,
                alternate: false,
            });
            if case == 1 && matches!(self.provider, Provider::Openai | Provider::Kimi) {
                variants.push(Variant {
                    case,
                    alternate: true,
                });
            }
        }
        variants
    }
}

#[derive(Clone, Copy, Debug)]
struct Variant {
    case: u8,
    alternate: bool,
}

impl Variant {
    fn name(self) -> String {
        format!("{}{}", self.case, if self.alternate { "b" } else { "a" })
    }
}

fn fixed_prefix() -> String {
    let mut text = String::from(
        "You are a concise reference assistant. Use tools for station and orbit lookups; never invent their results. Answer ordinary questions directly without tools. Tool results are authoritative.\nFixed reference archive:\n",
    );
    // Repeated, numbered reference sentences keep this above the 4,096-token cache
    // minimum without depending on random bytes, timestamps, or the chosen model.
    for index in 0..320 {
        text.push_str(&format!("Archive {index:04}: amber maps describe quiet rivers; silver charts record northern valleys.\n"));
    }
    text
}

fn user_text(variant: Variant, step: u8) -> &'static str {
    match step {
        1 => "Reply with the word READY. Do not call tools.",
        2 => "What color are the maps in the archive? One short sentence; no tools.",
        3 => "And what do the silver charts record? One short sentence; no tools.",
        4 => match variant.case {
            1 => "Call lookup_orbit_code for orbit LYRA. Only that tool knows the secret code; do not guess.",
            2 => "Look up station VEGA using lookup_station if it is available; otherwise say unavailable. Do not use another tool as a substitute.",
            3 => "Call lookup_station for station VEGA, using verification_tag KITE. Do not guess its status.",
            4 => "Say the archive is understood in one short sentence. Do not call tools.",
            _ => unreachable!(),
        },
        _ => unreachable!(),
    }
}

fn message(role: Role, text: &str) -> Message {
    Message::new(role, vec![ContentBlock::Text { text: text.into() }])
}

fn tool(name: &str, changed: bool) -> Tool {
    let mut schema = json!({"type":"object", "properties":{"name":{"type":"string"}}, "required":["name"], "additionalProperties":false});
    if changed {
        schema["properties"]["verification_tag"] = json!({"type":"string", "enum":["KITE"]});
        schema["required"]
            .as_array_mut()
            .unwrap()
            .push(json!("verification_tag"));
    }
    Tool {
        name: name.into(),
        description: format!("Return the authoritative secret reference value for {name}."),
        input_schema: schema,
        module: None,
        execution_mode: Default::default(),
        reply: None,
    }
}

fn options(
    provider: Provider,
    variant: Variant,
    changed: bool,
    mut prompt: Vec<Message>,
) -> CallOptions {
    if changed && variant.case == 4 {
        let mut instruction = message(Role::System, INSTRUCTION);
        instruction.turn_system = provider == Provider::Openai;
        let t4 = prompt
            .iter()
            .position(|message| {
                message.content
                    == vec![ContentBlock::Text {
                        text: user_text(variant, 4).into(),
                    }]
            })
            .expect("T4 user message");
        prompt.insert(t4, instruction);
    }
    let mut opts = CallOptions::new(prompt);
    let mut tools = vec![
        tool(CHANGED_TOOL, changed && variant.case == 3),
        tool("lookup_river", false),
        tool("lookup_valley", false),
    ];
    if provider == Provider::Gemini && variant.case == 2
        || variant.case == 1
            && (provider == Provider::Gemini
                || (provider == Provider::Openai && variant.alternate)
                || (changed
                    && (variant.alternate
                        || provider != Provider::Openai && provider != Provider::Kimi)))
    {
        tools.push(tool(NEW_TOOL, false));
    }
    if variant.case == 2 && changed && !matches!(provider, Provider::Openai | Provider::Gemini) {
        tools.remove(0);
    }
    opts.tools = tools.into();
    opts
}

/// The encoder, endpoint, authentication, transport, and decoder remain production.
/// Only changes the production family cannot express are patched into its rendered body.
struct ProbeFamily {
    provider: Provider,
    variant: Variant,
    changed: bool,
    body: Arc<Mutex<Vec<u8>>>,
}

impl ProbeFamily {
    fn patches(&self) -> Vec<&'static str> {
        let mut paths = Vec::new();
        match (self.provider, self.variant.case) {
            (Provider::Openai, 1) if !self.variant.alternate && self.changed => {
                paths.push("/input/- (additional_tools)")
            }
            (Provider::Kimi, 1) if !self.variant.alternate && self.changed => {
                paths.push("/messages/- (system.tools)")
            }
            (Provider::Openai, 1) if self.variant.alternate => {
                paths.push("/tool_choice (allowed_tools)")
            }
            (Provider::Openai, 2) => paths.push("/tool_choice (allowed_tools)"),
            (Provider::Gemini, 1 | 2) => {
                paths.push("/toolConfig/functionCallingConfig (allowedFunctionNames, VALIDATED)")
            }
            _ => {}
        }
        paths
    }

    fn patch(&self, mut body: Value) -> Value {
        let provider = self.provider;
        let variant = self.variant;
        let changed = self.changed;
        let mut allowed = vec![CHANGED_TOOL, "lookup_river", "lookup_valley"];
        if provider == Provider::Gemini && variant.case == 2 {
            allowed.push(NEW_TOOL);
        }
        if variant.case == 1 && changed {
            allowed.push(NEW_TOOL);
        }
        if variant.case == 2 && changed {
            allowed.remove(0);
        }
        if (variant.case == 1 && variant.alternate || variant.case == 2)
            && provider == Provider::Openai
        {
            body["tool_choice"] = json!({"type":"allowed_tools", "mode":"auto", "tools":allowed.iter().map(|name| json!({"type":"function", "name":name})).collect::<Vec<_>>()});
        }
        if matches!(variant.case, 1 | 2) && provider == Provider::Gemini {
            body["toolConfig"] = json!({"functionCallingConfig":{"mode":"VALIDATED", "allowedFunctionNames":allowed}});
        }
        let addition = if changed && variant.case == 1 && !variant.alternate {
            match provider {
                Provider::Openai => Some(
                    json!({"type":"additional_tools", "role":"developer", "tools":[{"type":"function", "name":NEW_TOOL, "description":tool(NEW_TOOL, false).description, "parameters":tool(NEW_TOOL, false).input_schema}]}),
                ),
                Provider::Kimi => Some(
                    json!({"role":"system", "tools":[{"type":"function", "function":{"name":NEW_TOOL, "description":tool(NEW_TOOL, false).description, "parameters":tool(NEW_TOOL, false).input_schema}}]}),
                ),
                _ => None,
            }
        } else {
            None
        };
        if let Some(addition) = addition {
            let key = if provider == Provider::Openai {
                "input"
            } else {
                "messages"
            };
            let messages = body[key].as_array_mut().unwrap();
            // The change belongs immediately before T4's user message and stays
            // there if a tool-result continuation adds further messages.
            let t4 = messages
                .iter()
                .position(|item| item.to_string().contains(user_text(variant, 4)))
                .expect("T4 user message");
            messages.insert(t4, addition);
        }
        body
    }
}

fn refusal() -> broca_protocol::ProviderError {
    serde_json::from_value(
        json!({"class":"permanent", "message":"probe forbids automatic reissues"}),
    )
    .unwrap()
}

/// A spike measures one physical request, never the transport's retry aggregate.
/// Refuse a second send (including a gzip fallback) before it can reach the socket.
struct OneSend(std::sync::atomic::AtomicBool);

#[async_trait::async_trait]
impl broca_core::run::AttemptHook for OneSend {
    async fn before_send(
        &self,
        _send: broca_core::run::PhysicalSend,
        _cancel: tokio_util::sync::CancellationToken,
    ) -> Result<broca_wal::record::AttemptId, broca_protocol::ProviderError> {
        if self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(refusal());
        }
        broca_core::attempt_id::mint_attempt_id().map_err(|_| refusal())
    }
    async fn finished(
        &self,
        _id: broca_wal::record::AttemptId,
        _attempt: broca_protocol::ModelAttempt,
        _outcome: broca_wal::record::BillingAttemptOutcome,
        _response: Option<broca_wal::record::AttemptEnvelope>,
    ) -> Result<(), broca_protocol::ProviderError> {
        Ok(())
    }
}

#[derive(Default)]
struct Answer {
    content: Vec<ContentBlock>,
    usage: broca_protocol::Usage,
    status: Option<u16>,
    error: Option<&'static str>,
}

async fn send(
    family: ProbeFamily,
    frozen: FrozenRenderConfig,
    auth: AuthHandle,
    opts: &CallOptions,
) -> Answer {
    use broca_core::Provider as _;
    use broca_protocol::{NormalizedEvent, ProviderControl, StepId};
    let live = broca_provider::LiveProvider::new(family, frozen, auth);
    let cancel = tokio_util::sync::CancellationToken::new();
    let mut events = live
        .step_with_runtime(
            StepId(1),
            Arc::new(opts.clone()),
            broca_core::provider::StepRuntime {
                cancel: cancel.clone(),
                deadline: Some(tokio::time::Instant::now() + Duration::from_secs(180)),
                seal: None,
                progress: None,
                first_attempt: 0,
                attempt_hook: Some(Arc::new(OneSend(false.into()))),
            },
        )
        .await;
    let mut answer = Answer::default();
    let exchange = async {
        let mut finished = false;
        while let Some(event) = events.recv().await {
            match event {
                NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                    if answer.status.is_none() {
                        answer.status = attempt.http_status;
                    }
                    if attempt
                        .http_status
                        // A WebSocket request records its 101 upgrade status, not a 2xx.
                        .is_some_and(|status| status != 101 && !(200..300).contains(&status))
                    {
                        answer.error = Some("non-2xx response; response body withheld");
                        cancel.cancel();
                        return;
                    }
                }
                NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                    answer.content = message.content
                }
                NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => {
                    answer.usage = usage;
                    finished = true;
                }
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                    if answer.status.is_none() {
                        answer.status = error.status;
                    }
                    answer.error = Some("provider failure; error text withheld");
                }
                _ => {}
            }
        }
        if !finished && answer.error.is_none() {
            answer.error = Some("stream ended without terminal success");
        }
    };
    if tokio::time::timeout(Duration::from_secs(185), exchange)
        .await
        .is_err()
    {
        cancel.cancel();
        answer.error = Some("request deadline exceeded");
    }
    answer
}

fn behaviour(variant: Variant, step: u8, answer: Option<&Answer>) -> Value {
    let Some(answer) = answer.filter(|answer| step == 4 && answer.error.is_none()) else {
        return json!({"result":"n/a", "why":"dry-run, baseline, or unsuccessful request"});
    };
    let calls: Vec<_> = answer
        .content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => Some(call),
            _ => None,
        })
        .collect();
    let (matched, why) = match variant.case {
        1 => (
            calls.iter().any(|call| call.tool_name == NEW_TOOL),
            "reply must call lookup_orbit_code",
        ),
        2 => (
            !calls.iter().any(|call| call.tool_name == CHANGED_TOOL),
            "reply must not call removed lookup_station",
        ),
        3 => (
            calls
                .iter()
                .find(|call| call.tool_name == CHANGED_TOOL)
                .is_some_and(|call| call.input.get("verification_tag") == Some(&json!("KITE"))),
            "lookup_station must supply verification_tag=KITE",
        ),
        4 => {
            let text: String = answer
                .content
                .iter()
                .filter_map(|block| match block {
                    ContentBlock::Text { text } => Some(text.as_str()),
                    _ => None,
                })
                .collect();
            // The reply's last 40 characters go in the row, so a near miss such as a trailing
            // full stop is visible rather than hidden behind "not_matched".
            let tail: String = {
                let chars: Vec<char> = text.trim_end().chars().collect();
                chars[chars.len().saturating_sub(40)..].iter().collect()
            };
            let matched = text.split_whitespace().last() == Some("KITE");
            return json!({"result":if matched {"matched"} else {"not_matched"},
                "why":"last whitespace-delimited word must be KITE", "reply_tail":tail});
        }
        _ => unreachable!(),
    };
    json!({"result":if matched {"matched"} else {"not_matched"}, "why":why})
}

fn write_row(
    out: &mut std::fs::File,
    args: &Args,
    variant: Variant,
    position: (u8, bool),
    body: &[u8],
    patches: &[&str],
    answer: Option<&Answer>,
) -> Result<(), Box<dyn std::error::Error>> {
    let (step, completing) = position;
    let assessment = behaviour(
        variant,
        if completing && variant.case != 4 {
            0
        } else {
            step
        },
        answer,
    );
    let mut row = json!({"provider":args.provider.name(), "model":args.model, "route":args.provider.route(), "case":variant.case, "variant":variant.name(), "step":format!("T{step}"), "ts_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis(), "behaviour":assessment["result"], "why":assessment["why"], "patched":!patches.is_empty(), "patched_paths":patches, "continuation":completing, "usage_semantics":"broca normalized: input_tokens is fresh input; cached and writes are separate; output includes reasoning"});
    if let Some(tail) = assessment.get("reply_tail") {
        row["reply_tail"] = tail.clone();
    }
    if variant.case == 4 && args.provider == Provider::Gemini {
        row["instruction_form"] =
            json!("request-level systemInstruction; Gemini has no mid-conversation system role");
    }
    if step == 4 {
        row["request_body"] = serde_json::from_slice(body)?;
    }
    if let Some(answer) = answer {
        for (key, value) in [
            ("http_status", answer.status.map(u64::from)),
            ("input_tokens", answer.usage.input_tokens),
            ("cached_tokens", answer.usage.cached_input_tokens),
            ("cache_write_tokens", answer.usage.cache_write_tokens),
            ("output_tokens", answer.usage.output_tokens),
        ] {
            if let Some(value) = value {
                row[key] = json!(value);
            }
        }
        if let Some(error) = answer.error {
            row["error"] = json!(error);
        }
    }
    serde_json::to_writer(&mut *out, &row)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

fn tool_results(content: &[ContentBlock]) -> Option<Message> {
    let results: Vec<_> = content
        .iter()
        .filter_map(|block| match block {
            ContentBlock::ToolCall(call) => {
                Some(ContentBlock::ToolResult(broca_protocol::ToolResult {
                    tool_call_id: call.tool_call_id.clone(),
                    output: broca_protocol::ToolOutput::Text {
                        text: "{\"value\":\"FIXED-REFERENCE-42\"}".into(),
                    },
                    is_error: false,
                }))
            }
            _ => None,
        })
        .collect();
    if results.is_empty() {
        None
    } else {
        Some(Message::new(Role::Tool, results))
    }
}

async fn credentials(args: &Args) -> Result<AuthHandle, Box<dyn std::error::Error>> {
    let id = CredentialId::parse(&args.credential).ok_or("invalid credential id")?;
    let handle = load_devtools_handle(&id).map_err(|_| "devtools credential handle unavailable")?;
    let subc = std::env::var_os("BROCA_LIVE_SUBC")
        .or_else(|| std::env::var_os("SUBC_SOCKET"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let conn = broca_subc::SubcConnection::connect(&subc)
        .await
        .map_err(|_| "cannot connect to subc")?;
    let identity = BindIdentity::new(
        std::env::current_dir()?,
        "tool-change-spike-probe",
        "tool-change-spike-probe",
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
        .map_err(|_| "vault credential.get refused")?;
    Ok(AuthHandle::Headers(vec![(
        if args.provider == Provider::Gemini {
            "x-goog-api-key"
        } else {
            "authorization"
        }
        .into(),
        if args.provider == Provider::Gemini {
            secret.token
        } else {
            format!("Bearer {}", secret.token)
        },
    )]))
}

fn fake_reply(step: u8) -> Message {
    message(
        Role::Assistant,
        match step {
            1 => "READY",
            2 => "The maps are amber.",
            _ => "The silver charts record northern valleys.",
        },
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse(std::env::args().skip(1))?;
    // This branch precedes even credential-id parsing and vault-handle loading.
    let auth = if args.dry_run {
        None
    } else {
        Some(credentials(&args).await?)
    };
    let mut out = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args.out)?;
    let mut requests = 0;
    for variant in args.variants() {
        let mut prompt = vec![message(Role::System, &fixed_prefix())];
        let mut t3 = Value::Null;
        for step in 1..=4 {
            prompt.push(message(Role::User, user_text(variant, step)));
            let mut completing = false;
            loop {
                if requests == args.max_requests {
                    return Err("request cap reached before conversation completed".into());
                }
                let opts = options(args.provider, variant, step == 4, prompt.clone());
                let body = Arc::new(Mutex::new(Vec::new()));
                let family = ProbeFamily {
                    provider: args.provider,
                    variant,
                    changed: step == 4,
                    body: body.clone(),
                };
                let patches = family.patches();
                let frozen = args.provider.frozen(&args.model);
                let answer = if let Some(auth) = &auth {
                    Some(send(family, frozen, auth.clone(), &opts).await)
                } else {
                    family.render(&frozen, &opts);
                    None
                };
                requests += 1;
                let bytes = body.lock().unwrap().clone();
                write_row(
                    &mut out,
                    &args,
                    variant,
                    (step, completing),
                    &bytes,
                    &patches,
                    answer.as_ref(),
                )?;
                if args.dry_run {
                    let rendered: Value = serde_json::from_slice(&bytes)?;
                    println!(
                        "{} {} T{step}: {} body bytes; patched paths={patches:?}",
                        args.provider.name(),
                        variant.name(),
                        bytes.len()
                    );
                    if step == 3 {
                        t3 = rendered.clone();
                    }
                    if step == 4 {
                        let mut changes = Vec::new();
                        diff(&t3, &rendered, "", &mut changes);
                        println!("T4 against T3: {}", serde_json::to_string(&changes)?);
                    }
                    prompt.push(fake_reply(step));
                    break;
                }
                let answer = answer.unwrap();
                if answer.error.is_some() {
                    return Err(format!(
                        "{} {} T{step} failed; measured row flushed, response withheld",
                        args.provider.name(),
                        variant.name()
                    )
                    .into());
                }
                let results = tool_results(&answer.content);
                prompt.push(Message::new(Role::Assistant, answer.content));
                if let Some(results) = results {
                    prompt.push(results);
                    completing = true;
                } else {
                    break;
                }
            }
        }
    }
    Ok(())
}

impl WireFamily for ProbeFamily {
    fn shape(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Value {
        self.patch(self.provider.family().shape(frozen, opts))
    }
    fn render(&self, frozen: &FrozenRenderConfig, opts: &CallOptions) -> Vec<u8> {
        self.render_with_provenance(frozen, opts, None)
    }
    fn shape_with_provenance(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Value {
        self.patch(
            self.provider
                .family()
                .shape_with_provenance(frozen, opts, provenance),
        )
    }
    fn render_with_provenance(
        &self,
        frozen: &FrozenRenderConfig,
        opts: &CallOptions,
        provenance: Option<&mut broca_protocol::render_provenance::RenderProvenance>,
    ) -> Vec<u8> {
        let rendered = self
            .provider
            .family()
            .render_with_provenance(frozen, opts, provenance);
        let bytes = if self.patches().is_empty() {
            rendered
        } else {
            serde_json::to_vec(&self.patch(serde_json::from_slice(&rendered).expect("family JSON")))
                .unwrap()
        };
        *self.body.lock().unwrap() = bytes.clone();
        bytes
    }
    fn endpoint(
        &self,
        frozen: &FrozenRenderConfig,
        auth: &AuthHandle,
        body: Vec<u8>,
    ) -> HttpRequest {
        let request = self.provider.family().endpoint(frozen, auth, body);
        *self.body.lock().unwrap() = request.body.clone();
        request
    }
    fn normalizer(
        &self,
        frozen: &FrozenRenderConfig,
        step: broca_protocol::StepId,
    ) -> Box<dyn broca_provider::wire::StreamNormalizer> {
        self.provider.family().normalizer(frozen, step)
    }
}

fn diff(before: &Value, after: &Value, path: &str, changes: &mut Vec<Value>) {
    if before == after {
        return;
    }
    if let (Some(left), Some(right)) = (before.as_object(), after.as_object()) {
        let keys: std::collections::BTreeSet<_> = left.keys().chain(right.keys()).collect();
        for key in keys {
            diff(&before[key], &after[key], &format!("{path}/{key}"), changes);
        }
    } else if let (Some(left), Some(right)) = (before.as_array(), after.as_array()) {
        for index in 0..left.len().max(right.len()) {
            diff(
                &before[index],
                &after[index],
                &format!("{path}/{index}"),
                changes,
            );
        }
    } else {
        changes.push(json!({"path":path, "before":before, "after":after}));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const PROVIDERS: [Provider; 5] = [
        Provider::Openai,
        Provider::Kimi,
        Provider::Deepseek,
        Provider::Xai,
        Provider::Gemini,
    ];

    fn args(provider: Provider) -> Args {
        Args::parse(
            [
                "--dry-run",
                "--provider",
                provider.name(),
                "--credential",
                "deliberately-invalid-dry-run-id",
                "--model",
                "test-model",
                "--out",
                "unused.jsonl",
            ]
            .into_iter()
            .map(str::to_owned),
        )
        .unwrap()
    }

    fn prompt(variant: Variant, step: u8) -> Vec<Message> {
        let mut prompt = vec![message(Role::System, &fixed_prefix())];
        for previous in 1..step {
            prompt.push(message(Role::User, user_text(variant, previous)));
            prompt.push(fake_reply(previous));
        }
        prompt.push(message(Role::User, user_text(variant, step)));
        prompt
    }

    fn render(provider: Provider, variant: Variant, step: u8, changed: bool) -> Value {
        let family = ProbeFamily {
            provider,
            variant,
            changed,
            body: Arc::new(Mutex::new(Vec::new())),
        };
        let opts = options(provider, variant, changed, prompt(variant, step));
        let frozen = provider.frozen("test-model");
        let first = family.render(&frozen, &opts);
        assert_eq!(
            first,
            family.render(&frozen, &opts),
            "{provider:?} {variant:?} T{step} nondeterministic"
        );
        assert_eq!(
            first,
            *family.body.lock().unwrap(),
            "recorded body differs from sent bytes"
        );
        serde_json::from_slice(&first).unwrap()
    }

    fn conversation_key(provider: Provider) -> &'static str {
        match provider {
            Provider::Openai => "input",
            Provider::Gemini => "contents",
            _ => "messages",
        }
    }

    fn declarations(provider: Provider, body: &Value) -> &Vec<Value> {
        if provider == Provider::Gemini {
            body["tools"][0]["functionDeclarations"].as_array().unwrap()
        } else {
            body["tools"].as_array().unwrap()
        }
    }

    #[test]
    fn every_provider_case_is_deterministic_and_changes_only_the_intended_wire_locations() {
        for provider in PROVIDERS {
            for variant in args(provider).variants() {
                let _t1 = render(provider, variant, 1, false);
                let mut t2 = render(provider, variant, 2, false);
                let mut t3 = render(provider, variant, 3, false);
                let key = conversation_key(provider);
                let t2_messages = t2[key].as_array().unwrap().clone();
                let t3_messages = t3[key].as_array().unwrap().clone();
                assert_eq!(
                    &t3_messages[..t2_messages.len()],
                    t2_messages,
                    "T3 must preserve T2's complete message prefix"
                );
                assert_eq!(t3_messages.len(), t2_messages.len() + 2);
                t2.as_object_mut().unwrap().remove(key);
                t3.as_object_mut().unwrap().remove(key);
                assert_eq!(t2, t3, "T3 may only append messages");

                // Independently remove the provider's documented change from T4.
                // What remains must equal a fresh unmodified T4 conversation,
                // including its unchanged system prefix and prior replies.
                let baseline = render(provider, variant, 4, false);
                let mut changed = render(provider, variant, 4, true);
                match variant.case {
                    1 if provider == Provider::Openai && !variant.alternate
                        || provider == Provider::Kimi && !variant.alternate =>
                    {
                        assert_eq!(declarations(provider, &changed).len(), 3);
                        let messages = changed[key].as_array_mut().unwrap();
                        let addition = messages.remove(messages.len() - 2);
                        assert_eq!(
                            addition["role"],
                            if provider == Provider::Openai {
                                "developer"
                            } else {
                                "system"
                            }
                        );
                        assert_eq!(addition["tools"].as_array().unwrap().len(), 1);
                        if provider == Provider::Openai {
                            assert_eq!(addition["type"], "additional_tools");
                            assert_eq!(addition["tools"][0]["name"], NEW_TOOL);
                        } else {
                            assert!(
                                addition.get("content").is_none(),
                                "Kimi dynamic declarations must omit content entirely"
                            );
                            assert_eq!(addition["tools"][0]["function"]["name"], NEW_TOOL);
                        }
                    }
                    1 | 2 if matches!(provider, Provider::Openai | Provider::Gemini) => {
                        let field = if provider == Provider::Openai {
                            "tool_choice"
                        } else {
                            "toolConfig"
                        };
                        assert_eq!(
                            changed["tools"], baseline["tools"],
                            "allowed-list changes must not rewrite definitions"
                        );
                        let allowed = if provider == Provider::Openai {
                            &changed[field]["tools"]
                        } else {
                            &changed[field]["functionCallingConfig"]["allowedFunctionNames"]
                        };
                        assert_eq!(
                            allowed.as_array().unwrap().len(),
                            if variant.case == 1 {
                                4
                            } else if provider == Provider::Gemini {
                                3
                            } else {
                                2
                            }
                        );
                        if provider == Provider::Openai {
                            assert_eq!(changed[field]["type"], "allowed_tools");
                        } else {
                            assert_eq!(
                                changed[field]["functionCallingConfig"]["mode"],
                                "VALIDATED"
                            );
                        }
                        changed[field] = baseline[field].clone();
                    }
                    1 | 2 => {
                        assert_eq!(
                            declarations(provider, &changed).len(),
                            if variant.case == 1 { 4 } else { 2 }
                        );
                        if variant.case == 1 {
                            assert_eq!(changed["tools"][3]["function"]["name"], NEW_TOOL);
                        } else {
                            assert_eq!(changed["tools"][0]["function"]["name"], "lookup_river");
                        }
                        changed["tools"] = baseline["tools"].clone();
                    }
                    3 => {
                        let schema = match provider {
                            Provider::Openai => &changed["tools"][0]["parameters"],
                            Provider::Gemini => {
                                &changed["tools"][0]["functionDeclarations"][0]["parameters"]
                            }
                            _ => &changed["tools"][0]["function"]["parameters"],
                        };
                        assert_eq!(schema["required"], json!(["name", "verification_tag"]));
                        assert_eq!(
                            schema["properties"]["verification_tag"]["enum"],
                            json!(["KITE"])
                        );
                        let mut expected_tools = baseline["tools"].clone();
                        let schema = match provider {
                            Provider::Openai => &mut expected_tools[0]["parameters"],
                            Provider::Gemini => {
                                &mut expected_tools[0]["functionDeclarations"][0]["parameters"]
                            }
                            _ => &mut expected_tools[0]["function"]["parameters"],
                        };
                        schema["required"] = json!(["name", "verification_tag"]);
                        schema["properties"]["verification_tag"] =
                            json!({"type":"string", "enum":["KITE"]});
                        assert_eq!(changed["tools"], expected_tools);
                        changed["tools"] = baseline["tools"].clone();
                    }
                    4 if provider == Provider::Gemini => {
                        let parts = changed["systemInstruction"]["parts"]
                            .as_array_mut()
                            .unwrap();
                        assert_eq!(parts.pop().unwrap(), json!({"text":INSTRUCTION}));
                    }
                    4 => {
                        let messages = changed[key].as_array_mut().unwrap();
                        let instruction = messages.remove(messages.len() - 2);
                        assert_eq!(
                            instruction,
                            if provider == Provider::Openai {
                                json!({"role":"developer", "content":[{"type":"input_text", "text":INSTRUCTION}]})
                            } else {
                                json!({"role":"system", "content":INSTRUCTION})
                            }
                        );
                    }
                    _ => unreachable!(),
                }
                assert_eq!(
                    changed, baseline,
                    "{provider:?} {variant:?}: unrelated T4 change"
                );
            }
        }
    }

    #[test]
    fn budget_and_dry_run_parse_without_a_credential_id() {
        for provider in PROVIDERS {
            assert!(args(provider).dry_run);
        }
        for limit in ["0", "31", "3"] {
            assert!(Args::parse(
                [
                    "--provider",
                    "openai",
                    "--credential",
                    "unused",
                    "--model",
                    "m",
                    "--out",
                    "unused",
                    "--max-requests",
                    limit
                ]
                .into_iter()
                .map(str::to_owned)
            )
            .is_err());
        }
        assert_eq!(refusal().message, "probe forbids automatic reissues");
    }

    #[test]
    fn behaviour_checks_observed_calls_and_instruction_not_requested_intent() {
        let mut answer = Answer::default();
        for case in [1, 3, 4] {
            assert_eq!(
                behaviour(
                    Variant {
                        case,
                        alternate: false
                    },
                    4,
                    Some(&answer)
                )["result"],
                "not_matched"
            );
        }
        assert_eq!(
            behaviour(
                Variant {
                    case: 2,
                    alternate: false
                },
                4,
                Some(&answer)
            )["result"],
            "matched"
        );
        answer.content = vec![ContentBlock::Text {
            text: "Understood KITE".into(),
        }];
        assert_eq!(
            behaviour(
                Variant {
                    case: 4,
                    alternate: false
                },
                4,
                Some(&answer)
            )["result"],
            "matched"
        );
        answer.content = vec![ContentBlock::Text {
            text: "KITE followed by other words".into(),
        }];
        assert_eq!(
            behaviour(
                Variant {
                    case: 4,
                    alternate: false
                },
                4,
                Some(&answer)
            )["result"],
            "not_matched"
        );
    }

    #[test]
    fn jsonl_omits_unmeasured_fields_and_only_t4_contains_the_body() {
        let directory = PathBuf::from("target/scratch");
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join(format!(
            "tool-change-spike-rows-{}.jsonl",
            std::process::id()
        ));
        let mut file = std::fs::File::create(&path).unwrap();
        let args = args(Provider::Openai);
        let variant = Variant {
            case: 1,
            alternate: false,
        };
        write_row(&mut file, &args, variant, (3, false), b"{}", &[], None).unwrap();
        write_row(
            &mut file,
            &args,
            variant,
            (4, false),
            b"{\"input\":[]}",
            &["/input/-"],
            Some(&Answer::default()),
        )
        .unwrap();
        let text = std::fs::read_to_string(&path).unwrap();
        std::fs::remove_file(path).unwrap();
        let rows: Vec<Value> = text
            .lines()
            .map(|line| serde_json::from_str(line).unwrap())
            .collect();
        for row in &rows {
            for field in [
                "http_status",
                "input_tokens",
                "cached_tokens",
                "cache_write_tokens",
                "output_tokens",
            ] {
                assert!(row.get(field).is_none(), "invented {field}");
            }
        }
        assert_eq!(rows[0]["behaviour"], "n/a");
        assert_eq!(rows[1]["behaviour"], "not_matched");
        assert!(rows[0].get("request_body").is_none());
        assert_eq!(rows[1]["request_body"], json!({"input":[]}));
        assert_eq!(rows[1]["patched"], true);
    }
}
