//! Prompt-cache and tool/instruction-change spike. No vault or network access in --dry-run.
//!
//! Usage: tool-change-spike-probe [--dry-run] --provider openai|chatgpt|kimi|deepseek|xai|gemini|anthropic
//! [--credential <id>] --model <model> --out <file.jsonl> [--case 1|2|3|4|all]
//! [--form a|b] [--place before-user|after-user] [--steps 3|4] [--max-requests N]. T4 is the
//! request after the tools or instructions change. On OpenAI and Kimi, `--form` runs one of
//! case 1's two forms (`a` the added-tools declaration, `b` the allowed-tools list).
//! ChatGPT requires `--case 1 --form a` and the `chatgpt:openai` credential for live runs.
//! The ChatGPT lane uses HTTPS to measure the request form, not the WebSocket transport.
//! Anthropic requires `--case 1 --form a --place after-user` and stops at T4's first response.
//! Use `apikey:anthropic` or `oauth:anthropic` for live runs, or one account's credential id
//! such as `oauth:anthropic:<account label>`.
//! Its dry-run needs no credential and prints the API-key shape, with only header names and
//! JSON bodies.
//! Placement defaults to `before-user`; `after-user` requires case 1 form a on OpenAI, ChatGPT,
//! Kimi or Anthropic. `--steps 3` omits T3, the second ordinary question, so T1, T2 and T4 measure
//! the cache with three requests per form. The output contains prompt text and model responses,
//! never header values.
//! Gemini uses VALIDATED with a fixed mode so only its allowed list changes; support
//! depends on model/API version. Its appended instruction changes systemInstruction
//! at request level, not a nonexistent mid-conversation system role.

use broca_module_serve::{vault_config::load_devtools_handle, AuthMethod, CredentialId};
use broca_protocol::{
    request::{CallOptions, Tool},
    BindIdentity, ContentBlock, Message, Role,
};
use broca_provider::{
    families::{
        gemini_frozen_config, openai_chat_frozen_config, openai_responses_chatgpt_frozen_config,
        openai_responses_frozen_config, AnthropicMessages, Gemini, OpenAiChat, OpenAiResponses,
        OpenAiResponsesChatgpt,
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

#[path = "tool_change_spike_probe/anthropic.rs"]
mod anthropic;

const REQUEST_CAP: usize = 30;
const NEW_TOOL: &str = "lookup_orbit_code";
const CHANGED_TOOL: &str = "lookup_station";
const INSTRUCTION: &str = "From now on, end every plain-text reply with the word KITE.";
const CHATGPT_CACHE_KEY: &str = "tool-change-spike-probe";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Provider {
    Openai,
    Chatgpt,
    Kimi,
    Deepseek,
    Xai,
    Gemini,
    Anthropic,
}

impl Provider {
    fn family(self) -> &'static dyn WireFamily {
        match self {
            Self::Openai => &OpenAiResponses,
            Self::Chatgpt => &OpenAiResponsesChatgpt,
            Self::Gemini => &Gemini,
            Self::Anthropic => &AnthropicMessages,
            _ => &OpenAiChat,
        }
    }

    fn frozen(self, model: &str) -> FrozenRenderConfig {
        let mut frozen = match self {
            Self::Openai => openai_responses_frozen_config(model),
            Self::Chatgpt => {
                let mut frozen = openai_responses_chatgpt_frozen_config(model);
                frozen.prompt_cache_key = Some(CHATGPT_CACHE_KEY.into());
                frozen
            }
            Self::Gemini => gemini_frozen_config(model),
            Self::Anthropic => return anthropic::frozen(model, None),
            _ => openai_chat_frozen_config(model),
        };
        frozen.max_output_tokens = 2048;
        // Every endpoint is named here, the family's default included, because a dev-tools
        // handle's reply carries no host list and the probe allows only its endpoint's host.
        frozen.base_url = Some(
            match self {
                Self::Openai => "https://api.openai.com/v1",
                Self::Chatgpt => "https://chatgpt.com/backend-api/codex",
                Self::Gemini => "https://generativelanguage.googleapis.com/v1beta",
                // The only Kimi key is the coding plan, served on its own endpoint.
                Self::Kimi => "https://api.kimi.ai/coding/v1",
                Self::Deepseek => "https://api.deepseek.com/v1",
                Self::Xai => "https://api.x.ai/v1",
                Self::Anthropic => unreachable!("the Anthropic endpoint is configured above"),
            }
            .into(),
        );
        frozen
    }

    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "openai" => Ok(Self::Openai),
            "chatgpt" => Ok(Self::Chatgpt),
            "kimi" => Ok(Self::Kimi),
            "deepseek" => Ok(Self::Deepseek),
            "xai" => Ok(Self::Xai),
            "gemini" => Ok(Self::Gemini),
            "anthropic" => Ok(Self::Anthropic),
            _ => Err(format!(
                "unknown provider {value}; expected openai|chatgpt|kimi|deepseek|xai|gemini|anthropic"
            )),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::Openai => "openai",
            Self::Chatgpt => "chatgpt",
            Self::Kimi => "kimi",
            Self::Deepseek => "deepseek",
            Self::Xai => "xai",
            Self::Gemini => "gemini",
            Self::Anthropic => "anthropic",
        }
    }

    fn route(self) -> &'static str {
        match self {
            Self::Openai => "openai-responses",
            Self::Chatgpt => "openai-responses-chatgpt",
            Self::Gemini => "gemini",
            Self::Anthropic => "anthropic-messages",
            _ => "openai-chat",
        }
    }

    fn additional_tools_key(self) -> Option<&'static str> {
        match self {
            Self::Openai | Self::Chatgpt => Some("input"),
            Self::Kimi | Self::Anthropic => Some("messages"),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Placement {
    BeforeUser,
    AfterUser,
}

impl Placement {
    fn parse(value: &str) -> Result<Self, String> {
        match value {
            "before-user" => Ok(Self::BeforeUser),
            "after-user" => Ok(Self::AfterUser),
            _ => Err("--place must be before-user|after-user".into()),
        }
    }

    fn name(self) -> &'static str {
        match self {
            Self::BeforeUser => "before-user",
            Self::AfterUser => "after-user",
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
    /// Case 1's form on OpenAI and Kimi: `Some(false)` runs only form a, `Some(true)` only
    /// form b, and `None` both.
    alternate: Option<bool>,
    placement: Placement,
    /// Requests per conversation: 4, or 3 without the second ordinary question. The tools
    /// change at the last one, T4, either way.
    steps: u8,
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
        let mut alternate = None;
        let mut placement = Placement::BeforeUser;
        let mut steps = 4;
        while let Some(flag) = args.next() {
            if flag == "--dry-run" {
                dry_run = true;
                continue;
            }
            if !matches!(
                flag.as_str(),
                "--provider"
                    | "--credential"
                    | "--model"
                    | "--out"
                    | "--case"
                    | "--max-requests"
                    | "--form"
                    | "--place"
                    | "--steps"
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
                "--form" => {
                    alternate = Some(match value.as_str() {
                        "a" => false,
                        "b" => true,
                        _ => return Err("--form must be a|b".into()),
                    });
                }
                "--place" => placement = Placement::parse(&value)?,
                "--steps" => {
                    steps = match value.as_str() {
                        "3" => 3,
                        "4" => 4,
                        _ => return Err("--steps must be 3|4".into()),
                    };
                }
                _ => unreachable!(),
            }
        }
        let credential = match credential.filter(|v| !v.is_empty()) {
            Some(credential) => credential,
            None if dry_run && provider == Some(Provider::Anthropic) => String::new(),
            None => return Err("--credential is required".into()),
        };
        let result = Self {
            dry_run,
            provider: provider.ok_or("--provider is required")?,
            credential,
            model: model
                .filter(|v| !v.is_empty())
                .ok_or("--model is required")?,
            out: out.ok_or("--out is required")?,
            case,
            max_requests,
            alternate,
            placement,
            steps,
        };
        let only_form_a = result.case == Some(1) && result.alternate == Some(false);
        if result.provider == Provider::Chatgpt && !only_form_a {
            return Err("--provider chatgpt supports only --case 1 --form a".into());
        }
        if result.provider == Provider::Anthropic
            && !(only_form_a && placement == Placement::AfterUser)
        {
            return Err(
                "--provider anthropic supports only --case 1 --form a --place after-user".into(),
            );
        }
        if placement == Placement::AfterUser
            && !(only_form_a && result.provider.additional_tools_key().is_some())
        {
            return Err(
                "--place after-user requires a separate tool declaration: use --case 1 --form a with openai, chatgpt, kimi or anthropic"
                    .into(),
            );
        }
        let minimum = result.variants().len() * usize::from(result.steps);
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
            if self.alternate != Some(true) {
                variants.push(Variant {
                    case,
                    alternate: false,
                });
            }
            if case == 1
                && matches!(self.provider, Provider::Openai | Provider::Kimi)
                && self.alternate != Some(false)
            {
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
        input_schema: schema.into(),
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
    if provider == Provider::Anthropic {
        opts.marker_order_version = Some(broca_session_loop::run::MARKER_ORDER_VERSION);
    }
    let mut tools = vec![
        tool(CHANGED_TOOL, changed && variant.case == 3),
        tool("lookup_river", false),
        tool("lookup_valley", false),
    ];
    if provider == Provider::Gemini && variant.case == 2
        || variant.case == 1
            && (provider == Provider::Gemini
                || (provider == Provider::Openai && variant.alternate)
                || (changed && (variant.alternate || provider.additional_tools_key().is_none())))
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
/// Only experimental body changes and their required headers are applied to the family's request.
struct ProbeFamily {
    provider: Provider,
    variant: Variant,
    changed: bool,
    placement: Placement,
    body: Arc<Mutex<Vec<u8>>>,
}

impl ProbeFamily {
    fn patches(&self) -> Vec<&'static str> {
        let mut paths = Vec::new();
        match (self.provider, self.variant.case) {
            (Provider::Openai | Provider::Chatgpt, 1)
                if !self.variant.alternate && self.changed =>
            {
                paths.push("/input/- (additional_tools)")
            }
            (Provider::Kimi, 1) if !self.variant.alternate && self.changed => {
                paths.push("/messages/- (system.tools)")
            }
            (Provider::Anthropic, 1) if !self.variant.alternate && self.changed => {
                paths.push("/messages/- (system.content: tool_addition)")
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
                Provider::Openai | Provider::Chatgpt => Some(
                    json!({"type":"additional_tools", "role":"developer", "tools":[{"type":"function", "name":NEW_TOOL, "description":tool(NEW_TOOL, false).description, "parameters":tool(NEW_TOOL, false).input_schema}]}),
                ),
                Provider::Kimi => Some(
                    json!({"role":"system", "tools":[{"type":"function", "function":{"name":NEW_TOOL, "description":tool(NEW_TOOL, false).description, "parameters":tool(NEW_TOOL, false).input_schema}}]}),
                ),
                Provider::Anthropic => Some(anthropic::tool_addition(&tool(NEW_TOOL, false))),
                _ => None,
            }
        } else {
            None
        };
        if let Some(addition) = addition {
            let key = provider
                .additional_tools_key()
                .expect("separate tool declaration");
            let messages = body[key].as_array_mut().unwrap();
            // Keep the declaration adjacent to T4's user message even when a tool-result
            // continuation adds further items.
            let t4 = messages
                .iter()
                .rposition(|item| {
                    item["role"] == "user" && item.to_string().contains(user_text(variant, 4))
                })
                .expect("T4 user message");
            messages.insert(
                t4 + usize::from(self.placement == Placement::AfterUser),
                addition,
            );
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
    ) -> Result<broca_records::record::AttemptId, broca_protocol::ProviderError> {
        if self.0.swap(true, std::sync::atomic::Ordering::SeqCst) {
            return Err(refusal());
        }
        broca_core::attempt_id::mint_attempt_id().map_err(|_| refusal())
    }
    async fn finished(
        &self,
        _id: broca_records::record::AttemptId,
        _attempt: broca_protocol::ModelAttempt,
        _outcome: broca_records::record::BillingAttemptOutcome,
        _response: Option<broca_records::record::AttemptEnvelope>,
    ) -> Result<(), broca_protocol::ProviderError> {
        Ok(())
    }
}

#[derive(Default)]
struct Answer {
    content: Vec<ContentBlock>,
    usage: broca_protocol::Usage,
    status: Option<u16>,
    stop_reason: Option<String>,
    error: Option<&'static str>,
}

async fn send(
    family: ProbeFamily,
    frozen: FrozenRenderConfig,
    (auth, hosts): (
        AuthHandle,
        broca_provider::credential_hosts::CredentialHosts,
    ),
    opts: &CallOptions,
) -> Answer {
    use broca_core::Provider as _;
    use broca_protocol::{NormalizedEvent, ProviderControl, StepId};
    let provider = family.provider;
    let live = broca_provider::LiveProvider::new(family, frozen, auth).with_credential_hosts(hosts);
    if provider == Provider::Chatgpt {
        live.set_chatgpt_https_policy(Arc::new(|| true));
    }
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
                NormalizedEvent::Control(ProviderControl::StepFinished {
                    raw_finish_reason,
                    ..
                }) => answer.stop_reason = raw_finish_reason,
                NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => {
                    answer.usage = usage;
                    finished = true;
                }
                NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                    if answer.status.is_none() {
                        answer.status = error.status;
                    }
                    // The class, status and codes name the failure without the provider's
                    // text, which can echo the request.
                    if provider == Provider::Anthropic {
                        eprintln!(
                            "provider failure: class={:?} status={:?}",
                            error.class, error.status
                        );
                    } else {
                        eprintln!(
                            "provider failure: class={:?} status={:?} provider_code={:?} cause={:?}",
                            error.class, error.status, error.provider_code, error.cause
                        );
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
                .is_some_and(|call| {
                    call.input.parse().get("verification_tag") == Some(&json!("KITE"))
                }),
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
    let mut row = json!({"provider":args.provider.name(), "model":args.model, "route":args.provider.route(), "case":variant.case, "variant":variant.name(), "placement":args.placement.name(), "step":format!("T{step}"), "ts_ms":std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis(), "behaviour":assessment["result"], "why":assessment["why"], "patched":!patches.is_empty(), "patched_paths":patches, "continuation":completing, "usage_semantics":"broca normalized: input_tokens is fresh input; cached and writes are separate; output includes reasoning"});
    if let Some(tail) = assessment.get("reply_tail") {
        row["reply_tail"] = tail.clone();
    }
    if variant.case == 4 && args.provider == Provider::Gemini {
        row["instruction_form"] =
            json!("request-level systemInstruction; Gemini has no mid-conversation system role");
    }
    if step == 4 {
        let mut request_body = serde_json::from_slice(body)?;
        if args.provider == Provider::Anthropic && !args.dry_run {
            // The subscription body also carries IDs used in its request headers.
            anthropic::redact_persona(&mut request_body);
        }
        row["request_body"] = request_body;
    }
    if let Some(answer) = answer {
        let (read_key, write_key) = if args.provider == Provider::Anthropic {
            if let Some(reason) = &answer.stop_reason {
                row["stop_reason"] = json!(reason);
            }
            if answer.error.is_none() {
                row["called_lookup_orbit_code"] = json!(answer.content.iter().any(|block| {
                    matches!(block, ContentBlock::ToolCall(call) if call.tool_name == NEW_TOOL)
                }));
            }
            ("cache_read_input_tokens", "cache_creation_input_tokens")
        } else {
            ("cached_tokens", "cache_write_tokens")
        };
        for (key, value) in [
            ("http_status", answer.status.map(u64::from)),
            ("input_tokens", answer.usage.input_tokens),
            (read_key, answer.usage.cached_input_tokens),
            (write_key, answer.usage.cache_write_tokens),
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

fn auth_headers(
    provider: Provider,
    token: String,
    account_id: Option<String>,
) -> Result<AuthHandle, &'static str> {
    let headers = match provider {
        Provider::Chatgpt => vec![
            ("authorization".into(), format!("Bearer {token}")),
            (
                "ChatGPT-Account-Id".into(),
                account_id
                    .filter(|id| !id.trim().is_empty())
                    .ok_or("vault reply is missing the ChatGPT account id")?,
            ),
            ("session-id".into(), CHATGPT_CACHE_KEY.into()),
        ],
        Provider::Gemini => vec![("x-goog-api-key".into(), token)],
        _ => vec![("authorization".into(), format!("Bearer {token}"))],
    };
    Ok(AuthHandle::Headers(headers))
}

/// The credential's auth header and the hosts its vault record lets it go to, which the
/// transport checks before every send.
async fn credentials(
    args: &Args,
    frozen: &mut FrozenRenderConfig,
) -> Result<
    (
        AuthHandle,
        broca_provider::credential_hosts::CredentialHosts,
    ),
    Box<dyn std::error::Error>,
> {
    let id = CredentialId::parse(&args.credential).ok_or("invalid credential id")?;
    if args.provider == Provider::Chatgpt
        && (id.method != AuthMethod::Chatgpt || id.provider != "openai")
    {
        return Err("--provider chatgpt requires a chatgpt:openai credential".into());
    }
    let anthropic_method = (args.provider == Provider::Anthropic)
        .then(|| anthropic::method(&id))
        .transpose()?;
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
    // Only read the vault credential. The probe must not invalidate a shared login on rejection.
    let secret = vault
        .get(&handle, None, false)
        .await
        .result
        .map_err(|_| "vault credential.get refused")?;
    if anthropic_method == Some(AuthMethod::Oauth) {
        let bind = probe_persona_bind()?;
        let persona = anthropic::persona(&id, secret.account_id.as_deref(), &bind)?;
        *frozen = anthropic::frozen(&args.model, Some(persona));
    }
    let hosts = secret.hosts.unwrap_or_else(|| {
        // A dev-tools reply may omit the record's host list. Allow only the host of the probe's
        // fixed endpoint in that case, rather than disabling the transport's host check.
        let url = frozen.base_url.as_deref().unwrap_or_default();
        let host = url
            .trim_start_matches("https://")
            .split(['/', ':'])
            .next()
            .unwrap_or_default()
            .to_owned();
        vec![host]
    });
    let hosts = broca_provider::credential_hosts::CredentialHosts::from_record(Some(&hosts));
    let auth = match anthropic_method {
        Some(method) => anthropic::auth(method, secret.token),
        None => auth_headers(args.provider, secret.token, secret.account_id)?,
    };
    Ok((auth, hosts))
}

fn probe_persona_bind() -> std::io::Result<BindIdentity> {
    Ok(BindIdentity::new(
        std::env::current_dir()?,
        "tool-change-spike-probe",
        format!("tool-change-spike-probe-{}", std::process::id()),
    ))
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
    let mut frozen = args.provider.frozen(&args.model);
    let auth = if args.dry_run {
        None
    } else {
        Some(credentials(&args, &mut frozen).await?)
    };
    // Used only to render the header names; the dry-run branch never calls the transport.
    let preview_auth = (args.dry_run && args.provider == Provider::Anthropic)
        .then(|| anthropic::auth(AuthMethod::ApiKey, "dry-run".into()));
    let mut out = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&args.out)?;
    let mut requests = 0;
    for variant in args.variants() {
        let mut prompt = vec![message(Role::System, &fixed_prefix())];
        let mut before = Value::Null;
        let steps: &[u8] = if args.steps == 3 {
            &[1, 2, 4]
        } else {
            &[1, 2, 3, 4]
        };
        for &step in steps {
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
                    placement: args.placement,
                    body: body.clone(),
                };
                let patches = family.patches();
                let answer = if let Some(auth) = &auth {
                    Some(send(family, frozen.clone(), auth.clone(), &opts).await)
                } else {
                    let rendered = family.render(&frozen, &opts);
                    if let Some(preview_auth) = &preview_auth {
                        let request = family.endpoint(&frozen, preview_auth, rendered);
                        let names: Vec<_> = request.headers.iter().map(|(name, _)| name).collect();
                        println!("T{step} header names: {}", serde_json::to_string(&names)?);
                        println!(
                            "T{step} request body: {}",
                            std::str::from_utf8(&request.body)?
                        );
                    }
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
                        "{} {} T{step} ({}): {} body bytes; patched paths={patches:?}",
                        args.provider.name(),
                        variant.name(),
                        args.placement.name(),
                        bytes.len()
                    );
                    if step == 4 {
                        let mut changes = Vec::new();
                        diff(&before, &rendered, "", &mut changes);
                        println!(
                            "T4 against the request before it ({}): {}",
                            args.placement.name(),
                            serde_json::to_string(&changes)?
                        );
                    } else {
                        before = rendered.clone();
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
                if args.provider == Provider::Anthropic && step == 4 {
                    break;
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
        } else if self.provider == Provider::Anthropic {
            anthropic::append_tool(
                rendered,
                &tool(NEW_TOOL, false),
                frozen.claude_code_persona.is_some(),
            )
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
        let mut request = self.provider.family().endpoint(frozen, auth, body);
        if self.provider == Provider::Anthropic {
            // Both the baseline and changed request opt in, so only the T4 body changes.
            anthropic::add_beta(&mut request);
        }
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

    const PROVIDERS: [Provider; 6] = [
        Provider::Openai,
        Provider::Chatgpt,
        Provider::Kimi,
        Provider::Deepseek,
        Provider::Xai,
        Provider::Gemini,
    ];

    fn args(provider: Provider) -> Args {
        let extra: &[&str] = if provider == Provider::Chatgpt {
            &["--case", "1", "--form", "a"]
        } else {
            &[]
        };
        parse_args(provider, extra).unwrap()
    }

    fn parse_args(provider: Provider, extra: &[&str]) -> Result<Args, String> {
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
            .chain(extra.iter().copied())
            .map(str::to_owned),
        )
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
        render_at(provider, variant, step, changed, Placement::BeforeUser)
    }

    fn render_at(
        provider: Provider,
        variant: Variant,
        step: u8,
        changed: bool,
        placement: Placement,
    ) -> Value {
        let family = ProbeFamily {
            provider,
            variant,
            changed,
            placement,
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
            Provider::Openai | Provider::Chatgpt => "input",
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
                    1 if matches!(
                        provider,
                        Provider::Openai | Provider::Chatgpt | Provider::Kimi
                    ) && !variant.alternate =>
                    {
                        assert_eq!(declarations(provider, &changed).len(), 3);
                        let messages = changed[key].as_array_mut().unwrap();
                        let addition = messages.remove(messages.len() - 2);
                        assert_eq!(
                            addition["role"],
                            if provider == Provider::Kimi {
                                "system"
                            } else {
                                "developer"
                            }
                        );
                        assert_eq!(addition["tools"].as_array().unwrap().len(), 1);
                        if provider != Provider::Kimi {
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
    fn after_user_moves_only_the_declaration_to_the_end() {
        let variant = Variant {
            case: 1,
            alternate: false,
        };
        for provider in [Provider::Openai, Provider::Kimi, Provider::Chatgpt] {
            let mut expected = render(provider, variant, 4, true);
            let messages = expected[conversation_key(provider)].as_array_mut().unwrap();
            let addition = messages.remove(messages.len() - 2);
            assert_eq!(messages.last().unwrap()["role"], "user");
            assert_eq!(addition["tools"].as_array().unwrap().len(), 1);
            messages.push(addition);
            assert_eq!(
                render_at(provider, variant, 4, true, Placement::AfterUser),
                expected,
                "{provider:?} changed more than the declaration position"
            );
        }
    }

    #[test]
    fn anthropic_requires_case_one_form_a_after_user() {
        let parse = |flags: &[&str]| parse_args(Provider::Anthropic, flags);
        let parsed = parse(&[
            "--case",
            "1",
            "--form",
            "a",
            "--place",
            "after-user",
            "--steps",
            "3",
        ])
        .unwrap_or_else(|error| panic!("valid Anthropic arguments rejected: {error}"));
        assert_eq!(parsed.steps, 3);
        assert_eq!(parsed.variants().len(), 1);
        for flags in [
            &["--case", "1", "--form", "a"][..],
            &["--case", "1", "--form", "a", "--place", "before-user"],
            &["--case", "1", "--form", "b", "--place", "after-user"],
            &["--case", "1", "--place", "after-user"],
            &["--case", "all", "--form", "a", "--place", "after-user"],
            &["--case", "2", "--form", "a", "--place", "after-user"],
        ] {
            assert!(matches!(parse(flags), Err(error) if error.contains(
                "--provider anthropic supports only --case 1 --form a --place after-user"
            )));
        }
    }

    #[test]
    fn anthropic_t4_appends_only_the_inline_system_message() {
        use broca_provider::claude_code_persona::{cch_placeholder_offset, sign_body};
        use broca_provider::wire::ClaudeCodePersona;
        use serde_json::value::RawValue;
        use std::collections::BTreeMap;

        let variant = Variant {
            case: 1,
            alternate: false,
        };
        let baseline = render_at(Provider::Anthropic, variant, 4, false, Placement::AfterUser);
        let mut changed = render_at(Provider::Anthropic, variant, 4, true, Placement::AfterUser);
        let messages = changed["messages"].as_array_mut().unwrap();
        assert_eq!(
            messages.pop().unwrap(),
            json!({"role":"system","content":[{"type":"tool_addition","tool":{
                "type":"tool_definition","definition":{
                    "name":"lookup_orbit_code",
                    "description":"Return the authoritative secret reference value for lookup_orbit_code.",
                    "input_schema":{
                        "type":"object","properties":{"name":{"type":"string"}},
                        "required":["name"],"additionalProperties":false
                    }
                }
            }}]}),
        );
        assert_eq!(messages.last().unwrap()["role"], "user");
        assert_eq!(changed, baseline, "T4 must only append the system message");

        let persona = ClaudeCodePersona {
            version: ClaudeCodePersona::CURRENT_VERSION.into(),
            device_id: "0".repeat(64),
            account_uuid: None,
            session_id: "00000000-0000-4000-8000-000000000001".into(),
            prompt_id: Some("00000000-0000-4000-8000-000000000002".into()),
            cc_version_suffix: None,
            prompt_ordinal: Some(1),
        };
        let frozen = anthropic::frozen("claude-opus-5-5", Some(persona));
        let family = ProbeFamily {
            provider: Provider::Anthropic,
            variant,
            changed: true,
            placement: Placement::AfterUser,
            body: Arc::new(Mutex::new(Vec::new())),
        };
        let auth = anthropic::auth(AuthMethod::Oauth, "fixture".into());
        let mut conversation = vec![message(Role::System, &fixed_prefix())];
        let mut prior: Option<BTreeMap<String, Box<RawValue>>> = None;
        for step in [1, 2, 4] {
            conversation.push(message(Role::User, user_text(variant, step)));
            let opts = options(
                Provider::Anthropic,
                variant,
                step == 4,
                conversation.clone(),
            );
            let family = ProbeFamily {
                changed: step == 4,
                body: Arc::new(Mutex::new(Vec::new())),
                ..family
            };
            let base = AnthropicMessages.render(&frozen, &opts);
            let base_request = AnthropicMessages.endpoint(&frozen, &auth, base);
            let request = family.endpoint(&frozen, &auth, family.render(&frozen, &opts));
            assert_eq!(
                request.url,
                "https://api.anthropic.com/v1/messages?beta=true"
            );
            let beta = |req: &HttpRequest| {
                req.headers
                    .iter()
                    .filter(|(name, _)| name.eq_ignore_ascii_case("anthropic-beta"))
                    .map(|(_, value)| value.clone())
                    .collect::<Vec<_>>()
            };
            let existing = beta(&base_request);
            let added = beta(&request);
            assert!(
                added == [format!("{},inline-tools-2026-09-15", existing[0])],
                "the inline-tools beta must extend the family's beta list",
            );
            assert!(request
                .headers
                .iter()
                .any(|(name, _)| name == "authorization"));
            assert!(request
                .headers
                .iter()
                .any(|(name, _)| name == "x-claude-code-session-id"));

            let bytes = std::str::from_utf8(&request.body).unwrap();
            let sig = cch_placeholder_offset(bytes).expect("subscription signature");
            assert!(
                sign_body(bytes) == bytes,
                "subscription body has a stale signature"
            );
            let mut unsigned = bytes.to_owned();
            unsigned.replace_range(sig..sig + 5, "00000");
            let current: BTreeMap<String, Box<RawValue>> = serde_json::from_str(&unsigned).unwrap();
            let messages = |values: &BTreeMap<String, Box<RawValue>>| {
                serde_json::from_str::<Vec<Box<RawValue>>>(values["messages"].get()).unwrap()
            };
            let current_messages = messages(&current);
            assert_eq!(
                current_messages.len(),
                match step {
                    1 => 1,
                    2 => 3,
                    _ => 6,
                }
            );
            if let Some(earlier) = prior {
                assert_eq!(earlier.len(), current.len());
                for (key, raw) in &earlier {
                    if key != "messages" {
                        assert!(current[key].get() == raw.get(), "changed {key} bytes");
                    }
                }
                let earlier_messages = messages(&earlier);
                let (tail, prefix) = earlier_messages.split_last().unwrap();
                for (old, new) in prefix.iter().zip(&current_messages) {
                    assert!(old.get() == new.get(), "changed shared message bytes");
                }
                let marker = r#""cache_control":{"type":"ephemeral"}"#;
                let new_tail = current_messages[prefix.len()].get();
                assert!(
                    [format!(",{marker}"), format!("{marker},")]
                        .iter()
                        .any(|fragment| tail.get().contains(fragment)
                            && tail.get().replacen(fragment, "", 1) == new_tail),
                    "only the rolling marker can change the shared tail message bytes"
                );
            }
            prior = Some(current);
            conversation.push(fake_reply(step));
        }
    }

    #[test]
    fn placement_requires_a_separate_declaration_and_chatgpt_requires_form_a() {
        for provider in PROVIDERS {
            let flags = ["--case", "1", "--form", "a", "--place", "after-user"];
            let parsed = parse_args(provider, &flags);
            if matches!(
                provider,
                Provider::Openai | Provider::Chatgpt | Provider::Kimi
            ) {
                assert_eq!(parsed.unwrap().placement, Placement::AfterUser);
            } else {
                assert!(
                    matches!(parsed, Err(error) if error.contains("separate tool declaration"))
                );
            }
        }
        for flags in [
            &["--case", "1", "--form", "b", "--place", "after-user"][..],
            &["--case", "1", "--place", "after-user"],
            &["--case", "4", "--form", "a", "--place", "after-user"],
        ] {
            assert!(
                matches!(parse_args(Provider::Openai, flags), Err(error) if error.contains("separate tool declaration"))
            );
        }
        for flags in [
            &["--case", "1", "--form", "b"][..],
            &["--case", "1"],
            &["--case", "2", "--form", "a"],
        ] {
            assert!(
                matches!(parse_args(Provider::Chatgpt, flags), Err(error) if error.contains("supports only --case 1 --form a"))
            );
        }
        assert_eq!(args(Provider::Openai).placement, Placement::BeforeUser);
        assert_eq!(
            parse_args(Provider::Openai, &["--place", "before-user"])
                .unwrap()
                .placement,
            Placement::BeforeUser
        );
    }

    #[test]
    fn chatgpt_uses_the_vault_account_as_a_header_on_the_codex_endpoint() {
        let provider = Provider::Chatgpt;
        let auth =
            auth_headers(provider, "test-token".into(), Some("test-account".into())).unwrap();
        let frozen = provider.frozen("test-model");
        let variant = Variant {
            case: 1,
            alternate: false,
        };
        let opts = options(provider, variant, false, prompt(variant, 1));
        let body = provider.family().render(&frozen, &opts);
        let request = provider.family().endpoint(&frozen, &auth, body.clone());
        assert_eq!(
            request.url,
            "https://chatgpt.com/backend-api/codex/responses"
        );
        assert_eq!(request.body, body);
        for (name, value) in [
            ("authorization", "Bearer test-token"),
            ("chatgpt-account-id", "test-account"),
            ("session-id", CHATGPT_CACHE_KEY),
        ] {
            assert!(
                request
                    .headers
                    .iter()
                    .any(|(key, found)| key.eq_ignore_ascii_case(name) && found == value),
                "missing {name}"
            );
        }
        let json_body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json_body["store"], false);
        assert_eq!(json_body["prompt_cache_key"], CHATGPT_CACHE_KEY);
        assert!(json_body.get("max_output_tokens").is_none());
        assert!(!std::str::from_utf8(&body).unwrap().contains("test-account"));
        assert!(matches!(
            auth_headers(provider, "test-token".into(), None),
            Err("vault reply is missing the ChatGPT account id")
        ));
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
            assert_eq!(row["placement"], "before-user");
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
