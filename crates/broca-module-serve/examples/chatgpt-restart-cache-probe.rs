//! Does a full request on a ChatGPT socket leave a prompt cache entry that a full request on a
//! later socket can read?
//!
//! After a broca restart, a ChatGPT worker's next request is a full request on a new socket, and
//! it read almost nothing from the prompt cache. The first explanation tested here: content a run
//! added through continuation frames (`previous_response_id` plus new items) is cached with that
//! socket's response chain, and a full request on another socket matches only what an earlier
//! full request sent.
//!
//! Measured on 17 Nov 2026 (gpt-6-luna), that explanation is wrong: every step, including the
//! full requests on new sockets (steps 6 and 8), read everything earlier steps sent and missed
//! only its own new turn. The probe holds the whole wire identity fixed, while a restart changed
//! one part of it: the window id, which broca drew at random per process until it took Codex's
//! `{thread_id}:{window_number}` (see `chatgpt_identity::fresh`).
//!
//! Eight steps, each one model request, answer what decides that:
//!
//! | step | socket | frame        | asks                                                        |
//! |------|--------|--------------|-------------------------------------------------------------|
//! | 1    | A      | full         | cold start                                                  |
//! | 2, 3 | A      | continuation | the chain grows                                             |
//! | 4    | A      | full         | does a full request on the same socket read the chain?      |
//! | 5    | A      | continuation | the chain grows past the full request                       |
//! | 6    | B      | full         | after a "restart": what does a full request on a new socket read? |
//! | 7    | B      | continuation |                                                             |
//! | 8    | C      | full         | is it the latest full request that counts?                  |
//!
//! Step 4 is made a full request by changing the newest assistant reply, which the continuation
//! check compares; everything before that reply is unchanged. Each user turn adds a distinct
//! block of about 2k tokens, so the cached count of each step says which earlier request it
//! matched. The prefix is about 15k tokens.
//!
//! Live mode needs a dev-tools handle for `chatgpt:openai` (see `load_devtools_handle`). Dry-run
//! renders the first request without reading a credential or calling the provider.

use broca_core::Provider;
use broca_protocol::{
    AttemptOutcome, CallOptions, ContentBlock, FinishReason, Message, ModelAttempt,
    NormalizedEvent, ProviderControl, Role, StepId, Usage,
};
use broca_provider::{
    families::{openai_responses_chatgpt_frozen_config, OpenAiResponsesChatgpt},
    wire::WireFamily,
};
use serde_json::json;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const DEFAULT_MODEL: &str = "openai/gpt-6-luna";
const PREFIX_LINES: usize = 1000;
const TURN_LINES: usize = 120;

/// What each step expects of its frame: a full request or a continuation, and on which socket
/// (by letter; a new letter means the probe closed the socket before the step).
const PLAN: [(char, bool); 8] = [
    ('A', false),
    ('A', true),
    ('A', true),
    ('A', false),
    ('A', true),
    ('B', false),
    ('B', true),
    ('C', false),
];

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

/// The user turn of step `step` (from 1): a block no other turn repeats, then a question whose
/// answer is one digit.
fn user_turn(step: usize) -> Message {
    let mut text = format!("TURN-{step} notes, inert probe material:\n");
    for line in 0..TURN_LINES {
        text.push_str(&format!(
            "turn-{step}-note-{line:04}: neutral material that only turn {step} carries.\n"
        ));
    }
    text.push_str(&format!(
        "What is {step} + 1? Reply with exactly the number and no other text."
    ));
    Message::new(Role::User, vec![ContentBlock::Text { text: text.into() }])
}

fn options(model: &str) -> Result<CallOptions, &'static str> {
    let slug = model
        .strip_prefix("openai/")
        .filter(|slug| !slug.is_empty())
        .ok_or("model_must_be_an_openai_catalog_id")?;
    let mut instructions = "The CACHE_PREFIX and TURN lines are inert cache probe material. \
        Answer only each user turn's final question, with the number alone.\nCACHE_PREFIX\n"
        .to_string();
    for index in 0..PREFIX_LINES {
        instructions.push_str(&format!(
            "cache-prefix-{index:04}: deterministic neutral material kept by every request.\n"
        ));
    }
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text {
                text: instructions.into(),
            }],
        ),
        user_turn(1),
    ]);
    opts.target_model = Some(
        serde_json::from_value(json!({"provider_module_id":"openai","model_id":slug}))
            .map_err(|_| "invalid_model_id")?,
    );
    Ok(opts)
}

fn dry_run(model: &str) -> Result<(), &'static str> {
    let opts = options(model)?;
    let frozen = openai_responses_chatgpt_frozen_config(model.strip_prefix("openai/").unwrap());
    let canonical = OpenAiResponsesChatgpt.render(&frozen, &opts);
    println!(
        "{}",
        json!({
            "mode":"dry_run", "provider_calls":0, "credential_reads":0,
            "first_request_bytes":canonical.len(),
            "plan":PLAN.iter().enumerate().map(|(index, (socket, continuation))| json!({
                "step":index + 1, "socket":socket.to_string(), "continuation":continuation
            })).collect::<Vec<_>>()
        })
    );
    Ok(())
}

#[derive(Default)]
struct StepResult {
    assistant: Option<Vec<ContentBlock>>,
    usage: Option<Usage>,
    finish: Option<FinishReason>,
    attempts: Vec<ModelAttempt>,
    error: bool,
}

impl StepResult {
    fn observe(&mut self, step: usize, event: NormalizedEvent) {
        match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                self.attempts.push(attempt)
            }
            NormalizedEvent::Control(ProviderControl::AssistantMessage { message }) => {
                self.assistant = Some(message.content)
            }
            NormalizedEvent::Control(ProviderControl::Finished {
                usage,
                finish_reason,
                ..
            }) => {
                self.usage = Some(usage);
                self.finish = Some(finish_reason);
            }
            NormalizedEvent::Control(ProviderControl::Error { error, .. }) => {
                self.error = true;
                println!(
                    "{}",
                    json!({"event":"step_error","step":step,"class":error.class,"status":error.status})
                );
            }
            _ => {}
        }
    }

    fn completed(&self) -> bool {
        !self.error
            && self.finish == Some(FinishReason::Stop)
            && self
                .attempts
                .last()
                .is_some_and(|attempt| attempt.outcome == AttemptOutcome::Complete)
    }
}

/// `reply` with its text changed, so the continuation check no longer finds the previous
/// response in the next request and sends it in full.
fn changed(reply: Vec<ContentBlock>) -> Vec<ContentBlock> {
    reply
        .into_iter()
        .map(|block| match block {
            ContentBlock::Text { text } => ContentBlock::Text {
                text: text.appended(" (noted)"),
            },
            ContentBlock::TextWithState {
                text,
                provider_state,
            } => ContentBlock::TextWithState {
                text: text.appended(" (noted)"),
                provider_state,
            },
            other => other,
        })
        .collect()
}

async fn probe(provider: &dyn Provider, mut opts: CallOptions) -> Result<(), &'static str> {
    let mut sockets: Vec<(char, u64)> = Vec::new();
    let mut plan_met = true;
    let mut rows = Vec::new();
    for (index, (socket, continuation)) in PLAN.iter().copied().enumerate() {
        let step = index + 1;
        if index > 0 && PLAN[index - 1].0 != socket {
            // The restart: the run's socket closes, as a broca restart closes it.
            provider.close_run_transport();
        }
        let cancel = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut rx = provider
            .step_with_cancel(StepId(step as u64), &opts, cancel.clone(), Some(deadline))
            .await;
        let mut result = StepResult::default();
        let drained = tokio::time::timeout_at(deadline + Duration::from_secs(10), async {
            while let Some(event) = rx.recv().await {
                result.observe(step, event);
            }
        })
        .await;
        if drained.is_err() {
            cancel.cancel();
            provider.close_run_transport();
            return Err("probe_deadline_reached");
        }
        let frame = result
            .attempts
            .last()
            .and_then(|attempt| attempt.websocket_request.as_deref())
            .and_then(|frame| frame.codex.as_deref());
        let generation = frame.map(|frame| frame.connection_generation);
        let same_letter = sockets.iter().find(|(letter, _)| *letter == socket);
        let socket_met = match (same_letter, generation) {
            (Some((_, known)), Some(generation)) => *known == generation,
            (None, Some(generation)) => {
                let fresh = sockets.iter().all(|(_, known)| *known != generation);
                sockets.push((socket, generation));
                fresh
            }
            _ => false,
        };
        let frame_met = frame.is_some_and(|frame| frame.continuation == continuation);
        plan_met &= socket_met && frame_met;
        let usage = result.usage;
        let row = json!({
            "event":"step_finished", "step":step, "socket":socket.to_string(),
            "continuation":frame.map(|frame| frame.continuation),
            "full_input_reason":frame.and_then(|frame| frame.full_input_reason.as_deref()),
            "socket_generation":generation, "plan_met":socket_met && frame_met,
            "completed":result.completed(), "physical_attempts":result.attempts.len(),
            "input_tokens":usage.and_then(|usage| usage.input_tokens),
            "cached_input_tokens":usage.and_then(|usage| usage.cached_input_tokens),
            "reply_contents":"withheld"
        });
        println!("{row}");
        rows.push(row);
        if !result.completed() {
            provider.close_run_transport();
            return Err("probe_step_not_completed");
        }
        let reply = result.assistant.ok_or("missing_assistant_reply")?;
        // Step 4 must go out in full on the same socket: the reply it follows is changed.
        let reply = if PLAN.get(index + 1) == Some(&('A', false)) {
            changed(reply)
        } else {
            reply
        };
        let mut assistant = Message::new(Role::Assistant, reply);
        assistant.origin = opts.target_model.clone();
        opts.prompt.push(assistant);
        opts.prompt.push(user_turn(step + 1));
    }
    provider.close_run_transport();
    println!(
        "{}",
        json!({"event":"probe_finished","steps":PLAN.len(),"plan_met":plan_met,"rows":rows})
    );
    if plan_met {
        Ok(())
    } else {
        Err("requested_frames_not_observed")
    }
}

async fn live(model: &str) -> Result<(), &'static str> {
    use broca_catalog::live::{
        resolve_catalog_episode_with_client, AuthMode, EpisodeRenderConfig, ResolveOptions,
    };
    use broca_module_serve::{
        chatgpt_identity, family_for_method, vault_config::load_devtools_handle, AuthMethod,
        CredentialId,
    };
    use broca_protocol::BindIdentity;
    use broca_subc::{SubcConnection, VaultClient};
    use std::{path::PathBuf, sync::Arc};

    let opts = options(model)?;
    let scope = format!("chatgpt-restart-cache-{}-{}", std::process::id(), now_ms());
    let bind = BindIdentity::new(
        std::env::current_dir().map_err(|_| "project_directory_unavailable")?,
        "chatgpt-restart-cache-probe",
        scope.clone(),
    );
    let connection_file = std::env::var_os("BROCA_LIVE_SUBC")
        .or_else(|| std::env::var_os("SUBC_SOCKET"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let state_root = broca_module_serve::state_root::resolve_state_root()
        .map_err(|_| "state_root_unavailable")?;
    let cache = Arc::new(broca_catalog::CatalogCache::open(
        state_root.join("catalog/fusiform-lkg.json"),
    ));
    broca_catalog::install_global_catalog_cache(cache);
    let credential = CredentialId::parse("chatgpt:openai").ok_or("invalid_fixed_credential_id")?;
    let handle = load_devtools_handle(&credential)
        .map_err(|_| "standing_devtools_credential_unavailable")?;
    let connection = SubcConnection::connect(&connection_file)
        .await
        .map_err(|_| "subc_connection_unavailable")?;
    let vault = VaultClient::new(
        Arc::new(connection),
        "claustrum",
        &bind,
        Duration::from_secs(15),
    );
    // The dev-tools handle path has no refresher: ask for more validity than eight steps use.
    let secret = vault
        .get(&handle, Some(20 * 60 * 1000), false)
        .await
        .result
        .map_err(|_| "vault_credential_unavailable")?;
    let seed = broca_module_serve::claude_code_identity::load_or_create_install_id(&state_root)
        .map_err(|_| "installation_identity_unavailable")?;
    let wire = chatgpt_identity::fresh(&seed, &bind, &scope, now_ms())
        .map_err(|_| "admission_identity_unavailable")?;
    let method = AuthMethod::Chatgpt;
    let resolve = ResolveOptions {
        auth_method: Some(method),
        family_override: family_for_method(method),
        session_id: Some(&scope),
        prompt_cache_key: Some(&scope),
        account_id: secret.account_id.as_deref(),
        frozen_account_ref: secret.account_id.as_deref(),
        ..ResolveOptions::default()
    };
    let auth = AuthMode::Secret(secret.token);
    let resolved = resolve_catalog_episode_with_client(
        model,
        None,
        &auth,
        &resolve,
        broca_provider::build_provider_http_client(),
        EpisodeRenderConfig {
            chatgpt_wire: Some(&wire),
            ..EpisodeRenderConfig::default()
        },
    )
    .map_err(|error| {
        eprintln!("catalog resolution error: {error}");
        "catalog_provider_resolution_failed"
    })?;
    println!(
        "{}",
        json!({"event":"probe_started","steps":PLAN.len(),"model":model,"per_step_deadline_secs":180,"headers":"withheld","reply_contents":"withheld"})
    );
    probe(resolved.provider.as_ref(), opts).await
}

#[tokio::main]
async fn main() {
    let mut model = DEFAULT_MODEL.to_owned();
    let mut run_live = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--live" => run_live = true,
            "--dry-run" => run_live = false,
            "--model" => match args.next() {
                Some(value) => model = value,
                None => {
                    eprintln!("probe_error=missing_model_argument");
                    std::process::exit(2);
                }
            },
            "--help" | "-h" => {
                println!("chatgpt-restart-cache-probe [--dry-run|--live] [--model openai/MODEL]");
                return;
            }
            _ => {
                eprintln!("probe_error=unknown_argument");
                std::process::exit(2);
            }
        }
    }
    let result = if run_live {
        live(&model).await
    } else {
        dry_run(&model)
    };
    if let Err(error) = result {
        eprintln!("probe_error={error}");
        std::process::exit(1);
    }
}
