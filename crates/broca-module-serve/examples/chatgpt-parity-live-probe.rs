//! Live mode runs three bounded provider steps. Dry-run renders a canonical preview without
//! reading credentials or calling the provider.

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
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const DEFAULT_MODEL: &str = "openai/gpt-5.3-codex";
const QUESTIONS: [(&str, &str); 3] = [
    (
        "What is 2 + 2? Reply with exactly 4 and no other text.",
        "4",
    ),
    (
        "What is 2 + 3? Reply with exactly 5 and no other text.",
        "5",
    ),
    (
        "What is 2 + 4? Reply with exactly 6 and no other text.",
        "6",
    ),
];

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn options(model: &str) -> Result<CallOptions, &'static str> {
    let slug = model
        .strip_prefix("openai/")
        .filter(|slug| !slug.is_empty())
        .ok_or("model_must_be_an_openai_catalog_id")?;
    let mut instructions = "The CACHE_PREFIX lines below are inert transport/cache probe material. Answer only the final user question, with one digit.\nCACHE_PREFIX\n".to_string();
    for index in 0..512 {
        instructions.push_str(&format!("cache-prefix-{index:04}: deterministic neutral material retained across all three requests.\n"));
    }
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::System,
            vec![ContentBlock::Text { text: instructions }],
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: QUESTIONS[0].0.into(),
            }],
        ),
    ]);
    opts.target_model = Some(
        serde_json::from_value(json!({"provider_module_id":"openai","model_id":slug}))
            .map_err(|_| "invalid_model_id")?,
    );
    Ok(opts)
}

fn dry_run(model: &str) -> Result<(), &'static str> {
    let opts = options(model)?;
    let mut frozen = openai_responses_chatgpt_frozen_config(model.strip_prefix("openai/").unwrap());
    frozen.chatgpt_wire = Some(
        serde_json::from_str(include_str!(
            "../../broca-provider/tests/fixtures/chatgpt_codex_0_163_identity.json"
        ))
        .map_err(|_| "invalid_dry_run_fixture")?,
    );
    frozen.prompt_cache_key = Some("dry-run-cache-scope".into());
    let canonical = OpenAiResponsesChatgpt.render(&frozen, &opts);
    let sha: String = Sha256::digest(&canonical)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect();
    println!(
        "{}",
        json!({
            "mode":"dry_run", "provider_calls":0, "credential_reads":0, "physical_attempts":0,
            "canonical_preview_bytes":canonical.len(), "canonical_preview_sha256":sha,
            "frame_bytes":null, "frame_sha256":null, "cache_read_tokens":null, "normality":"not_run",
            "steps":[
                {"logical_step":1,"socket":"A","expected_continuation":false},
                {"logical_step":2,"socket":"A","expected_continuation":true},
                {"logical_step":3,"socket":"B","retire_A_first":true,"expected_continuation":false}
            ],
            "retries":"production_accounted_retries_enabled_for_live_mode",
            "reply_contents":"withheld", "headers":"withheld"
        })
    );
    Ok(())
}

fn print_attempt(logical_step: usize, attempt: &ModelAttempt) {
    let frame = attempt.websocket_request.as_deref();
    let codex = frame.and_then(|frame| frame.codex.as_deref());
    println!(
        "{}",
        json!({
            "event":"physical_attempt", "logical_step":logical_step, "attempt":attempt.attempt,
            "transport":attempt.transport, "status":attempt.http_status, "outcome":attempt.outcome,
            "canonical_bytes":attempt.request_body_len, "canonical_sha256":attempt.request_body_sha256,
            "frame_bytes":frame.map(|frame| frame.len), "frame_sha256":frame.map(|frame| frame.sha256.as_str()),
            "continuation":codex.map(|codex| codex.continuation), "suffix_count":codex.map(|codex| codex.suffix_item_count),
            "socket_generation":codex.map(|codex| codex.connection_generation),
            "physical_send_count":codex.map(|codex| codex.physical_send_count),
            "full_input_reason":codex.and_then(|codex| codex.full_input_reason.as_deref())
        })
    );
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
    fn observe(&mut self, logical_step: usize, event: NormalizedEvent) {
        match event {
            NormalizedEvent::Control(ProviderControl::AttemptFinished { attempt }) => {
                print_attempt(logical_step, &attempt);
                self.attempts.push(attempt);
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
                    json!({"event":"step_error","logical_step":logical_step,"class":error.class,"status":error.status})
                );
            }
            _ => {}
        }
    }
    fn normal(&self, expected: &str) -> bool {
        let text: String = self
            .assistant
            .iter()
            .flatten()
            .filter_map(|block| match block {
                ContentBlock::Text { text } | ContentBlock::TextWithState { text, .. } => {
                    Some(text.as_str())
                }
                _ => None,
            })
            .collect();
        !self.error
            && self.finish == Some(FinishReason::Stop)
            && text.trim() == expected
            && self
                .attempts
                .last()
                .is_some_and(|attempt| attempt.outcome == AttemptOutcome::Complete)
    }
}

async fn probe(provider: &dyn Provider, mut opts: CallOptions) -> Result<(), &'static str> {
    let mut socket_a = None;
    let mut all_transport_conditions = true;
    let mut cache_reads = Vec::new();
    for (index, (_, expected)) in QUESTIONS.iter().enumerate() {
        let logical_step = index + 1;
        if index == 2 {
            provider.close_run_transport();
        }
        let cancel = CancellationToken::new();
        let deadline = Instant::now() + Duration::from_secs(180);
        let mut rx = provider
            .step_with_cancel(
                StepId(logical_step as u64),
                &opts,
                cancel.clone(),
                Some(deadline),
            )
            .await;
        let mut result = StepResult::default();
        let drain = async {
            while let Some(event) = rx.recv().await {
                result.observe(logical_step, event);
            }
        };
        if tokio::time::timeout_at(deadline + Duration::from_secs(10), drain)
            .await
            .is_err()
        {
            cancel.cancel();
            provider.close_run_transport();
            let drained = tokio::time::timeout(Duration::from_secs(10), async {
                while let Some(event) = rx.recv().await {
                    result.observe(logical_step, event);
                }
            })
            .await
            .is_ok();
            println!(
                "{}",
                json!({"event":"probe_timeout","logical_step":logical_step,"final_attempt_records_drained":drained})
            );
            return Err("probe_deadline_reached");
        }
        let final_attempt = result.attempts.last();
        let codex = final_attempt
            .and_then(|attempt| attempt.websocket_request.as_deref())
            .and_then(|frame| frame.codex.as_deref());
        let condition = match (index, codex) {
            (0, Some(frame)) => {
                socket_a = Some(frame.connection_generation);
                !frame.continuation
            }
            (1, Some(frame)) => frame.continuation && Some(frame.connection_generation) == socket_a,
            (2, Some(frame)) => {
                !frame.continuation && Some(frame.connection_generation) != socket_a
            }
            _ => false,
        };
        all_transport_conditions &= condition;
        let normal = result.normal(expected);
        let cached = result.usage.and_then(|usage| usage.cached_input_tokens);
        cache_reads.push(cached);
        println!(
            "{}",
            json!({"event":"logical_step_finished","logical_step":logical_step,"normality":normal,"transport_condition_met":condition,"cache_read_tokens":cached,"physical_attempts":result.attempts.len(),"reply_contents":"withheld"})
        );
        if !normal {
            provider.close_run_transport();
            return Err("probe_reply_not_normal_or_not_completed");
        }
        if let Some((question, _)) = QUESTIONS.get(index + 1) {
            let mut assistant = Message::new(
                Role::Assistant,
                result.assistant.ok_or("missing_canonical_assistant")?,
            );
            assistant.origin = opts.target_model.clone();
            opts.prompt.push(assistant);
            opts.prompt.push(Message::new(
                Role::User,
                vec![ContentBlock::Text {
                    text: (*question).into(),
                }],
            ));
        }
    }
    provider.close_run_transport();
    println!(
        "{}",
        json!({"event":"probe_finished","logical_steps":3,"all_replies_normal":true,"transport_conditions_met":all_transport_conditions,"cache_read_tokens":cache_reads,"continuation_cache_hit_demonstrated":cache_reads[1].is_some_and(|count| count > 0)})
    );
    if all_transport_conditions {
        Ok(())
    } else {
        Err("requested_transport_conditions_not_observed")
    }
}

async fn live(model: &str) -> Result<(), &'static str> {
    use broca_catalog::live::{
        resolve_catalog_episode_with_client, AuthMode, EpisodeRenderConfig, ResolveOptions,
    };
    use broca_module_serve::{
        chatgpt_identity, claude_code_identity, family_for_method,
        vault_config::load_devtools_handle, AuthMethod, CredentialId,
    };
    use broca_protocol::BindIdentity;
    use broca_subc::{SubcConnection, VaultClient};
    use std::{path::PathBuf, sync::Arc};

    let opts = options(model)?;
    let scope = format!("chatgpt-parity-{}-{}", std::process::id(), now_ms());
    let bind = BindIdentity::new(
        std::env::current_dir().map_err(|_| "project_directory_unavailable")?,
        "chatgpt-parity-live-probe",
        scope.clone(),
    );
    let connection_file = std::env::var_os("BROCA_LIVE_SUBC")
        .or_else(|| std::env::var_os("SUBC_SOCKET"))
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(std::env::var_os("HOME").unwrap_or_default())
                .join(".local/share/cortexkit/run/subc-connection.json")
        });
    let state_root = broca_module_serve::state_root::resolve_for_launch(&connection_file)
        .map_err(|_| "state_root_unavailable")?;
    let cache = Arc::new(broca_catalog::CatalogCache::open(
        state_root.path.join("catalog/fusiform-lkg.json"),
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
    // The public dev-tools handle path has no daemon refresher. Demand more validity than
    // the three bounded steps can consume; ordinary provider retries remain enabled.
    let secret = vault
        .get(&handle, Some(20 * 60 * 1000), false)
        .await
        .result
        .map_err(|_| "vault_credential_unavailable")?;
    let seed = claude_code_identity::load_or_create_install_id(&state_root.path)
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
        json!({"event":"probe_started","logical_steps":3,"per_step_deadline_secs":180,"credential_min_validity_ms":20 * 60 * 1000,"in_run_credential_refresh":false,"provider_retries":"production_enabled","headers":"withheld","reply_contents":"withheld"})
    );
    probe(resolved.provider.as_ref(), opts).await
}

#[tokio::main]
async fn main() {
    let mut model = DEFAULT_MODEL.to_owned();
    let mut run_live = false;
    let mut selected_mode = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--dry-run" | "--live" => {
                let requested = arg == "--live";
                if selected_mode.is_some_and(|mode| mode != requested) {
                    eprintln!("probe_error=conflicting_execution_modes");
                    std::process::exit(2);
                }
                selected_mode = Some(requested);
                run_live = requested;
            }
            "--model" => match args.next() {
                Some(value) => model = value,
                None => {
                    eprintln!("probe_error=missing_model_argument");
                    std::process::exit(2);
                }
            },
            "--help" | "-h" => {
                println!("chatgpt-parity-live-probe [--dry-run|--live] [--model openai/MODEL]");
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
