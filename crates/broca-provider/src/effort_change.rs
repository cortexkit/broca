//! A change of reasoning effort between two runs on the same model, and the prompt cache
//! (llm-runner §9.3.6).
//!
//! The model catalog says, per model, whether an effort change keeps the provider's cached
//! prefix (`capability.effort_change_cache`), and admission freezes that into the run
//! ([`FrozenRenderConfig::effort_change_cache`]). When it does, the run rides nothing for the
//! change and its renderer sends the change in the provider's cache-keeping form, leaving the
//! earlier bytes alone:
//!  - Anthropic Messages: a per-message effort, an empty `system` message carrying
//!    `output_config.effort` after the run's first user message, with the top-level effort left
//!    as it was ([`anthropic_plan`]).
//!  - OpenAI Responses, on the platform API and the ChatGPT backend: an input configuration
//!    update (see `openai_responses_chatgpt::effort_plan`).
//!
//! The catalog's word decides the ride, not the renderer: a request that can't carry the form
//! for one run (Anthropic thinking that isn't adaptive, which refuses a per-message change) is a
//! defect of that request path, not a reason to ride.

use broca_protocol::request::CallOptions;

use crate::policy::{ReasoningPolicy, SystemPromptPolicy};
use crate::spec::WireFamilyId;
use crate::variant::{FrozenVariant, VariantRequest};
use crate::wire::FrozenRenderConfig;

/// The beta an api-key Anthropic request needs for a per-message `output_config`. A Claude Code
/// persona request sends `per-turn-control` for the same purpose instead.
pub(crate) const PER_MESSAGE_EFFORT_BETA: &str = "mid-conversation-output-config-2026-07-01";

/// Whether an effort change from the run before keeps the cached prefix for the run `frozen`
/// renders, which is what the catalog said of its model when the run started. A run that answers
/// `false` rides `effort_change` for the change and sends it as a change of the request's own
/// effort.
pub fn keeps_cache(frozen: &FrozenRenderConfig) -> bool {
    frozen.effort_change_cache
}

/// Whether an Anthropic run sends its effort change as a per-message `output_config`. Anthropic
/// takes one only while the request thinks adaptively (Sonnet 5.5 under `between_tools` and
/// Haiku 5.5 with thinking disabled answer 400), and only with its beta: a Claude Code persona
/// request on a model its requests send `per-turn-control` for, or an api-key request, which adds
/// [`PER_MESSAGE_EFFORT_BETA`].
pub(crate) fn per_message_effort(frozen: &FrozenRenderConfig) -> bool {
    let adaptive = matches!(
        &frozen.variant,
        Some(FrozenVariant {
            request: VariantRequest::Anthropic { thinking: Some(thinking), .. },
            ..
        }) if thinking["type"] == "adaptive"
    );
    let beta = match frozen.system_prompt {
        SystemPromptPolicy::RequiredIdentityLead { .. } => {
            frozen.claude_code_persona.is_some()
                && crate::claude_code_persona::sends_per_turn_control(&frozen.wire_model_id)
        }
        _ => true,
    };
    keeps_cache(frozen) && adaptive && beta
}

/// The thinking an Anthropic Messages request sends, apart from its effort: a reasoning
/// variant's own `thinking` object (none when the variant sends none), else the interleaved
/// thinking budget of the reasoning policy. Any change of it loses the cache, so it always rides.
/// Other families have no thinking mode apart from their effort and answer `None`.
pub fn thinking_mode(frozen: &FrozenRenderConfig) -> Option<String> {
    if frozen.family_id != WireFamilyId::AnthropicMessages {
        return None;
    }
    let thinking = match (&frozen.variant, &frozen.reasoning) {
        (
            Some(FrozenVariant {
                request: VariantRequest::Anthropic { thinking, .. },
                ..
            }),
            _,
        ) => serde_json::json!({ "variant": thinking }),
        (_, ReasoningPolicy::AnthropicInterleaved { budget_tokens, .. }) => {
            serde_json::json!({ "budget_tokens": budget_tokens })
        }
        _ => serde_json::Value::Null,
    };
    Some(thinking.to_string())
}

/// The effort an Anthropic request sends at its top level: its reasoning variant's.
pub(crate) fn anthropic_effort(frozen: &FrozenRenderConfig) -> Option<&str> {
    match &frozen.variant {
        Some(FrozenVariant {
            request: VariantRequest::Anthropic { effort, .. },
            ..
        }) => effort.as_deref(),
        _ => None,
    }
}

/// Reasoning efforts from least to most, as the providers spell them.
const EFFORT_ORDER: [&str; 7] = ["none", "minimal", "low", "medium", "high", "xhigh", "max"];

/// The effort a fork's ask sends for its answer alone, given its caller's `hint` (llm-runner
/// `fork_answer.effort_policy`), for a request whose run froze `frozen`: the hint, when it is
/// lower than the effort the run froze, the catalog said an effort change keeps the model's
/// cached prefix ([`keeps_cache`]), and the route has a form that changes the effort for one
/// answer without touching the request's own settings: Anthropic's per-message effort, or
/// OpenAI's input configuration update on a model that takes one. `None` otherwise, including
/// for an effort that can't be ordered against the hint: the ask then runs at the run's own
/// effort, never a guessed or a higher one.
pub fn answer_effort(frozen: &FrozenRenderConfig, hint: &str) -> Option<String> {
    let frozen_effort = match frozen.family_id {
        WireFamilyId::AnthropicMessages if per_message_effort(frozen) => anthropic_effort(frozen)?,
        WireFamilyId::OpenAiResponses | WireFamilyId::OpenAiResponsesChatgpt
            if keeps_cache(frozen)
                && crate::families::openai_responses_chatgpt::takes_effort_updates(frozen) =>
        {
            crate::families::openai_responses_chatgpt::responses_effort(frozen)?
        }
        _ => return None,
    };
    let rank = |effort: &str| EFFORT_ORDER.iter().position(|known| *known == effort);
    (rank(hint)? < rank(frozen_effort)?).then(|| hint.to_owned())
}

/// One run's effort change, sent as a per-message effort.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffortMessage {
    /// The position (a message's ordinal, else its index in the prompt) of the run's first user
    /// message. The effort message goes right after it, before the first message positioned
    /// later, so the reply to that message already uses the new effort.
    pub after: usize,
    pub effort: String,
}

/// How an Anthropic request sends the efforts of its runs: the effort its top level keeps and
/// the per-message changes after it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EffortPlan {
    /// The effort in force when the newest run of cache-keeping changes began: the top level
    /// keeps it, so the cached prefix doesn't move.
    pub top_level: String,
    pub messages: Vec<EffortMessage>,
}

/// The per-message efforts an Anthropic request sends, read from its runs' frozen configs
/// (`opts.render_episodes`, oldest first, the request's own run last). `None` when the request
/// has no per-message effort to send, which renders the top-level effort as it always did.
///
/// Walking the runs, a run on another model, with other thinking, or whose own config can't send
/// a per-message effort, starts again from its own effort: that change was sent at the top level.
/// A run without an Anthropic effort (another family, or no effort variant) starts again too, and
/// the next run with an effort anchors the top level. Each later run whose effort differs from the
/// one in force adds a per-message effort. A config that doesn't read gives no plan.
pub(crate) fn anthropic_plan(
    frozen: &FrozenRenderConfig,
    opts: &CallOptions,
) -> Option<EffortPlan> {
    if frozen.family_id != WireFamilyId::AnthropicMessages || !per_message_effort(frozen) {
        return None;
    }
    let configs: Vec<FrozenRenderConfig> = opts
        .render_episodes
        .episodes
        .iter()
        .map(|episode| serde_json::from_value(episode.frozen_config.clone()))
        .collect::<Result<_, _>>()
        .ok()?;
    // `None` while no run since the last reset has sent an effort: there is no top level to keep.
    let mut plan: Option<EffortPlan> = None;
    for (index, episode) in opts.render_episodes.episodes.iter().enumerate() {
        let config = &configs[index];
        let Some(effort) = anthropic_effort(config) else {
            plan = None;
            continue;
        };
        let same_route = index > 0 && {
            let before = &configs[index - 1];
            config.family_id == before.family_id
                && config.wire_model_id == before.wire_model_id
                && thinking_mode(config) == thinking_mode(before)
        };
        match plan.as_mut() {
            Some(current) if same_route => {
                let in_force = current
                    .messages
                    .last()
                    .map_or(current.top_level.as_str(), |message| {
                        message.effort.as_str()
                    });
                if effort == in_force {
                    continue;
                }
                if per_message_effort(config) {
                    current.messages.push(EffortMessage {
                        after: episode.first_user.unwrap_or(episode.prompt_start),
                        effort: effort.to_owned(),
                    });
                    continue;
                }
            }
            _ => {}
        }
        plan = Some(EffortPlan {
            top_level: effort.to_owned(),
            messages: Vec::new(),
        });
    }
    plan.filter(|plan| !plan.messages.is_empty())
}

#[cfg(test)]
#[path = "effort_change_tests.rs"]
mod tests;
