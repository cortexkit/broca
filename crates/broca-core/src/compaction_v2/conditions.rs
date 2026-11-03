//! Session conditions are independent of one-step refusal fallbacks. A fallback
//! resend must not make the provider edit the session for a different model.

use cortexkit_role_compaction::{
    notice::Conditions,
    ride::RideCause,
    setup::{ModelRef, SetupRequest},
};

/// Evidence of the last accepted send, recovered from durable request facts.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SendContext {
    pub model: ModelRef,
    pub account: Option<String>,
    pub surface: String,
}

/// Every applicable cause, without precedence suppressing a simultaneous cause.
/// The caller supplies the session's model, never a one-step fallback model.
pub fn ride_causes(
    previous: Option<&SendContext>,
    current: &SendContext,
    cache_warm: bool,
    chain_reset: bool,
    request_tokens: u64,
    conditions: &Conditions,
) -> Vec<RideCause> {
    let mut causes = Vec::new();
    if !cache_warm {
        causes.push(RideCause::CacheExpired);
    }
    if chain_reset {
        causes.push(RideCause::ChainReset);
    }
    if let Some(previous) = previous {
        if previous.model.provider != current.model.provider
            || previous.model.model != current.model.model
            || previous.model.variant != current.model.variant
        {
            causes.push(RideCause::ModelChange);
        }
        if previous.model.service_tier != current.model.service_tier {
            causes.push(RideCause::TierChange);
        }
        if previous.account != current.account {
            causes.push(RideCause::AccountChange);
        }
        if previous.surface != current.surface {
            causes.push(RideCause::SurfaceChange);
        }
    }
    if request_tokens > usable_window(conditions) {
        causes.push(RideCause::OverLimit);
    }
    causes
}

pub fn usable_window(conditions: &Conditions) -> u64 {
    conditions
        .context_window
        .saturating_sub(conditions.output_reserve)
}

/// Setup and notices describe the same frozen model, output reserve and rules.
pub fn setup_request(preset: &str, conditions: &Conditions, now: u64) -> SetupRequest {
    let mut setup = SetupRequest::new(
        preset,
        conditions.model.clone(),
        conditions.context_window,
        conditions.output_reserve,
        conditions.rules.clone(),
        super::runtime::detects(),
        now,
    );
    setup.accepts_images = conditions.accepts_images;
    setup
}

/// The library, rather than host-local tables, owns the rule defaults.
pub fn conditions(
    model: ModelRef,
    context_window: u64,
    output_reserve: u64,
    chained: bool,
) -> Conditions {
    let mut rules = cortexkit_compaction_view::rules::defaults(&model.model);
    if chained {
        rules.history_edit_cost = cortexkit_role_compaction::rules::EditCost::FullResend;
    }
    Conditions::new(model, context_window, output_reserve, rules)
}

/// A version change could change provider bytes on resume, even when the view
/// records themselves still decode. Refuse by the field's name before rendering.
pub fn check_plan_version(recorded: u32) -> Result<(), String> {
    let supported = cortexkit_compaction_view::PLAN_VERSION;
    if recorded == supported {
        return Ok(());
    }
    Err(format!(
        "compaction_plan_version {recorded} cannot resume with compaction plan version {supported}"
    ))
}
