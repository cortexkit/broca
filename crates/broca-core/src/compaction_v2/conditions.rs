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
    /// None means the host has no account identity to compare, not an account switch.
    pub account: Option<String>,
    pub surface: String,
}

/// Every applicable cause, without precedence suppressing a simultaneous cause.
/// The caller supplies the session's model, never a one-step fallback model.
pub fn ride_causes(
    previous: Option<&SendContext>,
    current: &SendContext,
    cache_expired: bool,
    chain_reset: bool,
    request_tokens: u64,
    conditions: &Conditions,
) -> Vec<RideCause> {
    let mut causes = Vec::new();
    if cache_expired {
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
        if previous
            .account
            .as_ref()
            .zip(current.account.as_ref())
            .is_some_and(|(previous, current)| previous != current)
        {
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

/// Whether the session's prompt cache has provably expired: the log records the last send that
/// reached the cache, that send asked the provider for a cache lifetime it honours (`ttl_ms`),
/// and that lifetime has run out since. A lifetime broca only estimates proves nothing. When broca
/// can't tell (no requested lifetime, or no send yet), the answer is `false`.
///
/// Before a step, broca may *ride*: call the session's compaction provider with the causes that
/// are about to change the request anyway. The `cache_expired` cause tells the provider that
/// rewriting history now costs nothing extra, so it publishes any view it has queued. Claiming it when unsure made every step
/// on routes without a cache clock look like a free rebuild, and each published view then cost a
/// resend of the whole conversation behind its first edit.
pub fn cache_proven_expired(ttl_ms: Option<u64>, last_send_ms: Option<u64>, now_ms: u64) -> bool {
    ttl_ms
        .zip(last_send_ms)
        .is_some_and(|(ttl, sent)| now_ms >= sent.saturating_add(ttl))
}

pub fn usable_window(conditions: &Conditions) -> u64 {
    conditions
        .context_window
        .saturating_sub(conditions.output_reserve)
}

/// Setup and notices describe the same frozen model, output reserve and rules.
/// `detects_cache_expired` says whether the session's requests ask for a cache lifetime that can
/// prove expiry (see `runtime::detects`).
pub fn setup_request(
    preset: &str,
    conditions: &Conditions,
    now: u64,
    detects_cache_expired: bool,
) -> SetupRequest {
    // Broca hosts the session and creates sessions on their first session.send.
    // Leave child_harness absent so provider children inherit the session's harness
    // (compaction contract W5.1, CONTRACT.md:520).
    let mut setup = SetupRequest::new(
        preset,
        conditions.model.clone(),
        conditions.context_window,
        conditions.output_reserve,
        conditions.rules.clone(),
        super::runtime::detects(detects_cache_expired),
        now,
    );
    setup.accepts_images = conditions.accepts_images;
    enable_metadata_writer_view_anchor(&mut setup);
    setup
}

fn enable_metadata_writer_view_anchor(_setup: &mut SetupRequest) {
    // When cortexkit-role-compaction exposes Setup's view_anchor, set it to
    // Some(true) here: this common constructor covers both live and serving Setup,
    // and this activation point belongs only to builds that durably store anchors.
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
    if recorded == 2 || recorded == supported {
        return Ok(());
    }
    Err(format!(
        "compaction_plan_version {recorded} cannot resume; supported compaction plan version 2 or version {supported}"
    ))
}
