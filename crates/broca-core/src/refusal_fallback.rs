//! The client-side refusal-fallback schedule for Anthropic runs, as pure transitions over the
//! values the WAL records (`RefusalFallbackSchedule`). No I/O: replay folds the same transitions
//! from the records that the run loop applies live, so a restart or resume reaches the same
//! schedule.
//!
//! The schedule mirrors the OpenCode Anthropic plugin's client fallback. After the source model
//! refuses (`stop_reason: "refusal"`), the next 10 responses come from the fallback model. Then
//! the session probes the source again. A refusal during the probe, before ten clean source
//! responses, doubles the next round (20, then 40 at most); ten clean source responses make the
//! next refusal start again at 10. A refusal on the fallback model neither counts nor chains to
//! another model. The fallback model is an input: the run freezes it, the schedule only records
//! it.

use broca_protocol::request::ModelHandle;
use broca_wal::{RefusalFallbackPhase, RefusalFallbackSchedule, StepFallback, WalRecord};

/// Committed fallback responses in a round before the first doubling.
pub const ROUND_STEPS: u32 = 10;
/// Clean source responses after a round that make the next refusal start again at
/// [`ROUND_STEPS`]; also the cap on the probe's count.
pub const CLEAN_TO_RESET: u32 = 10;
/// How many times a round can double: 10, 20, then 40.
pub const MAX_DOUBLINGS: u8 = 2;
/// A schedule untouched this long is cleared when the next episode is admitted.
pub const IDLE_EXPIRY_MS: u64 = 24 * 60 * 60 * 1000;

/// Which model a step is sent to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StepRoute {
    /// The episode's own model.
    Source,
    /// The schedule's fallback model.
    Fallback,
}

/// How a committed step ended, as the schedule needs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CommittedStep {
    /// Where the step was sent.
    pub route: StepRoute,
    /// The step ended `content_filter` (a refusal that committed instead of being re-sent).
    pub refused: bool,
    /// An Anthropic server-side fallback served the step with another model.
    pub served_by_server_side_fallback: bool,
}

/// The schedule after the source model refuses a step. `previous` is the session's schedule
/// before the refusal, if any; `fallback` is the model the run froze for refusals; `at_ms` is
/// the time of the record that will carry the result.
pub fn on_source_refusal(
    previous: Option<&RefusalFallbackSchedule>,
    source: &ModelHandle,
    fallback: &ModelHandle,
    at_ms: Option<u64>,
) -> RefusalFallbackSchedule {
    let re_refusals = match previous {
        None => 0,
        // Ten clean source responses since the last round: the streak resets.
        Some(RefusalFallbackSchedule {
            phase: RefusalFallbackPhase::Probe { clean },
            ..
        }) if *clean >= CLEAN_TO_RESET => 0,
        Some(previous) => previous.re_refusals.saturating_add(1).min(MAX_DOUBLINGS),
    };
    RefusalFallbackSchedule {
        source: source.clone(),
        fallback: fallback.clone(),
        phase: RefusalFallbackPhase::Fallback {
            remaining: ROUND_STEPS << re_refusals.min(MAX_DOUBLINGS),
        },
        re_refusals,
        updated_at_ms: at_ms.or(previous.and_then(|previous| previous.updated_at_ms)),
    }
}

/// The schedule after a step commits. `None` stays `None`: only a refusal arms a schedule.
pub fn on_step_committed(
    schedule: Option<&RefusalFallbackSchedule>,
    step: CommittedStep,
    at_ms: Option<u64>,
) -> Option<RefusalFallbackSchedule> {
    let mut next = schedule?.clone();
    next.phase = match (next.phase, step.route) {
        // A refusal on the fallback model passes through: no count, no chain.
        (phase, StepRoute::Fallback) if step.refused => phase,
        (RefusalFallbackPhase::Fallback { remaining }, StepRoute::Fallback) => {
            match remaining.saturating_sub(1) {
                0 => RefusalFallbackPhase::Probe { clean: 0 },
                remaining => RefusalFallbackPhase::Fallback { remaining },
            }
        }
        // A step can reach the source during a round only when it was not eligible for the
        // fallback (for example a prompt larger than the fallback's context); it does not move
        // the round.
        (phase @ RefusalFallbackPhase::Fallback { .. }, StepRoute::Source) => phase,
        (phase @ RefusalFallbackPhase::Probe { .. }, StepRoute::Source) if step.refused => phase,
        // A source response a server-side fallback served is not a clean source response.
        (RefusalFallbackPhase::Probe { .. }, StepRoute::Source)
            if step.served_by_server_side_fallback =>
        {
            RefusalFallbackPhase::Probe { clean: 0 }
        }
        (RefusalFallbackPhase::Probe { clean }, StepRoute::Source) => RefusalFallbackPhase::Probe {
            clean: clean.saturating_add(1).min(CLEAN_TO_RESET),
        },
        // Only a round routes to the fallback; a probe step sent there is left as it is.
        (phase @ RefusalFallbackPhase::Probe { .. }, StepRoute::Fallback) => phase,
    };
    if at_ms.is_some() {
        next.updated_at_ms = at_ms;
    }
    Some(next)
}

/// The schedule a newly admitted episode starts from: cleared when the episode runs another
/// source model, or when it is admitted more than [`IDLE_EXPIRY_MS`] after the schedule last
/// changed. An episode or schedule without a time keeps the schedule.
pub fn on_episode_admitted(
    schedule: Option<RefusalFallbackSchedule>,
    model: &ModelHandle,
    admitted_at_ms: Option<u64>,
) -> Option<RefusalFallbackSchedule> {
    let schedule = schedule?;
    if &schedule.source != model {
        return None;
    }
    if let (Some(now), Some(updated)) = (admitted_at_ms, schedule.updated_at_ms) {
        if now.saturating_sub(updated) > IDLE_EXPIRY_MS {
            return None;
        }
    }
    Some(schedule)
}

/// Where the next step goes: the fallback model while a round is running and the run is
/// eligible for the fallback, the source otherwise.
pub fn route(schedule: Option<&RefusalFallbackSchedule>, eligible: bool) -> StepRoute {
    match schedule.map(|schedule| schedule.phase) {
        Some(RefusalFallbackPhase::Fallback { remaining }) if eligible && remaining > 0 => {
            StepRoute::Fallback
        }
        _ => StepRoute::Source,
    }
}

/// The model a step's usage is reported and billed under, as `(provider, model, variant)`:
/// the client-side fallback model for a step routed there, the model a server-side fallback
/// served the response with, or else the episode's model and variant. The variant is dropped
/// in the first two cases, because the caller's variant is not what the other model ran.
/// `None` for a record that carries no step usage.
pub fn usage_model<'a>(
    episode_model: &'a ModelHandle,
    episode_variant: Option<&'a str>,
    record: &'a WalRecord,
) -> Option<(&'a str, &'a str, Option<&'a str>)> {
    let (route, served_model) = match record {
        WalRecord::ModelStepFinished { fallback, .. } => (
            fallback.route.as_ref().map(|route| &route.model),
            fallback.served_model.as_deref(),
        ),
        WalRecord::ModelStepRefused { served_model, .. } => (None, served_model.as_deref()),
        _ => return None,
    };
    Some(match (route, served_model) {
        (_, Some(served)) => (episode_model.provider_module_id.as_str(), served, None),
        (Some(model), None) => (
            model.provider_module_id.as_str(),
            model.model_id.as_str(),
            None,
        ),
        (None, None) => (
            episode_model.provider_module_id.as_str(),
            episode_model.model_id.as_str(),
            episode_variant,
        ),
    })
}

/// Usage entries for a durable step, with the identity of each serving model. The
/// episode variant applies only to an ordinary single-model response: the provider
/// controls the models and settings used in a server-side iteration split.
pub fn usage_by_model<'a>(
    episode_model: &'a ModelHandle,
    episode_variant: Option<&'a str>,
    record: &'a WalRecord,
) -> Vec<(&'a str, &'a str, Option<&'a str>, broca_protocol::Usage)> {
    let (usage, iterations) = match record {
        WalRecord::ModelStepFinished {
            usage, fallback, ..
        } => (*usage, fallback.usage_iterations.as_deref()),
        WalRecord::ModelStepRefused {
            usage,
            usage_iterations,
            ..
        } => (*usage, usage_iterations.as_deref()),
        _ => return Vec::new(),
    };
    let Some((provider, model, variant)) = usage_model(episode_model, episode_variant, record)
    else {
        return Vec::new();
    };
    broca_protocol::usage::model_usage(model, usage, iterations)
        .into_iter()
        .map(|(model, usage)| {
            (
                provider,
                model,
                if iterations.is_some() { None } else { variant },
                usage,
            )
        })
        .collect()
}

/// Total usage of a durable step, including logs written before iteration totals
/// were stored in the top-level usage member.
pub fn total_step_usage(record: &WalRecord) -> Option<broca_protocol::Usage> {
    match record {
        WalRecord::ModelStepFinished {
            usage, fallback, ..
        } => Some(broca_protocol::usage::step_usage(
            *usage,
            fallback.usage_iterations.as_deref(),
        )),
        WalRecord::ModelStepRefused {
            usage,
            usage_iterations,
            ..
        } => Some(broca_protocol::usage::step_usage(
            *usage,
            usage_iterations.as_deref(),
        )),
        _ => None,
    }
}

/// The model that answered a committed step, when it was not the episode's own: the
/// client-side fallback model the step was routed to.
pub fn step_origin(fallback: &StepFallback) -> Option<&ModelHandle> {
    fallback.route.as_ref().map(|route| &route.model)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn source() -> ModelHandle {
        ModelHandle::new("anthropic", "claude-opus-5-5")
    }

    fn fallback() -> ModelHandle {
        ModelHandle::new("anthropic", "claude-opus-4-8")
    }

    fn refuse(previous: Option<&RefusalFallbackSchedule>) -> RefusalFallbackSchedule {
        on_source_refusal(previous, &source(), &fallback(), Some(1_000))
    }

    fn commit(
        schedule: &RefusalFallbackSchedule,
        route: StepRoute,
        refused: bool,
        served: bool,
    ) -> RefusalFallbackSchedule {
        on_step_committed(
            Some(schedule),
            CommittedStep {
                route,
                refused,
                served_by_server_side_fallback: served,
            },
            Some(2_000),
        )
        .unwrap()
    }

    /// Run a whole round on the fallback, then `clean` clean source responses.
    fn finish_round(mut schedule: RefusalFallbackSchedule, clean: u32) -> RefusalFallbackSchedule {
        while let RefusalFallbackPhase::Fallback { .. } = schedule.phase {
            assert_eq!(route(Some(&schedule), true), StepRoute::Fallback);
            schedule = commit(&schedule, StepRoute::Fallback, false, false);
        }
        for _ in 0..clean {
            assert_eq!(route(Some(&schedule), true), StepRoute::Source);
            schedule = commit(&schedule, StepRoute::Source, false, false);
        }
        schedule
    }

    fn remaining(schedule: &RefusalFallbackSchedule) -> u32 {
        match schedule.phase {
            RefusalFallbackPhase::Fallback { remaining } => remaining,
            RefusalFallbackPhase::Probe { .. } => panic!("expected a round: {schedule:?}"),
        }
    }

    #[test]
    fn a_first_refusal_arms_a_round_of_ten() {
        let schedule = refuse(None);
        assert_eq!(
            schedule.phase,
            RefusalFallbackPhase::Fallback { remaining: 10 }
        );
        assert_eq!(schedule.re_refusals, 0);
        assert_eq!(
            (schedule.source.clone(), schedule.fallback.clone()),
            (source(), fallback())
        );
        assert_eq!(schedule.updated_at_ms, Some(1_000));
    }

    #[test]
    fn refusals_during_probes_double_the_round_up_to_forty() {
        let mut schedule = refuse(None);
        let mut rounds = vec![remaining(&schedule)];
        for _ in 0..3 {
            // Fewer than ten clean source responses before the next refusal.
            schedule = refuse(Some(&finish_round(schedule, 3)));
            rounds.push(remaining(&schedule));
        }
        assert_eq!(rounds, [10, 20, 40, 40]);
        assert_eq!(schedule.re_refusals, MAX_DOUBLINGS);
    }

    #[test]
    fn a_round_ends_after_exactly_its_count_of_fallback_responses() {
        let mut schedule = refuse(None);
        for left in (1..10).rev() {
            schedule = commit(&schedule, StepRoute::Fallback, false, false);
            assert_eq!(
                schedule.phase,
                RefusalFallbackPhase::Fallback { remaining: left }
            );
        }
        schedule = commit(&schedule, StepRoute::Fallback, false, false);
        assert_eq!(schedule.phase, RefusalFallbackPhase::Probe { clean: 0 });
        assert_eq!(route(Some(&schedule), true), StepRoute::Source);
    }

    #[test]
    fn ten_clean_source_responses_reset_the_next_round_to_ten() {
        let doubled = refuse(Some(&finish_round(refuse(None), 0)));
        assert_eq!(remaining(&doubled), 20);
        let probed = finish_round(doubled, 15);
        assert_eq!(
            probed.phase,
            RefusalFallbackPhase::Probe {
                clean: CLEAN_TO_RESET
            }
        );
        let next = refuse(Some(&probed));
        assert_eq!(remaining(&next), 10);
        assert_eq!(next.re_refusals, 0);
        // Nine are not enough.
        let nine = finish_round(refuse(Some(&finish_round(refuse(None), 0))), 9);
        assert_eq!(remaining(&refuse(Some(&nine))), 40);
    }

    #[test]
    fn a_source_response_served_by_a_server_side_fallback_breaks_the_streak() {
        let mut schedule = finish_round(refuse(None), 9);
        assert_eq!(schedule.phase, RefusalFallbackPhase::Probe { clean: 9 });
        schedule = commit(&schedule, StepRoute::Source, false, true);
        assert_eq!(schedule.phase, RefusalFallbackPhase::Probe { clean: 0 });
        assert_eq!(remaining(&refuse(Some(&schedule))), 20);
    }

    #[test]
    fn a_refusal_on_the_fallback_neither_counts_nor_chains() {
        let schedule = commit(&refuse(None), StepRoute::Fallback, false, false);
        let refused = commit(&schedule, StepRoute::Fallback, true, false);
        assert_eq!(refused.phase, schedule.phase);
        assert_eq!(refused.fallback, fallback());
        assert_eq!(refused.re_refusals, schedule.re_refusals);
        assert_eq!(route(Some(&refused), true), StepRoute::Fallback);
    }

    #[test]
    fn a_committed_source_refusal_during_a_probe_is_not_clean() {
        let schedule = finish_round(refuse(None), 4);
        let refused = commit(&schedule, StepRoute::Source, true, false);
        assert_eq!(refused.phase, RefusalFallbackPhase::Probe { clean: 4 });
    }

    #[test]
    fn no_schedule_stays_none_and_routes_to_the_source() {
        let step = CommittedStep {
            route: StepRoute::Source,
            refused: false,
            served_by_server_side_fallback: false,
        };
        assert_eq!(on_step_committed(None, step, Some(5)), None);
        assert_eq!(route(None, true), StepRoute::Source);
        // An ineligible run never routes to the fallback, whatever the schedule says.
        assert_eq!(route(Some(&refuse(None)), false), StepRoute::Source);
    }

    #[test]
    fn a_model_change_or_a_day_idle_clears_the_schedule_at_admission() {
        let schedule = refuse(None);
        let other = ModelHandle::new("anthropic", "claude-fable-5-1");
        assert_eq!(
            on_episode_admitted(Some(schedule.clone()), &other, Some(1_001)),
            None
        );
        assert_eq!(
            on_episode_admitted(
                Some(schedule.clone()),
                &source(),
                Some(1_000 + IDLE_EXPIRY_MS)
            ),
            Some(schedule.clone())
        );
        assert_eq!(
            on_episode_admitted(
                Some(schedule.clone()),
                &source(),
                Some(1_000 + IDLE_EXPIRY_MS + 1)
            ),
            None
        );
        // Without a time on either side the age is unknown, and the schedule is kept.
        assert_eq!(
            on_episode_admitted(Some(schedule.clone()), &source(), None),
            Some(schedule.clone())
        );
        let mut untimed = schedule;
        untimed.updated_at_ms = None;
        assert_eq!(
            on_episode_admitted(Some(untimed.clone()), &source(), Some(u64::MAX)),
            Some(untimed)
        );
    }

    #[test]
    fn usage_goes_to_the_model_that_answered() {
        let episode = source();
        let step = |extra: serde_json::Value| -> WalRecord {
            let mut value = serde_json::json!({"type":"model_step_finished","step_id":1,
                "assistant_message":{"message_id":"m","content":[]},"usage":{},"retries_used":0});
            value
                .as_object_mut()
                .unwrap()
                .extend(extra.as_object().unwrap().clone());
            serde_json::from_value(value).unwrap()
        };
        let plain = step(serde_json::json!({}));
        assert_eq!(
            usage_model(&episode, Some("deep"), &plain),
            Some(("anthropic", "claude-opus-5-5", Some("deep")))
        );
        let routed = step(serde_json::json!({"route":{"model":fallback(),"reason":"schedule"}}));
        assert_eq!(
            usage_model(&episode, Some("deep"), &routed),
            Some(("anthropic", "claude-opus-4-8", None))
        );
        let served = step(serde_json::json!({"served_model":"claude-opus-4-8"}));
        assert_eq!(
            usage_model(&episode, Some("deep"), &served),
            Some(("anthropic", "claude-opus-4-8", None))
        );
        let refused: WalRecord =
            serde_json::from_value(serde_json::json!({"type":"model_step_refused",
            "step_id":1,"usage":{},"discarded_blocks":0,"schedule":refuse(None)}))
            .unwrap();
        assert_eq!(
            usage_model(&episode, Some("deep"), &refused),
            Some(("anthropic", "claude-opus-5-5", Some("deep")))
        );
        assert_eq!(usage_model(&episode, None, &WalRecord::Unknown), None);
    }
}
