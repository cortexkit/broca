//! The rules of a session's plan changes: what `session.refresh`, `session.refresh_policy` and
//! `session.flush_prefix` decide, which manifest a new run freezes, and which plan a later send
//! may carry. The session actor on today's storage applies them, and so does an actor that keeps
//! its sessions in another storage: each reads what a rule asks about the session its own way,
//! as [`ChangeFacts`], and both decide alike.

use std::sync::Arc;

use broca_fetch_plan::session_change::{self, PendingState, SessionChangeState};
use broca_wal::PlanManifest;

use crate::serve::SessionChangeError;

/// What one refresh was accepted with: its plan's identity and policy, and the reply flags it
/// was answered with, which an identical repeat is answered with again.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Accepted {
    pub plan_identity: String,
    pub policy: String,
    pub run_in_progress: bool,
    pub applies_from_next_run: bool,
}

/// What the plan-change rules read about a session for one request: its change counters, the
/// pending change and the latest run's manifest, and what the session recorded under the
/// request's own generation and `send_id`. Only those are looked up, so a storage can answer
/// without holding every generation the session ever accepted.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct ChangeFacts {
    /// The highest generation accepted; `0` before any.
    pub accepted_generation: u64,
    pub pending: Option<PendingState>,
    /// The latest run's frozen manifest.
    pub current_manifest: Option<Arc<PlanManifest>>,
    /// A flush hasn't reached a new run yet.
    pub flushed: bool,
    /// The refresh accepted under the request's generation, if one was.
    pub accepted: Option<Accepted>,
    /// How the request's generation applied (`fold` or `append`), if it did.
    pub applied_by: Option<String>,
    /// The policy change recorded under the request's `send_id`: its generation and policy.
    pub policy_send: Option<(u64, String)>,
    /// The flush recorded under the request's `send_id`, with the pending generation it
    /// reported.
    pub flush_send: Option<Option<u64>>,
}

impl ChangeFacts {
    /// The facts today's fold `state` holds, for a request naming `generation` and `send_id`.
    pub fn of(state: &SessionChangeState, generation: Option<u64>, send_id: Option<&str>) -> Self {
        let accepted = generation.and_then(|generation| {
            let (plan_identity, policy) = state.accepted.get(&generation)?;
            let (run_in_progress, applies_from_next_run) = state.accepted_replies[&generation];
            Some(Accepted {
                plan_identity: plan_identity.clone(),
                policy: policy.clone(),
                run_in_progress,
                applies_from_next_run,
            })
        });
        Self {
            accepted_generation: state.accepted_generation,
            pending: state.pending.clone(),
            current_manifest: state.current_manifest.clone(),
            flushed: state.flush_pending,
            accepted,
            applied_by: generation.and_then(|generation| state.applied.get(&generation).cloned()),
            policy_send: send_id.and_then(|send_id| state.policy_send_ids.get(send_id).cloned()),
            flush_send: send_id.and_then(|send_id| state.flush_send_ids.get(send_id).copied()),
        }
    }
}

/// Where a refresh of `generation`, whose plan has `plan_identity`, stands against what the
/// session accepted: an identical repeat of an accepted refresh, answered with the flags it was
/// answered with (`Some`); a new generation (`None`); or a stale one, refused.
pub(crate) fn refresh_order(
    facts: &ChangeFacts,
    generation: u64,
    plan_identity: &str,
    policy: &str,
) -> Result<Option<(bool, bool)>, SessionChangeError> {
    use cortexkit_role_llm_runner::session_change::{refresh_order, RefreshOrder};
    let earlier = facts
        .accepted
        .as_ref()
        .map(|accepted| (accepted.plan_identity.as_str(), accepted.policy.as_str()));
    match refresh_order(
        facts.accepted_generation,
        generation,
        plan_identity,
        policy,
        earlier,
    ) {
        RefreshOrder::Repeat => {
            let accepted = facts
                .accepted
                .as_ref()
                .expect("a repeat names an accepted refresh");
            Ok(Some((
                accepted.run_in_progress,
                accepted.applies_from_next_run,
            )))
        }
        RefreshOrder::Stale => Err(SessionChangeError::GenerationStale {
            accepted: facts.accepted_generation,
        }),
        RefreshOrder::New => Ok(None),
    }
}

/// What the actor does with a refresh that reached it.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum RefreshAcceptance {
    /// An identical repeat, answered with its original flags; nothing is written.
    Repeat {
        run_in_progress: bool,
        applies_from_next_run: bool,
    },
    Refused(SessionChangeError),
    /// A new refresh: it is written with these flags, and answered with them once durable.
    Accept {
        run_in_progress: bool,
        applies_from_next_run: bool,
    },
}

/// The actor's checks of a refresh of `generation` under `policy`, whose fetched manifest is
/// `manifest`, made where it writes: generation order against what the session accepted since
/// the serve layer looked, the compaction item, which stays the session's for its life, and the
/// rungs the changed surfaces support. A new refresh's reply says whether a run is in progress
/// and whether the change applies from the next run, which it does under `immediately` or after
/// a flush.
pub(crate) fn accept_refresh(
    facts: &ChangeFacts,
    generation: u64,
    policy: &str,
    manifest: &PlanManifest,
    run_in_progress: bool,
) -> RefreshAcceptance {
    match refresh_order(facts, generation, &manifest.plan_identity, policy) {
        Ok(Some((run_in_progress, applies_from_next_run))) => {
            return RefreshAcceptance::Repeat {
                run_in_progress,
                applies_from_next_run,
            }
        }
        Err(refusal) => return RefreshAcceptance::Refused(refusal),
        Ok(None) => {}
    }
    let next_plan =
        serde_json::from_str(manifest.plan.as_json_text()).expect("admitted plan is JSON");
    if session_change::check_compaction_item(facts.current_manifest.as_deref(), &next_plan).is_err()
    {
        return RefreshAcceptance::Refused(SessionChangeError::InvalidParams {
            field: "plan.compaction_item".into(),
            message: "the session's compaction provider, preset and params are fixed for its life"
                .into(),
        });
    }
    if let Some(surface) =
        session_change::unsupported_surface(facts.current_manifest.as_deref(), manifest, policy)
    {
        return RefreshAcceptance::Refused(SessionChangeError::RungUnsupported {
            surface: surface.into(),
            rung: policy.into(),
        });
    }
    RefreshAcceptance::Accept {
        run_in_progress,
        applies_from_next_run: policy == "immediately" || facts.flushed,
    }
}

/// Classify a `session.refresh_policy` without side effects: `Ok(true)` for an identical repeat
/// of an accepted change, `Ok(false)` for a new change to write, else its refusal. The serve
/// preflight checks it to avoid opening an actor for a refusal; the actor checks it again where
/// it writes.
pub(crate) fn check_policy(
    facts: &ChangeFacts,
    generation: u64,
    policy: &str,
    send_id: &str,
) -> Result<bool, SessionChangeError> {
    if let Some((earlier_generation, earlier_policy)) = &facts.policy_send {
        let field = if generation != *earlier_generation {
            Some("generation")
        } else if policy != earlier_policy {
            Some("policy")
        } else {
            None
        };
        return match field {
            None => Ok(true),
            Some(field) => Err(SessionChangeError::SendIdReuse {
                field: field.into(),
                message: format!("send_id {send_id} was used with another {field}"),
            }),
        };
    }
    if let Some(applied_by) = &facts.applied_by {
        return Err(SessionChangeError::AlreadyApplied {
            generation,
            applied_by: applied_by.clone(),
        });
    }
    let Some(pending) = facts
        .pending
        .as_ref()
        .filter(|pending| pending.generation == generation)
    else {
        return Err(SessionChangeError::PendingSuperseded {
            generation,
            pending: facts.accepted_generation,
        });
    };
    if let Some(surface) = session_change::unsupported_surface(
        facts.current_manifest.as_deref(),
        &pending.manifest,
        policy,
    ) {
        return Err(SessionChangeError::RungUnsupported {
            surface: surface.into(),
            rung: policy.into(),
        });
    }
    Ok(false)
}

/// What a `session.flush_prefix` does: a repeat of a recorded flush is answered with the pending
/// generation that flush reported (`Err`); a new flush is written reporting the generation
/// pending now (`Ok`).
pub(crate) fn flush(facts: &ChangeFacts) -> Result<Option<u64>, Option<u64>> {
    match facts.flush_send {
        Some(pending_generation) => Err(pending_generation),
        None => Ok(facts.pending.as_ref().map(|pending| pending.generation)),
    }
}

/// The pending change, when it applies at a run start on `model`: `rebuild` and `flushed` say
/// whether a prefix rebuild and a flush happened since it was accepted, and `last_model` is the
/// previous run's model key.
pub(crate) fn pending_applying<'a>(
    pending: Option<&'a PendingState>,
    rebuild: bool,
    flushed: bool,
    last_model: Option<&str>,
    model: &broca_protocol::ModelHandle,
) -> Option<&'a PendingState> {
    pending.filter(|pending| {
        session_change::applies_at_run_start(
            &pending.policy,
            rebuild,
            flushed,
            last_model,
            &session_change::model_key(model),
        )
        .is_some()
    })
}

/// The manifest a new run of a session that already ran freezes. A pending change applies at the
/// start of the first run at or after its policy's trigger, never inside a running run, so a run
/// re-driven after a crash (`resuming`) keeps the latest run's manifest, `current`. Otherwise
/// `applying`, the pending change when it applies at this run, gives the run its manifest, once
/// its model-bearing items accept the run's `model` and `variant`. Earlier runs keep resuming
/// from their own manifests.
pub(crate) fn run_manifest(
    current: Option<Arc<PlanManifest>>,
    applying: Option<&PendingState>,
    resuming: bool,
    model: &broca_protocol::ModelHandle,
    variant: Option<&str>,
) -> Result<Option<Arc<PlanManifest>>, broca_fetch_plan::Refusal> {
    if resuming {
        // A pending refresh cannot change an episode re-driven after a crash.
        return Ok(current);
    }
    match applying {
        Some(pending) => {
            session_change::check_applying_model(pending, model, variant)?;
            Ok(Some(pending.manifest.clone()))
        }
        None => Ok(current),
    }
}

/// Whether a later send's plan, of identity `sent`, may run on the session. Before any refresh
/// it must be the plan the first run froze, `frozen`. Once a refresh was accepted
/// (`refreshed`), it may be the plan the latest run froze or the latest accepted one, given in
/// that order.
pub(crate) fn later_send_plan(
    frozen: Option<&str>,
    refreshed: Option<(Option<&str>, Option<&str>)>,
    sent: &str,
) -> Result<(), broca_fetch_plan::Refusal> {
    match refreshed {
        Some((effective, accepted)) => {
            cortexkit_role_llm_runner::session_change::check_send_drift(effective, accepted, sent)
                .map_err(|detail| broca_fetch_plan::Refusal::PlanDrift {
                    frozen: detail.frozen,
                    sent: detail.sent,
                })
        }
        None => broca_fetch_plan::check_later_plan(frozen, sent),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(identity: &str, tools: &[&str], system_text: &str) -> PlanManifest {
        // An admissible empty plan, which a pending change's model check can parse.
        let composition = serde_json::json!({"providers": []});
        let digest =
            cortexkit_role_tool_provider::catalog::composition_digest(&composition).unwrap();
        let plan = serde_json::json!({
            "tool_items": [], "system_text_items": [], "step_transform_items": [],
            "composition": composition, "composition_digest": digest,
        });
        serde_json::from_value(serde_json::json!({
            "plan": plan.to_string(),
            "plan_identity": identity,
            "composition_digest": "c",
            "fetches": [],
            "tools": tools.iter().map(|name| serde_json::json!({
                "name": name, "description": "", "input_schema": {"type": "object"},
                "module": "m", "execution_mode": "parallel"
            })).collect::<Vec<_>>(),
            "system_text": system_text,
            "system_digest": "s",
            "join_version": 1,
            "role_versions": {}
        }))
        .unwrap()
    }

    fn facts() -> ChangeFacts {
        ChangeFacts {
            accepted_generation: 2,
            current_manifest: Some(Arc::new(manifest("old", &["a"], "text"))),
            accepted: Some(Accepted {
                plan_identity: "plan-2".into(),
                policy: "immediately".into(),
                run_in_progress: true,
                applies_from_next_run: true,
            }),
            ..ChangeFacts::default()
        }
    }

    #[test]
    fn a_refresh_is_a_repeat_new_or_stale_by_what_was_accepted() {
        let facts = facts();
        assert_eq!(
            refresh_order(&facts, 2, "plan-2", "immediately"),
            Ok(Some((true, true)))
        );
        assert_eq!(refresh_order(&facts, 3, "plan-3", "immediately"), Ok(None));
        assert_eq!(
            refresh_order(&facts, 2, "plan-other", "immediately"),
            Err(SessionChangeError::GenerationStale { accepted: 2 })
        );
        let mut older = facts;
        older.accepted = None;
        assert_eq!(
            refresh_order(&older, 1, "plan-1", "immediately"),
            Err(SessionChangeError::GenerationStale { accepted: 2 })
        );
    }

    #[test]
    fn a_new_refresh_applies_from_the_next_run_when_immediate_or_flushed() {
        let mut facts = facts();
        let next = manifest("plan-3", &["a"], "text");
        assert_eq!(
            accept_refresh(&facts, 3, "on_prefix_rebuild", &next, false),
            RefreshAcceptance::Accept {
                run_in_progress: false,
                applies_from_next_run: false
            }
        );
        facts.flushed = true;
        assert_eq!(
            accept_refresh(&facts, 3, "on_prefix_rebuild", &next, true),
            RefreshAcceptance::Accept {
                run_in_progress: true,
                applies_from_next_run: true
            }
        );
        assert_eq!(
            accept_refresh(
                &facts,
                2,
                "immediately",
                &manifest("plan-2", &[], ""),
                false
            ),
            RefreshAcceptance::Repeat {
                run_in_progress: true,
                applies_from_next_run: true
            }
        );
    }

    #[test]
    fn a_refresh_changing_the_compaction_item_or_an_unsupported_rung_is_refused() {
        let facts = facts();
        let mut compacting = manifest("plan-3", &["a"], "text");
        compacting.plan =
            broca_wal::OpaqueJson::from_json_text(r#"{"compaction_item":{"provider":"p"}}"#)
                .unwrap();
        assert!(matches!(
            accept_refresh(&facts, 3, "immediately", &compacting, false),
            RefreshAcceptance::Refused(SessionChangeError::InvalidParams { field, .. })
                if field == "plan.compaction_item"
        ));
        assert_eq!(
            accept_refresh(
                &facts,
                3,
                "on_system_rebuild",
                &manifest("plan-3", &["b"], "text"),
                false
            ),
            RefreshAcceptance::Refused(SessionChangeError::RungUnsupported {
                surface: "tools".into(),
                rung: "on_system_rebuild".into(),
            })
        );
    }

    #[test]
    fn a_policy_change_repeats_reuses_and_supersedes_by_what_was_recorded() {
        let mut facts = facts();
        facts.policy_send = Some((2, "immediately".into()));
        assert_eq!(check_policy(&facts, 2, "immediately", "p"), Ok(true));
        assert!(matches!(
            check_policy(&facts, 3, "immediately", "p"),
            Err(SessionChangeError::SendIdReuse { field, .. }) if field == "generation"
        ));
        assert!(matches!(
            check_policy(&facts, 2, "on_prefix_rebuild", "p"),
            Err(SessionChangeError::SendIdReuse { field, .. }) if field == "policy"
        ));
        facts.policy_send = None;
        facts.applied_by = Some("append".into());
        assert_eq!(
            check_policy(&facts, 2, "immediately", "p"),
            Err(SessionChangeError::AlreadyApplied {
                generation: 2,
                applied_by: "append".into()
            })
        );
        facts.applied_by = None;
        assert_eq!(
            check_policy(&facts, 2, "immediately", "p"),
            Err(SessionChangeError::PendingSuperseded {
                generation: 2,
                pending: 2
            })
        );
        facts.pending = Some(PendingState {
            generation: 2,
            policy: "on_prefix_rebuild".into(),
            manifest: Arc::new(manifest("plan-2", &["a"], "text")),
        });
        assert_eq!(check_policy(&facts, 2, "immediately", "p"), Ok(false));
    }

    #[test]
    fn a_flush_repeat_answers_what_it_reported() {
        let mut facts = facts();
        assert_eq!(flush(&facts), Ok(None));
        facts.pending = Some(PendingState {
            generation: 2,
            policy: "on_prefix_rebuild".into(),
            manifest: Arc::new(manifest("plan-2", &[], "")),
        });
        assert_eq!(flush(&facts), Ok(Some(2)));
        facts.flush_send = Some(None);
        assert_eq!(flush(&facts), Err(None));
    }

    #[test]
    fn a_resumed_run_keeps_its_manifest_and_a_new_one_takes_the_applying_change() {
        let current = Arc::new(manifest("old", &["a"], "text"));
        let pending = PendingState {
            generation: 2,
            policy: "immediately".into(),
            manifest: Arc::new(manifest("plan-2", &["b"], "text")),
        };
        let model = broca_protocol::ModelHandle::new("mock", "m");
        assert_eq!(
            run_manifest(Some(current.clone()), Some(&pending), true, &model, None),
            Ok(Some(current.clone()))
        );
        assert_eq!(
            run_manifest(Some(current.clone()), Some(&pending), false, &model, None),
            Ok(Some(pending.manifest.clone()))
        );
        assert_eq!(
            run_manifest(Some(current.clone()), None, false, &model, None),
            Ok(Some(current))
        );
    }

    #[test]
    fn a_pending_change_applies_by_its_policy_rebuilds_and_model_switches() {
        let pending = PendingState {
            generation: 1,
            policy: "on_prefix_rebuild".into(),
            manifest: Arc::new(manifest("plan-1", &[], "")),
        };
        let model = broca_protocol::ModelHandle::new("mock", "m");
        let applying = |rebuild, flushed, last: Option<&str>| {
            pending_applying(Some(&pending), rebuild, flushed, last, &model).is_some()
        };
        assert!(!applying(false, false, Some("mock:m")));
        assert!(applying(true, false, Some("mock:m")));
        assert!(applying(false, true, Some("mock:m")));
        assert!(applying(false, false, Some("mock:other")));
        assert!(!applying(false, false, None));
    }

    #[test]
    fn a_later_plan_is_checked_against_the_first_run_until_a_refresh() {
        assert!(later_send_plan(Some("a"), None, "a").is_ok());
        assert!(later_send_plan(Some("a"), None, "b").is_err());
        assert!(later_send_plan(Some("a"), Some((Some("b"), Some("c"))), "c").is_ok());
        assert!(later_send_plan(Some("a"), Some((Some("b"), Some("c"))), "b").is_ok());
        assert!(later_send_plan(Some("a"), Some((Some("b"), Some("c"))), "a").is_err());
    }
}
