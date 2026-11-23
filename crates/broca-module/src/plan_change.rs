//! The rules of a session's plan changes: what `session.refresh`, `session.refresh_policy` and
//! `session.flush_prefix` decide, which manifest a new run freezes, and which plan a later send
//! may carry. The session actor reads the session facts each rule needs as [`ChangeFacts`] and
//! applies the rules.

use std::collections::BTreeMap;
use std::sync::Arc;

use broca_fetch_plan::session_change::{self, PendingState};
use broca_records::record::PlanManifest;

use crate::serve::SessionChangeError;

/// What one refresh was accepted with: its plan's identity and policy, and the reply flags it
/// was answered with, which an identical repeat is answered with again.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct Accepted {
    pub plan_identity: String,
    pub policy: String,
    /// The providers it asked to read afresh, empty when it named none.
    pub refetch: Vec<String>,
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
    /// What the rules for adding a compaction provider read about a session in the session
    /// store, which can gain one at its next run (`broca_session_loop::enroll`).
    pub enrollment: Option<EnrollmentFacts>,
}

/// What a refresh that adds a compaction provider is checked against in the session store.
#[derive(Debug, Clone, Default, PartialEq)]
pub(crate) struct EnrollmentFacts {
    /// The window holds every message the session placed (`enroll::window_whole`).
    pub window_whole: bool,
    /// The session's earlier runs carried global system messages
    /// (`enroll::carries_global_system`).
    pub carries_global_system: bool,
    /// The model the session's next run takes when a refresh chooses none: the latest refresh
    /// that chose one, else its first run's plan, else its latest run's model.
    pub session_model: Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
}

impl ChangeFacts {
    /// The latest plan the session accepted: its pending refresh's while one waits to apply,
    /// else the one its latest run froze. A refresh's compaction item is compared with it.
    pub fn latest_manifest(&self) -> Option<&PlanManifest> {
        match &self.pending {
            Some(pending) => Some(pending.manifest.as_ref()),
            None => self.current_manifest.as_deref(),
        }
    }
}

/// Where a refresh of `generation`, whose plan has `plan_identity` and which asked to read
/// `refetch` afresh, stands against what the session accepted: an identical repeat of an
/// accepted refresh, answered with the flags it was answered with (`Some`); a new generation
/// (`None`); or a stale one, refused. An empty `refetch` is a refresh that named none.
pub(crate) fn refresh_order(
    facts: &ChangeFacts,
    generation: u64,
    plan_identity: &str,
    policy: &str,
    refetch: &[String],
) -> Result<Option<(bool, bool)>, SessionChangeError> {
    use cortexkit_role_llm_runner::session_change::{refresh_order_with_refetch, RefreshOrder};
    fn named(list: &[String]) -> Option<&[String]> {
        (!list.is_empty()).then_some(list)
    }
    let earlier = facts.accepted.as_ref().map(|accepted| {
        (
            accepted.plan_identity.as_str(),
            accepted.policy.as_str(),
            named(accepted.refetch.as_slice()),
        )
    });
    match refresh_order_with_refetch(
        facts.accepted_generation,
        generation,
        (plan_identity, policy, named(refetch)),
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
/// the serve layer looked, the compaction item, which stays the session's for its life once it
/// has one (a session in the session store may gain one, see [`ChangeFacts::enrollment`]), and the
/// rungs the changed surfaces support. A new refresh's reply says whether a run is in progress
/// and whether the change applies from the next run, which it does under `immediately` or after
/// a flush.
pub(crate) fn accept_refresh(
    facts: &ChangeFacts,
    generation: u64,
    policy: &str,
    manifest: &PlanManifest,
    refetch: &[String],
    run_in_progress: bool,
) -> RefreshAcceptance {
    match refresh_order(facts, generation, &manifest.plan_identity, policy, refetch) {
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
    match (
        session_change::compaction_item_change(facts.latest_manifest(), &next_plan),
        &facts.enrollment,
    ) {
        (Ok(session_change::CompactionItemChange::Same), _) => {}
        // Only the session store moves a session onto the compaction contract, at the run that
        // applies this refresh; the state that move needs is checked again here, where the
        // refresh is written, as the serve layer's preflight checked it before the fetch.
        (Ok(session_change::CompactionItemChange::Added), Some(enrollment)) => {
            if let Err(refusal) = broca_session_loop::enroll::check(
                enrollment.window_whole,
                enrollment.carries_global_system,
                !manifest.system_text.is_empty(),
            ) {
                return RefreshAcceptance::Refused(SessionChangeError::CompactionNotAddable(
                    refusal,
                ));
            }
        }
        _ => {
            return RefreshAcceptance::Refused(SessionChangeError::InvalidParams {
                field: "plan.compaction_item".into(),
                message:
                    "the session's compaction provider, preset and params are fixed for its life"
                        .into(),
            });
        }
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

/// Whether the pending refresh adds a compaction provider to a session that isn't on the
/// current compaction contract (`on_contract`), against `current`, the latest run's manifest.
/// Adding one rebuilds the request's prefix: the system text moves above the conversation and
/// part numbers show. So such a refresh applies at the next run whatever its policy, and its
/// `RefreshApplied` record says `applied_by: "fold"`, as one applied after a prefix rebuild does.
/// Only a session in the session store takes such a refresh; one kept in the write-ahead log
/// refuses it.
pub(crate) fn adds_compaction(
    on_contract: bool,
    current: Option<&PlanManifest>,
    pending: Option<&PendingState>,
) -> bool {
    !on_contract
        && pending.is_some_and(|pending| {
            serde_json::from_str::<serde_json::Value>(pending.manifest.plan.as_json_text())
                .ok()
                .and_then(|next| session_change::compaction_item_change(current, &next).ok())
                == Some(session_change::CompactionItemChange::Added)
        })
}

/// Whether the prefix a pending change waits for was rebuilt by this run start: the session's
/// records say so since the change's acceptance (`recorded`: a view that changed the
/// conversation's head, a model switch or a flush), the change adds a compaction provider, or
/// the session's prompt cache has provably lapsed. The cache lapsed when no request reached it,
/// and no keep-warm request refreshed it, within the lifetime the latest of those asked for
/// (`last_cache`). A run on a lapsed cache writes its whole prompt again in any case, so a
/// change applied there causes no extra rewrite.
pub(crate) fn prefix_rebuilt(
    recorded: bool,
    adds_compaction: bool,
    last_cache: Option<&broca_session_store::fold::operations::CacheSend>,
    now_ms: u64,
) -> bool {
    recorded
        || adds_compaction
        || broca_core::compaction_v2::conditions::cache_proven_expired(
            last_cache.and_then(|send| send.requested_ttl_ms),
            last_cache.map(|send| send.sent_at_ms),
            now_ms,
        )
}

/// Whether a run on `model` at `variant` changes the effort of `latest`, the latest run's model
/// and variant, in a way that loses the cached prefix: same model, another variant, and the
/// catalog doesn't say an effort change keeps the cache (`keeps_cache`, asked only then). Such a
/// run rides `effort_change`, which rebuilds the prefix as a change of model does (llm-runner
/// §9.3.6), so it is a rebuild point for a pending change and for versioned tools.
pub(crate) fn effort_change_rebuilds(
    latest: Option<(&broca_protocol::ModelHandle, Option<&str>)>,
    model: &broca_protocol::ModelHandle,
    variant: Option<&str>,
    keeps_cache: impl FnOnce() -> bool,
) -> bool {
    use broca_fetch_plan::session_change::model_key;
    latest.is_some_and(|(last_model, last_variant)| {
        model_key(last_model) == model_key(model) && last_variant != variant
    }) && !keeps_cache()
}

/// Whether a run start on the model keyed `model` writes its session's prompt afresh, so a
/// versioned tool's newer same-major definition can be taken there at no extra cost
/// (llm-runner `tool_versions`): the run changes model from the previous run's
/// (`last_model`), a flush is waiting (`flushed`), a newer compaction view than the latest
/// step rendered waits to apply at its first step (`view_waiting`), or the prompt cache has
/// provably lapsed (`last_cache`, as in [`prefix_rebuilt`]). Unlike the rebuild a pending change
/// waits for, which counts from the change's acceptance, this is about this run start only.
pub(crate) fn run_rebuilds_prefix(
    last_model: Option<&str>,
    model: &str,
    flushed: bool,
    view_waiting: bool,
    last_cache: Option<&broca_session_store::fold::operations::CacheSend>,
    now_ms: u64,
) -> bool {
    last_model.is_some_and(|last| last != model)
        || flushed
        || view_waiting
        || broca_core::compaction_v2::conditions::cache_proven_expired(
            last_cache.and_then(|send| send.requested_ttl_ms),
            last_cache.map(|send| send.sent_at_ms),
            now_ms,
        )
}

/// Whether the compaction provider handed over a newer view than the latest step rendered,
/// which the next run's first step applies.
pub(crate) fn view_waiting(operations: &broca_session_store::fold::operations::Operations) -> bool {
    operations.view_accepted.as_ref().is_some_and(|accepted| {
        operations
            .view_applied
            .as_ref()
            .is_none_or(|applied| applied.view.version < accepted.version)
    })
}

/// The plan a new run of a session that already ran freezes (`StoreActor::run_plan`): its
/// manifest, the pending change it applies, and whether it starts at a prefix rebuild or
/// applies that change.
pub(crate) type RunPlan = (
    Option<Arc<PlanManifest>>,
    Option<broca_protocol::session_change::AppliedChange>,
    bool,
);

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

/// The manifest a new run freezes when a pending change doesn't apply at it yet: `current`, the
/// latest run's manifest, carrying the pending change's rich UI stamp when that stamp was
/// declared for exactly the system text `current` already holds (llm-runner 9.17, "same text,
/// newer stamp"). That holds when both plans have the same system-text providers and each one's
/// answered text is byte-identical. Only the stamp moves: the tools, the system text and the
/// rest of the change wait until the change itself applies (at its policy's moment, such as a
/// prefix rebuild), and the session's applied generation doesn't change.
/// The run's start records the manifest it froze, so the stamp survives a restart and later runs
/// keep it. Otherwise `current` is returned as it is.
pub(crate) fn with_newer_stamp(
    current: Option<Arc<PlanManifest>>,
    pending: Option<&PendingState>,
) -> Option<Arc<PlanManifest>> {
    use broca_records::record::FetchOutcome;
    let (Some(effective), Some(pending)) = (current.as_ref(), pending) else {
        return current;
    };
    let newer = &pending.manifest.rich_ui;
    if newer.is_none() || *newer == effective.rich_ui {
        return current;
    }
    /// Each system-text provider's answered text, by provider.
    fn texts(manifest: &PlanManifest) -> BTreeMap<&str, &str> {
        manifest
            .fetches
            .iter()
            .filter_map(|fetch| match &fetch.outcome {
                FetchOutcome::Answered {
                    text: Some(text), ..
                } => Some((fetch.provider.as_str(), text.as_str())),
                _ => None,
            })
            .collect()
    }
    let effective_texts = texts(effective);
    if effective_texts.is_empty() || effective_texts != texts(&pending.manifest) {
        return current;
    }
    let mut stamped = (**effective).clone();
    stamped.rich_ui = newer.clone();
    Some(Arc::new(stamped))
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
                refetch: Vec::new(),
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
            refresh_order(&facts, 2, "plan-2", "immediately", &[]),
            Ok(Some((true, true)))
        );
        assert_eq!(
            refresh_order(&facts, 3, "plan-3", "immediately", &[]),
            Ok(None)
        );
        assert_eq!(
            refresh_order(&facts, 2, "plan-other", "immediately", &[]),
            Err(SessionChangeError::GenerationStale { accepted: 2 })
        );
        let mut older = facts;
        older.accepted = None;
        assert_eq!(
            refresh_order(&older, 1, "plan-1", "immediately", &[]),
            Err(SessionChangeError::GenerationStale { accepted: 2 })
        );
    }

    /// The providers a refresh asked to read afresh are part of what it repeats: the same
    /// list, in the same order, is a repeat, and any other list under that generation is stale.
    #[test]
    fn a_refresh_repeats_only_with_the_same_refetch_list() {
        let mut facts = facts();
        facts.accepted.as_mut().unwrap().refetch = vec!["aft".into(), "plexus".into()];
        let list = |names: &[&str]| {
            names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            refresh_order(
                &facts,
                2,
                "plan-2",
                "immediately",
                &list(&["aft", "plexus"])
            ),
            Ok(Some((true, true)))
        );
        for other in [list(&[]), list(&["aft"]), list(&["plexus", "aft"])] {
            assert_eq!(
                refresh_order(&facts, 2, "plan-2", "immediately", &other),
                Err(SessionChangeError::GenerationStale { accepted: 2 }),
                "{other:?}"
            );
        }
        assert_eq!(
            refresh_order(&facts, 3, "plan-2", "immediately", &list(&["aft"])),
            Ok(None)
        );
    }

    #[test]
    fn a_new_refresh_applies_from_the_next_run_when_immediate_or_flushed() {
        let mut facts = facts();
        let next = manifest("plan-3", &["a"], "text");
        assert_eq!(
            accept_refresh(&facts, 3, "on_prefix_rebuild", &next, &[], false),
            RefreshAcceptance::Accept {
                run_in_progress: false,
                applies_from_next_run: false
            }
        );
        facts.flushed = true;
        assert_eq!(
            accept_refresh(&facts, 3, "on_prefix_rebuild", &next, &[], true),
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
                &[],
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
        compacting.plan = broca_records::record::OpaqueJson::from_json_text(
            r#"{"compaction_item":{"provider":"p"}}"#,
        )
        .unwrap();
        assert!(matches!(
            accept_refresh(&facts, 3, "immediately", &compacting, &[], false),
            RefreshAcceptance::Refused(SessionChangeError::InvalidParams { field, .. })
                if field == "plan.compaction_item"
        ));
        assert_eq!(
            accept_refresh(
                &facts,
                3,
                "on_system_rebuild",
                &manifest("plan-3", &["b"], "text"),
                &[],
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

    /// A manifest whose provider `ui` answered `text` as system text, stamped `stamp` (a digest
    /// character), and whose tools are `tools`.
    fn taught(identity: &str, tools: &[&str], text: &str, stamp: Option<char>) -> PlanManifest {
        let mut manifest = manifest(identity, tools, text);
        manifest.fetches = serde_json::from_value(serde_json::json!([
            {"provider": "tools", "catalog_digest": "t", "tools": []},
            {"provider": "ui", "catalog_digest": "u", "tools": [], "text": text},
        ]))
        .unwrap();
        manifest.rich_ui = stamp.map(|digit| {
            broca_protocol::RichUiStamp::new("1.0", digit.to_string().repeat(64)).unwrap()
        });
        manifest
    }

    fn pending_of(manifest: PlanManifest) -> PendingState {
        PendingState {
            generation: 4,
            policy: "on_prefix_rebuild".into(),
            manifest: Arc::new(manifest),
        }
    }

    /// A pending change whose system text is byte-identical to the latest run's lends the next
    /// run its newer stamp, and only that: the tools and every other member stay the latest
    /// run's, so nothing the model sees changes. A changed text, a provider added or gone, or
    /// an older or equal stamp leave the latest run's manifest as it is.
    #[test]
    fn a_newer_stamp_on_the_same_text_moves_and_nothing_else_does() {
        let current = Arc::new(taught("gen-3", &["a"], "teach", Some('b')));
        let newer = pending_of(taught("gen-4", &["a", "b"], "teach", Some('4')));
        let moved = with_newer_stamp(Some(current.clone()), Some(&newer)).unwrap();
        assert_eq!(moved.rich_ui, newer.manifest.rich_ui);
        let mut expected = (*current).clone();
        expected.rich_ui = newer.manifest.rich_ui.clone();
        assert_eq!(*moved, expected, "only the stamp moves");

        // A stamp first declared on the same text also moves onto an unstamped manifest.
        let unstamped = Arc::new(taught("gen-3", &["a"], "teach", None));
        let moved = with_newer_stamp(Some(unstamped), Some(&newer)).unwrap();
        assert_eq!(moved.rich_ui, newer.manifest.rich_ui);

        let kept = |pending: PendingState| {
            let kept = with_newer_stamp(Some(current.clone()), Some(&pending)).unwrap();
            assert!(
                Arc::ptr_eq(&kept, &current),
                "the latest run's manifest is kept"
            );
        };
        kept(pending_of(taught("gen-4", &["a"], "teach more", Some('4'))));
        kept(pending_of(taught("gen-4", &["a"], "teach", None)));
        kept(pending_of(taught("gen-4", &["a"], "teach", Some('b'))));
        let mut gone = taught("gen-4", &["a"], "teach", Some('4'));
        gone.fetches.retain(|fetch| fetch.provider != "ui");
        kept(pending_of(gone));
        let mut added = taught("gen-4", &["a"], "teach", Some('4'));
        added.fetches = serde_json::from_value(serde_json::json!([
            {"provider": "other", "catalog_digest": "o", "tools": [], "text": "more"},
            {"provider": "ui", "catalog_digest": "u", "tools": [], "text": "teach"},
        ]))
        .unwrap();
        kept(pending_of(added));
        // No pending change, or no latest manifest, changes nothing.
        assert!(Arc::ptr_eq(
            &with_newer_stamp(Some(current.clone()), None).unwrap(),
            &current
        ));
        assert_eq!(with_newer_stamp(None, Some(&newer)), None);
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

    /// A change of effort on the same model is a rebuild point only when the catalog doesn't say
    /// it keeps the cache, and the catalog is asked only for such a change.
    #[test]
    fn an_effort_change_that_loses_the_cache_is_a_rebuild_point() {
        let opus = broca_protocol::ModelHandle::new("anthropic", "claude-opus-5-5");
        let sonnet = broca_protocol::ModelHandle::new("anthropic", "claude-sonnet-5-5");
        let unasked = || -> bool { panic!("the catalog is asked only about an effort change") };
        assert!(effort_change_rebuilds(
            Some((&opus, Some("high"))),
            &opus,
            Some("low"),
            || false
        ));
        assert!(!effort_change_rebuilds(
            Some((&opus, Some("high"))),
            &opus,
            Some("low"),
            || true
        ));
        assert!(effort_change_rebuilds(
            Some((&opus, None)),
            &opus,
            Some("max"),
            || false
        ));
        assert!(!effort_change_rebuilds(
            Some((&opus, Some("high"))),
            &opus,
            Some("high"),
            unasked
        ));
        // A change of model is the model switch's rebuild, decided by its own rule.
        assert!(!effort_change_rebuilds(
            Some((&sonnet, Some("high"))),
            &opus,
            Some("low"),
            unasked
        ));
        assert!(!effort_change_rebuilds(None, &opus, Some("low"), unasked));
    }

    /// A refresh adding a compaction provider rebuilds the request's prefix, so it applies at
    /// the next run even under `on_prefix_rebuild`, with `applied_by: "fold"`. One that
    /// keeps the item, or a session already on the contract, waits for a real rebuild.
    #[test]
    fn adding_a_compaction_provider_applies_at_the_next_run_as_a_prefix_rebuild() {
        let item = serde_json::json!({"provider": "ck-mc", "preset": "head", "params": {}});
        let current = with_item("old", "text", None);
        let pending = |manifest: PlanManifest| PendingState {
            generation: 1,
            policy: "on_prefix_rebuild".into(),
            manifest: Arc::new(manifest),
        };
        let adding = pending(with_item("adding", "text", Some(item)));
        let keeping = pending(with_item("keeping", "other text", None));
        let model = broca_protocol::ModelHandle::new("stub", "m");
        // The same model as the session's last run: no model switch rebuilds the prefix.
        let key = session_change::model_key(&model);
        let applies = |pending: &PendingState, on_contract: bool| {
            let rebuild = adds_compaction(on_contract, Some(&current), Some(pending));
            pending_applying(Some(pending), rebuild, false, Some(&key), &model).and_then(|_| {
                session_change::applies_at_run_start(
                    &pending.policy,
                    rebuild,
                    false,
                    Some(&key),
                    &key,
                )
            })
        };
        assert_eq!(applies(&adding, false), Some("fold"));
        assert_eq!(applies(&keeping, false), None);
        assert_eq!(applies(&adding, true), None);
    }

    /// [`manifest`] whose plan names `item` as its compaction item, or none.
    fn with_item(
        identity: &str,
        system_text: &str,
        item: Option<serde_json::Value>,
    ) -> PlanManifest {
        let mut manifest = manifest(identity, &[], system_text);
        let mut plan: serde_json::Value =
            serde_json::from_str(manifest.plan.as_json_text()).unwrap();
        plan["compaction_item"] = item.unwrap_or(serde_json::Value::Null);
        manifest.plan =
            broca_records::record::OpaqueJson::from_json_text(plan.to_string()).unwrap();
        manifest
    }

    /// Only a session in the session store takes a refresh that adds a compaction provider, and
    /// only while it can enroll. Every other change to the item is refused against the latest
    /// accepted plan: the pending one once there is one.
    #[test]
    fn only_the_new_storage_adds_a_compaction_provider_and_only_when_it_can_enroll() {
        use broca_session_loop::enroll::Refusal;
        let item = serde_json::json!({"provider": "ck-mc", "preset": "head", "params": {}});
        let fixed = SessionChangeError::InvalidParams {
            field: "plan.compaction_item".into(),
            message: "the session's compaction provider, preset and params are fixed for its life"
                .into(),
        };
        let not_addable =
            |refusal| RefreshAcceptance::Refused(SessionChangeError::CompactionNotAddable(refusal));
        let accepted =
            |acceptance: RefreshAcceptance| matches!(acceptance, RefreshAcceptance::Accept { .. });
        let adding = with_item("adding", "text", Some(item.clone()));
        let today = ChangeFacts {
            current_manifest: Some(Arc::new(with_item("old", "text", None))),
            ..ChangeFacts::default()
        };
        assert_eq!(
            accept_refresh(&today, 1, "immediately", &adding, &[], false),
            RefreshAcceptance::Refused(fixed.clone())
        );
        let store = ChangeFacts {
            enrollment: Some(EnrollmentFacts {
                window_whole: true,
                carries_global_system: true,
                session_model: None,
            }),
            ..today.clone()
        };
        assert!(accepted(accept_refresh(
            &store,
            1,
            "immediately",
            &adding,
            &[],
            false
        )));
        let mut cut = store.clone();
        cut.enrollment.as_mut().unwrap().window_whole = false;
        assert_eq!(
            accept_refresh(&cut, 1, "immediately", &adding, &[], false),
            not_addable(Refusal::WindowNotWhole)
        );
        let silent = with_item("silent", "", Some(item.clone()));
        assert_eq!(
            accept_refresh(&store, 1, "immediately", &silent, &[], false),
            not_addable(Refusal::NoSystemText)
        );
        let mut quiet = store.clone();
        quiet.enrollment.as_mut().unwrap().carries_global_system = false;
        assert!(accepted(accept_refresh(
            &quiet,
            1,
            "immediately",
            &silent,
            &[],
            false
        )));
        let pending = ChangeFacts {
            accepted_generation: 1,
            pending: Some(PendingState {
                generation: 1,
                policy: "immediately".into(),
                manifest: Arc::new(adding.clone()),
            }),
            ..store.clone()
        };
        let again = with_item("again", "text", Some(item.clone()));
        assert!(accepted(accept_refresh(
            &pending,
            2,
            "immediately",
            &again,
            &[],
            false
        )));
        let mut other = item.clone();
        other["preset"] = serde_json::json!("worker");
        for next in [
            with_item("removed", "text", None),
            with_item("changed", "text", Some(other)),
        ] {
            assert_eq!(
                accept_refresh(&pending, 2, "immediately", &next, &[], false),
                RefreshAcceptance::Refused(fixed.clone())
            );
            let today_pending = ChangeFacts {
                enrollment: None,
                ..pending.clone()
            };
            assert_eq!(
                accept_refresh(&today_pending, 2, "immediately", &next, &[], false),
                RefreshAcceptance::Refused(fixed.clone())
            );
        }
    }

    /// The three reasons a session that could gain a compaction provider refuses one, as the
    /// wire carries them: `invalid_params` on `plan.compaction_item`, with a stable reason.
    #[test]
    fn the_refusals_of_an_added_compaction_provider_name_their_reason() {
        use broca_session_loop::enroll::Refusal;
        for (refusal, detail, message) in [
            (
                Refusal::NoContextLimit,
                r#"{"field":"plan.compaction_item","reason":"no_context_limit"}"#,
                "a compaction provider can't be added: no context limit resolves for the plan's model",
            ),
            (
                Refusal::NoSystemText,
                r#"{"field":"plan.compaction_item","reason":"no_system_text"}"#,
                "a compaction provider can't be added by a plan without system text: the session's earlier runs carried a system prompt",
            ),
            (
                Refusal::WindowNotWhole,
                r#"{"field":"plan.compaction_item","reason":"window_not_whole"}"#,
                "a compaction provider can't be added: the session's history was cut or sealed, or a window change is pending",
            ),
        ] {
            let error = SessionChangeError::CompactionNotAddable(refusal);
            assert_eq!(error.code(), "invalid_params");
            assert!(!error.retryable());
            assert_eq!(
                serde_json::to_string(&crate::serve::compaction_not_addable_detail(refusal))
                    .unwrap(),
                detail
            );
            assert_eq!(refusal.message(), message);
        }
    }

    /// A resolved limit accepts the add; a resolution that gives none refuses it
    /// `no_context_limit`; one that couldn't run refuses it transiently with its cause, never as
    /// `no_context_limit`. Nothing a send names enters: the outcome depends on resolution alone.
    #[test]
    fn an_added_compaction_provider_needs_a_resolved_context_limit() {
        use crate::serve::context_limit_outcome;
        let model = cortexkit_role_llm_runner::head_selection::PlanModel::new("anthropic", "m");
        assert_eq!(context_limit_outcome(Ok(Some(200_000)), &model), Ok(()));
        assert_eq!(
            context_limit_outcome(Ok(None), &model),
            Err(SessionChangeError::CompactionNotAddable(
                broca_session_loop::enroll::Refusal::NoContextLimit
            ))
        );
        let unresolved = context_limit_outcome(Err("no catalog".into()), &model).unwrap_err();
        assert_eq!(
            unresolved,
            SessionChangeError::Transient(
                "the context limit of anthropic/m couldn't be resolved: no catalog".into()
            )
        );
        assert!(unresolved.retryable());
    }

    /// A run start counts as the prefix rebuild a pending change waits for when the records
    /// say one happened, when the change adds a compaction provider, or when the session's
    /// prompt cache has provably lapsed: no request reached it within the lifetime it asked
    /// for. A cache with no lifetime, or none sent yet, proves nothing.
    /// A run start rebuilds its prefix when it switches model, a flush or a newer view waits,
    /// or the cache lapsed; a plain next run on the same model rebuilds nothing.
    #[test]
    fn a_run_start_rebuilds_its_prefix_by_what_changed_since_the_last_run() {
        use broca_session_store::fold::operations::CacheSend;
        let sent = CacheSend {
            sent_at_ms: 1_000,
            run_id: "r1".into(),
            requested_ttl_ms: Some(3_600_000),
        };
        let fresh = Some(&sent);
        let rebuilds = |last: Option<&str>, flushed, view, now| {
            run_rebuilds_prefix(last, "stub:m", flushed, view, fresh, now)
        };
        assert!(!rebuilds(Some("stub:m"), false, false, 2_000));
        assert!(
            !rebuilds(None, false, false, 2_000),
            "a first run has no prefix to rebuild"
        );
        assert!(rebuilds(Some("stub:other"), false, false, 2_000));
        assert!(rebuilds(Some("stub:m"), true, false, 2_000));
        assert!(rebuilds(Some("stub:m"), false, true, 2_000));
        assert!(rebuilds(Some("stub:m"), false, false, 3_601_000));
    }

    #[test]
    fn a_lapsed_prompt_cache_counts_as_a_prefix_rebuild() {
        use broca_session_store::fold::operations::CacheSend;
        let sent = |requested_ttl_ms| CacheSend {
            sent_at_ms: 1_000,
            run_id: "r1".into(),
            requested_ttl_ms,
        };
        let hour = Some(3_600_000);
        assert!(!prefix_rebuilt(false, false, Some(&sent(hour)), 3_600_999));
        assert!(prefix_rebuilt(false, false, Some(&sent(hour)), 3_601_000));
        assert!(!prefix_rebuilt(false, false, Some(&sent(None)), u64::MAX));
        assert!(!prefix_rebuilt(false, false, None, u64::MAX));
        assert!(prefix_rebuilt(true, false, Some(&sent(hour)), 2_000));
        assert!(prefix_rebuilt(false, true, None, 2_000));
    }
}
