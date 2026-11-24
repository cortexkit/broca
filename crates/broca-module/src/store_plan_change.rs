//! The plan-change facts of a session kept in the session store, which the plan-change rules
//! (`crate::plan_change`) decide from. The operational fold holds the session's change counters,
//! its pending change and the manifests its latest run and latest refresh froze
//! (`Operations::plan`). What a request's own generation or `send_id` recorded is looked up in
//! the session's id index and read from that one record, so a lookup costs the same however
//! many refreshes the session took.

use std::sync::Arc;

use broca_fetch_plan::session_change::PendingState;
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::fold::operations::ManifestRef;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::Record;
use broca_session_store::session::Session;

use crate::plan_change::{Accepted, ChangeFacts, EnrollmentFacts};

/// The facts the plan-change rules read about `session`, for a request naming `generation` and
/// `send_id`, with its manifests read back from `parts`.
pub(crate) async fn facts<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    generation: Option<u64>,
    send_id: Option<&str>,
) -> Result<ChangeFacts, String> {
    let default = broca_session_store::fold::operations::Plan::default();
    let plan = session.operations.plan.as_ref().unwrap_or(&default);
    let pending = plan
        .pending
        .as_ref()
        .map(|pending| {
            Ok::<_, String>(PendingState {
                generation: pending.generation,
                policy: pending.policy.clone(),
                manifest: stored(&pending.manifest, parts)?,
            })
        })
        .transpose()?;
    let current_manifest = plan
        .current
        .as_ref()
        .map(|current| stored(current, parts))
        .transpose()?;
    let mut facts = ChangeFacts {
        accepted_generation: plan.accepted_generation,
        pending,
        current_manifest,
        flushed: plan.flushed_since_accept,
        enrollment: Some(enrollment_facts(session, parts)?),
        ..ChangeFacts::default()
    };
    if let Some(generation) = generation {
        let id = generation.to_string();
        facts.accepted = latest(session, IdKind::RefreshGeneration, &id)
            .await?
            .and_then(|record| match record {
                Record::SessionRefresh(refresh) => Some(Accepted {
                    plan_identity: ManifestRef::of_refresh(&refresh)
                        .plan_identity()
                        .unwrap_or_default()
                        .to_owned(),
                    policy: refresh.policy,
                    refetch: refresh.refetch,
                    run_in_progress: refresh.run_in_progress,
                    applies_from_next_run: refresh.applies_from_next_run,
                }),
                _ => None,
            });
        facts.applied_by = latest(session, IdKind::AppliedGeneration, &id)
            .await?
            .and_then(|record| match record {
                Record::RefreshApplied(applied) => Some(applied.applied_by),
                _ => None,
            });
    }
    if let Some(send_id) = send_id {
        facts.policy_send = latest(session, IdKind::RefreshPolicySendId, send_id)
            .await?
            .and_then(|record| match record {
                Record::RefreshPolicy(policy) => Some((policy.generation, policy.policy)),
                _ => None,
            });
        facts.flush_send = latest(session, IdKind::PrefixFlushSendId, send_id)
            .await?
            .and_then(|record| match record {
                Record::PrefixFlush(flush) => Some(flush.pending_generation),
                _ => None,
            });
    }
    Ok(facts)
}

/// What a refresh that adds a compaction provider is checked against (see
/// `broca_session_loop::enroll`): the window, the global system messages the session's runs
/// carried, and the model its next run takes when the refresh chooses none.
fn enrollment_facts<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
) -> Result<EnrollmentFacts, String> {
    let latest_model = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .map(|start| {
            let admission = broca_session_loop::run::admission_of_start(start, parts)
                .map_err(|error| format!("the session's latest run start: {error:?}"))?;
            let model = cortexkit_role_llm_runner::head_selection::PlanModel::new(
                start.model.provider_module_id.as_str(),
                start.model.model_id.as_str(),
            );
            Ok::<_, String>(match admission.variant {
                Some(variant) => model.with_variant(variant),
                None => model,
            })
        })
        .transpose()?;
    Ok(EnrollmentFacts {
        window_whole: broca_session_loop::enroll::window_whole(&session.conversation),
        carries_global_system: broca_session_loop::enroll::carries_global_system(
            &session.conversation,
        ),
        session_model: broca_session_loop::plan::selection(&session.operations, parts)?
            .or(latest_model),
    })
}

/// What a plan-change op's preflight reads about `session`, as `ChangeRead::of_wal` reads it
/// from the session's log: the scope and principal its first run froze, the module that opened
/// its latest scoped run (`crate::serve::scope_plan_opener`), which a refreshed plan's senders
/// are checked against, and its plan-change facts.
pub(crate) async fn change_read<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    generation: Option<u64>,
    send_id: Option<&str>,
) -> Result<crate::serve::ChangeRead, String> {
    let owner = &session.operations.owner;
    let first_run = owner
        .first_start
        .as_ref()
        .map(|first| {
            Ok::<_, String>((
                decode(first.scope.as_ref(), "the first run's scope")?,
                decode(first.principal.as_ref(), "the first run's principal")?,
            ))
        })
        .transpose()?;
    // The owner fold keeps the scope of the latest run that recorded one; preflight reads the
    // opener from that scope.
    let opener =
        decode::<broca_protocol::SessionScope>(owner.scope.as_ref(), "the session's scope")?
            .as_ref()
            .and_then(crate::serve::scope_plan_opener);
    Ok(crate::serve::ChangeRead {
        first_run,
        opener,
        facts: facts(session, parts, generation, send_id).await?,
    })
}

/// The plan identities a later send's plan is checked against once the session took a refresh,
/// as the serve layer reads them from the session's log: the applied plan's (the first run's
/// before any applied) and the latest accepted one's. `None` before any refresh.
pub(crate) async fn identities<B: SessionBackend>(
    session: &Session<B>,
) -> Result<Option<(Option<String>, Option<String>)>, String> {
    let Some(plan) = session
        .operations
        .plan
        .as_ref()
        .filter(|plan| plan.accepted_generation > 0)
    else {
        return Ok(None);
    };
    let identity_of = |generation: u64| async move {
        Ok::<_, String>(
            latest(session, IdKind::RefreshGeneration, &generation.to_string())
                .await?
                .and_then(|record| match record {
                    Record::SessionRefresh(refresh) => ManifestRef::of_refresh(&refresh)
                        .plan_identity()
                        .map(str::to_owned),
                    _ => None,
                }),
        )
    };
    let accepted = identity_of(plan.accepted_generation).await?;
    let effective = if plan.applied_generation > 0 {
        identity_of(plan.applied_generation).await?
    } else {
        session
            .operations
            .owner
            .first_start
            .as_ref()
            .and_then(|first| {
                ManifestRef::of_start(first)?
                    .plan_identity()
                    .map(str::to_owned)
            })
    };
    Ok(Some((effective, accepted)))
}

/// What `session.baseline` reads about a session kept in the session store, before the serve layer
/// checks the caller is the owner and adds the cache declaration (see `crate::serve`'s
/// `baseline`, which reads the same facts from the session's log).
#[derive(Debug)]
pub struct BaselineFacts {
    /// The scope and principal the session's first run froze; only its owner reads the baseline.
    pub first_run: (
        Option<broca_protocol::SessionScope>,
        Option<broca_protocol::BindPrincipal>,
    ),
    /// The first run's frozen config, whose prompt-cache declaration the reply carries.
    pub first_config: broca_records::record::RunConfig,
    /// The reply's baseline, without its cache declaration.
    pub baseline: broca_wire::BaselineWire,
}

/// The baseline facts of `session`, read from its owner and plan folds and the parts its first
/// run named: `None` before the session's first run. It uses the first run's manifest (or its
/// tools, without one), replaced by the latest prefix rebuild's manifest once there is one,
/// together with the change counters, the pending and effective changes and the rungs this
/// runner supports.
pub(crate) fn baseline<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
) -> Result<Option<BaselineFacts>, String> {
    let Some(first) = session.operations.owner.first_start.as_ref() else {
        return Ok(None);
    };
    let first_run = (
        decode(first.scope.as_ref(), "the first run's scope")?,
        decode(first.principal.as_ref(), "the first run's principal")?,
    );
    let admission = broca_session_loop::run::admission_of_start(first, parts)
        .map_err(|error| format!("the session's first run's start: {error:?}"))?;
    let first_config = broca_session_loop::run::run_config_of_start(first, parts)
        .map_err(|error| format!("the session's first run's config: {error:?}"))?;
    let default = broca_session_store::fold::operations::Plan::default();
    let plan = session.operations.plan.as_ref().unwrap_or(&default);
    let mut baseline = match &plan.prefix {
        Some(prefix) => {
            let prefix = stored(prefix, parts)?;
            crate::serve::planned_baseline(&prefix)
        }
        None => match admission.manifest.as_deref() {
            Some(manifest) => crate::serve::planned_baseline(manifest),
            None => broca_wire::BaselineWire::plan_less(
                admission
                    .tools
                    .iter()
                    .map(|tool| tool.name.clone())
                    .collect(),
            ),
        },
    };
    baseline.accepted_generation = plan.accepted_generation;
    baseline.applied_generation = plan.applied_generation;
    baseline.pending =
        plan.pending.as_ref().map(
            |pending| cortexkit_role_llm_runner::baseline::PendingChange {
                generation: pending.generation,
                policy: pending.policy.clone(),
            },
        );
    baseline.effective =
        plan.effective
            .as_ref()
            .map(|effective| broca_wire::EffectiveBaselineWire {
                generation: effective.generation,
                applied_by: effective.applied_by.clone(),
                manifest_digest: effective.manifest_digest.clone(),
                composition_digest: effective.composition_digest.clone(),
                absent_items: effective.absent_items.clone(),
            });
    baseline.rungs = Some(crate::serve::supported_rungs());
    Ok(Some(BaselineFacts {
        first_run,
        first_config,
        baseline,
    }))
}

/// The newest record the session's id index names under `kind` and `id`.
async fn latest<B: SessionBackend>(
    session: &Session<B>,
    kind: IdKind,
    id: &str,
) -> Result<Option<Record>, String> {
    let records = session
        .find_id(kind, id)
        .await
        .map_err(|error| format!("the session's id index: {error:?}"))?;
    Ok(records.into_iter().map(|(_, record)| record).next_back())
}

fn stored(
    manifest: &ManifestRef,
    parts: &dyn Parts,
) -> Result<Arc<broca_records::record::PlanManifest>, String> {
    broca_session_loop::run::stored_manifest(manifest, parts)
        .map_err(|error| format!("a plan manifest the session froze: {error:?}"))
}

fn decode<T: serde::de::DeserializeOwned>(
    value: Option<&serde_json::Value>,
    what: &str,
) -> Result<Option<T>, String> {
    value
        .map(|value| serde_json::from_value(value.clone()))
        .transpose()
        .map_err(|error| format!("{what}: {error}"))
}
