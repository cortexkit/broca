//! One cutoff per run, from its own recorded attempt after sending, otherwise from the
//! durable boundaries since the last request by the current Anthropic renderer.
use super::*;
use crate::parts::PartReadError;

mod evidence;
use evidence::{retry, FloorEvidence};

pub(super) const UNVERIFIABLE: &str = "the sent Anthropic run's signed cutoff cannot be verified";

#[derive(Default)]
pub struct ToolSetCache {
    by_run: std::collections::HashMap<String, Option<String>>,
    /// Lifetime of the live run. A restart reads the attempt, not this cache.
    current: Option<Cut>,
    previous: Option<Box<Prior>>,
}

/// The previous run's durable config, before admission replaces it in the session state.
pub(super) struct Prior {
    start: RunStart,
    refrozen: Option<Value>,
}

impl Prior {
    pub(super) fn of<W: Writer>(writer: &W) -> Option<Box<Self>> {
        let run = writer.operations().run.as_ref()?;
        Some(Box::new(Self {
            start: run.start.clone()?,
            refrozen: run.refrozen_render.clone(),
        }))
    }

    fn config(&self, parts: &dyn Parts) -> Result<Value, FloorEvidence> {
        match self.refrozen.as_ref() {
            Some(refrozen) => refrozen
                .get("render_config")
                .filter(|_| refrozen["renderer_version"].as_u64() == Some(4))
                .cloned()
                .ok_or(FloorEvidence::Damage),
            None => config_of_start(parts, &self.start),
        }
    }
}

#[derive(Debug)]
struct Cut {
    ordinal: Option<u64>,
    through: Option<String>,
}

impl ToolSetCache {
    pub(super) fn with_previous(previous: Option<Box<Prior>>) -> Self {
        Self {
            previous,
            ..Self::default()
        }
    }
    pub(super) fn through(&self) -> Option<&Option<String>> {
        self.current.as_ref().map(|cut| &cut.through)
    }
}

pub(super) async fn unavailable<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    detail: &str,
) -> Result<Finished, LoopError> {
    let mut error = crate::compaction::unavailable_error(detail);
    error.class = broca_protocol::ErrorClass::Transient;
    error.provider_code = Some("signed_floor_unavailable".into());
    end_with_error(
        writer,
        seams,
        &admission.run_id,
        RunFinishReason::Error,
        &error,
    )
    .await
}

pub(super) fn unverifiable() -> ProviderError {
    let mut error = crate::compaction::unavailable_error(UNVERIFIABLE);
    error.provider_code = Some("signed_history_unverifiable".into());
    error
}

pub async fn signed_floor<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    floors: &mut ToolSetCache,
) -> Result<Option<u64>, LoopError> {
    match render_history(writer, parts, floors).await {
        Err(LoopError::Unsupported(UNVERIFIABLE))
            if writer
                .operations()
                .run
                .as_ref()
                .is_some_and(|run| run.finished.is_some()) =>
        {
            Ok(None)
        }
        result => Ok(result?.signed_floor),
    }
}

pub(super) async fn render_history<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    floors: &mut ToolSetCache,
) -> Result<RenderHistory, LoopError> {
    let current = writer.operations().run.as_ref();
    let from = current.and_then(|run| run.start.as_ref());
    let refrozen = current.and_then(|run| run.refrozen_render.as_ref());
    let mut history = RenderHistory::from(
        if refrozen.map_or_else(
            || {
                from.is_some_and(|start| {
                    start.renderer_version >= 4
                        && start.model.provider_module_id.starts_with("anthropic")
                })
            },
            |refrozen| new_abi(&refrozen["render_config"]),
        ) {
            render_episodes_checked(writer, parts)?
        } else {
            render_episodes(writer, parts)?
        },
    );
    let Some(from) = from else {
        history.signed_floor = old_tool_floor(writer, parts, floors).await?;
        return Ok(history);
    };
    let config = refrozen
        .and_then(|refrozen| refrozen.get("render_config"))
        .or_else(|| {
            history
                .episodes
                .last()
                .map(|episode| &episode.frozen_config)
        })
        .unwrap_or(&Value::Null);
    if !new_abi(config) {
        history.signed_floor = old_tool_floor(writer, parts, floors).await?;
        return Ok(history);
    }
    if floors.current.is_none() {
        let records = if writer.replay_attempt().is_none() {
            match retry(|| async { writer.cut_records().await.map_err(FloorEvidence::session) })
                .await
            {
                Err(error @ LoopError::FloorRead(_)) => {
                    if current.is_some_and(|run| run.provider_call_started)
                        && run_has_attempt(writer, &from.run_id).await? != Some(false)
                    {
                        return Err(error);
                    }
                    // With no attempt to replay, drop earlier signed thinking rather than
                    // depend on the unreadable ledger to decide which blocks can still echo.
                    Some(Vec::new())
                }
                result => result?,
            }
        } else {
            None
        };
        let attempt = writer.replay_attempt().or_else(|| {
            match records.as_ref().and_then(|records| records.first()) {
                Some(Record::Attempt(attempt)) if attempt.run_id == from.run_id => Some(attempt),
                _ => None,
            }
        });
        let cut = if let Some(attempt) = attempt {
            retry(|| resolve_cut(writer, attempt))
                .await?
                .ok_or(LoopError::Unsupported(UNVERIFIABLE))?
        } else {
            if records.is_none() && current.is_some_and(|run| run.provider_call_started) {
                // A damaged earlier ledger segment may leave this run's own records intact.
                if run_has_attempt(writer, &from.run_id).await? != Some(false) {
                    return Err(LoopError::Unsupported(UNVERIFIABLE));
                }
            }
            fresh_cut(
                writer,
                parts,
                from,
                config,
                records.as_deref().unwrap_or_default(),
                floors.previous.as_deref(),
                &history,
            )
            .await?
        };
        floors.current = Some(cut);
    }
    history.signed_floor = floors.current.as_ref().and_then(|cut| cut.ordinal);
    Ok(history)
}

async fn run_has_attempt<W: Writer>(writer: &W, run_id: &str) -> Result<Option<bool>, LoopError> {
    retry(|| async {
        writer
            .run_has_attempt(run_id)
            .await
            .map_err(FloorEvidence::session)
    })
    .await
}

pub(super) async fn preview_floor<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    target: &ModelHandle,
    config: &Value,
) -> Result<Option<u64>, LoopError> {
    // The selected next route is only prospective here. Use the same ledger baseline and
    // place a hypothetical change at the newest entry, never inside the future run's input.
    let start = writer.conversation().placed;
    let prior = Prior::of(writer);
    let history = RenderHistory::from(render_episodes_checked(writer, parts)?);
    let cut = retry(|| async {
        let records = writer
            .cut_records()
            .await
            .map_err(|_| FloorEvidence::Damage)?;
        let old = prior.as_deref().ok_or(FloorEvidence::Damage)?;
        if route_changed(
            &old.start.model,
            &old.config(parts).map_err(|_| FloorEvidence::Damage)?,
            target,
            config,
        ) || history
            .episodes
            .last()
            .is_some_and(|episode| !keeps_effort_prefix(&episode.frozen_config, config))
        {
            return Ok(start);
        }
        derive(writer, parts, None, &records, Some(old), start, &history).await
    })
    .await?
    .unwrap_or(start);
    Ok((cut > 0).then_some(cut))
}

fn starts(
    conversation: &broca_session_store::fold::conversation::Conversation,
) -> impl DoubleEndedIterator<Item = &broca_session_store::fold::conversation::Episode> {
    conversation
        .sealed_episodes
        .iter()
        .chain(&conversation.episodes)
}

fn run_start_ordinal<W: Writer>(writer: &W, run: &str) -> Option<u64> {
    starts(writer.conversation())
        .rev()
        .find(|episode| episode.run_id == run)
        .map(|episode| episode.start - 1)
}

async fn old_tool_floor<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    cache: &mut ToolSetCache,
) -> Result<Option<u64>, LoopError> {
    let mut floor = broca_core::signed_floor::SignedFloor::default();
    for episode in starts(writer.conversation()) {
        let digest = if let Some(digest) = cache.by_run.get(&episode.run_id) {
            digest.clone()
        } else {
            let start = writer.run_start(&episode.run_id).await?.ok_or_else(|| {
                LoopError::Parts(format!(
                    "the conversation names run {} but the session holds no start for it",
                    episode.run_id
                ))
            })?;
            let digest = start_config::tool_set_of_start(&start, parts)?;
            cache.by_run.insert(episode.run_id.clone(), digest.clone());
            digest
        };
        if let Some(digest) = digest {
            floor.observe_run(episode.start - 1, digest);
        }
    }
    Ok(floor.floor())
}

async fn fresh_cut<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    run: &RunStart,
    config: &Value,
    records: &[Record],
    prior: Option<&Prior>,
    history: &RenderHistory,
) -> Result<Cut, LoopError> {
    let start = run_start_ordinal(writer, &run.run_id).unwrap_or(writer.conversation().placed);
    let candidate = retry(|| {
        derive(
            writer,
            parts,
            Some((run, config)),
            records,
            prior,
            start,
            history,
        )
    })
    .await?;
    if let Some(ordinal) = candidate.filter(|&ordinal| ordinal < start) {
        if let Some(cut) = retry(|| name_cut(writer, ordinal)).await? {
            return Ok(cut);
        }
    }
    if start == 0 {
        return Ok(Cut {
            ordinal: None,
            through: None,
        });
    }
    let conversation = writer.conversation();
    let first = conversation
        .entries
        .get((start + 1).saturating_sub(conversation.window_start()) as usize)
        .and_then(|entry| entry.facts.as_ref())
        .filter(|facts| facts.ordinal == start + 1)
        .ok_or_else(|| {
            LoopError::FloorRead("the run's first input has no stable entry id".into())
        })?;
    // A run's input has no signed reasoning. Cutting through it removes exactly the same
    // reasoning as cutting through the preceding entry, without looking that entry up.
    Ok(Cut {
        ordinal: Some(first.ordinal),
        through: Some(first.mid.clone()),
    })
}

async fn name_cut<W: Writer>(writer: &W, ordinal: u64) -> Result<Cut, FloorEvidence> {
    let through = if ordinal == 0 {
        None
    } else {
        Some(
            writer
                .entry_mid_at(ordinal)
                .await
                .map_err(FloorEvidence::session)?
                .ok_or(FloorEvidence::Damage)?,
        )
    };
    Ok(Cut {
        ordinal: (ordinal > 0).then_some(ordinal),
        through,
    })
}

async fn resolve_cut<W: Writer>(
    writer: &W,
    attempt: &broca_session_store::record::Attempt,
) -> Result<Cut, FloorEvidence> {
    let through = attempt.signed_floor.clone();
    let ordinal = match through.as_deref() {
        Some(mid) => Some(
            writer
                .cut_ordinal(mid)
                .await
                .map_err(FloorEvidence::session)?
                .ok_or(FloorEvidence::Damage)?,
        ),
        None => None,
    };
    Ok(Cut { ordinal, through })
}

async fn derive<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
    current: Option<(&RunStart, &Value)>,
    records: &[Record],
    prior: Option<&Prior>,
    start: u64,
    history: &RenderHistory,
) -> Result<u64, FloorEvidence> {
    let Some((Record::Attempt(attempt), later)) = records.split_first() else {
        return Ok(start);
    };
    // A pre-send refreeze can change top-level thinking or remove per-message effort updates,
    // even though the original episode settings are unchanged. Keep the first attempt's cut
    // for every continuation of this run.
    if current.is_some_and(|(run, _)| {
        later.iter().any(|record| matches!(record, Record::ConfigRefrozen(change) if change.run_id == run.run_id))
    }) {
        return Ok(start);
    }
    let fetched;
    // A pre-send refreeze leaves its own tool loop intact. Once a later run starts, cut
    // through the whole refrozen turn rather than relying on its mixed-config signatures.
    // A refreeze in the prior state can also be a persona-only move after the first send;
    // look for ConfigRefrozen before the first StepStart to tell them apart.
    let anchor = if let Some(old) =
        prior.filter(|old| old.start.run_id == attempt.run_id && old.refrozen.is_none())
    {
        old
    } else {
        let Ok(Some(evidence)) = writer.signing_config(&attempt.run_id).await else {
            return Ok(start);
        };
        if evidence.refrozen.is_some() {
            return Ok(start);
        }
        fetched = Prior {
            start: *evidence.start,
            refrozen: None,
        };
        &fetched
    };
    let mut owner = &anchor.start;
    let mut prior_system = system_digest(owner);
    let mut before = anchor.config(parts).map_err(|_| FloorEvidence::Damage)?;
    if !new_abi(&before) {
        return Ok(start);
    }
    let reverted = later
        .iter()
        .filter_map(|record| match record {
            Record::TranscriptReverted(reverted) => Some(reverted.ordinal.saturating_sub(1)),
            _ => None,
        })
        .min();
    let mut cut = match resolve_cut(writer, attempt).await {
        Ok(cut) => cut.ordinal.unwrap_or(0),
        Err(FloorEvidence::Damage) if reverted.is_some() => reverted.unwrap_or(0),
        Err(error) => return Err(error),
    };
    let mut digest = tools_of_start(parts, owner, &before).map_err(|_| FloorEvidence::Damage)?;
    for record in later {
        match record {
            Record::RunStart(next) => {
                let config = config_of_start(parts, next).map_err(|_| FloorEvidence::Damage)?;
                if !new_abi(&config)
                    && !current.is_some_and(|(run, effective)| {
                        run.run_id == next.run_id && new_abi(effective)
                    })
                {
                    return Ok(start);
                }
                let next_digest =
                    tools_of_start(parts, next, &config).map_err(|_| FloorEvidence::Damage)?;
                let changes_route = digest != next_digest
                    || route_changed(&owner.model, &before, &next.model, &config);
                match run_start_ordinal(writer, &next.run_id) {
                    Some(at) if changes_route || changes_effort_at(history, at) => {
                        cut = cut.max(at)
                    }
                    None if changes_route => return Err(FloorEvidence::Damage),
                    _ => {}
                }
                prior_system = system_digest(owner);
                owner = next;
                before = config;
                digest = next_digest;
            }
            Record::ConfigRefrozen(refrozen) if refrozen.run_id == owner.run_id => {
                if refrozen.renderer_version != 4 || !new_abi(&refrozen.render_config) {
                    return Ok(start);
                }
                let next_digest = tools_of_start(parts, owner, &refrozen.render_config)
                    .map_err(|_| FloorEvidence::Damage)?;
                if owner.run_id != anchor.start.run_id
                    || digest != next_digest
                    || route_changed(&owner.model, &before, &owner.model, &refrozen.render_config)
                {
                    cut = cut.max(
                        run_start_ordinal(writer, &owner.run_id).ok_or(FloorEvidence::Damage)?,
                    );
                }
                before = refrozen.render_config.clone();
                digest = next_digest;
            }
            Record::RefreshApplied(applied) => {
                // Tool declarations were already compared as canonical sets at RunStart.
                // The apply matters separately only if it changes system text or erases notes.
                let boundary = run_start_ordinal(writer, &owner.run_id).unwrap_or(start);
                if cut < boundary
                    && (prior_system != system_digest(owner)
                        || !matches!(
                            writer.apply_may_erase_notes(applied.generation).await,
                            Ok(false)
                        ))
                {
                    cut = boundary;
                }
            }
            Record::TranscriptReverted(reverted) => {
                cut = cut.min(reverted.ordinal.saturating_sub(1))
            }
            _ => {}
        }
    }
    if let Some((run, config)) = current {
        if owner.run_id != run.run_id
            || route_changed(&owner.model, &before, &run.model, config)
            || digest != tools_of_start(parts, run, config).map_err(|_| FloorEvidence::Damage)?
        {
            return Ok(start);
        }
    }
    Ok(cut)
}

fn system_digest(start: &RunStart) -> Option<&broca_session_store::codec::Digest> {
    start
        .setup
        .iter()
        .find(|part| part.kind == PartKind::SystemText)
        .map(|part| &part.digest)
}

fn route_changed(
    old_model: &ModelHandle,
    old: &Value,
    model: &ModelHandle,
    config: &Value,
) -> bool {
    old_model != model
        || old["wire_model_id"] != config["wire_model_id"]
        || old["family_id"] != config["family_id"]
        || old["reasoning"]["mode"] != config["reasoning"]["mode"]
        || old["reasoning"]["last_turn_only"] != config["reasoning"]["last_turn_only"]
        || old["reasoning"]["cross_model"] != config["reasoning"]["cross_model"]
        || old["reasoning"]["echo_scope"] != config["reasoning"]["echo_scope"]
}

fn changes_effort_at(history: &RenderHistory, ordinal: u64) -> bool {
    // The renderer plans historical effort using these same RunStart episode configs. A
    // ConfigRefrozen changes the actual route and tool shape, not those historical settings.
    history
        .episodes
        .windows(2)
        .find(|pair| pair[1].prompt_start as u64 == ordinal)
        .is_some_and(|pair| !keeps_effort_prefix(&pair[0].frozen_config, &pair[1].frozen_config))
}

fn keeps_effort_prefix(old: &Value, config: &Value) -> bool {
    use broca_provider::{effort_change::anthropic_keeps_effort_prefix, wire::FrozenRenderConfig};
    use serde::Deserialize;
    let Ok(before) = FrozenRenderConfig::deserialize(old) else {
        return false;
    };
    let Ok(after) = FrozenRenderConfig::deserialize(config) else {
        return false;
    };
    anthropic_keeps_effort_prefix(&before, &after)
}

fn config_of_start(parts: &dyn Parts, start: &RunStart) -> Result<Value, FloorEvidence> {
    let part = start
        .setup
        .iter()
        .find(|part| part.kind == PartKind::RenderConfig)
        .ok_or(FloorEvidence::Damage)?;
    let config = parts.get_checked(part).map_err(FloorEvidence::part)?;
    start
        .render_config(Arc::new(config))
        .map(Arc::unwrap_or_clone)
        .map_err(|_| FloorEvidence::Damage)
}

fn tools_of_start(
    parts: &dyn Parts,
    start: &RunStart,
    config: &Value,
) -> Result<String, FloorEvidence> {
    let tools: Vec<Tool> = start
        .setup
        .iter()
        .find(|part| part.kind == PartKind::Tools)
        .map(|part| {
            parts
                .get_checked(part)
                .map_err(FloorEvidence::part)
                .and_then(|value| serde_json::from_value(value).map_err(|_| FloorEvidence::Damage))
        })
        .transpose()?
        .unwrap_or_default();
    Ok(broca_core::signed_floor::tool_set_digest(&tools, config))
}

pub(super) fn new_abi(config: &Value) -> bool {
    config["family_id"] == "anthropic_messages"
        && config["renderer_version"]
            .as_u64()
            .is_some_and(|version| version >= 4)
}

pub(super) fn before_start(error: LoopError) -> LoopError {
    match error {
        LoopError::FloorRead(detail) => LoopError::Refused(broca_core::RunError::Transient(detail)),
        other => other,
    }
}

pub(super) fn render_episodes_checked<W: Writer>(
    writer: &W,
    parts: &dyn Parts,
) -> Result<Vec<EpisodeRenderInput>, LoopError> {
    starts(writer.conversation())
        .map(|episode| {
            let Some(part) = &episode.render_config else {
                return Ok(EpisodeRenderInput {
                    prompt_start: (episode.start - 1) as usize,
                    first_user: episode.first_user.map(|ordinal| (ordinal - 1) as usize),
                    frozen_config: Value::Null,
                });
            };
            let frozen_config = match parts.get_checked(part) {
                Err(PartReadError::Transient(_)) => parts.get_checked(part),
                result => result,
            }
            .map_err(|error| match error {
                PartReadError::Transient(detail) => LoopError::FloorRead(detail),
                PartReadError::Damaged(detail) => LoopError::Parts(format!(
                    "run {}'s historical render config: {detail}",
                    episode.run_id
                )),
            })?;
            Ok(EpisodeRenderInput {
                prompt_start: (episode.start - 1) as usize,
                first_user: episode.first_user.map(|ordinal| (ordinal - 1) as usize),
                frozen_config,
            })
        })
        .collect()
}

/// Initial preparation and re-render after a seal have the same terminal errors.
pub(super) async fn finish_error<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    admission: &Admission,
    error: LoopError,
) -> Result<Finished, LoopError> {
    let (reason, error) = match error {
        LoopError::Unsupported(UNVERIFIABLE) => (RunFinishReason::Interrupted, unverifiable()),
        LoopError::Parts(detail) if new_abi(&admission.render_config) => {
            let mut error = crate::compaction::unavailable_error(&detail);
            error.provider_code = Some("historical_render_config_unavailable".into());
            (RunFinishReason::Error, error)
        }
        LoopError::FloorRead(detail) => {
            return unavailable(writer, seams, admission, &detail).await
        }
        error => return Err(error),
    };
    end_with_error(writer, seams, &admission.run_id, reason, &error).await
}
