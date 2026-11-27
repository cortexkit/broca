use std::{borrow::Cow, sync::Arc};

use broca_protocol::{request::RenderHistory, CallOptions};
use broca_provider::wire::FrozenRenderConfig;
use broca_session_store::record::PlanNoteKind;
use cortexkit_role_compaction::notice::Conditions;

use super::{v2, Admission, Ctx, LoopError, Transcript, Writer};

/// A step has its base options. A post-step notice projects the next request from the latest transcript and
/// the same run-start ordinals, but the next step may already be scheduled for another model.
pub(super) enum Next<'a> {
    Step(u64, &'a CallOptions, &'a [broca_protocol::Message]),
    Boundary(Option<&'a Arc<RenderHistory>>),
}

pub(super) async fn count<W: Writer>(
    writer: &W,
    admission: &Admission,
    ctx: &Ctx,
    transcript: &Transcript,
    conditions: &mut Conditions,
    next: Next<'_>,
) {
    let unavailable = estimate(writer, admission, ctx, transcript, conditions, next)
        .await
        .is_err();
    ctx.session
        .runtime
        .tokens
        .lock()
        .unwrap_or_else(|error| error.into_inner())
        .note_estimate_unavailable(unavailable);
}

async fn estimate<W: Writer>(
    writer: &W,
    admission: &Admission,
    ctx: &Ctx,
    transcript: &Transcript,
    conditions: &mut Conditions,
    next: Next<'_>,
) -> Result<(), LoopError> {
    let operations = writer.operations();
    let Some(plan) = operations
        .plan
        .as_ref()
        .filter(|plan| plan.has_rendered_notes())
    else {
        return Ok(());
    };
    let next_step = operations
        .run
        .as_ref()
        .map_or(1, |run| run.completed_steps + 1);
    let (step, fixed_view, base) = match next {
        Next::Step(step, opts, stored) => (step, Some(step), opts.with_prompt(stored.to_vec())),
        Next::Boundary(episodes) => {
            let whole = crate::writer::whole_conversation(writer).await?;
            let mut prompt: Vec<_> = whole
                .held_messages()
                .map_err(super::not_held)?
                .into_iter()
                .map(|message| (*message).clone())
                .collect();
            if let Some((_, message, _)) = whole.open_entry() {
                prompt.push(message.as_ref().clone());
            }
            for (index, message) in prompt.iter_mut().enumerate() {
                message.ordinal = Some(whole.window_start() - 1 + index as u64);
            }
            let mut base = CallOptions::new(prompt);
            if let Some(episodes) = episodes {
                base.render_episodes = Arc::clone(episodes);
            }
            Arc::make_mut(&mut base.render_episodes).raw_ordinals = true;
            (next_step, None, super::with_sealed(base, transcript))
        }
    };
    let mut opts = base;
    opts.tools = Arc::clone(&admission.tools);
    opts.tool_choice = Arc::new(admission.tool_choice.clone());
    crate::budget::finalize_tool_choice(operations, &mut opts);
    opts.target_model = Some(admission.model.clone());
    let primary: Option<FrozenRenderConfig> =
        serde_json::from_value((*admission.render_config).clone()).ok();
    let view = {
        let state = ctx.session.state.lock().await;
        v2::shown::next_request_view(&state, &transcript.entries, conditions, fixed_view).cloned()
    };
    let rides = super::placed_rides(&transcript.rides, admission);
    let prompt = v2::render::prompt_with_rides(
        &opts.prompt,
        &transcript.entries,
        &rides,
        view.as_ref().map_or(&[][..], |view| &view.edits),
        v2::host::image_reference,
    )
    .map_err(LoopError::Parts)?;
    let opts = v2::render::options(&opts, prompt);
    let projected = crate::run::plan_notes::project(writer, admission, opts.clone())?;
    let route =
        crate::refusal::recorded_route(operations, &admission.render_config, step, &projected)?;
    let model = route
        .as_ref()
        .map_or(&admission.model, |route| &route.model);
    let render = if route.is_some() {
        primary
            .as_ref()
            .and_then(|config| config.refusal_fallback.as_ref())
            .map(|fallback| &fallback.frozen)
    } else {
        primary.as_ref()
    };
    let visible = crate::run::plan_notes::rendering_anchors(primary.as_ref(), &opts, true);
    let held: std::collections::BTreeSet<_> = opts
        .prompt
        .iter()
        .filter_map(|message| message.ordinal.map(|ordinal| ordinal + 1))
        .collect();
    let texts: Vec<_> = plan
        .visible_notes(
            |anchor| visible.contains(&anchor),
            |anchor| held.contains(&anchor),
        )
        .into_iter()
        .filter_map(|(_, note)| {
            if note.kind == PlanNoteKind::ToolAddition {
                let tool = note.tool.as_ref()?;
                return Some(Cow::Owned(format!(
                    "{} {} {} {}",
                    note.form, tool.name, tool.description, tool.input_schema
                )));
            }
            Some(Cow::Borrowed(note.text.as_str()))
        })
        .collect();
    let mut counts = ctx
        .session
        .runtime
        .tokens
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    let render_blob = render
        .as_ref()
        .and_then(|render| serde_json::to_value(render).ok());
    if let Some(render_blob) = render_blob.as_ref().filter(|_| route.is_some()) {
        let mut facts = super::frozen(admission);
        facts.model = model;
        facts.frozen_render_config = render_blob;
        counts.configure(&facts, admission.manifest.as_deref());
    }
    conditions.notes_tokens = counts.plan_notes(texts.iter().map(|text| text.as_ref()));
    Ok(())
}

/// Restoring a note can rewrite the prefix of replies signed after its redelivered copy.
/// Use the ordinary signed-part refusal if the proposed view still carries one of those parts.
pub(super) fn check_restore(
    operations: &broca_session_store::fold::operations::Operations,
    boundary: &v2::state::Boundary<'_>,
    previous: Option<&cortexkit_role_compaction::view::View>,
    next: &cortexkit_role_compaction::view::View,
    render_route: Option<(&Admission, &CallOptions)>,
) -> Result<(), cortexkit_role_compaction::view::ViewRefusal> {
    use cortexkit_compaction_view::render::{self, PlanItem};
    use cortexkit_role_compaction::{
        part::Seal,
        rules::{RuleBreak, SignedParts},
        view::ViewRefusal,
    };
    let Some(previous) = previous else {
        return Ok(());
    };
    let Some(plan) = operations
        .plan
        .as_ref()
        .filter(|_| has_marked_notes(operations))
    else {
        return Ok(());
    };
    if boundary.rules.signed_parts == SignedParts::None {
        return Ok(());
    }
    let build = |view: &cortexkit_role_compaction::view::View| {
        render::plan_view(boundary.entries, view, boundary.rules, boundary.floor)
    };
    let current = build(next)?;
    let legal =
        |view: &cortexkit_role_compaction::view::View| -> Option<std::collections::BTreeSet<u64>> {
            let (admission, base) = render_route?;
            let config = broca_core::provider_errors::recorded_render_route(
                &admission.render_config,
                base.target_model.as_ref(),
            );
            let frozen: FrozenRenderConfig = serde_json::from_value(config.clone()).ok()?;
            let prompt = v2::render::prompt_with_rides(
                &base.prompt,
                boundary.entries,
                &Default::default(),
                &view.edits,
                v2::host::image_reference,
            )
            .ok()?;
            let opts = v2::render::options(base, prompt);
            Some(crate::run::plan_notes::rendering_anchors(
                Some(&frozen),
                &opts,
                true,
            ))
        };
    let copied = match (legal(previous), legal(next)) {
        (Some(before), Some(after)) => {
            let old_notes: Vec<_> = plan
                .visible_note_changes(|anchor| before.contains(&anchor), |_| false)
                .collect();
            let next_notes = plan.visible_note_changes(|anchor| after.contains(&anchor), |_| false);
            next_notes
                .filter_map(|(change, restored, _)| {
                    match old_notes.iter().find(|(id, _, _)| *id == change) {
                        Some((_, prior, _)) if restored.anchor < prior.anchor => Some(prior.anchor),
                        None => Some(restored.anchor),
                        _ => None,
                    }
                })
                .min()
        }
        // If the prior wire prefix can't be proved, don't accept a changed view that may
        // retain a signature over it. An unchanged projection or no live signature needs no check.
        _ if previous.edits != next.edits => plan
            .notes
            .iter()
            .filter(|delivery| delivery.blocks == Some(true))
            .map(|delivery| delivery.anchor)
            .min(),
        _ => None,
    };
    let floor = render_route
        .and_then(|(_, opts)| opts.render_episodes.signed_floor)
        .unwrap_or(0);
    if let Some(copy) = copied {
        for item in current.items {
            let PlanItem::Entry {
                ordinal,
                mid,
                parts,
                ..
            } = item
            else {
                continue;
            };
            if ordinal <= copy.max(floor) {
                continue;
            }
            if let Some(entry) = boundary
                .entries
                .iter()
                .find(|entry| entry.ordinal == ordinal)
            {
                if let Some(part) = entry.parts.iter().find(|part| {
                    part.seal == Seal::Signed && parts.iter().any(|shown| shown.id == part.id)
                }) {
                    return Err(ViewRefusal::Rule(RuleBreak::SignedPartInvalid {
                        mid,
                        part: part.id.clone(),
                    }));
                }
            }
        }
    }
    Ok(())
}

pub(super) fn has_marked_notes(
    operations: &broca_session_store::fold::operations::Operations,
) -> bool {
    operations.plan.as_ref().is_some_and(|plan| {
        plan.notes
            .iter()
            .any(|delivery| delivery.blocks == Some(true) && !delivery.notes.is_empty())
    })
}

pub(super) async fn restore_options<W: Writer>(
    writer: &W,
    admission: &Admission,
    floor: Option<u64>,
) -> Result<CallOptions, LoopError> {
    let conversation = crate::writer::whole_conversation(writer).await?;
    let mut prompt = conversation
        .held_messages()
        .map_err(super::not_held)?
        .into_iter()
        .map(Arc::unwrap_or_clone)
        .collect::<Vec<_>>();
    if let Some((_, message, _)) = conversation.open_entry() {
        prompt.push(message.as_ref().clone());
    }
    for (index, message) in prompt.iter_mut().enumerate() {
        message.ordinal = Some(conversation.window_start() - 1 + index as u64);
    }
    let mut opts = CallOptions::new(prompt);
    opts.target_model = Some(admission.model.clone());
    let history = Arc::make_mut(&mut opts.render_episodes);
    history.raw_ordinals = true;
    history.signed_floor = floor;
    Ok(opts)
}
