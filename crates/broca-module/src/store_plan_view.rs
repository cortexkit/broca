//! The applied plan on the new storage, using the same read path and shared setup parts as
//! transcript reads. Attribution is computed, never written into a session's history.

use broca_protocol::BindIdentity;
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::record::Record;
use broca_session_store::session::Session;
use broca_wire::plan_view::*;

use crate::serve::ServeError;
use crate::store_open::StoreRuntime;

#[derive(Debug)]
pub enum Error {
    Serve(ServeError),
    NoPlan,
    TooLarge,
}

impl From<ServeError> for Error {
    fn from(error: ServeError) -> Self {
        Self::Serve(error)
    }
}

pub async fn serve(
    registry: &crate::LineageRegistry,
    store: &StoreRuntime,
    key: &BindIdentity,
) -> Result<PlanViewResponse, Error> {
    match store
        .read(
            registry,
            key,
            crate::store_reads::StoreReadRequest::PlanView,
        )
        .await?
    {
        Some(crate::store_reads::StoreReadReply::PlanView(reply)) => reply,
        None => Err(Error::NoPlan),
        Some(_) => Err(crate::store_reads::mismatched().into()),
    }
}

pub(crate) async fn build<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
) -> Result<PlanViewResponse, Error> {
    let plan = session.operations.plan.as_ref().ok_or(Error::NoPlan)?;
    let current = plan.current.as_ref().ok_or(Error::NoPlan)?;
    // This decoder shares the manifest and its tool-list Arc with the running loop. Reading
    // a plan must not decode another copy of a potentially large tool catalog.
    let manifest = broca_session_loop::run::stored_manifest(current, parts)
        .map_err(|error| damaged(format!("the frozen plan can't be read: {error:?}")))?;
    let items = system_items(&manifest);
    let (applied_at_ms, newest_step) = times(session, plan.applied_generation).await?;
    let model = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .ok_or_else(|| damaged("the current plan has no run start".into()))?;
    let tools = manifest
        .tools
        .iter()
        .map(|tool| {
            let provider = tool.module.clone().ok_or_else(|| {
                damaged(format!("the planned tool {:?} has no provider", tool.name))
            })?;
            let planned = PlanViewTool::new(
                tool.name.clone(),
                tool.description.clone(),
                tool.input_schema.clone(),
                provider,
            );
            Ok(
                match broca_core::compaction_v2::tokens::tool_definition_tokens(
                    &model.model.model_id,
                    std::slice::from_ref(tool),
                    &serde_json::Value::Null,
                ) {
                    Some(tokens) => planned.with_tokens(PlanViewTokens::estimate(tokens.count)),
                    None => planned,
                },
            )
        })
        .collect::<Result<_, Error>>()?;
    let system = PlanViewSystem::new(manifest.system_text.clone());
    let system = match items {
        Some(items) => system.with_items(items),
        None => system,
    };
    let mut reply = PlanViewResponse::new(plan.applied_generation, system, tools);
    if let Some(at_ms) = applied_at_ms {
        reply = reply.with_applied_at_ms(at_ms);
    }
    if let Some(step) = newest_step {
        reply = reply.with_newest_step(step);
    }
    // Broca never tells the model about a plan it hasn't applied yet, so a pending plan has no
    // notes to show.
    if let Some(pending) = &plan.pending {
        reply = reply.with_pending(PlanViewPending::new(pending.generation));
    }
    // The contract's own check, so broca never serves a reply its readers refuse to decode.
    reply
        .check()
        .map_err(|member| damaged(format!("the plan view's {member} is invalid")))?;
    if broca_wire::serialized_result_frame_bytes(&reply) > broca_wire::SUBC_MAX_FRAME_BYTES {
        return Err(Error::TooLarge);
    }
    Ok(reply)
}

fn system_items(manifest: &broca_wal::PlanManifest) -> Option<Vec<PlanViewSystemItem>> {
    let checked = || -> Option<Vec<PlanViewSystemItem>> {
        if manifest.join_version != broca_fetch_plan::admit::JOIN_VERSION {
            return None;
        }
        #[derive(serde::Deserialize)]
        struct Items {
            system_text_items: Vec<Item>,
        }
        #[derive(serde::Deserialize)]
        struct Item {
            provider: String,
            preset: Option<String>,
        }
        let plan: Items = serde_json::from_str(manifest.plan.as_json_text()).ok()?;
        let texts: Vec<_> = plan
            .system_text_items
            .iter()
            .map(|item| {
                manifest
                    .fetches
                    .iter()
                    .find_map(|fetch| {
                        if fetch.provider != item.provider {
                            return None;
                        }
                        match &fetch.outcome {
                            broca_wal::FetchOutcome::Answered { text, .. } => text.as_deref(),
                            _ => None,
                        }
                    })
                    .unwrap_or("")
            })
            .collect();
        let (joined, ranges) = broca_fetch_plan::admit::join_system_texts_with_ranges(texts);
        if joined != manifest.system_text {
            return None;
        }
        let mut items = Vec::new();
        for (item, range) in plan.system_text_items.into_iter().zip(ranges) {
            let Some(range) = range else {
                continue;
            };
            let ranged = PlanViewSystemItem::new(
                item.provider,
                u64::try_from(range.start).ok()?,
                u64::try_from(range.end).ok()?,
            );
            items.push(match item.preset {
                Some(preset) => ranged.with_preset(preset),
                None => ranged,
            });
        }
        Some(items)
    };
    let items = checked();
    if items.is_none() {
        tracing::warn!(target: "broca.plan_view", join_version = manifest.join_version,
            "the frozen system text can't be rejoined; omitting item ranges");
    }
    items
}

/// Only recorded times describe the age of a plan or request. In particular, a request in
/// flight has no stored send time yet and cannot displace the newest dated request.
async fn times<B: SessionBackend>(
    session: &Session<B>,
    generation: u64,
) -> Result<(Option<u64>, Option<PlanViewStep>), Error> {
    let mut application = false;
    let mut chosen = None;
    let records = session
        .ledger_tail_until(|record| {
            match record {
                Record::RefreshApplied(applied) if applied.generation == generation => {
                    application = true;
                }
                Record::Attempt(attempt) if chosen.is_none() && dated(attempt) => {
                    chosen = Some((attempt.run_id.clone(), attempt.step_id));
                }
                Record::RunStart(_) if application => return true,
                Record::StepStart(start)
                    if generation == 0
                        && chosen.as_ref().is_some_and(|(run, step)| {
                            run == &start.run_id && *step == start.step_id
                        }) =>
                {
                    return true;
                }
                _ => {}
            }
            false
        })
        .await
        .map_err(crate::store_read::read_failed)?;
    let applied_at_ms = if generation == 0 {
        session
            .operations
            .owner
            .first_start
            .as_ref()
            .and_then(|start| start.ts_ms)
    } else {
        records
            .iter()
            .rev()
            .find_map(|(_, record)| match record {
                Record::RunStart(start) => Some(start.ts_ms),
                _ => None,
            })
            .ok_or_else(|| damaged("the applied generation has no run start".into()))?
    };
    let Some((_, Record::Attempt(attempt))) = records
        .iter()
        .find(|(_, record)| matches!(record, Record::Attempt(attempt) if dated(attempt)))
    else {
        return Ok((applied_at_ms, None));
    };
    let step = records.iter().find_map(|(seq, record)| match record {
        Record::StepStart(start)
            if start.run_id == attempt.run_id && start.step_id == attempt.step_id =>
        {
            Some((*seq, start))
        }
        _ => None,
    });
    let mut reply_mid = None;
    if let Some((seq, start)) = step {
        if let Some(mid) = &start.mid {
            if session
                .entry_by_mid(mid)
                .await
                .map_err(crate::store_read::read_failed)?
                .is_some()
            {
                reply_mid = Some(mid.clone());
            }
        } else {
            // Older starts did not announce a mid. Read the matching assistant's stored entry
            // facts instead of deriving an id from its ordinal or a provider message id.
            let mut assistant = false;
            let end = records
                .iter()
                .rev()
                .find_map(|(next, record)| {
                    (*next > seq && matches!(record, Record::RunStart(_) | Record::StepStart(_)))
                        .then_some(next.saturating_sub(1))
                })
                .unwrap_or(u64::MAX);
            for (_, record) in session
                .records_between(seq, end)
                .await
                .map_err(crate::store_read::read_failed)?
            {
                match record {
                    Record::Assistant(message)
                        if message.run_id == attempt.run_id
                            && message.step_id == attempt.step_id =>
                    {
                        assistant = true
                    }
                    Record::EntryFacts(facts) if assistant => {
                        reply_mid = Some(facts.mid.clone());
                        break;
                    }
                    _ => {}
                }
            }
        }
    }
    let step = PlanViewStep::new(attempt.sent_at_ms.expect("a dated attempt has a send time"));
    Ok((
        applied_at_ms,
        Some(match reply_mid {
            Some(mid) => step.with_reply_mid(mid),
            None => step,
        }),
    ))
}

fn dated(attempt: &broca_session_store::record::Attempt) -> bool {
    attempt.sent_at_ms.is_some()
        && !attempt
            .billing
            .as_ref()
            .is_some_and(|billing| billing.parse()["reservation"]["warm"] == true)
}

fn damaged(message: String) -> Error {
    ServeError::Open(
        format!("the session's plan view can't be read: {message}"),
        None,
    )
    .into()
}
