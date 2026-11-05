//! Compaction on the current contract: what a send's calls to the compaction provider read from
//! the session's state. Those calls are Setup, notices of new entries, and rides (a call made just
//! before a request is sent, asking for a newer view when something changed, such as the model
//! or the cache expiring).
//!
//! The decisions are today's (`broca_core::compaction_v2`). Today's loop finds their inputs by
//! folding the whole log on every step; here they come from the operational fold, which keeps
//! each as a latest value (see `broca_session_store::fold::operations::Operations`).

use std::sync::Arc;

use broca_core::compaction::CompactionHistory;
use broca_core::compaction_v2::{
    self as v2,
    received::ReceivedView,
    state::{Journal, State},
};
use broca_protocol::BindIdentity;
use broca_session_store::fold::operations::{Estimate, Operations};
use broca_session_store::record::{Marker, Record, View, ViewApplied};
use broca_wal::WalRecord;

use crate::run::{Admission, LoopError, Seams};
use crate::writer::Writer;

/// What a run whose plan names a compaction provider needs for its calls to that provider.
pub struct Ctx {
    /// Today's per-session compaction state and call runtime, held for the session's life by
    /// whoever owns the session, as today's actor holds it.
    pub session: Arc<v2::session::Session>,
    /// The compaction provider's module and the preset the plan asks for.
    pub provider: String,
    pub preset: String,
    /// The send context of the newest accepted request, when an earlier run made it: what this
    /// run's first ride compares with. Once this run's own request is accepted, its own context
    /// is the previous one.
    pub earlier: Option<(String, v2::conditions::SendContext)>,
}

/// The send context of the session's newest accepted request when an earlier run than
/// `admission`'s made it, rebuilt from that run's start, which the session's id lookup finds
/// sealed or open. A run the fold names but the session can't find is damage.
pub async fn earlier_context<W: Writer>(
    writer: &W,
    admission: &Admission,
    parts: &dyn crate::parts::Parts,
) -> Result<Option<(String, v2::conditions::SendContext)>, LoopError> {
    let Some(run) = writer
        .operations()
        .last_send_run
        .clone()
        .filter(|run| *run != admission.run_id)
    else {
        return Ok(None);
    };
    let start = writer.run_start(&run).await?.ok_or_else(|| {
        LoopError::Parts(format!(
            "the session names run {run} as its newest request's, but holds no start for it"
        ))
    })?;
    let earlier = crate::run::admission_of_start(&start, parts)?;
    let context = v2::host::context_of(&frozen(&earlier), earlier.manifest.as_deref());
    Ok(Some((run, context)))
}

/// Today's compaction state, rebuilt from the open log's records (see
/// `Session::open_log_records`) by today's fold: each accepted view rebuilt from its stored
/// edits and checked against its checksum, the views steps applied, and the latest marker.
///
/// The operational fold names the accepted and applied views; a view it names that the open
/// log no longer holds was sealed out with the ledger, and rebuilding it needs the view bases a
/// seal will write (not built yet), so that refuses by name.
pub fn restore(
    records: &[(u64, Record)],
    bind: &BindIdentity,
    operations: &Operations,
) -> Result<State, String> {
    let mut state = State::default();
    for (_, record) in records {
        let today = match record {
            Record::RunStart(_) => {
                state.begin_run();
                continue;
            }
            Record::StepStart(step) => {
                state.start_step(step.step_id);
                continue;
            }
            Record::View(view) => WalRecord::CompactionViewV2(broca_wal::CompactionViewV2 {
                version: view.version,
                checksum: view.checksum.clone(),
                edits: serde_json::from_value(view.edits.clone())
                    .map_err(|error| format!("a stored compaction view's edits: {error}"))?,
            }),
            Record::ViewApplied(applied) => WalRecord::CompactionViewAppliedV2 {
                step_id: applied.step_id,
                version: applied.version,
                checksum: applied.checksum.clone(),
            },
            Record::Marker(marker) => WalRecord::CompactionMarker {
                harness: marker
                    .harness
                    .clone()
                    .ok_or("a compaction marker names no session")?,
                session: marker
                    .session
                    .clone()
                    .ok_or("a compaction marker names no session")?,
                view_version: marker.view_version,
                first_kept_mid: marker.first_kept_mid.clone(),
                first_kept_ordinal: marker.first_kept_ordinal,
                checkpoint: marker.checkpoint,
            },
            _ => continue,
        };
        v2::fold::fold(&mut state, &today, bind)?;
    }
    v2::fold::validate_identity(&state, bind)?;
    let accepted = operations
        .view_accepted
        .as_ref()
        .map(|slot| (slot.version, slot.checksum.as_str()));
    let applied = operations
        .view_applied
        .as_ref()
        .map(|slot| (slot.view.version, slot.view.checksum.as_str()));
    fn identity(view: Option<&cortexkit_role_compaction::view::View>) -> Option<(u64, &str)> {
        view.map(|view| (view.version, view.checksum.as_str()))
    }
    if accepted != identity(state.latest()) || applied != identity(state.applied()) {
        return Err(
            "a compaction view the session names is no longer in its open log, and rebuilding it \
             needs a view base, which isn't built yet"
                .into(),
        );
    }
    Ok(state)
}

/// Today's compaction journal over the session's writer: an accepted view, the view a step
/// applies and a marker are each appended durably before the call returns, as today's
/// `WalJournal` syncs each. The marker names the session it was written for.
pub struct StoreJournal<'w, W> {
    writer: tokio::sync::Mutex<&'w mut W>,
    bind: &'w BindIdentity,
}

impl<'w, W: Writer> StoreJournal<'w, W> {
    pub fn new(writer: &'w mut W, bind: &'w BindIdentity) -> Self {
        Self {
            writer: tokio::sync::Mutex::new(writer),
            bind,
        }
    }

    async fn commit(&self, record: Record) -> Result<(), String> {
        self.writer
            .lock()
            .await
            .append(&[record], true)
            .await
            .map(|_| ())
            .map_err(|error| format!("{error:?}"))
    }
}

impl<W: Writer> Journal for StoreJournal<'_, W> {
    async fn accepted(
        &self,
        previous: Option<&ReceivedView>,
        received: &ReceivedView,
    ) -> Result<(), String> {
        let view = received.view();
        let edits = serde_json::to_value(v2::storage::encode(previous, received))
            .map_err(|error| error.to_string())?;
        self.commit(Record::View(View {
            version: view.version,
            checksum: view.checksum.clone(),
            edits,
        }))
        .await
    }

    async fn applied(
        &self,
        step: u64,
        view: &cortexkit_role_compaction::view::View,
    ) -> Result<(), String> {
        self.commit(Record::ViewApplied(ViewApplied {
            step_id: step,
            version: view.version,
            checksum: view.checksum.clone(),
        }))
        .await
    }

    async fn marker(
        &self,
        view: &cortexkit_role_compaction::view::View,
        first_kept_mid: &str,
        first_kept_ordinal: u64,
        checkpoint: u64,
    ) -> Result<(), String> {
        // When the cut covers every entry, the first kept entry is the next message, whose id
        // isn't minted until its record is written.
        if first_kept_mid == NEXT_MID {
            return Err(
                "a compaction cut through the newest entry needs the next message's id, which \
                 isn't minted yet"
                    .into(),
            );
        }
        self.commit(Record::Marker(Marker {
            view_version: view.version,
            first_kept_mid: first_kept_mid.into(),
            first_kept_ordinal,
            checkpoint,
            harness: Some(self.bind.harness.clone()),
            session: Some(self.bind.session.clone()),
        }))
        .await
    }
}

/// How a step's request came out of compaction.
pub enum Send {
    /// The request to send: the prompt the applied view renders, with the run's settings.
    Ready(Box<broca_protocol::request::CallOptions>),
    /// The provider can't serve the run (Setup failed with no view to fall back on, or the run
    /// froze no window to state conditions from), so the run ends `compaction_unavailable`.
    Unavailable(String),
}

/// The error a run ends with when its compaction provider can't serve it: permanent, with code
/// `compaction_unavailable`, the same error today's loop writes in that case.
pub fn unavailable_error(message: &str) -> broca_protocol::ProviderError {
    broca_protocol::ProviderError {
        provider_details: None,
        provider_subcode: None,
        refusal: None,
        class: broca_protocol::ErrorClass::Permanent,
        message: message.into(),
        status: None,
        retry_after_secs: None,
        provider_code: Some("compaction_unavailable".into()),
        resets_at_ms: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
    }
}

/// What today's compaction code reads of the session's transcript: the entries, and the stored
/// messages they match position by position.
///
/// The entries are the window's, in today's terms: the window, and the assistant message whose
/// tool calls are still running. After a seal at a cut, the history before the window is sealed,
/// and today's code still expects each view's cut target among the entries, so each target the
/// views name before the window is read back and put in front, with its message. Every other
/// edit naming a sealed entry is skipped by the renderer, which changes nothing: those entries
/// are cut anyway, and the compaction provider never anchors an insert before an entry.
struct Transcript {
    entries: Vec<cortexkit_role_compaction::read::Entry>,
    /// The sealed messages put in front of the window's, for the stored prompt, with ordinals.
    sealed: Vec<(u64, broca_protocol::Message)>,
}

async fn transcript<W: Writer>(writer: &W, ctx: &Ctx) -> Result<Transcript, LoopError> {
    let conversation = writer.conversation();
    let window = broca_session_store::session::served_window(conversation);
    let mut stored = Vec::new();
    if conversation.window_start() > 1 {
        let mut targets: Vec<String> = {
            let state = ctx.session.state.lock().await;
            [state.applied(), state.latest()]
                .into_iter()
                .flatten()
                .flat_map(|view| &view.edits)
                .filter_map(|edit| match &edit.op {
                    cortexkit_role_compaction::view::EditOp::Cut { through } => {
                        Some(through.clone())
                    }
                    _ => None,
                })
                .collect()
        };
        targets.sort();
        targets.dedup();
        for mid in targets {
            let entry = writer.entry_by_mid(&mid).await?.ok_or_else(|| {
                LoopError::Parts(format!(
                    "a compaction view cuts through {mid}, which the session doesn't hold"
                ))
            })?;
            if entry.ordinal < conversation.window_start() {
                stored.push(entry);
            }
        }
        stored.sort_by_key(|entry| entry.ordinal);
    }
    let entries = stored
        .iter()
        .chain(&window)
        .map(crate::entries::project)
        .collect::<Result<Vec<_>, _>>()
        .map_err(LoopError::Parts)?;
    let sealed = stored
        .into_iter()
        .map(|entry| (entry.ordinal, entry.message))
        .collect();
    Ok(Transcript { entries, sealed })
}

/// `opts` with the sealed messages a [`Transcript`] put in front of the window's prepended to its
/// prompt, each stamped with its own position, so the stored prompt matches the entries.
fn with_sealed(
    mut opts: broca_protocol::request::CallOptions,
    transcript: &Transcript,
) -> broca_protocol::request::CallOptions {
    if transcript.sealed.is_empty() {
        return opts;
    }
    let mut prompt: Vec<_> = transcript
        .sealed
        .iter()
        .map(|(ordinal, message)| {
            let mut message = message.clone();
            message.ordinal = Some(ordinal - 1);
            message
        })
        .collect();
    prompt.extend(opts.prompt.iter().cloned());
    opts.prompt = prompt;
    opts
}

/// Seals the session's history before a cut the step just applied, and starts the new open log
/// with full copies of both view slots: a seal moves the ledger out, and a stored view is a
/// change against the views before it. The copies go applied view first, then the step that
/// applied it, then the accepted view, so today's fold rebuilds both slots from them.
pub async fn seal_at_cut<W: Writer, P, T>(
    writer: &mut W,
    seams: &Seams<'_, P, T>,
    ctx: &Ctx,
    first_kept_ordinal: u64,
) -> Result<(), LoopError> {
    let step = writer
        .operations()
        .view_applied
        .as_ref()
        .ok_or_else(|| LoopError::Parts("a cut was marked with no view applied".into()))?
        .step_id;
    let carry = {
        let state = ctx.session.state.lock().await;
        let applied = state
            .applied_received()
            .ok_or_else(|| LoopError::Parts("a cut was marked with no view applied".into()))?;
        let full = |previous: Option<&ReceivedView>, view: &ReceivedView| {
            serde_json::to_value(v2::storage::encode(previous, view))
                .map(|edits| {
                    Record::View(View {
                        version: view.view().version,
                        checksum: view.view().checksum.clone(),
                        edits,
                    })
                })
                .map_err(|error| LoopError::Parts(error.to_string()))
        };
        let mut carry = vec![
            full(None, applied)?,
            Record::ViewApplied(ViewApplied {
                step_id: step,
                version: applied.view().version,
                checksum: applied.view().checksum.clone(),
            }),
        ];
        if let Some(latest) = state
            .latest_received()
            .filter(|latest| latest.view().version != applied.view().version)
        {
            carry.push(full(Some(applied), latest)?);
        }
        carry
    };
    let window_repetition = spanning_repetition(writer.conversation(), first_kept_ordinal)?;
    writer
        .seal_at_entry(
            first_kept_ordinal,
            &broca_session_store::session::Carry {
                records: &carry,
                window_repetition,
            },
            (seams.now_ms)(),
        )
        .await?;
    Ok(())
}

/// The repetition counters a cut before ordinal `first_kept` must keep: when the cut splits a
/// run that already took steps below it, that run's counters over those steps, continuing from
/// the counters the window already kept for it when it spanned the previous cut too. `None`
/// when no run's steps are split.
fn spanning_repetition(
    conversation: &broca_session_store::fold::conversation::Conversation,
    first_kept: u64,
) -> Result<Option<serde_json::Value>, LoopError> {
    let window_start = conversation.window_start();
    let episodes = &conversation.episodes;
    let Some(at) = episodes
        .iter()
        .rposition(|episode| episode.start < first_kept)
    else {
        return Ok(None);
    };
    if episodes
        .get(at + 1)
        .is_some_and(|next| next.start <= first_kept)
    {
        return Ok(None);
    }
    let run = &episodes[at];
    // The run's window messages below the cut. When the run started before the window, its
    // earlier steps are counted in the counters the window kept.
    let from = run.start.max(window_start);
    let below: Vec<_> = (from..first_kept)
        .filter_map(|ordinal| conversation.window_message(ordinal).cloned())
        .collect();
    let seed =
        match (run.start < window_start, &conversation.window_repetition) {
            (true, Some(seed)) => Some(serde_json::from_value(seed.clone()).map_err(|error| {
                LoopError::Parts(format!("stored repetition counters: {error}"))
            })?),
            _ => None,
        };
    // Counting starts at the run's first step; its input before that has nothing to count.
    let steps = match &seed {
        Some(_) => &below[..],
        None => match below
            .iter()
            .position(|message| message.role == broca_protocol::content::Role::Assistant)
        {
            Some(first_step) => &below[first_step..],
            None => return Ok(None),
        },
    };
    match broca_core::run::repetition_checkpoint(steps, seed.as_ref()) {
        Ok(Some(checkpoint)) => serde_json::to_value(checkpoint)
            .map(Some)
            .map_err(|error| LoopError::Parts(error.to_string())),
        Ok(None) => Err(LoopError::Unsupported(
            "a compaction cut between a tool call and its result",
        )),
        Err(()) => Err(LoopError::Parts(
            "stored repetition counters were taken under other warning rules".into(),
        )),
    }
}

/// The run's frozen choices in the shape today's compaction conditions read. Its model
/// variant, service tier and account choice aren't in the new loop's admission yet, so they read
/// as none.
fn frozen(admission: &Admission) -> v2::host::Frozen<'_> {
    v2::host::Frozen {
        model: &admission.model,
        variant: None,
        service_tier: None,
        frozen_render_config: &admission.render_config,
        tools: &admission.tools,
        // A planned run's system text is its plan's, which the send context reads from the
        // manifest; only an unplanned run's own system text counts here, as today's config holds.
        global_system: (admission.manifest.is_none() && !admission.global_system.is_empty())
            .then_some(admission.global_system.as_slice()),
        auth_selection: None,
        context_limit: admission.context_limit,
        max_output_tokens: admission.generation.max_output_tokens,
    }
}

/// The context of the newest request the provider accepted, which a ride compares with the
/// current run's. `None` before any.
fn previous<W: Writer>(
    writer: &W,
    admission: &Admission,
    ctx: &Ctx,
) -> Result<Option<v2::conditions::SendContext>, LoopError> {
    match writer.operations().last_send_run.as_deref() {
        None => Ok(None),
        Some(run) if run == admission.run_id => Ok(Some(v2::host::context_of(
            &frozen(admission),
            admission.manifest.as_deref(),
        ))),
        Some(run) => match &ctx.earlier {
            Some((earlier, context)) if earlier == run => Ok(Some(context.clone())),
            _ => Err(LoopError::Parts(format!(
                "the newest request's run {run} has no context loaded"
            ))),
        },
    }
}

/// `prompt` with the run's settings, the rendering inputs and the system text above it, as
/// today's `render::finish` completes a compacted prompt.
fn finish(
    boundary: &broca_protocol::request::CallOptions,
    prompt: Vec<broca_protocol::Message>,
    admission: &Admission,
) -> Result<broca_protocol::request::CallOptions, String> {
    let opts = v2::render::options(boundary, prompt);
    Ok(v2::systems::view(&opts, &admission.global_system)?.unwrap_or(opts))
}

/// Applies at step `step`'s boundary the view the provider handed over, if it still fits the
/// transcript, and finishes a marker a crash left owed, as today's loop does before the step
/// starts. `Err` from the inner result means the run must end `compaction_unavailable`.
pub async fn apply_boundary<W: Writer, P, T>(
    writer: &mut W,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    ctx: &Ctx,
    step: u64,
) -> Result<Result<(), String>, LoopError> {
    let entries = transcript(writer, ctx).await?.entries;
    let conditions = match v2::host::conditions_of(&frozen(admission), &entries, false) {
        Ok(conditions) => conditions,
        Err(message) => return Ok(Err(message)),
    };
    let journal = StoreJournal::new(writer, &seams.bind);
    v2::request::apply_boundary(
        &ctx.session.state,
        &ctx.session.runtime,
        &journal,
        v2::state::Boundary {
            step,
            entries: &entries,
            rules: &conditions.rules,
            accepts_images: conditions.accepts_images == Some(true),
            next_mid: NEXT_MID,
        },
    )
    .await
    .map_err(LoopError::Parts)?;
    Ok(Ok(()))
}

/// Prepares step `step`'s request through today's compaction code: Setup when the provider
/// needs it, a ride when something changed, the notices owed, and the prompt the applied view
/// renders.
pub async fn prepare<W: Writer, P: broca_core::provider::Provider, T>(
    writer: &mut W,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    ctx: &Ctx,
    step: u64,
    opts: broca_protocol::request::CallOptions,
) -> Result<Send, LoopError> {
    let transcript = transcript(writer, ctx).await?;
    let opts = with_sealed(opts, &transcript);
    let entries = transcript.entries;
    let chained = seams.provider.history_edit_cost()
        == Some(broca_core::compaction_provider::notice::HistoryEditCost::FullResend);
    let conditions = match v2::host::conditions_of(&frozen(admission), &entries, chained) {
        Ok(conditions) => conditions,
        Err(message) => return Ok(Send::Unavailable(message)),
    };
    let current = v2::host::context_of(&frozen(admission), admission.manifest.as_deref());
    let previous = previous(writer, admission, ctx)?;
    let history = writer
        .operations()
        .estimate
        .as_ref()
        .map(history)
        .unwrap_or_default();
    let plane = v2::runtime::BoundPlane {
        plane: seams.compaction_plane,
        provider: &ctx.provider,
        bind: &seams.bind,
        scope: None,
    };
    let journal = StoreJournal::new(writer, &seams.bind);
    let prepared = v2::request::prepare(
        &ctx.session.state,
        &ctx.session.runtime,
        &journal,
        &plane,
        v2::request::At {
            step,
            preset: &ctx.preset,
            entries: &entries,
            stored: &opts.prompt,
            conditions: &conditions,
            previous: previous.as_ref(),
            current: &current,
            // Cache expiry is proven only from a cache lifetime the run's requests ask for,
            // which keep-warm reads; the new loop has no keep-warm yet, as a run without one
            // in today's loop.
            cache_expired: false,
            detects_cache_expired: false,
            chain_reset: chained && seams.provider.prepare_chain_reset(),
            now: (seams.now_ms)(),
            next_mid: NEXT_MID,
        },
        |_| Err("a compaction view's inserted images aren't stored yet".into()),
        |prompt| {
            let view = finish(&opts, prompt.to_vec(), admission)?;
            seams
                .provider
                .try_render_request(&view)
                .map(|bytes| broca_core::compaction::request_estimate(bytes.len(), &history).0)
                .map_err(|error| error.message.clone())
        },
    )
    .await
    .map_err(LoopError::Parts)?;
    match prepared {
        v2::request::Prepared::Ready {
            prompt, applied, ..
        } => {
            seams
                .provider
                .set_cache_evidence_view(applied.map(|view| view.version));
            Ok(Send::Ready(Box::new(
                finish(&opts, prompt, admission).map_err(LoopError::Parts)?,
            )))
        }
        v2::request::Prepared::CompactionUnavailable => Ok(Send::Unavailable(
            "compaction provider Setup is unavailable".into(),
        )),
        v2::request::Prepared::ContextCeiling { .. } => Err(LoopError::Unsupported(
            "a request over the window after the compaction provider's answer",
        )),
    }
}

/// Tells the compaction provider about entries written since its last notice, as today's loop
/// does after each durable model step.
pub async fn notice<W: Writer, P, T>(
    writer: &W,
    admission: &Admission,
    seams: &Seams<'_, P, T>,
    ctx: &Ctx,
) -> Result<(), LoopError> {
    // Without a frozen window no conditions can be stated, so nothing is announced; the next
    // step's boundary ends the run.
    if admission.context_limit.is_none() {
        return Ok(());
    }
    let entries = transcript(writer, ctx).await?.entries;
    let conditions =
        v2::host::conditions_of(&frozen(admission), &entries, false).map_err(LoopError::Parts)?;
    let applied = ctx.session.state.lock().await.applied_identity();
    let mut setup =
        v2::conditions::setup_request(&ctx.preset, &conditions, (seams.now_ms)(), false);
    setup.applied_view = applied.clone();
    setup.newest = v2::views::newest(&entries);
    ctx.session.runtime.observe(&entries);
    ctx.session
        .runtime
        .flush(
            &v2::runtime::BoundPlane {
                plane: seams.compaction_plane,
                provider: &ctx.provider,
                bind: &seams.bind,
                scope: None,
            },
            &setup,
            &conditions,
            applied.as_ref(),
        )
        .await;
    Ok(())
}

/// What the loop gives today's compaction code as the next message's id. A new message's id is
/// minted from the seq its record takes, so before it's written there's none; the journal
/// refuses a marker that would name it.
pub const NEXT_MID: &str = "";

/// The request-size history compaction's estimate reads, rebuilt from the fold's latest send
/// facts the way today's `CompactionHistory` folds them: the newest step's usage, measured for
/// the model that served it, and the request body lengths around it.
pub fn history(estimate: &Estimate) -> CompactionHistory {
    let mut history = CompactionHistory {
        current_model: estimate.run_model.clone(),
        current_request_bytes: estimate
            .current_request_bytes
            .and_then(|bytes| usize::try_from(bytes).ok()),
        previous_request_bytes: estimate
            .previous_request_bytes
            .and_then(|bytes| usize::try_from(bytes).ok()),
        ..CompactionHistory::default()
    };
    if !estimate.stepped {
        return history;
    }
    let fallback: broca_wal::record::StepFallback = estimate
        .step_fallback
        .clone()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    let requested = fallback
        .route
        .as_ref()
        .map(|route| &route.model)
        .or(estimate.step_model.as_ref());
    // One prompt measurement, not the sum of prompts sent to every model that served a
    // server-side handoff, as today's history keeps it.
    let native = requested.and_then(|requested| {
        broca_protocol::usage::model_usage(
            fallback
                .served_model
                .as_deref()
                .unwrap_or(&requested.model_id),
            estimate.step_usage.unwrap_or_default(),
            fallback.usage_iterations.as_deref(),
        )
        .last()
        .map(|(model, usage)| {
            (
                broca_protocol::ModelHandle::new(&requested.provider_module_id, *model),
                *usage,
            )
        })
    });
    history.previous_native_usage = native.as_ref().map(|(_, usage)| *usage);
    history.previous_model = native.map(|(model, _)| model);
    history
}
