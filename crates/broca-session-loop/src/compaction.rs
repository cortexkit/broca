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
use broca_session_store::fold::conversation::Delivered;
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
/// The operational fold names the accepted and applied views. Every seal carries whole copies of
/// them into the new log (`carried_views`), and the store refuses a seal that doesn't, so a view
/// the fold names but the open log doesn't hold means the log is damaged.
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
            "a compaction view the session names is missing from its open log, which every seal \
             carries it into"
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
    /// The request still doesn't fit the model's window after the provider's answer, so the run
    /// ends at the context ceiling without sending it. Carries the protocol's
    /// `ContextCeilingDetail`, as `run.status` reports it.
    Ceiling(serde_json::Value),
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
/// but the views still name entries there: the cut's target, and the messages earlier edits drop,
/// replace or anchor an insert before, which a provider may carry into every later view. Today's
/// code checks each new view against the entries, and refuses one naming a message it doesn't
/// hold, so each sealed entry the applied or the accepted view names is read back and put in
/// front, with its message, as today's loop, which seals nothing, would hold it. Rendering
/// changes nothing: every one of them is before the cut, which leaves it out.
pub struct Transcript {
    pub entries: Arc<Vec<cortexkit_role_compaction::read::Entry>>,
    /// The sealed messages put in front of the window's, for the stored prompt, with ordinals.
    pub sealed: Arc<Vec<(u64, broca_protocol::Message)>>,
    /// The ride envelopes of the entries' delivered texts, for rendering each ride.
    pub rides: Arc<v2::entries::RideEnvelopes>,
}

/// What the transcript keeps, for the open session, of the sealed entries its views name: the
/// session holds it while it's open and drops it when its history version moves (see
/// `Session::keep_sealed_derived`). Sealed history never changes otherwise, so each named entry
/// is read and projected once, and a later step with the same views and the same seal only
/// replaces the window's entries behind the sealed ones it built before.
#[derive(Default)]
struct SealedTranscript(std::sync::Mutex<SealedParts>);

#[derive(Default)]
struct SealedParts {
    /// Each sealed entry read so far, by message id.
    by_mid: std::collections::HashMap<String, SealedEntry>,
    /// The transcript last built.
    built: Option<Built>,
}

/// One sealed entry as read: its projection, its ordinal and message, and the texts delivered
/// with its calls, which give their rides' envelopes.
struct SealedEntry {
    entry: cortexkit_role_compaction::read::Entry,
    message: (u64, broca_protocol::Message),
    delivered: Vec<Delivered>,
}

/// A transcript as built for `key`: the sealed entries its views name, then the window's.
struct Built {
    key: BuiltFor,
    /// How many of `entries` are sealed ones, in front.
    sealed: usize,
    entries: Arc<Vec<cortexkit_role_compaction::read::Entry>>,
    messages: Arc<Vec<(u64, broca_protocol::Message)>>,
    /// The sealed entries' delivered texts, by message id, for their ride envelopes.
    delivered: Arc<Vec<(String, Vec<Delivered>)>>,
}

/// What decides which sealed entries a transcript holds: where the window starts, and the
/// applied and accepted views, each by version and checksum.
#[derive(Clone, PartialEq)]
struct BuiltFor {
    window_start: u64,
    views: [Option<(u64, String)>; 2],
}

/// The session's [`SealedTranscript`], kept by the writer's session; a new one when it keeps
/// none, or keeps nothing at all.
async fn sealed_transcript<W: Writer>(writer: &W) -> Result<Arc<SealedTranscript>, LoopError> {
    if let Some(kept) = writer.sealed_derived().await? {
        if let Ok(kept) = kept.downcast::<SealedTranscript>() {
            return Ok(kept);
        }
    }
    let fresh = Arc::new(SealedTranscript::default());
    writer.keep_sealed_derived(fresh.clone()).await?;
    Ok(fresh)
}

/// The transcript a step's compaction reads (see [`Transcript`]). A sealed entry is read back
/// through the session's id lookup, which finds broca's own ids and an import's source ids
/// alike, named entries close together in one read. What it reads is kept for the open session
/// (see [`SealedTranscript`]), so a view naming hundreds of sealed messages reads and projects
/// them once, not at every step. A view's cut target the session doesn't hold fails the step;
/// any other named message it doesn't hold is left out, and the check refuses a view naming it,
/// by that id.
pub async fn transcript<W: Writer>(writer: &W, ctx: &Ctx) -> Result<Transcript, LoopError> {
    use cortexkit_role_compaction::view::{EditOp, InsertAt};
    let conversation = writer.conversation();
    let window = broca_session_store::session::served_window(conversation);
    let window_entries = window
        .iter()
        .map(crate::entries::project)
        .collect::<Result<Vec<_>, _>>()
        .map_err(LoopError::Parts)?;
    let window_start = conversation.window_start();
    // Each entry's rides, sealed ones first, keyed as the entries key their deliveries.
    let rides_with = |sealed: &[(String, Vec<Delivered>)]| {
        let mut rides = v2::entries::RideEnvelopes::default();
        for (mid, delivered) in sealed {
            crate::entries::push_rides(&mut rides, mid, delivered);
        }
        for entry in &window {
            if let Some(facts) = &entry.state.facts {
                crate::entries::push_rides(&mut rides, &facts.mid, &entry.state.delivered);
            }
        }
        Arc::new(rides)
    };
    if window_start <= 1 {
        return Ok(Transcript {
            entries: Arc::new(window_entries),
            sealed: Arc::default(),
            rides: rides_with(&[]),
        });
    }
    let key = {
        let state = ctx.session.state.lock().await;
        BuiltFor {
            window_start,
            views: [state.applied(), state.latest()]
                .map(|view| view.map(|view| (view.version, view.checksum.clone()))),
        }
    };
    let kept = sealed_transcript(writer).await?;
    {
        let mut parts = kept.0.lock().expect("sealed transcript");
        if let Some(built) = parts.built.as_mut().filter(|built| built.key == key) {
            // The entries the last transcript was built from are kept whole; once that
            // transcript is gone, only the window's entries behind the sealed ones change.
            let entries = Arc::make_mut(&mut built.entries);
            entries.truncate(built.sealed);
            entries.extend(window_entries);
            return Ok(Transcript {
                entries: built.entries.clone(),
                sealed: built.messages.clone(),
                rides: rides_with(&built.delivered),
            });
        }
    }
    // Every message the views name, each once: the cut targets, and the rest.
    let (cuts, named) = {
        let state = ctx.session.state.lock().await;
        let mut cuts = std::collections::BTreeSet::new();
        let mut named = std::collections::BTreeSet::new();
        for edit in [state.applied(), state.latest()]
            .into_iter()
            .flatten()
            .flat_map(|view| &view.edits)
        {
            match &edit.op {
                EditOp::Cut { through } => {
                    cuts.insert(through.clone());
                }
                EditOp::Drop { mid, .. }
                | EditOp::Replace { mid, .. }
                | EditOp::Insert {
                    at: InsertAt::Before(mid),
                    ..
                } => {
                    named.insert(mid.clone());
                }
                _ => {}
            }
        }
        (cuts, named)
    };
    let in_window: std::collections::HashSet<&str> = window
        .iter()
        .filter_map(|entry| entry.state.facts.as_ref())
        .map(|facts| facts.mid.as_str())
        .collect();
    let wanted: Vec<&String> = cuts
        .union(&named)
        .filter(|mid| !in_window.contains(mid.as_str()))
        .collect();
    let missing: Vec<String> = {
        let parts = kept.0.lock().expect("sealed transcript");
        wanted
            .iter()
            .filter(|mid| !parts.by_mid.contains_key(mid.as_str()))
            .map(|mid| (*mid).clone())
            .collect()
    };
    let mut read = Vec::new();
    for stored in writer.entries_by_mid(&missing).await? {
        if stored.ordinal >= window_start {
            continue;
        }
        let Some(mid) = stored.state.facts.as_ref().map(|facts| facts.mid.clone()) else {
            continue;
        };
        let entry = crate::entries::project(&stored).map_err(LoopError::Parts)?;
        read.push((
            mid,
            SealedEntry {
                entry,
                message: (stored.ordinal, stored.message),
                delivered: stored.state.delivered,
            },
        ));
    }
    let mut parts = kept.0.lock().expect("sealed transcript");
    parts.by_mid.extend(read);
    if let Some(mid) = cuts
        .iter()
        .find(|mid| !in_window.contains(mid.as_str()) && !parts.by_mid.contains_key(*mid))
    {
        return Err(LoopError::Parts(format!(
            "a compaction view cuts through {mid}, which the session doesn't hold"
        )));
    }
    let mut sealed: Vec<_> = wanted
        .iter()
        .filter_map(|mid| parts.by_mid.get(mid.as_str()))
        .collect();
    sealed.sort_by_key(|named| named.message.0);
    sealed.dedup_by_key(|named| named.message.0);
    let mut entries: Vec<_> = sealed.iter().map(|named| named.entry.clone()).collect();
    let messages: Vec<_> = sealed.iter().map(|named| named.message.clone()).collect();
    let delivered: Vec<_> = sealed
        .iter()
        .map(|named| (named.entry.mid.clone(), named.delivered.clone()))
        .collect();
    let count = entries.len();
    entries.extend(window_entries);
    let built = Built {
        key,
        sealed: count,
        entries: Arc::new(entries),
        messages: Arc::new(messages),
        delivered: Arc::new(delivered),
    };
    let transcript = Transcript {
        entries: built.entries.clone(),
        sealed: built.messages.clone(),
        rides: rides_with(&built.delivered),
    };
    parts.built = Some(built);
    Ok(transcript)
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
    if writer.operations().view_applied.is_none() {
        return Err(LoopError::Parts(
            "a cut was marked with no view applied".into(),
        ));
    }
    let carry = carried_views(&*ctx.session.state.lock().await, writer.operations())?;
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

/// The request step `step` sent, rebuilt from stored state without calling the compaction
/// provider: the views the session recorded replace its answers. The compaction state is restored
/// from the open log, and the prompt is rendered from the view the session recorded as applied for
/// that step, by the same render and finish as a live send's (see `prepare`). For checking requests
/// rebuilt from stored sessions against the bytes that were sent.
pub async fn offline_prompt<W: Writer>(
    writer: &W,
    admission: &Admission,
    bind: &BindIdentity,
    step: u64,
    opts: broca_protocol::request::CallOptions,
) -> Result<broca_protocol::request::CallOptions, LoopError> {
    let records = writer.open_log_records().await?;
    let state = restore(&records, bind, writer.operations()).map_err(LoopError::Parts)?;
    let view = state.view_for_step(step).cloned();
    let session = Arc::new(v2::session::Session::default());
    *session.state.lock().await = state;
    let ctx = Ctx {
        session,
        provider: String::new(),
        preset: String::new(),
        earlier: None,
    };
    let transcript = transcript(writer, &ctx).await?;
    let opts = with_sealed(opts, &transcript);
    let prompt = v2::render::prompt_with_rides(
        &opts.prompt,
        &transcript.entries,
        &transcript.rides,
        view.as_ref().map_or(&[][..], |view| view.edits.as_slice()),
        |_| Err("a compaction view's inserted images aren't stored yet".into()),
    )
    .map_err(LoopError::Parts)?;
    finish(&opts, prompt, admission).map_err(LoopError::Parts)
}

/// Seals the session's ledger with no cut, carrying copies of its compaction views, as a seal
/// must (see `Carry::records`). `session` is the session's compaction state, as the actor holds
/// it.
pub async fn seal_ledger<W: Writer>(
    writer: &mut W,
    session: &v2::session::Session,
    now_ms: u64,
) -> Result<(), LoopError> {
    let carry = carried_views(&*session.state.lock().await, writer.operations())?;
    writer
        .seal_ledger(
            &broca_session_store::session::Carry {
                records: &carry,
                window_repetition: None,
            },
            now_ms,
        )
        .await?;
    Ok(())
}

/// Whole copies of the views a seal must carry into the new log: the applied view and a
/// `ViewApplied` naming it for the step that applied it, then the accepted view when it's a
/// newer one, stored as its change against the applied view carried just before it. Empty when
/// the session names no view.
pub fn carried_views(state: &State, operations: &Operations) -> Result<Vec<Record>, LoopError> {
    let record = |previous: Option<&ReceivedView>, view: &ReceivedView| {
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
    let mut carry = Vec::new();
    let applied = match (state.applied_received(), &operations.view_applied) {
        (Some(applied), Some(slot)) => {
            carry.push(record(None, applied)?);
            carry.push(Record::ViewApplied(ViewApplied {
                step_id: slot.step_id,
                version: applied.view().version,
                checksum: applied.view().checksum.clone(),
            }));
            Some(applied)
        }
        (None, None) => None,
        _ => {
            return Err(LoopError::Parts(
                "the compaction state and the store disagree on the applied view".into(),
            ))
        }
    };
    if let Some(latest) = state.latest_received().filter(|latest| {
        applied.is_none_or(|applied| latest.view().version != applied.view().version)
    }) {
        carry.push(record(applied, latest)?);
    }
    Ok(carry)
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
    use broca_core::run::RepetitionCheckpointTaken as Taken;
    match broca_core::run::repetition_checkpoint(steps, seed.as_ref()) {
        Taken::Taken(checkpoint) => serde_json::to_value(checkpoint)
            .map(Some)
            .map_err(|error| LoopError::Parts(error.to_string())),
        Taken::CallsWaiting => Err(LoopError::Unsupported(
            "a compaction cut between a tool call and its result",
        )),
        Taken::SeedRejected => Err(LoopError::Parts(
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
        variant: admission.variant.as_deref(),
        service_tier: admission.service_tier,
        frozen_render_config: &admission.render_config,
        tools: &admission.tools,
        // A planned run's system text is its plan's, which the send context reads from the
        // manifest; only an unplanned run's own system text counts here, as today's config holds.
        global_system: (admission.manifest.is_none() && !admission.global_system.is_empty())
            .then_some(admission.global_system.as_slice()),
        auth_selection: admission.auth_selection.as_ref(),
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
    // The last serving model's fill isn't measured on this storage yet, so the newest entry's
    // own reported prompt stands for the session's fill.
    let conditions = match v2::host::conditions_of(&frozen(admission), &entries, false, None) {
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
    let transcript_rides = transcript.rides;
    let now = (seams.now_ms)();
    let last_cache = writer.operations().last_cache_send.clone();
    let chained = seams.provider.history_edit_cost()
        == Some(broca_core::compaction_provider::notice::HistoryEditCost::FullResend);
    let conditions = match v2::host::conditions_of(&frozen(admission), &entries, chained, None) {
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
        scope: admission.send.scope.as_ref(),
    };
    let journal = StoreJournal::new(writer, &seams.bind);
    // A ride renders by its envelope, as today's loop renders a ride recorded with one; text
    // delivered before envelopes existed renders the way it always did.
    let rides = Arc::clone(&transcript_rides);
    let prepared = v2::request::prepare(
        &ctx.session.state,
        &ctx.session.runtime,
        &journal,
        &plane,
        v2::request::At {
            step,
            preset: &ctx.preset,
            entries: &entries,
            rides: &rides,
            stored: &opts.prompt,
            conditions: &conditions,
            previous: previous.as_ref(),
            current: &current,
            // Expiry counts from the last request that reached the cache, by the lifetime that
            // request asked the provider for; it's claimed only on a run whose own requests ask
            // for one, as in today's loop.
            cache_expired: v2::conditions::cache_proven_expired(
                last_cache.as_ref().and_then(|send| send.requested_ttl_ms),
                last_cache.as_ref().map(|send| send.sent_at_ms),
                now,
            ),
            detects_cache_expired: crate::run::requested_cache_ttl(admission).is_some(),
            chain_reset: chained && seams.provider.prepare_chain_reset(),
            now,
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
        v2::request::Prepared::ContextCeiling {
            request_tokens,
            usable_window,
        } => serde_json::to_value(broca_protocol::ContextCeilingDetail::new(
            usable_window,
            request_tokens,
            "over_window",
        ))
        .map(Send::Ceiling)
        .map_err(|error| LoopError::Parts(error.to_string())),
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
    let conditions = v2::host::conditions_of(&frozen(admission), &entries, false, None)
        .map_err(LoopError::Parts)?;
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
                scope: admission.send.scope.as_ref(),
            },
            &setup,
            &conditions,
            applied.as_ref(),
        )
        .await;
    Ok(())
}

/// What a run tells its session's compaction provider about the session's changes outside its
/// own steps: once the run's end is durable, once a tool batch's results are, and once a call a
/// provider held has its result. Those are the points today's actor has today's loop tell it,
/// through `RunControl::compaction_changes` (`compaction_v2::host::notice`). Like today's
/// notice, it writes nothing, and one that can't be sent fails nothing: the provider hears of
/// the same changes with the next notice.
pub struct Changes<'a> {
    admission: &'a Admission,
    ctx: Ctx,
}

impl<'a> Changes<'a> {
    /// The changes a run of `admission` tells its compaction provider about, when the seams ask
    /// for them (`Seams::compaction_changes`) and the run is on the current compaction contract
    /// under a plan that names a compaction provider with a preset. They go through the
    /// session's own compaction state (`Seams::compaction_session`), the one the run's steps
    /// use, so the provider hears each change once.
    pub fn of<P, T>(admission: &'a Admission, seams: &Seams<'_, P, T>) -> Option<Self> {
        if !seams.compaction_changes || admission.compaction_plan_version.is_none() {
            return None;
        }
        let manifest = admission.manifest.as_deref()?;
        let (provider, preset, _, _) = broca_core::compaction::item(manifest).ok().flatten()?;
        Some(Self {
            admission,
            ctx: Ctx {
                session: seams.compaction_session.clone()?,
                provider,
                preset: preset?,
                earlier: None,
            },
        })
    }

    /// Tells the provider of every change since its last notice. A failure is left for the next
    /// notice to make up, as `compaction_v2::host::notice` leaves its own.
    pub async fn tell<W: Writer, P, T>(&self, writer: &W, seams: &Seams<'_, P, T>) {
        let _ = notice(writer, self.admission, seams, &self.ctx).await;
    }
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
