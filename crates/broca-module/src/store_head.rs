//! `session.head` for a session kept in the session store.
//!
//! The head is read from the session's head, its folds and the setup parts its run starts
//! name, through a read-only load (`StoreRuntime::view`): no lease and no actor. The reply
//! uses the store's incarnation and history version; it has no subscribe cursor until
//! subscriptions are served from the store.
//!
//! Where each member comes from:
//! - the model: the latest run start's model and variant, unless the session's plan chooses
//!   one (`broca_session_loop::plan::selection`): the latest refresh that chooses one, else
//!   the first run's plan;
//! - the sender: the rule [`broca_core::head::head_sender`] applies over the owner summary
//!   the run starts folded and the senders the session admits
//!   (`broca_session_loop::plan::senders`): the latest accepted refresh's, else the first
//!   run's plan's;
//! - the usage: the session's usage by model, which the operational fold sums step by step;
//! - the title: the newest title any send carried, which the operational fold keeps;
//! - the latest run's state: as `session.read` reports it, except that a run still running
//!   when no live actor holds the session reads as interrupted. The store actor owns a
//!   session for as long as a run runs, so such a run was left by a process that stopped
//!   without ending it, and opening the session ends it interrupted;
//! - the input profile: from the latest run's frozen render config or the catalog;
//! - `updated_at`: the newest time a record of the open log carries.

use broca_core::{SessionReadLineageState, SessionReadReason, SessionReadStateKind};
use broca_protocol::{BindIdentity, BindPrincipal, RunFinishReason, SendScope};
use broca_session_loop::parts::Parts;
use broca_session_store::backend::SessionBackend;
use broca_session_store::idindex::IdKind;
use broca_session_store::record::{PartKind, Record, RunStart};
use broca_session_store::session::{Session, SessionError};
use cortexkit_role_llm_runner::delivery_order::PlanSender;
use cortexkit_role_llm_runner::head_selection::PlanModel;
use cortexkit_role_llm_runner::read::{HeadInput, HeadModel, HeadSender, LastRunError};
use cortexkit_role_llm_runner::usage::SessionUsage;

/// Where a head's `input` comes from. Only the serving module can quote the catalog, so the
/// head built from the session names the source and the module resolves it.
#[derive(Debug, Clone, PartialEq)]
pub enum InputSource {
    /// The session has no run yet, so nothing says what a send could carry.
    Unknown,
    /// The latest run has ended, so the selected model's media profile in the catalog applies.
    Catalog,
    /// The latest run's frozen render config's `media` member, absent when the run froze none.
    Frozen(Option<serde_json::Value>),
}

/// The input source of a session whose latest run is in `last_run_state` (`None` without a
/// run), by this rule: an ended run quotes the catalog, any other reads what the run froze.
/// `frozen_media` is read only when the rule needs it.
pub fn input_source<E>(
    last_run_state: Option<&str>,
    frozen_media: impl FnOnce() -> Result<Option<serde_json::Value>, E>,
) -> Result<InputSource, E> {
    Ok(match last_run_state {
        None => InputSource::Unknown,
        Some(state) if crate::registry::media_head::quotes_catalog(state) => InputSource::Catalog,
        Some(_) => InputSource::Frozen(frozen_media()?),
    })
}

/// The retry facts `session.head` serves beside a run that ended in an error, from the error its
/// end recorded.
pub fn last_run_error(error: &broca_protocol::ProviderError) -> LastRunError {
    let shared = broca_protocol::caller::SharedProviderError::from(error.clone());
    let mut summary = LastRunError::new(shared.class_wire());
    summary.retry_after_secs = error
        .retry_after_secs
        .and_then(cortexkit_role_llm_runner::read::RetryAfterSecs::new);
    summary.resets_at_ms = error.resets_at_ms;
    summary.provider_code = shared.provider_code.clone();
    summary
}

/// Why a store session's head couldn't be built.
#[derive(Debug)]
pub enum StoreHeadError {
    /// The session's newest message couldn't be read.
    Read(crate::store_read::StoreReadError),
    /// A lookup in the session failed.
    Session(SessionError),
    /// A run start's setup parts couldn't be read back.
    Setup(String),
    /// The stored owner summary couldn't be converted to the `SessionOwnershipV1` the head serves.
    Owner(String),
    /// The session has a latest run whose start the fold doesn't hold.
    NoRunStart,
    /// No record of the session's open log, nor its latest run's start, carries a time, so its
    /// last update is unknown. Refused `session_time_unknown`: only a session converted from a
    /// log that recorded no time can be one.
    NoTime,
}

/// A session's head as the session alone decides it.
#[derive(Clone)]
pub struct StoreHead {
    /// The reply, without what only the serving module knows: `input` (see [`Self::input`]),
    /// `cache`, and `sender`, which depends on the caller (see [`Self::sender`]).
    pub reply: broca_wire::HeadResponse,
    pub input: InputSource,
    ownership: broca_core::SessionOwnershipV1,
    senders: Vec<PlanSender>,
}

/// The delivery modes a head's sender grants are drawn from: every mode the serve layer offers.
pub const DELIVERY_MODES: &[cortexkit_role_llm_runner::delivery_order::Mode] =
    broca_wire::DELIVERY_MODES;

impl StoreHead {
    /// What the head tells `principal`, bound with `scope`, it may send.
    pub fn sender(
        &self,
        principal: Option<&BindPrincipal>,
        scope: &SendScope,
    ) -> Option<HeadSender> {
        broca_core::head::head_sender(
            &self.ownership,
            &self.senders,
            principal,
            scope,
            DELIVERY_MODES,
        )
    }
}

/// What a head says about a session, as either storage decides it. [`head_reply`] turns it into
/// the reply, the same way for both.
pub struct HeadFacts {
    /// The latest run's state, as `session.read` reports it beside a page.
    pub lineage_state: SessionReadLineageState,
    /// How many messages the transcript holds.
    pub prompt_len: u64,
    /// For a run that ended interrupted, the reason its end recorded.
    pub interrupt_reason: Option<String>,
    /// The model the session's plan chose for its runs, which takes the place of the run's.
    pub selection: Option<PlanModel>,
    /// The latest run's model, with the variant its send named, or else the effort it sends by
    /// default.
    pub run_model: Option<HeadModel>,
    /// The Rich UI stamp of the latest run's plan.
    pub rich_ui: Option<broca_protocol::RichUiStamp>,
    pub usage: Option<SessionUsage>,
    /// The newest title a send carried.
    pub title: Option<String>,
    /// Whether the session is on the current compaction contract: whether its latest run froze
    /// a compaction plan version (`Operations::on_contract`).
    pub mode3: bool,
    /// When the session last changed.
    pub updated_at: Option<u64>,
    /// The resume time the latest account-limit pause recorded. The reply shows it only while
    /// the latest run is still paused for an account limit.
    pub resume_not_before_ms: Option<u64>,
}

/// The `session.head` reply `facts` supply the head data assembled here; the store and serving
/// module add the incarnation and history version, the subscribe cursor, the input profile,
/// the cache clock and the caller's sender grants.
pub fn head_reply(facts: HeadFacts) -> broca_wire::HeadResponse {
    let HeadFacts {
        lineage_state: state,
        prompt_len,
        interrupt_reason,
        selection,
        run_model,
        rich_ui,
        usage,
        title,
        mode3,
        updated_at,
        resume_not_before_ms,
    } = facts;
    let paused_for_account = state.state == SessionReadStateKind::Paused
        && state.reason
            == Some(SessionReadReason::Pause(
                broca_protocol::PauseReason::AccountLimit,
            ));
    let service_tier = selection.as_ref().and_then(|selection| {
        selection
            .service_tier
            .filter(|tier| !tier.is_standard())
            .map(|tier| tier.as_str().into())
    });
    let model = match selection {
        Some(selection) => {
            let mut model = HeadModel::new(&selection.provider, &selection.model);
            model.variant = selection.variant;
            // The effort the latest run sent with no variant is what the next send of that same
            // model sends. Another model's default isn't known until a run uses it.
            if model.variant.is_none() {
                model.default_variant = run_model
                    .filter(|run| run.provider == model.provider && run.model == model.model)
                    .and_then(|run| run.default_variant);
            }
            Some(model)
        }
        None => run_model,
    };
    let last_run_error = (state.state == SessionReadStateKind::Error)
        .then(|| state.error.as_ref().map(last_run_error))
        .flatten();
    broca_wire::HeadResponse {
        mode3,
        last_run_rich_ui: rich_ui,
        last_run_error,
        model,
        service_tier,
        usage: usage.filter(|usage| usage.consistent()),
        last_ordinal: (prompt_len > 0).then_some(prompt_len),
        first_ordinal: (prompt_len > 0).then_some(1),
        last_run_state: state.last_run_id.map(|run_id| broca_wire::LastRunState {
            awaits: None,
            run_id: run_id.0,
            state: state.state.as_str().to_owned(),
            reason: interrupt_reason.or_else(|| {
                state
                    .reason
                    .filter(|reason| reason.as_str() != "interrupted")
                    .map(|reason| reason.as_str().to_owned())
            }),
            resume_not_before_ms: resume_not_before_ms.filter(|_| paused_for_account),
        }),
        updated_at,
        title,
        ..Default::default()
    }
}

/// Builds the head of `session`, reading its run starts' setup parts from `parts`. `live` says
/// whether a live actor holds the session: without one, a run the fold still calls running
/// reads as interrupted.
pub async fn build<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    live: bool,
) -> Result<StoreHead, StoreHeadError> {
    build_with_retry(session, parts, live, Default::default(), None).await
}

pub(crate) async fn build_with_retry<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    live: bool,
    labels: crate::store_awaits::Labels,
    retry: Option<&cortexkit_role_llm_runner::awaits::RunAwaits>,
) -> Result<StoreHead, StoreHeadError> {
    let operations = &session.operations;
    let page = crate::store_read::read_page(session, None, Some(1))
        .await
        .map_err(StoreHeadError::Read)?
        .page;
    let mut state = page.lineage_state;
    if state.state == SessionReadStateKind::Active && !live {
        state.state = SessionReadStateKind::Interrupted;
        state.reason = Some(SessionReadReason::Terminal(RunFinishReason::Interrupted));
    }
    let latest = operations.run.as_ref();
    let latest_start = latest
        .map(|run| run.start.as_ref().ok_or(StoreHeadError::NoRunStart))
        .transpose()?;
    let latest_manifest = latest_start
        .map(|latest| frozen_manifest(latest, parts))
        .transpose()?
        .flatten();
    let senders = broca_session_loop::plan::sender_manifest(operations, parts)
        .map_err(StoreHeadError::Setup)?
        .as_deref()
        .map(broca_core::head::head_senders)
        .unwrap_or_default();
    let selection =
        broca_session_loop::plan::selection(operations, parts).map_err(StoreHeadError::Setup)?;
    let ownership =
        broca_session_loop::owner::ownership(&operations.owner).map_err(StoreHeadError::Owner)?;
    let interrupt_reason = match (latest, state.state) {
        (Some(run), SessionReadStateKind::Interrupted)
            if run.finished == Some(RunFinishReason::Interrupted) =>
        {
            recorded_interrupt_reason(session, &run.run_id).await?
        }
        _ => None,
    };
    let input = input_source(
        state.last_run_id.as_ref().map(|_| state.state.as_str()),
        || match (latest, latest_start) {
            (Some(run), Some(start)) => frozen_media(run, start, parts),
            _ => Ok(None),
        },
    )?;
    // A known open-log time includes the authoritative frozen time of a move. The latest
    // run may belong to the converted prefix, so use its time only when the log has none;
    // taking their maximum would let a pre-move clock override the head the move preserved.
    let updated_at = session
        .open_log_activity_ms()
        .or_else(|| latest_start.and_then(|start| start.written_at_ms.or(start.ts_ms)))
        .ok_or(StoreHeadError::NoTime)?;
    let mut latest_model = latest_start.map(run_model);
    if let (Some(model), Some(run), Some(start)) = (&mut latest_model, latest, latest_start) {
        if model.variant.is_none() {
            model.default_variant =
                read_render_config(run, start, parts, default_effort)?.flatten();
        }
    }
    let mut reply = head_reply(HeadFacts {
        lineage_state: state,
        prompt_len: page.prompt_len,
        interrupt_reason,
        selection,
        run_model: latest_model,
        rich_ui: latest_manifest.and_then(|manifest| manifest.rich_ui.clone()),
        usage: crate::store_read::session_usage(operations.session_usage.as_ref()),
        title: operations.title.clone(),
        // Mode 3 is the session being on the current compaction contract, read from its latest
        // run as every reader in the session store reads it (`Operations::on_contract`): a
        // session that moved onto it serves as mode 3 from that run on.
        mode3: operations.on_contract(),
        updated_at: Some(updated_at),
        resume_not_before_ms: latest
            .and_then(|run| run.account_limit_pause.as_ref())
            .and_then(|pause| pause.get("resume_not_before_ms"))
            .and_then(serde_json::Value::as_u64),
    });
    if let Some(last) = &mut reply.last_run_state {
        let finished = latest
            .filter(|run| run.run_id == last.run_id)
            .is_some_and(|run| run.finished.is_some());
        if finished {
            // A finished run waits on nothing but what its end says, so only its end is read,
            // through the id index: walking a long run's records, such as an import's seeded run
            // that spans the whole imported history, took seconds.
            let mut wait = crate::store_awaits::RunWait::default();
            for (_, record) in session
                .find_id(IdKind::RunId, &last.run_id)
                .await
                .map_err(StoreHeadError::Session)?
            {
                if matches!(record, Record::RunEnd(_)) {
                    wait.record(&record).map_err(StoreHeadError::Setup)?;
                }
            }
            wait.set_labels(labels);
            last.awaits = wait
                .snapshot(crate::store_awaits::now_ms(), retry, None, None)
                .awaits;
        } else if let Some(mut facts) =
            crate::store_status::run_facts(session, parts, &last.run_id, live, None)
                .await
                .map_err(|error| {
                    StoreHeadError::Setup(format!("the latest run's wait: {error:?}"))
                })?
        {
            facts.wait.set_labels(labels);
            let wait = facts.wait.snapshot(
                crate::store_awaits::now_ms(),
                retry,
                facts
                    .state
                    .ceiling_deadline_ms
                    .or(facts.state.ride_wait_deadline_ms),
                None,
            );
            last.awaits = wait.awaits;
            if matches!(
                facts.state.state,
                broca_core::RunIndexState::Active | broca_core::RunIndexState::Paused
            ) {
                last.state = wait.state.into();
                last.reason = wait.reason;
            }
        }
    }
    let head = session.head();
    reply.history_version = crate::store_exposure::served_history_version(head.history_version);
    reply.incarnation = broca_session_store::codec::hex(&head.incarnation.0);
    Ok(StoreHead {
        reply,
        input,
        ownership,
        senders,
    })
}

/// Serves `session.head` for the session `key`, kept in the session store: the head built from a
/// read-only load of the session, the input profile it names, the session's prompt-cache clock,
/// and what the caller (`principal`, bound with `scope`) may send.
pub(crate) async fn serve_head(
    registry: &crate::registry::LineageRegistry,
    store: &crate::store_open::StoreRuntime,
    key: &BindIdentity,
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
) -> Result<broca_wire::HeadResponse, crate::serve::ServeError> {
    use crate::store_reads::{StoreReadReply, StoreReadRequest};
    let addr = broca_core::session_addr(key);
    let closed = store.closed_position(registry, key).await;
    let built = match closed.and_then(|position| store.heads().get(&addr, position)) {
        Some(built) => built,
        None => {
            let built = match store.read(registry, key, StoreReadRequest::Head).await? {
                Some(StoreReadReply::Head(built)) => *built?,
                Some(_) => return Err(crate::store_reads::mismatched()),
                // The session's creation never committed, so it holds nothing: this is the
                // answer for a session never written.
                None => return Ok(broca_wire::HeadResponse::default()),
            };
            // Kept only when the session is still where it was before the read, so the head
            // is the one that position holds, built from the session as stored.
            if let Some(position) = closed {
                if store.closed_position(registry, key).await == Some(position) {
                    store.heads().keep(addr, position, &built);
                }
            }
            built
        }
    };
    let sender = built.sender(principal, scope);
    let mut reply = built.reply;
    reply.sender = sender;
    reply.input = Some(match built.input {
        InputSource::Unknown => HeadInput::new("unknown"),
        InputSource::Catalog => registry.quoted_media_head(reply.model.as_ref()).await,
        InputSource::Frozen(media) => crate::registry::media_head::frozen_input(media),
    });
    reply.cache = registry
        .cache_clock_reading(key)
        .map(|reading| broca_wire::HeadCacheWire {
            last_sent_at_ms: reading.last_sent_at_ms,
            ttl_ms: reading.ttl_ms,
            expires_at_ms: reading.expires_at_ms(),
            prefix_tokens: reading.prefix_tokens,
            expiry_basis: broca_wire::EXPIRY_BASIS_ESTIMATED.into(),
        });
    Ok(reply)
}

/// The heads of sessions no actor holds, by session address, each with the position it was built
/// at. A session left at a position keeps the same head, and apps ask for the heads of the same
/// closed sessions again and again, while building one loads the session's whole view, a full
/// read of its log: after one restart, every closed-session view broca loaded was for a head, 142
/// MB of them kept. So the head is kept instead. Only a head that stays true while the session
/// stays put is kept: one whose latest run has ended and waits on nothing, or that has no run (a
/// running or paused run's head reports waits that change with time). At most [`HEADS_KEPT`]
/// sessions, the oldest forgotten first.
#[derive(Default)]
pub(crate) struct KeptHeads(std::sync::Mutex<KeptHeadsInner>);

const HEADS_KEPT: usize = 4096;

#[derive(Default)]
struct KeptHeadsInner {
    heads: std::collections::HashMap<String, (broca_session_store::session::Position, StoreHead)>,
    order: std::collections::VecDeque<String>,
}

impl KeptHeads {
    fn lock(&self) -> std::sync::MutexGuard<'_, KeptHeadsInner> {
        self.0
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// The head kept for the session at `addr`, when it was built at `position`.
    pub(crate) fn get(
        &self,
        addr: &str,
        position: broca_session_store::session::Position,
    ) -> Option<StoreHead> {
        self.lock()
            .heads
            .get(addr)
            .filter(|(at, _)| *at == position)
            .map(|(_, head)| head.clone())
    }

    /// Keeps `head`, built at `position` for the session at `addr`, when no clock changes it.
    pub(crate) fn keep(
        &self,
        addr: String,
        position: broca_session_store::session::Position,
        head: &StoreHead,
    ) {
        let settled = head.reply.last_run_state.as_ref().is_none_or(|last| {
            last.awaits.is_none()
                && last.resume_not_before_ms.is_none()
                && !matches!(last.state.as_str(), "active" | "paused")
        });
        if !settled {
            return;
        }
        let mut kept = self.lock();
        if kept
            .heads
            .insert(addr.clone(), (position, head.clone()))
            .is_none()
        {
            kept.order.push_back(addr);
        }
        while kept.order.len() > HEADS_KEPT {
            if let Some(oldest) = kept.order.pop_front() {
                kept.heads.remove(&oldest);
            }
        }
    }
}

/// A head that couldn't be built. Only a backend failure is worth a retry: anything else is the
/// session's stored bytes or state being wrong, which reading again won't change.
pub(crate) fn head_failed(error: StoreHeadError) -> crate::serve::ServeError {
    use broca_protocol::ErrorClass;
    let class =
        match &error {
            StoreHeadError::Session(SessionError::Backend(_))
            | StoreHeadError::Read(crate::store_read::StoreReadError::Session(
                SessionError::Backend(_),
            )) => ErrorClass::Transient,
            StoreHeadError::NoTime => return crate::serve::ServeError::Open(
                "session_time_unknown: no record of the session carries a time, so session.head \
                 can't say when it last changed"
                    .into(),
                Some(ErrorClass::Permanent),
            ),
            _ => ErrorClass::Permanent,
        };
    crate::serve::ServeError::Open(
        format!("session.head could not be read from the new storage: {error:?}"),
        Some(class),
    )
}

/// The plan manifest `start` froze, from the setup parts it names; `None` without a plan.
fn frozen_manifest(
    start: &RunStart,
    parts: &dyn Parts,
) -> Result<Option<std::sync::Arc<broca_records::record::PlanManifest>>, StoreHeadError> {
    broca_session_loop::run::frozen_manifest(start, parts)
        .map_err(|error| StoreHeadError::Setup(format!("run {}: {error:?}", start.run_id)))
}

/// The model a run start ran, with the variant its send named, read from the run's config: an
/// empty variant is none.
fn run_model(start: &RunStart) -> HeadModel {
    let mut model = HeadModel::new(&start.model.provider_module_id, &start.model.model_id);
    model.variant = start
        .run_choices
        .as_ref()
        .and_then(|choices| choices.get("config"))
        .and_then(|config| config.get("variant"))
        .and_then(|variant| variant.as_str())
        .filter(|variant| !variant.is_empty())
        .map(str::to_owned);
    model
}

/// The `media` member of the render config the latest run renders with.
fn frozen_media(
    run: &broca_session_store::fold::operations::Run,
    start: &RunStart,
    parts: &dyn Parts,
) -> Result<Option<serde_json::Value>, StoreHeadError> {
    Ok(read_render_config(run, start, parts, |config| config.get("media").cloned())?.flatten())
}

/// `read` of the render config the latest run renders with: the one a resume refroze, else the
/// one its start froze. `None` when the run froze none.
fn read_render_config<T>(
    run: &broca_session_store::fold::operations::Run,
    start: &RunStart,
    parts: &dyn Parts,
    read: impl FnOnce(&serde_json::Value) -> T,
) -> Result<Option<T>, StoreHeadError> {
    if let Some(refrozen) = &run.refrozen_render {
        return Ok(refrozen.get("render_config").map(read));
    }
    let Some(part) = start
        .setup
        .iter()
        .find(|part| part.kind == PartKind::RenderConfig)
    else {
        return Ok(None);
    };
    let config = broca_session_loop::parts::shared::<serde_json::Value, _>(parts, part)
        .map_err(|error| StoreHeadError::Setup(format!("run {}: {error}", start.run_id)))?;
    let config = start
        .render_config(config)
        .map_err(|error| StoreHeadError::Setup(error.to_string()))?;
    Ok(Some(read(&config)))
}

/// The effort a run sends when its send named no variant, read from its frozen render config:
/// the model's documented default, recorded there as a variant (only on the OpenAI families, see
/// `broca_catalog`'s `apply_default_effort`), else an effort the model itself froze, such as a
/// ChatGPT pro model's. `None` when the run sends no effort, as a Claude or Gemini run that named
/// none: the vendor then applies its own default, which broca can't vouch for.
fn default_effort(config: &serde_json::Value) -> Option<String> {
    config
        .pointer("/variant/name")
        .or_else(|| config.get("reasoning_effort"))
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

/// The reason the end of the run `run_id` recorded for interrupting it, read through the id
/// index, so a run whose records were sealed is found too.
async fn recorded_interrupt_reason<B: SessionBackend>(
    session: &Session<B>,
    run_id: &str,
) -> Result<Option<String>, StoreHeadError> {
    let records = session
        .find_id(IdKind::RunId, run_id)
        .await
        .map_err(StoreHeadError::Session)?;
    // The head takes the reason of the run's last interrupted end, even when that end recorded
    // none.
    Ok(records
        .into_iter()
        .rev()
        .find_map(|(_, record)| match record {
            Record::RunEnd(end) if end.reason == RunFinishReason::Interrupted => {
                Some(end.interrupt_reason)
            }
            _ => None,
        })
        .flatten())
}

#[cfg(test)]
#[path = "store_head_tests.rs"]
mod tests;
