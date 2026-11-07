//! `session.head` for a session kept in the new session storage.
//!
//! The head is read from the session's head, its folds and the setup parts its run starts name,
//! through a read-only load (`StoreRuntime::view`): no lease, no actor and no WAL. The reply
//! equals the one today's head builds from a WAL for the same history, apart from what each
//! storage keeps for itself: there is no WAL lineage; the incarnation and the history version
//! are the store's; and there is no subscribe cursor until subscriptions are served from the
//! store.
//!
//! Where each member comes from:
//! - the model: the latest run start's model and variant, unless the session's first run froze
//!   a plan that chooses one (`manifest_selection`). The store takes no plan refresh, so no
//!   later plan can change that choice;
//! - the sender: today's rule ([`broca_core::head::head_sender`]) over the owner summary the
//!   run starts folded and the senders of the first run's plan;
//! - the usage: the session's usage by model, which the operational fold sums step by step;
//! - the title: the newest title any send carried, which the operational fold keeps;
//! - the latest run's state: as `session.read` reports it, except that a run still running
//!   when no live actor holds the session reads as interrupted. The store actor owns a session
//!   for as long as a run runs, so such a run was left by a process that stopped without
//!   ending it, and opening the session ends it interrupted;
//! - the input profile: as today's head decides it, from the latest run's frozen render config
//!   or the catalog;
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
/// run), by today's rule: an ended run quotes the catalog, any other reads what the run froze.
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
    /// The latest run's model, with the variant its send named.
    pub run_model: Option<HeadModel>,
    /// The Rich UI stamp of the latest run's plan.
    pub rich_ui: Option<broca_protocol::RichUiStamp>,
    pub usage: Option<SessionUsage>,
    /// The newest title a send carried.
    pub title: Option<String>,
    /// Whether the session's first run froze a compaction plan version.
    pub mode3: bool,
    /// When the session last changed.
    pub updated_at: Option<u64>,
}

/// The `session.head` reply `facts` give, as today's head assembles it, without what each
/// storage or the serving module adds: the incarnation and history version, the subscribe
/// cursor, the input profile, the cache clock and the caller's sender grants.
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
    } = facts;
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
            run_id: run_id.0,
            state: state.state.as_str().to_owned(),
            reason: interrupt_reason.or_else(|| {
                state
                    .reason
                    .filter(|reason| reason.as_str() != "interrupted")
                    .map(|reason| reason.as_str().to_owned())
            }),
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
    let first_start = operations.owner.first_start.as_ref();
    let first_manifest = first_start
        .map(|start| frozen_manifest(start, parts))
        .transpose()?
        .flatten();
    let latest_manifest = match latest_start {
        Some(latest) if first_start.is_some_and(|first| first.run_id == latest.run_id) => {
            first_manifest.clone()
        }
        Some(latest) => frozen_manifest(latest, parts)?,
        None => None,
    };
    let senders = first_manifest
        .as_deref()
        .map(broca_core::head::head_senders)
        .unwrap_or_default();
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
    let updated_at = session
        .open_log_activity_ms()
        .into_iter()
        .chain(latest_start.and_then(|start| start.written_at_ms.or(start.ts_ms)))
        .max()
        .ok_or(StoreHeadError::NoTime)?;
    let mut reply = head_reply(HeadFacts {
        lineage_state: state,
        prompt_len: page.prompt_len,
        interrupt_reason,
        selection: first_manifest
            .as_deref()
            .and_then(broca_core::head::manifest_selection),
        run_model: latest_start.map(run_model),
        rich_ui: latest_manifest.and_then(|manifest| manifest.rich_ui.clone()),
        usage: crate::store_read::session_usage(operations.session_usage.as_ref()),
        title: operations.title.clone(),
        // Today's head reads mode 3 from the session's first run: whether it froze a compaction
        // plan version.
        mode3: first_start.is_some_and(|start| start.compaction_plan_version.is_some()),
        updated_at: Some(updated_at),
    });
    let head = session.head();
    reply.history_version = head.history_version;
    reply.incarnation = broca_session_store::codec::hex(&head.incarnation.0);
    Ok(StoreHead {
        reply,
        input,
        ownership,
        senders,
    })
}

/// Serves `session.head` for the session `key`, kept in the new storage: the head built from a
/// read-only load of the session, the input profile it names, the session's prompt-cache clock,
/// and what the caller (`principal`, bound with `scope`) may send.
pub(crate) async fn serve_head(
    registry: &crate::registry::LineageRegistry,
    store: &crate::store_open::StoreRuntime,
    key: &BindIdentity,
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
) -> Result<broca_wire::HeadResponse, crate::serve::ServeError> {
    let Some(session) = store.view(key).await? else {
        // The session's creation never committed, so it holds nothing: today's answer for a
        // session never written.
        return Ok(broca_wire::HeadResponse::default());
    };
    // The store actor of a session is the registry's open slot for it.
    let live = registry.slot_state(key) == Some("open");
    let built = build(&session, store.parts(), live)
        .await
        .map_err(head_failed)?;
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

/// A head that couldn't be built. Only a backend failure is worth a retry: anything else is the
/// session's stored bytes or state being wrong, which reading again won't change.
fn head_failed(error: StoreHeadError) -> crate::serve::ServeError {
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
) -> Result<Option<std::sync::Arc<broca_wal::PlanManifest>>, StoreHeadError> {
    broca_session_loop::run::frozen_manifest(start, parts)
        .map_err(|error| StoreHeadError::Setup(format!("run {}: {error:?}", start.run_id)))
}

/// The model a run start ran, with the variant its send named, as today's head reads it from
/// the run's config: an empty variant is none.
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

/// The `media` member of the render config the latest run renders with: the one a resume
/// refroze, else the one its start froze.
fn frozen_media(
    run: &broca_session_store::fold::operations::Run,
    start: &RunStart,
    parts: &dyn Parts,
) -> Result<Option<serde_json::Value>, StoreHeadError> {
    if let Some(refrozen) = &run.refrozen_render {
        return Ok(refrozen
            .get("render_config")
            .and_then(|config| config.get("media"))
            .cloned());
    }
    let Some(part) = start
        .setup
        .iter()
        .find(|part| part.kind == PartKind::RenderConfig)
    else {
        return Ok(None);
    };
    let config = parts
        .get(part)
        .map_err(|error| StoreHeadError::Setup(format!("run {}: {error}", start.run_id)))?;
    Ok(config.get("media").cloned())
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
    // Today's head takes the reason of the run's last interrupted end, even when that end
    // recorded none.
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
