//! Filesystem participant for moving a set of sessions kept in the session store. The service
//! bridge supplies sealed door data and contract replies; this module owns artifacts.
use crate::{
    cut_state::{refusal, CutArtifacts, CutGuard, CutState},
    registry::{LineageRegistry, MoveSessionGuard},
};
use broca_core::{FileLeaseStore, LeaseHandle, LeaseStore};
use broca_protocol::BindIdentity;
use broca_wal::migration::{self, Freeze, Moved};
use broca_wire::agent_data::{errors::ErrorBody, refs::Refs, request::Request, Op};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap},
    path::PathBuf,
    sync::{Arc, Mutex},
};

#[derive(Clone)]
pub struct PlannedSession {
    /// Where a source cut appends its newest export in the device-local stream copy.
    /// Receiver plans already name a sealed prefix and leave this absent.
    pub store_prefix: Option<u64>,
    pub reference: String,
    pub session: BindIdentity,
    pub bytes: Vec<u8>,
    pub end_offset: u64,
    pub digest: String,
    /// Mandatory for import and activation. A read-only divergence plan may use
    /// a held local copy without a verified incoming cut, but cannot report that
    /// the local bytes are a verified prefix of the incoming cut (at_cut).
    pub sealed_digest: Option<String>,
    pub format: String,
    pub lineage_id: String,
    /// Verified archive-member bytes, never an archive container.
    pub local_archive: Option<Vec<u8>>,
    /// Read-only verdict from the same local snapshot used during planning.
    pub divergence: Option<LocalDivergence>,
    /// Safe reason a cut could not be verified for an unknown divergence answer.
    pub divergence_reason: Option<String>,
}

#[derive(Clone, Copy)]
pub enum LocalDivergence {
    AtCut,
    Advanced,
    Unknown,
}
impl LocalDivergence {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::AtCut => "at_cut",
            Self::Advanced => "advanced",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Default)]
pub struct MovePlans {
    pub sessions: Vec<PlannedSession>,
    pub unknown_refs: BTreeSet<String>,
}
impl From<Vec<PlannedSession>> for MovePlans {
    fn from(sessions: Vec<PlannedSession>) -> Self {
        Self {
            sessions,
            unknown_refs: BTreeSet::new(),
        }
    }
}

#[derive(Clone)]
pub struct MoveObservation {
    pub reference: String,
    pub busy_reason: Option<String>,
    pub moving_cut: Option<String>,
    pub moved_holder: Option<String>,
    pub open_actor: bool,
}

/// The service bridge resolves requested session references across live and
/// archived sessions. Import plans derive format and identities from authenticated
/// WAL bytes and require the door's sealed digest before publishing staging.
/// Divergence plans omit absent local sessions; the adapter marks them at_cut
/// because they have no local bytes that the incoming cut could overwrite.
#[async_trait::async_trait]
pub trait MoveDriver: Send + Sync {
    async fn cache_store_stream(
        &self,
        _cut: &str,
        _plan: &PlannedSession,
        _op: Op,
    ) -> Result<(), ErrorBody> {
        Err(unavailable("store stream cache unavailable"))
    }
    async fn verify_store(
        &self,
        _plan: &PlannedSession,
        _staged: &std::path::Path,
    ) -> Result<(), ErrorBody> {
        Err(unavailable("store move verifier is unavailable"))
    }
    async fn plan(&self, cut_id: &str, refs: &Refs, op: Op) -> Result<MovePlans, ErrorBody>;
    async fn export(&self, reference: &str, frozen: &[u8]) -> Result<(), ErrorBody>;
    fn reply(
        &self,
        op: Op,
        plans: &[PlannedSession],
        observations: &[MoveObservation],
    ) -> Result<Value, ErrorBody>;
}
#[derive(Clone)]
struct LockedPlans {
    plans: Vec<PlannedSession>,
    unknown_refs: BTreeSet<String>,
    busy: Option<String>,
    open_actor: bool,
    observations: Vec<MoveObservation>,
}
struct ArtifactGuard {
    _sessions: MoveSessionGuard,
    _leases: Vec<Box<dyn LeaseHandle>>,
    cache: Arc<Mutex<HashMap<String, LockedPlans>>>,
    cut_id: String,
}
impl Drop for ArtifactGuard {
    fn drop(&mut self) {
        self.cache.lock().unwrap().remove(&self.cut_id);
    }
}

pub struct FileCutArtifacts {
    state_root: PathBuf,
    registry: LineageRegistry,
    /// Where a moved-in session's row is written before it is published, so a later move
    /// finds it by its ref.
    refs: Arc<dyn broca_core::SessionRefIndex>,
    leases: FileLeaseStore,
    driver: Arc<dyn MoveDriver>,
    plans: Arc<Mutex<HashMap<String, LockedPlans>>>,
}

pub mod store;

fn unavailable(e: impl std::fmt::Display) -> ErrorBody {
    refusal("unavailable", e.to_string(), Value::Null)
}
pub fn busy_ref(
    reference: &str,
    code: &str,
    reason: &str,
) -> broca_wire::agent_data::errors::BusyRef {
    broca_wire::agent_data::errors::BusyRef::new("broca-session", reference, reason)
        .with_reason_code(code)
}
pub fn busy(refs: Vec<broca_wire::agent_data::errors::BusyRef>, message: &str) -> ErrorBody {
    use broca_wire::agent_data::errors::{BusyDetail, Detail};
    ErrorBody::with_typed_detail(Detail::Busy(BusyDetail::new(refs)), message)
}
fn cut_id(request: &Request) -> Result<String, ErrorBody> {
    request.params()["cutId"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| unavailable("cut request has no cutId"))
}

/// Import already verified these bytes. A later digest change is corrupt local
/// staging. agent-data/v1 stops automatic activation for operator resolution
/// rather than retrying or returning the claim while another participant may
/// already be active.
pub fn staging_digest_failure(
    cut_id: &str,
    step: broca_wire::agent_data::step::Step,
    session: &str,
) -> ErrorBody {
    use broca_wire::agent_data::errors::{
        digest_mismatch_reason, driver_action_for, DigestMismatchDetail, RefusalCode,
    };
    let action = driver_action_for(Op::Activate, &RefusalCode::DigestMismatch);
    tracing::error!(
        session,
        cut_id,
        ?step,
        ?action,
        "agent-data verified activation staging is corrupt"
    );
    ErrorBody::with_digest_mismatch_detail(DigestMismatchDetail::new(digest_mismatch_reason::STAGING_DIGEST_MISMATCH),
        format!("session {session}, cut {cut_id} at {step:?}: verified activation staging no longer matches its sealed digest"))
}

pub fn activation_refusal(
    cut_id: &str,
    step: broca_wire::agent_data::step::Step,
    session: &str,
    error: ErrorBody,
) -> ErrorBody {
    if error.code.as_str() == "digest_mismatch" {
        staging_digest_failure(cut_id, step, session)
    } else {
        error
    }
}

/// The refusal of source operation `op` on a session that moved to `holder` at `cut_id`: the ref
/// is no longer this computer's, so the driver stops rather than retries.
pub(super) fn moved_source(plan: &PlannedSession, holder: &str, cut_id: &str, op: Op) -> ErrorBody {
    use broca_wire::agent_data::errors::{Detail, RefusalCode, UnknownRefDetail};
    let code = match op {
        Op::Prepare => RefusalCode::UnknownRef,
        Op::Export => RefusalCode::UnknownRef,
        Op::Release => RefusalCode::UnknownRef,
        _ => unreachable!("only source operations inspect moved ownership"),
    };
    let mut error = ErrorBody::with_typed_detail(
        Detail::UnknownRef(UnknownRefDetail::new("broca-session", &plan.reference)),
        format!(
            "session {} is held by {} after cut {}",
            plan.session.session, holder, cut_id
        ),
    );
    error.code = code;
    error
}

fn foreign_owner(
    plan: &PlannedSession,
    cut_id: &str,
    owner_cut: &str,
    op: Op,
    step: broca_wire::agent_data::step::Step,
) -> ErrorBody {
    use broca_wire::agent_data::errors::{Detail, InvalidStateDetail};
    if matches!(op, Op::Export | Op::Release) {
        // A prepared or exported cut retains its exclusive freeze until release
        // or abort. Conflicting ownership is corrupt authority state, not
        // retryable work. The agent-data/v1 refusal table disallows busy for
        // export/release; this inconsistency is terminal invalid_state instead.
        tracing::error!(session = %plan.session.session, session_ref = %plan.reference,
            cut_id, owner_cut, ?step, "agent-data cut authority contradicts session ownership");
        return ErrorBody::with_typed_detail(
            Detail::InvalidState(InvalidStateDetail::new(step)),
            format!(
                "cut {cut_id} at {step:?} cannot {} session {} owned by cut {owner_cut}",
                op.method(),
                plan.session.session
            ),
        );
    }
    busy(
        vec![busy_ref(
            &plan.reference,
            "moving",
            &format!("session is frozen by cut {owner_cut}"),
        )],
        "session is frozen by another cut",
    )
}
impl FileCutArtifacts {
    pub fn new(
        state_root: PathBuf,
        registry: LineageRegistry,
        refs: Arc<dyn broca_core::SessionRefIndex>,
        driver: Arc<dyn MoveDriver>,
    ) -> Self {
        Self {
            leases: FileLeaseStore::new(state_root.join("leases")),
            state_root,
            registry,
            refs,
            driver,
            plans: Arc::new(Mutex::new(HashMap::new())),
        }
    }
    fn locked(&self, cut_id: &str) -> Result<LockedPlans, ErrorBody> {
        self.plans
            .lock()
            .unwrap()
            .get(cut_id)
            .cloned()
            .ok_or_else(|| unavailable("session critical sections are not held"))
    }
    fn owned_freeze(
        &self,
        cut_id: &str,
        plan: &PlannedSession,
        required: bool,
        op: Op,
        step: broca_wire::agent_data::step::Step,
    ) -> Result<(), ErrorBody> {
        let live = self.move_sidecar(plan);
        if let Some(moved) =
            migration::read_json::<Moved>(&live.with_extension("moved")).map_err(unavailable)?
        {
            return Err(moved_source(plan, &moved.holder, &moved.cut_id, op));
        }
        match migration::read_json::<Freeze>(&live.with_extension("moving")).map_err(unavailable)? {
            Some(freeze) if freeze.cut_id != cut_id => {
                Err(foreign_owner(plan, cut_id, &freeze.cut_id, op, step))
            }
            Some(freeze)
                if freeze.end_offset != plan.end_offset || freeze.lineage_id != plan.lineage_id =>
            {
                Err(unavailable("owned freeze does not match the planned WAL"))
            }
            Some(_) => Ok(()),
            None if required => Err(unavailable("cut has no owned freeze")),
            None => Ok(()),
        }
    }
}

#[async_trait::async_trait]
impl CutArtifacts for FileCutArtifacts {
    fn resolved_sessions(&self, cut_id: &str) -> Result<BTreeMap<String, BindIdentity>, ErrorBody> {
        let locked = self.plans.lock().unwrap();
        let locked = locked
            .get(cut_id)
            .ok_or_else(|| unavailable("session critical sections are not held"))?;
        Ok(locked
            .plans
            .iter()
            .map(|plan| (plan.reference.clone(), plan.session.clone()))
            .collect())
    }
    async fn lock_refs(
        &self,
        cut_id: &str,
        refs: &Refs,
        op: Op,
    ) -> Result<Box<dyn CutGuard>, ErrorBody> {
        let resolved = self.driver.plan(cut_id, refs, op).await?;
        let mut plans = resolved.sessions;
        let references = serde_json::to_value(refs).map_err(unavailable)?;
        let wanted = references
            .as_array()
            .ok_or_else(|| unavailable("invalid ref set"))?;
        if (!matches!(op, Op::Abort | Op::Diverged) && wanted.len() != plans.len())
            && op != Op::Busy
            || (!matches!(op, Op::Abort | Op::Diverged)
                && wanted.iter().any(|r| {
                    !plans
                        .iter()
                        .any(|p| r["ref_kind"] == "broca-session" && r["ref"] == p.reference)
                }))
                && op != Op::Busy
            || plans.iter().any(|p| {
                !wanted
                    .iter()
                    .any(|r| r["ref_kind"] == "broca-session" && r["ref"] == p.reference)
            })
        {
            return Err(unavailable(
                "resolved plans do not cover the entire cut ref set",
            ));
        }
        for plan in &plans {
            if !broca_wire::agent_data::matches_session_ref(
                &plan.reference,
                &plan.session.harness,
                &plan.session.project_root.to_string_lossy(),
                &plan.session.session,
            ) {
                return Err(refusal(
                    "invalid_request",
                    "resolved session does not match its ref",
                    Value::Null,
                ));
            }
        }
        plans = wanted
            .iter()
            .filter_map(|r| plans.iter().find(|p| r["ref"] == p.reference).cloned())
            .collect();
        // Prepare, release and abort publish or remove the source's move markers. For
        // those, an idle actor whose writer owns its tenure closes that writer and hands
        // the still-held session lease to this guard before any marker changes. Every
        // other operation only reads, and inspection never takes a tenure.
        let mode = if matches!(op, Op::Prepare | Op::Release | Op::Abort) {
            crate::registry::CutLockMode::Mutate
        } else {
            crate::registry::CutLockMode::Inspect
        };
        let sessions = self
            .registry
            .lock_move_sessions_with_mode(
                &plans.iter().map(|p| p.session.clone()).collect::<Vec<_>>(),
                false,
                mode,
            )
            .await
            .map_err(unavailable)?;
        let mut busy = sessions.busy.iter().find_map(|(_, reason)| reason.clone());
        // A retired actor's slot stays `Closing` until the guard releases it, but it no
        // longer serves the session: the guard holds its lease instead.
        let actor_open = |session: &BindIdentity| {
            self.registry.slot_state(session).is_some() && !sessions.holds_tenure(session)
        };
        let open_actor = plans.iter().any(|p| actor_open(&p.session));
        let mut leases = Vec::new();
        let mut observations = Vec::new();
        for plan in &plans {
            let live = self.move_sidecar(plan);
            let moving = migration::read_json::<Freeze>(&live.with_extension("moving"))
                .map_err(unavailable)?;
            let moved = migration::read_json::<Moved>(&live.with_extension("moved"))
                .map_err(unavailable)?;
            let mut observation = MoveObservation {
                reference: plan.reference.clone(),
                busy_reason: sessions
                    .busy
                    .iter()
                    .find(|(key, _)| key == &plan.session)
                    .and_then(|(_, reason)| reason.clone()),
                moving_cut: migration::cut_fence(&self.state_root, &plan.reference)
                    .map_err(unavailable)?
                    .or_else(|| moving.map(|f| f.cut_id)),
                moved_holder: moved.map(|m| m.holder),
                open_actor: actor_open(&plan.session),
            };
            if observation.busy_reason.is_none() {
                observation.busy_reason = plan.divergence_reason.clone();
                if busy.is_none() {
                    busy = observation.busy_reason.clone();
                }
            }
            // A session whose lease this guard already holds (handed over by its closed
            // actor) must not be acquired again; one with an open actor keeps its lease
            // with that actor while it is paused.
            if self.registry.slot_state(&plan.session).is_none() {
                match self
                    .leases
                    .acquire(&broca_core::lease_key_for(&plan.session))
                {
                    Ok(lease) => leases.push(lease),
                    Err(broca_core::LeaseError::Held { .. }) => {
                        busy = Some("lease_held".into());
                        observation.busy_reason = Some("lease_held".into());
                    }
                    Err(e) => return Err(unavailable(e)),
                }
            }
            observations.push(observation);
        }
        self.plans.lock().unwrap().insert(
            cut_id.to_owned(),
            LockedPlans {
                plans,
                unknown_refs: resolved.unknown_refs,
                busy,
                open_actor,
                observations,
            },
        );
        Ok(Box::new(ArtifactGuard {
            _sessions: sessions,
            _leases: leases,
            cache: self.plans.clone(),
            cut_id: cut_id.to_owned(),
        }))
    }
    async fn check(&self, request: &Request, state: &CutState) -> Result<(), ErrorBody> {
        let cut_id = cut_id(request)?;
        let locked = self.locked(&cut_id)?;
        let op = request.op();
        if op != Op::Abort {
            if let Some(reason) = locked.busy {
                if matches!(op, Op::Export | Op::Release) {
                    return Err(unavailable(format!(
                        "source session is temporarily busy: {reason}"
                    )));
                }
                return Err(busy(
                    locked
                        .observations
                        .iter()
                        .filter(|o| o.busy_reason.is_some())
                        .map(|o| {
                            busy_ref(
                                &o.reference,
                                o.busy_reason.as_deref().unwrap_or(&reason),
                                "session has pending work",
                            )
                        })
                        .collect(),
                    "session cannot move while work is pending",
                ));
            }
        }
        if matches!(op, Op::Import | Op::Activate) && locked.open_actor {
            return Err(busy(
                locked
                    .observations
                    .iter()
                    .filter(|o| o.open_actor)
                    .map(|o| {
                        busy_ref(
                            &o.reference,
                            "actor_open",
                            "receiver session has an open actor",
                        )
                    })
                    .collect(),
                "receiver session has an open actor",
            ));
        }
        for plan in &locked.plans {
            self.store_check(&cut_id, plan, op, state, request).await?;
        }
        Ok(())
    }
    async fn apply(&self, request: &Request, _state: &CutState) -> Result<Value, ErrorBody> {
        if let Request::Busy(request) = request {
            // Busy has no cut identity or durable state. Reuse the session snapshot
            // locks without acquiring a cut lock or writing any move artifacts.
            static NEXT_READ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
            let key = format!(
                "busy-{}",
                NEXT_READ.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            );
            let _guard = self.lock_refs(&key, &request.refs, Op::Busy).await?;
            let locked = self.locked(&key)?;
            use broca_wire::agent_data::busy::{BusyEntry, BusyReply, BusyState};
            let held: BusyReply = serde_json::from_value(self.driver.reply(
                Op::Busy,
                &locked.plans,
                &locked.observations,
            )?)
            .map_err(unavailable)?;
            if held.refs.len() != locked.plans.len() {
                return Err(unavailable("busy reply does not cover held refs"));
            }
            return serde_json::to_value(BusyReply::new(
                request
                    .refs
                    .iter()
                    .map(|reference| {
                        locked
                            .plans
                            .iter()
                            .position(|plan| plan.reference == reference.r#ref)
                            .map_or_else(
                                || {
                                    BusyEntry::new(BusyState::Unknown)
                                        .with_reason("local session could not be inspected")
                                },
                                |i| held.refs[i].clone(),
                            )
                    })
                    .collect(),
            ))
            .map_err(unavailable);
        }
        let cut_id = cut_id(request)?;
        let locked = self.locked(&cut_id)?;
        let op = request.op();
        for plan in &locked.plans {
            self.store_apply(&cut_id, plan, op, request).await?;
        }
        match op {
            Op::Export => {
                for plan in &locked.plans {
                    self.driver.export(&plan.reference, &plan.bytes).await?;
                }
            }
            Op::Abort => {
                self.rollback(request, _state).await?;
            }
            _ => {}
        }
        let reply = self.driver.reply(op, &locked.plans, &locked.observations)?;
        if let Request::Diverged(request) = request {
            use broca_wire::agent_data::diverged::{DivergedEntry, DivergedReply, DivergedState};
            let held: DivergedReply = serde_json::from_value(reply).map_err(unavailable)?;
            if held.refs.len() != locked.plans.len() {
                return Err(unavailable("divergence reply does not cover held refs"));
            }
            // An absent copy needs neither a fabricated bind triple nor a door
            // read. Preserve the request's order when merging those at_cut entries.
            return serde_json::to_value(DivergedReply::new(
                request
                    .refs
                    .iter()
                    .map(|reference| {
                        if locked.unknown_refs.contains(&reference.r#ref) {
                            return DivergedEntry::new(DivergedState::Unknown);
                        }
                        locked
                            .plans
                            .iter()
                            .position(|plan| plan.reference == reference.r#ref)
                            .map_or_else(
                                || DivergedEntry::new(DivergedState::AtCut),
                                |i| held.refs[i].clone(),
                            )
                    })
                    .collect(),
            ))
            .map_err(unavailable);
        }
        Ok(reply)
    }
    async fn rollback(&self, request: &Request, _state: &CutState) -> Result<(), ErrorBody> {
        let cut_id = cut_id(request)?;
        let locked = self.locked(&cut_id)?;
        for plan in &locked.plans {
            self.store_apply(&cut_id, plan, Op::Abort, request).await?;
            migration::unlink(
                &self
                    .state_root
                    .join("session-store-moves/streams")
                    .join(broca_core::session_addr(&plan.session))
                    .with_extension(&cut_id),
            )
            .map_err(unavailable)?;
            let staged = self.store_staged(&cut_id, plan);
            if staged.exists() {
                std::fs::remove_dir_all(&staged).map_err(unavailable)?;
            }
            migration::abort_freeze(&self.move_sidecar(plan), &cut_id).map_err(unavailable)?;
        }
        Ok(())
    }
    async fn reconcile(
        &self,
        state: &CutState,
        read_only: bool,
    ) -> Result<Option<Value>, ErrorBody> {
        let Some(intent) = &state.intent else {
            return Ok(None);
        };
        let op = match intent.op.as_str() {
            "prepare" => Op::Prepare,
            "export" => Op::Export,
            "release" => Op::Release,
            "import" => Op::Import,
            "activate" => Op::Activate,
            "abort" => Op::Abort,
            _ => return Err(unavailable("unknown cut intent")),
        };
        if read_only || matches!(op, Op::Prepare | Op::Export) {
            return Ok(None);
        }
        let request = Request::decode(op.method(), &intent.request)?;
        let cut_id = cut_id(&request)?;
        let locked = self.locked(&cut_id)?;
        if (op != Op::Abort && locked.busy.is_some())
            || (matches!(op, Op::Import | Op::Activate) && locked.open_actor)
        {
            return Ok(None);
        }
        if op == Op::Import {
            for plan in &locked.plans {
                self.store_apply(&cut_id, plan, Op::Import, &request)
                    .await?;
            }
            return self
                .driver
                .reply(op, &locked.plans, &locked.observations)
                .map(Some);
        }
        self.check(&request, state).await?;
        self.apply(&request, state).await.map(Some)
    }
}
