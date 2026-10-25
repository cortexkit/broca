//! Filesystem participant for moving a set of session WALs. The service bridge
//! supplies sealed door data and contract replies; this module owns artifacts.
use crate::{
    cut_state::{refusal, CutArtifacts, CutGuard, CutState},
    move_install::{self, Installation},
    registry::{LineageRegistry, MoveSessionGuard},
};
use broca_core::{ExportIndex, FileLeaseStore, FileWalStore, LeaseHandle, LeaseStore};
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
    /// Media needs a transport carrying objects as well as WAL bytes. Until the
    /// bridge supplies that transport, refuse before any cut intent or freeze.
    fn prepare_media(
        &self,
        session: &BindIdentity,
        digests: &BTreeSet<String>,
    ) -> Result<(), ErrorBody> {
        if digests.is_empty() {
            return Ok(());
        }
        Err(refusal(
            "incompatible_version",
            "session media cannot move through the WAL-only transport",
            serde_json::json!({"session": session.session, "required": ["media-content/v1"]}),
        ))
    }
    async fn plan(&self, cut_id: &str, refs: &Refs, op: Op) -> Result<MovePlans, ErrorBody>;
    async fn cover(&self, reference: &str) -> Result<u64, ErrorBody>;
    async fn export(&self, reference: &str, frozen: &[u8]) -> Result<(), ErrorBody>;
    fn reply(
        &self,
        op: Op,
        plans: &[PlannedSession],
        durable_offsets: &[u64],
        observations: &[MoveObservation],
    ) -> Result<Value, ErrorBody>;
    fn drop_projection(&self, session: &BindIdentity) -> Result<(), String>;
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
    wal: Arc<FileWalStore>,
    leases: FileLeaseStore,
    export: Arc<dyn ExportIndex>,
    driver: Arc<dyn MoveDriver>,
    plans: Arc<Mutex<HashMap<String, LockedPlans>>>,
}
fn unavailable(e: impl std::fmt::Display) -> ErrorBody {
    refusal("unavailable", e.to_string(), Value::Null)
}
fn busy_ref(reference: &str, code: &str, reason: &str) -> broca_wire::agent_data::errors::BusyRef {
    broca_wire::agent_data::errors::BusyRef::new("broca-session", reference, reason)
        .with_reason_code(code)
}
fn busy(refs: Vec<broca_wire::agent_data::errors::BusyRef>, message: &str) -> ErrorBody {
    use broca_wire::agent_data::errors::{BusyDetail, Detail};
    ErrorBody::with_typed_detail(Detail::Busy(BusyDetail::new(refs)), message)
}
fn cut_id(request: &Request) -> Result<String, ErrorBody> {
    request.params()["cutId"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| unavailable("cut request has no cutId"))
}

/// WAL-derived busy state for closed actors, shared by the read probe and the
/// prepare preflight. Persisted records cover work that survives actor closure;
/// LineageRegistry supplies open actors' volatile cache-warm jobs, scheduled
/// keep-warms and transitions under each session's critical section.
pub fn source_busy_reason(
    range: &broca_wal::WalRangeRead,
) -> Result<Option<&'static str>, ErrorBody> {
    let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
        return Err(unavailable("session WAL cannot be inspected"));
    };
    let raw = records.iter().map(|f| f.record.clone()).collect::<Vec<_>>();
    if !broca_core::pending_submissions(&raw).is_empty() {
        return Ok(Some("send_queued"));
    }
    if let Some(lineage) = range.lineage_id {
        if !broca_core::deferred_call_obligations(lineage, records).is_empty() {
            return Ok(Some("approval_open"));
        }
    }
    if records.iter().any(|f| match &f.record {
        broca_wal::WalRecord::RunStarted { run_id, .. } => {
            broca_core::reconcile_run_state(&raw, &run_id.0).is_none_or(|s| !s.state.is_terminal())
        }
        _ => false,
    }) {
        return Ok(Some("run_active"));
    }
    Ok(None)
}

/// Retention is a property of the current copy, including legacy WALs outside
/// wal-ephemeral. Never let an older, expired ledger row classify a fresh WAL.
pub fn retained_move_copy(wal: &FileWalStore, session: &BindIdentity) -> Result<bool, ErrorBody> {
    if wal.resolve_live_path(session).map_err(unavailable)? == Some(wal.ephemeral_wal_path(session))
    {
        return Ok(true);
    }
    let Some(bytes) = wal.bytes_for_move(session).map_err(unavailable)? else {
        return Ok(false);
    };
    retained_bytes(&bytes)
}

/// Classify a previously read local snapshot without reopening its WAL.
pub fn retained_move_copy_from_bytes(
    wal: &FileWalStore,
    session: &BindIdentity,
    bytes: &[u8],
) -> Result<bool, ErrorBody> {
    if wal.resolve_live_path(session).map_err(unavailable)? == Some(wal.ephemeral_wal_path(session))
    {
        return Ok(true);
    }
    retained_bytes(bytes)
}

fn retained_bytes(bytes: &[u8]) -> Result<bool, ErrorBody> {
    let range =
        broca_wal::file_wal::read_verified_member_range(bytes, 0, 0, None).map_err(unavailable)?;
    match range.records() {
        broca_wal::WalRangeRecords::Complete(records) => {
            Ok(broca_core::is_retention_lineage(records))
        }
        _ => Err(unavailable("local WAL cannot be inspected for retention")),
    }
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

fn moved_source(plan: &PlannedSession, moved: &Moved, op: Op) -> ErrorBody {
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
            plan.session.session, moved.holder, moved.cut_id
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
    fn install_error(
        &self,
        cut_id: &str,
        state: &CutState,
        locked: &LockedPlans,
        error: ErrorBody,
    ) -> ErrorBody {
        if error.code.as_str() == "busy" {
            return busy(
                locked
                    .observations
                    .iter()
                    .filter_map(|o| {
                        o.moving_cut.as_ref().map(|cut| {
                            busy_ref(
                                &o.reference,
                                "moving",
                                &format!("local session is frozen by cut {cut}"),
                            )
                        })
                    })
                    .collect(),
                "receiver session is frozen",
            );
        }
        let names = locked
            .plans
            .iter()
            .map(|p| p.reference.as_str())
            .collect::<Vec<_>>()
            .join(",");
        activation_refusal(cut_id, state.step, &names, error)
    }
    pub fn new(
        state_root: PathBuf,
        registry: LineageRegistry,
        wal: Arc<FileWalStore>,
        export: Arc<dyn ExportIndex>,
        driver: Arc<dyn MoveDriver>,
    ) -> Self {
        let root = wal.root().parent().unwrap_or(std::path::Path::new("."));
        Self {
            state_root,
            registry,
            leases: FileLeaseStore::new(root.join("leases")),
            wal,
            export,
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
    fn staged(&self, cut_id: &str, plan: &PlannedSession) -> Result<PathBuf, ErrorBody> {
        migration::staging_path(
            self.wal.root(),
            cut_id,
            &broca_core::session_addr(&plan.session),
        )
        .map_err(unavailable)
    }
    fn installations<'a>(
        &self,
        plans: &'a [PlannedSession],
        paths: &'a [(PathBuf, PathBuf)],
    ) -> Vec<Installation<'a>> {
        plans
            .iter()
            .zip(paths)
            .map(|(plan, (staged, live))| Installation {
                session: &plan.session,
                staged,
                live,
                local_archive: plan.local_archive.as_deref(),
                end_offset: plan.end_offset,
                digest: &plan.digest,
            })
            .collect()
    }
    fn paths(
        &self,
        cut_id: &str,
        plans: &[PlannedSession],
    ) -> Result<Vec<(PathBuf, PathBuf)>, ErrorBody> {
        plans
            .iter()
            .map(|p| {
                let range = broca_wal::file_wal::read_verified_member_range(&p.bytes, 0, 0, None)
                    .map_err(unavailable)?;
                let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
                    return Err(unavailable("incoming move WAL has no complete records"));
                };
                let class = records
                    .iter()
                    .find_map(|frame| match &frame.record {
                        broca_wal::WalRecord::RunStarted { session_class, .. } => {
                            Some(session_class.as_deref())
                        }
                        _ => None,
                    })
                    .flatten();
                Ok((
                    self.staged(cut_id, p)?,
                    self.wal
                        .move_destination(&p.session, class)
                        .map_err(unavailable)?,
                ))
            })
            .collect()
    }
    fn owned_freeze(
        &self,
        cut_id: &str,
        plan: &PlannedSession,
        required: bool,
        op: Op,
        step: broca_wire::agent_data::step::Step,
    ) -> Result<(), ErrorBody> {
        let live = self.wal.wal_path(&plan.session);
        if let Some(moved) =
            migration::read_json::<Moved>(&live.with_extension("moved")).map_err(unavailable)?
        {
            return Err(moved_source(plan, &moved, op));
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
            if broca_wire::agent_data::session_ref(
                &plan.session.harness,
                &plan.session.project_root.to_string_lossy(),
                &plan.session.session,
            ) != plan.reference
            {
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
        let sessions = self
            .registry
            .lock_move_sessions(
                &plans.iter().map(|p| p.session.clone()).collect::<Vec<_>>(),
                false,
            )
            .await
            .map_err(unavailable)?;
        let mut busy = sessions.busy.iter().find_map(|(_, reason)| reason.clone());
        let open_actor = plans
            .iter()
            .any(|p| self.registry.slot_state(&p.session).is_some());
        let mut leases = Vec::new();
        let mut observations = Vec::new();
        for plan in &plans {
            let live = self.wal.wal_path(&plan.session);
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
                moving_cut: migration::cut_fence(self.wal.root(), &plan.reference)
                    .map_err(unavailable)?
                    .or_else(|| moving.map(|f| f.cut_id)),
                moved_holder: moved.map(|m| m.holder),
                open_actor: self.registry.slot_state(&plan.session).is_some(),
            };
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
        if matches!(op, Op::Prepare | Op::Import | Op::Activate) {
            for plan in &locked.plans {
                if retained_move_copy(self.wal.as_ref(), &plan.session)? {
                    if op == Op::Prepare {
                        return Err(refusal(
                            "not_exportable",
                            "retention session cannot be moved",
                            Value::Null,
                        ));
                    }
                    return Err(refusal(
                        "local_advanced",
                        "retained copy with a deletion deadline cannot be replaced by a cut",
                        Value::Null,
                    ));
                }
            }
        }
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
            let live = self.wal.wal_path(&plan.session);
            if let Some(other) =
                migration::cut_fence(self.wal.root(), &plan.reference).map_err(unavailable)?
            {
                if other != cut_id && op != Op::Abort {
                    return Err(foreign_owner(plan, &cut_id, &other, op, state.step));
                }
            }
            if matches!(op, Op::Prepare | Op::Export) {
                self.owned_freeze(&cut_id, plan, op == Op::Export, op, state.step)?;
                if migration::digest(&plan.bytes) != plan.digest
                    || plan.bytes.len() as u64 != plan.end_offset
                {
                    return Err(unavailable(
                        "source snapshot does not match the frozen item",
                    ));
                }
                move_install::verify_received(
                    &plan.reference,
                    &plan.bytes,
                    plan.end_offset,
                    &plan.digest,
                )?;
                if op == Op::Export {
                    let current = if live.try_exists().map_err(unavailable)? {
                        std::fs::read(&live).map_err(unavailable)?
                    } else {
                        plan.local_archive
                            .clone()
                            .ok_or_else(|| unavailable("frozen source WAL is missing"))?
                    };
                    if current != plan.bytes {
                        return Err(unavailable("frozen source bytes changed"));
                    }
                }
            }
            if op == Op::Prepare {
                let digests =
                    broca_core::media_store::session_media_digests(&self.state_root, &plan.session)
                        .map_err(unavailable)?;
                self.driver.prepare_media(&plan.session, &digests)?;
                if !live.try_exists().map_err(unavailable)? && plan.local_archive.is_none() {
                    return Err(refusal(
                        "unknown_ref",
                        "source session has no WAL",
                        Value::Null,
                    ));
                }
                if !live.try_exists().map_err(unavailable)?
                    && plan.local_archive.as_deref() != Some(plan.bytes.as_slice())
                {
                    return Err(unavailable("source archive snapshot changed"));
                }
                if migration::digest(&plan.bytes) != plan.digest {
                    return Err(unavailable("source snapshot digest changed"));
                }
                if plan.end_offset != plan.bytes.len() as u64 {
                    return Err(unavailable("source snapshot endOffset changed"));
                }
                if live.try_exists().map_err(unavailable)?
                    && std::fs::read(&live).map_err(unavailable)? != plan.bytes
                {
                    return Err(busy(
                        vec![busy_ref(
                            &plan.reference,
                            "wal_changed",
                            "source WAL changed before freeze",
                        )],
                        "source WAL changed before freeze",
                    ));
                }
                let records =
                    broca_wal::file_wal::read_verified_member_range(&plan.bytes, 0, 0, None)
                        .map_err(unavailable)?;
                if let Some(reason) = source_busy_reason(&records)? {
                    return Err(busy(
                        vec![busy_ref(
                            &plan.reference,
                            reason,
                            &format!("source has pending {reason}"),
                        )],
                        "source has pending work",
                    ));
                }
            }
            if op == Op::Release {
                let moved = migration::read_json::<Moved>(&live.with_extension("moved"))
                    .map_err(unavailable)?;
                if let Some(moved) = moved {
                    if moved.cut_id != cut_id || request.params()["holder"] != moved.holder {
                        return Err(moved_source(plan, &moved, Op::Release));
                    }
                } else {
                    self.owned_freeze(&cut_id, plan, true, Op::Release, state.step)?;
                }
            }
            if op == Op::Import {
                move_install::check_format(&plan.format)?;
                let sealed = plan
                    .sealed_digest
                    .as_deref()
                    .ok_or_else(|| unavailable("door read did not return its sealed digest"))?;
                if sealed != plan.digest {
                    return Err(refusal(
                        "digest_mismatch",
                        "door sealed digest differs from the cut item",
                        Value::Null,
                    ));
                }
                move_install::verify_received(
                    &plan.reference,
                    &plan.bytes,
                    plan.end_offset,
                    &plan.digest,
                )?;
                if migration::read_json::<Freeze>(&live.with_extension("moving"))
                    .map_err(unavailable)?
                    .is_some()
                {
                    return Err(busy(
                        vec![busy_ref(
                            &plan.reference,
                            "moving",
                            "receiver is frozen by a cut",
                        )],
                        "receiver is frozen by a cut",
                    ));
                }
                let local = if live.try_exists().map_err(unavailable)? {
                    std::fs::read(&live).map_err(unavailable)?
                } else {
                    plan.local_archive.clone().unwrap_or_default()
                };
                if !plan.bytes.starts_with(&local) {
                    return Err(refusal(
                        "local_advanced",
                        "local WAL is not a prefix of incoming bytes",
                        Value::Null,
                    ));
                }
            }
        }
        if op == Op::Activate {
            let paths = self.paths(&cut_id, &locked.plans)?;
            if state.intent.as_ref().is_none_or(|i| i.op != "activate")
                && paths.iter().any(|(staged, _)| !staged.exists())
            {
                return Err(unavailable(
                    "activation staging is missing without a pending activate intent",
                ));
            }
            move_install::check_installations(&self.installations(&locked.plans, &paths))
                .map_err(|e| self.install_error(&cut_id, state, &locked, e))?;
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
                &[],
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
        let mut offsets = Vec::new();
        match op {
            Op::Prepare => {
                for plan in &locked.plans {
                    migration::freeze(
                        &self.state_root,
                        &self.wal.wal_path(&plan.session),
                        &Freeze {
                            cut_id: cut_id.clone(),
                            end_offset: plan.end_offset,
                            lineage_id: plan.lineage_id.clone(),
                        },
                    )
                    .map_err(unavailable)?
                    .map_err(unavailable)?;
                    offsets.push(self.driver.cover(&plan.reference).await?);
                }
            }
            Op::Export => {
                for plan in &locked.plans {
                    self.driver.export(&plan.reference, &plan.bytes).await?;
                }
            }
            Op::Release => {
                let params = request.params();
                let holder = params["holder"]
                    .as_str()
                    .ok_or_else(|| unavailable("release holder missing"))?;
                for plan in &locked.plans {
                    migration::release(
                        &self.state_root,
                        &self.wal.wal_path(&plan.session),
                        &cut_id,
                        holder,
                    )
                    .map_err(unavailable)?
                    .map_err(unavailable)?;
                }
            }
            Op::Import => {
                for plan in &locked.plans {
                    migration::stage(
                        &self.state_root,
                        &self.staged(&cut_id, plan)?,
                        &plan.bytes,
                        &plan.digest,
                    )
                    .map_err(unavailable)?;
                }
            }
            Op::Activate => {
                let paths = self.paths(&cut_id, &locked.plans)?;
                let mut projection_admissions = Vec::new();
                // Keep failed-projection guards through installation so a census
                // cannot clear their durable marker before the WAL is published.
                for plan in &locked.plans {
                    let range =
                        broca_wal::file_wal::read_verified_member_range(&plan.bytes, 0, 0, None)
                            .map_err(unavailable)?;
                    let broca_wal::WalRangeRecords::Complete(records) = range.records() else {
                        return Err(unavailable(
                            "incoming move WAL has incomplete identity records",
                        ));
                    };
                    for frame in records {
                        if let broca_wal::WalRecord::RunStarted { session, .. } = &frame.record {
                            // One durable marker and lease protect the whole batch.
                            // Reacquiring the same lease for later identities would
                            // contend with the guard this installation already holds.
                            if projection_admissions.is_empty() {
                                if let Some(admission) = self
                                    .wal
                                    .record_session_before_install(
                                        &broca_core::session_addr(&plan.session),
                                        session,
                                    )
                                    .map_err(unavailable)?
                                {
                                    projection_admissions.push(admission);
                                }
                            }
                        }
                    }
                }
                move_install::install_verified(
                    &self.state_root,
                    &cut_id,
                    &self.installations(&locked.plans, &paths),
                    self.export.as_ref(),
                    |session| {
                        // Ownership sidecars retain the canonical wal/ address so every
                        // transcript tree shares one move refusal and one writer lease.
                        migration::unlink(&self.wal.wal_path(session).with_extension("moved"))
                            .map_err(|error| error.to_string())?;
                        self.driver.drop_projection(session)
                    },
                )
                .map_err(|e| self.install_error(&cut_id, _state, &locked, e))?;
            }
            Op::Abort => {
                self.rollback(request, _state).await?;
            }
            Op::Describe | Op::Busy | Op::Diverged | Op::Status => {}
        }
        let reply = self
            .driver
            .reply(op, &locked.plans, &offsets, &locked.observations)?;
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
            migration::abort_freeze(&self.wal.wal_path(&plan.session), &cut_id)
                .map_err(unavailable)?;
            migration::unlink(&self.staged(&cut_id, plan)?).map_err(unavailable)?;
        }
        // A damaged staged WAL may no longer reveal a bind triple. Staging is
        // private to this cut, so discard its remaining files without inventing
        // a session identity or touching another cut's directory.
        let directory = self
            .wal
            .root()
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .join("wal-staging")
            .join(&cut_id);
        match std::fs::read_dir(directory) {
            Ok(entries) => {
                for entry in entries {
                    migration::unlink(&entry.map_err(unavailable)?.path()).map_err(unavailable)?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(unavailable(error)),
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
            let mut complete = true;
            for plan in &locked.plans {
                let staged = self.staged(&cut_id, plan)?;
                match std::fs::read(&staged) {
                    Ok(bytes)
                        if bytes.len() as u64 == plan.end_offset
                            && migration::digest(&bytes) == plan.digest =>
                    {
                        // Cached bytes can survive a process death before the
                        // staging rename's directory barrier. Finish publication
                        // before returning a reply that records imported.
                        migration::durable_dir(&self.state_root, staged.parent().unwrap())
                            .map_err(unavailable)?;
                    }
                    Ok(_) => {
                        migration::unlink(&staged).map_err(unavailable)?;
                        complete = false;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                        complete = false;
                    }
                    Err(e) => return Err(unavailable(e)),
                }
            }
            if !complete {
                return Ok(None);
            }
            return self
                .driver
                .reply(op, &locked.plans, &[], &locked.observations)
                .map(Some);
        }
        self.check(&request, state).await?;
        self.apply(&request, state).await.map(Some)
    }
}

#[cfg(test)]
mod tests;
