//! Dispatch internal agent-data RPCs and resolve references to session WALs.
//! broca-module persists move progress, fences actor writes and keeps imported
//! runs from being billed again; this bridge reuses those durable operations.
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use broca_module::{
    cut_state::{refusal, CutStore},
    move_files::{FileCutArtifacts, MoveDriver, MoveObservation, MovePlans, PlannedSession},
    LineageRegistry,
};
use broca_protocol::BindIdentity;
use broca_subc::session_door::{DoorRead, SessionDoorClient, SessionDoorError, MAX_CALL_BYTES};
use broca_wire::agent_data::{self as contract, errors::ErrorBody, refs::Refs, Op};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::time::Instant;

fn unavailable(error: impl std::fmt::Display) -> ErrorBody {
    refusal("unavailable", error.to_string(), Value::Null)
}

/// A test seam at the sealed byte transport, not at cut-state or artifact logic.
/// Reads must obey SessionDoorClient's validated range and full-page invariants;
/// the participant checks the complete stream's size, metadata and sealed digest.
#[async_trait::async_trait]
pub trait SessionDoor: Send + Sync {
    async fn export_file(
        &self,
        reference: &str,
        path: &std::path::Path,
    ) -> Result<(), SessionDoorError> {
        let bytes = std::fs::read(path).map_err(|error| SessionDoorError {
            message: error.to_string(),
            door_code: None,
        })?;
        self.export(reference, &bytes).await
    }
    /// The door's durable byte end for `reference`: where an export resumes. Zero for a ref the
    /// door has never seen.
    async fn durable_offset(&self, reference: &str) -> Result<u64, SessionDoorError>;
    async fn export(&self, reference: &str, bytes: &[u8]) -> Result<(), SessionDoorError>;
    async fn read(
        &self,
        reference: &str,
        offset: u64,
        cut_id: &str,
    ) -> Result<DoorRead, SessionDoorError>;
}
#[async_trait::async_trait]
impl SessionDoor for SessionDoorClient {
    async fn export_file(
        &self,
        reference: &str,
        path: &std::path::Path,
    ) -> Result<(), SessionDoorError> {
        self.admit_file(reference, path, Instant::now() + Duration::from_secs(120))
            .await
            .map(|_| ())
    }
    async fn durable_offset(&self, reference: &str) -> Result<u64, SessionDoorError> {
        self.offsets(reference, Instant::now() + Duration::from_secs(60))
            .await
            .map(|c| c.durable_offset)
    }
    async fn export(&self, reference: &str, bytes: &[u8]) -> Result<(), SessionDoorError> {
        self.admit(reference, bytes, Instant::now() + Duration::from_secs(120))
            .await
            .map(|_| ())
    }
    async fn read(
        &self,
        reference: &str,
        offset: u64,
        cut_id: &str,
    ) -> Result<DoorRead, SessionDoorError> {
        self.read(
            reference,
            offset,
            MAX_CALL_BYTES,
            cut_id,
            Instant::now() + Duration::from_secs(60),
        )
        .await
    }
}

pub struct AgentDataService {
    /// broca's state root, which keeps the move state under `agent-data/`.
    root: PathBuf,
    cuts: CutStore,
    artifacts: FileCutArtifacts,
    inventory_readiness: Arc<dyn broca_module::registry::SessionInventoryReadiness>,
    op_counts: [AtomicU64; 10],
}
impl AgentDataService {
    /// Construct with operational capabilities from one store. Inventory repair uses the
    /// registry's shared, signalled readiness task.
    pub fn with_ports(
        root: PathBuf,
        registry: LineageRegistry,
        index: Arc<dyn broca_core::SessionInventory>,
        door: Arc<dyn SessionDoor>,
    ) -> Self {
        let driver = Arc::new(Driver {
            registry: registry.clone(),
            root: root.clone(),
            index: index.clone(),
            door,
        });
        let inventory_readiness = registry.set_session_inventory_readiness(Arc::new(
            crate::session_inventory::SessionInventory::with_inventory(root.clone(), index.clone()),
        ));
        Self {
            cuts: CutStore::new(&root),
            artifacts: FileCutArtifacts::new(root.clone(), registry, index, driver),
            root,
            inventory_readiness,
            op_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    pub fn new(
        root: PathBuf,
        registry: LineageRegistry,
        index: Arc<broca_store::SqliteStore>,
        door: Arc<dyn SessionDoor>,
    ) -> Self {
        let driver = Arc::new(Driver {
            registry: registry.clone(),
            root: root.clone(),
            index: index.clone(),
            door,
        });
        let inventory_readiness = registry.set_session_inventory_readiness(Arc::new(
            crate::session_inventory::SessionInventory::new(root.clone(), index.clone()),
        ));
        Self {
            cuts: CutStore::new(&root),
            artifacts: FileCutArtifacts::new(root.clone(), registry, index, driver),
            root,
            inventory_readiness,
            op_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }

    pub async fn reconcile_boot(&self) -> Result<(), ErrorBody> {
        broca_records::move_fence::rebuild_fence_index(&self.root)
            .map_err(|error| unavailable(format!("rebuild_fence_index: {error}")))?;
        self.cuts
            .reconcile_boot(&self.artifacts)
            .await
            .map_err(|mut error| {
                error.message = format!("cuts.reconcile_boot: {}", error.message);
                error
            })?;
        self.inventory_readiness.start();
        Ok(())
    }

    /// Finishes the move operations boot reconciliation had to leave pending because a session
    /// they name couldn't be read yet: an encrypted session needs the storage key, which loads
    /// only after boot. Run once the key has loaded. A cut with nothing pending is left alone, so
    /// running this again changes nothing.
    pub async fn reconcile_pending_cuts(&self) -> Result<(), ErrorBody> {
        self.cuts.reconcile_boot(&self.artifacts).await
    }
}

const OPS: [&str; 10] = [
    "describe", "busy", "diverged", "prepare", "export", "import", "release", "activate", "abort",
    "status",
];

pub(crate) fn request_is_read_only(bytes: &[u8]) -> bool {
    let Ok(rpc) = serde_json::from_slice::<Value>(bytes) else {
        return false;
    };
    matches!(
        rpc.get("method").and_then(Value::as_str),
        Some(
            "agent_data.describe" | "agent_data.busy" | "agent_data.diverged" | "agent_data.status"
        )
    )
}

pub(crate) fn health_counts(service: Option<&AgentDataService>) -> Value {
    Value::Object(
        OPS.iter()
            .enumerate()
            .map(|(i, op)| {
                (
                    (*op).to_owned(),
                    json!(service.map_or(0, |s| s.op_counts[i].load(Ordering::Relaxed))),
                )
            })
            .collect(),
    )
}

struct OpObservation {
    op: String,
    cut_id: String,
    ref_count: usize,
    start: Instant,
}
impl Drop for OpObservation {
    fn drop(&mut self) {
        tracing::info!(op = %self.op, cut_id = %self.cut_id, ref_count = self.ref_count,
            elapsed_ms = self.start.elapsed().as_millis() as u64, "agent-data op finished");
    }
}

pub(crate) async fn dispatch(
    service: Option<&AgentDataService>,
    caller: &str,
    bytes: &[u8],
) -> Result<Value, ErrorBody> {
    // Even malformed JSON must not disclose request validation to another principal.
    if !contract::caller_admitted(caller) {
        return Err(refusal(
            "caller_forbidden",
            "agent-data caller is not reserved:entorhinal",
            Value::Null,
        ));
    }
    let service = service.ok_or_else(|| unavailable("agent-data service is not configured"))?;
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Rpc {
        method: String,
        params: Value,
    }
    let rpc: Rpc = serde_json::from_slice(bytes)
        .map_err(|e| refusal("invalid_request", e.to_string(), Value::Null))?;
    if let Some(i) = OPS
        .iter()
        .position(|op| rpc.method == format!("agent_data.{op}"))
    {
        service.op_counts[i].fetch_add(1, Ordering::Relaxed);
    }
    // Drop also records cancelled calls, so a disconnected caller cannot hide
    // its operation's rate or elapsed time from the process log.
    let _observation = OpObservation {
        op: rpc.method.clone(),
        cut_id: rpc
            .params
            .get("cutId")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_owned(),
        ref_count: rpc
            .params
            .get("refs")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
        start: Instant::now(),
    };
    service
        .cuts
        .execute(caller, &rpc.method, &rpc.params, &service.artifacts)
        .await
}

struct Driver {
    /// Read for the session store's runtime at each use rather than once: a runtime that failed to
    /// open at boot can open later, when the key loader retries it.
    registry: LineageRegistry,
    root: PathBuf,
    index: Arc<dyn broca_core::SessionInventory>,
    door: Arc<dyn SessionDoor>,
}

mod snapshot;
mod store;
mod stream;
struct Incoming {
    bytes: Vec<u8>,
    end: u64,
    digest: String,
    /// Where each export frame of the stream ends, with the digest of the stream up to there.
    /// A copy whose own stream matches one of these is an ancestor of the cut: the stream only
    /// grows, one export per move.
    frame_ends: Vec<(u64, String)>,
}

#[derive(Default)]
struct SessionInventory {
    sessions: BTreeMap<String, BindIdentity>,
    unreadable: Vec<String>,
}

/// What a failed door read during `op` answers the mover. The driver's next step follows from
/// the code, and each op may refuse only the codes its row of the agent-data table lists, so a
/// door code maps to the first allowed code that carries its meaning, and to `unavailable` (a
/// plain retry) when the op allows none:
/// - a cut the door will serve once its import progresses (`cut_import_pending`, and
///   `not_found` from a door older than hub moves) is `cut_not_ready`, retried within the step;
/// - a cut the door will never serve here (`cut_not_published`, `cut_takeover_closed`,
///   `cut_range_not_local`, and `cut_item_missing`: the cut's verified manifest doesn't list
///   this session, and never will) aborts the move, as `cut_unknown` or `participant_failed`;
/// - bytes that fail the door's own check (`verify_failed`) are `digest_mismatch`.
///
/// `diverged` allows none of the aborting codes. A cut that cannot be compared there makes the
/// ref's divergence unknown, which blocks the move, so the failure carries that reason too.
/// The door's code stays in the message, and in `detail.reason` of a `participant_failed`.
fn door_failure(op: Op, error: SessionDoorError) -> CutReadFailure {
    let (wanted, unknown_cause): (&[&str], _) = match error.door_code.as_deref() {
        Some("not_found" | "cut_import_pending") => (&["cut_not_ready"], None),
        Some("verify_failed") => (
            &["digest_mismatch", "participant_failed"],
            Some("verify_failed"),
        ),
        Some("cut_not_published") => (&ABORTS, Some("cut_not_published")),
        Some("cut_takeover_closed") => (&ABORTS, Some("cut_takeover_closed")),
        Some("cut_range_not_local") => (&ABORTS, Some("cut_range_not_local")),
        Some("cut_item_missing") => (&ABORTS, Some("cut_item_missing")),
        _ => (&[], None),
    };
    const ABORTS: [&str; 2] = ["cut_unknown", "participant_failed"];
    let detail = |code: &str| match (code, error.door_code.as_deref()) {
        ("participant_failed", Some(door)) => json!({ "reason": door }),
        _ => Value::Null,
    };
    let refusal = wanted
        .iter()
        .map(|code| refusal(code, error.to_string(), detail(code)))
        .find(|body| contract::errors::may_refuse(op, &body.code))
        .unwrap_or_else(|| refusal("unavailable", error.to_string(), Value::Null));
    CutReadFailure {
        refusal,
        unknown_cause,
    }
}

struct CutReadFailure {
    refusal: ErrorBody,
    /// Why the cut could not be compared with the local copy, when that is a fact about the cut
    /// rather than a fault here: `diverged` then answers the ref's divergence unknown with this
    /// reason instead of refusing.
    unknown_cause: Option<&'static str>,
}
impl From<ErrorBody> for CutReadFailure {
    fn from(refusal: ErrorBody) -> Self {
        Self {
            refusal,
            unknown_cause: None,
        }
    }
}

impl Driver {
    /// The session store's runtime, when it has opened.
    fn store(&self) -> Option<Arc<broca_module::store_open::StoreRuntime>> {
        self.registry.store_runtime()
    }

    /// The session a mover's door serves for `reference` at `cut_id`. Only session-store moves
    /// are accepted: broca keeps no session in the old log format, so no computer sends one.
    async fn incoming(
        &self,
        reference: &str,
        cut_id: &str,
        op: Op,
    ) -> Result<Incoming, CutReadFailure> {
        let page = self
            .door
            .read(reference, 0, cut_id)
            .await
            .map_err(|error| door_failure(op, error))?;
        if page.bytes.starts_with(b"broca-store-export/") {
            return self.incoming_store(reference, cut_id, page, op).await;
        }
        let incompatible = refusal(
            "incompatible_version",
            "the cut is in the old session log format, which this computer no longer reads",
            json!({"found": "broca-wal", "required": broca_session_store::session::moves::FORMAT}),
        );
        Err(if contract::errors::may_refuse(op, &incompatible.code) {
            incompatible
        } else {
            unavailable(incompatible.message)
        }
        .into())
    }
    /// The sessions `refs` name, looked up in the session-ref index. Every session is in it:
    /// binding a new identity writes its row in the same transaction that mints the identity
    /// (`session_address`), and a moved-in session's row is written before broca replies to
    /// the mover's `activate`. So a ref the index lacks is not on this machine, and the stored
    /// sessions are never searched: a ref is a one-way hash, so a search must read every stored
    /// head, and on a machine with about 190,000 sessions that took 37 s, past a mover's 30 s
    /// deadline.
    fn local_sessions(&self, refs: &Refs) -> SessionInventory {
        match self
            .index
            .resolve_refs(&refs.iter().map(|r| r.r#ref.as_str()).collect::<Vec<_>>())
        {
            Ok(found) => SessionInventory {
                sessions: found
                    .sessions
                    .into_iter()
                    .map(|(reference, (_, session))| (reference, session))
                    .collect(),
                unreadable: if found.complete {
                    Vec::new()
                } else {
                    vec!["session-ref projection is behind admissions".into()]
                },
            },
            Err(error) => SessionInventory {
                sessions: BTreeMap::new(),
                unreadable: vec![error.to_string()],
            },
        }
    }
}

#[async_trait::async_trait]
impl MoveDriver for Driver {
    async fn cache_store_stream(
        &self,
        cut: &str,
        plan: &PlannedSession,
        op: Op,
    ) -> Result<(), ErrorBody> {
        self.cache_stream(cut, plan, op).await
    }
    async fn verify_store(
        &self,
        plan: &PlannedSession,
        staged: &std::path::Path,
    ) -> Result<(), ErrorBody> {
        let runtime = self
            .store()
            .ok_or_else(|| unavailable("store runtime unavailable"))?;
        let addr =
            broca_session_store::backend::SessionAddr(broca_core::session_addr(&plan.session));
        let export = broca_session_store::session::moves::Export::decode(
            broca_session_store::session::moves::last_export(&plan.bytes)
                .map_err(broca_module::move_files::store::failure)?,
            &addr,
            runtime.deployment(),
        )
        .map_err(broca_module::move_files::store::failure)?;
        let (_, version) = export
            .head(&addr, runtime.deployment())
            .map_err(broca_module::move_files::store::failure)?;
        let actual = broca_session_store::session::moves::Export::pinned(
            &broca_session_store::backend::LocalBackend::new(staged),
            &addr,
            version,
            &export.header.class,
            runtime.deployment(),
        )
        .await
        .map_err(broca_module::move_files::store::failure)?;
        let checks = snapshot::store_snapshot(&actual, runtime.deployment())
            .await
            .map_err(unavailable)?;
        if export.checks != checks {
            return Err(refusal(
                "digest_mismatch",
                "staged store move-check snapshot differs from source",
                json!({"checks":checks.keys().collect::<Vec<_>>()}),
            ));
        }
        Ok(())
    }
    async fn plan(&self, cut_id: &str, refs: &Refs, op: Op) -> Result<MovePlans, ErrorBody> {
        // Empty move probes must not depend on the projection or touch any session.
        if refs.iter().next().is_none() {
            return Ok(MovePlans::default());
        }
        // Every session broca serves lives in the session store, so every op but `abort` needs
        // it open: an encrypted store opens only once its key has loaded, and the mover retries.
        if op != Op::Abort && self.store().is_none() {
            return Err(unavailable("session storage is unavailable"));
        }
        let receiver = matches!(op, Op::Diverged | Op::Import | Op::Activate);
        let state: Option<broca_module::cut_state::CutState> = if op == Op::Busy {
            None
        } else {
            let path = CutStore::new(&self.root).path(cut_id);
            broca_records::move_fence::read_json(&path).map_err(|error| {
                unavailable(format!("cannot read cut {}: {error}", path.display()))
            })?
        };
        let bound = state.as_ref().is_some_and(|state| {
            state.refs.is_some()
                || state.intent.is_some()
                || state.step != contract::step::Step::None
        });
        // Once intent is durable, use only its recorded addresses. Discovering
        // unrelated sessions must never become a prerequisite for cut recovery.
        let inventory = if bound {
            SessionInventory {
                sessions: state.expect("bound cut exists").sessions,
                unreadable: Vec::new(),
            }
        } else if matches!(op, Op::Busy | Op::Diverged | Op::Prepare | Op::Import) {
            self.local_sessions(refs)
        } else {
            SessionInventory::default()
        };
        let sessions = inventory.sessions;
        let unreadable = inventory.unreadable;
        let mut unknown_refs = BTreeSet::new();
        let mut plans = Vec::new();
        for reference in refs.iter() {
            let session = sessions.get(&reference.r#ref);
            if let Some(session) = session {
                match self.store_plan(&reference.r#ref, session, cut_id, op).await {
                    Ok(Some(plan)) => {
                        plans.push(plan);
                        continue;
                    }
                    Ok(None) => {}
                    // A held session that can't be read here can't be compared with the cut:
                    // its divergence is unknown, and the other refs are still answered.
                    Err(error) if op == Op::Diverged => {
                        tracing::error!(session_ref = %reference.r#ref, cut_id, error = %error.message, "agent-data cannot read a held ref to compare it with the cut");
                        plans.push(store::unreadable_divergence(&reference.r#ref, session));
                        continue;
                    }
                    Err(error) => return Err(error),
                }
            }
            if reference.ref_kind == contract::BROCA_SESSION_REF_KIND
                && session.is_none()
                && (bound || !unreadable.is_empty())
            {
                if matches!(op, Op::Diverged | Op::Busy) {
                    unknown_refs.insert(reference.r#ref.clone());
                    continue;
                }
                let cause = if bound {
                    format!(
                        "cut {cut_id} has no recorded address for ref {}",
                        reference.r#ref
                    )
                } else {
                    format!(
                        "cannot resolve ref {} while session discovery is uncertain: {}",
                        reference.r#ref,
                        unreadable.join(", ")
                    )
                };
                return Err(unavailable(cause));
            }
            if reference.ref_kind != contract::BROCA_SESSION_REF_KIND
                || (!receiver && session.is_none())
            {
                if op == Op::Abort {
                    continue;
                }
                return Err(refusal(
                    if receiver {
                        "invalid_request"
                    } else {
                        "unknown_ref"
                    },
                    "broca does not hold this session ref",
                    json!(reference),
                ));
            }
            // The store has no copy of this session here: it is arriving in this move, or this
            // computer doesn't hold it.
            match op {
                // Nothing to abort, and a copy that isn't here can't have moved past the cut.
                Op::Abort | Op::Diverged => continue,
                Op::Import => {
                    let incoming = self
                        .incoming(&reference.r#ref, cut_id, op)
                        .await
                        .map_err(|error| error.refusal)?;
                    plans.push(self.incoming_store_plan(&reference.r#ref, incoming).await?);
                }
                Op::Activate => {
                    let staged = session.is_some_and(|session| {
                        self.root
                            .join("session-store-moves/staging")
                            .join(cut_id)
                            .join(broca_core::session_addr(session))
                            .exists()
                    });
                    if !staged {
                        return Err(unavailable("activation has no staged session"));
                    }
                    let incoming = self
                        .incoming(&reference.r#ref, cut_id, op)
                        .await
                        .map_err(|error| error.refusal)?;
                    plans.push(self.incoming_store_plan(&reference.r#ref, incoming).await?);
                }
                _ => {
                    return Err(refusal(
                        "unknown_ref",
                        "broca does not hold this session ref",
                        json!(reference),
                    ))
                }
            }
        }
        Ok(MovePlans {
            sessions: plans,
            unknown_refs,
        })
    }
    async fn export(&self, reference: &str, frozen: &[u8]) -> Result<(), ErrorBody> {
        if frozen.starts_with(b"broca-store-export/") {
            let frame = broca_session_store::session::moves::last_export(frozen)
                .map_err(broca_module::move_files::store::failure)?;
            let header = broca_session_store::session::moves::header(frame)
                .map_err(broca_module::move_files::store::failure)?;
            let session = BindIdentity {
                project_root: header.triple.project_root.into(),
                harness: header.triple.harness,
                session: header.triple.session,
            };
            return self
                .door
                .export_file(
                    reference,
                    &self
                        .root
                        .join("session-store-moves/streams")
                        .join(broca_core::session_addr(&session)),
                )
                .await
                .map_err(unavailable);
        }
        self.door
            .export(reference, frozen)
            .await
            .map_err(unavailable)
    }
    fn reply(
        &self,
        op: Op,
        plans: &[PlannedSession],
        observations: &[MoveObservation],
    ) -> Result<Value, ErrorBody> {
        Ok(match op {
            Op::Prepare => serde_json::to_value(contract::prepare::PrepareReply::new(
                plans
                    .iter()
                    .map(|p| {
                        contract::prepare::PreparedItem::new(
                            contract::BROCA_SESSION_REF_KIND,
                            &p.reference,
                            &p.reference,
                            "reserved:broca",
                            p.end_offset,
                            &p.digest,
                            &p.format,
                            true,
                        )
                    })
                    .collect(),
                Vec::new(),
            ))
            .map_err(unavailable)?,
            Op::Export => {
                json!({"items":plans.iter().map(|p|json!({"sessionRef":p.reference,"endOffset":p.end_offset})).collect::<Vec<_>>()})
            }
            Op::Import => {
                json!({"items":plans.iter().map(|p|json!({"sessionRef":p.reference,"endOffset":p.end_offset,"digest":p.digest})).collect::<Vec<_>>()})
            }
            Op::Release => json!({"enforced":true}),
            Op::Activate | Op::Abort => json!({}),
            Op::Busy => json!({"refs":plans.iter().zip(observations).map(|(plan, observation)| {
                if let Some(cut) = &observation.moving_cut {
                    return json!({"state":"busy","reason_code":"moving","reason":format!("session is frozen by cut {cut}")});
                }
                if let Some(holder) = &observation.moved_holder {
                    return json!({"state":"unknown","reason":format!("session moved to {holder}")});
                }
                if let Some(reason) = &observation.busy_reason {
                    return json!({"state":"busy","reason_code":reason,"reason":format!("session has pending {reason}")});
                }
                let _ = plan;
                json!({"state":"idle"})
            }).collect::<Vec<_>>()}),
            Op::Diverged => {
                use contract::diverged::{DivergedEntry, DivergedReply, DivergedState};

                let refs = plans
                    .iter()
                    .map(|plan| {
                        let state = match plan
                            .divergence
                            .unwrap_or(broca_module::move_files::LocalDivergence::Unknown)
                        {
                            broca_module::move_files::LocalDivergence::AtCut => {
                                DivergedState::AtCut
                            }
                            broca_module::move_files::LocalDivergence::Advanced => {
                                DivergedState::Advanced
                            }
                            broca_module::move_files::LocalDivergence::Unknown => {
                                DivergedState::Unknown
                            }
                        };
                        match &plan.divergence_reason {
                            Some(reason) => DivergedEntry::with_reason(state, reason.clone())
                                .map_err(unavailable),
                            None => Ok(DivergedEntry::new(state)),
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                serde_json::to_value(DivergedReply::new(refs)).map_err(unavailable)?
            }
            Op::Describe => {
                serde_json::to_value(contract::describe_reply()).map_err(unavailable)?
            }
            Op::Status => return Err(unavailable("status belongs to the cut-state authority")),
        })
    }
}

#[cfg(test)]
mod door_failure_tests {
    use super::*;
    use contract::errors::{driver_action_for, may_refuse, DriverAction};

    fn failure(op: Op, door_code: Option<&str>) -> CutReadFailure {
        door_failure(
            op,
            SessionDoorError {
                door_code: door_code.map(str::to_owned),
                message: "refused".into(),
            },
        )
    }

    const STOPS: [&str; 4] = [
        "cut_not_published",
        "cut_takeover_closed",
        "cut_range_not_local",
        "cut_item_missing",
    ];

    /// Every op that reads a cut answers only codes its row of the agent-data table allows, and
    /// a door code that means "never here" never comes back as something the driver retries.
    #[test]
    fn each_op_refuses_only_its_allowed_codes_with_the_door_codes_meaning() {
        let doors = [
            Some("not_found"),
            Some("cut_import_pending"),
            Some("verify_failed"),
            Some("lease_unavailable"),
            None,
        ]
        .into_iter()
        .chain(STOPS.map(Some));
        for op in [Op::Prepare, Op::Diverged, Op::Import, Op::Activate] {
            for door in doors.clone() {
                let failure = failure(op, door);
                let body = &failure.refusal;
                assert!(
                    may_refuse(op, &body.code),
                    "{op:?} {door:?} -> {:?}",
                    body.code
                );
                assert!(body.message.contains("refused"), "{}", body.message);
                if door.is_some_and(|door| STOPS.contains(&door)) && op != Op::Diverged {
                    assert!(
                        !driver_action_for(op, &body.code).retryable(),
                        "{op:?} {door:?} -> {:?}",
                        body.code
                    );
                }
            }
        }
    }

    #[test]
    fn import_retries_a_pending_cut_and_names_the_door_code_when_it_aborts() {
        for door in ["not_found", "cut_import_pending"] {
            let body = failure(Op::Import, Some(door)).refusal;
            assert_eq!(body.code.as_str(), "cut_not_ready");
            assert_eq!(
                driver_action_for(Op::Import, &body.code),
                DriverAction::RetryWithinStep
            );
        }
        for door in STOPS {
            let body = failure(Op::Import, Some(door)).refusal;
            assert_eq!(body.code.as_str(), "participant_failed");
            assert_eq!(body.detail, Some(json!({ "reason": door })));
        }
        assert_eq!(
            failure(Op::Import, Some("verify_failed"))
                .refusal
                .code
                .as_str(),
            "digest_mismatch"
        );
        assert_eq!(
            failure(Op::Activate, Some(STOPS[0])).refusal.code.as_str(),
            "cut_unknown"
        );
    }

    /// `diverged` has no aborting code, so a cut the door will never serve here leaves the ref's
    /// divergence unknown; a pending one is still retried.
    #[test]
    fn diverged_reports_a_cut_it_can_never_read_as_unknown() {
        for door in STOPS {
            assert_eq!(failure(Op::Diverged, Some(door)).unknown_cause, Some(door));
        }
        let pending = failure(Op::Diverged, Some("cut_import_pending"));
        assert_eq!(pending.unknown_cause, None);
        assert_eq!(pending.refusal.code.as_str(), "cut_not_ready");
    }
}

#[cfg(test)]
mod operational_port_tests {
    use super::*;
    use broca_core::{SessionInventory as InventoryPort, SessionRefIndex, SessionRefResolver};
    use broca_module::registry::{
        ActorHandle, LineageKey, LineageOpener, OpenError, RegistryHandle,
    };

    struct NeverOpen;
    #[async_trait::async_trait]
    impl LineageOpener for NeverOpen {
        /// A session store whose runtime never opens: this opener keeps no sessions.
        fn store_slot(&self) -> &std::sync::Arc<broca_module::store_open::StoreSlot> {
            static SLOT: std::sync::OnceLock<std::sync::Arc<broca_module::store_open::StoreSlot>> =
                std::sync::OnceLock::new();
            SLOT.get_or_init(|| {
                std::sync::Arc::new(broca_module::store_open::StoreSlot::awaiting_key(
                    std::path::Path::new("/nonexistent-broca-test-store"),
                    1,
                ))
            })
        }
        async fn open(
            &self,
            _: &LineageKey,
            _: u64,
            _: RegistryHandle,
        ) -> Result<ActorHandle, OpenError> {
            panic!("inventory must not open an actor")
        }
    }
    struct NoDoor;
    #[async_trait::async_trait]
    impl SessionDoor for NoDoor {
        async fn durable_offset(&self, _: &str) -> Result<u64, SessionDoorError> {
            panic!("inventory must not cover remote data")
        }
        async fn export(&self, _: &str, _: &[u8]) -> Result<(), SessionDoorError> {
            panic!("inventory must not export remote data")
        }
        async fn read(&self, _: &str, _: u64, _: &str) -> Result<DoorRead, SessionDoorError> {
            panic!("inventory must not read remote data")
        }
    }

    struct PortInventory {
        db: Arc<broca_store::SqliteStore>,
        repairs: std::sync::atomic::AtomicUsize,
    }
    impl SessionRefIndex for PortInventory {
        fn record_session(&self, address: &str, session: &BindIdentity) -> Result<(), String> {
            self.db.record_session(address, session)
        }
    }
    impl SessionRefResolver for PortInventory {
        fn resolve_refs(&self, refs: &[&str]) -> Result<broca_core::SessionRefs, String> {
            self.db.resolve_refs(refs)
        }
        fn named_refs(&self, name: &str, limit: u32) -> Result<broca_core::SessionRefs, String> {
            self.db.named_refs(name, limit)
        }
    }
    impl InventoryPort for PortInventory {
        fn rebuild_inventory(&self, root: &std::path::Path) -> Result<(), String> {
            self.repairs.fetch_add(1, Ordering::SeqCst);
            InventoryPort::rebuild_inventory(self.db.as_ref(), root)
        }
    }

    struct UnusedReadiness;
    #[async_trait::async_trait]
    impl broca_module::registry::SessionInventoryReadiness for UnusedReadiness {
        fn start(&self) {
            panic!("the registered readiness task must be shared")
        }
        async fn wait(&self) -> Result<(), broca_store::session_address::SessionAddressError> {
            panic!("the registered readiness task must be shared")
        }
    }

    #[tokio::test]
    async fn with_ports_shares_signalled_readiness_and_repairs_mid_run_gaps() {
        let root = broca_test_support::scratch_root()
            .join(format!("port-inventory-readiness-{}", std::process::id()));
        std::fs::create_dir_all(&root).unwrap();
        let db = Arc::new(broca_store::SqliteStore::open(root.join("run-index.db")).unwrap());
        let inventory = Arc::new(PortInventory {
            db: db.clone(),
            repairs: 0.into(),
        });
        let registry = LineageRegistry::new(Arc::new(NeverOpen));
        let service = AgentDataService::with_ports(
            root.clone(),
            registry.clone(),
            inventory.clone(),
            Arc::new(NoDoor),
        );
        let known = BindIdentity::new("/project", "runner", "known");
        inventory
            .record_session(&broca_core::session_addr(&known), &known)
            .unwrap();
        assert!(!inventory.resolve_refs(&[]).unwrap().complete);
        assert_eq!(inventory.named_refs("known", 1).unwrap().sessions.len(), 1);
        service.reconcile_boot().await.unwrap();
        let readiness = registry.set_session_inventory_readiness(Arc::new(UnusedReadiness));
        assert!(
            Arc::ptr_eq(&readiness, &service.inventory_readiness),
            "with_ports did not install the registry's readiness task"
        );
        readiness.wait().await.unwrap();
        assert_eq!(inventory.repairs.load(Ordering::SeqCst), 1);
        assert!(inventory.resolve_refs(&[]).unwrap().complete);
        std::fs::write(root.join(broca_core::SESSION_PROJECTION_INCOMPLETE), b"").unwrap();
        assert!(!inventory.resolve_refs(&[]).unwrap().complete);
        readiness.start();
        let waits = (0..16).map(|_| readiness.wait());
        for result in futures::future::join_all(waits).await {
            result.unwrap();
        }
        assert_eq!(
            inventory.repairs.load(Ordering::SeqCst),
            2,
            "mid-run waiters must share a single repair"
        );
        assert!(inventory.resolve_refs(&[]).unwrap().complete);
        assert!(!root
            .join(broca_core::SESSION_PROJECTION_INCOMPLETE)
            .exists());
        assert!(root.join("run-index.db").exists());
    }

    #[test]
    fn boot_and_move_discovery_use_the_selected_inventory_port() {
        {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            let root = runtime.block_on(async {
                let root = broca_test_support::scratch_root().join(format!(
                    "agent-inventory-port-{}-legacy",
                    std::process::id()
                ));
                std::fs::create_dir_all(&root).unwrap();
                let registry = LineageRegistry::new(Arc::new(NeverOpen));
                let (index, service): (Arc<dyn InventoryPort>, AgentDataService) = {
                    let db = Arc::new(
                        broca_store::SqliteStore::open(root.join("run-index.db")).unwrap(),
                    );
                    (
                        db.clone(),
                        AgentDataService::new(root.clone(), registry, db, Arc::new(NoDoor)),
                    )
                };
                let session = BindIdentity::new("/project", "runner", "moved");
                index
                    .record_session(&broca_core::session_addr(&session), &session)
                    .unwrap();
                let refs: Refs =
                    serde_json::from_value(serde_json::json!([contract::session_reference(
                        "runner", "/project", "moved"
                    )]))
                    .unwrap();
                let driver = Driver {
                    registry: LineageRegistry::new(Arc::new(NeverOpen)),
                    root: root.clone(),
                    index: index.clone(),
                    door: Arc::new(NoDoor),
                };
                assert!(!driver.local_sessions(&refs).unreadable.is_empty());
                index.rebuild_inventory(&root).unwrap();
                service.reconcile_boot().await.unwrap();
                let found = driver.local_sessions(&refs);
                assert!(found.unreadable.is_empty());
                assert_eq!(found.sessions.len(), 1);
                drop(service);
                drop(driver);
                drop(index);
                root
            });
            // Drain the spawned inventory readiness work so SQLite files are closed on Windows.
            drop(runtime);
            std::fs::remove_dir_all(root).unwrap();
        }
    }
}
