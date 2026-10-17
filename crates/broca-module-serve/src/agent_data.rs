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

use broca_core::FileWalStore;
use broca_module::{
    cut_state::{refusal, CutStore},
    move_files::{FileCutArtifacts, MoveDriver, MoveObservation, MovePlans, PlannedSession},
    move_install, LineageRegistry,
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
    async fn cover(&self, reference: &str) -> Result<u64, SessionDoorError>;
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
    async fn cover(&self, reference: &str) -> Result<u64, SessionDoorError> {
        self.cover(reference, Instant::now() + Duration::from_secs(60))
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
    cuts: CutStore,
    artifacts: FileCutArtifacts,
    wal: Arc<FileWalStore>,
    op_counts: [AtomicU64; 10],
}
impl AgentDataService {
    pub fn new(
        root: PathBuf,
        registry: LineageRegistry,
        wal: Arc<FileWalStore>,
        index: Arc<broca_store::SqliteStore>,
        door: Arc<dyn SessionDoor>,
    ) -> Self {
        let driver = Arc::new(Driver {
            root: root.clone(),
            wal: wal.clone(),
            index: index.clone(),
            door,
        });
        Self {
            cuts: CutStore::new(&root),
            artifacts: FileCutArtifacts::new(root.clone(), registry, wal.clone(), index, driver),
            wal,
            op_counts: std::array::from_fn(|_| AtomicU64::new(0)),
        }
    }
    pub fn start_inventory(&self) {
        // The store may outlive this service and be shared with another one.
        // A service drop must not cancel their one-shot boot inventory.
        self.wal
            .start_move_inventory(tokio_util::sync::CancellationToken::new());
    }
    pub async fn wait_inventory(&self) {
        self.start_inventory();
        self.wal.wait_move_inventory().await;
    }
    pub async fn reconcile_boot(&self) -> Result<(), ErrorBody> {
        self.start_inventory();
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

pub(crate) fn inventory_health(service: Option<&AgentDataService>) -> Value {
    service.map_or(Value::Null, |service| {
        let (ready, unreadable) = service.wal.move_inventory_health();
        json!({"ready": ready, "unreadable": unreadable})
    })
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
    root: PathBuf,
    wal: Arc<FileWalStore>,
    index: Arc<broca_store::SqliteStore>,
    door: Arc<dyn SessionDoor>,
}

#[derive(Default)]
struct SessionInventory {
    sessions: BTreeMap<String, BindIdentity>,
    unreadable: Vec<String>,
}

fn door_refusal(error: SessionDoorError) -> ErrorBody {
    let code = match error.door_code.as_deref() {
        Some("not_found") => "cut_not_ready",
        Some("verify_failed") => "digest_mismatch",
        _ => "unavailable",
    };
    refusal(code, error.to_string(), Value::Null)
}

struct CutReadFailure {
    refusal: ErrorBody,
    verification_cause: Option<&'static str>,
}
impl From<ErrorBody> for CutReadFailure {
    fn from(refusal: ErrorBody) -> Self {
        Self {
            refusal,
            verification_cause: None,
        }
    }
}

/// The door carries opaque WAL bytes, not a participant format. Inspect only the
/// verified frame envelope here, before publishing staging or installing a WAL.
/// Keeping this separate permits an advertised format to replace inspection later.
fn wal_format(bytes: &[u8]) -> Result<(String, String), ErrorBody> {
    use broca_wal::framing::{self, DecodeStep};
    let mut cursor = bytes;
    let mut version = framing::FRAMING_VERSION;
    let mut features = BTreeSet::new();
    let mut lineage = None;
    while !cursor.is_empty() {
        let consumed = match framing::decode_next(cursor) {
            DecodeStep::Record { consumed, .. } => {
                version = version.max(cursor[4]);
                if cursor[4] == framing::GATED_FRAMING_VERSION {
                    let envelope: Value =
                        serde_json::from_slice(&cursor[framing::HEADER_LEN..consumed])
                            .map_err(unavailable)?;
                    let requires: Vec<String> =
                        serde_json::from_value(envelope["requires"].clone())
                            .map_err(unavailable)?;
                    features.extend(requires);
                }
                consumed
            }
            DecodeStep::Lineage {
                lineage_id,
                consumed,
            } => {
                lineage = Some(lineage_id.to_string());
                consumed
            }
            DecodeStep::NewerWriter { ver, missing } => {
                return Err(refusal(
                    "incompatible_version",
                    "WAL cut requires unknown reader features",
                    json!({"found":format!("broca-wal/v{ver};features={}",missing.join(",")),"required":format!("broca-wal/v3;features={}",framing::KNOWN_FEATURES.join(","))}),
                ))
            }
            DecodeStep::Corrupt(framing::FrameError::UnsupportedVersion { ver }) => {
                return Err(refusal(
                    "incompatible_version",
                    "WAL cut requires an unknown frame version",
                    json!({"found":format!("broca-wal/v{ver}"),"required":"broca-wal/v3"}),
                ))
            }
            DecodeStep::Corrupt(error) => return Err(unavailable(error)),
            DecodeStep::TornTail => {
                return Err(unavailable("cut WAL contains an incomplete frame"))
            }
        };
        cursor = &cursor[consumed..];
    }
    let format = format!(
        "broca-wal/v{version};features={}",
        features.into_iter().collect::<Vec<_>>().join(",")
    );
    move_install::check_format(&format)?;
    Ok((
        format,
        lineage.ok_or_else(|| unavailable("cut WAL has no lineage identity"))?,
    ))
}

impl Driver {
    fn activation_bytes(
        &self,
        reference: &str,
        cut_id: &str,
        session: &BindIdentity,
    ) -> Result<Vec<u8>, ErrorBody> {
        let state: broca_module::cut_state::CutState =
            broca_wal::migration::read_json(&CutStore::new(&self.root).path(cut_id))
                .map_err(unavailable)?
                .ok_or_else(|| unavailable("activation has no cut state"))?;
        let reply = state
            .replies
            .get("agent_data.import")
            .ok_or_else(|| unavailable("activation has no imported reply"))?;
        let items: contract::import::ImportReply =
            serde_json::from_value(reply.value.clone()).map_err(unavailable)?;
        let item = items
            .items
            .iter()
            .find(|item| item.session_ref == reference)
            .ok_or_else(|| unavailable("imported reply does not cover the activation ref"))?;
        let path = broca_wal::migration::staging_path(
            self.wal.root(),
            cut_id,
            &broca_core::session_addr(session),
        )
        .map_err(unavailable)?;
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut bytes = self
                    .wal
                    .bytes_for_move(session)
                    .map_err(unavailable)?
                    .ok_or_else(|| unavailable("activation WAL is missing"))?;
                let end = usize::try_from(item.end_offset).map_err(unavailable)?;
                if end <= bytes.len() {
                    bytes.truncate(end);
                }
                bytes
            }
            Err(e) => return Err(unavailable(e)),
        };
        move_install::verify_received(reference, &bytes, item.end_offset, &item.digest).map_err(
            |error| {
                broca_module::move_files::activation_refusal(cut_id, state.step, reference, error)
            },
        )?;
        Ok(bytes)
    }
    async fn incoming(&self, reference: &str, cut_id: &str) -> Result<Vec<u8>, CutReadFailure> {
        let mut bytes = Vec::new();
        let mut metadata = None;
        loop {
            let page = self
                .door
                .read(reference, bytes.len() as u64, cut_id)
                .await
                .map_err(|error| {
                    let verification_cause = (error.door_code.as_deref() == Some("verify_failed"))
                        .then_some("verify_failed");
                    CutReadFailure {
                        refusal: door_refusal(error),
                        verification_cause,
                    }
                })?;
            if page.stream_end > self.wal.max_read_bytes() {
                return Err(unavailable("sealed cut exceeds the session WAL size limit").into());
            }
            let seal = (page.stream_end, page.digest.clone());
            if metadata.as_ref().is_some_and(|previous| previous != &seal) {
                return Err(unavailable("sealed door metadata changed between pages").into());
            }
            if page.digest.is_empty() {
                return Err(unavailable("door did not return a sealed digest").into());
            }
            let complete = page.end_offset == page.stream_end;
            metadata = Some(seal);
            bytes.extend_from_slice(&page.bytes);
            if complete {
                if broca_wal::migration::digest(&bytes) != page.digest {
                    return Err(CutReadFailure {
                        refusal: refusal(
                            "digest_mismatch",
                            "WAL bytes differ from the door's sealed digest",
                            Value::Null,
                        ),
                        verification_cause: Some("sealed SHA mismatch"),
                    });
                }
                return Ok(bytes);
            }
        }
    }
    fn local_sessions(&self, refs: &Refs) -> SessionInventory {
        let inventory = self
            .wal
            .move_session_refs(refs.iter().map(|r| r.r#ref.as_str()));
        SessionInventory {
            sessions: inventory
                .sessions
                .into_iter()
                .map(|(reference, (_, session))| (reference, session))
                .collect(),
            unreadable: inventory.unreadable,
        }
    }
}

#[async_trait::async_trait]
impl MoveDriver for Driver {
    async fn plan(&self, cut_id: &str, refs: &Refs, op: Op) -> Result<MovePlans, ErrorBody> {
        // Empty move probes must not depend on inventory readiness or touch WALs.
        if refs.iter().next().is_none() {
            return Ok(MovePlans::default());
        }
        let receiver = matches!(op, Op::Diverged | Op::Import | Op::Activate);
        let state: Option<broca_module::cut_state::CutState> = if op == Op::Busy {
            None
        } else {
            let path = CutStore::new(&self.root).path(cut_id);
            broca_wal::migration::read_json(&path).map_err(|error| {
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
        let mut inventory = if bound {
            SessionInventory {
                sessions: state.expect("bound cut exists").sessions,
                unreadable: Vec::new(),
            }
        } else if matches!(op, Op::Busy | Op::Diverged | Op::Prepare | Op::Import) {
            self.local_sessions(refs)
        } else {
            SessionInventory::default()
        };
        let retained = if op == Op::Prepare && !bound {
            self.index.retained_sessions().map_err(unavailable)?
        } else {
            Vec::new()
        };
        if inventory.unreadable.is_empty() {
            for session in &retained {
                inventory
                    .sessions
                    .entry(contract::session_ref(
                        &session.harness,
                        &session.project_root.to_string_lossy(),
                        &session.session,
                    ))
                    .or_insert_with(|| session.clone());
            }
        }
        let sessions = inventory.sessions;
        let unreadable = inventory.unreadable;
        let mut unknown_refs = BTreeSet::new();
        let mut plans = Vec::new();
        for reference in refs.iter() {
            let session = sessions.get(&reference.r#ref);
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
                        "cannot resolve ref {} while WAL addresses are unreadable: {}",
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
            let local_probe = if op == Op::Diverged {
                let Some(session) = session else {
                    continue;
                };
                let bytes = match self.wal.bytes_for_move(session) {
                    Ok(Some(bytes)) => bytes,
                    Ok(None) => continue,
                    Err(_) => {
                        tracing::error!(session_ref=%reference.r#ref,address=%broca_core::session_addr(session),"agent-data cannot inspect the requested local copy");
                        unknown_refs.insert(reference.r#ref.clone());
                        continue;
                    }
                };
                Some(bytes)
            } else {
                None
            };
            if op == Op::Prepare
                && session.is_some_and(|s| retained.contains(s))
                && self
                    .wal
                    .bytes_for_move(session.expect("retained session resolved"))
                    .map_err(unavailable)?
                    .is_none()
            {
                return Err(refusal(
                    "not_exportable",
                    "retention session must not leave this Mac",
                    Value::Null,
                ));
            }
            let mut sealed = receiver;
            let mut divergence = None;
            let mut divergence_reason = None;
            let bytes = if op == Op::Diverged {
                let local = local_probe.expect("held divergence ref has local bytes");
                match broca_module::move_files::retained_move_copy_from_bytes(
                    self.wal.as_ref(),
                    session.expect("held ref resolved"),
                    &local,
                ) {
                    Ok(true) => {
                        divergence = Some(broca_module::move_files::LocalDivergence::Advanced);
                        sealed = false;
                        local
                    }
                    Err(_) => {
                        unknown_refs.insert(reference.r#ref.clone());
                        continue;
                    }
                    Ok(false) => {
                        match self.incoming(&reference.r#ref, cut_id).await {
                            Ok(bytes) => {
                                divergence = Some(if bytes.starts_with(&local) {
                                    broca_module::move_files::LocalDivergence::AtCut
                                } else {
                                    broca_module::move_files::LocalDivergence::Advanced
                                });
                                bytes
                            }
                            Err(error) => match error.verification_cause {
                                Some(cause) => {
                                    // Divergence entries carry only a state, not a
                                    // per-ref message. Preserve the safe cause for the
                                    // answer without logging WAL content.
                                    tracing::error!(session_ref = %reference.r#ref, cut_id, cause, "agent-data cannot verify the cut for a held ref");
                                    sealed = false;
                                    divergence =
                                        Some(broca_module::move_files::LocalDivergence::Unknown);
                                    divergence_reason = Some(cause.to_owned());
                                    local
                                }
                                None => return Err(error.refusal),
                            },
                        }
                    }
                }
            } else if op == Op::Activate {
                self.activation_bytes(
                    &reference.r#ref,
                    cut_id,
                    session.expect("recorded activation address checked"),
                )?
            } else if op == Op::Abort {
                Vec::new()
            } else if receiver {
                self.incoming(&reference.r#ref, cut_id)
                    .await
                    .map_err(|error| error.refusal)?
            } else {
                let session = session.expect("source ref resolved");
                match self.wal.bytes_for_move(session).map_err(unavailable)? {
                    Some(bytes) => bytes,
                    None => {
                        return Err(refusal(
                            "unknown_ref",
                            "source session has no WAL",
                            json!(reference),
                        ))
                    }
                }
            };
            let (format, lineage_id) = if matches!(op, Op::Abort | Op::Busy) {
                ("broca-wal/v1;features=".into(), String::new())
            } else {
                wal_format(&bytes)?
            };
            let digest = if op == Op::Busy {
                String::new()
            } else {
                broca_wal::migration::digest(&bytes)
            };
            let session = if receiver && sealed {
                move_install::verify_received(
                    &reference.r#ref,
                    &bytes,
                    bytes.len() as u64,
                    &digest,
                )?
            } else {
                session.expect("local ref resolved").clone()
            };
            let local_archive = if matches!(op, Op::Abort | Op::Busy | Op::Diverged) {
                None
            } else if self
                .wal
                .resolve_live_path(&session)
                .map_err(unavailable)?
                .is_none()
            {
                self.wal.bytes_for_move(&session).map_err(unavailable)?
            } else {
                None
            };
            plans.push(PlannedSession {
                reference: reference.r#ref.clone(),
                session,
                end_offset: bytes.len() as u64,
                sealed_digest: sealed.then(|| digest.clone()),
                digest,
                bytes,
                format,
                lineage_id,
                local_archive,
                divergence,
                divergence_reason,
            });
        }
        Ok(MovePlans {
            sessions: plans,
            unknown_refs,
        })
    }
    async fn cover(&self, reference: &str) -> Result<u64, ErrorBody> {
        self.door.cover(reference).await.map_err(unavailable)
    }
    async fn export(&self, reference: &str, frozen: &[u8]) -> Result<(), ErrorBody> {
        self.door
            .export(reference, frozen)
            .await
            .map_err(unavailable)
    }
    fn reply(
        &self,
        op: Op,
        plans: &[PlannedSession],
        _offsets: &[u64],
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
                match source_busy_reason(&plan.bytes) {
                    Ok(Some(reason)) => json!({"state":"busy","reason_code":reason,"reason":format!("session has pending {reason}")}),
                    Ok(None) => json!({"state":"idle"}),
                    Err(_) => json!({"state":"unknown"}),
                }
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
    fn drop_projection(&self, session: &BindIdentity) -> Result<(), String> {
        self.index.drop_session_projection(session)
    }
}

fn source_busy_reason(bytes: &[u8]) -> Result<Option<&'static str>, ErrorBody> {
    let range =
        broca_wal::file_wal::read_verified_member_range(bytes, 0, 0, None).map_err(unavailable)?;
    broca_module::move_files::source_busy_reason(&range)
}
