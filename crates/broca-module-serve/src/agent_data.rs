//! Dispatch internal agent-data RPCs and resolve references to session WALs.
//! broca-module persists move progress, fences actor writes and keeps imported
//! runs from being billed again; this bridge reuses those durable operations.
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
    sync::Arc,
    time::Duration,
};

use broca_core::FileWalStore;
use broca_module::{
    cut_state::{refusal, CutStore},
    move_files::{FileCutArtifacts, MoveDriver, MoveObservation, PlannedSession},
    move_install, LineageRegistry,
};
use broca_protocol::BindIdentity;
use broca_subc::session_door::{DoorRead, SessionDoorClient, SessionDoorError, MAX_READ_BYTES};
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
            MAX_READ_BYTES,
            cut_id,
            Instant::now() + Duration::from_secs(60),
        )
        .await
    }
}

pub struct AgentDataService {
    cuts: CutStore,
    artifacts: FileCutArtifacts,
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
            cuts: CutStore::new(root),
            artifacts: FileCutArtifacts::new(registry, wal, index, driver),
        }
    }
    pub async fn reconcile_boot(&self) -> Result<(), ErrorBody> {
        self.cuts.reconcile_boot(&self.artifacts).await
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
        session: Option<&BindIdentity>,
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
        let session = session
            .ok_or_else(|| unavailable("activation has neither a staged nor installed WAL"))?;
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
    fn local_sessions(
        &self,
        cut_id: &str,
        op: Op,
    ) -> Result<BTreeMap<String, BindIdentity>, ErrorBody> {
        let mut sessions = BTreeMap::new();
        for session in self.index.retained_sessions().map_err(unavailable)? {
            sessions.insert(
                contract::session_ref(
                    &session.harness,
                    &session.project_root.to_string_lossy(),
                    &session.session,
                ),
                session,
            );
        }
        for address in self.wal.addresses().map_err(unavailable)? {
            let records = self
                .wal
                .read_address_records_for_move(&address)
                .map_err(unavailable)?;
            for record in records {
                if let broca_wal::WalRecord::RunStarted { session, .. } = record.record {
                    sessions.insert(
                        contract::session_ref(
                            &session.harness,
                            &session.project_root.to_string_lossy(),
                            &session.session,
                        ),
                        session,
                    );
                }
            }
        }
        // An abort can arrive before activation, when the only local identity is
        // in this cut's staged WAL. It must not need a working door to clean up.
        if !cut_id.is_empty() {
            let activation_step = if op == Op::Activate {
                let state: Option<broca_module::cut_state::CutState> =
                    broca_wal::migration::read_json(&CutStore::new(&self.root).path(cut_id))
                        .map_err(unavailable)?;
                Some(
                    state
                        .ok_or_else(|| unavailable("activation has no cut state"))?
                        .step,
                )
            } else {
                None
            };
            let dir = self.root.join("wal-staging").join(cut_id);
            match std::fs::read_dir(dir) {
                Ok(entries) => {
                    for entry in entries {
                        let path = entry.map_err(unavailable)?.path();
                        if path.extension().is_none_or(|e| e != "wal") {
                            continue;
                        }
                        let bytes = std::fs::read(&path).map_err(unavailable)?;
                        let range = match broca_wal::file_wal::read_verified_member_range(
                            &bytes, 0, 0, None,
                        ) {
                            Ok(range) => range,
                            Err(error) => {
                                if op == Op::Abort {
                                    continue;
                                }
                                if let Some(step) = activation_step {
                                    return Err(broca_module::move_files::staging_digest_failure(
                                        cut_id,
                                        step,
                                        path.file_stem()
                                            .and_then(|s| s.to_str())
                                            .unwrap_or("staged-session"),
                                    ));
                                }
                                return Err(unavailable(error));
                            }
                        };
                        if let broca_wal::WalRangeRecords::Complete(records) = range.records() {
                            for record in records {
                                if let broca_wal::WalRecord::RunStarted { session, .. } =
                                    &record.record
                                {
                                    sessions.insert(
                                        contract::session_ref(
                                            &session.harness,
                                            &session.project_root.to_string_lossy(),
                                            &session.session,
                                        ),
                                        session.clone(),
                                    );
                                }
                            }
                        }
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(unavailable(e)),
            }
        }
        Ok(sessions)
    }
}

#[async_trait::async_trait]
impl MoveDriver for Driver {
    async fn plan(
        &self,
        cut_id: &str,
        refs: &Refs,
        op: Op,
    ) -> Result<Vec<PlannedSession>, ErrorBody> {
        let receiver = matches!(op, Op::Diverged | Op::Import | Op::Activate);
        let sessions = if op == Op::Import {
            BTreeMap::new()
        } else {
            self.local_sessions(
                if matches!(op, Op::Busy | Op::Diverged) {
                    ""
                } else {
                    cut_id
                },
                op,
            )?
        };
        let retained = self.index.retained_sessions().map_err(unavailable)?;
        let mut plans = Vec::new();
        for reference in refs.iter() {
            let session = sessions.get(&reference.r#ref);
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
                let Some(bytes) = self.wal.bytes_for_move(session).map_err(unavailable)? else {
                    continue;
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
            let bytes = if op == Op::Diverged {
                let local = local_probe.expect("held divergence ref has local bytes");
                if broca_module::move_files::retained_move_copy(
                    self.wal.as_ref(),
                    session.expect("held ref resolved"),
                )? {
                    sealed = false;
                    local
                } else {
                    match self.incoming(&reference.r#ref, cut_id).await {
                        Ok(bytes) => bytes,
                        Err(error) => match error.verification_cause {
                            Some(cause) => {
                                tracing::error!(session_ref = %reference.r#ref, cut_id, cause, "agent-data cannot verify the cut for a held ref");
                                sealed = false;
                                local
                            }
                            None => return Err(error.refusal),
                        },
                    }
                }
            } else if op == Op::Activate {
                self.activation_bytes(&reference.r#ref, cut_id, session)?
            } else if receiver {
                self.incoming(&reference.r#ref, cut_id)
                    .await
                    .map_err(|error| error.refusal)?
            } else {
                let session = session.expect("source ref resolved");
                match self.wal.bytes_for_move(session).map_err(unavailable)? {
                    Some(bytes) => bytes,
                    None if op == Op::Abort => {
                        let path = broca_wal::migration::staging_path(
                            self.wal.root(),
                            cut_id,
                            &broca_core::session_addr(session),
                        )
                        .map_err(unavailable)?;
                        match std::fs::read(path) {
                            Ok(bytes) => bytes,
                            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                            Err(e) => return Err(unavailable(e)),
                        }
                    }
                    None => {
                        return Err(refusal(
                            "unknown_ref",
                            "source session has no WAL",
                            json!(reference),
                        ))
                    }
                }
            };
            let (format, lineage_id) = if op == Op::Abort {
                ("broca-wal/v1;features=".into(), String::new())
            } else {
                wal_format(&bytes)?
            };
            let digest = broca_wal::migration::digest(&bytes);
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
            let local_archive = if self
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
            });
        }
        Ok(plans)
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
            Op::Diverged => json!({"refs":plans.iter().map(|p| {
                let state = match broca_module::move_files::retained_move_copy(self.wal.as_ref(), &p.session) {
                    Ok(true) => "advanced",
                    Err(_) => "unknown",
                    Ok(false) if p.sealed_digest.is_none() => "unknown",
                    Ok(false) => match self.wal.bytes_for_move(&p.session) {
                    Ok(local) if p.bytes.starts_with(local.as_deref().unwrap_or_default()) => "at_cut",
                    Ok(_) => "advanced",
                    Err(_) => "unknown",
                    },
                };
                json!({"state":state})
            }).collect::<Vec<_>>()}),
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
