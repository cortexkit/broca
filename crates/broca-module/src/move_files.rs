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
use serde_json::{json, Value};
use std::{
    collections::HashMap,
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
    /// The door's sealed digest is mandatory on receiver plans; a claimed item
    /// digest alone cannot authorize importing bytes read from the door.
    pub sealed_digest: Option<String>,
    pub format: String,
    pub lineage_id: String,
    /// Verified archive-member bytes, never an archive container.
    pub local_archive: Option<Vec<u8>>,
}

#[derive(Clone)]
pub struct MoveObservation {
    pub reference: String,
    pub busy_reason: Option<String>,
    pub moving_cut: Option<String>,
    pub moved_holder: Option<String>,
    pub open_actor: bool,
}

/// The service bridge resolves refs over the store's live and archived sessions.
/// For receiver operations, it checks format before reading the door, requires
/// the door's sealed digest, and derives identities from verified RunStarted.
#[async_trait::async_trait]
pub trait MoveDriver: Send + Sync {
    async fn plan(
        &self,
        cut_id: &str,
        refs: &Refs,
        op: Op,
    ) -> Result<Vec<PlannedSession>, ErrorBody>;
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
fn cut_id(request: &Request) -> Result<String, ErrorBody> {
    request.params()["cutId"]
        .as_str()
        .map(String::from)
        .ok_or_else(|| unavailable("cut request has no cutId"))
}
impl FileCutArtifacts {
    pub fn new(
        registry: LineageRegistry,
        wal: Arc<FileWalStore>,
        export: Arc<dyn ExportIndex>,
        driver: Arc<dyn MoveDriver>,
    ) -> Self {
        let root = wal.root().parent().unwrap_or(std::path::Path::new("."));
        Self {
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
            .map(|p| Ok((self.staged(cut_id, p)?, self.wal.wal_path(&p.session))))
            .collect()
    }
    fn owned_freeze(
        &self,
        cut_id: &str,
        plan: &PlannedSession,
        required: bool,
    ) -> Result<(), ErrorBody> {
        let live = self.wal.wal_path(&plan.session);
        if let Some(moved) =
            migration::read_json::<Moved>(&live.with_extension("moved")).map_err(unavailable)?
        {
            return Err(refusal(
                "session_moved",
                "session moved to another holder",
                json!({"holder":moved.holder}),
            ));
        }
        match migration::read_json::<Freeze>(&live.with_extension("moving")).map_err(unavailable)? {
            Some(freeze) if freeze.cut_id != cut_id => Err(refusal(
                "busy",
                "session is frozen by another cut",
                json!({"reason":"moving","cutId":freeze.cut_id}),
            )),
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
    async fn lock_refs(
        &self,
        cut_id: &str,
        refs: &Refs,
        op: Op,
    ) -> Result<Box<dyn CutGuard>, ErrorBody> {
        let mut plans = self.driver.plan(cut_id, refs, op).await?;
        let references = serde_json::to_value(refs).map_err(unavailable)?;
        let wanted = references
            .as_array()
            .ok_or_else(|| unavailable("invalid ref set"))?;
        if wanted.len() != plans.len()
            || wanted.iter().any(|r| {
                !plans
                    .iter()
                    .any(|p| r["ref_kind"] == "broca-session" && r["ref"] == p.reference)
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
            .map(|r| {
                plans
                    .iter()
                    .find(|p| r["ref"] == p.reference)
                    .unwrap()
                    .clone()
            })
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
                if self
                    .wal
                    .resolve_live_path(&plan.session)
                    .map_err(unavailable)?
                    == Some(self.wal.ephemeral_wal_path(&plan.session))
                {
                    return Err(refusal(
                        "not_exportable",
                        "retention session cannot be moved",
                        Value::Null,
                    ));
                }
            }
        }
        if op != Op::Abort {
            if let Some(reason) = locked.busy {
                return Err(refusal(
                    "busy",
                    "session cannot move while work is pending",
                    json!({"reason":reason}),
                ));
            }
        }
        if matches!(op, Op::Import | Op::Activate) && locked.open_actor {
            return Err(refusal(
                "busy",
                "receiver session has an open actor",
                json!({"reason":"actor_open"}),
            ));
        }
        for plan in &locked.plans {
            let live = self.wal.wal_path(&plan.session);
            if let Some(other) =
                migration::cut_fence(self.wal.root(), &plan.reference).map_err(unavailable)?
            {
                if other != cut_id {
                    return Err(refusal(
                        "busy",
                        "another cut holds the session fence",
                        json!({"reason":"moving","cutId":other}),
                    ));
                }
            }
            if matches!(op, Op::Prepare | Op::Export) {
                self.owned_freeze(&cut_id, plan, op == Op::Export)?;
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
                if self
                    .wal
                    .resolve_live_path(&plan.session)
                    .map_err(unavailable)?
                    == Some(self.wal.ephemeral_wal_path(&plan.session))
                {
                    return Err(refusal(
                        "not_exportable",
                        "retention session cannot be moved",
                        Value::Null,
                    ));
                }
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
                    return Err(refusal(
                        "busy",
                        "source WAL changed before freeze",
                        Value::Null,
                    ));
                }
                let records =
                    broca_wal::file_wal::read_verified_member_range(&plan.bytes, 0, 0, None)
                        .map_err(unavailable)?;
                if let broca_wal::WalRangeRecords::Complete(records) = records.records() {
                    let raw = records.iter().map(|r| r.record.clone()).collect::<Vec<_>>();
                    if !broca_core::pending_submissions(&raw).is_empty() {
                        return Err(refusal(
                            "busy",
                            "source has queued submissions",
                            json!({"reason":"send_queued"}),
                        ));
                    }
                    if records.iter().any(|r| match &r.record {
                        broca_wal::WalRecord::RunStarted { run_id, .. } => {
                            broca_core::reconcile_run_state(&raw, &run_id.0)
                                .is_none_or(|s| !s.state.is_terminal())
                        }
                        _ => false,
                    }) {
                        return Err(refusal(
                            "busy",
                            "source has an unfinished run",
                            json!({"reason":"run_active"}),
                        ));
                    }
                    if records.iter().any(|r| match &r.record {
                        broca_wal::WalRecord::RunStarted { retention, .. } => retention.is_some(),
                        _ => false,
                    }) {
                        return Err(refusal(
                            "not_exportable",
                            "retention session cannot be moved",
                            Value::Null,
                        ));
                    }
                }
            }
            if op == Op::Release {
                let moved = migration::read_json::<Moved>(&live.with_extension("moved"))
                    .map_err(unavailable)?;
                if let Some(moved) = moved {
                    if moved.cut_id != cut_id || request.params()["holder"] != moved.holder {
                        return Err(refusal(
                            "session_moved",
                            "session moved to another holder",
                            json!({"holder":moved.holder}),
                        ));
                    }
                } else {
                    self.owned_freeze(&cut_id, plan, true)?;
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
                    return Err(refusal("busy", "receiver is frozen by a cut", Value::Null));
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
            move_install::check_installations(&self.installations(&locked.plans, &paths))?;
        }
        Ok(())
    }
    async fn apply(&self, request: &Request, _state: &CutState) -> Result<Value, ErrorBody> {
        let cut_id = cut_id(request)?;
        let locked = self.locked(&cut_id)?;
        let op = request.op();
        let mut offsets = Vec::new();
        match op {
            Op::Prepare => {
                for plan in &locked.plans {
                    migration::freeze(
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
                    migration::release(&self.wal.wal_path(&plan.session), &cut_id, holder)
                        .map_err(unavailable)?
                        .map_err(unavailable)?;
                }
            }
            Op::Import => {
                for plan in &locked.plans {
                    migration::stage(&self.staged(&cut_id, plan)?, &plan.bytes, &plan.digest)
                        .map_err(unavailable)?;
                }
            }
            Op::Activate => {
                let paths = self.paths(&cut_id, &locked.plans)?;
                move_install::install_verified(
                    &cut_id,
                    &self.installations(&locked.plans, &paths),
                    self.export.as_ref(),
                    |session| self.driver.drop_projection(session),
                )?;
            }
            Op::Abort => {
                self.rollback(request, _state).await?;
            }
            Op::Describe | Op::Busy | Op::Diverged | Op::Status => {}
        }
        self.driver
            .reply(op, &locked.plans, &offsets, &locked.observations)
    }
    async fn rollback(&self, request: &Request, _state: &CutState) -> Result<(), ErrorBody> {
        let cut_id = cut_id(request)?;
        let locked = self.locked(&cut_id)?;
        for plan in &locked.plans {
            migration::abort_freeze(&self.wal.wal_path(&plan.session), &cut_id)
                .map_err(unavailable)?;
            migration::unlink(&self.staged(&cut_id, plan)?).map_err(unavailable)?;
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
                            && migration::digest(&bytes) == plan.digest => {}
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
