//! Durable authority for moving a fixed set of sessions between machines.
//! Each cut id names a move batch. Its JSON file records the last completed step,
//! the next operation to finish after a crash, and the replies retries replay.
//! Artifact adapters own the affected actors' critical sections until that JSON
//! completion is fsynced. Older reader-only releases inspect pending intents to
//! refuse writes, but never advance these files.
use broca_wire::agent_data::{
    self as contract, errors::ErrorBody, idempotency::Repeat, refs::Refs, request::Request,
    step::Step, Op,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{Mutex as AsyncMutex, OwnedMutexGuard};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Intent {
    pub op: String,
    pub request: Value,
}

fn intent_request(intent: &Intent) -> Result<Request, ErrorBody> {
    let op = match intent.op.as_str() {
        "prepare" => Op::Prepare,
        "export" => Op::Export,
        "release" => Op::Release,
        "import" => Op::Import,
        "activate" => Op::Activate,
        "abort" => Op::Abort,
        _ => return Err(unavailable("invalid state-changing cut intent")),
    };
    Request::decode(op.method(), &intent.request)
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Reply {
    pub request: Value,
    pub value: Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CutState {
    pub step: Step,
    pub refs: Option<Refs>,
    pub intent: Option<Intent>,
    #[serde(default)]
    pub replies: BTreeMap<String, Reply>,
}
impl Default for CutState {
    fn default() -> Self {
        Self {
            step: Step::None,
            refs: None,
            intent: None,
            replies: BTreeMap::new(),
        }
    }
}

pub fn refusal(code: &str, message: impl Into<String>, detail: Value) -> ErrorBody {
    serde_json::from_value(json!({"code":code,"message":message.into(),"detail":detail}))
        .expect("contract error body")
}
fn unavailable(error: impl std::fmt::Display) -> ErrorBody {
    refusal("unavailable", error.to_string(), Value::Null)
}

fn status_value(state: &CutState) -> Result<Value, ErrorBody> {
    let reply: contract::status::StatusReply =
        serde_json::from_value(json!({"step":state.step})).map_err(unavailable)?;
    serde_json::to_value(reply).map_err(unavailable)
}

/// Pause the affected session actors and hold their single-writer WAL leases,
/// acquired in sorted session-ref order. Commands and new actor opens can proceed
/// when this guard drops, so all files and the move's completion must be fsynced first.
pub trait CutGuard: Send {}
impl<T: Send> CutGuard for T {}

/// Filesystem and door operations for one move batch. Admission checks must not
/// mutate. Apply and recovery are idempotent and may touch only this cut's freezes,
/// staging, tombstones and installed WALs. Recovery returns a reply only after
/// checking every session named by the batch, not just its first artifact.
#[async_trait::async_trait]
pub trait CutArtifacts: Send + Sync {
    async fn lock_refs(
        &self,
        cut_id: &str,
        refs: &Refs,
        op: Op,
    ) -> Result<Box<dyn CutGuard>, ErrorBody>;
    async fn check(&self, request: &Request, state: &CutState) -> Result<(), ErrorBody>;
    async fn apply(&self, request: &Request, state: &CutState) -> Result<Value, ErrorBody>;
    async fn rollback(&self, request: &Request, state: &CutState) -> Result<(), ErrorBody>;
    /// Read-only reconciliation may verify files, but may neither repair them
    /// nor advance the step stored in cut JSON. Status reports the recorded step
    /// even while the next operation remains unfinished.
    async fn reconcile(
        &self,
        state: &CutState,
        read_only: bool,
    ) -> Result<Option<Value>, ErrorBody>;
}

pub struct CutStore {
    root: PathBuf,
    locks: Mutex<HashMap<String, Weak<AsyncMutex<()>>>>,
}
impl CutStore {
    pub fn new(state_root: impl Into<PathBuf>) -> Self {
        Self {
            root: state_root.into(),
            locks: Mutex::new(HashMap::new()),
        }
    }
    pub fn path(&self, cut_id: &str) -> PathBuf {
        self.root
            .join("agent-data/cuts")
            .join(format!("{cut_id}.json"))
    }
    fn load(&self, cut_id: &str) -> Result<CutState, ErrorBody> {
        broca_wal::migration::read_json(&self.path(cut_id))
            .map(|s| s.unwrap_or_default())
            .map_err(unavailable)
    }
    fn save(&self, cut_id: &str, state: &CutState) -> Result<(), ErrorBody> {
        broca_wal::migration::atomic_json(&self.path(cut_id), state).map_err(unavailable)
    }
    async fn lock(
        &self,
        cut_id: &str,
    ) -> Result<(OwnedMutexGuard<()>, Box<dyn broca_core::LeaseHandle>), ErrorBody> {
        let lock = {
            let mut locks = self.locks.lock().unwrap();
            locks.retain(|_, lock| lock.strong_count() != 0);
            let lock = locks
                .get(cut_id)
                .and_then(Weak::upgrade)
                .unwrap_or_else(|| Arc::new(AsyncMutex::new(())));
            locks.insert(cut_id.to_owned(), Arc::downgrade(&lock));
            lock
        };
        let guard = lock.lock_owned().await;
        loop {
            match broca_core::lease::acquire_cut(&self.root, cut_id) {
                Ok(lease) => return Ok((guard, lease)),
                Err(broca_core::LeaseError::Held { .. }) => {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await
                }
                Err(error) => return Err(unavailable(error)),
            }
        }
    }
    fn complete(
        &self,
        cut_id: &str,
        state: &mut CutState,
        request: &Request,
        reply: Value,
    ) -> Result<(), ErrorBody> {
        let mut completed = state.clone();
        completed.step = contract::step::advance(request.op(), state.step).map_err(|e| {
            refusal(
                e.code().as_str(),
                "cut step refused",
                json!({"step":state.step,"refs":state.refs}),
            )
        })?;
        completed.replies.insert(
            request.op().method().to_owned(),
            Reply {
                request: request.params(),
                value: reply,
            },
        );
        completed.intent = None;
        if let Err(error) = self.save(cut_id, &completed) {
            // Rename can succeed before its directory barrier fails. Restore the
            // pending intent before releasing sessions, rather than exposing an
            // unconfirmed completion to a reader-only release.
            self.save(cut_id, state)?;
            return Err(error);
        }
        *state = completed;
        Ok(())
    }
    async fn reconcile_locked(
        &self,
        cut_id: &str,
        state: &mut CutState,
        artifacts: &dyn CutArtifacts,
        read_only: bool,
    ) -> Result<(), ErrorBody> {
        if let Some(intent) = &state.intent {
            let request = intent_request(intent)?;
            let reconciled = match artifacts.reconcile(state, read_only).await {
                Ok(reply) => reply,
                Err(error) => {
                    tracing::warn!(
                        cut_id,
                        code = error.code.as_str(),
                        "cut reconciliation remains pending"
                    );
                    None
                }
            };
            if let Some(reply) = reconciled {
                if !read_only {
                    self.complete(cut_id, state, &request, reply)?;
                }
            }
        }
        Ok(())
    }
    fn admission(state: &CutState, request: &Request) -> Result<Option<Value>, ErrorBody> {
        let op = request.op();
        let method = op.method();
        if matches!(op, Op::Busy | Op::Diverged | Op::Describe | Op::Status) {
            return Ok(None);
        }
        if let Some(first) = state.replies.get(method) {
            let original = Request::decode(method, &first.request)?;
            return match contract::idempotency::compare(&original, request) {
                Repeat::Replay => Ok(Some(first.value.clone())),
                changed => Err(refusal(
                    changed.refusal().expect("changed request refusal").as_str(),
                    "cut operation request changed",
                    Value::Null,
                )),
            };
        }
        contract::step::advance(op, state.step).map_err(|e| {
            refusal(
                e.code().as_str(),
                "cut step refused",
                json!({"step":state.step,"refs":state.refs}),
            )
        })?;
        let requested_refs = request.refs();
        if let Some(refs) = &requested_refs {
            contract::step::check_ref_set(op, state.step, state.refs.as_ref(), refs).map_err(
                |detail| {
                    refusal(
                        contract::errors::RefusalCode::InvalidState.as_str(),
                        "cut ref set changed",
                        serde_json::to_value(detail).expect("ref-set refusal detail"),
                    )
                },
            )?;
        }
        Ok(None)
    }
    /// The internal-service handler passes the caller identity authenticated by
    /// the daemon, not a request field. Reject every identity but reserved:entorhinal
    /// before reading cut JSON, so forbidden callers cannot probe move state.
    pub async fn execute(
        &self,
        caller: &str,
        method: &str,
        params: &Value,
        artifacts: &dyn CutArtifacts,
    ) -> Result<Value, ErrorBody> {
        if !contract::caller_admitted(caller) {
            return Err(refusal(
                "caller_forbidden",
                "agent-data caller is not reserved:entorhinal",
                Value::Null,
            ));
        }
        let request = Request::decode(method, params)?;
        let op = request.op();
        if op == Op::Describe {
            return serde_json::to_value(contract::describe_reply()).map_err(unavailable);
        }
        let Some(cut_id) = params.get("cutId").and_then(Value::as_str) else {
            return artifacts.apply(&request, &CutState::default()).await;
        };
        if cut_id.is_empty()
            || !cut_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
        {
            return Err(refusal(
                "invalid_request",
                "cutId is not a safe authority filename",
                Value::Null,
            ));
        }
        // Both the contract decoder and the filename check above validate cutId.
        let _cut_lock = self.lock(cut_id).await?;
        let mut state = self.load(cut_id)?;
        let requested_refs: Option<Refs> = params
            .get("refs")
            .map(|refs| serde_json::from_value(refs.clone()))
            .transpose()
            .map_err(unavailable)?;
        if op == Op::Status {
            let _sessions = match &state.refs {
                Some(refs) => artifacts
                    .lock_refs(
                        cut_id,
                        refs,
                        state
                            .intent
                            .as_ref()
                            .map(intent_request)
                            .transpose()?
                            .map_or(Op::Status, |r| r.op()),
                    )
                    .await
                    .ok(),
                None => None,
            };
            self.reconcile_locked(cut_id, &mut state, artifacts, true)
                .await?;
            return status_value(&state);
        }
        if op == Op::Diverged {
            // A divergence probe is read-only. In particular, an absent local
            // copy must not fetch the cut merely to inspect a pending import.
            Self::reconcile_locked(self, cut_id, &mut state, artifacts, true).await?;
            let refs = request.refs().expect("diverged carries refs");
            let _sessions = artifacts.lock_refs(cut_id, refs, Op::Diverged).await?;
            return artifacts.apply(&request, &state).await;
        }
        // A completed reply and step refusal do not depend on Engram's
        // session-door service or on session availability. A lost-reply retry
        // must not fetch bytes already covered by its durable completion.
        if state.intent.is_none() {
            if let Some(reply) = Self::admission(&state, &request)? {
                return Ok(reply);
            }
        }
        let rolling_back = op == Op::Abort
            && state
                .intent
                .as_ref()
                .is_some_and(|intent| matches!(intent.op.as_str(), "prepare" | "import"));
        let refs = state.refs.as_ref().or(requested_refs.as_ref());
        let _sessions = match refs {
            Some(refs) => Some(
                artifacts
                    .lock_refs(
                        cut_id,
                        refs,
                        if rolling_back {
                            Op::Abort
                        } else {
                            state
                                .intent
                                .as_ref()
                                .map(intent_request)
                                .transpose()?
                                .map_or(op, |r| r.op())
                        },
                    )
                    .await?,
            ),
            None => None,
        };
        if !rolling_back {
            self.reconcile_locked(cut_id, &mut state, artifacts, op == Op::Status)
                .await?;
        }
        if matches!(op, Op::Busy | Op::Diverged | Op::Describe) {
            return artifacts.apply(&request, &state).await;
        }
        if let Some(intent) = &state.intent {
            let pending = intent_request(intent)?;
            if op == Op::Abort && matches!(pending.op(), Op::Prepare | Op::Import) {
                // Record abort before removing files, so restart resumes cleanup
                // rather than recreating freezes or staging under the old operation.
            } else if pending.op() != op {
                // Release and activation cannot be undone. Their recorded step
                // is still the previous step, which alone could wrongly admit abort.
                let code = contract::step::advance(op, state.step)
                    .err()
                    .map_or(contract::errors::RefusalCode::InvalidState, |e| e.code());
                return Err(refusal(
                    code.as_str(),
                    "another cut operation is pending",
                    json!({"step":state.step,"refs":state.refs}),
                ));
            } else if contract::idempotency::compare(&pending, &request) != Repeat::Replay {
                return Err(refusal(
                    "id_reused",
                    "pending operation request changed",
                    Value::Null,
                ));
            }
        }
        if let Some(reply) = Self::admission(&state, &request)? {
            return Ok(reply);
        }
        // Check every refusal under the cut lock and all session critical
        // sections, before publishing an intent or changing a session's artifacts.
        artifacts.check(&request, &state).await?;
        if state.refs.is_none() {
            state.refs = requested_refs;
        }
        state.intent = Some(Intent {
            op: method.rsplit('.').next().unwrap().to_owned(),
            request: request.params(),
        });
        self.save(cut_id, &state)?;
        match artifacts.apply(&request, &state).await {
            Ok(reply) => {
                self.complete(cut_id, &mut state, &request, reply.clone())?;
                Ok(reply)
            }
            Err(error) => {
                match op {
                    Op::Prepare | Op::Import => {
                        artifacts.rollback(&request, &state).await?;
                        state.intent = None;
                        // A refused first prepare/import commits no session-ref
                        // set for this cutId. Forget the failed request's refs
                        // before admitting the next first request.
                        state.refs = None;
                        self.save(cut_id, &state)?;
                    }
                    Op::Export => {
                        state.intent = None;
                        self.save(cut_id, &state)?;
                    }
                    Op::Release | Op::Activate | Op::Abort => {}
                    Op::Describe | Op::Busy | Op::Diverged | Op::Status => {
                        unreachable!("read-only operation")
                    }
                }
                Err(error)
            }
        }
    }
    /// Run before session opening. The service adapter resolves every cut's refs
    /// through the same locked actors used for live operations.
    pub async fn reconcile_boot(&self, artifacts: &dyn CutArtifacts) -> Result<(), ErrorBody> {
        let dir = self.root.join("agent-data/cuts");
        let entries = match std::fs::read_dir(dir) {
            Ok(entries) => entries,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(unavailable(e)),
        };
        for entry in entries {
            let path = entry.map_err(unavailable)?.path();
            if path.extension().is_none_or(|e| e != "json") {
                continue;
            }
            let cut_id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .ok_or_else(|| unavailable("invalid cut filename"))?;
            contract::refs::CutId::new(cut_id).map_err(unavailable)?;
            let _cut_lock = self.lock(cut_id).await?;
            let mut state = self.load(cut_id)?;
            if state.intent.is_none() {
                continue;
            }
            let _sessions = match &state.refs {
                Some(refs) => match artifacts
                    .lock_refs(
                        cut_id,
                        refs,
                        state
                            .intent
                            .as_ref()
                            .map(intent_request)
                            .transpose()?
                            .map_or(Op::Status, |r| r.op()),
                    )
                    .await
                {
                    Ok(guard) => Some(guard),
                    Err(error) => {
                        // Pending intents still fence their sessions. A missing
                        // door or damaged staging must not prevent the service
                        // from answering status or the driver's repeat.
                        tracing::warn!(
                            cut_id,
                            code = error.code.as_str(),
                            "cut boot reconciliation remains pending"
                        );
                        continue;
                    }
                },
                None => None,
            };
            self.reconcile_locked(cut_id, &mut state, artifacts, false)
                .await?;
        }
        Ok(())
    }
    pub fn state_root(&self) -> &Path {
        &self.root
    }
}

#[cfg(test)]
mod tests;
