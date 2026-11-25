//! `session.restore_from_backup`: restores sessions kept in the session store after their files
//! came back from a backup (see `broca_session_store::session::restore`, which decides and
//! writes). Encrypted sessions need the deployment key to be checked, which only this process
//! reads, so the restore runs in the daemon.
//!
//! Each session asked for is first checked without holding it. A session whose head and
//! everything it names are there is reported kept and its session data is never written. An
//! unfinished kept session is registered for background recovery. A session that
//! lacks a file is held with [`LineageRegistry::fence_for_restore`]: an idle actor serving it
//! closes, a busy one refuses, and no other owner holds its lease. Then it is restored, which checks it again. Normal loading
//! never falls back to an older head; only this operator operation does.
//!
//! Every session gets one report: its class, address and identity, its outcome, the facts behind
//! it, and a `report` line an operator reads.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use broca_module::registry::store_move::{FenceRefusal, MoveRefusal};
use broca_module::registry::store_restore::StoreRestorer;
use broca_module::registry::LineageKey;
use broca_module::store_open::StoreSlot;
use broca_module::LineageRegistry;
use broca_protocol::BindIdentity;
use broca_records::session_storage::StorageClass;
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::codec::hex;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::restore::{self, Finding, RestoreError, Restored};
use serde_json::{json, Value};

/// What runs `session.restore_from_backup` for a daemon's new storage.
pub struct StoreRestore {
    registry: LineageRegistry,
    state_root: PathBuf,
    slot: Arc<StoreSlot>,
    leases: Arc<dyn broca_core::LeaseStore>,
}

impl StoreRestore {
    /// The restore of the session store under `state_root`, opened with `slot`'s runtime, holding
    /// a session through `registry` and `leases` while it writes.
    pub fn new(
        registry: LineageRegistry,
        state_root: &Path,
        slot: Arc<StoreSlot>,
        leases: Arc<dyn broca_core::LeaseStore>,
    ) -> Self {
        Self {
            registry,
            state_root: state_root.to_path_buf(),
            slot,
            leases,
        }
    }

    /// The sessions to restore, each with the class of the root holding it: every session the
    /// new storage's roots hold a head for with `all`, else the one `key` names.
    fn targets(
        &self,
        key: &LineageKey,
        all: bool,
    ) -> Result<Vec<(StorageClass, SessionAddr)>, MoveRefusal> {
        if !all {
            let address = broca_core::session_addr(key);
            return match broca_records::session_storage::session_storage_class(
                &self.state_root,
                &address,
            ) {
                Ok(Some(class)) => Ok(vec![(class, SessionAddr(address))]),
                Ok(None) => Err(MoveRefusal::new(
                    "not_in_new_storage",
                    "the session isn't kept in the new storage",
                )),
                Err(error) => Err(MoveRefusal::new(
                    "unavailable",
                    format!("the session's storage couldn't be found: {error}"),
                )),
            };
        }
        let mut targets = Vec::new();
        for class in StorageClass::ALL {
            let store = LocalBackend::new(class.store_root(&self.state_root));
            let sessions = store.stored_sessions().map_err(|error| {
                MoveRefusal::new(
                    "unavailable",
                    format!("the {class} root's sessions couldn't be listed: {error:?}"),
                )
            })?;
            targets.extend(sessions.into_iter().map(|addr| (class, addr)));
        }
        Ok(targets)
    }

    /// Restores the session at `addr` in the root of `class`, and reports what it did.
    async fn restore_one(
        &self,
        deployment: &DeploymentKey,
        class: StorageClass,
        addr: &SessionAddr,
    ) -> Value {
        let store = LocalBackend::new(class.store_root(&self.state_root));
        let mut report = Report::new(class, addr);
        // The session as its head names it: the fence holds a session by that identity.
        let head = match store.read_stored_head(addr) {
            Ok(Some((bytes, version))) => (bytes, version),
            Ok(None) => return report.refused("the session has no head"),
            Err(error) => return report.failed(format!("its head couldn't be read: {error:?}")),
        };
        let triple = match broca_session_store::head::stored_triple(&head.0) {
            Ok(triple) => triple,
            Err(error) => return report.refused(format!("its head doesn't decode: {error:?}")),
        };
        let bind = BindIdentity::new(
            PathBuf::from(&triple.project_root),
            &triple.harness,
            &triple.session,
        );
        report.session(&bind);
        if broca_core::session_addr(&bind) != addr.0 {
            return report.refused("its head names another session than its address");
        }
        match restore::check(&store, addr, deployment).await {
            Ok(None) => {
                let view = broca_session_store::session::Session::view(
                    LocalBackend::new(class.store_root(&self.state_root)),
                    addr.clone(),
                    &triple,
                    deployment,
                )
                .await;
                let pending = view.map_or(true, |view| {
                    view.operations
                        .run
                        .as_ref()
                        .is_some_and(|run| run.finished.is_none())
                        || !view.operations.queue.is_empty()
                        || !view.operations.billing_owed.is_empty()
                });
                if pending {
                    if let Err(error) = broca_module::store_open::recovery::register_candidate(
                        &class.store_root(&self.state_root),
                        addr,
                    ) {
                        return report
                            .failed(format!("its recovery could not be registered: {error}"));
                    }
                }
                return report.restored(Restored::Kept { version: head.1 });
            }
            Ok(Some(Finding::Refused(reason))) => {
                return report.restored(Restored::Refused { reason })
            }
            Ok(Some(Finding::Lacks(_))) => {}
            Err(error) => return report.error(error),
        }
        // Restore waits for an actor to close on its own and does not break a live stream,
        // even when the stream is waiting in the registry rather than holding the actor open.
        if let Some(reason) = self.registry.restore_subscriber_busy(&bind) {
            return report.busy(reason);
        }
        if let Some(state) = self.registry.slot_state(&bind) {
            return report.busy(&format!("an actor serves it ({state})"));
        }
        let fence = match self
            .registry
            .fence_for_restore(&bind, self.leases.as_ref())
            .await
        {
            Ok(fence) => fence,
            Err(FenceRefusal::Busy(reason)) => return report.busy(&reason),
            Err(FenceRefusal::Unavailable(error)) => {
                return report.failed(format!("the session couldn't be held: {error}"))
            }
        };
        // The sidecar of a cut is named by the wall clock, so restores in later runs of the
        // daemon never name the same file.
        let now_ms =
            broca_module::head_time::epoch_ms(std::time::SystemTime::now()).unwrap_or_default();
        if let Err(error) = broca_module::store_open::recovery::register_candidate(
            &class.store_root(&self.state_root),
            addr,
        ) {
            return report.failed(format!("its recovery could not be registered: {error}"));
        }
        let restored = restore::restore(&store, addr, deployment, now_ms).await;
        drop(fence);
        match restored {
            Ok(restored) => report.restored(restored),
            Err(error) => report.error(error),
        }
    }
}

#[async_trait::async_trait]
impl StoreRestorer for StoreRestore {
    async fn restore_from_backup(&self, key: &LineageKey, all: bool) -> Result<Value, MoveRefusal> {
        let Some(runtime) = self.slot.runtime() else {
            return Err(MoveRefusal::new(
                "store_unavailable",
                "the new storage isn't open",
            ));
        };
        let deployment = runtime.deployment();
        let mut sessions = Vec::new();
        for (class, addr) in self.targets(key, all)? {
            sessions.push(self.restore_one(deployment, class, &addr).await);
        }
        Ok(json!({ "sessions": sessions }))
    }
}

/// One session's report, built as restore proceeds.
struct Report {
    fields: serde_json::Map<String, Value>,
    /// Who the session is, as the report line names it.
    name: String,
}

impl Report {
    fn new(class: StorageClass, addr: &SessionAddr) -> Self {
        let mut fields = serde_json::Map::new();
        fields.insert("class".into(), json!(class.name()));
        fields.insert("address".into(), json!(addr.0));
        Self {
            fields,
            name: format!("{class} {}", addr.0),
        }
    }

    fn session(&mut self, bind: &BindIdentity) {
        self.fields.insert(
            "session".into(),
            json!({
                "project_root": bind.project_root.display().to_string(),
                "harness": bind.harness,
                "session": bind.session,
            }),
        );
        self.name = format!("{} ({})", self.name, bind.session);
    }

    fn finish(mut self, outcome: &str, line: String) -> Value {
        self.fields.insert("outcome".into(), json!(outcome));
        self.fields
            .insert("report".into(), json!(format!("{}: {line}", self.name)));
        Value::Object(self.fields)
    }

    fn refused(mut self, reason: impl Into<String>) -> Value {
        let reason = reason.into();
        self.fields.insert("reason".into(), json!(reason));
        self.finish(
            "refused",
            format!("not restored, {reason}; nothing changed"),
        )
    }

    fn busy(mut self, reason: &str) -> Value {
        self.fields.insert("reason".into(), json!(reason));
        self.finish(
            "busy",
            format!(
                "not restored, the session is busy ({reason}); nothing changed, and it can be \
                 restored once it is idle"
            ),
        )
    }

    fn failed(mut self, error: String) -> Value {
        self.fields.insert("reason".into(), json!(error));
        self.finish("failed", format!("restore failed: {error}"))
    }

    fn error(self, error: RestoreError) -> Value {
        match error {
            RestoreError::KeyUnavailable => self.finish(
                "key_unavailable",
                "not restored, the deployment key hasn't loaded; nothing changed".into(),
            ),
            RestoreError::StillLacking(what) => {
                self.failed(format!("restore wrote, and the session still lacks {what}"))
            }
            RestoreError::Backend(error) => self.failed(format!("{error:?}")),
        }
    }

    fn restored(mut self, restored: Restored) -> Value {
        match restored {
            Restored::Kept { version } => {
                self.fields.insert("version".into(), json!(version));
                self.finish(
                    "kept",
                    format!("kept at version {version}, everything it names is there"),
                )
            }
            Restored::Refused { reason } => self.refused(reason),
            Restored::Unrecoverable { lacking } => {
                let lines: Vec<String> = lacking
                    .iter()
                    .map(|(version, what)| format!("version {version} lacks {what}"))
                    .collect();
                self.fields.insert(
                    "lacking".into(),
                    json!(lacking
                        .iter()
                        .map(|(version, what)| json!({"version": version, "what": what}))
                        .collect::<Vec<_>>()),
                );
                self.finish(
                    "unrecoverable",
                    format!("unrecoverable: {}; nothing changed", lines.join("; ")),
                )
            }
            Restored::Repaired {
                from,
                to,
                written,
                cut,
            } => {
                let mut said = Vec::new();
                self.fields.insert("from_version".into(), json!(from));
                self.fields.insert("to_version".into(), json!(to));
                if let Some(written) = written {
                    self.fields.insert("written_version".into(), json!(written));
                    said.push(format!(
                        "rolled back from version {from} to version {to}, written as version {written}"
                    ));
                }
                if let Some(cut) = &cut {
                    self.fields.insert(
                        "cut".into(),
                        json!({
                            "log": hex(&cut.log.0),
                            "record": cut.record,
                            "first_cut": cut.first_cut,
                            "at": cut.at,
                            "was": cut.was,
                            "lacking": cut.lacking,
                            "sidecar": cut.sidecar.display().to_string(),
                        }),
                    );
                    said.push(format!(
                        "log {} cut at record {}, since record {} names {}, which the backup lacks; the {} bytes cut are kept in {}",
                        hex(&cut.log.0),
                        cut.first_cut,
                        cut.record,
                        cut.lacking,
                        cut.was - cut.at,
                        cut.sidecar.display(),
                    ));
                }
                let outcome = if written.is_some() {
                    "rolled_back"
                } else {
                    "log_cut"
                };
                self.finish(outcome, said.join("; "))
            }
        }
    }
}

#[cfg(test)]
#[path = "store_restore_tests.rs"]
mod tests;
