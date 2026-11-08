//! Opening a session kept in the new storage (`broca_session_store`) as the module's actor for
//! it.
//!
//! The opener takes the session lease as it does for any session, then hands the session here
//! when the session's head exists, or, for a session that exists in neither storage, when the
//! registry marked the send creating it as one the store actor serves. Every other session stays
//! on today's storage, which refuses a session kept here by name.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use broca_protocol::BindIdentity;
use broca_session_loop::durable::SetupParts;
use broca_session_loop::parts::{DeploymentParts, SharedParts};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::DeploymentKey;
use broca_session_store::session::{Position, Session, SessionError, SessionView};
use broca_session_store::setup::SetupStore;
use tokio::sync::mpsc;

use crate::executor::RunExecutor;
use crate::registry::{ActorHandle, OpenError, RegistryHandle};
use crate::store_actor::{Lifecycle, StoreActor};

/// The capacity of a store session's command channel, the same as today's actor's.
const COMMANDS: usize = 64;

/// What the module needs to serve sessions kept in the new storage.
pub struct StoreRuntime {
    state_root: PathBuf,
    deployment: DeploymentKey,
    /// The deployment's setup parts, loaded from its setup store when the module started.
    parts: SharedParts,
    executor: Arc<dyn RunExecutor>,
    idle_ttl: Duration,
    /// Where the spend facts of a session's runs are written. Without it, runs that end stay
    /// owed, and are billed when a module that has it opens their session.
    billing: Option<crate::store_billing::StoreBilling>,
    /// Where a head's file-source import may read from. Broca's own import folder under the
    /// state root unless the operator configured other roots (`with_import_roots`).
    import_roots: Arc<crate::store_import::ImportRoots>,
    /// The facts of runs that have ended, which `run.status` and `run.result` serve again
    /// without reading the runs' records. Shared with each store actor, which serves them too.
    settled: Arc<crate::store_status::SettledRuns>,
    /// Views of sessions no actor holds, kept by position (see `store_reads`).
    views: crate::store_reads::ViewCache,
}

impl StoreRuntime {
    /// The runtime for the new storage under `state_root`, its folder
    /// [`broca_wal::SESSION_STORAGE_DIR`]. Loads every setup part the deployment holds, so a run
    /// started before the module did reads its parts back.
    pub async fn open(
        state_root: &Path,
        deployment: DeploymentKey,
        executor: Arc<dyn RunExecutor>,
        idle_ttl: Duration,
    ) -> Result<Self, String> {
        let backend = LocalBackend::new(store_root(state_root));
        let key = deployment.setup_key();
        let loaded = SetupStore::new(&backend, &key)
            .load_all()
            .await
            .map_err(|error| {
                format!("the new storage's setup parts could not be loaded: {error:?}")
            })?;
        Ok(Self {
            state_root: state_root.to_path_buf(),
            deployment,
            parts: SharedParts::new(Box::new(DeploymentParts::loaded(loaded))),
            executor,
            idle_ttl,
            billing: None,
            import_roots: Arc::new(crate::store_import::ImportRoots::with_default(
                None,
                Some(&crate::store_import::default_import_folder(state_root)),
            )),
            settled: Arc::default(),
            views: crate::store_reads::ViewCache::default(),
        })
    }

    /// Reads file-source imports from `roots` (`import.roots` in `broca.jsonc`, loaded at
    /// boot) instead of broca's own import folder alone. That folder stays the only place an
    /// import deletes its source from.
    pub fn with_import_roots(mut self, roots: Option<Vec<PathBuf>>) -> Self {
        self.import_roots = Arc::new(crate::store_import::ImportRoots::with_default(
            roots,
            Some(&crate::store_import::default_import_folder(
                &self.state_root,
            )),
        ));
        self
    }

    /// Where a file-source import may read from.
    pub fn import_roots(&self) -> Arc<crate::store_import::ImportRoots> {
        Arc::clone(&self.import_roots)
    }

    /// The deployment's setup parts. A run start names its tools and plan by part, and a read or
    /// a head resolves them through these.
    pub fn parts(&self) -> &SharedParts {
        &self.parts
    }

    /// The facts of runs that have ended, kept for `run.status` and `run.result`.
    pub fn settled_runs(&self) -> &crate::store_status::SettledRuns {
        &self.settled
    }

    /// The views kept of sessions no actor holds.
    pub fn views(&self) -> &crate::store_reads::ViewCache {
        &self.views
    }

    /// Writes the spend facts of every run on the new storage to `billing`, the deployment's
    /// usage export, as today's runs are written.
    pub fn with_billing(mut self, billing: crate::store_billing::StoreBilling) -> Self {
        self.billing = Some(billing);
        self
    }

    /// Whether the session `key` is kept in the new storage: whether its head exists. A head
    /// that can't be checked is an error, never read as absent.
    pub fn holds(&self, key: &BindIdentity) -> Result<bool, OpenError> {
        let head =
            broca_wal::session_storage_head(&self.state_root, &broca_core::session_addr(key));
        match std::fs::symlink_metadata(head) {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(OpenError::Open(format!(
                "the new storage's head of this session could not be checked: {error}"
            ))),
        }
    }

    /// The session `key` as it stands now, read-only: writing nothing and taking no lease (see
    /// `Session::view`). The view is kept, and served again while the session stays where it
    /// was read (see `store_reads::ViewCache`). A session whose creation never committed holds no
    /// record, so it reads as having none (`None`). A log that needs a recovery refuses,
    /// transiently: the session's next send opens its actor, which repairs it.
    pub async fn current_view(
        &self,
        key: &BindIdentity,
    ) -> Result<Option<Arc<SessionView<LocalBackend>>>, OpenError> {
        let addr = SessionAddr(broca_core::session_addr(key));
        let backend = LocalBackend::new(store_root(&self.state_root));
        for _ in 0..POSITION_TRIES {
            let position = match Session::stored_position(&backend, &addr, &self.deployment).await {
                Ok(Some(position)) => position,
                Ok(None) => return Ok(None),
                Err(error) => return Err(view_error(error)),
            };
            match self.view_at(key, position).await {
                // A writer moved the head between the probe and the load: probe again.
                Err(SessionError::Moved) => continue,
                Err(SessionError::Missing) => return Ok(None),
                other => return other.map(Some).map_err(view_error),
            }
        }
        // Still moving: read it as it is, without keeping the view.
        match Session::view(backend, addr, &triple(key), &self.deployment).await {
            Ok(view) => Ok(Some(Arc::new(view))),
            Err(SessionError::Missing) => Ok(None),
            Err(error) => Err(view_error(error)),
        }
    }

    /// The session `key` as it stood at `position`: the kept view, or one loaded there and kept.
    pub async fn view_at(
        &self,
        key: &BindIdentity,
        position: Position,
    ) -> Result<Arc<SessionView<LocalBackend>>, SessionError> {
        let addr = broca_core::session_addr(key);
        let triple = triple(key);
        let load = Session::view_at(
            LocalBackend::new(store_root(&self.state_root)),
            SessionAddr(addr.clone()),
            &triple,
            &self.deployment,
            &position,
        );
        self.views.view(&addr, position, load).await
    }

    /// Serves `request` on the session `key`: by its store actor when the registry holds one open,
    /// else from the session as stored (see `store_reads`). `None` for a session whose creation
    /// never committed, which holds no record.
    pub(crate) async fn read(
        &self,
        registry: &crate::LineageRegistry,
        key: &BindIdentity,
        request: crate::store_reads::StoreReadRequest,
    ) -> Result<Option<crate::store_reads::StoreReadReply>, crate::serve::ServeError> {
        use crate::store_reads::{execute, ReadContext, StoreReadOutcome};
        let addr = broca_core::session_addr(key);
        let context = |held: bool, open: bool, live_step| ReadContext {
            parts: self.parts(),
            held,
            open,
            settled: Some((self.settled_runs(), &addr)),
            session_name: &key.session,
            live_step,
            stream_head: None,
        };
        for _ in 0..POSITION_TRIES {
            let Some(handle) = registry.open_handle(key) else {
                break;
            };
            let (reply, answer) = tokio::sync::oneshot::channel();
            let command = crate::actor::LineageCmd::StoreRead {
                request: Box::new(request.clone()),
                reply,
            };
            let outcome = match handle.send(command).await {
                Ok(()) => answer.await.ok(),
                Err(_) => None,
            };
            match outcome {
                Some(StoreReadOutcome::Served(reply)) => return Ok(Some(*reply)),
                Some(StoreReadOutcome::Refused(error)) => return Err(error),
                Some(StoreReadOutcome::Stored {
                    position,
                    live_step,
                }) => match self.view_at(key, position).await {
                    Ok(view) => {
                        let context = context(true, true, live_step.as_ref());
                        return Ok(Some(execute(&view, &context, &request).await));
                    }
                    // The actor sealed since it answered: ask it again.
                    Err(SessionError::Moved) => continue,
                    Err(error) => return Err(view_error(error).into()),
                },
                // The actor closed under the read: read the session as no actor holds it.
                Some(StoreReadOutcome::Reopen) | None => break,
            }
        }
        let Some(view) = self.current_view(key).await? else {
            return Ok(None);
        };
        let slot = registry.slot_state(key);
        let context = context(slot.is_some(), slot == Some("open"), None);
        Ok(Some(execute(&view, &context, &request).await))
    }

    /// Opens the session `key` as the actor for registry slot `gen`, creating it when `create`.
    /// The caller holds the session `lease`, which the actor keeps for its whole life.
    pub async fn open_actor(
        &self,
        key: &BindIdentity,
        gen: u64,
        registry: RegistryHandle,
        lease: Box<dyn std::any::Any + Send>,
        create: bool,
    ) -> Result<ActorHandle, OpenError> {
        let addr = SessionAddr(broca_core::session_addr(key));
        let triple = triple(key);
        let backend = LocalBackend::new(store_root(&self.state_root));
        let session = if create {
            // The feature floor lists what this state root's logs need a build to understand.
            // Release placement (`scripts/place-release.sh`) refuses to install a build that
            // doesn't know a listed feature, and builds older than the one that refuses
            // new-storage sessions by name don't know this one. So once a session lives here,
            // no build that would serve it from a WAL it doesn't have can be placed. The floor
            // is raised before the session's head exists.
            broca_wal::raise_floor(
                &self.state_root.join(broca_wal::FEATURE_FLOOR_FILENAME),
                &[broca_wal::SESSION_STORAGE_FEATURE],
            )
            .map_err(|error| {
                OpenError::Open(format!("the feature floor could not be raised: {error}"))
            })?;
            // A session on the current compaction contract keeps text delivered with a tool
            // result apart from the result, as every new session is.
            Session::create(backend, addr, triple, &self.deployment, false).await
        } else {
            Session::load(backend, addr, &triple, &self.deployment).await
        }
        .map_err(|error| open_error(error, create))?;
        let storage = SetupParts {
            backend: LocalBackend::new(store_root(&self.state_root)),
            key: self.deployment.setup_key(),
        };
        let mut actor = StoreActor::new(
            key.clone(),
            session,
            Arc::clone(&self.executor),
            Box::new(self.parts.clone()),
        )
        .with_storage(storage)
        .with_settled(Arc::clone(&self.settled))
        // A send's attachments are uploaded beside the session, under the state root, whichever
        // storage keeps it.
        .with_uploads(broca_core::media_uploads::Uploads::at(&self.state_root));
        let readers_root = store_root(&self.state_root);
        actor = actor.with_readers(Arc::new(move || LocalBackend::new(readers_root.clone())));
        if let Some(billing) = &self.billing {
            actor = actor.with_billing(billing.clone());
        }
        let (tx, commands) = mpsc::channel(COMMANDS);
        let run_in_flight = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let life = Lifecycle {
            registry: registry.clone(),
            gen,
            lease,
            idle_ttl: self.idle_ttl,
            run_in_flight: Arc::clone(&run_in_flight),
        };
        let exit_key = key.clone();
        let served = tokio::spawn(actor.serve_in_registry(commands, life));
        tokio::spawn(async move {
            // The actor gives its slot up itself; a panic doesn't, so its slot is cleared here
            // and the session reopens.
            if served.await.is_err() {
                registry.finalize_exit(&exit_key, gen);
            }
        });
        Ok(ActorHandle {
            media_head: None,
            gen,
            tx,
            // The new storage keeps no WAL, so there is no WAL offset to share.
            durable_head: Arc::new(std::sync::atomic::AtomicU64::new(0)),
            run_in_flight,
            approvals: Arc::new(crate::approval_state::ApprovalState::default()),
            first_episode: Arc::new(std::sync::OnceLock::new()),
        })
    }
}

/// How many times a read probes the session's position again after a writer moved its head
/// between the probe and the load. A head moves on a writer's open, a seal or a revert, never on
/// an append, so a second probe nearly always holds.
const POSITION_TRIES: usize = 3;

/// A view that couldn't be loaded, as the opener reports it.
fn view_error(error: SessionError) -> OpenError {
    match error {
        SessionError::NeedsRecovery => OpenError::Classified {
            reason: "session_needs_recovery: the session's log must be repaired before it can be \
                     read; its next send repairs it"
                .into(),
            class: broca_protocol::ErrorClass::Transient,
        },
        error => OpenError::Open(format!(
            "the session could not be read from the new storage: {error:?}"
        )),
    }
}

/// The new storage's folder under `state_root`.
fn store_root(state_root: &Path) -> PathBuf {
    state_root.join(broca_wal::SESSION_STORAGE_DIR)
}

/// The triple a session is created and loaded under: the bind identity's, spelled as
/// `broca_core::session_addr` spells it, so the triple and the address name one session.
fn triple(key: &BindIdentity) -> SessionTriple {
    SessionTriple {
        project_root: key.project_root.display().to_string(),
        harness: key.harness.clone(),
        session: key.session.clone(),
    }
}

fn open_error(error: SessionError, create: bool) -> OpenError {
    let action = if create { "created" } else { "opened" };
    OpenError::Open(format!(
        "the session could not be {action} in the new storage: {error:?}"
    ))
}
