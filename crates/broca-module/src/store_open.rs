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
use broca_session_store::session::{Session, SessionError, SessionView};
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
        })
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

    /// Loads the session `key` for a read, writing nothing and taking no lease (see
    /// `Session::view`). A session whose creation never committed holds no record, so it reads
    /// as having none (`None`). A log that needs a recovery refuses, transiently: the session's
    /// next send opens its actor, which repairs it.
    pub async fn view(
        &self,
        key: &BindIdentity,
    ) -> Result<Option<SessionView<LocalBackend>>, OpenError> {
        let addr = SessionAddr(broca_core::session_addr(key));
        let backend = LocalBackend::new(store_root(&self.state_root));
        match Session::view(backend, addr, &triple(key), &self.deployment).await {
            Ok(view) => Ok(Some(view)),
            Err(SessionError::Missing) => Ok(None),
            Err(SessionError::NeedsRecovery) => Err(OpenError::Classified {
                reason: "session_needs_recovery: the session's log must be repaired before it \
                         can be read; its next send repairs it"
                    .into(),
                class: broca_protocol::ErrorClass::Transient,
            }),
            Err(error) => Err(OpenError::Open(format!(
                "the session could not be read from the new storage: {error:?}"
            ))),
        }
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
        .with_storage(storage);
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
