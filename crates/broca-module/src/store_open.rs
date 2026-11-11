//! Opening a session kept in the new storage (`broca_session_store`) as the module's actor for
//! it.
//!
//! The opener takes the session lease as it does for any session, then hands the session here
//! when the session's head exists, or, for a session that exists in neither storage, when the
//! registry marked the send creating it as one the store actor serves. Every other session stays
//! on today's storage, which refuses a session kept here by name.
//!
//! A session here is encrypted or plain, as its head says (see `broca_session_store::keys`). A
//! plain session is served from the module's start; an encrypted one once the deployment key has
//! loaded from the vault. `session_storage.encryption` in `broca.jsonc` chooses the mode of the
//! sessions created here.
//!
//! The new storage is three complete stores, one per backup class (`broca_wal::StorageClass`):
//! each root has its own heads, logs, objects and setup parts, and all share the deployment key.
//! A session is created in the root its creating send's class places it in
//! (`broca_wal::placement`), and from then on the root holding its head is where it is opened,
//! read and written. A head found in two roots is refused by name, never served from either.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use broca_protocol::BindIdentity;
use broca_session_loop::durable::SetupParts;
use broca_session_loop::parts::{DeploymentParts, SharedParts};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::session::{Position, Session, SessionError, SessionView};
use broca_session_store::setup::SetupStore;
use broca_wal::StorageClass;
use tokio::sync::mpsc;

use crate::executor::RunExecutor;
use crate::registry::{ActorHandle, OpenError, RegistryHandle};
use crate::store_actor::{Lifecycle, StoreActor};

/// The capacity of a store session's command channel, the same as today's actor's.
const COMMANDS: usize = 64;

#[path = "store_expiry.rs"]
mod expiry;

/// What the module needs to serve sessions kept in the new storage. Opened without the
/// deployment key it serves plain sessions only; the key is installed into it once it loads
/// (see [`StoreSlot::load`]), and from then on it serves every session.
pub struct StoreRuntime {
    state_root: PathBuf,
    /// The id of the deployment key encrypted sessions open with.
    key_id: u16,
    /// The mode of the sessions this runtime creates.
    new_sessions: Cipher,
    /// The deployment key, once it has loaded.
    deployment: OnceLock<DeploymentKey>,
    /// What sessions are opened with before the key has loaded: plain sessions open, encrypted
    /// ones refuse.
    unloaded: DeploymentKey,
    /// Each class root's setup parts, by [`StorageClass::index`]. A root keeps its own, so a
    /// part stored in one root never counts as stored in another: a session's root holds every
    /// part its runs name.
    roots: [RootParts; 3],
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

/// One class root's setup parts.
struct RootParts {
    /// The encrypted sessions' setup parts, loaded from the root's setup store when the key
    /// loaded. Each mode keeps its own parts, so a part stored in one mode's name space never
    /// counts as stored in the other's.
    parts: SharedParts,
    /// The plain sessions' setup parts, loaded from the root's setup store when the runtime
    /// opened.
    plain_parts: SharedParts,
}

impl StoreRuntime {
    /// The runtime for the new storage under `state_root`, its three class roots (see
    /// [`StorageClass::store_dir`]), with its deployment key loaded. Loads every setup part each
    /// root holds, in both modes, so a run started before the module did reads its parts back.
    /// It creates sessions in the mode `deployment` creates them in.
    pub async fn open(
        state_root: &Path,
        deployment: DeploymentKey,
        executor: Arc<dyn RunExecutor>,
        idle_ttl: Duration,
    ) -> Result<Self, String> {
        let runtime = Self::open_without_key(
            state_root,
            deployment.id(),
            deployment.new_sessions(),
            executor,
            idle_ttl,
        )
        .await?;
        let parts = runtime.load_encrypted_parts(&deployment).await?;
        runtime.install_key(deployment, parts)?;
        Ok(runtime)
    }

    /// The runtime for the new storage under `state_root` before deployment key `key_id` has
    /// loaded. It serves plain sessions, whose setup parts it loads from each root, and creates
    /// sessions in mode `new_sessions`, which it can do only for plain ones until the key loads.
    pub async fn open_without_key(
        state_root: &Path,
        key_id: u16,
        new_sessions: Cipher,
        executor: Arc<dyn RunExecutor>,
        idle_ttl: Duration,
    ) -> Result<Self, String> {
        let plain = broca_session_store::setup::SetupKey::plain();
        let mut roots = Vec::with_capacity(StorageClass::ALL.len());
        for class in StorageClass::ALL {
            let backend = LocalBackend::new(class.store_root(state_root));
            recovery_index::prepare(&class.store_root(state_root))
                .map_err(|error| format!("prepare {class} recovery candidates: {error}"))?;
            let loaded = SetupStore::new(&backend, &plain)
                .load_all()
                .await
                .map_err(|error| {
                    format!(
                        "the new storage's plain setup parts in its {class} root could not be \
                         loaded: {error:?}"
                    )
                })?;
            roots.push(RootParts {
                parts: SharedParts::new(Box::new(DeploymentParts::default())),
                plain_parts: SharedParts::new(Box::new(DeploymentParts::loaded(loaded))),
            });
        }
        let roots: [RootParts; 3] = roots
            .try_into()
            .unwrap_or_else(|_| unreachable!("one entry per class"));
        Ok(Self {
            state_root: state_root.to_path_buf(),
            key_id,
            new_sessions,
            deployment: OnceLock::new(),
            unloaded: DeploymentKey::unloaded(key_id).creating(new_sessions),
            roots,
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

    /// The encrypted sessions' setup parts of each class root, by [`StorageClass::index`], read
    /// under `deployment`, which must have loaded.
    async fn load_encrypted_parts(
        &self,
        deployment: &DeploymentKey,
    ) -> Result<Vec<LoadedParts>, String> {
        let key = deployment
            .setup_key_for(Cipher::XChaCha20Poly1305)
            .ok_or("the deployment key offered has not loaded")?;
        let mut loaded = Vec::with_capacity(StorageClass::ALL.len());
        for class in StorageClass::ALL {
            let backend = self.backend(class);
            loaded.push(
                SetupStore::new(&backend, &key)
                    .load_all()
                    .await
                    .map_err(|error| {
                        format!(
                        "the new storage's setup parts in its {class} root could not be loaded: \
                         {error:?}"
                    )
                    })?,
            );
        }
        Ok(loaded)
    }

    /// Installs the deployment key and the encrypted sessions' setup parts read under it, one
    /// list per class root, so encrypted sessions are served from now on. The parts go in first:
    /// whoever sees the key installed finds them. A runtime keeps the first key installed.
    fn install_key(
        &self,
        deployment: DeploymentKey,
        parts: Vec<LoadedParts>,
    ) -> Result<(), String> {
        if deployment.id() != self.key_id {
            return Err(format!(
                "deployment key {} was offered for the new storage, which is opened with key {}",
                deployment.id(),
                self.key_id
            ));
        }
        if self.key_loaded() {
            return Ok(());
        }
        for (root, parts) in self.roots.iter().zip(parts) {
            *root
                .parts
                .0
                .lock()
                .map_err(|_| "a holder of the encrypted setup parts panicked")? =
                Box::new(DeploymentParts::loaded(parts));
        }
        let _ = self.deployment.set(deployment.creating(self.new_sessions));
        Ok(())
    }

    /// Whether the deployment key has loaded, so encrypted sessions are served.
    pub fn key_loaded(&self) -> bool {
        self.deployment.get().is_some()
    }

    /// The mode of the sessions this runtime creates.
    pub fn new_sessions(&self) -> Cipher {
        self.new_sessions
    }

    /// Whether this runtime can create a session now: a plain one always, an encrypted one once
    /// the deployment key has loaded.
    pub fn can_create(&self) -> bool {
        self.new_sessions == Cipher::None || self.key_loaded()
    }

    /// What sessions are opened and created with now: the deployment key once it has loaded,
    /// and before that a stand-in that opens plain sessions only. Either creates sessions in
    /// the mode `session_storage.encryption` chose ([`new_sessions`](Self::new_sessions)).
    pub fn deployment(&self) -> &DeploymentKey {
        self.deployment.get().unwrap_or(&self.unloaded)
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

    /// The setup parts of the sessions in mode `cipher` in the root of `class`. A run start names
    /// its tools and plan by part, and a read or a head resolves them through these. The
    /// encrypted sessions' parts are empty until the deployment key loads, and no encrypted
    /// session is served before then.
    pub fn parts_for(&self, class: StorageClass, cipher: Cipher) -> &SharedParts {
        let root = &self.roots[class.index()];
        match cipher {
            Cipher::XChaCha20Poly1305 => &root.parts,
            Cipher::None => &root.plain_parts,
        }
    }

    /// The facts of runs that have ended, kept for `run.status` and `run.result`.
    pub fn settled_runs(&self) -> &crate::store_status::SettledRuns {
        &self.settled
    }

    /// The state root whose new storage this runtime serves.
    pub fn state_root(&self) -> &Path {
        &self.state_root
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

    /// The id of the deployment key this runtime opens encrypted sessions with.
    pub fn key_id(&self) -> u16 {
        self.key_id
    }

    /// Whether the session `key` is kept in the new storage: whether its head exists in one of
    /// its roots. A head that can't be checked, or one found in two roots, is an error, never
    /// read as absent.
    pub fn holds(&self, key: &BindIdentity) -> Result<bool, OpenError> {
        Ok(self.class_of(key)?.is_some())
    }

    pub fn check_move(&self, key: &BindIdentity, write: bool) -> Result<(), OpenError> {
        let reference = broca_wire::agent_data::session_ref(
            &key.harness,
            &key.project_root.to_string_lossy(),
            &key.session,
        );
        let sidecar = self
            .state_root
            .join("session-store-moves/fences")
            .join(broca_core::session_addr(key));
        let read = |error: std::io::Error| OpenError::Open(error.to_string());
        if let Some(moved) = broca_wal::migration::read_json::<broca_wal::migration::Moved>(
            &sidecar.with_extension("moved"),
        )
        .map_err(read)?
        {
            return Err(OpenError::SessionMoved {
                holder: moved.holder,
            });
        }
        if write {
            if let Some(freeze) = broca_wal::migration::read_json::<broca_wal::migration::Freeze>(
                &sidecar.with_extension("moving"),
            )
            .map_err(read)?
            {
                return Err(OpenError::SessionMoving {
                    cut_id: freeze.cut_id,
                });
            }
            if let Some(cut_id) =
                broca_wal::migration::cut_fence(&self.state_root.join("wal"), &reference)
                    .map_err(read)?
            {
                return Err(OpenError::SessionMoving { cut_id });
            }
        }
        let Some(class) = self.class_of(key)? else {
            return Ok(());
        };
        let backend = self.backend(class);
        let Some((stored, _)) = backend
            .read_stored_head(&SessionAddr(broca_core::session_addr(key)))
            .map_err(|error| OpenError::Open(format!("{error:?}")))?
        else {
            return Ok(());
        };
        let (head, _) = broca_session_store::head::open(&stored, self.deployment())
            .map_err(|error| open_error(SessionError::Head(error), false))?;
        match head.residence {
            broca_session_store::head::Residence::Moving(cut_id) if write => {
                Err(OpenError::SessionMoving { cut_id })
            }
            broca_session_store::head::Residence::Moved(moved) => Err(OpenError::SessionMoved {
                holder: moved.holder,
            }),
            _ => Ok(()),
        }
    }

    /// The class of the root holding the session `key`'s head, `None` when no root holds it.
    /// Errors as [`holds`](Self::holds).
    pub fn class_of(&self, key: &BindIdentity) -> Result<Option<StorageClass>, OpenError> {
        head_class(&self.state_root, key)
    }

    /// The store of the root of `class`.
    fn backend(&self, class: StorageClass) -> LocalBackend {
        LocalBackend::new(class.store_root(&self.state_root))
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
        match self.class_of(key)? {
            Some(class) => self.current_view_in(key, class).await.map_err(view_error),
            None => Ok(None),
        }
    }

    /// [`current_view`](Self::current_view) of the session `key`, kept in the root of `class`.
    async fn current_view_in(
        &self,
        key: &BindIdentity,
        class: StorageClass,
    ) -> Result<Option<Arc<SessionView<LocalBackend>>>, SessionError> {
        let addr = SessionAddr(broca_core::session_addr(key));
        let backend = self.backend(class);
        for _ in 0..POSITION_TRIES {
            let position = match Session::stored_position(&backend, &addr, self.deployment()).await
            {
                Ok(Some(position)) => position,
                Ok(None) => return Ok(None),
                Err(error) => return Err(error),
            };
            match self.view_at(key, class, position).await {
                // A writer moved the head between the probe and the load: probe again.
                Err(SessionError::Moved) => continue,
                Err(SessionError::Missing) => return Ok(None),
                other => return other.map(Some),
            }
        }
        // Still moving: read it as it is, without keeping the view.
        match Session::view(backend, addr, &triple(key), self.deployment()).await {
            Ok(view) => Ok(Some(Arc::new(view))),
            Err(SessionError::Missing) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The session `key`, kept in the root of `class`, as it stood at `position`: the kept view,
    /// or one loaded there and kept.
    pub async fn view_at(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        position: Position,
    ) -> Result<Arc<SessionView<LocalBackend>>, SessionError> {
        let addr = broca_core::session_addr(key);
        let triple = triple(key);
        let load = Session::view_at(
            self.backend(class),
            SessionAddr(addr.clone()),
            &triple,
            self.deployment(),
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
        // Recovery can open an actor. Keep that large future off every caller's stack,
        // including callers that compose several reads into one request future.
        Box::pin(self.read_inner(registry, key, request)).await
    }

    async fn read_inner(
        &self,
        registry: &crate::LineageRegistry,
        key: &BindIdentity,
        request: crate::store_reads::StoreReadRequest,
    ) -> Result<Option<crate::store_reads::StoreReadReply>, crate::serve::ServeError> {
        use crate::store_reads::{execute, ReadContext, StoreReadOutcome};
        let run = match &request {
            crate::store_reads::StoreReadRequest::RunAdmission(run)
            | crate::store_reads::StoreReadRequest::RunStatus(run)
            | crate::store_reads::StoreReadRequest::RunStatusReply(run)
            | crate::store_reads::StoreReadRequest::RunResult(run) => Some(run.0.as_str()),
            _ => None,
        };
        registry.check_expired(key, run).await?;
        let addr = broca_core::session_addr(key);
        // The root the session is kept in. A session never changes roots, so a read that finds
        // none finds no session.
        let Some(class) = self.class_of(key)? else {
            return Ok(None);
        };
        if !request.is_authorization() && registry.open_handle(key).is_none() {
            let needs_actor = match self.current_view_in(key, class).await {
                Ok(Some(view)) => recovery_changes_reads(&view),
                Ok(None) => return Ok(None),
                Err(SessionError::NeedsRecovery) => true,
                Err(error) => return Err(view_error(error).into()),
            };
            if needs_actor {
                // The background inventory is only a liveness aid. This session's actor
                // must recover before any read can observe state that recovery would change.
                Box::pin(registry.actor_for_mode(key, None, None, true)).await?;
            }
        }
        // A read resolves setup parts in the root and mode of the session it reads.
        let context = |view: &SessionView<LocalBackend>,
                       held: bool,
                       open: bool,
                       live_step,
                       entry_speaks_for| ReadContext {
            token_counts: None,
            parts: self.parts_for(class, view.head().cipher()),
            held,
            open,
            settled: Some((self.settled_runs(), &addr)),
            session_name: &key.session,
            live_step,
            stream_head: None,
            entry_speaks_for,
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
                    entry_speaks_for,
                }) => match self.view_at(key, class, position).await {
                    Ok(view) => {
                        let counts = self.views.token_counts(&addr);
                        let mut context =
                            context(&view, true, true, live_step.as_ref(), entry_speaks_for);
                        context.token_counts = Some(&counts);
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
        let Some(view) = self.current_view_in(key, class).await.map_err(view_error)? else {
            return Ok(None);
        };
        if !request.is_authorization() && recovery_changes_reads(&view) {
            // The actor closed before answering. Refuse rather than serve a pre-recovery
            // snapshot; a later read will reopen it through the same barrier.
            return Err(view_error(SessionError::NeedsRecovery).into());
        }
        let slot = registry.slot_state(key);
        // No actor answered, so none holds a Setup: a compaction read asks the catalog.
        let attribution =
            crate::store_entries::EntryAttribution::Catalog(self.executor.compaction_plane());
        let counts = self.views.token_counts(&addr);
        let mut context = context(
            &view,
            slot.is_some(),
            slot == Some("open"),
            None,
            attribution,
        );
        context.token_counts = Some(&counts);
        Ok(Some(execute(&view, &context, &request).await))
    }

    /// Opens the session `key` as the actor for registry slot `gen`: creating it in the root of
    /// class `create` when that is given, else loading it from the root holding its head. The
    /// caller holds the session `lease`, which the actor keeps for its whole life.
    pub async fn open_actor(
        &self,
        key: &BindIdentity,
        gen: u64,
        registry: RegistryHandle,
        lease: Box<dyn std::any::Any + Send>,
        create: Option<StorageClass>,
        step_events: Option<&crate::step_events::StepEvents>,
    ) -> Result<ActorHandle, OpenError> {
        let timing = broca_session_store::metrics::Phase::Open
            .start()
            .with_session(|| Some(broca_core::session_addr(key)));
        let addr = SessionAddr(broca_core::session_addr(key));
        let triple = triple(key);
        let class = match create {
            Some(class) => class,
            None => self
                .class_of(key)?
                .ok_or_else(|| open_error(SessionError::Missing, false))?,
        };
        let backend = self.backend(class);
        let floors = self.expiry_markers(key)?;
        let mut session = if create.is_some() {
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
            // result apart from the result, as every new session is. It is created in the mode
            // `session_storage.encryption` chose.
            Session::create(backend, addr, triple, self.deployment(), false).await
        } else {
            Session::load(backend, addr, &triple, self.deployment()).await
        }
        .map_err(|error| open_error(error, create.is_some()))?;
        session.raise_retired_tag_floor(
            floors
                .iter()
                .map(|marker| marker.next_tag)
                .max()
                .unwrap_or(1),
        );
        drop(timing);
        // The session reads and stores its setup parts in its own root and mode.
        let cipher = session.head().cipher();
        let setup_key = self
            .deployment()
            .setup_key_for(cipher)
            .ok_or_else(|| key_unavailable(self.key_id))?;
        let storage = SetupParts {
            backend: self.backend(class),
            key: setup_key,
        };
        let mut actor = StoreActor::new(
            key.clone(),
            session,
            Arc::clone(&self.executor),
            Box::new(self.parts_for(class, cipher).clone()),
        )
        .with_recovery_index(class.store_root(&self.state_root))
        .with_storage(storage)
        .with_settled(Arc::clone(&self.settled))
        // A send's attachments are uploaded beside the session, under the state root, whichever
        // storage keeps it.
        .with_uploads(broca_core::media_uploads::Uploads::at(&self.state_root));
        if let Some(events) = step_events {
            actor = actor.with_step_events(events);
        }
        if let Some(service) = registry.retention_service() {
            if !service.defer_overhaul() {
                actor = actor.with_retention_clock(service.clock.clone());
            }
        }
        if create.is_some() {
            actor = actor.created_here();
        }
        let readers_root = class.store_root(&self.state_root);
        actor = actor.with_readers(Arc::new(move || LocalBackend::new(readers_root.clone())));
        if let Some(billing) = &self.billing {
            actor = actor.with_billing(billing.clone());
        }
        let first_episode = actor
            .first_episode_handle(&registry)
            .map_err(OpenError::Open)?;
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
            first_episode,
        })
    }
}

/// What the module opens its new storage's runtime with, besides the deployment key. The key
/// comes from the vault and may load after the module has started, so the runtime is opened from
/// this once it has (see [`StoreSlot::load`]).
#[derive(Clone)]
pub struct StoreRuntimeSetup {
    pub state_root: PathBuf,
    pub executor: Arc<dyn RunExecutor>,
    pub idle_ttl: Duration,
    /// `import.roots` from `broca.jsonc` (see [`StoreRuntime::with_import_roots`]).
    pub import_roots: Option<Vec<PathBuf>>,
    /// Where spend facts are written (see [`StoreRuntime::with_billing`]).
    pub billing: Option<crate::store_billing::StoreBilling>,
}

impl StoreRuntimeSetup {
    /// The runtime this setup describes, opened under `deployment`.
    pub async fn open(&self, deployment: DeploymentKey) -> Result<StoreRuntime, String> {
        let runtime = StoreRuntime::open(
            &self.state_root,
            deployment,
            Arc::clone(&self.executor),
            self.idle_ttl,
        )
        .await?;
        Ok(self.configured(runtime))
    }

    /// The runtime this setup describes before deployment key `key_id` has loaded: it serves
    /// plain sessions, and creates sessions in mode `new_sessions` (see
    /// [`StoreRuntime::open_without_key`]).
    pub async fn open_without_key(
        &self,
        key_id: u16,
        new_sessions: Cipher,
    ) -> Result<StoreRuntime, String> {
        let runtime = StoreRuntime::open_without_key(
            &self.state_root,
            key_id,
            new_sessions,
            Arc::clone(&self.executor),
            self.idle_ttl,
        )
        .await?;
        Ok(self.configured(runtime))
    }

    fn configured(&self, runtime: StoreRuntime) -> StoreRuntime {
        let mut runtime = runtime.with_import_roots(self.import_roots.clone());
        if let Some(billing) = &self.billing {
            runtime = runtime.with_billing(billing.clone());
        }
        runtime
    }
}

/// The new storage as the opener sees it from the module's start. Whether a session is kept
/// there needs only the state root, so such a session is told apart from today's from the first
/// open. Its runtime is opened at the start without the deployment key
/// ([`StoreSlot::open_without_key`]) and serves plain sessions from then on. Serving an encrypted
/// session needs the deployment key, which comes from the vault and may load after the module has
/// started; it is installed into the runtime once it has. Until then an encrypted session kept
/// here is refused `key_unavailable`, a transient refusal, and never served from today's storage.
pub struct StoreSlot {
    state_root: PathBuf,
    key_id: u16,
    /// The mode of the sessions created here (`session_storage.encryption`).
    new_sessions: Cipher,
    runtime: OnceLock<Arc<StoreRuntime>>,
    /// The sessions refused while no key was loaded. Opening a session runs the recovery its
    /// store actor performs on open (a crashed run sealed, an owed run billed, a queued send
    /// drained), which the refusal put off; the module opens these once the key loads.
    refused: Mutex<HashSet<BindIdentity>>,
}

impl StoreSlot {
    /// The new storage under `state_root`, whose deployment key `key_id` hasn't loaded yet. It
    /// creates encrypted sessions unless [`creating`](Self::creating) says otherwise.
    pub fn awaiting_key(state_root: &Path, key_id: u16) -> Self {
        Self {
            state_root: state_root.to_path_buf(),
            key_id,
            new_sessions: Cipher::XChaCha20Poly1305,
            runtime: OnceLock::new(),
            refused: Mutex::default(),
        }
    }

    /// This slot, creating sessions in mode `cipher`. Sessions kept here keep their own mode.
    pub fn creating(mut self, cipher: Cipher) -> Self {
        self.new_sessions = cipher;
        self
    }

    /// The new storage served by `runtime`, its key already loaded.
    pub fn loaded(runtime: Arc<StoreRuntime>) -> Self {
        let slot = Self::awaiting_key(&runtime.state_root, runtime.key_id())
            .creating(runtime.new_sessions());
        let _ = slot.runtime.set(runtime);
        slot
    }

    /// The id of the deployment key the new storage is opened with.
    pub fn key_id(&self) -> u16 {
        self.key_id
    }

    /// The mode of the sessions created here.
    pub fn new_sessions(&self) -> Cipher {
        self.new_sessions
    }

    /// Whether the deployment key has loaded, so every session kept here is served.
    pub fn key_loaded(&self) -> bool {
        self.runtime
            .get()
            .is_some_and(|runtime| runtime.key_loaded())
    }

    /// The runtime, once it is open: from the module's start, serving plain sessions, and every
    /// session once the key has loaded.
    pub fn runtime(&self) -> Option<Arc<StoreRuntime>> {
        self.runtime.get().cloned()
    }

    /// The runtime a session that exists in neither storage is created in, when it can create
    /// one now: a plain session from the module's start, an encrypted one once the key has
    /// loaded. `None` leaves the session to today's storage.
    pub fn creating_runtime(&self) -> Option<Arc<StoreRuntime>> {
        self.runtime
            .get()
            .filter(|runtime| runtime.can_create())
            .cloned()
    }

    /// Opens the runtime `setup` describes without the deployment key, so plain sessions kept
    /// here are served from now on. A runtime already open stays.
    pub async fn open_without_key(&self, setup: &StoreRuntimeSetup) -> Result<(), String> {
        if self.runtime.get().is_some() {
            return Ok(());
        }
        let runtime = setup
            .open_without_key(self.key_id, self.new_sessions)
            .await?;
        let _ = self.runtime.set(Arc::new(runtime));
        Ok(())
    }

    /// Installs `deployment` into the runtime, opening the runtime `setup` describes first when
    /// none is open, so every session kept here is served from now on. `deployment` must be the
    /// key this slot names. The first key installed stays: a second load changes nothing.
    pub async fn load(
        &self,
        setup: &StoreRuntimeSetup,
        deployment: DeploymentKey,
    ) -> Result<(), String> {
        if deployment.id() != self.key_id {
            return Err(format!(
                "deployment key {} was offered for the new storage, which is opened with key {}",
                deployment.id(),
                self.key_id
            ));
        }
        if self.key_loaded() {
            return Ok(());
        }
        self.open_without_key(setup).await?;
        let runtime = self.runtime().expect("the runtime was just opened");
        let parts = runtime.load_encrypted_parts(&deployment).await?;
        // Installed under the lock `holding` records refusals under, so a session refused just
        // before the key arrived is in the set `take_refused` returns afterwards.
        let _refused = self.refused.lock().unwrap_or_else(|e| e.into_inner());
        runtime.install_key(deployment, parts)
    }

    /// Whether the session `key` is kept in the new storage, whether or not the deployment key
    /// has loaded: one check of its head in each class root. A head found in two roots is
    /// refused `session_in_two_classes`.
    pub fn holds(&self, key: &BindIdentity) -> Result<bool, OpenError> {
        Ok(head_class(&self.state_root, key)?.is_some())
    }

    /// The runtime when the new storage keeps the session `key`; `None` for a session it
    /// doesn't keep. Once the key has loaded every session kept here is served. Before then the
    /// session's head names its mode: a plain session is served, and an encrypted one is refused
    /// `key_unavailable` and remembered, so the module opens it once the key loads
    /// (`take_refused`). The mode is read from the head without checking it, only to route the
    /// session: the open that serves it checks the head in that mode, and refuses a head that
    /// fails as damage, never reading it in the other mode.
    pub fn holding(&self, key: &BindIdentity) -> Result<Option<Arc<StoreRuntime>>, OpenError> {
        let Some(class) = head_class(&self.state_root, key)? else {
            return Ok(None);
        };
        if let Some(runtime) = self.runtime.get() {
            if runtime.key_loaded() || stored_cipher(&self.state_root, class, key)? == Cipher::None
            {
                return Ok(Some(Arc::clone(runtime)));
            }
        }
        let mut refused = self.refused.lock().unwrap_or_else(|e| e.into_inner());
        // The key may have loaded since the check above; `load` installs it under this lock.
        if let Some(runtime) = self.runtime.get().filter(|runtime| runtime.key_loaded()) {
            return Ok(Some(Arc::clone(runtime)));
        }
        refused.insert(key.clone());
        Err(key_unavailable(self.key_id))
    }

    /// The sessions refused since the last call because no key had loaded.
    pub fn take_refused(&self) -> Vec<BindIdentity> {
        let mut refused = self.refused.lock().unwrap_or_else(|e| e.into_inner());
        refused.drain().collect()
    }
}

/// The refusal of a session kept in the new storage while its deployment key `key_id` hasn't
/// loaded. Transient: the key loads without a restart, and the session is served then.
pub fn key_unavailable(key_id: u16) -> OpenError {
    OpenError::Classified {
        reason: format!(
            "key_unavailable: the new storage's deployment key {key_id} has not loaded yet; \
             retry once it has"
        ),
        class: broca_protocol::ErrorClass::Transient,
    }
}

/// The refusal of a session whose head is in two of the new storage's roots: damage, which an
/// operator must look at, so it is permanent and never served from either root.
fn session_in_two_classes(refusal: &broca_wal::SessionInTwoClasses) -> OpenError {
    OpenError::Classified {
        reason: refusal.to_string(),
        class: broca_protocol::ErrorClass::Permanent,
    }
}

/// The class of the root under `state_root` holding the session `key`'s head: one
/// `symlink_metadata` of its head in each root. A head that can't be checked is an error, never
/// read as absent, and a head in two roots is refused `session_in_two_classes`.
fn head_class(state_root: &Path, key: &BindIdentity) -> Result<Option<StorageClass>, OpenError> {
    let present = |head: &Path| match std::fs::symlink_metadata(head) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    };
    broca_wal::session_storage_class_by(state_root, &broca_core::session_addr(key), present)
        .map_err(|error| match broca_wal::SessionInTwoClasses::of(&error) {
            Some(refusal) => session_in_two_classes(refusal),
            None => OpenError::Open(format!(
                "the new storage's head of this session could not be checked: {error}"
            )),
        })
}

/// How many times a read probes the session's position again after a writer moved its head
/// between the probe and the load. A head moves on a writer's open, a seal or a revert, never on
/// an append, so a second probe nearly always holds.
const POSITION_TRIES: usize = 3;

fn recovery_changes_reads(view: &SessionView<LocalBackend>) -> bool {
    crate::store_actor::recovery_changes_reads(&view.operations)
}

/// A view that couldn't be loaded, as the opener reports it.
fn view_error(error: SessionError) -> OpenError {
    match error {
        SessionError::SessionMoving { cut_id } => OpenError::SessionMoving { cut_id },
        SessionError::SessionMoved { holder } => OpenError::SessionMoved { holder },
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

/// The cipher mode the head of the session `key`, kept in the root of `class`, names, read
/// without checking the head: only to route the session before the deployment key has loaded. A
/// head that can't be read or decoded is an error, so the session is never served on a guess.
fn stored_cipher(
    state_root: &Path,
    class: StorageClass,
    key: &BindIdentity,
) -> Result<Cipher, OpenError> {
    let unreadable = |error: String| {
        OpenError::Open(format!(
            "the new storage's head of this session could not be read: {error}"
        ))
    };
    let (stored, _) = LocalBackend::new(class.store_root(state_root))
        .read_stored_head(&SessionAddr(broca_core::session_addr(key)))
        .map_err(|error| unreadable(format!("{error:?}")))?
        .ok_or_else(|| unreadable("it no longer exists".into()))?;
    broca_session_store::head::stored_cipher(&stored)
        .map_err(|error| unreadable(format!("{error:?}")))
}

/// Setup parts as a setup store's `load_all` reads them.
type LoadedParts = Vec<(broca_session_store::codec::Digest, Arc<[u8]>)>;

/// The triple a session is created and loaded under: the bind identity's, spelled as
/// `broca_core::session_addr` spells it, so the triple and the address name one session.
pub(crate) fn triple(key: &BindIdentity) -> SessionTriple {
    SessionTriple {
        project_root: key.project_root.display().to_string(),
        harness: key.harness.clone(),
        session: key.session.clone(),
    }
}

fn open_error(error: SessionError, create: bool) -> OpenError {
    match error {
        SessionError::SessionMoving { cut_id } => return OpenError::SessionMoving { cut_id },
        SessionError::SessionMoved { holder } => return OpenError::SessionMoved { holder },
        _ => {}
    }
    let action = if create { "created" } else { "opened" };
    OpenError::Open(format!(
        "the session could not be {action} in the new storage: {error:?}"
    ))
}

pub mod recovery;
pub(crate) mod recovery_index;

#[cfg(test)]
#[path = "store_open_tests.rs"]
mod tests;
