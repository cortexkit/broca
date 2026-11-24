//! Opening a session kept in the session store (`broca_session_store`) as the module's actor for
//! it.
//!
//! The opener takes the session lease, then hands the session here when its head exists. For a
//! session that exists in neither store, it does so when the registry marked the creating send
//! as one the session store serves.
//!
//! A session here is encrypted or plain, as its head says (see `broca_session_store::keys`). A
//! plain session is served from the module's start; an encrypted one once the deployment key has
//! loaded from the vault. `session_storage.encryption` in `broca.jsonc` chooses the mode of the
//! sessions created here.
//!
//! The session store is three complete stores, one per backup class (`broca_records::session_storage::StorageClass`):
//! each root has its own heads, logs, objects and setup parts, and all share the deployment key.
//! A session is created in the root its creating send's class places it in
//! (`broca_records::session_storage::placement`), and from then on the root holding its head is where it is opened,
//! read and written. A head found in two roots is refused by name, never served from either.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;

use broca_protocol::BindIdentity;
use broca_records::session_storage::StorageClass;
use broca_session_loop::durable::SetupParts;
use broca_session_loop::parts::{DeploymentParts, SetupSource, SharedParts};
use broca_session_store::backend::{LocalBackend, SessionAddr};
use broca_session_store::head::SessionTriple;
use broca_session_store::keys::{Cipher, DeploymentKey};
use broca_session_store::session::{Position, Session, SessionError};
use tokio::sync::mpsc;

use crate::executor::RunExecutor;
use crate::registry::{ActorHandle, OpenError, RegistryHandle};
use crate::store_actor::{Lifecycle, StoreActor};

/// The capacity of a store session's command channel.
const COMMANDS: usize = 64;

#[path = "store_expiry.rs"]
mod expiry;

/// What the module needs to serve sessions kept in the session store. Opened without the
/// deployment key it serves plain sessions only; the key is installed into it once it loads
/// (see [`StoreSlot::load`]), and from then on it serves every session.
pub struct StoreRuntime {
    state_root: PathBuf,
    /// The id of the deployment key encrypted sessions open with.
    key_id: u16,
    /// The mode of the sessions this runtime creates.
    new_sessions: Cipher,
    /// The deployment key, once it has loaded.
    deployment: OnceLock<Arc<DeploymentKey>>,
    /// What sessions are opened with before the key has loaded: plain sessions open, encrypted
    /// ones refuse.
    unloaded: Arc<DeploymentKey>,
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
    views: crate::store_reads::ViewCache<crate::store_read::StoredView<LocalBackend>>,
    /// Heads of sessions no actor holds, kept by position (see `store_head::KeptHeads`).
    heads: crate::store_head::KeptHeads,
    /// Where each session's run events go once their records are durable; none in tests and
    /// while run events are off.
    run_events: Option<Arc<dyn crate::store_run_events::RunEventSink>>,
    /// Where each session's send state notices go once their records are durable; none in
    /// tests and while run events are off.
    send_states: Option<Arc<dyn crate::store_send_states::SendStateSink>>,
    /// Reads of a log's file, but for those of only its checkpoint frame, in tests.
    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) log_reads: Arc<std::sync::atomic::AtomicUsize>,
    /// Reads of only a log's checkpoint frame, which a view's stream head takes, in tests.
    #[cfg(all(test, feature = "test-clock"))]
    checkpoint_frame_reads: Arc<std::sync::atomic::AtomicUsize>,
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
    /// The runtime for the session store under `state_root`, its three class roots (see
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
        let parts = runtime.encrypted_parts(&deployment)?;
        runtime.install_key(deployment, parts)?;
        Ok(runtime)
    }

    /// The runtime for the session store under `state_root` before deployment key `key_id` has
    /// loaded. It serves plain sessions, whose setup parts it loads from each root, and creates
    /// sessions in mode `new_sessions`, which it can do only for plain ones until the key loads.
    pub async fn open_without_key(
        state_root: &Path,
        key_id: u16,
        new_sessions: Cipher,
        executor: Arc<dyn RunExecutor>,
        idle_ttl: Duration,
    ) -> Result<Self, String> {
        // What an import crashed while staging is never published; nothing is staging yet.
        crate::store_import_stage::clear_import_staging(state_root).map_err(|error| {
            format!("the new storage's import staging could not be cleared: {error}")
        })?;
        let mut roots = Vec::with_capacity(StorageClass::ALL.len());
        for class in StorageClass::ALL {
            let backend = LocalBackend::new(class.store_root(state_root));
            recovery_index::prepare(&class.store_root(state_root))
                .map_err(|error| format!("prepare {class} recovery candidates: {error}"))?;
            roots.push(RootParts {
                parts: SharedParts::new(Box::new(DeploymentParts::default())),
                plain_parts: SharedParts::new(Box::new(DeploymentParts::stored(Arc::new(
                    SetupSource {
                        backend,
                        key: broca_session_store::setup::SetupKey::plain(),
                    },
                )))),
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
            unloaded: Arc::new(DeploymentKey::unloaded(key_id).creating(new_sessions)),
            roots,
            executor,
            idle_ttl,
            billing: None,
            run_events: None,
            send_states: None,
            import_roots: Arc::new(
                crate::store_import::ImportRoots::with_default(
                    None,
                    Some(&crate::store_import::default_import_folder(state_root)),
                )
                .with_scratch(crate::store_import_stage::import_staging_root(state_root)),
            ),
            settled: Arc::default(),
            views: crate::store_reads::ViewCache::new(
                crate::store_reads::ViewCache::MAX_ENTRIES,
                crate::store_reads::ViewCache::MAX_BYTES,
            ),
            heads: Default::default(),
            #[cfg(all(test, feature = "test-clock"))]
            log_reads: Arc::default(),
            #[cfg(all(test, feature = "test-clock"))]
            checkpoint_frame_reads: Arc::default(),
        })
    }

    /// The encrypted sessions' setup parts of each class root, by [`StorageClass::index`], read
    /// from the root's setup store under `deployment`, which must have loaded. Nothing is read
    /// until a session names a part.
    fn encrypted_parts(&self, deployment: &DeploymentKey) -> Result<Vec<DeploymentParts>, String> {
        StorageClass::ALL
            .iter()
            .map(|class| {
                let key = deployment
                    .setup_key_for(Cipher::XChaCha20Poly1305)
                    .ok_or("the deployment key offered has not loaded")?;
                Ok(DeploymentParts::stored(Arc::new(SetupSource {
                    backend: self.backend(*class),
                    key,
                })))
            })
            .collect()
    }

    /// Installs the deployment key and the encrypted sessions' setup parts read under it, one
    /// per class root, so encrypted sessions are served from now on. The parts go in first:
    /// whoever sees the key installed finds them. A runtime keeps the first key installed.
    fn install_key(
        &self,
        deployment: DeploymentKey,
        parts: Vec<DeploymentParts>,
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
                .map_err(|_| "a holder of the encrypted setup parts panicked")? = Box::new(parts);
        }
        let _ = self
            .deployment
            .set(Arc::new(deployment.creating(self.new_sessions)));
        Ok(())
    }

    /// Whether the deployment key has loaded, so encrypted sessions are served.
    pub fn key_loaded(&self) -> bool {
        self.deployment.get().is_some()
    }

    /// Installs `deployment` into a runtime opened without it, as the module's key loader does
    /// once the vault answers.
    #[cfg(test)]
    pub(crate) fn load_key_for_test(&self, deployment: DeploymentKey) -> Result<(), String> {
        let parts = self.encrypted_parts(&deployment)?;
        self.install_key(deployment, parts)
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
        self.deployment_shared_ref()
    }

    /// The deployment key as [`StoreRuntime::deployment`] gives it, shared, for what outlives a
    /// borrow of the runtime (an import's staging loads the session it publishes with it).
    pub fn deployment_shared(&self) -> Arc<DeploymentKey> {
        Arc::clone(self.deployment_shared_ref())
    }

    fn deployment_shared_ref(&self) -> &Arc<DeploymentKey> {
        self.deployment.get().unwrap_or(&self.unloaded)
    }

    /// Reads file-source imports from `roots` (`import.roots` in `broca.jsonc`, loaded at
    /// boot) instead of broca's own import folder alone. That folder stays the only place an
    /// import deletes its source from.
    pub fn with_import_roots(mut self, roots: Option<Vec<PathBuf>>) -> Self {
        self.import_roots = Arc::new(
            crate::store_import::ImportRoots::with_default(
                roots,
                Some(&crate::store_import::default_import_folder(
                    &self.state_root,
                )),
            )
            // An SQLite source is copied where import staging lives: private to broca, and
            // cleared of what a crash left whenever the session store opens.
            .with_scratch(crate::store_import_stage::import_staging_root(
                &self.state_root,
            )),
        );
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

    /// Each class root's setup-part stores, encrypted and plain, with what they hold and how
    /// they have been read, for `memory.report`.
    pub fn setup_store_usage(&self) -> Vec<broca_wire::MemorySetupStore> {
        use broca_session_loop::parts::Parts as _;
        let mut stores = Vec::new();
        for class in StorageClass::ALL {
            for (mode, cipher) in [
                ("encrypted", Cipher::XChaCha20Poly1305),
                ("plain", Cipher::None),
            ] {
                let Some(usage) = self.parts_for(class, cipher).usage() else {
                    continue;
                };
                stores.push(broca_wire::MemorySetupStore {
                    class: class.to_string(),
                    mode: mode.to_owned(),
                    held_parts: usage.held_parts,
                    held_bytes: usage.held_bytes,
                    unstored_bytes: usage.unstored_bytes,
                    store_reads: usage.store_reads,
                    store_read_bytes: usage.store_read_bytes,
                    reads_after_eviction: usage.reads_after_eviction,
                    decodes: usage.decodes,
                    decoded_bytes: usage.decoded_bytes,
                });
            }
        }
        stores
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
    pub fn views(
        &self,
    ) -> &crate::store_reads::ViewCache<crate::store_read::StoredView<LocalBackend>> {
        &self.views
    }

    /// Writes the spend facts of every run in the session store to `billing`, the deployment's
    /// usage export.
    pub fn with_billing(mut self, billing: crate::store_billing::StoreBilling) -> Self {
        self.billing = Some(billing);
        self
    }

    /// Sessions opened from now on hand their run events to `sink`.
    pub fn with_run_events(mut self, sink: Arc<dyn crate::store_run_events::RunEventSink>) -> Self {
        self.run_events = Some(sink);
        self
    }

    /// Sessions opened from now on hand their send state notices to `sink`.
    pub fn with_send_states(
        mut self,
        sink: Arc<dyn crate::store_send_states::SendStateSink>,
    ) -> Self {
        self.send_states = Some(sink);
        self
    }

    /// The id of the deployment key this runtime opens encrypted sessions with.
    pub fn key_id(&self) -> u16 {
        self.key_id
    }

    /// Whether the session `key` is kept in the session store: whether its head exists in one of
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
        // The session's head, when a root keeps it.
        let head = || -> Result<Option<broca_session_store::head::Head>, OpenError> {
            let Some(class) = self.class_of(key)? else {
                return Ok(None);
            };
            let Some((stored, _)) = self
                .backend(class)
                .read_stored_head(&SessionAddr(broca_core::session_addr(key)))
                .map_err(|error| OpenError::Open(format!("{error:?}")))?
            else {
                return Ok(None);
            };
            let (head, _) = broca_session_store::head::open(&stored, self.deployment())
                .map_err(|error| open_error(SessionError::Head(error), false))?;
            Ok(Some(head))
        };
        if broca_records::move_fence::read_json::<broca_records::move_fence::Moved>(
            &sidecar.with_extension("moved"),
        )
        .map_err(read)?
        .is_some()
        {
            // The fence names the new computer only by its backup author id, which a caller is
            // never told. The head names its box id when the release gave one; a head that
            // can't be read yet still leaves the session refused, naming no computer.
            let holder = match head() {
                Ok(Some(head)) => match head.residence {
                    broca_session_store::head::Residence::Moved(moved) => moved.holder_box,
                    _ => None,
                },
                _ => None,
            };
            return Err(OpenError::SessionMoved { holder });
        }
        if write {
            if let Some(freeze) = broca_records::move_fence::read_json::<
                broca_records::move_fence::Freeze,
            >(&sidecar.with_extension("moving"))
            .map_err(read)?
            {
                return Err(OpenError::SessionMoving {
                    cut_id: freeze.cut_id,
                });
            }
            if let Some(cut_id) =
                broca_records::move_fence::cut_fence(&self.state_root, &reference).map_err(read)?
            {
                return Err(OpenError::SessionMoving { cut_id });
            }
        }
        let Some(head) = head()? else {
            return Ok(());
        };
        match head.residence {
            broca_session_store::head::Residence::Moving(cut_id) if write => {
                Err(OpenError::SessionMoving { cut_id })
            }
            broca_session_store::head::Residence::Moved(moved) => Err(OpenError::SessionMoved {
                holder: moved.holder_box,
            }),
            _ => Ok(()),
        }
    }

    /// The class of the root holding the session `key`'s head, `None` when no root holds it.
    /// Errors as [`holds`](Self::holds).
    pub fn class_of(&self, key: &BindIdentity) -> Result<Option<StorageClass>, OpenError> {
        head_class(&self.state_root, key)
    }

    /// Whether the session `key`, kept here, can be read now: every session once the deployment
    /// key has loaded, and a plain one before. `false` for a session no root holds.
    pub fn readable_now(&self, key: &BindIdentity) -> Result<bool, OpenError> {
        if self.key_loaded() {
            return Ok(true);
        }
        match self.class_of(key)? {
            Some(class) => Ok(stored_cipher(&self.state_root, class, key)? == Cipher::None),
            None => Ok(false),
        }
    }

    /// The store of the root of `class`.
    fn backend(&self, class: StorageClass) -> LocalBackend {
        let backend = LocalBackend::new(class.store_root(&self.state_root));
        #[cfg(all(test, feature = "test-clock"))]
        let backend = backend.with_log_reads_for_test(
            Arc::clone(&self.log_reads),
            Arc::clone(&self.checkpoint_frame_reads),
        );
        backend
    }

    /// Only lower the cache cap, so a small fixture can exercise an uncached read.
    #[cfg(all(test, feature = "test-clock"))]
    pub(crate) fn with_view_bytes_for_test(mut self, max_bytes: u64) -> Self {
        assert!(max_bytes <= crate::store_reads::ViewCache::MAX_BYTES);
        self.views = crate::store_reads::ViewCache::new(
            crate::store_reads::ViewCache::MAX_ENTRIES,
            max_bytes,
        );
        self
    }

    /// The store of the root of `class`, for a reader or writer that holds no actor: a fork's
    /// own session (`crate::fork_answer`).
    pub(crate) fn class_backend(&self, class: StorageClass) -> LocalBackend {
        self.backend(class)
    }

    /// The executor this runtime's sessions run on.
    pub(crate) fn executor(&self) -> &Arc<dyn RunExecutor> {
        &self.executor
    }

    /// Where spend facts are written, when this runtime writes them.
    pub fn billing(&self) -> Option<&crate::store_billing::StoreBilling> {
        self.billing.as_ref()
    }

    /// The session `key` as it stands now, read-only: writing nothing and taking no lease (see
    /// `Session::view`). The view is kept, and served again while the session stays where it
    /// was read (see `store_reads::ViewCache`). A session whose creation never committed holds no
    /// record, so it reads as having none (`None`). A log that needs a recovery refuses,
    /// transiently: the session's next send opens its actor, which repairs it.
    pub async fn current_view(
        &self,
        key: &BindIdentity,
    ) -> Result<Option<Arc<crate::store_read::StoredView<LocalBackend>>>, OpenError> {
        self.current_view_for(key, "unnamed").await
    }

    /// [`current_view`](Self::current_view), counting a load under `reason` in the memory
    /// report (see `store_reads::ViewCache::view`).
    pub async fn current_view_for(
        &self,
        key: &BindIdentity,
        reason: &'static str,
    ) -> Result<Option<Arc<crate::store_read::StoredView<LocalBackend>>>, OpenError> {
        match self.class_of(key)? {
            Some(class) => self
                .current_view_in(key, class, reason)
                .await
                .map_err(view_error),
            None => Ok(None),
        }
    }

    /// [`current_view`](Self::current_view) of the session `key`, kept in the root of `class`.
    async fn current_view_in(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        reason: &'static str,
    ) -> Result<Option<Arc<crate::store_read::StoredView<LocalBackend>>>, SessionError> {
        let addr = SessionAddr(broca_core::session_addr(key));
        let backend = self.backend(class);
        for _ in 0..POSITION_TRIES {
            let position = match Session::stored_position(&backend, &addr, self.deployment()).await
            {
                Ok(Some(position)) => position,
                Ok(None) => return Ok(None),
                Err(error) => return Err(error),
            };
            match self.view_at(key, class, position, reason).await {
                // A writer moved the head between the probe and the load: probe again.
                Err(SessionError::Moved) => continue,
                Err(SessionError::Missing) => return Ok(None),
                other => return other.map(Some),
            }
        }
        // Still moving: read it as it is, without keeping the view.
        match Session::view(backend, addr, &triple(key), self.deployment()).await {
            Ok(view) => {
                let position = view.position();
                Ok(Some(Arc::new(
                    crate::store_read::StoredView::new(Box::new(view), position).await,
                )))
            }
            Err(SessionError::Missing) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The heads kept for sessions no actor holds.
    pub(crate) fn heads(&self) -> &crate::store_head::KeptHeads {
        &self.heads
    }

    /// Where the session `key` stands now, when no actor holds it: the position a kept head is
    /// matched against, read from the session's head and its open log's length alone. `None`
    /// otherwise, and for anything this cheap read can't settle (a session moving, moved or
    /// needing a recovery), which the full read answers. A kept head skips the expiry check
    /// every other read makes, so a session in the ephemeral root, which can expire while it
    /// stays put, is left out once its deadline at this position has come. That deadline is
    /// read from its records once per position (see [`Self::deadline_of`]).
    pub(crate) async fn closed_position(
        &self,
        registry: &crate::LineageRegistry,
        key: &BindIdentity,
    ) -> Option<Position> {
        if registry.open_handle(key).is_some() {
            return None;
        }
        let class = self.class_of(key).ok()??;
        let addr = SessionAddr(broca_core::session_addr(key));
        let position = Session::stored_position(&self.backend(class), &addr, self.deployment())
            .await
            .ok()??;
        // Without retention nothing expires, as the expiry check finds.
        let retention = registry.handle().retention_service();
        if let (StorageClass::Ephemeral, Some(retention)) = (class, retention) {
            let deadline = match retention.kept_deadline(&addr.0, &position) {
                Some(deadline) => deadline,
                None => {
                    let view = self.view_at(key, class, position, "expiry").await.ok()?;
                    self.deadline_of(key, &view, Some(&retention)).await.ok()?
                }
            };
            if deadline.is_some_and(|deadline| deadline <= retention.clock.now_ms()) {
                return None;
            }
        }
        Some(position)
    }

    /// The session `key`, kept in the root of `class`, as it stood at `position`: the kept view,
    /// or one loaded there and kept.
    pub async fn view_at(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        position: Position,
        reason: &'static str,
    ) -> Result<Arc<crate::store_read::StoredView<LocalBackend>>, SessionError> {
        let addr = broca_core::session_addr(key);
        let triple = triple(key);
        let load = async {
            let view = Session::view_at(
                self.backend(class),
                SessionAddr(addr.clone()),
                &triple,
                self.deployment(),
                &position,
            )
            .await?;
            Ok(crate::store_read::StoredView::new(Box::new(view), position).await)
        };
        self.views.view(&addr, position, reason, load).await
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
        Box::pin(self.read_inner(registry, key, request, None)).await
    }

    /// The session `key` as stored up to its durable end, for a read that runs its own code over
    /// the session rather than one [`execute`](crate::store_reads::execute) runs. It passes the
    /// same checks as [`read`](Self::read): the expiry check, a recovery before a closed
    /// session that needs one is read, and an open actor making its records durable first, so
    /// the view never holds records a power loss could still take back. `reason` counts a load
    /// in the memory report. `None` for a session whose creation never committed.
    pub(crate) async fn durable_view(
        &self,
        registry: &crate::LineageRegistry,
        key: &BindIdentity,
        reason: &'static str,
    ) -> Result<Option<Arc<crate::store_read::StoredView<LocalBackend>>>, crate::serve::ServeError>
    {
        use crate::store_reads::{StoreReadReply, StoreReadRequest};
        let Some(class) = self.class_of(key)? else {
            return Ok(None);
        };
        for _ in 0..POSITION_TRIES {
            let mut served = None;
            let read = self.read_inner(
                registry,
                key,
                StoreReadRequest::DurableView(reason),
                Some(&mut served),
            );
            let position = match Box::pin(read).await? {
                Some(StoreReadReply::Position(position)) => position,
                None => return Ok(None),
                Some(_) => return Err(crate::store_reads::mismatched()),
            };
            // A closed session was read from a view loaded at that position: serve it again
            // rather than load it twice when it was too large to keep.
            if let Some(view) = served.filter(|view| view.position() == position) {
                return Ok(Some(view));
            }
            match self.view_at(key, class, position, reason).await {
                Ok(view) => return Ok(Some(view)),
                // The actor sealed after naming its position: ask it again.
                Err(SessionError::Moved) => continue,
                Err(SessionError::Missing) => return Ok(None),
                Err(error) => return Err(view_error(error).into()),
            }
        }
        Err(view_error(SessionError::Moved).into())
    }

    /// [`read`](Self::read), keeping in `served` the view a closed session's read was served
    /// from, when it was served from one.
    async fn read_inner(
        &self,
        registry: &crate::LineageRegistry,
        key: &BindIdentity,
        request: crate::store_reads::StoreReadRequest,
        served: Option<&mut Option<Arc<crate::store_read::StoredView<LocalBackend>>>>,
    ) -> Result<Option<crate::store_reads::StoreReadReply>, crate::serve::ServeError> {
        let run = match &request {
            crate::store_reads::StoreReadRequest::RunAdmission(run)
            | crate::store_reads::StoreReadRequest::RunStatus(run)
            | crate::store_reads::StoreReadRequest::RunStatusReply(run)
            | crate::store_reads::StoreReadRequest::RunResult(run) => Some(run.0.as_str()),
            _ => None,
        };
        let mut probed = registry.check_expired_with_view(key, run).await?;
        let addr = broca_core::session_addr(key);
        // The root the session is kept in. A session never changes roots, so a read that finds
        // none finds no session.
        let Some(class) = self.class_of(key)? else {
            return Ok(None);
        };
        if !request.is_authorization() && registry.open_handle(key).is_none() {
            let current = match probed.take() {
                Some(view) => Ok(Some(view)),
                None => self.current_view_in(key, class, request.name()).await,
            };
            let needs_actor = match current {
                Ok(Some(view)) => {
                    let needs_actor = recovery_changes_reads(&view);
                    if !needs_actor {
                        probed = Some(view);
                    }
                    needs_actor
                }
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
        if let Some(answer) = self
            .read_by_actor(registry, key, class, &request, probed.as_ref())
            .await
        {
            return answer.map(Some);
        }
        // The recovery probe already loaded this immutable position. Recheck its position,
        // not its contents, so a view too large to cache is still loaded only once per read.
        let probed = if let Some(view) = probed {
            let position = Session::stored_position(
                &self.backend(class),
                &SessionAddr(addr.clone()),
                self.deployment(),
            )
            .await
            .map_err(view_error)?;
            position
                .filter(|position| *position == view.position())
                .map(|_| view)
        } else {
            None
        };
        let current = match probed {
            Some(view) => Some(view),
            None => self
                .current_view_in(key, class, request.name())
                .await
                .map_err(view_error)?,
        };
        let Some(view) = current else {
            return Ok(None);
        };
        if !request.is_authorization() && recovery_changes_reads(&view) {
            // The actor closed before answering. Refuse rather than serve a pre-recovery
            // snapshot; a later read will reopen it through the same barrier.
            return Err(view_error(SessionError::NeedsRecovery).into());
        }
        let reply = self
            .read_stored(key, class, &view, registry.slot_state(key), &request)
            .await;
        if let Some(served) = served {
            *served = Some(view);
        }
        Ok(Some(reply))
    }

    /// Serves `request`, a read broca makes for itself (see `StoreReadRequest::is_internal`), on
    /// the session `key`: by its store actor, from memory, when the `registry` holds one open,
    /// else from the session as stored. Unlike [`read`](Self::read) it checks no expiry and
    /// opens no actor for a recovery: a session that needs one refuses, and the caller reads it
    /// again later. An actor that refuses the read leaves it to the session as stored. `None`
    /// for a session the store doesn't hold.
    pub(crate) async fn read_own(
        &self,
        registry: Option<&crate::LineageRegistry>,
        key: &BindIdentity,
        request: &crate::store_reads::StoreReadRequest,
    ) -> Result<Option<crate::store_reads::StoreReadReply>, crate::serve::ServeError> {
        let Some(class) = self.class_of(key)? else {
            return Ok(None);
        };
        if let Some(registry) = registry {
            if let Some(Ok(reply)) = self
                .read_by_actor(registry, key, class, request, None)
                .await
            {
                return Ok(Some(reply));
            }
        }
        let Some(view) = self
            .current_view_in(key, class, request.name())
            .await
            .map_err(view_error)?
        else {
            return Ok(None);
        };
        let slot = registry.and_then(|registry| registry.slot_state(key));
        Ok(Some(
            self.read_stored(key, class, &view, slot, request).await,
        ))
    }

    /// Asks the store actor of the session `key`, kept in the root of `class`, to serve
    /// `request`: from memory, or from the session as stored at the position the actor names
    /// when the read needs history it doesn't hold (`probed` when that is where it was loaded).
    /// `None` when no actor holds the session or it closed before answering, so the session is
    /// read as no actor holds it.
    async fn read_by_actor(
        &self,
        registry: &crate::LineageRegistry,
        key: &BindIdentity,
        class: StorageClass,
        request: &crate::store_reads::StoreReadRequest,
        probed: Option<&crate::store_read::StoredLocalView>,
    ) -> Option<Result<crate::store_reads::StoreReadReply, crate::serve::ServeError>> {
        use crate::store_reads::{execute, StoreReadOutcome};
        let addr = broca_core::session_addr(key);
        for _ in 0..POSITION_TRIES {
            let handle = registry.open_handle(key)?;
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
                Some(StoreReadOutcome::Served(reply)) => return Some(Ok(*reply)),
                Some(StoreReadOutcome::Refused(error)) => return Some(Err(error)),
                Some(StoreReadOutcome::Stored {
                    position,
                    live_step,
                    entry_speaks_for,
                }) => match if let Some(view) = probed.filter(|view| view.position() == position) {
                    Ok(Arc::clone(view))
                } else {
                    self.view_at(key, class, position, request.name()).await
                } {
                    Ok(view) => {
                        let counts = self.views.token_counts(&addr);
                        let mut context = self.read_context(key, &addr, class, &view);
                        context.held = true;
                        context.open = true;
                        context.live_step = live_step.as_ref();
                        context.entry_speaks_for = entry_speaks_for;
                        context.token_counts = Some(&counts);
                        return Some(Ok(execute(&view, &context, request).await));
                    }
                    // The actor sealed since it answered: ask it again.
                    Err(SessionError::Moved) => continue,
                    Err(error) => return Some(Err(view_error(error).into())),
                },
                // The actor closed under the read: read the session as no actor holds it.
                Some(StoreReadOutcome::Reopen) | None => return None,
            }
        }
        None
    }

    /// Serves `request` from `view`, the session `key` as stored in the root of `class`, whose
    /// registry `slot` (`registry.slot_state`) tells a live run from one a crash cut off.
    async fn read_stored(
        &self,
        key: &BindIdentity,
        class: StorageClass,
        view: &crate::store_read::StoredView<LocalBackend>,
        slot: Option<&'static str>,
        request: &crate::store_reads::StoreReadRequest,
    ) -> crate::store_reads::StoreReadReply {
        let addr = broca_core::session_addr(key);
        let counts = self.views.token_counts(&addr);
        let mut context = self.read_context(key, &addr, class, view);
        context.held = slot.is_some();
        context.open = slot == Some("open");
        context.token_counts = Some(&counts);
        crate::store_reads::execute(view, &context, request).await
    }

    /// What a read of `view`, the session `key` at address `addr` in the root of `class`, needs
    /// besides the session, as no actor holds it: the caller sets what an actor knows.
    fn read_context<'a>(
        &'a self,
        key: &'a BindIdentity,
        addr: &'a str,
        class: StorageClass,
        view: &crate::store_read::StoredView<LocalBackend>,
    ) -> crate::store_reads::ReadContext<'a> {
        crate::store_reads::ReadContext {
            labels: crate::store_awaits::Labels(Some(self.executor.clone())),
            token_counts: None,
            // A read resolves setup parts in the root and mode of the session it reads.
            parts: self.parts_for(class, view.head().cipher()),
            held: false,
            open: false,
            settled: Some((self.settled_runs(), addr)),
            session_name: &key.session,
            live_step: None,
            // A stream head the view couldn't project is left for the page to project again, so
            // that failure refuses the page, after its request and facts checks.
            stream_head: view.stream_head().ok(),
            // No actor answered, so none holds a Setup: a compaction read asks the catalog.
            entry_speaks_for: crate::store_entries::EntryAttribution::Catalog(
                self.executor.compaction_plane(),
            ),
            class: Some(class),
        }
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
            // session-store sessions by name don't know this one. So once a session lives here,
            // no build that would serve it from a log it doesn't have can be placed. The floor
            // is raised before the session's head exists.
            broca_records::floor::raise_floor(
                &self
                    .state_root
                    .join(broca_records::floor::FEATURE_FLOOR_FILENAME),
                &[broca_records::floor::SESSION_STORAGE_FEATURE],
            )
            .map_err(|error| {
                OpenError::Open(format!("the feature floor could not be raised: {error}"))
            })?;
            // A session on the current compaction contract keeps text delivered with a tool
            // result apart from the result, as every new session is. It is created in the mode
            // `session_storage.encryption` chose.
            // A session created where an expired one was deleted continues past the deleted
            // one's history version, so a reader holding that version sees a new history.
            let history_version = floors
                .iter()
                .map(broca_session_store::expired::Marker::next_history_version)
                .max()
                .unwrap_or(0);
            Session::create_with_history_version(
                backend,
                addr,
                triple,
                self.deployment(),
                false,
                history_version,
            )
            .await
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
        .with_class_placement(class, registry.backup_classes())
        .with_settled(Arc::clone(&self.settled))
        // A send's attachments are uploaded beside the session, under the state root, whichever
        // storage keeps it.
        .with_uploads(broca_core::media_uploads::Uploads::at(&self.state_root));
        if let Some(events) = step_events {
            actor = actor.with_step_events(events);
        }
        if let Some(service) = registry.retention_service() {
            actor = actor.with_retention_clock(service.clock.clone());
        }
        if create.is_some() {
            actor = actor.created_here();
        }
        actor =
            actor.with_import_staging(Arc::new(crate::store_import_stage::FsImportStaging::new(
                &self.state_root,
                class.store_root(&self.state_root),
                self.deployment_shared(),
            )));
        let readers_root = class.store_root(&self.state_root);
        actor = actor.with_readers(Arc::new(move || LocalBackend::new(readers_root.clone())));
        if let Some(billing) = &self.billing {
            actor = actor.with_billing(billing.clone());
        }
        if let Some(sink) = &self.run_events {
            actor = actor.with_run_events(Arc::clone(sink));
        }
        if let Some(sink) = &self.send_states {
            actor = actor.with_send_states(Arc::clone(sink));
        }
        actor = actor.with_run_outcomes();
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
            gen,
            tx,
            // The session store keeps no shared log, so there is no log offset to share.
            run_in_flight,
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
    /// Where run events go (see [`StoreRuntime::with_run_events`]).
    pub run_events: Option<Arc<dyn crate::store_run_events::RunEventSink>>,
    /// Where send state notices go (see [`StoreRuntime::with_send_states`]).
    pub send_states: Option<Arc<dyn crate::store_send_states::SendStateSink>>,
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
        if let Some(sink) = &self.run_events {
            runtime = runtime.with_run_events(Arc::clone(sink));
        }
        if let Some(sink) = &self.send_states {
            runtime = runtime.with_send_states(Arc::clone(sink));
        }
        runtime
    }
}

/// The state root alone tells the opener whether a session is kept in the session store, from
/// the first open. Its runtime is opened at the start without the deployment key
/// ([`StoreSlot::open_without_key`]) and serves plain sessions from then on. Serving an encrypted
/// session needs the deployment key, which comes from the vault and may load after the module has
/// started; it is installed into the runtime once it has. Until then an encrypted session kept
/// here is refused `key_unavailable`, a transient refusal.
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
    /// The session store under `state_root`, whose deployment key `key_id` hasn't loaded yet. It
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

    /// The session store served by `runtime`, its key already loaded.
    pub fn loaded(runtime: Arc<StoreRuntime>) -> Self {
        let slot = Self::awaiting_key(&runtime.state_root, runtime.key_id())
            .creating(runtime.new_sessions());
        let _ = slot.runtime.set(runtime);
        slot
    }

    /// The id of the deployment key the session store is opened with.
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

    /// The runtime used to create a session that exists in neither store, when creation is
    /// possible: a plain session from the module's start, an encrypted one once the key has
    /// loaded. `None` means this runtime does not create the session.
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
        let parts = runtime.encrypted_parts(&deployment)?;
        // Installed under the lock `holding` records refusals under, so a session refused just
        // before the key arrived is in the set `take_refused` returns afterwards.
        let _refused = self.refused.lock().unwrap_or_else(|e| e.into_inner());
        runtime.install_key(deployment, parts)
    }

    /// The runtime when the session store keeps the session `key`; `None` for a session it
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

/// The refusal of a session kept in the session store while its deployment key `key_id` hasn't
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

/// Whether `error` is the [`key_unavailable`] refusal. [`StoreSlot::holding`] records every
/// session it refuses this way, and the key loader opens them all once the key loads
/// ([`StoreSlot::take_refused`]), so a caller that opens sessions early (boot recovery) can
/// treat it as deferred rather than failed.
pub fn is_key_unavailable(error: &OpenError) -> bool {
    matches!(
        error,
        OpenError::Classified { reason, class: broca_protocol::ErrorClass::Transient }
            if reason.starts_with("key_unavailable:")
    )
}

/// The refusal of a session whose head is in two of the session store's roots: damage, which an
/// operator must look at, so it is permanent and never served from either root.
fn session_in_two_classes(
    refusal: &broca_records::session_storage::SessionInTwoClasses,
) -> OpenError {
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
    broca_records::session_storage::session_storage_class_by(
        state_root,
        &broca_core::session_addr(key),
        present,
    )
    .map_err(
        |error| match broca_records::session_storage::SessionInTwoClasses::of(&error) {
            Some(refusal) => session_in_two_classes(refusal),
            None => OpenError::Open(format!(
                "the new storage's head of this session could not be checked: {error}"
            )),
        },
    )
}

/// How many times a read probes the session's position again after a writer moved its head
/// between the probe and the load. A head moves on a writer's open, a seal or a revert, never on
/// an append, so a second probe nearly always holds.
const POSITION_TRIES: usize = 3;

fn recovery_changes_reads(view: &Session<LocalBackend>) -> bool {
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

/// The triple a session is created and loaded under: the bind identity's, spelled as
/// `broca_core::session_addr` spells it, so the triple and the address name one session.
/// The session triple a stored session's log names, from its bind identity.
pub fn triple(key: &BindIdentity) -> SessionTriple {
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

pub(crate) mod moved_here;
pub mod recovery;
pub(crate) mod recovery_index;

#[cfg(test)]
#[path = "store_open_tests.rs"]
mod tests;
