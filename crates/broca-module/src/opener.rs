//! The default lineage opener: acquire the session lease, then open the session's actor in the
//! session storage, which keeps the lease for the actor's lifetime.
//!
//! The opener is the seam between the writer-agnostic registry and the concrete lease, storage
//! and executor. The race harness supplies a fake opener (in-process lease + fake executor) so
//! the admission concurrency is deterministically testable; this is the real one.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use broca_core::{lease_key_for, LeaseError, LeaseStore};

use crate::actor::IDLE_TTL;
use crate::executor::RunExecutor;
use crate::registry::{
    ActorHandle, LineageKey, LineageOpener, OpenError, RegistryHandle, StoreCreation,
};

/// Opens a lineage with a real lease, the session storage and the configured executor.
pub struct DefaultOpener<L: LeaseStore> {
    lease_store: Arc<L>,
    /// The session classes whose sessions start in the backed-up root.
    backup_classes: Vec<String>,
    executor: Arc<dyn RunExecutor>,
    idle_ttl: Duration,
    charge_basis_resolver: Option<Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>>,
    /// Serves sessions kept in the session storage, and starts new ones there when the registry
    /// marks their creating send. `None` serves no session.
    store: Option<Arc<crate::store_open::StoreSlot>>,
    step_events: Option<Arc<crate::step_events::StepEvents>>,
}

impl<L: LeaseStore> DefaultOpener<L> {
    pub fn new(lease_store: Arc<L>, executor: Arc<dyn RunExecutor>) -> Self {
        Self {
            lease_store,
            backup_classes: vec!["head".into()],
            executor,
            idle_ttl: IDLE_TTL,
            charge_basis_resolver: None,
            store: None,
            step_events: None,
        }
    }

    /// The session classes whose new sessions start in the backed-up root.
    pub fn with_backup_classes(mut self, classes: Vec<String>) -> Self {
        self.backup_classes = classes;
        self
    }

    /// Install the independent, lossy attempt publisher on both storage paths.
    pub fn with_step_events(mut self, events: Arc<crate::step_events::StepEvents>) -> Self {
        self.step_events = Some(events);
        self
    }

    /// Serves sessions kept in the session store through `store`.
    pub fn with_store_runtime(mut self, store: Arc<crate::store_open::StoreRuntime>) -> Self {
        self.store = Some(Arc::new(crate::store_open::StoreSlot::loaded(store)));
        self
    }

    /// Serves sessions kept in the session store through `slot`, whose runtime is installed once
    /// the deployment key loads. Until then those sessions are refused `key_unavailable`.
    pub fn with_store_slot(mut self, slot: Arc<crate::store_open::StoreSlot>) -> Self {
        self.store = Some(slot);
        self
    }

    /// Override the idle-close TTL (tests use a short one to exercise the close races).
    pub fn with_idle_ttl(mut self, ttl: Duration) -> Self {
        self.idle_ttl = ttl;
        self
    }

    /// Configure the provider/auth resolver used to derive immutable charge-basis facts.
    pub fn with_charge_basis_resolver(
        mut self,
        resolver: Arc<dyn broca_core::ChargeBasisResolver + Send + Sync>,
    ) -> Self {
        self.charge_basis_resolver = Some(resolver);
        self
    }
}

#[async_trait]
impl<L: LeaseStore + 'static> LineageOpener for DefaultOpener<L> {
    async fn check_plan_selection(
        &self,
        plan: &serde_json::Value,
    ) -> Result<
        Option<cortexkit_role_llm_runner::head_selection::PlanModel>,
        cortexkit_role_llm_runner::head_selection::PlanModelProblem,
    > {
        self.executor.check_plan_selection(plan).await
    }
    async fn compaction_context_limit(
        &self,
        model: &cortexkit_role_llm_runner::head_selection::PlanModel,
    ) -> Result<Option<u64>, String> {
        self.executor.compaction_context_limit(model).await
    }
    fn on_session_deleted(&self, session: &LineageKey) {
        self.executor.on_session_deleted(session);
    }
    fn store_runtime(&self) -> Option<Arc<crate::store_open::StoreRuntime>> {
        self.store.as_ref().and_then(|slot| slot.runtime())
    }
    fn store_creation_pending(&self) -> Option<OpenError> {
        let slot = self.store.as_ref()?;
        match slot.creating_runtime() {
            Some(_) => None,
            None => Some(crate::store_open::key_unavailable(slot.key_id())),
        }
    }
    fn step_events(&self) -> Option<Arc<crate::step_events::StepEvents>> {
        self.step_events.clone()
    }
    fn store_holding(
        &self,
        key: &LineageKey,
    ) -> Result<Option<Arc<crate::store_open::StoreRuntime>>, OpenError> {
        match &self.store {
            Some(slot) => slot.holding(key),
            None => Ok(None),
        }
    }
    fn retire_session_transport(&self, session: &LineageKey) {
        self.executor.retire_session_transport(session);
    }

    fn retire_idle_session_transport(&self, session: &LineageKey) {
        self.executor.retire_idle_session_transport(session);
    }

    fn retire_all_transports(&self) {
        self.executor.retire_all_transports();
    }
    async fn media_profile(
        &self,
        request: &broca_protocol::request::SubmissionRequest,
    ) -> Result<Option<Box<broca_provider::media_profile::MediaProfile>>, broca_core::RunError>
    {
        self.executor.media_profile(request).await
    }
    fn check_move(&self, key: &LineageKey, write: bool) -> Result<(), OpenError> {
        match self.store_holding(key)? {
            Some(store) => store.check_move(key, write),
            None => Ok(()),
        }
    }
    fn serves_from_store(&self, key: &LineageKey) -> Result<bool, OpenError> {
        Ok(self.store_holding(key)?.is_some())
    }
    fn elicitation_plane(&self) -> Option<Arc<dyn broca_core::tool_plane::ElicitationPlane>> {
        self.executor.elicitation_plane()
    }
    fn hook_plane_available(&self) -> bool {
        self.executor.hook_plane().is_some()
    }

    fn compaction_supported(&self) -> bool {
        self.executor.compaction_plane().is_some()
    }
    fn backup_classes(&self) -> Vec<String> {
        self.backup_classes.clone()
    }

    async fn open(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.open_with_retention(key, gen, registry, None).await
    }

    async fn open_for_status(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
    ) -> Result<ActorHandle, OpenError> {
        self.open_mode(key, gen, registry, None, None, true).await
    }

    async fn open_with_retention(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
    ) -> Result<ActorHandle, OpenError> {
        self.open_mode(key, gen, registry, retention, None, false)
            .await
    }

    async fn open_with_class(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
        session_class: Option<String>,
    ) -> Result<ActorHandle, OpenError> {
        self.open_mode(key, gen, registry, retention, session_class, false)
            .await
    }
}

impl<L: LeaseStore + 'static> DefaultOpener<L> {
    async fn open_mode(
        &self,
        key: &LineageKey,
        gen: u64,
        registry: RegistryHandle,
        retention: Option<u64>,
        session_class: Option<String>,
        status: bool,
    ) -> Result<ActorHandle, OpenError> {
        // Every session lives in the session store: one it doesn't hold, and that no creating send
        // or import marked, doesn't exist.
        let Some(slot) = &self.store else {
            return Err(OpenError::UnknownSession);
        };
        debug_assert!(
            key.validate_keyable().is_ok(),
            "DefaultOpener identity must be validated before durable key derivation"
        );
        // Acquire the session's lease, held for the actor's whole lifetime.
        let authority = loop {
            match self.lease_store.acquire(&lease_key_for(key)) {
                Ok(authority) => break authority,
                Err(LeaseError::Held { .. }) => {
                    // Retention deleting this session's expired copy holds the lease; an open
                    // waits for the deletion. Another live writer is refused.
                    let deleting = !status
                        && registry
                            .retention_service()
                            .is_some_and(|service| service.store_deletion_in_flight(key));
                    if !deleting {
                        return Err(OpenError::LeaseHeld(key.session.clone()));
                    }
                    let stop = registry.stopping_signal();
                    tokio::select! {
                        _ = stop.cancelled() => return Err(OpenError::ModuleStopping),
                        _ = tokio::time::sleep(Duration::from_millis(50)) => {}
                    }
                }
                Err(error) => return Err(OpenError::Open(error.to_string())),
            }
        };
        if let Some(runtime) = slot.runtime() {
            // Deletion leaves an encrypted identity even after the last head is gone. A missing
            // key cannot turn that identity into a new session.
            runtime.expiry_markers(key)?;
        }
        // Decided under the lease, so no other open of this session decides beside it. A
        // session in the session store is served from there, and refused while it is encrypted and
        // its key hasn't loaded. One that exists nowhere starts there when the registry marked its
        // creating send or import; while the storage's key hasn't loaded (the first seconds after
        // a start), that send is refused as retryable. It starts in the root of the class its
        // send's label selects for backup; an import, which only brings an agent head onto broca,
        // always is. The actor keeps the lease until its session is closed.
        let mut held = slot.holding(key)?;
        if let (Some(store), Some(service)) = (&held, registry.retention_service()) {
            let now_ms = service.clock.now_ms();
            if let Some(expired_at_ms) = store.expired_at(key, None, now_ms).await? {
                if status {
                    return Err(OpenError::Expired { expired_at_ms });
                }
                if service
                    .delete_store_under_lease(store, key, now_ms)
                    .await
                    .map_err(OpenError::Open)?
                {
                    held = None;
                }
            }
        }
        let creating = if held.is_none() && !status {
            registry.take_store_eligible(key)
        } else {
            None
        };
        let store = match (held, creating) {
            (Some(store), _) => store,
            (None, Some(_)) => match slot.creating_runtime() {
                Some(runtime) => runtime,
                None => return Err(crate::store_open::key_unavailable(slot.key_id())),
            },
            (None, None) => return Err(OpenError::UnknownSession),
        };
        if retention.is_some() && registry.retention_service().is_none() {
            return Err(OpenError::Open(
                "retention service is not configured".into(),
            ));
        }
        let class = creating.map(|creation| {
            let durable = match creation {
                StoreCreation::Send | StoreCreation::Head => {
                    broca_wal::selected_for_backup(session_class.as_deref(), &self.backup_classes)
                }
                StoreCreation::Import => true,
            };
            broca_wal::placement(retention.is_some(), durable)
        });
        store
            .open_actor(
                key,
                gen,
                registry,
                Box::new(authority),
                class,
                self.step_events.as_deref(),
            )
            .await
    }
}
