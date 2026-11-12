//! Shares the first run's frozen tools and plan with the reader that adds a send's baseline.

use std::sync::{Arc, OnceLock};

use broca_core::FirstEpisodeV1;
use broca_session_loop::parts::Parts;
use broca_session_loop::writer::Writer;
use broca_session_store::backend::SessionBackend;
use broca_session_store::session::Session;

#[derive(Default)]
pub(super) struct Admission(Arc<OnceLock<FirstEpisodeV1>>);

impl Admission {
    /// The session's first episode, once known.
    pub(super) fn known(&self) -> Option<&FirstEpisodeV1> {
        self.0.get()
    }

    /// Learn only from the durable first start, never from a send that may still be refused.
    pub(super) fn learn<B: SessionBackend>(
        &self,
        session: &Session<B>,
        parts: &dyn Parts,
        key: &broca_protocol::BindIdentity,
        registry: Option<&crate::registry::RegistryHandle>,
    ) -> Result<(), String> {
        if self.0.get().is_some() {
            return Ok(());
        }
        if session.operations().owner.awaiting_import_admission() {
            return Ok(());
        }
        let Some(first) = session.operations().owner.first_start.as_ref() else {
            return Ok(());
        };
        let admission = broca_session_loop::run::admission_of_start(first, parts)
            .map_err(|error| format!("the admitting episode's baseline: {error:?}"))?;
        if let Some(registry) = registry.filter(|registry| registry.declares_cache()) {
            let config = broca_session_loop::run::run_config_of_start(first, parts)
                .map_err(|error| format!("the first run's cache declaration: {error:?}"))?;
            if let Some(declaration) = registry.cache_declaration(&config) {
                registry.note_admitted_cache(
                    key,
                    broca_protocol::RunId(first.run_id.clone()),
                    declaration,
                );
            }
        }
        let _ = self.0.set(FirstEpisodeV1 {
            run_id: broca_protocol::RunId(first.run_id.clone()),
            tool_names: admission
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect(),
            manifest: admission.manifest,
        });
        Ok(())
    }
}

impl<B: SessionBackend + Send + Sync + 'static> super::StoreActor<B> {
    /// Share the same immutable first episode on a fresh open and after an actor reopens.
    pub(crate) fn first_episode_handle(
        &self,
        registry: &crate::registry::RegistryHandle,
    ) -> Result<Arc<OnceLock<FirstEpisodeV1>>, String> {
        self.admission
            .learn(&self.session, &self.parts, &self.key, Some(registry))?;
        Ok(Arc::clone(&self.admission.0))
    }
}
