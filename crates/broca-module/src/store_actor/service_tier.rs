//! A send's service tier level on a session kept in the new storage, admitted by today's rule
//! (`LineageActor`'s send admission) before anything is written: a level the runner doesn't know,
//! or one the send's model can't be sent at, is refused; a running or paused run keeps the level
//! it froze; and with no level of its own, a session's first run takes the one its plan's model
//! selection names. The send then carries the level it was admitted at: into the run it starts,
//! whose admission applies it as today's executor applies a queued send's level, or into the
//! queue with it.

use broca_protocol::request::SubmissionRequest;
use broca_session_loop::writer::Writer as _;
use broca_session_store::backend::SessionBackend;

use super::StoreActor;
use crate::actor::SendOutcome;

impl<B: SessionBackend + Send + Sync + 'static> StoreActor<B> {
    /// Admits `request`'s service tier level, leaving it at the level its run is sent at, or
    /// answers today's refusal. `manifest` is the plan a session's first send fetched, whose
    /// model selection a first run follows.
    pub(super) fn admit_service_tier(
        &self,
        request: &mut SubmissionRequest,
        manifest: Option<&broca_wal::PlanManifest>,
    ) -> Result<(), SendOutcome> {
        use cortexkit_role_llm_runner::{head_selection, service_tier::ServiceTier};
        let operations = self.session.operations();
        // The session's choice of next-run model, as the send's model inheritance reads it: a
        // first send's from the plan it fetched, a later one's from the session.
        let selection = if operations.owner.needs_admission() {
            manifest.and_then(broca_core::head::manifest_selection)
        } else {
            broca_session_loop::plan::selection(operations, &self.parts)
                .map_err(SendOutcome::Transient)?
        };
        if !broca_protocol::service_tier_enabled() && selection.is_none() {
            return Ok(());
        }
        if request
            .service_tier_level
            .as_deref()
            .is_some_and(|tier| ServiceTier::parse(tier).is_none())
        {
            return Err(SendOutcome::ServiceTierInvalid {
                reason: "unsupported_tier",
                message: format!("unknown service_tier {:?}", request.service_tier_level),
            });
        }
        let latest = match operations.run.as_ref() {
            Some(run) => match run.start.as_ref() {
                Some(start) => Some(super::warm::run_config(run, start, &self.parts).map_err(
                    |error| {
                        SendOutcome::Error(format!("the session's latest run start: {error:?}"))
                    },
                )?),
                None => None,
            },
            None => None,
        };
        let mut effective = request.clone();
        if let Some(config) = &latest {
            crate::actor::inherit_frozen_parameters(&mut effective, config);
        }
        let fixed = latest
            .as_ref()
            .filter(|_| self.running().is_some())
            .and_then(|config| {
                ServiceTier::parse(broca_protocol::frozen_service_tier(
                    &config.frozen_render_config,
                ))
            });
        let model = head_selection::PlanModel::new(
            &effective.model.provider_module_id,
            &effective.model.model_id,
        );
        let wire = self
            .executor
            .head_selection_wire(&model)
            .unwrap_or_default();
        let applied = head_selection::check_send_tier(
            request
                .service_tier_level
                .as_deref()
                .and_then(ServiceTier::parse),
            fixed,
            selection.as_ref(),
            &wire,
            &broca_wire::broca_role_describe(env!("CARGO_PKG_VERSION")).role,
        )
        .map_err(|problem| SendOutcome::ServiceTierInvalid {
            reason: problem.reason(),
            message: format!(
                "service_tier {:?} cannot be applied to {}/{}: {}",
                request.service_tier_level,
                effective.model.provider_module_id,
                effective.model.model_id,
                problem.reason()
            ),
        })?;
        request.service_tier_level = (!applied.is_standard()
            || request.service_tier_level.is_some())
        .then(|| applied.as_str().to_owned());
        effective
            .service_tier_level
            .clone_from(&request.service_tier_level);
        self.executor
            .check_service_tier(&effective)
            .map_err(|message| SendOutcome::ServiceTierInvalid {
                reason: "unsupported_tier",
                message,
            })
    }
}
