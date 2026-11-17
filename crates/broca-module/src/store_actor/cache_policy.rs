//! The cache policy and warming a run starts with (llm-runner `cache_policy`).
//!
//! A planned session's policy is its plan's `cache`, read from the plan each fresh run starts
//! under, so every run follows it whoever sent the run's message: an app's message and a
//! module's notice keep the same cache lifetime as the owner's own sends. A planless session
//! takes its policy from its creating send and keeps it for life. A send carrying `cache` to a
//! session that already ran is refused before anything is written.

use broca_protocol::request::KeepWarm;
use broca_session_loop::writer::Writer as _;
use cortexkit_role_llm_runner::cache::Cache;

/// What a fresh run's admission freezes for its prompt cache.
#[derive(Debug, Default, PartialEq)]
pub(super) struct RunCache {
    /// The policy the executor resolves into the run's render config. `None` leaves the
    /// lifetime to broca's per-route default.
    pub cache: Option<Cache>,
    /// The run's warming; `None` warms nothing.
    pub keep_warm: Option<KeepWarm>,
    /// Take the previous run's frozen policy and key instead of resolving `cache`: a planless
    /// session's runs after its first.
    pub inherit: bool,
}

/// The refusal of a send that carries `cache` to a session that already ran.
pub(super) const CACHE_ON_EXISTING_SESSION: &str =
    "cache is set only by a planless session's creating send; a planned session takes it from \
     plan.cache, and this session already ran";

impl<B: broca_session_store::backend::SessionBackend + Send + Sync + 'static> super::StoreActor<B> {
    /// Whether a send may carry `cache`: only the creating send of a session that never ran.
    pub(super) fn admits_send_cache(&self, cache: Option<&Cache>) -> bool {
        cache.is_none() || self.session.operations().owner.first_start.is_none()
    }

    /// The cache policy a fresh run starts with. A planned session's comes from the plan the
    /// run starts under: the latest accepted refresh, else the session's first plan, else, on
    /// the session's first run, `manifest`, the plan its creating send carried. A planless
    /// session's first run takes `sent`, its creating send's `cache`; its later runs inherit
    /// the policy, key and warming the previous run froze.
    pub(super) fn run_cache(
        &self,
        sent: Option<&Cache>,
        manifest: Option<&broca_wal::PlanManifest>,
    ) -> Result<RunCache, String> {
        let operations = self.session.operations();
        let session_plan = broca_session_loop::plan::sender_manifest(operations, &self.parts)?;
        if let Some(plan) = session_plan.as_deref().or(manifest) {
            let cache = plan_cache(plan)?;
            return Ok(RunCache {
                keep_warm: cache.as_ref().and_then(KeepWarm::of),
                cache,
                inherit: false,
            });
        }
        if operations.owner.first_start.is_none() {
            return Ok(RunCache {
                keep_warm: sent.and_then(KeepWarm::of),
                cache: sent.cloned(),
                inherit: false,
            });
        }
        let keep_warm = operations
            .run
            .as_ref()
            .and_then(|run| run.start.as_ref())
            .and_then(|start| start.keep_warm.clone())
            .map(serde_json::from_value)
            .transpose()
            .map_err(|error| format!("the previous run's recorded keep_warm: {error}"))?;
        Ok(RunCache {
            cache: None,
            keep_warm,
            inherit: true,
        })
    }
}

/// The `cache` member of a stored plan. Admission checked it when the plan arrived, so a plan
/// whose `cache` no longer reads is a damaged record, refused rather than read as no policy.
fn plan_cache(plan: &broca_wal::PlanManifest) -> Result<Option<Cache>, String> {
    let value: serde_json::Value = serde_json::from_str(plan.plan.as_json_text())
        .map_err(|error| format!("the run's plan: {error}"))?;
    let object = value.as_object().ok_or("the run's plan is not an object")?;
    broca_fetch_plan::plan_cache(object)
        .map_err(|refusal| format!("the run's plan.cache: {refusal:?}"))
}
