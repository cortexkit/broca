//! Content-free facts from a run's own frozen admission on either storage.

use crate::serve::ServeError;
use broca_wire::{RunAdmissionFetch, RunAdmissionResponse, RunAdmissionStorage};

pub(crate) fn response(
    run_id: &str,
    session_class: Option<String>,
    started_at_ms: u64,
    storage: RunAdmissionStorage,
    manifest: Option<&broca_records::record::PlanManifest>,
    principal: Option<&broca_protocol::BindPrincipal>,
    scope: Option<&broca_protocol::SessionScope>,
) -> Result<RunAdmissionResponse, ServeError> {
    let fetches = manifest
        .into_iter()
        .flat_map(|manifest| &manifest.fetches)
        .map(|fetch| {
            let declarations = match &fetch.outcome {
                broca_records::record::FetchOutcome::Answered {
                    declarations_frozen,
                    ..
                } => declarations_frozen
                    .as_deref()
                    .map(serde_json::to_value)
                    .transpose(),
                _ => Ok(None),
            };
            Ok(RunAdmissionFetch {
                provider: fetch.provider.clone(),
                declarations_frozen: declarations
                    .map_err(|error| ServeError::Open(error.to_string(), None))?,
            })
        })
        .collect::<Result<_, ServeError>>()?;
    Ok(RunAdmissionResponse {
        run_id: run_id.to_owned(),
        session_class,
        started_at_ms,
        storage,
        fetches,
        scope: scope.map(|scope| broca_wire::RunAdmissionScope {
            owner: scope.owner.clone(),
            scope_ref: scope.scope_ref.clone(),
            epoch: scope.scope_epoch,
            principal: principal.cloned(),
        }),
    })
}
