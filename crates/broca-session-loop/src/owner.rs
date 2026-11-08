//! The session's owner facts in the form today's admission rules read them.
//!
//! The store's operational fold keeps the owner summary its run starts recorded
//! ([`Owner`]); `broca_core::scope_admission` decides who may send to, cancel or retract a
//! session from a `SessionOwnershipV1`. This builds that summary from the fold, so a session in
//! the new storage is judged by the same rules as one in today's.

use broca_core::admission::SessionOwnershipV1;
use broca_session_store::fold::operations::Owner;

/// The owner summary `owner` holds, as `SessionOwnershipV1::for_owner_access` gives it: who
/// drove the runs and the scope, without the first run's tools or plan. Its
/// `recorded_principals` holds at most the first two distinct principals, which the only rule
/// that reads them (scope adoption, which looks for the first principal other than the scope's
/// owner) answers the same from as from every one.
pub fn ownership(owner: &Owner) -> Result<SessionOwnershipV1, String> {
    let principal = |value: &serde_json::Value| {
        serde_json::from_value(value.clone()).map_err(|error| format!("a principal: {error}"))
    };
    Ok(SessionOwnershipV1 {
        episodes: owner.episodes,
        scope: owner
            .scope
            .as_ref()
            .map(|scope| serde_json::from_value(scope.clone()))
            .transpose()
            .map_err(|error| format!("a scope: {error}"))?,
        first_principal: owner.first_principal.as_ref().map(principal).transpose()?,
        any_unrecorded_principal: owner.any_unrecorded_principal,
        recorded_principals: owner
            .principals
            .iter()
            .map(principal)
            .collect::<Result<_, _>>()?,
        ..SessionOwnershipV1::default()
    }
    .for_owner_access())
}

/// [`ownership`] plus the first run's tools and fetch-plan manifest (`first_episode`), read
/// back from the setup parts the first run's start names, as `SessionOwnershipV1` carries them
/// for `route.bind` and the plan checks. `latest_manifest` stays absent: before a refresh every
/// run freezes the first run's manifest, and after one [`ownership_with_plan`] carries the
/// refreshed senders.
pub fn ownership_with_first_episode(
    owner: &Owner,
    parts: &dyn crate::parts::Parts,
) -> Result<SessionOwnershipV1, String> {
    let mut summary = ownership(owner)?;
    if let Some(first) = &owner.first_start {
        let admission = crate::run::admission_of_start(first, parts)
            .map_err(|error| format!("the first run's start: {error:?}"))?;
        summary.first_episode = Some(broca_core::admission::FirstEpisodeV1 {
            run_id: broca_protocol::RunId(admission.run_id.clone()),
            tool_names: admission
                .tools
                .iter()
                .map(|tool| tool.name.clone())
                .collect(),
            manifest: admission.manifest,
        });
    }
    Ok(summary)
}

/// [`ownership_with_first_episode`] plus the sender policy the session's latest accepted refresh
/// set (`sender_admission`), as today's summary carries it from the refresh's acceptance: the
/// senders every sender check reads ([`crate::plan::senders`]).
pub fn ownership_with_plan(
    operations: &broca_session_store::fold::operations::Operations,
    parts: &dyn crate::parts::Parts,
) -> Result<SessionOwnershipV1, String> {
    let mut summary = ownership_with_first_episode(&operations.owner, parts)?;
    if crate::plan::refreshed(operations) {
        summary.session_change_recorded = true;
        summary.sender_admission = Some(broca_core::admission::SenderAdmissionV1 {
            version: 1,
            accepted_senders: Some(crate::plan::senders(operations, parts)?),
        });
    }
    Ok(summary)
}

#[cfg(test)]
mod tests;
