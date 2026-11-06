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

#[cfg(test)]
mod tests;
