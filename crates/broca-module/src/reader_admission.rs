//! Read admission for plans in the session store, shared by live actors and stored views.

use broca_protocol::{BindPrincipal, RunId};
use broca_session_loop::parts::Parts;
use broca_session_store::{backend::SessionBackend, session::Session};
use cortexkit_role_llm_runner::app_readers::{self, ReadAdmission};
use serde_json::{Map, Value};

use crate::serve::ServeError;

/// Facts from the route, never from caller-supplied identity fields.
#[derive(Debug, Clone, Default)]
pub struct ReadAccess {
    pub principal: Option<BindPrincipal>,
    pub op: String,
    pub send: Option<(RunId, String)>,
}

/// The operations governed by the plan's reader list. Both send-status methods use the same
/// owner-or-named-sender rule, limited to the sender's own sends and its named predecessor's.
pub fn governs(op: &str) -> bool {
    matches!(
        op,
        "session.head"
            | "session.read"
            | "session.plan_view"
            | "session.history"
            | "run.status"
            | "role.describe"
            | "session.subscribe"
            | "session.send_status"
            | "send.status"
    )
}

/// Validate before fetching or writing. `supported` is false for a session the session store
/// can't hold, which takes no reader policy.
pub fn check_plan(plan: &Map<String, Value>, supported: bool) -> Result<(), ServeError> {
    let declared = if supported {
        &[cortexkit_role_llm_runner::capabilities::APP_READERS][..]
    } else {
        &[]
    };
    app_readers::check_plan(plan, declared).map_err(|field| ServeError::InvalidParams {
        field: field.into(),
        message: if supported {
            "plan.readers must be an array of distinct principal strings".into()
        } else {
            "plan.readers is served only for a session the session store holds".into()
        },
    })
}

fn plan(
    manifest: Option<&broca_records::record::PlanManifest>,
) -> Result<Map<String, Value>, ServeError> {
    manifest.map_or_else(
        || Ok(Map::new()),
        |manifest| {
            serde_json::from_str(manifest.plan.as_json_text()).map_err(|error| {
                ServeError::Transient(format!("the stored read policy is invalid: {error}"))
            })
        },
    )
}

/// Runs before page, status or stream work. A stored caller supplies the same cached view its
/// operation will use; a live actor supplies its own session, without flushing for authorization.
pub async fn admit<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    access: &ReadAccess,
) -> Result<(), ServeError> {
    let ownership = broca_session_loop::owner::ownership(&session.operations.owner)
        .map_err(ServeError::Transient)?;
    let current = broca_core::scope_admission::decide_sender_access(
        access.principal.as_ref(),
        (ownership.episodes > 0).then_some(ownership.first_principal.as_ref()),
    );
    let latest = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref());
    let effective = latest
        .map(|start| broca_session_loop::run::frozen_manifest(start, parts))
        .transpose()
        .map_err(|error| ServeError::Transient(format!("the effective read policy: {error:?}")))?
        .flatten();
    let effective = match effective {
        Some(manifest) => Some(manifest),
        None => session
            .operations
            .owner
            .first_start
            .as_ref()
            .map(|start| broca_session_loop::run::frozen_manifest(start, parts))
            .transpose()
            .map_err(|error| ServeError::Transient(format!("the first read policy: {error:?}")))?
            .flatten(),
    };
    let effective = plan(effective.as_deref())?;
    let accepted = broca_session_loop::plan::sender_manifest(&session.operations, parts)
        .map_err(ServeError::Transient)?;
    let accepted = plan(accepted.as_deref())?;
    let restricted = accepted.contains_key(app_readers::PLAN_READERS);
    if !restricted {
        return current.map_err(ServeError::ScopeRefused);
    }
    let refusal = || ServeError::ReadAccessRefused;
    let owner_principal = ownership
        .scope
        .as_ref()
        .map(|scope| &scope.owner)
        .or(ownership.first_principal.as_ref());
    let owner_module = match owner_principal {
        Some(BindPrincipal::Reserved { module_id }) => Some(module_id.as_str()),
        _ => None,
    };
    let direct_caller = owner_module
        .map(|owner| format!("{owner}\0"))
        .unwrap_or_default();
    let caller = match access.principal.as_ref() {
        Some(BindPrincipal::Reserved { module_id }) => module_id.as_str(),
        Some(BindPrincipal::Direct) => &direct_caller,
        _ => return Err(refusal()),
    };
    // Only a recorded Direct owner matches a Direct caller; an empty module id cannot confer
    // owner authority on a local operator for send status.
    let direct = matches!(access.principal, Some(BindPrincipal::Direct));
    let unmatched_owner = format!("{caller}\0");
    let owner = match owner_principal {
        Some(BindPrincipal::Reserved { module_id }) => module_id.as_str(),
        Some(BindPrincipal::Direct) if direct => caller,
        _ => &unmatched_owner,
    };
    let own_send = if !broca_protocol::same_module(caller, owner) {
        if let Some((run, send_id)) = &access.send {
            crate::store_status::authorization_own_send(
                session,
                parts,
                run,
                send_id,
                access.principal.as_ref(),
            )
            .await?
        } else {
            false
        }
    } else {
        false
    };
    let facts = ReadAdmission::new(current.is_ok())
        .with_local_operator(direct)
        .with_own_send(own_send);
    // The contract compares ids exactly; a renamed module's two ids compare as one.
    let allowed = app_readers::admit(
        &effective,
        Some(&accepted),
        broca_protocol::canonical_module(caller),
        broca_protocol::canonical_module(owner),
        &access.op,
        facts,
    )
    .map_err(|field| ServeError::InvalidParams {
        field: field.into(),
        message: "the stored plan's readers are malformed".into(),
    })?;
    if allowed {
        Ok(())
    } else {
        Err(refusal())
    }
}
