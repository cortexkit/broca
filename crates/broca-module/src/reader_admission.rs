//! Read admission for plans in the session store, shared by live actors and stored views.

use std::sync::Arc;

use broca_protocol::{BindPrincipal, RunId};
use broca_records::record::{OpaqueJson, PlanManifest};
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

// Keep this list in sync with what `app_readers::admit` and `decide` read. A kept head must
// not hold a whole plan just to decide read admission.
const READ_PLAN_MEMBERS: [&str; 3] = [app_readers::PLAN_READERS, "senders", "compaction_item"];

#[derive(Clone)]
enum ReadPlan {
    Missing,
    Json(Box<str>),
    // Preserve the decode error for admission without keeping the invalid plan or failing head
    // construction.
    Invalid(Box<str>),
}

impl ReadPlan {
    fn project(plan: Option<&OpaqueJson>) -> Self {
        let Some(plan) = plan else {
            return Self::Missing;
        };
        let mut full: Map<String, Value> = match serde_json::from_str(plan.as_json_text()) {
            Ok(full) => full,
            Err(error) => return Self::Invalid(error.to_string().into_boxed_str()),
        };
        // Read admission reads only each sender's `module`. A sender's notice templates can run
        // to tens of kilobytes, and a closed session's head kept for `session.head` (see
        // `store_head::KeptHeads`) holds this policy with two plans, so keep the modules alone
        // when the stored list parses. A list that doesn't parse stays as stored, so admission
        // refuses it the same way the full plan does.
        let sender_modules = cortexkit_role_llm_runner::delivery_order::plan_senders(&full)
            .ok()
            .map(|senders| {
                senders
                    .into_iter()
                    .map(|sender| serde_json::json!({ "module": sender.module }))
                    .collect::<Vec<_>>()
            });
        let mut projection = Map::new();
        for member in READ_PLAN_MEMBERS {
            let Some(mut value) = full.remove(member) else {
                continue;
            };
            if member == "senders" {
                if let Some(modules) = &sender_modules {
                    value = Value::Array(modules.clone());
                }
            }
            if member == "compaction_item" {
                let mut item = Map::new();
                if let Some(provider) = value
                    .as_object_mut()
                    .and_then(|item| item.remove("provider"))
                {
                    item.insert("provider".into(), provider);
                }
                value = Value::Object(item);
            }
            projection.insert(member.into(), value);
        }
        Self::Json(Value::Object(projection).to_string().into_boxed_str())
    }

    fn decode(&self) -> Result<Map<String, Value>, ServeError> {
        match self {
            Self::Missing => Ok(Map::new()),
            Self::Json(json) => serde_json::from_str(json).map_err(|error| error.to_string()),
            Self::Invalid(error) => Err(error.to_string()),
        }
        .map_err(|error| {
            ServeError::Transient(format!("the stored read policy is invalid: {error}"))
        })
    }

    fn heap_bytes(&self) -> usize {
        match self {
            Self::Missing => 0,
            Self::Json(text) | Self::Invalid(text) => text.len(),
        }
    }
}

/// What a read's admission reads from a session: its ownership, the plan in force for reads
/// (the latest run's, else the first run's) and the latest accepted plan, whose reader list
/// governs. The closed-session authorization cache can admit a read without loading the session.
/// Only the plan members used for admission are held;
/// tool composition, system text and other plan data aren't needed by a kept head.
#[derive(Clone)]
pub struct ReadPolicy {
    ownership: broca_core::SessionOwnershipV1,
    effective: ReadPlan,
    accepted: ReadPlan,
}

impl ReadPolicy {
    /// The policy from facts the caller already read: the session's `ownership`, the latest
    /// run's frozen plan (`latest`, else the first run's, read only then) and the latest
    /// accepted plan.
    pub fn new<B: SessionBackend>(
        session: &Session<B>,
        parts: &dyn Parts,
        ownership: broca_core::SessionOwnershipV1,
        latest: Option<Arc<PlanManifest>>,
        accepted: Option<Arc<PlanManifest>>,
    ) -> Result<Self, ServeError> {
        let effective = match latest {
            Some(manifest) => Some(manifest),
            None => session
                .operations
                .owner
                .first_start
                .as_ref()
                .map(|start| broca_session_loop::run::frozen_manifest(start, parts))
                .transpose()
                .map_err(|error| {
                    ServeError::Transient(format!("the first read policy: {error:?}"))
                })?
                .flatten(),
        };
        Ok(Self::from_plans(
            ownership,
            effective.as_ref().map(|manifest| &manifest.plan),
            accepted.as_ref().map(|manifest| &manifest.plan),
        ))
    }

    fn from_plans(
        ownership: broca_core::SessionOwnershipV1,
        effective: Option<&OpaqueJson>,
        accepted: Option<&OpaqueJson>,
    ) -> Self {
        Self {
            ownership,
            effective: ReadPlan::project(effective),
            accepted: ReadPlan::project(accepted),
        }
    }

    /// A policy whose plan in force and accepted plan are both `manifest`'s.
    #[cfg(test)]
    pub(crate) fn with_manifest(manifest: &PlanManifest) -> Self {
        Self::from_plans(
            Default::default(),
            Some(&manifest.plan),
            Some(&manifest.plan),
        )
    }

    /// What the policy holds, walked as `broca_session_store::held` walks values.
    pub(crate) fn heap_bytes(&self) -> usize {
        broca_session_store::held::serde_heap_bytes(&self.ownership)
            + self.effective.heap_bytes()
            + self.accepted.heap_bytes()
    }
}

/// The read policy of `session`, as [`admit`] reads it.
pub(crate) fn read_policy<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
) -> Result<ReadPolicy, ServeError> {
    let ownership = broca_session_loop::owner::ownership(&session.operations.owner)
        .map_err(ServeError::Transient)?;
    let latest = session
        .operations
        .run
        .as_ref()
        .and_then(|run| run.start.as_ref())
        .map(|start| broca_session_loop::run::frozen_manifest(start, parts))
        .transpose()
        .map_err(|error| ServeError::Transient(format!("the effective read policy: {error:?}")))?
        .flatten();
    let accepted = broca_session_loop::plan::sender_manifest(&session.operations, parts)
        .map_err(ServeError::Transient)?;
    ReadPolicy::new(session, parts, ownership, latest, accepted)
}

/// Runs before page, status or stream work. A stored caller supplies the same cached view its
/// operation will use; a live actor supplies its own session, without flushing for authorization.
pub async fn admit<B: SessionBackend>(
    session: &Session<B>,
    parts: &dyn Parts,
    access: &ReadAccess,
) -> Result<(), ServeError> {
    let policy = read_policy(session, parts)?;
    if let Some(decided) = decide(&policy, access, None) {
        return decided;
    }
    let own_send = match &access.send {
        Some((run, send_id)) => {
            crate::store_status::authorization_own_send(
                session,
                parts,
                run,
                send_id,
                access.principal.as_ref(),
            )
            .await?
        }
        None => false,
    };
    decide(&policy, access, Some(own_send)).unwrap_or(Err(ServeError::ReadAccessRefused))
}

/// Admits `access` under `policy`. `own_send` says whether the send `access` names is the
/// caller's own; `None` when it isn't known, and then the decision is `None` when it depends on
/// it, which only the session's records tell.
pub fn decide(
    policy: &ReadPolicy,
    access: &ReadAccess,
    own_send: Option<bool>,
) -> Option<Result<(), ServeError>> {
    let ownership = &policy.ownership;
    let current = broca_core::scope_admission::decide_sender_access(
        access.principal.as_ref(),
        (ownership.episodes > 0).then_some(ownership.first_principal.as_ref()),
    );
    let effective = match policy.effective.decode() {
        Ok(effective) => effective,
        Err(error) => return Some(Err(error)),
    };
    let accepted = match policy.accepted.decode() {
        Ok(accepted) => accepted,
        Err(error) => return Some(Err(error)),
    };
    let restricted = accepted.contains_key(app_readers::PLAN_READERS);
    if !restricted {
        return Some(current.map_err(ServeError::ScopeRefused));
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
        _ => return Some(Err(refusal())),
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
    let own_send = if !broca_protocol::same_module(caller, owner) && access.send.is_some() {
        own_send?
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
    });
    Some(match allowed {
        Ok(true) => Ok(()),
        Ok(false) => Err(refusal()),
        Err(error) => Err(error),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn opaque(plan: Value) -> OpaqueJson {
        OpaqueJson::from_json_text(plan.to_string()).unwrap()
    }

    #[test]
    fn projected_plans_make_the_same_read_decisions_as_full_plans() {
        let effective = opaque(json!({
            "readers": ["old-reader"],
            "senders": [{
                "module": "old-sender", "idle_start": true,
                "templates": [{"id": "wake", "text": "A long notice template."}]
            }],
            "compaction_item": {"provider": "active-compactor", "preset": "worker"},
            "composition": {"providers": []}
        }));
        let pending = opaque(json!({
            "readers": ["new-reader"], "senders": [{"module": "new-sender"}],
            "compaction_item": {"provider": "pending-compactor", "params": {"limit": 100}},
            "tool_items": [{"provider": "aft", "params": {}}]
        }));
        let unrestricted = opaque(json!({"system_text_items": []}));
        let module = |id: &str| BindPrincipal::Reserved {
            module_id: id.into(),
        };
        let ownership = broca_core::SessionOwnershipV1 {
            episodes: 1,
            first_principal: Some(module("owner")),
            ..Default::default()
        };
        let read = "session.head";
        let send = "send.status";
        let alias = "session.send_status";
        let cases = [
            (module("owner"), read, None, Some(true)),
            (module("new-reader"), read, None, Some(true)),
            (module("unlisted"), read, None, Some(false)),
            (module("old-reader"), read, None, Some(false)),
            (module("active-compactor"), read, None, Some(true)),
            (module("pending-compactor"), read, None, Some(false)),
            (BindPrincipal::Direct, read, None, Some(true)),
            (module("owner"), send, None, Some(true)),
            (module("new-sender"), send, Some(true), Some(true)),
            (module("new-sender"), alias, Some(false), Some(false)),
            (module("new-sender"), send, None, None),
            (module("old-sender"), send, Some(true), Some(false)),
            (module("new-reader"), send, Some(true), Some(false)),
            (BindPrincipal::Direct, alias, Some(true), Some(false)),
        ];
        let full_json = |plan: &OpaqueJson| ReadPlan::Json(plan.as_json_text().into());
        for accepted in [Some(&pending), Some(&unrestricted), None] {
            let full = ReadPolicy {
                ownership: ownership.clone(),
                effective: full_json(&effective),
                accepted: accepted.map_or(ReadPlan::Missing, full_json),
            };
            let projected = ReadPolicy::from_plans(ownership.clone(), Some(&effective), accepted);
            for (principal, op, own_send, restricted) in &cases {
                let access = ReadAccess {
                    principal: Some(principal.clone()),
                    op: (*op).into(),
                    send: (*op != read).then(|| (RunId("r1".into()), "s1".into())),
                };
                let outcome = |policy| match decide(policy, &access, *own_send) {
                    Some(Ok(())) => Some(true),
                    Some(Err(ServeError::ReadAccessRefused)) => Some(false),
                    None => None,
                    decision => panic!("unexpected decision for {access:?}: {decision:?}"),
                };
                let expected = if accepted == Some(&pending) {
                    *restricted
                } else {
                    Some(true)
                };
                let baseline = outcome(&full);
                assert_eq!(baseline, expected, "full plans, {access:?}, {own_send:?}");
                assert_eq!(
                    outcome(&projected),
                    baseline,
                    "projection, {access:?}, {own_send:?}"
                );
            }
            let expected_effective = json!({
                "readers": ["old-reader"], "senders": [{"module": "old-sender"}],
                "compaction_item": {"provider": "active-compactor"}
            });
            let expected_accepted = if accepted == Some(&pending) {
                json!({
                    "readers": ["new-reader"], "senders": [{"module": "new-sender"}],
                    "compaction_item": {"provider": "pending-compactor"}
                })
            } else {
                json!({})
            };
            assert_eq!(
                Value::Object(projected.effective.decode().unwrap()),
                expected_effective
            );
            assert_eq!(
                Value::Object(projected.accepted.decode().unwrap()),
                expected_accepted
            );
        }
    }

    #[test]
    fn projection_preserves_malformed_stored_plan_refusals() {
        let access = ReadAccess {
            principal: Some(BindPrincipal::Direct),
            op: "session.head".into(),
            send: None,
        };
        let malformed = opaque(json!({"readers": null}));
        let policy = ReadPolicy::from_plans(Default::default(), None, Some(&malformed));
        assert!(matches!(
            decide(&policy, &access, None),
            Some(Err(ServeError::InvalidParams { field, .. })) if field == "plan.readers"
        ));
        // A sender list that doesn't parse (here a module named twice) keeps its stored form,
        // so a send-status read is refused as with the full plan, not admitted by a projection.
        let duplicate = opaque(json!({
            "readers": ["reader"], "senders": [{"module": "twice"}, {"module": "twice"}]
        }));
        let status = ReadAccess {
            principal: Some(BindPrincipal::Reserved {
                module_id: "twice".into(),
            }),
            op: "send.status".into(),
            send: Some((RunId("r1".into()), "s1".into())),
        };
        let full = ReadPolicy {
            ownership: Default::default(),
            effective: ReadPlan::Json(duplicate.as_json_text().into()),
            accepted: ReadPlan::Json(duplicate.as_json_text().into()),
        };
        let projected =
            ReadPolicy::from_plans(Default::default(), Some(&duplicate), Some(&duplicate));
        assert_eq!(
            format!("{:?}", decide(&projected, &status, Some(true))),
            format!("{:?}", decide(&full, &status, Some(true)))
        );
        assert!(matches!(
            decide(&projected, &status, Some(true)),
            Some(Err(ServeError::InvalidParams { .. }))
        ));
        let non_object = opaque(json!(123));
        let policy = ReadPolicy::from_plans(Default::default(), Some(&non_object), None);
        let expected_error =
            serde_json::from_str::<Map<String, Value>>(non_object.as_json_text()).unwrap_err();
        assert!(matches!(
            decide(&policy, &access, None),
            Some(Err(ServeError::Transient(message)))
                if message == format!("the stored read policy is invalid: {expected_error}")
        ));
    }
}
