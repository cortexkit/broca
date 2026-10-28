//! Operator-only visibility and revocation of private-content grants. These operations
//! share the consent store, but never settle cards or release calls waiting on them.

use std::sync::Arc;

use broca_protocol::{BindPrincipal, SessionScope};
use broca_store::grants::{Grant, GrantKind, GrantsStore};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use subc_protocol::tool_call::ToolCallRequest;

use super::consent::{ConsentIo, RunLiveness};

type Answer = Result<Box<RawValue>, Vec<u8>>;

pub struct Permissions {
    store: Arc<GrantsStore>,
    io: Option<Arc<dyn ConsentIo>>,
}

impl Permissions {
    pub fn new(store: Arc<GrantsStore>, io: Option<Arc<dyn ConsentIo>>) -> Self {
        Self { store, io }
    }

    pub async fn answer(&self, body: &[u8], principal: Option<&BindPrincipal>) -> Answer {
        // Admission precedes even reading grants: the inventory is operator authority too.
        if !operator_admitted(principal) {
            return Err(error(
                "permissions_operator_required",
                "permissions operations require the direct operator or reserved:cingulate",
            ));
        }
        let request: ToolCallRequest = serde_json::from_slice(body).map_err(|e| {
            error(
                "invalid_params",
                format!("invalid permissions request: {e}"),
            )
        })?;
        let now = super::held::epoch_ms();
        match request.name.as_str() {
            "permissions.list" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Params {
                    agent_id: Option<String>,
                }
                let params: Params = serde_json::from_value(request.arguments).map_err(|e| {
                    error(
                        "invalid_params",
                        format!("invalid permissions.list arguments: {e}"),
                    )
                })?;
                let mut rows = Vec::new();
                for grant in self.store.grants().map_err(store_error)? {
                    if params
                        .agent_id
                        .as_ref()
                        .is_some_and(|id| id != &grant.holder)
                        || !self.visible(&grant, now).await
                    {
                        continue;
                    }
                    rows.push(row(&grant));
                }
                reply(json!({"grants":rows}))
            }
            "permissions.revoke" => {
                #[derive(Deserialize)]
                #[serde(deny_unknown_fields)]
                struct Params {
                    id: String,
                }
                let params: Params = serde_json::from_value(request.arguments).map_err(|e| {
                    error(
                        "invalid_params",
                        format!("invalid permissions.revoke arguments: {e}"),
                    )
                })?;
                let grant = self
                    .store
                    .grants()
                    .map_err(store_error)?
                    .into_iter()
                    .find(|g| g.id == params.id);
                let live = match grant {
                    Some(grant) => grant.kind == GrantKind::Run || self.visible(&grant, now).await,
                    None => false,
                };
                let revoked = live && self.store.revoke(&params.id).map_err(store_error)?;
                reply(json!({"outcome":if revoked { "revoked" } else { "not_found" }}))
            }
            _ => Err(error("unknown_method", "unknown permissions operation")),
        }
    }

    async fn visible(&self, grant: &Grant, now: i64) -> bool {
        match grant.kind {
            GrantKind::Always => grant.project.is_some(),
            GrantKind::Hours24 => grant.expires_at_ms.is_some_and(|expiry| now < expiry),
            GrantKind::Run => {
                // An unavailable liveness check must not hide authority the operator can revoke.
                // Authorization still requires a positive live answer in the consent coordinator.
                let (Some(scope), Some(io)) = (&grant.scope, &self.io) else {
                    return true;
                };
                let owner = match scope.owner.as_str() {
                    "direct" => BindPrincipal::Direct,
                    owner if owner.starts_with("reserved:") => BindPrincipal::Reserved {
                        module_id: owner["reserved:".len()..].into(),
                    },
                    _ => return true,
                };
                io.run_live(&SessionScope {
                    owner,
                    scope_ref: scope.scope_ref.clone(),
                    scope_epoch: scope.scope_epoch,
                    agent_id: Some(grant.holder.clone()),
                    run_id: None,
                    flow_id: None,
                    sender: None,
                })
                .await
                    != RunLiveness::Ended
            }
        }
    }
}

pub(crate) fn operator_admitted(principal: Option<&BindPrincipal>) -> bool {
    matches!(principal, Some(BindPrincipal::Direct))
        || matches!(principal, Some(BindPrincipal::Reserved { module_id }) if module_id == "cingulate")
}

fn row(grant: &Grant) -> Value {
    let duration = match grant.kind {
        GrantKind::Run => "for this program run".to_owned(),
        GrantKind::Hours24 => "for 24 hours".to_owned(),
        GrantKind::Always => format!("always in {}", grant.project.as_deref().unwrap_or("")),
    };
    let mut row = json!({
        "id":grant.id, "agent_id":grant.holder,
        "holder":{"kind":"agent","agent_id":grant.holder},
        "module":"broca", "subject_label":"Private content",
        "scope":"provider",
        "scope_label":format!("Send private content to {} {duration}", grant.provider),
        "granted_at_ms":grant.created_at_ms, "card_ref":grant.source_card_id,
    });
    if let Some(expiry) = grant.expires_at_ms {
        row["expires_at_ms"] = json!(expiry);
    }
    row
}

fn reply(value: Value) -> Answer {
    Ok(RawValue::from_string(value.to_string()).expect("permissions reply encodes"))
}

fn error(code: &str, message: impl Into<String>) -> Vec<u8> {
    crate::system_one::refusal_body(&super::admission::refusal(code, message))
}

fn store_error(error: broca_store::grants::GrantsError) -> Vec<u8> {
    crate::system_one::refusal_body(&crate::system_one::internal_refusal(&format!(
        "permissions store failed: {error}"
    )))
}
