//! The other id a route open is retried under when a renamed module's old id is refused.

use super::RouteTarget;

/// The other id a refused route open is tried under once (see
/// `SubcConnection::route_open_wire_target`).
#[derive(Debug, PartialEq)]
pub(super) enum Renamed {
    /// The module is unknown under the id the open named; open the target under the module's
    /// other id.
    Module(RouteTarget),
    /// The scope is not live under the owner the open named; open under the same `ref` and
    /// `scope_epoch` with the owner module's other id.
    ScopeOwner(subc_protocol::scope::ScopeSelector),
}

/// Whether an open of `target` under `scope`, refused with `code`, is worth one more try under
/// a renamed module's other id, and which.
pub(super) fn renamed_target(
    code: &str,
    target: &RouteTarget,
    scope: Option<&subc_protocol::scope::ScopeSelector>,
) -> Option<Renamed> {
    match code {
        "unknown_module" | "module_removed" => {
            let mut renamed = target.clone();
            let module_id = match &mut renamed {
                RouteTarget::ToolProvider { module_id }
                | RouteTarget::ManagementSurface { module_id }
                | RouteTarget::InternalService { module_id, .. } => module_id,
            };
            *module_id = broca_protocol::other_module_name(module_id)?.to_owned();
            Some(Renamed::Module(renamed))
        }
        "scope_not_live" | "scope_ended" => {
            let scope = scope?;
            let subc_protocol::Principal::Reserved { module_id: owner } = &scope.owner else {
                return None;
            };
            let other = broca_protocol::other_module_name(owner)?;
            let mut renamed = scope.clone();
            renamed.owner = subc_protocol::Principal::Reserved {
                module_id: other.to_owned(),
            };
            Some(Renamed::ScopeOwner(renamed))
        }
        _ => None,
    }
}
