//! Management scope compatibility, separate from existing role and resource authorization.
use broca_protocol::{BindPrincipal, BindScope, ScopeKind, SendScope, SessionScope};
use subc_protocol::ErrorBody;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Continue through the existing role, session, owner and operation-specific checks.
    Legacy,
    Diagnostics,
    /// Not a request/response management operation. Existing namespace/stream dispatch decides it.
    Undeclared,
}

#[derive(Debug)]
pub enum Refusal {
    Session(broca_core::ScopeRefusal),
    Classifier(cortexkit_role_classifier::errors::Refusal),
    Operator,
    Diagnostics(ErrorBody),
}

/// Compose the namespace's real scope/caller gate without granting resource access. Stateful
/// sender/owner, role and parameter checks remain in their original serving positions.
/// Discovery reuses these helpers rather than copying their predicates or error conditions.
pub fn admit(
    method: &str,
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
) -> Result<Admission, Refusal> {
    if method == crate::diagnostics::OP {
        crate::diagnostics::admit(principal, scope).map_err(Refusal::Diagnostics)?;
        return Ok(Admission::Diagnostics);
    }
    if method == crate::manifest::OP_SEND {
        broca_core::scope_admission::require_flow_scope(principal, scope)
            .map_err(Refusal::Session)?;
    } else if method == crate::system_one::OP_CLASSIFY_RUN {
        crate::system_one::caller_scope(principal, scope).map_err(Refusal::Classifier)?;
    } else if method.starts_with("permissions.")
        && !crate::llm_tools::permissions::operator_admitted(principal)
    {
        return Err(Refusal::Operator);
    }
    Ok(
        if crate::manifest::registered_management_operation(method) {
            Admission::Legacy
        } else {
            Admission::Undeclared
        },
    )
}

/// Discovery probes the real namespace gates. It neither duplicates an operation allowlist
/// nor claims that a valid scope bypasses resource or parameter checks.
pub(crate) fn flow_operations() -> Vec<String> {
    let principal = BindPrincipal::Reserved {
        module_id: "cingulate".into(),
    };
    let scope = SendScope::Stamped(BindScope {
        scope: SessionScope {
            owner: BindPrincipal::Reserved {
                module_id: "prefrontal-core".into(),
            },
            scope_ref: "flow-discovery".into(),
            scope_epoch: 1,
            flow_id: Some("flow-discovery".into()),
            sender: Some(principal.clone()),
            agent_id: None,
            run_id: None,
        },
        kind: ScopeKind::Head,
        parent: None,
        parent_state: None,
        owner_authorized: true,
    });
    let mut operations: Vec<_> = crate::manifest::management_operations()
        .into_iter()
        .filter_map(|operation| {
            matches!(
                admit(&operation.name, Some(&principal), &scope),
                Ok(Admission::Legacy | Admission::Diagnostics)
            )
            .then_some(operation.name)
        })
        .collect();
    operations.sort();
    operations
}
