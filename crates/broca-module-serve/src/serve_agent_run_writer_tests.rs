use super::*;

#[test]
fn basal_program_bind_preserves_attested_parent_identity_and_state() {
    use broca_protocol::{ParentState, ScopeKind};
    for (wire_state, state) in [
        ("linked", ParentState::Linked),
        ("pending", ParentState::Pending),
        ("ended", ParentState::Ended),
    ] {
        let stamp: subc_protocol::scope::ScopeStamp = serde_json::from_value(serde_json::json!({
            "owner": {"kind":"reserved", "module_id":"prefrontal-core"},
            "ref":"program-scope", "scope_epoch":7, "kind":"ephemeral",
            "owner_authorized": true,
            "attributes": {"agent_id":"program-agent", "run_id":"program-run"},
            "parent": {
                "owner":{"kind":"reserved", "module_id":"different-parent-owner"},
                "ref":"head-scope", "scope_epoch":3
            },
            "parent_state":wire_state
        }))
        .unwrap();
        let basal = subc_protocol::Principal::Reserved {
            module_id: "basal".into(),
        };
        let bind = to_bind_scope(&stamp, Some(&basal));
        assert_eq!(bind.kind, ScopeKind::Ephemeral);
        assert_eq!(bind.parent_state, Some(state));
        assert!(bind.owner_authorized);
        assert_eq!(bind.scope.agent_id.as_deref(), Some("program-agent"));
        assert_eq!(bind.scope.run_id.as_deref(), Some("program-run"));
        let parent = bind.parent.unwrap();
        assert_eq!(
            parent.owner.as_owner_string(),
            "reserved:different-parent-owner"
        );
        assert_eq!(parent.scope_ref, "head-scope");
        assert_eq!(parent.scope_epoch, 3);
    }
}

#[test]
fn run_scope_deadline_does_not_change_frozen_scope_bytes() {
    let stamp = subc_protocol::scope::ScopeStamp::new(
        subc_protocol::Principal::Reserved {
            module_id: "prefrontal-core".into(),
        },
        "parent-run",
        1,
        subc_protocol::scope::ScopeKind::Head,
    )
    .with_attributes(
        subc_protocol::scope::ScopeAttributes::new()
            .with_agent_id(Some("agent-1".into()))
            .with_run_id(Some("parent-run".into())),
    )
    .with_owner_authorized(true);
    let principal = subc_protocol::Principal::Reserved {
        module_id: "basal".into(),
    };
    let freeze = |stamp: &subc_protocol::scope::ScopeStamp| {
        let bind = to_bind_scope(stamp, Some(&principal));
        let broca_core::ScopeDecision::Freeze(scope) = broca_core::decide_scope(
            &broca_core::SessionOwnershipV1::default(),
            Some(&to_bind_principal(&principal)),
            &broca_protocol::SendScope::Stamped(bind),
        )
        .unwrap() else {
            panic!("the first send must freeze the scope")
        };
        serde_json::to_vec(&scope).unwrap()
    };
    let expected = br#"{"owner":{"kind":"reserved","module_id":"prefrontal-core"},"ref":"parent-run","scope_epoch":1,"sender":{"kind":"reserved","module_id":"basal"},"agent_id":"agent-1","run_id":"parent-run"}"#;
    assert_eq!(freeze(&stamp), expected);
    for deadline in [0, 1, u64::MAX] {
        assert_eq!(
            freeze(&stamp.clone().with_expires_at_ms(Some(deadline))),
            expected
        );
    }
}

#[test]
fn flow_bind_preserves_agent_identity_for_new_freezes() {
    let stamp: subc_protocol::scope::ScopeStamp = serde_json::from_value(serde_json::json!({
        "owner":{"kind":"reserved","module_id":"owner-module"},
        "ref":"flow","scope_epoch":1,"kind":"head","owner_authorized":true,
        "attributes":{"flow_id":"flow","agent_id":"agent-a"}
    }))
    .unwrap();
    let principal = subc_protocol::Principal::Reserved {
        module_id: "flow-carrier".into(),
    };
    let bind = to_bind_scope(&stamp, Some(&principal));
    assert_eq!(bind.scope.agent_id.as_deref(), Some("agent-a"));
    let decision = broca_core::decide_scope(
        &broca_core::SessionOwnershipV1::default(),
        Some(&to_bind_principal(&principal)),
        &broca_protocol::SendScope::Stamped(bind),
    )
    .unwrap();
    let scope = decision.scope().unwrap();
    assert!(scope.agent_id_frozen);
    assert_eq!(scope.agent_id.as_deref(), Some("agent-a"));
}

#[test]
fn agent_run_bind_identity_reaches_writer_admission() {
    let stamp: subc_protocol::scope::ScopeStamp = serde_json::from_value(serde_json::json!({
        "owner": {"kind": "reserved", "module_id": "prefrontal-core"},
        "ref": "parent-run", "scope_epoch": 1,
        "kind": "head", "owner_authorized": true,
        "attributes": {"agent_id": "agent-1", "run_id": "parent-run"}
    }))
    .unwrap();
    let principal = subc_protocol::Principal::Reserved {
        module_id: "basal".into(),
    };
    let bind = to_bind_scope(&stamp, Some(&principal));
    assert_eq!(bind.scope.run_id.as_deref(), Some("parent-run"));
    assert_eq!(bind.scope.agent_id.as_deref(), Some("agent-1"));
    assert_eq!(
        bind.scope
            .sender
            .as_ref()
            .map(broca_protocol::BindPrincipal::as_owner_string),
        Some("reserved:basal".into())
    );
    broca_core::scope_admission::require_flow_scope(
        bind.scope.sender.as_ref(),
        &broca_protocol::SendScope::Stamped(bind.clone()),
    )
    .unwrap();
    assert_eq!(
        broca_core::decide_scope(
            &broca_core::SessionOwnershipV1::default(),
            bind.scope.sender.as_ref(),
            &broca_protocol::SendScope::Stamped(bind.clone()),
        ),
        Ok(broca_core::ScopeDecision::Freeze(bind.scope))
    );
}
