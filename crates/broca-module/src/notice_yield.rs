//! Admission for an urgent steer, before sender authority is replaced by the session owner's.

use broca_fetch_plan::CALL_YIELD_GROUPS;
use broca_protocol::{BindPrincipal, Delivery, DeliveryOrder, SendScope, SessionScope};
use cortexkit_role_llm_runner::{delivery_order, send};

use crate::actor::SendOutcome;

/// The owner whose authority a first send can freeze, or the session's recorded scope owner.
/// The ordinary scope gate still decides whether a stamped route is allowed to open the session.
pub(crate) fn scope_owner(frozen: Option<&SessionScope>, route: &SendScope) -> Option<String> {
    frozen
        .or(match route {
            SendScope::Stamped(stamp) => Some(&stamp.scope),
            _ => None,
        })
        .map(|scope| scope.owner.canonical_owner_string())
}

pub(crate) fn check(
    proposed: Option<&broca_records::record::PlanManifest>,
    scope_owner: Option<&str>,
    urgent_yield: bool,
    sender: Option<&BindPrincipal>,
    delivery: Delivery,
    order: Option<&DeliveryOrder>,
    senders: &[delivery_order::PlanSender],
) -> Result<(), SendOutcome> {
    let proposed = proposed.and_then(|manifest| {
        serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(
            manifest.plan.as_json_text(),
        )
        .ok()
    });
    if let Some(plan) = &proposed {
        broca_fetch_plan::check_plan_call_yield(plan, scope_owner)
            .map_err(SendOutcome::PlanRefused)?;
    }
    if !urgent_yield {
        return Ok(());
    }
    let proposed_senders;
    let senders = if let Some(plan) = &proposed {
        proposed_senders =
            broca_fetch_plan::plan_senders(plan).map_err(SendOutcome::PlanRefused)?;
        &proposed_senders
    } else {
        senders
    };
    check_urgent_yield(scope_owner, sender, delivery, order, senders)
        .map_err(|field| SendOutcome::StructuredOutputInvalid { field })
}

/// Check a `yield: true` send against the sender policy that will admit it.
pub fn check_urgent_yield(
    scope_owner: Option<&str>,
    sender: Option<&BindPrincipal>,
    delivery: Delivery,
    order: Option<&DeliveryOrder>,
    senders: &[delivery_order::PlanSender],
) -> Result<(), String> {
    let Some(BindPrincipal::Reserved { module_id }) = sender else {
        return Err("yield".into());
    };
    let request = send::SendRequest::new("", "").with_yield(true);
    let request = if let Some(order) = order {
        request.with_delivery_order(&delivery_order::DeliveryOrder::new(order.modes.clone()))
    } else {
        request.with_delivery(match delivery {
            Delivery::Queue => send::Delivery::Queue,
            Delivery::Steer => send::Delivery::Steer,
            Delivery::Interrupt => send::Delivery::Interrupt,
        })
    };
    request
        .check_notice_yield(
            CALL_YIELD_GROUPS,
            scope_owner,
            broca_protocol::canonical_module(module_id),
            &broca_core::delivery_order::canonical_senders(senders),
        )
        .map(|_| ())
}
