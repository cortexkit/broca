//! The fetch plan a session's first `session.send` carries, read and checked before anything is
//! fetched.
//!
//! A plan names, by role, the providers whose tools, system text, compaction and step transforms
//! the session runs with, each with a preset and params passed to the provider verbatim, plus
//! the composition (every provider's model-facing tools and capability tags, and the
//! step-transform items) and that composition's digest.

use std::collections::{BTreeMap, BTreeSet};

use cortexkit_role_step_transform_provider::{
    grant::plan_user_grants,
    subscription::{
        check_reduction, InvalidSubscriptionDetail, OnUnavailable, Phase, Subscription,
    },
};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::refusal::Refusal;

/// The provider of the session's required persona or worker-role system text.
/// Its system-text item may never be optional, regardless of the role preset.
pub const PERSONA_PROVIDER: &str = "prefrontal-core";
/// Legacy persona preset spelling. Persona identity is determined by provider, not preset.
pub const PERSONA_PRESET: &str = "persona";

/// `session.send` members a send carrying a plan must not carry: the plan replaces them.
pub const MEMBERS_REPLACED_BY_PLAN: &[&str] = &["tools", "system", "context_management"];

/// One plan item: a provider, the preset it defines, and params passed to it verbatim.
#[derive(Debug, Clone, PartialEq)]
pub struct Item {
    pub provider: String,
    pub preset: String,
    pub params: Map<String, Value>,
    /// Only a system-text item may be optional.
    pub optional: bool,
}

/// A step-transform item: an [`Item`] and the hook subscriptions frozen into the plan.
#[derive(Debug, Clone, PartialEq)]
pub struct StepItem {
    pub item: Item,
    pub subscriptions: Vec<Subscription>,
}

/// One tool of a provider's composition entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComposedTool {
    pub name: String,
    pub capabilities: Vec<String>,
}

/// A checked plan.
#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    /// The plan as JSON text, from the value it arrived as.
    pub text: String,
    /// SHA-256, as 64 lowercase hex characters, of the plan's RFC 8785 (JCS) bytes.
    pub identity: String,
    pub tool_items: Vec<Item>,
    pub system_text_items: Vec<Item>,
    pub compaction_item: Option<Item>,
    pub step_items: Vec<StepItem>,
    /// The composition exactly as received, carried to every provider verbatim.
    pub composition: Map<String, Value>,
    pub composition_digest: String,
    /// Each provider's composed tools, by provider id.
    pub composed: BTreeMap<String, Vec<ComposedTool>>,
}

/// Hex SHA-256 of `bytes`.
pub fn sha256_hex(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

/// The identity of a plan: SHA-256 of its RFC 8785 bytes, as 64 lowercase hex characters.
pub fn plan_identity(plan: &Value) -> Result<String, Refusal> {
    let bytes = serde_jcs::to_vec(plan).map_err(|_| Refusal::invalid("plan"))?;
    Ok(sha256_hex(&bytes))
}

/// Check the plan a later send carries against the one the session froze: the identical plan
/// is accepted and changes nothing; any other plan, or any plan on a session admitted without
/// one (`frozen` is `None`), is refused `plan_drift` naming both identities.
pub fn check_later_plan(frozen: Option<&str>, sent: &str) -> Result<(), Refusal> {
    if frozen == Some(sent) {
        return Ok(());
    }
    Err(Refusal::PlanDrift {
        frozen: frozen.map(str::to_owned),
        sent: sent.to_owned(),
    })
}

/// Refuse a send that carries a plan beside a member the plan replaces, naming the member.
pub fn check_send_members(params: &Map<String, Value>) -> Result<(), Refusal> {
    match MEMBERS_REPLACED_BY_PLAN
        .iter()
        .find(|member| params.get(**member).is_some_and(|value| !value.is_null()))
    {
        Some(member) => Err(Refusal::invalid(*member)),
        None => Ok(()),
    }
}

fn string_member(object: &Map<String, Value>, key: &str, at: &str) -> Result<String, Refusal> {
    object
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| Refusal::invalid(format!("{at}.{key}")))
}

/// Read one item at `at` (for example `plan.tool_items[0]`). `may_be_optional` is true only
/// for system-text items, which may carry `optional: true`; no item may carry `false`.
fn item(value: &Value, at: &str, may_be_optional: bool) -> Result<Item, Refusal> {
    let object = value.as_object().ok_or_else(|| Refusal::invalid(at))?;
    let provider = string_member(object, "provider", at)?;
    let preset = string_member(object, "preset", at)?;
    if preset.is_empty()
        || preset.len() > 64
        || !preset.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_' || byte == b'-'
        })
    {
        return Err(Refusal::invalid(format!("{at}.preset")));
    }
    let params = object
        .get("params")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| Refusal::invalid(format!("{at}.params")))?;
    let optional = match object.get("optional") {
        None => false,
        Some(Value::Bool(true)) if may_be_optional => true,
        Some(_) => return Err(Refusal::invalid(format!("{at}.optional"))),
    };
    Ok(Item {
        provider,
        preset,
        params,
        optional,
    })
}

fn items(
    plan: &Map<String, Value>,
    list: &str,
    may_be_optional: bool,
) -> Result<Vec<Item>, Refusal> {
    let at = format!("plan.{list}");
    let values = plan
        .get(list)
        .and_then(Value::as_array)
        .ok_or_else(|| Refusal::invalid(&at))?;
    values
        .iter()
        .enumerate()
        .map(|(index, value)| item(value, &format!("{at}[{index}]"), may_be_optional))
        .collect()
}

/// Refuse a role list that names one provider twice, naming the list.
fn unique_providers<'a>(
    list: &str,
    providers: impl Iterator<Item = &'a str>,
) -> Result<(), Refusal> {
    let mut seen = BTreeSet::new();
    for provider in providers {
        if !seen.insert(provider) {
            return Err(Refusal::invalid(format!("plan.{list}")));
        }
    }
    Ok(())
}

/// Strictly ascending bytewise: sorted with no repeats.
fn strictly_ascending<'a>(values: impl Iterator<Item = &'a str>) -> bool {
    let values: Vec<&str> = values.collect();
    values.windows(2).all(|pair| pair[0] < pair[1])
}

/// Read the composition's provider entries, refusing any that is not in canonical order (sorted
/// bytewise by provider id, each provider's tools by name and each tool's capabilities, none
/// repeated).
fn composed(
    composition: &Map<String, Value>,
) -> Result<BTreeMap<String, Vec<ComposedTool>>, Refusal> {
    let refuse = || Refusal::invalid("plan.composition");
    let providers = composition
        .get("providers")
        .and_then(Value::as_array)
        .ok_or_else(refuse)?;
    let mut order = Vec::new();
    let mut composed = BTreeMap::new();
    for entry in providers {
        let entry = entry.as_object().ok_or_else(refuse)?;
        let provider = entry
            .get("provider")
            .and_then(Value::as_str)
            .ok_or_else(refuse)?;
        let tools = entry
            .get("tools")
            .and_then(Value::as_array)
            .ok_or_else(refuse)?;
        let mut composed_tools = Vec::with_capacity(tools.len());
        for tool in tools {
            let tool = tool.as_object().ok_or_else(refuse)?;
            let name = tool
                .get("name")
                .and_then(Value::as_str)
                .ok_or_else(refuse)?;
            let capabilities: Vec<String> = tool
                .get("capabilities")
                .and_then(Value::as_array)
                .ok_or_else(refuse)?
                .iter()
                .map(|tag| tag.as_str().map(str::to_owned).ok_or_else(refuse))
                .collect::<Result<_, _>>()?;
            if !strictly_ascending(capabilities.iter().map(String::as_str)) {
                return Err(refuse());
            }
            composed_tools.push(ComposedTool {
                name: name.to_owned(),
                capabilities,
            });
        }
        if !strictly_ascending(composed_tools.iter().map(|tool| tool.name.as_str())) {
            return Err(refuse());
        }
        order.push(provider.to_owned());
        composed.insert(provider.to_owned(), composed_tools);
    }
    if !strictly_ascending(order.iter().map(String::as_str)) {
        return Err(refuse());
    }
    Ok(composed)
}

/// Whether the runner release can serve the compaction writer surface.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompactionSupport {
    Unsupported,
    Supported,
}

/// Read and check `plan`, the `plan` member of a send, before anything is fetched.
///
/// Refuses `invalid_params` naming the field for a malformed plan, and then
/// `{field: "plan.compaction_item"}` for a plan naming a compaction provider on a runner
/// without the writer surface. Malformed means a missing or mistyped
/// member, `optional` anywhere but on a system-text item (or `false`, or on the persona item),
/// a provider named twice in one role list, a malformed or forbidden step subscription, a
/// composition whose digest is not the one stated, or a composition that is not in canonical
/// order, does not list exactly the plan's providers, or does not repeat the plan's
/// step-transform items.
pub fn parse_plan(plan: &Value, compaction: CompactionSupport) -> Result<Plan, Refusal> {
    let object = plan.as_object().ok_or_else(|| Refusal::invalid("plan"))?;
    let tool_items = items(object, "tool_items", false)?;
    let system_text_items = items(object, "system_text_items", true)?;
    if let Some((index, _)) = system_text_items
        .iter()
        .enumerate()
        .find(|(_, item)| item.optional && item.provider == PERSONA_PROVIDER)
    {
        return Err(Refusal::invalid(format!(
            "plan.system_text_items[{index}].optional"
        )));
    }
    let compaction_item = match object.get("compaction_item") {
        None => None,
        Some(value) => Some(item(value, "plan.compaction_item", false)?),
    };
    // Only the frozen compaction provider may replace the model's conversation on these hooks.
    let reduction_owner = compaction_item.as_ref().map(|item| item.provider.as_str());
    let step_values = object
        .get("step_transform_items")
        .and_then(Value::as_array)
        .ok_or_else(|| Refusal::invalid("plan.step_transform_items"))?;
    let mut step_items = Vec::with_capacity(step_values.len());
    for (index, value) in step_values.iter().enumerate() {
        let at = format!("plan.step_transform_items[{index}]");
        let item = item(value, &at, false)?;
        let subscriptions: Vec<Subscription> = value
            .get("subscriptions")
            .and_then(Value::as_array)
            .ok_or_else(|| Refusal::invalid(format!("{at}.subscriptions")))?
            .iter()
            .enumerate()
            .map(|(position, subscription)| {
                serde_json::from_value(subscription.clone())
                    .map_err(|_| Refusal::invalid(format!("{at}.subscriptions[{position}]")))
            })
            .collect::<Result<_, _>>()?;
        for (position, subscription) in subscriptions.iter().enumerate() {
            if let Err(problem) = subscription
                .check()
                .and_then(|()| check_reduction(subscription, &item.provider, reduction_owner))
            {
                return Err(Refusal::InvalidParams {
                    detail: serde_json::to_value(InvalidSubscriptionDetail::new(
                        index, position, problem,
                    ))
                    .expect("a subscription detail is JSON"),
                });
            }
        }
        step_items.push(StepItem {
            item,
            subscriptions,
        });
    }
    unique_providers("tool_items", tool_items.iter().map(|i| i.provider.as_str()))?;
    unique_providers(
        "system_text_items",
        system_text_items.iter().map(|i| i.provider.as_str()),
    )?;
    unique_providers(
        "step_transform_items",
        step_items.iter().map(|i| i.item.provider.as_str()),
    )?;

    // Only the dedicated top-level field conveys user permission. Grant-shaped data in
    // params or composition is ordinary provider data, never an authorization source.
    plan_user_grants(object).map_err(|_| Refusal::invalid("plan.user_grants"))?;

    let composition = object
        .get("composition")
        .and_then(Value::as_object)
        .cloned()
        .ok_or_else(|| Refusal::invalid("plan.composition"))?;
    if composition.contains_key("compaction") {
        return Err(Refusal::invalid("plan.composition.compaction"));
    }
    let stated = object
        .get("composition_digest")
        .and_then(Value::as_str)
        .ok_or_else(|| Refusal::invalid("plan.composition_digest"))?;
    let digest = cortexkit_role_tool_provider::catalog::composition_digest(&Value::Object(
        composition.clone(),
    ))
    .map_err(|_| Refusal::invalid("plan.composition"))?;
    if digest != stated {
        return Err(Refusal::invalid("plan.composition_digest"));
    }
    let composed = composed(&composition)?;
    let named: BTreeSet<&str> = tool_items
        .iter()
        .chain(&system_text_items)
        .chain(compaction_item.iter())
        .chain(step_items.iter().map(|step| &step.item))
        .map(|item| item.provider.as_str())
        .collect();
    if !named
        .iter()
        .copied()
        .eq(composed.keys().map(String::as_str))
    {
        return Err(Refusal::invalid("plan.composition"));
    }
    // The composition repeats the plan's step-transform items, frozen policy included. When
    // there are none it may omit the member or carry it empty; the digest already covers
    // whichever form the caller chose.
    let composed_steps = composition.get("step_transform_items");
    let steps_match = if step_values.is_empty() {
        composed_steps.is_none_or(|steps| steps.as_array().is_some_and(Vec::is_empty))
    } else {
        composed_steps == object.get("step_transform_items")
    };
    if !steps_match {
        return Err(Refusal::invalid("plan.composition"));
    }

    // Reader releases can replay compaction records but cannot serve the writer surface.
    // Refuse before admission starts any provider fetch or declaration.
    if compaction == CompactionSupport::Unsupported && compaction_item.is_some() {
        return Err(Refusal::invalid("plan.compaction_item"));
    }

    for (index, step) in step_items.iter().enumerate() {
        for (position, subscription) in step.subscriptions.iter().enumerate() {
            // Approval must fail closed: an unavailable hook cannot grant permission.
            if subscription.phase == Some(Phase::Approve)
                && subscription.on_unavailable == OnUnavailable::Pass
            {
                return Err(Refusal::InvalidParams {
                    detail: serde_json::to_value(InvalidSubscriptionDetail {
                        field: "plan.step_transform_items".into(),
                        item: index,
                        subscription: position,
                        problem: "approve_on_unavailable_pass".into(),
                    })
                    .expect("a subscription detail is JSON"),
                });
            }
            if subscription.budget_ms > broca_protocol::STEP_TRANSFORM_HOOK_CAP {
                return Err(Refusal::InvalidParams {
                    detail: serde_json::json!({
                        "field": format!("plan.step_transform_items[{index}].subscriptions[{position}].budget_ms"),
                        "cap": broca_protocol::STEP_TRANSFORM_HOOK_CAP
                    }),
                });
            }
        }
    }

    Ok(Plan {
        text: serde_json::to_string(plan).expect("a JSON value serializes"),
        identity: plan_identity(plan)?,
        tool_items,
        system_text_items,
        compaction_item,
        step_items,
        composition,
        composition_digest: digest,
        composed,
    })
}
