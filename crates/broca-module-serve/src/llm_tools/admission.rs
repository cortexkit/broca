use broca_protocol::{BindPrincipal, SendScope};
use broca_store::system_one::BatchScope;
use cortexkit_llm_tools::classify;
use cortexkit_role_classifier::errors::{ErrorClass, Refusal, RefusalDetail};
use serde::Deserialize;
use serde_json::{json, value::RawValue, Value};
use sha2::{Digest, Sha256};

pub(super) struct Admission {
    pub scope: BatchScope,
    pub batch_id: String,
    pub args_digest: String,
    pub params: String,
    pub provider: String,
}

#[derive(Deserialize)]
struct RawArguments<'a> {
    #[serde(borrow)]
    questions: &'a RawValue,
    #[serde(borrow)]
    item: &'a RawValue,
    #[serde(borrow)]
    max_cost_usd: &'a RawValue,
}

pub(super) fn refusal(code: &str, message: impl Into<String>) -> Refusal {
    Refusal::new(code, message).with_detail(RefusalDetail::new(ErrorClass::Permanent))
}

pub(super) fn hash(value: &Value) -> String {
    let bytes = serde_jcs::to_vec(value).expect("JSON values have a canonical encoding");
    format!("{:x}", Sha256::digest(bytes))
}

/// Scope admission precedes argument decoding: a flow carrier cannot use an unscoped or
/// agent-scoped bind even when the call's model-visible arguments are malformed.
pub(super) fn admit(
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
    arguments: &RawValue,
    call_key: Option<&str>,
) -> Result<Admission, Refusal> {
    let stamp = match scope {
        SendScope::Stamped(bind) => Some(&bind.scope),
        _ => None,
    };
    if broca_core::scope_admission::is_flow_only(principal)
        && stamp.and_then(|s| s.flow_id.as_ref()).is_none()
    {
        return Err(refusal(
            "flow_scope_required",
            "Basal model tools require a daemon-stamped flow scope",
        ));
    }
    let stamped = stamp.ok_or_else(|| {
        refusal(
            "scope_required",
            "model tools require a daemon-stamped run or flow scope",
        )
    })?;
    let scope = if let Some(flow_id) = &stamped.flow_id {
        BatchScope::Flow {
            flow_id: flow_id.clone(),
        }
    } else if let (Some(agent_id), Some(run_id)) = (&stamped.agent_id, &stamped.run_id) {
        BatchScope::Run {
            agent_id: agent_id.clone(),
            run_id: run_id.clone(),
        }
    } else {
        return Err(refusal(
            "scope_required",
            "model tools require a daemon-stamped agent and run or flow",
        ));
    };
    let value: Value = serde_json::from_str(arguments.get())
        .map_err(|error| Refusal::invalid_params("arguments", error.to_string()))?;
    let object = value
        .as_object()
        .ok_or_else(|| Refusal::invalid_params("arguments", "arguments must be an object"))?;
    for field in object.keys() {
        if !["model", "questions", "item", "max_cost_usd"].contains(&field.as_str()) {
            return Err(Refusal::invalid_params(
                field,
                format!("unknown argument {field}"),
            ));
        }
    }
    if !object.get("max_cost_usd").is_some_and(Value::is_number) {
        return Err(Refusal::invalid_params(
            "max_cost_usd",
            "max_cost_usd is a required non-negative number",
        ));
    }
    let request: classify::Request = serde_json::from_str(arguments.get())
        .map_err(|e| Refusal::invalid_params("arguments", e.to_string()))?;
    classify::validate_request(&request).map_err(|e| match e {
        cortexkit_llm_tools::ValidationError::Role(refusal) => refusal,
        cortexkit_llm_tools::ValidationError::NegativeCost { field } => {
            Refusal::invalid_params(field, "cost must not be negative")
        }
        other => Refusal::invalid_params("arguments", other.to_string()),
    })?;
    let batch_id = if let Some(call_key) = call_key {
        let principal = principal.ok_or_else(|| {
            refusal(
                "scope_required",
                "a keyed model tool requires a stamped caller principal",
            )
        })?;
        hash(&json!({
            "principal": principal.as_owner_string(),
            "owner": stamped.owner.as_owner_string(),
            "scope_ref": stamped.scope_ref,
            "scope_epoch": stamped.scope_epoch,
            "call_key": call_key,
        }))
    } else {
        let mut random = [0u8; 32];
        getrandom::fill(&mut random)
            .map_err(|_| crate::system_one::internal_refusal("cannot generate a fresh batch id"))?;
        random.iter().map(|byte| format!("{byte:02x}")).collect()
    };
    let args_digest = hash(&value);
    let raw: RawArguments<'_> = serde_json::from_str(arguments.get())
        .expect("validated arguments have their required fields");
    // JSON Canonicalization Scheme (JCS) defines replay identity, not the provider payload. Preserve question order and
    // structured state JSON exactly as the caller supplied them.
    let params = format!(
        "{{\"batch_id\":{},\"model\":{},\"questions\":{},\"items\":[{}],\"max_cost_usd\":{}}}",
        serde_json::to_string(&batch_id).expect("batch id encodes"),
        serde_json::to_string(&request.model).expect("model encodes"),
        raw.questions.get(),
        raw.item.get(),
        raw.max_cost_usd.get(),
    );
    Ok(Admission {
        scope,
        batch_id,
        args_digest,
        params,
        provider: request.model.split('/').next().unwrap_or_default().into(),
    })
}
