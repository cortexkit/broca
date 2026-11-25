//! Wire admission and reply formatting for a single Clairic item.

use broca_protocol::{BindPrincipal, SendScope};
use cortexkit_llm_tools::{extract as contract, ValidationError};
use cortexkit_role_extractor::errors::Refusal;
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};

use super::admission::{admit_scope, call_id, hash, Admission};

pub(crate) const NAME: &str = "llm_extract";
pub(crate) const DESCRIPTION: &str = "Extract typed fields from one item.";
const BATCH_PREFIX: &str = "extract-";

pub(super) fn is_key(batch_id: &str) -> bool {
    batch_id.starts_with(BATCH_PREFIX)
}

pub(super) fn input_schema(references: bool) -> Value {
    if references {
        contract::input_schema_with_image_references()
    } else {
        contract::input_schema()
    }
}

pub(super) fn admit(
    principal: Option<&BindPrincipal>,
    scope: &SendScope,
    arguments: &RawValue,
    call_key: Option<&str>,
    references: bool,
) -> Result<Admission, Refusal> {
    let (scope, stamped) = admit_scope(principal, scope)?;
    let invalid = |e: serde_json::Error| Refusal::invalid_params("arguments", e.to_string());
    let value: Value = serde_json::from_str(arguments.get()).map_err(invalid)?;
    if !value.get("max_cost_usd").is_some_and(Value::is_number) {
        return Err(Refusal::invalid_params(
            "max_cost_usd",
            "max_cost_usd is a required non-negative number",
        ));
    }
    let request: contract::Request = serde_json::from_str(arguments.get()).map_err(invalid)?;
    (if references {
        contract::validate_request_with_image_references(&request)
    } else {
        contract::validate_request(&request)
    })
    .map_err(|error| match error {
        ValidationError::Role(refusal) => refusal,
        ValidationError::EmptyModel => Refusal::invalid_params("model", "model must not be empty"),
        ValidationError::NegativeCost { field } => {
            Refusal::invalid_params(field, "cost must not be negative")
        }
        other => Refusal::invalid_params("arguments", other.to_string()),
    })?;
    let (key, _) = call_id(principal, stamped, call_key)?;
    // Consent storage and in-flight calls share a key space with classify and complete.
    let batch_id = format!("{BATCH_PREFIX}{key}");
    #[derive(Deserialize)]
    struct RawArguments<'a> {
        #[serde(borrow)]
        fields: &'a RawValue,
        #[serde(borrow)]
        item: &'a RawValue,
        #[serde(borrow)]
        options: Option<&'a RawValue>,
        #[serde(borrow)]
        max_cost_usd: &'a RawValue,
    }
    #[derive(Serialize)]
    struct Batch<'a> {
        batch_id: &'a str,
        model: &'a str,
        fields: &'a RawValue,
        items: [&'a RawValue; 1],
        #[serde(skip_serializing_if = "Option::is_none")]
        options: Option<&'a RawValue>,
        max_cost_usd: &'a RawValue,
    }
    let raw: RawArguments<'_> = serde_json::from_str(arguments.get()).map_err(invalid)?;
    // Hash the arguments for replay, but send ordered fields and exact numbers to the role.
    let params = serde_json::to_string(&Batch {
        batch_id: &batch_id,
        model: &request.model,
        fields: raw.fields,
        items: [raw.item],
        options: raw.options,
        max_cost_usd: raw.max_cost_usd,
    })
    .expect("validated extractor arguments encode");
    Ok(Admission {
        scope,
        batch_id,
        args_digest: hash(&value),
        params,
        provider: request.model.split('/').next().unwrap_or_default().into(),
    })
}

/// Embed the original result rather than decoding provider numbers into JSON values.
pub(super) fn tool_result(body: &RawValue) -> Result<Box<RawValue>, serde_json::Error> {
    #[derive(Serialize)]
    struct Wrapped<'a> {
        content: Value,
        #[serde(rename = "structuredContent")]
        structured: &'a RawValue,
    }
    serde_json::value::to_raw_value(&Wrapped {
        content: json!([{"type":"text","text":body.get()}]),
        structured: body,
    })
}
