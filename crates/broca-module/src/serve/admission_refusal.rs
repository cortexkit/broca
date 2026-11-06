//! Admission refusals share their caller-facing representation whether a send starts directly
//! or is drained from a durable queue.

use super::ServeError;
use broca_core::RunError;
use broca_protocol::{ErrorClass, ProviderError};
use serde::Serialize;
use serde_json::{json, Value};

#[derive(Serialize)]
pub struct RefusalBody {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<Value>,
}

// Keep the typed routing bodies so the detail member order remains stable in serialized replies.
#[derive(Serialize)]
#[serde(untagged)]
pub enum AdmissionRefusal {
    AccountSelection(broca_wire::AccountSelectionRefused),
    RoutingUnavailable(broca_wire::RoutingUnavailable),
    Other(RefusalBody),
}

pub fn upload_refusal(
    error: &broca_core::media_uploads::UploadError,
    for_send: bool,
) -> RefusalBody {
    let mut detail = json!({});
    if let Some(field) = error.field {
        detail["field"] = field.into();
    }
    if let Some(received) = error.received {
        detail["received"] = received.into();
    }
    if for_send || error.error_class() == ErrorClass::Transient {
        detail["class"] = serde_json::to_value(error.error_class().shared_class()).unwrap();
    }
    RefusalBody {
        code: error.code.into(),
        message: error.to_string(),
        detail: Some(detail),
    }
}

/// The pure part of the direct-send refusal serializer. Other management errors keep their
/// operation-specific rendering in the serving layer.
pub fn admission_refusal(error: &ServeError) -> Option<AdmissionRefusal> {
    let (code, detail) = match error {
        ServeError::AccountSelectionRefused(refusal) => {
            return Some(AdmissionRefusal::AccountSelection(
                broca_wire::AccountSelectionRefused::new(
                    refusal.routing_code.clone(),
                    refusal.class,
                    refusal.retry_after_s,
                    refusal.decision_id.clone(),
                )
                .with_routing_detail(refusal.routing_detail.clone(), refusal.resets_at_ms),
            ))
        }
        ServeError::RoutingUnavailable(unavailable) => {
            return Some(AdmissionRefusal::RoutingUnavailable(
                broca_wire::RoutingUnavailable::new(
                    unavailable.reason.as_str(),
                    unavailable.budget_ms,
                ),
            ))
        }
        ServeError::Upload(error) => {
            return Some(AdmissionRefusal::Other(upload_refusal(error, false)))
        }
        ServeError::Open(_, class) => (
            "open_failed",
            class.map(|class| json!({"class": class.shared_class()})),
        ),
        ServeError::Transient(_) => ("transient", None),
        ServeError::VariantUnsupported(refusal) => (
            "variant_unsupported",
            Some(json!({
                "model": refusal.model, "variant": refusal.variant, "defined": refusal.defined,
            })),
        ),
        ServeError::ToolAliasInvalid(refusal) => (
            "tool_alias_invalid",
            Some(json!({"tool": refusal.tool, "class": "permanent"})),
        ),
        ServeError::SessionTooLarge { max_bytes } => (
            "session_too_large",
            Some(json!({"class": "permanent", "max_bytes": max_bytes})),
        ),
        ServeError::RetainedSessionTooLarge { max_bytes } => (
            "retained_session_too_large",
            Some(json!({"class": "permanent", "max_bytes": max_bytes})),
        ),
        ServeError::WalNewerWriter { missing, .. } => (
            broca_wire::WAL_NEWER_WRITER,
            Some(
                serde_json::to_value(broca_wire::WalNewerWriterDetail::NewerWriter {
                    missing: missing.clone(),
                })
                .unwrap(),
            ),
        ),
        ServeError::DeliveryRefused { code, .. } => (*code, Some(json!({"field": "delivery"}))),
        ServeError::InvalidParams { field, .. } => {
            (broca_wire::INVALID_PARAMS, Some(json!({"field": field})))
        }
        ServeError::MediaTooLarge(detail) => (
            cortexkit_role_llm_runner::errors::MEDIA_TOO_LARGE,
            Some(serde_json::to_value(detail).unwrap()),
        ),
        ServeError::SendIdReuse { differing } => (
            "send_id_reuse",
            send_field_of(differing).map(|field| json!({"field": field})),
        ),
        _ => return None,
    };
    Some(AdmissionRefusal::Other(RefusalBody {
        code: code.into(),
        message: error.to_string(),
        detail,
    }))
}

pub fn send_field_of(differing: &str) -> Option<&'static str> {
    Some(match differing {
        "input" => "prompt",
        "policy_allowed" | "tools" => "tools",
        "delivery" => "delivery",
        "conversation_key" => "conversation_key",
        "model" => "model",
        "variant" => "model.variant",
        "tool_choice" => "tool_choice",
        "generation" => "generation",
        "cache" => "cache",
        "budget" => "budget",
        "auth" => "auth",
        "context_limit" => "context_limit",
        "service_tier" => "service_tier",
        "stop_when" => "stop_when",
        "context_management" => "context_management",
        "client_message_id" => "client_message_id",
        _ => return None,
    })
}

/// Preserve the same typed refusal a direct start would send, but in the existing durable
/// terminal shape. That shape has no field for routing's `routing_detail` or `decision_id`.
pub(crate) fn queued_admission_error(error: RunError) -> ProviderError {
    let class = error.admission_error_class();
    let served = match error {
        RunError::Upload(error) => ServeError::Upload(error),
        RunError::MediaTooLarge(detail) => ServeError::MediaTooLarge(detail),
        RunError::MediaUnsupported(message) => ServeError::DeliveryRefused {
            code: "media_unsupported",
            message,
        },
        RunError::Transient(message) => ServeError::Transient(message),
        RunError::VariantUnsupported(refusal) => ServeError::VariantUnsupported(refusal),
        RunError::ToolAliasInvalid(refusal) => ServeError::ToolAliasInvalid(refusal),
        RunError::AccountSelectionRefused(refusal) => ServeError::AccountSelectionRefused(refusal),
        RunError::RoutingUnavailable(unavailable) => ServeError::RoutingUnavailable(unavailable),
        RunError::SessionTooLarge { max_bytes } => ServeError::SessionTooLarge { max_bytes },
        RunError::RetainedSessionTooLarge { max_bytes } => {
            ServeError::RetainedSessionTooLarge { max_bytes }
        }
        RunError::NewerWriter { ref missing } => ServeError::WalNewerWriter {
            missing: missing.clone(),
            reason: error.to_string(),
        },
        RunError::SendIdReuse { differing } => ServeError::SendIdReuse {
            differing: differing.into(),
        },
        RunError::CompactionContextLimitMissing => ServeError::InvalidParams {
            field: "context_limit".into(),
            message: error.to_string(),
        },
        error => ServeError::Open(error.to_string(), Some(class)),
    };
    let refusal = admission_refusal(&served).expect("run admission errors have a wire refusal");
    let (code, message, retry_after_secs, resets_at_ms) = match refusal {
        AdmissionRefusal::AccountSelection(body) => (
            body.code,
            body.message,
            body.detail.retry_after_secs,
            body.detail.resets_at_ms,
        ),
        AdmissionRefusal::RoutingUnavailable(body) => (body.code, body.message, None, None),
        AdmissionRefusal::Other(body) => (body.code, body.message, None, None),
    };
    ProviderError {
        class,
        message,
        provider_code: Some(code),
        retry_after_secs,
        resets_at_ms,
        status: None,
        provider_subcode: None,
        refusal: None,
        resets_at_source: None,
        limit_window: None,
        account_ref: None,
        credential_id: None,
        affordable_max_tokens: None,
        provider_details: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queued_account_refusal_preserves_its_class_wire_code_message_and_retry_hints() {
        for class in [
            ErrorClass::Transient,
            ErrorClass::Permanent,
            ErrorClass::Auth,
            ErrorClass::ContextOverflow,
        ] {
            let error = queued_admission_error(RunError::AccountSelectionRefused(
                broca_core::AccountSelectionRefusal {
                    routing_code: "account_hold".into(),
                    retry_after_s: Some(17),
                    resets_at_ms: Some(123_456),
                    routing_detail: Some(Box::new(json!({"future": "kept on the direct reply"}))),
                    decision_id: Some("decision".into()),
                    class,
                },
            ));
            assert_eq!(error.class, class);
            assert_eq!(
                error.provider_code.as_deref(),
                Some("account_limit_exhausted")
            );
            assert_eq!(error.message, "the routing module refused to choose a provider account for this send (account_hold); retry after 17 s");
            assert_eq!(error.retry_after_secs, Some(17.0));
            assert_eq!(error.resets_at_ms, Some(123_456));
        }
    }

    #[test]
    fn queued_permanent_and_transient_refusals_use_the_direct_send_code_and_message() {
        let permanent = queued_admission_error(RunError::Config("unknown model".into()));
        assert_eq!(permanent.class, ErrorClass::Permanent);
        assert_eq!(permanent.provider_code.as_deref(), Some("open_failed"));
        assert_eq!(
            permanent.message,
            "open failed: run resolution failed: unknown model"
        );
        let transient = queued_admission_error(RunError::Transient("catalog offline".into()));
        assert_eq!(transient.class, ErrorClass::Transient);
        assert_eq!(transient.provider_code.as_deref(), Some("transient"));
        assert_eq!(
            transient.message,
            ServeError::Transient("catalog offline".into()).to_string()
        );
    }
}
