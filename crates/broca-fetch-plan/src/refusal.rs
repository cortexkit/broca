//! Why a send carrying a plan is not admitted, as the refusal code and detail its reply carries.
//! Every refusal is decided before anything is written, so a refused send leaves the session's
//! WAL exactly as it was.

use cortexkit_role_llm_runner::errors::{self, PlanDriftDetail, ToolNameCollisionDetail};
use serde_json::{json, Value};

/// Why a provider gave no usable answer to a fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Unanswered {
    /// No answer arrived before the admission deadline.
    Timeout,
    /// The daemon knows no module by that id.
    UnknownModule,
    /// The provider refused the fetch with this code.
    Refused { code: String },
    /// A catalog reply declaration failed validation, naming its tool and member.
    InvalidReply { tool: String, member: String },
    /// An optional provider's answer no longer matches its planned tools or composition.
    PlanStale,
}

impl Unanswered {
    /// The `reason` member of a `fetch_unavailable` detail or an absent item.
    pub fn reason(&self) -> &'static str {
        match self {
            Self::Timeout => "timeout",
            Self::UnknownModule => "unknown_module",
            Self::Refused { .. } => "refused",
            Self::InvalidReply { .. } => "refused",
            Self::PlanStale => "plan_stale",
        }
    }

    /// The provider's own refusal code, present only for a refusal.
    pub fn provider_code(&self) -> Option<&str> {
        match self {
            Self::Refused { code } => Some(code),
            Self::InvalidReply { .. } => Some("invalid_answer"),
            _ => None,
        }
    }

    /// `{provider, reason, provider_code?}`: the provider's code is a sibling of `reason`, never
    /// folded into it, so it cannot be mistaken for the refusal's own top-level code.
    pub fn detail(&self, provider: &str) -> Value {
        let mut detail = json!({"provider": provider, "reason": self.reason()});
        if let Some(code) = self.provider_code() {
            detail["provider_code"] = json!(code);
        }
        if let Self::InvalidReply { tool, member } = self {
            detail["tool"] = json!(tool);
            detail["member"] = json!(member);
        }
        detail
    }

    /// How the manifest records an absent optional item: `timeout`, `unknown_module`,
    /// `plan_stale`, or `refused:<provider code>`.
    pub fn absence(&self) -> String {
        match self {
            Self::Refused { code } => format!("refused:{code}"),
            Self::InvalidReply { .. } => "refused:invalid_answer".into(),
            other => other.reason().to_owned(),
        }
    }

    /// The reverse of [`Unanswered::absence`]. An absence reason is an open string a later
    /// build may extend; one this build does not know is kept whole as its reason.
    pub fn from_absence(absence: &str) -> AbsenceReason {
        match absence {
            "timeout" => AbsenceReason::Known(Self::Timeout),
            "unknown_module" => AbsenceReason::Known(Self::UnknownModule),
            "plan_stale" => AbsenceReason::Known(Self::PlanStale),
            other => match other.strip_prefix("refused:") {
                Some(code) => AbsenceReason::Known(Self::Refused {
                    code: code.to_owned(),
                }),
                None => AbsenceReason::Other(other.to_owned()),
            },
        }
    }
}

/// A recorded absence reason, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AbsenceReason {
    Known(Unanswered),
    /// A reason a later build wrote, reported as it was recorded.
    Other(String),
}

/// A refused admission.
#[derive(Debug, Clone, PartialEq)]
pub enum Refusal {
    /// A send that would run under a daemon scope declared tools without a fetch plan.
    /// Tool providers require a per-call preset on scoped routes, and broca takes presets
    /// only from a plan.
    ScopedToolsRequirePlan,
    /// A preset-less system-text item names a provider that has not declared a default.
    SystemTextPresetRequired { provider: String },
    /// The send or its plan is malformed. `detail` names the offending `field`, and for a
    /// malformed subscription also its `item`, `subscription` and `problem`.
    InvalidParams { detail: Value },
    /// A required item's provider gave no usable answer before the deadline.
    FetchUnavailable {
        provider: String,
        unanswered: Unanswered,
        /// Safe provider response metadata, kept separate from the fetch reason.
        provider_detail: Option<Value>,
    },
    /// What the providers answer no longer matches the plan; each difference is internally
    /// tagged with `kind`.
    PlanStale { differences: Vec<Value> },
    /// Two providers offer a tool under the same model-facing name.
    ToolNameCollision {
        name: String,
        providers: Vec<String>,
    },
    /// A provider would render system text for a model other than the one the caller requested,
    /// giving the session persona guidance meant for the wrong model.
    PlanModelMismatch {
        provider: String,
        plan_model: Value,
        send_model: Value,
    },
    /// The next episode cannot apply its pending refresh to the resolved model.
    PendingPlanModelMismatch {
        generation: u64,
        mismatch: Box<Refusal>,
    },
    /// The plan's compaction provider answered `role.describe`, and the answer lacks the
    /// `compaction-provider/v1` major or one of its required ops. The provider was reached and
    /// said what it serves, so sending the plan again cannot help: the code is the runner role's
    /// non-retryable `invalid_params`, naming `plan.compaction_item`, the provider and each
    /// problem. A provider that could not be asked is `FetchUnavailable` instead.
    CompactionRoleNotServed {
        provider: String,
        problems: Vec<Value>,
    },
    /// A later send's plan is not the one the session froze. `frozen` is absent when the session
    /// was admitted without a plan.
    PlanDrift {
        frozen: Option<String>,
        sent: String,
    },
}

impl Refusal {
    /// `invalid_params` naming `field`.
    pub fn invalid(field: impl Into<String>) -> Self {
        Self::InvalidParams {
            detail: json!({"field": field.into()}),
        }
    }

    /// The refusal code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::PendingPlanModelMismatch { mismatch, .. } => mismatch.code(),
            Self::ScopedToolsRequirePlan => "scoped_tools_require_plan",
            Self::SystemTextPresetRequired { .. } => "system_text_preset_required",
            Self::InvalidParams { .. } | Self::CompactionRoleNotServed { .. } => {
                errors::INVALID_PARAMS
            }
            Self::FetchUnavailable { .. } => errors::FETCH_UNAVAILABLE,
            Self::PlanStale { .. } => errors::PLAN_STALE,
            Self::ToolNameCollision { .. } => errors::TOOL_NAME_COLLISION,
            Self::PlanDrift { .. } => errors::PLAN_DRIFT,
            Self::PlanModelMismatch { .. } => "plan_model_mismatch",
        }
    }

    /// The refusal's detail object.
    pub fn detail(&self) -> Value {
        match self {
            Self::PendingPlanModelMismatch {
                generation,
                mismatch,
            } => {
                let mut detail = mismatch.detail();
                let fields = detail
                    .as_object_mut()
                    .expect("plan-model detail is an object");
                fields.insert("generation".into(), json!(generation));
                fields.insert("class".into(), json!("permanent"));
                detail
            }
            Self::ScopedToolsRequirePlan => json!({"class": "permanent"}),
            Self::SystemTextPresetRequired { provider } => json!({"provider": provider}),
            Self::InvalidParams { detail } => detail.clone(),
            Self::CompactionRoleNotServed { provider, problems } => json!({
                "field": "plan.compaction_item", "provider": provider, "problems": problems,
            }),
            Self::PlanModelMismatch {
                provider,
                plan_model,
                send_model,
            } => json!({
                "provider": provider, "plan_model": plan_model, "send_model": send_model,
            }),
            Self::FetchUnavailable {
                provider,
                unanswered,
                provider_detail,
            } => {
                let mut detail = unanswered.detail(provider);
                if let (Some(detail), Some(provider_detail)) = (
                    detail.as_object_mut(),
                    provider_detail.as_ref().and_then(Value::as_object),
                ) {
                    detail.extend(provider_detail.clone());
                }
                detail
            }
            Self::PlanStale { differences } => json!({"differences": differences}),
            Self::ToolNameCollision { name, providers } => {
                serde_json::to_value(ToolNameCollisionDetail {
                    name: name.clone(),
                    providers: providers.clone(),
                })
                .expect("a collision detail is JSON")
            }
            Self::PlanDrift { frozen, sent } => serde_json::to_value(PlanDriftDetail {
                frozen: frozen.clone(),
                sent: sent.clone(),
            })
            .expect("a drift detail is JSON"),
        }
    }

    /// Whether the same send may succeed when retried unchanged, as the runner role classifies
    /// the code: only a provider that did not answer may answer next time.
    pub fn retryable(&self) -> bool {
        match self {
            Self::PendingPlanModelMismatch { .. } => false,
            Self::ScopedToolsRequirePlan | Self::PlanModelMismatch { .. } => false,
            Self::SystemTextPresetRequired { .. } => false,
            _ => errors::is_retryable(self.code()),
        }
    }

    /// A one-line human-readable account, for the error body's message.
    pub fn message(&self) -> String {
        match self {
            Self::PendingPlanModelMismatch { generation, mismatch } => format!("pending refresh generation {generation}: {}", mismatch.message()),
            Self::ScopedToolsRequirePlan => "tools on a scoped session must come from a fetch plan, because tool providers require a per-call preset on scoped routes".into(),
            Self::SystemTextPresetRequired { provider } => format!("system-text provider {provider} requires a preset because it does not declare system-text-default/v1"),
            Self::PlanModelMismatch { provider, .. } => {
                format!("provider {provider} has a plan model different from the send model")
            }
            Self::InvalidParams { detail } => format!("invalid plan or send: {detail}"),
            Self::CompactionRoleNotServed { provider, .. } => format!(
                "compaction provider {provider} does not serve {}",
                cortexkit_role_compaction_provider::PROVIDES
            ),
            Self::FetchUnavailable {
                provider,
                unanswered,
                ..
            } => match unanswered.provider_code() {
                Some(code) => format!("provider {provider} refused the fetch: {code}"),
                None => format!(
                    "provider {provider} gave no answer: {}",
                    unanswered.reason()
                ),
            },
            Self::PlanStale { differences } => format!(
                "the plan no longer matches what its providers answer: {}",
                Value::Array(differences.clone())
            ),
            Self::ToolNameCollision { name, providers } => format!(
                "tool name {name} is offered by more than one provider: {}",
                providers.join(", ")
            ),
            Self::PlanDrift { frozen, sent } => match frozen {
                Some(frozen) => {
                    format!("the session froze plan {frozen}; this send carries plan {sent}")
                }
                None => format!(
                    "the session was admitted without a plan; this send carries plan {sent}"
                ),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scoped_tools_require_plan_is_permanent() {
        let refusal = Refusal::ScopedToolsRequirePlan;
        assert_eq!(refusal.code(), "scoped_tools_require_plan");
        assert_eq!(refusal.detail(), json!({"class": "permanent"}));
        assert!(!refusal.retryable());
    }

    #[test]
    fn model_mismatch_is_permanent() {
        let refusal = Refusal::PlanModelMismatch {
            provider: "prefrontal-core".into(),
            plan_model: json!({"provider": "fake", "id": "other"}),
            send_model: json!({"provider": "fake", "id": "fake"}),
        };
        assert_eq!(refusal.code(), "plan_model_mismatch");
        assert!(!refusal.retryable());
    }

    #[test]
    fn compaction_role_not_served_is_permanent_invalid_params_naming_the_provider() {
        let refusal = Refusal::CompactionRoleNotServed {
            provider: "magic-context".into(),
            problems: vec![json!({"problem": "missing_op", "op": "compaction.step"})],
        };
        assert_eq!(refusal.code(), "invalid_params");
        assert_eq!(
            refusal.detail(),
            json!({"field": "plan.compaction_item", "provider": "magic-context",
                "problems": [{"problem": "missing_op", "op": "compaction.step"}]})
        );
        assert!(!refusal.retryable());
        assert_eq!(
            refusal.message(),
            "compaction provider magic-context does not serve compaction-provider/v1"
        );
    }
}
