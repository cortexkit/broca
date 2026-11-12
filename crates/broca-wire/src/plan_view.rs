//! The as-planned session inspection contract. Replace these local wire types with the
//! llm-runner contract crate's types when upgrading to 0.8.40.

use serde::{Deserialize, Serialize};

pub const OP_SESSION_PLAN_VIEW: &str = "session.plan_view";

/// The session is selected by the bound route, just as it is for `session.read`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PlanViewParams {}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanViewResponse {
    pub generation: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub applied_at_ms: Option<u64>,
    /// The newest request with a recorded send time under this generation, not necessarily
    /// the request in flight. A request without a stored time cannot displace a dated one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub newest_step: Option<PlanViewStep>,
    pub system: PlanViewSystem,
    pub tools: Vec<PlanViewTool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pending: Option<PlanViewPending>,
    pub as_planned: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanViewStep {
    pub at_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_mid: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanViewSystem {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items: Option<Vec<PlanViewSystemItem>>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanViewSystemItem {
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preset: Option<String>,
    pub start: u64,
    pub end: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlanViewTool {
    pub name: String,
    pub description: String,
    pub input_schema: serde_json::Value,
    pub provider: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tokens: Option<PlanViewTokens>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanViewTokens {
    pub count: u64,
    pub exact: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanViewPending {
    pub generation: u64,
    /// Exact pending-change texts received by the model; absent when it was not told.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<Vec<String>>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn local_plan_view_serialization_matches_target_contract_shape() {
        // The expected object follows the contract's plan-view example, with pending texts
        // as an array. Keep it independent of local decoding so a changed member goes red.
        let mut expected = serde_json::json!({
            "generation":3,
            "system":{"text":"é\n猫|ok", "items":[
                {"provider":"first", "preset":"p", "start":0, "end":2},
                {"provider":"second", "start":3, "end":6},
                {"provider":"third", "start":7, "end":9}
            ]},
            "tools":[{"name":"tool", "description":"description", "input_schema":{"type":"object"}, "provider":"tools", "tokens":{"count":4,"exact":false}}],
            "newest_step":{"at_ms":120, "reply_mid":"reply-1"},
            "pending":{"generation":4, "notes":["pending change"]},
            "as_planned":true
        });
        let mut reply = PlanViewResponse {
            generation: 3,
            applied_at_ms: None,
            newest_step: Some(PlanViewStep {
                at_ms: 120,
                reply_mid: Some("reply-1".into()),
            }),
            system: PlanViewSystem {
                text: "é\n猫|ok".into(),
                items: Some(vec![
                    PlanViewSystemItem {
                        provider: "first".into(),
                        preset: Some("p".into()),
                        start: 0u64,
                        end: 2u64,
                    },
                    PlanViewSystemItem {
                        provider: "second".into(),
                        preset: None,
                        start: 3u64,
                        end: 6u64,
                    },
                    PlanViewSystemItem {
                        provider: "third".into(),
                        preset: None,
                        start: 7u64,
                        end: 9u64,
                    },
                ]),
            },
            tools: vec![PlanViewTool {
                name: "tool".into(),
                description: "description".into(),
                input_schema: serde_json::json!({"type":"object"}),
                provider: "tools".into(),
                tokens: Some(PlanViewTokens {
                    count: 4,
                    exact: false,
                }),
            }],
            pending: Some(PlanViewPending {
                generation: 4,
                notes: Some(vec!["pending change".into()]),
            }),
            as_planned: true,
        };
        assert_eq!(serde_json::to_value(&reply).unwrap(), expected);
        reply.applied_at_ms = Some(100);
        expected["applied_at_ms"] = serde_json::json!(100);
        assert_eq!(serde_json::to_value(&reply).unwrap(), expected);
    }

    #[test]
    fn plan_view_unknown_times_and_pending_notes_are_absent_not_null() {
        let reply = PlanViewResponse {
            generation: 0,
            applied_at_ms: None,
            newest_step: None,
            system: PlanViewSystem {
                text: "known text".into(),
                items: None,
            },
            tools: vec![],
            pending: Some(PlanViewPending {
                generation: 1,
                notes: None,
            }),
            as_planned: true,
        };
        let value = serde_json::to_value(&reply).unwrap();
        assert_eq!(
            value,
            serde_json::json!({
                "generation":0,"system":{"text":"known text"},"tools":[],
                "pending":{"generation":1},"as_planned":true,
            })
        );
        assert_eq!(
            serde_json::from_value::<PlanViewResponse>(value).unwrap(),
            reply
        );
    }
}
