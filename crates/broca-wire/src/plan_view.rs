//! The as-planned session inspection contract, `session.plan_view`, served with the llm-runner
//! contract crate's own types under the names broca's handlers use.

pub use cortexkit_role_llm_runner::plan_view::{
    NewestStep as PlanViewStep, Pending as PlanViewPending, PlanView as PlanViewResponse,
    PlanViewRequest as PlanViewParams, System as PlanViewSystem, SystemItem as PlanViewSystemItem,
    Tool as PlanViewTool, ToolTokens as PlanViewTokens,
};

pub const OP_SESSION_PLAN_VIEW: &str = cortexkit_role_llm_runner::ops::SESSION_PLAN_VIEW;

#[cfg(test)]
mod tests {
    use super::*;

    /// The bytes broca served with its own local types before the contract crate had them.
    /// The expected object is written out by hand, so a member the contract renames or a
    /// default that starts serializing turns this red.
    #[test]
    fn plan_view_serializes_as_broca_served_it_before_the_contract_types() {
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
        let reply = PlanViewResponse::new(
            3,
            PlanViewSystem::new("é\n猫|ok").with_items(vec![
                PlanViewSystemItem::new("first", 0, 2).with_preset("p"),
                PlanViewSystemItem::new("second", 3, 6),
                PlanViewSystemItem::new("third", 7, 9),
            ]),
            vec![PlanViewTool::new(
                "tool",
                "description",
                serde_json::json!({"type":"object"}),
                "tools",
            )
            .with_tokens(PlanViewTokens::estimate(4))],
        )
        .with_newest_step(PlanViewStep::new(120).with_reply_mid("reply-1"))
        .with_pending(
            PlanViewPending::new(4)
                .with_notes(vec!["pending change".into()])
                .unwrap(),
        );
        reply.check().unwrap();
        assert_eq!(serde_json::to_value(&reply).unwrap(), expected);
        let reply = reply.with_applied_at_ms(100);
        expected["applied_at_ms"] = serde_json::json!(100);
        assert_eq!(serde_json::to_value(&reply).unwrap(), expected);
    }

    #[test]
    fn plan_view_unknown_times_and_pending_notes_are_absent_not_null() {
        let reply = PlanViewResponse::new(0, PlanViewSystem::new("known text"), vec![])
            .with_pending(PlanViewPending::new(1));
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
