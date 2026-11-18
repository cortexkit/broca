use super::*;
use serde_json::json;

fn facts(selection: Option<PlanModel>, run_model: Option<HeadModel>) -> HeadFacts {
    HeadFacts {
        lineage_state: SessionReadLineageState {
            last_run_id: None,
            state: SessionReadStateKind::Idle,
            reason: None,
            error: None,
            usage: None,
        },
        prompt_len: 0,
        interrupt_reason: None,
        selection,
        run_model,
        rich_ui: None,
        usage: None,
        title: None,
        mode3: false,
        updated_at: None,
        resume_not_before_ms: None,
    }
}

fn model(provider: &str, model: &str, variant: Option<&str>, default: Option<&str>) -> HeadModel {
    let mut head = HeadModel::new(provider, model);
    head.variant = variant.map(str::to_owned);
    head.default_variant = default.map(str::to_owned);
    head
}

/// A run's default effort is the variant recorded in its frozen render config, or else an effort
/// the model froze (a ChatGPT pro model's). A run that froze neither sends no effort, so it has
/// none.
#[test]
fn the_default_effort_is_the_frozen_variant_or_effort() {
    let variant = json!({"variant": {"name": "medium", "request": {"kind": "open_ai_responses", "effort": "medium"}}});
    assert_eq!(default_effort(&variant).as_deref(), Some("medium"));
    assert_eq!(
        default_effort(&json!({"reasoning_effort": "max"})).as_deref(),
        Some("max")
    );
    assert_eq!(
        default_effort(&json!({"family_id": "anthropic_messages"})),
        None
    );
}

/// The head shows a default effort only beside a model with no chosen variant, and only for
/// the model the latest run used: another selected model's default isn't known yet.
#[test]
fn a_selected_model_shows_the_latest_runs_default_only_when_it_is_that_model() {
    let run = || Some(model("openai", "gpt-6.2", None, Some("medium")));
    let head = |selection| head_reply(facts(selection, run())).model.unwrap();

    let same = head(Some(PlanModel::new("openai", "gpt-6.2")));
    assert_eq!(same.default_variant.as_deref(), Some("medium"));

    let mut chosen = PlanModel::new("openai", "gpt-6.2");
    chosen.variant = Some("high".into());
    let chosen = head(Some(chosen));
    assert_eq!(
        (chosen.variant.as_deref(), chosen.default_variant),
        (Some("high"), None)
    );

    let other = head(Some(PlanModel::new("openai", "gpt-6.2-astra")));
    assert_eq!(other.default_variant, None);

    let unselected = head(None);
    assert_eq!(unselected.default_variant.as_deref(), Some("medium"));
}
