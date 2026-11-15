use super::*;
use broca_core::provider::TemperaturePolicy;

tokio::task_local! {
    pub static REQUESTED: Option<f64>;
    pub static POLICY: TemperaturePolicy;
}

#[tokio::test]
async fn temperature_effective_and_legacy_values_resume_unchanged() {
    let scenario = Scenario {
        input: vec![user("hello")],
        tools: vec![],
        stop_when: vec![],
        manifest: None,
        steps: vec![text_step("a1", "hi")],
    };
    for (requested, policy, expected) in [
        (Some(2.0), TemperaturePolicy::default(), Some(1.0)),
        (
            Some(0.1),
            TemperaturePolicy {
                supported: Some(false),
                ..Default::default()
            },
            None,
        ),
        // This start has no new fields and the same generation bytes an older build wrote.
        (
            Some(1.75),
            TemperaturePolicy {
                supported: Some(true),
                min: Some(0.0),
                max: Some(2.0),
            },
            Some(1.75),
        ),
    ] {
        let source = REQUESTED
            .scope(
                requested,
                POLICY.scope(policy, new_source(&scenario, None, scenario.steps.clone())),
            )
            .await;
        assert_eq!(source.outcome.requests[0].generation.temperature, expected);
        let start = source
            .records
            .iter()
            .position(|record| matches!(record, Record::RunStart(_)))
            .unwrap();
        // If the resume re-applies admission, the unsupported policy would erase the recorded
        // value. A divergent send must not replace it either.
        let changed = TemperaturePolicy {
            supported: Some(false),
            ..Default::default()
        };
        let resumed = REQUESTED
            .scope(
                Some(0.25),
                POLICY.scope(
                    changed,
                    new_from(
                        &scenario,
                        Some(source.cut(start + 1)),
                        scenario.steps.clone(),
                    ),
                ),
            )
            .await;
        assert_eq!(resumed.requests.len(), 1);
        assert_eq!(resumed.requests[0].generation.temperature, expected);
    }
}
