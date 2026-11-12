use super::*;
use broca_core::provider::TemperaturePolicy;

tokio::task_local! {
    pub static REQUESTED: Option<f64>;
    pub static POLICY: TemperaturePolicy;
}

#[tokio::test]
async fn temperature_effective_and_legacy_values_resume_unchanged_on_both_storages() {
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
        let (fresh, full, name) = REQUESTED
            .scope(
                requested,
                POLICY.scope(policy, today_from(&scenario, None, scenario.steps.clone())),
            )
            .await;
        assert_eq!(fresh.requests[0].generation.temperature, expected);
        let frames = full.framed();
        let start = frames
            .iter()
            .position(|frame| matches!(frame.record, broca_wal::WalRecord::RunStarted { .. }))
            .unwrap();
        let bytes = broca_crash_harness::cut_log(&full, start + 1, false);
        let cut = LogImage::parse(bytes.clone());
        let converted = convert_unbilled(&cut.framed(), lineage(&cut).as_deref()).unwrap();
        // If either resume re-applies admission, today's unsupported policy would erase the
        // recorded value. A divergent send must not replace it either.
        let changed = TemperaturePolicy {
            supported: Some(false),
            ..Default::default()
        };
        let (wal, _, _) = REQUESTED
            .scope(
                Some(0.25),
                POLICY.scope(
                    changed,
                    today_from(&scenario, Some((&name, &bytes)), scenario.steps.clone()),
                ),
            )
            .await;
        let session = REQUESTED
            .scope(
                Some(0.25),
                POLICY.scope(
                    changed,
                    new_from(&scenario, Some(converted), scenario.steps.clone()),
                ),
            )
            .await;
        for resumed in [wal, session] {
            assert_eq!(resumed.requests.len(), 1);
            assert_eq!(resumed.requests[0].generation.temperature, expected);
        }
    }
}
