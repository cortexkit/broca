use super::*;

pub(crate) fn current_temperature(
    model: &broca_protocol::request::ModelHandle,
    frozen: &serde_json::Value,
    requested: f64,
) -> Option<Option<f64>> {
    let supported = match model.model_id.as_str() {
        "temperature-clamped" => true,
        "temperature-omitted" => false,
        _ => return None,
    };
    let policy = broca_core::provider::TemperaturePolicy {
        supported: Some(supported),
        min: Some(0.0),
        max: Some(1.0),
    };
    Some(
        match serde_json::from_value::<broca_provider::wire::FrozenRenderConfig>(frozen.clone()) {
            Ok(frozen) => {
                broca_provider::families::admit_temperature(&frozen, policy, Some(requested))
            }
            Err(_) => policy.apply(Some(requested)),
        },
    )
}
fn clamped(request: &mut SubmissionRequest) {
    request.model = broca_protocol::request::ModelHandle::new("xai", "temperature-clamped");
    request.generation.temperature = Some(2.0);
}

fn omitted(request: &mut SubmissionRequest) {
    request.model = broca_protocol::request::ModelHandle::new("xai", "temperature-omitted");
    request.generation.temperature = Some(0.1);
}

fn missing(request: &mut SubmissionRequest) {
    request.model = broca_protocol::request::ModelHandle::new("xai", "temperature-missing");
    request.generation.temperature = Some(2.0);
}

async fn repeated_temperature_joins(
    tweak: fn(&mut SubmissionRequest),
    frozen: Option<serde_json::Value>,
) {
    let answers = compare_with_render(
        hello(),
        true,
        vec![
            tweaked(send("r1", "first"), tweak),
            tweaked(steered(send("s1", "joining")), tweak),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
        frozen,
    )
    .await;
    assert!(
        matches!(answers[0], SendOutcome::Active { .. }),
        "{:?}",
        answers[0]
    );
    assert!(
        matches!(answers[1], SendOutcome::Active { .. }),
        "{:?}",
        answers[1]
    );
}

#[tokio::test]
async fn temperature_join_repeating_a_clamped_request_joins_on_both_storages() {
    repeated_temperature_joins(clamped, None).await;
}

#[tokio::test]
async fn temperature_join_repeating_an_omitted_request_joins_on_both_storages() {
    let frozen = broca_provider::families::anthropic_frozen_config("thinking-model");
    repeated_temperature_joins(omitted, Some(serde_json::to_value(frozen).unwrap())).await;
}

#[tokio::test]
async fn temperature_join_missing_catalog_row_keeps_literal_comparison_on_both_storages() {
    let answers = compare(
        hello(),
        true,
        vec![
            tweaked(send("r1", "first"), missing),
            tweaked(steered(send("s1", "joining")), missing),
            Step::Release,
            Step::Release,
            Step::Idle,
        ],
    )
    .await;
    assert!(matches!(
        answers[1],
        SendOutcome::SteerSettingsConflict {
            field: "generation.temperature"
        }
    ));
}
