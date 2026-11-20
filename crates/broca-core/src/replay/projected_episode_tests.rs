use super::episodes_with_first_steps;
use broca_protocol::request::EpisodeRenderInput;

#[test]
fn an_episode_without_a_model_step_takes_no_other_episodes_step() {
    let render = |start: usize| EpisodeRenderInput {
        prompt_start: start,
        first_user: Some(start),
        frozen_config: serde_json::json!({}),
    };
    // The first episode ended before any model step; only the second has one.
    let projected = episodes_with_first_steps(&[render(0), render(3)], &[4]);
    assert_eq!(
        projected
            .iter()
            .map(|episode| episode.first_step)
            .collect::<Vec<_>>(),
        vec![Some(None), Some(Some(4))]
    );
}
