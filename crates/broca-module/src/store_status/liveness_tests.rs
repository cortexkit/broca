use super::*;

#[test]
fn step_liveness_wait_only_after_the_first_model_step() {
    let status = |model_started, state| {
        let facts = RunFacts {
            wait: Default::default(),
            state,
            model_started,
            open_tools: false,
            admission_error_detail: None,
            interrupt_reason: None,
            wake_directive: false,
            final_message: None,
        };
        serde_json::to_value(status_response(Some(facts), "run", None).unwrap()).unwrap()
    };
    let mut state = fresh_state(RunIndexState::Active);
    state.ride_wait_deadline_ms = Some(1_795_000_000_000);
    let before_step = status(false, state.clone());
    assert!(before_step.get("step").is_none());
    let after_step = status(true, state);
    assert_eq!(after_step["state"], "active");
    assert_eq!(after_step["reason"], "compaction_wait");
    assert_eq!(after_step["deadline_ms"], 1_795_000_000_000u64);
    assert_eq!(
        after_step["step"],
        serde_json::json!({ "phase": "waiting" })
    );
}
