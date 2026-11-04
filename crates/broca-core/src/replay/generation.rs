use broca_protocol::{usage::GenerationUsage, StepId, Usage};
use broca_wal::WalRecord;

#[derive(Debug, Clone)]
pub(super) struct AttemptTiming {
    pub step_id: StepId,
    pub sent: Option<u64>,
    pub first: Option<u64>,
    pub last: Option<u64>,
}

pub(super) fn record(
    entries: &mut Vec<GenerationUsage>,
    attempt: Option<AttemptTiming>,
    record: &WalRecord,
    by_model: &[(&str, &str, Option<&str>, Usage)],
) {
    let WalRecord::ModelStepFinished {
        step_id,
        service_tier_used,
        speed_used,
        ..
    } = record
    else {
        return;
    };
    let Some(attempt) = attempt.filter(|attempt| attempt.step_id == *step_id) else {
        return;
    };
    let (Some(sent), Some(first), Some(last)) = (attempt.sent, attempt.first, attempt.last) else {
        return;
    };
    let (Some(_), Some(sent_to_last), Some(first_to_last)) = (
        first.checked_sub(sent),
        last.checked_sub(sent),
        last.checked_sub(first),
    ) else {
        return;
    };
    let Some((_, model, _, _)) = by_model.first() else {
        return;
    };
    // A server handoff may bill several models, but the attempt has only one pair
    // of content timestamps. Assigning that whole interval to each model would
    // invent per-model throughput, so mixed-model calls have no sample.
    if by_model.iter().any(|(_, other, _, _)| other != model) {
        return;
    }
    let Some(output_tokens) = by_model.iter().try_fold(0u64, |total, (_, _, _, usage)| {
        Some(total + usage.output_tokens?)
    }) else {
        return;
    };
    let reasoning_tokens = by_model.iter().try_fold(0u64, |total, (_, _, _, usage)| {
        Some(total + usage.reasoning_tokens?)
    });
    if let Some(entry) = entries.iter_mut().find(|entry| {
        entry.model == *model
            && entry.service_tier == *service_tier_used
            && entry.speed == *speed_used
    }) {
        entry.model_calls += 1;
        entry.output_tokens += output_tokens;
        entry.reasoning_tokens = entry
            .reasoning_tokens
            .zip(reasoning_tokens)
            .map(|(a, b)| a + b);
        entry.sent_to_last_content_ms += sent_to_last;
        entry.first_to_last_content_ms += first_to_last;
    } else {
        entries.push(GenerationUsage {
            model: (*model).to_owned(),
            service_tier: service_tier_used.clone(),
            speed: speed_used.clone(),
            model_calls: 1,
            output_tokens,
            reasoning_tokens,
            sent_to_last_content_ms: sent_to_last,
            first_to_last_content_ms: first_to_last,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replay::RunUsageProjection;
    use broca_protocol::RunId;
    use serde_json::{json, Value};

    fn projection() -> RunUsageProjection {
        let mut projection = RunUsageProjection::new(RunId("run".into()));
        projection.observe(&super::super::projection_boundary_tests::run_started(1, "run").record);
        projection
    }

    fn attempt(step: u64, ordinal: u32, times: [Option<u64>; 3]) -> WalRecord {
        serde_json::from_value(json!({
            "type":"model_attempt_finished", "step_id":step, "attempt":ordinal,
            "request_body_len":0, "request_body_sha256":"00", "response_body_len":0,
            "response_body_sha256":"00", "outcome":"complete", "ts_ms":999999,
            "sent_at_ms":times[0], "first_content_at_ms":times[1], "last_content_at_ms":times[2]
        }))
        .unwrap()
    }

    fn step(id: u64, extra: Value) -> WalRecord {
        let mut value = json!({
            "type":"model_step_finished", "step_id":id,
            "assistant_message":{"message_id":format!("m{id}"),"content":[]},
            "usage":{"output_tokens":30,"reasoning_tokens":10}, "retries_used":0,
            "service_tier_used":"standard", "speed_used":"fast"
        });
        value
            .as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        serde_json::from_value(value).unwrap()
    }

    fn measured(projection: &mut RunUsageProjection, id: u64, extra: Value) {
        projection.observe(&attempt(id, 0, [Some(1000), Some(1040), Some(1100)]));
        projection.observe(&step(id, extra));
    }

    #[test]
    fn generation_folds_steps_and_only_the_final_retry() {
        let mut projection = projection();
        projection.observe(&attempt(1, 0, [Some(0), Some(1), Some(900)]));
        projection.observe(&attempt(1, 1, [Some(1000), Some(1040), Some(1100)]));
        projection.observe(&step(1, json!({})));
        // The long tool/runner gap before the next send is not model-call time.
        projection.observe(&attempt(2, 0, [Some(10000), Some(10020), Some(10050)]));
        projection.observe(&step(
            2,
            json!({"usage":{"output_tokens":20,"reasoning_tokens":5}}),
        ));
        assert_eq!(
            serde_json::to_value(&projection.usage.generation).unwrap(),
            json!([{
                "model":"m", "service_tier":"standard", "speed":"fast", "model_calls":2,
                "output_tokens":50, "reasoning_tokens":15,
                "sent_to_last_content_ms":150, "first_to_last_content_ms":90
            }])
        );
        assert_eq!(projection.usage.output_tokens, Some(50));
    }

    #[test]
    fn generation_keys_include_served_model_tier_and_speed() {
        let mut projection = projection();
        measured(&mut projection, 1, json!({"served_model":"fallback"}));
        measured(&mut projection, 2, json!({"service_tier_used":"priority"}));
        measured(&mut projection, 3, json!({"speed_used":"standard"}));
        measured(
            &mut projection,
            4,
            json!({"service_tier_used":null,"speed_used":null}),
        );
        let entries = serde_json::to_value(&projection.usage.generation).unwrap();
        assert_eq!(entries.as_array().unwrap().len(), 4);
        assert_eq!(entries[0]["model"], "fallback");
        assert_eq!(entries[1]["service_tier"], "priority");
        assert_eq!(entries[2]["speed"], "standard");
        assert!(entries[3].get("speed").is_none());
        assert!(entries[3].get("service_tier").is_none());
    }

    #[test]
    fn generation_uses_billing_model_split_but_excludes_mixed_model_timing() {
        let mut projection = projection();
        measured(
            &mut projection,
            1,
            json!({"served_model":"fallback", "usage_iterations":[
                {"type":"message","model":"fallback","output_tokens":2,"output_tokens_details":{"thinking_tokens":1}},
                {"type":"fallback_message","model":"fallback","output_tokens":8,"output_tokens_details":{"thinking_tokens":3}}
            ]}),
        );
        let before = projection.usage.generation.clone();
        assert_eq!(before[0].model, "fallback");
        assert_eq!(before[0].output_tokens, 10);
        assert_eq!(before[0].reasoning_tokens, Some(4));
        measured(
            &mut projection,
            2,
            json!({"served_model":"fallback", "usage_iterations":[
                {"type":"message","model":"m","output_tokens":2},
                {"type":"fallback_message","model":"fallback","output_tokens":8}
            ]}),
        );
        assert_eq!(projection.usage.generation, before);
        assert_eq!(
            projection.usage.output_tokens,
            Some(20),
            "billing still counts both models"
        );
    }

    #[test]
    fn generation_excludes_missing_reversed_and_unreported_measurements() {
        for times in [
            [None, Some(1040), Some(1100)],
            [Some(1000), None, Some(1100)],
            [Some(1000), Some(1040), None],
            [Some(1050), Some(1040), Some(1100)],
            [Some(1000), Some(1100), Some(1040)],
        ] {
            let mut projection = projection();
            projection.observe(&attempt(1, 0, times));
            projection.observe(&step(1, json!({})));
            assert!(serde_json::to_value(projection.usage)
                .unwrap()
                .get("generation")
                .is_none());
        }
        let mut projection = projection();
        measured(&mut projection, 1, json!({"usage":{}}));
        assert!(projection.usage.generation.is_empty());
        projection.observe(&step(2, json!({})));
        assert!(
            projection.usage.generation.is_empty(),
            "old logs have no attempts"
        );
        projection.observe(&attempt(3, 0, [Some(1000), Some(1040), Some(1100)]));
        projection.observe(&step(4, json!({})));
        assert!(
            projection.usage.generation.is_empty(),
            "step identities must match"
        );
    }

    #[test]
    fn generation_does_not_reuse_a_retry_or_a_previous_steps_timing() {
        let mut projection = projection();
        projection.observe(&attempt(1, 0, [Some(1000), Some(1040), Some(1100)]));
        projection.observe(&attempt(1, 1, [Some(1000), Some(1040), None]));
        projection.observe(&step(1, json!({})));
        projection.observe(&step(2, json!({})));
        assert!(projection.usage.generation.is_empty());
    }

    #[test]
    fn generation_reasoning_requires_every_counted_call_and_zero_is_measured() {
        let mut projection = projection();
        measured(&mut projection, 1, json!({}));
        measured(&mut projection, 2, json!({"usage":{"output_tokens":20}}));
        measured(&mut projection, 3, json!({}));
        let entry = &projection.usage.generation[0];
        assert_eq!(entry.model_calls, 3);
        assert_eq!(entry.output_tokens, 80);
        assert_eq!(entry.reasoning_tokens, None);
        projection.observe(&attempt(4, 0, [Some(1), Some(1), Some(1)]));
        projection.observe(&step(
            4,
            json!({"served_model":"zero","usage":{"output_tokens":0,"reasoning_tokens":0}}),
        ));
        let entry = &projection.usage.generation[1];
        assert_eq!(entry.model_calls, 1);
        assert_eq!(entry.output_tokens, 0);
        assert_eq!(entry.reasoning_tokens, Some(0));
        assert_eq!(entry.sent_to_last_content_ms, 0);
        assert_eq!(entry.first_to_last_content_ms, 0);
    }
}
