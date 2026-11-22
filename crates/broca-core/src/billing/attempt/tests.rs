use std::collections::BTreeMap;

use crate::billing::{BillingEvent, BillingRecord};
use broca_protocol::{caller::RunFinishReason, request::ModelHandle, RunId, StepId, Usage};
use serde_json::json;

use super::{compute_attempt_facts, AttemptEvidence, AttemptFact};
use crate::billing::tests::{
    frame, resolver, started, step, synthetic_iteration_records, terminal, usage,
};

fn attempt(
    seq: u64,
    step_id: u64,
    try_number: u32,
    outcome: &str,
    content: bool,
) -> AttemptEvidence {
    AttemptEvidence {
        seq,
        step_id,
        attempt: try_number,
        attempt_id: Some(format!("att_{seq}")),
        outcome: outcome.into(),
        http_status: content.then_some(200),
        response_bytes: if content { 512 } else { 0 },
        content_received: content,
        sent_at_ms: Some(1_000 + seq),
        recorded_at_ms: 2_000 + seq,
        served_account_ref: None,
        input_transformations: None,
    }
}

/// Each segment's usage, and the usage its attempts' shares sum to, by segment fact id.
fn segment_and_attempt_sums(
    segments: &[crate::SpendSegment],
    attempts: &[AttemptFact],
) -> (BTreeMap<String, Usage>, BTreeMap<String, Usage>) {
    let by_segment = segments
        .iter()
        .filter(|segment| !segment.warm)
        .map(|segment| (segment.fact_id.clone(), segment.usage))
        .collect();
    let mut by_attempts: BTreeMap<String, Usage> = BTreeMap::new();
    for share in attempts
        .iter()
        .flat_map(|fact| fact.usage_by_model.iter().flatten())
    {
        *by_attempts
            .entry(share.segment_fact_id.clone())
            .or_default() += share.usage;
    }
    (by_segment, by_attempts)
}

/// A step tried twice, a step its model refused and its fallback answered, and a last try that
/// got no answer before the run ended.
fn refused_and_retried_run() -> (Vec<BillingRecord>, Vec<AttemptEvidence>) {
    let refused = BillingEvent::StepRefused {
        step_id: StepId(2),
        ts_ms: Some(150),
        usage: usage(30, 1),
        served_model: None,
        usage_iterations: None,
    };
    let mut routed = step(80, 2, usage(20, 3), 0);
    if let BillingEvent::StepFinished { fallback, .. } = &mut routed.event {
        fallback.route = Some(broca_records::record::StepRoute {
            model: ModelHandle::new("anthropic", "claude-fallback"),
            reason: broca_records::record::StepRouteReason::Schedule,
        });
    }
    let records = vec![
        started(10, "run", Some(100)),
        step(40, 1, usage(10, 2), 1),
        frame(60, refused),
        routed,
        frame(
            100,
            BillingEvent::RunFinished {
                reason: RunFinishReason::Error,
                ts_ms: Some(500),
                retries_used: 1,
            },
        ),
    ];
    let attempts = vec![
        attempt(20, 1, 1, "chunk_error", true),
        attempt(30, 1, 2, "complete", true),
        attempt(50, 2, 1, "complete", true),
        attempt(70, 2, 1, "complete", true),
        attempt(90, 3, 1, "connect_failed", false),
    ];
    (records, attempts)
}

#[test]
fn each_attempt_carries_its_requests_usage_and_a_segments_attempts_sum_to_it() {
    let (records, attempts) = refused_and_retried_run();
    let facts = compute_attempt_facts(
        &records,
        &RunId("run".into()),
        &attempts,
        "instance-a",
        &resolver,
    );
    assert_eq!(facts.attempts.len(), 5, "one fact per attempt");
    assert!(facts
        .segments
        .iter()
        .all(|segment| segment.usage_in_attempts));

    let shares: Vec<_> = facts
        .attempts
        .iter()
        .map(|fact| {
            (
                fact.step_id,
                fact.model.as_str(),
                fact.usage_by_model.as_ref().map(|shares| {
                    shares
                        .iter()
                        .map(|share| (share.model.as_str(), share.usage))
                        .collect::<Vec<_>>()
                }),
            )
        })
        .collect();
    assert_eq!(
        shares,
        vec![
            (1, "claude", None),
            (1, "claude", Some(vec![("claude", usage(10, 2))])),
            (2, "claude", Some(vec![("claude", usage(30, 1))])),
            (
                2,
                "claude-fallback",
                Some(vec![("claude-fallback", usage(20, 3))])
            ),
            (3, "claude", None),
        ],
        "a retried try and a try with no answer carry no usage; the refused try is billed to \
         the model that refused it, and the next try is sent to the fallback model"
    );

    let (by_segment, by_attempts) = segment_and_attempt_sums(&facts.segments, &facts.attempts);
    assert_eq!(by_segment, by_attempts);
}

#[test]
fn an_attempt_that_got_no_answer_names_no_usage_at_all() {
    let (records, attempts) = refused_and_retried_run();
    let facts = compute_attempt_facts(
        &records,
        &RunId("run".into()),
        &attempts,
        "instance-a",
        &resolver,
    );
    let unanswered = serde_json::to_value(&facts.attempts[4]).unwrap();
    assert_eq!(unanswered["kind"], "model_attempt");
    assert_eq!(unanswered["content_received"], false);
    assert_eq!(unanswered["response_bytes"], 0);
    assert!(unanswered.get("usage_by_model").is_none(), "{unanswered}");
    assert!(unanswered.get("http_status").is_none(), "{unanswered}");
    assert_eq!(unanswered["reasoning_included_in_output"], true);
    assert_eq!(unanswered["requested_service_tier"], "standard");
}

#[test]
fn a_request_handed_on_inside_the_provider_is_one_attempt_with_each_models_share() {
    let mut records = synthetic_iteration_records(["claude-opus-5-5", "claude-sonnet-5"], 0);
    for (index, framed) in records.iter_mut().enumerate() {
        framed.seq = (index as u64 + 1) * 10;
    }
    let attempts = vec![attempt(15, 1, 1, "complete", true)];
    let facts = compute_attempt_facts(
        &records,
        &RunId("synthetic-run".into()),
        &attempts,
        "instance-a",
        &resolver,
    );
    let shares = facts.attempts[0].usage_by_model.as_ref().unwrap();
    assert_eq!(
        shares
            .iter()
            .map(|share| share.model.as_str())
            .collect::<Vec<_>>(),
        vec!["claude-opus-5-5", "claude-sonnet-5"]
    );
    assert_ne!(
        shares[0].segment_fact_id, shares[1].segment_fact_id,
        "each model's share is counted in that model's segment"
    );
    let (by_segment, by_attempts) = segment_and_attempt_sums(&facts.segments, &facts.attempts);
    assert_eq!(by_segment, by_attempts);
}

#[test]
fn a_run_whose_usage_has_no_attempt_before_it_gets_no_attempt_facts() {
    let records = vec![
        started(10, "run", Some(100)),
        step(20, 1, usage(10, 2), 0),
        terminal(30, Some(200)),
    ];
    // The step's attempt was recorded after its end, so the usage has no attempt to go to.
    let attempts = vec![attempt(25, 1, 1, "complete", true)];
    let facts = compute_attempt_facts(
        &records,
        &RunId("run".into()),
        &attempts,
        "instance-a",
        &resolver,
    );
    assert!(facts.attempts.is_empty());
    assert!(facts
        .segments
        .iter()
        .all(|segment| !segment.usage_in_attempts));
    let bytes = serde_json::to_value(&facts.segments[0]).unwrap();
    assert!(bytes.get("usage_in_attempts").is_none(), "{bytes}");
}

/// A run on a pooled API key that resolved to one key of the pool, asking for the fast tier: one
/// step served fast, one served at the standard tier, each with a cache write split by lifetime.
fn pooled_fast_run() -> (Vec<BillingRecord>, Vec<AttemptEvidence>) {
    let mut start = started(10, "pooled", Some(100));
    if let BillingEvent::RunStarted { config, .. } = &mut start.event {
        config.frozen_render_config = json!({"service_tier": "fast"});
        let selection = config.auth_selection.as_mut().unwrap();
        selection.credential_id = "apikey:anthropic".into();
        selection.resolved_credential_id = Some("apikey:anthropic-pool-key-2".into());
        selection.account_ref = Some("acct2".into());
    }
    let split = |input: u64, output: u64| Usage {
        cache_write_tokens: Some(3_000),
        cache_write_5m_tokens: Some(1_000),
        cache_write_1h_tokens: Some(2_000),
        cached_input_tokens: Some(40_000),
        ..usage(input, output)
    };
    let mut fast = step(30, 1, split(12, 400), 0);
    let mut standard = step(50, 2, split(9, 300), 0);
    for (framed, tier, speed) in [
        (&mut fast, "priority", "fast"),
        (&mut standard, "standard", "standard"),
    ] {
        if let BillingEvent::StepFinished {
            service_tier_used,
            speed_used,
            ..
        } = &mut framed.event
        {
            *service_tier_used = Some(tier.into());
            *speed_used = Some(speed.into());
        }
    }
    let records = vec![start, fast, standard, terminal(60, Some(300))];
    let mut attempts = vec![
        attempt(20, 1, 1, "complete", true),
        attempt(40, 2, 1, "complete", true),
    ];
    for attempt in &mut attempts {
        attempt.served_account_ref = Some("acct2".into());
    }
    (records, attempts)
}

/// A ChatGPT-login run of two steps on `gpt-6.2-sol`. Before step 1's model ran, the provider
/// removed two reasoning items whose earlier context had changed; step 2's response reported no
/// change.
fn reasoning_removed_run() -> (Vec<BillingRecord>, Vec<AttemptEvidence>) {
    let mut start = started(10, "reasoning", Some(100));
    if let BillingEvent::RunStarted { config, .. } = &mut start.event {
        **config = crate::billing::tests::config("openai", "gpt-6.2-sol", Some("chatgpt:openai"));
    }
    let records = vec![
        start,
        step(20, 1, usage(40, 900), 0),
        step(40, 2, usage(30, 200), 0),
        terminal(50, Some(300)),
    ];
    let removed = |id: &str| broca_protocol::InputTransformation {
        kind: Some("reasoning".into()),
        path: None,
        id: Some(id.into()),
        action: Some("removed".into()),
        reason: Some("prefix_mismatch".into()),
    };
    let mut attempts = vec![
        attempt(15, 1, 1, "complete", true),
        attempt(30, 2, 1, "complete", true),
    ];
    attempts[0].input_transformations = Some(vec![removed("rs_1"), removed("rs_2")]);
    attempts[1].input_transformations = Some(Vec::new());
    (records, attempts)
}

/// An attempt's fact carries the provider's input-transformation report verbatim: each removed
/// item when there were some, an empty list for a report of no change, and nothing at all when
/// there was no report, so the three never read alike.
#[test]
fn an_attempt_fact_carries_its_reported_input_transformations() {
    let (records, mut attempts) = reasoning_removed_run();
    attempts.push(attempt(45, 3, 1, "complete", true));
    let facts = compute_attempt_facts(
        &records,
        &RunId("reasoning".into()),
        &attempts,
        "instance-a",
        &resolver,
    );
    let facts: Vec<serde_json::Value> = facts
        .attempts
        .iter()
        .map(|fact| serde_json::to_value(fact).unwrap())
        .collect();
    assert_eq!(
        facts[0]["input_transformations"],
        json!([
            {"type": "reasoning", "id": "rs_1", "action": "removed", "reason": "prefix_mismatch"},
            {"type": "reasoning", "id": "rs_2", "action": "removed", "reason": "prefix_mismatch"},
        ])
    );
    assert_eq!(facts.len(), 3, "{facts:?}");
    assert_eq!(facts[1]["input_transformations"], json!([]));
    assert!(
        facts[2].get("input_transformations").is_none(),
        "{}",
        facts[2]
    );
}

/// Writes the facts of the runs above, as the usage export stores them, to
/// `$BROCA_ATTEMPT_FACT_SAMPLES` for the export's readers to check: each run's segments and
/// attempts in export order, one fact per line.
#[test]
fn write_attempt_fact_samples_when_requested() {
    let Some(dir) = std::env::var_os("BROCA_ATTEMPT_FACT_SAMPLES") else {
        return;
    };
    let dir = std::path::PathBuf::from(dir);
    std::fs::create_dir_all(&dir).unwrap();
    let (records, attempts) = refused_and_retried_run();
    let mut handed_on = synthetic_iteration_records(["claude-opus-5-5", "claude-sonnet-5"], 0);
    for (index, framed) in handed_on.iter_mut().enumerate() {
        framed.seq = (index as u64 + 1) * 10;
    }
    let unmatched = vec![
        started(10, "run", Some(100)),
        step(20, 1, usage(10, 2), 0),
        terminal(30, Some(200)),
    ];
    let cases = [
        (
            "retried-refused-and-unanswered",
            records,
            RunId("run".into()),
            attempts,
        ),
        (
            "handed-on-inside-one-request",
            handed_on,
            RunId("synthetic-run".into()),
            vec![attempt(15, 1, 1, "complete", true)],
        ),
        {
            let (records, attempts) = pooled_fast_run();
            (
                "pooled-key-fast-tier-cache-split",
                records,
                RunId("pooled".into()),
                attempts,
            )
        },
        (
            "no-attempt-before-the-usage-so-unmarked",
            unmatched,
            RunId("run".into()),
            vec![attempt(25, 1, 1, "complete", true)],
        ),
        {
            let (records, attempts) = reasoning_removed_run();
            (
                "reasoning-removed-before-the-model-ran",
                records,
                RunId("reasoning".into()),
                attempts,
            )
        },
    ];
    for (name, records, run_id, attempts) in cases {
        let facts = compute_attempt_facts(&records, &run_id, &attempts, "instance-a", &resolver);
        // One fact per line, in the exact bytes the usage export stores.
        let mut lines: Vec<String> = facts
            .segments
            .iter()
            .map(|segment| serde_json::to_string(segment).unwrap())
            .collect();
        lines.extend(
            facts
                .attempts
                .iter()
                .map(|fact| serde_json::to_string(fact).unwrap()),
        );
        let bytes = lines.join("\n") + "\n";
        std::fs::write(dir.join(format!("{name}.jsonl")), bytes).unwrap();
    }
}
