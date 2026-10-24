//! Durable-queue crash conformance (Phase 2, B3).
//!
//! The pending set is derived purely from the durable WAL records, so it must be exactly
//! what those records justify at EVERY crash boundary — a crash before a consuming
//! `RunStarted` leaves the submission pending (re-drained on re-open), a crash after
//! makes it consumed (never double-started), and a `Retracted` removes it. We build a
//! Queued/Retracted/consume sequence, truncate at every record boundary, and assert the
//! pending set matches the records-so-far. No re-running of any side effect is possible
//! because consume/retract are pure record appends; the set algebra is the safety.

use broca_core::{pending_submissions, retract_decision, RetractDecision};
use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::{GenerationConfig, ModelHandle, SubmissionRequest, ToolChoice},
    BindIdentity, RunId, SubmissionId,
};
use broca_wal::{RunConfig, RunOrigin, WalRecord};

fn msg(text: &str) -> Message {
    Message {
        cache_prefix_blocks: None,
        role: Role::User,
        content: vec![ContentBlock::Text { text: text.into() }],
        origin: None,
        ordinal: None,
        tags: Default::default(),
    }
}

fn queued(id: &str) -> WalRecord {
    WalRecord::Queued {
        written_at_ms: None,
        retention: None,
        ts_ms: None,
        hooks: None,
        title: None,
        keep_warm: None,
        conversation_key: None,
        send_id: None,
        delivery: None,
        submission_id: SubmissionId(id.into()),
        request: Box::new(SubmissionRequest {
            directives: None,
            on_restart: None,
            keep_warm: None,
            scope: Default::default(),
            project_id: None,
            principal: None,
            model: ModelHandle::new("stub", "m"),
            input: vec![msg(id)],
            tools: Some(vec![]),
            policy_allowed: Default::default(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            cache: None,
            stop_when: vec![],
            budget: None,
            auth: None,
            context_limit: None,
            work_class: None,
            context_management: None,
            variant: None,
            service_tier: None,
            inherit_frozen: Default::default(),
            structured_output: None,
            client_message_id: None,
        }),
        request_identity_digest: None,
        principal: None,
    }
}

fn retracted(id: &str) -> WalRecord {
    WalRecord::Retracted {
        submission_id: SubmissionId(id.into()),
        failure: None,
    }
}

fn consume(run: &str, sub: &str) -> WalRecord {
    WalRecord::RunStarted {
        written_at_ms: None,
        tags: None,
        session_class: None,
        retention: None,
        hooks: None,
        title: None,
        on_restart: None,
        keep_warm: None,
        marker_order_version: None,
        manifest: None,
        scope: None,
        send_id: None,
        request_identity_digest: None,
        project_id: None,
        principal: None,
        ts_ms: None,
        run_id: RunId(run.into()),
        session: BindIdentity::new("/tmp/p", "h", "s"),
        config: Box::new(RunConfig {
            turn_directives_v1: None,
            render_tags: false,
            computed_tags: None,
            model: ModelHandle::new("stub", "m"),
            tools: vec![].into(),
            tool_choice: ToolChoice::Auto,
            generation: GenerationConfig::default(),
            response_format: Default::default(),
            provider_options: serde_json::Value::Null,
            cache_tiers: serde_json::Value::Null,
            context_epoch: 0,
            renderer_version: 1,
            frozen_render_config: serde_json::Value::Null,
            auth_selection: None,
            tools_were_omitted: false,
            budget: None,
            context_limit: None,
            work_class: None,
            variant: None,
            service_tier: None,
            stop_when: Vec::new(),
            context_management: None,
        }),
        input: vec![].into(),
        origin: RunOrigin::Fresh,
        submission_id: Some(SubmissionId(sub.into())),
    }
}

fn pending_ids(records: &[WalRecord]) -> Vec<String> {
    pending_submissions(records)
        .iter()
        .map(|p| p.submission_id.0.clone())
        .collect()
}

/// At EVERY truncation boundary of a Queued/Retracted/consume sequence, the pending set
/// equals exactly what the records so far justify: queued − retracted − consumed.
#[test]
fn pending_set_is_exactly_what_the_durable_records_justify_at_every_cut() {
    let full = [
        queued("s1"),        // 1: s1 pending
        queued("s2"),        // 2: s1,s2 pending
        retracted("s1"),     // 3: s2 pending (s1 retracted)
        queued("s3"),        // 4: s2,s3 pending
        consume("r1", "s2"), // 5: s3 pending (s2 consumed)
        consume("r2", "s3"), // 6: none pending (s3 consumed)
    ];

    // The reference expected pending set after each prefix length, computed independently.
    let expected: Vec<Vec<&str>> = vec![
        vec![],           // 0 records
        vec!["s1"],       // after Queued s1
        vec!["s1", "s2"], // after Queued s2
        vec!["s2"],       // after Retracted s1
        vec!["s2", "s3"], // after Queued s3
        vec!["s3"],       // after consume s2
        vec![],           // after consume s3
    ];

    for cut in 0..=full.len() {
        let prefix = &full[..cut];
        let got = pending_ids(prefix);
        let want: Vec<String> = expected[cut].iter().map(|s| s.to_string()).collect();
        assert_eq!(
            got, want,
            "pending set at cut {cut} must match the records so far"
        );
    }
}

/// A consume NEVER resurrects a retracted submission: once `Retracted{s}` is durable, no
/// later record (including a buggy `RunStarted{submission_id: s}`) can make `s` pending
/// again, and a retract decision for it is never `Retract`.
#[test]
fn consume_never_resurrects_a_retracted_submission() {
    // s1 queued then retracted.
    let mut recs = vec![queued("s1"), retracted("s1")];
    assert!(
        pending_ids(&recs).is_empty(),
        "s1 is retracted, not pending"
    );
    assert_eq!(
        retract_decision(&recs, &SubmissionId("s1".into())),
        RetractDecision::NotPending,
        "an already-retracted submission is NotPending, never Retract"
    );

    // A (buggy/hypothetical) consume referencing the retracted id appends a RunStarted —
    // s1 is now consumed AND retracted; it must NOT reappear as pending, and the set
    // counts it exactly once (gone).
    recs.push(consume("r1", "s1"));
    assert!(
        pending_ids(&recs).is_empty(),
        "a consume of a retracted id does not resurrect it into pending"
    );
    // (The coordinator avoids this by checking the decision under its lock before
    // consuming; even if it didn't, the set algebra keeps s1 out of pending.)
}

/// The crash-before vs crash-after-RunStarted boundary: the exact pair the durability
/// argument hinges on.
#[test]
fn crash_before_consume_pending_crash_after_consumed() {
    let before = vec![queued("s1")]; // crash before the consuming RunStarted
    assert_eq!(
        pending_ids(&before),
        vec!["s1"],
        "still pending → re-drained on re-open"
    );

    let after = vec![queued("s1"), consume("r1", "s1")]; // crash after
    assert!(
        pending_ids(&after).is_empty(),
        "consumed → never double-started"
    );
}
