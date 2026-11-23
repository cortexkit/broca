//! A run start's `render_identity`: stored with its must-understand mark, and joined back on
//! top of the shared render config part.

use std::sync::Arc;

use broca_protocol::request::ModelHandle;
use serde_json::json;

use super::*;
use crate::codec::Digest;

fn start(render_identity: Option<serde_json::Value>) -> RunStart {
    RunStart {
        named_speaker: None,
        kept_notice: None,
        imported_ids: None,
        render_identity,
        message_marks: None,
        run_id: "run-1".into(),
        model: ModelHandle {
            provider_module_id: "anthropic".into(),
            model_id: "claude-opus-5".into(),
        },
        setup: vec![PartRef {
            kind: PartKind::RenderConfig,
            digest: Digest([3; 32]),
        }],
        renderer_version: 3,
        submission_id: None,
        send_id: Some("s".into()),
        request_identity_digest: None,
        ts_ms: Some(0),
        written_at_ms: None,
        session_class: None,
        retention_secs: None,
        title: None,
        context_limit: None,
        service_tier: None,
        route_decision_id: None,
        compaction_plan_version: None,
        auth_selection: None,
        run_choices: None,
        origin: None,
        principal: None,
        scope: None,
        hooks: None,
        keep_warm: None,
        on_restart: None,
        seeded: None,
        refusal_schedule: None,
    }
}

fn identity() -> serde_json::Value {
    json!({
        "prompt_cache_key": "session-key",
        "claude_code_persona": {"session_id": "s-1", "prompt_id": "p-7"},
    })
}

fn must_bit(bytes: &[u8], number: u32) -> Option<bool> {
    crate::codec::Fields::new(bytes, "test")
        .map(Result::unwrap)
        .find(|field| field.number == number)
        .map(|field| field.must)
}

/// The identity round-trips with field 32's must-understand bit, so a reader that predates it
/// refuses the run start rather than render without the run's identity. A start without one
/// writes no field, so its bytes are what they were before the field existed.
#[test]
fn render_identity_round_trips_with_its_must_understand_mark() {
    let record = Record::RunStart(Box::new(start(Some(identity()))));
    let body = record.encode_body();
    assert_eq!(must_bit(&body, 32), Some(true));
    assert_eq!(Record::decode(record.kind(), &body).unwrap(), record);

    let without = Record::RunStart(Box::new(start(None)));
    assert_eq!(must_bit(&without.encode_body(), 32), None);
}

/// Without an identity the part is the whole config, and the very same value comes back.
#[test]
fn a_start_without_an_identity_reads_its_part_as_the_whole_config() {
    let part = Arc::new(json!({"family_id": "anthropic_messages", "prompt_cache_key": "k"}));
    let config = start(None).render_config(Arc::clone(&part)).unwrap();
    assert!(Arc::ptr_eq(&config, &part));
}

/// The identity's members go on top of the part's: added where the part lacks them, and in
/// place of the part's own where both have one.
#[test]
fn the_identity_members_go_on_top_of_the_shared_part() {
    let part = Arc::new(json!({
        "family_id": "anthropic_messages",
        "wire_model_id": "claude-opus-5",
        "prompt_cache_key": "stale",
    }));
    let config = start(Some(identity())).render_config(part).unwrap();
    assert_eq!(
        *config,
        json!({
            "family_id": "anthropic_messages",
            "wire_model_id": "claude-opus-5",
            "prompt_cache_key": "session-key",
            "claude_code_persona": {"session_id": "s-1", "prompt_id": "p-7"},
        })
    );
}

/// An identity or a part that isn't a JSON object can't be joined, and says which half.
#[test]
fn a_half_that_isnt_an_object_is_refused() {
    let refused = start(Some(json!(["prompt_cache_key"])))
        .render_config(Arc::new(json!({})))
        .unwrap_err();
    assert_eq!(refused.reason, "render_identity is not a JSON object");
    let refused = start(Some(identity()))
        .render_config(Arc::new(json!(null)))
        .unwrap_err();
    assert_eq!(
        refused.reason,
        "the render config part is not a JSON object"
    );
    assert_eq!(refused.run_id, "run-1");
}
