//! Asynchronous views: assembly, the checks before applying, strip-or-defer for signed
//! reasoning, durable rendering on resume, advance notices and the ceiling.

use super::*;
use crate::compaction::{
    model_options,
    tests::{setup_record, text},
};
use crate::compaction_provider::{lineage::MessageRef, notice::ViewReady, view::ViewRange};
use broca_protocol::CallOptions;
use broca_wal::WalRecord;
use serde_json::json;

const LINEAGE: &str = "lineage-1";
const SESSION_REF: &str = "0123456789abcdef0123456789abcdef";

fn stamped(mut prompt: Vec<Message>) -> Vec<Message> {
    broca_protocol::content::stamp_ordinals(&mut prompt);
    prompt
}

fn replace_bytes(messages: &[Message]) -> String {
    serde_json::to_string(messages).unwrap()
}

fn make_view(version: u64, covers: u64, ranges: Vec<ViewRange>) -> (CompactionView, ViewReady) {
    let view = CompactionView::new(
        SESSION_REF,
        version,
        LINEAGE,
        MessageRef {
            ordinal: covers,
            mid: format!("m{covers}"),
        },
        ranges,
    );
    let ready = ViewReady::for_view(&view);
    (view, ready)
}

/// A state after Setup's empty head, as the run loop holds it at its first boundary.
fn after_setup() -> CompactionState {
    let mut state = CompactionState::default();
    state.fold(&setup_record(None));
    state
}

#[test]
fn ordinal_ranges_bound_the_work_to_the_requested_window() {
    let mut prompt = stamped(vec![text(Role::User, "old"); 150_000]);
    prompt[0].role = Role::System;
    let window = ordinal_range(&prompt, 149_800, 149_999);
    assert_eq!(window.len(), 199);
    assert_eq!(window[0].ordinal, Some(149_800));
    assert!(ordinal_range(&prompt, 150_000, u64::MAX).is_empty());
    assert_eq!(ordinal_range(&prompt, 10, 10).len(), 0);
}

#[test]
fn indexed_render_keeps_interior_system_messages_and_episode_coordinates() {
    let prompt = stamped(vec![
        text(Role::User, "hidden"),
        text(Role::System, "system"),
        text(Role::User, "kept"),
        text(Role::Assistant, "answer"),
    ]);
    let mut opts = CallOptions::new(prompt.clone());
    opts.render_episodes = std::sync::Arc::new(broca_protocol::request::RenderHistory {
        raw_ordinals: false,
        compaction_prefix_len: None,
        signed_floor: None,
        episodes: (0..4)
            .map(|i| broca_protocol::request::EpisodeRenderInput {
                prompt_start: i,
                first_user: Some(i),
                frozen_config: json!({}),
            })
            .collect(),
    });
    let (view, _) = make_view(
        2,
        1,
        vec![ViewRange::replace(
            0,
            2,
            replace_bytes(&[text(Role::User, "summary")]),
        )],
    );
    let mut state = after_setup();
    state.view = Some(AppliedView {
        view,
        choice: broca_wal::CompactionViewChoice::Applied,
    });
    let rendered = crate::compaction::model_options_indexed(&opts, &state, &[1]).unwrap();
    assert_eq!(rendered.prompt[0], prompt[1]);
    let coords: Vec<_> = rendered
        .render_episodes
        .iter()
        .map(|e| (e.prompt_start, e.first_user))
        .collect();
    assert_eq!(coords, vec![(2, Some(2)), (3, Some(3))]);
}

/// A session whose Setup inserted nothing, with Setup's thinking boundary at its first
/// message, as the run loop records Setup at the first boundary of a one-message transcript.
fn after_empty_setup() -> CompactionState {
    let mut state = after_setup();
    let applied = state.applied.as_mut().unwrap();
    applied.replacement = std::sync::Arc::new(Vec::new());
    applied.thinking_prefix_len = 1;
    state
}

/// A recorded view that does not assemble over the prompt is an error to render, never a
/// silent fallback to Setup's head without its strips. A view that passed its checks always
/// assembles again, so this takes a record no check passed: a replacement outside broca's
/// message schema, as a corrupted or foreign WAL could hold.
#[test]
fn a_recorded_view_that_does_not_assemble_is_an_error_not_a_fallback() {
    let prompt = stamped(vec![text(Role::User, "q"), text(Role::User, "q2")]);
    let (view, _) = make_view(
        2,
        0,
        vec![ViewRange::replace(
            0,
            1,
            json!([{"role":"user","content":[{"type":"image","data":"x"}]}]).to_string(),
        )],
    );
    let mut state = after_empty_setup();
    state.fold(&WalRecord::CompactionViewApplied {
        provider: "provider".into(),
        view_version: 2,
        view: broca_wal::OpaqueJson::from_json_text(serde_json::to_string(&view).unwrap()).unwrap(),
        choice: CompactionViewChoice::Applied,
    });
    let error = model_options(&CallOptions::new(prompt), &state).unwrap_err();
    assert_eq!(error.view_version, 2);
    assert!(matches!(error.problem, AssemblyProblem::Rejected(_)));
}

fn routed_selection(account_ref: &str) -> broca_wal::AuthSelection {
    broca_wal::AuthSelection {
        billing_vault_facts: None,
        credential_id: format!("vault-cred-{account_ref}"),
        account_ref: Some(account_ref.to_owned()),
        resolved_credential_id: Some(format!("vault-cred-{account_ref}")),
        vault_request: None,
        account_decision: Some(broca_wal::AccountDecision {
            conversation_key: None,
            conversation_key_source: None,
            path: broca_wal::ACCOUNT_DECISION_ROUTED.to_owned(),
            admission_id: Some("admission".into()),
            decision_id: Some("decision".into()),
        }),
        auth_method: Some("oauth".into()),
    }
}

fn static_selection() -> broca_wal::AuthSelection {
    broca_wal::AuthSelection {
        billing_vault_facts: None,
        credential_id: "oauth:anthropic".into(),
        account_ref: Some("static-acct".into()),
        resolved_credential_id: Some("vault-static".into()),
        vault_request: None,
        account_decision: Some(broca_wal::AccountDecision {
            conversation_key: None,
            conversation_key_source: None,
            path: broca_wal::ACCOUNT_DECISION_SINGLE_CANDIDATE.to_owned(),
            admission_id: Some("admission".into()),
            decision_id: None,
        }),
        auth_method: None,
    }
}

/// The account label is a truncated hash of the vault-served account id: opaque, stable,
/// and pinned here so a change to the hash changes this test, not silent bytes.
#[test]
fn account_labels_are_truncated_hashes_of_the_vault_account() {
    // SHA-256("acct-test-1"), first 32 hex characters (128 bits).
    assert_eq!(
        account_label("acct-test-1"),
        "1a0b66a4c753580e7b1710d6e6057933"
    );
    assert_eq!(
        selection_account_label(&routed_selection("acct-test-1")),
        Some("1a0b66a4c753580e7b1710d6e6057933".into())
    );
    // The static credential's account reports the same way when routing chose nothing.
    assert!(selection_account_label(&static_selection()).is_some());
}

#[path = "media_view_tests.rs"]
mod media;

#[path = "view_negotiation_tests.rs"]
mod negotiation;
