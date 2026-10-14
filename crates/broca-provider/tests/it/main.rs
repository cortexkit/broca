//! All of this crate's integration tests that can share one process build into this single test
//! binary. Each former `tests/<name>.rs` is the module `<name>` here, so its tests are named
//! `<name>::<test>` (run one file's tests with `cargo test -p broca-provider --test it <name>::`).
//!
//! One binary rather than one per file because every test executable is linked separately and, on
//! macOS, is assessed by the code-signing check the first time it runs, so per-file binaries made
//! the suite slow to build and slow to start.
//!
//! Two groups keep binaries of their own, because sharing a process with these tests would change
//! what they check:
//! - `tests/conformance`: the render-determinism checks rewrite process-wide environment variables
//!   (TZ, LANG, LC_ALL) while they render, and only other conformance checks wait on the lock that
//!   guards that rewrite.
//! - `tests/live`: the live-provider suite. It spends real provider credit, runs only by hand, and
//!   the Rust gate leaves it out by its binary name.

#[cfg(feature = "live")]
mod family_e2e;
#[path = "../live_common/mod.rs"]
mod live_common;
mod render_golden_support;

#[cfg(feature = "live")]
mod aimock_guard;
#[cfg(feature = "live")]
mod all_families_smoke;
mod anthropic_apikey_distinctness;
mod anthropic_cross_model_reasoning;
mod anthropic_decode;
#[cfg(feature = "live")]
mod anthropic_e2e;
mod anthropic_encode;
mod anthropic_family_golden;
mod anthropic_render_golden;
mod anthropic_tool_id_alias;
mod attempt_records;
mod base_url_override;
mod bedrock_converse_golden;
#[cfg(feature = "live")]
mod bedrock_e2e;
mod bedrock_tool_id_alias;
mod cache_marker_render_view;
mod cache_policy_serde;
mod cache_warm_e2e;
mod conformance_report_is_not_vacuous;
mod empty_assistant_turn_content;
mod empty_text_drop;
mod fixture_coverage;
mod frozen_config_excludes_raw;
mod frozen_config_resume;
#[cfg(feature = "live")]
mod gemini_e2e;
mod gemini_merged_text_regression;
mod gemini_render_golden;
mod gemini_thought_parts;
mod google_codeassist_render_golden;
mod live_consent_gate;
#[cfg(feature = "live")]
mod mixed_family_concurrency_e2e;
mod normalize_fixture;
#[cfg(feature = "live")]
mod openai_chat_e2e;
mod openai_chat_golden;
mod openai_responses_chatgpt_render_golden;
#[cfg(feature = "live")]
mod openai_responses_e2e;
mod openai_responses_golden;
mod ordinal_render_invariant;
mod reasoning_content_required;
mod render_golden_version_enforcement;
mod responses_cache_write_usage;
mod service_tier_used;
#[cfg(feature = "live")]
mod stream_errors_e2e;
mod synthetic_capture_replay;
#[cfg(feature = "live")]
mod tool_arg_errors_e2e;
mod tool_name_alias;
#[cfg(feature = "live")]
mod transport_errors_e2e;
mod usage_presence;

mod chatgpt_effort_updates;
mod compaction_thinking;
mod hooks_render_golden;

#[cfg(not(feature = "live"))]
#[test]
fn live_transport_suites_need_their_feature_enabled() {
    eprintln!(
        "SKIP live transport suites: compiled without the live feature; re-run with \
         `-F broca-provider/live` to exercise the family e2e and aimock guard suites."
    );
}
