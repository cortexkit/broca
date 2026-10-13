//! Regression goldens for assistant text persisted before Gemini thought-part classification.

use crate::render_golden_support;

use broca_protocol::{
    content::{ContentBlock, Message, Role},
    request::{CallOptions, ModelHandle},
    StepContext,
};
use broca_provider::families::{
    gemini_frozen_config, google_codeassist_frozen_config, Gemini, GoogleCodeAssist,
    GEMINI_RENDERER_VERSION, GOOGLE_CODEASSIST_RENDERER_VERSION,
};
use broca_provider::wire::WireFamily;

const MODEL: &str = "gemini-3.5-flash-low";
const PRE_FIX_MERGED_TEXT: &str = "**Analyzing the Problem**\n\nWe need answer the multiplication question directly.\n\n17 × 23 = **391**.";
const GEMINI_GOLDEN: &[u8] = include_bytes!("../fixtures/gemini_merged_text_c7_golden.json");
const GEMINI_GOLDEN_VERSION: &str =
    include_str!("../fixtures/gemini_merged_text_c7_golden.json.version");
const CODEASSIST_GOLDEN: &[u8] =
    include_bytes!("../fixtures/google_codeassist_merged_text_c7_golden.json");
const CODEASSIST_GOLDEN_VERSION: &str =
    include_str!("../fixtures/google_codeassist_merged_text_c7_golden.json.version");

fn request() -> CallOptions {
    let mut opts = CallOptions::new(vec![
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "What is 17 × 23?".into(),
            }],
        ),
        Message::assistant_from(
            vec![ContentBlock::Text {
                text: PRE_FIX_MERGED_TEXT.into(),
            }],
            ModelHandle::new("google", MODEL),
        ),
        Message::new(
            Role::User,
            vec![ContentBlock::Text {
                text: "Now add 9.".into(),
            }],
        ),
    ]);
    opts.target_model = Some(ModelHandle::new("google", MODEL));
    opts.step_context = Some(StepContext {
        run_id: "run-merged-text-regression".into(),
        step_index: 7,
    });
    opts
}

fn assert_or_update(
    path: &str,
    version_path: &str,
    actual: &[u8],
    expected: &[u8],
    expected_version: &str,
    current_version: u32,
    name: &str,
) {
    if render_golden_support::update_if_requested(
        path,
        version_path,
        actual,
        expected,
        expected_version,
        current_version,
        name,
    ) {
        return;
    }

    render_golden_support::assert_matches_or_requires_bump(
        actual,
        expected,
        expected_version,
        current_version,
        name,
    );
}

#[test]
fn pre_fix_merged_text_renders_byte_identically_for_gemini() {
    let actual = Gemini.render(&gemini_frozen_config(MODEL), &request());
    assert_or_update(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_merged_text_c7_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/gemini_merged_text_c7_golden.json.version"
        ),
        &actual,
        GEMINI_GOLDEN,
        GEMINI_GOLDEN_VERSION,
        GEMINI_RENDERER_VERSION,
        "Gemini merged-text regression",
    );
}

#[test]
fn pre_fix_merged_text_renders_byte_identically_for_codeassist() {
    let mut frozen = google_codeassist_frozen_config(MODEL);
    frozen.project_id = Some("project-merged-text-regression".into());
    let actual = GoogleCodeAssist.render(&frozen, &request());
    assert_or_update(
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/google_codeassist_merged_text_c7_golden.json"
        ),
        concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/google_codeassist_merged_text_c7_golden.json.version"
        ),
        &actual,
        CODEASSIST_GOLDEN,
        CODEASSIST_GOLDEN_VERSION,
        GOOGLE_CODEASSIST_RENDERER_VERSION,
        "Google Code Assist merged-text regression",
    );
}
