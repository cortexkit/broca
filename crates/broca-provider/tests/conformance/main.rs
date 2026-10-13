//! The wire-family conformance suites: every test file that renders through
//! `broca_provider::conformance::check_render_determinism`. Each former `tests/<name>.rs` is the
//! module `<name>` here.
//!
//! They are kept out of the crate's main `it` test binary because that check rewrites process-wide
//! environment variables (TZ, LANG, LC_ALL) for the length of a render. The harness serializes the
//! rewrite with its own lock, which only other conformance checks take, so any other test sharing
//! the process could read the perturbed environment or hand it to a child process.

mod anthropic_conformance;
mod bedrock_converse_conformance;
mod conformance_catches_bugs;
mod gemini_conformance;
mod generation_render;
mod google_codeassist_conformance;
mod openai_chat_conformance;
mod openai_responses_chatgpt_conformance;
mod openai_responses_conformance;
mod responses_decode;
