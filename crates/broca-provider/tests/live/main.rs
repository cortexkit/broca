//! The live-provider suite: these tests drive real provider APIs with real credentials, so they
//! run only by hand (feature `live`, with `BROCA_LIVE_TESTS` consent) and the Rust gate excludes this
//! binary by name. Each former `tests/<name>.rs` is the module `<name>` here; run one family with
//! `cargo test -p broca-provider --features live --test live <name>:: -- --nocapture`.
//!
//! It is one binary rather than one per family for the same reason as the crate's main `it`
//! binary: every test executable is linked separately and, on macOS, pays a code-signing check on
//! its first run.

#![cfg(feature = "live")]

#[path = "../live_common/mod.rs"]
mod live_common;

mod anthropic_live;
mod bedrock_live;
mod gemini_live;
mod openai_chat_live;
mod openai_compat_live;
mod openai_responses_live;
mod provider_switch_live;
