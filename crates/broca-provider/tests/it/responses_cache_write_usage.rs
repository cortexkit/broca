//! Cache-write accounting on the OpenAI Responses family, pinned to synthetic wire bodies.
//!
//! OpenAI counts both cache reads (`input_tokens_details.cached_tokens`) and cache writes
//! (`input_tokens_details.cache_write_tokens`) INSIDE `input_tokens`. broca's `Usage` keeps
//! fresh input, cache reads and cache writes disjoint, so the normalizer must subtract both
//! and surface the write. The fixtures:
//!
//! - `openai_responses_cache_write_response.sse`: inclusive input 3173, cache writes 3170,
//!   cache reads 0, output 5 and total 3178; only 3 input tokens are fresh.
//! - `codex_cache_write_zero_response.sse`: explicitly reports zero writes.
//! - `codex_reasoning_turn_response.sse`: omits the write key, so write usage must stay
//!   absent rather than become a reported zero.
//!
//! Each body runs through both terminal parsers: the typed one taken for an ordinary
//! `response.completed`, and the generic JSON one taken when the discriminator is spelled
//! with a JSON escape that the byte prefilter does not recognise.

use broca_protocol::{NormalizedEvent, ProviderControl, StepId, Usage};
use broca_provider::families::{
    openai_responses_chatgpt_frozen_config, openai_responses_frozen_config, OpenAiResponses,
    OpenAiResponsesChatgpt,
};
use broca_provider::wire::WireFamily;

const METERED_WRITE: &str = include_str!("../fixtures/openai_responses_cache_write_response.sse");
const CODEX_ZERO_WRITE: &str = include_str!("../fixtures/codex_cache_write_zero_response.sse");
const CODEX_NO_WRITE_KEY: &str = include_str!("../fixtures/codex_reasoning_turn_response.sse");

#[derive(Clone, Copy)]
enum Terminal {
    /// The unmodified synthetic body: the terminal event takes the typed parser.
    Typed,
    /// The terminal discriminator rewritten as `response\u002ecompleted`, which is the same
    /// JSON string but misses the byte prefilter, so the generic parser handles it.
    Generic,
}

fn prepare(sse: &str, terminal: Terminal) -> String {
    match terminal {
        Terminal::Typed => sse.to_owned(),
        Terminal::Generic => {
            let escaped = sse.replace(
                r#""type":"response.completed""#,
                r#""type":"response\u002ecompleted""#,
            );
            assert_ne!(escaped, sse, "capture has a response.completed event");
            // The prefilter scans each `data:` payload; the `event:` line is not part of it.
            assert!(
                escaped
                    .lines()
                    .filter(|line| line.starts_with("data:"))
                    .all(|line| !line.contains("response.completed")),
                "no data payload may still reach the typed prefilter"
            );
            escaped
        }
    }
}

fn step_usage(family: &dyn WireFamily, model: &str, chatgpt: bool, sse: &str) -> Usage {
    let frozen = if chatgpt {
        openai_responses_chatgpt_frozen_config(model)
    } else {
        openai_responses_frozen_config(model)
    };
    let mut normalizer = family.normalizer(&frozen, StepId(1));
    let mut events = Vec::new();
    for chunk in sse.as_bytes().chunks(97) {
        events.extend(normalizer.push(chunk).expect("captured SSE normalizes"));
    }
    events.extend(normalizer.finish().expect("EOF finalization is clean"));
    let step: Vec<Usage> = events
        .iter()
        .filter_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::StepFinished { usage, .. }) => Some(*usage),
            _ => None,
        })
        .collect();
    assert_eq!(step.len(), 1, "exactly one StepFinished");
    let finished = events
        .iter()
        .find_map(|event| match event {
            NormalizedEvent::Control(ProviderControl::Finished { usage, .. }) => Some(*usage),
            _ => None,
        })
        .expect("Finished carries usage");
    assert_eq!(finished, step[0], "Finished and StepFinished agree");
    step[0]
}

fn metered_write_usage(terminal: Terminal) -> Usage {
    step_usage(
        &OpenAiResponses,
        "gpt-6-luna",
        false,
        &prepare(METERED_WRITE, terminal),
    )
}

fn codex_usage(sse: &str, terminal: Terminal) -> Usage {
    step_usage(
        &OpenAiResponsesChatgpt,
        "gpt-6-luna",
        true,
        &prepare(sse, terminal),
    )
}

/// 3173 reported input minus the 3170 written to cache leaves 3 fresh tokens.
/// Both fresh and written tokens occupy the prompt, so fill remains 3173.
fn expected_metered_write() -> Usage {
    Usage {
        input_tokens: Some(3),
        cached_input_tokens: Some(0),
        cache_write_tokens: Some(3170),
        output_tokens: Some(5),
        reasoning_tokens: Some(0),
    }
}

fn expected_codex_zero_write() -> Usage {
    Usage {
        input_tokens: Some(14),
        cached_input_tokens: Some(0),
        cache_write_tokens: Some(0),
        output_tokens: Some(6),
        reasoning_tokens: Some(0),
    }
}

fn expected_codex_no_write_key() -> Usage {
    Usage {
        input_tokens: Some(4_267),
        cached_input_tokens: Some(9_600),
        cache_write_tokens: None,
        output_tokens: Some(178),
        reasoning_tokens: Some(98),
    }
}

#[test]
fn metered_capture_splits_cache_write_out_of_fresh_input_typed_path() {
    assert_eq!(
        metered_write_usage(Terminal::Typed),
        expected_metered_write()
    );
}

#[test]
fn metered_capture_splits_cache_write_out_of_fresh_input_generic_path() {
    assert_eq!(
        metered_write_usage(Terminal::Generic),
        expected_metered_write()
    );
}

#[test]
fn codex_capture_keeps_a_reported_zero_cache_write_typed_path() {
    assert_eq!(
        codex_usage(CODEX_ZERO_WRITE, Terminal::Typed),
        expected_codex_zero_write()
    );
}

#[test]
fn codex_capture_keeps_a_reported_zero_cache_write_generic_path() {
    assert_eq!(
        codex_usage(CODEX_ZERO_WRITE, Terminal::Generic),
        expected_codex_zero_write()
    );
}

#[test]
fn codex_capture_without_the_cache_write_key_leaves_it_absent_typed_path() {
    assert_eq!(
        codex_usage(CODEX_NO_WRITE_KEY, Terminal::Typed),
        expected_codex_no_write_key()
    );
}

#[test]
fn codex_capture_without_the_cache_write_key_leaves_it_absent_generic_path() {
    assert_eq!(
        codex_usage(CODEX_NO_WRITE_KEY, Terminal::Generic),
        expected_codex_no_write_key()
    );
}
