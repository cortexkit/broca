//! Sessions of different families running at the same time must not contaminate each other.
//!
//! Every other e2e suite drives one family alone, which leaves the machinery those runs
//! CONTEND for uncovered: the shared HTTP client pool, the blocking pool that request
//! rendering is offloaded to, and the catalog memo. A defect in any of those is invisible to
//! a suite that only ever has one run in flight, and shows up in production as a session
//! receiving another session's content -- a failure that looks like a model mistake rather
//! than an engine one.
//!
//! The assertion here is isolation, not timing. Each run carries a sentinel unique to it, and
//! every run must see its own and no other's. Cross-contamination is deterministic: it either
//! happened or it did not, so the test cannot flake in the conditions it exists to exercise.
//! Asserting on ordering or duration instead would make concurrency both the subject and the
//! source of noise.

use crate::family_e2e;

use broca_protocol::caller::RunFinishReason;
use broca_provider::families::{
    anthropic_frozen_config, gemini_frozen_config, openai_chat_frozen_config, AnthropicMessages,
    Gemini, OpenAiChat,
};
use family_e2e::{run_family_e2e, RunOutcome, RunSpec};

/// One run's expected content. The sentinels embed the slot index, so a result crossing
/// between two runs of the SAME family is caught as well as one crossing between families.
struct Slot {
    label: &'static str,
    tool_result: String,
    final_text: String,
}

impl Slot {
    fn new(label: &'static str, index: usize) -> Self {
        Slot {
            label,
            tool_result: format!("TOOL-RESULT-{label}-{index}"),
            final_text: format!("FINAL-TEXT-{label}-{index}"),
        }
    }

    fn fixtures(&self) -> String {
        format!(
            r#"{{
              "fixtures": [
                {{ "match": {{ "hasToolResult": false }},
                  "response": {{ "toolCalls": [{{ "name": "probe", "arguments": {{ "slot": "{label}" }} }}] }} }},
                {{ "match": {{ "hasToolResult": true }},
                  "response": {{ "content": "{final_text}" }} }}
              ]
            }}"#,
            label = self.label,
            final_text = self.final_text,
        )
    }

    fn spec(&self) -> RunSpec {
        RunSpec::new(self.fixtures())
            .prompt("Call the probe tool, then report.")
            .tool("probe", &self.tool_result)
    }
}

/// Every sentinel this run must NOT have seen: all the others, from both families and
/// sibling slots. Built by exclusion rather than listed, so adding a slot automatically
/// widens what each existing slot is checked against.
fn foreign_sentinels(slots: &[Slot], mine: usize) -> Vec<&str> {
    slots
        .iter()
        .enumerate()
        .filter(|(i, _)| *i != mine)
        .flat_map(|(_, slot)| [slot.tool_result.as_str(), slot.final_text.as_str()])
        .collect()
}

fn assert_isolated(outcome: &RunOutcome, slot: &Slot, foreign: &[&str]) {
    if let Some(err) = outcome.first_error() {
        panic!(
            "{}: unexpected provider error ({:?}): {}",
            slot.label, err.class, err.message
        );
    }
    assert_eq!(
        outcome.summary.reason,
        RunFinishReason::Completed,
        "{} completed",
        slot.label
    );

    let results = outcome.tool_result_texts();
    assert!(
        results.iter().any(|text| text == &slot.tool_result),
        "{} saw its own tool result, got {results:?}",
        slot.label
    );

    // The whole event stream, so contamination is caught wherever it lands -- a foreign
    // sentinel in display text is as much a leak as one in a tool result.
    let seen = format!("{:?}", outcome.events);
    for sentinel in foreign {
        assert!(
            !seen.contains(sentinel),
            "{} saw a sentinel belonging to another concurrent run: {sentinel}",
            slot.label
        );
    }
}

/// Five runs across three families, started together and awaited together: two of one
/// family, two of a second, one of a third. The duplicate families matter as much as the
/// mix, since a per-family cache keyed on something run-independent would only ever be hit
/// by two runs of the SAME family overlapping.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_runs_across_families_do_not_contaminate_each_other() {
    let slots = vec![
        Slot::new("anthropic", 0),
        Slot::new("anthropic", 1),
        Slot::new("openai-chat", 2),
        Slot::new("openai-chat", 3),
        Slot::new("gemini", 4),
    ];

    // Guards the exclusion check: identical sentinels across slots would make every
    // "did not see a foreign sentinel" assertion vacuously true.
    for (i, slot) in slots.iter().enumerate() {
        for other in foreign_sentinels(&slots, i) {
            assert_ne!(
                slot.tool_result, other,
                "sentinels must be unique across slots or the isolation check proves nothing"
            );
        }
    }

    let a0 = run_family_e2e(
        AnthropicMessages,
        anthropic_frozen_config("mock-model"),
        slots[0].spec(),
    );
    let a1 = run_family_e2e(
        AnthropicMessages,
        anthropic_frozen_config("mock-model"),
        slots[1].spec(),
    );
    let c2 = run_family_e2e(
        OpenAiChat,
        openai_chat_frozen_config("mock-model"),
        slots[2].spec(),
    );
    let c3 = run_family_e2e(
        OpenAiChat,
        openai_chat_frozen_config("mock-model"),
        slots[3].spec(),
    );
    let g4 = run_family_e2e(Gemini, gemini_frozen_config("mock-model"), slots[4].spec());

    // Started together rather than sequentially: the point is overlap, and awaiting each
    // before starting the next would exercise nothing this suite does not already cover.
    let (r0, r1, r2, r3, r4) = tokio::join!(a0, a1, c2, c3, g4);
    let outcomes = [r0, r1, r2, r3, r4];

    // Every run returns None when the mock server is unavailable, which happens locally
    // before its dependencies are installed. That path is safe to take because
    // `run_family_e2e` panics instead of returning None whenever the mock is REQUIRED --
    // in CI, or under BROCA_REQUIRE_AIMOCK -- so a uniform None here cannot be a silently
    // skipped test run in an environment that was supposed to exercise the mock.
    if outcomes.iter().all(Option::is_none) {
        return;
    }

    for (i, outcome) in outcomes.iter().enumerate() {
        let outcome = outcome
            .as_ref()
            .unwrap_or_else(|| panic!("slot {i} produced no outcome while its siblings ran"));
        assert_isolated(outcome, &slots[i], &foreign_sentinels(&slots, i));
    }
}
